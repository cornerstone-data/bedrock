"""Dispatch GitHub Actions diagnostics runs for the Step 7 EF time-series.

Two scenarios:

- ``isolate_a_matrix`` — vary only the A-matrix scaling methodology, hold
  every other config knob to v0 defaults. Reuses the four Step 6 candidate
  YAMLs.
- ``bundle_v0_2`` — full v0.2 release-candidate ensembles that bundle
  A-matrix selection with every other v0.2 change. YAMLs TBD; populate
  ``BUNDLE_V0_2_YAMLS`` when v0.2 is assembled.

Per ``(scenario, approach, year)`` cell:

1. Create a Sheet in the Drive folder ``EF_TIME_SERIES_DRIVE_FOLDER_ID`` with a
   deterministic title.
2. Trigger the ``generate_diagnostics`` workflow via ``gh workflow run`` with
   ``config_name``, ``model_base_year``, ``sheet_id``, and
   ``use_useeio_baseline=false`` (CEDA-baseline only for this starting cut).
3. Append a row to ``output/results/ef_run_index.csv`` so the compile step has
   a complete audit trail.

The script is idempotent — already-recorded ``(scenario, approach, year)`` cells
are skipped, so re-running picks up only the unfilled cells.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.dispatch_ef_time_series \\
        --git-ref main \\
        [--scenarios isolate_a_matrix,bundle_v0_2] \\
        [--years 2019,2020,2021,2022,2023] \\
        [--dry-run]
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import subprocess
import time

import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR

logger = logging.getLogger(__name__)

# v0.3 Diagnostics Drive folder — same one Step 6 used.
EF_TIME_SERIES_DRIVE_FOLDER_ID = "1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s"

EF_RUN_INDEX_PATH = RESULTS_DIR / "ef_run_index.csv"

DEFAULT_YEARS: tuple[int, ...] = (2019, 2020, 2021, 2022, 2023)

# `isolate_a_matrix`: candidate A-matrix methodologies, everything else held
# to v0 defaults. Reuses Step 6's YAMLs.
ISOLATE_A_MATRIX_YAMLS: dict[str, str] = {
    "useeio": "2025_usa_cornerstone_A_useeio",
    "summary_tables": "2025_usa_cornerstone_A_summary_tables",
    "industry_price_index": "2025_usa_cornerstone_A_industry_price_index",
    "commodity_price_index": "2025_usa_cornerstone_A_commodity_price_index",
}

# `bundle_v0_2`: full v0.2 release-candidate config. Single YAML —
# `2025_usa_cornerstone_full_model` carries all v0.2 flags (cornerstone
# 2026 schema, cornerstone GHG FBS, USEEIO B method, waste disagg). The
# `model_base_year` and `usa_ghg_data_year` overrides drive the time
# series; the YAML itself is year-agnostic.
BUNDLE_V0_2_YAMLS: dict[str, str] = {
    "full_model": "2025_usa_cornerstone_full_model",
}

SCENARIO_YAMLS: dict[str, dict[str, str]] = {
    "isolate_a_matrix": ISOLATE_A_MATRIX_YAMLS,
    "bundle_v0_2": BUNDLE_V0_2_YAMLS,
}

# Human-readable labels for sheet titles. `useeio` is renamed to make
# clear that the approach uses BEA 2017 detail benchmark A directly with
# no temporal adjustment — the same "2017 benchmark A" the USEEIO method
# applies.
APPROACH_LABELS: dict[str, str] = {
    "useeio": "A matrix with 2017 benchmark A",
    "summary_tables": "A matrix with summary tables",
    "industry_price_index": "A matrix with industry price index",
    "commodity_price_index": "A matrix with commodity price index",
    "full_model": "full v0.2 model",
}
BASELINE_LABELS: dict[str, str] = {
    "ceda": "CEDA based",
    "useeio": "USEEIO based",
}

INDEX_COLUMNS = (
    "scenario",
    "approach",
    "year",
    "baseline",
    "config_name",
    "sheet_id",
    "sheet_title",
    "useeio_box_ticked",
    "git_ref",
    "triggered_at",
)


def _drive_client():  # type: ignore[no-untyped-def]
    """Return an authenticated Drive v3 client for the active ADC."""
    import google.auth  # noqa: PLC0415
    import googleapiclient.discovery  # noqa: PLC0415

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
    return googleapiclient.discovery.build("drive", "v3", credentials=creds)


def _create_sheet(folder_id: str, title: str) -> str:
    drive = _drive_client()
    body = {
        "name": title,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
    }
    file = (
        drive.files().create(body=body, fields="id", supportsAllDrives=True).execute()
    )
    return str(file["id"])


def _trigger_workflow(
    *,
    git_ref: str,
    config_name: str,
    sheet_id: str,
    model_base_year: int,
    use_useeio_baseline: bool,
    usa_ghg_data_year: int | None = None,
) -> None:
    cmd = [
        "gh",
        "workflow",
        "run",
        "generate_diagnostics.yml",
        "--ref",
        git_ref,
        "-f",
        f"config_name={config_name}",
        "-f",
        f"sheet_id={sheet_id}",
        "-f",
        f"model_base_year={model_base_year}",
        "-f",
        f"use_useeio_baseline={'true' if use_useeio_baseline else 'false'}",
    ]
    if usa_ghg_data_year is not None:
        cmd += ["-f", f"usa_ghg_data_year={usa_ghg_data_year}"]
    subprocess.run(cmd, check=True)


def _busy_count(workflow: str = "generate_diagnostics") -> int:
    """Number of `queued` + `in_progress` runs of the named workflow."""
    count = 0
    for status in ("queued", "in_progress"):
        result = subprocess.run(
            [
                "gh",
                "run",
                "list",
                "--workflow",
                f"{workflow}.yml",
                "--status",
                status,
                "--limit",
                "20",
                "--json",
                "databaseId",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        count += len(json.loads(result.stdout))
    return count


def _wait_for_capacity(
    *,
    poll_interval: int = 15,
    timeout: int = 1800,
    workflow: str = "generate_diagnostics",
) -> None:
    """Block until no `generate_diagnostics` run is queued or in progress.

    Combined with the workflow's `concurrency:` directive, this is the
    primary serialization mechanism — guarantees we don't dispatch a new
    run while one is still in flight (which would either get cancelled by
    the directive or stack up against the Sheets API write quota).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        n = _busy_count(workflow)
        if n == 0:
            return
        logger.info("Waiting for %d in-flight %s run(s) to clear...", n, workflow)
        time.sleep(poll_interval)
    raise TimeoutError(
        f"{workflow} runs still busy after {timeout}s; aborting dispatch"
    )


def _throttle(mode: str) -> None:
    """Apply the configured throttle between successive dispatches.

    Modes:
      - ``poll``: block until the workflow has no queued or in-progress runs.
      - ``sleep:N``: sleep N seconds (e.g. ``sleep:120``).
      - ``none``: no throttle; useful only with a bumped Sheets API quota.
    """
    if mode == "poll":
        _wait_for_capacity()
    elif mode.startswith("sleep:"):
        seconds = int(mode.split(":", 1)[1])
        logger.info("Sleeping %ds before next dispatch...", seconds)
        time.sleep(seconds)
    elif mode == "none":
        return
    else:
        raise ValueError(
            f"Unknown throttle {mode!r}. Valid: 'poll', 'sleep:N', 'none'."
        )


def _load_index() -> pd.DataFrame:
    if EF_RUN_INDEX_PATH.exists():
        df = pd.read_csv(EF_RUN_INDEX_PATH)
    else:
        df = pd.DataFrame(columns=list(INDEX_COLUMNS))
    # Backfill any columns this script needs that the CSV doesn't carry.
    # The Step 6 CSV pre-dates `scenario` / `year` / `git_ref` etc.; we
    # treat missing values as "not recorded by this dispatch."
    for col in INDEX_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _append_index_row(row: dict[str, object]) -> None:
    df = _load_index()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    EF_RUN_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(EF_RUN_INDEX_PATH, index=False)


def _already_recorded(
    df: pd.DataFrame, scenario: str, approach: str, year: int, baseline: str
) -> bool:
    if df.empty:
        return False
    # `year` may be int or empty-string for legacy rows; coerce non-numeric
    # entries to NaN so the equality compare safely returns False for those.
    year_col = pd.to_numeric(df["year"], errors="coerce")
    matches = df[
        (df["scenario"] == scenario)
        & (df["approach"] == approach)
        & (year_col == year)
        & (df["baseline"] == baseline)
    ]
    return not matches.empty


def dispatch(
    *,
    git_ref: str,
    scenarios: tuple[str, ...] = ("bundle_v0_2",),
    years: tuple[int, ...] = DEFAULT_YEARS,
    use_useeio_baseline: bool = False,
    dry_run: bool = False,
    throttle: str = "poll",
) -> None:
    baseline_label = "useeio" if use_useeio_baseline else "ceda"
    today = dt.datetime.utcnow().strftime("%Y-%m-%d")

    n_planned = 0
    n_skipped = 0
    n_dispatched = 0

    for scenario in scenarios:
        if scenario not in SCENARIO_YAMLS:
            raise ValueError(
                f"Unknown scenario {scenario!r}. Valid: " f"{sorted(SCENARIO_YAMLS)}"
            )
        yamls = SCENARIO_YAMLS[scenario]
        if not yamls:
            raise ValueError(
                f"Scenario {scenario!r} has no YAMLs configured — populate "
                f"the corresponding mapping in this script before dispatching."
            )
        for approach, config_name in yamls.items():
            for year in years:
                n_planned += 1
                index_df = _load_index()
                if _already_recorded(
                    index_df, scenario, approach, year, baseline_label
                ):
                    logger.info(
                        "Skip already-recorded cell (%s, %s, %d, %s)",
                        scenario,
                        approach,
                        year,
                        baseline_label,
                    )
                    n_skipped += 1
                    continue

                approach_label = APPROACH_LABELS[approach]
                baseline_text = BASELINE_LABELS[baseline_label]
                title = (
                    f"[{today}, {year}, {baseline_text}, "
                    f"{approach_label}, {scenario}] EFs diagnostics"
                )

                if dry_run:
                    logger.info("DRY-RUN would create sheet: %s", title)
                    logger.info(
                        "DRY-RUN would dispatch: config=%s year=%d use_useeio=%s",
                        config_name,
                        year,
                        use_useeio_baseline,
                    )
                    n_dispatched += 1
                    continue

                if n_dispatched > 0:
                    _throttle(throttle)

                sheet_id = _create_sheet(EF_TIME_SERIES_DRIVE_FOLDER_ID, title)
                logger.info(
                    "Created sheet %s for (%s, %s, %d, %s)",
                    sheet_id,
                    scenario,
                    approach,
                    year,
                    baseline_label,
                )
                _trigger_workflow(
                    git_ref=git_ref,
                    config_name=config_name,
                    sheet_id=sheet_id,
                    model_base_year=year,
                    use_useeio_baseline=use_useeio_baseline,
                    usa_ghg_data_year=year,
                )
                _append_index_row(
                    {
                        "scenario": scenario,
                        "approach": approach,
                        "year": year,
                        "baseline": baseline_label,
                        "config_name": config_name,
                        "sheet_id": sheet_id,
                        "sheet_title": title,
                        "useeio_box_ticked": str(use_useeio_baseline).lower(),
                        "git_ref": git_ref,
                        "triggered_at": dt.datetime.utcnow().isoformat() + "Z",
                    }
                )
                n_dispatched += 1

    logger.info(
        "Done. planned=%d skipped=%d dispatched=%d",
        n_planned,
        n_skipped,
        n_dispatched,
    )


def re_dispatch_from_csv(
    *,
    git_ref: str,
    scenarios: tuple[str, ...] | None = None,
    throttle: str = "poll",
    dry_run: bool = False,
) -> None:
    """Re-trigger workflow runs for cells already recorded in
    ``ef_run_index.csv``. Used to recover from rate-limit batch failures —
    re-uses the existing Sheets (no new ones created), so the audit trail
    keeps the same `sheet_id`s.

    Successful runs that get re-triggered will overwrite their own data
    with deterministic identical content; harmless but wastes a few
    minutes of runner time. Filter with ``scenarios`` to scope down.
    """
    df = _load_index()
    if df.empty:
        logger.info("No rows in %s; nothing to re-dispatch", EF_RUN_INDEX_PATH)
        return

    # Re-dispatch only Step 7 rows (have non-empty scenario + year).
    has_step7 = (df["scenario"].astype(str).str.strip() != "") & (
        df["year"].astype(str).str.strip() != ""
    )
    df = df[has_step7].copy()
    if scenarios:
        df = df[df["scenario"].isin(scenarios)]
    if df.empty:
        logger.info("No matching rows to re-dispatch")
        return

    logger.info("Re-dispatching %d cells from %s", len(df), EF_RUN_INDEX_PATH)
    n_dispatched = 0
    for _, row in df.iterrows():
        if dry_run:
            logger.info(
                "DRY-RUN would re-dispatch sheet=%s config=%s year=%s",
                row["sheet_id"],
                row["config_name"],
                row["year"],
            )
            n_dispatched += 1
            continue

        if n_dispatched > 0:
            _throttle(throttle)

        _trigger_workflow(
            git_ref=git_ref,
            config_name=str(row["config_name"]),
            sheet_id=str(row["sheet_id"]),
            model_base_year=int(float(row["year"])),
            use_useeio_baseline=str(row["useeio_box_ticked"]).strip().lower() == "true",
            usa_ghg_data_year=int(float(row["year"])),
        )
        logger.info(
            "Re-dispatched sheet %s (%s × %s)",
            row["sheet_id"],
            row["approach"],
            row["year"],
        )
        n_dispatched += 1

    logger.info("Done. re-dispatched=%d", n_dispatched)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--git-ref",
        required=True,
        help="Git ref (branch or tag) to run the workflow against.",
    )
    parser.add_argument(
        "--scenarios",
        default="bundle_v0_2",
        help=(
            "Comma-separated scenarios to dispatch. Valid values: "
            "'isolate_a_matrix', 'bundle_v0_2'."
        ),
    )
    parser.add_argument(
        "--years",
        default=",".join(str(y) for y in DEFAULT_YEARS),
        help=f"Comma-separated years (default: {','.join(str(y) for y in DEFAULT_YEARS)}).",
    )
    parser.add_argument(
        "--use-useeio-baseline",
        action="store_true",
        help="Tick the USEEIO baseline box. Default is CEDA-only baseline.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without creating Sheets or triggering workflows.",
    )
    parser.add_argument(
        "--throttle",
        default="poll",
        help=(
            "How to space successive workflow triggers. 'poll' (default) "
            "blocks until prior runs clear; 'sleep:N' sleeps N seconds; "
            "'none' fires immediately (only safe with bumped Sheets API quota)."
        ),
    )
    parser.add_argument(
        "--re-dispatch-from-csv",
        action="store_true",
        help=(
            "Re-trigger workflows for cells already in ef_run_index.csv. "
            "Used to recover from rate-limit batch failures — re-uses "
            "existing Sheets, no new ones created."
        ),
    )
    args = parser.parse_args()

    scenarios = tuple(s.strip() for s in args.scenarios.split(",") if s.strip())
    years = tuple(int(y) for y in args.years.split(",") if y.strip())

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.re_dispatch_from_csv:
        re_dispatch_from_csv(
            git_ref=args.git_ref,
            scenarios=scenarios,
            throttle=args.throttle,
            dry_run=args.dry_run,
        )
    else:
        dispatch(
            git_ref=args.git_ref,
            scenarios=scenarios,
            years=years,
            use_useeio_baseline=args.use_useeio_baseline,
            dry_run=args.dry_run,
            throttle=args.throttle,
        )


if __name__ == "__main__":
    main()
