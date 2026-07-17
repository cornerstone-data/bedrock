"""Shared diagnostics dispatch: Drive sheets + generate_diagnostics workflow.

Owns create-sheet / trigger / throttle helpers used by epic dispatchers and by
the feature-impact CLI. Default Drive folder for feature evaluation is
``V04_DIAGNOSTICS_DRIVE_FOLDER_ID``.

Usage (feature configs)::

    uv run python -m bedrock.utils.validation.dispatch_diagnostics \\
        --git-ref main \\
        --configs my_atomic_config \\
        --baseline-label "Bedrock v0.3 snapshot based" \\
        --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import subprocess
import time
from pathlib import Path

import pandas as pd

from bedrock.utils.config.usa_config import _load_usa_config_from_file_name

logger = logging.getLogger(__name__)

# Methodology feature evaluations (v0.4 Diagnostics Drive folder).
V04_DIAGNOSTICS_DRIVE_FOLDER_ID = "1W6I4q2ssfgaaVz6eLNICNiETP05dhrCK"

# Wholesale v0→v0.3 waterfall diagnostics.
V03_WATERFALL_DRIVE_FOLDER_ID = "107RNHx1OUGN6roYdRi3BbdCSrMNFhl6u"

_DEFAULT_FEATURE_INDEX = (
    Path(__file__).resolve().parent / "output" / "ef_run_index_feature.csv"
)

FEATURE_INDEX_COLUMNS = (
    "config_name",
    "baseline_label",
    "sheet_id",
    "sheet_title",
    "useeio_box_ticked",
    "model_base_year",
    "usa_ghg_data_year",
    "git_ref",
    "drive_folder_id",
    "triggered_at",
)


def drive_client():  # type: ignore[no-untyped-def]
    """Return an authenticated Drive v3 client for the active ADC."""
    import google.auth  # noqa: PLC0415
    import googleapiclient.discovery  # noqa: PLC0415

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
    return googleapiclient.discovery.build("drive", "v3", credentials=creds)


def create_sheet(folder_id: str, title: str) -> str:
    """Create an empty Google Sheet in ``folder_id``; return its spreadsheet ID."""
    drive = drive_client()
    body = {
        "name": title,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
    }
    file = (
        drive.files().create(body=body, fields="id", supportsAllDrives=True).execute()
    )
    return str(file["id"])


def trigger_workflow(
    *,
    git_ref: str,
    config_name: str,
    sheet_id: str,
    use_useeio_baseline: bool,
    model_base_year: int | None = None,
    usa_ghg_data_year: int | None = None,
) -> None:
    """Trigger ``generate_diagnostics.yml`` via ``gh workflow run``.

    Omit ``model_base_year`` / ``usa_ghg_data_year`` to leave the config YAML's
    years unchanged (recommended for configs that hard-code years).
    """
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
        f"use_useeio_baseline={'true' if use_useeio_baseline else 'false'}",
    ]
    if model_base_year is not None:
        cmd += ["-f", f"model_base_year={model_base_year}"]
    if usa_ghg_data_year is not None:
        cmd += ["-f", f"usa_ghg_data_year={usa_ghg_data_year}"]
    subprocess.run(cmd, check=True)


def busy_count(workflow: str = "generate_diagnostics") -> int:
    """Number of ``queued`` + ``in_progress`` runs of the named workflow.

    Retries each ``gh run list`` call on transient subprocess failures.
    After exhausted retries, returns ``1`` so the caller keeps polling
    rather than dispatching into an unknown queue state.
    """
    count = 0
    for status in ("queued", "in_progress"):
        for attempt in range(3):
            try:
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
                break
            except subprocess.CalledProcessError as e:
                stderr = (e.stderr or "").strip()
                if attempt == 2:
                    logger.warning(
                        "gh run list failed for status=%s after 3 attempts: %s; "
                        "treating as busy and continuing poll loop",
                        status,
                        stderr or "(no stderr)",
                    )
                    return 1
                time.sleep(5)
    return count


def wait_for_capacity(
    *,
    poll_interval: int = 15,
    timeout: int = 1800,
    workflow: str = "generate_diagnostics",
    initial_delay: int = 15,
) -> None:
    """Block until no named workflow run is queued or in progress.

    Combined with the workflow's ``concurrency:`` directive, this serializes
    dispatches so pending runs are not dropped (``cancel-in-progress: false``).

    ``initial_delay`` waits before the first poll so a just-triggered run can
    register as ``queued``.
    """
    if initial_delay:
        time.sleep(initial_delay)
    deadline = time.time() + timeout
    while time.time() < deadline:
        n = busy_count(workflow)
        if n == 0:
            return
        logger.info("Waiting for %d in-flight %s run(s) to clear...", n, workflow)
        time.sleep(poll_interval)
    raise TimeoutError(
        f"{workflow} runs still busy after {timeout}s; aborting dispatch"
    )


def throttle(mode: str) -> None:
    """Space successive dispatches.

    Modes:
      - ``poll``: block until the workflow has no queued or in-progress runs.
      - ``sleep:N``: sleep N seconds (e.g. ``sleep:120``).
      - ``none``: no throttle.
    """
    if mode == "poll":
        wait_for_capacity()
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


def feature_sheet_title(
    *,
    today: str,
    model_year: int,
    baseline_label: str,
    feature_label: str,
) -> str:
    """Canonical title for a feature-impact diagnostics sheet."""
    return (
        f"[{today}, bedrock repo, {model_year}, {baseline_label}, "
        f"{feature_label}] EFs diagnostics"
    )


def _years_for_config(config_name: str) -> tuple[int, int]:
    cfg = _load_usa_config_from_file_name(f"{config_name}.yaml")
    return cfg.model_base_year, cfg.usa_ghg_data_year


def _load_feature_index(index_path: Path) -> pd.DataFrame:
    if index_path.exists():
        df = pd.read_csv(index_path)
    else:
        df = pd.DataFrame(columns=list(FEATURE_INDEX_COLUMNS))
    for col in FEATURE_INDEX_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _append_feature_index_row(index_path: Path, row: dict[str, object]) -> None:
    df = _load_feature_index(index_path)
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(index_path, index=False)


def _already_recorded_title(index_path: Path, sheet_title: str) -> bool:
    df = _load_feature_index(index_path)
    if df.empty:
        return False
    return bool((df["sheet_title"] == sheet_title).any())


def dispatch_feature_configs(
    *,
    git_ref: str,
    configs: tuple[str, ...],
    baseline_label: str,
    use_useeio_baseline: bool = False,
    drive_folder_id: str = V04_DIAGNOSTICS_DRIVE_FOLDER_ID,
    feature_label: str | None = None,
    index_path: Path | None = None,
    dry_run: bool = False,
    throttle_mode: str = "poll",
    title_date: str | None = None,
    pass_year_overrides: bool = True,
) -> None:
    """Create sheets + trigger diagnostics for a list of USA config stems.

    Default folder is v0.4 Diagnostics. Titles follow
    ``feature_sheet_title``. Skips titles already present in the index CSV.
    """
    if not configs:
        raise ValueError("At least one --configs stem is required")
    if not baseline_label.strip():
        raise ValueError("--baseline-label is required")

    today = title_date or dt.datetime.utcnow().strftime("%Y-%m-%d")
    index = index_path or _DEFAULT_FEATURE_INDEX
    n_dispatched = 0

    for config_name in configs:
        model_base_year, usa_ghg_data_year = _years_for_config(config_name)
        label = feature_label or config_name
        title = feature_sheet_title(
            today=today,
            model_year=model_base_year,
            baseline_label=baseline_label,
            feature_label=label,
        )
        if _already_recorded_title(index, title):
            logger.info("Skip already-recorded: %s", title)
            continue

        if dry_run:
            logger.info(
                "DRY-RUN would create: %s | folder=%s config=%s use_useeio=%s "
                "model_base_year=%d usa_ghg_data_year=%d pass_years=%s",
                title,
                drive_folder_id,
                config_name,
                use_useeio_baseline,
                model_base_year,
                usa_ghg_data_year,
                pass_year_overrides,
            )
            n_dispatched += 1
            continue

        if n_dispatched > 0:
            throttle(throttle_mode)

        sheet_id = create_sheet(drive_folder_id, title)
        logger.info("Created sheet %s: %s", sheet_id, title)
        trigger_workflow(
            git_ref=git_ref,
            config_name=config_name,
            sheet_id=sheet_id,
            use_useeio_baseline=use_useeio_baseline,
            model_base_year=model_base_year if pass_year_overrides else None,
            usa_ghg_data_year=usa_ghg_data_year if pass_year_overrides else None,
        )
        _append_feature_index_row(
            index,
            {
                "config_name": config_name,
                "baseline_label": baseline_label,
                "sheet_id": sheet_id,
                "sheet_title": title,
                "useeio_box_ticked": str(use_useeio_baseline).lower(),
                "model_base_year": model_base_year,
                "usa_ghg_data_year": usa_ghg_data_year,
                "git_ref": git_ref,
                "drive_folder_id": drive_folder_id,
                "triggered_at": dt.datetime.utcnow().isoformat() + "Z",
            },
        )
        n_dispatched += 1

    logger.info("Done. configs=%d dispatched_or_planned=%d", len(configs), n_dispatched)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--git-ref",
        required=True,
        help="Git ref (branch or tag) to run the workflow against.",
    )
    parser.add_argument(
        "--configs",
        required=True,
        help="Comma-separated USA config stems (filename without .yaml).",
    )
    parser.add_argument(
        "--baseline-label",
        required=True,
        help=(
            'Title baseline text, e.g. "CEDA-US v0 based", '
            '"Bedrock v0.3 snapshot based", '
            '"USEEIO USEEIOv2.6.0-phoebe-23 based".'
        ),
    )
    parser.add_argument(
        "--use-useeio-baseline",
        action="store_true",
        help="Tick the USEEIO Excel baseline pin on the workflow.",
    )
    parser.add_argument(
        "--drive-folder-id",
        default=V04_DIAGNOSTICS_DRIVE_FOLDER_ID,
        help=(
            "Google Drive folder for new Sheets "
            f"(default: v0.4 Diagnostics {V04_DIAGNOSTICS_DRIVE_FOLDER_ID})."
        ),
    )
    parser.add_argument(
        "--feature-label",
        default="",
        help=(
            "Optional title middle label (default: each config stem). "
            "When dispatching multiple configs, omit this so each stem "
            "appears in its own title."
        ),
    )
    parser.add_argument(
        "--index-csv",
        default=str(_DEFAULT_FEATURE_INDEX),
        help="Run-index CSV path (default: validation/output/ef_run_index_feature.csv).",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--throttle", default="poll")
    parser.add_argument(
        "--title-date",
        default="",
        help="YYYY-MM-DD prefix for sheet titles (default: UTC today).",
    )
    parser.add_argument(
        "--omit-year-overrides",
        action="store_true",
        help=(
            "Do not pass model_base_year / usa_ghg_data_year to the workflow "
            "(YAML values win). Default passes the years loaded from each config."
        ),
    )
    args = parser.parse_args()
    configs = tuple(s.strip() for s in args.configs.split(",") if s.strip())
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    dispatch_feature_configs(
        git_ref=args.git_ref,
        configs=configs,
        baseline_label=args.baseline_label,
        use_useeio_baseline=args.use_useeio_baseline,
        drive_folder_id=args.drive_folder_id,
        feature_label=args.feature_label or None,
        index_path=Path(args.index_csv),
        dry_run=args.dry_run,
        throttle_mode=args.throttle,
        title_date=args.title_date or None,
        pass_year_overrides=not args.omit_year_overrides,
    )


if __name__ == "__main__":
    main()
