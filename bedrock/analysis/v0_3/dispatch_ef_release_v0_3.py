"""Dispatch v0.3 release-progression EF diagnostics.

Five configs × two baselines (CEDA v0 + USEEIO) → ten Sheets in the
EF time-series Drive folder. Persists to
``output/release_v0_3/ef_run_index_release_v0_3.csv`` (not the time-series
``ef_run_index.csv``).

Usage:
    uv run python -m bedrock.analysis.v0_3.dispatch_ef_release_v0_3 --dry-run
    uv run python -m bedrock.analysis.v0_3.dispatch_ef_release_v0_3
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bedrock.analysis.a_matrix_time_series.dispatch_ef_time_series import (
    EF_TIME_SERIES_DRIVE_FOLDER_ID,
)
from bedrock.utils.config.usa_config import _load_usa_config_from_file_name
from bedrock.utils.validation.dispatch_diagnostics import (
    create_sheet,
    trigger_workflow,
)
from bedrock.utils.validation.dispatch_diagnostics import (
    throttle as apply_throttle,
)

logger = logging.getLogger(__name__)

_RELEASE_DIR = Path(__file__).resolve().parent / "output" / "release_v0_3"
RELEASE_INDEX_PATH = _RELEASE_DIR / "ef_run_index_release_v0_3.csv"

INDEX_COLUMNS = (
    "config_name",
    "baseline",
    "sheet_id",
    "sheet_title",
    "useeio_box_ticked",
    "model_base_year",
    "usa_ghg_data_year",
    "git_ref",
    "triggered_at",
)


@dataclass(frozen=True)
class ReleaseCell:
    config_name: str
    step_label: str
    title_year: int  # year token in the sheet title (bedrock repo, {year}, …)


RELEASE_STEPS: tuple[ReleaseCell, ...] = (
    ReleaseCell(
        "2025_usa_cornerstone_full_model_v0_3_ghgi_mecs", "MECS adjustment", 2023
    ),
    ReleaseCell(
        "2025_usa_cornerstone_full_model_v0_3_umd_2023_ghgia",
        "Switch to 2023 UMD data",
        2023,
    ),
    ReleaseCell(
        "2025_usa_cornerstone_full_model_v0_3_umd_2024_ghgia",
        "Update to 2024 UMD data",
        2024,
    ),
    ReleaseCell(
        "2025_usa_cornerstone_full_model_v0_3_2024_io_ghg",
        "2024 US IO+GHG data",
        2024,
    ),
    ReleaseCell("2025_usa_cornerstone_v0_3", "FINAL v0.3", 2024),
)


def _sheet_title(*, today: str, title_year: int, baseline: str, step_label: str) -> str:
    return (
        f"[{today}, bedrock repo, {title_year}, {baseline} based, "
        f"v0.3 / {step_label}] EFs diagnostics"
    )


def _years_for_config(config_name: str) -> tuple[int, int]:
    cfg = _load_usa_config_from_file_name(f"{config_name}.yaml")
    return cfg.model_base_year, cfg.usa_ghg_data_year


def _load_index() -> pd.DataFrame:
    if RELEASE_INDEX_PATH.exists():
        df = pd.read_csv(RELEASE_INDEX_PATH)
    else:
        df = pd.DataFrame(columns=list(INDEX_COLUMNS))
    for col in INDEX_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _append_index_row(row: dict[str, object]) -> None:
    df = _load_index()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    RELEASE_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RELEASE_INDEX_PATH, index=False)


def _already_recorded(df: pd.DataFrame, sheet_title: str) -> bool:
    if df.empty:
        return False
    return bool((df["sheet_title"] == sheet_title).any())


def dispatch_release(
    *,
    git_ref: str = "main",
    dry_run: bool = False,
    throttle: str = "poll",
    only_configs: tuple[str, ...] | None = None,
    title_date: str | None = None,
    only_baselines: tuple[str, ...] | None = None,
) -> None:
    today = title_date or dt.datetime.utcnow().strftime("%Y-%m-%d")
    n_dispatched = 0

    steps = RELEASE_STEPS
    if only_configs:
        only_set = set(only_configs)
        steps = tuple(c for c in RELEASE_STEPS if c.config_name in only_set)
        if not steps:
            raise ValueError(f"No RELEASE_STEPS match --only-configs {only_configs!r}")

    for cell in steps:
        model_base_year, usa_ghg_data_year = _years_for_config(cell.config_name)
        for use_useeio in (False, True):
            baseline_name = "USEEIO" if use_useeio else "CEDA"
            baseline_key = "useeio" if use_useeio else "ceda"
            if only_baselines and baseline_key not in only_baselines:
                continue
            title = _sheet_title(
                today=today,
                title_year=cell.title_year,
                baseline=baseline_name,
                step_label=cell.step_label,
            )
            index_df = _load_index()
            if _already_recorded(index_df, title):
                logger.info("Skip already-recorded: %s", title)
                continue

            if dry_run:
                logger.info(
                    "DRY-RUN would create: %s | config=%s use_useeio=%s "
                    "model_base_year=%d usa_ghg_data_year=%d",
                    title,
                    cell.config_name,
                    use_useeio,
                    model_base_year,
                    usa_ghg_data_year,
                )
                n_dispatched += 1
                continue

            apply_throttle(throttle)

            sheet_id = create_sheet(EF_TIME_SERIES_DRIVE_FOLDER_ID, title)
            logger.info("Created sheet %s: %s", sheet_id, title)
            trigger_workflow(
                git_ref=git_ref,
                config_name=cell.config_name,
                sheet_id=sheet_id,
                model_base_year=model_base_year,
                use_useeio_baseline=use_useeio,
                usa_ghg_data_year=usa_ghg_data_year,
            )
            _append_index_row(
                {
                    "config_name": cell.config_name,
                    "baseline": baseline_key,
                    "sheet_id": sheet_id,
                    "sheet_title": title,
                    "useeio_box_ticked": str(use_useeio).lower(),
                    "model_base_year": model_base_year,
                    "usa_ghg_data_year": usa_ghg_data_year,
                    "git_ref": git_ref,
                    "triggered_at": dt.datetime.utcnow().isoformat() + "Z",
                }
            )
            n_dispatched += 1

    logger.info("Done. cells=%d", n_dispatched)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--git-ref", default="main")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--throttle", default="poll")
    parser.add_argument(
        "--only-configs",
        default="",
        help="Comma-separated config_name stems to dispatch (default: all five).",
    )
    parser.add_argument(
        "--title-date",
        default="",
        help="YYYY-MM-DD prefix for sheet titles (default: UTC today).",
    )
    parser.add_argument(
        "--only-baselines",
        default="",
        help="Comma-separated baseline keys to dispatch: ceda, useeio (default: both).",
    )
    args = parser.parse_args()
    only = tuple(s.strip() for s in args.only_configs.split(",") if s.strip()) or None
    baselines = (
        tuple(s.strip() for s in args.only_baselines.split(",") if s.strip()) or None
    )
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    dispatch_release(
        git_ref=args.git_ref,
        dry_run=args.dry_run,
        throttle=args.throttle,
        only_configs=only,
        title_date=args.title_date or None,
        only_baselines=baselines,
    )


if __name__ == "__main__":
    main()
