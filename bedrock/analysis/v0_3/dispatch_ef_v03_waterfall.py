"""Dispatch v03_waterfall USEEIO diagnostics (four cumulative group endpoints).

Four ``v03_waterfall_*`` configs × USEEIO baseline → four Sheets in the
v03 waterfall Drive folder (default below). Persists to
``output/release_v0_v03_groups/ef_run_index_v03_waterfall.csv``.

Usage:
    uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --dry-run
    uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bedrock.analysis.a_matrix_time_series.dispatch_ef_time_series import (
    _create_sheet,
    _throttle,
    _trigger_workflow,
)
from bedrock.utils.config.usa_config import _load_usa_config_from_file_name
from bedrock.utils.validation.analysis.release_v0_v03_useeio_groups import (
    V03_WATERFALL_CONFIGS,
)

logger = logging.getLogger(__name__)

# v0.3 wholesale waterfall diagnostics (separate from Step 7 EF time-series folder).
V03_WATERFALL_DRIVE_FOLDER_ID = "107RNHx1OUGN6roYdRi3BbdCSrMNFhl6u"

_OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "release_v0_v03_groups"
WATERFALL_INDEX_PATH = _OUTPUT_DIR / "ef_run_index_v03_waterfall.csv"

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
class WaterfallCell:
    config_name: str
    step_label: str
    title_year: int


WATERFALL_STEPS: tuple[WaterfallCell, ...] = (
    WaterfallCell("v03_waterfall_useeio_g1_schema_ghg", "waterfall USEEIO G1 schema/GHG", 2024),
    WaterfallCell("v03_waterfall_g2_methods", "waterfall G2 methods", 2024),
    WaterfallCell("v03_waterfall_g3_data", "waterfall G3 data", 2024),
    WaterfallCell("v03_waterfall_final", "waterfall FINAL v0.3", 2024),
)


def _sheet_title(*, today: str, title_year: int, step_label: str) -> str:
    return (
        f"[{today}, bedrock repo, {title_year}, USEEIO based, "
        f"v0.3 / {step_label}] EFs diagnostics"
    )


def _years_for_config(config_name: str) -> tuple[int, int]:
    cfg = _load_usa_config_from_file_name(f"{config_name}.yaml")
    return cfg.model_base_year, cfg.usa_ghg_data_year


def _load_index() -> pd.DataFrame:
    if WATERFALL_INDEX_PATH.exists():
        df = pd.read_csv(WATERFALL_INDEX_PATH)
    else:
        df = pd.DataFrame(columns=list(INDEX_COLUMNS))
    for col in INDEX_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _append_index_row(row: dict[str, object]) -> None:
    df = _load_index()
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    WATERFALL_INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(WATERFALL_INDEX_PATH, index=False)


def _already_recorded(df: pd.DataFrame, sheet_title: str) -> bool:
    if df.empty:
        return False
    return bool((df["sheet_title"] == sheet_title).any())


def dispatch_waterfall(
    *,
    git_ref: str = "main",
    drive_folder_id: str = V03_WATERFALL_DRIVE_FOLDER_ID,
    dry_run: bool = False,
    throttle: str = "poll",
    only_configs: tuple[str, ...] | None = None,
    title_date: str | None = None,
) -> None:
    unknown = set(only_configs or ()) - set(V03_WATERFALL_CONFIGS)
    if unknown:
        raise ValueError(
            f"Unknown config(s) {sorted(unknown)!r}; expected subset of "
            f"{list(V03_WATERFALL_CONFIGS)!r}"
        )

    today = title_date or dt.datetime.utcnow().strftime("%Y-%m-%d")
    n_dispatched = 0

    steps = WATERFALL_STEPS
    if only_configs:
        only_set = set(only_configs)
        steps = tuple(c for c in WATERFALL_STEPS if c.config_name in only_set)
        if not steps:
            raise ValueError(f"No WATERFALL_STEPS match --only-configs {only_configs!r}")

    for cell in steps:
        model_base_year, usa_ghg_data_year = _years_for_config(cell.config_name)
        title = _sheet_title(
            today=today,
            title_year=cell.title_year,
            step_label=cell.step_label,
        )
        index_df = _load_index()
        if _already_recorded(index_df, title):
            logger.info("Skip already-recorded: %s", title)
            continue

        if dry_run:
            logger.info(
                "DRY-RUN would create: %s | folder=%s config=%s use_useeio=true "
                "model_base_year=%d usa_ghg_data_year=%d",
                title,
                drive_folder_id,
                cell.config_name,
                model_base_year,
                usa_ghg_data_year,
            )
            n_dispatched += 1
            continue

        _throttle(throttle)

        sheet_id = _create_sheet(drive_folder_id, title)
        logger.info("Created sheet %s: %s", sheet_id, title)
        _trigger_workflow(
            git_ref=git_ref,
            config_name=cell.config_name,
            sheet_id=sheet_id,
            model_base_year=model_base_year,
            use_useeio_baseline=True,
            usa_ghg_data_year=usa_ghg_data_year,
        )
        _append_index_row(
            {
                "config_name": cell.config_name,
                "baseline": "useeio",
                "sheet_id": sheet_id,
                "sheet_title": title,
                "useeio_box_ticked": "true",
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
        help=(
            "Comma-separated v03_waterfall config stems to dispatch "
            f"(default: all {len(V03_WATERFALL_CONFIGS)})."
        ),
    )
    parser.add_argument(
        "--drive-folder-id",
        default=V03_WATERFALL_DRIVE_FOLDER_ID,
        help=(
            "Google Drive folder ID for new diagnostics Sheets "
            f"(default: v03 waterfall folder {V03_WATERFALL_DRIVE_FOLDER_ID})."
        ),
    )
    parser.add_argument(
        "--title-date",
        default="",
        help="YYYY-MM-DD prefix for sheet titles (default: UTC today).",
    )
    args = parser.parse_args()
    only = tuple(s.strip() for s in args.only_configs.split(",") if s.strip()) or None
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    dispatch_waterfall(
        git_ref=args.git_ref,
        drive_folder_id=args.drive_folder_id,
        dry_run=args.dry_run,
        throttle=args.throttle,
        only_configs=only,
        title_date=args.title_date or None,
    )


if __name__ == "__main__":
    main()
