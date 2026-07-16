"""Dispatch v03_waterfall diagnostics (cumulative group endpoints).

Five configs for CEDA baseline (G1a/G1b split, then shared G2/G3/FINAL)
or four for USEEIO baseline. Persists to a baseline-specific run index CSV under
``output/release_v0_v03_groups/``.

Usage:
    uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --dry-run
    uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --baseline ceda
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from bedrock.utils.config.usa_config import _load_usa_config_from_file_name
from bedrock.utils.validation.analysis.release_v0_v03_ceda_groups import (
    V03_WATERFALL_CEDA_CONFIGS,
)
from bedrock.utils.validation.analysis.release_v0_v03_useeio_groups import (
    V03_WATERFALL_CONFIGS,
)
from bedrock.utils.validation.dispatch_diagnostics import (
    V03_WATERFALL_DRIVE_FOLDER_ID,
    create_sheet,
    trigger_workflow,
)
from bedrock.utils.validation.dispatch_diagnostics import (
    throttle as apply_throttle,
)

logger = logging.getLogger(__name__)

_OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "release_v0_v03_groups"

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


USEEIO_WATERFALL_STEPS: tuple[WaterfallCell, ...] = (
    WaterfallCell(
        "v03_waterfall_useeio_g1_schema_ghg", "waterfall USEEIO G1 schema/GHG", 2024
    ),
    WaterfallCell("v03_waterfall_g2_methods", "waterfall G2 methods", 2024),
    WaterfallCell("v03_waterfall_g3_data", "waterfall G3 data", 2024),
    WaterfallCell("v03_waterfall_final", "waterfall FINAL v0.3", 2024),
)

CEDA_WATERFALL_STEPS: tuple[WaterfallCell, ...] = (
    WaterfallCell(
        "v03_waterfall_ceda_g1a_schema_ghg", "waterfall CEDA G1a schema/GHG", 2024
    ),
    WaterfallCell(
        "v03_waterfall_ceda_g1b_waste_disagg", "waterfall CEDA G1b waste disagg", 2024
    ),
    WaterfallCell("v03_waterfall_g2_methods", "waterfall G2 methods", 2024),
    WaterfallCell("v03_waterfall_g3_data", "waterfall G3 data", 2024),
    WaterfallCell("v03_waterfall_final", "waterfall FINAL v0.3", 2024),
)


@dataclass(frozen=True)
class WaterfallBaselineSpec:
    name: str
    steps: tuple[WaterfallCell, ...]
    config_names: tuple[str, ...]
    index_path: Path
    use_useeio_baseline: bool
    title_baseline_label: str


WATERFALL_BASELINES: dict[str, WaterfallBaselineSpec] = {
    "useeio": WaterfallBaselineSpec(
        name="useeio",
        steps=USEEIO_WATERFALL_STEPS,
        config_names=V03_WATERFALL_CONFIGS,
        index_path=_OUTPUT_DIR / "ef_run_index_v03_waterfall.csv",
        use_useeio_baseline=True,
        title_baseline_label="USEEIO based",
    ),
    "ceda": WaterfallBaselineSpec(
        name="ceda",
        steps=CEDA_WATERFALL_STEPS,
        config_names=V03_WATERFALL_CEDA_CONFIGS,
        index_path=_OUTPUT_DIR / "ef_run_index_v03_waterfall_ceda.csv",
        use_useeio_baseline=False,
        title_baseline_label="CEDA based",
    ),
}


def _sheet_title(
    *,
    today: str,
    title_year: int,
    step_label: str,
    baseline_label: str,
) -> str:
    return (
        f"[{today}, bedrock repo, {title_year}, {baseline_label}, "
        f"v0.3 / {step_label}] EFs diagnostics"
    )


def _years_for_config(config_name: str) -> tuple[int, int]:
    cfg = _load_usa_config_from_file_name(f"{config_name}.yaml")
    return cfg.model_base_year, cfg.usa_ghg_data_year


def _load_index(index_path: Path) -> pd.DataFrame:
    if index_path.exists():
        df = pd.read_csv(index_path)
    else:
        df = pd.DataFrame(columns=list(INDEX_COLUMNS))
    for col in INDEX_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df


def _append_index_row(index_path: Path, row: dict[str, object]) -> None:
    df = _load_index(index_path)
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(index_path, index=False)


def _already_recorded(df: pd.DataFrame, sheet_title: str) -> bool:
    if df.empty:
        return False
    return bool((df["sheet_title"] == sheet_title).any())


def dispatch_waterfall(
    *,
    baseline: WaterfallBaselineSpec,
    git_ref: str = "main",
    drive_folder_id: str = V03_WATERFALL_DRIVE_FOLDER_ID,
    dry_run: bool = False,
    throttle: str = "poll",
    only_configs: tuple[str, ...] | None = None,
    title_date: str | None = None,
) -> None:
    unknown = set(only_configs or ()) - set(baseline.config_names)
    if unknown:
        raise ValueError(
            f"Unknown config(s) {sorted(unknown)!r}; expected subset of "
            f"{list(baseline.config_names)!r}"
        )

    today = title_date or dt.datetime.utcnow().strftime("%Y-%m-%d")
    n_dispatched = 0

    steps = baseline.steps
    if only_configs:
        only_set = set(only_configs)
        steps = tuple(c for c in baseline.steps if c.config_name in only_set)
        if not steps:
            raise ValueError(
                f"No waterfall steps match --only-configs {only_configs!r}"
            )

    for cell in steps:
        model_base_year, usa_ghg_data_year = _years_for_config(cell.config_name)
        title = _sheet_title(
            today=today,
            title_year=cell.title_year,
            step_label=cell.step_label,
            baseline_label=baseline.title_baseline_label,
        )
        index_df = _load_index(baseline.index_path)
        if _already_recorded(index_df, title):
            logger.info("Skip already-recorded: %s", title)
            continue

        if dry_run:
            logger.info(
                "DRY-RUN would create: %s | folder=%s config=%s baseline=%s "
                "use_useeio=%s model_base_year=%d usa_ghg_data_year=%d",
                title,
                drive_folder_id,
                cell.config_name,
                baseline.name,
                baseline.use_useeio_baseline,
                model_base_year,
                usa_ghg_data_year,
            )
            n_dispatched += 1
            continue

        apply_throttle(throttle)

        sheet_id = create_sheet(drive_folder_id, title)
        logger.info("Created sheet %s: %s", sheet_id, title)
        trigger_workflow(
            git_ref=git_ref,
            config_name=cell.config_name,
            sheet_id=sheet_id,
            model_base_year=model_base_year,
            use_useeio_baseline=baseline.use_useeio_baseline,
            usa_ghg_data_year=usa_ghg_data_year,
        )
        _append_index_row(
            baseline.index_path,
            {
                "config_name": cell.config_name,
                "baseline": baseline.name,
                "sheet_id": sheet_id,
                "sheet_title": title,
                "useeio_box_ticked": (
                    "true" if baseline.use_useeio_baseline else "false"
                ),
                "model_base_year": model_base_year,
                "usa_ghg_data_year": usa_ghg_data_year,
                "git_ref": git_ref,
                "triggered_at": dt.datetime.utcnow().isoformat() + "Z",
            },
        )
        n_dispatched += 1

    logger.info("Done. baseline=%s cells=%d", baseline.name, n_dispatched)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline",
        choices=sorted(WATERFALL_BASELINES),
        default="useeio",
        help="Diagnostics baseline (default: useeio).",
    )
    parser.add_argument("--git-ref", default="main")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--throttle", default="poll")
    parser.add_argument(
        "--only-configs",
        default="",
        help="Comma-separated v03_waterfall config stems to dispatch.",
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
        baseline=WATERFALL_BASELINES[args.baseline],
        git_ref=args.git_ref,
        drive_folder_id=args.drive_folder_id,
        dry_run=args.dry_run,
        throttle=args.throttle,
        only_configs=only,
        title_date=args.title_date or None,
    )


if __name__ == "__main__":
    main()
