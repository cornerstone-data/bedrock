"""Shared constants for the a_matrix_time_series analysis package.

Centralizes paths, the BEA detail base year, the latest target year all
approaches have data for, and the Drive folder for run-report Sheets.
"""

from __future__ import annotations

from pathlib import Path

from bedrock.utils.config.usa_config import get_usa_config

OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_DIR = OUTPUT_DIR / "results"
PLOTS_DIR = OUTPUT_DIR / "plots"
LAST_RUN_SHEET_ID_PATH = RESULTS_DIR / "last_run_sheet_id.txt"

ANALYSIS_DRIVE_FOLDER_ID = "1UcPmwLnL6MwTq9pMYJw5d43FJQOFQVO_"

# BEA detail IO base year — the inflation `original_year` for every script.
ORIGINAL_YEAR: int = get_usa_config().usa_base_io_data_year

# Latest year for which all approaches have data (USEEIO, industry_pi,
# commodity_pi at 2024; summary_tables falls back to 2023 internally).
LATEST_TARGET_YEAR: int = 2024

# Canonical 5-approach plot order (useeio + ceda_default baselines first,
# then the three alternatives). Top-left → bottom-right when laid out in a
# grid.
APPROACH_ORDER: tuple[str, ...] = (
    "useeio",
    "ceda_default",
    "summary_tables",
    "industry_price_index",
    "commodity_price_index",
)

# Subset of APPROACH_ORDER that drops both baselines — the three
# A-matrix-derivation alternatives we evaluate against either baseline.
ALTERNATIVE_APPROACHES: tuple[str, ...] = (
    "summary_tables",
    "industry_price_index",
    "commodity_price_index",
)

# Per-approach colors used by every plot in this package. Keys match
# APPROACH_ORDER. Missing keys (e.g. `ceda` vs `ceda_default`) should
# fall back via `.get(approach, default)` at the call site.
APPROACH_COLORS: dict[str, str] = {
    "useeio": "#7f7f7f",
    "ceda_default": "#bcbd22",
    "summary_tables": "#1f77b4",
    "industry_price_index": "#ff7f0e",
    "commodity_price_index": "#2ca02c",
}

# (approach_key, display_label) for the two baselines that the alternatives
# are compared against. Order matches APPROACH_ORDER.
BASELINES: tuple[tuple[str, str], ...] = (
    ("useeio", "USEEIO"),
    ("ceda_default", "CEDA-US"),
)
