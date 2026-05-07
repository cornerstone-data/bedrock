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
