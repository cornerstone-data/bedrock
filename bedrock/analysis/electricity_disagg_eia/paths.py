"""Paths for the prior-production electricity disaggregation freeze."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
OUT_DIR = PACKAGE_DIR / 'output'
BASELINE_DIR = OUT_DIR / 'baseline_current_production'

DISAGG_CONFIG = '2025_usa_cornerstone_v0_3_electricity_disaggregation'
MIXED_CONFIG = '2025_usa_cornerstone_v0_3_electricity_mixed_units'

SNAPSHOT_FILES = (
    'q.parquet',
    'x.parquet',
    'use_y_generation.parquet',
    'intersection_3x3.parquet',
    'electricity_rows_y.parquet',
    'E.parquet',
    'D.parquet',
    'N.parquet',
    'BLy.parquet',
    'class_generation_mwh.parquet',
    'run_metadata.json',
)


def config_dir(config_stem: str) -> Path:
    return BASELINE_DIR / config_stem


def ensure_dirs() -> None:
    for stem in (DISAGG_CONFIG, MIXED_CONFIG):
        config_dir(stem).mkdir(parents=True, exist_ok=True)
