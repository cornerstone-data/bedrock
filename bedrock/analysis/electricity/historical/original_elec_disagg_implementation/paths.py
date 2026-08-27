"""Paths for the original electricity-disaggregation implementation freeze."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
OUT_DIR = PACKAGE_DIR / 'output'

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
    return OUT_DIR / config_stem
