"""Paths for the pre-MECS Industrial-weight freeze (EIA G/T/D + dollar manufacturing)."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
OUT_DIR = PACKAGE_DIR / 'output'

MIXED_CONFIG = '2025_usa_cornerstone_v0_3_electricity_mixed_units'

SNAPSHOT_FILES = ('q.parquet', 'N.parquet')


def config_dir(config_stem: str = MIXED_CONFIG) -> Path:
    return OUT_DIR / config_stem
