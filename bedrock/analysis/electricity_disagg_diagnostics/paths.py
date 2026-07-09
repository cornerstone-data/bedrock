"""Output paths for electricity disaggregation BLy dispersion diagnostics."""

from __future__ import annotations

from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
OUT_DIR = PACKAGE_DIR / 'output'
MANIFEST_PATH = PACKAGE_DIR / 'manifest.yaml'

V02_SNAPSHOT_SHA = '7372464249c434c9bebb172c065a4d0e3702176e'


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
