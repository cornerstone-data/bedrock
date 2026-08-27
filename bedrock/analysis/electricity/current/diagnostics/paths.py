"""Shared package paths for electricity disaggregation diagnostics.

``OUT_DIR`` stays at the package-level ``output/`` so every analysis subpackage
writes to the same stable layout.
"""

from __future__ import annotations

from pathlib import Path

from bedrock.utils.snapshots import releases

PACKAGE_DIR = Path(__file__).resolve().parent
OUT_DIR = PACKAGE_DIR / 'output'
LOCAL_DATA_DIR = PACKAGE_DIR / 'local_data'
MANIFEST_PATH = PACKAGE_DIR / 'manifest.yaml'

V03_SNAPSHOT_SHA = releases.v0_3_1


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
