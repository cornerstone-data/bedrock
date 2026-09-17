"""Shared paths and illicit-sign predicate for #839 hygiene analysis."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from bedrock.transform.iot.nowcast_mask import INVENTORY_CHANGE_COLUMN
from bedrock.utils.economic.balance.mask import SutMask

PACKAGE_DIR = Path(__file__).resolve().parent
#: Working artifacts (parquets, JSON censuses). Gitignored via parent ``output/``.
OUTPUT_DIR = PACKAGE_DIR.parent / 'output' / 'ras_improvements'
#: Prior local runs before the reorg (still readable as a fallback).
_LEGACY_OUTPUT_DIR = PACKAGE_DIR.parent / 'output' / 'ras_hygiene_839'

RESIDUAL_VA_ROW = 'V00300'
GCS_BALANCED_SUT = 'flowsa/BalancedSUT'


def resolve_artifact_dir(*, create: bool = False) -> Path:
    """Directory for BalancedSUT copies and census JSON.

    Prefers :data:`OUTPUT_DIR`; falls back to the pre-reorg
    ``output/ras_hygiene_839`` folder when it already has artifacts.
    """
    if OUTPUT_DIR.exists() and any(OUTPUT_DIR.iterdir()):
        return OUTPUT_DIR
    if _LEGACY_OUTPUT_DIR.exists() and any(_LEGACY_OUTPUT_DIR.iterdir()):
        return _LEGACY_OUTPUT_DIR
    if create:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        return OUTPUT_DIR
    return OUTPUT_DIR


def illicit_mask(
    balanced: pd.DataFrame,
    mask: SutMask,
    pattern2017: pd.DataFrame,
) -> pd.DataFrame:
    """Boolean frame: Use negatives outside the #839 hygiene whitelist.

    Whitelist: ``sign_lock == -1``, :data:`INVENTORY_CHANGE_COLUMN`,
    :data:`RESIDUAL_VA_ROW`, and cells with ``pattern2017 < 0``.
    """
    illicit = (balanced < 0.0) & (mask.sign_lock != -1) & ~(pattern2017 < 0.0)
    if INVENTORY_CHANGE_COLUMN in balanced.columns:
        illicit = illicit.copy()
        illicit.loc[:, INVENTORY_CHANGE_COLUMN] = False
    if RESIDUAL_VA_ROW in balanced.index:
        illicit = illicit.copy()
        illicit.loc[RESIDUAL_VA_ROW, :] = False
    return illicit
