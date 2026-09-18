"""Shared paths and illicit-sign predicate for #839 hygiene analysis."""

from __future__ import annotations

from pathlib import Path
from typing import TypedDict

import pandas as pd

from bedrock.transform.iot.nowcast_sut_assembly import illicit_negative_mask
from bedrock.utils.economic.balance.mask import SutMask

PACKAGE_DIR = Path(__file__).resolve().parent
#: Working artifacts (parquets, JSON censuses). Gitignored via parent ``output/``.
OUTPUT_DIR = PACKAGE_DIR.parent / 'output' / 'ras_improvements'
#: Prior local runs before the reorg (still readable as a fallback).
_LEGACY_OUTPUT_DIR = PACKAGE_DIR.parent / 'output' / 'ras_hygiene_839'

GCS_BALANCED_SUT = 'flowsa/BalancedSUT'


class ExemptionFillSplit(TypedDict):
    """Unprefixed metrics from :func:`exemption_pattern_fill_split`."""

    seed_fill_cells: int
    seed_fill_mass_usd_m: float
    bal_fill_cells: int
    bal_fill_mass_usd_m: float
    ras_introduced_cells: int
    ras_introduced_mass_usd_m: float
    ras_cleared_cells: int
    ras_cleared_mass_usd_m: float
    ras_moved_l1_usd_m: float
    bal_mass_from_seed_cells_usd_m: float
    ras_introduced_share: float
    seed_share_of_bal_fill_mass: float


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

    Delegates to
    :func:`~bedrock.transform.iot.nowcast_sut_assembly.illicit_negative_mask`
    so analysis stays in lockstep with production.
    """
    return illicit_negative_mask(balanced, mask, pattern2017)


def _require_matching_labels(a: pd.DataFrame, b: pd.DataFrame, what: str) -> None:
    if not a.index.equals(b.index):
        raise ValueError(f'{what}: row labels differ')
    if not a.columns.equals(b.columns):
        raise ValueError(f'{what}: column labels differ')


def exemption_pattern_fill_split(
    seed: pd.DataFrame,
    balanced: pd.DataFrame,
    pattern2017: pd.DataFrame,
    mask: SutMask,
) -> ExemptionFillSplit:
    """Decompose non-structural published-pattern fills into seed vs RAS.

    Eligible cells are zero in the published 2017 pattern and **not**
    ``mask.structural_zero`` (trade/fiscal exemptions). *seed* should be the
    post-``conform_seeds`` frame entering GRAS (``YearBalance.seeds``), not
    raw ``assemble_seeds``.

    Primary attribution ratio is ``ras_introduced_share`` (RAS-opened share of
    balanced exemption-fill mass). ``seed_share_of_bal_fill_mass`` is a
    **location** share only — balanced dollars on cells that were already
    nonzero in the seed, not seed-dollar attribution.
    """
    what = 'exemption_pattern_fill_split'
    _require_matching_labels(seed, balanced, what)
    _require_matching_labels(seed, pattern2017, f'{what} pattern2017')
    _require_matching_labels(seed, mask.structural_zero, f'{what} structural_zero')

    eligible = (pattern2017 == 0.0) & ~mask.structural_zero
    seed_nz = eligible & (seed != 0.0)
    bal_nz = eligible & (balanced != 0.0)
    ras_introduced = eligible & (seed == 0.0) & (balanced != 0.0)
    ras_cleared = eligible & (seed != 0.0) & (balanced == 0.0)
    both_nz = seed_nz & bal_nz
    moved = eligible & (balanced != seed)

    seed_fill_cells = int(seed_nz.to_numpy().sum())
    seed_fill_mass = float(seed.where(seed_nz, 0.0).abs().sum().sum())
    bal_fill_cells = int(bal_nz.to_numpy().sum())
    bal_fill_mass = float(balanced.where(bal_nz, 0.0).abs().sum().sum())
    ras_introduced_cells = int(ras_introduced.to_numpy().sum())
    ras_introduced_mass = float(balanced.where(ras_introduced, 0.0).abs().sum().sum())
    ras_cleared_cells = int(ras_cleared.to_numpy().sum())
    ras_cleared_mass = float(seed.where(ras_cleared, 0.0).abs().sum().sum())
    ras_moved_l1 = float((balanced - seed).where(moved, 0.0).abs().sum().sum())
    bal_mass_from_seed = float(balanced.where(both_nz, 0.0).abs().sum().sum())

    if bal_fill_mass == 0.0:
        ras_introduced_share = 0.0
        seed_share = 0.0
    else:
        ras_introduced_share = ras_introduced_mass / bal_fill_mass
        seed_share = bal_mass_from_seed / bal_fill_mass

    return {
        'seed_fill_cells': seed_fill_cells,
        'seed_fill_mass_usd_m': seed_fill_mass,
        'bal_fill_cells': bal_fill_cells,
        'bal_fill_mass_usd_m': bal_fill_mass,
        'ras_introduced_cells': ras_introduced_cells,
        'ras_introduced_mass_usd_m': ras_introduced_mass,
        'ras_cleared_cells': ras_cleared_cells,
        'ras_cleared_mass_usd_m': ras_cleared_mass,
        'ras_moved_l1_usd_m': ras_moved_l1,
        'bal_mass_from_seed_cells_usd_m': bal_mass_from_seed,
        'ras_introduced_share': ras_introduced_share,
        'seed_share_of_bal_fill_mass': seed_share,
    }
