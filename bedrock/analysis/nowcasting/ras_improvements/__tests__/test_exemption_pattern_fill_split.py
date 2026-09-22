"""Hermetic tests for seed vs RAS exemption-fill split (#915 follow-up)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.nowcasting.ras_improvements.common import (
    exemption_pattern_fill_split,
)
from bedrock.utils.economic.balance.mask import SutMask


def _mask(
    *,
    index: list[str],
    columns: list[str],
    structural_zero: pd.DataFrame | None = None,
) -> SutMask:
    zeros = (
        pd.DataFrame(False, index=index, columns=columns)
        if structural_zero is None
        else structural_zero.astype(bool)
    )
    fixed = pd.DataFrame(False, index=index, columns=columns)
    locks = pd.DataFrame(0, index=index, columns=columns, dtype=int)
    return SutMask(structural_zero=zeros, fixed_value=fixed, sign_lock=locks)


def test_all_mass_already_in_seed_no_ras_introduced() -> None:
    index, columns = ['c1', 'c2'], ['i1', 'i2']
    # Pattern zero on (c1,i1); free (not structural). Seed and bal both filled.
    pattern = pd.DataFrame([[0.0, 1.0], [1.0, 1.0]], index=index, columns=columns)
    seed = pd.DataFrame([[10.0, 0.0], [0.0, 0.0]], index=index, columns=columns)
    bal = pd.DataFrame([[12.0, 0.0], [0.0, 0.0]], index=index, columns=columns)
    mask = _mask(index=index, columns=columns)

    split = exemption_pattern_fill_split(seed, bal, pattern, mask)
    assert split['seed_fill_cells'] == 1
    assert split['seed_fill_mass_usd_m'] == pytest.approx(10.0)
    assert split['bal_fill_cells'] == 1
    assert split['bal_fill_mass_usd_m'] == pytest.approx(12.0)
    assert split['ras_introduced_cells'] == 0
    assert split['ras_introduced_mass_usd_m'] == pytest.approx(0.0)
    assert split['ras_introduced_share'] == pytest.approx(0.0)
    assert split['seed_share_of_bal_fill_mass'] == pytest.approx(1.0)
    assert split['ras_moved_l1_usd_m'] == pytest.approx(2.0)
    assert split['ras_introduced_share'] + split[
        'seed_share_of_bal_fill_mass'
    ] == pytest.approx(1.0)


def test_seed_empty_bal_fills_all_ras_introduced() -> None:
    index, columns = ['c1', 'c2'], ['i1', 'i2']
    pattern = pd.DataFrame([[0.0, 1.0], [0.0, 1.0]], index=index, columns=columns)
    seed = pd.DataFrame(0.0, index=index, columns=columns)
    bal = pd.DataFrame([[5.0, 0.0], [3.0, 0.0]], index=index, columns=columns)
    mask = _mask(index=index, columns=columns)

    split = exemption_pattern_fill_split(seed, bal, pattern, mask)
    assert split['seed_fill_cells'] == 0
    assert split['bal_fill_cells'] == 2
    assert split['bal_fill_mass_usd_m'] == pytest.approx(8.0)
    assert split['ras_introduced_cells'] == 2
    assert split['ras_introduced_mass_usd_m'] == pytest.approx(8.0)
    assert split['ras_introduced_share'] == pytest.approx(1.0)
    assert split['seed_share_of_bal_fill_mass'] == pytest.approx(0.0)
    assert split['ras_introduced_share'] + split[
        'seed_share_of_bal_fill_mass'
    ] == pytest.approx(1.0)


def test_seed_fill_cleared_by_balance() -> None:
    index, columns = ['c1'], ['i1', 'i2']
    pattern = pd.DataFrame([[0.0, 0.0]], index=index, columns=columns)
    seed = pd.DataFrame([[7.0, 0.0]], index=index, columns=columns)
    bal = pd.DataFrame([[0.0, 0.0]], index=index, columns=columns)
    mask = _mask(index=index, columns=columns)

    split = exemption_pattern_fill_split(seed, bal, pattern, mask)
    assert split['ras_cleared_cells'] == 1
    assert split['ras_cleared_mass_usd_m'] == pytest.approx(7.0)
    assert split['bal_fill_cells'] == 0
    assert split['bal_fill_mass_usd_m'] == pytest.approx(0.0)
    assert split['ras_introduced_share'] == pytest.approx(0.0)
    assert split['seed_share_of_bal_fill_mass'] == pytest.approx(0.0)


def test_structural_zero_excluded_from_eligible() -> None:
    index, columns = ['c1', 'c2'], ['i1']
    pattern = pd.DataFrame([[0.0], [0.0]], index=index, columns=columns)
    structural = pd.DataFrame([[True], [False]], index=index, columns=columns)
    seed = pd.DataFrame([[0.0], [0.0]], index=index, columns=columns)
    # Tier-0 cell filled + exemption cell filled — only exemption counts.
    bal = pd.DataFrame([[99.0], [4.0]], index=index, columns=columns)
    mask = _mask(index=index, columns=columns, structural_zero=structural)

    split = exemption_pattern_fill_split(seed, bal, pattern, mask)
    assert split['bal_fill_cells'] == 1
    assert split['bal_fill_mass_usd_m'] == pytest.approx(4.0)
    assert split['ras_introduced_cells'] == 1
    assert split['ras_introduced_mass_usd_m'] == pytest.approx(4.0)


def test_denom_zero_shares_are_zero() -> None:
    index, columns = ['c1'], ['i1']
    pattern = pd.DataFrame([[0.0]], index=index, columns=columns)
    seed = pd.DataFrame([[0.0]], index=index, columns=columns)
    bal = pd.DataFrame([[0.0]], index=index, columns=columns)
    mask = _mask(index=index, columns=columns)

    split = exemption_pattern_fill_split(seed, bal, pattern, mask)
    assert split['bal_fill_mass_usd_m'] == pytest.approx(0.0)
    assert split['ras_introduced_share'] == pytest.approx(0.0)
    assert split['seed_share_of_bal_fill_mass'] == pytest.approx(0.0)


def test_mismatched_labels_raise() -> None:
    index, columns = ['c1'], ['i1']
    pattern = pd.DataFrame([[0.0]], index=index, columns=columns)
    seed = pd.DataFrame([[0.0]], index=index, columns=columns)
    bal = pd.DataFrame([[1.0]], index=['c2'], columns=columns)
    mask = _mask(index=index, columns=columns)
    with pytest.raises(ValueError, match='row labels differ'):
        exemption_pattern_fill_split(seed, bal, pattern, mask)
