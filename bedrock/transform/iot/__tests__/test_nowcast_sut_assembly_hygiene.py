"""Hermetic tests for #839 post-balance hygiene and residue sweep."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_mask import INVENTORY_CHANGE_COLUMN
from bedrock.transform.iot.nowcast_sut_assembly import (
    ZERO_PATTERN_MASS_USD_M,
    assert_post_balance_hygiene,
    balance_year,
    illicit_sign_residue,
    structural_zero_leak,
    sweep_offset_residue,
    zero_pattern_leak,
)
from bedrock.utils.economic.balance.mask import SutMask


def _mask(
    *,
    index: list[str],
    columns: list[str],
    structural_zero: pd.DataFrame | None = None,
    sign_lock: pd.DataFrame | None = None,
) -> SutMask:
    zeros = (
        pd.DataFrame(False, index=index, columns=columns)
        if structural_zero is None
        else structural_zero.astype(bool)
    )
    locks = (
        pd.DataFrame(0, index=index, columns=columns, dtype=int)
        if sign_lock is None
        else sign_lock.astype(int)
    )
    fixed = pd.DataFrame(False, index=index, columns=columns)
    return SutMask(structural_zero=zeros, fixed_value=fixed, sign_lock=locks)


def test_zero_pattern_leak_uses_published_pattern_not_structural_mask() -> None:
    """#839 2(a): count fills where the published pattern is zero.

    A cell that is free in the mask (not structural_zero) but zero in the
    published pattern must still count — those are the deliberate exemptions.
    """
    index, columns = ['c1', 'c2'], ['i1', 'i2']
    # Pattern has zeros in col i1; those cells are free in a typical mask
    # (trade/fiscal exemptions) — 2(a) must still count them.
    pattern = pd.DataFrame([[0.0, 5.0], [0.0, 5.0]], index=index, columns=columns)
    dusty = pd.DataFrame([[0.4, 1.0], [0.4, 2.0]], index=index, columns=columns)
    n_cells, mass = zero_pattern_leak(dusty, pattern)
    assert n_cells == 2
    assert mass == pytest.approx(0.8)


def test_structural_zero_leak_mass_gates_on_dollars_not_count() -> None:
    index, columns = ['c1', 'c2'], ['i1', 'i2']
    # Fail gate is structural_zero; published-pattern leak is census-only.
    structural = pd.DataFrame(
        [[True, False], [True, False]], index=index, columns=columns
    )
    mask = _mask(index=index, columns=columns, structural_zero=structural)
    pattern = pd.DataFrame([[0.0, 5.0], [0.0, 5.0]], index=index, columns=columns)
    supply_pattern = pd.DataFrame(1.0, index=index, columns=columns)
    patterns = {'use': pattern, 'supply': supply_pattern}

    dusty = pd.DataFrame([[0.4, 1.0], [0.4, 2.0]], index=index, columns=columns)
    n_pub, mass_pub = zero_pattern_leak(dusty, pattern)
    assert n_pub == 2
    assert mass_pub == pytest.approx(0.8)
    n_sz, mass_sz = structural_zero_leak(dusty, mask)
    assert n_sz == 2
    assert mass_sz == pytest.approx(0.8)
    assert_post_balance_hygiene(
        2022,
        {'use': dusty, 'supply': dusty.copy()},
        {'use': mask, 'supply': mask},
        patterns2017=patterns,
        zero_mass_bound=ZERO_PATTERN_MASS_USD_M,
    )
    heavy = pd.DataFrame([[0.6, 1.0], [0.6, 2.0]], index=index, columns=columns)
    n_sz, mass_sz = structural_zero_leak(heavy, mask)
    assert n_sz == 2
    assert mass_sz == pytest.approx(1.2)
    with pytest.raises(ValueError, match='structural-zero leak mass'):
        assert_post_balance_hygiene(
            2022,
            {'use': heavy, 'supply': dusty.copy()},
            {'use': mask, 'supply': mask},
            patterns2017=patterns,
        )


def test_illicit_sign_residue_above_eps_fails_below_passes() -> None:
    index = ['c1', 'V00300']
    columns = ['i1', INVENTORY_CHANGE_COLUMN]
    mask = _mask(index=index, columns=columns)
    # Nonzero published pattern everywhere so 2(a) does not fire; this test is 2(b).
    pattern = pd.DataFrame(1.0, index=index, columns=columns)
    supply_pattern = pd.DataFrame(1.0, index=['c1'], columns=['i1'])
    # Illicit below eps → pass; max_abs of above-eps set is 0.
    dusty = pd.DataFrame([[-0.01, -1.0], [-2.0, 0.0]], index=index, columns=columns)
    n_above, max_abs = illicit_sign_residue(dusty, mask, pattern)
    assert n_above == 0
    assert max_abs == 0.0
    # Illicit above eps → fail with that cell's abs.
    bad = dusty.copy()
    bad.loc['c1', 'i1'] = -0.06
    n_above, max_abs = illicit_sign_residue(bad, mask, pattern)
    assert n_above == 1
    assert max_abs == pytest.approx(0.06)
    with pytest.raises(ValueError, match='illicit negative'):
        assert_post_balance_hygiene(
            2023,
            {'use': bad, 'supply': pd.DataFrame(0.0, index=['c1'], columns=['i1'])},
            {
                'use': mask,
                'supply': _mask(index=['c1'], columns=['i1']),
            },
            patterns2017={'use': pattern, 'supply': supply_pattern},
        )


def test_illicit_sign_whitelist_and_label_contract() -> None:
    index = ['c1', 'V00300', 'sub']
    columns = ['i1', INVENTORY_CHANGE_COLUMN]
    locks = pd.DataFrame(0, index=index, columns=columns, dtype=int)
    locks.loc['sub', 'i1'] = -1
    mask = _mask(index=index, columns=columns, sign_lock=locks)
    pattern = pd.DataFrame(0.0, index=index, columns=columns)
    pattern.loc['c1', 'i1'] = -10.0  # published negative → not illicit
    balanced = pd.DataFrame(
        [[-5.0, -5.0], [-5.0, 0.0], [-5.0, 0.0]],
        index=index,
        columns=columns,
    )
    n_above, max_abs = illicit_sign_residue(balanced, mask, pattern)
    assert n_above == 0
    assert max_abs == 0.0

    misaligned = pattern.drop(index='sub')
    with pytest.raises(ValueError, match='row labels differ'):
        illicit_sign_residue(balanced, mask, misaligned)


def test_sweep_offset_residue_illicit_below_eps_only() -> None:
    index = ['c1', 'V00300']
    columns = ['i1', INVENTORY_CHANGE_COLUMN]
    mask = _mask(index=index, columns=columns)
    pattern = pd.DataFrame(0.0, index=index, columns=columns)
    use = pd.DataFrame(
        [[-0.01, -1.0], [-2.0, 1e-4]],
        index=index,
        columns=columns,
    )
    cleaned, n_swept = sweep_offset_residue(use, mask, pattern)
    assert n_swept == 1
    assert cleaned.loc['c1', 'i1'] == 0.0
    assert cleaned.loc['c1', INVENTORY_CHANGE_COLUMN] == pytest.approx(-1.0)
    assert cleaned.loc['V00300', 'i1'] == pytest.approx(-2.0)
    assert cleaned.loc['V00300', INVENTORY_CHANGE_COLUMN] == pytest.approx(1e-4)
    assert use.loc['c1', 'i1'] == pytest.approx(-0.01)


def test_balance_year_calls_assert_post_balance_hygiene(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wiring gate: balance_year must surface hygiene failures after restore."""
    use = pd.DataFrame([[1.0]], index=['c1'], columns=['i1'])
    supply = pd.DataFrame([[1.0]], index=['c1'], columns=['i1'])
    mask = _mask(index=['c1'], columns=['i1'])
    assembled = MagicMock()
    assembled.year = 2022
    assembled.seeds = {'use': use, 'supply': supply}
    assembled.masks = {'use': mask, 'supply': mask}
    assembled.targets = MagicMock()
    assembled.sweep = pd.DataFrame()

    monkeypatch.setattr(
        'bedrock.transform.iot.nowcast_sut_assembly.assemble',
        lambda *args, **kwargs: assembled,
    )
    monkeypatch.setattr(
        'bedrock.transform.iot.nowcast_sut_assembly.split_fixed_blocks',
        lambda seeds, masks: ({}, seeds),
    )
    monkeypatch.setattr(
        'bedrock.transform.iot.nowcast_sut_assembly.offset_targets',
        lambda targets, frozen: targets,
    )
    result = MagicMock()
    result.blocks = {'use': use, 'supply': supply}
    monkeypatch.setattr(
        'bedrock.transform.iot.nowcast_sut_assembly.engine',
        lambda *args, **kwargs: result,
    )
    monkeypatch.setattr(
        'bedrock.transform.iot.nowcast_sut_assembly.restore_fixed_blocks',
        lambda blocks, frozen: blocks,
    )

    def _boom(*args: object, **kwargs: object) -> None:
        raise ValueError('2022 use: hygiene boom')

    monkeypatch.setattr(
        'bedrock.transform.iot.nowcast_sut_assembly.assert_post_balance_hygiene',
        _boom,
    )
    with pytest.raises(ValueError, match='hygiene boom'):
        balance_year(2022)
