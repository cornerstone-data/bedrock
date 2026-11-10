"""Hermetic tests for the #1229 scale-then-fix helpers."""

from __future__ import annotations

import typing as ta

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_sut_assembly as nsa
from bedrock.transform.iot.nowcast_sut_assembly import (
    DUST_USD_M,
    REFINERY_ROW_ROOM_FRAC,
    REFINERY_SHARE_TOL_PP,
    ColumnPin,
    _assert_refinery_row_room,
    apply_refinery_secondaries_go_mix_pin,
    balance_with_column_pin,
    hold_column_shares,
)
from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.targets import TargetSet


def _mask(
    frame: pd.DataFrame,
    *,
    zero: ta.Iterable[tuple[str, str]] = (),
    fixed: ta.Iterable[tuple[str, str]] = (),
) -> SutMask:
    """A mask on *frame*'s labels, free except the named cells."""
    structural = pd.DataFrame(False, index=frame.index, columns=frame.columns)
    fixed_value = structural.copy()
    for row, col in zero:
        structural.at[row, col] = True
    for row, col in fixed:
        fixed_value.at[row, col] = True
    sign = pd.DataFrame(0, index=frame.index, columns=frame.columns, dtype=int)
    return SutMask(structural_zero=structural, fixed_value=fixed_value, sign_lock=sign)


def test_hold_column_shares_scales_to_pin() -> None:
    supply = pd.DataFrame(
        {'i1': [70.0, 30.0], 'i2': [1.0, 1.0]},
        index=['own', 'sec'],
    )
    scaled, held = hold_column_shares(supply, 'i1', ['sec'], 80.0)
    assert held == ['sec']
    assert scaled.at['sec', 'i1'] == pytest.approx(24.0)
    assert scaled.at['own', 'i1'] == 70.0
    assert supply.at['sec', 'i1'] == 30.0  # input unchanged


def test_hold_column_shares_skips_dust_cells() -> None:
    supply = pd.DataFrame(
        {'i1': [100.0, DUST_USD_M]},
        index=['own', 'sec'],
    )
    scaled, held = hold_column_shares(supply, 'i1', ['sec'], 80.0)
    assert held == []
    assert scaled.at['sec', 'i1'] == DUST_USD_M


def test_hold_column_shares_raises_when_held_exceeds_pin() -> None:
    # Negative free cell makes a held share > 1 of the column total.
    supply = pd.DataFrame(
        {'i1': [-50.0, 100.0]},
        index=['own', 'sec'],
    )
    with pytest.raises(ValueError, match='exceeds column pin'):
        hold_column_shares(supply, 'i1', ['sec'], 50.0)


# --- the row-room guard: what the free Supply cells must carry ---------------


def test_assert_refinery_row_room_raises_when_tight() -> None:
    use = pd.DataFrame({'a': [10.0], 'b': [0.0]}, index=['sec'])
    supply = pd.DataFrame({'i1': [9.6], 'i2': [1.0]}, index=['sec'])
    with pytest.raises(ValueError, match='leaves room'):
        _assert_refinery_row_room(2020, use, supply, _mask(supply), 'i1', ['sec'])


def test_assert_refinery_row_room_passes_with_margin() -> None:
    use_row = 100.0
    held = use_row * (1.0 - REFINERY_ROW_ROOM_FRAC) - 1.0
    use = pd.DataFrame({'a': [use_row]}, index=['sec'])
    supply = pd.DataFrame({'i1': [held], 'i2': [5.0]}, index=['sec'])
    _assert_refinery_row_room(2022, use, supply, _mask(supply), 'i1', ['sec'])


def test_other_fixed_cells_on_the_row_count_against_the_room() -> None:
    """Held 50 of a 100 row leaves room, until another fixed cell takes 48."""
    use = pd.DataFrame({'a': [100.0]}, index=['sec'])
    supply = pd.DataFrame({'i1': [50.0], 'i2': [2.0], 'MCIF': [48.0]}, index=['sec'])
    _assert_refinery_row_room(2020, use, supply, _mask(supply), 'i1', ['sec'])

    with pytest.raises(ValueError, match='leaves room'):
        _assert_refinery_row_room(
            2020, use, supply, _mask(supply, fixed=[('sec', 'MCIF')]), 'i1', ['sec']
        )


def test_free_cells_with_no_positive_mass_cannot_carry_the_row() -> None:
    use = pd.DataFrame({'a': [100.0]}, index=['sec'])
    supply = pd.DataFrame({'i1': [30.0], 'i2': [-5.0]}, index=['sec'])
    with pytest.raises(ValueError, match='leaves room'):
        _assert_refinery_row_room(2020, use, supply, _mask(supply), 'i1', ['sec'])


# --- flag on: the wiring inside assemble -------------------------------------


class _T1:
    """The two attributes of a ``T1`` target the pin reads."""

    name = 'T1'

    def __init__(self, values: pd.Series) -> None:
        self.values = values


def _refinery_year() -> tuple[dict[str, pd.DataFrame], dict[str, SutMask]]:
    """324110 makes own product 80 and 325110 20; its 325190 cell is a 2017
    zero, so the conformed seed has 0 there. The column carries 4.0 of tax."""
    supply = pd.DataFrame(
        {
            '324110': [80.0, 20.0, 0.0],
            '325000': [0.0, 200.0, 300.0],
        },
        index=['324110', '325110', '325190'],
    )
    use = pd.DataFrame(
        {
            '324110': [10.0, 5.0, 5.0, 6.0, -2.0],
            'F01000': [70.0, 215.0, 295.0, 0.0, 0.0],
        },
        index=['324110', '325110', '325190', 'T00TOP', 'T00SUB'],
    )
    masks = {
        'supply': _mask(supply, zero=[('325190', '324110')]),
        'use': _mask(use),
    }
    return {'supply': supply, 'use': use}, masks


@pytest.fixture
def flag_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        nsa, 'pinned_refinery_secondaries', lambda: ('325110', '325190')
    )
    monkeypatch.setattr(
        nsa,
        'rest_of_world_adjustment_supply_make',
        lambda: pd.Series({'324110': 1.0}),
    )


def _targets(go: float) -> TargetSet:
    return ta.cast(TargetSet, [_T1(pd.Series({'324110': go}))])


@pytest.mark.usefixtures('flag_on')
def test_flag_on_holds_the_co_product_at_seed_share_of_the_pin() -> None:
    """Pin = GO 85 - (TOP 6 + SUB -2) - S00900 1 = 80; 325110 holds 20% of it."""
    seeds, masks = _refinery_year()

    new_seeds, new_masks, pin = apply_refinery_secondaries_go_mix_pin(
        2022, seeds, masks, _targets(85.0)
    )

    assert pin is not None
    assert pin.industry == '324110'
    assert pin.shares == {'325110': pytest.approx(0.2)}
    assert new_seeds['supply'].at['325110', '324110'] == pytest.approx(16.0)
    # 325190 is a 2017 zero for refineries: zero on the seed, so never held.
    assert new_seeds['supply'].at['325190', '324110'] == 0.0
    fixed = new_masks['supply'].fixed_value
    assert bool(fixed.at['325110', '324110'])
    assert int(fixed.to_numpy().sum()) == 1
    assert seeds['supply'].at['325110', '324110'] == 20.0  # input unchanged


def test_flag_off_leaves_seeds_and_masks_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(nsa, 'pinned_refinery_secondaries', lambda: ())
    seeds, masks = _refinery_year()

    out_seeds, out_masks, pin = apply_refinery_secondaries_go_mix_pin(
        2022, seeds, masks, _targets(85.0)
    )

    assert pin is None
    assert out_seeds is seeds
    assert out_masks is masks


# --- flag on: the re-pin on the balanced column ------------------------------

_Run = ta.Callable[[dict[str, pd.DataFrame]], tuple[ta.Any, dict[str, pd.DataFrame]]]


def _fake_run(totals: ta.Iterator[float], calls: list[pd.DataFrame]) -> _Run:
    """A balance that lands the 324110 column on the next of *totals*, with the
    held 325110 cell fixed and own product taking the rest - as when the
    balance moves the column's tax rows away from the seed's."""

    def run(seeds: dict[str, pd.DataFrame]) -> tuple[ta.Any, dict[str, pd.DataFrame]]:
        supply = seeds['supply'].copy()
        calls.append(supply)
        held = float(np.asarray(supply.loc['325110', '324110']).item())
        supply.loc['324110', '324110'] = next(totals) - held
        return object(), {**seeds, 'supply': supply}

    return run


def _pinned() -> tuple[dict[str, pd.DataFrame], dict[str, SutMask], ColumnPin]:
    seeds, masks = _refinery_year()
    seeds, masks, pin = apply_refinery_secondaries_go_mix_pin(
        2022, seeds, masks, _targets(85.0)
    )
    assert pin is not None
    return seeds, masks, pin


@pytest.mark.usefixtures('flag_on')
def test_the_pin_follows_the_balanced_column_until_the_share_holds() -> None:
    """Seed pin 80, but the balance lands the column on 90: the held 16 is
    17.8% of it. One re-pin at 90 puts it at 18, back to 20%."""
    seeds, masks, pin = _pinned()
    calls: list[pd.DataFrame] = []

    final, _out, balanced = balance_with_column_pin(
        2022, seeds, masks, pin, _fake_run(iter([90.0, 90.0]), calls)
    )

    assert len(calls) == 2
    assert final['supply'].at['325110', '324110'] == pytest.approx(18.0)
    assert float(pin.drift_pp(balanced['supply']).abs().max()) <= REFINERY_SHARE_TOL_PP


@pytest.mark.usefixtures('flag_on')
def test_a_column_that_never_settles_is_refused() -> None:
    seeds, masks, pin = _pinned()
    calls: list[pd.DataFrame] = []

    with pytest.raises(ValueError, match='still off by'):
        balance_with_column_pin(
            2022,
            seeds,
            masks,
            pin,
            _fake_run(iter([90.0, 100.0, 110.0]), calls),
            rounds=3,
        )
    assert len(calls) == 3


def test_without_a_pin_the_balance_runs_once() -> None:
    seeds, masks = _refinery_year()
    calls: list[pd.DataFrame] = []

    balance_with_column_pin(2022, seeds, masks, None, _fake_run(iter([90.0]), calls))

    assert len(calls) == 1
