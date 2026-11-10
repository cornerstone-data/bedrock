"""Hermetic tests for the #1229 scale-then-fix helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot.nowcast_sut_assembly import (
    DUST_USD_M,
    REFINERY_ROW_ROOM_FRAC,
    _assert_refinery_row_room,
    hold_column_shares,
)


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


def test_assert_refinery_row_room_raises_when_tight() -> None:
    use = pd.DataFrame({'a': [10.0], 'b': [0.0]}, index=['sec'])
    supply = pd.DataFrame({'i1': [9.6]}, index=['sec'])
    with pytest.raises(ValueError, match='leaves room'):
        _assert_refinery_row_room(2020, use, supply, 'i1', ['sec'])


def test_assert_refinery_row_room_passes_with_margin() -> None:
    use_row = 100.0
    held = use_row * (1.0 - REFINERY_ROW_ROOM_FRAC) - 1.0
    use = pd.DataFrame({'a': [use_row]}, index=['sec'])
    supply = pd.DataFrame({'i1': [held]}, index=['sec'])
    _assert_refinery_row_room(2022, use, supply, 'i1', ['sec'])

