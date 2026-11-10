"""Tests for the Step 6 margin cap at each buyer's purchase (#1177).

Synthetic: the cap is the one piece of :mod:`nowcast_mut` that is arithmetic
rather than wiring, so it is checked on small frames; the build's own gates
cover it on the real tables.
"""

from __future__ import annotations

import functools

import pandas as pd
import pytest

from bedrock.transform.iot import nowcast_mut as nm

M = 1e6
BUYERS = ['621100', '622000', 'F01000', 'F03000']


def _purchaser() -> pd.DataFrame:
    return pd.DataFrame(
        [[2_800.0, 1_000.0, 6_000.0, -50.0], [100.0, 100.0, 100.0, 0.0]],
        index=['325411', '315000'],
        columns=BUYERS,
    ).mul(M)


def _placed() -> dict[str, pd.DataFrame]:
    """Wholesale and truck margins; 621100 is 300 over its purchase of
    325411, and F03000 carries a negative margin on a negative purchase."""
    wholesale = pd.DataFrame(
        [[3_000.0, 400.0, 1_200.0, -10.0], [20.0, 20.0, 20.0, 0.0]],
        index=['325411', '315000'],
        columns=BUYERS,
    ).mul(M)
    truck = pd.DataFrame(
        [[100.0, 100.0, 300.0, -2.0], [5.0, 5.0, 5.0, 0.0]],
        index=['325411', '315000'],
        columns=BUYERS,
    ).mul(M)
    return {'424200': wholesale, '484000': truck}


def _stripped(placed: dict[str, pd.DataFrame]) -> pd.DataFrame:
    return functools.reduce(lambda a, b: a + b, placed.values())


def test_the_cap_holds_each_buyer_to_its_purchase() -> None:
    capped = nm._cap_at_purchase(_placed(), _purchaser())
    stripped = _stripped(capped)
    assert stripped.loc['325411', '621100'] == pytest.approx(2_800.0 * M, abs=0.1 * M)
    # The 300 excess goes to 622000 and F01000 by their margins, 500 and 1,500.
    assert stripped.loc['325411', '622000'] == pytest.approx(575.0 * M, abs=0.1 * M)
    assert stripped.loc['325411', 'F01000'] == pytest.approx(1_725.0 * M, abs=0.1 * M)


def test_the_cap_keeps_each_margin_commodity_total_per_good() -> None:
    placed = _placed()
    capped = nm._cap_at_purchase(placed, _purchaser())
    for code in placed:
        pd.testing.assert_series_equal(
            capped[code].sum(axis=1), placed[code].sum(axis=1), atol=1.0
        )


def test_inventories_and_goods_under_their_purchase_are_left_alone() -> None:
    placed = _placed()
    capped = nm._cap_at_purchase(placed, _purchaser())
    for code in placed:
        assert (
            capped[code].loc['325411', 'F03000'] == placed[code].loc['325411', 'F03000']
        )
        assert (
            (capped[code].loc[['315000']] == placed[code].loc[['315000']]).all().all()
        )


def test_the_excess_cascades_past_a_buyer_it_fills() -> None:
    margin = pd.Series([300.0, 90.0, 10.0], index=['a', 'b', 'c']) * M
    limit = pd.Series([200.0, 100.0, 200.0], index=['a', 'b', 'c']) * M
    target = nm._fill_to_limit(margin, limit)
    assert target is not None
    assert (target / M).tolist() == pytest.approx([200.0, 100.0, 100.0])


def test_a_margin_above_all_purchases_cannot_be_filled() -> None:
    margin = pd.Series([300.0, 90.0], index=['a', 'b']) * M
    limit = pd.Series([200.0, 100.0], index=['a', 'b']) * M
    assert nm._fill_to_limit(margin, limit) is None


def _margins(producer: float) -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(
        [('621100', '325411'), ('F03000', '325411'), ('621100', '424200')],
        names=['Industry Code', 'Commodity Code'],
    )
    return pd.DataFrame(
        {
            nm._PRO: [producer, -60.0 * M, -5.0 * M],
            nm._PUR: [2_800.0 * M, -50.0 * M, 10.0 * M],
        },
        index=index,
    )


def test_the_gate_counts_only_positive_purchases_of_goods() -> None:
    report = nm._negative_producer_value(2024, _margins(-300.0 * M), enforce=False)
    assert report['cells'] == 1
    assert report['sum_usd_m'] == pytest.approx(-300.0)
    assert report['worst_cell'] == '621100/325411'


def test_the_gate_refuses_a_negative_cell_with_the_cap_on() -> None:
    with pytest.raises(AssertionError, match='621100/325411'):
        nm._negative_producer_value(2024, _margins(-300.0 * M), enforce=True)
    report = nm._negative_producer_value(2024, _margins(-0.5 * M), enforce=True)
    assert report['cells'] == 1
