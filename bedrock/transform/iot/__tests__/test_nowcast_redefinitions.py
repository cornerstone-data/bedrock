"""The Step 7 mechanics on synthetic frames.

The 2017 replay against the published tables is the module's ``--check``;
here only the pure movement machinery is under test, on tiny frames built
from real taxonomy codes (the functions filter by the taxonomy sets).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot.margin_rates import MARGIN_COMMODITIES
from bedrock.transform.iot.nowcast_redefinitions import (
    ATOL,
    RedefinitionAnchor,
    _carry_ratios,
    _close_rows,
    learn_fractions,
    make_after_redef,
    margins_after_redef,
    use_after_redef,
)

M = 1e6
COMMODITIES = ['1111A0', '1111B0', '111200']
INDUSTRIES = ['1111A0', '1111B0', '111200']
VA_ROWS = ['V00100', 'V00200', 'V00300']
FD = ['F01000']


def _make(values: list[list[float]]) -> pd.DataFrame:
    return pd.DataFrame(
        np.asarray(values, dtype=float) * M, index=INDUSTRIES, columns=COMMODITIES
    )


def test_learn_fractions_full_partial_and_rounding() -> None:
    before = _make([[100, 40, 0], [10, 200, 0], [0, 0.4, 300]])
    after = _make([[100, 0, 0], [10 - 4, 244, 0], [0, 0.4, 300]])
    # (1111B0 -> 1111B0's donors): 1111A0 loses its whole 40, 111200's 0.4
    # is sub-ATOL noise; (1111A0 column) 1111B0... here 1111B0 row loses 4 of
    # its 10 in column 1111A0? No - keep it simple: 1111B0 row, 1111A0
    # column stays; the 4 lost is from row 1111B0, column 1111A0.
    after.loc['1111B0', '1111A0'] = 6 * M
    after.loc['1111A0', '1111A0'] = 104 * M
    fractions = learn_fractions(before, after)
    assert fractions.at['1111A0', '1111B0'] == pytest.approx(1.0)
    assert fractions.at['1111B0', '1111A0'] == pytest.approx(0.4)
    assert fractions.at['111200', '1111B0'] == 0.0  # sub-ATOL never learned
    for code in COMMODITIES:
        assert fractions.at[code, code] == 0.0  # the diagonal is implied


def test_make_after_redef_moves_and_conserves() -> None:
    before = _make([[100, 40, 0], [0, 200, 0], [0, 0, 300]])
    fractions = pd.DataFrame(0.0, index=INDUSTRIES, columns=COMMODITIES)
    fractions.at['1111A0', '1111B0'] = 1.0
    after = make_after_redef(before, fractions)
    assert after.at['1111A0', '1111B0'] == 0.0
    assert after.at['1111B0', '1111B0'] == pytest.approx(240 * M)
    # commodity output invariant, industry output moved
    pd.testing.assert_series_equal(after.sum(axis=0), before.sum(axis=0))
    assert after.sum(axis=1)['1111A0'] == pytest.approx(100 * M)


def test_carry_ratios_empty_before_is_one() -> None:
    before = _make([[0, 40, 0], [0, 200, 0], [0, 0, 300]])
    after = _make([[7, 80, 0], [0, 100, 0], [0, 0, 300]])
    ratios = _carry_ratios(before, after)
    assert ratios.at['1111A0', '1111A0'] == 1.0  # 0 -> 7 cannot be a ratio
    assert ratios.at['1111A0', '1111B0'] == pytest.approx(2.0)
    assert ratios.at['1111B0', '1111B0'] == pytest.approx(0.5)


def test_close_rows_zeroes_net_change_and_passes_untouched_rows() -> None:
    before = _make([[100, 50, 0], [10, 200, 30], [0, 0, 300]])
    carried = before.copy()
    carried.loc['1111A0'] = [120 * M, 60 * M, 0.0]  # net +30 on the row
    closed = _close_rows(carried, before)
    pd.testing.assert_series_equal(closed.sum(axis=1), before.sum(axis=1))
    pd.testing.assert_frame_equal(closed.loc[['111200']], before.loc[['111200']])
    # the correction is spread over the touched cells, not dumped on one
    assert float(closed.to_numpy()[0, 0]) < 120 * M
    assert float(closed.to_numpy()[0, 1]) < 60 * M


def _full_use() -> pd.DataFrame:
    interior = pd.DataFrame(
        np.array([[50, 20, 5], [10, 80, 15], [5, 10, 120]], dtype=float) * M,
        index=COMMODITIES,
        columns=INDUSTRIES,
    )
    value_added = pd.DataFrame(
        np.array([[30, 40, 60], [5, 5, 10], [20, 25, 30]], dtype=float) * M,
        index=VA_ROWS,
        columns=INDUSTRIES,
    )
    fd = pd.DataFrame(
        np.array([[7], [8], [9], [0], [0], [0]], dtype=float) * M,
        index=COMMODITIES + VA_ROWS,
        columns=FD,
    )
    return pd.concat([pd.concat([interior, value_added]), fd], axis=1)


def test_use_after_redef_identities_and_fd_passthrough() -> None:
    use_before = _full_use()
    interior_ratios = pd.DataFrame(1.0, index=COMMODITIES, columns=INDUSTRIES)
    interior_ratios.at['1111A0', '1111A0'] = 0.8
    interior_ratios.at['1111A0', '1111B0'] = 1.5
    anchor = RedefinitionAnchor(
        fractions=pd.DataFrame(0.0, index=INDUSTRIES, columns=COMMODITIES),
        use_ratios=interior_ratios,
        va_ratios=pd.DataFrame(1.0, index=VA_ROWS, columns=INDUSTRIES),
    )
    x_after = pd.Series([115.0 * M, 190.0 * M, 240.0 * M], index=INDUSTRIES)

    table = use_after_redef(use_before, anchor, x_after)

    # commodity rows keep their totals through the carry
    pd.testing.assert_series_equal(
        table.loc[COMMODITIES].sum(axis=1),
        use_before.loc[COMMODITIES].sum(axis=1),
    )
    # every industry column closes on the after-redefinitions output
    closed = table.loc[COMMODITIES + VA_ROWS, INDUSTRIES].sum(axis=0)
    assert (closed - x_after).abs().max() <= ATOL
    # final demand crosses untouched
    pd.testing.assert_frame_equal(table[FD], use_before[FD])


def test_margins_after_redef_scales_rows_and_recomputes_routing() -> None:
    wholesale = MARGIN_COMMODITIES[0]  # a real margin commodity code
    goods = '1111A0'
    buyer = '111200'
    index = pd.MultiIndex.from_tuples(
        [(buyer, goods), (buyer, wholesale)],
        names=['Industry Code', 'Commodity Code'],
    )
    columns = [
        "Producers' Value",
        'Transportation',
        'Wholesale',
        'Retail',
        "Purchasers' Value",
        wholesale,
    ]
    before = pd.DataFrame(
        [
            [80.0 * M, 0.0, 20.0 * M, 0.0, 100.0 * M, 20.0 * M],
            [25.0 * M, 0.0, 0.0, 0.0, 5.0 * M, 0.0],
        ],
        index=index,
        columns=columns,
    )
    # The producer-price Use books a margin commodity as direct + routed
    # (5 + 20 before; the goods transaction grows 1.5x so routing becomes 30
    # while the direct purchase stays 5). Scaling the margin row by its own
    # Use-cell ratio would distort the direct part - the found defect - so
    # both of its values must be rebuilt from the after cell instead.
    use_before = pd.DataFrame(
        {buyer: [100.0 * M, (5.0 + 20.0) * M]}, index=[goods, wholesale]
    )
    use_after = pd.DataFrame(
        {buyer: [150.0 * M, (5.0 + 30.0) * M]}, index=[goods, wholesale]
    )

    after = margins_after_redef(before, use_before, use_after)

    # the goods transaction scales 1.5x, identity intact
    assert after.at[(buyer, goods), "Purchasers' Value"] == pytest.approx(150.0 * M)
    assert after.at[(buyer, goods), 'Wholesale'] == pytest.approx(30.0 * M)
    # the margin row's direct purchase survives unchanged, its routing is
    # recomputed, and Producers' Value equals the after-Use cell exactly
    assert after.at[(buyer, wholesale), "Purchasers' Value"] == pytest.approx(5.0 * M)
    assert after.at[(buyer, wholesale), "Producers' Value"] == pytest.approx(
        use_after.at[wholesale, buyer]
    )


def test_close_rows_never_reopens_anchor_zeroed_cells() -> None:
    before = pd.DataFrame(
        [[5.0 * M, 100.0 * M, 0.0]], index=['1111A0'], columns=INDUSTRIES
    )
    carried = pd.DataFrame(
        [[0.0, 195.0 * M, 0.0]], index=['1111A0'], columns=INDUSTRIES
    )
    frozen = pd.DataFrame([[True, False, False]], index=['1111A0'], columns=INDUSTRIES)
    closed = _close_rows(carried, before, frozen=frozen)
    # the deliberately zeroed cell stays zero instead of going to -4.5
    assert closed.at['1111A0', '1111A0'] == 0.0
    assert closed.at['1111A0', '1111B0'] == pytest.approx(105.0 * M)
    assert closed.sum(axis=1)['1111A0'] == pytest.approx(105.0 * M)


def test_va_closure_never_flips_a_negative_column() -> None:
    use_before = _full_use()
    # an industry with negative total value added, like transit at 2017
    use_before.loc['V00100', '1111A0'] = 5.0 * M
    use_before.loc['V00200', '1111A0'] = -2.0 * M
    use_before.loc['V00300', '1111A0'] = -43.0 * M  # column VA sums to -40
    anchor = RedefinitionAnchor(
        fractions=pd.DataFrame(0.0, index=INDUSTRIES, columns=COMMODITIES),
        use_ratios=pd.DataFrame(1.0, index=COMMODITIES, columns=INDUSTRIES),
        va_ratios=pd.DataFrame(1.0, index=VA_ROWS, columns=INDUSTRIES),
    )
    interior_total = use_before.loc[COMMODITIES, '1111A0'].sum()
    # positive target over the negative VA column: a bare scale would flip
    x_after = pd.Series(
        [interior_total + 10.0 * M, 190.0 * M, 240.0 * M], index=INDUSTRIES
    )
    table = use_after_redef(use_before, anchor, x_after)
    assert table.at['V00100', '1111A0'] == pytest.approx(5.0 * M)  # not flipped
    assert table.loc[COMMODITIES + VA_ROWS, '1111A0'].sum() == pytest.approx(
        x_after['1111A0']
    )


def test_use_after_redef_refuses_unclosable_column() -> None:
    use_before = _full_use()
    use_before.loc[VA_ROWS, '1111A0'] = 0.0  # no VA to absorb the residual
    anchor = RedefinitionAnchor(
        fractions=pd.DataFrame(0.0, index=INDUSTRIES, columns=COMMODITIES),
        use_ratios=pd.DataFrame(1.0, index=COMMODITIES, columns=INDUSTRIES),
        va_ratios=pd.DataFrame(1.0, index=VA_ROWS, columns=INDUSTRIES),
    )
    x_after = pd.Series([200.0 * M, 190.0 * M, 240.0 * M], index=INDUSTRIES)
    with pytest.raises(AssertionError, match='no value added'):
        use_after_redef(use_before, anchor, x_after)
