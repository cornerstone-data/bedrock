"""Acceptance checks for ``NIPA_VA_compensation_<year>`` and ``NIPA_VA_surplus_<year>``.

Step 2's two large rows (#538). As with ``T00OTOP``, the bar is not "the method
generates" but "the method reproduces the published 2017 row" -- and with 2017 as
both anchor and target that tests the plumbing rather than the movement series
(Phase 5.3). What is worth testing beyond the replay is the structure each method
claims: compensation runs on 69 separate NIPA controls, so it can lose mass
between them; surplus runs on eight controls across five tables and has to keep a
negative industry negative.

For 2018-2024 the claim changes and so does the bar. ``V00300`` is the residual
T18 solves for, so its later-year files are a **seed**: the level has to be that
year's own eight-line assembly, the shares are deliberately frozen at 2017, and
the sign structure has to survive both. The frozen shares are weak here in a way
they are not for ``T00OTOP`` -- 12.51% summary drift against 2.10% -- which is
the reason the tests below assert that nothing moved them rather than that they
are close to anything.

Runs the real methods against the real workbooks, so it is slow and needs the
``BEA_NIPA`` FBA for every year and the 2017 ``BEA_Detail_Use_SUT``.
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd
import pytest

from bedrock.analysis.nowcasting.compensation_allocation import (
    surplus_assembly,
    use_row,
)
from bedrock.transform.flowbysector import getFlowBySector

#: The FBS is in USD; the Use SUT workbook is in millions.
MILLION = 1e6

#: Each method, the Use row it writes, and the NIPA total it must reproduce.
METHODS = {
    'NIPA_VA_compensation_2017': 'V00100',
    'NIPA_VA_surplus_2017': 'V00300',
}


@functools.cache
def _estimate(method: str, row: str) -> 'pd.Series[float]':
    """The method's row by industry, in $M, on the published row's index.

    Aggregated over the ``Table``/``Code``/``Line`` columns that survive from the
    FBA -- every NIPA line produces its own row per industry, the same shape
    ``NIPA_final_dom_uses`` has.
    """
    fbs = getFlowBySector(method, download_FBAs_if_missing=True)
    by_industry = fbs.groupby('SectorConsumedBy')['FlowAmount'].sum() / MILLION
    return by_industry.reindex(use_row(row).index).fillna(0.0)


@pytest.fixture(scope='module')
def compensation() -> 'pd.Series[float]':
    return _estimate('NIPA_VA_compensation_2017', 'V00100')


@pytest.fixture(scope='module')
def surplus() -> 'pd.Series[float]':
    return _estimate('NIPA_VA_surplus_2017', 'V00300')


@pytest.mark.parametrize(('method', 'row'), sorted(METHODS.items()))
def test_row_code_is_on_sector_produced_by(method: str, row: str) -> None:
    """Value added is a Use *row*: the code produced by an industry.

    Failure mode: the transpose is skipped or reversed and the block is written
    as a Use column. Nothing downstream raises -- the money is simply in the
    wrong half of the table.
    """
    fbs = getFlowBySector(method, download_FBAs_if_missing=True)
    assert set(fbs['SectorProducedBy']) == {row}
    assert fbs['SectorConsumedBy'].notna().all()


def test_compensation_reproduces_the_published_row(
    compensation: 'pd.Series[float]',
) -> None:
    """Correlation 1.0000 against the published detail ``V00100``.

    Exact is the floor rather than an achievement, since the benchmark is the
    attribution weight -- but compensation runs on 69 independent controls, so
    anything less means mass was lost or misrouted between them.
    """
    published = use_row('V00100')
    assert np.corrcoef(compensation, published)[0, 1] == pytest.approx(1.0, abs=1e-6)
    error = float((compensation - published).abs().sum() / published.sum())
    assert error < 1e-4, f'{error:.2%} of the row is misplaced'


def test_compensation_holds_each_of_the_69_nipa_controls(
    compensation: 'pd.Series[float]',
) -> None:
    """No industry drifts by more than BEA's own rounding.

    The point of 69 controls rather than one is that the frozen shares only have
    to hold *within* a NIPA group. If a group's control leaked into another
    group, the total could still be right while individual industries moved --
    which a total-only assertion would miss entirely.
    """
    published = use_row('V00100')
    worst = float((compensation - published).abs().max())
    assert worst <= 5, f'largest industry difference is {worst:,.0f}'


def test_surplus_reproduces_the_published_row(surplus: 'pd.Series[float]') -> None:
    """Correlation 1.0000 against the published detail ``V00300``."""
    published = use_row('V00300')
    assert np.corrcoef(surplus, published)[0, 1] == pytest.approx(1.0, abs=1e-6)
    error = float((surplus - published).abs().sum() / published.abs().sum())
    assert error < 1e-4, f'{error:.2%} of the row is misplaced'


def test_surplus_total_matches_the_eight_line_assembly(
    surplus: 'pd.Series[float]',
) -> None:
    """The row sums to the eight NIPA controls, not to the SUT.

    They differ by 13 on 7.87 trillion, and it is the assembly that has to hold:
    a total that has drifted from it means one of the eight lines stopped
    resolving, or resolved to a table root instead of the domestic line (#536).
    """
    assert float(surplus.sum()) == pytest.approx(
        float(surplus_assembly()['value'].sum()), abs=1.0
    )


def test_surplus_keeps_the_negative_industry_negative(
    surplus: 'pd.Series[float]',
) -> None:
    """``S00201`` public transit stays at its published -36,919.

    Gross operating surplus is genuinely negative for state and local passenger
    transit, and a share method that clips at zero would delete it silently
    while leaving every total looking right.
    """
    published = use_row('V00300')
    assert published['S00201'] < 0
    assert float(surplus['S00201']) == pytest.approx(
        float(published['S00201']), abs=1.0
    )


# --------------------------------------------------------------------------
# 2018-2024 -- the seed, not the replay
# --------------------------------------------------------------------------

#: The years ``NIPA_VA_surplus_<year>`` has a file for, less the benchmark.
#: ``NIPA_VA_compensation_<year>`` is graded separately: it moves its shares on
#: QCEW, so the frozen-share assertions below do not apply to it and the
#: movement itself is tested in ``test_compensation_movement.py``.
LATER_YEARS = tuple(range(2018, 2025))


@functools.cache
def _surplus(year: int) -> 'pd.Series[float]':
    return _estimate(f'NIPA_VA_surplus_{year}', 'V00300')


@pytest.mark.parametrize('year', LATER_YEARS)
def test_surplus_later_years_take_their_own_assembly(year: int) -> None:
    """The level is that year's eight lines, not 2017's carried forward.

    ``V00300`` is the residual T18 solves for, so what Step 2 owes it is a seed
    of the right magnitude. That makes the level the part of this method that
    still has to be right -- and it is the part that fails silently, since a
    frozen level is a perfectly plausible-looking value-added row.
    """
    total = float(_surplus(year).sum())
    assert total == pytest.approx(float(surplus_assembly(year)['value'].sum()), abs=1.0)
    assert total != pytest.approx(float(surplus_assembly()['value'].sum()), abs=1.0)


@pytest.mark.parametrize('year', LATER_YEARS)
def test_surplus_later_years_leave_the_2017_shares_alone(year: int) -> None:
    """Shares are frozen, and frozen means exactly rather than nearly.

    ⚠️ The *decision* to freeze them is much weaker here than for ``T00OTOP`` --
    summary composition drift reaches 12.51% by 2022 against ``T00OTOP``'s
    2.10% -- and it is defensible only because T18 makes this row the residual
    the balance overwrites. That is an argument for keeping the seed honest
    about what it is, not for letting something move the weights unasked: if
    they drift here, the measured 12.51% no longer bounds anything.
    """
    shares = _surplus(year) / float(_surplus(year).sum())
    base = _surplus(2017) / float(_surplus(2017).sum())
    assert float((shares - base).abs().sum()) < 1e-9


@pytest.mark.parametrize('year', LATER_YEARS)
def test_surplus_later_years_manufacture_no_new_negatives(year: int) -> None:
    """``S00201`` stays negative and stays alone.

    Both halves matter. A share method that clips at zero deletes the one
    genuinely negative industry; one that lets the assembly drive a new industry
    negative has invented a fact the published row does not support, and #710's
    sign guard is what would have to catch it downstream.
    """
    estimate = _surplus(year)
    assert float(estimate['S00201']) < 0
    assert set(estimate[estimate < 0].index) == {'S00201'}
