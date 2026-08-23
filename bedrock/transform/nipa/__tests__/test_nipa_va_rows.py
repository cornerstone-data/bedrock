"""Acceptance checks for ``NIPA_VA_compensation_2017`` and ``NIPA_VA_surplus_2017``.

Step 2's two large rows (#538). As with ``T00OTOP``, the bar is not "the method
generates" but "the method reproduces the published 2017 row" -- and with 2017 as
both anchor and target that tests the plumbing rather than the movement series
(Phase 5.3). What is worth testing beyond the replay is the structure each method
claims: compensation runs on 69 separate NIPA controls, so it can lose mass
between them; surplus runs on eight controls across five tables and has to keep a
negative industry negative.

Runs the real methods against the real workbooks, so it is slow and needs the
2017 ``BEA_NIPA`` and ``BEA_Detail_Use_SUT`` FBAs.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.analysis.nowcasting.compensation_allocation import (
    surplus_assembly,
    use_row,
)
from bedrock.transform.flowbysector import FlowBySector

#: The FBS is in USD; the Use SUT workbook is in millions.
MILLION = 1e6

#: Each method, the Use row it writes, and the NIPA total it must reproduce.
METHODS = {
    'NIPA_VA_compensation_2017': 'V00100',
    'NIPA_VA_surplus_2017': 'V00300',
}


def _estimate(method: str, row: str) -> 'pd.Series[float]':
    """The method's row by industry, in $M, on the published row's index.

    Aggregated over the ``Table``/``Code``/``Line`` columns that survive from the
    FBA -- every NIPA line produces its own row per industry, the same shape
    ``NIPA_final_dom_uses`` has.
    """
    fbs = FlowBySector.generateFlowBySector(method, download_sources_ok=True)
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
    fbs = FlowBySector.generateFlowBySector(method, download_sources_ok=True)
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
