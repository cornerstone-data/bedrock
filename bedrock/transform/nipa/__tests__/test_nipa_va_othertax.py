"""Acceptance check for ``NIPA_VA_othertax_2017`` (#538).

Not "the method generates" but "the method reproduces the published 2017
``T00OTOP`` row". With 2017 as both anchor and target the shares are the
identity, so this tests the plumbing rather than the movement series -- which is
exactly what the plan's Phase 5.3 says it is, and the plumbing is the whole of
what the first method in the ``NIPA_VA_*`` family had unproven: the
``BEA_2017_Code`` identity crosswalk rows, ``Sector_Crosswalk_BEA_NIPA_VA.csv``,
the melted ``BEA_Detail_Use_SUT`` as an attribution source, and the Use-row
transpose.

Runs the real method against the real workbooks, so it is slow and needs the
2017 ``BEA_NIPA`` and ``BEA_Detail_Use_SUT`` FBAs.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bedrock.analysis.nowcasting.other_taxes_allocation import (
    control,
    government_industries,
    published_row,
)
from bedrock.transform.flowbysector import FlowBySector

METHOD = 'NIPA_VA_othertax_2017'

#: The FBS is in USD; the Use SUT workbook is in millions.
MILLION = 1e6


@pytest.fixture(scope='module')
def estimate() -> 'pd.Series[float]':
    """``T00OTOP`` by industry from a live run of the method, in $M.

    Aggregated over the ``Table``/``Code``/``Line`` columns that survive from
    the FBA: the two ``T30500`` lines each produce their own row per industry,
    the same shape ``NIPA_final_dom_uses`` has (192 of its 651 sector pairs are
    likewise split). Consumers of either method aggregate; this does too.
    """
    fbs = FlowBySector.generateFlowBySector(METHOD, download_sources_ok=True)
    by_industry = fbs.groupby('SectorConsumedBy')['FlowAmount'].sum() / MILLION
    return by_industry.reindex(published_row().index).fillna(0.0)


def test_row_code_is_on_sector_produced_by() -> None:
    """Value added is a Use *row*: ``T00OTOP`` produced by an industry.

    Failure mode: the transpose is skipped or reversed, and the block is written
    as a Use column. Nothing downstream would raise -- the money would simply be
    in the wrong half of the table.
    """
    fbs = FlowBySector.generateFlowBySector(METHOD, download_sources_ok=True)
    assert set(fbs['SectorProducedBy']) == {'T00OTOP'}
    assert fbs['SectorConsumedBy'].notna().all()


def test_reproduces_the_published_2017_row(estimate: 'pd.Series[float]') -> None:
    """Correlation 1.0000 against the published detail row.

    The benchmark is the attribution weight, so an exact replay is the *floor*
    rather than an achievement -- but anything less means the plumbing loses or
    misplaces mass on the way through.
    """
    published = published_row()
    assert np.corrcoef(estimate, published)[0, 1] == pytest.approx(1.0, abs=1e-6)
    error = float((estimate - published).abs().sum() / published.sum())
    assert error < 1e-4, f'{error:.2%} of the row is misplaced'


def test_total_matches_the_nipa_control(estimate: 'pd.Series[float]') -> None:
    """The row sums to ``T30500`` ``LA000365`` + ``LA000237``, not to the SUT.

    The 9 between them is BEA's own rounding, and it is the control that has to
    hold: ``proportional`` normalises within the group, so a total that has
    drifted from the control means the attribution group broke apart.
    """
    assert float(estimate.sum()) == pytest.approx(control(), abs=1.0)


def test_government_industries_take_nothing(estimate: 'pd.Series[float]') -> None:
    """The ten government codes stay at zero.

    An accounting rule rather than a data gap -- a tax levied by government and
    remitted by a government producer nets out -- and it is enforced by the
    crosswalk, so this fails if the crosswalk is regenerated without the
    exclusion even though the 2017 weights would hide it.
    """
    assert float(estimate[government_industries()].sum()) == 0.0


def test_populates_only_the_industries_the_benchmark_does(
    estimate: 'pd.Series[float]',
) -> None:
    """389 industries, the same 389 the published row populates.

    Catches the crosswalk reaching industries the benchmark gives no weight, and
    the reverse -- an industry silently dropped between the two.
    """
    published = published_row()
    assert set(estimate[estimate > 0].index) == set(published[published > 0].index)
