"""Acceptance check for ``NIPA_VA_othertax_<year>`` (#538).

Not "the method generates" but "the method reproduces the published 2017
``T00OTOP`` row". With 2017 as both anchor and target the shares are the
identity, so this tests the plumbing rather than the movement series -- which is
exactly what the plan's Phase 5.3 says it is, and the plumbing is the whole of
what the first method in the ``NIPA_VA_*`` family had unproven: the
``BEA_2017_Code`` identity crosswalk rows, ``Sector_Crosswalk_BEA_NIPA_VA.csv``,
the melted ``BEA_Detail_Use_SUT`` as an attribution source, and the Use-row
transpose.

The later years then check the *other* half of the contract, which is a
different claim and fails in a different way. 2018-2024 must move three things
and hold one: the level onto their own ``T30500`` control, the housing block
onto ``T70405``, the farm block onto ``T70305``, and the within-block 2017
shares exactly alone. Every one of those is a silent failure -- a frozen level
looks like a plausible row, a missed lookup still sums to the right total, and a
drifted share vector does too -- so none is observable downstream.

Runs the real method against the real workbooks, so it is slow and needs the
``BEA_NIPA`` FBA for every year and the 2017 ``BEA_Detail_Use_SUT``.
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd
import pytest

from bedrock.analysis.nowcasting.other_taxes_allocation import (
    control,
    government_industries,
    published_row,
)
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.transform.nipa.othertax_lookups import (
    FARM_LINE,
    HOUSING_CODES,
    HOUSING_LINE,
    farm_industries,
    nipa_series,
)

BENCHMARK_YEAR = 2017
METHOD = f'NIPA_VA_othertax_{BENCHMARK_YEAR}'

#: The years with a file, less the benchmark: the ones that have to move.
LATER_YEARS = tuple(range(2018, 2025))

#: The FBS is in USD; the Use SUT workbook is in millions.
MILLION = 1e6


@functools.cache
def _estimate(year: int) -> 'pd.Series[float]':
    """``T00OTOP`` by industry from a live run of the method, in $M.

    Aggregated over the ``Table``/``Code``/``Line`` columns that survive from
    the FBA: the two ``T30500`` lines each produce their own row per industry,
    the same shape ``NIPA_final_dom_uses`` has (192 of its 651 sector pairs are
    likewise split). Consumers of either method aggregate; this does too.
    """
    fbs = getFlowBySector(
        f'NIPA_VA_othertax_{year}', download_FBAs_if_missing=True
    )
    by_industry = fbs.groupby('SectorConsumedBy')['FlowAmount'].sum() / MILLION
    return by_industry.reindex(published_row().index).fillna(0.0)


@pytest.fixture(scope='module')
def estimate() -> 'pd.Series[float]':
    """The benchmark year, which the original checks below are all about."""
    return _estimate(BENCHMARK_YEAR)


def test_row_code_is_on_sector_produced_by() -> None:
    """Value added is a Use *row*: ``T00OTOP`` produced by an industry.

    Failure mode: the transpose is skipped or reversed, and the block is written
    as a Use column. Nothing downstream would raise -- the money would simply be
    in the wrong half of the table.
    """
    fbs = getFlowBySector(METHOD, download_FBAs_if_missing=True)
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


@pytest.mark.parametrize('year', LATER_YEARS)
def test_later_years_take_their_own_control(year: int) -> None:
    """The level is that year's ``T30500``, not 2017's carried forward.

    The whole of what makes ``NIPA_VA_othertax_<year>`` a later-year file: the
    control is read for ``year`` while the benchmark attribution source stays
    pinned at 2017. Failure mode is silent - if the top-level ``year`` stopped
    reaching ``source_names.BEA_NIPA``, every year would quietly return the
    608,533 of 2017 and still look like a value-added row.
    """
    total = float(_estimate(year).sum())
    assert total == pytest.approx(control(year), abs=1.0)
    assert total != pytest.approx(control(BENCHMARK_YEAR), abs=1.0)


@pytest.mark.parametrize('year', LATER_YEARS)
def test_later_years_look_the_housing_block_up(year: int) -> None:
    """``531HSO + 531HST`` equals that year's ``T70405`` ``B1031C``, exactly.

    41.8% of the row, and the largest single thing this method knows rather than
    assumes. It arrives through a ``clean_fba`` socket on the attribution source
    rather than as its own activity set, because NIPA states no "other taxes on
    production excluding housing" line to control a second set with -- so the
    only place this can fail is silently, in the weights.
    """
    housing = float(_estimate(year)[list(HOUSING_CODES)].sum())
    assert housing == pytest.approx(nipa_series(*HOUSING_LINE, year), abs=1.0)


@pytest.mark.parametrize('year', LATER_YEARS)
def test_later_years_look_the_farm_block_up(year: int) -> None:
    """The ten 111/112 codes equal that year's ``T70305`` ``B1017C``.

    Small -- 1.5% of the row -- and kept for the same reason the housing lookup
    is: it is published, it is exact, and the alternative is a frozen share.
    """
    farm = float(_estimate(year)[farm_industries()].sum())
    assert farm == pytest.approx(nipa_series(*FARM_LINE, year), abs=1.0)


@pytest.mark.parametrize('year', LATER_YEARS)
def test_later_years_freeze_the_shares_inside_each_block(year: int) -> None:
    """What is *not* looked up still moves only with its block.

    The three blocks are rescaled independently, so within each one the 2017
    shape has to survive to floating point. ``other_taxes_allocation.py`` grades
    the *decision* to freeze it -- summary composition drift is 1.01-2.10%
    against a 5% bar; this grades the implementation, and any drift here means
    something moved the weights that was not asked to.
    """
    estimate = _estimate(year)
    base = _estimate(BENCHMARK_YEAR)
    blocks = {
        'housing': list(HOUSING_CODES),
        'farm': farm_industries(),
        'rest': [
            code
            for code in base.index
            if code not in set(HOUSING_CODES) | set(farm_industries())
        ],
    }
    for name, codes in blocks.items():
        shares = estimate[codes] / float(estimate[codes].sum())
        frozen = base[codes] / float(base[codes].sum())
        drift = float((shares - frozen).abs().sum())
        assert drift < 1e-9, f'{name} block drifted by {drift:.3e}'


@pytest.mark.parametrize('year', LATER_YEARS)
def test_later_years_keep_government_at_zero(year: int) -> None:
    """The accounting rule is not a 2017 fact, so it has to hold every year.

    Cheap to check and the crosswalk is shared, but the benchmark weights hide a
    regression here in 2017 - the zero is in the weights - so the later years
    are where a crosswalk regenerated without the exclusion would first show.
    """
    assert float(_estimate(year)[government_industries()].sum()) == 0.0
