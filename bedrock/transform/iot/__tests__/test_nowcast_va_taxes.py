"""Tests for the Use table's ``T00TOP`` and ``T00SUB`` rows (Step 2, #538).

These two rows are **converted**, not estimated, so what can go wrong here is
different from what can go wrong in the three NIPA rows beside them.

**The sign.** ``T00SUB`` is stored negative here and positive by BEA. A missed
flip fails ``VAPRO`` by twice the subsidy rather than by the subsidy, and in
2020 that is 1.4tn on a 21tn column - large enough to look like a different
table rather than like a sign.

**Mass conservation.** Every leg of both operators is a redistribution of an
observed Supply column. A total that moves is a leg applied twice or dropped,
and the grand total hides it because the legs partition the same money.

**The two exact pieces.** Duties are a lookup onto ``4200ID`` and the ten
government columns are zero by an accounting rule. Both are cheap to break by
"improving" the allocator, and neither shows up in a correlation.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.nowcasting.tax_axis_conversion import _frames, published_row
from bedrock.transform.eeio.nowcast import (
    USE_VALUE_ADDED_ROWS,
    derive_initial_value_added,
)
from bedrock.transform.iot import nowcast_va_taxes as vt
from bedrock.transform.iot.nowcast_product_taxes import top_by_level, top_column
from bedrock.transform.iot.nowcast_subsidies import sub_column
from bedrock.transform.trade.duties import mdty_detail_usd
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

MILLION = 1e6

#: Published 2017 Use rows, $M. ``T00SUB`` is BEA's sign, positive.
PUBLISHED_T00TOP_2017 = 755_451
PUBLISHED_T00SUB_2017 = 59_876

#: The measured operator's benchmark score. ``tax_axis_conversion`` gets 0.948
#: from the published Supply table; this module reads ``Detail_Supply_2017`` so
#: the same code serves every year, which costs 0.001.
T00TOP_BENCHMARK_CORRELATION = 0.947
T00TOP_BENCHMARK_ERROR_SHARE = 0.279


# --- the sign convention ---------------------------------------------------


def test_subsidies_are_returned_negative() -> None:
    """The balance's convention, not BEA's: ``VAPRO`` adds this row."""
    for year in vt.VA_TAX_YEARS:
        row = vt.t00sub_row(year)
        assert (row <= 0).all(), f'{year} has a positive T00SUB cell'


def test_product_taxes_are_returned_positive() -> None:
    row = vt.t00top_row(vt.ANCHOR_YEAR)

    assert row.sum() > 0
    # The row itself may not be non-negative cell by cell - renormalisation can
    # leave a negative market share where a commodity's output is negative -
    # but the trade block and customs must be positive.
    assert row[vt.CUSTOMS_INDUSTRY] > 0
    assert row[vt.PETROLEUM_WHOLESALERS] > 0


# --- mass conservation ------------------------------------------------------


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_t00top_conserves_the_supply_columns(year: int) -> None:
    """``T00TOP`` is ``TOP + MDTY`` redistributed, so its total is theirs."""
    expected = float(top_column(year).sum() + mdty_detail_usd(year, False).sum())

    assert vt.t00top_row(year).sum() == pytest.approx(expected, abs=MILLION)


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_t00sub_conserves_the_supply_column(year: int) -> None:
    """Same money, same sign - ``SUB`` is already negative on both axes here."""
    assert vt.t00sub_row(year).sum() == pytest.approx(
        float(sub_column(year).sum()), abs=MILLION
    )


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_the_wedge_agrees_with_the_supply_bridge(year: int) -> None:
    """``T00TOP + T00SUB`` is the Supply table's ``T015`` block, by identity."""
    supply_side = float(
        top_column(year).sum()
        + sub_column(year).sum()
        + mdty_detail_usd(year, False).sum()
    )
    rows = vt.va_tax_rows(year)

    assert rows.to_numpy().sum() == pytest.approx(supply_side, abs=MILLION)


# --- the two exact pieces ---------------------------------------------------


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_duties_land_whole_on_the_customs_industry(year: int) -> None:
    """A lookup, not an allocation - BEA books all of them to ``4200ID``."""
    duties = float(mdty_detail_usd(year, False).sum())

    assert vt.t00top_row(year)[vt.CUSTOMS_INDUSTRY] == pytest.approx(
        duties, abs=MILLION
    )


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_government_industries_take_no_product_tax(year: int) -> None:
    """Zero by an accounting rule, and a wrong seed here reaches Step 7."""
    on_government = vt.t00top_row(year)[vt.government_industries()]

    assert on_government.abs().sum() == pytest.approx(0.0, abs=MILLION)


def test_the_customs_industry_is_not_treated_as_wholesale() -> None:
    """``4200ID`` starts ``42``; a bare prefix rule would spread duties by output."""
    assert vt.CUSTOMS_INDUSTRY not in vt.trade_industries()
    assert vt.PETROLEUM_WHOLESALERS in vt.trade_industries()


# --- the benchmark, which is the whole argument for the operator ------------


def test_t00top_reproduces_the_published_row_at_the_measured_score() -> None:
    scores = vt.benchmark_scores()['T00TOP']

    assert scores['correlation'] == pytest.approx(
        T00TOP_BENCHMARK_CORRELATION, abs=0.005
    )
    assert scores['error_share'] == pytest.approx(
        T00TOP_BENCHMARK_ERROR_SHARE, abs=0.01
    )


def test_t00sub_reproduces_the_published_row_exactly() -> None:
    """Not a seed: code identity plus two routings closes 2017 to the dollar.

    Checked on **shape**, because the level carries a known 1 $M gap - NIPA's
    59,875 against the workbook's 59,876 - that is BEA's rounding rather than a
    misallocation.
    """
    _, use = _frames()
    published = published_row(use, 'T00SUB')
    estimate = (
        (-vt.t00sub_row(vt.ANCHOR_YEAR) / MILLION).reindex(published.index).fillna(0.0)
    )

    shape_error = float(
        (estimate / estimate.sum() - published / published.sum()).abs().sum()
    )
    assert shape_error < 1e-6
    assert estimate.sum() == pytest.approx(PUBLISHED_T00SUB_2017, abs=1.5)


def test_t00top_total_matches_the_published_row() -> None:
    """The level is observed; only the split is estimated."""
    total = float(vt.t00top_row(vt.ANCHOR_YEAR).sum()) / MILLION

    assert total == pytest.approx(PUBLISHED_T00TOP_2017, abs=20)


# --- the two subsidy routings ----------------------------------------------


def test_the_routings_are_the_whole_code_identity_residual() -> None:
    """Two pairs of cells, and adding a third means identity stopped closing."""
    shares = vt.government_enterprise_shares()

    assert set(shares.index) == set(vt.GOVERNMENT_ENTERPRISE_ROUTINGS)
    assert shares[('housing', '531HST')] == pytest.approx(0.544, abs=0.005)
    assert shares[('other', '5241XX')] == pytest.approx(1.0, abs=0.005)


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_the_insurance_routing_fires_only_outside_the_ppp_years(year: int) -> None:
    """2020-21 ``other`` is private-sector PPP, not a federal enterprise line.

    Routing it would put pandemic support on ``S00102``. The two cells swap:
    outside the PPP years the whole of ``5241XX``'s subsidy goes to the federal
    enterprise and the private industry takes none, and inside them it is the
    other way round.
    """
    federal = -vt.t00sub_row(year)['S00102']
    private = -vt.t00sub_row(year)['5241XX']

    if year in (2020, 2021):
        assert federal == pytest.approx(0.0, abs=MILLION)
        assert private > 0
    else:
        assert federal > 0
        assert private == pytest.approx(0.0, abs=MILLION)


def test_public_housing_keeps_taking_its_share_every_year() -> None:
    """The housing line is anchored and moved, so ``S00203`` is never empty."""
    for year in vt.VA_TAX_YEARS:
        assert -vt.t00sub_row(year)['S00203'] > 0


# --- the frame --------------------------------------------------------------


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_both_rows_span_every_industry(year: int) -> None:
    rows = vt.va_tax_rows(year)

    assert list(rows.index) == ['T00TOP', 'T00SUB']
    assert list(rows.columns) == list(USA_2017_INDUSTRY_CODES)
    assert not rows.isna().to_numpy().any()


def test_years_outside_the_span_are_refused() -> None:
    with pytest.raises(ValueError, match='built for'):
        vt.t00top_row(2016)
    with pytest.raises(ValueError, match='built for'):
        vt.t00sub_row(2025)


def test_market_shares_sum_to_one_where_a_commodity_has_output() -> None:
    """Including after the government columns are dropped and renormalised."""
    for exclude in (False, True):
        shares = vt.market_share_matrix(vt.ANCHOR_YEAR, exclude_government=exclude)
        produced = vt.make_block(vt.ANCHOR_YEAR).sum(axis=1) > 0
        totals = shares[produced].sum(axis=1)

        assert totals.min() == pytest.approx(1.0, abs=1e-9)
        assert totals.max() == pytest.approx(1.0, abs=1e-9)


def test_excluding_government_moves_mass_rather_than_deleting_it() -> None:
    """Renormalisation, not zeroing - the tax stays with its own commodity.

    The government columns go to zero *and* the producer-level leg keeps its
    total. Zeroing without renormalising would pass the first assertion and
    quietly lose 10,513 $M of 2017 tax.
    """
    year = vt.ANCHOR_YEAR
    government = vt.government_industries()
    producer = (
        top_by_level(year)['producer_level']
        .reindex(vt.make_block(year).index)
        .fillna(0.0)
    )

    plain = vt.market_share_matrix(year).mul(producer, axis=0).sum(axis=0)
    kept = (
        vt.market_share_matrix(year, exclude_government=True)
        .mul(producer, axis=0)
        .sum(axis=0)
    )

    assert kept[government].abs().sum() == pytest.approx(0.0, abs=MILLION)
    assert plain[government].abs().sum() > 1_000 * MILLION
    assert kept.sum() == pytest.approx(float(plain.sum()), abs=MILLION)


def test_the_wedge_table_covers_the_whole_span() -> None:
    table = vt.wedge_table()

    assert list(table.index) == list(vt.VA_TAX_YEARS)
    assert (table['T00TOP'] > 0).all()
    assert (table['T00SUB'] < 0).all()
    # 2020-21 pandemic subsidies swamp the tax side: the wedge narrows sharply
    # and must not be allowed to silently go negative or stay flat.
    wedge = table['wedge'].astype(float)
    assert wedge[2020] < wedge[2019] / 3


def test_check_passes() -> None:
    """The module's own ``--check`` is the docstring's assertion set."""
    assert vt.check() == 0


def test_the_block_stacks_all_five_value_added_rows() -> None:
    block = derive_initial_value_added(vt.ANCHOR_YEAR)

    assert list(block.index) == list(USE_VALUE_ADDED_ROWS)
    assert isinstance(block, pd.DataFrame)
    assert block.loc['T00SUB'].sum() < 0
