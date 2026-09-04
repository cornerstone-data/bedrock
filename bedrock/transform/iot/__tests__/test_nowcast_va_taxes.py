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

import functools
from collections.abc import Generator

import pandas as pd
import pytest

from bedrock.analysis.nowcasting.tax_axis_conversion import _frames, published_row
from bedrock.transform.iot import nowcast
from bedrock.transform.iot import nowcast_va_taxes as vt
from bedrock.transform.iot.nowcast import USE_VALUE_ADDED_ROWS
from bedrock.transform.iot.nowcast_product_taxes import top_by_level, top_column
from bedrock.transform.iot.nowcast_subsidies import sub_column
from bedrock.transform.trade.duties import mdty_detail_usd
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

MILLION = 1e6

#: Published 2017 Use rows, $M. ``T00SUB`` is BEA's sign, positive.
PUBLISHED_T00TOP_2017 = 755_451
PUBLISHED_T00SUB_2017 = 59_876

#: The measured operator's benchmark score. ``tax_axis_conversion`` gets 0.948
#: from the published Supply table; this module reads ``Detail_Supply_Mix_2017`` so
#: the same code serves every year, which costs 0.001.
T00TOP_BENCHMARK_CORRELATION = 0.947
T00TOP_BENCHMARK_ERROR_SHARE = 0.279


@pytest.fixture(scope='module', autouse=True)
def cache_rows_across_assertions() -> Generator[None, None, None]:
    """Build each expensive annual row once; tests only read returned objects."""
    monkeypatch = pytest.MonkeyPatch()
    t00top_row = vt.t00top_row
    cached_t00top_row = functools.cache(t00top_row)

    def cache_default_block(year: int, block: pd.DataFrame | None = None) -> pd.Series:
        if block is not None:
            return t00top_row(year, block=block)
        return cached_t00top_row(year)

    monkeypatch.setattr(vt, 't00top_row', cache_default_block)
    monkeypatch.setattr(vt, 't00sub_row', functools.cache(vt.t00sub_row))
    yield
    monkeypatch.undo()


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
    """``T00TOP + T00SUB`` is the Supply table's ``T015`` block, by identity.

    ``T00OSUB`` is deliberately outside this sum: subsidies on production are
    industry money with no Supply-bridge counterpart (#784).
    """
    supply_side = float(
        top_column(year).sum()
        + sub_column(year).sum()
        + mdty_detail_usd(year, False).sum()
    )
    rows = vt.va_tax_rows(year)

    assert rows.loc[['T00TOP', 'T00SUB']].to_numpy().sum() == pytest.approx(
        supply_side, abs=MILLION
    )


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
def test_the_insurance_routing_fires_every_year(year: int) -> None:
    """Products-only (#784), ``5241XX``'s subsidy is the federal insurance line
    in every year — the PPP carve-out that used to leave it on the private
    industry in 2020-21 retired with the production subsidies themselves.
    """
    federal = -vt.t00sub_row(year)['S00102']
    private = -vt.t00sub_row(year)['5241XX']

    assert federal > 0
    assert private == pytest.approx(0.0, abs=MILLION)


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_transit_routes_to_the_state_and_local_enterprise(year: int) -> None:
    """The injected ``485000`` subsidy (from 2020) belongs to ``S00201`` — the
    published summary row books it all on the enterprises, none on private
    transit."""
    row = vt.t00sub_row(year)

    assert row['485000'] == pytest.approx(0.0, abs=MILLION)
    if year >= 2020:
        assert -row['S00201'] > 1_000 * MILLION


def test_public_housing_keeps_taking_its_share_every_year() -> None:
    """The housing line is anchored and moved, so ``S00203`` is never empty."""
    for year in vt.VA_TAX_YEARS:
        assert -vt.t00sub_row(year)['S00203'] > 0


# --- the frame --------------------------------------------------------------


@pytest.mark.parametrize('year', list(vt.VA_TAX_YEARS))
def test_both_rows_span_every_industry(year: int) -> None:
    rows = vt.va_tax_rows(year)

    assert list(rows.index) == ['T00TOP', 'T00SUB', 'T00OSUB']
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
    # Products-only (#784), the wedge no longer collapses in 2020: the old
    # sub-third-of-2019 reading was ~580bn of production subsidies (PPP) booked
    # onto commodities. It still dips - the products column itself grows 62% -
    # but must stay well above half of 2019's.
    wedge = table['wedge'].astype(float)
    assert wedge[2019] / 2 < wedge[2020] < wedge[2019]


def test_the_block_stacks_all_six_value_added_rows(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    industry = USA_2017_INDUSTRY_CODES[0]
    amounts = {
        'NIPA_VA_compensation': 1.0,
        'NIPA_VA_othertax': 2.0,
        'NIPA_VA_surplus': 3.0,
    }
    called: list[str] = []

    def generate(method: str, **_: object) -> pd.DataFrame:
        name = method.rsplit('_', 1)[0]
        called.append(name)
        return pd.DataFrame(
            {'SectorConsumedBy': [industry], 'FlowAmount': [amounts[name]]}
        )

    taxes = pd.DataFrame(
        [[4.0], [-5.0], [-6.0]],
        index=['T00TOP', 'T00SUB', 'T00OSUB'],
        columns=[industry],
    )
    monkeypatch.setattr(nowcast.FlowBySector, 'generateFlowBySector', generate)
    monkeypatch.setattr(nowcast, '_resolve_both_sector_columns', lambda frame: frame)
    monkeypatch.setattr(nowcast, 'va_tax_rows', lambda year: taxes)
    nowcast.derive_initial_value_added.cache_clear()
    request.addfinalizer(nowcast.derive_initial_value_added.cache_clear)

    block = nowcast.derive_initial_value_added(vt.ANCHOR_YEAR)

    assert list(block.index) == list(USE_VALUE_ADDED_ROWS)
    assert list(block.columns) == list(USA_2017_INDUSTRY_CODES)
    assert called == list(amounts)
    assert block.loc[:, industry].tolist() == [1.0, 2.0, -6.0, 3.0, 4.0, -5.0]


# --- T00OSUB, subsidies on production (#784) --------------------------------


def test_t00osub_is_zero_before_the_pandemic_and_the_published_total_after() -> None:
    assert vt.t00osub_row(2017).abs().sum() == 0.0
    assert vt.t00osub_row(2019).abs().sum() == 0.0
    total_2020 = -vt.t00osub_row(2020).sum() / MILLION
    assert total_2020 == pytest.approx(538_490, abs=30)


def test_t00osub_reaches_the_industries_the_products_column_no_longer_carries() -> None:
    """The 21*/72*/81 cells Wes traced: same money, industry axis, this row."""
    row = -vt.t00osub_row(2021)

    for industry in ('722110', '811100', '211000'):
        assert row[industry] > 0
    # and the products row for those industries stays empty
    sub = -vt.t00sub_row(2021)
    for industry in ('722110', '811100', '211000'):
        assert sub[industry] == pytest.approx(0.0, abs=MILLION)


def test_t00osub_groups_conserve_the_published_row() -> None:
    """Compensation is a within-group weight only - group totals are BEA's."""
    row = vt.t00osub_row(2020)

    grouped: dict[str, float] = {}
    group_of: dict[str, str] = {
        str(code): str(groups[0])
        for code, groups in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }
    for industry, amount in row.items():
        grouped[group_of[str(industry)]] = grouped.get(
            group_of[str(industry)], 0.0
        ) + float(amount)
    assert grouped['722'] / MILLION == pytest.approx(-36_200, abs=2)
    assert grouped['81'] / MILLION == pytest.approx(-20_837, abs=2)
