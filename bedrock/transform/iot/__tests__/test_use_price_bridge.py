"""Tests for the per-cell price bridge.

The placement and the tax strip run on constructed frames - they are arithmetic
over shares, so the cases that decide whether they are right should be
hand-checkable. The identities, the collapse and the family partition run
against the published benchmarks, because what they assert is a property of
BEA's tables.
"""

from __future__ import annotations

import functools
import operator

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.iot.io_2017 import load_benchmark_margins_before_redef_usa
from bedrock.transform.iot.margin_rates import (
    BUYER_CODES,
    MARGIN_COMMODITIES,
    RatePanel,
    build_rate_panel,
    goods_commodities,
)
from bedrock.transform.iot.tax_subsidy_layer import FiscalLayer
from bedrock.transform.iot.use_price_bridge import (
    BASIC,
    BRIDGE_COLUMNS,
    COLLAPSE_CEILING,
    FISCAL_COLUMNS,
    FROZEN_COLLAPSE_CEILING,
    MARGIN_FAMILIES,
    PRODUCERS_VALUE,
    PURCHASERS_VALUE,
    REPLAYS,
    ROW_IDENTITY_TOLERANCE,
    SALES_TAX,
    benchmark_bridge,
    collapse_to_bea,
    column_identities,
    family_placement,
    margin_columns,
    margin_row_recovery,
    price_bridge,
    row_identities,
    score_collapse,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_BENCHMARK_DETAIL_SUT_YEARS

MILLION = MILLION_CURRENCY_TO_CURRENCY

WHOLESALE = MARGIN_FAMILIES['wholesale'][0]
RETAIL = MARGIN_FAMILIES['retail'][0]
TRANSPORT = MARGIN_FAMILIES['transport'][0]


def _panel(rates: dict[str, float], placement: dict[str, float]) -> RatePanel:
    """A panel with one rate per component and one placement share per commodity."""
    goods = goods_commodities()
    buyers = list(BUYER_CODES)
    return RatePanel(
        year=2017,
        rates={
            component: pd.DataFrame(value, index=goods, columns=buyers, dtype=float)
            for component, value in rates.items()
        },
        placement=pd.DataFrame(
            [[placement.get(c, 0.0)] * len(buyers) for c in MARGIN_COMMODITIES],
            index=list(MARGIN_COMMODITIES),
            columns=buyers,
            dtype=float,
        ),
    )


def _purchaser(value: float) -> pd.DataFrame:
    return pd.DataFrame(
        value, index=goods_commodities(), columns=list(BUYER_CODES), dtype=float
    )


# --- the placement ---------------------------------------------------------


def test_each_family_is_placed_only_on_its_own_codes() -> None:
    """⚠️ Transport margin on a wholesale code would be a silent misattribution."""
    panel = _panel(
        {'pro': 0.7, 'trans': 0.1, 'whl': 0.1, 'ret': 0.1},
        {WHOLESALE: 1.0, RETAIL: 1.0, TRANSPORT: 1.0},
    )
    columns = margin_columns(_purchaser(100.0), panel)

    assert float(columns[WHOLESALE].to_numpy().sum()) > 0
    for family, commodities in MARGIN_FAMILIES.items():
        others = [
            c
            for c in MARGIN_COMMODITIES
            if c in commodities and c not in (WHOLESALE, RETAIL, TRANSPORT)
        ]
        for commodity in others:
            assert float(columns[commodity].abs().to_numpy().sum()) == 0.0, family


def test_a_family_share_column_sums_to_one() -> None:
    shares = family_placement(build_rate_panel(2017))

    assert set(shares) == set(MARGIN_FAMILIES)
    for family, frame in shares.items():
        assert list(frame.index) == list(MARGIN_FAMILIES[family])
        assert np.allclose(frame.sum(axis=0).to_numpy(), 1.0)
        assert not frame.isna().to_numpy().any()


def test_the_margin_columns_reproduce_the_cells_own_margin() -> None:
    """Placing splits the amount across codes; it never creates or loses any."""
    panel = _panel(
        {'pro': 0.7, 'trans': 0.1, 'whl': 0.15, 'ret': 0.05},
        {
            WHOLESALE: 0.5,
            MARGIN_FAMILIES['wholesale'][1]: 0.5,
            RETAIL: 1.0,
            TRANSPORT: 1.0,
        },
    )
    columns = margin_columns(_purchaser(100.0), panel)
    placed = functools.reduce(operator.add, columns.values())

    assert float(placed.to_numpy().max()) == pytest.approx(30.0)
    assert float(placed.to_numpy().min()) == pytest.approx(30.0)


def test_the_sales_tax_comes_out_of_the_trade_pair_only() -> None:
    """Transport collects none of it, so its column must not move."""
    panel = _panel(
        {'pro': 0.7, 'trans': 0.1, 'whl': 0.1, 'ret': 0.1},
        {WHOLESALE: 1.0, RETAIL: 1.0, TRANSPORT: 1.0},
    )
    purchaser = _purchaser(100.0)
    fiscal = build_fiscal_stub(purchaser, tax=4.0)

    inclusive = margin_columns(purchaser, panel)
    free = margin_columns(purchaser, panel, fiscal)

    assert float(free[TRANSPORT].to_numpy().max()) == pytest.approx(
        float(inclusive[TRANSPORT].to_numpy().max())
    )
    trade_before = inclusive[WHOLESALE] + inclusive[RETAIL]
    trade_after = free[WHOLESALE] + free[RETAIL]
    assert float((trade_before - trade_after).to_numpy().max()) == pytest.approx(4.0)


def build_fiscal_stub(purchaser: pd.DataFrame, tax: float) -> FiscalLayer:
    """A fiscal layer carrying only a flat sales tax."""
    zero = pd.DataFrame(
        0.0, index=purchaser.index, columns=purchaser.columns, dtype=float
    )
    return FiscalLayer(
        year=2017,
        sales_tax=zero + tax,
        top_rest=zero,
        duties=zero,
        subsidies=zero,
    )


# --- the identities --------------------------------------------------------


@pytest.mark.parametrize(('year', 'anchor'), REPLAYS)
def test_both_row_identities_close_cell_by_cell(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
    anchor: USA_BENCHMARK_DETAIL_SUT_YEARS | None,
) -> None:
    """Exact arithmetic, so anything past float noise is a construction bug."""
    residual = row_identities(benchmark_bridge(year, anchor))

    assert float(residual.abs().to_numpy().max()) < ROW_IDENTITY_TOLERANCE


def test_the_bridge_has_the_contract_columns_in_order() -> None:
    bridge = benchmark_bridge(2017)

    assert list(bridge.columns) == list(BRIDGE_COLUMNS)
    assert bridge.index.names == ['Industry Code', 'Commodity Code']
    assert list(BRIDGE_COLUMNS[:5]) == [BASIC, *FISCAL_COLUMNS, PRODUCERS_VALUE]
    assert list(BRIDGE_COLUMNS[-2:]) == [SALES_TAX, PURCHASERS_VALUE]


def test_the_table_is_usd_not_million_usd() -> None:
    """⚠️ A 10^6 error passes every ratio check, so test magnitude."""
    total = float(benchmark_bridge(2017)[PURCHASERS_VALUE].sum())

    assert 1e13 < total < 1e14


def test_purchasers_value_is_the_input_untouched() -> None:
    scores = score_collapse(benchmark_bridge(2017), 2017)

    assert scores[PURCHASERS_VALUE].gross == pytest.approx(0.0, abs=1.0)


# --- the answer-keyed half -------------------------------------------------


@pytest.mark.parametrize(('year', 'anchor'), REPLAYS)
def test_the_collapse_reproduces_the_published_five_columns(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
    anchor: USA_BENCHMARK_DETAIL_SUT_YEARS | None,
) -> None:
    """BEA's layout is the derived view, and it is answer-keyed at both years."""
    ceilings = COLLAPSE_CEILING if anchor is None else FROZEN_COLLAPSE_CEILING
    scores = score_collapse(benchmark_bridge(year, anchor), year)

    for column, ceiling in ceilings.items():
        assert scores[column].gross / MILLION <= ceiling, column


def test_the_collapse_re_embeds_the_sales_tax() -> None:
    """Dropping the tax on the way out would understate the trade columns."""
    bridge = benchmark_bridge(2017)
    collapsed = collapse_to_bea(bridge)

    trade = collapsed['Wholesale'] + collapsed['Retail']
    tax_free = bridge[[*MARGIN_FAMILIES['wholesale'], *MARGIN_FAMILIES['retail']]].sum(
        axis=1
    )

    assert float((trade - tax_free).sum()) == pytest.approx(
        float(bridge[SALES_TAX].sum()), rel=1e-9
    )


def test_the_margin_rows_come_back_from_their_columns() -> None:
    """The contract's tie to the Use conversion, and the placement's own grade."""
    recovery = margin_row_recovery(2017)

    assert recovery.gross / MILLION < 6_000


def test_freezing_the_anchor_costs_more_than_replaying_a_year_on_itself() -> None:
    """⚠️ A year on its own structure cannot fail; only the freeze can."""
    same_year = margin_row_recovery(2012).gross
    frozen = margin_row_recovery(2012, panel=build_rate_panel(2017)).gross

    assert frozen > 20 * same_year


# --- the allocated half ----------------------------------------------------


@pytest.mark.parametrize('year', [2012, 2017])
def test_the_fiscal_columns_reproduce_the_bridge_exactly(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    """⚠️ Per commodity. These have no per-cell key, so column sums are the grade."""
    gaps = column_identities(benchmark_bridge(year), year)

    for name in ('TOP', 'MDTY', 'SUB'):
        assert float(gaps[name].abs().max()) == pytest.approx(0.0, abs=1.0), name


@pytest.mark.parametrize('year', [2012, 2017])
def test_the_margin_columns_reproduce_the_bridge_per_commodity(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    gaps = column_identities(benchmark_bridge(year), year)

    assert float(gaps['TRADE'].abs().sum()) / MILLION < 5_000
    assert float(gaps['TRANS'].abs().sum()) / MILLION < 1_000


# --- the published partition -----------------------------------------------


@pytest.mark.parametrize('year', [2012, 2017])
def test_the_anchor_partitions_margin_three_ways_by_family(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    """What licenses per-family placement: BEA already keeps the families apart.

    Per buyer, wholesale margin paid equals what the ten wholesale codes earn,
    and likewise retail and transport. Pooling them would be a modelling
    choice; keeping them apart is reading the published table.
    """
    margins = load_benchmark_margins_before_redef_usa(year)
    goods = goods_commodities()
    is_goods = margins.index.get_level_values('Commodity Code').isin(goods)

    def wide(frame: pd.DataFrame, column: str) -> pd.DataFrame:
        return (
            frame[column]
            .unstack('Industry Code')
            .reindex(columns=list(BUYER_CODES))
            .fillna(0.0)
        )

    earned = wide(margins, PRODUCERS_VALUE) - wide(margins, PURCHASERS_VALUE)
    for family, column in (
        ('wholesale', 'Wholesale'),
        ('retail', 'Retail'),
        ('transport', 'Transportation'),
    ):
        paid = wide(margins.loc[is_goods], column).sum(axis=0)
        received = earned.reindex(list(MARGIN_FAMILIES[family])).sum(axis=0)
        gap = received - paid

        assert float(gap.abs().max()) / MILLION < 20, family


def test_the_families_cover_every_margin_commodity_once() -> None:
    covered = [c for family in MARGIN_FAMILIES.values() for c in family]

    assert sorted(covered) == sorted(MARGIN_COMMODITIES)
    assert len(covered) == len(set(covered)) == 24


def test_a_truncated_purchaser_table_is_refused_rather_than_dropping_tax() -> None:
    """⚠️ The fiscal layer carries commodity totals; a subset cannot hold them."""
    narrow = pd.DataFrame(
        1_000.0,
        index=goods_commodities()[:5],
        columns=list(BUYER_CODES)[:3],
        dtype=float,
    )

    with pytest.raises(AssertionError, match='no live cell|no cell for'):
        price_bridge(2017, narrow)


def test_the_fiscal_layer_follows_the_input_cell_pattern() -> None:
    """A buyer absent from the Use table takes no tax, and its share re-spreads.

    ``build_fiscal_layer`` allocates on the *anchor's* cells, so without the
    re-spread a nowcast pattern would book tax where nothing was bought and
    still miss the commodity total.
    """
    margins = load_benchmark_margins_before_redef_usa(2017)
    purchaser = margins[PURCHASERS_VALUE].unstack('Industry Code')
    dropped = BUYER_CODES[0]
    purchaser[dropped] = 0.0

    bridge = price_bridge(2017, purchaser)
    on_dropped = bridge.loc[bridge.index.get_level_values('Industry Code') == dropped]

    assert float(on_dropped[list(FISCAL_COLUMNS)].abs().to_numpy().sum()) == 0.0
    assert float(on_dropped[SALES_TAX].abs().sum()) == 0.0

    gaps = column_identities(bridge, 2017)
    for name in ('TOP', 'MDTY', 'SUB'):
        assert float(gaps[name].abs().max()) == pytest.approx(0.0, abs=1.0), name
