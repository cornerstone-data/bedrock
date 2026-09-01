"""Tests for margin rates and placement shares (#815).

Unit tests run on a synthetic margins frame via monkeypatch; the answer-key
tests load the published workbooks, because what they pin is a property of
BEA's tables (and of five years of real drift) rather than of our code.
"""

from __future__ import annotations

import operator
from functools import reduce

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.iot import margin_rates as mr
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)

MILLION = MILLION_CURRENCY_TO_CURRENCY

VALS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def synthetic_margins(rows: dict[tuple[str, str], tuple[float, ...]]) -> pd.DataFrame:
    """A margins frame from ``{(buyer, commodity): (PRO, T, W, R, PUR)}``, $M."""
    index = pd.MultiIndex.from_tuples(
        rows.keys(), names=['Industry Code', 'Commodity Code']
    )
    return pd.DataFrame(
        [[v * MILLION for v in values] for values in rows.values()],
        index=index,
        columns=VALS,
    )


@pytest.fixture
def toy_panel(monkeypatch: pytest.MonkeyPatch) -> mr.RatePanel:
    """Two goods rows (one sub-floor), two margin rows, one absent commodity."""
    margins = synthetic_margins(
        {
            # goods, above the floor: rates 0.8 / 0.05 / 0.10 / 0.05
            ('1111A0', '111200'): (80.0, 5.0, 10.0, 5.0, 100.0),
            # goods, same commodity, below the floor with junk decomposition -
            # must take the commodity rate, not 5/1
            ('112120', '111200'): (5.0, 0.0, -4.0, 0.0, 1.0),
            # margin rows: routed mass 5 and 5 -> placement 0.5 / 0.5
            ('1111A0', '441000'): (7.0, 0.0, 0.0, 0.0, 2.0),
            ('1111A0', '484000'): (6.0, 0.0, 0.0, 0.0, 1.0),
        }
    )
    monkeypatch.setattr(
        mr, 'load_benchmark_margins_before_redef_usa', lambda year: margins
    )
    return mr.build_rate_panel(2017)


def test_cell_rates_where_the_anchor_cell_clears_the_floor(
    toy_panel: mr.RatePanel,
) -> None:
    assert toy_panel.rates['pro'].loc['111200', '1111A0'] == pytest.approx(0.8)
    assert toy_panel.rates['whl'].loc['111200', '1111A0'] == pytest.approx(0.1)


def test_subfloor_cells_take_the_commodity_rate(toy_panel: mr.RatePanel) -> None:
    """5/1 on a $1M cell is noise; the commodity's 85/101 is the structure."""
    assert toy_panel.rates['pro'].loc['111200', '112120'] == pytest.approx(85 / 101)


def test_absent_commodities_pass_through(toy_panel: mr.RatePanel) -> None:
    assert toy_panel.rates['pro'].loc['325110', '1111A0'] == pytest.approx(1.0)
    assert toy_panel.rates['ret'].loc['325110', '1111A0'] == pytest.approx(0.0)


def test_rates_sum_to_one_everywhere(toy_panel: mr.RatePanel) -> None:
    total = reduce(operator.add, (toy_panel.rates[c] for c in mr.RATE_COMPONENTS))
    assert np.allclose(total.to_numpy(), 1.0)


def test_placement_shares_split_the_routed_mass(toy_panel: mr.RatePanel) -> None:
    shares = toy_panel.placement['1111A0']
    assert shares.loc['441000'] == pytest.approx(0.5)
    assert shares.loc['484000'] == pytest.approx(0.5)
    assert shares.sum() == pytest.approx(1.0)
    # a buyer with no anchor margins rows takes the economy-wide shares, so
    # margin stripped through fallback rates has somewhere to land
    assert toy_panel.placement.loc['441000', '112120'] == pytest.approx(0.5)
    assert toy_panel.placement['112120'].sum() == pytest.approx(1.0)


def test_conversion_conserves_every_buyer_column(toy_panel: mr.RatePanel) -> None:
    """Margins move money between rows of a column, never out of it."""
    rng = np.random.default_rng(0)
    use = pd.DataFrame(
        rng.uniform(0.0, 100.0, size=(len(mr.goods_commodities()) + 2, 2)),
        index=[*mr.goods_commodities(), '441000', '484000'],
        columns=['1111A0', '112120'],
    )
    goods = mr.producer_goods_interior(use, toy_panel)
    placed = mr.placed_margin_rows(use, toy_panel)
    for buyer in use.columns:
        produced = goods[buyer].sum() + placed[buyer].sum()
        assert produced == pytest.approx(use[buyer].sum())


# --- the answer keys, against the published workbooks -----------------------


@pytest.mark.parametrize('year', [2012, 2017])
def test_the_margin_commodity_set_is_what_the_data_selects(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> None:
    """Both benchmark years select the same 24 codes, so the constant is safe."""
    assert mr.derive_margin_commodities(year) == tuple(sorted(mr.MARGIN_COMMODITIES))


def test_published_rates_sum_to_one_on_goods_rows() -> None:
    panel = mr.build_rate_panel(2017)
    total = reduce(operator.add, (panel.rates[c] for c in mr.RATE_COMPONENTS))
    assert float((total - 1.0).abs().max().max()) < 1e-6


def test_anchor_self_replay_costs_only_the_guardrail() -> None:
    """2017 structure on the 2017 SUT: exact but for the sub-floor fallback."""
    goods_gross, margin_gross = _replay(anchor=2017, year=2017)
    assert goods_gross < 0.1  # % of the goods interior
    assert margin_gross < 0.5  # % of margin-row mass


def test_2012_measures_five_years_of_drift() -> None:
    """The out-of-anchor grade: frozen structure's measured cost, not a target.

    Pinned loosely so a real change shows up without every data refresh
    breaking the build: goods ~2.1%, margin rows ~25%.
    """
    goods_gross, margin_gross = _replay(anchor=2017, year=2012)
    assert goods_gross == pytest.approx(2.06, abs=0.3)
    assert margin_gross == pytest.approx(25.0, abs=3.0)


def _replay(
    anchor: USA_BENCHMARK_DETAIL_SUT_YEARS, year: USA_BENCHMARK_DETAIL_SUT_YEARS
) -> tuple[float, float]:
    from bedrock.extract.iot.io_2017 import (  # noqa: PLC0415
        _load_benchmark_detail_supply_use_usa,
    )
    from bedrock.transform.iot.sut_use_to_mut_use import (  # noqa: PLC0415
        published_mut_use,
    )
    from bedrock.utils.taxonomy.bea.v2017_commodity import (  # noqa: PLC0415
        USA_2017_COMMODITY_CODES,
    )
    from bedrock.utils.taxonomy.bea.v2017_industry import (  # noqa: PLC0415
        USA_2017_INDUSTRY_CODES,
    )

    panel = mr.build_rate_panel(anchor)
    industries = list(USA_2017_INDUSTRY_CODES)
    sut = (
        _load_benchmark_detail_supply_use_usa('Use_SUT_detail', year)
        .loc[list(USA_2017_COMMODITY_CODES), industries]
        .apply(pd.to_numeric, errors='coerce')
        .fillna(0.0)
    )
    mut = (
        published_mut_use(year).loc[list(USA_2017_COMMODITY_CODES), industries]
        / MILLION
    )
    built = mr.producer_goods_interior(sut, panel)[industries]
    reference = mut.loc[mr.goods_commodities()]
    goods_gross = (
        100 * (built - reference).abs().sum().sum() / reference.abs().sum().sum()
    )
    placed = mr.placed_margin_rows(sut, panel)[industries]
    reference_margin = mut.loc[list(mr.MARGIN_COMMODITIES)]
    margin_gross = (
        100
        * (placed - reference_margin).abs().sum().sum()
        / reference_margin.abs().sum().sum()
    )
    return float(goods_gross), float(margin_gross)
