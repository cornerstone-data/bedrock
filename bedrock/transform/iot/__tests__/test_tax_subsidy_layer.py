"""Tests for the per-cell tax and subsidy layer (#815 part 2).

Unit tests run on synthetic margins + bridge frames via monkeypatch; the
answer-key tests pin the measured partition facts of the published tables.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.iot import tax_subsidy_layer as tsl
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

MILLION = MILLION_CURRENCY_TO_CURRENCY

VALS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def synthetic_margins(rows: dict[tuple[str, str], tuple[float, ...]]) -> pd.DataFrame:
    index = pd.MultiIndex.from_tuples(
        rows.keys(), names=['Industry Code', 'Commodity Code']
    )
    return pd.DataFrame(
        [[v * MILLION for v in values] for values in rows.values()],
        index=index,
        columns=VALS,
    )


@pytest.fixture
def toy_layer(monkeypatch: pytest.MonkeyPatch) -> tsl.FiscalLayer:
    """One retail-taxed commodity, one wholesale-fallback commodity."""
    margins = synthetic_margins(
        {
            # 111200: retail base 10 + 30, wholesale 20 on exports
            ('1111A0', '111200'): (100.0, 0.0, 0.0, 10.0, 110.0),
            ('F01000', '111200'): (300.0, 0.0, 0.0, 30.0, 330.0),
            ('F04000', '111200'): (100.0, 0.0, 20.0, 0.0, 120.0),
            # 112120: no retail anywhere -> wholesale fallback
            ('1111A0', '112120'): (50.0, 0.0, 30.0, 0.0, 80.0),
            ('F04000', '112120'): (50.0, 0.0, 10.0, 0.0, 60.0),
        }
    )
    # W+R sums: 111200 -> 60, 112120 -> 40
    supply = pd.DataFrame(
        {
            'TRADE': [40.0, 30.0],
            'TRANS': [0.0, 0.0],
            'TOP': [30.0, 15.0],
            'MDTY': [8.0, 0.0],
            'SUB': [-4.0, 0.0],
        },
        index=['111200', '112120'],
    )
    monkeypatch.setattr(
        tsl, 'load_benchmark_margins_before_redef_usa', lambda year: margins
    )
    monkeypatch.setattr(
        tsl, '_load_benchmark_detail_supply_use_usa', lambda name, year: supply
    )
    return tsl.build_fiscal_layer(2017)


def test_sales_tax_follows_the_retail_base(toy_layer: tsl.FiscalLayer) -> None:
    """Wedge 60-40=20 splits 10:30 across the retail buyers."""
    assert toy_layer.sales_tax.loc['111200', '1111A0'] == pytest.approx(5 * MILLION)
    assert toy_layer.sales_tax.loc['111200', 'F01000'] == pytest.approx(15 * MILLION)


def test_exports_carry_no_margin_collected_tax(toy_layer: tsl.FiscalLayer) -> None:
    """F04000 gets zero even where the wholesale fallback is the base."""
    assert float(toy_layer.sales_tax['F04000'].abs().sum()) == 0.0
    # 112120's wedge 40-30=10 lands entirely on the non-export wholesale buyer
    assert toy_layer.sales_tax.loc['112120', '1111A0'] == pytest.approx(10 * MILLION)


def test_producer_level_wedges_follow_producers_value(
    toy_layer: tsl.FiscalLayer,
) -> None:
    """111200: top_rest 30-20=10, duties 8, subsidies -4, all split 1:3:1."""
    assert toy_layer.top_rest.loc['111200', '1111A0'] == pytest.approx(2 * MILLION)
    assert toy_layer.top_rest.loc['111200', 'F01000'] == pytest.approx(6 * MILLION)
    assert toy_layer.duties.loc['111200', 'F04000'] == pytest.approx(1.6 * MILLION)
    assert toy_layer.subsidies.loc['111200', '1111A0'] == pytest.approx(-0.8 * MILLION)


def test_column_sums_reproduce_the_bridge(toy_layer: tsl.FiscalLayer) -> None:
    top = toy_layer.sales_tax.sum(axis=1) + toy_layer.top_rest.sum(axis=1)
    assert top.loc['111200'] == pytest.approx(30 * MILLION)
    assert top.loc['112120'] == pytest.approx(15 * MILLION)
    assert toy_layer.duties.sum(axis=1).loc['111200'] == pytest.approx(8 * MILLION)
    assert toy_layer.subsidies.sum(axis=1).loc['111200'] == pytest.approx(-4 * MILLION)


# --- the re-spread onto a target table's live cells --------------------------


def _cells(live: list[tuple[str, str]]) -> pd.DataFrame:
    """A target table, commodity x buyer, nonzero on *live* only."""
    commodities = sorted({c for c, _ in live})
    buyers = sorted({b for _, b in live})
    frame = pd.DataFrame(0.0, index=commodities, columns=buyers)
    for commodity, buyer in live:
        frame.loc[commodity, buyer] = 1.0
    return frame


def test_tax_on_a_dead_cell_moves_onto_the_live_cells(
    toy_layer: tsl.FiscalLayer,
) -> None:
    """111200 loses F01000: its 15 of sales tax moves onto 1111A0, the only live cell."""
    cells = _cells([('111200', '1111A0'), ('112120', '1111A0')])
    layer = tsl.restrict_to_cells(toy_layer, cells)

    assert layer.sales_tax.loc['111200', '1111A0'] == pytest.approx(20 * MILLION)
    # The result is on the goods x buyer grid, zero off the live cells.
    assert list(layer.sales_tax.columns) == list(tsl.BUYER_CODES)
    assert float(layer.sales_tax['F01000'].abs().sum()) == 0.0
    assert layer.top_rest.loc['111200', '1111A0'] == pytest.approx(10 * MILLION)


def test_the_re_spread_keeps_every_commodity_total(
    toy_layer: tsl.FiscalLayer,
) -> None:
    cells = _cells([('111200', '1111A0'), ('111200', 'F04000'), ('112120', '1111A0')])
    layer = tsl.restrict_to_cells(toy_layer, cells)

    for name in tsl.FISCAL_FRAMES:
        before = getattr(toy_layer, name).sum(axis=1)
        after = getattr(layer, name).sum(axis=1)
        assert float((after - before).abs().max()) < tsl.CONSERVATION_TOLERANCE, name


def test_build_with_cells_is_build_then_restrict(toy_layer: tsl.FiscalLayer) -> None:
    cells = _cells([('111200', '1111A0'), ('112120', '1111A0')])
    built = tsl.build_fiscal_layer(2017, cells=cells)
    restricted = tsl.restrict_to_cells(toy_layer, cells)

    for name in tsl.FISCAL_FRAMES:
        pd.testing.assert_frame_equal(getattr(built, name), getattr(restricted, name))


def test_tax_with_no_live_cell_in_its_commodity_is_refused(
    toy_layer: tsl.FiscalLayer,
) -> None:
    """112120 has no live cell, so its 15 of TOP cannot be re-spread."""
    cells = _cells([('111200', '1111A0')])

    with pytest.raises(ValueError, match='would vanish'):
        tsl.restrict_to_cells(toy_layer, cells)


def _layer(**frames: pd.DataFrame) -> tsl.FiscalLayer:
    """A layer with *frames* set and every other frame zero on the same grid."""
    grid = next(iter(frames.values()))
    zero = pd.DataFrame(0.0, index=grid.index, columns=grid.columns)
    return tsl.FiscalLayer(
        year=2017, **{name: frames.get(name, zero) for name in tsl.FISCAL_FRAMES}
    )


def test_the_allocation_puts_no_tax_on_negative_purchases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inventory draw-down (F03000 -60) takes no share, so nothing offsets.

    A signed base nets to 100 - 60 = 40 and would put +20 and -12 of an 8 duty
    on the two cells (#1240); the positive base puts all 8 on 1111A0.
    """
    margins = synthetic_margins(
        {
            ('1111A0', '111200'): (100.0, 0.0, 0.0, 0.0, 100.0),
            ('F03000', '111200'): (-60.0, 0.0, 0.0, 0.0, -60.0),
        }
    )
    supply = pd.DataFrame(
        {'TRADE': [0.0], 'TRANS': [0.0], 'TOP': [0.0], 'MDTY': [8.0], 'SUB': [-4.0]},
        index=['111200'],
    )
    monkeypatch.setattr(
        tsl, 'load_benchmark_margins_before_redef_usa', lambda year: margins
    )
    monkeypatch.setattr(
        tsl, '_load_benchmark_detail_supply_use_usa', lambda name, year: supply
    )
    layer = tsl.build_fiscal_layer(2017)

    assert layer.duties.loc['111200', '1111A0'] == pytest.approx(8 * MILLION)
    assert layer.duties.loc['111200', 'F03000'] == 0.0
    assert layer.subsidies.loc['111200', '1111A0'] == pytest.approx(-4 * MILLION)


def test_the_re_spread_keeps_every_cell_on_its_commodity_sign() -> None:
    """Subsidies of -1 and -0.9 live, -8.1 dead: both live cells grow, neither flips."""
    subsidies = pd.DataFrame(
        {'1111A0': [-1.0], 'F01000': [-0.9], 'F02E00': [-8.1]}, index=['111200']
    )
    cells = pd.DataFrame(
        {'1111A0': [1.0], 'F01000': [1.0], 'F02E00': [0.0]}, index=['111200']
    )
    layer = tsl.restrict_to_cells(_layer(subsidies=subsidies * MILLION), cells)

    assert layer.subsidies.loc['111200', '1111A0'] == pytest.approx(
        -1.0 * 10 / 1.9 * MILLION
    )
    assert layer.subsidies.loc['111200', 'F01000'] == pytest.approx(
        -0.9 * 10 / 1.9 * MILLION
    )
    assert float(layer.subsidies.to_numpy().max()) <= 0.0


def test_a_layer_that_mixes_signs_is_refused() -> None:
    """A hand-built layer with +1 and -0.9 live and +9.9 dead would flip F01000."""
    subsidies = pd.DataFrame(
        {'1111A0': [1.0], 'F01000': [-0.9], 'F02E00': [9.9]}, index=['111200']
    )
    cells = pd.DataFrame(
        {'1111A0': [1.0], 'F01000': [1.0], 'F02E00': [0.0]}, index=['111200']
    )
    with pytest.raises(ValueError, match='flips sign'):
        tsl.restrict_to_cells(_layer(subsidies=subsidies * MILLION), cells)


def test_negative_purchases_are_not_live() -> None:
    """A target cell of -50 (an inventory draw-down) takes no tax."""
    top_rest = pd.DataFrame({'1111A0': [2.0], 'F03000': [0.0]}, index=['111200'])
    cells = pd.DataFrame({'1111A0': [0.0], 'F03000': [-50.0]}, index=['111200'])

    with pytest.raises(ValueError, match='no cell the target buys'):
        tsl.restrict_to_cells(_layer(top_rest=top_rest * MILLION), cells)


def test_live_cells_without_anchor_tax_take_it_by_the_target_purchases() -> None:
    """All of the anchor tax sits on a dead cell; the live cells split it 1:3."""
    top_rest = pd.DataFrame(
        {'1111A0': [0.0], 'F01000': [0.0], 'F02E00': [8.0]}, index=['111200']
    )
    cells = pd.DataFrame(
        {'1111A0': [10.0], 'F01000': [30.0], 'F02E00': [0.0]}, index=['111200']
    )
    layer = tsl.restrict_to_cells(_layer(top_rest=top_rest * MILLION), cells)

    assert layer.top_rest.loc['111200', '1111A0'] == pytest.approx(2 * MILLION)
    assert layer.top_rest.loc['111200', 'F01000'] == pytest.approx(6 * MILLION)


def test_sales_tax_does_not_fall_back_onto_exports() -> None:
    """A commodity bought only by exports cannot hold margin-collected tax."""
    sales_tax = pd.DataFrame({'F01000': [5.0], 'F04000': [0.0]}, index=['111200'])
    cells = pd.DataFrame({'F01000': [0.0], 'F04000': [50.0]}, index=['111200'])

    with pytest.raises(ValueError, match='no live cell other than exports'):
        tsl.restrict_to_cells(_layer(sales_tax=sales_tax * MILLION), cells)


def test_sales_tax_falls_back_onto_the_trade_margin_only() -> None:
    """F01000 and F02000 are live, but only F01000 pays a trade margin."""
    sales_tax = pd.DataFrame(
        {'F01000': [0.0], 'F02E00': [0.0], 'F03000': [6.0]}, index=['111200']
    )
    cells = pd.DataFrame(
        {'F01000': [40.0], 'F02E00': [40.0], 'F03000': [0.0]}, index=['111200']
    )
    trade = pd.DataFrame(
        {'F01000': [10.0], 'F02E00': [0.0], 'F03000': [0.0]}, index=['111200']
    )
    layer = tsl.restrict_to_cells(
        _layer(sales_tax=sales_tax * MILLION), cells, trade * MILLION
    )

    assert layer.sales_tax.loc['111200', 'F01000'] == pytest.approx(6 * MILLION)
    assert layer.sales_tax.loc['111200', 'F02E00'] == 0.0


def test_sales_tax_with_no_trade_margin_is_refused() -> None:
    sales_tax = pd.DataFrame({'F01000': [5.0], 'F02E00': [0.0]}, index=['111200'])
    cells = pd.DataFrame({'F01000': [0.0], 'F02E00': [40.0]}, index=['111200'])
    trade = pd.DataFrame({'F01000': [0.0], 'F02E00': [0.0]}, index=['111200'])

    with pytest.raises(ValueError, match='no cell with a trade margin'):
        tsl.restrict_to_cells(
            _layer(sales_tax=sales_tax * MILLION), cells, trade * MILLION
        )


def test_cells_off_the_buyer_grid_are_refused() -> None:
    """A total column such as T001 must not become a live cell."""
    top_rest = pd.DataFrame({'F01000': [2.0]}, index=['111200'])
    cells = pd.DataFrame({'F01000': [10.0], 'T001': [10.0]}, index=['111200'])

    with pytest.raises(ValueError, match='off the goods x buyer grid'):
        tsl.restrict_to_cells(_layer(top_rest=top_rest * MILLION), cells)


def test_sub_dollar_losses_cannot_pool_past_the_tolerance() -> None:
    """200 commodities losing $0.001 each pass; losing $0.99 each does not."""
    commodities = tsl.goods_commodities()[:200]
    cells = pd.DataFrame({'F01000': 0.0}, index=commodities)

    small = pd.DataFrame({'F01000': 0.001}, index=commodities)
    layer = tsl.restrict_to_cells(_layer(top_rest=small), cells)
    assert float(layer.top_rest.abs().sum().sum()) == 0.0

    large = pd.DataFrame({'F01000': 0.99}, index=commodities)
    with pytest.raises(ValueError, match='would vanish'):
        tsl.restrict_to_cells(_layer(top_rest=large), cells)


# --- the answer keys, against the published tables ---------------------------


@pytest.mark.parametrize(('year', 'wedge_total'), [(2012, 322_515), (2017, 391_097)])
def test_the_sales_tax_wedge_is_what_the_two_tables_pin(
    year: int, wedge_total: int
) -> None:
    """sum(W+R) - TRADE on goods rows, and it stays inside [0, TOP]."""
    wedge = tsl.sales_tax_wedge(year)  # type: ignore[arg-type]
    assert wedge.sum() / MILLION == pytest.approx(wedge_total, rel=1e-3)
    assert float(wedge.min()) >= 0.0
    share = tsl.top_sales_share(year)  # type: ignore[arg-type]
    assert float(share.max()) <= 1.0 + 1e-9


@pytest.mark.parametrize('year', [2012, 2017])
def test_published_layer_reproduces_the_bridge_and_spares_exports(
    year: int,
) -> None:
    layer = tsl.build_fiscal_layer(year)  # type: ignore[arg-type]
    bridge = tsl._bridge(year)  # type: ignore[arg-type]
    top = layer.sales_tax.sum(axis=1) + layer.top_rest.sum(axis=1)
    assert float((top - bridge['TOP']).abs().max()) < 1.0
    assert float((layer.duties.sum(axis=1) - bridge['MDTY']).abs().max()) < 1.0
    assert float((layer.subsidies.sum(axis=1) - bridge['SUB']).abs().max()) < 1.0
    assert float(layer.sales_tax['F04000'].abs().sum()) == 0.0
    # The base is positive cells only (#1240), so every cell carries its
    # commodity's sign: subsidies are non-positive cell by cell.
    assert float(layer.subsidies.to_numpy().max()) <= 1e-9
    assert float(layer.duties.to_numpy().min()) >= -1e-9
