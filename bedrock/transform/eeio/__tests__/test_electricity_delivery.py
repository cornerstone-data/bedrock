"""Generation and buyer-specific delivery (#902). Synthetic throughout; no extract."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.nowcasting import services_transport_expense_seed as st
from bedrock.transform.eeio import electricity_delivery as ed
from bedrock.transform.eeio import electricity_price as ep
from bedrock.transform.iot import nowcast_intermediate as ni
from bedrock.utils.schemas import cornerstone_schemas as cs


def _split(generation: float = 5.0) -> ed.DeliverySplit:
    bills = pd.Series(
        {
            'F01000': 150.0e9,
            '531ORE': 100.0e9,
            '331110': 6.0e9,
            '334413': 10.0e9,
            '111200': 2.0e9,
            '221100': 12.0e9,
            'F04000': 0.5e9,
        }
    )
    classes = pd.Series(
        {
            'F01000': 'Residential',
            '531ORE': 'Commercial',
            '331110': 'Industrial',
            '334413': 'Industrial',
            '111200': 'Industrial',
            '221100': ed.OWN_USE,
            'F04000': ed.EXPORTS,
        }
    )
    prices = pd.Series(
        {
            'F01000': 15.0,
            '531ORE': 12.0,
            '331110': 6.0,
            '334413': 10.0,
            '111200': 8.0,
        }
    )
    return ed.split_bills(
        bills,
        classes,
        prices,
        {'Residential': 1_000e6, 'Commercial': 800e6, 'Industrial': 150e6},
        generation,
        export_mwh=10e6,
        losses_mwh=100e6,
    )


def test_each_buyers_generation_and_delivery_equal_its_bill() -> None:
    frame = _split().buyers
    pd.testing.assert_series_equal(
        frame['gen'] + frame['delivery'], frame['bill'], check_names=False
    )


@pytest.mark.parametrize(
    ('cls', 'mwh'),
    [('Residential', 1_000e6), ('Commercial', 800e6), ('Industrial', 150e6)],
)
def test_class_kwh_equals_eia(cls: str, mwh: float) -> None:
    frame = _split().buyers
    assert frame.loc[frame['class'] == cls, 'kwh'].sum() == pytest.approx(mwh * 1000.0)


def test_kwh_inside_a_class_follow_bill_over_price() -> None:
    kwh = _split().buyers['kwh'].astype(float)
    expected = (6.0e9 / 6.0) / (10.0e9 / 10.0)
    assert kwh['331110'] / kwh['334413'] == pytest.approx(expected)


def test_a_dearer_buyer_has_a_larger_margin_per_kwh() -> None:
    frame = _split().buyers
    margin = frame['delivery'] / frame['kwh']
    assert margin['334413'] > margin['331110']


def test_generation_is_one_price_per_kwh_unless_floored() -> None:
    frame = _split().buyers
    open_rows = frame[~frame['floored'] & (frame['class'] != ed.OWN_USE)]
    assert (open_rows['gen'] / open_rows['kwh']).to_numpy() == pytest.approx(0.05)


def test_a_buyer_below_the_generation_price_is_floored_at_its_bill() -> None:
    frame = _split(generation=30.0).buyers
    assert bool(frame.at['331110', 'floored'])
    assert frame.at['331110', 'gen'] == pytest.approx(6.0e9)
    assert frame.at['331110', 'delivery'] == pytest.approx(0.0)


def test_no_delivery_is_negative() -> None:
    assert (_split(generation=30.0).buyers['delivery'] >= 0).all()


def test_own_use_stays_in_generation() -> None:
    frame = _split().buyers
    assert frame.at['221100', 'gen'] == pytest.approx(12.0e9)
    assert frame.at['221100', 'delivery'] == 0.0
    assert not bool(frame.at['221100', 'floored'])


def test_exports_take_the_eia_trade_kwh() -> None:
    frame = _split().buyers
    assert frame.at['F04000', 'kwh'] == pytest.approx(10e9)
    assert frame.at['F04000', 'gen'] == pytest.approx(0.5e9)
    assert frame.at['F04000', 'delivery'] == pytest.approx(0.0)


def test_exports_are_floored_when_generation_costs_more_than_their_bill() -> None:
    frame = _split(generation=30.0).buyers
    assert bool(frame.at['F04000', 'floored'])
    assert frame.at['F04000', 'gen'] == pytest.approx(0.5e9)


def test_a_buyer_in_a_scaled_class_needs_a_price() -> None:
    with pytest.raises(ValueError, match='no price'):
        ed.split_bills(
            pd.Series({'531ORE': 1.0}),
            pd.Series({'531ORE': 'Commercial'}),
            pd.Series(dtype=float),
            {'Commercial': 1.0},
            5.0,
            0.0,
            0.0,
        )


def test_buyers_with_no_bill_are_left_out() -> None:
    frame = ed.split_bills(
        pd.Series({'A': 1.0e9, 'B': 0.0}),
        pd.Series({'A': 'Commercial', 'B': 'Commercial'}),
        pd.Series({'A': 10.0, 'B': 10.0}),
        {'Commercial': 10e6},
        5.0,
        0.0,
        0.0,
    ).buyers
    assert list(frame.index) == ['A']


def test_only_rail_and_transit_stay_in_the_transportation_class() -> None:
    classes = ed.delivery_classes(
        ['484000', '493000', '482000', '485000'],
        dict.fromkeys(['484000', '493000', '482000', '485000'], 'Transportation'),
    )
    assert classes['484000'] == 'Commercial'
    assert classes['493000'] == 'Commercial'
    assert classes['482000'] == 'Transportation'
    assert classes['485000'] == 'Transportation'


def test_electric_columns_are_own_use() -> None:
    classes = ed.delivery_classes(['221100', 'S00101'], {'221100': 'Commercial'})
    assert set(classes) == {ed.OWN_USE}


def test_manufacturers_take_their_price_and_others_the_class_price() -> None:
    prices = ed.buyer_prices(
        pd.Series(
            {'331110': 'Industrial', '111200': 'Industrial', '531ORE': 'Commercial'}
        ),
        pd.Series({'331110': 6.0}),
        {'Industrial': 8.0, 'Commercial': 12.0}.__getitem__,
    )
    assert prices.to_dict() == {'331110': 6.0, '111200': 8.0, '531ORE': 12.0}


def test_a_buyer_missing_from_the_end_use_map_takes_the_naics_rule() -> None:
    classes = ed.delivery_classes(['331314', '541100'], {})
    assert classes['331314'] == 'Industrial'
    assert classes['541100'] == 'Commercial'


def test_a_manufacturer_with_no_price_raises() -> None:
    with pytest.raises(ValueError, match='no manufacturing price'):
        ed.buyer_prices(
            pd.Series({'339999': 'Industrial'}),
            pd.Series(dtype=float),
            {'Industrial': 8.0}.__getitem__,
        )


def test_losses_are_what_generation_leaves_after_end_use_and_exports() -> None:
    assert ed.line_losses_mwh(100.0, 90.0, 2.0) == pytest.approx(8.0)
    assert ed.line_losses_mwh(100.0, 110.0, 2.0) == 0.0


def test_losses_are_priced_at_the_generation_price() -> None:
    assert _split().losses_usd == pytest.approx(100e6 * 1000.0 * 0.05)


def test_the_generation_price_pools_three_years_of_revenue_and_volume() -> None:
    revenue = pd.Series({2020: 100.0, 2021: 200.0, 2022: 600.0})
    volume = pd.Series({2020: 2e6, 2021: 2e6, 2022: 6e6})
    assert ep.pooled_resale_price(2022, revenue, volume) == pytest.approx(
        ep.resale_price(900.0, 10e6)
    )


def test_a_missing_year_in_the_window_raises() -> None:
    with pytest.raises(ValueError, match='sales for resale'):
        ep.pooled_resale_price(2022, pd.Series({2022: 1.0}), pd.Series({2022: 1.0}))


# --- the tables with a delivery sector ----------------------------------------------

G, E = ed.GENERATION_SECTOR, ed.DELIVERY_CODE
#: Wholesale agents and brokers, and the wholesaler they once merged into.
BROKERS, HOST = '425000', '423A00'
COMMODITIES = [G, BROKERS, HOST, '211000', '531ORE']
TABLES = ('V', 'Udom', 'Uimp', 'VA', 'Y')


def _tables() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    """A small balanced economy. Electric power: output 100, value added 60."""
    v = pd.DataFrame(0.0, index=COMMODITIES, columns=COMMODITIES)
    for code, out in {
        G: 100.0,
        BROKERS: 10.0,
        HOST: 40.0,
        '211000': 30.0,
        '531ORE': 80.0,
    }.items():
        v.loc[code, code] = out
    udom = pd.DataFrame(0.0, index=COMMODITIES, columns=COMMODITIES)
    udom.loc['211000', G] = 20.0  # fuel
    udom.loc['531ORE', G] = 15.0  # non-fuel
    udom.loc[G, G] = 5.0  # own purchases
    udom.loc[G, '531ORE'] = 30.0  # a buyer
    udom.loc[BROKERS, '531ORE'] = 2.0  # brokers' sale
    uimp = pd.DataFrame(0.0, index=COMMODITIES, columns=COMMODITIES)
    va = pd.DataFrame(
        {c: [v.loc[c, c] - udom[c].sum()] for c in COMMODITIES}, index=['V00100']
    )
    y = pd.DataFrame(0.0, index=COMMODITIES, columns=['F01000', 'F05000'])
    y.loc[G, 'F01000'] = 65.0
    y.loc[BROKERS, 'F01000'] = 8.0
    y.loc[G, 'F05000'] = -1.0
    return v, udom, uimp, va, y


def _delivery_split() -> ed.DeliverySplit:
    frame = pd.DataFrame(
        {
            'bill': [30.0, 65.0, 5.0],
            'class': ['Commercial', 'Residential', ed.OWN_USE],
            'gen': [10.0, 15.0, 5.0],
            'delivery': [20.0, 50.0, 0.0],
        },
        index=['531ORE', 'F01000', G],
    )
    return ed.DeliverySplit(frame, 5.0, 0.04)  # 0.04 MWh x 5 c/kWh = $2


def _restructured() -> tuple[pd.DataFrame, ...]:
    return ed.restructure_tables(*_tables(), _delivery_split(), 0.6)


def test_the_tables_gain_the_sector_last() -> None:
    v, udom, uimp, va, y = _restructured()
    for frame in (v, udom, uimp):
        assert frame.index[-1] == E and frame.columns[-1] == E
    assert va.columns[-1] == E and y.index[-1] == E


@pytest.mark.parametrize('code', [BROKERS, HOST])
@pytest.mark.parametrize('position', range(len(TABLES)))
def test_brokers_and_their_old_host_keep_their_data(code: str, position: int) -> None:
    before = _tables()[position]
    after = _restructured()[position]
    if code in before.index:
        pd.testing.assert_series_equal(
            pd.Series(after.loc[code], dtype=float).reindex(before.columns),
            pd.Series(before.loc[code], dtype=float),
        )
    if code in before.columns:
        pd.testing.assert_series_equal(
            pd.Series(after[code], dtype=float).reindex(before.index),
            pd.Series(before[code], dtype=float),
        )


def test_each_buyers_bill_divides_between_the_two_rows() -> None:
    _, udom, _, _, y = _restructured()
    assert udom.at[G, '531ORE'] == pytest.approx(10.0)
    assert udom.at[E, '531ORE'] == pytest.approx(20.0)
    assert y.at[G, 'F01000'] == pytest.approx(15.0)
    assert y.at[E, 'F01000'] == pytest.approx(50.0)


def test_imports_stay_on_generation() -> None:
    assert _restructured()[4].at[G, 'F05000'] == pytest.approx(-1.0)


def test_fuel_and_own_purchases_stay_with_generation() -> None:
    _, udom, _, _, _ = _restructured()
    assert udom.at['211000', G] == pytest.approx(20.0)
    assert udom.at['211000', E] == 0.0
    assert udom.at[G, G] == pytest.approx(5.0)


def test_other_inputs_divide_by_the_share() -> None:
    _, udom, _, _, _ = _restructured()
    assert udom.at['531ORE', E] == pytest.approx(9.0)
    assert udom.at['531ORE', G] == pytest.approx(6.0)


def test_delivery_buys_line_losses_from_generation() -> None:
    assert _restructured()[1].at[G, E] == pytest.approx(2.0)


@pytest.mark.parametrize('code', [G, E, BROKERS, HOST])
def test_each_column_adds_up_to_its_output(code: str) -> None:
    v, udom, uimp, va, _ = _restructured()
    total = udom[code].sum() + uimp[code].sum() + va[code].sum()
    assert total == pytest.approx(v.loc[code].sum())


def test_output_rises_only_by_the_losses_sold() -> None:
    v0 = _tables()[0]
    v = _restructured()[0]
    assert v.loc[G].sum() + v.loc[E].sum() == pytest.approx(v0.loc[G].sum() + 2.0)


def test_total_value_added_is_unchanged() -> None:
    va0 = _tables()[3]
    assert _restructured()[3].sum().sum() == pytest.approx(va0.sum().sum())


def test_value_added_divides_by_what_each_column_leaves() -> None:
    _, _, _, va, _ = _restructured()
    assert va.at['V00100', E] == pytest.approx(70.0 - 2.0 - 9.0)
    assert va.at['V00100', G] == pytest.approx(60.0 - 59.0)


def test_delivery_that_cannot_fit_the_value_added_raises() -> None:
    frame = _delivery_split().buyers.copy()
    frame.loc['F01000', 'delivery'] = 200.0
    with pytest.raises(ValueError, match='does not fit'):
        ed.restructure_tables(*_tables(), ed.DeliverySplit(frame, 5.0, 0.04), 0.6)


def test_a_buyer_in_no_table_raises() -> None:
    frame = _delivery_split().buyers.rename(index={'531ORE': 'NOWHERE'})
    with pytest.raises(ValueError, match='no table'):
        ed.restructure_tables(*_tables(), ed.DeliverySplit(frame, 5.0, 0.04), 0.6)


def test_a_named_index_keeps_its_name() -> None:
    frame = pd.DataFrame({'x': [1.0]}, index=pd.Index([G], name='sector'))
    out = ed.add_delivery_sector(frame, rows=True, columns=False)
    assert out.index.name == 'sector'


def test_sf6_moves_to_delivery_and_other_gases_and_brokers_stay() -> None:
    e = pd.DataFrame(
        {G: [100.0, 3.0], BROKERS: [1.0, 0.5], HOST: [2.0, 0.0]},
        index=['CO2', 'SF6'],
    )
    out = ed.move_delivery_emissions(e)
    assert out.at['CO2', G] == pytest.approx(100.0)
    assert out.at['SF6', G] == 0.0
    assert out.at['SF6', E] == pytest.approx(3.0)
    pd.testing.assert_series_equal(out[BROKERS], e[BROKERS])
    pd.testing.assert_series_equal(out[HOST], e[HOST])
    assert out.sum().sum() == pytest.approx(e.sum().sum())


def test_long_flows_move_the_same_way_as_the_table() -> None:
    flows = pd.DataFrame(
        {
            'SectorProducedBy': [G, G, BROKERS, BROKERS, HOST],
            'Flowable': ['CO2', 'SF6', 'CO2', 'SF6', 'CO2'],
            'FlowAmount': [100.0, 3.0, 1.0, 0.5, 2.0],
        }
    )
    wide = flows.pivot_table(
        index='Flowable', columns='SectorProducedBy', values='FlowAmount', aggfunc='sum'
    ).fillna(0.0)
    expected = ed.move_delivery_emissions(wide)
    moved = ed.move_delivery_flows(flows).pivot_table(
        index='Flowable', columns='SectorProducedBy', values='FlowAmount', aggfunc='sum'
    )
    for code in expected.columns:
        for flow in expected.index:
            got = moved.at[flow, code] if code in moved.columns else 0.0
            assert (0.0 if pd.isna(got) else got) == pytest.approx(
                expected.at[flow, code]
            )


def test_long_flows_move_sf6_under_its_full_name() -> None:
    flows = pd.DataFrame(
        {
            'SectorProducedBy': [G, G, BROKERS],
            'Flowable': ['Sulfur hexafluoride', 'Carbon dioxide', 'Carbon dioxide'],
        }
    )
    moved = ed.move_delivery_flows(flows)
    assert moved['SectorProducedBy'].tolist() == [E, G, BROKERS]


def test_the_share_comes_from_delivery_against_other_production_costs() -> None:
    share = ed.non_fuel_delivery_share(
        {
            'expenses: Transmission': 10.0,
            'expenses: Distribution': 5.0,
            'expenses: Customer Accounts': 3.0,
            'expenses: Customer Service': 1.0,
            'expenses: Sales': 1.0,
            'expenses: Other': 20.0,
        }
    )
    assert share == pytest.approx(0.5)


def test_margins_keep_what_the_purchaser_pays_and_brokers_keep_theirs() -> None:
    margins = pd.DataFrame(
        {"Producers' Value": [100.0, 4.0, 6.0], 'Wholesale': [0.0, 1.0, 2.0]},
        index=[G, BROKERS, HOST],
    )
    out = ed.restructure_margins(margins, rate=1.5)
    assert out.at[G, "Producers' Value"] == pytest.approx(40.0)
    assert out.at[G, 'Electricity delivery'] == pytest.approx(60.0)
    assert out.at[BROKERS, "Producers' Value"] == pytest.approx(4.0)
    assert out.at[HOST, "Producers' Value"] == pytest.approx(6.0)
    assert out.loc[E].sum() == 0.0


def test_the_margin_rate_leaves_out_own_purchases() -> None:
    assert ed.delivery_margin_rate(_delivery_split()) == pytest.approx(70.0 / 25.0)


def test_labels_change_only_with_the_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ed, 'delivery_margin_on', lambda: False)
    assert ed.published_label(G, 'Electricity') == (G, 'Electricity')
    monkeypatch.setattr(ed, 'delivery_margin_on', lambda: True)
    assert ed.published_label(E, '') == (
        E,
        'Electricity transmission and distribution',
    )
    assert ed.published_label(G, 'Electricity')[1] == 'Electricity generation'
    assert ed.published_label(BROKERS, 'Brokers') == (BROKERS, 'Brokers')


def test_the_axis_gains_the_sector_last(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cs, '_electricity_disaggregation_active', lambda: False)
    monkeypatch.setattr(cs, 'electricity_delivery_sector_active', lambda: True)
    for active, base in (
        (cs.active_cornerstone_commodities(), cs.CORNERSTONE_COMMODITIES),
        (cs.active_cornerstone_industries(), cs.CORNERSTONE_INDUSTRIES),
    ):
        assert len(active) == len(base) + 1 == 406
        assert active[:-1] == base and active[-1] == E
    monkeypatch.setattr(cs, 'electricity_delivery_sector_active', lambda: False)
    assert cs.active_cornerstone_commodities() == cs.CORNERSTONE_COMMODITIES


def _electric_value() -> 'pd.Series[float]':
    return pd.Series({'V00100': 60.0, 'V00200': 10.0, 'V00300': 30.0})


def test_value_added_divides_in_one_proportion_without_a_compensation_share() -> None:
    part = ed.divide_value_added(_electric_value(), 60.0, None)
    pd.testing.assert_series_equal(part, _electric_value() * 0.6)


def test_delivery_takes_its_share_of_compensation_and_the_rest_make_up_the_total() -> (
    None
):
    part = ed.divide_value_added(_electric_value(), 60.0, 0.5)
    assert part['V00100'] == pytest.approx(30.0)
    assert part.sum() == pytest.approx(60.0)
    # The other two lines divide the remaining 30 of their 40 in their own proportion.
    assert part['V00200'] == pytest.approx(7.5)
    assert part['V00300'] == pytest.approx(22.5)


def test_a_compensation_share_delivery_cannot_afford_raises() -> None:
    with pytest.raises(ValueError, match='cannot take'):
        ed.divide_value_added(_electric_value(), 20.0, 0.9)
    with pytest.raises(ValueError, match='cannot take'):
        ed.divide_value_added(_electric_value(), 95.0, 0.1)


def test_the_payroll_share_is_delivery_over_electric_power() -> None:
    shares = [ed.delivery_payroll_share(y) for y in range(2017, 2025)]
    assert all(0.5 < s < 0.7 for s in shares)
    with pytest.raises(ValueError, match='no payroll'):
        ed.delivery_payroll_share(2016)


# --- commercial kWh on another basis (#899, #705) ------------------------------------


def _commercial_split(basis: pd.Series | None) -> pd.DataFrame:
    bills = pd.Series({'531ORE': 10.0e9, '445000': 10.0e9, '331110': 6.0e9})
    classes = pd.Series(
        {'531ORE': 'Commercial', '445000': 'Commercial', '331110': 'Industrial'}
    )
    prices = pd.Series({'531ORE': 10.0, '445000': 10.0, '331110': 6.0})
    return ed.split_bills(
        bills,
        classes,
        prices,
        {'Commercial': 200e6, 'Industrial': 100e6},
        5.0,
        export_mwh=0.0,
        losses_mwh=0.0,
        commercial_basis=basis,
    ).buyers


def test_a_commercial_basis_moves_kwh_between_commercial_buyers_only() -> None:
    off = _commercial_split(None)
    on = _commercial_split(pd.Series({'531ORE': 15.0e9, '445000': 5.0e9}))
    kwh = pd.Series(on['kwh'], dtype=float)
    assert kwh['531ORE'] / kwh['445000'] == pytest.approx(3.0)
    commercial = on['class'] == 'Commercial'
    assert kwh[commercial].sum() == pytest.approx(200e6 * 1000.0)
    pd.testing.assert_frame_equal(off.loc[['331110']], on.loc[['331110']])
    pd.testing.assert_series_equal(on['bill'], off['bill'])


def test_a_commercial_buyer_missing_from_the_basis_keeps_its_bill() -> None:
    off = _commercial_split(None)
    on = _commercial_split(pd.Series({'531ORE': 10.0e9}))
    pd.testing.assert_series_equal(on['kwh'], off['kwh'])


@pytest.fixture
def _basis_inputs(monkeypatch: pytest.MonkeyPatch) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = pd.DataFrame(
        {
            'bill_2017_usd': [2.0, 4.0, 1.0],
            'other_inputs_2017_usd': [10.0, 20.0, 5.0],
            'other_inputs_2022_usd': [26.0, 24.0, 6.0],
        },
        index=pd.Index(['531ORE', '445000', '518200'], name='industry'),
    )
    monkeypatch.setattr(ed, 'commercial_electricity_base', lambda: base)
    path = (1.1, 1.2, 1.3, 1.4, 1.5)  # 2018-2022
    monkeypatch.setattr(
        ed, 'survey_electricity_path', lambda code: path if code == '531ORE' else None
    )
    monkeypatch.setattr(ni, 'pinned_electricity_buyers', lambda year: ['518200'])
    use = pd.DataFrame(
        {'531ORE': [1.0, 39.0], '445000': [3.0, 27.0], '518200': [1.0, 9.0]},
        index=[ed.GENERATION_SECTOR, '541100'],
    )
    return use, use * 0.0


def test_a_survey_year_moves_the_2017_bill_on_the_survey(
    _basis_inputs: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    basis = ed.commercial_kwh_basis(*_basis_inputs, 2020)
    assert basis['531ORE'] == pytest.approx(2.0 * 1.3)
    assert basis['445000'] == pytest.approx(4.0 * 27.0 / 20.0)  # other inputs


def test_after_the_survey_the_2022_value_moves_on_other_inputs(
    _basis_inputs: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    basis = ed.commercial_kwh_basis(*_basis_inputs, 2024)
    assert basis['531ORE'] == pytest.approx(2.0 * 1.5 * 39.0 / 26.0)
    assert basis['445000'] == pytest.approx(4.0 * 27.0 / 20.0)
    assert '518200' not in basis.index  # pinned on its own series


def test_a_survey_withheld_in_any_year_gives_no_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = {2018: 1.0, 2019: 1.1, 2020: None, 2021: 1.2, 2022: 1.3}
    monkeypatch.setattr(
        st, 'electricity_spending_index', lambda code, year: values[year]
    )
    ed.survey_electricity_path.cache_clear()
    assert ed.survey_electricity_path('532400') is None
    ed.survey_electricity_path.cache_clear()
