"""Generation and buyer-specific delivery (#902). Synthetic throughout; no extract."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.eeio import electricity_delivery as ed
from bedrock.transform.eeio import electricity_price as ep


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


def test_a_manufacturer_with_no_price_takes_the_class_price() -> None:
    prices = ed.buyer_prices(
        pd.Series({'339999': 'Industrial'}),
        pd.Series(dtype=float),
        {'Industrial': 8.0}.__getitem__,
    )
    assert prices['339999'] == 8.0


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


# --- the tables with a delivery sector (#1216, #1217) --------------------------------

G, D, HOST = ed.GENERATION_SECTOR, ed.DELIVERY_SLOT, ed.BROKERS_HOST
COMMODITIES = [G, D, HOST, '211000', '531ORE']


def _tables() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    """A small balanced economy. Electric power: output 100, value added 60."""
    v = pd.DataFrame(0.0, index=COMMODITIES, columns=COMMODITIES)
    for code, out in {
        G: 100.0,
        D: 10.0,
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
    udom.loc[D, '531ORE'] = 2.0  # brokers' existing sale
    uimp = pd.DataFrame(0.0, index=COMMODITIES, columns=COMMODITIES)
    va = pd.DataFrame(
        {c: [v.loc[c, c] - udom[c].sum()] for c in COMMODITIES}, index=['V00100']
    )
    y = pd.DataFrame(0.0, index=COMMODITIES, columns=['F01000', 'F05000'])
    y.loc[G, 'F01000'] = 65.0
    y.loc[D, 'F01000'] = 8.0
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


def test_brokers_merge_into_the_host() -> None:
    v, udom, _, va, y = _restructured()
    assert v.at[HOST, HOST] == pytest.approx(50.0)
    assert udom.at[HOST, '531ORE'] == pytest.approx(2.0)
    assert y.at[HOST, 'F01000'] == pytest.approx(8.0)
    assert va.at['V00100', HOST] == pytest.approx(50.0)


def test_each_buyers_bill_divides_between_the_two_rows() -> None:
    _, udom, _, _, y = _restructured()
    assert udom.at[G, '531ORE'] == pytest.approx(10.0)
    assert udom.at[D, '531ORE'] == pytest.approx(20.0)
    assert y.at[G, 'F01000'] == pytest.approx(15.0)
    assert y.at[D, 'F01000'] == pytest.approx(50.0)


def test_imports_stay_on_generation() -> None:
    assert _restructured()[4].at[G, 'F05000'] == pytest.approx(-1.0)


def test_fuel_and_own_purchases_stay_with_generation() -> None:
    _, udom, _, _, _ = _restructured()
    assert udom.at['211000', G] == pytest.approx(20.0)
    assert udom.at['211000', D] == 0.0
    assert udom.at[G, G] == pytest.approx(5.0)


def test_other_inputs_divide_by_the_share() -> None:
    _, udom, _, _, _ = _restructured()
    assert udom.at['531ORE', D] == pytest.approx(9.0)
    assert udom.at['531ORE', G] == pytest.approx(6.0)


def test_delivery_buys_line_losses_from_generation() -> None:
    assert _restructured()[1].at[G, D] == pytest.approx(2.0)


@pytest.mark.parametrize('code', [G, D, HOST])
def test_each_column_adds_up_to_its_output(code: str) -> None:
    v, udom, uimp, va, _ = _restructured()
    total = udom[code].sum() + uimp[code].sum() + va[code].sum()
    assert total == pytest.approx(v.loc[code].sum())


def test_output_rises_only_by_the_losses_sold() -> None:
    v0 = _tables()[0]
    v = _restructured()[0]
    assert v.loc[G].sum() + v.loc[D].sum() == pytest.approx(v0.loc[G].sum() + 2.0)


def test_total_value_added_is_unchanged() -> None:
    va0 = _tables()[3]
    assert _restructured()[3].sum().sum() == pytest.approx(va0.sum().sum())


def test_value_added_divides_by_what_each_column_leaves() -> None:
    _, _, _, va, _ = _restructured()
    assert va.at['V00100', D] == pytest.approx(70.0 - 2.0 - 9.0)
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


def test_sf6_moves_to_delivery_and_other_gases_stay() -> None:
    e = pd.DataFrame(
        {G: [100.0, 3.0], D: [1.0, 0.5], HOST: [2.0, 0.0]}, index=['CO2', 'SF6']
    )
    out = ed.move_delivery_emissions(e)
    assert out.at['CO2', G] == pytest.approx(100.0)
    assert out.at['SF6', G] == 0.0
    assert out.at['SF6', D] == pytest.approx(3.0)
    assert out.at['CO2', HOST] == pytest.approx(3.0)
    assert out.sum().sum() == pytest.approx(e.sum().sum())


def test_long_flows_move_the_same_way_as_the_table() -> None:
    flows = pd.DataFrame(
        {
            'SectorProducedBy': [G, G, D, D, HOST],
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


def test_margins_keep_what_the_purchaser_pays() -> None:
    margins = pd.DataFrame(
        {"Producers' Value": [100.0, 4.0, 6.0], 'Wholesale': [0.0, 1.0, 2.0]},
        index=[G, D, HOST],
    )
    out = ed.restructure_margins(margins, rate=1.5)
    assert out.at[G, "Producers' Value"] == pytest.approx(40.0)
    assert out.at[G, 'Electricity delivery'] == pytest.approx(60.0)
    assert out.at[HOST, "Producers' Value"] == pytest.approx(10.0)
    assert out.at[D, 'Wholesale'] == 0.0


def test_the_margin_rate_leaves_out_own_purchases() -> None:
    assert ed.delivery_margin_rate(_delivery_split()) == pytest.approx(70.0 / 25.0)


def test_labels_change_only_with_the_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ed, 'delivery_margin_on', lambda: False)
    assert ed.published_label(D, 'Brokers') == (D, 'Brokers')
    monkeypatch.setattr(ed, 'delivery_margin_on', lambda: True)
    assert ed.published_label(D, 'Brokers')[0] == ed.DELIVERY_CODE
    assert ed.published_label('531ORE', 'Other real estate') == (
        '531ORE',
        'Other real estate',
    )
