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
