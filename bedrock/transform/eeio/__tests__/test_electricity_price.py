"""Annual electricity price by buyer (#901). Synthetic throughout; no extract."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.eeio import electricity_price as ep

INDUSTRIES = {
    'CHILD': ('325110',),
    'REST': ('325190', '325199'),
    'METAL': ('331110',),
    'MOVED': ('327100',),
}


def _table(rows: dict[str, tuple[float, float]]) -> pd.DataFrame:
    frame = pd.DataFrame.from_dict(rows, orient='index', columns=['usd', 'kwh'])
    frame['price'] = frame['usd'] / frame['kwh'] * 100.0
    return frame


def _tables() -> dict[int, pd.DataFrame]:
    return {
        2018: _table(
            {
                '31-33': (20.0, 300.0),
                '325': (10.0, 150.0),
                '325110': (4.0, 80.0),
                '331': (6.0, 100.0),
                '327': (4.0, 50.0),
            }
        ),
        2022: _table(
            {
                '31-33': (30.0, 300.0),
                '325': (15.0, 150.0),
                '325110': (6.0, 80.0),
                '331': (9.0, 100.0),
                '327': (6.0, 50.0),
                '3271': (3.0, 25.0),
            }
        ),
    }


def _eia(year: int) -> float:
    return {2017: 6.0, 2018: 6.5, 2020: 7.0, 2022: 9.0, 2024: 8.0}[year]


def _price(year: int) -> pd.Series:
    return ep.manufacturing_price_from_tables(year, _tables(), INDUSTRIES, _eia)


def test_a_row_without_a_total_falls_back_to_local_utility() -> None:
    table = ep.mecs_table_from_flows(
        pd.Series({'31-33': 10.0, '337': 0.0}),
        pd.Series({'31-33': 100.0, '337': 0.0}),
        pd.Series({'31-33': 8.0, '337': 2.0}),
        pd.Series({'31-33': 80.0, '337': 20.0}),
    )
    assert table.at['31-33', 'price'] == pytest.approx(10.0)
    assert table.at['337', 'price'] == pytest.approx(10.0)


def test_a_row_with_no_price_is_left_out() -> None:
    table = ep.mecs_table_from_flows(
        pd.Series({'322110': 0.0}),
        pd.Series({'322110': 22.0}),
        pd.Series({'322110': 0.0}),
        pd.Series({'322110': 12.0}),
    )
    assert table.empty


def test_a_naics_takes_its_deepest_published_row() -> None:
    table = _tables()[2022]
    assert ep.deepest_row('325110', table) == '325110'
    assert ep.deepest_row('325190', table) == '325'
    assert ep.deepest_row('339999', table) is None


def test_the_naics_overlay_applies_only_where_the_survey_merged_codes() -> None:
    merged = _table({'336110': (1.0, 10.0), '336': (2.0, 20.0)})
    separate = _table({'336111': (1.0, 10.0), '336': (2.0, 20.0)})
    assert ep.deepest_row('336111', merged) == '336110'
    assert ep.deepest_row('336111', separate) == '336111'


def test_a_parent_row_prices_what_its_children_leave() -> None:
    kwh, price = ep.remainder('325', _tables()[2022])
    assert kwh == pytest.approx(70.0)
    assert price == pytest.approx((15.0 - 6.0) / 70.0 * 100.0)


def test_only_the_nearest_children_are_subtracted() -> None:
    table = _table({'325': (10.0, 100.0), '3251': (4.0, 40.0), '325110': (1.0, 10.0)})
    assert ep.remainder('325', table)[0] == pytest.approx(60.0)


def test_a_remainder_that_is_mostly_rounding_takes_the_row_price() -> None:
    table = _table({'325': (10.0, 100.0), '325110': (9.9, 99.5)})
    _, price = ep.remainder('325', table)
    assert price == pytest.approx(10.0)


def test_a_shared_row_gives_each_industry_its_price_and_part_of_its_kwh() -> None:
    table = _table({'325': (10.0, 100.0)})
    prices = ep.survey_prices(table, {'A': ('325110',), 'B': ('325190',)})
    assert prices.at['A', 'price'] == pytest.approx(10.0)
    assert prices.at['B', 'price'] == pytest.approx(10.0)
    assert prices.at['A', 'kwh'] == pytest.approx(50.0)


def test_an_industry_with_no_published_row_has_no_price() -> None:
    prices = ep.survey_prices(_tables()[2022], {'FURNITURE': ('337110',)})
    assert prices.empty


def test_the_level_reproduces_the_mecs_national_price_in_both_surveys() -> None:
    national = {2018: 6.0, 2022: 10.0}
    assert ep.price_level(2018, _eia, national) == pytest.approx(6.0)
    assert ep.price_level(2022, _eia, national) == pytest.approx(10.0)


def test_the_ratio_to_eia_holds_outside_the_surveys() -> None:
    national = {2018: 6.0, 2022: 10.0}
    assert ep.price_level(2017, _eia, national) == pytest.approx(6.0 * 6.0 / 6.5)
    assert ep.price_level(2024, _eia, national) == pytest.approx(10.0 * 8.0 / 9.0)


def test_the_anchor_survey_is_reproduced_exactly() -> None:
    prices = _price(2022)
    assert prices['CHILD'] == pytest.approx(7.5)
    assert prices['METAL'] == pytest.approx(9.0)
    assert prices['REST'] == pytest.approx(9.0 / 70.0 * 100.0)


def test_the_national_row_does_not_scale_published_prices() -> None:
    tables = _tables()
    tables[2022].loc['31-33', 'price'] = 50.0
    prices = ep.manufacturing_price_from_tables(2022, tables, INDUSTRIES, _eia)
    assert prices['CHILD'] == pytest.approx(7.5)


def test_kwh_weighted_prices_average_the_level_in_every_year() -> None:
    tables = _tables()
    national = {}
    surveys = {}
    for survey in ep.SURVEYS:
        frame = ep.survey_prices(tables[survey], INDUSTRIES)
        national[survey] = ep.survey_average(frame)
        frame['price'] = frame['price'] / national[survey]
        surveys[survey] = frame
    for year in (2017, 2018, 2020, 2022, 2024):
        _, kwh = ep.relative_path(surveys[2018], surveys[2022], year)
        prices = _price(year)
        average = float((prices * kwh).sum() / kwh.sum())
        assert average == pytest.approx(ep.price_level(year, _eia, national))


def test_relatives_move_from_the_early_survey_to_the_anchor() -> None:
    early = pd.DataFrame(
        {'price': [0.5], 'kwh': [10.0], 'rows': ['325110']}, index=['CHILD']
    )
    anchor = pd.DataFrame(
        {'price': [2.0], 'kwh': [10.0], 'rows': ['325110']}, index=['CHILD']
    )
    assert ep.relative_path(early, anchor, 2018)[0]['CHILD'] == pytest.approx(0.5)
    assert ep.relative_path(early, anchor, 2017)[0]['CHILD'] == pytest.approx(0.5)
    assert ep.relative_path(early, anchor, 2020)[0]['CHILD'] == pytest.approx(1.0)
    assert ep.relative_path(early, anchor, 2022)[0]['CHILD'] == pytest.approx(2.0)
    assert ep.relative_path(early, anchor, 2024)[0]['CHILD'] == pytest.approx(2.0)


def test_an_industry_on_different_rows_stays_at_the_anchor() -> None:
    early = pd.DataFrame(
        {'price': [0.5], 'kwh': [10.0], 'rows': ['327']}, index=['MOVED']
    )
    anchor = pd.DataFrame(
        {'price': [2.0], 'kwh': [10.0], 'rows': ['3271']}, index=['MOVED']
    )
    assert ep.relative_path(early, anchor, 2018)[0]['MOVED'] == pytest.approx(2.0)


def test_an_industry_only_in_the_anchor_survey_stays_at_the_anchor() -> None:
    early = pd.DataFrame(columns=['price', 'kwh', 'rows'])
    anchor = pd.DataFrame(
        {'price': [2.0], 'kwh': [10.0], 'rows': ['3271']}, index=['NEW']
    )
    assert ep.relative_path(early, anchor, 2018)[0]['NEW'] == pytest.approx(2.0)


def test_resale_price_is_revenue_over_volume_in_cents() -> None:
    assert ep.resale_price(150_000.0, 3_000_000_000.0) == pytest.approx(5.0)


def test_resale_price_needs_a_positive_volume() -> None:
    with pytest.raises(ValueError, match='non-positive'):
        ep.resale_price(1.0, 0.0)


def test_buyers_below_lists_the_cheapest_first() -> None:
    prices = pd.Series({'A': 6.0, 'B': 4.0, 'C': 5.0})
    assert list(ep.buyers_below(prices, 5.5).index) == ['B', 'C']
    assert ep.buyers_below(prices, 3.0).empty
