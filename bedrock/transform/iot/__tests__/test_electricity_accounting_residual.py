"""Unit tests for the electricity accounting residual, on synthetic numbers."""

import pandas as pd
import pytest

from bedrock.transform.iot import electricity_accounting_residual as ear
from bedrock.utils.config.usa_config import get_usa_config

CODE = ear.RESIDUAL_CODE


def _tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    industries = ['221100', 'S00101', 'S00202', '111200', '531ORE']
    commodities = ['221100', '211000', '531ORE']
    use = pd.DataFrame(
        [
            [5.0, 1.0, 1.0, 40.0, 60.0, 200.0],
            [20.0, 2.0, 10.0, 15.0, 5.0, 3.0],
            [4.0, 0.0, 0.0, 5.0, 10.0, 50.0],
            [10.0, 2.0, 4.0, 20.0, 10.0, 0.0],
            [61.0, 5.0, 45.0, 40.0, 15.0, 0.0],
        ],
        index=[*commodities, 'V00100', 'V00300'],
        columns=[*industries, 'F01000'],
    )
    supply = pd.DataFrame(
        [
            [100.0, 10.0, 60.0, 0.0, 0.0, 2.0],
            [0.0, 0.0, 0.0, 120.0, 0.0, 1.0],
            [0.0, 0.0, 0.0, 0.0, 100.0, 0.0],
        ],
        index=commodities,
        columns=[*industries, 'MCIF'],
    )
    return use, supply


PURCHASES = pd.Series({'111200': 4.0, '531ORE': 6.0})


def test_the_residual_follows_the_non_fuel_target() -> None:
    """Non-fuel 2017 is 100 - 60 - 10 = 30; it doubles to 60."""
    total = ear.residual_total(
        output_2017=100.0,
        value_added_2017=60.0,
        value_added=90.0,
        fuel_2017=10.0,
        fuel=20.0,
        retail_relative=1.5,
        input_price_relative=1.6,
        kwh_relative=1.25,
    )
    # Electric value added is 150 - 20 - 60 = 70, against BEA's 90.
    assert total == pytest.approx(20.0)


def test_at_the_base_year_the_residual_is_zero() -> None:
    assert ear.residual_total(100.0, 60.0, 60.0, 10.0, 10.0, 1.0, 1.0, 1.0) == 0.0


def test_a_negative_residual_is_set_to_zero() -> None:
    assert ear.residual_total(100.0, 60.0, 50.0, 10.0, 10.0, 1.2, 1.0, 1.0) == 0.0


def test_the_residual_splits_across_columns_by_value_added() -> None:
    split = ear.split_across_columns(10.0, pd.Series({'221100': 90.0, 'S00101': 10.0}))
    assert split['221100'] == pytest.approx(9.0)
    assert split['S00101'] == pytest.approx(1.0)


def test_buyers_split_by_class_excess_then_by_bill() -> None:
    """Commercial bills 130 against 100; industrial 60 against 50: 75% to 25%."""
    bills = pd.Series({'531ORE': 100.0, '722110': 30.0, '331110': 45.0, '111200': 15.0})
    shares = ear.buyer_shares(bills, 100.0, 50.0)
    assert shares.sum() == pytest.approx(1.0)
    assert shares['531ORE'] == pytest.approx(0.75 * 100.0 / 130.0)
    assert shares['722110'] == pytest.approx(0.75 * 30.0 / 130.0)
    assert shares['331110'] == pytest.approx(0.25 * 45.0 / 60.0)
    assert shares['111200'] == pytest.approx(0.25 * 15.0 / 60.0)


def test_a_class_below_its_revenue_takes_none() -> None:
    bills = pd.Series({'531ORE': 130.0, '331110': 40.0})
    shares = ear.buyer_shares(bills, 100.0, 50.0)
    assert shares['531ORE'] == pytest.approx(1.0)
    assert shares['331110'] == 0.0


def test_no_excess_raises() -> None:
    with pytest.raises(ValueError, match='no class'):
        ear.buyer_shares(pd.Series({'531ORE': 50.0, '331110': 40.0}), 100.0, 50.0)


def test_the_pair_is_added_and_only_buyer_surplus_moves() -> None:
    use, supply = _tables()
    new_use, new_supply = ear.split_residual(use, supply, PURCHASES)

    assert new_use.at[CODE, '111200'] == pytest.approx(4.0)
    assert new_use.at['V00300', '111200'] == pytest.approx(36.0)
    assert new_use.at['V00300', CODE] == pytest.approx(10.0)
    assert new_use.at['221100', '111200'] == use.at['221100', '111200']
    assert new_use.at['211000', '221100'] == use.at['211000', '221100']
    assert new_use.at['V00300', '221100'] == use.at['V00300', '221100']
    assert new_use[['111200', '531ORE']].sum().tolist() == pytest.approx(
        use[['111200', '531ORE']].sum().tolist()
    )
    assert new_use.loc[['221100', '211000', '531ORE', CODE], CODE].sum() == 0.0
    assert new_use.loc['V00300'].sum() == pytest.approx(use.loc['V00300'].sum())
    assert new_supply.at[CODE, CODE] == pytest.approx(10.0)
    assert new_supply.at['221100', '221100'] == supply.at['221100', '221100']


def test_the_inputs_are_not_changed_in_place() -> None:
    use, supply = _tables()
    original = use.copy(), supply.copy()
    ear.split_residual(use, supply, PURCHASES)
    pd.testing.assert_frame_equal(use, original[0])
    pd.testing.assert_frame_equal(supply, original[1])


def test_row_and_column_order_is_kept() -> None:
    use, supply = _tables()
    new_use, new_supply = ear.split_residual(use, supply, PURCHASES)
    assert list(new_use.index) == [
        '221100',
        '211000',
        '531ORE',
        CODE,
        'V00100',
        'V00300',
    ]
    assert list(new_use.columns)[-2:] == [CODE, 'F01000']
    assert list(new_supply.columns)[-2:] == [CODE, 'MCIF']


def test_a_negative_purchase_raises() -> None:
    use, supply = _tables()
    with pytest.raises(ValueError, match='negative purchase'):
        ear.split_residual(use, supply, pd.Series({'111200': -1.0}))


def test_the_pathway_is_off_and_needs_retail_output() -> None:
    assert get_usa_config().electricity_accounting_residual is False
    with pytest.raises(ValueError, match='requires'):
        get_usa_config().model_validate(
            {
                **get_usa_config().model_dump(),
                'electric_output_on_retail_revenue': False,
                'electricity_accounting_residual': True,
            }
        )
