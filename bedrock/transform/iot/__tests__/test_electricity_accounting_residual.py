"""Unit tests for the electricity accounting residual, on synthetic numbers."""

import pandas as pd
import pytest

from bedrock.transform.iot import electricity_accounting_residual as ear
from bedrock.utils.config.usa_config import USAConfig, get_usa_config


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


def test_the_pathway_is_off_and_needs_retail_output() -> None:
    assert USAConfig.model_fields['electricity_accounting_residual'].default is False
    with pytest.raises(ValueError, match='requires'):
        get_usa_config().model_validate(
            {
                **get_usa_config().model_dump(),
                'electric_output_on_retail_revenue': False,
                'electricity_accounting_residual': True,
            }
        )


def test_the_excess_is_what_buyers_paid_above_revenue() -> None:
    bills = pd.Series({'721000': 60.0, '311111': 30.0})
    assert ear.base_year_excess(bills, 50.0, 25.0) == pytest.approx(15.0)


def test_buyers_at_revenue_leave_no_excess() -> None:
    assert ear.base_year_excess(pd.Series({'721000': 60.0}), 50.0, 25.0) == 0.0


def test_at_the_base_year_the_residual_is_the_excess_on_eia_level() -> None:
    total = ear.residual_total(
        output_2017=400.0,
        value_added_2017=250.0,
        value_added=250.0,
        fuel_2017=50.0,
        fuel=50.0,
        retail_relative=1.0,
        input_price_relative=1.0,
        kwh_relative=1.0,
        excess_2017=60.0,
    )
    assert total == pytest.approx(60.0)


def test_later_years_carry_the_excess_on_retail_revenue() -> None:
    def total(excess: float) -> float:
        return ear.residual_total(
            output_2017=400.0,
            value_added_2017=250.0,
            value_added=350.0,
            fuel_2017=50.0,
            fuel=50.0,
            retail_relative=1.2,
            input_price_relative=1.1,
            kwh_relative=1.0,
            excess_2017=excess,
        )

    assert total(60.0) - total(0.0) == pytest.approx(60.0 * 1.2)


def test_the_level_flag_is_off_by_default_and_needs_the_residual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert USAConfig.model_fields['electric_output_on_eia_level'].default is False
    monkeypatch.setattr(
        get_usa_config(), 'electric_output_on_eia_level', False, raising=True
    )
    assert ear.level_excess() == 0.0
    with pytest.raises(ValueError, match='electric_output_on_eia_level requires'):
        USAConfig.model_validate(
            {
                **get_usa_config().model_dump(),
                'electricity_accounting_residual': False,
                'electric_output_on_eia_level': True,
            }
        )
