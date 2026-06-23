"""Tests for EIA Table 8.3 / 2.4 query helpers."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity.d_85.eia_inputs import (
    table_2_4_prices_cents_kwh,
    table_8_3_gtd_expenses_musd,
    table_8_3_purchased_power_gtd_expenses_musd,
)


def test_table_8_3_gtd_expenses_musd(mock_fba: pd.DataFrame) -> None:
    out = table_8_3_gtd_expenses_musd(2017, fba=mock_fba)
    assert set(out) == {'Production', 'Transmission', 'Distribution'}
    assert out['Production'] == pytest.approx(98659.0)
    assert out['Transmission'] == pytest.approx(10804.0)
    assert out['Distribution'] == pytest.approx(4358.0)


def test_table_8_3_purchased_power_gtd_expenses_musd(mock_fba: pd.DataFrame) -> None:
    out = table_8_3_purchased_power_gtd_expenses_musd(2017, fba=mock_fba)
    assert set(out) == {'PurchasedPower', 'Transmission', 'Distribution'}
    assert out['PurchasedPower'] == pytest.approx(49030.0)
    assert out['Transmission'] == pytest.approx(10804.0)
    assert out['Distribution'] == pytest.approx(4358.0)


def test_table_2_4_prices_cents_kwh(mock_fba: pd.DataFrame) -> None:
    prices = table_2_4_prices_cents_kwh(2017, fba=mock_fba)
    assert prices['Residential'] == pytest.approx(12.89)
    assert prices['Total'] == pytest.approx(10.54)


def test_table_8_3_missing_flowname_raises(mock_fba: pd.DataFrame) -> None:
    bad = mock_fba.loc[mock_fba['FlowName'] != 'expenses: Production']
    with pytest.raises(ValueError, match='expenses: Production'):
        table_8_3_gtd_expenses_musd(2017, fba=bad)
