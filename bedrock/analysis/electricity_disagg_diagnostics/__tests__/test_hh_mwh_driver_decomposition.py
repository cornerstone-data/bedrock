from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry.hh_mwh_driver_decomposition import (
    _convert_flows,
    _driver_metrics,
    _flows_by_bucket,
    _supply_chain_summary,
)


def test_uniform_conversion_preserves_monetary_shares() -> None:
    intermediate = pd.Series({'221110': 60.0, '531ORE': 20.0})
    final_demand = pd.Series({'F01000': 20.0, 'F03000': 0.0})
    factors = pd.Series(2.0, index=[*intermediate.index, *final_demand.index])

    inter_mwh, fd_mwh = _convert_flows(intermediate, final_demand, factors)
    monetary = _flows_by_bucket(intermediate, final_demand)
    converted = _flows_by_bucket(inter_mwh, fd_mwh)

    assert converted['total'] == pytest.approx(200.0)
    assert converted['household_share'] == pytest.approx(monetary['household_share'])
    assert converted['intermediate_share'] == pytest.approx(
        monetary['intermediate_share']
    )
    assert monetary['intermediate_221110_self_use'] == pytest.approx(60.0)
    assert monetary['intermediate_other'] == pytest.approx(20.0)
    assert converted['intermediate_221110_self_use'] == pytest.approx(120.0)
    assert converted['intermediate_other'] == pytest.approx(40.0)


def test_driver_metrics_separate_uniform_and_price_effects() -> None:
    monetary = {'household_share': 0.25}
    uniform = {
        'household_F01000': 100.0,
        'intermediate': 300.0,
        'household_share': 0.25,
    }
    production = {
        'household_F01000': 80.0,
        'intermediate': 320.0,
        'household_share': 0.20,
    }
    eia = {
        'Residential': 120.0,
        'Nonresidential sales': 280.0,
        'Total sales': 400.0,
    }

    result = _driver_metrics(monetary, uniform, production, eia)

    assert result['household']['total_eia_minus_production_gap_MWh'] == 40.0
    assert result['household']['share_of_total_shortfall_added_by_class_prices'] == 0.5
    assert result['intermediate']['total_production_minus_eia_excess_MWh'] == 40.0
    assert result['intermediate']['share_of_total_excess_added_by_class_prices'] == 0.5


def test_supply_chain_summary_separates_electricity_children() -> None:
    intermediate = pd.Series(
        {
            '221110': 25.0,
            '221121': 5.0,
            '221122': 10.0,
            '531ORE': 60.0,
        }
    )

    result = _supply_chain_summary(intermediate)

    assert result['electricity_supply_chain_total_MWh'] == 40.0
    assert result['other_intermediate_purchasers_MWh'] == 60.0
    assert result['share_of_intermediate_in_electricity_supply_chain'] == 0.4
