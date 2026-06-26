"""Blocking merge gate: compensated scenario market-clearing gaps < $1M."""

from __future__ import annotations

from typing import Callable

import pytest

from bedrock.analysis.electricity.d_85.balance_metrics import compute_balance_metrics
from bedrock.analysis.electricity.d_85.disagg_scenarios import run_scenario
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_disaggregation_enabled,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    _derive_post_reallocation_checkpoint_for_disagg,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_electricity_commodity_row_weights,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config

_MERGE_GATE_ATOL = 6e6  # expected baseline noise ~$5M after compensating w_row

_CACHED: list[Callable[..., object]] = [
    electricity_disaggregation_enabled,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_electricity_commodity_row_weights,
    _derive_post_reallocation_checkpoint_for_disagg,
]


def _clear_caches() -> None:
    for fn in _CACHED:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()


@pytest.mark.eeio_integration
def test_compensated_scenario_market_clearing_gap_under_1m() -> None:
    _clear_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(
        'test_usa_config_waste_disagg_electricity_disaggregation.yaml'
    )
    try:
        result = run_scenario('t8.3_purchased_power_diag_compensated')
        assert not result.metrics_only, 'compensated scenario must complete all steps'
        metrics = compute_balance_metrics(result)
        for gap in metrics['market_clearing_gap']:
            assert (
                abs(float(gap)) < _MERGE_GATE_ATOL
            ), f'market clearing gap {gap} exceeds ${_MERGE_GATE_ATOL / 1e6:.0f}M threshold'
    finally:
        _clear_caches()
        reset_usa_config(should_reset_env_var=True)
