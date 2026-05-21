"""Unit tests for EEIO accounting balance validations.

Port of R validation: commodity output adjusted by CPI equals market share matrix
times CPI-adjusted industry output (ValidateModel.R#L200-L240).
"""

from __future__ import annotations

import typing as ta
from typing import Callable

import pytest

import bedrock.utils.config.common as common
from bedrock.transform.eeio.derived_2017 import (
    derive_2017_q_usa,
    derive_2017_U_with_negatives,
    derive_2017_V_usa,
    derive_2017_x_usa,
)
from bedrock.transform.eeio.derived_cornerstone import (
    _derive_cornerstone_Ytot_with_trade,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_q,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_y_nab,
    derive_cornerstone_Ytot_matrix_set,
    get_waste_disagg_weights,
)
from bedrock.utils.config.usa_config import (
    USAConfig,
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    _cornerstone_to_ceda_v7_parent,
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.validation.eeio_diagnostics import (
    assert_diagnostic_passed,
    commodity_industry_output_cpi_consistency,
    compare_output_from_make_and_use,
)

CORNERSTONE_FULL_MODEL_CONFIG = '2025_usa_cornerstone_full_model.yaml'

# keep in sync with test_waste_disagg_pipeline_integration._CACHED_FUNCTIONS + inflation helpers
_CORNERSTONE_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    derive_cornerstone_V,
    derive_cornerstone_x,
    derive_cornerstone_q,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    _derive_cornerstone_Ytot_with_trade,
    derive_cornerstone_Ytot_matrix_set,
    derive_cornerstone_VA,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_y_nab,
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
    _cornerstone_to_ceda_v7_parent,
]


def _clear_cornerstone_caches() -> None:
    for fn in _CORNERSTONE_CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()


def _setup_cornerstone_config() -> USAConfig:
    _clear_cornerstone_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(CORNERSTONE_FULL_MODEL_CONFIG)
    common.download_fba_on_api_error = True
    cfg = get_usa_config()
    assert cfg.use_cornerstone_2026_model_schema
    assert cfg.implement_waste_disaggregation
    assert cfg.load_E_from_flowsa
    assert cfg.new_ghg_method
    assert cfg.use_E_data_year_for_x_in_B
    assert cfg.model_base_year == 2023
    assert cfg.usa_io_data_year == 2022
    assert not cfg.scale_a_matrix_with_useeio_method
    return cfg


def _teardown_cornerstone_config() -> None:
    _clear_cornerstone_caches()
    reset_usa_config(should_reset_env_var=True)
    common.download_fba_on_api_error = False


@pytest.fixture
def cornerstone_full_model_config(request: pytest.FixtureRequest) -> USAConfig:
    cfg = _setup_cornerstone_config()
    request.addfinalizer(_teardown_cornerstone_config)
    return cfg


@pytest.mark.eeio_integration
def test_commodity_industry_output_cpi_consistency(
    base_year: int = 2017,
    target_year: int = 2022,
    tolerance: float = 0.05,
    include_details: bool = False,
) -> None:
    """Test that commodity output adjusted by CPI equals market share matrix times CPI-adjusted industry output."""
    # 2024 is the upper limit of the inflation factors data
    if not (2017 <= base_year <= target_year <= 2024):
        raise ValueError("Base or target year is out of range")

    V = derive_2017_V_usa()  # Make table
    q = derive_2017_q_usa()  # commodity output
    x = derive_2017_x_usa()  # industry output

    r_c_x_cpi_consistency = commodity_industry_output_cpi_consistency(
        V=V,
        q=q,
        x=x,
        base_year=base_year,
        target_year=target_year,
        tolerance=tolerance,
        include_details=True,
    )

    assert len(r_c_x_cpi_consistency.failing_sectors) == 0


@pytest.mark.skip
@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    "output, tolerance, include_details",
    [("Commodity", 0.05, True), ("Industry", 0.05, True)],
)
def test_compare_industry_output_in_make_and_use(
    output: ta.Literal['Industry', 'Commodity'],
    tolerance: float,
    include_details: bool,
) -> None:
    """Test that the industry ouput from the Make and Use tables are the same."""

    V = derive_2017_V_usa()  # Make table
    U_set = derive_2017_U_with_negatives()  # Use table output
    U = U_set.Udom + U_set.Uimp

    r_output_in_V_and_U = compare_output_from_make_and_use(
        output=output,
        V=V,
        U=U,
        tolerance=tolerance,
        include_details=include_details,
    )

    assert len(r_output_in_V_and_U.failing_sectors) == 0


@pytest.mark.eeio_integration
def test_cornerstone_commodity_industry_output_cpi_consistency(
    cornerstone_full_model_config: USAConfig,
    tolerance: float = 0.01,
) -> None:
    """Cornerstone CPI consistency under 2025_usa_cornerstone_full_model."""
    cfg = cornerstone_full_model_config
    V = derive_cornerstone_V()
    q = derive_cornerstone_q()
    x = derive_cornerstone_x()

    result = commodity_industry_output_cpi_consistency(
        V=V,
        q=q,
        x=x,
        base_year=cfg.usa_base_io_data_year,
        target_year=cfg.model_base_year,
        tolerance=tolerance,
        include_details=True,
        cpi_source='cornerstone',
    )

    assert_diagnostic_passed(result)


@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    'output, tolerance',
    [
        pytest.param(
            'Commodity',
            0.01,
            marks=pytest.mark.xfail(
                reason=(
                    '12 commodity sectors fail Make vs Use balance (S00402 has q=0 on '
                    'Make; 562* waste disagg splits V/U inconsistently).'
                ),
            ),
        ),
        pytest.param(
            'Industry',
            0.01,
            marks=pytest.mark.xfail(
                reason=(
                    '36 industry sectors fail Make vs Use+VA balance (metals, waste, '
                    'and services; max rel_diff ~26% in 331314).'
                ),
            ),
        ),
    ],
)
def test_cornerstone_compare_industry_output_in_make_and_use(
    cornerstone_full_model_config: USAConfig,
    output: ta.Literal['Industry', 'Commodity'],
    tolerance: float,
) -> None:
    """Cornerstone Make vs Use output balance under full model config."""
    # Commodity: V column sums (Make-side q) disagree with Use-side totals in 12
    # sectors at 1% tolerance. S00402 (used goods) is structural: Make gives q=0 while
    # Use/FD shows ~$32B (rel_diff=inf). Waste subsectors 562* show ~4–11% gaps
    # consistent with waste disaggregation allocating V, U, Y, and VA on different
    # weight bases. Industry: 36 sectors fail x from V vs U row sum + VA + net trade;
    # largest gaps are metals (331314 ~26%), waste (562HAZ ~14%), and several service
    # codes—suggesting industry-row conservation is not fully restored after cornerstone
    # scaling and waste disagg.
    del cornerstone_full_model_config
    V = derive_cornerstone_V()
    u_set = derive_cornerstone_U_with_negatives()
    U = u_set.Udom + u_set.Uimp
    y_set = derive_cornerstone_Ytot_matrix_set()

    result = compare_output_from_make_and_use(
        output=output,
        V=V,
        U=U,
        tolerance=tolerance,
        include_details=True,
        va=derive_cornerstone_VA(),
        y_set=y_set,
    )

    assert_diagnostic_passed(result)
