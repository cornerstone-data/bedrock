"""Unit tests for EEIO accounting balance validations.

Port of R validation: commodity output adjusted by CPI equals market share matrix
times CPI-adjusted industry output (ValidateModel.R#L200-L240).
"""

from __future__ import annotations

import typing as ta

import pytest

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_q,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_x,
    derive_cornerstone_Ytot_matrix_set,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.schemas.single_region_types import SingleRegionYtotAndTradeVectorSet
from bedrock.utils.validation.eeio_diagnostics import (
    commodity_industry_output_cpi_consistency,
    compare_output_from_make_and_use,
)


@pytest.mark.eeio_integration
def test_commodity_industry_output_cpi_consistency(
    base_year: int = 2017,
    target_year: int = 2022,
    tolerance: float = 0.05,
) -> None:
    """Commodity output adjusted by CPI matches market-share times CPI-adjusted x."""
    if not (2017 <= base_year <= target_year <= 2024):
        raise ValueError("Base or target year is out of range")

    V = derive_cornerstone_V()
    q = derive_cornerstone_q()
    x = derive_cornerstone_x()
    industry_CPI_ratio = get_cornerstone_industry_price_ratio(
        base_year, target_year
    ).reindex(x.index, fill_value=1.0)
    commodity_CPI_ratio = get_vnorm_adjusted_commodity_price_ratio(
        base_year, target_year
    ).reindex(q.index, fill_value=1.0)

    r_c_x_cpi_consistency = commodity_industry_output_cpi_consistency(
        V=V,
        q=q,
        x=x,
        industry_CPI_ratio=industry_CPI_ratio,
        commodity_CPI_ratio=commodity_CPI_ratio,
        tolerance=tolerance,
        include_details=True,
    )

    assert len(r_c_x_cpi_consistency.failing_sectors) == 0


_MAKE_USE_CASES = [
    pytest.param(
        "Commodity",
        0.05,
        True,
        marks=pytest.mark.xfail(
            reason="Cornerstone: Make q≠Use q for 4 waste/special codes (562*, S00402); disagg V vs trade Y.",
        ),
    ),
    pytest.param(
        "Industry",
        0.05,
        True,
        marks=pytest.mark.xfail(
            reason="Cornerstone: Make x≠Use x+VA for 10 industries; BEA→CS remap and 562 waste split.",
        ),
    ),
]


@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    "output, tolerance, include_details",
    _MAKE_USE_CASES,
)
def test_compare_output_from_make_and_use(
    output: ta.Literal['Industry', 'Commodity'],
    tolerance: float,
    include_details: bool,
) -> None:
    """Output implied by Make matches Use (+ VA or final demand)."""
    VA = derive_cornerstone_VA()
    y_set: SingleRegionYtotAndTradeVectorSet = derive_cornerstone_Ytot_matrix_set()
    V = derive_cornerstone_V()
    U_set = derive_cornerstone_U_with_negatives()
    U = U_set.Udom + U_set.Uimp

    r_output_in_V_and_U = compare_output_from_make_and_use(
        output=output,
        V=V,
        U=U,
        VA=VA,
        y_set=y_set,
        tolerance=tolerance,
        include_details=include_details,
    )

    assert len(r_output_in_V_and_U.failing_sectors) == 0
