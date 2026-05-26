"""Unit tests for EEIO accounting balance validations.

Port of R validation: commodity output adjusted by CPI equals market share matrix
times CPI-adjusted industry output (ValidateModel.R#L200-L240).
"""

from __future__ import annotations

import typing as ta

import pandas as pd
import pytest

from bedrock.transform.eeio.derived_2017 import (
    derive_2017_q_usa,
    derive_2017_U_with_negatives,
    derive_2017_V_usa,
    derive_2017_x_usa,
    derive_2017_Ytot_usa_matrix_set,
    derive_detail_VA_usa,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_q,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_x,
    derive_cornerstone_Ytot_matrix_set,
)
from bedrock.utils.economic.inflation_helpers_ceda import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.math.formulas import compute_Vnorm_matrix
from bedrock.utils.schemas.single_region_types import SingleRegionYtotAndTradeVectorSet
from bedrock.utils.validation.eeio_diagnostics import (
    commodity_industry_output_cpi_consistency,
    compare_output_from_make_and_use,
)


@pytest.mark.eeio_integration
@pytest.mark.parametrize("pipeline", ["ceda", "cornerstone"])
def test_commodity_industry_output_cpi_consistency(
    pipeline: str,
    base_year: int = 2017,
    target_year: int = 2022,
    tolerance: float = 0.05,
    include_details: bool = False,
) -> None:
    """Test that commodity output adjusted by CPI equals market share matrix times CPI-adjusted industry output."""
    # 2024 is the upper limit of the inflation factors data
    if not (2017 <= base_year <= target_year <= 2024):
        raise ValueError("Base or target year is out of range")

    V: pd.DataFrame
    q: pd.Series[float]
    x: pd.Series[float]
    industry_CPI_ratio: pd.Series[float]
    commodity_CPI_ratio: pd.Series[float]
    if pipeline != "cornerstone":
        # 2017 BEA detail Make/Use vectors; CPI ratios from CEDA inflation tables
        # and market shares M_s(V, q).
        V = derive_2017_V_usa()  # Make table
        q = derive_2017_q_usa()  # commodity output
        x = derive_2017_x_usa()  # industry output
        market_shares = compute_Vnorm_matrix(V=V, q=q)
        industry_CPI = obtain_inflation_factors_from_reference_data()
        commodity_CPI = pd.DataFrame().reindex_like(industry_CPI)
        for i in range(len(industry_CPI.columns)):
            commodity_CPI.iloc[:, i] = industry_CPI.iloc[:, i] @ market_shares
        industry_CPI_ratio = industry_CPI[target_year] / industry_CPI[base_year]
        commodity_CPI_ratio = commodity_CPI[target_year] / commodity_CPI[base_year]
    else:
        # Cornerstone-mapped V/q/x; CPI ratios from cornerstone price-index helpers.
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
        "ceda",
        "Commodity",
        0.05,
        True,
        marks=pytest.mark.xfail(
            reason="CEDA: Make q≠Use q for 4 appliance sectors (335221–335228); IoT redefinition mismatch.",
        ),
    ),
    pytest.param(
        "ceda",
        "Industry",
        0.05,
        True,
        marks=pytest.mark.xfail(
            reason="CEDA: Make x≠Use x+VA for 11 industries; detail Use/VA vs Make row sums.",
        ),
    ),
    pytest.param(
        "cornerstone",
        "Commodity",
        0.05,
        True,
        marks=pytest.mark.xfail(
            reason="Cornerstone: Make q≠Use q for 4 waste/special codes (562*, S00402); disagg V vs trade Y.",
        ),
    ),
    pytest.param(
        "cornerstone",
        "Industry",
        0.05,
        True,
        marks=pytest.mark.xfail(
            reason="Cornerstone: Make x≠Use x+VA for 10 industries; BEA→CS remap and 562 waste split.",
        ),
    ),
]
# CEDA: Commodity check compares V column sums (Make q) to U row sums plus net
# final demand from Ytot trade. Four redefined appliance commodities (335221,
# 335222, 335224, 335228) fail at 5%—Make detail from V disagrees with Use
# plus trade-as-final-demand, consistent with IoT redefinition handling.
# Industry check compares V row sums to U column sums + VA; 11 industries fail
# (e.g. 221300, 331110, 481000, 523A00) where detail VA/Use treatment does
# not reconcile to Make gross output row sums.


# Cornerstone: Commodity failures are confined to new waste-disagg codes
# (562HAZ, 562212, 562OTH) and special code S00402—Make q from disaggregated
# V while Use-side final demand uses Ytot/trade mapped through correspondence
# without the same split. Industry: 10 failures include those waste parents/
# children plus expanded industries (331314 vs BEA 331313) where BEA→CS
# correspondence duplicates Make/Use differently across V and VA paths.
@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    "pipeline, output, tolerance, include_details",
    _MAKE_USE_CASES,
)
def test_compare_industry_output_in_make_and_use(
    pipeline: str,
    output: ta.Literal['Industry', 'Commodity'],
    tolerance: float,
    include_details: bool,
) -> None:
    """Test that output implied by the Make table matches the Use table (+ VA or final demand)."""
    V: pd.DataFrame
    VA: pd.DataFrame
    y_set: SingleRegionYtotAndTradeVectorSet
    if pipeline != "cornerstone":
        VA = derive_detail_VA_usa()
        y_set = derive_2017_Ytot_usa_matrix_set()
        V = derive_2017_V_usa()  # Make table
        U_set = derive_2017_U_with_negatives()  # Use table output
        U = U_set.Udom + U_set.Uimp
    else:
        VA = derive_cornerstone_VA()
        y_set = derive_cornerstone_Ytot_matrix_set()
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
