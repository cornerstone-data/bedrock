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
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_q,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_x,
)
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
    if pipeline != "cornerstone":
        # 2017 BEA detail Make/Use vectors; CPI path builds commodity inflation from
        # CEDA reference tables and market shares M_s(V, q) inside the compare fn.
        V = derive_2017_V_usa()  # Make table
        q = derive_2017_q_usa()  # commodity output
        x = derive_2017_x_usa()  # industry output
    else:
        # Cornerstone-mapped V/q/x (BEA→CS correspondence, optional waste disagg);
        # CPI path uses cornerstone industry/commodity price-ratio helpers instead.
        V = derive_cornerstone_V()
        q = derive_cornerstone_q()
        x = derive_cornerstone_x()

    r_c_x_cpi_consistency = commodity_industry_output_cpi_consistency(
        V=V,
        q=q,
        x=x,
        base_year=base_year,
        target_year=target_year,
        tolerance=tolerance,
        include_details=True,
        cpi_source=pipeline,  # selects CEDA vs cornerstone inflation inside compare fn
    )

    assert len(r_c_x_cpi_consistency.failing_sectors) == 0


@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    "pipeline, output, tolerance, include_details",
    [
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
    ],
)
def test_compare_industry_output_in_make_and_use(
    pipeline: str,
    output: ta.Literal['Industry', 'Commodity'],
    tolerance: float,
    include_details: bool,
) -> None:
    """Test that output implied by the Make table matches the Use table (+ VA or final demand)."""
    V: pd.DataFrame
    if pipeline != "cornerstone":
        # CEDA: Commodity check compares V column sums (Make q) to U row sums plus net
        # final demand from Ytot trade. Four redefined appliance commodities (335221,
        # 335222, 335224, 335228) fail at 5%—Make detail from V disagrees with Use
        # plus trade-as-final-demand, consistent with IoT redefinition handling.
        # Industry check compares V row sums to U column sums + VA; 11 industries fail
        # (e.g. 221300, 331110, 481000, 523A00) where detail VA/Use treatment does
        # not reconcile to Make gross output row sums.
        # 2017 BEA detail V/U; compare fn loads derive_detail_VA_usa / 2017 Ytot internally.
        V = derive_2017_V_usa()  # Make table
        U_set = derive_2017_U_with_negatives()  # Use table output
        U = U_set.Udom + U_set.Uimp
    else:
        # Cornerstone: Commodity failures are confined to new waste-disagg codes
        # (562HAZ, 562212, 562OTH) and special code S00402—Make q from disaggregated
        # V while Use-side final demand uses Ytot/trade mapped through correspondence
        # without the same split. Industry: 10 failures include those waste parents/
        # children plus expanded industries (331314 vs BEA 331313) where BEA→CS
        # correspondence duplicates Make/Use differently across V and VA paths.
        # Cornerstone-mapped V/U; compare fn loads derive_cornerstone_VA / Ytot internally.
        V = derive_cornerstone_V()
        U_set = derive_cornerstone_U_with_negatives()
        U = U_set.Udom + U_set.Uimp

    r_output_in_V_and_U = compare_output_from_make_and_use(
        output=output,
        V=V,
        U=U,
        tolerance=tolerance,
        include_details=include_details,
        pipeline=pipeline,
    )

    assert len(r_output_in_V_and_U.failing_sectors) == 0
