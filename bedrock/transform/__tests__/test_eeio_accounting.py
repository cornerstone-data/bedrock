"""Unit tests for EEIO accounting balance validations.

Port of R validation: commodity output adjusted by CPI equals market share matrix
times CPI-adjusted industry output (ValidateModel.R#L200-L240).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import bedrock.utils.math.formulas as formulas
from bedrock.transform.eeio.derived_2017 import (
    derive_2017_g_usa,
    derive_2017_q_usa,
    derive_2017_V_usa,
)
from bedrock.utils.economic.inflation import (
    obtain_inflation_factors_from_reference_data,
)


# TODO: Review this function
@pytest.mark.parametrize(
    "base_year, target_year, tolerance",
    [
        (2017, 2023, 0.01),
    ],
)
# @pytest.mark.eeio_integration
def test_commodity_industry_output_cpi_consistency(
    base_year: int,
    target_year: int,
    tolerance: float,
) -> None:
    """Test that commodity output adjusted by CPI equals market share matrix times CPI-adjusted industry output."""
    # 2024 is the upper limit of the inflation factors data
    if not (2017 <= base_year <= target_year <= 2024):
        raise ValueError("Base or target year is out of range")

    V = derive_2017_V_usa()  # Make table
    q = derive_2017_q_usa()  # commodity output
    x = derive_2017_g_usa()  # industry output

    # Market share matrix C_m (industry x commodity)
    ##C_m = V.divide(x, axis=0).T.fillna(0)
    C_m = formulas.compute_Vnorm_matrix(V=V, q=q)
    # The above is equivalent to generateMarketSharesfromMake which also uses V and q

    # CPI vectors from bedrock's inflation utilities
    industry_CPI = obtain_inflation_factors_from_reference_data()
    # The above is equivalent to Detail_CPI_IO_17sch.rda which in turn is the same as model$MultiYearIndustryCPI

    # Create commodity CPI by multiplying an I x 1 matrix @ a C x I matrix which yields a C x 1 matrix
    # for each column of industry_CPI, which are the various years
    commodity_CPI = pd.DataFrame().reindex_like(industry_CPI)
    for i in range(len(industry_CPI.columns)):
        commodity_CPI.iloc[:, i] = industry_CPI.iloc[:, i] @ C_m

    # Calculate CPI ratios.
    # Cannot use inflate_q_or_y from inflate_to_target_year.py for x_check as the
    # formula required is different than for q_check, so calculating both here.
    industry_CPI_ratio = industry_CPI[target_year] / industry_CPI[base_year]
    commodity_CPI_ratio = commodity_CPI[target_year] / commodity_CPI[base_year]

    # Calculate q_check and x_check
    q_check = q * commodity_CPI_ratio
    x_check = C_m @ (x * industry_CPI_ratio)

    # Convert pandas Series to numpy arrays of float64 for compatibility with assert_allclose
    q_check_arr = np.asarray(q_check.values, dtype=np.float64)
    x_check_arr = np.asarray(x_check.values, dtype=np.float64)
    np.testing.assert_allclose(
        q_check_arr,
        x_check_arr,
        rtol=tolerance,
        err_msg="CPI-adjusted commodity output should equal C_m @ (CPI-adjusted industry output)",
    )
