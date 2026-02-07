"""Unit tests for EEIO accounting balance validations.

Port of R validation: commodity output adjusted by CPI equals market share matrix
times CPI-adjusted industry output (ValidateModel.R#L200-L240).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_2017 import (
    derive_2017_g_usa,
    derive_2017_q_usa,
    derive_2017_V_usa,
)
from bedrock.utils.economic.inflation import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.math.formulas import compute_Vnorm_matrix


# TODO: Review this function
# @pytest.mark.eeio_integration
def test_commodity_industry_output_cpi_consistency(
    base_year: int = 2017,
    target_year: int = 2022,
    tolerance: float = 0.05,
) -> None:
    """Test that commodity output adjusted by CPI equals market share matrix times CPI-adjusted industry output."""
    # 2024 is the upper limit of the inflation factors data
    if not (2017 <= base_year <= target_year <= 2024):
        raise ValueError("Base or target year is out of range")

    V = derive_2017_V_usa()  # Make table
    q = derive_2017_q_usa()  # commodity output
    x = derive_2017_g_usa()  # industry output

    # Commodity mix matrix C_m (commodity x industry) (Marketshares transposed)
    # This is equivalent to generateCommodityMixMatrix in useeior which also uses V and q
    C_m = V.divide(x, axis=0).T.fillna(0)

    # Market share matrix M_s (industry x commodity)
    # This is equivalent to generateMarketSharesfromMake in useeior which also uses V and q
    M_s = compute_Vnorm_matrix(V=V, q=q)

    # CPI vectors from bedrock's inflation utilities
    # This is equivalent to Detail_CPI_IO_17sch.rda which in turn is the same as model$MultiYearIndustryCPI
    industry_CPI = obtain_inflation_factors_from_reference_data()

    # Create commodity CPI by multiplying an I x 1 matrix @ a I x C matrix which yields a C x 1 matrix
    # for each column of industry_CPI, which are the various years
    commodity_CPI = pd.DataFrame().reindex_like(industry_CPI)
    for i in range(len(industry_CPI.columns)):
        commodity_CPI.iloc[:, i] = industry_CPI.iloc[:, i] @ M_s

    # Calculate CPI ratios
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
