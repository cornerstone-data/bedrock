"""Unit tests for EEIO accounting balance validations.

Port of R validation: commodity output adjusted by CPI equals market share matrix
times CPI-adjusted industry output (ValidateModel.R#L200-L240).
"""

from __future__ import annotations

from bedrock.transform.eeio.derived_2017 import (
    derive_2017_g_usa,
    derive_2017_q_usa,
    derive_2017_V_usa,
)
from bedrock.utils.validation.eeio_diagnostics import (
    commodity_industry_output_cpi_consistency,
)


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

    commodity_industry_output_cpi_consistency(
        V=V, q=q, x=x, base_year=base_year, target_year=target_year, tolerance=tolerance
    )
