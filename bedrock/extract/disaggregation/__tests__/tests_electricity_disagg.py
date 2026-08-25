"""Tests for the electricity disaggregation workstream."""

from __future__ import annotations

import math
from unittest.mock import patch

import pandas as pd
import pytest

import bedrock.transform.eeio.derived_cornerstone as derived_cornerstone
from bedrock.extract.disaggregation.egrid_generation import (
    eia_table_2_14_export_mwh,
    eia_table_2_14_year_for_egrid_year,
    load_egrid_ggl,
    us_total_net_generation_mwh,
)
from bedrock.utils.validation.exceptions import FBANotAvailableError

# eGRID 2022 workbook, US sheet: USNGENAN ("U.S. annual net generation (MWh)")
_EGRID_2022_NET_GENERATION_MWH = 4_240_140_533


def test_derive_cornerstone_ytot_full_cs_matrix_is_copy_of_underlying() -> None:
    # Electricity disaggregation step 1 (PR1): export of waste-disaggregated matrices.
    # This test guards the thin wrapper used to produce cornerstone_Ytot_full_cs.csv
    # (full commodity x FD Y matrix in cornerstone space, including trade columns).
    fake = pd.DataFrame({"F1": [1.0, 2.0]}, index=["c1", "c2"])
    with (
        patch.object(
            derived_cornerstone, "cornerstone_sector_disagg_active", return_value=True
        ),
        patch.object(
            derived_cornerstone, "derive_disagg_Ytot_with_trade", return_value=fake
        ),
    ):
        out = derived_cornerstone.derive_cornerstone_Ytot_full_cs_matrix()
    pd.testing.assert_frame_equal(out, fake)
    assert out is not fake


@pytest.mark.eeio_integration
def test_us_total_net_generation_mwh_2022_matches_stewi_egrid() -> None:
    """US net generation matches eGRID 2022 Excel US tab USNGENAN."""
    actual = us_total_net_generation_mwh(2022, download_if_missing=True)
    assert math.isclose(
        actual,
        _EGRID_2022_NET_GENERATION_MWH,
        rel_tol=0,
        abs_tol=1.0,
    )


@pytest.mark.eeio_integration
def test_load_egrid_ggl_2018_us_grid_gross_loss() -> None:
    """GGL sheet via stewi extract_eGRID_excel; U.S. grid gross loss for 2018."""
    ggl = load_egrid_ggl(2018, download_if_missing=True)
    us_row = ggl.loc[ggl["region"] == "U.S.", "grid_gross_loss"].iloc[0]
    assert math.isclose(us_row, 0.048681, rel_tol=0, abs_tol=1e-6)


def _table_2_14_fba(year: int, *, canada_mwh: float, mexico_mwh: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            'Year': [year, year],
            'FlowName': ['electricity exports', 'electricity exports'],
            'Description': ['Table 2.14 trade', 'Table 2.14 trade'],
            'Location': ['Canada', 'Mexico'],
            'FlowAmount': [canada_mwh, mexico_mwh],
        }
    )


def test_eia_table_2_14_export_mwh_requires_exact_year() -> None:
    def fake_epa(year: int) -> pd.DataFrame:
        if year == 2024:
            raise FBANotAvailableError(year=2024)
        if year == 2022:
            return _table_2_14_fba(2022, canada_mwh=100.0, mexico_mwh=50.0)
        raise FBANotAvailableError(year=year)

    eia_table_2_14_export_mwh.cache_clear()
    try:
        with patch(
            'bedrock.extract.disaggregation.egrid_generation._epa_fba',
            side_effect=fake_epa,
        ):
            with pytest.raises(FBANotAvailableError):
                eia_table_2_14_export_mwh(2024)
            assert eia_table_2_14_export_mwh(2022) == 150.0
    finally:
        eia_table_2_14_export_mwh.cache_clear()


def test_eia_table_2_14_year_for_egrid_year_when_table_lags() -> None:
    def fake_epa(year: int) -> pd.DataFrame:
        if year == 2022:
            return _table_2_14_fba(2022, canada_mwh=10.0, mexico_mwh=5.0)
        raise FBANotAvailableError(year=year)

    eia_table_2_14_year_for_egrid_year.cache_clear()
    try:
        with patch(
            'bedrock.extract.disaggregation.egrid_generation._epa_fba',
            side_effect=fake_epa,
        ):
            assert eia_table_2_14_year_for_egrid_year(2024) == 2022
            assert eia_table_2_14_year_for_egrid_year(2022) == 2022
    finally:
        eia_table_2_14_year_for_egrid_year.cache_clear()
