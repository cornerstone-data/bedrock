"""Unit tests for diagnostics helper functions."""

from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import bedrock.transform.eeio.cornerstone_bea_intermediates as bea
import bedrock.transform.iot.derived_gross_industry_output as go
import bedrock.utils.validation.diagnostics_helpers as dh
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.validation.diagnostics_helpers import (
    OldEfSet,
    construct_ef_diff_dataframe,
    d_n_new_inflated_eligibility,
    diff_and_perc_diff_two_vectors,
    inflation_adjust_ef_denom_to_new_base_year,
)


class TestDNNewInflatedEligibility:
    def test_emit_when_deflate_path(self) -> None:
        cfg = USAConfig(
            use_E_data_year_for_x_in_B=True,
            deflate_x_to_detail_io_year_for_B=True,
        )
        ok, reason = d_n_new_inflated_eligibility(cfg)
        assert ok is True
        assert reason == ''

    def test_skip_when_model_base_year_equals_detail_original_year(self) -> None:
        cfg = USAConfig(
            use_E_data_year_for_x_in_B=True,
            deflate_x_to_detail_io_year_for_B=True,
            model_base_year=2017,
        )
        ok, reason = d_n_new_inflated_eligibility(cfg)
        assert ok is False
        assert 'identity' in reason.lower()

    def test_skip_use_e_without_deflate(self) -> None:
        cfg = USAConfig(
            use_E_data_year_for_x_in_B=True,
            deflate_x_to_detail_io_year_for_B=False,
        )
        ok, reason = d_n_new_inflated_eligibility(cfg)
        assert ok is False
        assert 'GHG-year nominal' in reason

    def test_skip_default_b_inflation_path(self) -> None:
        cfg = USAConfig(
            deflate_x_to_detail_io_year_for_B=False,
            use_E_data_year_for_x_in_B=False,
        )
        ok, reason = d_n_new_inflated_eligibility(cfg)
        assert ok is False
        assert 'double-apply' in reason


SECTORS = ["1111A0", "1111B0", "221100"]


class TestDiffAndPercDiffTwoVectors:
    def test_basic_diff_and_perc_diff(self) -> None:
        idx = pd.Index(SECTORS)
        new = pd.DataFrame({"v": [10.0, 20.0, 30.0]}, index=idx)
        old = pd.DataFrame({"v": [8.0, 25.0, 30.0]}, index=idx)

        result = diff_and_perc_diff_two_vectors(new, old, old_val_name="ef")

        assert list(result.columns) == ["ef_new", "ef_old", "ef_diff", "ef_perc_diff"]

        assert result["ef_diff"].iloc[0] == pytest.approx(2.0)
        assert result["ef_diff"].iloc[1] == pytest.approx(-5.0)
        assert result["ef_diff"].iloc[2] == pytest.approx(0.0)

        assert result["ef_perc_diff"].iloc[0] == pytest.approx(2.0 / 8.0)
        assert result["ef_perc_diff"].iloc[1] == pytest.approx(-5.0 / 25.0)
        assert result["ef_perc_diff"].iloc[2] == pytest.approx(0.0)

    def test_division_by_zero_returns_zero(self) -> None:
        """When old value is zero, perc_diff should be 0 (not inf or NaN)."""
        idx = pd.Index(SECTORS)
        new = pd.DataFrame({"v": [5.0, 0.0, 10.0]}, index=idx)
        old = pd.DataFrame({"v": [0.0, 0.0, 10.0]}, index=idx)

        result = diff_and_perc_diff_two_vectors(new, old, old_val_name="ef")

        assert result["ef_perc_diff"].iloc[0] == 0.0
        assert result["ef_perc_diff"].iloc[1] == 0.0
        assert not result["ef_perc_diff"].isna().any()
        assert not np.isinf(result["ef_perc_diff"]).any()


class TestConstructEfDiffDataframe:
    def test_output_columns(self) -> None:
        idx = pd.Index(SECTORS)
        ef_new = pd.DataFrame({"v": [1.0, 2.0, 3.0]}, index=idx)
        ef_old = OldEfSet(
            raw=pd.DataFrame({"v": [0.9, 2.1, 2.8]}, index=idx),
            inflated=pd.DataFrame({"v": [0.95, 2.05, 2.85]}, index=idx),
        )

        result = construct_ef_diff_dataframe(
            ef_name="D",
            ef_new=ef_new,
            ef_old=ef_old,
        )

        expected_cols = [
            "sector_name",
            "D_new",
            "D_old_inflated",
            "D_old",
            "D_perc_diff",
        ]
        assert list(result.columns) == expected_cols

    def test_perc_diff_computed_against_inflated(self) -> None:
        """Percentage diff should be computed using inflated (not raw) old values."""
        idx = pd.Index(SECTORS)
        ef_new = pd.DataFrame({"v": [1.0, 2.0, 3.0]}, index=idx)
        inflated = [0.5, 2.0, 3.0]
        ef_old = OldEfSet(
            raw=pd.DataFrame({"v": [0.4, 1.9, 2.9]}, index=idx),
            inflated=pd.DataFrame({"v": inflated}, index=idx),
        )

        result = construct_ef_diff_dataframe(
            ef_name="D",
            ef_new=ef_new,
            ef_old=ef_old,
        )

        # (1.0 - 0.5) / 0.5 = 1.0
        assert result["D_perc_diff"].iloc[0] == pytest.approx(1.0)
        # (2.0 - 2.0) / 2.0 = 0.0
        assert result["D_perc_diff"].iloc[1] == pytest.approx(0.0)
        # (3.0 - 3.0) / 3.0 = 0.0
        assert result["D_perc_diff"].iloc[2] == pytest.approx(0.0)


class TestInflationAdjust:
    def test_basic_adjustment(self) -> None:
        """Inflation adjustment scales EFs by old_price / new_price per sector."""
        sectors = pd.Index(["1111A0", "1111B0"])
        old_ef = pd.Series([10.0, 20.0], index=sectors)

        price_index = pd.DataFrame(
            {2023: [100.0, 200.0], 2024: [110.0, 220.0]},
            index=sectors,
        )

        with patch(
            "bedrock.utils.validation.diagnostics_helpers.obtain_inflation_factors_from_reference_data",
            return_value=price_index,
        ):
            result = inflation_adjust_ef_denom_to_new_base_year(
                old_ef_vector=old_ef,
                new_base_year=2024,
                old_base_year=2023,
            )

        expected = pd.Series(
            [10.0 * 100.0 / 110.0, 20.0 * 200.0 / 220.0], index=sectors
        )
        pd.testing.assert_series_equal(result, expected)

    def test_missing_sectors_get_no_adjustment(self) -> None:
        """Sectors absent from the price index are unchanged (ratio defaults to 1.0)."""
        sectors = pd.Index(["1111A0", "MISSING"])
        old_ef = pd.Series([10.0, 20.0], index=sectors)

        price_index = pd.DataFrame(
            {2023: [100.0], 2024: [110.0]},
            index=pd.Index(["1111A0"]),
        )

        with patch(
            "bedrock.utils.validation.diagnostics_helpers.obtain_inflation_factors_from_reference_data",
            return_value=price_index,
        ):
            result = inflation_adjust_ef_denom_to_new_base_year(
                old_ef_vector=old_ef,
                new_base_year=2024,
                old_base_year=2023,
            )

        assert result["MISSING"] == pytest.approx(20.0)
        assert result["1111A0"] == pytest.approx(10.0 * 100.0 / 110.0)

    def test_nan_in_price_index_filled_with_one(self) -> None:
        """NaN values in the price ratio should be filled with 1.0."""
        sectors = pd.Index(["1111A0", "1111B0"])
        old_ef = pd.Series([10.0, 20.0], index=sectors)

        price_index = pd.DataFrame(
            {2023: [100.0, np.nan], 2024: [110.0, 200.0]},
            index=sectors,
        )

        with patch(
            "bedrock.utils.validation.diagnostics_helpers.obtain_inflation_factors_from_reference_data",
            return_value=price_index,
        ):
            result = inflation_adjust_ef_denom_to_new_base_year(
                old_ef_vector=old_ef,
                new_base_year=2024,
                old_base_year=2023,
            )

        assert result["1111A0"] == pytest.approx(10.0 * 100.0 / 110.0)
        # NaN in old year → ratio = NaN → fillna(1.0) → no change
        assert result["1111B0"] == pytest.approx(20.0)


class TestNowcastEffectiveXComparison:
    """Under nowcast the ``x_decomposition`` tab is Make x against BEA GO."""

    def test_make_x_against_bea_go_under_nowcast(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cfg = USAConfig(
            usa_detail_io_source='nowcast',
            usa_base_io_data_year=2023,
            model_base_year=2023,
            usa_ghg_data_year=2023,
        )
        industries = ['I1', 'I2', 'I3']
        seen: list[tuple[str, int]] = []

        def after(year: int) -> pd.Series:
            seen.append(('after', year))
            return pd.Series([100.0, 200.0, 300.0], index=industries)

        def before(year: int) -> pd.Series:
            seen.append(('before', year))
            return pd.Series([120.0, 180.0, 300.0], index=industries)

        monkeypatch.setattr(dh, 'get_usa_config', lambda: cfg)
        monkeypatch.setattr(
            dh,
            'get_aligned_sector_desc',
            lambda: {'I1': 'one', 'I2': 'two', 'I3': 'three'},
        )
        monkeypatch.setattr(
            bea, 'bea_x', lambda: pd.Series([110.0, 190.0, 300.0], index=industries)
        )
        monkeypatch.setattr(go, 'derive_gross_output_after_redefinition', after)
        monkeypatch.setattr(go, 'derive_gross_output_before_redefinition', before)

        comp = dh.compute_effective_x_comparison()

        assert seen == [('after', 2023), ('before', 2023)]
        assert list(comp.columns) == [
            'sector_name',
            'x_make_after_redef_2023',
            'x_bea_go_after_redef_2023',
            'x_bea_go_before_redef_2023',
            'x_make_over_go_after_redef',
        ]
        # Sorted by the ratio: I2 (0.95) < I3 (1.0) < I1 (1.1).
        assert list(comp.index) == ['I2', 'I3', 'I1']
        np.testing.assert_allclose(
            comp['x_make_over_go_after_redef'].to_numpy(dtype=float),
            np.array([0.95, 1.0, 1.1]),
        )
        assert comp.loc['I1', 'sector_name'] == 'one'
        assert comp.loc['I1', 'x_bea_go_before_redef_2023'] == 120.0
