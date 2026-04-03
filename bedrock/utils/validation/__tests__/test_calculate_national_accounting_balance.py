"""Unit tests for national accounting balance diagnostics."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
    calculate_national_accounting_balance_diagnostics,
)

SECTORS = ["1111A0", "1111B0", "221100"]


class TestCalculateNationalAccountingBalanceDiagnostics:
    """Test the NAB diagnostics function with mocked data sources.

    Only external dependencies (derived matrices, snapshots, Google Sheets)
    are mocked. The actual math (compute_d, compute_L_matrix) runs against
    the synthetic data so we verify the real computation pipeline.
    """

    def _run_diagnostics_with_mocked_data(
        self,
        B: pd.DataFrame,
        Adom: pd.DataFrame,
        y: pd.Series[float],
        E_orig: pd.DataFrame,
    ) -> pd.DataFrame:
        """Helper to run the NAB function with controlled inputs.

        Returns the DataFrame that would be written to Google Sheets.
        """
        mock_update_sheet = MagicMock()
        mock_Aq = MagicMock()
        mock_Aq.Adom = Adom

        with (
            patch(
                "bedrock.transform.eeio.derived.derive_B_usa_non_finetuned",
                return_value=B,
            ),
            patch(
                "bedrock.transform.eeio.derived.derive_Aq_usa",
                return_value=mock_Aq,
            ),
            patch(
                "bedrock.transform.eeio.derived.derive_y_for_national_accounting_balance_usa",
                return_value=y,
            ),
            patch(
                "bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.load_configured_snapshot",
                return_value=E_orig,
            ),
            patch(
                "bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.update_sheet_tab",
                mock_update_sheet,
            ),
        ):
            calculate_national_accounting_balance_diagnostics(sheet_id="test_sheet")

        written_df: pd.DataFrame = mock_update_sheet.call_args[0][2]
        return written_df

    def test_output_structure_and_units(self) -> None:
        """USA summary unchanged; blank row; per-sector BLy_i and E_orig_i."""
        idx = pd.Index(SECTORS)
        n = len(SECTORS)

        # With B=I and A=0: d=[1,1,1], L=I, BLy = y
        B = pd.DataFrame(np.eye(n), index=idx, columns=idx)
        Adom = pd.DataFrame(np.zeros((n, n)), index=idx, columns=idx)
        y = pd.Series([1e9, 2e9, 3e9], index=idx)

        # BLy_total = 6e9 kgCO2e = 6 MtCO2e
        # E_orig_total = 9e9 kgCO2e = 9 MtCO2e
        E_orig = pd.DataFrame([[3e9, 3e9, 3e9]], index=["CO2"], columns=idx)

        result = self._run_diagnostics_with_mocked_data(B, Adom, y, E_orig)

        assert len(result) == 1 + 1 + n
        expected_cols = {
            "index",
            "BLy (MtCO2e)",
            "E_orig (MtCO2e)",
            "BLy - E_orig (MtCO2e)",
            "(BLy - E_orig) / E_orig (%)",
        }
        assert set(result.columns) == expected_cols

        assert result["BLy (MtCO2e)"].iloc[0] == pytest.approx(6.0)
        assert result["E_orig (MtCO2e)"].iloc[0] == pytest.approx(9.0)
        assert result["BLy - E_orig (MtCO2e)"].iloc[0] == pytest.approx(-3.0)
        assert result["(BLy - E_orig) / E_orig (%)"].iloc[0] == pytest.approx(-1 / 3)

        sep = result.iloc[1]
        assert sep["index"] == ""
        for col in (
            "BLy (MtCO2e)",
            "E_orig (MtCO2e)",
            "BLy - E_orig (MtCO2e)",
            "(BLy - E_orig) / E_orig (%)",
        ):
            assert pd.isna(sep[col])

        detail = result.iloc[2:]
        sector_order = list(idx.sort_values())
        assert list(detail["index"]) == sector_order
        y_mt = y.reindex(sector_order) / 1e9
        e_mt = E_orig.sum(axis=0).reindex(sector_order) / 1e9
        pd.testing.assert_series_equal(
            detail["BLy (MtCO2e)"].reset_index(drop=True),
            y_mt.reset_index(drop=True),
            check_names=False,
        )
        pd.testing.assert_series_equal(
            detail["E_orig (MtCO2e)"].reset_index(drop=True),
            e_mt.reset_index(drop=True),
            check_names=False,
        )
        pd.testing.assert_series_equal(
            detail["BLy - E_orig (MtCO2e)"].reset_index(drop=True),
            (y_mt - e_mt).reset_index(drop=True),
            check_names=False,
        )
        e_arr = e_mt.to_numpy(dtype=float)
        d_arr = (y_mt - e_mt).to_numpy(dtype=float)
        exp_pct = np.zeros(len(sector_order), dtype=float)
        valid = np.isfinite(e_arr) & (e_arr != 0)
        exp_pct[valid] = d_arr[valid] / e_arr[valid]
        np.testing.assert_allclose(
            np.asarray(detail["(BLy - E_orig) / E_orig (%)"], dtype=np.float64),
            exp_pct,
        )

    def test_sector_diff_treats_missing_side_as_zero(self) -> None:
        """BLy/E_orig stay blank when missing; diff uses 0 for missing side."""
        idx_bly = pd.Index(["1111A0", "1111B0"])
        n = len(idx_bly)
        B = pd.DataFrame(np.eye(n), index=idx_bly, columns=idx_bly)
        Adom = pd.DataFrame(np.zeros((n, n)), index=idx_bly, columns=idx_bly)
        y = pd.Series([1e9, 2e9], index=idx_bly)
        E_cols = pd.Index(["1111B0", "221100"])
        E_orig = pd.DataFrame([[2e9, 3e9]], index=["CO2"], columns=E_cols)

        result = self._run_diagnostics_with_mocked_data(B, Adom, y, E_orig)

        detail = result.iloc[2:]
        assert list(detail["index"]) == ["1111A0", "1111B0", "221100"]

        assert detail["BLy (MtCO2e)"].iloc[0] == pytest.approx(1.0)
        assert pd.isna(detail["E_orig (MtCO2e)"].iloc[0])
        assert detail["BLy - E_orig (MtCO2e)"].iloc[0] == pytest.approx(1.0)
        assert pd.isna(detail["(BLy - E_orig) / E_orig (%)"].iloc[0])

        assert detail["BLy (MtCO2e)"].iloc[1] == pytest.approx(2.0)
        assert detail["E_orig (MtCO2e)"].iloc[1] == pytest.approx(2.0)
        assert detail["BLy - E_orig (MtCO2e)"].iloc[1] == pytest.approx(0.0)
        assert detail["(BLy - E_orig) / E_orig (%)"].iloc[1] == pytest.approx(0.0)

        assert pd.isna(detail["BLy (MtCO2e)"].iloc[2])
        assert detail["E_orig (MtCO2e)"].iloc[2] == pytest.approx(3.0)
        assert detail["BLy - E_orig (MtCO2e)"].iloc[2] == pytest.approx(-3.0)
        assert detail["(BLy - E_orig) / E_orig (%)"].iloc[2] == pytest.approx(-1.0)
