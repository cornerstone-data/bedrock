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
        *,
        B_old: pd.DataFrame | None = None,
        Adom_old: pd.DataFrame | None = None,
        y_old: pd.Series[float] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Run NAB diagnostics; return (BLy_vs_E_df, BLy_new_vs_old_df)."""
        B_old = B if B_old is None else B_old
        Adom_old = Adom if Adom_old is None else Adom_old
        y_old = y if y_old is None else y_old

        def fake_load_snapshot(name: str, key: str) -> pd.DataFrame | pd.Series:
            if name == "B_USA_non_finetuned":
                return B_old
            if name == "Adom_USA":
                return Adom_old
            if name == "y_nab_USA":
                return y_old
            raise AssertionError(f"unexpected snapshot {name}")

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
                "bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.load_snapshot",
                side_effect=fake_load_snapshot,
            ),
            patch(
                "bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.resolve_snapshot_key",
                return_value="test_snap_key",
            ),
            patch(
                "bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.update_sheet_tab",
                mock_update_sheet,
            ),
        ):
            calculate_national_accounting_balance_diagnostics(sheet_id="test_sheet")

        assert mock_update_sheet.call_count == 2
        calls = mock_update_sheet.call_args_list
        assert calls[0][0][1] == "BLy_and_E_orig_diffs"
        assert calls[1][0][1] == "BLy_new_vs_BLy_old"
        return calls[0][0][2], calls[1][0][2]

    def test_bly_vs_e_national_only(self) -> None:
        """BLy_and_E_orig_diffs is a single USA row."""
        idx = pd.Index(SECTORS)
        n = len(SECTORS)

        B = pd.DataFrame(np.eye(n), index=idx, columns=idx)
        Adom = pd.DataFrame(np.zeros((n, n)), index=idx, columns=idx)
        y = pd.Series([1e9, 2e9, 3e9], index=idx)

        E_orig = pd.DataFrame([[3e9, 3e9, 3e9]], index=["CO2"], columns=idx)

        e_df, bly_diff_df = self._run_diagnostics_with_mocked_data(B, Adom, y, E_orig)

        assert len(e_df) == 1
        expected_cols = {
            "index",
            "BLy (MtCO2e)",
            "E_orig (MtCO2e)",
            "BLy - E_orig (MtCO2e)",
            "(BLy - E_orig) / E_orig (%)",
        }
        assert set(e_df.columns) == expected_cols

        assert e_df["BLy (MtCO2e)"].iloc[0] == pytest.approx(6.0)
        assert e_df["E_orig (MtCO2e)"].iloc[0] == pytest.approx(9.0)
        assert e_df["BLy - E_orig (MtCO2e)"].iloc[0] == pytest.approx(-3.0)
        assert e_df["(BLy - E_orig) / E_orig (%)"].iloc[0] == pytest.approx(-1 / 3)

        assert len(bly_diff_df) == n
        bly_cols = {
            "index",
            "BLy_new (MtCO2e)",
            "BLy_old (MtCO2e)",
            "BLy_new - BLy_old (MtCO2e)",
            "(BLy_new - BLy_old) / BLy_old (%)",
        }
        assert set(bly_diff_df.columns) == bly_cols
        sector_order = list(idx.sort_values())
        assert list(bly_diff_df["index"]) == sector_order
        y_mt = y.reindex(sector_order) / 1e9
        np.testing.assert_allclose(
            np.asarray(bly_diff_df["BLy_new (MtCO2e)"], dtype=np.float64),
            y_mt.to_numpy(),
        )
        np.testing.assert_allclose(
            np.asarray(bly_diff_df["BLy_old (MtCO2e)"], dtype=np.float64),
            y_mt.to_numpy(),
        )
        assert np.allclose(
            np.asarray(bly_diff_df["BLy_new - BLy_old (MtCO2e)"], dtype=np.float64),
            0.0,
        )
        assert np.allclose(
            np.asarray(
                bly_diff_df["(BLy_new - BLy_old) / BLy_old (%)"], dtype=np.float64
            ),
            0.0,
        )

    def test_bly_new_vs_old_missing_old_sector(self) -> None:
        """Baseline BLy missing a sector: new column filled, old blank, diff uses 0 for old."""
        idx_live = pd.Index(SECTORS)
        n_live = len(idx_live)
        B = pd.DataFrame(np.eye(n_live), index=idx_live, columns=idx_live)
        Adom = pd.DataFrame(
            np.zeros((n_live, n_live)), index=idx_live, columns=idx_live
        )
        y = pd.Series([1e9, 2e9, 3e9], index=idx_live)

        idx_old = pd.Index(["1111A0", "1111B0"])
        n_old = len(idx_old)
        B_old = pd.DataFrame(np.eye(n_old), index=idx_old, columns=idx_old)
        Adom_old = pd.DataFrame(
            np.zeros((n_old, n_old)), index=idx_old, columns=idx_old
        )
        y_old = pd.Series([1e9, 2e9], index=idx_old)

        E_orig = pd.DataFrame([[3e9, 3e9, 3e9]], index=["CO2"], columns=idx_live)

        _e_df, detail = self._run_diagnostics_with_mocked_data(
            B,
            Adom,
            y,
            E_orig,
            B_old=B_old,
            Adom_old=Adom_old,
            y_old=y_old,
        )

        assert list(detail["index"]) == ["1111A0", "1111B0", "221100"]

        assert detail["BLy_new (MtCO2e)"].iloc[0] == pytest.approx(1.0)
        assert detail["BLy_old (MtCO2e)"].iloc[0] == pytest.approx(1.0)
        assert detail["BLy_new - BLy_old (MtCO2e)"].iloc[0] == pytest.approx(0.0)

        assert detail["BLy_new (MtCO2e)"].iloc[2] == pytest.approx(3.0)
        assert pd.isna(detail["BLy_old (MtCO2e)"].iloc[2])
        assert detail["BLy_new - BLy_old (MtCO2e)"].iloc[2] == pytest.approx(3.0)
        assert pd.isna(detail["(BLy_new - BLy_old) / BLy_old (%)"].iloc[2])
