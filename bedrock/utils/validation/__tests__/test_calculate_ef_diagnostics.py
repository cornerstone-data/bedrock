"""Unit tests for EF diagnostics."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from bedrock.utils.validation.calculate_ef_diagnostics import (
    calculate_ef_diagnostics,
    diff_and_perc_diff_two_output_contribution_matrices,
)
from bedrock.utils.validation.diagnostics_helpers import EfsForDiagnostics, OldEfSet

SECTORS = ["1111A0", "1111B0", "221100"]


class TestCalculateEfDiagnostics:
    """Test the EF diagnostics orchestrator with mocked data sources.

    Only external dependencies (pull_efs_for_diagnostics, derive_Aq_usa,
    load_current_snapshot, update_sheet_tab) are mocked. The actual math
    (construct_ef_diff_dataframe, compute_L_matrix, compute_output_contribution,
    etc.) runs against synthetic data so we verify the real pipeline.
    """

    def _build_efs(self, idx: pd.Index) -> EfsForDiagnostics:
        """Build a small EfsForDiagnostics with known values."""
        D_new = pd.DataFrame({"ef": [1.0, 2.0, 3.0]}, index=idx)
        N_new = pd.DataFrame({"ef": [1.5, 3.0, 4.5]}, index=idx)
        D_old = OldEfSet(
            raw=pd.DataFrame({"ef": [0.9, 2.1, 2.8]}, index=idx),
            inflated=pd.DataFrame({"ef": [0.95, 2.05, 2.85]}, index=idx),
        )
        N_old = OldEfSet(
            raw=pd.DataFrame({"ef": [1.4, 3.2, 4.3]}, index=idx),
            inflated=pd.DataFrame({"ef": [1.45, 3.1, 4.35]}, index=idx),
        )
        return EfsForDiagnostics(D_new=D_new, N_new=N_new, D_old=D_old, N_old=N_old)

    def _run_ef_diagnostics(self) -> list[tuple[str, str, pd.DataFrame]]:
        """Run calculate_ef_diagnostics with mocked dependencies.

        Returns a list of (sheet_id, tab_name, data) tuples for each
        update_sheet_tab call.
        """
        idx = pd.Index(SECTORS)
        n = len(SECTORS)
        efs = self._build_efs(idx)

        mock_update_sheet = MagicMock()
        mock_Aq = MagicMock()
        mock_Aq.Adom = pd.DataFrame(np.eye(n) * 0.05, index=idx, columns=idx)
        mock_Aq.Aimp = pd.DataFrame(np.eye(n) * 0.02, index=idx, columns=idx)

        def mock_load(name: str) -> pd.DataFrame:
            if name == "Adom_USA":
                return pd.DataFrame(np.eye(n) * 0.04, index=idx, columns=idx)
            if name == "Aimp_USA":
                return pd.DataFrame(np.eye(n) * 0.01, index=idx, columns=idx)
            raise ValueError(f"Unexpected snapshot: {name}")

        # Mock SIGNIFICANT_SECTORS to use test sectors that actually exist
        mock_significant_sectors = [
            {"sector": "1111A0"},
            {"sector": "1111B0"},
        ]

        with (
            patch(
                "bedrock.utils.validation.diagnostics_helpers.pull_efs_for_diagnostics",
                return_value=efs,
            ),
            patch(
                "bedrock.transform.eeio.derived.derive_Aq_usa",
                return_value=mock_Aq,
            ),
            patch(
                "bedrock.utils.validation.calculate_ef_diagnostics.load_current_snapshot",
                side_effect=mock_load,
            ),
            patch(
                "bedrock.utils.validation.calculate_ef_diagnostics.update_sheet_tab",
                mock_update_sheet,
            ),
            patch(
                "bedrock.utils.validation.calculate_ef_diagnostics.SIGNIFICANT_SECTORS",
                mock_significant_sectors,
            ),
        ):
            calculate_ef_diagnostics(sheet_id="test_sheet")

        return [
            (c.args[0], c.args[1], c.args[2]) for c in mock_update_sheet.call_args_list
        ]

    def test_tab_names_are_correct(self) -> None:
        tabs = self._run_ef_diagnostics()
        tab_names = [t[1] for t in tabs]

        assert tab_names == [
            "N_and_diffs",
            "D_and_diffs",
            "D_and_N_significant_sectors",
            "N_and_D_summary_stats",
            "output_contrib_new_vs_old",
        ]

    def test_n_and_diffs_has_expected_structure(self) -> None:
        tabs = self._run_ef_diagnostics()
        n_diffs = tabs[0][2]

        assert "N_new" in n_diffs.columns
        assert "N_old_inflated" in n_diffs.columns
        assert "N_old" in n_diffs.columns
        assert "N_perc_diff" in n_diffs.columns
        assert len(n_diffs) == len(SECTORS)

    def test_significant_sectors_has_both_d_and_n(self) -> None:
        tabs = self._run_ef_diagnostics()
        sig_sectors = tabs[2][2]

        # Should have D columns and N columns combined
        assert "D_new" in sig_sectors.columns
        assert "D_old_inflated" in sig_sectors.columns
        assert "D_perc_diff" in sig_sectors.columns
        assert "N_new" in sig_sectors.columns
        assert "N_old_inflated" in sig_sectors.columns
        assert "N_perc_diff" in sig_sectors.columns
        # Only significant sectors (2 in our mock)
        assert len(sig_sectors) == 2

    def test_output_contribution_has_expected_columns(self) -> None:
        tabs = self._run_ef_diagnostics()
        oc = tabs[4][2]

        expected_cols = {
            "EF_sector",
            "EF_sector_name",
            "contributor_sector",
            "contributor_sector_name",
            "EF_contributor_old",
            "EF_sum_old",
            "EF_contributor_new",
            "EF_sum_new",
            "EF_diff",
            "EF_perc_diff",
        }
        assert set(oc.columns) == expected_cols


class TestDiffAndPercDiffTwoOutputContributionMatrices:
    def _build_oc_matrices(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        idx = pd.Index(SECTORS)
        old = pd.DataFrame(
            np.array([[0.50, 0.10, 0.20], [0.30, 0.60, 0.10], [0.20, 0.30, 0.70]]),
            index=idx,
            columns=idx,
        )
        new = pd.DataFrame(
            np.array([[0.55, 0.12, 0.22], [0.28, 0.58, 0.12], [0.17, 0.30, 0.66]]),
            index=idx,
            columns=idx,
        )
        return old, new

    def test_top_N_limits_rows_per_sector(self) -> None:
        old, new = self._build_oc_matrices()

        result = diff_and_perc_diff_two_output_contribution_matrices(
            old, new, old_val_name="old", new_val_name="new", top_N=2
        )

        # 3 sectors × 2 top contributors = 6 rows
        assert len(result) == 6

    def test_near_zero_diff_avoids_floating_point_noise(self) -> None:
        """When diff sum is negligible (<1e-10), perc_diff should be exactly zero."""
        idx = pd.Index(SECTORS)
        matrix = pd.DataFrame(
            np.array([[0.5, 0.1, 0.2], [0.3, 0.6, 0.1], [0.2, 0.3, 0.7]]),
            index=idx,
            columns=idx,
        )
        perturbed = matrix + 1e-15

        result = diff_and_perc_diff_two_output_contribution_matrices(
            matrix, perturbed, old_val_name="old", new_val_name="new"
        )

        assert (result["EF_perc_diff"] == 0.0).all()

    def test_column_sums_are_correct(self) -> None:
        old, new = self._build_oc_matrices()

        result = diff_and_perc_diff_two_output_contribution_matrices(
            old, new, old_val_name="old", new_val_name="new", top_N=10
        )

        for sector in SECTORS:
            sector_rows = result[result["EF_sector"] == sector]

            assert sector_rows["EF_sum_old"].nunique() == 1
            assert sector_rows["EF_sum_new"].nunique() == 1

            assert sector_rows["EF_sum_old"].iloc[0] == pytest.approx(old[sector].sum())
            assert sector_rows["EF_sum_new"].iloc[0] == pytest.approx(new[sector].sum())

    def test_perc_diff_sums_to_one_when_diff_nonzero(self) -> None:
        """When all contributors are included and diff is nonzero, perc_diff sums to ~1."""
        old, new = self._build_oc_matrices()

        result = diff_and_perc_diff_two_output_contribution_matrices(
            old, new, old_val_name="old", new_val_name="new", top_N=10
        )

        for sector in SECTORS:
            sector_rows = result[result["EF_sector"] == sector]
            total_diff = sector_rows["EF_diff"].sum()
            if abs(total_diff) > 1e-10:
                assert sector_rows["EF_perc_diff"].sum() == pytest.approx(1.0, abs=1e-8)
