# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import typing as ta

import numpy as np
import pandas as pd

from bedrock.utils.io.gcp import update_sheet_tab
from bedrock.utils.snapshots.loader import load_current_snapshot
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR_DESC
from bedrock.utils.validation.diagnostics_helpers import (
    calculate_summary_stats_for_ef_diff_dataframe,
    construct_ef_diff_dataframe,
)

logger = logging.getLogger(__name__)


def calculate_ef_diagnostics(sheet_id: str) -> None:
    """Calculate EF diagnostics for the US portion of Cornerstone's MRIO model.

    Compares current emission factors (EFs) against a previous snapshot release,
    writing results to a Google Sheet. The following tabs are produced:

    - N_and_diffs: Total EFs new vs old, with absolute and percent diffs.
    - D_and_diffs: Direct EFs new vs old.
    - N_and_D_summary_stats: Summary statistics of percent diffs.
    - output_contrib_new_vs_old: Top N contributing sectors to each EF's change,
      derived from the output contribution matrix.

    Old EFs are inflation-adjusted to the current base year before comparison.
    """
    # Late-binding import - depends on global config
    from bedrock.transform.eeio.derived import derive_Aq_usa
    from bedrock.utils.math.formulas import (
        compute_L_matrix,
        compute_output_contribution,
    )
    from bedrock.utils.validation.diagnostics_helpers import pull_efs_for_diagnostics

    efs = pull_efs_for_diagnostics()

    logger.info("------ Calculating EF Diagnostics ------")

    # Compare N (total EFs) new vs old
    N_comparison = construct_ef_diff_dataframe(
        ef_name="N",
        ef_new=efs.N_new,
        ef_old=efs.N_old,
    )

    update_sheet_tab(
        sheet_id,
        "N_and_diffs",
        N_comparison.reset_index(),
    )

    # Compare D (direct EFs) new vs old
    D_comparison = construct_ef_diff_dataframe(
        ef_name="D",
        ef_new=efs.D_new,
        ef_old=efs.D_old,
    )

    update_sheet_tab(
        sheet_id,
        "D_and_diffs",
        D_comparison.reset_index(),
    )

    # Summary statistics
    N_summary = calculate_summary_stats_for_ef_diff_dataframe(
        ef_name="N",
        ef_comparison=N_comparison,
        cols_to_summarize=["N_perc_diff"],
    )

    D_summary = calculate_summary_stats_for_ef_diff_dataframe(
        ef_name="D",
        ef_comparison=D_comparison,
        cols_to_summarize=["D_perc_diff"],
    )

    update_sheet_tab(
        sheet_id,
        "N_and_D_summary_stats",
        pd.concat([N_summary, D_summary]),
    )

    # Compare output contribution
    Aq_set = derive_Aq_usa()
    L_new = compute_L_matrix(A=Aq_set.Adom + Aq_set.Aimp)

    OC_new = compute_output_contribution(
        L=L_new, D=ta.cast("pd.Series[float]", efs.D_new.squeeze())
    )

    Adom_old = load_current_snapshot("Adom_USA")
    Aimp_old = load_current_snapshot("Aimp_USA")
    L_old = compute_L_matrix(A=Adom_old + Aimp_old)

    OC_old = compute_output_contribution(
        L=L_old, D=ta.cast("pd.Series[float]", efs.D_old.inflated.squeeze())
    )

    OC_comparison = diff_and_perc_diff_two_output_contribution_matrices(
        OC_old,
        OC_new,
        old_val_name="old",
        new_val_name="new",
    )

    update_sheet_tab(
        sheet_id,
        "output_contrib_new_vs_old",
        OC_comparison,
    )


def diff_and_perc_diff_two_output_contribution_matrices(
    matrix_old: pd.DataFrame,
    matrix_new: pd.DataFrame,
    old_val_name: str,
    new_val_name: str,
    top_N: int = 5,
) -> pd.DataFrame:
    """Extract top N contributors and column sums for each EF sector.

    For each sector (column), finds the top N contributing sectors (rows)
    by absolute percentage of total difference.
    """
    assert top_N > 0, "top_N must be greater than 0"

    diff_df = matrix_new - matrix_old

    # Use numpy for performance
    matrix_old_values = matrix_old.values
    matrix_new_values = matrix_new.values
    diff_df_values = diff_df.values

    df_data = []
    for i, ef_sector in enumerate(matrix_new.columns):
        col_values_old = matrix_old_values[:, i]
        col_values_new = matrix_new_values[:, i]
        col_values_diff = diff_df_values[:, i]
        col_values_perc_diff = np.nan_to_num(
            (col_values_diff / col_values_diff.sum()), nan=0.0
        )

        col_sum_old = np.nansum(col_values_old)
        col_sum_new = np.nansum(col_values_new)

        # Use argpartition for O(n) instead of full sort
        if len(col_values_old) > top_N:
            top_indices = np.argpartition(np.abs(col_values_perc_diff), -top_N)[-top_N:]
        else:
            top_indices = np.arange(len(col_values_perc_diff))

        contributor_index = matrix_new.index
        df_data.extend(
            [
                {
                    "EF_sector": ef_sector,
                    "EF_sector_name": CEDA_V7_SECTOR_DESC[ef_sector],  # type: ignore[index]
                    "contributor_sector": contributor_index[idx],
                    "contributor_sector_name": CEDA_V7_SECTOR_DESC[
                        contributor_index[idx]
                    ],
                    f"EF_contributor_{old_val_name}": col_values_old[idx],
                    f"EF_sum_{old_val_name}": col_sum_old,
                    f"EF_contributor_{new_val_name}": col_values_new[idx],
                    f"EF_sum_{new_val_name}": col_sum_new,
                    "EF_diff": col_values_diff[idx],
                    "EF_perc_diff": col_values_perc_diff[idx],
                }
                for idx in top_indices
            ]
        )

    return pd.DataFrame(df_data)
