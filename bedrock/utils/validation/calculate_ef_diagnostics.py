# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import time
import typing as ta

import numpy as np
import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import update_sheet_tab
from bedrock.utils.snapshots.loader import load_current_snapshot
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR_DESC
from bedrock.utils.validation.diagnostics_helpers import (
    align_efs_across_schemas,
    calculate_summary_stats_for_ef_diff_dataframe,
    construct_ef_diff_dataframe,
    get_aligned_sector_desc,
)
from bedrock.utils.validation.significant_sectors import SIGNIFICANT_SECTORS

logger = logging.getLogger(__name__)


def _add_comparison_type_column(
    df: pd.DataFrame,
    mapped_sectors: ta.Dict[str, str],
) -> pd.DataFrame:
    """Append a ``comparison_type`` column indicating how each row was compared.

    Direct (1:1) sectors are labelled ``"direct"``; mapped sectors use the
    label from *mapped_sectors*.
    """
    df['comparison_type'] = df.index.map(
        lambda code: mapped_sectors.get(code, 'direct')
    )
    return df


def _build_sector_mapping_notes(
    mapped_sectors: ta.Dict[str, str],
    old_ef: pd.DataFrame,
    new_ef: pd.DataFrame,
) -> pd.DataFrame:
    """Build a small DataFrame documenting which sectors were mapped or excluded."""
    rows: ta.List[ta.Dict[str, str]] = []
    for code, comparison_type in mapped_sectors.items():
        rows.append({'sector': code, 'comparison_type': comparison_type})
    # Report new-only sectors (present in new but absent from old and not mapped)
    old_idx = set(old_ef.index)
    new_idx = set(new_ef.index)
    new_only = sorted(new_idx - old_idx - set(mapped_sectors.keys()))
    for code in new_only:
        rows.append(
            {'sector': code, 'comparison_type': 'excluded (new-only, no baseline)'}
        )
    return pd.DataFrame(rows)


def calculate_ef_diagnostics(sheet_id: str) -> None:
    """Calculate EF diagnostics for the US portion of Cornerstone's MRIO model.

    Compares current emission factors (EFs) against a previous snapshot release,
    writing results to a Google Sheet. The following tabs are produced:

    - N_and_diffs: Total EFs new vs old, with absolute and percent diffs.
    - D_and_diffs: Direct EFs new vs old.
    - D_and_N_significant_sectors: Combined D and N comparisons for significant sectors.
    - N_and_D_summary_stats: Summary statistics of percent diffs.
    - output_contrib_new_vs_old: Top N contributing sectors to each EF's change,
      derived from the output contribution matrix.
    - sector_mapping_notes (cornerstone only): Documents mapped/excluded sectors.

    Old EFs are inflation-adjusted to the current base year before comparison.

    When ``use_cornerstone_2026_model_schema`` is active, old (CEDA v7) and new
    (cornerstone) EF vectors are aligned before comparison so that sectors with
    different granularity are still comparable.

    Args:
        sheet_id: Google Sheets spreadsheet ID to write results to.
    """
    # Late-binding import - depends on global config
    from bedrock.transform.eeio.derived import derive_Aq_usa
    from bedrock.utils.math.formulas import (
        compute_L_matrix,
        compute_output_contribution,
    )
    from bedrock.utils.validation.diagnostics_helpers import pull_efs_for_diagnostics

    config = get_usa_config()
    use_cornerstone = config.use_cornerstone_2026_model_schema

    t0 = time.time()
    efs_raw = pull_efs_for_diagnostics()
    logger.info(
        f'[TIMING] pull_efs_for_diagnostics completed in {time.time() - t0:.1f}s'
    )

    # When the cornerstone schema is active, align old/new sector indices
    active_mappings: ta.Dict[str, str] = {}
    if use_cornerstone:
        logger.info('Aligning EF vectors across CEDA v7 / cornerstone schemas')
        efs, active_mappings = align_efs_across_schemas(efs_raw)
        sector_desc: ta.Optional[ta.Dict[str, str]] = get_aligned_sector_desc()
    else:
        efs = efs_raw
        sector_desc = None  # use default CEDA_V7_SECTOR_DESC

    logger.info('------ Calculating EF Diagnostics ------')

    # Compare N (total EFs) new vs old
    N_comparison = construct_ef_diff_dataframe(
        ef_name='N',
        ef_new=efs.N_new,
        ef_old=efs.N_old,
        sector_desc=sector_desc,
    )

    if use_cornerstone:
        _add_comparison_type_column(N_comparison, active_mappings)

    t0 = time.time()
    update_sheet_tab(
        sheet_id,
        'N_and_diffs',
        N_comparison.reset_index(),
        clean_nans=True,
    )
    logger.info(
        f'[TIMING] Write N_and_diffs to Google Sheets in {time.time() - t0:.1f}s'
    )

    # Compare D (direct EFs) new vs old
    D_comparison = construct_ef_diff_dataframe(
        ef_name='D',
        ef_new=efs.D_new,
        ef_old=efs.D_old,
        sector_desc=sector_desc,
    )

    if use_cornerstone:
        _add_comparison_type_column(D_comparison, active_mappings)

    t0 = time.time()
    update_sheet_tab(
        sheet_id,
        'D_and_diffs',
        D_comparison.reset_index(),
        clean_nans=True,
    )
    logger.info(
        f'[TIMING] Write D_and_diffs to Google Sheets in {time.time() - t0:.1f}s'
    )

    # Effective g decomposition (Cornerstone method only)
    if config.transform_b_matrix_with_useeio_method:
        from bedrock.utils.validation.diagnostics_helpers import (
            compute_effective_g_comparison,
        )

        t0 = time.time()
        g_comparison = compute_effective_g_comparison()
        update_sheet_tab(
            sheet_id, 'g_decomposition', g_comparison.reset_index(), clean_nans=True
        )
        logger.info(
            f'[TIMING] Write g_decomposition to Google Sheets in {time.time() - t0:.1f}s'
        )

    # Compare D and N for significant sectors
    significant_sectors = [sector['sector'] for sector in SIGNIFICANT_SECTORS]
    # When aligned, some significant sectors may not be in the index (e.g. if
    # they were removed).  Filter to those present.
    available_significant = [s for s in significant_sectors if s in D_comparison.index]
    drop_cols = ['sector_name']
    if use_cornerstone:
        drop_cols.append('comparison_type')
    significant_sectors_comparison = D_comparison.loc[available_significant].join(
        N_comparison.loc[available_significant].drop(columns=drop_cols)
    )
    update_sheet_tab(
        sheet_id,
        'D_and_N_significant_sectors',
        significant_sectors_comparison.reset_index(),
        clean_nans=True,
    )

    # Summary statistics
    N_summary = calculate_summary_stats_for_ef_diff_dataframe(
        ef_name='N',
        ef_comparison=N_comparison,
        cols_to_summarize=['N_perc_diff'],
    )

    D_summary = calculate_summary_stats_for_ef_diff_dataframe(
        ef_name='D',
        ef_comparison=D_comparison,
        cols_to_summarize=['D_perc_diff'],
    )

    t0 = time.time()
    update_sheet_tab(
        sheet_id,
        'N_and_D_summary_stats',
        pd.concat([N_summary, D_summary]),
        clean_nans=True,
    )
    logger.info(
        f'[TIMING] Write N_and_D_summary_stats to Google Sheets in {time.time() - t0:.1f}s'
    )

    # Sector mapping notes (cornerstone only)
    if use_cornerstone:
        mapping_notes = _build_sector_mapping_notes(
            active_mappings,
            old_ef=efs_raw.D_old.raw,
            new_ef=efs_raw.D_new,
        )
        update_sheet_tab(
            sheet_id, 'sector_mapping_notes', mapping_notes, clean_nans=True
        )
        logger.info('Wrote sector_mapping_notes tab')

    # Compare output contribution
    t0 = time.time()
    Aq_set = derive_Aq_usa()
    L_new = compute_L_matrix(A=Aq_set.Adom + Aq_set.Aimp)

    OC_new = compute_output_contribution(
        L=L_new, D=ta.cast('pd.Series[float]', efs_raw.D_new.squeeze())
    )

    Adom_old = load_current_snapshot('Adom_USA')
    Aimp_old = load_current_snapshot('Aimp_USA')
    L_old = compute_L_matrix(A=Adom_old + Aimp_old)

    OC_old = compute_output_contribution(
        L=L_old, D=ta.cast('pd.Series[float]', efs_raw.D_old.inflated.squeeze())
    )

    if use_cornerstone:
        full_idx = OC_new.index.union(OC_old.index).sort_values()
        full_cols = OC_new.columns.union(OC_old.columns).sort_values()
        OC_new = OC_new.reindex(index=full_idx, columns=full_cols, fill_value=0.0)
        OC_old = OC_old.reindex(index=full_idx, columns=full_cols, fill_value=0.0)

    OC_comparison = diff_and_perc_diff_two_output_contribution_matrices(
        OC_old,
        OC_new,
        old_val_name='old',
        new_val_name='new',
        sector_desc=sector_desc,
    )
    logger.info(f'[TIMING] Output contribution computed in {time.time() - t0:.1f}s')

    t0 = time.time()
    update_sheet_tab(
        sheet_id,
        'output_contrib_new_vs_old',
        OC_comparison,
        clean_nans=True,
    )
    logger.info(
        f'[TIMING] Write output_contrib to Google Sheets in {time.time() - t0:.1f}s'
    )


def diff_and_perc_diff_two_output_contribution_matrices(
    matrix_old: pd.DataFrame,
    matrix_new: pd.DataFrame,
    old_val_name: str,
    new_val_name: str,
    top_N: int = 5,
    sector_desc: ta.Optional[ta.Dict[str, str]] = None,
) -> pd.DataFrame:
    """Extract top N contributors and column sums for each EF sector.

    For each sector (column), finds the top N contributing sectors (rows)
    by absolute percentage of total difference.
    """
    assert top_N > 0, 'top_N must be greater than 0'

    _desc: ta.Dict[str, str] = sector_desc or ta.cast(
        ta.Dict[str, str], CEDA_V7_SECTOR_DESC
    )
    matrix_old = matrix_old.reindex(
        index=matrix_new.index, columns=matrix_new.columns, fill_value=0.0
    )
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

        col_sum_old = np.nansum(col_values_old)
        col_sum_new = np.nansum(col_values_new)

        diff_sum = col_values_diff.sum()
        diff_is_noise = col_sum_new == 0 or np.abs(diff_sum / col_sum_new) < 1e-12

        if diff_is_noise:
            # Diffs are floating-point noise — rank by absolute contribution
            col_values_perc_diff = np.where(
                col_sum_new != 0, col_values_new / col_sum_new, 0.0
            )
        else:
            col_values_perc_diff = np.nan_to_num((col_values_diff / diff_sum), nan=0.0)

        if len(col_values_old) > top_N:
            top_unsorted = np.argpartition(np.abs(col_values_perc_diff), -top_N)[
                -top_N:
            ]
            top_indices = top_unsorted[
                np.argsort(-np.abs(col_values_perc_diff[top_unsorted]))
            ]
        else:
            top_indices = np.argsort(-np.abs(col_values_perc_diff))

        contributor_index = matrix_new.index
        df_data.extend(
            [
                {
                    'EF_sector': ef_sector,
                    'EF_sector_name': _desc.get(ef_sector, ef_sector),
                    'contributor_sector': contributor_index[idx],
                    'contributor_sector_name': _desc.get(
                        contributor_index[idx], contributor_index[idx]
                    ),
                    f'EF_contributor_{old_val_name}': col_values_old[idx],
                    f'EF_sum_{old_val_name}': col_sum_old,
                    f'EF_contributor_{new_val_name}': col_values_new[idx],
                    f'EF_sum_{new_val_name}': col_sum_new,
                    'EF_diff': col_values_diff[idx],
                    'EF_perc_diff': col_values_perc_diff[idx],
                }
                for idx in top_indices
            ]
        )

    return pd.DataFrame(df_data)
