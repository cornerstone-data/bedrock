# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import time
import typing as ta

import numpy as np
import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import update_sheet_tab
from bedrock.utils.snapshots.loader import load_configured_snapshot
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR_DESC
from bedrock.utils.validation.diagnostics_helpers import (
    align_efs_across_schemas,
    calculate_summary_stats_for_ef_diff_dataframe,
    construct_ef_diff_dataframe,
    get_aligned_sector_desc,
)
from bedrock.utils.validation.significant_sectors import SIGNIFICANT_SECTORS

logger = logging.getLogger(__name__)


def _ef_vector_as_series(single_col_df: pd.DataFrame) -> pd.Series[float]:
    return ta.cast('pd.Series[float]', single_col_df.iloc[:, 0])


def _vector_perc_diff(new: pd.Series[float], old: pd.Series[float]) -> pd.Series[float]:
    """Match :func:`diff_and_perc_diff_two_vectors` percent branch (old in denominator)."""
    diff = new - old
    return (
        (diff / old.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    )


def _merge_ef_new_inflated_into_comparison(
    comparison: pd.DataFrame,
    inflated_new: pd.DataFrame,
    *,
    ef_name: str,
) -> pd.DataFrame:
    """Place ``{ef}_new_inflated`` before ``{ef}_new``; ``{ef}_perc_diff`` uses inflated vs old."""
    new_col = f'{ef_name}_new'
    old_inflated_col = f'{ef_name}_old_inflated'
    inflated_col = f'{ef_name}_new_inflated'
    perc_col = f'{ef_name}_perc_diff'

    ser = _ef_vector_as_series(inflated_new).reindex(comparison.index)
    old_infl = ta.cast('pd.Series[float]', comparison[old_inflated_col])

    out = comparison.copy()
    out[inflated_col] = ser
    out[perc_col] = _vector_perc_diff(ser, old_infl)

    cols = comparison.columns.tolist()
    insert_at = cols.index(new_col)
    ordered = cols[:insert_at] + [inflated_col] + cols[insert_at:]
    return out[ordered]


def _merge_ef_new_purchaser_into_comparison(
    comparison: pd.DataFrame,
    purchaser_new: pd.DataFrame,
    *,
    ef_name: str,
    purchaser_old: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Insert purchaser columns; ``{ef}_perc_diff`` uses new vs old purchaser price."""
    new_col = f'{ef_name}_new'
    inflated_col = f'{ef_name}_new_inflated'
    old_inflated_col = f'{ef_name}_old_inflated'
    purchaser_new_col = f'{ef_name}_new_purchaser'
    purchaser_old_col = f'{ef_name}_old_purchaser'
    perc_col = f'{ef_name}_perc_diff'

    new_ser = _ef_vector_as_series(purchaser_new).reindex(comparison.index)
    if purchaser_old is not None:
        old_ser = _ef_vector_as_series(purchaser_old).reindex(comparison.index)
    else:
        old_ser = ta.cast('pd.Series[float]', comparison[old_inflated_col])

    out = comparison.copy()
    out[purchaser_new_col] = new_ser
    if purchaser_old is not None:
        out[purchaser_old_col] = old_ser
    out[perc_col] = _vector_perc_diff(new_ser, old_ser)

    cols = comparison.columns.tolist()
    if inflated_col in cols:
        insert_at = cols.index(inflated_col)
    else:
        insert_at = cols.index(new_col)
    ordered = cols[:insert_at] + [purchaser_new_col] + cols[insert_at:]
    if purchaser_old is not None:
        old_insert_at = ordered.index(old_inflated_col)
        ordered = (
            ordered[:old_insert_at] + [purchaser_old_col] + ordered[old_insert_at:]
        )
    return out[ordered]


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

    - N_and_diffs: Total EFs new vs old. When eligible, inserts ``N_new_inflated``
      before ``N_new`` and sets ``N_perc_diff`` from ``N_new_inflated`` vs
      ``N_old_inflated``. Omitted when ``model_base_year`` equals
      ``usa_detail_original_year`` (identity adjustment).
    - D_and_diffs: Direct EFs new vs old; same ``D_new_inflated`` / ``D_perc_diff``
      rules as ``N_and_diffs`` when eligible.
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
        compute_d,
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
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

    # Compare D (direct EFs) new vs old
    D_comparison = construct_ef_diff_dataframe(
        ef_name='D',
        ef_new=efs.D_new,
        ef_old=efs.D_old,
        sector_desc=sector_desc,
    )

    if use_cornerstone:
        _add_comparison_type_column(N_comparison, active_mappings)
        _add_comparison_type_column(D_comparison, active_mappings)

    if efs.D_new_inflated is not None:
        assert efs.N_new_inflated is not None
        t0 = time.time()
        N_comparison = _merge_ef_new_inflated_into_comparison(
            N_comparison,
            efs.N_new_inflated,
            ef_name='N',
        )
        D_comparison = _merge_ef_new_inflated_into_comparison(
            D_comparison,
            efs.D_new_inflated,
            ef_name='D',
        )
        logger.info(
            '[TIMING] attach D_new_inflated/N_new_inflated columns to EF tabs in %.1fs',
            time.time() - t0,
        )

    if efs.N_new_purchaser is not None:
        t0 = time.time()
        N_comparison = _merge_ef_new_purchaser_into_comparison(
            N_comparison,
            efs.N_new_purchaser,
            ef_name='N',
            purchaser_old=efs.N_old_purchaser,
        )
        logger.info(
            '[TIMING] attach N_new_purchaser/N_old_purchaser columns to N_and_diffs '
            'in %.1fs',
            time.time() - t0,
        )

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

    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        compute_mixed_unit_ef_vectors,
        electricity_mixed_units_enabled,
        table_2_4_prices_cents_kwh,
    )
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_B_usa_non_finetuned,
    )
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Aq_scaled,
        derive_cornerstone_B_non_finetuned,
    )
    from bedrock.utils.validation.diagnostics_helpers import (  # noqa: PLC0415
        MIXED_VS_MONETARY_TAB_COLUMNS,
        build_mixed_vs_monetary_comparison_df,
        sectors_for_mixed_vs_monetary_tab,
    )

    if electricity_mixed_units_enabled():
        t0 = time.time()
        aq_mon = derive_cornerstone_Aq_scaled()
        b_mon = derive_cornerstone_B_non_finetuned()
        b_live = derive_B_usa_non_finetuned()
        d_mon = ta.cast(
            'pd.Series[float]',
            compute_d(B=b_live).squeeze(),
        )
        l_mon = compute_L_matrix(A=aq_mon.Adom + aq_mon.Aimp)
        m_mon = compute_M_matrix(B=b_live, L=l_mon)
        n_mon = ta.cast('pd.Series[float]', compute_n(M=m_mon).squeeze())
        d_mix = _ef_vector_as_series(efs.D_new)
        n_mix = _ef_vector_as_series(efs.N_new)
        table_prices = table_2_4_prices_cents_kwh(config.usa_ghg_data_year)
        total_price = float(table_prices['Total'])
        equal_prices: dict[str, float] = {str(k): total_price for k in table_prices}
        uniform_result = compute_mixed_unit_ef_vectors(
            aq_mon, b_mon, prices_by_class=equal_prices
        )
        sectors = sectors_for_mixed_vs_monetary_tab(n_mix, n_mon)
        mixed_vs_mon = build_mixed_vs_monetary_comparison_df(
            sectors=sectors,
            D_mon=d_mon,
            N_mon=n_mon,
            D_mix=d_mix,
            N_mix=n_mix,
            N_uniform=uniform_result.N,
            c_col=uniform_result.c_col,
            sector_desc_lookup=sector_desc,
        )
        assert list(mixed_vs_mon.columns) == list(MIXED_VS_MONETARY_TAB_COLUMNS)
        update_sheet_tab(
            sheet_id,
            'mixed_vs_monetary_221110',
            mixed_vs_mon,
            clean_nans=True,
        )
        logger.info(
            '[TIMING] Write mixed_vs_monetary_221110 tab in %.1fs',
            time.time() - t0,
        )
    else:
        logger.info('Skipping mixed_vs_monetary_221110 (mixed-units gate off)')

    # Effective x decomposition (Cornerstone method only)
    if config.use_E_data_year_for_x_in_B:
        from bedrock.utils.validation.diagnostics_helpers import (
            compute_effective_x_comparison,
        )

        t0 = time.time()
        x_comparison = compute_effective_x_comparison()
        update_sheet_tab(
            sheet_id, 'x_decomposition', x_comparison.reset_index(), clean_nans=True
        )
        logger.info(
            f'[TIMING] Write x_decomposition to Google Sheets in {time.time() - t0:.1f}s'
        )

    # Compare D and N for significant sectors
    significant_sectors = [sector['sector'] for sector in SIGNIFICANT_SECTORS]
    # When aligned, some significant sectors may not be in the index (e.g. if
    # they were removed).  Filter to those present.
    available_significant = [s for s in significant_sectors if s in D_comparison.index]
    drop_cols = ['sector_name']
    if use_cornerstone:
        drop_cols.append('comparison_type')
    d_sig = D_comparison.loc[available_significant]
    n_sig = N_comparison.loc[available_significant].drop(columns=drop_cols, errors='ignore')
    # Mixed-units configs add ``exemption_reason`` to both D and N tabs; drop
    # N-side duplicates so ``join`` does not require suffixes.
    n_sig = n_sig.drop(columns=list(n_sig.columns.intersection(d_sig.columns)), errors='ignore')
    significant_sectors_comparison = d_sig.join(n_sig)
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

    N_sig_summary = calculate_summary_stats_for_ef_diff_dataframe(
        ef_name='N_significant_sectors',
        ef_comparison=significant_sectors_comparison,
        cols_to_summarize=['N_perc_diff'],
    )

    D_sig_summary = calculate_summary_stats_for_ef_diff_dataframe(
        ef_name='D_significant_sectors',
        ef_comparison=significant_sectors_comparison,
        cols_to_summarize=['D_perc_diff'],
    )

    t0 = time.time()
    update_sheet_tab(
        sheet_id,
        'N_and_D_summary_stats',
        pd.concat([N_summary, D_summary, N_sig_summary, D_sig_summary]),
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

    # Compare output contribution (parquet baseline only; omitted for gcs_useeio_xlsx)
    if config.diagnostics_baseline_source != 'gcs_useeio_xlsx':
        t0 = time.time()
        from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
            electricity_mixed_units_enabled,
        )
        from bedrock.transform.eeio.derived import derive_Aq_usa  # noqa: PLC0415
        from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
            derive_cornerstone_Aq_mixed_units,
        )

        if electricity_mixed_units_enabled():
            Aq_set = derive_cornerstone_Aq_mixed_units()
        else:
            Aq_set = derive_Aq_usa()
        L_new = compute_L_matrix(A=Aq_set.Adom + Aq_set.Aimp)

        OC_new = compute_output_contribution(
            L=L_new, D=ta.cast('pd.Series[float]', efs_raw.D_new.squeeze())
        )

        Adom_old = load_configured_snapshot('Adom_USA')
        Aimp_old = load_configured_snapshot('Aimp_USA')
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
    else:
        logger.info(
            'Skipping output_contrib_new_vs_old (USEEIO Excel baseline; OC not defined)'
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
