from __future__ import annotations

from typing import cast

import numpy as np
import pandas as pd

from bedrock.extract.disaggregation.waste_weights import WasteDisaggWeights


def _assert_non_waste_unchanged(
    input_df: pd.DataFrame,
    result_df: pd.DataFrame,
    waste_set: set[str],
    original_code: str,
) -> None:
    """Assert that cells with row/col not in waste_set and not original_code are unchanged."""
    waste_and_orig = waste_set | {original_code}
    common_idx = [
        i for i in result_df.index if i in input_df.index and i not in waste_and_orig
    ]
    common_cols = [
        j
        for j in result_df.columns
        if j in input_df.columns and j not in waste_and_orig
    ]
    if not common_idx or not common_cols:
        return
    np.testing.assert_allclose(
        result_df.loc[common_idx, common_cols].values,
        input_df.loc[common_idx, common_cols].values,
        rtol=1e-9,
        atol=1e-12,
        err_msg="Non-waste values changed",
    )


def _waste_codes(weights: WasteDisaggWeights) -> list[str]:
    """Return the list of waste subsector codes from the intersection table columns."""
    return list(weights.make_intersection.columns)


def _original_code(weights: WasteDisaggWeights) -> str | None:
    """Infer the original aggregate code (e.g. '562000') from the weight tables.

    The original code appears as a row index in make_waste_commodity_columns_all_rows
    but is NOT one of the waste subsector codes.
    """
    waste = set(_waste_codes(weights))
    tbl = weights.make_waste_commodity_columns_all_rows
    for code in tbl.index:
        if code not in waste:
            return str(code)
    tbl2 = weights.use_waste_industry_columns_all_rows
    for code in tbl2.index:
        if code not in waste:
            return str(code)
    return None


def _aggregate_waste_sector_in_V(
    V: pd.DataFrame,
    waste_codes: list[str],
    original_code: str,
) -> pd.DataFrame:
    """Collapse waste_codes rows and columns in V into a single row/column (original_code).

    Returns a new DataFrame with index/columns (non-waste + original_code). Used when
    V already has waste subsectors so we can re-aggregate then re-disaggregate with weights.
    """
    waste_set = set(waste_codes)
    non_waste_idx = [i for i in V.index if i not in waste_set]
    non_waste_cols = [j for j in V.columns if j not in waste_set]
    new_index = non_waste_idx + [original_code]
    new_columns = non_waste_cols + [original_code]
    output = pd.DataFrame(0.0, index=new_index, columns=new_columns, dtype=float)
    output.loc[non_waste_idx, non_waste_cols] = V.loc[
        non_waste_idx, non_waste_cols
    ].values
    output.loc[original_code, non_waste_cols] = (
        V.loc[waste_codes, non_waste_cols].sum(axis=0).values
    )
    output.loc[non_waste_idx, original_code] = (
        V.loc[non_waste_idx, waste_codes].sum(axis=1).values
    )
    output.loc[original_code, original_code] = (
        V.loc[waste_codes, waste_codes].sum().sum()
    )
    return output


def apply_waste_disagg_to_V(
    V: pd.DataFrame,
    weights: WasteDisaggWeights,
    original_code: str = "562000",
) -> pd.DataFrame:
    """Disaggregate the 562 waste sector in Make matrix V.

    Mirrors useeior's specifiedMakeDisagg():
    - Intersection: the (original_code, original_code) cell is split into
      a (waste_industry x waste_commodity) block using make_intersection weights.
    - Columns: for each non-waste industry row i, V[i, original_code] is split
      across waste commodity columns using make_waste_commodity_columns_all_rows
      (or make_waste_commodity_columns_specific_rows if a row-specific weight exists).
    - Rows: for each non-waste commodity column j, V[original_code, j] is split
      across waste industry rows using make_waste_industry_rows_specific_columns
      (or uniform if no specific weight exists).
    """
    waste_codes = _waste_codes(weights)
    waste_set = set(waste_codes)

    if original_code not in V.index or original_code not in V.columns:
        # Re-aggregate waste subsectors into original_code, then disaggregate with weights
        if not waste_set.issubset(V.index) or not waste_set.issubset(V.columns):
            return V
        V_aggregated = _aggregate_waste_sector_in_V(V, waste_codes, original_code)
        result = apply_waste_disagg_to_V(V_aggregated, weights, original_code)
        output_reindexed = result.reindex(
            index=V.index, columns=V.columns, fill_value=0.0
        )
        _assert_non_waste_unchanged(V, output_reindexed, waste_set, original_code)
        return output_reindexed

    output = V.copy()

    # --- Intersection block ---
    orig_val = cast(float, output.loc[original_code, original_code])
    intersection_w = weights.make_intersection
    for ind in waste_codes:
        for com in waste_codes:
            w = (
                cast(float, intersection_w.loc[ind, com])
                if (ind in intersection_w.index and com in intersection_w.columns)
                else 0.0
            )
            output.loc[ind, com] = orig_val * w

    # --- Column disaggregation (non-waste industry rows, waste commodity columns) ---
    col_w = weights.make_waste_commodity_columns_all_rows
    specific_col_w = weights.make_waste_commodity_columns_specific_rows
    for ind in output.index:
        if ind in waste_set or ind == original_code:
            continue
        orig_row_val = cast(float, output.loc[ind, original_code])
        if orig_row_val == 0.0:
            for com in waste_codes:
                output.loc[ind, com] = 0.0
            continue
        if not specific_col_w.empty and ind in specific_col_w.index:
            row_weights = specific_col_w.loc[ind]
        elif not col_w.empty and ind in col_w.index:
            row_weights = col_w.loc[ind]
        elif not col_w.empty and len(col_w) == 1:
            row_weights = col_w.iloc[0]
        else:
            n = len(waste_codes)
            row_weights = pd.Series(1.0 / n, index=waste_codes)
        for com in waste_codes:
            w = float(row_weights[com]) if com in row_weights.index else 0.0
            output.loc[ind, com] = orig_row_val * w

    # --- Row disaggregation (waste industry rows, non-waste commodity columns) ---
    row_w = weights.make_waste_industry_rows_specific_columns
    for com in output.columns:
        if com in waste_set or com == original_code:
            continue
        orig_col_val = cast(float, output.loc[original_code, com])
        if orig_col_val == 0.0:
            for ind in waste_codes:
                output.loc[ind, com] = 0.0
            continue
        if not row_w.empty and com in row_w.index:
            col_weights = row_w.loc[com]
        else:
            n = len(waste_codes)
            col_weights = pd.Series(1.0 / n, index=waste_codes)
        for ind in waste_codes:
            w = float(col_weights[ind]) if ind in col_weights.index else 0.0
            output.loc[ind, com] = orig_col_val * w

    # --- Remove the original aggregate row and column ---
    output = output.drop(index=original_code, columns=original_code)

    _assert_non_waste_unchanged(V, output, waste_set, original_code)
    return output


def _aggregate_waste_sector_in_U(
    U: pd.DataFrame,
    waste_codes: list[str],
    original_code: str,
) -> pd.DataFrame:
    """Collapse waste_codes rows (commodities) and columns (industries) in U into original_code.

    U is commodity x industry. Returns a new DataFrame with index/columns (non-waste + original_code).
    """
    waste_set = set(waste_codes)
    non_waste_idx = [i for i in U.index if i not in waste_set]
    non_waste_cols = [j for j in U.columns if j not in waste_set]
    new_index = non_waste_idx + [original_code]
    new_columns = non_waste_cols + [original_code]
    output = pd.DataFrame(0.0, index=new_index, columns=new_columns, dtype=float)
    output.loc[non_waste_idx, non_waste_cols] = U.loc[
        non_waste_idx, non_waste_cols
    ].values
    output.loc[original_code, non_waste_cols] = (
        U.loc[waste_codes, non_waste_cols].sum(axis=0).values
    )
    output.loc[non_waste_idx, original_code] = (
        U.loc[non_waste_idx, waste_codes].sum(axis=1).values
    )
    output.loc[original_code, original_code] = (
        U.loc[waste_codes, waste_codes].sum().sum()
    )
    return output


def _apply_waste_disagg_to_U_single(
    U: pd.DataFrame,
    weights: WasteDisaggWeights,
    original_code: str,
) -> pd.DataFrame:
    """Disaggregate waste sector in a single Use matrix (assumes original_code in U)."""
    waste_codes = _waste_codes(weights)
    waste_set = set(waste_codes)
    output = U.copy()

    # In Use tables: index=commodities, columns=industries
    # use_intersection: index=industry_subsectors, columns=commodity_subsectors

    # --- Intersection block ---
    orig_val = cast(float, output.loc[original_code, original_code])
    intersection_w = weights.use_intersection
    for com in waste_codes:
        for ind in waste_codes:
            w = (
                cast(float, intersection_w.loc[ind, com])
                if (ind in intersection_w.index and com in intersection_w.columns)
                else 0.0
            )
            output.loc[com, ind] = orig_val * w

    # --- Column disaggregation (industry columns) ---
    col_w = weights.use_waste_industry_columns_all_rows
    va_rows = set(weights.use_va_rows_for_waste_industry_columns.index)
    for com in output.index:
        if com in waste_set or com == original_code or com in va_rows:
            continue
        orig_row_val = cast(float, output.loc[com, original_code])
        if orig_row_val == 0.0:
            for ind in waste_codes:
                output.loc[com, ind] = 0.0
            continue
        if not col_w.empty and com in col_w.index:
            row_weights = col_w.loc[com]
        elif not col_w.empty and len(col_w) == 1:
            row_weights = col_w.iloc[0]
        else:
            n = len(waste_codes)
            row_weights = pd.Series(1.0 / n, index=waste_codes)
        for ind in waste_codes:
            w = float(row_weights[ind]) if ind in row_weights.index else 0.0
            output.loc[com, ind] = orig_row_val * w

    # --- Row disaggregation (commodity rows) ---
    # Industry-specific allocations (useeior rowsPercentages) override default per column.
    specific_row_w = weights.use_waste_rows_specific_columns
    default_row_w = weights.use_waste_commodity_rows_all_columns
    fd_cols = set(weights.use_fd_columns_for_waste_commodity_rows.index)
    for ind in output.columns:
        if ind in waste_set or ind == original_code or ind in fd_cols:
            continue
        orig_col_val = cast(float, output.loc[original_code, ind])
        if orig_col_val == 0.0:
            for com in waste_codes:
                output.loc[com, ind] = 0.0
            continue
        if not specific_row_w.empty and ind in specific_row_w.index:
            col_weights = (
                specific_row_w.loc[ind]
                .reindex(waste_codes, fill_value=0.0)
                .astype(float)
            )
            total = float(col_weights.sum())
            if total > 0:
                col_weights = col_weights / total
            else:
                n = len(waste_codes)
                col_weights = pd.Series(1.0 / n, index=waste_codes)
        elif not default_row_w.empty and ind in default_row_w.index:
            col_weights = default_row_w.loc[ind]
        elif not default_row_w.empty and len(default_row_w) >= 1:
            col_weights = default_row_w.iloc[0]
        else:
            n = len(waste_codes)
            col_weights = pd.Series(1.0 / n, index=waste_codes)
        col_weights = col_weights.reindex(waste_codes, fill_value=0.0).astype(float)
        for com in waste_codes:
            w = float(col_weights[com]) if com in col_weights.index else 0.0
            output.loc[com, ind] = orig_col_val * w

    output = output.drop(index=original_code, columns=original_code)
    return output


def apply_waste_disagg_to_U(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    weights: WasteDisaggWeights,
    original_code: str = "562000",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Disaggregate the 562 waste sector in Use matrices Udom and Uimp.

    Mirrors useeior's specifiedUseDisagg():
    - Intersection: the (original_code, original_code) cell is split into
      a (waste_commodity x waste_industry) block using use_intersection weights.
    - Columns (industry disaggregation): for each non-waste, non-VA commodity row,
      U[commodity, original_code] is split across waste industry columns.
    - Rows (commodity disaggregation): for each non-waste, non-FD industry column,
      U[original_code, industry] is split across waste commodity rows. Uses
      use_waste_rows_specific_columns when the industry has a row there (useeior
      rowsPercentages); otherwise use_waste_commodity_rows_all_columns (original
      or default row).

    VA and FD are handled by separate functions.
    """
    waste_codes = _waste_codes(weights)
    waste_set = set(waste_codes)

    results: list[pd.DataFrame] = []
    for U in (Udom, Uimp):
        U_orig = U
        if original_code not in U.index or original_code not in U.columns:
            if waste_set.issubset(U.index) and waste_set.issubset(U.columns):
                U = _aggregate_waste_sector_in_U(U, waste_codes, original_code)
            else:
                results.append(U)
                continue
        result = _apply_waste_disagg_to_U_single(U, weights, original_code)
        if original_code in U_orig.index:
            desired_index = U_orig.index.drop(original_code)
            desired_columns = U_orig.columns.drop(original_code)
        else:
            desired_index = U_orig.index
            desired_columns = U_orig.columns
        results.append(
            result.reindex(index=desired_index, columns=desired_columns, fill_value=0.0)
        )

    _assert_non_waste_unchanged(Udom, results[0], waste_set, original_code)
    _assert_non_waste_unchanged(Uimp, results[1], waste_set, original_code)
    return results[0], results[1]


def _aggregate_waste_sector_in_VA(
    va: pd.DataFrame,
    waste_codes: list[str],
    original_code: str,
) -> pd.DataFrame:
    """Collapse waste_codes columns (industries) in va into a single column (original_code).

    va is VA rows x industry columns. Returns a new DataFrame with columns (non-waste + original_code).
    """
    waste_set = set(waste_codes)
    non_waste_cols = [c for c in va.columns if c not in waste_set]
    new_columns = non_waste_cols + [original_code]
    output = pd.DataFrame(0.0, index=va.index, columns=new_columns, dtype=float)
    output.loc[:, non_waste_cols] = va.loc[:, non_waste_cols].values
    output.loc[:, original_code] = va.loc[:, waste_codes].sum(axis=1).values
    return output


def apply_waste_disagg_to_VA(
    va: pd.DataFrame,
    weights: WasteDisaggWeights,
    original_code: str = "562000",
) -> pd.DataFrame:
    """Disaggregate the 562 waste sector in Value Added.

    Mirrors useeior's disaggregateVA():
    - va is a DataFrame with index=VA row codes and columns=industry codes.
    - For each VA row, va[va_row, original_code] is split across waste industry
      subsector columns using use_va_rows_for_waste_industry_columns.
    """
    waste_codes = _waste_codes(weights)
    waste_set = set(waste_codes)
    va_orig = va
    desired_index = va.index
    if original_code in va.columns:
        desired_columns = [c for c in va.columns if c != original_code] + list(
            waste_codes
        )
    else:
        desired_columns = list(va.columns)

    if original_code not in va.columns:
        if waste_set.issubset(va.columns):
            va = _aggregate_waste_sector_in_VA(va, waste_codes, original_code)
        else:
            return va

    output = va.copy()
    va_w = weights.use_va_rows_for_waste_industry_columns

    for va_row in output.index:
        orig_val = cast(float, output.loc[va_row, original_code])
        if orig_val == 0.0:
            for ind in waste_codes:
                output.loc[va_row, ind] = 0.0
            continue
        if not va_w.empty and va_row in va_w.index:
            row_weights = va_w.loc[va_row]
        elif not va_w.empty and len(va_w) >= 1:
            row_weights = va_w.iloc[0]
        else:
            n = len(waste_codes)
            row_weights = pd.Series(1.0 / n, index=waste_codes)
        for ind in waste_codes:
            w = float(row_weights[ind]) if ind in row_weights.index else 0.0
            output.loc[va_row, ind] = orig_val * w

    output = output.drop(columns=original_code)
    output_reindexed = output.reindex(
        index=desired_index, columns=desired_columns, fill_value=0.0
    )
    _assert_non_waste_unchanged(va_orig, output_reindexed, waste_set, original_code)
    return output_reindexed


def _aggregate_waste_sector_in_Ytot(
    Ytot: pd.DataFrame,
    waste_codes: list[str],
    original_code: str,
) -> pd.DataFrame:
    """Collapse waste_codes rows (commodities) in Ytot into a single row (original_code).

    Ytot is commodity x FD columns. Returns a new DataFrame with index (non-waste + original_code).
    """
    waste_set = set(waste_codes)
    non_waste_idx = [i for i in Ytot.index if i not in waste_set]
    new_index = non_waste_idx + [original_code]
    output = pd.DataFrame(0.0, index=new_index, columns=Ytot.columns, dtype=float)
    output.loc[non_waste_idx, :] = Ytot.loc[non_waste_idx, :].values
    output.loc[original_code, :] = Ytot.loc[waste_codes, :].sum(axis=0).values
    return output


def apply_waste_disagg_to_Ytot(
    Ytot: pd.DataFrame,
    weights: WasteDisaggWeights,
    original_code: str = "562000",
) -> pd.DataFrame:
    """Disaggregate the 562 waste sector in Final Demand matrix Y.

    Mirrors useeior's disaggregateFinalDemand():
    - Ytot is a DataFrame with index=commodity codes and columns=FD column codes.
    - For each FD column, Ytot[original_code, fd_col] is split across waste commodity
      subsector rows using use_fd_columns_for_waste_commodity_rows.
    - If no FD-specific weight exists for a column, use_waste_commodity_rows_all_columns
      is used as fallback (original_code row if present, else first row; matches useeior
      getDefaultAllocationPercentages(UseFileDF, ..., 'Commodity')).
    """
    waste_codes = _waste_codes(weights)
    waste_set = set(waste_codes)
    Ytot_orig = Ytot
    if original_code in Ytot.index:
        desired_index = [i for i in Ytot.index if i != original_code] + list(
            waste_codes
        )
    else:
        desired_index = list(Ytot.index)
    desired_columns = Ytot.columns

    if original_code not in Ytot.index:
        if waste_set.issubset(Ytot.index):
            Ytot = _aggregate_waste_sector_in_Ytot(Ytot, waste_codes, original_code)
        else:
            return Ytot

    output = Ytot.copy()
    fd_w = weights.use_fd_columns_for_waste_commodity_rows
    default_table = weights.use_waste_commodity_rows_all_columns
    if not default_table.empty and len(default_table.columns) > 0:
        if original_code in default_table.index:
            default_row = default_table.loc[original_code]
        else:
            default_row = default_table.iloc[0]
        fallback_w = default_row.reindex(waste_codes, fill_value=0.0).astype(float)
        fallback_total = float(fallback_w.sum())
        if fallback_total > 0:
            fallback_w = fallback_w / fallback_total
        else:
            fallback_w = pd.Series(
                {c: 1.0 / len(waste_codes) for c in waste_codes}, dtype=float
            )
    else:
        fallback_w = pd.Series(
            {c: 1.0 / len(waste_codes) for c in waste_codes}, dtype=float
        )

    for fd_col in output.columns:
        orig_val = cast(float, output.loc[original_code, fd_col])
        if orig_val == 0.0:
            for com in waste_codes:
                output.loc[com, fd_col] = 0.0
            continue
        if not fd_w.empty and fd_col in fd_w.index:
            col_weights = fd_w.loc[fd_col]
        else:
            col_weights = fallback_w
        for com in waste_codes:
            w = float(col_weights[com]) if com in col_weights.index else 0.0
            output.loc[com, fd_col] = orig_val * w

    output = output.drop(index=original_code)
    output_reindexed = output.reindex(
        index=desired_index, columns=desired_columns, fill_value=0.0
    )
    _assert_non_waste_unchanged(Ytot_orig, output_reindexed, waste_set, original_code)
    return output_reindexed
