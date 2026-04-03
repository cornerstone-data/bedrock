# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import typing as ta

import numpy as np
import pandas as pd

from bedrock.utils.io.gcp import update_sheet_tab
from bedrock.utils.snapshots.loader import load_configured_snapshot

logger = logging.getLogger(__name__)


def calculate_national_accounting_balance_diagnostics(
    sheet_id: str,
) -> None:
    """Validate that the US portion of Cornerstone's MRIO model conserves total emissions.

    Checks whether sum(BLy) (total production-induced emissions from the
    IO framework) matches sum(E_orig) (total original emissions data)
    at the national level.

    BLy = diag(d) @ L @ y_nab
    Where:
    - d = compute_d(B) = column sums of B (direct emissions per $ output)
    - L = Leontief inverse (total requirements matrix)
    - y_nab = final demand vector for national accounting balance

    If the model is balanced, sum(BLy) should equal sum(E_orig).

    The ``BLy_and_E_orig_diffs`` sheet lists the USA total first, a blank row,
    then per-sector BLy_i vs E_orig_i (same columns; sector-level % uses E_orig_i).
    """
    # Late-binding imports - depend on global config
    from bedrock.transform.eeio.derived import (
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
        derive_y_for_national_accounting_balance_usa,
    )
    from bedrock.utils.math.formulas import compute_d, compute_L_matrix

    logger.info("------ Calculating national accounting balance diagnostics ------")

    # Derive B, L, y from current model
    B_new = derive_B_usa_non_finetuned()
    Aq_set = derive_Aq_usa()
    L_new = compute_L_matrix(A=Aq_set.Adom)
    y_new = derive_y_for_national_accounting_balance_usa()

    # BLy = diag(d) @ L @ y
    logger.info("1. Calculating BLy...")
    d_new = compute_d(B=B_new)
    BLy_new = (
        pd.DataFrame(np.diag(d_new), index=L_new.index, columns=L_new.columns)
        @ L_new
        @ y_new
    )

    # E_orig: original emissions from snapshot (gas x sector matrix)
    logger.info("2. Loading E_orig from snapshot...")
    E_orig = load_configured_snapshot("E_USA_ES")
    E_orig_by_sector = ta.cast("pd.Series[float]", E_orig.sum(axis=0))

    # National-level totals
    BLy_total = float(BLy_new.sum())
    E_orig_total = float(E_orig_by_sector.sum())
    diff = BLy_total - E_orig_total
    perc_diff = diff / E_orig_total if E_orig_total != 0 else 0.0

    logger.info("3. Building national accounting balance summary...")
    comparison = pd.DataFrame(
        {
            "BLy (MtCO2e)": [BLy_total / 1e9],
            "E_orig (MtCO2e)": [E_orig_total / 1e9],
            "BLy - E_orig (MtCO2e)": [diff / 1e9],
            "(BLy - E_orig) / E_orig (%)": [perc_diff],
        },
        index=pd.Index(["USA"]),
    )
    summary_out = comparison.reset_index()

    _bly = BLy_new
    if isinstance(_bly, pd.DataFrame):
        _bly = _bly.iloc[:, 0] if _bly.shape[1] == 1 else _bly.squeeze()
    bly_s = _bly.astype(float)

    sector_index = bly_s.index.union(E_orig_by_sector.index).sort_values()
    bly_by_sec = bly_s.reindex(sector_index)
    e_by_sec = E_orig_by_sector.reindex(sector_index)
    diff_by_sec = bly_by_sec - e_by_sec.astype(float)
    e_arr = e_by_sec.to_numpy(dtype=float, copy=True)
    d_arr = diff_by_sec.to_numpy(dtype=float, copy=True)
    perc_arr = np.zeros(len(sector_index), dtype=float)
    valid = np.isfinite(e_arr) & (e_arr != 0)
    perc_arr[valid] = d_arr[valid] / e_arr[valid]

    detail_out = pd.DataFrame(
        {
            "index": sector_index,
            "BLy (MtCO2e)": bly_by_sec / 1e9,
            "E_orig (MtCO2e)": e_by_sec / 1e9,
            "BLy - E_orig (MtCO2e)": diff_by_sec / 1e9,
            "(BLy - E_orig) / E_orig (%)": perc_arr,
        }
    )
    # NaN for numeric columns keeps float dtypes after concat; empty index cell only.
    sep_row: dict[str, object] = {
        c: ("" if c == "index" else np.nan) for c in summary_out.columns
    }
    separator = pd.DataFrame([sep_row])
    combined = pd.concat(
        [summary_out, separator, detail_out],
        ignore_index=True,
    )

    update_sheet_tab(
        sheet_id,
        "BLy_and_E_orig_diffs",
        combined,
    )
