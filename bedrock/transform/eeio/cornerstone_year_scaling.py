"""Year-scaling helpers for Cornerstone A, q, and B matrices.

Scales detail-level Cornerstone matrices using summary-level ratios from
BEA summary IO tables.  The summary → detail correspondence uses the
Cornerstone mapping (not the CEDA v7 version).
"""

from __future__ import annotations

import typing as ta

import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_2017 import (
    derive_summary_Adom_usa,
    derive_summary_Aimp_usa,
    derive_summary_q_usa,
)
from bedrock.utils.math.formulas import compute_total_industry_inputs
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_MUT_YEARS
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    load_bea_v2017_summary_to_cornerstone,
)


def _get_summary_A(
    year: USA_SUMMARY_MUT_YEARS,
    dom_or_imp_or_total: ta.Literal['dom', 'imp', 'total'],
) -> pd.DataFrame:
    """Fetch the summary A matrix for *year* in the requested variant."""
    match dom_or_imp_or_total:
        case 'dom':
            return derive_summary_Adom_usa(year)
        case 'imp':
            return derive_summary_Aimp_usa(year)
        case 'total':
            return derive_summary_Adom_usa(year) + derive_summary_Aimp_usa(year)


def _apply_summary_ratio_to_sectors(
    ratio: pd.Series[float],
    target: pd.DataFrame | pd.Series[float],
    *,
    axis: ta.Literal['rows', 'columns'],
) -> pd.DataFrame | pd.Series[float]:
    """Multiply *target* entries by sector-mapped summary *ratio*.

    Parameters
    ----------
    axis : 'rows' or 'columns'
        Whether to apply the ratio to rows (vector / A-matrix rows) or
        columns (B-matrix columns).
    """
    result = target.copy()
    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    for summary_sector, val in ratio.items():
        if summary_sector not in summary_to_cornerstone:
            continue
        sectors = summary_to_cornerstone[summary_sector]  # type: ignore
        if axis == 'rows':
            result.loc[sectors] *= val  # type: ignore[index,assignment]
        else:
            result.loc[:, sectors] *= val  # type: ignore[assignment]
    return result


def scale_cornerstone_A(
    A: pd.DataFrame,
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
    dom_or_imp_or_total: ta.Literal['dom', 'imp', 'total'],
) -> pd.DataFrame:
    """Scale detail A element-wise using summary A ratios."""
    A_summary_base = _get_summary_A(original_year, dom_or_imp_or_total)
    A_summary_target = _get_summary_A(target_year, dom_or_imp_or_total)

    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    detail_sectors = list(A.index)
    summary_ratios = (A_summary_target / A_summary_base).fillna(1.0)
    summary_ratios[np.isinf(summary_ratios)] = 1.0

    A_scaled = A.copy()

    block_rows = []
    for i, row in summary_ratios.iterrows():
        if i not in summary_to_cornerstone:
            continue
        block_mat = pd.DataFrame(
            index=summary_to_cornerstone[i],  # type: ignore
            columns=detail_sectors,
            data=0,
            dtype=float,
        )
        for col_summary_sector, val in row.items():
            if val == 0:
                continue
            if col_summary_sector in ['Used', 'Other']:
                continue
            col_sectors = summary_to_cornerstone[col_summary_sector]  # type: ignore
            block_mat.loc[:, col_sectors] = val
        block_rows.append(block_mat)

    ratio_multiplier = pd.concat(block_rows, axis=0).reindex(
        index=A_scaled.index, columns=A_scaled.columns, fill_value=1.0
    )
    A_scaled = A_scaled * ratio_multiplier

    total_industry_inputs = compute_total_industry_inputs(A=A_scaled)
    oob_idx = total_industry_inputs[total_industry_inputs > 1].index
    for col in oob_idx:
        A_scaled[col] *= 0.98 / total_industry_inputs[col]

    assert (compute_total_industry_inputs(A=A_scaled) <= 1).all(), (
        'A column sums exceed 1 after scaling.'
    )

    return A_scaled


def scale_cornerstone_q(
    q: pd.Series[float],
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.Series[float]:
    ratio = (
        derive_summary_q_usa(target_year) / derive_summary_q_usa(original_year)
    ).fillna(1.0)
    return ta.cast(
        pd.Series,
        _apply_summary_ratio_to_sectors(ratio, q, axis='rows'),
    )


def scale_cornerstone_B(
    B: pd.DataFrame,
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.DataFrame:
    ratio = (
        derive_summary_q_usa(original_year) / derive_summary_q_usa(target_year)
    ).fillna(1.0)
    return ta.cast(
        pd.DataFrame,
        _apply_summary_ratio_to_sectors(ratio, B, axis='columns'),
    )
