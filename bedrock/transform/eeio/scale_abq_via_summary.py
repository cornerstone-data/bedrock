from __future__ import annotations

import typing as ta

import numpy as np
import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.transform.eeio.derived_2017 import (
    derive_summary_Adom_usa,
    derive_summary_Aimp_usa,
    derive_summary_q_usa,
)
from bedrock.utils.math.formulas import compute_total_industry_inputs
from bedrock.utils.schemas.single_region_schemas import AMatrix
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_MUT_YEARS
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    load_bea_v2017_summary_to_ceda_v7,
)


@pa.check_output(AMatrix.to_schema())
def scale_detail_A_based_on_summary_A(
    A: pd.DataFrame,
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
    dom_or_imp_or_total: ta.Literal["dom", "imp", "total"],
) -> pt.DataFrame[AMatrix]:
    """
    Derive A_ratio from the summary tables and scale A element-wise based on A_ratio.
    """

    match dom_or_imp_or_total:
        case "dom":
            A_summary_base = derive_summary_Adom_usa(original_year)
            A_summary_target = derive_summary_Adom_usa(target_year)
        case "imp":
            A_summary_base = derive_summary_Aimp_usa(original_year)
            A_summary_target = derive_summary_Aimp_usa(target_year)
        case "total":
            A_summary_base = derive_summary_Adom_usa(
                original_year
            ) + derive_summary_Aimp_usa(original_year)
            A_summary_target = derive_summary_Adom_usa(
                target_year
            ) + derive_summary_Aimp_usa(target_year)

    summary_to_ceda_v7 = load_bea_v2017_summary_to_ceda_v7()
    summary_ratios = (A_summary_target / A_summary_base).fillna(1.0)
    summary_ratios[np.isinf(summary_ratios)] = 1.0

    A_scaled = A.copy()

    block_rows = []
    for i, row in summary_ratios.iterrows():
        if i not in summary_to_ceda_v7:
            continue
        block_mat = pd.DataFrame(
            index=summary_to_ceda_v7[i],  # type: ignore
            columns=CEDA_V7_SECTORS,
            data=0,
            dtype=float,
        )
        for col_summary_sector, val in row.items():
            if val == 0:
                continue
            if col_summary_sector in ["Used", "Other"]:
                continue
            col_ceda_sectors = summary_to_ceda_v7[col_summary_sector]  # type: ignore
            block_mat.loc[:, col_ceda_sectors] = val
        block_rows.append(block_mat)
    ratio_multiplier = pd.concat(block_rows, axis=0).loc[
        A_scaled.index, A_scaled.columns
    ]
    A_scaled_by_A_summary = A_scaled * ratio_multiplier

    # Adjust A if column sum exceeds 1, force the column sum to be 0.98
    A_scaled_by_A_summary = _rescale_A_matrix_by_capping_total_industry_inputs(
        A=A_scaled_by_A_summary, max_total_industry_inputs=0.98
    )

    assert (
        compute_total_industry_inputs(A=A_scaled_by_A_summary) <= 1
    ).all(), "A column sums exceed 1 after scaled via A_ratio."

    return pt.DataFrame[AMatrix](A_scaled_by_A_summary)


def scale_detail_q_based_on_summary_q(
    q: pd.Series[float],
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.Series[float]:
    q_scaled = q.copy()
    q_summary_base = derive_summary_q_usa(original_year)
    q_summary_target = derive_summary_q_usa(target_year)
    q_ratio = (q_summary_target / q_summary_base).fillna(1.0)

    summary_to_ceda_v7 = load_bea_v2017_summary_to_ceda_v7()
    for i, val in q_ratio.items():
        if i not in summary_to_ceda_v7:
            continue
        ceda_sectors = summary_to_ceda_v7[i]  # type: ignore
        q_scaled.loc[ceda_sectors] *= val

    return q_scaled


def scale_detail_B_based_on_summary_q(
    B: pd.DataFrame,
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.DataFrame:
    B_scaled = B.copy()
    q_summary_base = derive_summary_q_usa(original_year)
    q_summary_target = derive_summary_q_usa(target_year)
    # b_ratio is the opposite of q_ratio because
    # the price value for B is in the denominator
    b_ratio = (q_summary_base / q_summary_target).fillna(1.0)

    summary_to_ceda_v7 = load_bea_v2017_summary_to_ceda_v7()
    for i, val in b_ratio.items():
        if i not in summary_to_ceda_v7:
            continue
        ceda_sectors = summary_to_ceda_v7[i]  # type: ignore
        B_scaled.loc[:, ceda_sectors] *= val

    return B_scaled


def _rescale_A_matrix_by_capping_total_industry_inputs(
    *, A: pd.DataFrame, max_total_industry_inputs: float  # noqa: ARG001
) -> pd.DataFrame:
    A_rescaled = A.copy()
    total_industry_inputs = compute_total_industry_inputs(A=A)
    oob_idx = total_industry_inputs[total_industry_inputs > 1].index
    for col in oob_idx:
        A_rescaled[col] *= 0.98 / total_industry_inputs[col]
    return A_rescaled
