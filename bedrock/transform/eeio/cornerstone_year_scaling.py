"""Year-scaling helpers for Cornerstone A, q, and B matrices.

Scales detail-level Cornerstone matrices using summary-level ratios from
BEA summary IO tables.  The summary → detail correspondence uses the
Cornerstone mapping (not the CEDA v7 version).
"""

from __future__ import annotations

import functools
import typing as ta
from dataclasses import dataclass

import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_2017 import (
    derive_summary_Adom_usa,
    derive_summary_Aimp_usa,
    derive_summary_q_usa,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    adjust_summary_A_dollar_year,
    adjust_summary_q_dollar_year,
)
from bedrock.utils.math.formulas import compute_total_industry_inputs
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_MUT_YEARS
from bedrock.utils.taxonomy.bea_v2017_to_cornerstone_helpers import (
    load_bea_v2017_summary_to_cornerstone,
)


@dataclass(frozen=True)
class Pre1aAq:
    """Live A/q after summary ``\"22\"`` and the 0.98 column-sum fix, before 1a."""

    Adom: pd.DataFrame
    q: pd.Series


_PRE_1A_A: dict[tuple[int, int], pd.DataFrame] = {}
_PRE_1A_Q: dict[tuple[int, int], pd.Series] = {}


def clear_pre_1a_aq() -> None:
    """Wipe the pre-1a intercept store (not only ``@cache``)."""
    _PRE_1A_A.clear()
    _PRE_1A_Q.clear()
    get_pre_1a_aq.cache_clear()


def _store_pre_1a_a(original_year: int, target_year: int, a: pd.DataFrame) -> None:
    _PRE_1A_A[(int(original_year), int(target_year))] = a.copy()


def _store_pre_1a_q(original_year: int, target_year: int, q: pd.Series) -> None:
    _PRE_1A_Q[(int(original_year), int(target_year))] = q.copy()


@functools.cache
def get_pre_1a_aq(original_year: int, target_year: int) -> Pre1aAq:
    """Return intercepted live pre-1a Adom/q. Does not re-invoke scale."""
    key = (int(original_year), int(target_year))
    if key not in _PRE_1A_A:
        raise RuntimeError(
            'pre-1a Adom intercept missing; scale_cornerstone_A(dom) must run first'
        )
    if key not in _PRE_1A_Q:
        raise RuntimeError(
            'pre-1a q intercept missing; scale_cornerstone_q must run first'
        )
    return Pre1aAq(Adom=_PRE_1A_A[key], q=_PRE_1A_Q[key])


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
    """Scale detail A element-wise using summary A ratios.

    When ``cfg.apply_io_year_adjustments`` is set, the target-year summary A
    is rebased into ``original_year`` USD via the ITA-based (industry technology assumption)
    summary commodity price ratio before the ratio is derived and taken, so the structural
    cross-year ratio is formed in a consistent dollar year.

    Since the target-year summary A is rebased into ``original_year`` USD, the resulting
    detail A is in ``original_year`` USD.
    """
    A_summary_base = _get_summary_A(original_year, dom_or_imp_or_total)
    A_summary_target = _get_summary_A(target_year, dom_or_imp_or_total)
    if get_usa_config().apply_io_year_adjustments:
        A_summary_target = adjust_summary_A_dollar_year(
            A_summary=A_summary_target,
            from_year=target_year,
            to_year=original_year,
        )

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

    assert (
        compute_total_industry_inputs(A=A_scaled) <= 1
    ).all(), 'A column sums exceed 1 after scaling.'

    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        electricity_disaggregation_enabled,
    )

    if electricity_disaggregation_enabled() and dom_or_imp_or_total == 'dom':
        _store_pre_1a_a(original_year, target_year, A_scaled)

    if electricity_disaggregation_enabled():
        from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
            rescale_electricity_children_to_detail_GO_growth_A,
        )

        A_scaled = rescale_electricity_children_to_detail_GO_growth_A(
            A_scaled, original_year, target_year
        )

    return A_scaled


def scale_cornerstone_q(
    q: pd.Series[float],
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.Series[float]:
    """Scale detail q element-wise using summary q ratios.

    When ``cfg.apply_io_year_adjustments`` is set, the target-year summary q
    is rebased into ``original_year`` USD via the ITA-based summary commodity
    price ratio before forming the cross-year ratio. Otherwise the ratio is
    taken on the raw target-year summary q (pre-realignment behavior).
    """
    q_summary_target = derive_summary_q_usa(target_year)
    if get_usa_config().apply_io_year_adjustments:
        q_summary_target = adjust_summary_q_dollar_year(
            q_summary=q_summary_target,
            from_year=target_year,
            to_year=original_year,
        )
    ratio = (q_summary_target / derive_summary_q_usa(original_year)).fillna(1.0)
    q_scaled = ta.cast(
        pd.Series,
        _apply_summary_ratio_to_sectors(ratio, q, axis='rows'),
    )

    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        electricity_disaggregation_enabled,
    )

    if electricity_disaggregation_enabled():
        from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
            rescale_electricity_children_to_detail_GO_growth_q,
        )

        _store_pre_1a_q(original_year, target_year, q_scaled)
        q_scaled = rescale_electricity_children_to_detail_GO_growth_q(
            q_scaled, original_year, target_year
        )

    return q_scaled


def scale_cornerstone_B(
    B: pd.DataFrame,
    target_year: USA_SUMMARY_MUT_YEARS,
    original_year: USA_SUMMARY_MUT_YEARS,
) -> pd.DataFrame:
    """Scale B columns using summary q ratios (legacy pre-IO-adjustments footing)."""
    ratio = (
        derive_summary_q_usa(original_year) / derive_summary_q_usa(target_year)
    ).fillna(1.0)
    return ta.cast(
        pd.DataFrame,
        _apply_summary_ratio_to_sectors(ratio, B, axis='columns'),
    )
