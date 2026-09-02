"""Roll detail MUT frames up to BEA 2017 summary labels."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.table_match import Tolerance, compare_tables
from bedrock.transform.iot.nowcast_redefinition_ratios import ATOL
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    USA_2017_SUMMARY_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_value_added import SUMMARY_VA_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_SUMMARY_COMMODITY_INDEX,
    USA_2017_SUMMARY_INDUSTRY_INDEX,
)

DETAIL_VA_TO_SUMMARY = {
    'V00100': 'V001',
    'V00200': 'V002',
    'V00300': 'V003',
}


@dataclass(frozen=True)
class RollupGateResult:
    """Detail→summary rollup comparison against published summary MUT."""

    label: str
    ok: bool
    max_abs_diff: float
    n_partial: int
    n_miss: int
    n_extra: int


def first_parent_map(mapping: dict[str, list[str]]) -> dict[str, str]:
    """Collapse multi-parent BEA maps to parents[0] (value_added_timeseries precedent)."""
    return {detail: parents[0] for detail, parents in mapping.items() if parents}


def industry_first_parent_map() -> dict[str, str]:
    return first_parent_map(load_bea_v2017_industry_to_bea_v2017_summary())


def commodity_first_parent_map() -> dict[str, str]:
    return first_parent_map(load_bea_v2017_commodity_to_bea_v2017_summary())


def _require_mapped(codes: list[str], parent_map: dict[str, str], *, axis: str) -> None:
    missing = sorted({code for code in codes if code not in parent_map})
    if missing:
        raise ValueError(f'unmapped {axis} codes for summary rollup: {missing}')


def _groupby_sum_axis(
    frame: pd.DataFrame, parent_map: dict[str, str], *, axis: int
) -> pd.DataFrame:
    if axis == 0:
        codes = [str(c) for c in frame.index]
        _require_mapped(codes, parent_map, axis='row')
        grouped = frame.copy()
        grouped.index = pd.Index([parent_map[str(c)] for c in frame.index])
        return grouped.groupby(level=0, sort=False).sum()
    codes = [str(c) for c in frame.columns]
    _require_mapped(codes, parent_map, axis='column')
    grouped = frame.copy()
    grouped.columns = pd.Index([parent_map[str(c)] for c in frame.columns])
    return grouped.T.groupby(level=0, sort=False).sum().T


def rollup_make_to_summary(V_detail: pd.DataFrame) -> pd.DataFrame:
    """Roll detail Make (industry × commodity) to summary labels."""
    industry_map = industry_first_parent_map()
    commodity_map = commodity_first_parent_map()
    rolled = _groupby_sum_axis(V_detail.astype(float), industry_map, axis=0)
    rolled = _groupby_sum_axis(rolled, commodity_map, axis=1)
    return (
        rolled.reindex(
            index=list(USA_2017_SUMMARY_INDUSTRY_CODES),
            columns=list(USA_2017_SUMMARY_COMMODITY_CODES),
            fill_value=0.0,
        )
        .astype(float)
        .set_axis(USA_2017_SUMMARY_INDUSTRY_INDEX.copy(), axis=0)
        .set_axis(USA_2017_SUMMARY_COMMODITY_INDEX.copy(), axis=1)
    )


def rollup_use_intermediate_to_summary(U_detail: pd.DataFrame) -> pd.DataFrame:
    """Roll detail Use intermediate (commodity × industry) to summary labels."""
    industry_cols = [
        c for c in U_detail.columns if str(c) in set(USA_2017_INDUSTRY_CODES)
    ]
    frame = U_detail.loc[:, industry_cols].astype(float)
    commodity_map = commodity_first_parent_map()
    industry_map = industry_first_parent_map()
    rolled = _groupby_sum_axis(frame, commodity_map, axis=0)
    rolled = _groupby_sum_axis(rolled, industry_map, axis=1)
    return (
        rolled.reindex(
            index=list(USA_2017_SUMMARY_COMMODITY_CODES),
            columns=list(USA_2017_SUMMARY_INDUSTRY_CODES),
            fill_value=0.0,
        )
        .astype(float)
        .set_axis(USA_2017_SUMMARY_COMMODITY_INDEX.copy(), axis=0)
        .set_axis(USA_2017_SUMMARY_INDUSTRY_INDEX.copy(), axis=1)
    )


def rollup_va_to_summary(VA_detail: pd.DataFrame) -> pd.DataFrame:
    """Remap detail VA rows then roll industry columns to summary."""
    industry_cols = [
        c for c in VA_detail.columns if str(c) in set(USA_2017_INDUSTRY_CODES)
    ]
    frame = VA_detail.loc[:, industry_cols].astype(float).copy()
    remapped = []
    for code in frame.index:
        key = str(code)
        if key not in DETAIL_VA_TO_SUMMARY:
            raise ValueError(f'unmapped detail VA row for summary rollup: {key}')
        remapped.append(DETAIL_VA_TO_SUMMARY[key])
    frame.index = pd.Index(remapped)
    frame = frame.groupby(level=0, sort=False).sum()
    industry_map = industry_first_parent_map()
    rolled = _groupby_sum_axis(frame, industry_map, axis=1)
    return (
        rolled.reindex(
            index=list(SUMMARY_VA_CODES),
            columns=list(USA_2017_SUMMARY_INDUSTRY_CODES),
            fill_value=0.0,
        )
        .astype(float)
        .set_axis(pd.Index(list(SUMMARY_VA_CODES), name='value_added'), axis=0)
        .set_axis(USA_2017_SUMMARY_INDUSTRY_INDEX.copy(), axis=1)
    )


def rollup_import_to_summary(Uimp_detail: pd.DataFrame) -> pd.DataFrame:
    """Roll detail import matrix the same way as Use intermediate."""
    return rollup_use_intermediate_to_summary(Uimp_detail)


def compare_rollup_block(
    candidate: pd.DataFrame, reference: pd.DataFrame, *, label: str
) -> RollupGateResult:
    """Score one rolled block against published summary at Step 7 ATOL."""
    match = compare_tables(
        candidate, reference, tolerance=Tolerance(atol=ATOL, rtol=0.0)
    )
    left, right = candidate.align(reference, fill_value=0.0)
    max_abs = float(
        np.nanmax(np.abs(left.to_numpy(dtype=float) - right.to_numpy(dtype=float)))
    )
    cell_counts = match.counts().loc['cells']
    n_partial = int(cell_counts['partial'])
    n_miss = int(cell_counts['miss'])
    n_extra = int(cell_counts['extra'])
    ok = match.ok(max_partial=0, max_miss=0, max_extra=0, max_margin_partial=0)
    return RollupGateResult(
        label=label,
        ok=ok,
        max_abs_diff=max_abs,
        n_partial=n_partial,
        n_miss=n_miss,
        n_extra=n_extra,
    )
