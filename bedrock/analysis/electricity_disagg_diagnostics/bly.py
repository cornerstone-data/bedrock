"""BLy sector vectors from diagnostics sheets + sum-preserving alignment."""

from __future__ import annotations

import pandas as pd

from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES
from bedrock.utils.validation.analysis.bly_plots import TAB_BLY
from bedrock.utils.validation.analysis.fetch import load_tab

WASTE_AGGREGATE = '562000'
WASTE_PREFIX = '562'
BLy_NEW_COL = 'BLy_new (MtCO2e)'


def sector_bly_new(sheet_id: str, *, refresh: bool = False) -> pd.Series[float]:
    """Per-sector BLy_new (MtCO2e) with waste 562* collapsed to 562000."""
    tab = load_tab(sheet_id, TAB_BLY, refresh=refresh)
    frame = tab[['index', BLy_NEW_COL]].copy()
    frame['index'] = frame['index'].astype(str)
    frame[BLy_NEW_COL] = pd.to_numeric(frame[BLy_NEW_COL], errors='coerce')
    series = frame.set_index('index')[BLy_NEW_COL].astype(float)
    return _collapse_waste_bly(series)


def footing_total_bly_mmt(footing_sheet_id: str, *, refresh: bool = False) -> float:
    """Cornerstone v0.2 total attributed BLy (MtCO2e) — % denominator."""
    return float(sector_bly_new(footing_sheet_id, refresh=refresh).sum())


def _collapse_waste_bly(series: pd.Series[float]) -> pd.Series[float]:
    """Sum waste children's absolute BLy into 562000; drop child rows."""
    disagg_children = set(WASTE_DISAGG_COMMODITIES.get(WASTE_AGGREGATE, []))
    if not disagg_children:
        return series
    present = set(series.index.astype(str))
    if WASTE_AGGREGATE not in present:
        return series
    if not (present & disagg_children):
        return series

    out = series.copy()
    labels = out.index.astype(str)
    waste_mask = labels.str.startswith(WASTE_PREFIX)
    child_mask = labels.isin(disagg_children)
    lumped = out.loc[waste_mask | child_mask]
    out.loc[WASTE_AGGREGATE] = float(lumped.sum())
    drop = child_mask & (labels != WASTE_AGGREGATE)
    return out.loc[~drop]


def align_bly_pair(
    a: pd.Series[float],
    b: pd.Series[float],
) -> tuple[pd.Series[float], pd.Series[float]]:
    """Sum-preserving alignment on union index before L1 BLy diffs."""
    idx = a.index.union(b.index)
    a_aligned = a.reindex(idx, fill_value=0.0).astype(float).copy()
    b_aligned = b.reindex(idx, fill_value=0.0).astype(float).copy()

    children = list(ELECTRICITY_DISAGG_SECTORS)
    agg = ELECTRICITY_AGGREGATE_SECTOR
    if not all(c in idx for c in children):
        return a_aligned, b_aligned

    shares = _electricity_child_shares(a_aligned, b_aligned, children)
    _canonicalize_electricity_bly(a_aligned, shares, agg=agg, children=children)
    _canonicalize_electricity_bly(b_aligned, shares, agg=agg, children=children)
    return a_aligned, b_aligned


def _electricity_child_shares(
    a: pd.Series[float],
    b: pd.Series[float],
    children: list[str],
) -> pd.Series[float]:
    third = 1.0 / len(children)
    fallback = pd.Series({c: third for c in children}, dtype=float)
    for side in (b, a):
        if any(side.get(c, 0.0) for c in children):
            ref = side[children].astype(float)
            total = float(ref.sum())
            if total:
                return ref / total
    return fallback


def _canonicalize_electricity_bly(
    side: pd.Series[float],
    ref_shares: pd.Series[float],
    *,
    agg: str,
    children: list[str],
) -> None:
    agg_val = float(side.get(agg, 0.0))
    child_vals = {c: float(side.get(c, 0.0)) for c in children}
    has_children = any(v != 0.0 for v in child_vals.values())

    if agg_val and not has_children:
        for c in children:
            side.loc[c] = agg_val * float(ref_shares[c])
        side.loc[agg] = 0.0
    elif has_children:
        side.loc[agg] = 0.0
