from __future__ import annotations

import typing as ta
from collections.abc import Iterable

import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__cornerstone_industry import (
    load_bea_v2017_industry_to_cornerstone_industry,
)


def get_allocation_sectors() -> list[str]:
    """
    Return the sector list (taxonomy) for allocation based on model config.

    When use_cornerstone_2026_model_schema is True, returns Cornerstone INDUSTRIES;
    otherwise returns CEDA v7 sectors.
    """
    if get_usa_config().use_cornerstone_2026_model_schema:
        return list(INDUSTRIES)
    return list(CEDA_V7_SECTORS)


def parse_index_with_aggregates(
    idx: pd.Index[ta.Any], aggregates: ta.List[str]
) -> pd.MultiIndex:
    """
    parses columns that have aggregate subtotals, so long as we know which those are
    """

    tups: ta.List[ta.Tuple[str, str]] = []
    assert idx[0] in aggregates, "index must start with an aggregate"

    current_agg: str
    for val in idx:
        if val in aggregates:
            current_agg = val
            tups.append((current_agg, "TOTAL"))
        else:
            tups.append((current_agg, val))

    multi_idx = pd.MultiIndex.from_tuples(tups)
    assert multi_idx.is_unique
    return multi_idx


def flatten_items(items: ta.Iterable[ta.Any]) -> ta.Iterable[ta.Any]:
    """Yield items from any nested iterable."""
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            yield from flatten_items(x)
        else:
            yield x


def reindex_allocated_to_schema(allocated: pd.Series[float]) -> pd.Series[float]:
    """
    Return allocated emissions in the schema implied by config.

    When use_cornerstone_2026_model_schema is True, map CEDA-indexed allocated
    series to Cornerstone industries (same logic as derived.derive_E_usa).
    Otherwise return the series unchanged (CEDA v7 sectors).
    """
    if not get_usa_config().use_cornerstone_2026_model_schema:
        return allocated
    mapping = load_bea_v2017_industry_to_cornerstone_industry()
    target_columns: list[str] = list(INDUSTRIES)
    col_to_target = {k: v[0] for k, v in mapping.items()}
    for c in allocated.index:
        if c not in col_to_target and c in target_columns:
            col_to_target[c] = c
    target_index = allocated.index.map(lambda i: col_to_target.get(i, i))
    out = allocated.groupby(target_index).sum()
    return out.reindex(target_columns, fill_value=0.0)
