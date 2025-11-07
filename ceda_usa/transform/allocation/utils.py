from __future__ import annotations

import typing as ta
from collections.abc import Iterable

import pandas as pd


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
