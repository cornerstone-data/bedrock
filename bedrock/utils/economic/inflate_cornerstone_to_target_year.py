"""Cornerstone-specific inflation helpers.

Mirrors inflate_to_target_year.py but reindexes the CEDA v7 price index to
cornerstone commodity codes (405).

Codes that exist only in cornerstone and were disaggregated from a CEDA v7 parent
(e.g. waste 562111 ← 562000) inherit the parent's price ratio.  Codes with no
identifiable parent (e.g. S00402 used goods) receive a neutral ratio of 1.0.
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd

from bedrock.utils.economic.inflation import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__cornerstone_commodity import (
    load_bea_v2017_commodity_to_cornerstone_commodity,
)

CORNERSTONE_COMMODITIES: list[str] = list(COMMODITIES)

get_price_index = functools.cache(
    lambda: obtain_inflation_factors_from_reference_data()
)


@functools.cache
def _cornerstone_to_ceda_v7_parent() -> dict[str, str]:
    """Map cornerstone-only codes back to their CEDA v7 parent for price lookup.

    Built from the BEA 2017 → cornerstone commodity mapping.  For any BEA code
    that maps to multiple cornerstone codes, each child inherits the BEA code
    (which is also the CEDA v7 code) as its price-index parent.  1:1 mapped
    codes already exist in the CEDA v7 index and need no override.
    """
    mapping = load_bea_v2017_commodity_to_cornerstone_commodity()
    parent: dict[str, str] = {}
    for bea_code, corner_codes in mapping.items():
        if len(corner_codes) > 1:
            for child in corner_codes:
                parent[child] = bea_code
    return parent


@functools.cache
def get_cornerstone_price_ratio(
    original_year: int, target_year: int
) -> pd.Series[float]:
    """Price ratio reindexed to cornerstone commodity codes.

    Cornerstone-only child codes (e.g. waste subsectors) inherit their CEDA v7
    parent's price ratio so that inflation is applied consistently.
    """
    price_index = get_price_index()
    ceda_ratio: pd.Series[float] = price_index[target_year] / price_index[original_year]

    # Start with direct reindex (codes shared with CEDA v7 get their own ratio)
    ratio = ceda_ratio.reindex(CORNERSTONE_COMMODITIES, fill_value=np.nan)

    # Fill cornerstone-only children with their CEDA v7 parent's ratio
    parent_map = _cornerstone_to_ceda_v7_parent()
    for child, parent_code in parent_map.items():
        if child in ratio.index and pd.isna(ratio[child]):
            if parent_code in ceda_ratio.index:
                ratio[child] = ceda_ratio[parent_code]

    # Anything still NaN (truly no parent, e.g. S00402) gets neutral 1.0
    ratio = ratio.fillna(1.0)
    return ratio


def inflate_cornerstone_A_matrix(
    A: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_ratio = get_cornerstone_price_ratio(original_year, target_year)
    return pd.DataFrame(
        (np.diag(price_ratio) @ A @ np.diag(1 / price_ratio)).values,
        index=A.index,
        columns=A.columns,
    )


def inflate_cornerstone_B_matrix(
    B: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_ratio = get_cornerstone_price_ratio(target_year, original_year)
    return B * price_ratio.reindex(B.columns, fill_value=1.0).values


def inflate_cornerstone_q_or_y(
    q_or_y: pd.Series[float], original_year: int, target_year: int
) -> pd.Series[float]:
    price_ratio = get_cornerstone_price_ratio(original_year, target_year)
    return q_or_y * price_ratio.reindex(q_or_y.index, fill_value=1.0)


def inflate_cornerstone_V_to_target_year(
    V: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    """Inflate V (industry × commodity) along commodity axis."""
    price_ratio = get_cornerstone_price_ratio(original_year, target_year)
    return pd.DataFrame(
        V.multiply(price_ratio, axis=1).values,
        index=V.index,
        columns=V.columns,
    )
