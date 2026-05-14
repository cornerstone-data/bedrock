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

from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_ceda import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__cornerstone_commodity import (
    load_bea_v2017_commodity_to_cornerstone_commodity,
)

CORNERSTONE_INDUSTRIES: list[str] = list(INDUSTRIES)
CORNERSTONE_COMMODITIES: list[str] = list(COMMODITIES)


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
def get_cornerstone_industry_price_ratio(
    original_year: int, target_year: int
) -> pd.Series[float]:
    """Price ratio reindexed to cornerstone industry codes.

    Cornerstone-only child codes (e.g. waste subsectors) inherit their CEDA v7
    parent's price ratio so that inflation is applied consistently.
    """
    cfg = get_usa_config()
    if cfg.apply_inflation_to_V:
        price_index = derive_industry_price_index()
        target_codes = CORNERSTONE_INDUSTRIES
    else:
        # Reindex to commodities so downstream `diag(p) @ A @ diag(1/p)`
        # aligns positionally against a commodity-indexed A. INDUSTRIES and
        # COMMODITIES order diverges at 351/405 positions, so the target
        # list is load-bearing despite the function's name.
        price_index = obtain_inflation_factors_from_reference_data()
        target_codes = CORNERSTONE_COMMODITIES
    pi_ratio: pd.Series[float] = price_index[target_year] / price_index[original_year]

    # Start with direct reindex (codes shared with CEDA v7 get their own ratio)
    ratio = pi_ratio.reindex(target_codes, fill_value=np.nan)

    # Fill cornerstone-only children with their CEDA v7 parent's ratio
    parent_map = _cornerstone_to_ceda_v7_parent()
    for child, parent_code in parent_map.items():
        if child in ratio.index and pd.isna(ratio[child]):
            if parent_code in pi_ratio.index:
                ratio[child] = pi_ratio[parent_code]

    # Anything still NaN (truly no parent, e.g. S00402) gets neutral 1.0
    ratio = ratio.fillna(1.0)
    return ratio


def inflate_cornerstone_A_matrix_with_industry_pi(
    A: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_ratio = get_cornerstone_industry_price_ratio(original_year, target_year)
    return pd.DataFrame(
        (np.diag(price_ratio) @ A @ np.diag(1 / price_ratio)).values,
        index=A.index,
        columns=A.columns,
    )


def inflate_cornerstone_q_or_y_with_industry_pi(
    q_or_y: pd.Series[float], original_year: int, target_year: int
) -> pd.Series[float]:
    price_ratio = get_cornerstone_industry_price_ratio(original_year, target_year)
    return q_or_y * price_ratio.reindex(q_or_y.index, fill_value=1.0)


@functools.cache
def get_vnorm_adjusted_commodity_price_ratio(
    original_year: int, target_year: int
) -> pd.Series[float]:
    """V-norm-weighted commodity price ratio.

    The price index is industry-level. ``get_cornerstone_industry_price_ratio``
    reindexes those industry ratios onto cornerstone commodity codes 1:1,
    treating each industry as its own primary commodity. This helper instead
    produces a commodity ratio that reflects the actual mix of industries
    supplying each commodity, weighted by V_norm:

        r_com[j] = sum_i V_norm[i, j] * r_ind[i]

    i.e. a column-weighted average of industry price ratios across the
    industries that supply commodity j. This is a ratio-of-ratios
    approximation of the (theoretically purer) ratio of V-norm-aggregated
    price levels; the two coincide when industry prices are uniform within
    a commodity's supplying mix.

    V is inflated to ``cfg.model_base_year`` when ``cfg.apply_inflation_to_V``
    is set; the V-norm weights then reflect supplier mix at the model year
    rather than at ``cfg.usa_base_io_data_year``.
    """
    # local import to avoid a circular dependency on transform.eeio
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Vnorm_scrap_corrected,
    )

    cfg = get_usa_config()
    industry_ratio = get_cornerstone_industry_price_ratio(original_year, target_year)
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected(
        apply_inflation=cfg.apply_inflation_to_V,
        target_year=cfg.model_base_year,
    )
    aligned = industry_ratio.reindex(Vnorm.index, fill_value=1.0)

    # Normalize V_norm columns to sum to 1 so the dot-product is a true weighted
    # *average*, not a weighted *sum*. Scrap correction in
    # `derive_cornerstone_Vnorm_scrap_corrected` is a row-axis scaling that
    # leaves column sums slightly > 1 — using the un-renormalized form would
    # produce a non-1 ratio at year=year (off by the column-sum drift, ~5–7%).
    # Done locally here rather than upstream because A/B matrix consumers want
    # the un-renormalized scrap-corrected V_norm.
    column_sums = Vnorm.sum(axis=0)
    weights = Vnorm.divide(column_sums.where(column_sums > 1e-9, 1.0), axis=1)
    commodity_ratio = aligned @ weights

    # Commodities with no industry coverage (V_norm column ≈ 0, e.g. S00402
    # used goods) yield a 0 weighted-average, which would inject zeros into
    # diag(p) @ A @ diag(1/p). Fall back to the industry ratio, mirroring the
    # neutral 1.0 default in get_cornerstone_industry_price_ratio.
    no_coverage = column_sums < 1e-9
    fallback = industry_ratio.reindex(commodity_ratio.index, fill_value=1.0)
    commodity_ratio = commodity_ratio.where(~no_coverage, fallback)

    return commodity_ratio.reindex(CORNERSTONE_COMMODITIES, fill_value=1.0)


def inflate_cornerstone_A_matrix_with_commodity_pi(
    A: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    """Same `diag(p) @ A @ diag(1/p)` form as ``inflate_cornerstone_A_matrix``,
    but with the V-norm-derived commodity price ratio.
    """
    price_ratio = get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)
    return pd.DataFrame(
        (np.diag(price_ratio) @ A @ np.diag(1 / price_ratio)).values,
        index=A.index,
        columns=A.columns,
    )


def inflate_cornerstone_q_or_y_with_commodity_pi(
    q_or_y: pd.Series[float], original_year: int, target_year: int
) -> pd.Series[float]:
    price_ratio = get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)
    return q_or_y * price_ratio.reindex(q_or_y.index, fill_value=1.0)


def inflate_cornerstone_B_matrix_with_industry_pi(
    B: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_ratio = get_cornerstone_industry_price_ratio(target_year, original_year)
    return B * price_ratio.reindex(B.columns, fill_value=1.0).values
