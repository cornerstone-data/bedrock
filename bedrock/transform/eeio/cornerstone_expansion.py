"""BEA/CEDA ↔ Cornerstone taxonomy correspondence and matrix expansion utilities.

Loads and caches binary correspondence matrices between BEA 2017 (or CEDA v7)
and Cornerstone 2026 taxonomies, and provides functions to *expand*
BEA-space or CEDA-space matrices/vectors to the 405-sector Cornerstone space.

Expansion covers both taxonomy directions:

- **One-to-many disaggregation** (e.g. waste ``562000`` → seven children):
  ``expand_vector`` / ``expand_square_matrix`` **duplicate** the BEA parent onto
  each Cornerstone child so per-unit intensities are preserved. Waste
  subsectors then get special intragroup treatment (and, for industry gross
  output, a V-share redistribute) to prevent Leontief-inverse inflation.
- **Many-to-one aggregation** (e.g. government electric utilities into
  ``221100``): ``expand_industry_output_vector`` **sums** all BEA parents via
  the raw industry correspondence. Reverse-map helpers keep a single parent
  for intensity-preserving matrix expand and must not be used for industry
  gross-output vectors.
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.utils.taxonomy.cornerstone.commodities import (
    COMMODITIES as _CS_COMMODITIES,
)
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES as _CS_INDUSTRIES
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    load_ceda_v7_commodity__cornerstone_commodity_correspondence,
    load_usa_2017_commodity__cornerstone_commodity_correspondence,
    load_usa_2017_industry__cornerstone_industry_correspondence,
)

# ---------------------------------------------------------------------------
# Correspondence loading / normalisation
# ---------------------------------------------------------------------------

CS_COMMODITY_LIST: list[str] = list(_CS_COMMODITIES)
CS_INDUSTRY_LIST: list[str] = list(_CS_INDUSTRIES)


@functools.cache
def commodity_corresp_raw() -> pd.DataFrame:
    """Raw binary (Cornerstone_commodity × BEA_2017_commodity) correspondence."""
    return load_usa_2017_commodity__cornerstone_commodity_correspondence()


@functools.cache
def industry_corresp_raw() -> pd.DataFrame:
    """Raw binary (Cornerstone_industry × BEA_2017_industry) correspondence."""
    return load_usa_2017_industry__cornerstone_industry_correspondence()


def _col_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Column-normalize so each BEA code's total is distributed (not duplicated)."""
    col_sums = df.sum(axis=0)
    return df.div(col_sums.replace(0, 1), axis=1)


@functools.cache
def commodity_corresp() -> pd.DataFrame:
    """Column-normalized commodity correspondence.

    Ensures one-to-many BEA→cornerstone splits (e.g. waste 562000 → 7 subsectors)
    distribute values proportionally rather than duplicating.
    """
    return _col_normalize(commodity_corresp_raw())


@functools.cache
def industry_corresp() -> pd.DataFrame:
    """Column-normalized industry correspondence.

    Same principle as commodity: one-to-many splits distribute proportionally.
    """
    return _col_normalize(industry_corresp_raw())


# ---------------------------------------------------------------------------
# Reverse maps  (Cornerstone code → BEA parent code)
# ---------------------------------------------------------------------------


def _build_reverse_map(corresp: pd.DataFrame) -> dict[str, str]:
    """Build {new_code: bea_parent_code} from a binary correspondence matrix."""
    mapping: dict[str, str] = {}
    for code in corresp.index:
        bea_hits = corresp.columns[corresp.loc[code] > 0].tolist()
        if bea_hits:
            mapping[code] = bea_hits[0]
    return mapping


@functools.cache
def cs_commodity_to_bea_map() -> dict[str, str]:
    """Map each Cornerstone commodity to its BEA 2017 parent commodity code."""
    return _build_reverse_map(commodity_corresp_raw())


@functools.cache
def cs_industry_to_bea_map() -> dict[str, str]:
    """Map each Cornerstone industry to its BEA 2017 parent industry code."""
    return _build_reverse_map(industry_corresp_raw())


# ---------------------------------------------------------------------------
# CEDA v7 → Cornerstone correspondence
# ---------------------------------------------------------------------------


@functools.cache
def ceda_commodity_corresp_raw() -> pd.DataFrame:
    """Raw binary (Cornerstone_commodity × CEDA_v7_commodity) correspondence."""
    return load_ceda_v7_commodity__cornerstone_commodity_correspondence()


@functools.cache
def ceda_commodity_corresp() -> pd.DataFrame:
    """Column-normalized CEDA v7 → Cornerstone commodity correspondence.

    Ensures one-to-many CEDA→Cornerstone splits (e.g. waste 562000 → 7 subsectors)
    distribute values proportionally rather than duplicating.
    """
    return _col_normalize(ceda_commodity_corresp_raw())


@functools.cache
def cs_commodity_to_ceda_map() -> dict[str, str]:
    """Map each Cornerstone commodity to its CEDA v7 parent commodity code."""
    return _build_reverse_map(ceda_commodity_corresp_raw())


# ---------------------------------------------------------------------------
# Expansion helpers
# ---------------------------------------------------------------------------


def _valid_pairs(
    target_codes: list[str],
    code_map: dict[str, str],
    valid_labels: pd.Index,
) -> tuple[list[str], list[str]]:
    """Return (cs_codes, bea_codes) for Cornerstone codes whose BEA parent is in *valid_labels*."""
    pairs = [
        (cs, code_map[cs])
        for cs in target_codes
        if cs in code_map and code_map[cs] in valid_labels
    ]
    return [c for c, _ in pairs], [b for _, b in pairs]


def _apply_waste_intragroup_treatment(expanded: pd.DataFrame) -> None:
    """Zero cross-terms and divide non-group entries for waste subsectors in-place.

    Only applies to waste — these have no real disaggregated data, so the BEA
    parent value is duplicated.  Other disaggregations (e.g. aluminum) have
    real correspondence-informed splits and are left as-is.
    """
    from bedrock.utils.taxonomy.cornerstone.commodities import (  # noqa: PLC0415
        WASTE_DISAGG_COMMODITIES,
    )

    for _old_code, new_codes in WASTE_DISAGG_COMMODITIES.items():
        siblings = [c for c in new_codes if c in expanded.index]
        n = len(siblings)
        if n <= 1:
            continue
        for i in siblings:
            for j in siblings:
                if i != j:
                    expanded.loc[i, j] = 0.0
        sibling_set = set(siblings)
        non_siblings = [c for c in expanded.index if c not in sibling_set]
        for s in siblings:
            expanded.loc[non_siblings, s] /= n
            expanded.loc[s, non_siblings] /= n  # type: ignore[index]


def expand_square_matrix(
    M: pd.DataFrame,
    target_codes: list[str],
    code_map: dict[str, str],
    *,
    zero_intragroup_cross_terms: bool = False,
) -> pd.DataFrame:
    """Expand a BEA square matrix to Cornerstone space.

    Rows and columns for disaggregated sectors are **duplicated** (not split).
    When *zero_intragroup_cross_terms* is True, waste subsector cross-terms
    are zeroed and non-group entries divided by n to prevent L inflation.
    """
    cs_valid, bea_valid = _valid_pairs(target_codes, code_map, M.index)

    expanded = M.loc[bea_valid, bea_valid].copy()
    expanded.index = cs_valid
    expanded.columns = cs_valid
    expanded = expanded.reindex(
        index=target_codes, columns=target_codes, fill_value=0.0
    )

    if zero_intragroup_cross_terms:
        _apply_waste_intragroup_treatment(expanded)

    expanded.index.name = 'sector'
    expanded.columns.name = 'sector'
    return expanded


def expand_vector(
    v: pd.Series[float],
    target_codes: list[str],
    code_map: dict[str, str],
) -> pd.Series[float]:
    """Expand a BEA vector to Cornerstone by duplicating entries.

    Uses a single-parent reverse map: one-to-many children each receive the
    full parent value; many-to-one Cornerstone rows keep only the first BEA
    parent.

    Raises ``ValueError`` if *code_map* is the cached industry reverse map
    from :func:`cs_industry_to_bea_map`. That map silently drops secondary
    parents on aggregates (government enterprises into ``221100`` /
    ``485000``). Industry gross output must use
    :func:`expand_industry_output_vector` instead. Commodity expand and
    matrix helpers that need first-parent duplication are unaffected
    (they do not pass the industry reverse map into this function).
    """
    if code_map is cs_industry_to_bea_map():
        raise ValueError(
            "expand_vector with cs_industry_to_bea_map() drops many-to-one BEA "
            "parents (e.g. S00101/S00202 into 221100). Use "
            "expand_industry_output_vector for industry gross output."
        )
    cs_valid, bea_valid = _valid_pairs(target_codes, code_map, v.index)

    expanded = v.loc[bea_valid].copy()
    expanded.index = cs_valid
    return expanded.reindex(target_codes, fill_value=0.0)


def expand_industry_output_vector(x_bea: pd.Series[float]) -> pd.Series[float]:
    """Expand BEA industry gross output to Cornerstone industries.

    Multiplies by the raw binary industry correspondence
    (Cornerstone × BEA) so:

    - many-to-one rows (``221100`` ← private + gov utilities; ``485000`` ←
      private + gov transit) **sum** parent gross output;
    - one-to-many rows (waste children ← ``562000``) **duplicate** the parent
      scalar onto each child (same intermediate as :func:`expand_vector`).

    The waste duplication is **not** final industry output: callers must run
    ``_distribute_waste_parent_x_using_v_row_shares`` (or equivalent) so the
    waste children share a single copy of parent gross output rather than
    counting it seven times. Without that step, ``sum(x)`` overstates waste.
    """
    corresp = industry_corresp_raw()
    x_aligned = x_bea.reindex(corresp.columns, fill_value=0.0)
    expanded = corresp @ x_aligned
    return expanded.reindex(CS_INDUSTRY_LIST, fill_value=0.0)


def expand_ghg_matrix_from_bea_to_cornerstone(
    M: pd.DataFrame,
    target_col_codes: list[str],
    col_map: dict[str, str],
) -> pd.DataFrame:
    """Expand a (ghg × BEA_sector) matrix to Cornerstone columns."""
    cs_valid, bea_valid = _valid_pairs(target_col_codes, col_map, M.columns)

    expanded = M.loc[:, bea_valid].copy()
    expanded.columns = cs_valid
    expanded = expanded.reindex(columns=target_col_codes, fill_value=0.0)
    expanded.index.name = 'ghg'
    expanded.columns.name = 'sector'
    return expanded


def expand_ghg_matrix_from_ceda_to_cornerstone(
    M: pd.DataFrame,
    target_col_codes: list[str],
) -> pd.DataFrame:
    """Expand a (ghg × CEDA_v7_sector) matrix to Cornerstone columns.

    Uses matrix multiplication with the column-normalized CEDA→Cornerstone
    correspondence.  Many-to-one cases (e.g. 4 appliance CEDA codes → 1
    Cornerstone code) are summed; one-to-many cases (waste) are distributed
    proportionally.
    """
    corresp = ceda_commodity_corresp()  # (Cornerstone × CEDA), col-normalized
    # M is (ghg × CEDA), corresp.T is (CEDA × Cornerstone)
    expanded = M.reindex(columns=corresp.columns, fill_value=0.0) @ corresp.T
    expanded = expanded.reindex(columns=target_col_codes, fill_value=0.0)
    expanded.index.name = 'ghg'
    expanded.columns.name = 'sector'
    return expanded
