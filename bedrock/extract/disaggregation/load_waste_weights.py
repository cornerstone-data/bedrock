"""Load waste disaggregation weights from Make and Use table CSV files.

Weight data is used to allocate sector 562000 (Waste management and remediation)
to 7 sub-sectors (562111, 562HAZ, 562212, 562213, 562910, 562920, 562OTH) in
the cornerstone EEIO schema.

Note types in the CSV files:
- Make: "Make column sum" (562000→7 allocation), "Make table intersection" (7×7),
  "commodity disaggregation", "industry disaggregation"
- Use: "Use column sum, industry output", "Use row sum, commodity output",
  "Use table intersection", "Commodity disaggregation", "VA disaggregation"

Rows are kept when industry and commodity are in cornerstone COMMODITIES,
FINAL_DEMANDS, VALUE_ADDEDS, or 562000 (source). Other codes (e.g. S00101,
S00401) may be excluded. See implementation-steps_v2.md.
"""

from __future__ import annotations

import functools
from pathlib import Path

import pandas as pd

from bedrock.utils.config.eeio_disaggregation_config import (
    get_waste_disaggregation_make_weight_path,
    get_waste_disaggregation_use_weight_path,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS
from bedrock.utils.taxonomy.cornerstone.industries import WASTE_DISAGG_INDUSTRIES
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

# Waste sub-sectors in cornerstone (562000 is replaced by these 7)
WASTE_SUB_SECTORS = WASTE_DISAGG_INDUSTRIES["562000"]
EQUAL_SPLIT_WEIGHT = 1.0 / 7  # Fallback when weight is missing or zero

# Valid sectors for filtering: cornerstone sectors, 562000 (source), final demand, value added
_VALID_SECTORS = (
    frozenset(COMMODITIES)
    | {"562000"}
    | frozenset(FINAL_DEMANDS)
    | frozenset(VALUE_ADDEDS)
)


def _normalize_sector_code(code: str) -> str:
    """Normalize sector code from weight CSV to match target sector format.

    Input codes use /US suffix (e.g. 562000/US). Strip the suffix to align
    with cornerstone sector codes (e.g. 562000, 562111).

    Args:
        code: Raw code from CSV (may include /US or /country suffix)

    Returns:
        Normalized code without suffix
    """
    if "/" in code:
        return code.split("/")[0]
    return code


def _load_raw_weights(
    path: Path,
    value_col: str,
    industry_col: str = "IndustryCode",
    commodity_col: str = "CommodityCode",
) -> pd.DataFrame:
    """Load raw weight DataFrame from CSV with normalized codes."""
    df = pd.read_csv(path)
    df["industry"] = df[industry_col].astype(str).apply(_normalize_sector_code)
    df["commodity"] = df[commodity_col].astype(str).apply(_normalize_sector_code)
    df = df.rename(columns={value_col: "weight"})
    return df[["industry", "commodity", "weight", "Note"]]


@functools.cache
def load_waste_make_weights(year: int = 2017) -> pd.DataFrame:
    """Load Make table weights for waste disaggregation.

    Returns DataFrame with MultiIndex (industry, commodity) and column 'weight'.
    Filtered to rows where both industry and commodity are in cornerstone
    COMMODITIES, FINAL_DEMANDS, VALUE_ADDEDS, or 562000 (source).

    Note types: Make column sum, Make table intersection, commodity
    disaggregation, industry disaggregation.

    Args:
        year: Data year (2017 supported)

    Returns:
        DataFrame with columns: weight, Note (index: industry, commodity)
    """
    path = get_waste_disaggregation_make_weight_path()
    if not path.exists():
        raise FileNotFoundError(f"Make weight file not found: {path}")
    df = _load_raw_weights(path, value_col="PercentMake")
    df = df[
        df["industry"].isin(_VALID_SECTORS) & df["commodity"].isin(_VALID_SECTORS)
    ].copy()
    df = df.set_index(["industry", "commodity"])
    return df


@functools.cache
def load_waste_use_weights(year: int = 2017) -> pd.DataFrame:
    """Load Use table weights for waste disaggregation.

    Returns DataFrame with MultiIndex (industry, commodity) and column 'weight'.
    Filtered to rows where both industry and commodity are in cornerstone
    COMMODITIES, FINAL_DEMANDS, VALUE_ADDEDS, or 562000 (source).

    Note types: Use column sum, Use row sum, Use table intersection,
    Commodity disaggregation, VA disaggregation.

    Args:
        year: Data year (2017 supported)

    Returns:
        DataFrame with columns: weight, Note (index: industry, commodity)
    """
    path = get_waste_disaggregation_use_weight_path()
    if not path.exists():
        raise FileNotFoundError(f"Use weight file not found: {path}")
    df = _load_raw_weights(path, value_col="PercentUsed")
    df = df[
        df["industry"].isin(_VALID_SECTORS) & df["commodity"].isin(_VALID_SECTORS)
    ].copy()
    df = df.set_index(["industry", "commodity"])
    return df


def _build_weight_matrix(
    weights_df: pd.DataFrame,
    sector_list: list[str],
    col_sum_note: str,
    row_sum_note: str,
    intersection_note: str,
    industry_disagg_note: str,
    commodity_disagg_note: str,
) -> pd.DataFrame:
    """Build square weight matrix for structural reflection.

    Index and columns = sector_list (cornerstone sectors).
    - Identity (1.0) for non-waste diagonal cells
    - 7×7 block: intersection weights
    - waste × non-waste: industry disaggregation (or col_sum fallback)
    - non-waste × waste: commodity disaggregation (or row_sum fallback)
    - Fallback: equal split (1/7) when weight is zero or missing
    """
    sectors = pd.Index(sector_list)
    waste_set = set(WASTE_SUB_SECTORS)
    result = pd.DataFrame(0.0, index=sectors, columns=sectors)

    # Identity for non-waste sectors
    for s in sectors:
        if s not in waste_set:
            result.loc[s, s] = 1.0

    # Extract weights by Note type
    col_sum = weights_df[
        weights_df["Note"].str.contains(col_sum_note, case=False, na=False)
    ]
    row_sum = weights_df[
        weights_df["Note"].str.contains(row_sum_note, case=False, na=False)
    ]
    intersection = weights_df[
        weights_df["Note"].str.contains(intersection_note, case=False, na=False)
    ]
    industry_disagg = weights_df[
        weights_df["Note"].str.contains(industry_disagg_note, case=False, na=False)
    ]
    commodity_disagg = weights_df[
        weights_df["Note"].str.contains(commodity_disagg_note, case=False, na=False)
    ]

    # Col sum: 562000 (industry) → 562xxx (commodities). Used when splitting
    # 562000 row to 7 rows. Fallback for waste × non-waste.
    col_sum_weights = {}
    for idx, row in col_sum.iterrows():
        ind, com = idx[0], idx[1]
        if ind == "562000" and com in waste_set:
            w = float(row["weight"])
            col_sum_weights[com] = w if w > 0 else EQUAL_SPLIT_WEIGHT

    # Row sum: 562000 (commodity) → 562xxx (industries). Used when splitting
    # 562000 column to 7 columns. Fallback for non-waste × waste.
    # Make table has no row sum; Use table has "Use row sum".
    row_sum_weights = {}
    for idx, row in row_sum.iterrows():
        ind, com = idx[0], idx[1]
        if com == "562000" and ind in waste_set:
            w = float(row["weight"])
            row_sum_weights[ind] = w if w > 0 else EQUAL_SPLIT_WEIGHT

    # If no row sum (e.g. Make table), use col_sum as proxy for column allocation
    if not row_sum_weights and col_sum_weights:
        row_sum_weights = dict(col_sum_weights)

    # Normalize and apply fallback
    for s in WASTE_SUB_SECTORS:
        if s not in col_sum_weights:
            col_sum_weights[s] = EQUAL_SPLIT_WEIGHT
        if s not in row_sum_weights:
            row_sum_weights[s] = EQUAL_SPLIT_WEIGHT
    col_total = sum(col_sum_weights[s] for s in WASTE_SUB_SECTORS)
    row_total = sum(row_sum_weights[s] for s in WASTE_SUB_SECTORS)
    if col_total > 0:
        col_sum_weights = {s: col_sum_weights[s] / col_total for s in WASTE_SUB_SECTORS}
    else:
        col_sum_weights = {s: EQUAL_SPLIT_WEIGHT for s in WASTE_SUB_SECTORS}
    if row_total > 0:
        row_sum_weights = {s: row_sum_weights[s] / row_total for s in WASTE_SUB_SECTORS}
    else:
        row_sum_weights = {s: EQUAL_SPLIT_WEIGHT for s in WASTE_SUB_SECTORS}

    # 7×7 intersection block
    for idx, row in intersection.iterrows():
        ind, com = idx[0], idx[1]
        if ind in waste_set and com in waste_set:
            w = float(row["weight"])
            result.loc[ind, com] = w if w > 0 else EQUAL_SPLIT_WEIGHT

    # Fallback for zeros in 7×7: equal split
    for i in WASTE_SUB_SECTORS:
        for j in WASTE_SUB_SECTORS:
            if result.loc[i, j] == 0:
                result.loc[i, j] = EQUAL_SPLIT_WEIGHT
    block_sum = result.loc[WASTE_SUB_SECTORS, WASTE_SUB_SECTORS].sum().sum()
    if block_sum > 0:
        result.loc[WASTE_SUB_SECTORS, WASTE_SUB_SECTORS] /= block_sum

    # waste × non-waste: industry disaggregation, fallback to col_sum
    for idx, row in industry_disagg.iterrows():
        ind, com = idx[0], idx[1]
        if ind in waste_set and com in sectors and com not in waste_set:
            w = float(row["weight"])
            result.loc[ind, com] = (
                w if w > 0 else col_sum_weights.get(ind, EQUAL_SPLIT_WEIGHT)
            )
    for i in WASTE_SUB_SECTORS:
        for j in sectors:
            if j not in waste_set and result.loc[i, j] == 0:
                result.loc[i, j] = col_sum_weights[i]

    # non-waste × waste: commodity disaggregation, fallback to row_sum
    for idx, row in commodity_disagg.iterrows():
        ind, com = idx[0], idx[1]
        if com in waste_set and ind in sectors and ind not in waste_set:
            w = float(row["weight"])
            result.loc[ind, com] = (
                w if w > 0 else row_sum_weights.get(com, EQUAL_SPLIT_WEIGHT)
            )
    for i in sectors:
        if i not in waste_set:
            for j in WASTE_SUB_SECTORS:
                if result.loc[i, j] == 0:
                    result.loc[i, j] = row_sum_weights[j]

    return result


@functools.cache
def build_make_weight_matrix_for_cornerstone() -> pd.DataFrame:
    """Build Make table weight matrix for cornerstone structural reflection.

    Returns (COMMODITIES × COMMODITIES) DataFrame. Identity for non-waste;
    7×7 waste block from Make table intersection; waste×non-waste from
    industry disaggregation; non-waste×waste from commodity disaggregation.
    Fallback: equal split (1/7) when weight is missing or zero.

    For use with structural_reflect_matrix: df_weights must have shape
    row_target × col_target (405×405).
    """
    weights = load_waste_make_weights()
    return _build_weight_matrix(
        weights,
        sector_list=list(COMMODITIES),
        col_sum_note="Make column sum",
        row_sum_note="Make column sum",  # Make has no row sum; use col_sum as fallback
        intersection_note="Make table intersection",
        industry_disagg_note="industry disaggregation",
        commodity_disagg_note="commodity disaggregation",
    )


@functools.cache
def build_use_weight_matrix_for_cornerstone() -> pd.DataFrame:
    """Build Use table weight matrix for cornerstone structural reflection.

    Returns (COMMODITIES × COMMODITIES) DataFrame. Same structure as Make.
    Fallback: equal split (1/7) when weight is missing or zero.
    """
    weights = load_waste_use_weights()
    return _build_weight_matrix(
        weights,
        sector_list=list(COMMODITIES),
        col_sum_note="Use column sum",
        row_sum_note="Use row sum",
        intersection_note="Use table intersection",
        industry_disagg_note="VA disaggregation",  # waste×VA (V00100, V00200, V00300)
        commodity_disagg_note="Commodity disaggregation",
    )
