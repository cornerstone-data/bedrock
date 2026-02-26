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

Public weight matrix builders and their intended consumers:
- build_make_weight_matrix_for_cornerstone → A matrix (commodity × commodity)
- build_use_weight_matrix_for_cornerstone  → A matrix (commodity × commodity)
- build_V_weight_matrix_for_cornerstone    → V matrix (industry × commodity)
- build_U_weight_matrix_for_cornerstone    → U matrix (commodity × industry)
- build_Y_weight_vector_for_cornerstone    → Y vectors (commodity)
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import cast

import pandas as pd

from bedrock.utils.config.eeio_disaggregation_config import (
    get_waste_disaggregation_make_weight_path,
    get_waste_disaggregation_use_weight_path,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS
from bedrock.utils.taxonomy.cornerstone.industries import (
    INDUSTRIES,
    WASTE_DISAGG_INDUSTRIES,
)
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

WASTE_SUB_SECTORS = WASTE_DISAGG_INDUSTRIES["562000"]
EQUAL_SPLIT_WEIGHT = 1.0 / 7

_VALID_SECTORS = (
    frozenset(COMMODITIES)
    | {"562000"}
    | frozenset(FINAL_DEMANDS)
    | frozenset(VALUE_ADDEDS)
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_sector_code(code: str) -> str:
    """Strip /US (or /country) suffix from sector codes."""
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


def _extract_sum_weights(sum_df: pd.DataFrame, waste_set: set[str]) -> dict[str, float]:
    """Extract waste sub-sector proportions from col_sum or row_sum data.

    Handles both CSV orientations:
    - Make col_sum: (562000, waste_xxx)
    - Use col_sum: (waste_xxx, 562000)
    Returns {waste_code: proportion}.
    """
    weights: dict[str, float] = {}
    for idx, row in sum_df.iterrows():
        a, b = cast(tuple[str, str], idx)
        w = float(row["weight"])
        if w <= 0:
            w = EQUAL_SPLIT_WEIGHT
        if a == "562000" and b in waste_set:
            weights[b] = w
        elif b == "562000" and a in waste_set:
            weights[a] = w
    return weights


def _normalize_alloc_weights(
    weights: dict[str, float],
) -> dict[str, float]:
    """Ensure all 7 waste sub-sectors have a weight, then normalize to sum=1."""
    for s in WASTE_SUB_SECTORS:
        if s not in weights:
            weights[s] = EQUAL_SPLIT_WEIGHT
    total = sum(weights[s] for s in WASTE_SUB_SECTORS)
    if total > 0:
        return {s: weights[s] / total for s in WASTE_SUB_SECTORS}
    return {s: EQUAL_SPLIT_WEIGHT for s in WASTE_SUB_SECTORS}


# ---------------------------------------------------------------------------
# Raw CSV loaders
# ---------------------------------------------------------------------------


@functools.cache
def load_waste_make_weights(year: int = 2017) -> pd.DataFrame:
    """Load Make table weights. MultiIndex (industry, commodity), column 'weight'."""
    path = get_waste_disaggregation_make_weight_path()
    if not path.exists():
        raise FileNotFoundError(f"Make weight file not found: {path}")
    df = _load_raw_weights(path, value_col="PercentMake")
    df = df[
        df["industry"].isin(_VALID_SECTORS) & df["commodity"].isin(_VALID_SECTORS)
    ].copy()
    return df.set_index(["industry", "commodity"])


@functools.cache
def load_waste_use_weights(year: int = 2017) -> pd.DataFrame:
    """Load Use table weights. MultiIndex (industry, commodity), column 'weight'."""
    path = get_waste_disaggregation_use_weight_path()
    if not path.exists():
        raise FileNotFoundError(f"Use weight file not found: {path}")
    df = _load_raw_weights(path, value_col="PercentUsed")
    df = df[
        df["industry"].isin(_VALID_SECTORS) & df["commodity"].isin(_VALID_SECTORS)
    ].copy()
    return df.set_index(["industry", "commodity"])


# ---------------------------------------------------------------------------
# Generalized weight matrix builder
# ---------------------------------------------------------------------------


def _build_weight_matrix(
    weights_df: pd.DataFrame,
    row_sectors: list[str],
    col_sectors: list[str],
    row_alloc_note: str,
    col_alloc_note: str,
    intersection_note: str,
    row_disagg_note: str,
    col_disagg_note: str,
    transpose: bool = False,
) -> pd.DataFrame:
    """Build a weight matrix for structural reflection.

    Args:
        weights_df: Loaded weights with MultiIndex (industry, commodity).
        row_sectors: Sector codes for the matrix rows.
        col_sectors: Sector codes for the matrix columns.
        row_alloc_note: Note pattern for proportions used in row fallback
            (waste_row × non-waste_col).
        col_alloc_note: Note pattern for proportions used in column fallback
            (non-waste_row × waste_col).
        intersection_note: Note pattern for 7×7 waste block.
        row_disagg_note: Note pattern for explicit waste_row × non-waste_col.
        col_disagg_note: Note pattern for explicit non-waste_row × waste_col.
        transpose: When True, CSV (industry, commodity) is placed as
            (commodity, industry) in the result — needed for U (com × ind).
    """
    rows = pd.Index(row_sectors)
    cols = pd.Index(col_sectors)
    rows_set: set[str] = set(rows)
    cols_set: set[str] = set(cols)
    waste_set: set[str] = set(WASTE_SUB_SECTORS)
    result = pd.DataFrame(0.0, index=rows, columns=cols)

    # Identity for non-waste sectors present on both axes
    for s in rows_set & cols_set - waste_set:
        result.loc[s, s] = 1.0

    def _pos(ind: str, com: str) -> tuple[str, str]:
        return (com, ind) if transpose else (ind, com)

    # Filter by Note type
    def _filter(note: str) -> pd.DataFrame:
        return weights_df[weights_df["Note"].str.contains(note, case=False, na=False)]

    row_alloc_df = _filter(row_alloc_note)
    col_alloc_df = _filter(col_alloc_note)
    intersection_df = _filter(intersection_note)
    row_disagg_df = _filter(row_disagg_note)
    col_disagg_df = _filter(col_disagg_note)

    # Allocation proportions (orientation-agnostic extraction)
    row_alloc = _normalize_alloc_weights(_extract_sum_weights(row_alloc_df, waste_set))
    col_alloc = _normalize_alloc_weights(_extract_sum_weights(col_alloc_df, waste_set))

    # Track sectors with explicit disaggregation data
    row_disagg_cols: set[str] = set()
    col_disagg_rows: set[str] = set()

    for entry in row_disagg_df.index:
        ind, com = cast(tuple[str, str], entry)
        r, c = _pos(ind, com)
        if r in waste_set and c not in waste_set and c in cols_set:
            row_disagg_cols.add(c)

    for entry in col_disagg_df.index:
        ind, com = cast(tuple[str, str], entry)
        r, c = _pos(ind, com)
        if r not in waste_set and c in waste_set and r in rows_set:
            col_disagg_rows.add(r)

    # 7×7 intersection block
    for idx, row in intersection_df.iterrows():
        ind, com = cast(tuple[str, str], idx)
        if ind in waste_set and com in waste_set:
            r, c = _pos(ind, com)
            if r in rows_set and c in cols_set:
                w = float(row["weight"])
                result.loc[r, c] = w if w > 0 else EQUAL_SPLIT_WEIGHT

    # Fallback zeros in 7×7
    waste_in_rows: list[str] = [s for s in WASTE_SUB_SECTORS if s in rows_set]
    waste_in_cols: list[str] = [s for s in WASTE_SUB_SECTORS if s in cols_set]
    for i in waste_in_rows:
        for j in waste_in_cols:
            if result.loc[i, j] == 0:
                result.loc[i, j] = EQUAL_SPLIT_WEIGHT
    block = result.loc[waste_in_rows, waste_in_cols]
    block_sum = block.sum().sum()
    if block_sum > 0:
        result.loc[waste_in_rows, waste_in_cols] = block / block_sum

    # Explicit row disagg: waste_row × non-waste_col
    for idx, row in row_disagg_df.iterrows():
        ind, com = cast(tuple[str, str], idx)
        r, c = _pos(ind, com)
        if r in waste_set and c not in waste_set and r in rows_set and c in cols_set:
            w = float(row["weight"])
            result.loc[r, c] = w if w > 0 else row_alloc.get(r, EQUAL_SPLIT_WEIGHT)

    # Fallback: waste_row × non-waste_col without explicit data
    for i in waste_in_rows:
        for j in cols:
            if j not in waste_set and j not in row_disagg_cols:
                if result.loc[i, j] == 0:
                    result.loc[i, j] = row_alloc[i]

    # Explicit col disagg: non-waste_row × waste_col
    for idx, row in col_disagg_df.iterrows():
        ind, com = cast(tuple[str, str], idx)
        r, c = _pos(ind, com)
        if r not in waste_set and c in waste_set and r in rows_set and c in cols_set:
            w = float(row["weight"])
            result.loc[r, c] = w if w > 0 else col_alloc.get(c, EQUAL_SPLIT_WEIGHT)

    # Fallback: non-waste_row × waste_col without explicit data
    for i in rows:
        if i not in waste_set and i not in col_disagg_rows:
            for j in waste_in_cols:
                if result.loc[i, j] == 0:
                    result.loc[i, j] = col_alloc[j]

    return result


# ---------------------------------------------------------------------------
# A matrix weights (commodity × commodity)
# ---------------------------------------------------------------------------


@functools.cache
def build_make_weight_matrix_for_cornerstone() -> pd.DataFrame:
    """Make weight matrix for A: COMMODITIES × COMMODITIES.

    For structural_reflect_matrix with commodity correspondence on both axes.
    """
    return _build_weight_matrix(
        load_waste_make_weights(),
        row_sectors=list(COMMODITIES),
        col_sectors=list(COMMODITIES),
        row_alloc_note="Make column sum",
        col_alloc_note="Make column sum",
        intersection_note="Make table intersection",
        row_disagg_note="industry disaggregation",
        col_disagg_note="commodity disaggregation",
    )


@functools.cache
def build_use_weight_matrix_for_cornerstone() -> pd.DataFrame:
    """Use weight matrix for A: COMMODITIES × COMMODITIES.

    For structural_reflect_matrix with commodity correspondence on both axes.
    """
    return _build_weight_matrix(
        load_waste_use_weights(),
        row_sectors=list(COMMODITIES),
        col_sectors=list(COMMODITIES),
        row_alloc_note="Use row sum",
        col_alloc_note="Use row sum",
        intersection_note="Use table intersection",
        row_disagg_note="Commodity disaggregation",
        col_disagg_note="Commodity disaggregation",
    )


# ---------------------------------------------------------------------------
# V matrix weights (industry × commodity)
# ---------------------------------------------------------------------------


@functools.cache
def build_V_weight_matrix_for_cornerstone() -> pd.DataFrame:
    """Make weight matrix for V: INDUSTRIES × COMMODITIES.

    For structural_reflect_matrix with industry_corresp (rows) and
    commodity_corresp (columns).
    """
    return _build_weight_matrix(
        load_waste_make_weights(),
        row_sectors=list(INDUSTRIES),
        col_sectors=list(COMMODITIES),
        row_alloc_note="Make column sum",
        col_alloc_note="Make column sum",
        intersection_note="Make table intersection",
        row_disagg_note="industry disaggregation",
        col_disagg_note="commodity disaggregation",
    )


# ---------------------------------------------------------------------------
# U matrix weights (commodity × industry)
# ---------------------------------------------------------------------------


@functools.cache
def build_U_weight_matrix_for_cornerstone() -> pd.DataFrame:
    """Use weight matrix for U: COMMODITIES × INDUSTRIES.

    For structural_reflect_matrix with commodity_corresp (rows) and
    industry_corresp (columns). transpose=True maps CSV (industry, commodity)
    to result (commodity, industry).

    Allocation notes are swapped relative to the CSV orientation:
    - row_alloc = "Use row sum" → commodity proportions for commodity rows
    - col_alloc = "Use column sum" → industry proportions for industry columns
    """
    return _build_weight_matrix(
        load_waste_use_weights(),
        row_sectors=list(COMMODITIES),
        col_sectors=list(INDUSTRIES),
        row_alloc_note="Use row sum",
        col_alloc_note="Use column sum",
        intersection_note="Use table intersection",
        row_disagg_note="Commodity disaggregation",
        col_disagg_note="VA disaggregation",
        transpose=True,
    )


# ---------------------------------------------------------------------------
# Y vector weights (commodity)
# ---------------------------------------------------------------------------


@functools.cache
def build_Y_weight_vector_for_cornerstone() -> pd.Series[float]:
    """Commodity-proportion vector for disaggregating Y vectors.

    Non-waste sectors get 1.0; waste sub-sectors get their commodity proportion
    from Use row_sum (how commodity 562000 splits across waste commodities).
    Suitable for structural_reflect_vector or direct proportion-based splitting.
    """
    waste_set: set[str] = set(WASTE_SUB_SECTORS)
    use_df = load_waste_use_weights()
    row_sum_df = use_df[
        use_df["Note"].str.contains("Use row sum", case=False, na=False)
    ]
    raw = _extract_sum_weights(row_sum_df, waste_set)
    alloc = _normalize_alloc_weights(raw)

    weights = pd.Series(1.0, index=pd.Index(list(COMMODITIES), name="sector"))
    for s in WASTE_SUB_SECTORS:
        if s in weights.index:
            weights.loc[s] = alloc[s]
    return weights
