"""SAS Table 2/3 → Cornerstone waste child share vectors."""

from __future__ import annotations

import pandas as pd

from bedrock.extract.disaggregation.waste_static_rules import (
    SAS_NAICS_TO_CORNERSTONE,
    WASTE_CHILDREN,
)
from bedrock.extract.flowbyactivity import getFlowByActivity


def map_sas_naics_to_cornerstone(code: str) -> str | None:
    digits = "".join(c for c in str(code) if c.isdigit())
    if not digits:
        return None
    if digits in SAS_NAICS_TO_CORNERSTONE:
        return SAS_NAICS_TO_CORNERSTONE[digits]
    if len(digits) >= 6 and digits[:6] in SAS_NAICS_TO_CORNERSTONE:
        return SAS_NAICS_TO_CORNERSTONE[digits[:6]]
    if len(digits) >= 5 and digits[:5] in SAS_NAICS_TO_CORNERSTONE:
        return SAS_NAICS_TO_CORNERSTONE[digits[:5]]
    return None


def _child_shares_from_fba(
    fba: pd.DataFrame,
    *,
    value_col: str,
    table_prefix: str,
) -> pd.Series:
    df = fba.copy()
    if "Description" in df.columns:
        df = df[df["Description"].astype(str).str.startswith(table_prefix)]
    if value_col not in df.columns:
        # FlowByActivity often stores amounts in FlowAmount with Activity in name cols
        raise KeyError(f"Expected column {value_col} in SAS FBA")
    # Prefer ActivityProducedBy / ActivityConsumedBy style if present
    naics_col = None
    for cand in ("ActivityProducedBy", "ActivityConsumedBy", "Sector", "NAICS"):
        if cand in df.columns:
            naics_col = cand
            break
    if naics_col is None:
        raise KeyError("No NAICS-like column in SAS FBA")

    totals: dict[str, float] = dict.fromkeys(WASTE_CHILDREN, 0.0)
    for _, row in df.iterrows():
        child = map_sas_naics_to_cornerstone(row[naics_col])
        if child is None:
            continue
        try:
            val = float(row[value_col])
        except (TypeError, ValueError):
            continue
        if val > 0:
            totals[child] += val
    s = pd.Series(totals, dtype=float)
    total = float(s.sum())
    if total <= 0:
        raise ValueError(f"All-zero SAS {table_prefix} waste child totals")
    # Suppressions → zero cells stay zero; caller may fallback if critical children missing
    return s / total


def load_sas_table3_expense_shares(year: int) -> pd.Series:
    """Use column-sum industry mix from SAS Table 3 total Expenses."""
    fba = getFlowByActivity(datasource="Census_SAS", year=year)
    # Parsed SAS puts expense in FlowAmount; filter Table 3 via Description
    df = fba.copy()
    if "Description" in df.columns:
        df = df[df["Description"].astype(str).str.startswith("Table 3")]
    # Expense rows: ActivityConsumedBy holds NAICS in Census_SAS parse
    naics_col = (
        "ActivityConsumedBy"
        if "ActivityConsumedBy" in df.columns
        else "ActivityProducedBy"
    )
    amount_col = "FlowAmount"
    totals: dict[str, float] = dict.fromkeys(WASTE_CHILDREN, 0.0)
    for _, row in df.iterrows():
        child = map_sas_naics_to_cornerstone(row[naics_col])
        if child is None:
            continue
        val = float(row[amount_col]) if pd.notna(row[amount_col]) else 0.0
        if val > 0:
            totals[child] += val
    s = pd.Series(totals, dtype=float)
    total = float(s.sum())
    if total <= 0:
        raise ValueError(f"All-zero SAS Table 3 expense totals for {year}")
    return (s / total).reindex(WASTE_CHILDREN).fillna(0.0)


def load_sas_table2_revenue_shares(year: int) -> pd.Series:
    """SAS Table 2 revenue shares (row-sum proxy / Make ≤2022)."""
    fba = getFlowByActivity(datasource="Census_SAS", year=year)
    df = fba.copy()
    if "Description" in df.columns:
        df = df[df["Description"].astype(str).str.startswith("Table 2")]
    naics_col = (
        "ActivityProducedBy"
        if "ActivityProducedBy" in df.columns
        else "ActivityConsumedBy"
    )
    amount_col = "FlowAmount"
    totals: dict[str, float] = dict.fromkeys(WASTE_CHILDREN, 0.0)
    for _, row in df.iterrows():
        child = map_sas_naics_to_cornerstone(row[naics_col])
        if child is None:
            continue
        val = float(row[amount_col]) if pd.notna(row[amount_col]) else 0.0
        if val > 0:
            totals[child] += val
    s = pd.Series(totals, dtype=float)
    total = float(s.sum())
    if total <= 0:
        raise ValueError(f"All-zero SAS Table 2 revenue totals for {year}")
    return (s / total).reindex(WASTE_CHILDREN).fillna(0.0)
