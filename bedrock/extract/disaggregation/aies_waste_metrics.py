"""AIES EXPS_TOT_DVAL → Cornerstone waste child industry-mix shares."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from bedrock.extract.disaggregation.sas_waste_metrics import (
    map_sas_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.extract.flowbyactivity import getFlowByActivity

AIES_SOURCE = "Census_AIES_Waste_Child_Expenses"


def _shares_from_aies_fba(fba: pd.DataFrame) -> pd.Series:
    df = fba.copy()
    naics_col = None
    for cand in ("ActivityConsumedBy", "ActivityProducedBy", "NAICS", "Sector"):
        if cand in df.columns and df[cand].notna().any():
            naics_col = cand
            break
    if naics_col is None:
        raise KeyError("No NAICS column in AIES waste-child FBA")
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
        raise ValueError("All-zero AIES EXPS_TOT_DVAL waste child totals")
    return (s / total).reindex(WASTE_CHILDREN).fillna(0.0)


def load_aies_child_expense_shares(
    year: int,
    *,
    table: Literal["EXP01", "BASIC"],
) -> pd.Series:
    """Industry-mix shares from AIES total expenses by 6-digit waste NAICS.

    ``table`` is recorded for provenance; the FBA YAML selects EXP01 vs BASIC
    by year. Hard-fail if the pull is EXP02-shaped (aggregate 562 only).
    """
    del table  # year selects dataset in YAML; kept for API clarity / provenance
    fba = getFlowByActivity(datasource=AIES_SOURCE, year=year)
    # Guard: must see 6-digit 562* detail, not only aggregate 562
    naics_vals = []
    for cand in ("ActivityConsumedBy", "ActivityProducedBy"):
        if cand in fba.columns:
            naics_vals = [str(x) for x in fba[cand].dropna().unique()]
            break
    six_digit = [
        n
        for n in naics_vals
        if n.replace(".0", "").isdigit()
        and len(n.replace(".0", "")) == 6
        and n.replace(".0", "").startswith("562")
    ]
    if not six_digit:
        raise ValueError(
            f"AIES waste-child expenses for {year} lack 6-digit 562* codes "
            "(refusing EXP02 / aggregate-only pulls)"
        )
    return _shares_from_aies_fba(fba)
