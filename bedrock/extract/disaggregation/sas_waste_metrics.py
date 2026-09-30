"""SAS Table 2/3 → Cornerstone waste child share vectors."""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.extract.disaggregation.waste_static_rules import (
    SAS_NAICS_TO_CORNERSTONE,
    WASTE_CHILDREN,
)
from bedrock.extract.flowbyactivity import getFlowByActivity

#: SAS control total for waste management (NAICS 562). Published 6-digit
#: (and 5-digit residual) detail should partition this total; suppressed
#: detail is recovered from the shortfall (PxI-style equal residual).
WASTE_SAS_PARENT_NAICS = "562"
# Back-compat alias
WASTE_SAS_TABLE3_PARENT_NAICS = WASTE_SAS_PARENT_NAICS


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


def _naics_col(df: pd.DataFrame) -> str:
    for cand in ("ActivityConsumedBy", "ActivityProducedBy", "Sector", "NAICS"):
        if cand in df.columns:
            return cand
    raise KeyError("No NAICS-like column in SAS FBA")


def _suppressed_mask(df: pd.DataFrame) -> pd.Series:
    if "Suppressed" not in df.columns:
        return pd.Series(False, index=df.index)
    s = df["Suppressed"]
    return s.notna() & ~s.astype(str).str.strip().isin(("", "None", "nan"))


def recover_suppressed_sas_waste_detail(
    frame: pd.DataFrame,
    *,
    table_label: str,
    naics_col: str | None = None,
    amount_col: str = "FlowAmount",
    parent_naics: str = WASTE_SAS_PARENT_NAICS,
) -> tuple[pd.DataFrame, list[str]]:
    """Recover Census-suppressed SAS waste detail under published NAICS ``562``.

    **Pattern:** same family as ``estimate_suppressed_ec_pxi`` / Table 3 waste
    expenses — ``residual = parent(562) − sum(unsuppressed waste detail)``,
    shared **equally** across suppressed detail NAICS that map to Cornerstone
    waste children.

    Applies to both SAS **Table 3 expenses** (industry mix) and **Table 2
    revenue** (Use row-sum / commodity mix). Chosen over single-cell
    subtraction when ``n_suppressed > 1``, and over NAICS hierarchy walk
    (5621/5622/5629 absent). When ``n_suppressed == 1``, fill equals exact
    single-cell subtraction.

    Returns a copy with filled ``FlowAmount``, ``SuppressionRecovery=
    'equal_residual'`` where applied, plus provenance notes.
    """
    notes: list[str] = []
    out = frame.copy()
    if amount_col not in out.columns:
        raise KeyError(f"Expected column {amount_col} in SAS {table_label} frame")
    ncol = naics_col or _naics_col(out)
    naics = out[ncol].astype(str)
    child = naics.map(map_sas_naics_to_cornerstone)
    suppressed = _suppressed_mask(out)
    is_detail = child.notna()
    is_parent = naics == str(parent_naics)

    parent_rows = out.loc[is_parent & ~suppressed, amount_col]
    if parent_rows.empty:
        notes.append(
            f"SAS {table_label} suppression recovery skipped: no unsuppressed "
            f"parent NAICS {parent_naics} control total"
        )
        return out, notes

    parent_total = float(parent_rows.sum())
    if parent_total <= 0:
        notes.append(
            f"SAS {table_label} suppression recovery skipped: parent "
            f"{parent_naics} total is non-positive ({parent_total})"
        )
        return out, notes

    published_mask = is_detail & ~suppressed & (out[amount_col].fillna(0.0) > 0)
    suppressed_mask = is_detail & suppressed
    published_sum = float(out.loc[published_mask, amount_col].sum())
    residual = parent_total - published_sum
    n_suppressed = int(suppressed_mask.sum())

    if n_suppressed == 0:
        notes.append(
            f"SAS {table_label} suppression recovery: no suppressed waste "
            "detail rows"
        )
        return out, notes

    if residual <= 0:
        notes.append(
            f"SAS {table_label} suppression recovery skipped: residual "
            f"{residual:.0f} <= 0 (parent={parent_total:.0f}, "
            f"published_detail={published_sum:.0f}, n_suppressed={n_suppressed})"
        )
        return out, notes

    fill = residual / n_suppressed
    if "SuppressionRecovery" not in out.columns:
        out["SuppressionRecovery"] = pd.Series(np.nan, index=out.index, dtype=object)
    out.loc[suppressed_mask, amount_col] = fill
    out.loc[suppressed_mask, "SuppressionRecovery"] = "equal_residual"
    recovered_naics = sorted(naics.loc[suppressed_mask].unique())
    notes.append(
        f"SAS {table_label} waste suppression recovery "
        f"(equal_residual under NAICS {parent_naics}): "
        f"parent={parent_total:.0f}, published_detail={published_sum:.0f}, "
        f"residual={residual:.0f}, n_suppressed={n_suppressed}, "
        f"fill_each={fill:.0f}, recovered_naics={recovered_naics}"
    )
    return out, notes


def recover_suppressed_sas_table3_waste_expenses(
    table3: pd.DataFrame,
    *,
    naics_col: str | None = None,
    amount_col: str = "FlowAmount",
    parent_naics: str = WASTE_SAS_PARENT_NAICS,
) -> tuple[pd.DataFrame, list[str]]:
    """Recover suppressed SAS Table 3 waste **expense** cells (industry mix)."""
    return recover_suppressed_sas_waste_detail(
        table3,
        table_label="Table 3 expenses",
        naics_col=naics_col,
        amount_col=amount_col,
        parent_naics=parent_naics,
    )


def recover_suppressed_sas_table2_waste_revenue(
    table2: pd.DataFrame,
    *,
    naics_col: str | None = None,
    amount_col: str = "FlowAmount",
    parent_naics: str = WASTE_SAS_PARENT_NAICS,
) -> tuple[pd.DataFrame, list[str]]:
    """Recover suppressed SAS Table 2 waste **revenue** cells (Use row-sum)."""
    return recover_suppressed_sas_waste_detail(
        table2,
        table_label="Table 2 revenue",
        naics_col=naics_col,
        amount_col=amount_col,
        parent_naics=parent_naics,
    )


def _child_totals_from_frame(df: pd.DataFrame, *, naics_col: str) -> pd.Series:
    totals: dict[str, float] = dict.fromkeys(WASTE_CHILDREN, 0.0)
    for _, row in df.iterrows():
        child = map_sas_naics_to_cornerstone(row[naics_col])
        if child is None:
            continue
        val = float(row["FlowAmount"]) if pd.notna(row["FlowAmount"]) else 0.0
        if val > 0:
            totals[child] += val
    return pd.Series(totals, dtype=float)


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
        raise KeyError(f"Expected column {value_col} in SAS FBA")
    naics_col = _naics_col(df)

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
    return s / total


def load_sas_table3_expense_shares(year: int) -> tuple[pd.Series, list[str]]:
    """Use column-sum industry mix from SAS Table 3 total Expenses.

    Applies :func:`recover_suppressed_sas_table3_waste_expenses` before
    normalizing so Census ``Suppressed=S`` cells (e.g. 562112/562211 → 562HAZ)
    receive an equal share of the NAICS-562 residual rather than wiping the
    child to 0%.

    Returns ``(shares, provenance_notes)``.
    """
    fba = getFlowByActivity(datasource="Census_SAS", year=year)
    df = fba.copy()
    if "Description" in df.columns:
        df = df[df["Description"].astype(str).str.startswith("Table 3")]
    naics_col = _naics_col(df)
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(
        df, naics_col=naics_col
    )
    s = _child_totals_from_frame(recovered, naics_col=naics_col)
    total = float(s.sum())
    if total <= 0:
        raise ValueError(f"All-zero SAS Table 3 expense totals for {year}")
    shares = (s / total).reindex(WASTE_CHILDREN).fillna(0.0)
    return shares, notes


def load_sas_table2_revenue_shares(year: int) -> tuple[pd.Series, list[str]]:
    """SAS Table 2 revenue shares (Use row-sum / commodity mix).

    Applies :func:`recover_suppressed_sas_table2_waste_revenue` (same
    equal-residual pattern as Table 3) before normalizing so suppressed
    revenue cells (e.g. 2021 ``562213``) do not wipe Use-row child shares.

    Returns ``(shares, provenance_notes)``.
    """
    fba = getFlowByActivity(datasource="Census_SAS", year=year)
    df = fba.copy()
    if "Description" in df.columns:
        df = df[df["Description"].astype(str).str.startswith("Table 2")]
    naics_col = (
        "ActivityProducedBy"
        if "ActivityProducedBy" in df.columns
        else "ActivityConsumedBy"
    )
    recovered, notes = recover_suppressed_sas_table2_waste_revenue(
        df, naics_col=naics_col
    )
    s = _child_totals_from_frame(recovered, naics_col=naics_col)
    total = float(s.sum())
    if total <= 0:
        raise ValueError(f"All-zero SAS Table 2 revenue totals for {year}")
    shares = (s / total).reindex(WASTE_CHILDREN).fillna(0.0)
    return shares, notes
