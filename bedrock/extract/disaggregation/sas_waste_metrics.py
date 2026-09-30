"""SAS Table 2/3 → Cornerstone waste child share vectors."""

from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd

from bedrock.extract.disaggregation.waste_static_rules import (
    SAS_NAICS_TO_CORNERSTONE,
    WASTE_CHILDREN,
)
from bedrock.extract.flowbyactivity import getFlowByActivity

#: SAS control total for waste management (NAICS 562). Published 6-digit
#: (and 5-digit residual) detail should partition this total; suppressed
#: detail is recovered from the shortfall (prior-weighted residual when
#: priors are available; equal residual otherwise).
WASTE_SAS_PARENT_NAICS = "562"
# Back-compat alias
WASTE_SAS_TABLE3_PARENT_NAICS = WASTE_SAS_PARENT_NAICS

#: Cornerstone waste-weight vintage floor for prior-year search. Exclude
#: earlier Census_SAS.yaml years (2013–2016) even when present.
SAS_PRIOR_YEAR_FLOOR = 2017

#: SAS years available for prior search (Census_SAS.yaml minus pre-floor).
SAS_PRIOR_SEARCH_YEARS: tuple[int, ...] = (2017, 2018, 2019, 2020, 2021, 2022)

#: AIES calendar year used as Table 3 detail-dollar prior fallback.
AIES_TABLE3_PRIOR_FALLBACK_YEAR = 2023


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
    """True only for Census suppression codes ``S`` and ``D``.

    ``(s)`` is sampling-error markup on a published estimate; ``Z`` rounds to
    zero. Neither participates in residual recovery.
    """
    if "Suppressed" not in df.columns:
        return pd.Series(False, index=df.index)
    s = df["Suppressed"].astype(str).str.strip().str.upper()
    return s.isin({"S", "D"})


def recover_suppressed_sas_waste_detail(
    frame: pd.DataFrame,
    *,
    table_label: str,
    naics_col: str | None = None,
    amount_col: str = "FlowAmount",
    parent_naics: str = WASTE_SAS_PARENT_NAICS,
    prior_by_naics: Mapping[str, float] | None = None,
    require_complete_priors: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    """Recover Census-suppressed SAS waste detail under published NAICS ``562``.

    **Pattern:** ``residual = parent(562) − sum(unsuppressed waste detail)``,
    then allocate across suppressed detail NAICS that map to Cornerstone waste
    children:

    1. Complete positive priors → prior-weighted split.
    2. No priors and ``require_complete_priors=False`` → equal split.
    3. Partial priors and ``require_complete_priors=False`` → two-pool split
       (prior-weighted subset + equal remainder) that always sums to residual.

    When ``require_complete_priors=True`` and ``n_suppressed > 1``, raise if any
    suppressed detail NAICS lacks a positive prior.

    ``n_suppressed == 1`` always fills the exact residual (``equal_residual``).

    Returns a copy with filled ``FlowAmount``, ``SuppressionRecovery`` where
    applied, plus provenance notes.
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

    suppressed_naics = naics.loc[suppressed_mask]
    prior_map = {
        str(k): float(v)
        for k, v in (prior_by_naics or {}).items()
        if v is not None and float(v) > 0
    }
    priors_for_supp = suppressed_naics.map(lambda n: prior_map.get(str(n), 0.0)).astype(
        float
    )
    has_prior = priors_for_supp > 0
    n_prior = int(has_prior.sum())
    n_missing = n_suppressed - n_prior
    missing_naics = sorted(suppressed_naics.loc[~has_prior].unique())

    if "SuppressionRecovery" not in out.columns:
        out["SuppressionRecovery"] = pd.Series(np.nan, index=out.index, dtype=object)

    fills = pd.Series(0.0, index=out.index)

    if n_suppressed == 1:
        fills.loc[suppressed_mask] = residual
        tag = "equal_residual"
        notes.append(
            f"SAS {table_label} waste suppression recovery "
            f"(equal_residual under NAICS {parent_naics}): "
            f"parent={parent_total:.0f}, published_detail={published_sum:.0f}, "
            f"residual={residual:.0f}, n_suppressed=1, "
            f"require_complete_priors={require_complete_priors}, "
            f"recovered_naics={sorted(suppressed_naics.unique())}"
        )
    elif require_complete_priors and n_missing > 0:
        raise ValueError(
            f"SAS {table_label} suppression recovery: require_complete_priors=True "
            f"but missing positive priors for suppressed NAICS {missing_naics} "
            f"(n_suppressed={n_suppressed}, n_prior={n_prior})"
        )
    elif n_prior == 0:
        fill = residual / n_suppressed
        fills.loc[suppressed_mask] = fill
        tag = "equal_residual"
        notes.append(
            f"SAS {table_label} waste suppression recovery "
            f"(equal_residual under NAICS {parent_naics}): "
            f"parent={parent_total:.0f}, published_detail={published_sum:.0f}, "
            f"residual={residual:.0f}, n_suppressed={n_suppressed}, "
            f"fill_each={fill:.0f}, require_complete_priors={require_complete_priors}, "
            f"recovered_naics={sorted(suppressed_naics.unique())}"
        )
    elif n_missing == 0:
        weights = priors_for_supp / float(priors_for_supp.sum())
        fills.loc[suppressed_mask] = residual * weights
        tag = "prior_weighted_residual"
        prior_used = {
            str(n): float(prior_map[str(n)]) for n in sorted(suppressed_naics.unique())
        }
        notes.append(
            f"SAS {table_label} waste suppression recovery "
            f"(prior_weighted_residual under NAICS {parent_naics}): "
            f"parent={parent_total:.0f}, published_detail={published_sum:.0f}, "
            f"residual={residual:.0f}, n_suppressed={n_suppressed}, "
            f"priors={prior_used}, require_complete_priors={require_complete_priors}, "
            f"recovered_naics={sorted(suppressed_naics.unique())}"
        )
    else:
        # Partial priors: two-pool split that always sums to residual.
        residual_prior = residual * (n_prior / n_suppressed)
        residual_equal = residual - residual_prior
        prior_backed = suppressed_mask & has_prior.reindex(out.index, fill_value=False)
        missing_mask = suppressed_mask & ~has_prior.reindex(out.index, fill_value=False)
        prior_vals = priors_for_supp.loc[has_prior]
        w = prior_vals / float(prior_vals.sum())
        fills.loc[prior_backed] = residual_prior * w
        fills.loc[missing_mask] = residual_equal / n_missing
        tag = "prior_weighted_residual"
        prior_used = {
            str(n): float(prior_map[str(n)])
            for n in sorted(suppressed_naics.loc[has_prior].unique())
        }
        notes.append(
            f"SAS {table_label} waste suppression recovery "
            f"(prior_weighted_residual two-pool under NAICS {parent_naics}): "
            f"parent={parent_total:.0f}, published_detail={published_sum:.0f}, "
            f"residual={residual:.0f}, n_suppressed={n_suppressed}, "
            f"n_prior={n_prior}, n_missing={n_missing}, "
            f"residual_prior={residual_prior:.0f}, residual_equal={residual_equal:.0f}, "
            f"priors={prior_used}, missing_priors={missing_naics}, "
            f"require_complete_priors={require_complete_priors}, "
            f"recovered_naics={sorted(suppressed_naics.unique())}"
        )

    out.loc[suppressed_mask, amount_col] = fills.loc[suppressed_mask]
    out.loc[suppressed_mask, "SuppressionRecovery"] = tag
    return out, notes


def recover_suppressed_sas_table3_waste_expenses(
    table3: pd.DataFrame,
    *,
    naics_col: str | None = None,
    amount_col: str = "FlowAmount",
    parent_naics: str = WASTE_SAS_PARENT_NAICS,
    prior_by_naics: Mapping[str, float] | None = None,
    require_complete_priors: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    """Recover suppressed SAS Table 3 waste **expense** cells (industry mix)."""
    return recover_suppressed_sas_waste_detail(
        table3,
        table_label="Table 3 expenses",
        naics_col=naics_col,
        amount_col=amount_col,
        parent_naics=parent_naics,
        prior_by_naics=prior_by_naics,
        require_complete_priors=require_complete_priors,
    )


def recover_suppressed_sas_table2_waste_revenue(
    table2: pd.DataFrame,
    *,
    naics_col: str | None = None,
    amount_col: str = "FlowAmount",
    parent_naics: str = WASTE_SAS_PARENT_NAICS,
    prior_by_naics: Mapping[str, float] | None = None,
    require_complete_priors: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    """Recover suppressed SAS Table 2 waste **revenue** cells (Use row-sum)."""
    return recover_suppressed_sas_waste_detail(
        table2,
        table_label="Table 2 revenue",
        naics_col=naics_col,
        amount_col=amount_col,
        parent_naics=parent_naics,
        prior_by_naics=prior_by_naics,
        require_complete_priors=require_complete_priors,
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


def _table_slice(
    fba: pd.DataFrame,
    *,
    table_prefix: str,
    preferred_naics: str | None = None,
) -> tuple[pd.DataFrame, str]:
    df = fba.copy()
    if "Description" in df.columns:
        df = df[df["Description"].astype(str).str.startswith(table_prefix)]
    if preferred_naics and preferred_naics in df.columns:
        return df, preferred_naics
    return df, _naics_col(df)


def _published_detail_dollars_by_naics(
    frame: pd.DataFrame,
    *,
    naics_col: str,
    amount_col: str = "FlowAmount",
) -> dict[str, float]:
    """Raw published (non-S/D) waste-detail dollars keyed by detail NAICS.

    Does **not** run suppression recovery — priors must come from published
    cells only.
    """
    out: dict[str, float] = {}
    naics = frame[naics_col].astype(str)
    suppressed = _suppressed_mask(frame)
    for idx in frame.index:
        code = str(naics.loc[idx])
        if map_sas_naics_to_cornerstone(code) is None:
            continue
        if bool(suppressed.loc[idx]):
            continue
        val = frame.loc[idx, amount_col]
        try:
            amount = float(val) if pd.notna(val) else 0.0
        except (TypeError, ValueError):
            continue
        if amount > 0:
            out[code] = out.get(code, 0.0) + amount
    return out


def _suppressed_detail_naics(
    frame: pd.DataFrame,
    *,
    naics_col: str,
) -> list[str]:
    naics = frame[naics_col].astype(str)
    child = naics.map(map_sas_naics_to_cornerstone)
    suppressed = _suppressed_mask(frame)
    return sorted(naics.loc[child.notna() & suppressed].unique())


def _nearest_sas_priors_for_naics(
    *,
    year: int,
    table_prefix: str,
    suppressed_naics: list[str],
    preferred_naics: str | None = None,
) -> dict[str, float]:
    """Per-NAICS nearest published SAS prior in ``{Y-1 … 2017}``."""
    if not suppressed_naics:
        return {}
    remaining = set(suppressed_naics)
    priors: dict[str, float] = {}
    for y in range(year - 1, SAS_PRIOR_YEAR_FLOOR - 1, -1):
        if y not in SAS_PRIOR_SEARCH_YEARS or not remaining:
            continue
        fba = getFlowByActivity(
            datasource="Census_SAS", year=y, download_FBA_if_missing=True
        )
        frame, ncol = _table_slice(
            fba, table_prefix=table_prefix, preferred_naics=preferred_naics
        )
        published = _published_detail_dollars_by_naics(frame, naics_col=ncol)
        found = remaining & published.keys()
        for code in found:
            priors[code] = published[code]
        remaining -= found
    return priors


def _build_table3_priors(
    year: int, suppressed_naics: list[str]
) -> tuple[dict[str, float], list[str]]:
    """Same-table SAS expense priors, then AIES 2023 detail-dollar fallback."""
    notes: list[str] = []
    priors = _nearest_sas_priors_for_naics(
        year=year,
        table_prefix="Table 3",
        suppressed_naics=suppressed_naics,
    )
    missing = [n for n in suppressed_naics if n not in priors]
    if missing:
        # Lazy import avoids sas ↔ aies cycle (aies imports map_sas from here).
        from bedrock.extract.disaggregation.aies_waste_metrics import (  # noqa: PLC0415
            load_aies_waste_detail_expense_dollars,
        )

        aies = load_aies_waste_detail_expense_dollars(AIES_TABLE3_PRIOR_FALLBACK_YEAR)
        used_aies: dict[str, float] = {}
        for code in missing:
            if code in aies and aies[code] > 0:
                priors[code] = float(aies[code])
                used_aies[code] = float(aies[code])
        if used_aies:
            notes.append(
                f"SAS Table 3 prior fallback to AIES "
                f"{AIES_TABLE3_PRIOR_FALLBACK_YEAR} detail dollars: {used_aies}"
            )
    still_missing = [n for n in suppressed_naics if n not in priors]
    if still_missing:
        notes.append(
            f"SAS Table 3 priors still missing after AIES fallback: {still_missing}"
        )
    if priors:
        notes.append(f"SAS Table 3 prior_by_naics={priors}")
    return priors, notes


def _build_table2_priors(
    year: int, suppressed_naics: list[str]
) -> tuple[dict[str, float], list[str]]:
    """Same-table SAS revenue priors only (no AIES / Table 3 cross-table)."""
    notes: list[str] = []
    priors = _nearest_sas_priors_for_naics(
        year=year,
        table_prefix="Table 2",
        suppressed_naics=suppressed_naics,
        preferred_naics="ActivityProducedBy",
    )
    missing = [n for n in suppressed_naics if n not in priors]
    if missing:
        notes.append(
            f"SAS Table 2 revenue priors missing (no AIES fallback): {missing}"
        )
    if priors:
        notes.append(f"SAS Table 2 prior_by_naics={priors}")
    return priors, notes


def load_sas_table3_expense_shares(
    year: int,
    *,
    require_complete_priors: bool = False,
) -> tuple[pd.Series, list[str]]:
    """Use column-sum industry mix from SAS Table 3 total Expenses.

    Applies prior-weighted
    :func:`recover_suppressed_sas_table3_waste_expenses` before normalizing so
    Census ``Suppressed=S/D`` cells receive a share of the NAICS-562 residual.

    Returns ``(shares, provenance_notes)``.
    """
    fba = getFlowByActivity(
        datasource="Census_SAS", year=year, download_FBA_if_missing=True
    )
    df, naics_col = _table_slice(fba, table_prefix="Table 3")
    suppressed_naics = _suppressed_detail_naics(df, naics_col=naics_col)
    prior_notes: list[str] = []
    priors: dict[str, float] = {}
    if suppressed_naics:
        priors, prior_notes = _build_table3_priors(year, suppressed_naics)
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(
        df,
        naics_col=naics_col,
        prior_by_naics=priors or None,
        require_complete_priors=require_complete_priors,
    )
    notes = prior_notes + notes
    s = _child_totals_from_frame(recovered, naics_col=naics_col)
    total = float(s.sum())
    if total <= 0:
        raise ValueError(f"All-zero SAS Table 3 expense totals for {year}")
    shares = (s / total).reindex(WASTE_CHILDREN).fillna(0.0)
    return shares, notes


def load_sas_table2_revenue_shares(
    year: int,
    *,
    require_complete_priors: bool = False,
) -> tuple[pd.Series, list[str]]:
    """SAS Table 2 revenue shares (Use row-sum / commodity mix).

    Applies prior-weighted
    :func:`recover_suppressed_sas_table2_waste_revenue` before normalizing.
    Priors are same-table SAS revenue only (no AIES expense cross-table).

    Returns ``(shares, provenance_notes)``.
    """
    fba = getFlowByActivity(
        datasource="Census_SAS", year=year, download_FBA_if_missing=True
    )
    preferred = "ActivityProducedBy" if "ActivityProducedBy" in fba.columns else None
    df, naics_col = _table_slice(fba, table_prefix="Table 2", preferred_naics=preferred)
    suppressed_naics = _suppressed_detail_naics(df, naics_col=naics_col)
    prior_notes: list[str] = []
    priors: dict[str, float] = {}
    if suppressed_naics:
        priors, prior_notes = _build_table2_priors(year, suppressed_naics)
    recovered, notes = recover_suppressed_sas_table2_waste_revenue(
        df,
        naics_col=naics_col,
        prior_by_naics=priors or None,
        require_complete_priors=require_complete_priors,
    )
    notes = prior_notes + notes
    s = _child_totals_from_frame(recovered, naics_col=naics_col)
    total = float(s.sum())
    if total <= 0:
        raise ValueError(f"All-zero SAS Table 2 revenue totals for {year}")
    shares = (s / total).reindex(WASTE_CHILDREN).fillna(0.0)
    return shares, notes
