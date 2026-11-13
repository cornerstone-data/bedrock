"""EC ecnclcust → fixed customer-class Use rows (2017 parity: 9 codes).

Business firms and farms feed default Use row-sum shares (ordinary industry
buyers of waste commodities), not the special customer-class IndustryCode
rows — see ``build_business_firms_row_sum_shares_from_ec_fba``.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from bedrock.extract.disaggregation.sas_waste_metrics import (
    map_sas_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.utils.mapping.location import US_FIPS

# Fixed 2017-parity customer-class industry codes (bundled Use CSV).
CUSTOMER_CLASS_CODES: tuple[str, ...] = (
    "F01000",  # FD / households
    "S00102",
    "GSLGO",
    "GSLGE",
    "GSLGH",
    "S00203",
    "813100",
    "813A00",
    "813B00",
)

#: Economic Census vintages for waste who-buys (ecnclcust), same quinquennial
#: span as ``Census_EC.PXI_VINTAGES``. Interior match_io years blend between
#: these with ``interpolate_shares`` (shipped geometric form).
EC_CLCUST_VINTAGES: tuple[int, int] = (2017, 2022)

_EQUAL_SHARE_NOTE_MARKER = "equal-share fallback"

# Default Use row-sum path — never emitted as IndustryCode rows.
BUSINESS_FIRMS_LABEL = "Business firms and farms"

# EC CLASSCUST_LABEL substring → Cornerstone industry codes that share the same
# child-mix. "Business firms and farms" feeds the default Use row-sum path and
# is intentionally omitted here.
_CLASS_LABEL_TO_CODES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Household consumers and individuals", ("F01000",)),
    ("Federal government", ("S00102",)),
    (
        "State and local governments",
        ("S00203", "GSLGE", "GSLGH", "GSLGO"),
    ),
    (
        "Not-for-profit organizations",
        ("813100", "813A00", "813B00"),
    ),
)

_SALES_FLOW = "Sales, value of shipments, or revenue"
_DIST_FLOW = "Distribution of sales, value of shipments, or revenue"
_ALL_CLASSES = "All classes of customer"


def load_bundled_customer_class_rows(use_csv_path: str) -> pd.DataFrame:
    """Return Use long-format rows for the fixed 9 customer-class codes from bundled CSV."""
    du = pd.read_csv(use_csv_path, dtype=str)
    du["IndustryCode"] = du["IndustryCode"].map(lambda c: str(c).split("/")[0])
    du["CommodityCode"] = du["CommodityCode"].map(lambda c: str(c).split("/")[0])
    mask = du["IndustryCode"].isin(CUSTOMER_CLASS_CODES)
    out = du.loc[mask].copy()
    out["PercentUsed"] = pd.to_numeric(out["PercentUsed"], errors="coerce")
    return out


def _match_class_label(label: str) -> tuple[str, ...] | None:
    text = str(label)
    for needle, codes in _CLASS_LABEL_TO_CODES:
        if needle.lower() in text.lower():
            return codes
    return None


def _class_note(label_key: str) -> str:
    return f"Commodity disaggregation, {label_key}"


def _is_six_digit_naics(code: object) -> bool:
    digits = "".join(c for c in str(code) if c.isdigit())
    return len(digits) == 6


def _is_business_firms_label(label: str) -> bool:
    return BUSINESS_FIRMS_LABEL.lower() in str(label).lower()


def _reconstruct_class_receipts(
    fba: pd.DataFrame,
    *,
    label_keep: Callable[[str], bool] | None = None,
) -> pd.DataFrame:
    """Build (NAICS, class_label, receipts) from EC FBA.

    EC 2022 often publishes class-level ``RCPTOT`` as 0 while ``RCPTOT_DIST`` and
    the ``All classes of customer`` total remain. Reconstruct:

        receipts = total_sales × (dist_pct / 100)

    Prefer reconstructed values; fall back to published class sales when > 0
    (EC 2017 pattern).

    ``label_keep`` defaults to special customer-class labels only (not Business
    firms). Pass a predicate to include other class labels.
    """
    keep = label_keep or (lambda lab: _match_class_label(lab) is not None)
    df = fba.copy()
    if "Location" in df.columns:
        df = df[df["Location"].astype(str) == US_FIPS]
    df = df[df["ActivityProducedBy"].map(_is_six_digit_naics)]

    sales = df[df["FlowName"] == _SALES_FLOW]
    dist = df[df["FlowName"] == _DIST_FLOW]

    totals = (
        sales[
            sales["ActivityConsumedBy"]
            .astype(str)
            .str.contains(_ALL_CLASSES, case=False)
        ]
        .groupby("ActivityProducedBy", as_index=True)["FlowAmount"]
        .sum()
    )

    rows: list[dict[str, object]] = []
    # Index published class sales for fallback
    class_sales = sales[
        ~sales["ActivityConsumedBy"].astype(str).str.contains(_ALL_CLASSES, case=False)
    ]

    for _, row in dist.iterrows():
        label = str(row["ActivityConsumedBy"])
        if _ALL_CLASSES.lower() in label.lower():
            continue
        if not keep(label):
            continue
        naics = row["ActivityProducedBy"]
        try:
            pct = float(row["FlowAmount"])
        except (TypeError, ValueError):
            continue
        total = float(totals.get(naics, 0.0) or 0.0)
        reconstructed = total * (pct / 100.0) if total > 0 and pct > 0 else 0.0

        published = 0.0
        match = class_sales[
            (class_sales["ActivityProducedBy"] == naics)
            & (class_sales["ActivityConsumedBy"] == row["ActivityConsumedBy"])
        ]
        if not match.empty:
            try:
                published = float(match["FlowAmount"].iloc[0])
            except (TypeError, ValueError):
                published = 0.0

        amount = published if published > 0 else reconstructed
        if amount <= 0:
            continue
        rows.append(
            {
                "ActivityProducedBy": naics,
                "ActivityConsumedBy": label,
                "FlowAmount": amount,
            }
        )
    return pd.DataFrame(rows)


def build_business_firms_row_sum_shares_from_ec_fba(fba: pd.DataFrame) -> pd.Series:
    """Child-mix shares among waste commodities sold to Business firms and farms.

    Indexed by ``WASTE_CHILDREN``. Never emits IndustryCode rows — callers feed
    these into ``_replace_row_sum`` only.
    """
    receipts = _reconstruct_class_receipts(fba, label_keep=_is_business_firms_label)
    if receipts.empty:
        raise ValueError("No positive EC waste receipts for Business firms and farms")

    mass: dict[str, float] = {child: 0.0 for child in WASTE_CHILDREN}
    for _, row in receipts.iterrows():
        child = map_sas_naics_to_cornerstone(row["ActivityProducedBy"])
        if child is None:
            continue
        mass[child] = mass.get(child, 0.0) + float(row["FlowAmount"])

    total = float(sum(mass.values()))
    if total <= 0:
        raise ValueError(
            "No positive EC waste receipts for Business firms and farms "
            "after NAICS→Cornerstone map"
        )
    return pd.Series(
        {child: mass[child] / total for child in WASTE_CHILDREN}, dtype=float
    )


def load_census_ec_fba(ec_year: int) -> pd.DataFrame:
    """Load Census_EC FlowByActivity for a supported ecnclcust year.

    Raises ``ValueError`` for unsupported years; otherwise returns the FBA frame
    (or propagates loader errors). Callers that need both Business-firms row-sum
    and special customer-class rows should load once and pass the frame into the
    ``build_*_from_ec_fba`` helpers.
    """
    if ec_year not in (2012, 2017, 2022):
        raise ValueError(f"Census_EC ecnclcust unsupported for ec_year={ec_year}")
    return getFlowByActivity(datasource="Census_EC", year=ec_year)


def load_ec_business_firms_row_sum_shares(ec_year: int) -> pd.Series:
    """Load EC ecnclcust and return Business-firms default Use row-sum shares.

    Raises on load/parse/empty failure (caller decides match_io vs soft fallback).
    Prefer ``load_census_ec_fba`` + ``build_business_firms_row_sum_shares_from_ec_fba``
    when the same FBA is also needed for customer-class rows.
    """
    return build_business_firms_row_sum_shares_from_ec_fba(load_census_ec_fba(ec_year))


def build_customer_class_rows_from_ec_fba(fba: pd.DataFrame) -> pd.DataFrame:
    """Convert parsed Census_EC FBA into long-format Use rows for the 9 codes.

    For each special purchaser class C and waste child j:
    ``PercentUsed[C, j] = receipts[j→C] / Σ_j receipts[j→C]``.
    Multiple Cornerstone codes that share a class (e.g. GSLG*) get identical shares.
    """
    receipts = _reconstruct_class_receipts(fba)
    if receipts.empty:
        raise ValueError(
            "No positive EC waste customer-class sales for special classes"
        )

    mass: dict[tuple[str, str], float] = {}
    label_by_code: dict[str, str] = {}
    for _, row in receipts.iterrows():
        child = map_sas_naics_to_cornerstone(row["ActivityProducedBy"])
        if child is None:
            continue
        codes = _match_class_label(row["ActivityConsumedBy"])
        if codes is None:
            continue
        amount = float(row["FlowAmount"])
        label_key = next(
            needle for needle, mapped in _CLASS_LABEL_TO_CODES if mapped == codes
        )
        for code in codes:
            label_by_code[code] = label_key
            key = (code, child)
            mass[key] = mass.get(key, 0.0) + amount

    if not mass:
        raise ValueError(
            "No positive EC waste customer-class sales for special classes"
        )

    rows: list[dict[str, object]] = []
    for code in CUSTOMER_CLASS_CODES:
        child_vals = {child: mass.get((code, child), 0.0) for child in WASTE_CHILDREN}
        total = float(sum(child_vals.values()))
        if total <= 0:
            shares = {c: 1.0 / len(WASTE_CHILDREN) for c in WASTE_CHILDREN}
            note_suffix = label_by_code.get(
                code, "customer class (equal-share fallback)"
            )
        else:
            shares = {c: v / total for c, v in child_vals.items()}
            note_suffix = label_by_code[code]
        note = _class_note(note_suffix)
        for child in WASTE_CHILDREN:
            rows.append(
                {
                    "IndustryCode": code,
                    "CommodityCode": child,
                    "PercentUsed": float(shares[child]),
                    "Note": note,
                }
            )
    return pd.DataFrame(rows)


def ec_clcust_blend_t(target_year: int) -> float | None:
    """Return blend ``t`` in ``(0, 1)`` for interior census years, else ``None``.

    Matches ``Census_EC.pxi_weights_for_year``: ``t = (year - 2017) / 5``,
    endpoints hold their own vintage.
    """
    base_year, end_year = EC_CLCUST_VINTAGES
    if int(target_year) <= base_year or int(target_year) >= end_year:
        return None
    return (int(target_year) - base_year) / (end_year - base_year)


def _is_equal_share_fallback_note(note: str) -> bool:
    """True only when the builder stamped the equal-share fallback marker.

    Do not infer fallback from a numeric 1/n mix: a real census class can be
    uniform, and holding that side would skip a legitimate blend.
    """
    return _EQUAL_SHARE_NOTE_MARKER in str(note)


def blend_business_firms_shares(base: pd.Series, end: pd.Series, t: float) -> pd.Series:
    """Geometric share blend of Business-firms row-sum child mixes (PxI form)."""
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        interpolate_shares,
    )

    base_df = base.reindex(WASTE_CHILDREN).astype(float).fillna(0.0).to_frame(0)
    end_df = end.reindex(WASTE_CHILDREN).astype(float).fillna(0.0).to_frame(0)
    blended = interpolate_shares(base_df, end_df, float(t))[0]
    total = float(blended.sum())
    if total <= 0:
        return base.reindex(WASTE_CHILDREN).astype(float).fillna(0.0)
    return blended / total


def blend_customer_class_share_rows(
    base_rows: pd.DataFrame, end_rows: pd.DataFrame, t: float
) -> pd.DataFrame:
    """Blend special customer-class Use rows between two EC vintages.

    Holds a class to the real census when the other vintage is an equal-share
    fallback (same spirit as PxI holding missing cells rather than blending
    toward zero / uniform).
    """
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        interpolate_shares,
    )

    def _pivot(rows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
        notes = (
            rows.drop_duplicates(subset=["IndustryCode"])
            .set_index("IndustryCode")["Note"]
            .astype(str)
            .to_dict()
        )
        wide = (
            rows.pivot_table(
                index="CommodityCode",
                columns="IndustryCode",
                values="PercentUsed",
                aggfunc="sum",
            )
            .reindex(index=list(WASTE_CHILDREN), columns=list(CUSTOMER_CLASS_CODES))
            .astype(float)
            .fillna(0.0)
        )
        return wide, notes

    base_wide, base_notes = _pivot(base_rows)
    end_wide, end_notes = _pivot(end_rows)
    for code in CUSTOMER_CLASS_CODES:
        b_eq = _is_equal_share_fallback_note(base_notes.get(code, ""))
        e_eq = _is_equal_share_fallback_note(end_notes.get(code, ""))
        if b_eq and not e_eq:
            base_wide[code] = end_wide[code]
            base_notes[code] = end_notes[code]
        elif e_eq and not b_eq:
            end_wide[code] = base_wide[code]
            end_notes[code] = base_notes[code]

    blended = interpolate_shares(base_wide, end_wide, float(t))
    out_rows: list[dict[str, object]] = []
    for code in CUSTOMER_CLASS_CODES:
        note = end_notes.get(code) or base_notes.get(code) or _class_note(code)
        for child in WASTE_CHILDREN:
            out_rows.append(
                {
                    "IndustryCode": code,
                    "CommodityCode": child,
                    "PercentUsed": float(blended.loc[child, code]),
                    "Note": note,
                }
            )
    return pd.DataFrame(out_rows)


def blend_business_firms_row_sum_shares_from_ec_fbas(
    fba_base: pd.DataFrame, fba_end: pd.DataFrame, t: float
) -> pd.Series:
    """Business-firms row-sum shares blended from two loaded EC FBAs."""
    return blend_business_firms_shares(
        build_business_firms_row_sum_shares_from_ec_fba(fba_base),
        build_business_firms_row_sum_shares_from_ec_fba(fba_end),
        t,
    )


def blend_customer_class_rows_from_ec_fbas(
    fba_base: pd.DataFrame, fba_end: pd.DataFrame, t: float
) -> pd.DataFrame:
    """Special customer-class Use rows blended from two loaded EC FBAs."""
    return blend_customer_class_share_rows(
        build_customer_class_rows_from_ec_fba(fba_base),
        build_customer_class_rows_from_ec_fba(fba_end),
        t,
    )


def customer_class_shares_from_ec_fba(fba: pd.DataFrame) -> pd.DataFrame | None:
    """Build special customer-class Use rows from an already-loaded EC FBA.

    Returns ``None`` on parse/empty failure (caller freezes bundled 2017 rows).
    """
    try:
        return build_customer_class_rows_from_ec_fba(fba)
    except Exception:  # noqa: BLE001 — pilot: any parse failure → freeze
        return None


def load_ec_customer_class_shares(ec_year: int) -> pd.DataFrame | None:
    """Load EC ecnclcust and return long-format customer-class Use rows.

    Returns ``None`` on load/parse failure so the caller can freeze bundled 2017
    rows (pilot failure-mode). Years other than 2012/2017/2022 return ``None``.
    Prefer ``load_census_ec_fba`` + ``customer_class_shares_from_ec_fba`` when the
    same FBA is also needed for Business-firms row-sum.
    """
    try:
        return customer_class_shares_from_ec_fba(load_census_ec_fba(ec_year))
    except Exception:  # noqa: BLE001 — unsupported year or load failure → freeze
        return None
