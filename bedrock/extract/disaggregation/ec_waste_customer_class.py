"""EC ecnclcust → fixed customer-class Use rows (2017 parity: 9 codes)."""

from __future__ import annotations

import pandas as pd

from bedrock.extract.disaggregation.sas_waste_metrics import (
    map_sas_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.utils.mapping.location import US_FIPS

# Fixed 2017-parity customer-class industry codes (workbook / bundled CSV).
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

# EC CLASSCUST_LABEL substring → Cornerstone industry codes that share the same
# child-mix (workbook Notes / 3B_EC_USEEIO_Mapping). "Business firms and farms"
# feeds the default Use row-sum path and is intentionally omitted here.
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


def _reconstruct_class_receipts(fba: pd.DataFrame) -> pd.DataFrame:
    """Build (NAICS, class_label, receipts) from EC FBA.

    EC 2022 often publishes class-level ``RCPTOT`` as 0 while ``RCPTOT_DIST`` and
    the ``All classes of customer`` total remain. Reconstruct:

        receipts = total_sales × (dist_pct / 100)

    Prefer reconstructed values; fall back to published class sales when > 0
    (EC 2017 pattern).
    """
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
        if _match_class_label(label) is None:
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


def load_ec_customer_class_shares(ec_year: int) -> pd.DataFrame | None:
    """Load EC ecnclcust and return long-format customer-class Use rows.

    Returns ``None`` on load/parse failure so the caller can freeze bundled 2017
    rows (pilot failure-mode). Years other than 2012/2017/2022 return ``None``.
    """
    if ec_year not in (2012, 2017, 2022):
        return None
    try:
        fba = getFlowByActivity(datasource="Census_EC", year=ec_year)
        return build_customer_class_rows_from_ec_fba(fba)
    except Exception:  # noqa: BLE001 — pilot: any load failure → freeze
        return None
