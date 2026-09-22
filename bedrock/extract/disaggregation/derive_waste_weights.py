"""Derive year-Y waste DisaggWeights (Option B in-memory) for the 2024 pilot."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, cast

import pandas as pd

from bedrock.extract.disaggregation.aies_waste_metrics import (
    load_aies_child_expense_shares,
)
from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeights,
    build_disagg_weights_from_long_format,
)
from bedrock.extract.disaggregation.ec_waste_customer_class import (
    CUSTOMER_CLASS_CODES,
    load_bundled_customer_class_rows,
    load_ec_customer_class_shares,
)
from bedrock.extract.disaggregation.rcra_waste_flows import (
    load_rcra_intersection_shares,
)
from bedrock.extract.disaggregation.sas_waste_metrics import (
    load_sas_table2_revenue_shares,
    load_sas_table3_expense_shares,
)
from bedrock.extract.disaggregation.waste_static_rules import (
    NAICS_MAP_VERSION,
    WASTE_CHILDREN,
    WASTE_PARENT,
)
from bedrock.extract.disaggregation.waste_weight_types import WeightDerivationProvenance
from bedrock.extract.disaggregation.waste_year_resolvers import (
    resolve_ec_year,
    resolve_industry_mix_source,
    resolve_rcra_year,
    resolve_sas_year,
)
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

_BUNDLED_DIR = Path(__file__).resolve().parent / "waste_disagg_inputs"
_BUNDLED_USE = _BUNDLED_DIR / "WasteDisaggregationDetail2017_Use.csv"
_BUNDLED_MAKE = _BUNDLED_DIR / "WasteDisaggregationDetail2017_Make.csv"

_WASTE_NEW_CODES: list[str] = list(WASTE_DISAGG_COMMODITIES["562000"])


def _strip(code: str) -> str:
    return str(code).split("/")[0].strip()


def _load_bundled_long() -> tuple[pd.DataFrame, pd.DataFrame]:
    use = pd.read_csv(_BUNDLED_USE, dtype=str)
    make = pd.read_csv(_BUNDLED_MAKE, dtype=str)
    for df in (use, make):
        df["IndustryCode"] = df["IndustryCode"].map(_strip)
        df["CommodityCode"] = df["CommodityCode"].map(_strip)
    use["PercentUsed"] = pd.to_numeric(use["PercentUsed"], errors="coerce")
    make["PercentMake"] = pd.to_numeric(make["PercentMake"], errors="coerce")
    return use, make


def _industry_mix_shares(
    target_year: int, prov: WeightDerivationProvenance
) -> pd.Series:
    kind, survey_year = resolve_industry_mix_source(target_year)
    if kind == "sas_table3":
        shares = load_sas_table3_expense_shares(survey_year)
        prov.sas_source_year = survey_year
        return shares
    table: Literal["EXP01", "BASIC"] = "EXP01" if kind == "aies_exp01" else "BASIC"
    try:
        shares = load_aies_child_expense_shares(survey_year, table=table)
        prov.aies_source_year = survey_year
        prov.aies_table = table
        return shares
    except Exception as exc:  # noqa: BLE001
        # Fallback: scale prior year (2022 SAS Table 3) per plan failure-mode
        prov.fallback_notes.append(
            f"AIES {table} {survey_year} failed ({type(exc).__name__}: {exc}); "
            "falling back to 2022 SAS Table 3 expense shares"
        )
        shares = load_sas_table3_expense_shares(2022)
        prov.sas_source_year = 2022
        return shares


def _replace_column_sum(use: pd.DataFrame, shares: pd.Series) -> pd.DataFrame:
    note = "Use column sum, industry output"
    keep = use[use["Note"] != note]
    rows = [
        {
            "IndustryCode": child,
            "CommodityCode": WASTE_PARENT,
            "PercentUsed": float(shares.loc[child]),
            "Note": note,
        }
        for child in WASTE_CHILDREN
    ]
    return pd.concat([keep, pd.DataFrame(rows)], ignore_index=True)


def _replace_row_sum(use: pd.DataFrame, shares: pd.Series) -> pd.DataFrame:
    note = "Use row sum, commodity output"
    keep = use[use["Note"] != note]
    rows = [
        {
            "IndustryCode": WASTE_PARENT,
            "CommodityCode": child,
            "PercentUsed": float(shares.loc[child]),
            "Note": note,
        }
        for child in WASTE_CHILDREN
    ]
    return pd.concat([keep, pd.DataFrame(rows)], ignore_index=True)


def _replace_use_intersection(use: pd.DataFrame, mat: pd.DataFrame) -> pd.DataFrame:
    note = "Use table intersection"
    keep = use[use["Note"] != note]
    rows = []
    for recv in WASTE_CHILDREN:
        for ship in WASTE_CHILDREN:
            rows.append(
                {
                    "IndustryCode": recv,
                    "CommodityCode": ship,
                    "PercentUsed": float(cast(float, mat.loc[recv, ship])),
                    "Note": note,
                }
            )
    return pd.concat([keep, pd.DataFrame(rows)], ignore_index=True)


def _replace_make_column_sum(make: pd.DataFrame, shares: pd.Series) -> pd.DataFrame:
    # Bundled Make uses IndustryCode=562000, CommodityCode=child for column sum
    note_candidates = make["Note"].astype(str)
    # Prefer rows matching parent industry + waste commodity
    mask = (make["IndustryCode"] == WASTE_PARENT) & make["CommodityCode"].isin(
        WASTE_CHILDREN
    )
    keep = make[~mask]
    rows = [
        {
            "IndustryCode": WASTE_PARENT,
            "CommodityCode": child,
            "PercentMake": float(shares.loc[child]),
            "Note": "Make column sum",
        }
        for child in WASTE_CHILDREN
    ]
    del note_candidates
    return pd.concat([keep, pd.DataFrame(rows)], ignore_index=True)


def _replace_make_intersection_diagonal(
    make: pd.DataFrame, shares: pd.Series
) -> pd.DataFrame:
    """Diagonal Make intersection from industry-mix shares (adjusted_q / sum)."""
    note = "Make table intersection"
    keep = make[make["Note"] != note]
    s = shares.reindex(WASTE_CHILDREN).fillna(0.0).astype(float)
    total = float(s.sum())
    if total <= 0:
        s = pd.Series(1.0 / len(WASTE_CHILDREN), index=WASTE_CHILDREN)
    else:
        s = s / total
    rows = []
    for i in WASTE_CHILDREN:
        for j in WASTE_CHILDREN:
            rows.append(
                {
                    "IndustryCode": i,
                    "CommodityCode": j,
                    "PercentMake": float(s.loc[i]) if i == j else 0.0,
                    "Note": note,
                }
            )
    return pd.concat([keep, pd.DataFrame(rows)], ignore_index=True)


def _replace_va_from_column_sum(use: pd.DataFrame, shares: pd.Series) -> pd.DataFrame:
    """VA shares follow industry mix (workbook Step 7)."""
    va_set = set(VALUE_ADDEDS)
    keep = use[~use["CommodityCode"].isin(va_set)]
    # Keep structure of bundled VA rows: each VA commodity × waste industry
    bundled_va = use[use["CommodityCode"].isin(va_set)]
    if bundled_va.empty:
        return use
    rows = []
    for va_code in bundled_va["CommodityCode"].unique():
        for child in WASTE_CHILDREN:
            rows.append(
                {
                    "IndustryCode": child,
                    "CommodityCode": va_code,
                    "PercentUsed": float(shares.loc[child]),
                    "Note": bundled_va.loc[
                        bundled_va["CommodityCode"] == va_code, "Note"
                    ].iloc[0],
                }
            )
    return pd.concat([keep, pd.DataFrame(rows)], ignore_index=True)


def _replace_customer_class(use: pd.DataFrame, ec_rows: pd.DataFrame) -> pd.DataFrame:
    keep = use[~use["IndustryCode"].isin(CUSTOMER_CLASS_CODES)]
    return pd.concat([keep, ec_rows], ignore_index=True)


def derive_waste_weights(
    target_year: int,
    *,
    mut_dollar_year: int | None = None,
    ec_2022_wired: bool = True,
) -> tuple[DisaggWeights, WeightDerivationProvenance]:
    """Build DisaggWeights for *target_year* and return in-memory provenance."""
    mut_year = mut_dollar_year if mut_dollar_year is not None else target_year
    rcra_year = resolve_rcra_year(target_year)
    ec_year = resolve_ec_year(target_year, ec_2022_wired=ec_2022_wired)
    prov = WeightDerivationProvenance(
        target_year=target_year,
        rcra_source_year=rcra_year,
        ec_source_year=ec_year,
        mut_dollar_year=mut_year,
        naics_map_version=NAICS_MAP_VERSION,
    )
    if ec_year == 2017 and target_year >= 2022 and not ec_2022_wired:
        prov.fallback_notes.append(
            "EC customer-class rows frozen at 2017 (EC 2022 not wired / pilot gate)"
        )

    use, make = _load_bundled_long()

    # Industry mix → Use column sum, Make column sum, VA, Make intersection diagonal
    col_shares = _industry_mix_shares(target_year, prov)
    use = _replace_column_sum(use, col_shares)
    use = _replace_va_from_column_sum(use, col_shares)
    make = _replace_make_column_sum(make, col_shares)
    make = _replace_make_intersection_diagonal(make, col_shares)

    # Use row sum: 2023–2024 carry 2022 SAS Table 2 revenue
    try:
        sas_row_year = resolve_sas_year(min(target_year, 2022))
        if target_year >= 2023:
            sas_row_year = 2022
        row_shares = load_sas_table2_revenue_shares(sas_row_year)
        use = _replace_row_sum(use, row_shares)
        if prov.sas_source_year is None:
            prov.sas_source_year = sas_row_year
    except Exception as exc:  # noqa: BLE001
        prov.fallback_notes.append(
            f"SAS Table 2 row-sum failed ({type(exc).__name__}); keeping 2017 bundled"
        )

    # RCRA intersection
    try:
        mat, rcra_notes = load_rcra_intersection_shares(rcra_year)
        use = _replace_use_intersection(use, mat)
        prov.fallback_notes.extend(rcra_notes)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"RCRA intersection load failed for year {rcra_year} "
            f"(CRHW_national_{rcra_year}): {type(exc).__name__}: {exc}"
        ) from exc

    # EC customer-class (2024 pilot: EC 2022 when wired; else bundled 2017 freeze)
    ec_rows = load_ec_customer_class_shares(ec_year)
    if ec_rows is not None:
        use = _replace_customer_class(use, ec_rows)
    else:
        _ = load_bundled_customer_class_rows(str(_BUNDLED_USE))
        prov.ec_source_year = 2017
        if target_year >= 2022:
            prov.fallback_notes.append(
                f"EC {ec_year} customer-class load failed; frozen at bundled 2017 rows"
            )

    weights = build_disagg_weights_from_long_format(
        use,
        make,
        year=target_year,
        source_name=f"WasteDisaggregationDetail{target_year}_derived",
        original_code=WASTE_PARENT,
        new_codes=_WASTE_NEW_CODES,
        disagg_sectors=_WASTE_NEW_CODES,
    )
    return weights, prov
