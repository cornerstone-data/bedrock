"""Validate Census_EC 2022 ecnclcust for waste who-buys (2024 pilot / Decision 4).

Run:
  .venv\\Scripts\\python.exe -m \\
    bedrock.analysis.nowcasting.waste_disaggregation.scripts.test_ec_2022_ecnclcust
"""

from __future__ import annotations

import json
from pathlib import Path

from bedrock.extract.disaggregation.ec_waste_customer_class import (
    CUSTOMER_CLASS_CODES,
    build_customer_class_rows_from_ec_fba,
)
from bedrock.extract.disaggregation.sas_waste_metrics import (
    map_sas_naics_to_cornerstone,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.utils.mapping.location import US_FIPS

_CACHE = (
    Path(__file__).resolve().parents[1] / "cache" / "ec_2022_ecnclcust_validation.json"
)

_REQUIRED_LABEL_FRAGMENTS = (
    "Household consumers and individuals",
    "Federal government",
    "State and local governments",
    "Not-for-profit organizations",
)


def main() -> int:
    fba = getFlowByActivity(datasource="Census_EC", year=2022)
    sales = fba[
        (fba["FlowName"] == "Sales, value of shipments, or revenue")
        & (fba["Location"].astype(str) == US_FIPS)
    ].copy()
    sales["child"] = sales["ActivityProducedBy"].map(map_sas_naics_to_cornerstone)
    waste = sales[sales["child"].notna()]
    labels = sorted({str(x) for x in waste["ActivityConsumedBy"].dropna().unique()})
    missing_frags = [
        frag
        for frag in _REQUIRED_LABEL_FRAGMENTS
        if not any(frag.lower() in lab.lower() for lab in labels)
    ]
    rows = build_customer_class_rows_from_ec_fba(fba)
    sum_ok = True
    sum_detail: dict[str, float] = {}
    for code in CUSTOMER_CLASS_CODES:
        s = float(rows.loc[rows["IndustryCode"] == code, "PercentUsed"].sum())
        sum_detail[code] = s
        if abs(s - 1.0) > 1e-6:
            sum_ok = False

    report = {
        "year": 2022,
        "group": "EC2200CLCUST",
        "n_waste_sales_rows": int(len(waste)),
        "waste_children_present": sorted({c for c in waste["child"].unique() if c}),
        "all_waste_children_covered": set(WASTE_CHILDREN)
        <= set(waste["child"].dropna().unique()),
        "class_labels": labels,
        "missing_required_label_fragments": missing_frags,
        "customer_class_row_count": int(len(rows)),
        "share_sums": sum_detail,
        "share_sums_ok": sum_ok,
        "pass": (
            len(waste) > 0
            and not missing_frags
            and sum_ok
            and set(WASTE_CHILDREN) <= set(waste["child"].dropna().unique())
        ),
    }
    _CACHE.parent.mkdir(parents=True, exist_ok=True)
    _CACHE.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
