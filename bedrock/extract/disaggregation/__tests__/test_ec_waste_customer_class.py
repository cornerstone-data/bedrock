"""Unit tests for EC → customer-class Use row builder."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.disaggregation.ec_waste_customer_class import (
    BUSINESS_FIRMS_LABEL,
    CUSTOMER_CLASS_CODES,
    build_business_firms_row_sum_shares_from_ec_fba,
    build_customer_class_rows_from_ec_fba,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.utils.mapping.location import US_FIPS


def _fake_ec_fba_with_dist() -> pd.DataFrame:
    """EC 2022-style: class Sales=0, Dist published, totals on All classes."""
    rows = []
    specs = [
        # naics, label, dist_pct, total_sales (only on All classes)
        ("562111", "All classes of customer", 100.0, 100.0),
        ("562111", "Household consumers and individuals", 70.0, 0.0),
        ("562111", "Federal government", 10.0, 0.0),
        ("562111", "State and local governments", 15.0, 0.0),
        ("562111", "Not-for-profit organizations", 5.0, 0.0),
        ("562111", "Business firms and farms", 50.0, 0.0),
        ("562112", "All classes of customer", 100.0, 100.0),
        ("562112", "Household consumers and individuals", 30.0, 0.0),
        ("562112", "Federal government", 40.0, 0.0),
        ("562112", "State and local governments", 20.0, 0.0),
        ("562112", "Not-for-profit organizations", 10.0, 0.0),
        ("562112", "Business firms and farms", 50.0, 0.0),
        ("562212", "All classes of customer", 100.0, 200.0),
        ("562212", "Business firms and farms", 100.0, 0.0),
    ]
    for naics, label, pct, sales in specs:
        rows.append(
            {
                "ActivityProducedBy": naics,
                "ActivityConsumedBy": label,
                "FlowName": "Distribution of sales, value of shipments, or revenue",
                "FlowAmount": pct,
                "Location": US_FIPS,
            }
        )
        rows.append(
            {
                "ActivityProducedBy": naics,
                "ActivityConsumedBy": label,
                "FlowName": "Sales, value of shipments, or revenue",
                "FlowAmount": sales,
                "Location": US_FIPS,
            }
        )
    return pd.DataFrame(rows)


def test_build_customer_class_rows_from_dist_reconstruction() -> None:
    out = build_customer_class_rows_from_ec_fba(_fake_ec_fba_with_dist())
    assert set(out["IndustryCode"]) == set(CUSTOMER_CLASS_CODES)
    assert set(out["CommodityCode"]) <= set(WASTE_CHILDREN)
    # Households: 70 on 562111 + 30 on 562HAZ → 0.7 / 0.3
    hh = out[out["IndustryCode"] == "F01000"].set_index("CommodityCode")["PercentUsed"]
    assert abs(float(hh["562111"]) - 0.7) < 1e-9
    assert abs(float(hh["562HAZ"]) - 0.3) < 1e-9
    assert abs(float(hh.sum()) - 1.0) < 1e-9
    for code in ("GSLGO", "GSLGE", "GSLGH", "S00203"):
        s = out[out["IndustryCode"] == code].set_index("CommodityCode")["PercentUsed"]
        assert abs(float(s["562111"]) - 15.0 / 35.0) < 1e-9
        assert abs(float(s["562HAZ"]) - 20.0 / 35.0) < 1e-9
    # Business firms never appear as IndustryCode rows on the special-class path.
    assert BUSINESS_FIRMS_LABEL not in set(out["Note"].astype(str))
    assert "Business firms" not in " ".join(out["Note"].astype(str))


def test_build_business_firms_row_sum_shares_from_ec_fba() -> None:
    shares = build_business_firms_row_sum_shares_from_ec_fba(_fake_ec_fba_with_dist())
    assert list(shares.index) == WASTE_CHILDREN
    # 562111: 50, 562HAZ(562112): 50, 562212: 200 → totals 300
    assert abs(float(shares["562111"]) - 50.0 / 300.0) < 1e-9
    assert abs(float(shares["562HAZ"]) - 50.0 / 300.0) < 1e-9
    assert abs(float(shares["562212"]) - 200.0 / 300.0) < 1e-9
    assert abs(float(shares.sum()) - 1.0) < 1e-9


def test_build_business_firms_raises_when_empty() -> None:
    fba = _fake_ec_fba_with_dist()
    fba = fba[
        ~fba["ActivityConsumedBy"]
        .astype(str)
        .str.contains("Business firms", case=False)
    ]
    with pytest.raises(ValueError, match="Business firms"):
        build_business_firms_row_sum_shares_from_ec_fba(fba)
