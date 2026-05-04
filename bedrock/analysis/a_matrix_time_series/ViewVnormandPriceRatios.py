from __future__ import annotations

from pathlib import Path

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)

INFLATE_V = True
OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_DIR = OUTPUT_DIR / "results"

original_year = 2017
target_year = 2023


def main() -> None:
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected(
        apply_inflation=INFLATE_V, target_year=target_year
    )
    industry = get_cornerstone_industry_price_ratio(original_year, target_year).rename(
        "industry_ratio"
    )

    V = derive_cornerstone_V()
    inddiff = set(industry.index) - set(V.index)
    print(f"Industries in V but not in price index: {inddiff}")

    # Check if index order is different
    v_industries = V.index.tolist()
    industry_indices = industry.index.tolist()
    order_diff = v_industries != industry_indices
    print(f"Index order is different: {order_diff}")
    if order_diff:
        print(f"First 5 V indices: {v_industries[:5]}")
        print(f"First 5 industry indices: {industry_indices[:5]}")

    commodity = get_vnorm_adjusted_commodity_price_ratio(
        original_year, target_year
    ).rename("commodity_ratio")

    Vnorm.to_csv(RESULTS_DIR / "Vnorm2023.csv")
    industry.to_csv(RESULTS_DIR / "industry_price_ratio_2023.csv")
    commodity.to_csv(RESULTS_DIR / "commodity_price_ratio_2023.csv")
    print(
        "Vnorm written to VNorm2023.csv."
        "Industry ratios written to industry_price_ratio_2023.csv"
        "Commodity ratios written to commodity_price_ratio_2023.csv "
    )
    return None


if __name__ == "__main__":
    main()
