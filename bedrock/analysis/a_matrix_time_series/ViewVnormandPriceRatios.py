from __future__ import annotations

from bedrock.analysis.a_matrix_time_series.constants import (
    LATEST_TARGET_YEAR,
    ORIGINAL_YEAR,
    RESULTS_DIR,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_cornerstone_industry_price_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)

original_year = ORIGINAL_YEAR
target_year = LATEST_TARGET_YEAR


def main() -> None:
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected(
        apply_inflation=get_usa_config().apply_inflation_to_V,
        target_year=target_year,
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

    vnorm_path = RESULTS_DIR / f"Vnorm{target_year}.csv"
    industry_path = RESULTS_DIR / f"industry_price_ratio_{target_year}.csv"
    commodity_path = RESULTS_DIR / f"commodity_price_ratio_{target_year}.csv"
    Vnorm.to_csv(vnorm_path)
    industry.to_csv(industry_path)
    commodity.to_csv(commodity_path)
    print(
        f"Vnorm written to {vnorm_path.name}. "
        f"Industry ratios written to {industry_path.name}. "
        f"Commodity ratios written to {commodity_path.name}."
    )
    return None


if __name__ == "__main__":
    main()
