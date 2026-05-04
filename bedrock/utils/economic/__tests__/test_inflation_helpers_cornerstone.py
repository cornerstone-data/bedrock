from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_vnorm_adjusted_commodity_price_ratio,
)


def test_vnorm_commodity_price_ratio_is_identity_at_year_to_self() -> None:
    """When original_year == target_year the industry-level price ratio is
    1.0 everywhere, so the V-norm-weighted commodity-level ratio must also
    be 1.0 for every commodity (modulo floating-point noise).

    Regression guard: ``derive_cornerstone_Vnorm_scrap_corrected`` applies a
    row-axis scaling that leaves V_norm columns drifting >1, so a naive
    `V_norm.T @ r_ind` would return ~1.05–1.07 instead of 1 here. The helper
    must column-normalize V_norm before applying as weights.
    """
    ratio = get_vnorm_adjusted_commodity_price_ratio(2017, 2017)
    max_abs_dev = (ratio - 1.0).abs().max()
    assert (
        max_abs_dev < 1e-12
    ), f"Expected ratio == 1.0 at year=year, got max abs deviation {max_abs_dev:.2e}"
