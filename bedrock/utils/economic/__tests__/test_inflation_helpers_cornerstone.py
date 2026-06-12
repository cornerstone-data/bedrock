import pytest

from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Vnorm_scrap_corrected,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    get_cornerstone_industry_price_ratio,
    get_rho_inflation_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)


def test_rho_inflation_ratio_is_inverse_of_industry_price_ratio() -> None:
    """useeior PRO margin scaling uses PI[orig]/PI[targ], not PI[targ]/PI[orig]."""
    original_year, target_year = 2017, 2024
    industry = get_cornerstone_industry_price_ratio(original_year, target_year)
    rho = get_rho_inflation_ratio(original_year, target_year)
    product = (industry * rho).replace([float("inf"), float("-inf")], float("nan"))
    max_dev = (product - 1.0).abs().max()
    assert max_dev < 1e-9, f"expected industry * rho == 1, max deviation {max_dev:.2e}"


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


def test_v_inflation_uses_industry_row_axis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: V inflation in
    ``derive_cornerstone_Vnorm_scrap_corrected(apply_inflation=True)`` must
    scale industry rows of V by their own price ratio (axis=0), not commodity
    columns (axis=1). Cornerstone industry & commodity codes overlap on
    ~404/405 string values, so axis=1 silently aligns by name and "works"
    while applying the wrong ratio per cell.

    Discriminating property used here: the per-cell ratio
    ``Vnorm_True / Vnorm_False`` must vary across commodity columns within
    at least some industry rows.

    - Under axis=0 (correct): per-row scaling produces a column-varying ratio
      because each commodity has a different supplier mix
      (pi[i] / weighted_avg_pi[c] depends on c).
    - Under axis=1 (incorrect): uniform per-column scaling cancels in
      column-normalization, leaving V-norm itself unchanged. The True/False
      ratio reduces to a row-wise scrap-correction factor — *constant* across
      commodity columns within each row → row std = 0.
    """
    # apply_inflation=True is the new BEA-derived industry-PI path; pin the
    # flag so the price ratio is industry-indexed (matching V's industry
    # rows). Under update_inflation_factors=False the helper returns
    # commodity-indexed values for the legacy A-matrix flow.
    monkeypatch.setattr(get_usa_config(), 'update_inflation_factors', True)

    Vnorm_True = derive_cornerstone_Vnorm_scrap_corrected(
        apply_inflation=True, target_year=2024
    )
    Vnorm_False = derive_cornerstone_Vnorm_scrap_corrected(apply_inflation=False)

    both_nonzero = (Vnorm_True.abs() > 1e-12) & (Vnorm_False.abs() > 1e-12)
    ratio = (Vnorm_True / Vnorm_False).where(both_nonzero)

    row_stds = ratio.std(axis=1, ddof=0).dropna()
    max_row_std = float(row_stds.max())

    assert max_row_std > 1e-3, (
        f"Vnorm True/False ratio appears row-uniform across commodity columns "
        f"(max row std {max_row_std:.2e}). Under correct axis=0, per-industry "
        f"scaling yields column-varying ratios; under axis=1, uniform column "
        f"scaling cancels in normalization, yielding row-constant ratios."
    )
