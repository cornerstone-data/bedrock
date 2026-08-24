from __future__ import annotations

from typing import Callable

import pytest

from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_disaggregation_enabled,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    _derive_post_reallocation_checkpoint_for_disagg,
    build_electricity_detail_GO_growth_ratios,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_2017_eia_purchaser_allocation,
)
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    _aggregate_industry_pi,
    _cornerstone_indexed_industry_pi,
    _get_summary_industry_price_index,
    clear_cornerstone_inflation_caches,
    get_cornerstone_industry_price_ratio,
    get_rho_inflation_ratio,
    get_vnorm_adjusted_commodity_price_ratio,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES_ELEC,
    CORNERSTONE_INDUSTRIES_ELEC,
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
    active_cornerstone_industries,
)

# Same cache set as test_electricity_disaggregation: flag caches are keyed only
# on years, so config switches must clear disagg + derived + inflation together.
_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    derive_disagg_io_bundle,
    cornerstone_sector_disagg_active,
    derive_disagg_Ytot_with_trade,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    build_electricity_detail_GO_growth_ratios,
    get_2017_eia_purchaser_allocation,
    _derive_post_reallocation_checkpoint_for_disagg,
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    derive_cornerstone_VA,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
]


def _clear_all_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_cornerstone_inflation_caches()
    from bedrock.transform.eeio.cornerstone_year_scaling import (  # noqa: PLC0415
        clear_summary_year_scaled_aq,
    )
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        clear_reanchored_electricity_q,
    )

    clear_summary_year_scaled_aq()
    clear_reanchored_electricity_q()


def _setup_config(config_name: str) -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)


def test_rho_inflation_ratio_is_inverse_of_industry_price_ratio() -> None:
    """useeior PRO margin scaling uses PI[orig]/PI[targ], not PI[targ]/PI[orig]."""
    original_year, target_year = 2017, 2024
    industry = get_cornerstone_industry_price_ratio(original_year, target_year)
    rho = get_rho_inflation_ratio(original_year, target_year)
    product = (industry * rho).replace([float('inf'), float('-inf')], float('nan'))
    max_dev = (product - 1.0).abs().max()
    assert max_dev < 1e-9, f'expected industry * rho == 1, max deviation {max_dev:.2e}'


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
    ), f'Expected ratio == 1.0 at year=year, got max abs deviation {max_abs_dev:.2e}'


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
    # rows). Under apply_io_year_adjustments=False the helper returns
    # commodity-indexed values for the legacy A-matrix flow.
    monkeypatch.setattr(get_usa_config(), 'apply_io_year_adjustments', True)

    Vnorm_True = derive_cornerstone_Vnorm_scrap_corrected(
        apply_inflation=True, target_year=2024
    )
    Vnorm_False = derive_cornerstone_Vnorm_scrap_corrected(apply_inflation=False)

    both_nonzero = (Vnorm_True.abs() > 1e-12) & (Vnorm_False.abs() > 1e-12)
    ratio = (Vnorm_True / Vnorm_False).where(both_nonzero)

    row_stds = ratio.std(axis=1, ddof=0).dropna()
    max_row_std = float(row_stds.max())

    assert max_row_std > 1e-3, (
        f'Vnorm True/False ratio appears row-uniform across commodity columns '
        f'(max row std {max_row_std:.2e}). Under correct axis=0, per-industry '
        f'scaling yields column-varying ratios; under axis=1, uniform column '
        f'scaling cancels in normalization, yielding row-constant ratios.'
    )


def test_industry_price_ratio_apply_io_plus_elec_is_industry_elec_indexed() -> None:
    """apply_io + elec: industry PI on industries-elec; commodity PI on commodities-elec."""
    _setup_config('2025_usa_cornerstone_v0_3.yaml')
    try:
        baseline = get_cornerstone_industry_price_ratio(2017, 2024)
        parent_pi = float(baseline.loc[ELECTRICITY_AGGREGATE_SECTOR])
        industry_only_pi = float(baseline.loc['331314'])
    finally:
        _teardown()

    _setup_config('2025_usa_cornerstone_v0_3_electricity_disaggregation.yaml')
    try:
        industry = get_cornerstone_industry_price_ratio(2017, 2024)
        commodity = get_vnorm_adjusted_commodity_price_ratio(2017, 2024)
        assert list(industry.index) == CORNERSTONE_INDUSTRIES_ELEC
        assert list(commodity.index) == CORNERSTONE_COMMODITIES_ELEC
        assert '331314' in industry.index
        assert 'S00402' not in industry.index
        assert ELECTRICITY_AGGREGATE_SECTOR not in industry.index
        for code in ELECTRICITY_DISAGG_SECTORS:
            assert industry.loc[code] == pytest.approx(parent_pi)
        assert industry.loc['331314'] == pytest.approx(industry_only_pi)
        assert industry_only_pi != pytest.approx(1.0)
    finally:
        _teardown()


def test_industry_pi_under_elec_is_industries_elec_indexed() -> None:
    """Elec must expand industry PI to 407; summary \"22\" must keep children."""
    _setup_config('2025_usa_cornerstone_v0_3.yaml')
    try:
        parent_pi = float(
            _cornerstone_indexed_industry_pi(2022).loc[ELECTRICITY_AGGREGATE_SECTOR]
        )
    finally:
        _teardown()

    _setup_config('2025_usa_cornerstone_v0_3_electricity_disaggregation.yaml')
    try:
        pi = _cornerstone_indexed_industry_pi(2022)
        assert list(pi.index) == active_cornerstone_industries()
        assert ELECTRICITY_AGGREGATE_SECTOR not in pi.index
        for code in ELECTRICITY_DISAGG_SECTORS:
            assert float(pi.loc[code]) == pytest.approx(parent_pi)

        from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (  # noqa: PLC0415
            load_bea_v2017_industry_to_bea_v2017_summary,
        )

        x_y = derive_cornerstone_x()
        bea_fixed: dict[str, list[str]] = {
            str(k): [str(s) for s in v]
            for k, v in load_bea_v2017_industry_to_bea_v2017_summary().items()
        }
        parent_summaries = list(bea_fixed.get(ELECTRICITY_AGGREGATE_SECTOR, ['22']))
        bea_fixed.pop(ELECTRICITY_AGGREGATE_SECTOR, None)
        for child in ELECTRICITY_DISAGG_SECTORS:
            bea_fixed[child] = list(parent_summaries)
        expected_22 = _aggregate_industry_pi(pi, x_y, bea_fixed)['22']
        assert float(
            _get_summary_industry_price_index(2022).loc['22']
        ) == pytest.approx(expected_22)
    finally:
        _teardown()
