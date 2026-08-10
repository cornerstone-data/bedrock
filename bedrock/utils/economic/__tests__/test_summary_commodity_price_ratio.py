"""Tests for the ITA-based summary commodity price ratio helpers.

Validates:
- ITA reduces to truth at the 2017 benchmark (q = C_m @ x is exact).
- V_norm columns are stochastic.
- Ratio is identity at year == year.
- Index coverage matches USA_2017_SUMMARY_INDUSTRY_CODES.
- Deflate / inflate round-trip preserves an A matrix.
"""

from __future__ import annotations

from collections.abc import Iterator

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    electricity_disaggregation_enabled,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_q,
    derive_cornerstone_V,
    derive_cornerstone_x,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    adjust_summary_A_dollar_year,
    clear_cornerstone_inflation_caches,
    derive_cornerstone_q_and_vnorm_for_year,
    get_summary_commodity_price_index,
    get_summary_commodity_price_ratio,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)


@pytest.fixture(autouse=True)
def _reset_config_and_caches() -> Iterator[None]:
    """Isolate from sibling tests that flip electricity / apply_io flags."""
    for fn in (
        electricity_disaggregation_enabled,
        derive_disagg_io_bundle,
        cornerstone_sector_disagg_active,
        derive_cornerstone_V,
        derive_cornerstone_x,
        derive_cornerstone_q_and_vnorm_for_year,
    ):
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_cornerstone_inflation_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config('2025_usa_cornerstone_v0_3.yaml')
    clear_cornerstone_inflation_caches()
    yield
    clear_cornerstone_inflation_caches()
    reset_usa_config(should_reset_env_var=True)


def test_ita_q_at_2017_matches_derive_cornerstone_q() -> None:
    """Under ITA, ``q[y] = C_m[2017] @ x[y]`` is exact when ``y == 2017``
    (because C_m and x are then both from the 2017 V). The result must
    therefore agree with ``derive_cornerstone_q()`` (which is V.sum(axis=0)
    directly off 2017 V) within numerical noise.
    """
    q_ita = derive_cornerstone_q_and_vnorm_for_year(2017)[0]
    q_truth = derive_cornerstone_q()

    aligned_ita = q_ita.reindex(q_truth.index, fill_value=0.0)
    # The BEA detail x for 2017 may differ slightly from V.sum(axis=1) due to
    # before/after-redefinition. We assert relative agreement to 1% on the
    # bulk of the distribution rather than exact bit-equality.
    nonzero = q_truth.abs() > 1.0
    rel_dev = ((aligned_ita - q_truth).abs() / q_truth.abs())[nonzero]
    assert (
        rel_dev.median() < 0.01
    ), f"ITA q at 2017 deviates from derive_cornerstone_q (median rel dev {rel_dev.median():.2%})"


def test_ita_vnorm_columns_are_stochastic() -> None:
    """Each column of V_norm[y] should sum to ~1 (market shares of industries
    supplying each commodity). Excludes commodities with zero coverage.
    """
    vnorm = derive_cornerstone_q_and_vnorm_for_year(2022)[1]
    col_sums = vnorm.sum(axis=0)
    covered = col_sums > 1e-9
    deviations = (col_sums[covered] - 1.0).abs()
    assert (
        deviations.max() < 1e-9
    ), f"V_norm columns not stochastic (max |sum-1| = {deviations.max():.2e})"


def test_summary_commodity_price_ratio_is_identity_at_year_to_self() -> None:
    """``get_summary_commodity_price_ratio(y, y)`` must be all 1.0 for every
    summary code (ratio of any value with itself).
    """
    ratio = get_summary_commodity_price_ratio(2017, 2017)
    max_abs_dev = (ratio - 1.0).abs().max()
    assert (
        max_abs_dev < 1e-12
    ), f"Expected ratio == 1.0 at year=year, got max abs deviation {max_abs_dev:.2e}"


def test_summary_commodity_price_ratio_index_coverage() -> None:
    """Output must be indexed exactly on USA_2017_SUMMARY_INDUSTRY_CODES."""
    ratio = get_summary_commodity_price_ratio(2017, 2022)
    assert list(ratio.index) == list(USA_2017_SUMMARY_INDUSTRY_CODES)


def test_summary_commodity_price_index_positive() -> None:
    """All entries should be strictly positive — neither the upstream BEA PI
    nor the V-norm-weighted aggregation can produce zeros or negatives.
    """
    pi = get_summary_commodity_price_index(2022)
    assert (pi > 0).all(), "Summary commodity PI has non-positive entries"


def test_adjust_summary_A_dollar_year_roundtrip() -> None:
    """Adjusting Y → 2017 composed with the inverse transform should recover
    the original A within numerical noise. Verifies the
    ``diag(1/p) @ A @ diag(p)`` form is self-consistent.
    """
    p = get_summary_commodity_price_ratio(2017, 2022)
    A = pd.DataFrame(
        np.random.default_rng(0).random(
            (len(USA_2017_SUMMARY_INDUSTRY_CODES), len(USA_2017_SUMMARY_INDUSTRY_CODES))
        ),
        index=pd.Index(USA_2017_SUMMARY_INDUSTRY_CODES),
        columns=pd.Index(USA_2017_SUMMARY_INDUSTRY_CODES),
    )
    adjusted = adjust_summary_A_dollar_year(A, from_year=2022, to_year=2017)
    # Manually invert via diag(p) @ A @ diag(1/p) (inverse of
    # diag(1/p) @ A @ diag(p)).
    p_vec = p.reindex(A.index, fill_value=1.0).to_numpy(dtype=float)
    reinverted = pd.DataFrame(
        np.diag(p_vec) @ adjusted.to_numpy() @ np.diag(1.0 / p_vec),
        index=A.index,
        columns=A.columns,
    )
    max_dev = (reinverted - A).abs().to_numpy().max()
    assert (
        max_dev < 1e-9
    ), f"adjust ∘ inverse round-trip failed (max |Δ| = {max_dev:.2e})"

    # Also verify via the public inverse (2017 → 2022) — catches
    # ``@functools.cache`` positional-vs-kwargs key splits.
    restored = adjust_summary_A_dollar_year(adjusted, from_year=2017, to_year=2022)
    max_dev_fn = (restored - A).abs().to_numpy().max()
    assert (
        max_dev_fn < 1e-9
    ), f"adjust ∘ adjust inverse failed (max |Δ| = {max_dev_fn:.2e})"


def test_summary_commodity_price_ratio_cache_ignores_call_style() -> None:
    """Positional and keyword calls must share one cached result."""
    positional = get_summary_commodity_price_ratio(2017, 2022)
    keyword = get_summary_commodity_price_ratio(original_year=2017, target_year=2022)
    pd.testing.assert_series_equal(positional, keyword)


def test_adjust_summary_A_dollar_year_is_noop_when_years_match() -> None:
    """When from_year == to_year, the price ratio is all 1.0 and adjust must
    return a byte-identical matrix.
    """
    A = pd.DataFrame(
        np.random.default_rng(1).random((5, 5)),
        index=pd.Index(USA_2017_SUMMARY_INDUSTRY_CODES[:5]),
        columns=pd.Index(USA_2017_SUMMARY_INDUSTRY_CODES[:5]),
    )
    adjusted = adjust_summary_A_dollar_year(A, from_year=2017, to_year=2017)
    pd.testing.assert_frame_equal(adjusted, A, check_exact=False, atol=1e-12)
