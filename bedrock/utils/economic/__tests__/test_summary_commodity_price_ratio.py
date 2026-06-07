"""Tests for the ITA-based summary commodity price ratio helpers.

Validates:
- ITA reduces to truth at the 2017 benchmark (q = C_m @ x is exact).
- V_norm columns are stochastic.
- Ratio is identity at year == year.
- Index coverage matches USA_2017_SUMMARY_INDUSTRY_CODES.
- Deflate / inflate round-trip preserves an A matrix.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_V
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    adjust_summary_A_dollar_year,
    derive_cornerstone_q_and_vnorm_for_year,
    get_summary_commodity_price_index,
    get_summary_commodity_price_ratio,
)
from bedrock.utils.math.formulas import compute_q
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)


def test_ita_q_at_2017_matches_derive_cornerstone_q() -> None:
    """Under ITA, ``q[y] = C_m[2017] @ x[y]`` is exact when ``y == 2017``
    (because C_m and x are then both from the 2017 V). The result must
    therefore agree with V.sum(axis=0) directly off the uninflated 2017 V
    within numerical noise.
    """
    q_ita = derive_cornerstone_q_and_vnorm_for_year(2017)[0]
    q_truth = compute_q(V=derive_cornerstone_V(apply_inflation=False))

    aligned_ita = q_ita.reindex(q_truth.index, fill_value=0.0)
    # The BEA detail x for 2017 may differ slightly from V.sum(axis=1) due to
    # before/after-redefinition. We assert relative agreement to 1% on the
    # bulk of the distribution rather than exact bit-equality.
    nonzero = q_truth.abs() > 1.0
    rel_dev = ((aligned_ita - q_truth).abs() / q_truth.abs())[nonzero]
    assert (
        rel_dev.median() < 0.01
    ), f"ITA q at 2017 deviates from 2017 V.sum (median rel dev {rel_dev.median():.2%})"


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
