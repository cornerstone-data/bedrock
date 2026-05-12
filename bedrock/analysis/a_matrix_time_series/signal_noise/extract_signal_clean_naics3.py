"""Phase B.3 — Identify NAICS-3 groups whose physical residual looks signal-like.

A NAICS-3 × kind is flagged signal-clean when its 7-transition series of
``phys_effect_avg_annual`` passes all three internal-consistency thresholds:

- **Persistence**: per-group lag-1 r > ``MIN_R``. Real efficiency drift
  shouldn't flip sign every year.
- **Plausible magnitude**: median |annualized phys-effect| ≤ ``MAX_MEDIAN_PCT``.
  Multi-percent-per-year structural change is reasonable; double-digit isn't.
- **Bounded tail**: max single-transition |effect| ≤ ``MAX_TAIL_PCT``. A
  single ±30 % year drowns out 6 quiet years and almost always means a BEA
  reclassification, not real motion.

Impact ranking from Phase A (sum of |lmdi_phys_contrib| per NAICS-3) is
attached so we can read both "which sectors are clean" and "which sectors
that matter for N are clean".

Outputs:
- ``output/results/lmdi_signal_clean_naics3.csv`` — full ranking with the
  three thresholds and pass/fail per (NAICS-3, kind).

Usage:
    python -m bedrock.analysis.a_matrix_time_series.signal_noise.extract_signal_clean_naics3
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR

logger = logging.getLogger(__name__)

BY_NAICS3_PATH = RESULTS_DIR / "lmdi_phys_by_naics3.csv"
TESTS_PATH = RESULTS_DIR / "lmdi_consistency_tests.csv"
OUT_PATH = RESULTS_DIR / "lmdi_signal_clean_naics3.csv"

# Thresholds. Chosen to be lenient on the persistence side (so we don't
# falsely reject sectors whose 6 pairs happen to be unlucky) and stricter
# on magnitude (so we don't admit sectors whose "signal" is really
# revision-driven steps).
MIN_R: float = 0.20  # lag-1 r threshold (per-NAICS-3, n=6)
MAX_MEDIAN_PCT: float = 7.0  # %/yr — typical signal is < 5 %/yr
MAX_TAIL_PCT: float = 25.0  # %/yr — single-year spikes above this = BEA jitter


def _impact_ranking(by_naics3: pd.DataFrame) -> pd.DataFrame:
    return (
        by_naics3.assign(_impact=lambda d: d["lmdi_phys_contrib"].abs())
        .groupby(["naics3", "kind"], as_index=False)
        .agg(abs_impact_sum=("_impact", "sum"))
    )


def _per_group_stats(by_naics3: pd.DataFrame) -> pd.DataFrame:
    """Per (naics3, kind): median + max |phys_effect_avg_annual| as %/yr."""
    df = by_naics3.copy()
    df["pct_annual"] = (np.exp(df["phys_effect_avg_annual"]) - 1.0) * 100.0
    out = df.groupby(["naics3", "kind"], as_index=False).agg(
        n_transitions=("transition", "count"),
        median_abs_pct=("pct_annual", lambda s: float(np.median(np.abs(s)))),
        max_abs_pct=("pct_annual", lambda s: float(np.max(np.abs(s)))),
        mean_pct=("pct_annual", "mean"),
    )
    return out


def main() -> None:
    logger.info("Loading Phase A + B outputs...")
    by_naics3 = pd.read_csv(BY_NAICS3_PATH, dtype={"naics3": str})
    tests = pd.read_csv(TESTS_PATH, dtype={"naics3": str})

    autocorr = tests[tests["scope"] == "naics3"][
        ["naics3", "kind", "lag1_autocorr"]
    ].rename(columns={"lag1_autocorr": "r_lag1"})
    impact = _impact_ranking(by_naics3)
    mag = _per_group_stats(by_naics3)

    merged = impact.merge(autocorr, on=["naics3", "kind"], how="left").merge(
        mag, on=["naics3", "kind"], how="left"
    )
    merged["pass_persistence"] = merged["r_lag1"] > MIN_R
    merged["pass_magnitude"] = merged["median_abs_pct"] <= MAX_MEDIAN_PCT
    merged["pass_tail"] = merged["max_abs_pct"] <= MAX_TAIL_PCT
    merged["pass_all"] = (
        merged["pass_persistence"] & merged["pass_magnitude"] & merged["pass_tail"]
    )

    merged = merged.sort_values(
        ["pass_all", "abs_impact_sum"], ascending=[False, False]
    )
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_PATH, index=False)
    logger.info("  → %s (%d rows)", OUT_PATH, len(merged))

    # Headline summary.
    print("\n=== Pass rate per kind ===")
    for kind, grp in merged.groupby("kind"):
        n_total = len(grp)
        n_pass = int(grp["pass_all"].sum())
        impact_share = float(grp.loc[grp["pass_all"], "abs_impact_sum"].sum()) / max(
            float(grp["abs_impact_sum"].sum()), 1e-12
        )
        print(
            f"  {kind}: {n_pass}/{n_total} NAICS-3 pass all 3 tests"
            f"   ({impact_share:.1%} of total |impact|)"
        )

    print("\n=== Top signal-clean NAICS-3 by |impact|, dom ===")
    clean_dom = merged[(merged["kind"] == "dom") & merged["pass_all"]].head(15)
    print(
        clean_dom[
            [
                "naics3",
                "abs_impact_sum",
                "r_lag1",
                "median_abs_pct",
                "max_abs_pct",
                "mean_pct",
            ]
        ]
        .round(3)
        .to_string(index=False)
    )

    print("\n=== Top signal-clean NAICS-3 by |impact|, imp ===")
    clean_imp = merged[(merged["kind"] == "imp") & merged["pass_all"]].head(15)
    print(
        clean_imp[
            [
                "naics3",
                "abs_impact_sum",
                "r_lag1",
                "median_abs_pct",
                "max_abs_pct",
                "mean_pct",
            ]
        ]
        .round(3)
        .to_string(index=False)
    )

    print("\n=== Top NAICS-3 by |impact| (regardless of pass), dom ===")
    print(
        merged[merged["kind"] == "dom"]
        .head(15)[
            [
                "naics3",
                "abs_impact_sum",
                "r_lag1",
                "median_abs_pct",
                "max_abs_pct",
                "pass_persistence",
                "pass_magnitude",
                "pass_tail",
            ]
        ]
        .round(3)
        .to_string(index=False)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
