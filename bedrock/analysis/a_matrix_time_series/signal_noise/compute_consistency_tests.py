"""Phase B — Internal consistency tests on the LMDI physical effect.

Reads ``lmdi_phys_cells.parquet`` and ``lmdi_phys_by_naics3.csv`` from
Phase A. Phase A produces an algebraic residual — everything in
``A_summary`` not explained by commodity-PI price motion. That residual
mixes:

- real physical/structural change (efficiency, output-mix shifts) — signal
- BEA summary-table revisions and methodological reconciliations — noise
- summary→detail aggregation jitter — noise
- imperfect deflation (commodity PI not perfectly tracking every cell) — residual

Phase B uses statistical properties of the residual to separate these:

1. **Lag-1 autocorrelation across transitions** (pooled and per NAICS-3).
   - Signal ⇒ ρ₁ > 0 — real drift persists year-to-year.
   - Noise ⇒ ρ₁ ≈ 0 — revisions are uncorrelated across years.

2. **Within-NAICS-3 coherence** — ICC = σ²_between / (σ²_between + σ²_within)
   computed on cell-level ``ln_phys_delta_annual``, LMDI-weighted.
   - Signal ⇒ ICC high — industry-wide shifts move cells together.
   - Noise ⇒ ICC low — no group structure.

3. **Magnitude / shape** of ``|phys_effect_avg_annual|`` at NAICS-3 level.
   - Signal ⇒ small (<5–10%/yr), right-skewed (directional drift).
   - Noise ⇒ fat-tailed, symmetric around 0.

All three tests are split by ``kind`` (dom, imp). Domestic vs imported
input channels can plausibly carry different signal-to-noise profiles —
e.g., imports are more price-volatile and may show noisier physical
residuals after commodity-PI deflation.

Outputs:
- ``output/results/lmdi_consistency_tests.csv`` — per-NAICS-3 rows + a
  pooled summary row per kind.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.signal_noise.compute_consistency_tests
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import RESULTS_DIR


def _skew(x: np.ndarray) -> float:
    if x.size < 3:
        return float("nan")
    m = float(np.mean(x))
    s = float(np.std(x, ddof=0))
    if s == 0:
        return float("nan")
    return float(np.mean(((x - m) / s) ** 3))


def _excess_kurtosis(x: np.ndarray) -> float:
    if x.size < 4:
        return float("nan")
    m = float(np.mean(x))
    s = float(np.std(x, ddof=0))
    if s == 0:
        return float("nan")
    return float(np.mean(((x - m) / s) ** 4) - 3.0)


logger = logging.getLogger(__name__)

CELLS_PATH = RESULTS_DIR / "lmdi_phys_cells.parquet"
BY_NAICS3_PATH = RESULTS_DIR / "lmdi_phys_by_naics3.csv"
TESTS_OUT_PATH = RESULTS_DIR / "lmdi_consistency_tests.csv"

# Minimum transitions needed to compute lag-1 autocorr with any
# meaningful degrees of freedom. With 7 transitions we have 6 pairs;
# require at least 4 to keep per-NAICS-3 rows interpretable.
MIN_PAIRS_FOR_AUTOCORR = 4


def _transition_sort_key(t: str) -> int:
    """Sort transitions chronologically: '2017->2018' → 2017."""
    return int(t.split("->")[0])


def _autocorr_per_group(
    by_naics3: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Lag-1 autocorr of phys_effect_avg_annual per (naics3, kind).

    Also returns a pooled-r per kind, computed as Pearson r over all
    (x_t, x_{t+1}) pairs across NAICS-3, after demeaning each NAICS-3's
    series so that cross-NAICS-3 level differences don't drive the
    correlation.
    """
    rows: list[dict[str, object]] = []
    pooled_pairs: dict[str, list[tuple[float, float]]] = {"dom": [], "imp": []}

    for (naics3, kind), grp in by_naics3.groupby(["naics3", "kind"]):
        ordered = grp.sort_values(
            "transition", key=lambda s: s.map(_transition_sort_key)
        )
        series = ordered["phys_effect_avg_annual"].to_numpy(dtype=float)
        if len(series) < MIN_PAIRS_FOR_AUTOCORR + 1:
            continue
        x_prev = series[:-1]
        x_next = series[1:]
        if np.std(x_prev) == 0 or np.std(x_next) == 0:
            r = np.nan
        else:
            r = float(np.corrcoef(x_prev, x_next)[0, 1])
        rows.append(
            {
                "naics3": naics3,
                "kind": kind,
                "n_transitions": int(len(series)),
                "lag1_autocorr": r,
                "mean_phys_annual": float(np.mean(series)),
                "std_phys_annual": float(np.std(series, ddof=1)),
            }
        )
        # Demean for the pooled estimate so level differences don't dominate.
        demeaned = series - np.mean(series)
        for a, b in zip(demeaned[:-1], demeaned[1:]):
            pooled_pairs[str(kind)].append((float(a), float(b)))

    pooled: dict[str, float] = {}
    for kind, pairs in pooled_pairs.items():
        if len(pairs) < 2:
            pooled[kind] = float("nan")
            continue
        arr = np.array(pairs)
        if np.std(arr[:, 0]) == 0 or np.std(arr[:, 1]) == 0:
            pooled[kind] = float("nan")
        else:
            pooled[kind] = float(np.corrcoef(arr[:, 0], arr[:, 1])[0, 1])

    return pd.DataFrame(rows), pooled


def _coherence_per_transition(cells: pd.DataFrame) -> pd.DataFrame:
    """Within-NAICS-3 coherence per (transition, kind).

    Computes LMDI-weighted ICC on cell-level ``ln_phys_delta_annual``:

        ICC = σ²_between / (σ²_between + σ²_within)

    where σ²_between is the weighted variance of NAICS-3-mean physical
    effects, and σ²_within is the weighted-average of within-NAICS-3
    variances. ICC ≈ 1 → cells within a NAICS-3 move together (signal-like
    group structure); ICC ≈ 0 → no group structure (noise-like).
    """
    df = cells.assign(
        naics3=cells["j"].astype(str).str[:3],
        _wx=cells["lmdi_weight"] * cells["ln_phys_delta_annual"],
    )

    rows: list[dict[str, object]] = []
    for (transition, kind), grp in df.groupby(["transition", "kind"], sort=False):
        total_w = float(grp["lmdi_weight"].sum())
        if total_w == 0:
            continue
        grand_mean = float(grp["_wx"].sum() / total_w)

        # Per-NAICS-3 weighted mean μ_g, then between/within sums-of-squares
        # without an inner Python loop.
        per_n = grp.groupby("naics3", sort=False).agg(
            w_sum=("lmdi_weight", "sum"),
            wx_sum=("_wx", "sum"),
        )
        per_n = per_n[per_n["w_sum"] > 0]
        mu = per_n["wx_sum"] / per_n["w_sum"]
        mu_per_row = grp["naics3"].map(mu).to_numpy()
        between_ss = float((per_n["w_sum"] * (mu - grand_mean) ** 2).sum())
        within_ss = float(
            (grp["lmdi_weight"] * (grp["ln_phys_delta_annual"] - mu_per_row) ** 2).sum()
        )
        sigma2_between = between_ss / total_w
        sigma2_within = within_ss / total_w
        sigma2_total = sigma2_between + sigma2_within
        icc = sigma2_between / sigma2_total if sigma2_total > 0 else float("nan")

        rows.append(
            {
                "transition": transition,
                "kind": kind,
                "n_cells": int(len(grp)),
                "n_naics3": int(per_n.shape[0]),
                "sigma2_between": sigma2_between,
                "sigma2_within": sigma2_within,
                "sigma2_total": sigma2_total,
                "icc": icc,
            }
        )
    return pd.DataFrame(rows)


def _magnitude_summary(by_naics3: pd.DataFrame) -> pd.DataFrame:
    """Distribution of |phys_effect_avg_annual| at NAICS-3 level, per kind.

    Signal expectation: mostly small (<5–10 %/yr), right-skewed.
    Noise expectation: fat-tailed, symmetric around 0.
    """
    rows: list[dict[str, object]] = []
    for kind, grp in by_naics3.groupby("kind"):
        x = grp["phys_effect_avg_annual"].to_numpy(dtype=float)
        x = x[np.isfinite(x)]
        # Convert log-effect to percent-change for readability.
        x_pct = (np.exp(x) - 1.0) * 100.0
        abs_pct = np.abs(x_pct)
        rows.append(
            {
                "kind": kind,
                "n": int(x_pct.size),
                "mean_pct": float(np.mean(x_pct)),
                "median_pct": float(np.median(x_pct)),
                "skew": _skew(x_pct),
                "excess_kurtosis": _excess_kurtosis(x_pct),
                "abs_mean_pct": float(np.mean(abs_pct)),
                "abs_p50_pct": float(np.percentile(abs_pct, 50)),
                "abs_p90_pct": float(np.percentile(abs_pct, 90)),
                "abs_p95_pct": float(np.percentile(abs_pct, 95)),
                "frac_within_5pct": float(np.mean(abs_pct < 5.0)),
                "frac_within_10pct": float(np.mean(abs_pct < 10.0)),
                "frac_above_20pct": float(np.mean(abs_pct > 20.0)),
            }
        )
    return pd.DataFrame(rows)


def _build_combined_report(
    autocorr_per_group: pd.DataFrame,
    pooled_autocorr: dict[str, float],
    coherence: pd.DataFrame,
    magnitude: pd.DataFrame,
) -> pd.DataFrame:
    """Stack per-NAICS-3 autocorr rows + pooled summary rows per kind.

    The pooled summary rows carry per-kind aggregates of the coherence
    (mean ICC across transitions) and magnitude tests as well, so the
    CSV has a self-contained per-kind headline row.
    """
    detail = autocorr_per_group.copy()
    detail["scope"] = "naics3"

    pooled_rows: list[dict[str, object]] = []
    for kind in sorted(magnitude["kind"].unique()):
        coh_kind = coherence[coherence["kind"] == kind]
        mag_kind = magnitude[magnitude["kind"] == kind].iloc[0]
        pooled_rows.append(
            {
                "scope": "pooled",
                "naics3": "ALL",
                "kind": kind,
                "n_transitions": coh_kind["n_cells"].size,
                "lag1_autocorr": pooled_autocorr.get(kind, float("nan")),
                "mean_phys_annual": float("nan"),
                "std_phys_annual": float("nan"),
                "mean_icc": float(coh_kind["icc"].mean()),
                "median_icc": float(coh_kind["icc"].median()),
                "abs_mean_pct": float(mag_kind["abs_mean_pct"]),
                "abs_p50_pct": float(mag_kind["abs_p50_pct"]),
                "abs_p90_pct": float(mag_kind["abs_p90_pct"]),
                "skew": float(mag_kind["skew"]),
                "excess_kurtosis": float(mag_kind["excess_kurtosis"]),
                "frac_within_5pct": float(mag_kind["frac_within_5pct"]),
                "frac_within_10pct": float(mag_kind["frac_within_10pct"]),
                "frac_above_20pct": float(mag_kind["frac_above_20pct"]),
            }
        )
    pooled_df = pd.DataFrame(pooled_rows)
    return pd.concat([pooled_df, detail], ignore_index=True)


def _print_headline(
    pooled_autocorr: dict[str, float],
    coherence: pd.DataFrame,
    magnitude: pd.DataFrame,
) -> None:
    print("\n=== Phase B — Internal consistency headlines ===")
    for kind in sorted(magnitude["kind"].unique()):
        mag_kind = magnitude[magnitude["kind"] == kind].iloc[0]
        coh_kind = coherence[coherence["kind"] == kind]
        print(f"\n[kind={kind}]")
        print(
            f"  Lag-1 autocorr (pooled, demeaned): {pooled_autocorr.get(kind, float('nan')):+.3f}"
            "    [+ = persistent drift; ~0 = noise]"
        )
        print(
            f"  Coherence (mean ICC across transitions): {coh_kind['icc'].mean():.3f}"
            f"   (median: {coh_kind['icc'].median():.3f})"
            "    [~1 = group structure; ~0 = no signal]"
        )
        print(
            f"  Magnitude: median |phys|={mag_kind['abs_p50_pct']:.2f}%/yr,"
            f"  p90={mag_kind['abs_p90_pct']:.2f}%/yr,"
            f"  frac<5%={mag_kind['frac_within_5pct']:.1%},"
            f"  frac>20%={mag_kind['frac_above_20pct']:.1%}"
        )
        print(
            f"  Shape: mean={mag_kind['mean_pct']:+.2f}%/yr,"
            f"  skew={mag_kind['skew']:+.2f},"
            f"  excess_kurt={mag_kind['excess_kurtosis']:+.2f}"
            "    [symmetric+heavy tails = noise]"
        )


def main() -> None:
    logger.info("Loading Phase A outputs...")
    cells = pd.read_parquet(CELLS_PATH)
    by_naics3 = pd.read_csv(BY_NAICS3_PATH, dtype={"naics3": str})

    logger.info("Computing lag-1 autocorr per (NAICS-3, kind)...")
    autocorr_per_group, pooled_autocorr = _autocorr_per_group(by_naics3)
    logger.info("  %d (NAICS-3, kind) groups", len(autocorr_per_group))

    logger.info("Computing within-NAICS-3 coherence per (transition, kind)...")
    coherence = _coherence_per_transition(cells)

    logger.info("Computing magnitude/shape per kind...")
    magnitude = _magnitude_summary(by_naics3)

    combined = _build_combined_report(
        autocorr_per_group, pooled_autocorr, coherence, magnitude
    )
    TESTS_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(TESTS_OUT_PATH, index=False)
    logger.info("  → %s (%d rows)", TESTS_OUT_PATH, len(combined))

    _print_headline(pooled_autocorr, coherence, magnitude)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
