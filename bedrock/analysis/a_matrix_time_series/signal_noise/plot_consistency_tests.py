"""Phase B.2 — Visualize the LMDI consistency tests.

Two plots, split by ``kind`` (dom, imp):

- ``lmdi_consistency_autocorr.png`` — pooled lag-1 scatter (x_t vs x_{t+1})
  with per-NAICS-3 autocorr histogram inset. Tight diagonal → signal-like
  drift; round cloud → noise-like.

- ``lmdi_consistency_magnitude.png`` — histogram of |phys_effect_avg_annual|
  in %/yr on a symmetric x-axis, with reference lines at ±5 %/yr and
  ±10 %/yr (typical signal range) and ±20 %/yr (implausible).

(The within-NAICS-3 coherence test 2 is still computed and reported in
``lmdi_consistency_tests.csv``; the per-transition ICC bar chart was
dropped as visually uninformative — all bars cluster near 0.18.)

Usage:
    python -m bedrock.analysis.a_matrix_time_series.signal_noise.plot_consistency_tests
"""

from __future__ import annotations

import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import PLOTS_DIR, RESULTS_DIR

logger = logging.getLogger(__name__)

BY_NAICS3_PATH = RESULTS_DIR / "lmdi_phys_by_naics3.csv"
TESTS_PATH = RESULTS_DIR / "lmdi_consistency_tests.csv"

AUTOCORR_PLOT = PLOTS_DIR / "lmdi_consistency_autocorr.png"
MAGNITUDE_PLOT = PLOTS_DIR / "lmdi_consistency_magnitude.png"


def _transition_sort_key(t: str) -> int:
    return int(t.split("->")[0])


def _pooled_pairs(by_naics3: pd.DataFrame, kind: str) -> tuple[np.ndarray, np.ndarray]:
    """Demeaned (x_t, x_{t+1}) pairs across NAICS-3 for one kind."""
    pairs_a: list[float] = []
    pairs_b: list[float] = []
    sub = by_naics3[by_naics3["kind"] == kind]
    for _, grp in sub.groupby("naics3"):
        ordered = grp.sort_values(
            "transition", key=lambda s: s.map(_transition_sort_key)
        )
        series = ordered["phys_effect_avg_annual"].to_numpy(dtype=float)
        if series.size < 2:
            continue
        demeaned = series - np.mean(series)
        pairs_a.extend(demeaned[:-1].tolist())
        pairs_b.extend(demeaned[1:].tolist())
    return np.array(pairs_a), np.array(pairs_b)


_DEMEAN_CAPTION = (
    "Why demean?  Some NAICS-3 sit at a non-zero baseline year after year (petroleum 324 runs ~+15%/yr "
    "median; cement 327 near +1.5%/yr). On raw values, every consecutive pair for 324 would land in the "
    "upper-right corner just because 324 runs hot — the scatter would $\\bf{look}$ correlated even if "
    "its YoY motion was random noise around that high baseline. The pooled $r$ would be inflated by "
    "between-NAICS-3 level differences rather than measuring the question we care about.\n\n"
    "Demeaning strips the sector-specific level out, leaving only within-sector fluctuation. The pooled "
    "$r$ on demeaned data answers: \"is the year-to-year $\\it{deviation}$ from each sector's own trend "
    "persistent (signal) or sign-flipping (noise)?\"\n\n"
    "The per-NAICS-3 $r$ in the inset is computed on the same series, but correlation is scale/location "
    "invariant — so within a single NAICS-3 demeaning is just a constant shift and the per-NAICS-3 $r$ "
    "is unchanged. Demeaning only matters for the pooled estimate."
)


def _plot_autocorr(by_naics3: pd.DataFrame, tests: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 9))
    pooled = tests[tests["scope"] == "pooled"].set_index("kind")
    per_grp = tests[tests["scope"] == "naics3"]
    lim_pct = 25.0  # %/yr axis range

    for ax, kind in zip(axes, ["dom", "imp"]):
        a, b = _pooled_pairs(by_naics3, kind)
        a_pct = (np.exp(a) - 1.0) * 100.0
        b_pct = (np.exp(b) - 1.0) * 100.0
        ax.axhline(0, color="gray", linewidth=0.6, alpha=0.5)
        ax.axvline(0, color="gray", linewidth=0.6, alpha=0.5)
        ax.plot(
            [-lim_pct, lim_pct],
            [-lim_pct, lim_pct],
            color="red",
            linewidth=1.0,
            linestyle=":",
            alpha=0.6,
            label="y=x (perfect persistence)",
        )
        ax.scatter(a_pct, b_pct, s=14, alpha=0.4, color="steelblue", edgecolor="none")

        r = float(pooled.loc[kind, "lag1_autocorr"])  # type: ignore[arg-type]
        ax.set_xlim(-lim_pct, lim_pct)
        ax.set_ylim(-lim_pct, lim_pct)
        ax.set_xlabel("Demeaned phys-effect at t (%/yr)")
        ax.set_ylabel("Demeaned phys-effect at t+1 (%/yr)")
        ax.set_title(
            f"kind = {kind}    pooled lag-1 r = {r:+.3f}\n"
            f"({a_pct.size} consecutive-year pairs across NAICS-3)"
        )
        ax.set_aspect("equal")
        ax.grid(alpha=0.3)
        ax.legend(loc="lower right", fontsize=9)

        # Inset: per-NAICS-3 autocorr histogram.
        ins = ax.inset_axes((0.04, 0.62, 0.30, 0.33))
        per_r = per_grp[per_grp["kind"] == kind]["lag1_autocorr"].dropna()
        ins.hist(
            per_r,
            bins=np.linspace(-1, 1, 21),
            color="steelblue",
            edgecolor="black",
            linewidth=0.4,
            alpha=0.85,
        )
        ins.axvline(0, color="gray", linewidth=0.6)
        ins.axvline(
            per_r.median(),
            color="red",
            linewidth=1.0,
            linestyle="--",
            label=f"median {per_r.median():+.2f}",
        )
        ins.set_xlim(-1, 1)
        ins.set_xlabel("per-NAICS-3 r", fontsize=8)
        ins.tick_params(labelsize=7)
        ins.legend(fontsize=7, framealpha=0.7)
        ins.set_title("per-NAICS-3 lag-1 r", fontsize=8)

    fig.suptitle(
        "Test 1: lag-1 autocorrelation of annual physical-effect across transitions\n"
        "Tight diagonal cloud → persistent (signal); round cloud → uncorrelated (noise)",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0.32, 1, 1))
    fig.text(
        0.04,
        0.30,
        _DEMEAN_CAPTION,
        ha="left",
        va="top",
        fontsize=9.5,
        wrap=True,
        bbox=dict(
            boxstyle="round,pad=0.6",
            facecolor="#f7f7f7",
            edgecolor="#cccccc",
            linewidth=0.6,
        ),
    )
    AUTOCORR_PLOT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(AUTOCORR_PLOT, dpi=120, bbox_inches="tight")
    plt.close(fig)


_MAG_CAPTION_DOM = (
    "$\\bf{Noise\\text{-}shaped,\\ thin\\ signal\\ admixture.}$\n"
    "• Sharp central peak at 0; 58% of cells within ±5%/yr.\n"
    "• $\\bf{Symmetric}$ around 0 — but real efficiency drift should\n"
    "  be right-skewed (sectors typically improve, rarely worsen).\n"
    "• Fat tails to ±30%/yr (excess kurt +12.9); ~6% of cells with\n"
    "  implausible >20%/yr spikes — BEA-revision step-change signature.\n"
    "• Read: mostly quiet noise + a revision-spike tail; possible thin\n"
    "  real-signal admixture in the center, not separable visually."
)
_MAG_CAPTION_IMP = (
    "$\\bf{Fully\\ noise\\ shaped.}$\n"
    "• No sharp central peak — broad, almost rectangular from −15 to\n"
    "  +15%/yr, plus bumps at the ±30% clip edges.\n"
    "• Median |x| = 7.8%/yr, p90 = 24.1%/yr — far above plausible\n"
    "  physical-change scale (typically 1–3%/yr).\n"
    "• Essentially no characteristic scale, opposite of real change.\n"
    "• Read: residual dominated by mis-deflation — commodity-PI doesn't\n"
    "  track imported landed prices well, leaving residual price motion\n"
    "  without a natural amplitude."
)


def _plot_magnitude(by_naics3: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(13, 8.5))
    lim_pct = 30.0
    bins = np.linspace(-lim_pct, lim_pct, 31)
    for ax, kind, color in zip(axes, ["dom", "imp"], ["#1f77b4", "#d62728"]):
        x = by_naics3[by_naics3["kind"] == kind]["phys_effect_avg_annual"].to_numpy()
        x = x[np.isfinite(x)]
        x_pct = (np.exp(x) - 1.0) * 100.0
        # Clip for display but report fraction clipped.
        clipped = np.clip(x_pct, -lim_pct, lim_pct)
        ax.hist(
            clipped,
            bins=bins,
            color=color,
            alpha=0.75,
            edgecolor="black",
            linewidth=0.4,
        )
        for thresh, lbl, c, ls in [
            (5, "±5%", "green", ":"),
            (10, "±10%", "orange", ":"),
            (20, "±20%", "red", "--"),
        ]:
            ax.axvline(+thresh, color=c, linestyle=ls, linewidth=1.0, alpha=0.7)
            ax.axvline(
                -thresh, color=c, linestyle=ls, linewidth=1.0, alpha=0.7, label=lbl
            )
        ax.axvline(0, color="black", linewidth=0.8, alpha=0.6)
        ax.set_xlim(-lim_pct, lim_pct)
        median_abs = float(np.median(np.abs(x_pct)))
        p90_abs = float(np.percentile(np.abs(x_pct), 90))
        ax.set_title(
            f"kind = {kind}    median |x| = {median_abs:.1f}%/yr"
            f"    p90 |x| = {p90_abs:.1f}%/yr\n"
            f"(n = {x_pct.size} NAICS-3×transition cells; "
            f"{(np.abs(x_pct) > lim_pct).mean():.1%} beyond axis)"
        )
        ax.set_xlabel("Annualized phys-effect (%/yr)")
        ax.set_ylabel("Count of NAICS-3 × transition")
        ax.grid(axis="y", alpha=0.3)
        ax.set_axisbelow(True)
        ax.legend(loc="upper right", fontsize=9, framealpha=0.9)

    fig.suptitle(
        "Test 3: magnitude / shape of NAICS-3 annual physical-effect\n"
        "Signal: small, right-skewed.  Noise: fat-tailed, symmetric around 0.",
        fontsize=12,
    )
    fig.tight_layout(rect=(0, 0.34, 1, 1))
    for x_left, caption in [(0.045, _MAG_CAPTION_DOM), (0.535, _MAG_CAPTION_IMP)]:
        fig.text(
            x_left,
            0.32,
            caption,
            ha="left",
            va="top",
            fontsize=9.5,
            bbox=dict(
                boxstyle="round,pad=0.6",
                facecolor="#f7f7f7",
                edgecolor="#cccccc",
                linewidth=0.6,
            ),
        )
    fig.savefig(MAGNITUDE_PLOT, dpi=120, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    logger.info("Loading Phase A + B outputs...")
    by_naics3 = pd.read_csv(BY_NAICS3_PATH, dtype={"naics3": str})
    tests = pd.read_csv(TESTS_PATH, dtype={"naics3": str})

    logger.info("Plotting autocorrelation...")
    _plot_autocorr(by_naics3, tests)
    logger.info("  → %s", AUTOCORR_PLOT)

    logger.info("Plotting magnitude...")
    _plot_magnitude(by_naics3)
    logger.info("  → %s", MAGNITUDE_PLOT)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
