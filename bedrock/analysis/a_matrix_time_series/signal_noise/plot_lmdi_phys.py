"""Phase A.3 — Visualize the LMDI physical-effect decomposition.

``lmdi_phys_naics3_bars.png`` — top-N NAICS-3 (by impact-weighted contribution)
as groups of stacked horizontal bars. Within each NAICS-3 group, the 7
transitions are stacked top-to-bottom in chronological order (2017→2018 at
top, 2023→2024 at bottom), each as a colored horizontal bar on a shared
x-axis of annualized physical-effect rate. Reads:

- Bars all on one side of 0 → persistent same-sign drift (signal candidate).
- Bars flipping sides across consecutive transitions → oscillation (noise).
- Magnitude reads directly from bar length (no color decoding).
- Direction-change between consecutive years is now adjacent and obvious.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.signal_noise.plot_lmdi_phys
"""

from __future__ import annotations

import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

from bedrock.analysis.a_matrix_time_series.constants import PLOTS_DIR, RESULTS_DIR

logger = logging.getLogger(__name__)

BY_NAICS3_PATH = RESULTS_DIR / "lmdi_phys_by_naics3.csv"
CELLS_PATH = RESULTS_DIR / "lmdi_phys_cells.parquet"
BARS_PATH = PLOTS_DIR / "lmdi_phys_naics3_bars.png"

TOP_N_NAICS3 = 15


def _collapse_dom_imp(by_naics3: pd.DataFrame) -> pd.DataFrame:
    """Sum dom + imp into a single 'total' kind per (NAICS-3, transition)."""
    out = by_naics3.groupby(["naics3", "transition"], as_index=False).agg(
        year_gap=("year_gap", "first"),
        lmdi_phys_contrib=("lmdi_phys_contrib", "sum"),
        lmdi_weight_total=("lmdi_weight_total", "sum"),
        A_sum_curr_total=("A_sum_curr_total", "sum"),
    )
    out["phys_effect_avg"] = out["lmdi_phys_contrib"] / out["lmdi_weight_total"]
    out["phys_effect_avg_annual"] = out["phys_effect_avg"] / out["year_gap"]
    return out


def _plot_naics3_bars(by_naics3: pd.DataFrame) -> None:
    """Top-N NAICS-3 as groups of stacked horizontal bars per transition.

    Per NAICS-3 group: 7 bars stacked top-to-bottom in chronological order,
    each colored by transition. Lets the eye scan for sign-flip oscillation
    between consecutive years.
    """
    naics_total = _collapse_dom_imp(by_naics3)

    impact = (
        naics_total.assign(impact=lambda d: d["lmdi_phys_contrib"].abs())
        .groupby("naics3")["impact"]
        .sum()
        .sort_values(ascending=False)
    )
    top_naics = list(impact.head(TOP_N_NAICS3).index)

    pivot = (
        naics_total[naics_total["naics3"].isin(top_naics)]
        .pivot(index="naics3", columns="transition", values="phys_effect_avg_annual")
        .loc[top_naics]
    )
    pivot_pct = pd.DataFrame(
        (np.exp(pivot.to_numpy()) - 1) * 100,
        index=pivot.index,
        columns=pivot.columns,
    )

    # Time-order transitions (oldest first).
    transitions = sorted(pivot.columns, key=lambda t: int(t.split("->")[0]))
    pivot_pct = pivot_pct[transitions]

    n_trans = len(transitions)
    bar_height = 0.8  # within one y-unit slot
    group_gap = 1.2  # extra space between NAICS-3 groups
    group_pitch = n_trans + group_gap

    x_lo, x_hi = -30.0, 30.0

    fig_h = 0.42 * group_pitch * TOP_N_NAICS3 + 4.0
    fig, ax = plt.subplots(figsize=(22, fig_h))

    cmap = plt.get_cmap("viridis")
    colors = [cmap(i / max(1, n_trans - 1)) for i in range(n_trans)]

    for naics_idx, naics in enumerate(top_naics):
        group_top = naics_idx * group_pitch
        # Alternating light-blue band per NAICS-3 group for visual separation.
        if naics_idx % 2 == 0:
            ax.axhspan(
                group_top - group_gap / 2,
                group_top + (n_trans - 1) + group_gap / 2,
                facecolor="lightblue",
                alpha=0.18,
                zorder=0,
            )
        for trans_idx, transition in enumerate(transitions):
            value = float(pivot_pct.loc[naics, transition])
            if np.isnan(value):
                continue
            y = group_top + trans_idx
            clipped = max(min(value, x_hi), x_lo)
            ax.barh(
                y,
                clipped,
                height=bar_height,
                color=colors[trans_idx],
                edgecolor="black",
                linewidth=0.3,
                zorder=2,
            )
            # Triangle marker for out-of-range values.
            if value > x_hi:
                ax.scatter(
                    x_hi,
                    y,
                    marker=">",
                    color=colors[trans_idx],
                    s=60,
                    zorder=3,
                    edgecolor="black",
                    linewidth=0.4,
                )
            elif value < x_lo:
                ax.scatter(
                    x_lo,
                    y,
                    marker="<",
                    color=colors[trans_idx],
                    s=60,
                    zorder=3,
                    edgecolor="black",
                    linewidth=0.4,
                )

    # Faint horizontal separators between NAICS-3 groups.
    for naics_idx in range(1, TOP_N_NAICS3):
        sep_y = naics_idx * group_pitch - group_gap / 2
        ax.axhline(sep_y, color="gray", linewidth=0.5, alpha=0.4, zorder=1)

    # Y-ticks at center of each group, label = NAICS-3.
    group_centers = [
        naics_idx * group_pitch + (n_trans - 1) / 2 for naics_idx in range(TOP_N_NAICS3)
    ]
    ax.set_yticks(group_centers)
    ax.set_yticklabels([str(n) for n in top_naics], fontsize=33)

    # Highest impact at top.
    y_top = -group_gap / 2
    y_bot = TOP_N_NAICS3 * group_pitch - group_gap / 2
    ax.set_ylim(y_bot, y_top)

    # X-axis: ticks at every 10%, range -30 to +30.
    ax.set_xlim(x_lo, x_hi)
    xticks = list(range(int(x_lo), int(x_hi) + 1, 10))
    ax.set_xticks(xticks)
    ax.set_xticklabels(
        [f"{t:+d}%" if t != 0 else "0" for t in xticks],
        fontsize=30,
    )
    ax.axvline(0, color="black", linewidth=1.2, alpha=0.8, zorder=2)
    ax.axvspan(x_lo, -20, alpha=0.05, color="red", zorder=0)
    ax.axvspan(20, x_hi, alpha=0.05, color="red", zorder=0)

    ax.set_xlabel(
        "Annualized physical-effect rate (% per year, exp(log) − 1)", fontsize=33
    )
    ax.set_ylabel("NAICS-3 (top by impact)", fontsize=33)
    ax.grid(axis="x", alpha=0.3, zorder=0)
    ax.set_axisbelow(True)

    legend_handles = [
        Patch(
            facecolor=colors[i],
            edgecolor="black",
            linewidth=0.3,
            label=transitions[i].replace("->", "→"),
        )
        for i in range(n_trans)
    ]
    legend = ax.legend(
        handles=legend_handles,
        loc="center left",
        bbox_to_anchor=(1.01, 0.5),
        title="Transition (top→bottom)",
        fontsize=27,
        framealpha=0.9,
    )
    legend.get_title().set_fontsize(29)
    ax.set_title(
        f"Annualized LMDI physical effect — top {TOP_N_NAICS3} NAICS-3 by |Δ| impact\n"
        "Within each group, bars stack 2017→2024 top-to-bottom.\n"
        "All bars on one side of 0 → persistent drift (signal);\n"
        "flipping → oscillation (noise). Shaded zones = implausibly large.",
        fontsize=50,
    )
    fig.tight_layout()
    BARS_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(BARS_PATH, dpi=120, bbox_inches="tight")
    plt.close(fig)


def _print_summary(by_naics3: pd.DataFrame) -> None:
    naics_total = _collapse_dom_imp(by_naics3)
    print("\n=== NAICS-3 ranking by |impact|, top 10 ===")
    impact = (
        naics_total.assign(impact=lambda d: d["lmdi_phys_contrib"].abs())
        .groupby("naics3")["impact"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    print(impact.round(4).to_string())


def main() -> None:
    logger.info("Loading aggregated NAICS-3 results...")
    by_naics3 = pd.read_csv(BY_NAICS3_PATH, dtype={"naics3": str})

    logger.info("Plotting NAICS-3 stacked-bar plot...")
    _plot_naics3_bars(by_naics3)
    logger.info("  → %s", BARS_PATH)

    _print_summary(by_naics3)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
