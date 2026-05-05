"""Phase 3 of Step 6: scatter + histogram plots comparing approach EFs vs each baseline.

Reads ``ef_scatter_coords.parquet`` (Phase 2 output) and produces, for
each ``(baseline, ef_kind)`` pair, two figures:

- ``ef_scatter_{baseline}_{ef_kind}.png`` — 2×2 grid of approach panels.
  Per panel: linear-scale scatter of ``(x_baseline, y_approach)`` per
  Cornerstone sector, ``y=x`` reference dashed in black. Top-left
  annotation reports ``R²`` of ``y ~ x`` against the ``y=x`` line, ``p95``
  of ``|y - x| / |x|``, and the count of sectors with that quotient
  exceeding ``SIGNIFICANT_PCT_THRESHOLD``.
- ``ef_pct_hist_{baseline}_{ef_kind}.png`` — 2×2 grid of approach panels.
  Per panel: per-sector ``% diff = (y - x) / x × 100`` clipped to ±100%,
  60-bin histogram. Vertical zero line. Mirrors the
  ``baseline_snapshot_comparison`` ``compare_B_Adom`` convention.

vs CEDA (v0) panels: 4 candidates (``useeio``, ``summary_tables``,
``industry_price_index``, ``commodity_price_index``).
vs USEEIO panels: 3 candidates (``useeio`` is the comparator and is
hidden).

Usage:
    python -m bedrock.analysis.a_matrix_time_series.plot_ef_diagnostics
"""

from __future__ import annotations

import logging
import typing as ta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from bedrock.analysis.a_matrix_time_series.compile_ef_diagnostics import (
    EF_SCATTER_COORDS_PATH,
    SIGNIFICANT_PCT_THRESHOLD,
)
from bedrock.analysis.a_matrix_time_series.constants import PLOTS_DIR

logger = logging.getLogger(__name__)

PanelFn = ta.Callable[[Axes, pd.DataFrame, str], None]

# Approach order (top-left → bottom-right in the 2×2 grid). Mirrors
# `summary_a_errors.py` and `key_sector_deep_dive.py`; consolidate per the
# redundancy cleanup plan.
APPROACH_ORDER: tuple[str, ...] = (
    "useeio",
    "summary_tables",
    "industry_price_index",
    "commodity_price_index",
)
APPROACH_COLORS: dict[str, str] = {
    "useeio": "#7f7f7f",
    "ceda_default": "#bcbd22",
    "summary_tables": "#1f77b4",
    "industry_price_index": "#ff7f0e",
    "commodity_price_index": "#2ca02c",
}
BASELINE_LABEL: dict[str, str] = {"ceda": "CEDA-US (v0)", "useeio": "USEEIO"}
EF_KIND_LABEL: dict[str, str] = {"N": "total EF (N)", "D": "direct EF (D)"}

TITLE_FONTSIZE = 20
HIST_PCT_CLIP = 100.0
HIST_BINS = 60


def _panel_stats(panel_df: pd.DataFrame) -> dict[str, float]:
    """``R²`` against ``y=x``, ``p95`` of ``|y-x|/|x|``, and ``n_significant``.

    ``R²`` is the coefficient of determination of ``y`` against the ``y=x``
    reference (not an OLS fit), so values can go negative when ``y=x`` is
    worse than predicting the mean of ``y``.
    """
    df = panel_df[panel_df["x_baseline"].abs() > 0]
    if len(df) < 2:
        return {"r2": float("nan"), "p95": float("nan"), "n_sig": 0.0}
    x = df["x_baseline"].to_numpy(dtype=float)
    y = df["y_approach"].to_numpy(dtype=float)
    ss_tot = float(((y - y.mean()) ** 2).sum())
    ss_res = float(((y - x) ** 2).sum())
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else float("nan")
    perc = (df["y_approach"] - df["x_baseline"]).abs() / df["x_baseline"].abs()
    return {
        "r2": float(r2),
        "p95": float(perc.quantile(0.95)),
        "n_sig": float((perc > SIGNIFICANT_PCT_THRESHOLD).sum()),
    }


def _scatter_panel(ax: Axes, panel_df: pd.DataFrame, approach: str) -> None:
    color = APPROACH_COLORS.get(approach, "#000000")
    sub = panel_df.dropna(subset=["x_baseline", "y_approach"])
    ax.scatter(
        sub["x_baseline"],
        sub["y_approach"],
        marker="o",
        color=color,
        s=18,
        alpha=0.55,
        linewidths=0,
        label=f"n={len(sub)}",
    )
    if not sub.empty:
        lo = float(min(sub["x_baseline"].min(), sub["y_approach"].min()))
        hi = float(max(sub["x_baseline"].max(), sub["y_approach"].max()))
        ax.plot(
            [lo, hi], [lo, hi], color="black", linestyle="--", linewidth=0.7, alpha=0.5
        )

    ax.grid(True, which="both", alpha=0.2)

    stats = _panel_stats(panel_df)
    ax.text(
        0.04,
        0.96,
        f"R²={stats['r2']:.3f}\np95={stats['p95']:.3f}\nn_sig={int(stats['n_sig'])}",
        transform=ax.transAxes,
        fontsize=10,
        va="top",
        ha="left",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", alpha=0.85, edgecolor="0.7"
        ),
    )
    ax.set_title(approach, fontsize=TITLE_FONTSIZE, color="black")
    ax.legend(loc="lower right", fontsize=9, frameon=False)


def _hist_panel(ax: Axes, panel_df: pd.DataFrame, approach: str) -> None:
    color = APPROACH_COLORS.get(approach, "tab:blue")
    df = panel_df[panel_df["x_baseline"].abs() > 0].dropna(
        subset=["x_baseline", "y_approach"]
    )
    if df.empty:
        ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center", va="center")
        ax.set_title(approach, fontsize=TITLE_FONTSIZE, color="black")
        return
    pct = ((df["y_approach"] - df["x_baseline"]) / df["x_baseline"].abs()).to_numpy(
        dtype=float
    ) * 100.0
    finite = pct[np.isfinite(pct)]
    clipped = np.clip(finite, -HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.hist(clipped, bins=HIST_BINS, color=color, alpha=0.85)
    ax.axvline(0, color="k", lw=0.5)
    ax.set_xlim(-HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.grid(True, ls=":", alpha=0.3)
    ax.text(
        0.04,
        0.96,
        f"n={len(finite)}\nmedian={np.median(finite):.1f}%\np95(|·|)={np.quantile(np.abs(finite), 0.95):.1f}%",
        transform=ax.transAxes,
        fontsize=10,
        va="top",
        ha="left",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", alpha=0.85, edgecolor="0.7"
        ),
    )
    ax.set_title(approach, fontsize=TITLE_FONTSIZE, color="black")


def _grid_2x2(
    coords: pd.DataFrame,
    baseline: str,
    ef_kind: str,
    panel_fn: PanelFn,
    xlabel: str,
    ylabel: str,
    suptitle: str,
) -> Figure | None:
    sub = coords[(coords["baseline"] == baseline) & (coords["ef_kind"] == ef_kind)]
    approaches = [a for a in APPROACH_ORDER if a in sub["approach"].unique()]
    if not approaches:
        logger.warning("No approaches for baseline=%s ef_kind=%s", baseline, ef_kind)
        return None

    fig, axes_grid = plt.subplots(2, 2, figsize=(11.0, 10.5))
    flat_axes = list(axes_grid.flat)
    for ax, approach in zip(flat_axes, approaches):
        panel_fn(ax, sub[sub["approach"] == approach], approach)
    for ax in flat_axes[len(approaches) :]:
        ax.axis("off")

    for ax in axes_grid[:, 0]:
        ax.set_ylabel(ylabel, fontsize=11)
    for ax in axes_grid[-1, :]:
        ax.set_xlabel(xlabel, fontsize=11)

    fig.suptitle(suptitle, fontsize=14, y=1.0)
    fig.tight_layout()
    return fig


def main() -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    if not EF_SCATTER_COORDS_PATH.exists():
        raise FileNotFoundError(
            f"{EF_SCATTER_COORDS_PATH} not found — run "
            "`compile_ef_diagnostics` first."
        )
    coords = pd.read_parquet(EF_SCATTER_COORDS_PATH)

    for baseline in ("ceda", "useeio"):
        for ef_kind in ("N", "D"):
            kind_label = EF_KIND_LABEL[ef_kind]
            base_label = BASELINE_LABEL[baseline]

            scatter_fig = _grid_2x2(
                coords,
                baseline,
                ef_kind,
                _scatter_panel,
                xlabel=f"{base_label} {kind_label} (inflation-adjusted)",
                ylabel=f"approach {kind_label}",
                suptitle=f"{kind_label} per sector — approach vs {base_label}",
            )
            if scatter_fig is not None:
                out = PLOTS_DIR / f"ef_scatter_{baseline}_{ef_kind}.png"
                scatter_fig.savefig(out, dpi=150, bbox_inches="tight")
                plt.close(scatter_fig)
                logger.info("Wrote %s", out)

            hist_fig = _grid_2x2(
                coords,
                baseline,
                ef_kind,
                _hist_panel,
                xlabel=(
                    f"% diff = (approach − {base_label}) / {base_label} × 100 "
                    f"(clipped ±{int(HIST_PCT_CLIP)}%)"
                ),
                ylabel="sector count",
                suptitle=f"{kind_label} per-sector % diff distribution — vs {base_label}",
            )
            if hist_fig is not None:
                out = PLOTS_DIR / f"ef_pct_hist_{baseline}_{ef_kind}.png"
                hist_fig.savefig(out, dpi=150, bbox_inches="tight")
                plt.close(hist_fig)
                logger.info("Wrote %s", out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
