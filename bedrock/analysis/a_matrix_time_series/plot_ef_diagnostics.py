"""Phase 3 of Step 6: scatter + histogram plots comparing approach EFs vs each baseline.

Reads ``ef_scatter_coords.parquet`` (Phase 2 output) and produces, for
each ``(scenario, baseline, ef_kind)`` triple, two figures:

- ``ef_scatter_{scenario}_{baseline}_{ef_kind}.png`` — 2×2 grid of
  approach panels. Per panel: linear-scale scatter of ``(x_baseline,
  y_approach)`` per Cornerstone sector, ``y=x`` reference dashed in
  black. Top-left annotation reports ``R²`` of ``y ~ x`` against the
  ``y=x`` line, ``p95`` of ``|y - x| / |x|``, and the count of sectors
  with that quotient exceeding ``SIGNIFICANT_PCT_THRESHOLD``.
- ``ef_pct_hist_{scenario}_{baseline}_{ef_kind}.png`` — 2×2 grid of
  approach panels. Per panel: per-sector
  ``% diff = (y - x) / x × 100`` clipped to ±100%, 60-bin histogram.
  Vertical zero line. Mirrors the ``baseline_snapshot_comparison``
  ``compare_B_Adom`` convention.

The ``scenario`` axis separates runs that **isolate the A-matrix
derivation** (``isolate_a_matrix``: only one A-matrix flag flipped
versus the Cornerstone 2026 schema) from runs that **bundle the
A-matrix change with the full bedrock v0.2 stack**
(``bundle_v0_2``). Pooling them would mix two different counterfactuals
against the same CEDA-US (v0) baseline.

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
from matplotlib.ticker import PercentFormatter

from bedrock.analysis.a_matrix_time_series.compile_ef_diagnostics import (
    EF_SCATTER_COORDS_PATH,
    SIGNIFICANT_PCT_THRESHOLD,
)
from bedrock.analysis.a_matrix_time_series.constants import (
    APPROACH_COLORS,
    PLOTS_DIR,
)

logger = logging.getLogger(__name__)

# Last positional arg is a font-scale multiplier so the same panel
# functions can render at default size (scatter) and enlarged size
# (histograms).
PanelFn = ta.Callable[[Axes, pd.DataFrame, str, float], None]

# EF panels show only the v0.2 focus approaches — summary_tables and
# commodity_price_index are the two top internal candidates, useeio_nowcast
# is the external reference. industry_price_index is dropped (superseded by
# commodity_price_index) and useeio (BEA-2017 do-nothing) is omitted because
# it doesn't vary across years and confuses the time-series story.
# ceda_default is the baseline on the x-axis, not a panel.
APPROACH_ORDER: tuple[str, ...] = (
    "summary_tables",
    "commodity_price_index",
    "useeio_nowcast",
)
BASELINE_LABEL: dict[str, str] = {"ceda": "CEDA-US (v0)", "useeio": "USEEIO"}
EF_KIND_LABEL: dict[str, str] = {"N": "total EF (N)", "D": "direct EF (D)"}
SCENARIO_LABEL: dict[str, str] = {
    "isolate_a_matrix": "isolate A-matrix method",
    "bundle_v0_2": "A-matrix method bundled with bedrock v0.2",
}

# Base font sizes (multiplied by ``font_scale`` per panel).
TITLE_FONTSIZE = 20
AXIS_LABEL_FONTSIZE = 11
SUPTITLE_FONTSIZE = 14
STATS_FONTSIZE = 10
LEGEND_FONTSIZE = 9
TICK_LABEL_FONTSIZE = 10

# Histograms enlarge text 2× for readability in side-by-side reports.
HIST_FONT_SCALE = 2.0
# Stats annotation gets an extra 2× on top of HIST_FONT_SCALE so the per-
# panel n / median / p95 box is easy to read at a glance.
HIST_STATS_EXTRA_SCALE = 2.0

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


def _scatter_panel(
    ax: Axes, panel_df: pd.DataFrame, approach: str, font_scale: float
) -> None:
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
        fontsize=STATS_FONTSIZE * font_scale,
        va="top",
        ha="left",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", alpha=0.4, edgecolor="0.7"
        ),
    )
    ax.set_title(approach, fontsize=TITLE_FONTSIZE * font_scale, color="black")
    ax.legend(loc="lower right", fontsize=LEGEND_FONTSIZE * font_scale, framealpha=0.4)


def _hist_panel(
    ax: Axes, panel_df: pd.DataFrame, approach: str, font_scale: float
) -> None:
    color = APPROACH_COLORS.get(approach, "tab:blue")
    df = panel_df[panel_df["x_baseline"].abs() > 0].dropna(
        subset=["x_baseline", "y_approach"]
    )
    if df.empty:
        ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center", va="center")
        ax.set_title(approach, fontsize=TITLE_FONTSIZE * font_scale, color="black")
        return
    pct = ((df["y_approach"] - df["x_baseline"]) / df["x_baseline"].abs()).to_numpy(
        dtype=float
    ) * 100.0
    finite = pct[np.isfinite(pct)]
    clipped = np.clip(finite, -HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.hist(clipped, bins=HIST_BINS, color=color, alpha=0.85)
    ax.axvline(0, color="k", lw=0.5)
    ax.set_xlim(-HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.xaxis.set_major_formatter(PercentFormatter(decimals=0))
    ax.grid(True, ls=":", alpha=0.3)
    ax.text(
        0.04,
        0.96,
        f"n={len(finite)}\nmedian={np.median(finite):.1f}%\np95(|·|)={np.quantile(np.abs(finite), 0.95):.1f}%",
        transform=ax.transAxes,
        fontsize=STATS_FONTSIZE * font_scale * HIST_STATS_EXTRA_SCALE,
        va="top",
        ha="left",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", alpha=0.4, edgecolor="0.7"
        ),
    )
    ax.set_title(approach, fontsize=TITLE_FONTSIZE * font_scale, color="black")


def _latest_year_in(sub: pd.DataFrame) -> str:
    """Return the latest year string in ``sub`` or '' if none populated.

    Year stored as the original ``year`` string from the run index (e.g.
    ``"2023.0"`` for time-series cells, ``""`` for legacy single-year
    cells). When a scenario has multiple years pooling them is rarely
    what we want — picking the latest gives a single-year snapshot per
    panel, matching the convention the histograms were designed for.
    """
    years = [y for y in sub["year"].dropna().astype(str).unique() if y]
    if not years:
        return ""
    return max(years, key=lambda y: float(y))


def _grid_2x2(
    coords: pd.DataFrame,
    scenario: str,
    baseline: str,
    ef_kind: str,
    panel_fn: PanelFn,
    xlabel: str,
    ylabel: str,
    suptitle: str,
    font_scale: float = 1.0,
) -> Figure | None:
    sub = coords[
        (coords["scenario"] == scenario)
        & (coords["baseline"] == baseline)
        & (coords["ef_kind"] == ef_kind)
    ]
    approaches = [a for a in APPROACH_ORDER if a in sub["approach"].unique()]
    if not approaches:
        logger.warning(
            "No approaches for scenario=%s baseline=%s ef_kind=%s",
            scenario,
            baseline,
            ef_kind,
        )
        return None
    # When a scenario carries multiple years, pool would mix dollar-years
    # and inflate `n` per panel — restrict to the latest year so each
    # panel is a single-year snapshot. Legacy single-year runs predate the
    # `year` column and store it as empty string; treat those as
    # participating in the latest year (otherwise approaches without a
    # tagged year would render "no data" while sibling approaches' year-
    # tagged rows take over the panel).
    latest_year = _latest_year_in(sub)
    if latest_year:
        year_str = sub["year"].fillna("").astype(str)
        sub = sub[(year_str == latest_year) | (year_str == "")]
        suptitle = f"{suptitle} — year {int(float(latest_year))}"

    # Layout: 1×N for N ≤ 3 (typical focus comparison), else 2×ceil(N/2)
    # so the canvas stays roughly square as more approaches are added.
    n = len(approaches)
    if n <= 3:
        nrows, ncols = 1, n
        base_w, base_h = 5.5 * ncols, 6.0
    else:
        nrows = 2
        ncols = (n + 1) // 2
        base_w, base_h = 5.5 * ncols, 5.5 * nrows

    fig, axes_grid = plt.subplots(
        nrows, ncols, figsize=(base_w * font_scale, base_h * font_scale), squeeze=False
    )
    flat_axes = list(axes_grid.flat)
    for ax, approach in zip(flat_axes, approaches):
        panel_fn(ax, sub[sub["approach"] == approach], approach, font_scale)
        ax.tick_params(axis="both", labelsize=TICK_LABEL_FONTSIZE * font_scale)
    for ax in flat_axes[len(approaches) :]:
        ax.axis("off")

    for ax in axes_grid[:, 0]:
        ax.set_ylabel(ylabel, fontsize=AXIS_LABEL_FONTSIZE * font_scale)
    for ax in axes_grid[-1, :]:
        ax.set_xlabel(xlabel, fontsize=AXIS_LABEL_FONTSIZE * font_scale)

    fig.suptitle(suptitle, fontsize=SUPTITLE_FONTSIZE * font_scale, y=1.0)
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
    if "scenario" not in coords.columns:
        raise ValueError(
            f"{EF_SCATTER_COORDS_PATH} is missing the `scenario` column — "
            "rebuild with the current `compile_ef_diagnostics`."
        )

    scenarios = [s for s in coords["scenario"].dropna().unique() if s]
    for scenario in scenarios:
        scen_label = SCENARIO_LABEL.get(scenario, scenario)
        for baseline in ("ceda", "useeio"):
            for ef_kind in ("N", "D"):
                kind_label = EF_KIND_LABEL[ef_kind]
                base_label = BASELINE_LABEL[baseline]

                scatter_fig = _grid_2x2(
                    coords,
                    scenario,
                    baseline,
                    ef_kind,
                    _scatter_panel,
                    xlabel=f"{base_label} {kind_label} (inflation-adjusted)",
                    ylabel=f"approach {kind_label}",
                    suptitle=(
                        f"{kind_label} per sector — approach vs {base_label} "
                        f"[{scen_label}]"
                    ),
                )
                if scatter_fig is not None:
                    out = PLOTS_DIR / f"ef_scatter_{scenario}_{baseline}_{ef_kind}.png"
                    scatter_fig.savefig(out, dpi=150, bbox_inches="tight")
                    plt.close(scatter_fig)
                    logger.info("Wrote %s", out)

                hist_fig = _grid_2x2(
                    coords,
                    scenario,
                    baseline,
                    ef_kind,
                    _hist_panel,
                    xlabel="Percentage Diff (%)",
                    ylabel="sector count",
                    suptitle=(
                        f"{kind_label} per-sector % diff distribution — "
                        f"vs {base_label} [{scen_label}]"
                    ),
                    font_scale=HIST_FONT_SCALE,
                )
                if hist_fig is not None:
                    out = PLOTS_DIR / f"ef_pct_hist_{scenario}_{baseline}_{ef_kind}.png"
                    hist_fig.savefig(out, dpi=150, bbox_inches="tight")
                    plt.close(hist_fig)
                    logger.info("Wrote %s", out)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
