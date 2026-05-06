"""Compare year-over-year stability of N across 3 A-matrix methods.

Reads the per-pair tabs in ``ef_comparison.xlsx`` (produced by
``compile_ef_diagnostics.py`` after dispatching the time-series cells),
computes per-sector year-over-year (YoY) changes in ``N_new_ref`` (deflated
to 2023$), and rolls up to per-approach metrics.

The ``useeio`` approach is excluded — it pins A to the 2017 detail benchmark
with no temporal scaling, so its N time series reflects only B/x drift and
does not represent a comparable method here.

Per-approach metrics:
- ``mean_abs_yoy_pct`` — average |YoY %| across the 4 transitions
  (2019→20, 20→21, 21→22, 22→23). The headline stability metric.
- ``max_abs_yoy_pct`` — biggest single-year swing (tail).
- ``total_drift_pct`` — ``N_2023 / N_2019 - 1``. End-to-end shift.

Each metric is rolled up three ways: median (typical sector), p95 (tail),
and emissions-weighted (by ``mean_N``, the metric to optimize).

Outputs:
- ``output/results/n_yoy_ranking.csv``
- ``output/results/n_yoy_per_sector.csv``
- ``output/plots/n_indexed_lines.png``      — top-K sectors, N indexed to
                                                year-2019 = 100, faceted by method
- ``output/plots/n_yoy_distribution.png``   — boxplot of |YoY %|
                                                distributions per method

Usage:
    python -m bedrock.analysis.a_matrix_time_series.compare_method_stability
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.a_matrix_time_series.constants import (
    PLOTS_DIR,
    RESULTS_DIR,
)

logger = logging.getLogger(__name__)

EF_COMPARISON_XLSX_PATH = RESULTS_DIR / "ef_comparison.xlsx"
YOY_RANKING_PATH = RESULTS_DIR / "n_yoy_ranking.csv"
YOY_PER_SECTOR_PATH = RESULTS_DIR / "n_yoy_per_sector.csv"
INDEXED_LINES_PLOT_PATH = PLOTS_DIR / "n_indexed_lines.png"
YOY_DISTRIBUTION_PLOT_PATH = PLOTS_DIR / "n_yoy_distribution.png"

# `compile_ef_diagnostics.py` keys per-pair tabs as
# `{scenario}_{year}.0_{approach}__vs_{baseline}` truncated to 31 chars.
TAB_RE = re.compile(r"^(?P<scenario>[a-z0-9_]+?)_(?P<year>\d{4})(?:\.0)?_(?P<rest>.+)$")
APPROACH_PREFIXES: tuple[tuple[str, str], ...] = (
    ("commodity_pr", "commodity_price_index"),
    ("industry_pri", "industry_price_index"),
    ("summary_tabl", "summary_tables"),
    ("useeio", "useeio"),
)

EXCLUDED_APPROACHES: frozenset[str] = frozenset({"useeio"})

YEARS: tuple[int, ...] = (2019, 2020, 2021, 2022, 2023)
YOY_TRANSITIONS: tuple[tuple[int, int], ...] = tuple(
    (YEARS[i], YEARS[i + 1]) for i in range(len(YEARS) - 1)
)

# Drop sectors with mean N below this percentile from non-weighted metrics
# to avoid YoY % blow-up from numerical noise on near-zero means.
MIN_MEAN_PERCENTILE = 5

# Cumulative-share threshold for the indexed line plot: include the smallest
# set of sectors whose cumulative |mean_N| reaches this fraction of the total.
# These are the lines that actually move corporate-footprint outcomes.
LINE_PLOT_CUMULATIVE_SHARE = 0.30
# Hard ceiling so the plot stays readable even if the head is very long-tailed.
LINE_PLOT_MAX_SECTORS = 8


def _parse_tab(tab: str) -> tuple[str, int, str] | None:
    """Return ``(scenario, year, approach)`` for a time-series tab, else None."""
    m = TAB_RE.match(tab)
    if m is None:
        return None
    rest = m.group("rest")
    for prefix, approach in APPROACH_PREFIXES:
        if rest.startswith(prefix):
            return m.group("scenario"), int(m.group("year")), approach
    return None


def _read_panel(xlsx_path: Path) -> pd.DataFrame:
    """Long-format panel: ``(scenario, approach, year, sector, N_new_ref)``.

    Excludes the approaches in ``EXCLUDED_APPROACHES``.
    """
    if not xlsx_path.exists():
        raise FileNotFoundError(
            f"{xlsx_path} not found — run compile_ef_diagnostics.py first."
        )
    xls = pd.ExcelFile(xlsx_path)
    rows: list[pd.DataFrame] = []
    for tab in xls.sheet_names:
        tab_str = str(tab)
        parsed = _parse_tab(tab_str)
        if parsed is None:
            continue
        scenario, year, approach = parsed
        if approach in EXCLUDED_APPROACHES:
            continue
        df = pd.read_excel(xls, sheet_name=tab_str)
        if "N_new_ref" not in df.columns:
            logger.warning(
                "Tab %r missing N_new_ref — re-run compile after the deflation "
                "step was added.",
                tab_str,
            )
            continue
        sector_col = df.columns[0]
        chunk = pd.DataFrame(
            {
                "scenario": scenario,
                "approach": approach,
                "year": year,
                "sector": df[sector_col].astype(str),
                "N_new_ref": pd.to_numeric(df["N_new_ref"], errors="coerce"),
            }
        )
        rows.append(chunk)
    if not rows:
        raise RuntimeError(
            f"No time-series tabs found in {xlsx_path}; verify compile output."
        )
    return pd.concat(rows, ignore_index=True)


def _yoy_per_sector(panel: pd.DataFrame) -> pd.DataFrame:
    """Per ``(approach, sector)``: per-year N + YoY %s + aggregates."""
    wide = panel.pivot_table(
        index=["approach", "sector"],
        columns="year",
        values="N_new_ref",
        aggfunc="first",
    )
    # Make sure every year-column we expect exists (NaN if a cell missing).
    for y in YEARS:
        if y not in wide.columns:
            wide[y] = np.nan
    wide = wide[list(YEARS)]
    wide.columns = pd.Index([f"N_{y}" for y in YEARS])
    wide = wide.reset_index()

    # YoY % change for each transition.
    yoy_cols: list[str] = []
    for y0, y1 in YOY_TRANSITIONS:
        col = f"yoy_{y0}_{y1}"
        prev = wide[f"N_{y0}"]
        curr = wide[f"N_{y1}"]
        wide[col] = (curr - prev) / prev.abs()
        yoy_cols.append(col)

    abs_yoy = wide[yoy_cols].abs()
    wide["mean_abs_yoy_pct"] = abs_yoy.mean(axis=1)
    wide["max_abs_yoy_pct"] = abs_yoy.max(axis=1)
    wide["total_drift_pct"] = (wide[f"N_{YEARS[-1]}"] - wide[f"N_{YEARS[0]}"]) / wide[
        f"N_{YEARS[0]}"
    ].abs()
    wide["abs_total_drift_pct"] = wide["total_drift_pct"].abs()
    wide["mean_N"] = wide[[f"N_{y}" for y in YEARS]].mean(axis=1)
    return wide


def _aggregate(per_sector: pd.DataFrame) -> pd.DataFrame:
    """Per approach: median / p95 / emissions-weighted of each YoY metric."""
    metrics = ("mean_abs_yoy_pct", "max_abs_yoy_pct", "abs_total_drift_pct")
    rows: list[dict[str, object]] = []
    for approach in sorted(per_sector["approach"].unique()):
        grp = per_sector[per_sector["approach"] == approach]
        cutoff = grp["mean_N"].abs().quantile(MIN_MEAN_PERCENTILE / 100)
        big = grp[grp["mean_N"].abs() >= cutoff]
        weights = big["mean_N"].abs()
        row: dict[str, object] = {
            "approach": approach,
            "n_sectors": int(len(grp)),
        }
        for metric in metrics:
            values = big[metric]
            row[f"{metric}__median"] = float(values.median())
            row[f"{metric}__p95"] = float(values.quantile(0.95))
            row[f"{metric}__weighted"] = float(
                (values * weights).sum() / weights.sum()
                if weights.sum() > 0
                else np.nan
            )
        rows.append(row)
    cols = ["approach", "n_sectors"] + [
        f"{m}__{r}" for m in metrics for r in ("median", "p95", "weighted")
    ]
    ranking = pd.DataFrame(rows, columns=pd.Index(cols))
    return ranking.sort_values("mean_abs_yoy_pct__weighted").reset_index(drop=True)


def _select_head_sectors(
    per_sector: pd.DataFrame,
    cumulative_share: float = LINE_PLOT_CUMULATIVE_SHARE,
    max_sectors: int = LINE_PLOT_MAX_SECTORS,
) -> list[str]:
    """Return the smallest set of sectors covering ``cumulative_share`` of total |mean_N|.

    Across approaches, take the per-sector ``|mean_N|`` averaged over methods,
    sort descending, and accumulate until coverage hits the threshold (capped
    by ``max_sectors``). These are the lines that actually move
    corporate-footprint outcomes.
    """
    avg_abs_mean = (
        per_sector.groupby("sector")["mean_N"].mean().abs().sort_values(ascending=False)
    )
    total = float(avg_abs_mean.sum())
    if total <= 0:
        return list(avg_abs_mean.head(max_sectors).index)
    cum = avg_abs_mean.cumsum() / total
    head = avg_abs_mean[cum <= cumulative_share]
    # Always include the next sector that pushes us across the threshold.
    if len(head) < len(avg_abs_mean):
        head = avg_abs_mean.iloc[: len(head) + 1]
    return list(head.head(max_sectors).index)


def _indexed_lines_plot(
    panel: pd.DataFrame,
    per_sector: pd.DataFrame,
    out_path: Path,
) -> None:
    """Head-sector indexed (2019=100) line plot, faceted by method."""
    approaches = sorted(panel["approach"].unique())
    head_sectors = _select_head_sectors(per_sector)
    head_share = (
        per_sector[per_sector["sector"].isin(head_sectors)]
        .groupby("sector")["mean_N"]
        .mean()
        .abs()
        .sum()
        / per_sector.groupby("sector")["mean_N"].mean().abs().sum()
    )
    cmap = plt.get_cmap("tab10")
    color_by_sector = {s: cmap(i % 10) for i, s in enumerate(head_sectors)}

    n = len(approaches)
    fig, axes = plt.subplots(1, n, figsize=(5 * n, 5.5), sharey=True)
    if n == 1:
        axes = np.array([axes])
    for ax, approach in zip(axes, approaches):
        sub = panel[
            (panel["approach"] == approach) & panel["sector"].isin(head_sectors)
        ]
        for sector in head_sectors:
            sg = sub[sub["sector"] == sector].sort_values("year")
            if sg.empty or sg["year"].min() != YEARS[0]:
                continue
            base = sg.loc[sg["year"] == YEARS[0], "N_new_ref"].iloc[0]
            if base == 0 or pd.isna(base):
                continue
            indexed = sg["N_new_ref"] / base * 100
            ax.plot(
                sg["year"],
                indexed,
                marker="o",
                label=sector,
                color=color_by_sector[sector],
            )
        ax.axhline(100, color="black", linestyle=":", linewidth=1, alpha=0.6)
        ax.set_title(approach, fontsize=14)
        ax.set_xlabel("Year")
        ax.set_xticks(list(YEARS))
        ax.set_xticklabels([str(y) for y in YEARS])
        ax.grid(alpha=0.3)
    axes[0].set_ylabel("N indexed (2019 = 100)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="center right",
        fontsize=8,
        bbox_to_anchor=(1.05, 0.5),
        title="sector",
    )
    fig.suptitle(
        f"Head sectors covering {head_share:.0%} of |mean_N| "
        f"(n={len(head_sectors)}), N rebased to 2019=100, by A-matrix method",
        fontsize=14,
    )
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


def _yoy_distribution_plot(per_sector: pd.DataFrame, out_path: Path) -> None:
    """Boxplot of per-sector |YoY %| distributions, one box per (method, transition)."""
    approaches = sorted(per_sector["approach"].unique())
    cutoff = per_sector["mean_N"].abs().quantile(MIN_MEAN_PERCENTILE / 100)
    big = per_sector[per_sector["mean_N"].abs() >= cutoff]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))
    # Left: boxplot of mean_abs_yoy_pct per method.
    ax = axes[0]
    data = [
        big.loc[big["approach"] == a, "mean_abs_yoy_pct"].dropna() for a in approaches
    ]
    ax.boxplot(data, tick_labels=approaches, showfliers=False)
    ax.set_title("Per-sector mean |YoY %|", fontsize=13)
    ax.set_ylabel("|YoY %| (lower = more stable)")
    ax.grid(axis="y", alpha=0.3)

    # Right: per-transition |YoY %|, grouped boxes per method.
    ax = axes[1]
    transition_labels = [f"{y0}→{y1}" for y0, y1 in YOY_TRANSITIONS]
    n_methods = len(approaches)
    width = 0.8 / n_methods
    for i, approach in enumerate(approaches):
        sub = big[big["approach"] == approach]
        per_transition = [
            sub[f"yoy_{y0}_{y1}"].abs().dropna() for y0, y1 in YOY_TRANSITIONS
        ]
        positions = [
            j + (i - (n_methods - 1) / 2) * width for j in range(len(YOY_TRANSITIONS))
        ]
        bp = ax.boxplot(
            per_transition,
            positions=positions,
            widths=width * 0.9,
            patch_artist=True,
            showfliers=False,
        )
        color = plt.get_cmap("tab10")(i)
        for patch in bp["boxes"]:
            patch.set_facecolor(color)
            patch.set_alpha(0.6)
        ax.plot([], [], color=color, label=approach, linewidth=8, alpha=0.6)
    ax.set_xticks(range(len(YOY_TRANSITIONS)))
    ax.set_xticklabels(transition_labels)
    ax.set_title("|YoY %| per transition, by method", fontsize=13)
    ax.set_ylabel("|YoY %|")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(axis="y", alpha=0.3)

    fig.suptitle("Year-over-year fluctuation in N across A-matrix methods", fontsize=15)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


def _print_summary(ranking: pd.DataFrame, per_sector: pd.DataFrame) -> None:
    print("\n=== YoY stability ranking (lower = more stable) ===")
    cols_show = [
        "approach",
        "mean_abs_yoy_pct__weighted",
        "mean_abs_yoy_pct__median",
        "max_abs_yoy_pct__weighted",
        "abs_total_drift_pct__weighted",
    ]
    print(ranking[cols_show].round(4).to_string(index=False))

    print("\n=== Top-5 most-fluctuating big-N sectors per method ===")
    cutoff = per_sector["mean_N"].abs().quantile(MIN_MEAN_PERCENTILE / 100)
    big = per_sector[per_sector["mean_N"].abs() >= cutoff]
    for approach in sorted(big["approach"].unique()):
        grp = big[big["approach"] == approach]
        worst = grp.nlargest(n=5, columns="mean_abs_yoy_pct")[
            [
                "sector",
                "mean_N",
                "mean_abs_yoy_pct",
                "max_abs_yoy_pct",
                "total_drift_pct",
            ]
        ]
        print(f"\n[{approach}]")
        print(worst.round(4).to_string(index=False))


def main() -> None:
    panel = _read_panel(EF_COMPARISON_XLSX_PATH)
    per_sector = _yoy_per_sector(panel)
    ranking = _aggregate(per_sector)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    ranking.to_csv(YOY_RANKING_PATH, index=False)
    per_sector.to_csv(YOY_PER_SECTOR_PATH, index=False)
    _indexed_lines_plot(panel, per_sector, INDEXED_LINES_PLOT_PATH)
    _yoy_distribution_plot(per_sector, YOY_DISTRIBUTION_PLOT_PATH)

    _print_summary(ranking, per_sector)
    print(f"\nWrote: {YOY_RANKING_PATH}")
    print(f"Wrote: {YOY_PER_SECTOR_PATH}")
    print(f"Wrote: {INDEXED_LINES_PLOT_PATH}")
    print(f"Wrote: {YOY_DISTRIBUTION_PLOT_PATH}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
