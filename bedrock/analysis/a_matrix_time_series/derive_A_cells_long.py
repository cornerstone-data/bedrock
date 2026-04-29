"""Step 2 of epic #337: cell-by-cell time-series diagnostics for the A matrix.

Reads the parquet caches produced by ``derive_A_time_series.py`` (Step 1, #340)
and produces:

- ``A_cells_long.parquet`` — long-format
  ``(row_sector, col_sector, year, approach, dom_or_imp, A_value,
    delta_from_2017, delta_yoy)``. ~11M rows.
- ``step2_heatmap_{dom,imp}.png`` — per-year heatmap of ``log10|A_y − A_2017|``
  cell counts, faceted by approach. Surfaces where in cell-magnitude space the
  cumulative drift lives over time.
- ``step2_ridgeline_{dom,imp}.png`` — overlaid per-year step histogram of
  ``log10|A_y − A_2017|``, faceted by approach. Same data as the heatmap, more
  direct year-curve comparison.
- ``step2_yoy_norms_{dom,imp}.png`` — time series of column-L1 and column-L2
  YoY norms, one line per approach.
- ``step2_magnitude_quantiles`` and ``step2_yoy_norms`` tabs appended to the
  run-report Sheet (sheet ID read from ``last_run_sheet_id.txt`` written by
  Step 1; if missing, Sheet publish is skipped with a warning).

Usage:
    python -m bedrock.analysis.a_matrix_time_series.derive_A_cells_long
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.utils.io.gcp import update_sheet_tab

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_DIR = OUTPUT_DIR / "results"
PLOTS_DIR = OUTPUT_DIR / "plots"
LAST_RUN_SHEET_ID_PATH = RESULTS_DIR / "last_run_sheet_id.txt"
A_CELLS_LONG_PATH = RESULTS_DIR / "A_cells_long.parquet"

# Magnitude bins on log10 |A| scale. Cells with A == 0 are dropped from the
# magnitude analysis; their count is reported separately.
_LOG10_BIN_EDGES = np.arange(-12.0, 1.5, 0.5)


def _list_pairs() -> list[tuple[str, int]]:
    """Discover (approach, year) pairs from the parquet cache.

    Skips files that don't match ``A_{approach}_{4-digit-year}.parquet`` so
    other artifacts in the same dir (e.g. ``A_cells_long.parquet``) are
    ignored.
    """
    pairs: list[tuple[str, int]] = []
    for path in sorted(RESULTS_DIR.glob("A_*.parquet")):
        stem = path.stem  # e.g. A_useeio_2017
        body = stem[2:]  # strip leading "A_"
        approach, _, year_str = body.rpartition("_")
        if not (year_str.isdigit() and len(year_str) == 4):
            continue
        pairs.append((approach, int(year_str)))
    return pairs


def _load_pair(approach: str, year: int) -> dict[str, pd.DataFrame]:
    """Load Adom + Aimp from the combined parquet for one (approach, year)."""
    combined = pd.read_parquet(RESULTS_DIR / f"A_{approach}_{year}.parquet")
    return {
        "dom": pd.DataFrame(combined.loc["dom"]),
        "imp": pd.DataFrame(combined.loc["imp"]),
    }


def _melt(df: pd.DataFrame, approach: str, year: int, kind: str) -> pd.DataFrame:
    """Wide A matrix → long (row_sector, col_sector, A_value) + metadata.

    Direct numpy-based melt: faster than ``df.stack().reset_index(...)`` and
    avoids the duplicate-column collision when both axes share the source
    parquet's ``sector`` name.
    """
    rows = df.index.to_numpy()
    cols = df.columns.to_numpy()
    return pd.DataFrame(
        {
            "row_sector": np.repeat(rows, len(cols)),
            "col_sector": np.tile(cols, len(rows)),
            "A_value": df.to_numpy().ravel(),
            "approach": approach,
            "year": year,
            "dom_or_imp": kind,
        }
    )


def build_a_cells_long() -> pd.DataFrame:
    """Concat all (approach, year, dom_or_imp) cells, attach ``delta_from_2017``
    and ``delta_yoy`` per (approach, dom_or_imp, row_sector, col_sector)."""
    chunks: list[pd.DataFrame] = []
    for approach, year in _list_pairs():
        matrices = _load_pair(approach, year)
        for kind, mat in matrices.items():
            chunks.append(_melt(mat, approach, year, kind))

    long = pd.concat(chunks, ignore_index=True)

    cell_keys = ["approach", "dom_or_imp", "row_sector", "col_sector"]
    long = long.sort_values(cell_keys + ["year"]).reset_index(drop=True)

    base_2017 = (
        long[long["year"] == 2017]
        .set_index(cell_keys)["A_value"]
        .rename("A_value_2017")
    )
    long = long.join(base_2017, on=cell_keys)
    long["delta_from_2017"] = long["A_value"] - long["A_value_2017"]
    long.drop(columns="A_value_2017", inplace=True)

    long["delta_yoy"] = long.groupby(cell_keys)["A_value"].diff()

    return long[
        [
            "approach",
            "dom_or_imp",
            "year",
            "row_sector",
            "col_sector",
            "A_value",
            "delta_from_2017",
            "delta_yoy",
        ]
    ]


def compute_magnitude_quantiles(long: pd.DataFrame) -> pd.DataFrame:
    """Per (approach, year, dom_or_imp): nonzero-cell magnitude quantiles."""
    rows: list[dict[str, object]] = []
    for (approach, year, kind), group in long.groupby(
        ["approach", "year", "dom_or_imp"]
    ):
        nonzero = group["A_value"].abs()
        nonzero = nonzero[nonzero > 0]
        if nonzero.empty:
            continue
        rows.append(
            {
                "approach": approach,
                "year": int(year),
                "dom_or_imp": kind,
                "n_nonzero": int(nonzero.size),
                "p05": float(nonzero.quantile(0.05)),
                "p25": float(nonzero.quantile(0.25)),
                "p50": float(nonzero.quantile(0.50)),
                "p75": float(nonzero.quantile(0.75)),
                "p95": float(nonzero.quantile(0.95)),
                "max": float(nonzero.max()),
            }
        )
    return pd.DataFrame(rows)


def compute_yoy_norms(long: pd.DataFrame) -> pd.DataFrame:
    """Per (approach, year, dom_or_imp): summary of column-wise L1 & L2 norms
    of the YoY delta. Year-2017 rows have no YoY (returns NaN)."""
    rows: list[dict[str, object]] = []
    for (approach, year, kind), group in long.groupby(
        ["approach", "year", "dom_or_imp"]
    ):
        delta = group["delta_yoy"].dropna()
        if delta.empty:
            continue
        delta_by_col = group.dropna(subset=["delta_yoy"]).groupby("col_sector")[
            "delta_yoy"
        ]
        col_l1 = delta_by_col.apply(lambda s: float(s.abs().sum()))
        col_l2 = delta_by_col.apply(lambda s: float(np.sqrt((s**2).sum())))
        rows.append(
            {
                "approach": approach,
                "year": int(year),
                "dom_or_imp": kind,
                "max_col_L1": float(col_l1.max()),
                "mean_col_L1": float(col_l1.mean()),
                "max_col_L2": float(col_l2.max()),
                "mean_col_L2": float(col_l2.mean()),
                "total_L1": float(delta.abs().sum()),
            }
        )
    return pd.DataFrame(rows)


def _approach_axes(
    fig: plt.Figure, approaches: list[str], rows: int = 1
) -> dict[str, plt.Axes]:
    """Build a dict of approach-keyed axes laid out in `rows` rows."""
    n = len(approaches)
    cols = (n + rows - 1) // rows
    axes = fig.subplots(rows, cols, squeeze=False)
    flat = [axes[r][c] for r in range(rows) for c in range(cols)]
    for j in range(n, rows * cols):
        flat[j].axis("off")
    return dict(zip(approaches, flat[:n]))


def plot_delta_from_2017_heatmap(long: pd.DataFrame, kind: str, path: Path) -> None:
    """Per-approach heatmap of cumulative drift from 2017.

    Plots ``log10|A_y - A_2017|`` per year. Year 2017 itself is excluded
    (zero by definition). Cells with no drift (``delta_from_2017 == 0``) are
    excluded from the magnitude binning. Approaches with no drift in any year
    (e.g. ``useeio``) appear as empty subplots — that is the correct signal.
    """
    sub = long[
        (long["dom_or_imp"] == kind)
        & (long["year"] > 2017)
        & (long["delta_from_2017"].abs() > 0)
    ]
    approaches = sorted(long.loc[long["dom_or_imp"] == kind, "approach"].unique())
    fig = plt.figure(figsize=(4 * len(approaches), 4))
    fig.suptitle(f"|A_y − A_2017| distribution per year — {kind}")
    axes = _approach_axes(fig, approaches, rows=1)
    for approach in approaches:
        ax = axes[approach]
        approach_sub = sub[sub["approach"] == approach]
        years = sorted(approach_sub["year"].unique())
        H = np.zeros((len(years), len(_LOG10_BIN_EDGES) - 1))
        for i, year in enumerate(years):
            vals = approach_sub.loc[
                approach_sub["year"] == year, "delta_from_2017"
            ].abs()
            counts, _ = np.histogram(np.log10(vals), bins=_LOG10_BIN_EDGES)
            H[i] = counts
        ax.imshow(H, aspect="auto", cmap="viridis", origin="lower")
        ax.set_yticks(range(len(years)))
        ax.set_yticklabels(years)
        ax.set_xticks(range(0, len(_LOG10_BIN_EDGES) - 1, 4))
        ax.set_xticklabels(
            [
                f"{_LOG10_BIN_EDGES[k]:.0f}"
                for k in range(0, len(_LOG10_BIN_EDGES) - 1, 4)
            ]
        )
        ax.set_xlabel("log10 |A_y − A_2017|")
        ax.set_title(approach, fontsize=10)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_delta_from_2017_ridgeline(long: pd.DataFrame, kind: str, path: Path) -> None:
    """Per-approach ridgeline of cumulative drift from 2017.

    Same data as :func:`plot_delta_from_2017_heatmap` but as overlaid per-year
    step histograms; easier to compare year-curve shape directly.
    """
    sub = long[
        (long["dom_or_imp"] == kind)
        & (long["year"] > 2017)
        & (long["delta_from_2017"].abs() > 0)
    ]
    approaches = sorted(long.loc[long["dom_or_imp"] == kind, "approach"].unique())
    fig = plt.figure(figsize=(4 * len(approaches), 4))
    fig.suptitle(f"|A_y − A_2017| distribution by year — {kind}")
    axes = _approach_axes(fig, approaches, rows=1)
    for approach in approaches:
        ax = axes[approach]
        approach_sub = sub[sub["approach"] == approach]
        years = sorted(approach_sub["year"].unique())
        cmap = plt.get_cmap("viridis", max(len(years), 2))
        for i, year in enumerate(years):
            vals = np.log10(
                approach_sub.loc[approach_sub["year"] == year, "delta_from_2017"].abs()
            )
            ax.hist(
                vals,
                bins=list(_LOG10_BIN_EDGES),
                histtype="step",
                color=cmap(i),
                label=str(year),
                alpha=0.8,
            )
        ax.set_xlabel("log10 |A_y − A_2017|")
        ax.set_ylabel("cell count")
        ax.set_title(approach, fontsize=10)
        if years:
            ax.legend(fontsize=7, loc="upper left")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_yoy_norms(yoy_df: pd.DataFrame, kind: str, path: Path) -> None:
    """Two-panel: max_col_L1 and max_col_L2 over time, line per approach."""
    sub = yoy_df[yoy_df["dom_or_imp"] == kind].copy()
    approaches = sorted(sub["approach"].unique())
    fig, (ax_l1, ax_l2) = plt.subplots(1, 2, figsize=(10, 4))
    fig.suptitle(f"YoY column-norms over time — {kind}")
    cmap = plt.get_cmap("tab10", max(len(approaches), 3))
    for i, approach in enumerate(approaches):
        approach_sub = sub[sub["approach"] == approach].sort_values("year")
        ax_l1.plot(
            approach_sub["year"],
            approach_sub["max_col_L1"],
            marker="o",
            color=cmap(i),
            label=approach,
        )
        ax_l2.plot(
            approach_sub["year"],
            approach_sub["max_col_L2"],
            marker="o",
            color=cmap(i),
            label=approach,
        )
    ax_l1.set_xlabel("year")
    ax_l1.set_ylabel("max column L1(YoY)")
    ax_l1.legend(fontsize=8)
    ax_l1.grid(True, alpha=0.3)
    ax_l2.set_xlabel("year")
    ax_l2.set_ylabel("max column L2(YoY)")
    ax_l2.legend(fontsize=8)
    ax_l2.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def _publish_step2_tabs(magnitude_df: pd.DataFrame, yoy_df: pd.DataFrame) -> None:
    """Append step2 summary tabs to the run-report Sheet, if available."""
    if not LAST_RUN_SHEET_ID_PATH.exists():
        logger.warning(
            "No %s found — skipping Sheet publish. Run derive_A_time_series "
            "first (with valid Drive auth) to create the run report.",
            LAST_RUN_SHEET_ID_PATH,
        )
        return
    sheet_id = LAST_RUN_SHEET_ID_PATH.read_text().strip()
    try:
        update_sheet_tab(sheet_id, "step2_magnitude_quantiles", magnitude_df)
        update_sheet_tab(sheet_id, "step2_yoy_norms", yoy_df)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Sheet publish skipped (%s: %s). Local artifacts still complete.",
            type(e).__name__,
            e,
        )
        return
    logger.info("Updated tabs on sheet %s", sheet_id)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Building A_cells_long.parquet from %s", RESULTS_DIR)
    long = build_a_cells_long()
    long.to_parquet(A_CELLS_LONG_PATH)
    logger.info("Wrote %s (rows=%d)", A_CELLS_LONG_PATH, len(long))

    magnitude_df = compute_magnitude_quantiles(long)
    yoy_df = compute_yoy_norms(long)
    magnitude_df.to_csv(RESULTS_DIR / "step2_magnitude_quantiles.csv", index=False)
    yoy_df.to_csv(RESULTS_DIR / "step2_yoy_norms.csv", index=False)

    for kind in ("dom", "imp"):
        plot_delta_from_2017_heatmap(
            long, kind, PLOTS_DIR / f"step2_heatmap_{kind}.png"
        )
        plot_delta_from_2017_ridgeline(
            long, kind, PLOTS_DIR / f"step2_ridgeline_{kind}.png"
        )
        plot_yoy_norms(yoy_df, kind, PLOTS_DIR / f"step2_yoy_norms_{kind}.png")

    _publish_step2_tabs(magnitude_df, yoy_df)
    logger.info("Step 2 outputs written to %s and %s", RESULTS_DIR, PLOTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
