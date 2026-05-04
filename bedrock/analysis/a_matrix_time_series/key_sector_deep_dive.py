"""Step 4 of epic #337: data-driven sector deep dive, impact-weighted, two-baseline.

Reframed from a curated-shortlist deep dive into a data-driven discovery: rank
all A-matrix cells by `max_alt |A_alt − A_baseline| × |A_baseline|` for each
of the two baselines (USEEIO, CEDA-US), then surface (a) what BEA summary
parent groups dominate the ranking and (b) which specific cells matter most.

The two-baseline rule from the analysis plan applies: every divergence
quantity is reported against both USEEIO and CEDA-US.

Reads ``A_cells_long.parquet`` (Step 2 output). Produces:

- ``keysector_impact_heatmap.png`` — single figure, two side-by-side panels
  (vs USEEIO | vs CEDA-US). Rows = BEA summary parent groups of
  ``col_sector`` (the consuming industry), filtered to top
  ``HEATMAP_TOP_GROUPS`` by total impact across both baselines. Cols = the
  three alternative approaches (USEEIO and CEDA-US are baselines, not
  candidates). Cell value = sum of ``impact_vs_baseline`` across every
  ``dom`` A cell whose ``col_sector`` rolls up to that summary group.
  Color: viridis, shared scale across panels for direct cross-baseline
  reading. Annotated with the numeric value.

- ``keysector_top_cells_grid.png`` — drill-in grid. Top
  ``DRILL_IN_TOP_N`` cells by ``max(impact_vs_useeio, impact_vs_ceda)``,
  laid out 3 columns × N/3 rows. Each panel = one cell, all five
  approaches overlaid. Title shows ``{row_sector} → {col_sector}`` plus
  the ``col_sector``'s BEA summary parent. Per-panel annotation reports
  both impact scores so the reader sees which baseline drove the cell
  into the top-N.

- ``keysector_top_cells_ranked.csv`` — full top-``CSV_TOP_N`` ranked cells
  with both impact scores, the BEA summary group of ``col_sector`` and
  ``row_sector``, and the per-approach values at ``RANK_TARGET_YEAR``.

- ``keysector_curated_shortlist.csv`` — appendix retained from the
  curated theory-driven shortlist. The headline figures are now
  data-driven; the curated list is preserved here for the "did the priors
  hold?" methodology check in the eventual write-up.

- Sheet tabs ``keysector_top_cells_ranked``, ``keysector_curated_shortlist``
  appended to the run-report Sheet.

Tunable constants: ``RANK_TARGET_YEAR``, ``ALTERNATIVE_APPROACHES``,
``BASELINES``, ``HEATMAP_TOP_GROUPS``, ``DRILL_IN_TOP_N``, ``CSV_TOP_N``.

Usage:
    python -m bedrock.analysis.a_matrix_time_series.key_sector_deep_dive
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.utils.io.gcp import update_sheet_tab
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    USA_2017_SUMMARY_COMMODITY_DESC,
)
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    load_bea_v2017_summary_to_cornerstone,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_DIR = OUTPUT_DIR / "results"
PLOTS_DIR = OUTPUT_DIR / "plots"
LAST_RUN_SHEET_ID_PATH = RESULTS_DIR / "last_run_sheet_id.txt"
A_CELLS_LONG_PATH = RESULTS_DIR / "A_cells_long.parquet"

RANK_TARGET_YEAR = 2024
ALTERNATIVE_APPROACHES: tuple[str, ...] = (
    "summary_tables",
    "industry_price_index",
    "commodity_price_index",
)
BASELINES: tuple[tuple[str, str], ...] = (
    ("useeio", "USEEIO"),
    ("ceda_default", "CEDA-US"),
)
ALL_APPROACHES_PLOT_ORDER: tuple[str, ...] = (
    "useeio",
    "ceda_default",
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

HEATMAP_TOP_GROUPS = 15
DRILL_IN_TOP_N = 12
CSV_TOP_N = 30
DRILL_IN_NCOLS = 3

# Curated theory-driven shortlist — kept as appendix CSV. The headline
# figures are now data-driven. Categories from the original Step 4 plan.
CURATED_SHORTLIST: dict[str, list[tuple[str, str]]] = {
    "energy": [
        ("221100", "Electric power generation/transmission/distribution"),
        ("221200", "Natural gas distribution"),
        ("211000", "Oil and gas extraction"),
        ("324110", "Petroleum refineries"),
    ],
    "travel": [
        ("481000", "Air transportation"),
        ("721000", "Accommodation"),
        ("722110", "Full-service restaurants"),
        ("722211", "Limited-service restaurants"),
        ("711100", "Performing arts companies"),
    ],
    "waste": [
        ("562111", "Solid waste collection"),
        ("562212", "Solid waste landfills"),
        ("562213", "Solid waste combustors"),
        ("562910", "Remediation services"),
        ("562HAZ", "Hazardous waste (Cornerstone disagg)"),
        ("562OTH", "Other waste (Cornerstone disagg)"),
    ],
    "volatility": [
        ("331110", "Iron and steel mills"),
        ("331313", "Alumina refining"),
        ("331200", "Steel product manufacturing"),
        ("311224", "Oilseed processing"),
        ("336111", "Automobile manufacturing"),
    ],
    "disagg": [
        ("33131B", "Other nonferrous (BEA aggregate)"),
        ("31151A", "Dairy products (BEA aggregate)"),
        ("31161A", "Animal slaughtering (BEA aggregate)"),
        ("3118A0", "Other food (BEA aggregate)"),
        ("722A00", "All other food services (BEA aggregate)"),
        ("711A00", "Independent artists / spectator sports (BEA aggregate)"),
    ],
}


def _cornerstone_to_summary() -> dict[str, str]:
    """Invert ``load_bea_v2017_summary_to_cornerstone`` into a
    cornerstone_code → bea_summary_code lookup."""
    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    return {
        code: str(summary)
        for summary, codes in summary_to_cornerstone.items()
        for code in codes
    }


# Widen the upstream Literal-keyed dicts to plain str → str so descriptive
# lookups work on arbitrary cell codes coming out of the parquet without
# tripping mypy's call-overload check on the Literal `.get()` signature.
_SUMMARY_DESC: dict[str, str] = {
    str(k): str(v) for k, v in USA_2017_SUMMARY_COMMODITY_DESC.items()
}
_COMMODITY_DESC: dict[str, str] = {str(k): str(v) for k, v in COMMODITY_DESC.items()}


def _summary_desc(code: str, max_len: int = 32) -> str:
    """Short BEA summary code description, truncated to ``max_len``."""
    desc = _SUMMARY_DESC.get(code, "")
    if not desc:
        return ""
    return desc if len(desc) <= max_len else desc[: max_len - 1] + "…"


def _commodity_desc(code: str, max_len: int = 28) -> str:
    """Short cornerstone commodity description, truncated to ``max_len``."""
    desc = _COMMODITY_DESC.get(code, "")
    if not desc:
        return ""
    return desc if len(desc) <= max_len else desc[: max_len - 1] + "…"


def _heatmap_row_label(code: str) -> str:
    """``CODE — short description`` for heatmap y-axis tick labels."""
    desc = _summary_desc(code, max_len=36)
    return f"{code} — {desc}" if desc else code


def compute_impact_table(long: pd.DataFrame) -> pd.DataFrame:
    """Cell-level impact metrics for both baselines at ``RANK_TARGET_YEAR``.

    For each (row_sector, col_sector) cell on the ``dom`` matrix, computes:
    - ``impact_vs_useeio = max_alt |A_alt − A_useeio| × |A_useeio|``
    - ``impact_vs_ceda   = max_alt |A_alt − A_ceda|   × |A_ceda|``
    - per-approach values at ``RANK_TARGET_YEAR``

    Where ``alt`` ranges over ``ALTERNATIVE_APPROACHES`` only — the two
    baselines are anchors, not candidates being evaluated.
    """
    snap = long.loc[(long["dom_or_imp"] == "dom") & (long["year"] == RANK_TARGET_YEAR)]
    pivot = snap.pivot_table(
        index=["row_sector", "col_sector"],
        columns="approach",
        values="A_value",
    ).fillna(0.0)

    # Compute max-over-alternatives |A_alt − A_baseline| per cell.
    alt_cols = [c for c in ALTERNATIVE_APPROACHES if c in pivot.columns]
    out = pivot.reset_index()

    for baseline_col, _ in BASELINES:
        if baseline_col not in pivot.columns:
            out[f"impact_vs_{baseline_col}"] = 0.0
            continue
        baseline_vals = pivot[baseline_col].to_numpy()
        max_abs_diff = np.zeros_like(baseline_vals)
        for alt in alt_cols:
            diff = np.abs(pivot[alt].to_numpy() - baseline_vals)
            max_abs_diff = np.maximum(max_abs_diff, diff)
        impact = max_abs_diff * np.abs(baseline_vals)
        out[f"impact_vs_{baseline_col}"] = impact

    out["impact_max_either"] = np.maximum(
        out["impact_vs_useeio"].to_numpy(),
        out["impact_vs_ceda_default"].to_numpy(),
    )

    cs2sum = _cornerstone_to_summary()
    out["col_summary_group"] = out["col_sector"].map(cs2sum).fillna("UNMAPPED")
    out["row_summary_group"] = out["row_sector"].map(cs2sum).fillna("UNMAPPED")

    return out


def aggregate_impact_by_group(
    long: pd.DataFrame, top_n_groups: int
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Sum impact per (col_summary_group, alternative) for each baseline.

    Returns two DataFrames (vs_useeio, vs_ceda) of shape
    ``(n_groups × n_alternatives)`` and the list of group codes (rows)
    selected as the top-N by total impact across both baselines. Computed
    fresh from the snap pivot — needs the per-alternative breakdown rather
    than the per-cell ``max-over-alternatives`` from ``compute_impact_table``.
    """
    cs2sum = _cornerstone_to_summary()
    rows_useeio: list[dict[str, object]] = []
    rows_ceda: list[dict[str, object]] = []

    snap = long.loc[(long["dom_or_imp"] == "dom") & (long["year"] == RANK_TARGET_YEAR)]
    pivot = snap.pivot_table(
        index=["row_sector", "col_sector"],
        columns="approach",
        values="A_value",
    ).fillna(0.0)
    pivot = pivot.reset_index()
    pivot["col_summary_group"] = pivot["col_sector"].map(cs2sum).fillna("UNMAPPED")

    for baseline_col, _ in BASELINES:
        if baseline_col not in pivot.columns:
            continue
        for alt in ALTERNATIVE_APPROACHES:
            if alt not in pivot.columns:
                continue
            cell_impact = np.abs(
                pivot[alt].to_numpy() - pivot[baseline_col].to_numpy()
            ) * np.abs(pivot[baseline_col].to_numpy())
            tmp = pd.DataFrame(
                {
                    "col_summary_group": pivot["col_summary_group"],
                    "impact": cell_impact,
                }
            )
            for grp, grp_sum in (
                tmp.groupby("col_summary_group")["impact"].sum().items()
            ):
                row = {
                    "col_summary_group": str(grp),
                    "alternative": alt,
                    "impact": float(grp_sum),
                }
                if baseline_col == "useeio":
                    rows_useeio.append(row)
                else:
                    rows_ceda.append(row)

    df_u = pd.DataFrame(rows_useeio).pivot_table(
        index="col_summary_group",
        columns="alternative",
        values="impact",
        fill_value=0.0,
    )
    df_c = pd.DataFrame(rows_ceda).pivot_table(
        index="col_summary_group",
        columns="alternative",
        values="impact",
        fill_value=0.0,
    )

    # Make column order stable.
    cols_order = [c for c in ALTERNATIVE_APPROACHES if c in df_u.columns]
    df_u = df_u.reindex(columns=cols_order, fill_value=0.0)
    df_c = df_c.reindex(columns=cols_order, fill_value=0.0)

    # Align row index across the two and pick top-N by combined impact.
    combined = df_u.sum(axis=1).add(df_c.sum(axis=1), fill_value=0.0)
    top_groups = combined.nlargest(top_n_groups).index.tolist()
    df_u = df_u.reindex(index=top_groups, fill_value=0.0)
    df_c = df_c.reindex(index=top_groups, fill_value=0.0)

    return df_u, df_c, top_groups


def plot_impact_heatmap(df_u: pd.DataFrame, df_c: pd.DataFrame, path: Path) -> None:
    """Two-panel heatmap, vs USEEIO | vs CEDA-US, shared color scale.

    Reading: each cell is the total ``|A_alt − A_baseline| × |A_baseline|``
    summed over every dom A cell whose ``col_sector`` rolls up to the row's
    BEA summary parent. Dark = method choice changes a lot of A·A_baseline
    in that group. Shared color scale across panels means one row darker
    on the left than on the right ⇒ that group of cells diverges from
    USEEIO more than from CEDA-US.
    """
    vmax = float(max(df_u.to_numpy().max(), df_c.to_numpy().max()) or 1.0)
    fig, axes = plt.subplots(
        1,
        2,
        figsize=(6.5 * 2, 0.45 * len(df_u) + 2.0),
        squeeze=False,
        constrained_layout=True,
    )
    fig.suptitle(
        f"Impact-weighted divergence by BEA summary group — dom — {RANK_TARGET_YEAR}",
        fontsize=12,
    )

    im = None
    for ax, (df, baseline_label) in zip(
        axes[0], [(df_u, "vs USEEIO"), (df_c, "vs CEDA-US")], strict=True
    ):
        im = ax.imshow(df.to_numpy(), aspect="auto", cmap="viridis", vmin=0, vmax=vmax)
        ax.set_xticks(range(len(df.columns)))
        ax.set_xticklabels(list(df.columns), rotation=30, ha="right", fontsize=9)
        ax.set_yticks(range(len(df.index)))
        ax.set_yticklabels(
            [_heatmap_row_label(str(code)) for code in df.index], fontsize=9
        )
        ax.set_title(baseline_label, fontsize=10)

        arr = df.to_numpy()
        for i in range(arr.shape[0]):
            for j in range(arr.shape[1]):
                val = arr[i, j]
                if val == 0:
                    txt = "0"
                elif val < 0.001:
                    txt = f"{val:.1e}"
                else:
                    txt = f"{val:.3g}"
                color = "white" if val < vmax * 0.55 else "black"
                ax.text(j, i, txt, ha="center", va="center", fontsize=7, color=color)

    if im is not None:
        cbar = fig.colorbar(im, ax=axes[0], shrink=0.7, pad=0.02)
        cbar.set_label("Σ |A_alt − A_baseline| · |A_baseline|", fontsize=9)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_top_cells_grid(
    long: pd.DataFrame, top_cells: pd.DataFrame, path: Path
) -> None:
    """Drill-in grid: top-N cells by ``max(impact_vs_useeio, impact_vs_ceda)``.

    One panel per cell. All five approaches overlaid. Title shows
    ``{row_sector} → {col_sector}`` and the BEA summary parent of the
    consuming industry. Per-panel annotation lists both impact scores.
    """
    n = len(top_cells)
    n_cols = DRILL_IN_NCOLS
    n_rows = (n + n_cols - 1) // n_cols
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(4.0 * n_cols, 3.0 * n_rows),
        squeeze=False,
        sharex=True,
    )
    fig.suptitle(
        f"Top-{n} cells by max(impact_vs_useeio, impact_vs_ceda) — dom — "
        f"ranked at {RANK_TARGET_YEAR}",
        fontsize=12,
    )

    legend_handles: list[Any] = []
    legend_labels: list[str] = []

    for k, (_, row) in enumerate(top_cells.iterrows()):
        ax = axes[k // n_cols][k % n_cols]
        row_sector = str(row["row_sector"])
        col_sector = str(row["col_sector"])
        col_summary = str(row.get("col_summary_group", ""))
        imp_u = float(row["impact_vs_useeio"])
        imp_c = float(row["impact_vs_ceda_default"])

        cell_data = long.loc[
            (long["dom_or_imp"] == "dom")
            & (long["row_sector"] == row_sector)
            & (long["col_sector"] == col_sector)
        ]
        for approach in ALL_APPROACHES_PLOT_ORDER:
            ad = cell_data.loc[cell_data["approach"] == approach].sort_values("year")
            if ad.empty:
                continue
            (line,) = ax.plot(
                ad["year"].to_numpy(),
                ad["A_value"].to_numpy(),
                color=APPROACH_COLORS[approach],
                lw=1.5,
                marker="o",
                markersize=3,
                label=approach,
            )
            if approach not in legend_labels:
                legend_handles.append(line)
                legend_labels.append(approach)

        col_summary_short = _summary_desc(col_summary, max_len=22)
        row_short = _commodity_desc(row_sector)
        col_short = _commodity_desc(col_sector)
        subtitle = (
            f"{row_short} → {col_short}"
            if (row_short and col_short)
            else f"in {col_summary_short}"
        )
        # Multi-line title: code line + plain-text descriptive line. Using
        # set_title rather than separate ax.text avoids the overlap that arises
        # when both claim y=1.0 in axes coordinates.
        ax.set_title(
            f"{row_sector} → {col_sector}  [{col_summary}]\n{subtitle}",
            fontsize=8,
            linespacing=1.3,
        )
        ax.grid(True, alpha=0.3)
        ax.text(
            0.02,
            0.98,
            f"vs USEEIO: {imp_u:.2e}\nvs CEDA  : {imp_c:.2e}",
            transform=ax.transAxes,
            va="top",
            ha="left",
            fontsize=7,
            family="monospace",
            bbox={
                "boxstyle": "round,pad=0.2",
                "facecolor": "white",
                "alpha": 0.85,
                "edgecolor": "lightgray",
            },
        )
        if k % n_cols == 0:
            ax.set_ylabel("A_value", fontsize=8)
        if k // n_cols == n_rows - 1:
            ax.set_xlabel("year", fontsize=8)

    for k in range(n, n_rows * n_cols):
        axes[k // n_cols][k % n_cols].axis("off")

    if legend_handles:
        fig.legend(
            legend_handles,
            legend_labels,
            loc="lower center",
            ncol=len(legend_labels),
            bbox_to_anchor=(0.5, -0.01),
            fontsize=9,
            frameon=False,
        )
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def build_curated_shortlist_df() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for category, entries in CURATED_SHORTLIST.items():
        for code, justification in entries:
            rows.append(
                {
                    "category": category,
                    "bea_code": code,
                    "justification": justification,
                }
            )
    return pd.DataFrame(rows)


def _publish_keysector_tabs(
    top_cells_df: pd.DataFrame, curated_df: pd.DataFrame
) -> None:
    if not LAST_RUN_SHEET_ID_PATH.exists():
        logger.warning(
            "No %s found — skipping Sheet publish. Run derive_A_time_series "
            "first (with valid Drive auth) to create the run report.",
            LAST_RUN_SHEET_ID_PATH,
        )
        return
    sheet_id = LAST_RUN_SHEET_ID_PATH.read_text().strip()
    try:
        update_sheet_tab(sheet_id, "keysector_top_cells_ranked", top_cells_df)
        update_sheet_tab(sheet_id, "keysector_curated_shortlist", curated_df)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Sheet publish skipped (%s: %s). Local artifacts still complete.",
            type(e).__name__,
            e,
        )
        return
    logger.info("Updated key-sector tabs on sheet %s", sheet_id)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Loading %s", A_CELLS_LONG_PATH)
    long = pd.read_parquet(A_CELLS_LONG_PATH)

    impact_df = compute_impact_table(long)
    impact_df_sorted = impact_df.sort_values(
        "impact_max_either", ascending=False
    ).reset_index(drop=True)

    top_cells_df = impact_df_sorted.head(CSV_TOP_N).copy()
    top_cells_df.to_csv(RESULTS_DIR / "keysector_top_cells_ranked.csv", index=False)

    curated_df = build_curated_shortlist_df()
    curated_df.to_csv(RESULTS_DIR / "keysector_curated_shortlist.csv", index=False)

    df_u, df_c, _ = aggregate_impact_by_group(long, HEATMAP_TOP_GROUPS)
    plot_impact_heatmap(df_u, df_c, PLOTS_DIR / "keysector_impact_heatmap.png")

    plot_top_cells_grid(
        long,
        impact_df_sorted.head(DRILL_IN_TOP_N),
        PLOTS_DIR / "keysector_top_cells_grid.png",
    )

    _publish_keysector_tabs(top_cells_df, curated_df)
    logger.info("Step 4 outputs written to %s and %s", RESULTS_DIR, PLOTS_DIR)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
