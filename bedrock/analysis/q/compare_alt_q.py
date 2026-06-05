"""Compare three q-estimation pathways at Cornerstone detail and BEA summary level.

Pathways
--------
1. q_V_scaled
   derive_q_from_scaled_cornerstone_V_from_authoritative_x()
   V is inflated to model_base_year then rescaled so each industry row sum equals
   the authoritative BEA gross-output x. q = V.sum(axis=0).

2. q_Aq_scaled
   scale_cornerstone_q (with dollar-year adjustment) + inflate_with_commodity_pi.
   Replicates the derive_cornerstone_Aq_scaled() branch that is active when
   scale_a_matrix_with_summary_tables=True AND adjust_summary_A_and_q_dollar_year=True.
   The dollar-year flag is hardwired here so the comparison is independent of the
   live config.

3. q_inflated
   inflate_cornerstone_q_or_y_with_commodity_pi applied directly to the base 2017 q
   (no structural summary-table scaling). This is the commodity-price-index-only path.

Reference vector
----------------
x_authoritative
   derive_cornerstone_x_after_redefinition(year=model_base_year): BEA annual gross
   industry output at model_base_year, after redefinitions, expanded to the 405-sector
   Cornerstone schema. Already in model_base_year current dollars — no additional
   inflation applied. Industry codes and commodity codes share the same 405-sector
   namespace, so each q pathway is compared directly to this x.

At the summary level each pathway is summed via the Cornerstone→BEA-summary mapping
and compared both to derive_summary_q_usa(model_base_year) (the "Make Table" q) and
to the aggregated x_authoritative.

Usage
-----
    python -m bedrock.analysis.q.compare_alt_q
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.transform.eeio.derived_2017 import derive_summary_q_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_V,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_q_from_scaled_cornerstone_V_from_authoritative_x,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    adjust_summary_q_dollar_year,
    inflate_cornerstone_q_or_y_with_commodity_pi,
)
from bedrock.utils.math.formulas import compute_q
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    load_bea_v2017_summary_to_cornerstone,
)

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / "output"
PLOTS_DIR = OUTPUT_DIR / "plots"

_PATHWAY_STYLE: list[tuple[str, str, str]] = [
    ("q_V_scaled", "V_scaled", "#1f77b4"),
    ("q_Aq_scaled", "Aq_scaled", "#ff7f0e"),
    ("q_inflated", "inflated", "#2ca02c"),
]


# ---------------------------------------------------------------------------
# Pathway builders
# ---------------------------------------------------------------------------


def _build_q_V_scaled() -> pd.Series:
    """Pathway 1: q from V rescaled to authoritative gross-output x."""
    return derive_q_from_scaled_cornerstone_V_from_authoritative_x()


def _build_q_Aq_scaled(
    base_q: pd.Series,
    detail_year: int,
    model_year: int,
) -> pd.Series:
    """Pathway 2: scale (dollar-year-adjusted) + inflate with commodity PI.

    Replicates derive_cornerstone_Aq_scaled() with
    scale_a_matrix_with_summary_tables=True and
    adjust_summary_A_and_q_dollar_year=True, regardless of the active config.
    """
    # --- scale step (dollar-year-adjusted) ---
    q_summary_target = derive_summary_q_usa(model_year)
    q_summary_target_adj = adjust_summary_q_dollar_year(
        q_summary=q_summary_target,
        from_year=model_year,
        to_year=detail_year,
    )
    q_summary_base = derive_summary_q_usa(detail_year)
    ratio = (q_summary_target_adj / q_summary_base).fillna(1.0)
    ratio = ratio.replace([np.inf, -np.inf], 1.0)

    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    q_scaled = base_q.copy()
    for summary_sector, cs_sectors in summary_to_cornerstone.items():
        if summary_sector not in ratio.index:
            continue
        val = ratio.loc[summary_sector]
        sectors = [s for s in cs_sectors if s in q_scaled.index]
        if sectors:
            q_scaled.loc[sectors] *= float(val)

    # --- inflate step ---
    return inflate_cornerstone_q_or_y_with_commodity_pi(
        q_scaled, original_year=detail_year, target_year=model_year
    )


def _build_q_inflated(
    base_q: pd.Series,
    detail_year: int,
    model_year: int,
) -> pd.Series:
    """Pathway 3: inflate base 2017 q directly with commodity PI (no scaling)."""
    return inflate_cornerstone_q_or_y_with_commodity_pi(
        base_q, original_year=detail_year, target_year=model_year
    )


# ---------------------------------------------------------------------------
# Aggregation to summary level
# ---------------------------------------------------------------------------


def _cornerstone_to_summary_map() -> dict[str, str]:
    """cornerstone_code → bea_summary_code lookup."""
    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    mapping = {
        code: str(summary)
        for summary, codes in summary_to_cornerstone.items()
        for code in codes
    }
    # BEA industry 331314 (secondary aluminum smelting) is not a commodity code;
    # after redefinitions its output is attributed to commodity 331313.
    if "331313" in mapping:
        mapping["331314"] = mapping["331313"]
    return mapping


def aggregate_q_to_summary(q: pd.Series, cs_to_summary: dict[str, str]) -> pd.Series:
    """Sum q within each BEA summary parent.

    Any sector in q that is absent from cs_to_summary is silently dropped
    by the valid_summaries filter. A warning is emitted with the count and
    dollar sum so that unexpected losses surface immediately.
    """
    unmapped = [c for c in q.index if c not in cs_to_summary]
    if unmapped:
        lost = q.loc[unmapped].sum()
        logger.warning(
            "aggregate_q_to_summary: %d sector(s) not in cs_to_summary "
            "and will be dropped (total $M %.0f): %s",
            len(unmapped),
            lost,
            unmapped,
        )
    mapped = q.rename(index=cs_to_summary)
    agg = mapped.groupby(level=0).sum()
    valid_summaries = set(cs_to_summary.values())
    agg = agg.loc[[c for c in agg.index if c in valid_summaries]]
    return agg


# ---------------------------------------------------------------------------
# Comparison tables
# ---------------------------------------------------------------------------


def _pct_diff(a: pd.Series, b: pd.Series, label_a: str, label_b: str) -> pd.Series:
    """(a − b) / b × 100, aligned on common index."""
    common = a.index.intersection(b.index)
    denom = b.reindex(common).replace(0, np.nan)
    return ((a.reindex(common) - denom) / denom * 100).rename(
        f"pct_diff_{label_a}_vs_{label_b}"
    )


def _build_detail_comparison(
    q_V_scaled: pd.Series,
    q_Aq_scaled: pd.Series,
    q_inflated: pd.Series,
    x_authoritative: pd.Series,
) -> pd.DataFrame:
    """Side-by-side detail table with pairwise pct differences.

    x_authoritative is industry gross output; q series are commodity gross
    output. Both share the 405-sector Cornerstone code space so direct
    element-wise comparison is valid.
    """
    common = (
        q_V_scaled.index.intersection(q_Aq_scaled.index)
        .intersection(q_inflated.index)
        .intersection(x_authoritative.index)
    )
    df = pd.DataFrame(
        {
            "x_authoritative": x_authoritative.reindex(common),
            "q_V_scaled": q_V_scaled.reindex(common),
            "q_Aq_scaled": q_Aq_scaled.reindex(common),
            "q_inflated": q_inflated.reindex(common),
        }
    )
    df["pct_Aq_vs_inflated"] = _pct_diff(
        q_Aq_scaled, q_inflated, "Aq_scaled", "inflated"
    ).reindex(common)
    df["pct_V_vs_Aq"] = _pct_diff(
        q_V_scaled, q_Aq_scaled, "V_scaled", "Aq_scaled"
    ).reindex(common)
    df["pct_V_vs_inflated"] = _pct_diff(
        q_V_scaled, q_inflated, "V_scaled", "inflated"
    ).reindex(common)
    df["pct_V_vs_x"] = _pct_diff(q_V_scaled, x_authoritative, "V_scaled", "x").reindex(
        common
    )
    df["pct_Aq_vs_x"] = _pct_diff(
        q_Aq_scaled, x_authoritative, "Aq_scaled", "x"
    ).reindex(common)
    df["pct_inflated_vs_x"] = _pct_diff(
        q_inflated, x_authoritative, "inflated", "x"
    ).reindex(common)
    return df.sort_values("x_authoritative", ascending=False)


def _build_summary_comparison(
    q_V_scaled: pd.Series,
    q_Aq_scaled: pd.Series,
    q_inflated: pd.Series,
    x_authoritative: pd.Series,
    q_make_table: pd.Series,
    cs_to_summary: dict[str, str],
) -> pd.DataFrame:
    """Summary-level table: each pathway vs Make Table q and vs x_authoritative."""
    agg_V = aggregate_q_to_summary(q_V_scaled, cs_to_summary)
    agg_Aq = aggregate_q_to_summary(q_Aq_scaled, cs_to_summary)
    agg_inf = aggregate_q_to_summary(q_inflated, cs_to_summary)
    agg_x = aggregate_q_to_summary(x_authoritative, cs_to_summary)

    common = (
        agg_V.index.intersection(agg_Aq.index)
        .intersection(agg_inf.index)
        .intersection(agg_x.index)
        .intersection(q_make_table.index)
    )
    df = pd.DataFrame(
        {
            "x_authoritative_agg": agg_x.reindex(common),
            "q_make_table": q_make_table.reindex(common),
            "q_V_scaled_agg": agg_V.reindex(common),
            "q_Aq_scaled_agg": agg_Aq.reindex(common),
            "q_inflated_agg": agg_inf.reindex(common),
        }
    )
    denom_mt = df["q_make_table"].replace(0, np.nan)
    denom_x = df["x_authoritative_agg"].replace(0, np.nan)
    for col, label in [
        ("q_V_scaled_agg", "V_scaled"),
        ("q_Aq_scaled_agg", "Aq_scaled"),
        ("q_inflated_agg", "inflated"),
    ]:
        df[f"pct_{label}_vs_make_table"] = (df[col] - denom_mt) / denom_mt * 100
        df[f"pct_{label}_vs_x"] = (df[col] - denom_x) / denom_x * 100

    return df.sort_values("x_authoritative_agg", ascending=False)


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------


def _scatter_panel(
    ax: plt.Axes,
    x: np.ndarray,
    y: np.ndarray,
    title: str,
    xlabel: str,
    ylabel: str,
    color: str,
) -> None:
    """Shared helper: log-log scatter with identity line on a single Axes."""
    mask = (x > 0) & (y > 0)
    ax.scatter(x[mask], y[mask], s=5, alpha=0.5, color=color, zorder=2)
    lo = min(x[mask].min(), y[mask].min()) * 0.9
    hi = max(x[mask].max(), y[mask].max()) * 1.1
    ax.plot([lo, hi], [lo, hi], "k--", lw=0.8, zorder=3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_title(title, fontsize=10)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.set_ylabel(ylabel, fontsize=8)
    ax.grid(True, alpha=0.25, which="both")


def _plot_detail_scatter(
    detail_df: pd.DataFrame,
    q_2017: pd.Series,
    x_2017: pd.Series,
    model_year: int,
) -> Path:
    """1×4 log-log scatter at detail level.

    Panels 1–3: each q pathway vs x_authoritative (model_year).
    Panel 4 (reference): 2017 q vs 2017 x, both from the Cornerstone Make
    matrix after redefinitions — shows the baseline q/x relationship before
    any scaling or inflation is applied.
    """
    fig, axes = plt.subplots(1, 4, figsize=(20, 5))
    fig.suptitle(
        f"Detail-level q vs x — model_year {model_year}  |  panel 4: 2017 reference",
        fontsize=11,
    )

    x_auth = detail_df["x_authoritative"].to_numpy(dtype=float)
    for ax, (col, label, color) in zip(axes[:3], _PATHWAY_STYLE):
        _scatter_panel(
            ax,
            x=x_auth,
            y=detail_df[col].to_numpy(dtype=float),
            title=label,
            xlabel="x_authoritative ($M)",
            ylabel=f"q  {label} ($M)",
            color=color,
        )

    # Reference panel: 2017 q vs 2017 x
    common_2017 = q_2017.index.intersection(x_2017.index)
    _scatter_panel(
        axes[3],
        x=x_2017.reindex(common_2017).to_numpy(dtype=float),
        y=q_2017.reindex(common_2017).to_numpy(dtype=float),
        title="2017 reference (q vs x, same V)",
        xlabel="x  2017 Cornerstone Make ($M)",
        ylabel="q  2017 Cornerstone Make ($M)",
        color="#9467bd",
    )

    fig.tight_layout()
    path = PLOTS_DIR / f"detail_scatter_{model_year}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _plot_summary_levels(summary_df: pd.DataFrame, model_year: int) -> Path:
    """Ranked line plot of summary-level absolute q values (log y-axis).

    Sectors sorted by x_authoritative_agg descending (left = largest).
    """
    df = summary_df.sort_values("x_authoritative_agg", ascending=False)
    ranks = np.arange(len(df))

    series = [
        ("x_authoritative_agg", "x_authoritative", "black", "o", 2.0),
        ("q_make_table", "q_make_table", "#7f7f7f", "s", 1.5),
    ] + [(f"{col}_agg", label, color, "^", 1.0) for col, label, color in _PATHWAY_STYLE]

    fig, ax = plt.subplots(figsize=(14, 5))
    for colname, label, color, marker, lw in series:
        vals = df[colname].to_numpy(dtype=float)
        ax.plot(
            ranks,
            vals,
            marker=marker,
            markersize=3,
            linewidth=lw,
            color=color,
            label=label,
            alpha=0.85,
        )

    ax.set_yscale("log")
    ax.set_xticks(ranks[::4])
    ax.set_xticklabels(df.index[::4], rotation=45, ha="right", fontsize=7)
    ax.set_xlabel("BEA summary sector (ranked by x_authoritative)", fontsize=9)
    ax.set_ylabel("$M (log scale)", fontsize=9)
    ax.set_title(f"Summary-level q pathways vs references — {model_year}", fontsize=11)
    ax.legend(fontsize=8, frameon=False)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    path = PLOTS_DIR / f"summary_levels_{model_year}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


def _plot_summary_pct_deviations(summary_df: pd.DataFrame, model_year: int) -> Path:
    """1×2 grouped horizontal bar chart: % deviation from x_authoritative (left)
    and from Make Table q (right), sectors sorted by x_authoritative_agg descending.
    """
    df = summary_df.sort_values(
        "x_authoritative_agg", ascending=True
    )  # ascending → biggest at top
    sectors = df.index.tolist()
    n = len(sectors)
    y = np.arange(n)
    bar_h = 0.25

    fig, axes = plt.subplots(1, 2, figsize=(16, max(6, n * 0.22)), sharey=True)
    fig.suptitle(f"Summary-level q % deviation — model_year {model_year}", fontsize=11)

    for ax, ref_suffix, ref_label in [
        (axes[0], "x", "x_authoritative"),
        (axes[1], "make_table", "Make Table q"),
    ]:
        for i, (_, label, color) in enumerate(_PATHWAY_STYLE):
            pct_col = f"pct_{label}_vs_{ref_suffix}"
            vals = df[pct_col].to_numpy(dtype=float)
            ax.barh(
                y + (i - 1) * bar_h,
                vals,
                height=bar_h,
                color=color,
                label=label,
                alpha=0.8,
            )
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_xlabel("% deviation", fontsize=9)
        ax.set_title(f"vs {ref_label}", fontsize=10)
        ax.grid(True, axis="x", alpha=0.3)
        ax.legend(fontsize=8, frameon=False)

    axes[0].set_yticks(y)
    axes[0].set_yticklabels(sectors, fontsize=7)
    fig.tight_layout()
    path = PLOTS_DIR / f"summary_pct_deviations_{model_year}.png"
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    cfg = get_usa_config()
    detail_year: int = cfg.usa_detail_original_year
    model_year: int = cfg.model_base_year

    print(f"compare_alt_q   detail_year={detail_year}  model_year={model_year}")

    base_q = derive_cornerstone_Aq().scaled_q
    x_2017 = derive_cornerstone_x()
    q_2017 = compute_q(V=derive_cornerstone_V())
    q_V_scaled = _build_q_V_scaled()
    q_Aq_scaled = _build_q_Aq_scaled(base_q, detail_year, model_year)
    q_inflated = _build_q_inflated(base_q, detail_year, model_year)
    x_authoritative = derive_cornerstone_x_after_redefinition(year=model_year)
    q_make_table = derive_summary_q_usa(model_year)

    detail_df = _build_detail_comparison(
        q_V_scaled, q_Aq_scaled, q_inflated, x_authoritative
    )
    cs_to_summary = _cornerstone_to_summary_map()
    summary_df = _build_summary_comparison(
        q_V_scaled,
        q_Aq_scaled,
        q_inflated,
        x_authoritative,
        q_make_table,
        cs_to_summary,
    )

    # ------------------------------------------------------------------
    # Printed summary stats
    # ------------------------------------------------------------------
    print("\nAggregate totals at summary level ($M):")
    for col, label in [
        ("x_authoritative_agg", "x_authoritative"),
        ("q_make_table", "q_make_table"),
        ("q_V_scaled_agg", "V_scaled"),
        ("q_Aq_scaled_agg", "Aq_scaled"),
        ("q_inflated_agg", "inflated"),
    ]:
        print(f"  {label:<20} {summary_df[col].sum():>14,.0f}")

    print("\nOverall pct deviation from Make Table q:")
    denom_mt = summary_df["q_make_table"].sum()
    for col, label, _ in _PATHWAY_STYLE:
        agg_col = f"{col}_agg"
        print(
            f"  {label:<18} {(summary_df[agg_col].sum() - denom_mt) / denom_mt * 100:+.3f}%"
        )

    print("\nOverall pct deviation from x_authoritative:")
    denom_x = summary_df["x_authoritative_agg"].sum()
    for col, label, _ in _PATHWAY_STYLE:
        agg_col = f"{col}_agg"
        print(
            f"  {label:<18} {(summary_df[agg_col].sum() - denom_x) / denom_x * 100:+.3f}%"
        )

    # ------------------------------------------------------------------
    # Plots
    # ------------------------------------------------------------------
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    p1 = _plot_detail_scatter(
        detail_df, q_2017=q_2017, x_2017=x_2017, model_year=model_year
    )
    p2 = _plot_summary_levels(summary_df, model_year)
    p3 = _plot_summary_pct_deviations(summary_df, model_year)
    print(f"\nPlots saved to {PLOTS_DIR}:")
    for p in (p1, p2, p3):
        print(f"  {p.name}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    main()
