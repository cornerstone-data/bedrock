"""Render per-sector N + D % diff histograms from a single diagnostics Sheet.

The reference 2×2 plot from ``plot_ef_diagnostics`` (e.g.
``ef_pct_hist_bundle_v0_2_ceda_N.png``) panels four A-matrix methods side
by side. Each panel is one run's ``N_and_diffs`` / ``D_and_diffs`` tab vs
the CEDA-US (v0) baseline. This script renders the same panel style for a
*single* diagnostics Sheet — useful when the run you want to inspect lives
outside the dispatched ``ef_run_index.csv`` set (e.g. a one-off
investigation sheet, an ad-hoc config variation).

The default Sheet ID points at the v0.2 "all changes" diagnostics run; pass
a different ID on the command line to render any other run.

Source schema: each tab must contain ``{kind}_perc_diff`` (the script also
accepts ``{kind}_new`` / ``{kind}_old_inflated`` as a fallback), where
``kind`` is ``N`` (total EF) or ``D`` (direct EF). Percent-formatted cells
(``"14.2%"``) are coerced automatically.

The approach label (used in the title and panel color) is derived from the
``config_summary`` tab — whichever of the three A-matrix scaling flags is
``TRUE`` wins; if all three are ``FALSE`` the run is labeled
``cornerstone_default`` (price-index-free default Cornerstone path).

Outputs:
- ``output/plots/ef_pct_hist_v0_2_sheet_ceda_N.png``
- ``output/plots/ef_pct_hist_v0_2_sheet_ceda_D.png``

Usage:
    python -m bedrock.analysis.a_matrix_time_series.plot_v0_2_n_pct_hist
    python -m bedrock.analysis.a_matrix_time_series.plot_v0_2_n_pct_hist <sheet_id>
"""

from __future__ import annotations

import logging
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import PercentFormatter

from bedrock.analysis.a_matrix_time_series.compile_ef_diagnostics import (
    _coerce_numeric,
)
from bedrock.analysis.a_matrix_time_series.constants import (
    APPROACH_COLORS,
    PLOTS_DIR,
)
from bedrock.analysis.a_matrix_time_series.plot_ef_diagnostics import (
    AXIS_LABEL_FONTSIZE,
    EF_KIND_LABEL,
    HIST_BINS,
    HIST_FONT_SCALE,
    HIST_PCT_CLIP,
    HIST_STATS_EXTRA_SCALE,
    STATS_FONTSIZE,
    SUPTITLE_FONTSIZE,
    TICK_LABEL_FONTSIZE,
    TITLE_FONTSIZE,
)
from bedrock.utils.io.gcp import read_sheet_tab

logger = logging.getLogger(__name__)

DEFAULT_SHEET_ID = "1pCSgLD14lmrQg3OtfHvnK4lQrFtqiavrjCt-C3R_bSw"
CONFIG_TAB = "config_summary"

# Per-EF-kind tab name and the columns we read from it. ``D`` mirrors ``N``
# in the diagnostics schema (`compile_ef_diagnostics._read_pair`).
_KIND_SPEC: dict[str, dict[str, object]] = {
    "N": {
        "tab": "N_and_diffs",
        "pct_col": "N_perc_diff",
        "new_col": "N_new",
        "old_col": "N_old_inflated",
        "numeric_cols": ("N_new", "N_old", "N_old_inflated", "N_perc_diff"),
    },
    "D": {
        "tab": "D_and_diffs",
        "pct_col": "D_perc_diff",
        "new_col": "D_new",
        "old_col": "D_old_inflated",
        "numeric_cols": ("D_new", "D_old", "D_old_inflated", "D_perc_diff"),
    },
}

# Map config_summary flag → display approach name. The flag set in the
# v0.2 config_summary mirrors the YAML names referenced in epic #337.
_FLAG_TO_APPROACH: tuple[tuple[str, str], ...] = (
    ("scale_a_matrix_with_useeio_method", "useeio"),
    ("scale_a_matrix_with_summary_tables", "summary_tables"),
    ("scale_a_matrix_with_price_index", "price_index"),
)
_DEFAULT_APPROACH_LABEL = "cornerstone_default"


def _output_path(ef_kind: str) -> object:
    return PLOTS_DIR / f"ef_pct_hist_v0_2_sheet_ceda_{ef_kind}.png"


def _approach_from_config(config_df: pd.DataFrame) -> str:
    """Pick a label for this run from the ``config_summary`` flags.

    Returns the first flag that is ``TRUE``; falls back to
    ``cornerstone_default`` when no scaling flag is enabled (the
    price-index-free Cornerstone default path).
    """
    if config_df.empty:
        return _DEFAULT_APPROACH_LABEL
    flags = (
        config_df.set_index("config_field")["value"].astype(str).str.strip().str.upper()
    )
    for flag, label in _FLAG_TO_APPROACH:
        if flags.get(flag) == "TRUE":
            return label
    return _DEFAULT_APPROACH_LABEL


def _pct_values(df: pd.DataFrame, ef_kind: str) -> np.ndarray:
    """Return per-sector pct diff as a fraction array (not percent)."""
    spec = _KIND_SPEC[ef_kind]
    pct_col = str(spec["pct_col"])
    new_col = str(spec["new_col"])
    old_col = str(spec["old_col"])
    numeric_cols = spec["numeric_cols"]
    assert isinstance(numeric_cols, tuple)
    df = _coerce_numeric(df.copy(), numeric_cols)
    if pct_col in df.columns:
        series = pd.Series(pd.to_numeric(df[pct_col], errors="coerce")).dropna()
    elif new_col in df.columns and old_col in df.columns:
        new = pd.Series(pd.to_numeric(df[new_col], errors="coerce"))
        old = pd.Series(pd.to_numeric(df[old_col], errors="coerce"))
        series = ((new - old) / old.abs()).dropna()
    else:
        raise KeyError(
            f"Tab {spec['tab']!r} has neither {pct_col!r} nor "
            f"({new_col!r}, {old_col!r}); got columns: {list(df.columns)}"
        )
    arr = series.to_numpy(dtype=float)
    return arr[np.isfinite(arr)]


def _render(
    pct: np.ndarray,
    approach: str,
    sheet_id: str,
    ef_kind: str,
    out_path: str,
) -> None:
    """Render a single-panel histogram matching the per-panel style of
    ``ef_pct_hist_*`` figures.

    Font scale, bins, clipping, stats-box layout, and percent x-axis are
    pulled from ``plot_ef_diagnostics`` so this output reads as one of the
    four panels in the reference bundle plot.
    """
    # Fall back to grey (the useeio palette entry) for unknown approaches like
    # ``cornerstone_default`` — visually signals "no scaling flag set".
    color = APPROACH_COLORS.get(approach, "#7f7f7f")
    kind_label = EF_KIND_LABEL[ef_kind]
    pct_percent = pct * 100.0
    clipped = np.clip(pct_percent, -HIST_PCT_CLIP, HIST_PCT_CLIP)

    font_scale = HIST_FONT_SCALE
    fig, ax = plt.subplots(figsize=(11.0 * font_scale * 0.6, 10.5 * font_scale * 0.6))
    ax.hist(clipped, bins=HIST_BINS, color=color, alpha=0.85)
    ax.axvline(0, color="k", lw=0.5)
    ax.set_xlim(-HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.xaxis.set_major_formatter(PercentFormatter(decimals=0))
    ax.grid(True, ls=":", alpha=0.3)
    ax.text(
        0.04,
        0.96,
        (
            f"n={len(pct_percent)}\n"
            f"median={np.median(pct_percent):.1f}%\n"
            f"p95(|·|)={np.quantile(np.abs(pct_percent), 0.95):.1f}%"
        ),
        transform=ax.transAxes,
        fontsize=STATS_FONTSIZE * font_scale * HIST_STATS_EXTRA_SCALE,
        va="top",
        ha="left",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", alpha=0.4, edgecolor="0.7"
        ),
    )
    ax.set_title(approach, fontsize=TITLE_FONTSIZE * font_scale, color="black")
    ax.set_xlabel("Percentage Diff (%)", fontsize=AXIS_LABEL_FONTSIZE * font_scale)
    ax.set_ylabel("sector count", fontsize=AXIS_LABEL_FONTSIZE * font_scale)
    ax.tick_params(axis="both", labelsize=TICK_LABEL_FONTSIZE * font_scale)

    fig.suptitle(
        f"{kind_label} per-sector % diff distribution — vs CEDA-US (v0) "
        f"[A-matrix method bundled with bedrock v0.2] — sheet {sheet_id[:10]}…",
        fontsize=SUPTITLE_FONTSIZE * font_scale,
        y=1.0,
    )
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _read_config(sheet_id: str) -> pd.DataFrame:
    try:
        return read_sheet_tab(sheet_id, CONFIG_TAB)
    except Exception as e:
        logger.warning(
            "Could not read %r from sheet %s (%s); using default approach label.",
            CONFIG_TAB,
            sheet_id,
            e,
        )
        return pd.DataFrame({"config_field": [], "value": []})


def main(sheet_id: str = DEFAULT_SHEET_ID) -> None:
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    approach = _approach_from_config(_read_config(sheet_id))
    logger.info("Resolved approach=%r for sheet=%s", approach, sheet_id)

    for ef_kind in ("N", "D"):
        spec = _KIND_SPEC[ef_kind]
        tab = str(spec["tab"])
        df = read_sheet_tab(sheet_id, tab)
        if df.empty:
            logger.warning("Tab %r in sheet %s is empty; skipping.", tab, sheet_id)
            continue
        pct = _pct_values(df, ef_kind)
        if pct.size == 0:
            logger.warning(
                "No finite pct values in %r of sheet %s; skipping.", tab, sheet_id
            )
            continue
        out_path = _output_path(ef_kind)
        _render(
            pct,
            approach=approach,
            sheet_id=sheet_id,
            ef_kind=ef_kind,
            out_path=str(out_path),
        )
        logger.info("Wrote %s", out_path)
        print(f"Wrote: {out_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sheet_id = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SHEET_ID
    main(sheet_id)
