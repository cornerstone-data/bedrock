"""Clipped per-sector % diff histogram panels for diagnostics sheets.

Release-progression grids, single-sheet renders, and A-matrix bundle histograms
share this panel style: ±100% clip, 60 bins, zero line, n/median/p95 stats box.
Distinct from ``plotting.percent_histogram`` (wider xlim, median reference lines).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.ticker import PercentFormatter

from bedrock.utils.validation.analysis.diagnostics_plots import _parse_pct

EF_KIND_LABEL: dict[str, str] = {"N": "total EF (N)", "D": "direct EF (D)"}

# Panel font sizes (multiplied by ``font_scale`` per panel).
PANEL_TITLE_FONTSIZE = 20
PANEL_AXIS_LABEL_FONTSIZE = 11
PANEL_STATS_FONTSIZE = 10
PANEL_TICK_LABEL_FONTSIZE = 10
PANEL_SUPTITLE_FONTSIZE = 14

HIST_FONT_SCALE = 2.0
HIST_STATS_EXTRA_SCALE = 2.0
HIST_PCT_CLIP = 100.0
HIST_BINS = 60

_KIND_SPEC: dict[str, dict[str, object]] = {
    "N": {
        "pct_col": "N_perc_diff",
        "new_col": "N_new",
        "old_col": "N_old_inflated",
        "numeric_cols": ("N_new", "N_old", "N_old_inflated", "N_perc_diff"),
    },
    "D": {
        "pct_col": "D_perc_diff",
        "new_col": "D_new",
        "old_col": "D_old_inflated",
        "numeric_cols": ("D_new", "D_old", "D_old_inflated", "D_perc_diff"),
    },
}


def _coerce_kind_columns(df: pd.DataFrame, ef_kind: str) -> pd.DataFrame:
    """Coerce diagnostics tab columns, including percent-formatted cells."""
    spec = _KIND_SPEC[ef_kind]
    numeric_cols = spec["numeric_cols"]
    assert isinstance(numeric_cols, tuple)
    out = df.copy()
    for col in numeric_cols:
        if col not in out.columns:
            continue
        if col.endswith("_perc_diff"):
            out[col] = out[col].map(_parse_pct)
        else:
            s = out[col].astype(str).str.strip()
            is_pct = s.str.endswith("%")
            cleaned = s.str.rstrip("%").str.replace(",", "", regex=False)
            numeric = pd.to_numeric(cleaned, errors="coerce")
            out[col] = numeric.mask(is_pct, numeric / 100)
    return out


def pct_values(df: pd.DataFrame, ef_kind: str) -> np.ndarray:
    """Return per-sector pct diff as a fraction array (not percent)."""
    spec = _KIND_SPEC[ef_kind]
    pct_col = str(spec["pct_col"])
    new_col = str(spec["new_col"])
    old_col = str(spec["old_col"])
    df = _coerce_kind_columns(df.copy(), ef_kind)
    if pct_col in df.columns:
        series = pd.Series(pd.to_numeric(df[pct_col], errors="coerce")).dropna()
    elif new_col in df.columns and old_col in df.columns:
        new = pd.Series(pd.to_numeric(df[new_col], errors="coerce"))
        old = pd.Series(pd.to_numeric(df[old_col], errors="coerce"))
        series = ((new - old) / old.abs()).dropna()
    else:
        raise KeyError(
            f"Tab has neither {pct_col!r} nor ({new_col!r}, {old_col!r}); "
            f"got columns: {list(df.columns)}"
        )
    arr = series.to_numpy(dtype=float)
    return arr[np.isfinite(arr)]


def draw_per_sector_pct_hist_panel(
    ax: Axes,
    pct_fraction: np.ndarray,
    *,
    title: str,
    color: str,
    font_scale: float = 1.0,
    ylabel: str = "sector count",
) -> None:
    """Draw one clipped per-sector % diff histogram on ``ax``.

    ``pct_fraction`` values are unit fractions (0.15 = 15%).
    """
    pct_percent = np.asarray(pct_fraction, dtype=float) * 100.0
    finite = pct_percent[np.isfinite(pct_percent)]
    if finite.size == 0:
        ax.text(0.5, 0.5, "no data", transform=ax.transAxes, ha="center", va="center")
        ax.set_title(title, fontsize=PANEL_TITLE_FONTSIZE * font_scale, color="black")
        return
    clipped = np.clip(finite, -HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.hist(clipped, bins=HIST_BINS, color=color, alpha=0.85)
    ax.axvline(0, color="k", lw=1.0)
    ax.set_xlim(-HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.xaxis.set_major_formatter(PercentFormatter(decimals=0))
    ax.grid(True, ls=":", alpha=0.3)
    ax.text(
        0.04,
        0.96,
        (
            f"n={len(finite)}\n"
            f"median={np.median(finite):.1f}%\n"
            f"p95(|·|)={np.quantile(np.abs(finite), 0.95):.1f}%"
        ),
        transform=ax.transAxes,
        fontsize=PANEL_STATS_FONTSIZE * font_scale * HIST_STATS_EXTRA_SCALE,
        va="top",
        ha="left",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", alpha=0.4, edgecolor="0.7"
        ),
    )
    ax.set_title(title, fontsize=PANEL_TITLE_FONTSIZE * font_scale, color="black")
    ax.set_xlabel(
        "Percentage Diff (%)", fontsize=PANEL_AXIS_LABEL_FONTSIZE * font_scale
    )
    ax.set_ylabel(ylabel, fontsize=PANEL_AXIS_LABEL_FONTSIZE * font_scale)
    ax.tick_params(axis="both", labelsize=PANEL_TICK_LABEL_FONTSIZE * font_scale)
