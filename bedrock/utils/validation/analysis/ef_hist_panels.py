"""Clipped per-sector % diff histogram panels for diagnostics sheets.

Release-progression grids, single-sheet renders, and A-matrix bundle histograms
share this panel style: ±100% clip, 60 bins, zero line, n/median/p95 stats box.
Distinct from ``plotting.percent_histogram`` (wider xlim, median reference lines).

Optional key-sector overlays (rugs + callout) mirror the ceda
``usa_mrio_final`` N histograms: annotation-only; the %-diff distribution is
unchanged.
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

# Pinned callout sectors for N histograms (same shortlist as ceda usa_mrio_final).
# Top |BLy| emitters after dropping waste (562*) and |N%| > 100 outliers.
KEY_USA_SECTORS: tuple[str, ...] = (
    "221100",  # Electric power generation, transmission, and distribution
    "211000",  # Oil and gas extraction
    "1121A0",  # Beef cattle ranching and farming (incl. feedlots)
    "GSLGO",  # State and local government (other services)
    "481000",  # Air transportation
    "1111B0",  # Grain farming
    "324110",  # Petroleum refineries
    "484000",  # Truck transportation
)
_COLOR_KEY_SECTOR = "#4a5568"  # neutral; no up/down encoding

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


def key_sectors_frame(sector_names: pd.Series | None = None) -> pd.DataFrame:
    """Pinned ``KEY_USA_SECTORS`` with optional live ``sector_name`` labels.

    ``sector_names`` is indexed by sector code. Missing names fall back to "".
    """
    names = (
        sector_names.astype(str) if sector_names is not None else pd.Series(dtype=str)
    )
    return pd.DataFrame(
        {
            "sector": list(KEY_USA_SECTORS),
            "sector_name": names.reindex(list(KEY_USA_SECTORS)).fillna("").to_numpy(),
        }
    )


def annotate_key_sectors(
    ax: Axes,
    sectors: pd.DataFrame,
    pct_by_sector: pd.Series,
    *,
    font_scale: float = 1.0,
) -> None:
    """Neutral rug ticks (sector-ID labels) + name/N% callout (hist unchanged).

    ``pct_by_sector`` is in **percent** units (same as the panel x-axis), indexed
    by stripped sector code. Values outside ``±HIST_PCT_CLIP`` are clipped for
    rug placement only.
    """
    lo, hi = -HIST_PCT_CLIP, HIST_PCT_CLIP
    y0, y1 = ax.get_ylim()
    rug_h = (y1 - y0) * 0.07
    pct_lookup = pct_by_sector.copy()
    pct_lookup.index = pct_lookup.index.astype(str).str.strip()

    marks: list[tuple[str, float, float]] = []
    rows: list[str] = []
    for r in sectors.itertuples(index=False):
        sec = str(r.sector).strip()
        pct_val = pct_lookup.get(sec)
        if pct_val is None:
            has_pct = False
            pct = float("nan")
        else:
            pct = float(pct_val)
            has_pct = bool(np.isfinite(pct))
            if not has_pct:
                pct = float("nan")

        if has_pct:
            marks.append((sec, float(np.clip(pct, lo, hi)), pct))

        name = str(r.sector_name).strip() or sec
        if len(name) > 28:
            name = name[:27] + "…"
        pct_str = f"{pct:+5.1f}%" if has_pct else "  n/a"
        rows.append(f"{sec:<6} {name:<28} {pct_str}")

    marks.sort(key=lambda t: t[1])
    heights: list[int] = []
    last_x_at_level: dict[int, float] = {}
    min_sep = (hi - lo) * 0.04
    for _, x, _ in marks:
        level = 0
        while level in last_x_at_level and abs(x - last_x_at_level[level]) < min_sep:
            level += 1
        heights.append(level)
        last_x_at_level[level] = x

    label_fs = 7.5 * font_scale
    for (sec, x, _), level in zip(marks, heights, strict=True):
        ax.vlines(
            x,
            y0,
            y0 + rug_h,
            colors=_COLOR_KEY_SECTOR,
            linewidths=2.0,
            zorder=5,
        )
        ax.plot(x, y0 + rug_h, "^", color=_COLOR_KEY_SECTOR, markersize=6, zorder=6)
        ax.text(
            x,
            y0 + rug_h * (1.15 + 0.85 * level),
            sec,
            ha="center",
            va="bottom",
            fontsize=label_fs,
            color=_COLOR_KEY_SECTOR,
            fontweight="bold",
            zorder=7,
        )

    if not rows:
        return

    header = f"{'code':<6} {'name':<28} {'N%':>6}"
    box = "Key sectors (by BLy)\n" + header + "\n" + "\n".join(rows)
    ax.text(
        0.98,
        0.97,
        box,
        transform=ax.transAxes,
        fontsize=max(6.5, 8.5 * font_scale),
        va="top",
        ha="right",
        fontfamily="monospace",
        linespacing=1.25,
        bbox=dict(
            boxstyle="round,pad=0.35", facecolor="white", edgecolor="0.6", alpha=0.92
        ),
        zorder=7,
    )


def draw_per_sector_pct_hist_panel(
    ax: Axes,
    pct_fraction: np.ndarray,
    *,
    title: str,
    color: str,
    font_scale: float = 1.0,
    ylabel: str = "sector count",
    pct_by_sector: pd.Series | None = None,
    key_sectors: pd.DataFrame | None = None,
) -> None:
    """Draw one clipped per-sector % diff histogram on ``ax``.

    ``pct_fraction`` values are unit fractions (0.15 = 15%).
    When ``key_sectors`` and ``pct_by_sector`` are provided, draws the key-sector
    overlay. ``pct_by_sector`` may be fractions or percent; values with typical
    |median| <= 2 are treated as fractions and scaled to percent.
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

    if key_sectors is not None and pct_by_sector is not None and not key_sectors.empty:
        overlay = pd.to_numeric(pct_by_sector, errors="coerce")
        finite_overlay = overlay[np.isfinite(overlay.to_numpy(dtype=float))]
        if (
            finite_overlay.size
            and float(np.nanmedian(np.abs(finite_overlay.to_numpy(dtype=float)))) <= 2.0
        ):
            overlay = overlay * 100.0
        annotate_key_sectors(ax, key_sectors, overlay, font_scale=font_scale)
