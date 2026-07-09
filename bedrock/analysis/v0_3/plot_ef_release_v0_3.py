"""Release-deck EF plots for the v0.3 diagnostics progression.

Uses existing diagnostics Sheets on Drive (no re-dispatch). Figures:

- v0.3 progression grids (CEDA 7-step N+D; USEEIO 7-step N purchaser + D)
- FINAL v0.2 vs v0.3 overlays vs CEDA v0 (N and D)
- FINAL v0.2 vs v0.3 overlay vs USEEIO purchaser (N)
- BLy pair: FINAL v0.2 + v0.3 (CEDA baseline runs only)

Usage:
    uv run python -m bedrock.analysis.v0_3.plot_ef_release_v0_3
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from pathlib import Path

import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import PercentFormatter

from bedrock.analysis.a_matrix_time_series.plot_ef_diagnostics import (
    AXIS_LABEL_FONTSIZE,
    EF_KIND_LABEL,
    HIST_BINS,
    HIST_PCT_CLIP,
    HIST_STATS_EXTRA_SCALE,
    STATS_FONTSIZE,
    TICK_LABEL_FONTSIZE,
    TITLE_FONTSIZE,
)
from bedrock.analysis.a_matrix_time_series.plot_v0_3_n_pct_hist import _pct_values
from bedrock.utils.validation.analysis.bly_plots import TAB_BLY, build_sector_stack_frame
from bedrock.utils.validation.analysis.diagnostics_plots import (
    _drop_old_only,
    _normalize_schema,
    bly_figsize,
)
from bedrock.utils.validation.analysis.fetch import load_tab
from bedrock.utils.validation.analysis.overlay_ef_hist import _series_for_kind
from bedrock.utils.validation.analysis.plotting import (
    overlay_pct_diff_histogram,
    plot_stacked_net_change,
    save_and_close,
    setup_mpl,
)
from bedrock.utils.validation.analysis.release_v0_3_progression import (
    ProgressionSheet,
    V02_FINAL_CEDA,
    V02_FINAL_USEEIO,
    V03_CEDA_STEPS,
    V03_USEEIO_STEPS,
)

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent / "output" / "release_v0_3"

CEDA_BAR = "#1f77b4"
USEEIO_BAR = "#ff7f0e"

FINAL_V02_CEDA = V02_FINAL_CEDA.sheet_id
FINAL_V03_CEDA = V03_CEDA_STEPS[-1].sheet_id
FINAL_V02_USEEIO = V02_FINAL_USEEIO.sheet_id
FINAL_V03_USEEIO = V03_USEEIO_STEPS[-1].sheet_id


def _ceda_kind_fractions(sheet_id: str, ef_kind: str) -> np.ndarray:
    tab = f"{ef_kind}_and_diffs"
    df = _drop_old_only(_normalize_schema(load_tab(sheet_id, tab)))
    return _pct_values(df, ef_kind)


def _useeio_purchaser_fractions(sheet_id: str) -> np.ndarray:
    df = _drop_old_only(_normalize_schema(load_tab(sheet_id, "N_and_diffs")))
    num = pd.to_numeric(df["N_new_purchaser"], errors="coerce")
    den = pd.to_numeric(df["N_old_purchaser"], errors="coerce")
    return ((num - den) / den.abs()).dropna().to_numpy(dtype=float)


def _draw_deck_panel(
    ax: plt.Axes,
    pct_fraction: np.ndarray,
    title: str,
    *,
    bar_color: str,
    font_scale: float = 1.0,
) -> None:
    pct_percent = pct_fraction * 100.0
    finite = pct_percent[np.isfinite(pct_percent)]
    clipped = np.clip(finite, -HIST_PCT_CLIP, HIST_PCT_CLIP)
    ax.hist(clipped, bins=HIST_BINS, color=bar_color, alpha=0.85)
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
        fontsize=STATS_FONTSIZE * font_scale * HIST_STATS_EXTRA_SCALE,
        va="top",
        ha="left",
        bbox=dict(
            boxstyle="round,pad=0.3", facecolor="white", alpha=0.4, edgecolor="0.7"
        ),
    )
    ax.set_title(title, fontsize=TITLE_FONTSIZE * font_scale, color="black")
    ax.set_xlabel("Percentage Diff (%)", fontsize=AXIS_LABEL_FONTSIZE * font_scale)
    ax.set_ylabel("sector count", fontsize=AXIS_LABEL_FONTSIZE * font_scale)
    ax.tick_params(axis="both", labelsize=TICK_LABEL_FONTSIZE * font_scale)


def _plot_progression_grid(
    steps: tuple[ProgressionSheet, ...],
    *,
    group_title: str,
    out_path: Path,
    bar_color: str,
    pct_loader: Callable[[str], np.ndarray],
    ncols: int = 3,
) -> None:
    n = len(steps)
    cols = min(ncols, n)
    rows = math.ceil(n / cols)
    fig, axes = plt.subplots(
        rows, cols, figsize=(5.5 * cols, 4.8 * rows), squeeze=False
    )
    flat = list(axes.flat)
    ymax = 0.0
    for i, step in enumerate(steps):
        ax = flat[i]
        pct = pct_loader(step.sheet_id)
        _draw_deck_panel(ax, pct, step.step_label, bar_color=bar_color, font_scale=0.85)
        ymax = max(ymax, ax.get_ylim()[1])
    for i in range(n):
        flat[i].set_ylim(0, ymax)
    for ax in flat[n:]:
        ax.axis("off")
    fig.suptitle(group_title, fontsize=14, y=1.02)
    fig.tight_layout()
    save_and_close(fig, out_path)
    print(f"Wrote: {out_path}")


def _plot_v03_progressions(out_dir: Path) -> None:
    for ef_kind in ("N", "D"):

        def ceda_loader(sid: str, kind: str = ef_kind) -> np.ndarray:
            return _ceda_kind_fractions(sid, kind)

        _plot_progression_grid(
            V03_CEDA_STEPS,
            group_title=(
                f"[GROUP: v0.3 · baseline: CEDA-US (v0)] per-sector {ef_kind} % diff, "
                "by release step"
            ),
            out_path=out_dir / f"progression_v03_ceda_{ef_kind}.png",
            bar_color=CEDA_BAR,
            pct_loader=ceda_loader,
        )

    _plot_progression_grid(
        V03_USEEIO_STEPS,
        group_title=(
            "[GROUP: v0.3 · baseline: USEEIO (purchaser)] per-sector N % diff, "
            "by release step"
        ),
        out_path=out_dir / "progression_v03_useeio_N.png",
        bar_color=USEEIO_BAR,
        pct_loader=_useeio_purchaser_fractions,
    )

    _plot_progression_grid(
        V03_USEEIO_STEPS,
        group_title=(
            "[GROUP: v0.3 · baseline: USEEIO (purchaser)] per-sector D % diff, "
            "by release step (producer / CEDA v0)"
        ),
        out_path=out_dir / "progression_v03_useeio_D.png",
        bar_color=USEEIO_BAR,
        pct_loader=lambda sid: _ceda_kind_fractions(sid, "D"),
    )


def _useeio_purchaser_series_percent(sheet_id: str) -> "pd.Series[float]":
    return pd.Series(_useeio_purchaser_fractions(sheet_id) * 100.0)


def _plot_final_overlays(out_dir: Path) -> None:
    for ef_kind in ("N", "D"):
        series_by_label = {
            "v0.2": _series_for_kind(FINAL_V02_CEDA, ef_kind),
            "v0.3": _series_for_kind(FINAL_V03_CEDA, ef_kind),
        }
        fig, ax = plt.subplots(figsize=(11, 6.5))
        kind_label = EF_KIND_LABEL[ef_kind]
        overlay_pct_diff_histogram(
            ax,
            series_by_label,
            xlabel="EF change vs CEDA v0 baseline (%)",
            title=f"{kind_label} per-sector % diff vs CEDA v0 — v0.2 vs v0.3",
        )
        fig.tight_layout()
        out = out_dir / f"overlay_final_ceda_{ef_kind}.png"
        save_and_close(fig, out)
        print(f"Wrote: {out}")

    series_by_label = {
        "v0.2": _useeio_purchaser_series_percent(FINAL_V02_USEEIO),
        "v0.3": _useeio_purchaser_series_percent(FINAL_V03_USEEIO),
    }
    fig, ax = plt.subplots(figsize=(11, 6.5))
    overlay_pct_diff_histogram(
        ax,
        series_by_label,
        xlabel="EF change vs USEEIO baseline (purchaser, %)",
        title=f"{EF_KIND_LABEL['N']} per-sector % diff vs USEEIO — v0.2 vs v0.3",
    )
    fig.tight_layout()
    out = out_dir / "overlay_final_useeio_N.png"
    save_and_close(fig, out)
    print(f"Wrote: {out}")


def _plot_bly_pair(
    out_dir: Path,
    *,
    bly_group_small_threshold: float,
    bly_max_sectors: int,
) -> None:
    runs = (
        ("v0.2", FINAL_V02_CEDA, "[GROUP: v0.2 FINAL vs CEDA v0] BLy net change by sector"),
        ("v0.3", FINAL_V03_CEDA, "[GROUP: v0.3 FINAL vs CEDA v0] BLy net change by sector"),
    )
    fig, axes = plt.subplots(1, 2, figsize=(2 * 7.5, bly_figsize(bly_max_sectors)[1]))
    for ax, (version, sheet_id, title) in zip(axes, runs, strict=True):
        bly_frame = build_sector_stack_frame(
            load_tab(sheet_id, TAB_BLY),
            group_small_threshold=bly_group_small_threshold,
            max_sectors=bly_max_sectors,
        )
        plot_stacked_net_change(
            ax,
            bly_frame,
            title=title,
            ylabel="Gross change (MMT CO2e)",
        )
    fig.tight_layout()
    out = out_dir / "bly_final_v02_v03_ceda.png"
    save_and_close(fig, out)
    print(f"Wrote: {out}")


@click.command()
@click.option(
    "--out-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=OUT_DIR,
    show_default=True,
)
@click.option("--skip-progression", is_flag=True)
@click.option("--skip-overlay", is_flag=True)
@click.option("--skip-bly", is_flag=True)
@click.option("--bly-group-small-threshold", type=float, default=3.0, show_default=True)
@click.option("--bly-max-sectors", type=int, default=0, show_default=True)
def main(
    out_dir: Path,
    skip_progression: bool,
    skip_overlay: bool,
    skip_bly: bool,
    bly_group_small_threshold: float,
    bly_max_sectors: int,
) -> None:
    setup_mpl(font_size=13)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not skip_progression:
        _plot_v03_progressions(out_dir)
    if not skip_overlay:
        _plot_final_overlays(out_dir)
    if not skip_bly:
        _plot_bly_pair(
            out_dir,
            bly_group_small_threshold=bly_group_small_threshold,
            bly_max_sectors=bly_max_sectors,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
