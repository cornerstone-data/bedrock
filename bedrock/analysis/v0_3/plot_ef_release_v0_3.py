"""Release EF progression plots for the v0.3 diagnostics refresh.

Uses existing diagnostics Sheets on Drive (no re-dispatch). Figures:

- v0.3 progression grids (CEDA 7-step N+D; USEEIO 7-step N purchaser + D)
- FINAL v0.2 vs v0.3 overlays vs CEDA v0 (N and D)
- FINAL v0.2 vs v0.3 overlay vs USEEIO purchaser (N)
- BLy pair: FINAL v0.2 + v0.3 (CEDA baseline runs only)

Usage:
    uv run python -m bedrock.analysis.v0_3.plot_ef_release_v0_3
    uv run python -m bedrock.analysis.v0_3.plot_ef_release_v0_3 --compare-to v0.2
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.utils.validation.analysis.bly_plots import (
    TAB_BLY,
    build_sector_stack_frame,
)
from bedrock.utils.validation.analysis.diagnostics_plots import bly_figsize
from bedrock.utils.validation.analysis.ef_hist_panels import (
    EF_KIND_LABEL,
    draw_per_sector_pct_hist_panel,
)
from bedrock.utils.validation.analysis.ef_progression import (
    pct_fractions_useeio_purchaser_vs_v0,
    pct_fractions_vs_baseline_sheet,
    pct_fractions_vs_v0,
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
    V02_FINAL_CEDA,
    V02_FINAL_USEEIO,
    V03_CEDA_STEPS,
    V03_USEEIO_STEPS,
    ProgressionSheet,
)

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent / "output" / "release_v0_3"

CEDA_BAR = "#1f77b4"
USEEIO_BAR = "#ff7f0e"
PANEL_FONT_SCALE = 0.85

CompareTo = Literal["v0", "v0.2"]

FINAL_V02_CEDA = V02_FINAL_CEDA.sheet_id
FINAL_V03_CEDA = V03_CEDA_STEPS[-1].sheet_id
FINAL_V02_USEEIO = V02_FINAL_USEEIO.sheet_id
FINAL_V03_USEEIO = V03_USEEIO_STEPS[-1].sheet_id

V02_BASELINE_YEAR = 2023


@dataclass(frozen=True)
class PanelLoad:
    fractions: np.ndarray
    inflation_applied: bool = False


def _ref_2023_sheet_id(steps: tuple[ProgressionSheet, ...]) -> str | None:
    for step in steps:
        if "umd_2023_ghgia" in step.config_name:
            return step.sheet_id
    return steps[0].sheet_id if steps else None


def _panel_title(step_label: str, *, inflation_applied: bool) -> str:
    if inflation_applied:
        return f"{step_label} [+1yr infl]"
    return step_label


def _loader_ceda_v0(ef_kind: str) -> Callable[[str], PanelLoad]:
    def load(sheet_id: str) -> PanelLoad:
        return PanelLoad(pct_fractions_vs_v0(sheet_id, ef_kind))

    return load


def _loader_useeio_purchaser_v0() -> Callable[[str], PanelLoad]:
    def load(sheet_id: str) -> PanelLoad:
        return PanelLoad(pct_fractions_useeio_purchaser_vs_v0(sheet_id))

    return load


def _loader_vs_v02(
    baseline_sheet_id: str,
    ef_kind: str,
    *,
    ref_2023_sheet_id: str | None,
    prefer_purchaser: bool = False,
) -> Callable[[str], PanelLoad]:
    def load(sheet_id: str) -> PanelLoad:
        fractions, inflation_applied = pct_fractions_vs_baseline_sheet(
            sheet_id,
            baseline_sheet_id,
            ef_kind,
            ref_2023_sheet_id=ref_2023_sheet_id,
            baseline_year=V02_BASELINE_YEAR,
            prefer_purchaser=prefer_purchaser,
        )
        return PanelLoad(fractions, inflation_applied=inflation_applied)

    return load


def _useeio_purchaser_series_percent(sheet_id: str) -> pd.Series[float]:
    return pd.Series(pct_fractions_useeio_purchaser_vs_v0(sheet_id) * 100.0)


def _plot_progression_grid(
    steps: tuple[ProgressionSheet, ...],
    *,
    group_title: str,
    out_path: Path,
    bar_color: str,
    panel_loader: Callable[[str], PanelLoad],
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
        loaded = panel_loader(step.sheet_id)
        title = _panel_title(
            step.step_label, inflation_applied=loaded.inflation_applied
        )
        draw_per_sector_pct_hist_panel(
            ax,
            loaded.fractions,
            title=title,
            color=bar_color,
            font_scale=PANEL_FONT_SCALE,
        )
        ymax = max(ymax, ax.get_ylim()[1])
    for i in range(n):
        flat[i].set_ylim(0, ymax)
    for ax in flat[n:]:
        ax.axis("off")
    fig.suptitle(group_title, fontsize=14, y=1.02)
    fig.tight_layout()
    save_and_close(fig, out_path)
    print(f"Wrote: {out_path}")


def _baseline_label(compare_to: CompareTo) -> str:
    return "CEDA-US (v0)" if compare_to == "v0" else "v0.2 FINAL"


def _compare_slug(compare_to: CompareTo) -> str:
    return "" if compare_to == "v0" else "_vs_v02"


def _plot_v03_progressions(out_dir: Path, compare_to: CompareTo) -> None:
    slug = _compare_slug(compare_to)
    baseline_label = _baseline_label(compare_to)

    ceda_ref_2023 = _ref_2023_sheet_id(V03_CEDA_STEPS)
    useeio_ref_2023 = _ref_2023_sheet_id(V03_USEEIO_STEPS)

    for ef_kind in ("N", "D"):
        if compare_to == "v0":
            loader = _loader_ceda_v0(ef_kind)
        else:
            loader = _loader_vs_v02(
                FINAL_V02_CEDA,
                ef_kind,
                ref_2023_sheet_id=ceda_ref_2023,
            )

        _plot_progression_grid(
            V03_CEDA_STEPS,
            group_title=(
                f"[GROUP: v0.3 · baseline: {baseline_label}] per-sector {ef_kind} % diff, "
                "by release step"
            ),
            out_path=out_dir / f"progression_v03_ceda_{ef_kind}{slug}.png",
            bar_color=CEDA_BAR,
            panel_loader=loader,
        )

    if compare_to == "v0":
        useeio_n_loader = _loader_useeio_purchaser_v0()
    else:
        useeio_n_loader = _loader_vs_v02(
            FINAL_V02_USEEIO,
            "N",
            ref_2023_sheet_id=useeio_ref_2023,
            prefer_purchaser=True,
        )

    _plot_progression_grid(
        V03_USEEIO_STEPS,
        group_title=(
            f"[GROUP: v0.3 · baseline: {'USEEIO (purchaser)' if compare_to == 'v0' else 'v0.2 FINAL (purchaser)'}] "
            "per-sector N % diff, by release step"
        ),
        out_path=out_dir / f"progression_v03_useeio_N{slug}.png",
        bar_color=USEEIO_BAR,
        panel_loader=useeio_n_loader,
    )

    if compare_to == "v0":
        useeio_d_loader = _loader_ceda_v0("D")
        d_subtitle = "by release step (producer / CEDA v0)"
    else:
        useeio_d_loader = _loader_vs_v02(
            FINAL_V02_CEDA,
            "D",
            ref_2023_sheet_id=ceda_ref_2023,
        )
        d_subtitle = "by release step (producer / vs v0.2 FINAL)"

    _plot_progression_grid(
        V03_USEEIO_STEPS,
        group_title=(
            f"[GROUP: v0.3 · baseline: {baseline_label}] per-sector D % diff, "
            f"{d_subtitle}"
        ),
        out_path=out_dir / f"progression_v03_useeio_D{slug}.png",
        bar_color=USEEIO_BAR,
        panel_loader=useeio_d_loader,
    )


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
        (
            "v0.2",
            FINAL_V02_CEDA,
            "[GROUP: v0.2 FINAL vs CEDA v0] BLy net change by sector",
        ),
        (
            "v0.3",
            FINAL_V03_CEDA,
            "[GROUP: v0.3 FINAL vs CEDA v0] BLy net change by sector",
        ),
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
@click.option(
    "--compare-to",
    type=click.Choice(["v0", "v0.2"], case_sensitive=False),
    default="v0",
    show_default=True,
    help=(
        "Baseline for progression panels: in-sheet diff vs v0 (default) or "
        "cross-sheet diff vs FINAL v0.2 (with 2023→2024 inflation on 2024 steps)."
    ),
)
@click.option("--skip-progression", is_flag=True)
@click.option("--skip-overlay", is_flag=True)
@click.option("--skip-bly", is_flag=True)
@click.option("--bly-group-small-threshold", type=float, default=3.0, show_default=True)
@click.option("--bly-max-sectors", type=int, default=0, show_default=True)
def main(
    out_dir: Path,
    compare_to: str,
    skip_progression: bool,
    skip_overlay: bool,
    skip_bly: bool,
    bly_group_small_threshold: float,
    bly_max_sectors: int,
) -> None:
    setup_mpl(font_size=13)
    out_dir.mkdir(parents=True, exist_ok=True)
    compare: CompareTo = "v0.2" if compare_to.lower() == "v0.2" else "v0"

    if not skip_progression:
        _plot_v03_progressions(out_dir, compare)
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
