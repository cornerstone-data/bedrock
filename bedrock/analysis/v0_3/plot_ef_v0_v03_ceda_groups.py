"""Stacked G1a→G1b→G2→G3 EF histograms for wholesale v0→v0.3 CEDA progression.

Writes sequential panels (each vs the prior group endpoint) and cumulative
panels (each vs CEDA v0), plus an optional FINAL vs CEDA v0 overlay. Uses
existing diagnostics Sheets only.

Usage:
    uv run python -m bedrock.analysis.v0_3.plot_ef_v0_v03_ceda_groups
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.utils.validation.analysis.ef_hist_panels import (
    EF_KIND_LABEL,
    draw_per_sector_pct_hist_panel,
)
from bedrock.utils.validation.analysis.ef_progression import (
    pct_fractions_producer_vs_old,
    pct_fractions_stacked_group,
)
from bedrock.utils.validation.analysis.plotting import (
    overlay_pct_diff_histogram,
    save_and_close,
    setup_mpl,
)
from bedrock.utils.validation.analysis.release_v0_3_progression import ProgressionSheet
from bedrock.utils.validation.analysis.release_v0_v03_ceda_groups import (
    FINAL_V03_CEDA,
    V0_V03_CEDA_STACK_SHEETS,
)

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent / "output" / "release_v0_v03_groups"

USEEIO_BAR = "#ff7f0e"
CEDA_BAR = "#1f77b4"
PANEL_FONT_SCALE = 0.9

_STACK_PANEL_TITLES = (
    "GHG reconciliation",
    "Waste disaggregation",
    "IO year adjustments",
    "US data update",
)


@dataclass(frozen=True)
class PanelLoad:
    fractions: np.ndarray
    inflation_applied: bool = False


def _panel_title(step_label: str, *, inflation_applied: bool) -> str:
    if inflation_applied:
        return f"{step_label} [+1yr infl]"
    return step_label


def _prior_sheet_id(steps: Sequence[ProgressionSheet], index: int) -> str | None:
    if index == 0:
        return None
    return steps[index - 1].sheet_id


def _loader_stacked(
    steps: Sequence[ProgressionSheet],
    ef_kind: str,
    *,
    prefer_purchaser: bool,
    ref_2023_sheet_id: str | None,
) -> Callable[[int], PanelLoad]:
    def load(index: int) -> PanelLoad:
        step = steps[index]
        prior = _prior_sheet_id(steps, index)
        fractions, inflation_applied = pct_fractions_stacked_group(
            step.sheet_id,
            prior,
            ef_kind,
            ref_2023_sheet_id=ref_2023_sheet_id,
            prefer_purchaser=prefer_purchaser,
        )
        return PanelLoad(fractions, inflation_applied=inflation_applied)

    return load


def _loader_cumulative_vs_baseline(
    steps: Sequence[ProgressionSheet],
    ef_kind: str,
) -> Callable[[int], PanelLoad]:
    """Each panel: in-sheet % diff vs CEDA v0 on that step's sheet."""

    def load(index: int) -> PanelLoad:
        step = steps[index]
        return PanelLoad(pct_fractions_producer_vs_old(step.sheet_id, ef_kind))

    return load


def _plot_group_grid(
    steps: Sequence[ProgressionSheet],
    *,
    group_title: str,
    out_path: Path,
    bar_color: str,
    panel_loader: Callable[[int], PanelLoad],
) -> None:
    n = len(steps)
    cols = min(3, n)
    rows = math.ceil(n / cols)
    fig, axes = plt.subplots(
        rows, cols, figsize=(5.5 * cols, 4.8 * rows), squeeze=False
    )
    flat = list(axes.flat)
    ymax = 0.0
    for i, step in enumerate(steps):
        ax = flat[i]
        loaded = panel_loader(i)
        title = _panel_title(
            _STACK_PANEL_TITLES[i],
            inflation_applied=loaded.inflation_applied,
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
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    save_and_close(fig, out_path)
    print(f"Wrote: {out_path}")


def _plot_final_ceda_overlays(out_dir: Path) -> None:
    """Cumulative FINAL vs CEDA v0 (producer) on the CEDA-baseline FINAL sheet."""
    for ef_kind in ("N", "D"):
        series_by_label = {
            "FINAL v0.3": pd.Series(
                pct_fractions_producer_vs_old(FINAL_V03_CEDA.sheet_id, ef_kind) * 100.0
            ),
        }
        fig, ax = plt.subplots(figsize=(11, 6.5))
        kind_label = EF_KIND_LABEL[ef_kind]
        overlay_pct_diff_histogram(
            ax,
            series_by_label,
            xlabel="EF change vs CEDA v0 baseline (producer, %)",
            title=(
                f"{kind_label} per-sector % diff vs CEDA v0 — "
                "shipped v0.3 (cumulative, producer)"
            ),
        )
        fig.tight_layout()
        out = out_dir / f"overlay_final_ceda_{ef_kind}_cumulative.png"
        save_and_close(fig, out)
        print(f"Wrote: {out}")


@click.command()
@click.option(
    "--out-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=OUT_DIR,
    show_default=True,
)
@click.option("--skip-overlay", is_flag=True)
def main(out_dir: Path, skip_overlay: bool) -> None:
    setup_mpl(font_size=13)
    out_dir.mkdir(parents=True, exist_ok=True)
    steps = V0_V03_CEDA_STACK_SHEETS
    ref_2023_sheet_id: str | None = None
    prefer_purchaser = False

    for ef_kind in ("N", "D"):
        bar_color = CEDA_BAR if ef_kind == "N" else USEEIO_BAR
        _plot_group_grid(
            steps,
            group_title=(
                f"[v0→v0.3 CEDA · stacked groups] per-sector {ef_kind} % diff "
                "(producer, sequential comparisons)"
            ),
            out_path=out_dir / f"progression_v0_v03_ceda_groups_{ef_kind}.png",
            bar_color=bar_color,
            panel_loader=_loader_stacked(
                steps,
                ef_kind,
                prefer_purchaser=prefer_purchaser,
                ref_2023_sheet_id=ref_2023_sheet_id,
            ),
        )
        _plot_group_grid(
            steps,
            group_title=(
                f"[v0→v0.3 CEDA · stacked groups] per-sector {ef_kind} % diff "
                "(producer, cumulative vs CEDA v0)"
            ),
            out_path=(
                out_dir / f"progression_v0_v03_ceda_groups_{ef_kind}_cumulative.png"
            ),
            bar_color=bar_color,
            panel_loader=_loader_cumulative_vs_baseline(steps, ef_kind),
        )

    if not skip_overlay:
        _plot_final_ceda_overlays(out_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
