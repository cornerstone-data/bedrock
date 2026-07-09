"""Stacked G1→G2→G3 EF histograms for wholesale v0→v0.3 USEEIO progression.

Three marginal panels (each vs the prior group endpoint) plus an optional
FINAL vs pinned USEEIO overlay. Uses existing diagnostics Sheets only.

Usage:
    uv run python -m bedrock.analysis.v0_3.plot_ef_v0_v03_useeio_groups
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
    pct_fractions_stacked_group,
    pct_fractions_useeio_purchaser_vs_v0,
    pct_fractions_vs_v0,
)
from bedrock.utils.validation.analysis.plotting import (
    overlay_pct_diff_histogram,
    save_and_close,
    setup_mpl,
)
from bedrock.utils.validation.analysis.release_v0_3_progression import ProgressionSheet
from bedrock.utils.validation.analysis.release_v0_v03_useeio_groups import (
    FINAL_V03_USEEIO,
    REF_2023_FOR_INFLATION,
    V0_V03_USEEIO_STACK_SHEETS,
)

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent / "output" / "release_v0_v03_groups"

USEEIO_BAR = "#ff7f0e"
CEDA_BAR = "#1f77b4"
PANEL_FONT_SCALE = 0.9


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


def _plot_final_useeio_overlay(out_dir: Path) -> None:
    series_by_label = {
        "FINAL v0.3": pd.Series(
            pct_fractions_useeio_purchaser_vs_v0(FINAL_V03_USEEIO.sheet_id) * 100.0
        ),
    }
    fig, ax = plt.subplots(figsize=(11, 6.5))
    overlay_pct_diff_histogram(
        ax,
        series_by_label,
        xlabel="EF change vs USEEIO baseline (purchaser, %)",
        title=(
            f"{EF_KIND_LABEL['N']} per-sector % diff vs pinned USEEIO — "
            "shipped v0.3 (cumulative)"
        ),
    )
    fig.tight_layout()
    out = out_dir / "overlay_final_useeio_N_cumulative.png"
    save_and_close(fig, out)
    print(f"Wrote: {out}")


def _plot_final_ceda_overlay(out_dir: Path) -> None:
    for ef_kind in ("N", "D"):
        series_by_label = {
            "FINAL v0.3": pd.Series(
                pct_fractions_vs_v0(FINAL_V03_USEEIO.sheet_id, ef_kind) * 100.0
            ),
        }
        fig, ax = plt.subplots(figsize=(11, 6.5))
        kind_label = EF_KIND_LABEL[ef_kind]
        overlay_pct_diff_histogram(
            ax,
            series_by_label,
            xlabel="EF change vs CEDA v0 baseline (%)",
            title=f"{kind_label} per-sector % diff vs CEDA v0 — shipped v0.3 (cumulative)",
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
    steps = V0_V03_USEEIO_STACK_SHEETS

    _plot_group_grid(
        steps,
        group_title=(
            "[v0→v0.3 USEEIO · stacked groups] per-sector N % diff "
            "(purchaser / pinned USEEIO)"
        ),
        out_path=out_dir / "progression_v0_v03_useeio_groups_N.png",
        bar_color=USEEIO_BAR,
        panel_loader=_loader_stacked(
            steps,
            "N",
            prefer_purchaser=True,
            ref_2023_sheet_id=REF_2023_FOR_INFLATION,
        ),
    )

    for ef_kind in ("N", "D"):
        _plot_group_grid(
            steps,
            group_title=(
                f"[v0→v0.3 USEEIO · stacked groups] per-sector {ef_kind} % diff "
                "(producer / CEDA v0)"
            ),
            out_path=out_dir / f"progression_v0_v03_useeio_groups_{ef_kind}.png",
            bar_color=CEDA_BAR,
            panel_loader=_loader_stacked(
                steps,
                ef_kind,
                prefer_purchaser=False,
                ref_2023_sheet_id=REF_2023_FOR_INFLATION,
            ),
        )

    if not skip_overlay:
        _plot_final_useeio_overlay(out_dir)
        _plot_final_ceda_overlay(out_dir)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
