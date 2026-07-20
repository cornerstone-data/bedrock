"""Plot N/D EF diagnostics for electricity disagg steps vs Cornerstone v0.2."""

from __future__ import annotations

import logging
import math
import re
from pathlib import Path

import click
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from bedrock.analysis.electricity_disagg_diagnostics.local_data import (
    EF_TABS,
    REQUIRED_TABS,
    seed_cache_from_local_dir,
)
from bedrock.analysis.electricity_disagg_diagnostics.manifest import load_manifest
from bedrock.analysis.electricity_disagg_diagnostics.paths import (
    LOCAL_DATA_DIR,
    OUT_DIR,
    ensure_dirs,
)
from bedrock.analysis.electricity_disagg_diagnostics.vs_footing_frames import (
    DroppedSector,
    VsFootingFrames,
    format_drop_footnote,
    vs_footing_ef_frames,
)
from bedrock.utils.validation.analysis.diagnostics_plots import (
    _beyond_20_text,
    plot_ef_abs_change_histogram,
    plot_ef_pct_change_vs_abs_change,
    plot_ef_pct_change_vs_ef_size,
    plot_ef_perc_diff_histogram,
    plot_n_perc_diff_histogram,
)
from bedrock.utils.validation.analysis.plotting import (
    DEFAULT_XLIM,
    apply_axis_fonts,
    percent_histogram,
    save_and_close,
    setup_mpl,
)

logger = logging.getLogger(__name__)

SUPTITLE = 'vs Cornerstone v0.2 footing'
EF_OUT_DIR = OUT_DIR / 'ef'


def _slug(config_name: str) -> str:
    return config_name.removeprefix('2025_usa_cornerstone_v0_2_').removeprefix(
        '2025_usa_cornerstone_'
    )


def _annotate_figure(fig: Figure, drops: list[DroppedSector]) -> None:
    fig.suptitle(SUPTITLE, fontsize=14, y=1.02)
    footnote = format_drop_footnote(drops)
    if footnote:
        fig.subplots_adjust(bottom=0.14)
        fig.text(
            0.01,
            0.01,
            footnote,
            fontsize=8,
            ha='left',
            va='bottom',
            wrap=True,
            family='monospace',
        )


def _save_suite(frames: VsFootingFrames, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    specs: list[tuple[str, Figure]] = [
        (
            'ef_perc_diff_histogram.png',
            plot_ef_perc_diff_histogram(frames.df_n, frames.df_d, frames.df_sig),
        ),
        (
            'ef_pct_change_vs_abs_change.png',
            plot_ef_pct_change_vs_abs_change(frames.df_scatter),
        ),
        (
            'ef_abs_change_histogram.png',
            plot_ef_abs_change_histogram(frames.df_scatter),
        ),
        (
            'ef_n_perc_diff_histogram.png',
            plot_n_perc_diff_histogram(frames.df_n),
        ),
        (
            'ef_pct_change_vs_ef_size.png',
            plot_ef_pct_change_vs_ef_size(frames.df_scatter),
        ),
    ]
    for name, fig in specs:
        _annotate_figure(fig, frames.drops)
        save_and_close(fig, out_dir / name)


def _panel_footnote_on_ax(ax: Axes, drops: list[DroppedSector]) -> None:
    footnote = format_drop_footnote(drops)
    if not footnote:
        return
    ax.text(
        0.0,
        -0.18,
        footnote,
        transform=ax.transAxes,
        fontsize=7,
        ha='left',
        va='top',
        family='monospace',
        wrap=True,
    )


def write_panel_pngs(
    step_frames: list[tuple[str, VsFootingFrames]],
    out_dir: Path,
) -> None:
    """Three-panel N and D histograms (realloc / 3-way / mixed) vs footing."""
    out_dir.mkdir(parents=True, exist_ok=True)
    n = len(step_frames)
    ncols = min(3, n)
    nrows = math.ceil(n / ncols)

    for ef_kind, col in (('N', 'N_perc_diff'), ('D', 'D_perc_diff')):
        fig, axes = plt.subplots(
            nrows, ncols, figsize=(10.0 * ncols, 9.0 * nrows), squeeze=False
        )
        flat = list(axes.flat)
        for i, (label, frames) in enumerate(step_frames):
            ax = flat[i]
            df = frames.df_n if ef_kind == 'N' else frames.df_d
            percent_histogram(
                ax,
                df[col].dropna() * 100,
                xlim=DEFAULT_XLIM,
                xlabel='Percentage Diff (%)',
                ylabel='Count',
                title=label,
                text_box=_beyond_20_text(df, col),
                text_box_fontsize=8,
                legend_fontsize=10,
            )
            apply_axis_fonts(ax)
            _panel_footnote_on_ax(ax, frames.drops)
        for ax in flat[n:]:
            ax.axis('off')
        # Reallocation keeps its own y-scale (tall spike near 0%). Later steps
        # share a y-max so their wider distributions stay readable. X-lim is
        # already shared via DEFAULT_XLIM in percent_histogram.
        if n >= 2:
            later_ymax = max(flat[i].get_ylim()[1] for i in range(1, n))
            for i in range(1, n):
                flat[i].set_ylim(0, later_ymax)

        kind_label = 'total EF (N)' if ef_kind == 'N' else 'direct EF (D)'
        fig.suptitle(
            f'{kind_label} per-sector % diff {SUPTITLE} — electricity disagg steps',
            fontsize=16,
        )
        fig.tight_layout(rect=(0, 0.06, 1, 0.96))
        out = out_dir / f'ef_panels_vs_v0_2_{ef_kind}.png'
        save_and_close(fig, out)


def run_plot_ef(*, local_dir: Path) -> Path:
    ensure_dirs()
    setup_mpl(font_size=13)
    manifest = load_manifest()
    print(f'=== Import local workbooks (EF tabs) from {local_dir} ===')
    seed_cache_from_local_dir(manifest, local_dir, tabs=REQUIRED_TABS + EF_TABS)

    footing_id = manifest.footing.sheet_id
    step_frames: list[tuple[str, VsFootingFrames]] = []
    for step in manifest.steps:
        slug = _slug(step.config)
        label = re.sub(r'\s+', ' ', step.label).strip()
        print(f'=== Plot {label} ({slug}) vs v0.2 ===')
        frames = vs_footing_ef_frames(step.sheet_id, footing_id)
        if frames.drops:
            print('  drops:', format_drop_footnote(frames.drops).replace('\n', '; '))
        _save_suite(frames, EF_OUT_DIR / slug)
        step_frames.append((label, frames))

    panel_dir = EF_OUT_DIR / 'panel'
    print('=== Panel N/D vs v0.2 ===')
    write_panel_pngs(step_frames, panel_dir)
    return EF_OUT_DIR


@click.command()
@click.option(
    '--local-dir',
    type=click.Path(file_okay=False, path_type=Path),
    default=LOCAL_DATA_DIR,
    show_default=True,
)
def main(local_dir: Path) -> None:
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    out = run_plot_ef(local_dir=local_dir)
    print(f'Done. EF plots under: {out}')


if __name__ == '__main__':
    main()
