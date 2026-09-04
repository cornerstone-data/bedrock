"""Two-row × three-panel D/N percent-diff histograms for slides 3 and 5."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from PIL import Image

from bedrock.analysis.electricity.current.diagnostics.deck.data import (
    ImplBundle,
    StepSnapshot,
    ef_kg_per_usd,
)
from bedrock.analysis.electricity.current.diagnostics.deck.pairs import (
    GENERATION_SECTOR,
    HIST_PANEL_TITLE,
    IMPLEMENTATIONS,
    ImplId,
    Pair,
    StepId,
)
from bedrock.analysis.electricity.current.diagnostics.ef_comparison.vs_footing_frames import (
    DroppedSector,
    format_drop_footnote,
)
from bedrock.analysis.electricity.historical.original_vs_eia_anchored_deck.paths import (
    FIGURES_DIR as _PACK_FIGURES_DIR,
)
from bedrock.analysis.electricity.historical.original_vs_eia_anchored_deck.paths import (
    PANEL_PNG as FROZEN_PANEL_PNG,
)
from bedrock.utils.validation.analysis.diagnostics_plots import _beyond_20_text
from bedrock.utils.validation.analysis.plotting import (
    DEFAULT_XLIM,
    apply_axis_fonts,
    percent_histogram,
    save_and_close,
    setup_mpl,
)

MIXED_UNITS_DROP = 'mixed units are incompatible for plotting (kg/MWh vs kg/USD)'
NOT_IN_FOOTING = 'not present in footing (cannot compare on a shared sector code)'
ONLY_IN_FOOTING = 'present only in footing (aggregate electricity; not in this step)'

FIGURES_DIR = _PACK_FIGURES_DIR


def _sector_names(index: pd.Index) -> pd.Series:
    try:
        from bedrock.utils.validation.diagnostics_helpers import (  # noqa: PLC0415
            get_aligned_sector_desc,
        )

        desc = get_aligned_sector_desc()
    except Exception:
        desc = {}
    return pd.Series({str(s): desc.get(str(s), str(s)) for s in index})


def perc_frame(step: pd.Series, base: pd.Series) -> pd.DataFrame:
    """``(step − base) / |base|`` on the inner-joined sector index."""
    a = step.astype(float).copy()
    a.index = a.index.map(str)
    b = base.astype(float).copy()
    b.index = b.index.map(str)
    idx = a.index.intersection(b.index)
    a = a.reindex(idx)
    b = b.reindex(idx)
    denom = b.abs()
    perc = (a - b) / denom
    perc = perc.where(denom != 0)
    names = _sector_names(idx)
    return pd.DataFrame(
        {
            'sector': list(idx),
            'sector_name': names.reindex(idx).astype(str).to_numpy(),
            'perc_diff': perc.to_numpy(),
        }
    )


def _apply_electricity_drops(
    frame: pd.DataFrame,
    drops: list[DroppedSector],
) -> pd.DataFrame:
    drop_sectors = {d.sector for d in drops}
    return frame[~frame['sector'].isin(drop_sectors)].reset_index(drop=True)


def footing_drops(
    step: pd.Series,
    footing: pd.Series,
    *,
    mixed_step: bool,
) -> list[DroppedSector]:
    """Same electricity-sector drop rules as vs-footing diagnostics plots."""
    step_secs = set(step.index.map(str))
    foot_secs = set(footing.index.map(str))
    drops: list[DroppedSector] = []
    seen: set[str] = set()
    candidates = {'221100', '221110', '221121', '221122'}
    for sector in sorted(candidates):
        if sector in seen:
            continue
        if mixed_step and sector == GENERATION_SECTOR and sector in step_secs:
            drops.append(DroppedSector(sector, MIXED_UNITS_DROP))
            seen.add(sector)
            continue
        if sector in step_secs and sector not in foot_secs:
            drops.append(DroppedSector(sector, NOT_IN_FOOTING))
            seen.add(sector)
            continue
        if sector in foot_secs and sector not in step_secs:
            drops.append(DroppedSector(sector, ONLY_IN_FOOTING))
            seen.add(sector)
    return drops


def _usd_or_native(step: StepSnapshot, kind: str) -> pd.Series | None:
    src = step.d if kind == 'D' else step.n
    if src is None:
        return None
    if step.mixed:
        return src.astype(float)
    return ef_kg_per_usd(src, mixed=False, c_col=step.c_col)


def pairwise_frame(
    left: StepSnapshot,
    right: StepSnapshot,
    kind: str,
) -> tuple[pd.DataFrame, list[DroppedSector]]:
    a = _usd_or_native(left, kind)
    b = _usd_or_native(right, kind)
    if a is None or b is None:
        return pd.DataFrame(columns=['sector', 'sector_name', 'perc_diff']), []
    # Both mixed or both monetary: generation is comparable; no unit drop.
    frame = perc_frame(a, b)
    return frame, []


def vs_footing_frame(
    step: StepSnapshot,
    footing: StepSnapshot,
    kind: str,
) -> tuple[pd.DataFrame, list[DroppedSector]]:
    a = _usd_or_native(step, kind)
    b = _usd_or_native(footing, kind)
    if a is None or b is None:
        return pd.DataFrame(columns=['sector', 'sector_name', 'perc_diff']), []
    drops = footing_drops(a, b, mixed_step=step.mixed)
    frame = _apply_electricity_drops(perc_frame(a, b), drops)
    return frame, drops


def _panel_df(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out['perc_diff'] = pd.to_numeric(out['perc_diff'], errors='coerce')
    return out


def frozen_panel_png(impl_id: ImplId, kind: str) -> Path | None:
    """Return the published 1×3 panel for this implementation, if present."""
    name = FROZEN_PANEL_PNG.get((impl_id, kind))
    if name is None:
        return None
    path = FIGURES_DIR / name
    return path if path.is_file() else None


def stack_panel_pngs(paths: list[Path], out: Path) -> Path:
    """Stack 1×3 panel PNGs top-to-bottom without redrawing them."""
    images = [Image.open(p).convert('RGBA') for p in paths]
    width = max(im.width for im in images)
    aligned = []
    for im in images:
        if im.width == width:
            aligned.append(im)
            continue
        height = max(1, round(im.height * width / im.width))
        aligned.append(im.resize((width, height), Image.Resampling.LANCZOS))
    canvas = Image.new(
        'RGBA',
        (width, sum(im.height for im in aligned)),
        (255, 255, 255, 255),
    )
    y = 0
    for im in aligned:
        canvas.paste(im, (0, y))
        y += im.height
    out.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(out)
    return out


def _draw_hist_panel(
    ax: Axes,
    pair: Pair,
    bundle: ImplBundle,
    impl_id: ImplId,
    top: ImplBundle,
    bottom: ImplBundle,
    step_id: StepId,
    kind: str,
    *,
    footnote_y: float = -0.22,
    text_box_fontsize: int = 7,
    legend_fontsize: int = 8,
) -> None:
    title = HIST_PANEL_TITLE[step_id]
    frame, drops = _panel_data(pair, bundle, impl_id, top, bottom, step_id, kind)
    if frame.empty or frame['perc_diff'].dropna().empty:
        ax.set_title(title)
        ax.text(
            0.5,
            0.5,
            'no data',
            ha='center',
            va='center',
            transform=ax.transAxes,
        )
        ax.set_xticks([])
        ax.set_yticks([])
        return
    percent_histogram(
        ax,
        frame['perc_diff'].dropna() * 100,
        xlim=DEFAULT_XLIM,
        xlabel='Percentage Diff (%)',
        ylabel='Count',
        title=title,
        text_box=_beyond_20_text(frame, 'perc_diff'),
        text_box_fontsize=text_box_fontsize,
        legend_fontsize=legend_fontsize,
    )
    apply_axis_fonts(ax)
    footnote = format_drop_footnote(drops)
    if footnote:
        ax.text(
            0.0,
            footnote_y,
            footnote,
            transform=ax.transAxes,
            fontsize=6,
            ha='left',
            va='top',
            family='monospace',
            wrap=True,
        )


def _footing_suptitle(pair: Pair, impl_id: ImplId, kind: str) -> str:
    kind_label = 'direct EF (D)' if kind == 'D' else 'total EF (N)'
    if pair.hist_baseline == 'peer':
        vs = 'vs Cornerstone v0.3 production (non-disagg)'
    elif IMPLEMENTATIONS[impl_id].footing_label == 'v0.2':
        vs = 'vs Cornerstone v0.2 footing'
    else:
        vs = 'vs Cornerstone v0.3.1 electricity footing'
    return f'{kind_label} per-sector % diff {vs} — electricity disagg steps'


def render_one_row_hist(
    pair: Pair,
    bundle: ImplBundle,
    impl_id: ImplId,
    kind: str,
    top: ImplBundle,
    bottom: ImplBundle,
) -> Figure:
    """Single 1×3 vs-footing row, matching ``plot_ef.write_panel_pngs`` layout."""
    setup_mpl(font_size=13)
    fig, axes = plt.subplots(1, 3, figsize=(30.0, 9.0), squeeze=False)
    for c, step_id in enumerate(pair.hist_steps):
        _draw_hist_panel(
            axes[0][c],
            pair,
            bundle,
            impl_id,
            top,
            bottom,
            step_id,
            kind,
            footnote_y=-0.18,
            text_box_fontsize=8,
            legend_fontsize=10,
        )
    later_ymax = max(axes[0][i].get_ylim()[1] for i in range(1, 3))
    for i in range(1, 3):
        axes[0][i].set_ylim(0, later_ymax)
    fig.suptitle(_footing_suptitle(pair, impl_id, kind), fontsize=16)
    fig.tight_layout(rect=(0, 0.06, 1, 0.96))
    return fig


def render_hist_figure(
    pair: Pair,
    top: ImplBundle,
    bottom: ImplBundle,
    kind: str,
) -> Figure:
    setup_mpl(font_size=11)
    fig, axes = plt.subplots(2, 3, figsize=(16.0, 9.2), squeeze=False)
    row_bundles = (top, bottom)
    row_ids = (pair.top, pair.bottom)
    for r, (bundle, impl_id) in enumerate(zip(row_bundles, row_ids)):
        impl = IMPLEMENTATIONS[impl_id]
        for c, step_id in enumerate(pair.hist_steps):
            _draw_hist_panel(
                axes[r][c],
                pair,
                bundle,
                impl_id,
                top,
                bottom,
                step_id,
                kind,
            )
        axes[r][0].set_ylabel(f'{impl.title}\nCount')
    kind_label = 'direct EF (D)' if kind == 'D' else 'total EF (N)'
    fig.suptitle(f'{kind_label} per-sector % diff', fontsize=16)
    fig.tight_layout(rect=(0, 0.04, 1, 0.95))
    return fig


def _peer_baseline(
    pair: Pair,
    top: ImplBundle,
    bottom: ImplBundle,
) -> StepSnapshot | None:
    if pair.hist_baseline != 'peer':
        return None
    for bundle in (top, bottom):
        if IMPLEMENTATIONS[bundle.impl_id].schema != 'aggregate':
            continue
        snap = bundle.steps.get('footing')
        if snap is None and bundle.steps:
            snap = next(iter(bundle.steps.values()))
        if snap is not None:
            return snap
    return None


def _panel_data(
    pair: Pair,
    bundle: ImplBundle,
    impl_id: ImplId,
    top: ImplBundle,
    bottom: ImplBundle,
    step_id: StepId,
    kind: str,
) -> tuple[pd.DataFrame, list[DroppedSector]]:
    step = bundle.steps.get(step_id)
    if step is None:
        return pd.DataFrame(columns=['sector', 'sector_name', 'perc_diff']), []
    if pair.hist_mode == 'pairwise':
        top_step = top.steps.get(step_id)
        bottom_step = bottom.steps.get(step_id)
        if top_step is None or bottom_step is None:
            return pd.DataFrame(columns=['sector', 'sector_name', 'perc_diff']), []
        if impl_id == pair.top:
            frame, drops = pairwise_frame(top_step, bottom_step, kind)
        else:
            frame, drops = pairwise_frame(bottom_step, top_step, kind)
        return _panel_df(frame), drops
    footing = _peer_baseline(pair, top, bottom) or bundle.steps.get('footing')
    if footing is None:
        return pd.DataFrame(columns=['sector', 'sector_name', 'perc_diff']), []
    frame, drops = vs_footing_frame(step, footing, kind)
    return _panel_df(frame), drops


def write_hist_png(
    pair: Pair,
    top: ImplBundle,
    bottom: ImplBundle,
    kind: str,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if pair.hist_mode == 'vs_footing':
        rows: list[Path] = []
        for impl_id, bundle in ((pair.top, top), (pair.bottom, bottom)):
            frozen = frozen_panel_png(impl_id, kind)
            if frozen is not None:
                rows.append(frozen)
                continue
            row_path = path.parent / f'{pair.key}_{impl_id}_{kind}_row.png'
            fig = render_one_row_hist(pair, bundle, impl_id, kind, top, bottom)
            save_and_close(fig, row_path)
            rows.append(row_path)
        return stack_panel_pngs(rows, path)
    fig = render_hist_figure(pair, top, bottom, kind)
    save_and_close(fig, path)
    return path
