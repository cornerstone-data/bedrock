"""Multi-panel per-config N/D % diff histograms (one panel per diagnostics Sheet).

Each panel is the standalone-histogram style from ``diagnostics_plots`` — a
percent-diff histogram with median / 0 / ±10 / ±20 reference lines and a
``Beyond ±20%`` top-30 sector text box. Lay several runs side by side (e.g. a
v0.2 baseline panel plus each underlying v0.3 config) to compare where the EF
distribution moves config-by-config. Renders one figure per EF kind (N, D).

Percent-diff columns are read via ``fetch.load_tab`` (which coerces bare-fraction
strings to floats) and ``_normalize_perc_diff`` / ``_parse_pct`` (which also
handle ``"X%"`` strings), so older percent-string runs and newer fraction runs
panel together correctly.

Usage:
    python -m bedrock.utils.validation.analysis.panel_ef_hist \\
        --series "v0.2 (baseline)=1pCSgLD..." \\
        --series "v0.3 step: update inflation factors=1-uMBFx2..." \\
        --out-dir bedrock/utils/validation/v0.3/output [--ncols 3]
"""

from __future__ import annotations

import logging
import math
import re
from pathlib import Path

import click
import matplotlib.pyplot as plt
import pandas as pd

from .diagnostics_plots import (
    _beyond_20_text,
    _drop_old_only,
    _normalize_perc_diff,
    _normalize_schema,
)
from .fetch import load_tab
from .plotting import (
    DEFAULT_XLIM,
    apply_axis_fonts,
    percent_histogram,
    save_and_close,
    setup_mpl,
)

logger = logging.getLogger(__name__)

EF_KIND_LABEL = {'N': 'total EF (N)', 'D': 'direct EF (D)'}
KIND_TAB = {'N': 'N_and_diffs', 'D': 'D_and_diffs'}


def _parse_series(series: tuple[str, ...]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in series:
        if '=' not in item:
            raise click.BadParameter(f"--series must be 'label=sheet_id', got {item!r}")
        label, sheet_id = item.split('=', 1)
        out[label.strip()] = sheet_id.strip()
    return out


def _own_baseline_frame(sheet_id: str, ef_kind: str) -> pd.DataFrame:
    """Per-sector ``{kind}_perc_diff`` vs the sheet's own baseline (CEDA v0)."""
    df = _drop_old_only(_normalize_schema(load_tab(sheet_id, KIND_TAB[ef_kind])))
    return _normalize_perc_diff(df, ef_kind)


def _new_ef(sheet_id: str, ef_kind: str) -> pd.DataFrame:
    """Sector-indexed absolute ``{kind}_new`` (float) + ``sector_name``."""
    col = f'{ef_kind}_new'
    df = _drop_old_only(_normalize_schema(load_tab(sheet_id, KIND_TAB[ef_kind])))
    out = df[['sector', 'sector_name', col]].copy()
    out[col] = pd.to_numeric(out[col], errors='coerce')
    return out


def _vs_baseline_frame(cfg_sheet: str, base_sheet: str, ef_kind: str) -> pd.DataFrame:
    """Per-sector ``{kind}_perc_diff`` of one config vs a chosen baseline sheet.

    Recomputed from absolute EFs as ``(cfg_new - base_new) / |base_new|`` so the
    denominator is the baseline run (e.g. v0.2), not each sheet's stored
    CEDA-v0 comparison. Returns the schema ``_beyond_20_text`` /
    ``percent_histogram`` expect (``sector``, ``sector_name``,
    ``{kind}_perc_diff`` as a fraction).
    """
    col = f'{ef_kind}_new'
    cfg = _new_ef(cfg_sheet, ef_kind)
    base = _new_ef(base_sheet, ef_kind)[['sector', col]].rename(columns={col: '_base'})
    m = cfg.merge(base, on='sector', how='inner')
    pdiff = (m[col] - m['_base']) / m['_base'].abs()
    return pd.DataFrame(
        {
            'sector': m['sector'],
            'sector_name': m['sector_name'],
            f'{ef_kind}_perc_diff': pdiff,
        }
    )


@click.command()
@click.option(
    '--series',
    multiple=True,
    required=True,
    help="Repeatable 'label=sheet_id' pair, one per panel (first = baseline).",
)
@click.option(
    '--out-dir',
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
)
@click.option('--ncols', default=3, show_default=True, help='Panels per row.')
@click.option(
    '--baseline',
    default=None,
    help=(
        "Optional 'label=sheet_id'. When set, each --series config is "
        "recomputed vs this sheet's absolute EFs (e.g. v0.2 as baseline) "
        "instead of each sheet's own CEDA-v0 comparison."
    ),
)
def main(
    series: tuple[str, ...], out_dir: Path, ncols: int, baseline: str | None
) -> None:
    setup_mpl(font_size=13)
    label_to_sheet = _parse_series(series)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_label = base_sheet = None
    if baseline is not None:
        ((base_label, base_sheet),) = _parse_series((baseline,)).items()

    for ef_kind in ('N', 'D'):
        col = f'{ef_kind}_perc_diff'
        frames: list[tuple[str, pd.DataFrame]] = []
        for label, sheet_id in label_to_sheet.items():
            try:
                if base_sheet is not None:
                    frames.append(
                        (label, _vs_baseline_frame(sheet_id, base_sheet, ef_kind))
                    )
                else:
                    frames.append((label, _own_baseline_frame(sheet_id, ef_kind)))
            except Exception as e:  # noqa: BLE001
                logger.warning('Skipping %s for %s: %s', label, ef_kind, e)
        if not frames:
            logger.warning('No data for %s; skipping.', ef_kind)
            continue

        n = len(frames)
        cols = min(ncols, n)
        rows = math.ceil(n / cols)
        fig, axes = plt.subplots(
            rows, cols, figsize=(10.0 * cols, 8.5 * rows), squeeze=False
        )
        flat = list(axes.flat)
        for i, (label, df) in enumerate(frames):
            ax = flat[i]
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
        for ax in flat[n:]:
            ax.axis('off')
        # Shared y-limit so panel heights are comparable across configs.
        ymax = max(flat[i].get_ylim()[1] for i in range(n))
        for i in range(n):
            flat[i].set_ylim(0, ymax)

        baseline_name = base_label if base_label is not None else 'CEDA v0'
        fig.suptitle(
            f'{EF_KIND_LABEL[ef_kind]} per-sector % diff vs {baseline_name} — '
            'v0.3 configs',
            fontsize=18,
        )
        fig.tight_layout()
        slug = (
            '_vs_' + re.sub(r'[^0-9a-zA-Z]+', '_', base_label).strip('_')
            if base_label is not None
            else ''
        )
        out = out_dir / f'ef_panels{slug}_{ef_kind}.png'
        save_and_close(fig, out)
        print(f'Wrote: {out}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main()
