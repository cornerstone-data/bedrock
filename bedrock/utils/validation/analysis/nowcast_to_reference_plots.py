"""Render a :class:`~...nowcast_to_reference_table_match.TableMatch` as a picture.

One figure per Use-table section: the interior as a raster, with the row totals
as a strip down the right edge and the column totals as a strip along the
bottom, on the same colour scale.  The margins are not decoration -- ``T014``
nets to ~1 economy-wide and redefinition preserves every total, so a green
interior above a yellow column strip localises an error that the grand total
cannot see.

The interior is drawn with a single ``imshow`` of a pre-built RGB array rather
than per-cell artists, so the same code renders a 3 x 402 value-added block and
a 402 x 402 intermediate block without changing approach.

Colour
------

============ ==========================================================
white        ``ABSENT``  -- neither side has a value
green        ``MATCH``   -- both present, within tolerance
yellow-amber ``PARTIAL`` -- both present, shaded by how far off
purple       ``MISS``    -- the reference has a value, we produced none
blue         ``EXTRA``   -- we produced a value the reference does not have
============ ==========================================================

The yellow ramp runs from the tolerance boundary (severity 0) to
``Tolerance.ramp`` (severity 1), so the shading has a stated scale and the
colour bar can print it.

These five anchors were checked, not assumed, against simulated protanopia,
deuteranopia and tritanopia: every category pair separates by at least
``dE 27`` (CIE76) under all four vision models.  :func:`palette_separation`
re-runs that check on whatever the palette currently is, so an edit to it can
be re-verified rather than argued about -- ``--check-palette`` on the CLI.

The binding constraint is not the one people expect.  Green against yellow is
the notorious pair, and it is handled by lightness: the match green is dark and
the whole yellow ramp is light.  The closest pair in the palette is actually
*white against the palest yellow* under tritanopia, which is why the ramp
starts at a saturated ``#ffd84d`` rather than the near-white a "shade of
yellow" would suggest.

CLI::

    uv run python -m bedrock.utils.validation.analysis.nowcast_to_reference_plots \\
        --section use_fd_detail_sut --year 2017
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import click
import matplotlib
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.figure import Figure

from bedrock.utils.validation.nowcast_to_reference_sections import (
    SECTIONS,
    Section,
    get_section,
)
from bedrock.utils.validation.nowcast_to_reference_table_match import (
    STATUS_NAMES,
    CellStatus,
    TableMatch,
)

OUTPUT_DIR = Path(__file__).parent / 'output'

#: Flat category colours.  ``PARTIAL`` is a ramp, see :data:`PARTIAL_RAMP`.
PALETTE: dict[CellStatus, str] = {
    CellStatus.ABSENT: '#ffffff',
    CellStatus.MATCH: '#166534',
    CellStatus.MISS: '#6a3d9a',
    CellStatus.EXTRA: '#67a9cf',
}

#: ``(severity 0, severity 1)``.  The light end is deliberately a saturated
#: yellow rather than a near-white one: a cell just outside tolerance has to
#: stay distinguishable from an empty cell, including under tritanopia, where
#: white and pale yellow are the closest pair in the whole palette.
PARTIAL_RAMP: tuple[str, str] = ('#ffd84d', '#b87700')

#: A severity that could not be computed still has to be drawn as ``PARTIAL``;
#: mid-ramp is the honest placeholder.
DEFAULT_SEVERITY = 0.5

LABEL_AXIS_MAX = 60  # tick labels stop being legible somewhere around here
TICK_FONTSIZE = 7
#: Longest description kept on a tick label before it is elided.  BEA's names
#: run to 70-odd characters and the margin they need grows with them.
NAME_MAX_CHARS = 52
#: Rough width of one character at :data:`TICK_FONTSIZE`, in inches.  Rotated
#: labels extend by their length, so this is what sizes the margins.
CHAR_INCHES = 0.055

#: Inches reserved above the axes for the title block, and below for the legend.
TITLE_INCHES = 1.5
LEGEND_INCHES = 1.1


def _hex_to_rgb(value: str) -> np.ndarray:
    value = value.lstrip('#')
    return np.array([int(value[i : i + 2], 16) / 255 for i in (0, 2, 4)])


def partial_rgb(severity: np.ndarray) -> np.ndarray:
    """Interpolate the yellow ramp at ``severity`` (0-1), shape ``(..., 3)``."""
    lo, hi = (_hex_to_rgb(c) for c in PARTIAL_RAMP)
    t = np.clip(np.nan_to_num(severity, nan=DEFAULT_SEVERITY), 0.0, 1.0)[..., None]
    return lo * (1 - t) + hi * t


def status_rgb(status: np.ndarray, severity: np.ndarray) -> np.ndarray:
    """Build the RGB raster for a status/severity pair, shape ``(..., 3)``.

    Kept separate from any figure so it can be asserted on directly, and so the
    margin strips and the interior are coloured by exactly one function.
    """
    status = np.asarray(status)
    rgb = np.zeros((*status.shape, 3), dtype=float)
    for code, hexval in PALETTE.items():
        rgb[status == int(code)] = _hex_to_rgb(hexval)
    partial = status == int(CellStatus.PARTIAL)
    if partial.any():
        rgb[partial] = partial_rgb(np.asarray(severity, dtype=float)[partial])
    return rgb


def tick_labels(axis: pd.Index, names: Mapping[str, str] | None) -> list[str] | None:
    """``code — description`` per label, or ``None`` when the axis is too long.

    A picture of 19 final-demand columns is unreadable as bare ``F02N00``
    codes, so the description is carried onto the tick.  Above
    :data:`LABEL_AXIS_MAX` entries no labelling scheme helps and the axis goes
    unlabelled.
    """
    if len(axis) > LABEL_AXIS_MAX:
        return None
    out = []
    for code in axis:
        name = (names or {}).get(str(code), '')
        if len(name) > NAME_MAX_CHARS:
            name = name[: NAME_MAX_CHARS - 1].rstrip() + '…'
        out.append(f'{code} — {name}' if name else str(code))
    return out


def _draw(
    ax: Axes,
    status: np.ndarray,
    severity: np.ndarray,
    *,
    row_labels: list[str] | None = None,
    col_labels: list[str] | None = None,
    strip: str = '',
) -> None:
    rgb = status_rgb(status, severity)
    if rgb.ndim == 2:  # a margin strip arrives 1-D
        rgb = rgb[None, :, :] if strip == 'column' else rgb[:, None, :]
    ax.imshow(rgb, aspect='auto', interpolation='nearest', origin='upper')
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_edgecolor('#999999')
        spine.set_linewidth(0.6)
    if row_labels is not None:
        ax.set_yticks(range(len(row_labels)))
        ax.set_yticklabels(row_labels, fontsize=TICK_FONTSIZE)
    if col_labels is not None:
        ax.set_xticks(range(len(col_labels)))
        ax.set_xticklabels(col_labels, fontsize=TICK_FONTSIZE, rotation=90)


# ------------------------------------------------------- colour-vision check

#: Machado, Oliveira & Fernandes (2009) severity-1.0 linear-RGB matrices.
_CVD_MATRICES: dict[str, np.ndarray] = {
    'normal': np.eye(3),
    'protan': np.array(
        [
            [0.152286, 1.052583, -0.204868],
            [0.114503, 0.786281, 0.099216],
            [-0.003882, -0.048116, 1.051998],
        ]
    ),
    'deutan': np.array(
        [
            [0.367322, 0.860646, -0.227968],
            [0.280085, 0.672501, 0.047413],
            [-0.011820, 0.042940, 0.968881],
        ]
    ),
    'tritan': np.array(
        [
            [1.255528, -0.076749, -0.178779],
            [-0.078411, 0.930809, 0.147602],
            [0.004733, 0.691367, 0.303900],
        ]
    ),
}

#: The floor the current palette clears.  Below ~25 two categories start to be
#: mistaken for each other at small cell sizes.
MIN_SEPARATION = 25.0


def _srgb_to_linear(c: np.ndarray) -> np.ndarray:
    return np.where(c <= 0.04045, c / 12.92, ((c + 0.055) / 1.055) ** 2.4)


def _linear_to_srgb(c: np.ndarray) -> np.ndarray:
    c = np.clip(c, 0.0, 1.0)
    return np.where(c <= 0.0031308, c * 12.92, 1.055 * c ** (1 / 2.4) - 0.055)


def _to_lab(rgb: np.ndarray) -> np.ndarray:
    to_xyz = np.array(
        [
            [0.4124, 0.3576, 0.1805],
            [0.2126, 0.7152, 0.0722],
            [0.0193, 0.1192, 0.9505],
        ]
    )
    xyz = to_xyz @ _srgb_to_linear(rgb) / np.array([0.95047, 1.0, 1.08883])
    f = np.where(xyz > 0.008856, np.cbrt(xyz), 7.787 * xyz + 16 / 116)
    return np.array([116 * f[1] - 16, 500 * (f[0] - f[1]), 200 * (f[1] - f[2])])


def palette_separation() -> pd.DataFrame:
    """CIE76 distance between every category pair, under four vision models.

    The ramp is sampled at both ends and its middle, and ramp-vs-ramp pairs are
    skipped: shading within ``PARTIAL`` is a magnitude cue, not a category
    boundary, so those are meant to be close.

    :return: ``vision``, ``a``, ``b``, ``delta_e``, ``delta_l``, worst first
        reversed -- sort ascending and read the top row for the binding pair.
    """
    import itertools  # noqa: PLC0415

    anchors: dict[str, np.ndarray] = {
        STATUS_NAMES[code]: _hex_to_rgb(value) for code, value in PALETTE.items()
    }
    for t in (0.0, 0.5, 1.0):
        anchors[f'partial@{t:.1f}'] = partial_rgb(np.array(t))

    rows = []
    for vision, matrix in _CVD_MATRICES.items():
        seen = {
            name: _to_lab(_linear_to_srgb(matrix @ _srgb_to_linear(rgb)))
            for name, rgb in anchors.items()
        }
        for a, b in itertools.combinations(seen, 2):
            if a.startswith('partial@') and b.startswith('partial@'):
                continue
            rows.append(
                {
                    'vision': vision,
                    'a': a,
                    'b': b,
                    'delta_e': float(np.linalg.norm(seen[a] - seen[b])),
                    'delta_l': float(abs(seen[a][0] - seen[b][0])),
                }
            )
    return pd.DataFrame(rows).sort_values('delta_e').reset_index(drop=True)


def _legend_handles() -> list[matplotlib.patches.Patch]:
    from matplotlib.patches import Patch  # noqa: PLC0415

    handles = [
        Patch(
            facecolor=PALETTE[CellStatus.ABSENT], edgecolor='#999999', label='absent'
        ),
        Patch(facecolor=PALETTE[CellStatus.MATCH], label='match'),
        Patch(facecolor=PARTIAL_RAMP[0], label='partial (at tolerance)'),
        Patch(facecolor=PARTIAL_RAMP[1], label='partial (at ramp)'),
        Patch(facecolor=PALETTE[CellStatus.MISS], label='miss (reference only)'),
        Patch(facecolor=PALETTE[CellStatus.EXTRA], label='extra (ours only)'),
    ]
    return handles


def _core_size(rows: int, cols: int) -> tuple[float, float]:
    """Inches the raster itself wants, before margins."""
    return (
        float(np.clip(cols * 0.055, 5.0, 20.0)),
        float(np.clip(rows * 0.055, 2.0, 13.0)),
    )


def _margin_inches(labels: list[str] | None, floor: float) -> float:
    """Room a set of rotated/long tick labels needs, in inches."""
    if not labels:
        return floor
    return max(floor, max(len(text) for text in labels) * CHAR_INCHES + 0.35)


def figure_layout(
    match: TableMatch,
    row_names: Mapping[str, str] | None = None,
    column_names: Mapping[str, str] | None = None,
) -> tuple[tuple[float, float], list[str] | None, list[str] | None]:
    """Figure size and tick labels, sized so the labels actually fit.

    The margins come from the labels rather than a constant, because the
    descriptions BEA gives these codes run to fifty-odd characters and a fixed
    margin either crops them or wastes half the page on the sections that have
    none.
    """
    rows, cols = match.status.shape
    row_ticks = tick_labels(match.status.index, row_names)
    col_ticks = tick_labels(match.status.columns, column_names)
    core_w, core_h = _core_size(rows, cols)
    width = core_w + _margin_inches(row_ticks, 1.0) + 1.2
    height = core_h + TITLE_INCHES + LEGEND_INCHES + _margin_inches(col_ticks, 0.6)
    return (width, height), row_ticks, col_ticks


def plot_match(
    match: TableMatch,
    *,
    title: str | None = None,
    subtitle: str = '',
    row_names: Mapping[str, str] | None = None,
    column_names: Mapping[str, str] | None = None,
) -> Figure:
    """Interior raster plus the row-total and column-total strips, one figure."""
    import matplotlib.pyplot as plt  # noqa: PLC0415

    rows, cols = match.status.shape
    (width, height), row_ticks, col_ticks = figure_layout(
        match, row_names, column_names
    )
    fig = plt.figure(figsize=(width, height))
    # Reserve fixed *inches* for the title, the legend and the tick labels
    # rather than fixed fractions: these figures range from 5 to 16 inches tall,
    # and a fraction that suits one crops the other.
    grid = fig.add_gridspec(
        2,
        2,
        width_ratios=[max(cols, 8), 1.2],
        height_ratios=[max(rows, 8), 1.2],
        wspace=0.03,
        hspace=0.03,
        left=_margin_inches(row_ticks, 1.0) / width,
        right=1 - 0.25 / width,
        top=1 - TITLE_INCHES / height,
        bottom=(LEGEND_INCHES + _margin_inches(col_ticks, 0.6)) / height,
    )

    interior = fig.add_subplot(grid[0, 0])
    _draw(
        interior,
        match.status.to_numpy(),
        match.severity.to_numpy(),
        row_labels=row_ticks,
    )
    interior.set_ylabel(str(match.status.index.name or ''), fontsize=9)
    # The strips share axes with the interior so the cells line up; without
    # this the shared ticks print their labels on both.
    interior.tick_params(labelbottom=False)

    right = fig.add_subplot(grid[0, 1], sharey=interior)
    _draw(
        right,
        match.row_totals.status.to_numpy(),
        match.row_totals.severity.to_numpy(),
        strip='row',
    )
    right.set_title('row\ntotals', fontsize=8, pad=4)

    bottom = fig.add_subplot(grid[1, 0], sharex=interior)
    _draw(
        bottom,
        match.col_totals.status.to_numpy(),
        match.col_totals.severity.to_numpy(),
        col_labels=col_ticks,
        strip='column',
    )
    bottom.set_ylabel('column\ntotals', fontsize=8, rotation=0, ha='right', va='center')
    bottom.set_xlabel(str(match.status.columns.name or ''), fontsize=9)

    # The corner is the grand total -- drawn, and drawn small, because it is the
    # check that passes on broken data.
    corner = fig.add_subplot(grid[1, 1])
    gt = match.grand_total
    gt_status, _, gt_sev = _classify_scalar(match, gt)
    _draw(corner, np.array([[gt_status]]), np.array([[gt_sev]]))
    corner.set_title('grand\ntotal', fontsize=7, pad=2, y=-0.9)

    fig.suptitle(title or match.label, fontsize=13, y=1 - 0.35 / height)
    if subtitle:
        fig.text(
            0.5,
            1 - 0.65 / height,
            subtitle,
            ha='center',
            va='top',
            fontsize=8,
            color='#444444',
        )

    fig.legend(
        handles=_legend_handles(),
        loc='lower center',
        ncol=3,
        fontsize=8,
        frameon=False,
        bbox_to_anchor=(0.5, 0.005),
    )
    return fig


def _classify_scalar(match: TableMatch, gt: pd.Series) -> tuple[int, float, float]:
    """The grand total put through the same rules as every other cell."""
    from bedrock.utils.validation.nowcast_to_reference_table_match import (  # noqa: PLC0415
        classify,
    )

    status, rel, sev = classify(
        pd.Series([gt['candidate']]),
        pd.Series([gt['reference']]),
        match.tolerance,
    )
    return int(status.iloc[0]), float(rel.iloc[0]), float(sev.iloc[0])


def _subtitle(match: TableMatch, section: Section | None, width: float) -> str:
    """Two fixed lines of numbers, then the section's caveat, wrapped to fit."""
    import textwrap  # noqa: PLC0415

    n = match.counts().loc['cells']
    counts = '   '.join(
        f'{name} {int(n[name]):,}' for name in STATUS_NAMES.values() if name != 'absent'
    )
    lines = [
        f'tolerance {match.tolerance.describe()}   |   '
        f'coverage {match.coverage:.1%}   |   accuracy {match.accuracy:.1%}',
        f'cells: {counts}   |   grand total off by '
        f'{match.grand_total["rel_error"]:.2%}',
    ]
    if match.residual:
        lines.append(
            f'residual outside the frame: {match.residual.total:,.0f} on '
            f'{len(match.residual.rows)} rows / {len(match.residual.columns)} '
            'columns, not drawn'
        )
    # ~16 characters per inch at 8pt, which is what the note has to wrap to.
    wrap_at = max(int(width * 16), 40)
    if section and section.note:
        lines += textwrap.wrap(section.note, wrap_at)
    return '\n'.join(lines)


def render_section(
    name: str,
    year: int = 2017,
    out_dir: Path = OUTPUT_DIR,
) -> tuple[Path, TableMatch]:
    """Run one section and write its picture.  Returns the path and the match."""
    matplotlib.use('Agg')
    section = get_section(name)
    match = section.run(year)
    (width, _), _, _ = figure_layout(match, section.row_names, section.column_names)
    fig = plot_match(
        match,
        title=f'{section.title} — {year}',
        subtitle=_subtitle(match, section, width),
        row_names=section.row_names,
        column_names=section.column_names,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'{section.name}_{year}.png'
    fig.savefig(path, dpi=200)
    fig.clf()
    return path, match


@click.command()
@click.option(
    '--section',
    'names',
    multiple=True,
    type=click.Choice(sorted(SECTIONS)),
    help='Section to render; repeatable. Default: all of them.',
)
@click.option('--year', default=2017, show_default=True, type=int)
@click.option(
    '--out-dir',
    default=str(OUTPUT_DIR),
    show_default=True,
    type=click.Path(file_okay=False, path_type=Path),
)
@click.option('--report/--no-report', default=True, show_default=True)
@click.option(
    '--check-palette',
    is_flag=True,
    help='Print the colour-vision separation check and exit.',
)
def main(
    names: tuple[str, ...],
    year: int,
    out_dir: Path,
    report: bool,
    check_palette: bool,
) -> None:
    """Render the Supply/Use match pictures for the sections we can compare."""
    if check_palette:
        separation = palette_separation()
        click.echo(separation.head(10).to_string(index=False))
        worst = separation['delta_e'].min()
        verdict = 'ok' if worst >= MIN_SEPARATION else 'TOO CLOSE'
        click.echo(
            f'\nworst pair dE {worst:.1f}, floor {MIN_SEPARATION:.0f}: {verdict}'
        )
        return

    for name in names or tuple(sorted(SECTIONS)):
        section = get_section(name)
        if not section.runnable:
            click.echo(
                f'{name}: no candidate yet - {section.step} has not been built. '
                'Skipping.'
            )
            click.echo('')
            continue
        path, match = render_section(name, year, out_dir)
        if report:
            click.echo(match.report(n_worst=10, n_margins=8))
            click.echo('')
        click.echo(f'wrote {path}')
        click.echo('')


if __name__ == '__main__':
    main()
