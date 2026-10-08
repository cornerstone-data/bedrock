"""Render a :class:`~..table_match.TableMatch` as a picture.

One figure per Use-table section: the interior as a raster, with the row totals
as a strip down the right edge and the column totals as a strip along the
bottom, on the same colour scale.  The margins are not decoration -- ``T014``
nets to ~1 economy-wide and redefinition preserves every total, so a green
interior above a red column strip localises an error that the grand total
cannot see.

The interior is drawn with a single ``imshow`` of a pre-built RGB array rather
than per-cell artists, so the same code renders a 3 x 402 value-added block and
a 402 x 402 intermediate block without changing approach.

Colour
------

=========== ==========================================================
white       ``ABSENT`` -- neither side has a value
green-red   both sides have a value, coloured by relative difference
purple      ``MISS``   -- the reference has a value, we produced none
blue        ``EXTRA``  -- we produced a value the reference does not have
=========== ==========================================================

Where both sides have a value the cell sits on one continuous ramp: green at a
relative difference of 0, red at 1.0 and beyond.  There is no threshold in the
picture.  A cell 0.9% off and a cell 1.1% off are drawn almost identically,
because that is what they are; the earlier within-tolerance / outside-tolerance
split drew them as two different colours, which put a step in the picture where
the data has none.  ``Tolerance`` still exists in :mod:`~..table_match`, where
the pass/fail gates need a boundary -- it just no longer decides a colour.

Lightness falls monotonically along the ramp, light green through amber to a
dark red.  That is what keeps it readable without hue discrimination: under
protanopia and deuteranopia the two ends converge in hue, so position along the
ramp has to be carried by something else, and lightness is the only channel
left.  A green-to-red ramp built on hue alone is the one gradient those viewers
cannot read at all.

The anchors were checked, not assumed, against simulated protanopia,
deuteranopia and tritanopia.  :func:`palette_separation` re-runs the check on
whatever the palette currently is, so an edit to it can be re-verified rather
than argued about -- ``--check-palette`` on the CLI.  Ramp-against-ramp pairs
are exempt from the floor, since adjacent points on a continuous scale are
meant to be close, but the two ends are held to it: if 0% and 100% are not
separable the ramp is not carrying its own scale.

CLI::

    uv run python -m bedrock.analysis.nowcasting.plots \\
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

from bedrock.analysis.nowcasting.sections import (
    SECTIONS,
    Section,
    get_section,
)
from bedrock.analysis.nowcasting.table_match import (
    STATUS_NAMES,
    CellStatus,
    TableMatch,
)

OUTPUT_DIR = Path(__file__).parent / 'output'

#: Flat colours for the three presence states.  Cells both sides populate are
#: not in here; they are drawn from :data:`DIFF_RAMP`.
PALETTE: dict[CellStatus, str] = {
    CellStatus.ABSENT: '#ffffff',
    CellStatus.MISS: '#6a3d9a',
    CellStatus.EXTRA: '#67a9cf',
}

#: ``(position, colour)`` from a relative difference of 0 to :data:`DIFF_MAX`.
#: Lightness falls monotonically along it -- ``L*`` 81, 73, 64, 48, 33 -- which
#: is the property that carries the scale for viewers who cannot separate the
#: two hues.  The steps are kept roughly even rather than merely monotonic:
#: most cells in a Use table sit in the first quarter of this scale, so a ramp
#: that is flat there has thrown away the channel where it needs it most.
#:
#: These anchors were searched, not chosen: the green end is squeezed between
#: white (``ABSENT``) above it and the blue of ``EXTRA`` below it, and under
#: tritanopia both of those sit close to a light green.  The best available
#: worst-pair separation is ``dE 26.0`` against a floor of
#: :data:`MIN_SEPARATION`, binding on white against the green end -- the same
#: pair that bound the previous palette.
DIFF_RAMP: tuple[tuple[float, str], ...] = (
    (0.00, '#a5d96a'),
    (0.25, '#a8b84e'),
    (0.50, '#c9902e'),
    (0.75, '#b85526'),
    (1.00, '#94201f'),
)

#: Relative difference at which the ramp saturates.  The legend states it, so a
#: saturated cell reads as "100% off or worse" rather than "exactly 100%".
DIFF_MAX = 1.0

#: A relative difference that could not be computed still has to be drawn;
#: mid-ramp is the honest placeholder.
DEFAULT_REL = 0.5

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


def diff_rgb(rel: np.ndarray) -> np.ndarray:
    """Interpolate :data:`DIFF_RAMP` at relative difference ``rel``.

    ``rel`` is a fraction, not a percentage, and is clipped to
    ``[0, DIFF_MAX]``: everything at or past the top of the scale is the same
    red, so one cell 40x out does not decide how the rest of the table reads.
    """
    positions = np.array([p for p, _ in DIFF_RAMP]) * DIFF_MAX
    colours = np.stack([_hex_to_rgb(c) for _, c in DIFF_RAMP])
    t = np.clip(
        np.nan_to_num(np.asarray(rel, dtype=float), nan=DEFAULT_REL), 0.0, DIFF_MAX
    )
    return np.stack([np.interp(t, positions, colours[:, i]) for i in range(3)], axis=-1)


def status_rgb(status: np.ndarray, rel: np.ndarray) -> np.ndarray:
    """Build the RGB raster for a status/relative-difference pair, ``(..., 3)``.

    Kept separate from any figure so it can be asserted on directly, and so the
    margin strips and the interior are coloured by exactly one function.

    ``MATCH`` and ``PARTIAL`` are both "the two sides have a value here" and
    are coloured identically, by their difference.  The distinction between
    them survives only in the gates.
    """
    status = np.asarray(status)
    rgb = np.zeros((*status.shape, 3), dtype=float)
    for code, hexval in PALETTE.items():
        rgb[status == int(code)] = _hex_to_rgb(hexval)
    both = (status == int(CellStatus.MATCH)) | (status == int(CellStatus.PARTIAL))
    if both.any():
        rgb[both] = diff_rgb(np.asarray(rel, dtype=float)[both])
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
    rel: np.ndarray,
    *,
    row_labels: list[str] | None = None,
    col_labels: list[str] | None = None,
    strip: str = '',
) -> None:
    rgb = status_rgb(status, rel)
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

    The ramp is sampled at each of its anchors.  Ramp-vs-ramp pairs are skipped
    -- shading within the ramp is a magnitude cue, not a category boundary, so
    neighbouring samples are meant to be close -- with one exception: the two
    *ends* are held to the floor like any other pair, because a ramp whose ends
    are not separable is not carrying a scale.

    :return: ``vision``, ``a``, ``b``, ``delta_e``, ``delta_l``, worst first
        reversed -- sort ascending and read the top row for the binding pair.
    """
    import itertools  # noqa: PLC0415

    anchors: dict[str, np.ndarray] = {
        STATUS_NAMES[code]: _hex_to_rgb(value) for code, value in PALETTE.items()
    }
    for position, _ in DIFF_RAMP:
        anchors[f'diff@{position:.2f}'] = diff_rgb(np.array(position))
    ends = {f'diff@{DIFF_RAMP[0][0]:.2f}', f'diff@{DIFF_RAMP[-1][0]:.2f}'}

    rows = []
    for vision, matrix in _CVD_MATRICES.items():
        seen = {
            name: _to_lab(_linear_to_srgb(matrix @ _srgb_to_linear(rgb)))
            for name, rgb in anchors.items()
        }
        for a, b in itertools.combinations(seen, 2):
            ramp_pair = a.startswith('diff@') and b.startswith('diff@')
            if ramp_pair and {a, b} != ends:
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
        Patch(facecolor=DIFF_RAMP[0][1], label='0% different'),
        Patch(facecolor=DIFF_RAMP[2][1], label='50% different'),
        Patch(facecolor=DIFF_RAMP[-1][1], label=f'{DIFF_MAX:.0%} or more'),
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
    chrome: bool = True,
) -> Figure:
    """Interior raster plus the row-total and column-total strips, one figure.

    ``chrome=False`` drops the title, the subtitle block and the legend -
    the style for a figure whose caption carries that text instead - and
    reclaims the space they reserve.
    """
    import matplotlib.pyplot as plt  # noqa: PLC0415

    rows, cols = match.status.shape
    (width, height), row_ticks, col_ticks = figure_layout(
        match, row_names, column_names
    )
    if not chrome:
        height = height - TITLE_INCHES - LEGEND_INCHES + 0.4
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
        top=1 - (TITLE_INCHES if chrome else 0.2) / height,
        bottom=((LEGEND_INCHES if chrome else 0.2) + _margin_inches(col_ticks, 0.6))
        / height,
    )

    interior = fig.add_subplot(grid[0, 0])
    _draw(
        interior,
        match.status.to_numpy(),
        match.rel_error.to_numpy(),
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
        match.row_totals.rel_error.to_numpy(),
        strip='row',
    )
    right.set_title('row\ntotals', fontsize=8, pad=4)

    bottom = fig.add_subplot(grid[1, 0], sharex=interior)
    _draw(
        bottom,
        match.col_totals.status.to_numpy(),
        match.col_totals.rel_error.to_numpy(),
        col_labels=col_ticks,
        strip='column',
    )
    bottom.set_ylabel('column\ntotals', fontsize=8, rotation=0, ha='right', va='center')
    bottom.set_xlabel(str(match.status.columns.name or ''), fontsize=9)

    # The corner is the grand total -- drawn, and drawn small, because it is the
    # check that passes on broken data.
    corner = fig.add_subplot(grid[1, 1])
    gt = match.grand_total
    gt_status, gt_rel, _ = _classify_scalar(match, gt)
    _draw(corner, np.array([[gt_status]]), np.array([[gt_rel]]))
    corner.set_title('grand\ntotal', fontsize=7, pad=2, y=-0.9)

    if chrome:
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
    from bedrock.analysis.nowcasting.table_match import (  # noqa: PLC0415
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
    # 'match' and 'partial' are the two halves of "both sides have a value";
    # they are one thing here, because the picture no longer splits them.
    counts = (
        f'both {int(n["match"]) + int(n["partial"]):,}   '
        f'miss {int(n["miss"]):,}   extra {int(n["extra"]):,}'
    )
    lines = [
        f'coverage {match.coverage:.1%}   |   median difference '
        f'{match.median_rel_error:.1%}   |   value-weighted '
        f'{match.weighted_rel_error:.1%}',
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


#: Screen-quality default.  The figures are sized in inches, so dpi is purely
#: how many pixels that becomes -- ``REPORT_DPI`` is the setting for a copy that
#: gets committed alongside a document.
DEFAULT_DPI = 200
REPORT_DPI = 110


def render_section(
    name: str,
    year: int = 2017,
    out_dir: Path = OUTPUT_DIR,
    dpi: int = DEFAULT_DPI,
    chrome: bool = True,
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
        chrome=chrome,
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'{section.name}_{year}.png'
    fig.savefig(path, dpi=dpi)
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
    '--dpi',
    default=DEFAULT_DPI,
    show_default=True,
    type=int,
    help=f'Output resolution. Use {REPORT_DPI} for a copy that gets committed.',
)
@click.option(
    '--check-palette',
    is_flag=True,
    help='Print the colour-vision separation check and exit.',
)
@click.option(
    '--chrome/--plain',
    'chrome',
    default=True,
    show_default=True,
    help='--plain drops the drawn title, subtitle and legend, for figures '
    'whose caption carries that text (the standing report style).',
)
def main(
    names: tuple[str, ...],
    year: int,
    out_dir: Path,
    report: bool,
    dpi: int,
    check_palette: bool,
    chrome: bool,
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
        path, match = render_section(name, year, out_dir, dpi, chrome=chrome)
        if report:
            click.echo(match.report(n_worst=10, n_margins=8))
            click.echo('')
        click.echo(f'wrote {path}')
        click.echo('')


if __name__ == '__main__':
    main()
