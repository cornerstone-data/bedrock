"""The build pipeline as one picture: Steps 1-7, the sources that feed each
step, and the data product each one hands on.

Draws ``images/nowcast_pipeline_steps_1_to_7.png``. The content is the plan
([`plan.md`](plan.md) §"Phase 1 build steps" and §"Data sources") reconciled
against what the modules actually do -- ``transform/iot/nowcast.py`` for the
Step 1-4 seed derivations, ``nowcast_interior_fit`` / ``nowcast_sut_assembly``
/ ``nowcast_sut_gras`` for Step 5, ``nowcast_mut`` for Step 6 and
``nowcast_redefinitions`` for Step 7.

Four rules the figure keeps:

* **Year-free.** One pass of the pipeline for any nowcast year. Sources are
  named by role -- the benchmark, the census -- never by vintage, and there is
  no storage layout, file name or unit anywhere in it.
* **Names, not prose.** Steps and substeps carry names only; what each one
  *does* belongs in the plan.
* **Steps 1-4 flow into the table itself.** Rather than four abstract product
  boxes, the seed is drawn as the Supply and Use tables, and each step arrows
  into the block it fills. That is the whole reason the four are parallel: they
  do not feed each other, they fill four parts of one pair of tables.
* **One product, one box.** Downstream of the balance, every table a step hands
  on gets its own box, so the count of deliverables is readable off the picture.

Run::

    uv run python -m bedrock.analysis.nowcasting.pipeline_diagram
    uv run python -m bedrock.analysis.nowcasting.pipeline_diagram --dpi 200
"""

from __future__ import annotations

import argparse
import textwrap
import typing as ta
from pathlib import Path

import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle  # noqa: E402
from matplotlib.path import Path as MplPath  # noqa: E402

#: Where the tracked figures live.
IMAGE_DIR = Path(__file__).parent / 'images'

#: The one tracked output of this module.
FIGURE_NAME = 'nowcast_pipeline_steps_1_to_7.png'

INK = '#22303c'
MUTED = '#5d6b76'

#: The blocks of the Supply and Use tables, as ``(fill, edge)``.  These are the
#: whole colour vocabulary of the figure: Steps 1-4 arrow into these blocks, and
#: the balanced tables downstream carry the same colours in their stripes.
BLOCK_INTERMEDIATE = ('#d6ebe8', '#2b8a81')
BLOCK_FINAL_USES = ('#fbe8d5', '#c4842a')
BLOCK_VALUE_ADDED = ('#e7e1f2', '#6f52a5')
BLOCK_SUPPLY_OUTPUT = ('#dbe7f3', '#3a6b9c')
BLOCK_SUPPLY_BRIDGE = ('#dfeedd', '#4a8b46')

#: Products that are not a block of the SUT -- the four Make-Use tables.
NEUTRAL = ('#e9edf0', '#68757f')

SOURCE_FC, SOURCE_EC = '#eef2f6', '#8ba4b8'
#: A source's outline colour and dash pattern are also the colour and dash of
#: every arrow it sends, which is the only way two dozen edges stay traceable.
#: Six hues x two dash patterns, assigned so neighbouring sources never share
#: a hue.
SOURCE_HUES = ('#0072b2', '#d55e00', '#009e73', '#7b3294', '#b8860b', '#c2185b')
SOURCE_DASHES: tuple[ta.Any, ...] = (
    'solid',
    (0, (5.0, 2.2)),
    (0, (5.0, 1.8, 1.0, 1.8)),
)
#: What the balance is held to: the target set and the mask.  Not data going in,
#: not a product coming out -- constraints, so they get their own shape.
CONSTRAINT_FC, CONSTRAINT_EC = '#f1ebf7', '#6f5591'
STEP_FC, STEP_EC = '#ffffff', '#8d9aa4'
EMPTY_FC, EMPTY_EC = '#f2f4f5', '#c8d0d6'

SEED_HDR = '#41708f'
BALANCE_HDR = '#6f5591'
CONVERT_HDR = '#2c7f63'
REDEF_HDR = '#a85a2b'

FLOW = '#3c6b8c'
WIRE = '#9db4c4'

FIGSIZE = (16.0, 11.5)
XLIM = (0.0, 100.0)
YLIM = (0.0, 100.0)

# ---- upper band: sources, Steps 1-4, and the tables they fill ---------------
BAND_TOP, BAND_BOTTOM = 99.0, 48.0

SRC_X, SRC_W = 1.0, 14.0
#: One vertical lane per source, in the channel between the source column and
#: the step column.  A source feeding three steps leaves by one lane, which is
#: what keeps the wiring readable at two dozen edges.
LANE_X0, LANE_DX = 15.8, 0.46
STEP_X, STEP_W = 22.2, 19.0
#: One routing lane per outgoing edge, between the steps and the tables.
EXIT_X = (43.0, 43.7, 44.4, 45.1, 45.8)
PANEL_X, PANEL_W = 47.0, 52.0
PANEL_Y0 = 46.5

#: The schematic Supply and Use tables inside the panel.
GRID_X, GRID_IND_W, GRID_END_W = 54.0, 26.0, 13.0
GRID_END_X = GRID_X + GRID_IND_W
GRID_RIGHT = GRID_END_X + GRID_END_W
USE_ROWS_Y0, USE_ROWS_Y1 = 75.5, 88.5
USE_VA_Y0, USE_VA_Y1 = 69.4, 74.8
SUP_ROWS_Y0, SUP_ROWS_Y1 = 52.0, 63.7
#: Margins for the two edges that have to come in over the top or under the
#: bottom, because the block they land on is behind another one.
OVER_Y, UNDER_Y, RETURN_X = 93.9, 49.4, 95.8

# ---- lower band: Steps 5-7 --------------------------------------------------
RUN_Y = 45.8
LOW_SRC_Y, LOW_SRC_H = 39.2, 5.0
#: The target set and the mask, between the control totals and the balance.
CONSTRAINT_Y, CONSTRAINT_H = 28.6, 8.8
LOW_MID = 17.5

#: The four Make-Use tables, produced before redefinitions by Step 6 and after
#: them by Step 7.  One box each.
MUT_TABLES = ('Make', 'Use, Producer Prices', 'Import Matrix', 'Margins')


def _row_span(i: int, n: int = 4, gap: float = 1.7) -> tuple[float, float]:
    """``(y0, y1)`` of row ``i`` of ``n`` in the upper band, top-down."""
    h = (BAND_TOP - BAND_BOTTOM - (n - 1) * gap) / n
    y1 = BAND_TOP - i * (h + gap)
    return y1 - h, y1


def _row_mid(i: int) -> float:
    y0, y1 = _row_span(i)
    return (y0 + y1) / 2


def _source_style(i: int) -> tuple[str, ta.Any]:
    """``(colour, linestyle)`` for source ``i``: its outline, and its arrows."""
    return SOURCE_HUES[i % len(SOURCE_HUES)], SOURCE_DASHES[i // len(SOURCE_HUES)]


def _wrap(lines: ta.Sequence[str], width: float, fontsize: float) -> str:
    """Wrap ``lines`` to a box ``width`` data units wide, hanging-indenting any
    line that starts with a bullet.

    The character budget is derived rather than guessed: one data unit is
    ``FIGSIZE[0] / (XLIM[1] - XLIM[0])`` inches wide, and DejaVu Sans averages
    close to 0.55 em per character.
    """
    inches = width * FIGSIZE[0] / (XLIM[1] - XLIM[0])
    chars = max(10, int(inches * 72 / (fontsize * 0.55)))
    out: list[str] = []
    for line in lines:
        if not line:
            out.append('')
            continue
        indent = '   ' if line.startswith('·') else ''
        out.extend(textwrap.wrap(line, chars, subsequent_indent=indent) or [''])
    return '\n'.join(out)


def _box(
    ax: plt.Axes,
    x: float,
    y: float,
    w: float,
    h: float,
    *,
    title: str = '',
    lines: ta.Sequence[str] = (),
    fc: str,
    ec: str,
    header_fc: str | None = None,
    title_fs: float = 9.0,
    body_fs: float = 7.6,
    body_color: str | None = None,
    align_middle: bool = False,
    ls: ta.Any = 'solid',
    lw: float = 1.1,
) -> None:
    """One node: rounded box, optional coloured header band, wrapped body."""
    ax.add_patch(
        FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle='round,pad=0,rounding_size=0.7',
            facecolor=fc,
            edgecolor=ec,
            linewidth=lw,
            linestyle=ls,
            zorder=2,
        )
    )
    pad = 0.9
    # ``body_ceiling`` is the top of the area the body may use; centring is
    # against that rather than against the whole box, so a header never pushes
    # the body off-centre.
    body_top, body_ceiling = y + h - 1.0, y + h

    if header_fc is not None:
        band = 3.0
        for style, height in (
            ('round,pad=0,rounding_size=0.7', band),
            ('square,pad=0', band / 2),
        ):
            ax.add_patch(
                FancyBboxPatch(
                    (x, y + h - band),
                    w,
                    height,
                    boxstyle=style,
                    facecolor=header_fc,
                    edgecolor=header_fc,
                    linewidth=0,
                    zorder=3,
                )
            )
        ax.text(
            x + pad,
            y + h - band / 2,
            title,
            fontsize=title_fs,
            fontweight='bold',
            color='#ffffff',
            va='center',
            ha='left',
            zorder=4,
        )
        body_top, body_ceiling = y + h - band - 0.8, y + h - band
    elif title:
        ax.text(
            x + pad,
            y + h - 1.1,
            title,
            fontsize=title_fs,
            fontweight='bold',
            color=INK,
            va='top',
            ha='left',
            zorder=4,
        )
        body_top = y + h - 1.1 - title_fs * 0.16 - 0.7
        body_ceiling = body_top + 0.7

    if lines:
        ax.text(
            x + pad,
            (y + body_ceiling) / 2 if align_middle else body_top,
            _wrap(lines, w - 2 * pad, body_fs),
            fontsize=body_fs,
            color=body_color or INK,
            va='center' if align_middle else 'top',
            ha='left',
            linespacing=1.5,
            zorder=4,
        )


def _product(
    ax: plt.Axes,
    x: float,
    y: float,
    w: float,
    h: float,
    *,
    title: str,
    blocks: ta.Sequence[tuple[str, str]],
) -> None:
    """One data product, striped in the colours of the table blocks it carries.

    A balanced table spans several blocks, so its stripe is segmented -- which
    is how the four parallel seed steps stay visible downstream of the balance.
    """
    single = len(blocks) == 1
    ax.add_patch(
        FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle='round,pad=0,rounding_size=0.7',
            facecolor=blocks[0][0] if single else '#f7f9fa',
            edgecolor=blocks[0][1] if single else NEUTRAL[1],
            linewidth=1.1,
            zorder=2,
        )
    )
    seg = h / len(blocks)
    for i, (_, dark) in enumerate(blocks):
        ax.add_patch(
            Rectangle(
                (x + 0.35, y + i * seg + 0.28),
                0.8,
                seg - 0.56,
                facecolor=dark,
                edgecolor='none',
                zorder=5,
            )
        )
    ax.text(
        x + 2.1,
        y + h / 2,
        _wrap([title], w - 3.0, 8.2),
        fontsize=8.2,
        fontweight='bold',
        color=INK,
        ha='left',
        va='center',
        linespacing=1.35,
        zorder=4,
    )


def _arrow(
    ax: plt.Axes,
    a: tuple[float, float],
    b: tuple[float, float],
    *,
    color: str = FLOW,
    lw: float = 1.8,
    scale: float = 14.0,
) -> None:
    ax.add_patch(
        FancyArrowPatch(
            a,
            b,
            arrowstyle='-|>',
            mutation_scale=scale,
            linewidth=lw,
            color=color,
            shrinkA=0,
            shrinkB=0,
            zorder=5,
        )
    )


def _elbow(
    ax: plt.Axes,
    points: ta.Sequence[tuple[float, float]],
    *,
    color: str = FLOW,
    lw: float = 1.8,
    scale: float = 14.0,
    zorder: float = 5.0,
    ls: ta.Any = 'solid',
) -> None:
    """A right-angled polyline with a head on the last segment."""
    path = MplPath(
        list(points), [MplPath.MOVETO] + [MplPath.LINETO] * (len(points) - 1)
    )
    ax.add_patch(
        FancyArrowPatch(
            path=path,
            arrowstyle='-|>',
            mutation_scale=scale,
            linewidth=lw,
            linestyle=ls,
            color=color,
            shrinkA=0,
            shrinkB=0,
            zorder=zorder,
        )
    )


def _stack(y_mid: float, n: int, h: float, gap: float) -> list[float]:
    """Bottom edges of ``n`` boxes of height ``h`` stacked about ``y_mid``."""
    total = n * h + (n - 1) * gap
    top = y_mid + total / 2
    return [top - (i + 1) * h - i * gap for i in range(n)]


# --------------------------------------------------------------------------
# Content
# --------------------------------------------------------------------------

#: Source, and the seed steps it feeds.  Ordered to keep the wiring shallow:
#: sources feeding the upper steps sit high, sources feeding Supply sit low.
SOURCES: tuple[tuple[str, tuple[int, ...]], ...] = (
    ('BEA PCE and PEQ Bridges', (0,)),
    ('BEA NIPAs', (0, 1, 3)),
    ('BLS QCEW', (1,)),
    ('BEA GDP by Industry', (1, 3)),
    ('BEA Benchmark SUTs and MUTs', (0, 2, 3)),
    ('BEA Summary SUTs and MUTs', (0, 2, 3)),
    ('Census EC (Economic Census)', (0, 2, 3)),
    ('Census AIES (Formerly ASM, SAS, ARTS, AWTS)', (0, 2, 3)),
    ('USDA ERS Farm Income and Inventories', (0, 2)),
    ('EIA Form 923 Fuel Receipts', (2,)),
    ('Census International Goods Trade', (0, 3)),
    ('BEA ITAs and Services Trade', (0, 3)),
    ('STB Rail Revenue and BTS FAF', (3,)),
)

#: The four parallel seed steps.
SEED_STEPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        'Step 1: Use Final',
        ('1A  Final Domestic Uses', '1B  Exports', '1C  Change in Inventories'),
    ),
    (
        'Step 2: Use Value Added',
        (
            '·  Compensation of Employees',
            '·  Other Taxes on Production',
            '·  Gross Operating Surplus',
            '·  Product Taxes and Subsidies',
        ),
    ),
    (
        'Step 3: Use Intermediate',
        (
            '·  Manufacturing and Mining',
            '·  Services and Transportation',
            '·  Agriculture',
            '·  Utilities',
            '·  Held Columns: Benchmark Mix, Price Carry',
        ),
    ),
    (
        'Step 4: Supply',
        (
            '4A  Domestic Output',
            '4B  Imports and Duties',
            '4C  Margins',
            '4D  Product Taxes and Subsidies',
        ),
    ),
)


def _draw_sources_and_steps(ax: plt.Axes) -> None:
    """The source column, the four seed steps, and the wiring between them."""
    entries: dict[int, list[int]] = {s: [] for s in range(4)}
    for i, (_, steps) in enumerate(SOURCES):
        for s in steps:
            entries[s].append(i)

    src_gap = 0.6
    src_h = (BAND_TOP - BAND_BOTTOM - (len(SOURCES) - 1) * src_gap) / len(SOURCES)
    src_mid: list[float] = []
    for i, (name, _) in enumerate(SOURCES):
        y1 = BAND_TOP - i * (src_h + src_gap)
        src_mid.append(y1 - src_h / 2)
        hue, dash = _source_style(i)
        _box(
            ax,
            SRC_X,
            y1 - src_h,
            SRC_W,
            src_h,
            lines=[name],
            fc=SOURCE_FC,
            ec=hue,
            body_fs=7.2,
            body_color=INK,
            align_middle=True,
            ls=dash,
            lw=1.5,
        )

    for i, (title, lines) in enumerate(SEED_STEPS):
        y0, y1 = _row_span(i)
        _box(
            ax,
            STEP_X,
            y0,
            STEP_W,
            y1 - y0,
            title=title,
            lines=lines,
            fc=STEP_FC,
            ec=STEP_EC,
            header_fc=SEED_HDR,
            title_fs=9.6,
            body_fs=7.8,
            align_middle=True,
        )
        incoming = entries[i]
        entry_top = y1 - 3.0
        for k, src in enumerate(incoming):
            entry_y = entry_top - (k + 1) * (entry_top - y0) / (len(incoming) + 1)
            lane = LANE_X0 + src * LANE_DX
            hue, dash = _source_style(src)
            _elbow(
                ax,
                [
                    (SRC_X + SRC_W, src_mid[src]),
                    (lane, src_mid[src]),
                    (lane, entry_y),
                    (STEP_X - 0.15, entry_y),
                ],
                color=hue,
                lw=1.0,
                scale=8.0,
                zorder=1,
                ls=dash,
            )


def _grid_block(
    ax: plt.Axes,
    x: float,
    y: float,
    w: float,
    h: float,
    label: str,
    block: tuple[str, str],
) -> None:
    """One cell of the schematic Supply or Use table."""
    fill, edge = block
    ax.add_patch(
        Rectangle((x, y), w, h, facecolor=fill, edgecolor=edge, linewidth=1.3, zorder=2)
    )
    ax.text(
        x + w / 2,
        y + h / 2,
        label,
        fontsize=8.8,
        fontweight='bold',
        color=INK,
        ha='center',
        va='center',
        linespacing=1.35,
        zorder=3,
    )


def _draw_seed_tables(ax: plt.Axes) -> None:
    """The seed itself: the two tables, with Steps 1-4 arrowing into their blocks."""
    ax.add_patch(
        FancyBboxPatch(
            (PANEL_X, PANEL_Y0),
            PANEL_W,
            BAND_TOP - PANEL_Y0,
            boxstyle='round,pad=0,rounding_size=0.7',
            facecolor='#fbfcfc',
            edgecolor='#c8d0d6',
            linewidth=1.1,
            zorder=1,
        )
    )
    ax.text(
        PANEL_X + PANEL_W / 2,
        97.2,
        'Unbalanced Supply and Use Tables',
        fontsize=10.5,
        fontweight='bold',
        color=INK,
        ha='center',
        va='center',
    )

    def caption(y: float, text: str) -> None:
        ax.text(
            GRID_X,
            y,
            text,
            fontsize=9.0,
            fontweight='bold',
            color=MUTED,
            ha='left',
            va='bottom',
        )

    def header(x: float, w: float, y: float, text: str) -> None:
        ax.text(x + w / 2, y, text, fontsize=7.6, color=MUTED, ha='center', va='bottom')

    def side(y: float, text: str, fs: float = 7.6) -> None:
        ax.text(
            GRID_X - 1.0,
            y,
            text,
            rotation=90,
            fontsize=fs,
            color=MUTED,
            ha='right',
            va='center',
            linespacing=1.1,
        )

    # ---- Use table ----
    caption(90.9, 'Use Table, Purchaser Prices')
    header(GRID_X, GRID_IND_W, 89.4, 'Industries')
    header(GRID_END_X, GRID_END_W, 89.4, 'Final Demand')
    _grid_block(
        ax,
        GRID_X,
        USE_ROWS_Y0,
        GRID_IND_W,
        USE_ROWS_Y1 - USE_ROWS_Y0,
        'Intermediate',
        BLOCK_INTERMEDIATE,
    )
    _grid_block(
        ax,
        GRID_END_X,
        USE_ROWS_Y0,
        GRID_END_W,
        USE_ROWS_Y1 - USE_ROWS_Y0,
        'Final Uses',
        BLOCK_FINAL_USES,
    )
    _grid_block(
        ax,
        GRID_X,
        USE_VA_Y0,
        GRID_IND_W,
        USE_VA_Y1 - USE_VA_Y0,
        'Value Added',
        BLOCK_VALUE_ADDED,
    )
    ax.add_patch(
        Rectangle(
            (GRID_END_X, USE_VA_Y0),
            GRID_END_W,
            USE_VA_Y1 - USE_VA_Y0,
            facecolor=EMPTY_FC,
            edgecolor=EMPTY_EC,
            linewidth=1.0,
            zorder=2,
        )
    )
    side((USE_ROWS_Y0 + USE_ROWS_Y1) / 2, 'Commodities')
    side((USE_VA_Y0 + USE_VA_Y1) / 2, 'Value\nAdded', fs=6.8)

    # ---- Supply table ----
    caption(66.0, 'Supply Table, Basic to Purchaser Prices')
    header(GRID_X, GRID_IND_W, 64.5, 'Industries')
    header(GRID_END_X, GRID_END_W, 64.5, 'Bridge')
    _grid_block(
        ax,
        GRID_X,
        SUP_ROWS_Y0,
        GRID_IND_W,
        SUP_ROWS_Y1 - SUP_ROWS_Y0,
        'Domestic Output',
        BLOCK_SUPPLY_OUTPUT,
    )
    _grid_block(
        ax,
        GRID_END_X,
        SUP_ROWS_Y0,
        GRID_END_W,
        SUP_ROWS_Y1 - SUP_ROWS_Y0,
        'Imports\nMargins\nTaxes',
        BLOCK_SUPPLY_BRIDGE,
    )
    side((SUP_ROWS_Y0 + SUP_ROWS_Y1) / 2, 'Commodities')

    # ---- each step into the block it fills ---------------------------------
    right = STEP_X + STEP_W
    use_mid = (USE_ROWS_Y0 + USE_ROWS_Y1) / 2
    va_mid = (USE_VA_Y0 + USE_VA_Y1) / 2
    sup_mid = (SUP_ROWS_Y0 + SUP_ROWS_Y1) / 2

    # Step 1 lands on Final Uses, which sits behind Intermediate: over the top.
    _elbow(
        ax,
        [
            (right, _row_mid(0)),
            (EXIT_X[0], _row_mid(0)),
            (EXIT_X[0], OVER_Y),
            (RETURN_X, OVER_Y),
            (RETURN_X, use_mid),
            (GRID_RIGHT + 0.3, use_mid),
        ],
        lw=1.7,
    )
    _elbow(
        ax,
        [
            (right, _row_mid(1)),
            (EXIT_X[1], _row_mid(1)),
            (EXIT_X[1], va_mid),
            (GRID_X - 0.3, va_mid),
        ],
        lw=1.7,
    )
    _elbow(
        ax,
        [
            (right, _row_mid(2)),
            (EXIT_X[2], _row_mid(2)),
            (EXIT_X[2], use_mid),
            (GRID_X - 0.3, use_mid),
        ],
        lw=1.7,
    )
    _elbow(
        ax,
        [
            (right, _row_mid(3)),
            (EXIT_X[3], _row_mid(3)),
            (EXIT_X[3], sup_mid),
            (GRID_X - 0.3, sup_mid),
        ],
        lw=1.7,
    )
    # Step 4's bridge columns sit behind Domestic Output: under the bottom.
    _elbow(
        ax,
        [
            (right, _row_span(3)[0] + 2.5),
            (EXIT_X[4], _row_span(3)[0] + 2.5),
            (EXIT_X[4], UNDER_Y),
            (RETURN_X, UNDER_Y),
            (RETURN_X, sup_mid),
            (GRID_RIGHT + 0.3, sup_mid),
        ],
        lw=1.7,
    )


def _draw_lower(ax: plt.Axes) -> None:
    """Steps 5-7 and their products, left to right along the bottom."""
    s5_x, s5_w = 4.0, 14.0
    c5_x, c5_w = 20.5, 11.5
    s6_x, s6_w = 34.5, 14.0
    c6_x, c6_w = 51.0, 11.5
    s7_x, s7_w = 65.0, 14.0
    c7_x, c7_w = 81.5, 12.0

    # Each step box is sized to the stack of products it hands on.
    s5_h = 2 * 7.0 + 1.4 + 2.4
    mut_h = len(MUT_TABLES) * 4.6 + (len(MUT_TABLES) - 1) * 1.2 + 1.8
    s5_y0, s5_y1 = LOW_MID - s5_h / 2, LOW_MID + s5_h / 2
    mut_y0, mut_y1 = LOW_MID - mut_h / 2, LOW_MID + mut_h / 2

    # The seed comes down out of the tables and enters the balance from the
    # left, leaving the top of Step 5 free for what constrains it.
    _elbow(
        ax,
        [
            (PANEL_X + PANEL_W / 2, PANEL_Y0),
            (PANEL_X + PANEL_W / 2, RUN_Y),
            (1.6, RUN_Y),
            (1.6, LOW_MID + 4.0),
            (s5_x - 0.3, LOW_MID + 4.0),
        ],
        lw=2.4,
        scale=17,
    )

    # ---- what holds the balance: the control totals, the targets, the mask --
    _box(
        ax,
        s5_x,
        LOW_SRC_Y,
        s5_w,
        LOW_SRC_H,
        lines=['BEA GDP by Industry', 'BEA Summary SUTs'],
        fc=SOURCE_FC,
        ec=SOURCE_EC,
        body_fs=7.2,
        body_color='#2f4d61',
        align_middle=True,
    )
    _box(
        ax,
        s5_x,
        CONSTRAINT_Y,
        s5_w,
        CONSTRAINT_H,
        title='Balance Constraints',
        lines=[
            '·  Targets: Gross Output, Value Added, Identities',
            '·  Frozen Cells: Structural Zeros, Sign Locks',
        ],
        fc=CONSTRAINT_FC,
        ec=CONSTRAINT_EC,
        title_fs=8.4,
        body_fs=7.0,
        body_color='#4a3a63',
    )
    mid_x = s5_x + s5_w / 2
    _arrow(
        ax,
        (mid_x, LOW_SRC_Y),
        (mid_x, CONSTRAINT_Y + CONSTRAINT_H + 0.3),
        color=SOURCE_EC,
        lw=1.4,
    )
    _arrow(ax, (mid_x, CONSTRAINT_Y), (mid_x, s5_y1 + 0.3), color=CONSTRAINT_EC, lw=1.6)

    mut_src_x, mut_src_w = 40.0, 25.0
    _box(
        ax,
        mut_src_x,
        LOW_SRC_Y,
        mut_src_w,
        LOW_SRC_H,
        lines=['BEA Benchmark MUTs, Before and After Redefinitions'],
        fc=SOURCE_FC,
        ec=SOURCE_EC,
        body_fs=7.2,
        body_color='#2f4d61',
        align_middle=True,
    )
    for x_from, x_to in (
        (mut_src_x + 4.0, s6_x + s6_w / 2),
        (mut_src_x + mut_src_w - 4.0, s7_x + s7_w / 2),
    ):
        _arrow(ax, (x_from, LOW_SRC_Y), (x_to, mut_y1 + 0.3), color=SOURCE_EC, lw=1.4)

    # ---- Step 5 ----
    _box(
        ax,
        s5_x,
        s5_y0,
        s5_w,
        s5_y1 - s5_y0,
        title='Step 5: SUT Balance',
        lines=['5A  Interior Fit', '5B  Generalized RAS'],
        fc=STEP_FC,
        ec=STEP_EC,
        header_fc=BALANCE_HDR,
        title_fs=9.2,
        body_fs=7.8,
        align_middle=True,
    )
    balanced = (
        ('Balanced Supply', [BLOCK_SUPPLY_BRIDGE, BLOCK_SUPPLY_OUTPUT]),
        ('Balanced Use', [BLOCK_VALUE_ADDED, BLOCK_FINAL_USES, BLOCK_INTERMEDIATE]),
    )
    for (title, blocks), cy in zip(balanced, _stack(LOW_MID, 2, 7.0, 1.4)):
        _product(ax, c5_x, cy, c5_w, 7.0, title=title, blocks=blocks)
        _arrow(ax, (s5_x + s5_w, cy + 3.5), (c5_x - 0.3, cy + 3.5), lw=1.6)
        _arrow(ax, (c5_x + c5_w, cy + 3.5), (s6_x - 0.3, cy + 3.5), lw=1.6)

    # ---- Steps 6 and 7: one box per table produced ----
    for x, w, title, lines, hdr in (
        (
            s6_x,
            s6_w,
            'Step 6: SUT → MUT',
            (
                '6A  Make',
                '6B  Use at Producer Prices',
                '6C  Import Matrix',
                '6D  Margins',
            ),
            CONVERT_HDR,
        ),
        (
            s7_x,
            s7_w,
            'Step 7: Redefinitions',
            (
                '·  Make: Pattern Move',
                '·  Use: Ratio Carry, Row Closure',
                '·  Value Added: Column Closure',
                '·  Import Matrix: Reallocation',
            ),
            REDEF_HDR,
        ),
    ):
        _box(
            ax,
            x,
            mut_y0,
            w,
            mut_h,
            title=title,
            lines=lines,
            fc=STEP_FC,
            ec=STEP_EC,
            header_fc=hdr,
            title_fs=9.2,
            body_fs=7.6,
            align_middle=True,
        )

    rows = _stack(LOW_MID, len(MUT_TABLES), 4.6, 1.2)
    for caption, cx, cw, src_x, src_w, sink in (
        ('Before Redefinitions', c6_x, c6_w, s6_x, s6_w, s7_x),
        ('After Redefinitions', c7_x, c7_w, s7_x, s7_w, None),
    ):
        ax.text(
            cx + cw / 2,
            rows[0] + 5.9,
            caption,
            fontsize=8.0,
            fontweight='bold',
            color=MUTED,
            ha='center',
            va='bottom',
        )
        for name, cy in zip(MUT_TABLES, rows):
            _product(ax, cx, cy, cw, 4.6, title=name, blocks=[NEUTRAL])
            _arrow(ax, (src_x + src_w, cy + 2.3), (cx - 0.3, cy + 2.3), lw=1.4)
            if sink is not None:
                _arrow(ax, (cx + cw, cy + 2.3), (sink - 0.3, cy + 2.3), lw=1.4)


def _draw_key(ax: plt.Axes) -> None:
    """Shape key, along the bottom."""
    items: tuple[tuple[str, str, str], ...] = (
        (
            SOURCE_FC,
            SOURCE_EC,
            'Input Data Source — Outline Colour and Dash Match Its Arrows',
        ),
        (STEP_FC, STEP_EC, 'Build Step'),
        (CONSTRAINT_FC, CONSTRAINT_EC, 'Constraint on the Balance'),
        ('#f7f9fa', NEUTRAL[1], 'Data Product'),
    )
    x = 1.0
    for fc, ec, label in items:
        ax.add_patch(
            FancyBboxPatch(
                (x, 1.2),
                2.4,
                2.2,
                boxstyle='round,pad=0,rounding_size=0.4',
                facecolor=fc,
                edgecolor=ec,
                linewidth=1.1,
            )
        )
        ax.text(x + 3.2, 2.3, label, fontsize=8.0, color=INK, ha='left', va='center')
        x += 4.4 + len(label) * 0.5


def draw(ax: plt.Axes) -> None:
    """Render the whole figure onto ``ax``."""
    _draw_sources_and_steps(ax)
    _draw_seed_tables(ax)
    _draw_lower(ax)
    _draw_key(ax)


def render(path: Path, dpi: int) -> Path:
    """Draw the figure and write it to ``path``."""
    fig = plt.figure(figsize=FIGSIZE)
    ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
    ax.set_xlim(*XLIM)
    ax.set_ylim(*YLIM)
    ax.set_axis_off()
    fig.patch.set_facecolor('#ffffff')

    draw(ax)

    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=dpi, facecolor='#ffffff')
    plt.close(fig)
    return path


def main(argv: ta.Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=IMAGE_DIR / FIGURE_NAME)
    parser.add_argument('--dpi', type=int, default=150)
    args = parser.parse_args(argv)

    path = render(args.out, args.dpi)
    print(f'wrote {path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
