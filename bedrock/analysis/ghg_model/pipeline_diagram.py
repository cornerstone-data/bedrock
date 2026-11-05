"""The GHG model as one picture: the sources, the build steps that turn them
into attribution data, the Sector Attribution Model that places each family of
inventory emissions on industries, and the chain from the Flow-By-Sector to the
model's emission factors.

Draws ``images/ghg_pipeline.png``, the companion to the nowcast tables'
``analysis/nowcasting/images/nowcast_pipeline_steps_1_to_7.png`` and drawn with
the same helpers, so the two read as a set. The content is the v0.5 method as
the code runs it:

* ``transform/ghg/GHG_national_Cornerstone_nowcast_facilities_{year}.yaml`` and
  the ``GHG_national_Cornerstone_nowcast_{year}.yaml`` it includes, for the
  families and the attribution route each activity set takes;
* ``transform/energy/Energy_manufacturing_national_nowcast_{year}.yaml`` for the
  energy-survey split;
* ``extract/stewifbs/facility_combustion.py`` (``build_facility_combustion``)
  and ``stewiFBS.hybrid_facility_mecs_to_sector`` with
  ``transform/ghg/facility_coverage.py`` for the facility data and the gate;
* ``transform/allocation/derived.py`` (``load_E_from_flowsa``) for the chain
  from the Flow-By-Sector to ``E``, and ``transform/eeio/derived_cornerstone.py``
  for ``B``.

It keeps the nowcast figure's rules: year-free, names rather than prose, and
one box per product.

Run::

    uv run python -m bedrock.analysis.ghg_model.pipeline_diagram
    uv run python -m bedrock.analysis.ghg_model.pipeline_diagram --dpi 200
"""

from __future__ import annotations

import argparse
import typing as ta
from pathlib import Path

import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch, Rectangle  # noqa: E402

from bedrock.analysis.nowcasting.pipeline_diagram import (  # noqa: E402
    CONSTRAINT_EC,
    CONSTRAINT_FC,
    FIGSIZE,
    FLOW,
    INK,
    MUTED,
    SOURCE_FC,
    STEP_EC,
    STEP_FC,
    WIRE,
    XLIM,
    YLIM,
    _arrow,
    _box,
    _elbow,
    _source_style,
    _wrap,
)

IMAGE_DIR = Path(__file__).parent / 'images'
FIGURE_NAME = 'ghg_pipeline.png'

# ---- the attribution routes: the colour vocabulary of the figure -----------
#: ``(header, cell fill)`` per route. A build step that feeds a route carries
#: that route's colour in its header, so the steps and the columns they fill
#: read as pairs.
ROUTES: tuple[tuple[str, str, str], ...] = (
    ('Named Directly by the Inventory', '#5b6f82', '#e3e8ed'),
    ('Facility Reports', '#1f7a74', '#d4ece9'),
    ('Facility and Energy Survey Blend', '#6f5591', '#e7e1f2'),
    ('Purchases in the Use Table', '#b8742a', '#f8e7d3'),
    ('Other Secondary Data', '#4a8b46', '#dfeedd'),
)
TOTALS_HDR = '#22303c'

# ---- layout -----------------------------------------------------------------
BAND_TOP, BAND_BOTTOM = 99.0, 47.0
SRC_X, SRC_W = 1.0, 14.0
LANE_X0, LANE_DX = 15.8, 0.5
STEP_X, STEP_W = 22.2, 18.6
PANEL_X, PANEL_W = 46.0, 53.0
PANEL_Y0 = 45.5
LABEL_X, LABEL_W = 47.0, 13.4
COL_X0 = LABEL_X + LABEL_W + 0.4
COL_W = (PANEL_X + PANEL_W - 0.8 - COL_X0) / len(ROUTES)
HEAD_Y0, HEAD_Y1 = 86.2, 93.6
ROWS_Y1 = 85.6
ROW_GAP = 0.35

# ---- content ----------------------------------------------------------------
#: Source, and the build steps it feeds.
SOURCES: tuple[tuple[tuple[str, ...], tuple[int, ...]], ...] = (
    (('EPA GHG Inventory', 'via UMD GHGIA'), (0,)),
    (('EPA GHG Inventory', 'Detail Tables'), (0, 3)),
    (('EPA eGRID',), (1,)),
    (('EPA GHG Reporting', 'Program'), (1,)),
    (('EPA National', 'Emissions Inventory'), (1,)),
    (('EPA Facility Registry', 'Service'), (1,)),
    (('EIA Manufacturing Energy', 'Consumption Survey'), (2,)),
    (('Cornerstone Nowcast', 'Use Tables'), (2, 3)),
    (('USDA Census of', 'Agriculture'), (3,)),
    (('BEA Gross Output and', 'PCE; EIA Prices'), (3,)),
)

#: ``(title, lines, header colour)``.
STEPS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    (
        'National Totals',
        (
            '·  Emissions by Activity and Gas',
            '·  Petroleum Split by Fuel',
            '·  Hydrofluorocarbons by Use',
        ),
        TOTALS_HDR,
    ),
    (
        'Facility Emissions',
        (
            '·  Power Plants',
            '·  One Record per Industrial Site',
            '·  Purchased or Self-Supplied Fuel',
        ),
        ROUTES[1][1],
    ),
    (
        'Facility-Survey Blend',
        (
            '·  Survey Split on Census-Year Use',
            '·  Facility Coverage Gate, 80%',
            '·  Mode Held Across Years',
        ),
        ROUTES[2][1],
    ),
    (
        'Secondary Attribution Data',
        (
            '·  Purchase Rows of the Use Table',
            '·  Census-Year Blend, Fertilizer',
            '·  Cropland and Farm Counts',
            '·  Gross Output, Household Fuel',
        ),
        ROUTES[3][1],
    ),
)

#: Families of inventory emissions, and the label in each route they use.
#: Route order is ``ROUTES``; ``''`` = the family does not use that route.
FAMILIES: tuple[tuple[str, tuple[str, str, str, str, str]], ...] = (
    ('Electricity Generation', ('', 'Power Plants', '', '', '')),
    (
        'Industrial Fuel Combustion',
        (
            '',
            'Still Gas, Lease and Plant Gas',
            'Manufacturing Gas, Coal, Coke, HGL',
            'Distillate, Gasoline, Other Gas',
            '',
        ),
    ),
    (
        'Other Stationary Combustion',
        ('Residential, Wood', '', '', 'Commercial Fuels', ''),
    ),
    ('Mobile Combustion', ('Road, Rail, Air, Water', '', '', 'Off-Road Equipment', '')),
    (
        'Industrial Processes and Fugitives',
        (
            'Processes, Oil and Gas Systems',
            '',
            '',
            'Lime, Urea, Carbonates',
            'Non-Energy Use',
        ),
    ),
    (
        'Agriculture',
        (
            'Livestock, Manure',
            '',
            '',
            'Fertilizer, Urea',
            'Cropland, Pasture',
        ),
    ),
    (
        'Fluorinated Gases',
        (
            'Producers, Electronics',
            '',
            '',
            'Refrigerants, Foams',
            'Magnesium',
        ),
    ),
    (
        'Waste',
        ('Landfills, Wastewater, Incineration', '', '', 'Fuels', ''),
    ),
)

#: Which route columns each step feeds (the totals feed the rows).
STEP_ROUTES: tuple[tuple[int, ...], ...] = ((0,), (1,), (2,), (3, 4))


def _row_span(
    i: int, n: int, top: float, bottom: float, gap: float
) -> tuple[float, float]:
    h = (top - bottom - (n - 1) * gap) / n
    y1 = top - i * (h + gap)
    return y1 - h, y1


def _draw_sources_and_steps(ax: plt.Axes) -> list[tuple[float, float]]:
    """The source column, the four build steps, and the wiring; returns each
    step's ``(y0, y1)``."""
    entries: dict[int, list[int]] = {s: [] for s in range(len(STEPS))}
    for i, (_, steps) in enumerate(SOURCES):
        for s in steps:
            entries[s].append(i)

    src_mid = []
    for i, (name, _) in enumerate(SOURCES):
        y0, y1 = _row_span(i, len(SOURCES), BAND_TOP, BAND_BOTTOM, 0.6)
        src_mid.append((y0 + y1) / 2)
        hue, dash = _source_style(i)
        _box(
            ax,
            SRC_X,
            y0,
            SRC_W,
            y1 - y0,
            lines=list(name),
            fc=SOURCE_FC,
            ec=hue,
            body_fs=7.0,
            align_middle=True,
            ls=dash,
            lw=1.5,
        )

    spans = []
    for i, (title, lines, hdr) in enumerate(STEPS):
        y0, y1 = _row_span(i, len(STEPS), BAND_TOP, BAND_BOTTOM, 1.7)
        spans.append((y0, y1))
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
            header_fc=hdr,
            title_fs=9.4,
            body_fs=7.6,
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

    # The blend takes the facility shares: step 1 hands on to step 2.
    (f0, _), (_, b1) = spans[1], spans[2]
    mid_x = STEP_X + STEP_W * 0.5
    _arrow(ax, (mid_x, f0), (mid_x, b1 + 0.1), color=ROUTES[1][1], lw=1.4, scale=10)
    return spans


def _draw_attribution_panel(ax: plt.Axes, spans: list[tuple[float, float]]) -> None:
    """The Sector Attribution Model: each family of inventory emissions against
    the routes that place it on industries."""
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
        'Sector Attribution Model',
        fontsize=10.5,
        fontweight='bold',
        color=INK,
        ha='center',
        va='center',
    )

    # Route headers.
    for j, (name, hdr, _) in enumerate(ROUTES):
        x = COL_X0 + j * COL_W
        ax.add_patch(
            FancyBboxPatch(
                (x + 0.15, HEAD_Y0),
                COL_W - 0.3,
                HEAD_Y1 - HEAD_Y0,
                boxstyle='round,pad=0,rounding_size=0.5',
                facecolor=hdr,
                edgecolor=hdr,
                zorder=2,
            )
        )
        ax.text(
            x + COL_W / 2,
            (HEAD_Y0 + HEAD_Y1) / 2,
            _wrap([name], COL_W - 3.2, 7.0),
            fontsize=7.0,
            fontweight='bold',
            color='#ffffff',
            ha='center',
            va='center',
            linespacing=1.3,
            zorder=3,
        )
    ax.text(
        LABEL_X + 0.3,
        (HEAD_Y0 + HEAD_Y1) / 2,
        'Inventory Emissions',
        fontsize=8.2,
        fontweight='bold',
        color=MUTED,
        ha='left',
        va='center',
    )

    # Family rows.
    n = len(FAMILIES)
    for i, (family, cells) in enumerate(FAMILIES):
        y0, y1 = _row_span(i, n, ROWS_Y1, PANEL_Y0 + 1.0, ROW_GAP)
        ax.add_patch(
            Rectangle(
                (LABEL_X, y0),
                LABEL_W,
                y1 - y0,
                facecolor='#eef1f3',
                edgecolor='#c8d0d6',
                linewidth=0.8,
                zorder=2,
            )
        )
        ax.text(
            LABEL_X + 0.6,
            (y0 + y1) / 2,
            _wrap([family], LABEL_W - 4.0, 7.6),
            fontsize=7.6,
            fontweight='bold',
            color=INK,
            ha='left',
            va='center',
            linespacing=1.25,
            zorder=3,
        )
        for j, label in enumerate(cells):
            x = COL_X0 + j * COL_W
            used = bool(label)
            _, edge, fill = ROUTES[j]
            ax.add_patch(
                Rectangle(
                    (x + 0.15, y0),
                    COL_W - 0.3,
                    y1 - y0,
                    facecolor=fill if used else '#ffffff',
                    edgecolor=edge if used else '#e1e6ea',
                    linewidth=1.0 if used else 0.6,
                    zorder=2,
                )
            )
            if used:
                ax.text(
                    x + COL_W / 2,
                    (y0 + y1) / 2,
                    _wrap([label], COL_W - 3.4, 6.4),
                    fontsize=6.4,
                    color=INK,
                    ha='center',
                    va='center',
                    linespacing=1.2,
                    zorder=3,
                )

    # Each step into the columns it fills, over the top of the panel; the
    # totals into the family rows.
    exit_x = (42.0, 42.8, 43.6, 44.4)
    over_y = (95.4, 95.4, 95.4, 95.4)
    for i, ((y0, y1), routes) in enumerate(zip(spans, STEP_ROUTES)):
        hdr = STEPS[i][2]
        start = (STEP_X + STEP_W, (y0 + y1) / 2)
        if i == 0:
            rows_mid = (ROWS_Y1 + PANEL_Y0 + 1.0) / 2
            _elbow(
                ax,
                [start, (45.2, start[1]), (45.2, rows_mid), (LABEL_X - 0.1, rows_mid)],
                color=hdr,
                lw=1.6,
                scale=11,
            )
            continue
        for j in routes:
            cx = COL_X0 + j * COL_W + COL_W / 2
            _elbow(
                ax,
                [
                    start,
                    (exit_x[i], start[1]),
                    (exit_x[i], over_y[i] - 0.5 * i + 0.5),
                    (cx, over_y[i] - 0.5 * i + 0.5),
                    (cx, HEAD_Y1 + 0.1),
                ],
                color=hdr,
                lw=1.6,
                scale=11,
            )


def _draw_chain(ax: plt.Axes) -> None:
    """From the Flow-By-Sector to the model's factors."""
    y0, h = 7.5, 15.0
    boxes: tuple[tuple[str, tuple[str, ...], float, float], ...] = (
        (
            'GHG Flow-By-Sector',
            ('·  Emissions by Gas, Industry and Source',),
            3.0,
            15.0,
        ),
        (
            'Model Industries',
            ('·  Map to the Cornerstone Schema', '·  Re-Split Waste Fuel Emissions'),
            21.5,
            16.0,
        ),
        (
            'Characterization',
            ('·  100-Year GWPs, AR6', '·  Fossil and Non-Fossil Methane'),
            41.0,
            15.0,
        ),
        ('E', ('·  Emissions by Industry',), 59.5, 11.5),
        ('B', ('·  Emissions per Dollar of Output',), 74.5, 11.5),
        ('D and N', ('·  Direct and Total Factors by Commodity',), 89.5, 9.5),
    )
    hdrs = (ROUTES[0][1], FLOW, FLOW, FLOW, FLOW, FLOW)
    for (title, lines, x, w), hdr in zip(boxes, hdrs):
        _box(
            ax,
            x,
            y0,
            w,
            h,
            title=title,
            lines=lines,
            fc=STEP_FC,
            ec=STEP_EC,
            header_fc=hdr,
            title_fs=9.2,
            body_fs=7.4,
            align_middle=True,
        )
    for (_, _, x, w), (_, _, nx, _) in zip(boxes, boxes[1:]):
        _arrow(
            ax,
            (x + w + 0.1, y0 + h / 2 - 1.5),
            (nx - 0.15, y0 + h / 2 - 1.5),
            lw=1.6,
            scale=12,
        )

    # The panel's product comes down into the Flow-By-Sector.
    _elbow(
        ax,
        [
            (PANEL_X + PANEL_W / 2, PANEL_Y0),
            (PANEL_X + PANEL_W / 2, 43.2),
            (6.5, 43.2),
            (6.5, y0 + h + 0.1),
        ],
        lw=2.0,
        scale=14,
    )

    # What the chain is held to, and the tables it reads.
    _box(
        ax,
        9.5,
        27.0,
        16.0,
        11.0,
        title='Checks',
        lines=(
            '·  Totals by Gas Match the Inventory',
            '·  Facility Totals Are a Floor',
        ),
        fc=CONSTRAINT_FC,
        ec=CONSTRAINT_EC,
        title_fs=8.8,
        body_fs=7.2,
    )
    _elbow(
        ax,
        [(17.5, 27.0), (17.5, 24.8), (13.0, 24.8), (13.0, y0 + h + 0.1)],
        color=CONSTRAINT_EC,
        lw=1.4,
        scale=10,
    )
    _box(
        ax,
        41.0,
        29.0,
        44.5,
        6.0,
        lines=['Cornerstone Nowcast Make and Use Tables'],
        fc=SOURCE_FC,
        ec=WIRE,
        body_fs=7.6,
        align_middle=True,
    )
    for tx in (29.5, 80.25):
        _elbow(
            ax,
            [
                (tx if tx > 41 else 43.0, 29.0),
                (tx if tx > 41 else 43.0, 26.0),
                (tx, 26.0),
                (tx, y0 + h + 0.1),
            ],
            color=WIRE,
            lw=1.3,
            scale=10,
        )


def _draw_legend(ax: plt.Axes) -> None:
    y = 2.2
    items: tuple[tuple[str, str, str, ta.Any], ...] = (
        ('Input Data Source', SOURCE_FC, '#8ba4b8', 'solid'),
        ('Build Step', STEP_FC, STEP_EC, 'solid'),
        ('Check on the Result', CONSTRAINT_FC, CONSTRAINT_EC, 'solid'),
    )
    x = 1.0
    for label, fc, ec, ls in items:
        ax.add_patch(
            FancyBboxPatch(
                (x, y - 0.9),
                2.4,
                1.8,
                boxstyle='round,pad=0,rounding_size=0.4',
                facecolor=fc,
                edgecolor=ec,
                linestyle=ls,
                linewidth=1.1,
            )
        )
        ax.text(x + 3.0, y, label, fontsize=8.2, color=INK, va='center')
        x += 16.0
    ax.text(
        x,
        y,
        'A step header takes the colour of the attribution route it fills.',
        fontsize=8.2,
        color=MUTED,
        va='center',
    )


def draw(path: Path, dpi: int = 150) -> Path:
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.set_xlim(*XLIM)
    ax.set_ylim(*YLIM)
    ax.axis('off')
    spans = _draw_sources_and_steps(ax)
    _draw_attribution_panel(ax, spans)
    _draw_chain(ax)
    _draw_legend(ax)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=dpi, bbox_inches='tight', facecolor='#ffffff')
    plt.close(fig)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    parser.add_argument('--dpi', type=int, default=150)
    parser.add_argument('--out', type=Path, default=IMAGE_DIR / FIGURE_NAME)
    args = parser.parse_args()
    print(draw(args.out, dpi=args.dpi))


if __name__ == '__main__':
    main()
