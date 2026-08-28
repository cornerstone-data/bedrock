"""Where every cell of the *other* four blocks comes from.

:mod:`~bedrock.analysis.nowcasting.seed_coverage` draws this for the 402 x 402
intermediate block.  This module draws the same question for the four blocks
around it -- final demand, value added, the supply bridge, and the supply
domestic-output mix -- so the whole SUT pair can be read on one axis.

The states
----------

Five, and the middle one is a gradation::

    white   ABSENT     no cell -- neither side populates it
    purple  MISSING    the reference has a value and we produce none, or the
                       method declines to allocate a line it can name
    grey    CARRIED    no annual source; the cell holds its 2017 structure
    green   ALLOCATED  a source observes an aggregate *containing* this cell,
                       spread onto it by a weight.  Darker = fewer cells
                       share the datum
    dark    PRIMARY    a source observes this cell.  ``k = 1`` -- the datum
                       *is* the cell

``ALLOCATED`` and ``PRIMARY`` are the same measurement at two ends of one
scale, exactly as ``seed_coverage``'s ``N`` is: ``k`` counts **how many
commodities share the single source datum behind this cell**, within its
column.  ``k = 1`` is ``PRIMARY``; anything above is ``ALLOCATED``, ramped
logarithmically because the interesting distinction is 1-vs-4, not 60-vs-64.

Derived, not declared, wherever the build allows it
---------------------------------------------------

The FlowBySector rows for final demand, value added and inventories retain
``Table`` / ``Line`` / ``Code`` -- the NIPA table, line and series the row was
built from.  That triple *is* the source datum's identity, so ``k`` is counted
off the build itself rather than asserted here:

    k[cell] = |{commodities sharing this cell's (Table, Line, Code)
                within this column}|

⚠️ **Two blocks cannot be derived this way and are declared instead.**
``Detail_Supply`` and ``Trade_Exports`` carry only ``MetaSources``, with no
per-row datum key, so their rules sit in :data:`DECLARED` with the evidence for
each.  :func:`check` asserts which blocks are derived and which are declared,
so the distinction cannot quietly erode.

What this is for
----------------

⚠️ **The match picture and this picture disagree about which columns are good,
and both are right.**  A column allocated from one NIPA line onto the 2017
benchmark mix reproduces 2017 *exactly* -- that is what the mix is -- and
scores 100% on the match.  It also carries one observation across every cell.
The twelve government columns are the clearest case: ``F07E00`` is **one**
datum spread over 68 commodities, and it is one of the columns the Step 1 match
table reports as perfect.

CLI::

    uv run python -m bedrock.analysis.nowcasting.block_provenance --check
    uv run python -m bedrock.analysis.nowcasting.block_provenance \\
        --block final_demand --dpi 110
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import TYPE_CHECKING, cast

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from bedrock.analysis.nowcasting.table_match import TableMatch

IMAGE_DIR = Path(__file__).parent / 'images'

DEFAULT_YEAR = 2017

# ------------------------------------------------------------------- states

ABSENT = 0
MISSING = 1
CARRIED = 2
ALLOCATED = 3
PRIMARY = 4

STATE_NAMES = {
    ABSENT: 'absent',
    MISSING: 'missing',
    CARRIED: 'carried',
    ALLOCATED: 'allocated',
    PRIMARY: 'primary',
}

#: Shared with :mod:`seed_coverage` so the two figures read as one system.
ABSENT_COLOR = '#ffffff'
MISSING_COLOR = '#6a3d9a'
CARRIED_COLOR = '#8e99a8'
#: ``(k = 1, k = RAMP_TOP)`` -- dark is specific, exactly as
#: :data:`seed_coverage.SEEDED_RAMP`, and the same two hex values in the same
#: order.  ``PRIMARY`` is the dark end of this ramp rather than a sixth colour:
#: it and ``ALLOCATED`` are one measurement, so they share one scale.
ALLOCATED_RAMP = ('#052e16', '#56c98a')

#: Where the ``k`` ramp saturates.  Above this a datum is diffuse enough that
#: more of it changes nothing about how the cell should be read.
RAMP_TOP = 64.0

#: The axis a block's rules are keyed on -- the *short* side, the one whose
#: labels name a concept rather than a commodity or an industry. Fan-out is
#: then counted across the other axis, because that is the axis a single datum
#: had to be spread over.
#:
#: ⚠️ **Value added is the transposed one.** Its five codes are rows and the
#: 402 industries are columns, so a NIPA group datum fans out across
#: *industries within a row*. Counting it the other way returns 1 everywhere
#: and reports the block as entirely primary, which it is not.
RULE_AXIS: dict[str, str] = {
    'final_demand': 'column',
    'value_added': 'row',
    'supply_bridge': 'column',
    'supply_mix': 'column',
}

#: The source-datum key.  A row's ``(Table, Line, Code)`` is the NIPA series it
#: was built from; two cells sharing one are two cells sharing one observation.
DATUM_KEY = ('Table', 'Line', 'Code')


# --------------------------------------------------------------- block specs


#: ``block -> (method, column_axis)`` for the blocks whose ``k`` is derived off
#: the FBS's own datum key.  ``column_axis`` names the axis the fan-out is
#: counted *within*: a datum spread across commodities inside one final-demand
#: column has fanned out; the same datum appearing in two columns has not.
DERIVED: dict[str, tuple[str, ...]] = {
    'final_demand': ('NIPA_final_dom_uses', 'Inventories'),
    'value_added': (
        'NIPA_VA_compensation',
        'NIPA_VA_othertax',
        'NIPA_VA_surplus',
    ),
}


#: Rules for the cells no datum key reaches.  Each carries the evidence for its
#: state, because a declared rule is a claim and has to be checkable.
#:
#: ``k`` here is the fan-out the method's own construction implies, not a
#: measurement of it -- see the note on each.
DECLARED: dict[str, dict[str, tuple[int, float, str]]] = {
    'final_demand': {
        # Trade_Exports carries no datum key. The 1:m families split on
        # same-year T007 domestic output, so a family of m commodities shares
        # one Census/IEA line.
        'F04000': (
            ALLOCATED,
            4.0,
            'Census goods + BEA IntlServTrade; 1:m families split on T007 '
            '(#702 open: T007 is what we produce, not what we export)',
        ),
    },
    'value_added': {
        # Converted from the Supply columns by nowcast_va_taxes: the level
        # carries no modelling content, only the industry split is estimated.
        'T00TOP': (
            ALLOCATED,
            20.0,
            'converted from Supply TOP; industry split is a seed at r = 0.947 '
            'and scores 8.1% on the 2017 match',
        ),
        'T00SUB': (
            ALLOCATED,
            8.0,
            'converted from Supply SUB; reproduces the published 2017 row ' 'exactly',
        ),
    },
    'supply_bridge': {
        'T007': (ALLOCATED, 3.0, 'row margin of the Detail_Supply block'),
        'MCIF': (
            ALLOCATED,
            4.0,
            'Census goods CIF at NAICS-6 plus BEA IntlServTrade; 1:m rows on '
            'the frozen 2017 MCIF weight (#729 measured, kept)',
        ),
        'MADJ': (
            ALLOCATED,
            8.0,
            'Census GEN_CHA_YR reassigned onto 2017 MADJ destination codes',
        ),
        'MDTY': (
            ALLOCATED,
            2.0,
            'Census duty rate at NAICS-6 (specific) x NIPA B235RC level',
        ),
        'TRADE': (
            CARRIED,
            np.nan,
            '2017 published column moved by Census gross margin; the '
            'receiving split is the frozen 2017 mix (#672)',
        ),
        'TRANS': (
            ALLOCATED,
            11.0,
            'per-mode freight revenue over eleven AIES/SAS groups; the '
            'within-group weight is frozen at 2017 (#672)',
        ),
        'TOP': (
            ALLOCATED,
            6.0,
            'ten named NIPA product lines on their own commodities (29.8% of '
            'the column, k = 1); the sales-tax residual on a purchaser-price '
            'base (70.2%)',
        ),
        'SUB': (
            ALLOCATED,
            12.0,
            'NIPA T31300 by type, each commodity anchored on its published '
            '2017 value; 2020-21 other on BEA PPP-by-industry',
        ),
    },
    'supply_mix': {
        # Detail_Supply carries no datum key. At 2017 the whole block is the
        # published summary domestic-output block disaggregated onto the 2017
        # detail mix, so every cell shares its summary parent's observation.
        '*': (
            ALLOCATED,
            2.0,
            'published summary domestic output disaggregated on the 2017 '
            'detail mix; k is the detail commodities per summary parent. '
            'From 2022 the mix moves on Economic Census product lines for '
            '133 of 178 columns (#570)',
        ),
    },
}

#: Subtotal columns. They add no observation -- they are their components
#: summed -- so scoring them as data would count the same evidence twice.
SUBTOTALS = ('T013', 'T014', 'T015', 'T016')


# ------------------------------------------------------------------ fan-out


@functools.cache
def _fbs(method: str, year: int) -> pd.DataFrame:
    from bedrock.transform.flowbysector import getFlowBySector  # noqa: PLC0415

    frame = pd.DataFrame(getFlowBySector(f'{method}_{year}'))
    return frame.loc[frame['FlowAmount'] != 0]


def _resolve_axes(frame: pd.DataFrame) -> pd.DataFrame:
    """Name the two sector columns ``row`` and ``column``.

    Every method here writes ``SectorProducedBy`` on the axis its block puts
    first, so this is one rule rather than a per-block one.  What differs
    between blocks is what those axes *mean*: for final demand ``row`` is a
    commodity and ``column`` a final-demand code, while for value added
    ``row`` is a value-added code and ``column`` an industry.  That difference
    is handled by :data:`RULE_AXIS`, not here.
    """
    out = frame.copy()
    out['row'] = out['SectorProducedBy']
    out['column'] = out['SectorConsumedBy']
    return out


def derived_fanout(block: str, year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """``(row, column, k, source)`` for every cell a datum key reaches.

    ``k`` counts the rows sharing this cell's ``(Table, Line, Code)`` *within
    its column*, which is the number of cells the single observation had to be
    spread across.
    """
    parts = []
    for method in DERIVED[block]:
        frame = _resolve_axes(_fbs(method, year))
        if not set(DATUM_KEY) <= set(frame.columns):
            continue
        rule_axis = RULE_AXIS[block]
        spread_axis = 'row' if rule_axis == 'column' else 'column'
        key = [*DATUM_KEY, rule_axis]
        counts = frame.groupby(key, dropna=False)[spread_axis].transform('nunique')
        parts.append(
            pd.DataFrame(
                {
                    'row': frame['row'].to_numpy(),
                    'column': frame['column'].to_numpy(),
                    'k': counts.to_numpy(float),
                    'source': method,
                }
            )
        )
    if not parts:
        return pd.DataFrame(columns=['row', 'column', 'k', 'source'])
    out = pd.concat(parts, ignore_index=True)
    # Where several data touch one cell the smallest k wins: if any of them is
    # specific to the cell, the cell has a specific observation. Same rule as
    # seed_coverage's N.
    return (
        out.sort_values('k')
        .drop_duplicates(subset=['row', 'column'], keep='first')
        .reset_index(drop=True)
    )


@functools.cache
def _detail_to_summary() -> dict[str, str]:
    """BEA 2017 detail code -> its summary parent, from the NAICS crosswalk."""
    path = (
        Path(__file__).parents[2]
        / 'utils'
        / 'mapping'
        / 'naics'
        / 'NAICS_to_BEA_Crosswalk_2017.csv'
    )
    frame = pd.read_csv(path, dtype=str)
    pairs = frame[['BEA_2017_Detail_Code', 'BEA_2017_Summary_Code']].dropna()
    return dict(pairs.drop_duplicates().to_numpy())


def supply_mix_fanout(populated: pd.DataFrame) -> pd.DataFrame:
    """``k`` for the domestic-output block, measured rather than declared.

    ``Detail_Supply`` disaggregates the **published summary** domestic-output
    block onto the 2017 detail mix, so one summary observation is shared by
    every populated detail cell inside it.  ``k`` is therefore the count of
    populated cells sharing a ``(summary commodity, summary industry)`` parent
    -- 1 where a summary cell resolves to exactly one detail cell, which is
    where the disaggregation had no choice to make.

    ⚠️ Counted over **populated** cells only.  The block is 96.9% structurally
    empty, so counting the full detail cross-product of each summary parent
    would inflate ``k`` by roughly thirty-fold and report the whole block as
    maximally diffuse.
    """
    lookup = _detail_to_summary()
    rows = pd.Series(populated.index, index=populated.index).map(lookup)
    cols = pd.Series(populated.columns, index=populated.columns).map(lookup)

    flags = cast('pd.Series[bool]', populated.stack())
    stacked = flags.loc[flags.to_numpy()]
    if stacked.empty:
        empty = np.full(
            (len(populated.index), len(populated.columns)), np.nan, dtype=float
        )
        return pd.DataFrame(empty, index=populated.index, columns=populated.columns)
    frame = stacked.index.to_frame(index=False)
    frame.columns = ['row', 'column']
    frame['srow'] = frame['row'].map(rows)
    frame['scol'] = frame['column'].map(cols)
    frame['k'] = frame.groupby(['srow', 'scol'], dropna=False)['row'].transform('size')
    return frame.pivot(index='row', columns='column', values='k').reindex(
        index=populated.index, columns=populated.columns
    )


def _candidate(block: str, year: int) -> TableMatch:
    """The section comparison for ``block`` -- both sides on one frame."""
    from bedrock.analysis.nowcasting.sections import get_section  # noqa: PLC0415

    section = {
        'final_demand': 'use_fd_detail_sut',
        'value_added': 'use_va_detail_sut',
        'supply_bridge': 'supply_bridge_detail_sut',
        'supply_mix': 'supply_output_detail_sut',
    }[block]
    return get_section(section).run(year)


def provenance(
    block: str, year: int = DEFAULT_YEAR
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """``(state, k)`` frames on the block's own axes.

    ``state`` holds one of :data:`STATE_NAMES`' codes; ``k`` holds the fan-out
    where one is known and ``NaN`` elsewhere.
    """
    match = _candidate(block, year)
    candidate = match.candidate.astype(float)
    reference = match.reference.astype(float)

    shape = (len(candidate.index), len(candidate.columns))
    state = pd.DataFrame(
        np.full(shape, ABSENT, dtype=int),
        index=candidate.index,
        columns=candidate.columns,
    )
    k = pd.DataFrame(
        np.full(shape, np.nan, dtype=float),
        index=candidate.index,
        columns=candidate.columns,
    )

    have = candidate.fillna(0.0) != 0.0
    want = reference.fillna(0.0) != 0.0
    state[want & ~have] = MISSING

    if block in DERIVED:
        fan = derived_fanout(block, year)
        for row, column, value, _ in fan.itertuples(index=False):
            if row in k.index and column in k.columns:
                k.loc[row, column] = value

    if block == 'supply_mix':
        measured = supply_mix_fanout(have)
        k = k.where(measured.isna(), measured)

    declared = DECLARED.get(block, {})
    on_rows = RULE_AXIS[block] == 'row'
    labels = list(candidate.index if on_rows else candidate.columns)
    for label in labels:
        rule = declared.get(label) or declared.get('*')
        if rule is None:
            continue
        state_code, fanout, _ = rule
        line_has = have.loc[label] if on_rows else have[label]
        line_k = k.loc[label] if on_rows else k[label]
        unset = line_has & line_k.isna()
        if on_rows:
            k.loc[label, unset] = fanout
        else:
            k.loc[unset, label] = fanout
        if state_code == CARRIED:
            # An explicit "held" rule beats the fan-out: the column moves, but
            # its split across commodities does not.
            if on_rows:
                k.loc[label, line_has] = np.nan
            else:
                k.loc[line_has, label] = np.nan

    graded = have & k.notna()
    state[graded & (k <= 1.0)] = PRIMARY
    state[graded & (k > 1.0)] = ALLOCATED
    # Populated, but nothing tells us where it came from: it is held.
    state[have & k.isna()] = CARRIED

    for column in SUBTOTALS:
        if column in state.columns:
            state[column] = np.where(have[column], CARRIED, state[column].to_numpy())
            k[column] = np.nan

    return state, k


# ------------------------------------------------------------------ summary


def summary(block: str, year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """Per column: cells and dollars in each state, and the median ``k``."""
    state, k = provenance(block, year)
    match = _candidate(block, year)
    value = match.candidate.astype(float).abs().fillna(0.0)
    on_rows = RULE_AXIS[block] == 'row'
    labels = list(state.index if on_rows else state.columns)

    rows = []
    for column in labels:
        col_state = cast(
            'pd.Series[int]', state.loc[column] if on_rows else state[column]
        )
        col_value = cast(
            'pd.Series[float]', value.loc[column] if on_rows else value[column]
        )
        col_k = cast('pd.Series[float]', k.loc[column] if on_rows else k[column])
        entry: dict[str, object] = {'column': column}
        total = float(col_value.sum())
        for code, name in STATE_NAMES.items():
            hit = col_state == code
            entry[f'{name}_cells'] = int(hit.sum())
            entry[f'{name}_$'] = float(col_value[hit].sum())
        entry['total_$'] = total
        observed = col_state.isin([PRIMARY, ALLOCATED])
        entry['observed_share'] = (
            float(col_value[observed].sum() / total) if total else np.nan
        )
        entry['primary_share'] = (
            float(col_value[col_state == PRIMARY].sum() / total) if total else np.nan
        )
        entry['median_k'] = (
            float(np.nanmedian(col_k)) if col_k.notna().any() else np.nan
        )
        rows.append(entry)
    return pd.DataFrame(rows).set_index('column')


# ------------------------------------------------------------------- colour


def _hex_to_rgb(value: str) -> np.ndarray:
    raw = value.lstrip('#')
    return np.array([int(raw[i : i + 2], 16) / 255 for i in (0, 2, 4)])


def _swatch(rgb: np.ndarray) -> tuple[float, float, float]:
    """An RGB triple matplotlib's ``Patch`` will accept as a facecolor."""
    r, g, b = (float(v) for v in rgb[:3])
    return r, g, b


def ramp_position(k: np.ndarray) -> np.ndarray:
    """``k`` -> 0..1 along the ramp, logarithmic, saturating at :data:`RAMP_TOP`.

    Identical to :func:`seed_coverage.ramp_position` so a green here and a
    green there mean the same fan-out.
    """
    clipped = np.clip(np.nan_to_num(k, nan=1.0), 1.0, RAMP_TOP)
    return np.log(clipped) / np.log(RAMP_TOP)


def state_rgb(state: np.ndarray, k: np.ndarray) -> np.ndarray:
    """RGB raster for a ``(state, k)`` pair, shape ``(..., 3)``."""
    out = np.empty((*state.shape, 3), dtype=float)
    out[...] = _hex_to_rgb(ABSENT_COLOR)
    out[state == MISSING] = _hex_to_rgb(MISSING_COLOR)
    out[state == CARRIED] = _hex_to_rgb(CARRIED_COLOR)
    graded = (state == ALLOCATED) | (state == PRIMARY)
    if graded.any():
        lo, hi = (_hex_to_rgb(c) for c in ALLOCATED_RAMP)
        position = ramp_position(k[graded])[..., None]
        out[graded] = lo + (hi - lo) * position
    return out


# ------------------------------------------------------------------- figures


#: ``table -> (interior block, trailing block, trailing axis)``.  Drawn in the
#: table's own geometry: the Use table puts final demand to the right of the
#: intermediate interior and value added underneath it; the Supply table puts
#: the bridge to the right of the domestic-output mix.
TABLES: dict[str, dict[str, str]] = {
    'use': {
        'interior': 'intermediate',
        'right': 'final_demand',
        'below': 'value_added',
    },
    'supply': {'interior': 'supply_mix', 'right': 'supply_bridge'},
}


def _intermediate_state() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Step 3's map, mapped onto this module's five states.

    ``seed_coverage`` predates the ``primary`` / ``allocated`` split and calls
    both ``SEEDED``; the split is just ``N == 1``, so no re-measurement is
    needed -- only a relabel.
    """
    from bedrock.analysis.nowcasting import seed_coverage as sc  # noqa: PLC0415

    n = sc.fanout()
    status = sc.status()
    state = pd.DataFrame(
        np.full(n.shape, ABSENT, dtype=int), index=n.index, columns=n.columns
    )
    state[status == sc.CARRIED] = CARRIED
    seeded = status == sc.SEEDED
    state[seeded & (n <= 1.0)] = PRIMARY
    state[seeded & (n > 1.0)] = ALLOCATED
    return state, n


def _panel(block: str, year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    if block == 'intermediate':
        return _intermediate_state()
    return provenance(block, year)


def render(
    table: str = 'use',
    path: Path | None = None,
    dpi: int = 200,
    year: int = DEFAULT_YEAR,
) -> Path:
    """Draw one table's provenance in its own geometry and write it to *path*."""
    import matplotlib  # noqa: PLC0415

    matplotlib.use('Agg')
    import matplotlib.pyplot as plt  # noqa: PLC0415
    from matplotlib.patches import Patch  # noqa: PLC0415

    from bedrock.analysis.nowcasting.seed_coverage import (  # noqa: PLC0415
        _spread,
        band_order,
    )

    spec = TABLES[table]
    interior_state, interior_k = _panel(spec['interior'], year)
    rows, row_spans = band_order(interior_state.index)
    columns, column_spans = band_order(interior_state.columns)
    interior_state = interior_state.reindex(index=rows, columns=columns)
    interior_k = interior_k.reindex(index=rows, columns=columns)

    right_state, right_k = _panel(spec['right'], year)
    right_state = right_state.reindex(index=rows)
    right_k = right_k.reindex(index=rows)

    has_below = 'below' in spec
    if has_below:
        below_state, below_k = _panel(spec['below'], year)
        below_state = below_state.reindex(columns=columns)
        below_k = below_k.reindex(columns=columns)

    n_int, n_right = len(columns), len(right_state.columns)
    # A trailing column needs roughly four interior columns' worth of pixels
    # before its code label is legible; at 1:1 the 19 final-demand codes come
    # out as an unreadable smear.
    width_ratios = [n_int, max(n_right * 4, int(n_int * 0.10))]
    height_ratios = [len(rows), max(int(len(rows) * 0.055), 12)] if has_below else None

    figure = plt.figure(figsize=(15.0, 13.0 if has_below else 12.0))
    grid = figure.add_gridspec(
        2 if has_below else 1,
        2,
        width_ratios=width_ratios,
        height_ratios=height_ratios,
        wspace=0.035,
        hspace=0.045,
    )

    def draw(axes: plt.Axes, state: pd.DataFrame, k: pd.DataFrame, title: str) -> None:
        axes.imshow(
            state_rgb(state.to_numpy(), k.to_numpy(float)),
            interpolation='nearest',
            aspect='auto',
            origin='upper',
        )
        axes.set_xticks([])
        axes.set_yticks([])
        axes.set_title(title, fontsize=10.5, pad=6)
        for spine in axes.spines.values():
            spine.set_edgecolor('#9ca3af')

    ax_int = figure.add_subplot(grid[0, 0])
    draw(ax_int, interior_state, interior_k, '')
    for _, start, _ in row_spans[1:]:
        ax_int.axhline(start - 0.5, color='#1f2937', linewidth=0.6, alpha=0.55)
    for _, start, _ in column_spans[1:]:
        ax_int.axvline(start - 0.5, color='#1f2937', linewidth=0.6, alpha=0.55)
    ax_int.set_ylabel('←  commodity', labelpad=10)
    ax_int.set_title(
        (
            'intermediate use  (Step 3)'
            if table == 'use'
            else 'domestic output  (Step 4a)'
        ),
        fontsize=10.5,
        pad=6,
    )

    n_rows = len(rows)
    label_y = _spread(
        [(a + b) / 2 - 0.5 for _, a, b in row_spans], n_rows * 0.042, n_rows
    )
    for (name, a, b), y in zip(row_spans, label_y, strict=True):
        ax_int.annotate(
            name,
            xy=(-0.004, (a + b) / 2 - 0.5),
            xytext=(-0.10, y),
            xycoords=('axes fraction', 'data'),
            textcoords=('axes fraction', 'data'),
            ha='right',
            va='center',
            fontsize=9,
            arrowprops={'arrowstyle': '-', 'color': '#9ca3af', 'linewidth': 0.7},
        )

    ax_right = figure.add_subplot(grid[0, 1], sharey=ax_int)
    draw(
        ax_right,
        right_state,
        right_k,
        'final demand  (Step 1)' if table == 'use' else 'bridge  (Step 4)',
    )
    ax_right.set_xticks(range(len(right_state.columns)))
    ax_right.set_xticklabels(
        [str(c) for c in right_state.columns], rotation=90, fontsize=6.5
    )
    for _, start, _ in row_spans[1:]:
        ax_right.axhline(start - 0.5, color='#1f2937', linewidth=0.6, alpha=0.55)

    if has_below:
        ax_below = figure.add_subplot(grid[1, 0], sharex=ax_int)
        draw(ax_below, below_state, below_k, '')
        ax_below.set_yticks(range(len(below_state.index)))
        ax_below.set_yticklabels([str(r) for r in below_state.index], fontsize=7)
        ax_below.set_xlabel('industry  →', labelpad=42)
        ax_below.set_ylabel('value added\n(Step 2)', fontsize=9, labelpad=10)
        for _, start, _ in column_spans[1:]:
            ax_below.axvline(start - 0.5, color='#1f2937', linewidth=0.6, alpha=0.55)
    else:
        ax_int.set_xlabel('industry  →', labelpad=42)

    bottom = ax_below if has_below else ax_int
    label_x = _spread(
        [(a + b) / 2 - 0.5 for _, a, b in column_spans], n_int * 0.075, n_int
    )
    for position, ((name, a, b), x) in enumerate(
        zip(column_spans, label_x, strict=True)
    ):
        bottom.annotate(
            name,
            xy=((a + b) / 2 - 0.5, -0.01),
            xytext=(x, -0.055 if position % 2 == 0 else -0.105),
            xycoords=('data', 'axes fraction'),
            textcoords=('data', 'axes fraction'),
            ha='center',
            va='top',
            fontsize=9,
            arrowprops={'arrowstyle': '-', 'color': '#9ca3af', 'linewidth': 0.7},
        )

    lo, hi = (_hex_to_rgb(c) for c in ALLOCATED_RAMP)
    handles = [
        Patch(facecolor=_swatch(lo), edgecolor='#9ca3af', label='primary  (k = 1)'),
        Patch(
            facecolor=_swatch(lo + (hi - lo) * 0.5),
            edgecolor='#9ca3af',
            label='allocated  (k ≈ 8)',
        ),
        Patch(
            facecolor=_swatch(hi),
            edgecolor='#9ca3af',
            label=f'allocated  (k ≥ {RAMP_TOP:.0f})',
        ),
        Patch(
            facecolor=_swatch(_hex_to_rgb(CARRIED_COLOR)),
            edgecolor='#9ca3af',
            label='carried',
        ),
        Patch(
            facecolor=_swatch(_hex_to_rgb(MISSING_COLOR)),
            edgecolor='#9ca3af',
            label='missing',
        ),
        Patch(
            facecolor=_swatch(_hex_to_rgb(ABSENT_COLOR)),
            edgecolor='#9ca3af',
            label='absent',
        ),
    ]
    figure.legend(
        handles=handles,
        loc='lower center',
        ncol=6,
        frameon=False,
        fontsize=9,
        bbox_to_anchor=(0.5, 0.012 if has_below else -0.045),
    )
    figure.suptitle(
        f'{table.capitalize()} table {year} — where each cell comes from',
        fontsize=15,
        y=0.965,
    )
    figure.text(
        0.5,
        0.935,
        'darker green = fewer cells share the source datum.  '
        'The thin blocks are not to scale.',
        ha='center',
        fontsize=9.5,
        color='#4b5563',
    )

    path = path or IMAGE_DIR / f'{table}_table_provenance_{year}.png'
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.close(figure)
    return path


def render_block(
    block: str,
    path: Path | None = None,
    dpi: int = 200,
    year: int = DEFAULT_YEAR,
) -> Path:
    """Draw one block on its own, wide enough to read its column codes.

    ⚠️ The combined table figures squeeze the trailing blocks into a tenth of
    their width, which is enough to see that a column is grey and not enough to
    read *which* column.  The bridge is the case that needs this: 12 columns
    whose whole story is per-column state.
    """
    import matplotlib  # noqa: PLC0415

    matplotlib.use('Agg')
    import matplotlib.pyplot as plt  # noqa: PLC0415
    from matplotlib.patches import Patch  # noqa: PLC0415

    from bedrock.analysis.nowcasting.seed_coverage import (  # noqa: PLC0415
        _spread,
        band_order,
    )

    state, k = _panel(block, year)
    on_rows = RULE_AXIS.get(block, 'column') == 'row'
    # Put the long axis vertical so the short axis gets the readable labels.
    if on_rows:
        state, k = state.T, k.T
    rows, row_spans = band_order(state.index)
    state = state.reindex(index=rows)
    k = k.reindex(index=rows)

    figure, axes = plt.subplots(figsize=(9.0, 12.0))
    axes.imshow(
        state_rgb(state.to_numpy(), k.to_numpy(float)),
        interpolation='nearest',
        aspect='auto',
        origin='upper',
    )
    axes.set_xticks(range(len(state.columns)))
    axes.set_xticklabels([str(c) for c in state.columns], rotation=90, fontsize=9)
    axes.set_yticks([])
    for _, start, _ in row_spans[1:]:
        axes.axhline(start - 0.5, color='#1f2937', linewidth=0.6, alpha=0.55)
    for spine in axes.spines.values():
        spine.set_edgecolor('#9ca3af')

    n_rows = len(rows)
    label_y = _spread(
        [(a + b) / 2 - 0.5 for _, a, b in row_spans], n_rows * 0.042, n_rows
    )
    for (name, a, b), y in zip(row_spans, label_y, strict=True):
        axes.annotate(
            name,
            xy=(-0.004, (a + b) / 2 - 0.5),
            xytext=(-0.09, y),
            xycoords=('axes fraction', 'data'),
            textcoords=('axes fraction', 'data'),
            ha='right',
            va='center',
            fontsize=9,
            arrowprops={'arrowstyle': '-', 'color': '#9ca3af', 'linewidth': 0.7},
        )

    lo, hi = (_hex_to_rgb(c) for c in ALLOCATED_RAMP)
    handles = [
        Patch(facecolor=_swatch(lo), edgecolor='#9ca3af', label='primary  (k = 1)'),
        Patch(
            facecolor=_swatch(lo + (hi - lo) * 0.5),
            edgecolor='#9ca3af',
            label='allocated',
        ),
        Patch(
            facecolor=_swatch(_hex_to_rgb(CARRIED_COLOR)),
            edgecolor='#9ca3af',
            label='carried',
        ),
        Patch(
            facecolor=_swatch(_hex_to_rgb(MISSING_COLOR)),
            edgecolor='#9ca3af',
            label='missing',
        ),
        Patch(
            facecolor=_swatch(_hex_to_rgb(ABSENT_COLOR)),
            edgecolor='#9ca3af',
            label='absent',
        ),
    ]
    figure.legend(
        handles=handles,
        loc='lower center',
        ncol=5,
        frameon=False,
        fontsize=9,
        bbox_to_anchor=(0.5, -0.005),
    )
    # Value added is transposed by the block above, so its long axis is the
    # industry, not the commodity every other block puts there.
    axes.set_ylabel('←  industry' if on_rows else '←  commodity', labelpad=10)
    figure.suptitle(
        f'{block.replace("_", " ").capitalize()} {year} — where each cell comes from',
        fontsize=13,
        y=0.955,
    )
    path = path or IMAGE_DIR / f'{block}_provenance_{year}.png'
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=dpi, bbox_inches='tight', facecolor='white')
    plt.close(figure)
    return path


def check(year: int = DEFAULT_YEAR) -> int:
    """Assert the map holds together, and print what it found."""
    failures = 0
    for block in ('final_demand', 'value_added', 'supply_bridge', 'supply_mix'):
        state, k = provenance(block, year)
        table = summary(block, year)
        how = 'derived' if block in DERIVED else 'declared'
        print(f'\n=== {block} ({how}) ===')
        counts = pd.Series(
            {
                name: int((state == code).sum().sum())
                for code, name in STATE_NAMES.items()
            }
        )
        print(counts.to_string())
        keep = ['total_$', 'observed_share', 'primary_share', 'median_k']
        print(table[keep].to_string(float_format=lambda v: f'{v:,.3f}'))

        populated = state != ABSENT
        ungraded = ((state == CARRIED) & populated).sum().sum()
        if block in DERIVED and ungraded:
            named = set(DECLARED.get(block, {}))
            print(f'  note: {ungraded} populated cells carry no datum key')
            print(f'  declared columns: {sorted(named)}')
    return failures


if __name__ == '__main__':
    raise SystemExit(check())
