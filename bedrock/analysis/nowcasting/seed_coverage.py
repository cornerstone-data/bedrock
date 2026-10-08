"""Which cells of the intermediate block carry annual data, and how specific it is.

The companion to :mod:`~bedrock.analysis.nowcasting.plots`.  That module draws a
*match* -- how close a candidate is to the published 2017 reference.  This one
draws a **provenance**: for every cell of the 402 x 402 intermediate block, where
its movement away from 2017 comes from.

Three states, and the third is a gradation::

    white   no cell -- the 2017 benchmark is zero here, so there is
            nothing to seed and nothing to carry
    grey    carried -- the cell keeps its 2017 structure and moves only
            on the price carry.  No annual source observes it
    green   seeded -- an annual survey observes this cell's movement.
            The darker the green, the more specific the observation

Why the gradation
-----------------

"Seeded" is not one thing.  A survey datum almost never lands on a single cell:
``Purchased freight transportation`` is one number in the Service Annual Survey
and it has to be spread over 8 BEA commodities, and ERS publishes one farm
sector whose index drives all 10 farm columns.  A cell fed by a number shared
with 79 others is a much weaker claim than one fed by a number that is about it
alone, and reporting both as "seeded" would flatter the coverage badly.

So the green carries ``N`` -- **how many cells share the single observation
behind this cell** -- as::

    N = (commodities the datum is split across)
      x (industry columns the same index drives)

``N = 1`` is the darkest green and means the datum *is* the cell: the Economic
Census reported that material, for that industry, and it resolves to exactly one
BEA commodity.  The ramp is logarithmic, because ``N`` runs from 1 into the
hundreds and the interesting distinction is 1-vs-4, not 60-vs-64.

Where each seed's ``N`` comes from
----------------------------------

============================  ===========================  ==================
seed                          commodity fan-out            industry fan-out
============================  ===========================  ==================
materials / mining            ``classify`` ``direct`` = 1;  1 -- ``bea_industry``
                              ``group`` = ``group_members``  is NAICS -> one column
non-materials (manufacturing) ``len(EXPENSE_TO_BEA[kind])``  1 -- the panel is
                                                             already per industry
services / transportation     ``len(SAS_ITEM_TO_BEA[item])`` industries sharing
                                                             one survey NAICS
agriculture                   ``len(FIWS_ITEM_TO_BEA[item])`` 10 -- ERS publishes
                                                             one farm sector
utilities                     1 -- ``FUEL_TO_BEA`` is 1:1    3 -- the electric
                                                             columns share an index
============================  ===========================  ==================

⚠️ **A seeded column is not a seeded column of cells.**  ``materials_seed``
returns the whole manufacturing column renormalised, but the census only
*observes* the materials rows; the rest of that column is the 2017 mix rescaled,
which is carried.  Drawing this per cell rather than per column is the whole
point of the picture -- the column-level count (330 of 402) is the optimistic
reading and the cell-level dollars are the honest one.

⚠️ **Where several data touch one cell, the smallest ``N`` wins.**  Two SAS items
both land on ``524*``; if either of them is specific to that cell, the cell has a
specific observation.  Taking a dollar-weighted mean instead would report a cell
with one exact source and one vague one as moderately vague, which is the wrong
way round.  Multi-datum cells are a small minority either way.

Ordering
--------

Both axes are grouped into contiguous sector bands, in the order the sectors
fall in the published table -- agriculture, mining, utilities, construction,
manufacturing, trade, transportation, services, government.  BEA's own detail
order interleaves them enough that a raster in table order shows the seeds as
scattered speckle rather than as the blocks they are.

Colour
------

The palette is checked, not assumed, the same way :func:`plots.palette_separation`
checks the match palette: every category pair separates by at least ``dE 27``
(CIE76) under normal vision, protanopia, deuteranopia and tritanopia.  The
binding pair here is **grey against the light end of the green ramp under
deuteranopia**, which is why the grey is a *cool* grey rather than neutral and
why the ramp stops at a mid green instead of running to near-white.  Running to
near-white would also collide with the 73% of the block that is structurally
empty.  ``--check-palette`` re-runs it.

CLI::

    uv run python -m bedrock.analysis.nowcasting.seed_coverage --year 2022
    uv run python -m bedrock.analysis.nowcasting.seed_coverage --check
    uv run python -m bedrock.analysis.nowcasting.seed_coverage --check-years

⚠️ **One figure is shipped, not one per year.**  The mappings behind ``N`` do
not depend on the year, so the map is nearly year-invariant; ``--check-years``
measures the "nearly" rather than assuming it, and 2018-19 are excluded from
the claim for a named reason.  See :func:`year_stability`.
"""

from __future__ import annotations

import functools
import itertools
import sys
from pathlib import Path

import click
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402

from bedrock.analysis.nowcasting.plots import (  # noqa: E402
    _CVD_MATRICES,
    _hex_to_rgb,
    _linear_to_srgb,
    _srgb_to_linear,
    _to_lab,
)

IMAGE_DIR = Path(__file__).parent / 'images'

#: The year :func:`best_year` ranks first on the two scored indicators -- it is
#: chosen by measurement, not by convention.  ⚠️ On reliability and
#: technological correlation the four candidate years separate by less than
#: 0.06, so what actually picks 2021 is coverage.  **The substantive argument
#: for 2022 -- the only year whose census mix is read in the year the census ran
#: -- is temporal correlation, which is not computed yet.**
DEFAULT_YEAR = 2021

#: ``ABSENT`` white, ``CARRIED`` cool grey.  ``SEEDED`` is the ramp below.
ABSENT, CARRIED, SEEDED = 0, 1, 2

ABSENT_COLOR = '#ffffff'

#: ⚠️ Cool, not neutral.  Deuteranopia flattens red-green, so a neutral grey
#: collapses onto the light end of the ramp; the blue lift is what holds them
#: apart.  Verified by :func:`palette_separation`.
CARRIED_COLOR = '#8e99a8'

#: ``N = 1`` -> ``N >= RAMP_TOP``.  ⚠️ The light end stops at a mid green rather
#: than running to near-white, which would collide with ``ABSENT``.
SEEDED_RAMP = ('#052e16', '#56c98a')

#: ``N`` at which the ramp saturates.  Beyond this the claim is weak enough that
#: the difference stops mattering.
RAMP_TOP = 64.0

#: The nine bands, in the order the sectors fall in the table, over BEA's own
#: sector taxonomy.  Several BEA sectors fold into one band -- trade is
#: wholesale plus retail, services is six sectors.
BANDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('agriculture', ('11',)),
    ('mining', ('21',)),
    ('utilities', ('22',)),
    ('construction', ('23',)),
    ('manufacturing', ('31G',)),
    ('trade', ('42', '44RT')),
    ('transportation', ('48TW',)),
    ('services', ('51', 'FIRE', 'PROF', '6', '7', '81')),
    ('government', ('G',)),
    ('other', ('Used', 'Other')),
)


@functools.cache
def _detail_to_sector() -> dict[str, str]:
    from bedrock.utils.taxonomy.mappings.bea_v2017_sector__bea_v2017_commodity import (  # noqa: PLC0415, E501
        load_bea_v2017_sector_commodity_to_bea_v2017_commodity,
    )

    return {
        str(detail): str(sector)
        for sector, details in (
            load_bea_v2017_sector_commodity_to_bea_v2017_commodity().items()
        )
        for detail in details
    }


@functools.cache
def _benchmark() -> pd.DataFrame:
    """The published 2017 intermediate block in $M, commodity x industry."""
    from bedrock.transform.iot.nowcast_intermediate import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
        benchmark_intermediate,
    )

    return benchmark_intermediate() / MILLION_CURRENCY_TO_CURRENCY


def band_of(code: str) -> str:
    """The band a BEA detail code sits in, or ``'other'``."""
    sector = _detail_to_sector().get(str(code))
    for name, sectors in BANDS:
        if sector in sectors:
            return name
    return 'other'


def band_order(codes: pd.Index) -> tuple[list[str], list[tuple[str, int, int]]]:
    """*codes* re-ordered into contiguous bands, plus ``(name, start, stop)``.

    Within a band the published order is kept, so the picture stays comparable
    to a reader who knows the table.
    """
    ordered: list[str] = []
    spans: list[tuple[str, int, int]] = []
    for name, _ in BANDS:
        members = [str(c) for c in codes if band_of(c) == name]
        if not members:
            continue
        spans.append((name, len(ordered), len(ordered) + len(members)))
        ordered.extend(members)
    return ordered, spans


# --------------------------------------------------------------------------
# Pedigree scoring, on the EPA LCA data-quality ladder: 1 best, 5 worst.
# --------------------------------------------------------------------------

#: **Reliability**, per source, on the EPA pedigree's measured -> estimated
#: ladder.  ⚠️ These are judgements about collection method, not measurements,
#: and they are the most arguable numbers in this module -- they sit in one
#: table so a disagreement is a one-line change rather than an argument.
#:
#: ``census``            complete mandatory enumeration, published as filed,
#: 					   in a year the census actually ran
#: ``census_interpolated`` the same census, read *between* its two vintages
#: ``census_held``       the same census, carried past its 2022 vintage
#:
#: ⚠️ **The last two score 1, exactly as ``census`` does, and that is
#: deliberate.**  How old a source is relative to the year it is used for is
#: **temporal correlation** -- a third pedigree indicator, scored on data age --
#: not reliability, which is about how the data were collected.  An interpolated
#: census mix was still collected by a complete mandatory enumeration.  The
#: labels are kept apart so the temporal indicator can be added later without
#: re-deriving anything; they are **not scored** here (Wes, deferred).
#: ``census_recovered``  same, but Census withheld the cell and it was
#:                       re-estimated here: a calculation, not a measurement,
#:                       which is the 1/2 -> 3 degrade
#:                       :mod:`bedrock.utils.mapping.dqi` already applies
#: ``asm``               Annual Survey of Manufactures -- a probability sample
#: ``sas`` / ``aies``    annual business surveys, sampled and item-imputed
#: ``eia923``            mandatory plant-level filing, verified, but a fuel
#:                       bill rather than the purchase this cell wants
#: ``ers``               modelled national farm-income estimates, not a survey
#:                       of purchases
#: ``carried``           no observation at all
RELIABILITY: dict[str, int] = {
    'census': 1,
    'eia923': 2,
    'census_interpolated': 1,
    'census_held': 1,
    'census_recovered': 3,
    'asm': 3,
    'sas': 3,
    'aies': 3,
    'ers': 4,
    'carried': 5,
}

#: Which source supplies the manufacturing expense cells, by year.  Mirrors
#: ``inputs_structure.EXPENSE_SOURCES``: the census years are stronger evidence
#: than the sample years between them, which is why 2022 scores best.
EXPENSE_SOURCE_YEARS: dict[str, tuple[int, ...]] = {
    'census': (2017, 2022),
    'asm': (2018, 2019, 2020, 2021),
    'aies': (2023, 2024),
}

#: Years the shipped figure is claimed to stand for.  ⚠️ **2018 and 2019 are
#: deliberately not in here** -- see :func:`year_stability`.
STABLE_YEARS: tuple[int, ...] = (2020, 2021, 2022, 2023, 2024)

#: Widest spread in seeded dollars across :data:`STABLE_YEARS` that still lets
#: one figure stand for all of them, in percentage points.
STABILITY_BAND = 3.0


#: Worst score, used for every carried cell on both indicators.
WORST = 5

#: NAICS digits BEA detail sits at.  A source collected coarser than this is
#: mapping *down*, which is the degrade condition in
#: :func:`bedrock.utils.mapping.dqi.adjust_dqi_reliability_collection_scores`.
BEA_DETAIL_DIGITS = 6


def expense_source_for(year: int) -> str:
    """Which survey supplies the manufacturing expense cells in *year*."""
    for source, years in EXPENSE_SOURCE_YEARS.items():
        if year in years:
            return source
    return 'asm'


def _ladder(count: int) -> int:
    """``1, 2, 3-4, 5-9, >=10`` cells sharing one datum -> ``1..5``.

    The steps are roughly geometric because the difference that matters is
    1-vs-2, not 30-vs-40.  ⚠️ The thresholds are a choice, not a finding.
    """
    if count <= 1:
        return 1
    if count == 2:
        return 2
    if count <= 4:
        return 3
    if count <= 9:
        return 4
    return WORST


def _depth_score(depth: int) -> int:
    """NAICS digits collected at -> ``1..5``, one step per digit of shortfall."""
    return int(min(WORST, max(1, 1 + (BEA_DETAIL_DIGITS - int(depth)))))


def technological_correlation(frame: pd.DataFrame) -> pd.DataFrame:
    """Add ``tc_commodity``, ``tc_industry`` and the combined ``tc``.

    Two axes, because the question has two halves and they fail independently:

    **Commodity.** ``1`` when the reported item *is* that BEA commodity, worse
    as one item is spread over more of them.  Driven by ``k``.

    **Industry.** ``1`` when the source reports the specific BEA industry, worse
    when it collected at an aggregation BEA splits finer -- a 2-digit survey
    NAICS against a 6-digit BEA industry -- or when one index drives many
    columns.  Driven by the **worse** of the digit shortfall and ``m``, since
    either alone breaks the correlation.

    ⚠️ **Combined by the mean, rounded up.**  Taking the max would let one weak
    axis erase a strong one, and the mean alone would flatter a cell that is
    exact on commodity and hopeless on industry.  Both sub-scores are kept, so a
    different combination can be taken without re-deriving anything.
    """
    out = frame.copy()
    out['tc_commodity'] = out['k'].map(_ladder).astype(int)
    out['tc_industry'] = np.maximum(
        out['m'].map(_ladder).astype(int), out['depth'].map(_depth_score).astype(int)
    )
    out['tc'] = np.ceil((out['tc_commodity'] + out['tc_industry']) / 2).astype(int)
    return out


# --------------------------------------------------------------------------
# Fan-out, one function per seed.  Each returns long ``commodity, industry, n``.
# --------------------------------------------------------------------------


#: Long-frame columns every ``_*_fanout`` emits.  ``k`` is the commodity
#: fan-out, ``m`` the industry fan-out, ``depth`` the NAICS digits the source
#: collected at, and ``source`` keys :data:`RELIABILITY`.
OBSERVATION_COLUMNS = ('commodity', 'industry', 'k', 'm', 'depth', 'source')


def _records(pairs: list[tuple[str, str, int, int, int, str]]) -> pd.DataFrame:
    """Long observations, keeping the **most specific** one per cell.

    Sorted by ``n`` then by the source's reliability, so where two seeds reach
    the same cell the surviving row is the one that says the most about it.
    """
    frame = pd.DataFrame(list(pairs), columns=list(OBSERVATION_COLUMNS))
    if frame.empty:
        return frame
    frame['n'] = frame['k'] * frame['m']
    frame['reliability'] = frame['source'].map(RELIABILITY).astype(float)
    frame = frame.sort_values(['n', 'reliability'])
    return frame.drop_duplicates(subset=['commodity', 'industry'], keep='first')


def _materials_fanout(year: int, columns: tuple[str, ...] | None) -> pd.DataFrame:
    """Economic Census materials: ``direct`` is one commodity, ``group`` is many."""
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        VINTAGES,
        _manufacturing_bea_industries,
        bea_industry,
        group_members,
        materials,
    )

    vintage = VINTAGES[1] if year >= VINTAGES[1] else VINTAGES[0]
    wanted = set(columns) if columns else set(_manufacturing_bea_industries())
    rows = _benchmark().index
    fba = materials(vintage)

    # ⚠️ Only a census year reads a census. `materials_seed` interpolates the
    # mix between the 2017 and 2022 vintages for everything in between, so in a
    # non-census year even an unsuppressed cell is a calculation rather than an
    # observation -- the same 1/2 -> 3 degrade `dqi` applies when a value stops
    # being a direct representation.
    if year in VINTAGES:
        vintage_source = 'census'
    elif year > VINTAGES[-1]:
        vintage_source = 'census_held'
    else:
        vintage_source = 'census_interpolated'

    def source_of(recovery: object) -> str:
        # A cell Census withheld and this pipeline re-estimated is a calculation,
        # not a measurement, which is the distinction `dqi` degrades 1/2 -> 3 on.
        return 'census_recovered' if isinstance(recovery, str) else vintage_source

    pairs: list[tuple[str, str, int, int, int, str]] = []
    for material, industry, tier, bea, recovery in zip(
        fba['material'].astype(str),
        fba['industry'].astype(str),
        fba['tier'].astype(str),
        fba['bea'],
        fba['SuppressionRecovery'],
        strict=True,
    ):
        column = bea_industry(industry)
        if column is None or column not in wanted:
            continue
        if tier == 'direct':
            members = [str(bea)]
        elif tier == 'group':
            members = list(group_members(material))
        else:
            continue
        members = [m for m in members if m in rows]
        if not members:
            continue
        for commodity in members:
            pairs.append(
                (commodity, str(column), len(members), 1, 6, source_of(recovery))
            )
    return _records(pairs)


def _nonmaterial_fanout(year: int) -> pd.DataFrame:
    """The manufacturing expense cells: one survey kind over its commodity list."""
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        EXPENSE_TO_BEA,
        _manufacturing_bea_industries,
        expense_panel,
    )

    rows, columns = _benchmark().index, set(_manufacturing_bea_industries())
    wide = expense_panel().pivot_table(
        index='bea_industry', columns=['kind', 'year'], values='FlowAmount'
    )
    source = expense_source_for(year)
    pairs: list[tuple[str, str, int, int, int, str]] = []
    for kind, codes in EXPENSE_TO_BEA.items():
        present = [c for c in codes if c in rows]
        if not present:
            continue
        if (kind, year) not in wide.columns or (kind, 2017) not in wide.columns:
            continue
        base, later = wide[(kind, 2017)], wide[(kind, year)]
        movable = base.index[base.notna() & (base > 0) & later.notna()]
        for industry in movable:
            if str(industry) not in columns:
                continue
            for commodity in present:
                pairs.append((commodity, str(industry), len(present), 1, 6, source))
    return _records(pairs)


def _services_fanout(year: int) -> pd.DataFrame:
    """SAS / AIES: one item over its commodities, one survey NAICS over columns."""
    from bedrock.analysis.nowcasting.services_transport_expense_seed import (  # noqa: PLC0415, E501
        SAS_ITEM_TO_BEA,
        _bea_to_survey_industry,
        _panel_for,
        usable_items,
    )

    rows = _benchmark().index
    mapping = _bea_to_survey_industry()
    per_survey: dict[str, list[str]] = {}
    for bea, naics in mapping.items():
        per_survey.setdefault(str(naics), []).append(str(bea))

    panel = _panel_for(year)
    source = 'aies' if year >= 2023 else 'sas'
    pairs: list[tuple[str, str, int, int, int, str]] = []
    for naics, industries in per_survey.items():
        items = usable_items(naics, year, panel=panel)
        for item in items:
            present = [c for c in SAS_ITEM_TO_BEA.get(item, ()) if c in rows]
            if not present:
                continue
            for industry in industries:
                for commodity in present:
                    pairs.append(
                        (
                            commodity,
                            industry,
                            len(present),
                            len(industries),
                            len(str(naics)),
                            source,
                        )
                    )
    return _records(pairs)


def _agriculture_fanout(year: int) -> pd.DataFrame:
    """ERS: one farm sector, so every seeded cell is shared with all ten columns."""
    from bedrock.analysis.nowcasting.agriculture_expense_seed import (  # noqa: PLC0415
        BASE_YEAR,
        FIWS_ITEM_TO_BEA,
        farm_industries,
        usable_items,
    )

    rows = _benchmark().index
    industries = [str(c) for c in farm_industries()]
    pairs: list[tuple[str, str, int, int, int, str]] = []
    for item in usable_items(year, BASE_YEAR):
        present = [c for c in FIWS_ITEM_TO_BEA.get(item, ()) if c in rows]
        if not present:
            continue
        for industry in industries:
            for commodity in present:
                pairs.append(
                    (commodity, industry, len(present), len(industries), 2, 'ers')
                )
    return _records(pairs)


def _utilities_fanout(year: int) -> pd.DataFrame:
    """EIA 923: fuels are 1:1 onto a commodity, shared by the three electric columns."""
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        ELECTRIC,
        relative_index,
    )

    rows = _benchmark().index
    index = relative_index(year, 2017)
    industries = [c for c in ELECTRIC if c in _benchmark().columns]
    pairs: list[tuple[str, str, int, int, int, str]] = []
    for commodity in index.index:
        if str(commodity) not in rows:
            continue
        for industry in industries:
            pairs.append(
                (str(commodity), str(industry), 1, len(industries), 6, 'eia923')
            )
    return _records(pairs)


@functools.cache
def _observations(year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """Every seeded cell in *year*, one row each, most specific observation kept.

    The single place the five seeds are combined.  Both :func:`fanout`, which
    draws the picture, and :func:`pedigree_cells`, which scores it, read this,
    so the map and the scores cannot disagree about which cells are seeded.
    """
    from bedrock.analysis.nowcasting.inputs_structure import (  # noqa: PLC0415
        MINING_SEEDED,
    )

    frames = [
        _materials_fanout(year, None),
        _materials_fanout(year, MINING_SEEDED),
        _nonmaterial_fanout(year),
        _services_fanout(year),
        _agriculture_fanout(year),
        _utilities_fanout(year),
    ]
    stacked = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    block = _benchmark()
    inside = stacked['commodity'].isin(block.index.astype(str)) & stacked[
        'industry'
    ].isin(block.columns.astype(str))
    stacked = stacked[inside]
    # A cell the benchmark leaves empty cannot be seeded: there is no 2017 value
    # for an index to move, and every seed is an index on BEA's own cell.
    nonzero = pd.Series(block.abs().stack())
    nonzero = nonzero[nonzero != 0.0]
    live = {
        (str(c), str(i))
        for c, i in zip(
            nonzero.index.get_level_values(0),
            nonzero.index.get_level_values(1),
            strict=True,
        )
    }
    keys = list(zip(stacked['commodity'], stacked['industry'], strict=True))
    stacked = stacked[[key in live for key in keys]]
    return (
        stacked.sort_values(['n', 'reliability'])
        .drop_duplicates(subset=['commodity', 'industry'], keep='first')
        .reset_index(drop=True)
    )


def fanout(year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """``N`` per cell, ``NaN`` where no annual source reaches it.

    Commodity x industry on the benchmark axes.  See the module docstring for
    what ``N`` counts and why the minimum wins where seeds overlap.
    """
    block = _benchmark()
    best = _observations(year)
    out = pd.DataFrame(np.nan, index=block.index, columns=block.columns)
    rows = best['commodity'].to_numpy()
    cols = best['industry'].to_numpy()
    keep = np.isin(rows, out.index.to_numpy()) & np.isin(cols, out.columns.to_numpy())
    out.values[
        out.index.get_indexer(rows[keep]), out.columns.get_indexer(cols[keep])
    ] = best['n'].to_numpy()[keep]
    # A cell the benchmark leaves empty cannot be seeded: there is no 2017 value
    # for an index to move, and every seed is an index on BEA's own cell.
    return out.where(block != 0.0)


def status(year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """``ABSENT`` / ``CARRIED`` / ``SEEDED`` per cell."""
    block, n = _benchmark(), fanout(year)
    out = pd.DataFrame(CARRIED, index=block.index, columns=block.columns)
    out = out.where(block != 0.0, ABSENT)
    return out.where(n.isna(), SEEDED)


def coverage(year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """Per band: cells and **dollars** seeded against carried, and how specific.

    ⚠️ **Read the dollar columns, not the cell columns.**  73% of the block is
    structurally empty and the seeded cells are the large ones, so a cell-count
    share understates the seeds by a wide margin.
    """
    block, n, state = _benchmark(), fanout(year), status(year)
    bands = pd.Series({str(c): band_of(c) for c in block.columns})
    records = []
    for name, _ in BANDS:
        columns = [c for c in block.columns if bands.get(str(c)) == name]
        if not columns:
            continue
        sub_block, sub_state, sub_n = block[columns], state[columns], n[columns]
        seeded = sub_state == SEEDED
        dollars = float(sub_block.abs().sum().sum())
        seeded_dollars = float(sub_block.abs().where(seeded).sum().sum())
        specific = float(sub_block.abs().where(seeded & (sub_n <= 1)).sum().sum())
        records.append(
            {
                'band': name,
                'columns': len(columns),
                'cells': int((sub_block != 0.0).sum().sum()),
                'cells_seeded': int(seeded.sum().sum()),
                '$M': dollars,
                '$M_seeded': seeded_dollars,
                'seeded_%': 100.0 * seeded_dollars / dollars if dollars else 0.0,
                'exact_%': 100.0 * specific / dollars if dollars else 0.0,
                'median_N': (
                    float(sub_n.where(seeded).stack().median())
                    if seeded.any().any()
                    else float('nan')
                ),
            }
        )
    frame = pd.DataFrame(records).set_index('band')
    total = frame[['columns', 'cells', 'cells_seeded', '$M', '$M_seeded']].sum()
    frame.loc['TOTAL'] = {
        **total.to_dict(),
        'seeded_%': 100.0 * total['$M_seeded'] / total['$M'],
        'exact_%': float('nan'),
        'median_N': float(n.where(state == SEEDED).stack().median()),
    }
    return frame


def pedigree_cells(year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """Every non-empty cell of the block, scored on both pedigree indicators.

    One row per cell: ``band``, ``dollars`` (2017 basis, absolute), ``source``,
    ``k``, ``m``, ``n``, ``reliability``, ``tc_commodity``, ``tc_industry``,
    ``tc``.  Carried cells are included and score :data:`WORST` on both -- they
    are the majority of the block and excluding them would score the seeds
    against themselves.

    ⚠️ **Dollars are the 2017 benchmark's, not the seeded year's.**  The seeds
    move shares and Step 5 owns the level, so the benchmark is the only
    consistent weight available at cell resolution across every band.
    """
    block_ = _benchmark()
    observed = technological_correlation(_observations(year))

    stacked = pd.Series(block_.abs().stack())
    stacked = stacked[stacked != 0.0]
    cells = stacked.rename('dollars').reset_index()
    cells.columns = ['commodity', 'industry', 'dollars']
    cells['commodity'] = cells['commodity'].astype(str)
    cells['industry'] = cells['industry'].astype(str)

    merged = cells.merge(
        observed[
            [
                'commodity',
                'industry',
                'k',
                'm',
                'n',
                'source',
                'reliability',
                'tc_commodity',
                'tc_industry',
                'tc',
            ]
        ],
        on=['commodity', 'industry'],
        how='left',
    )
    carried = merged['source'].isna()
    merged.loc[carried, 'source'] = 'carried'
    for column in ('reliability', 'tc_commodity', 'tc_industry', 'tc'):
        merged.loc[carried, column] = float(WORST)
    # A carried cell has no observation behind it, so `n` is undefined rather
    # than large.  It is left NaN and the N-weighted aggregation says what it
    # does with that.
    merged['seeded'] = ~carried
    merged['band'] = merged['industry'].map(band_of)
    return merged


def _weighted(frame: pd.DataFrame, column: str, weight: pd.Series) -> float:
    total = float(weight.sum())
    return float((frame[column] * weight).sum() / total) if total else float('nan')


def pedigree_summary(year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """Mean reliability and technological correlation, under two weightings.

    **By dollars** -- what a dollar of this block is worth as evidence. This is
    the headline: it answers "how good is the number we are shipping".

    **By N x dollars** -- the same, with each cell weighted by how many cells
    share its observation as well as by its size. It deliberately *up*-weights
    the spread-thin evidence, so the gap between the two columns is a direct
    read on how much of the block's quality rests on data that had to be
    allocated. ⚠️ Carried cells have no ``N``; they are given ``N = 1`` here,
    which is the most generous choice available and still leaves them at 5.
    """
    cells = pedigree_cells(year)
    dollars = cells['dollars']
    n_dollars = cells['dollars'] * cells['n'].fillna(1.0)

    records = []
    for name, _ in BANDS:
        part = cells[cells['band'] == name]
        if part.empty:
            continue
        records.append(
            {
                'band': name,
                '$M': float(part['dollars'].sum()),
                'seeded_%': 100.0
                * float(part.loc[part['seeded'], 'dollars'].sum())
                / float(part['dollars'].sum()),
                'reliability_$': _weighted(part, 'reliability', part['dollars']),
                'tc_$': _weighted(part, 'tc', part['dollars']),
                'reliability_N$': _weighted(
                    part, 'reliability', part['dollars'] * part['n'].fillna(1.0)
                ),
                'tc_N$': _weighted(part, 'tc', part['dollars'] * part['n'].fillna(1.0)),
            }
        )
    table = pd.DataFrame(records).set_index('band')
    table.loc['TOTAL'] = {
        '$M': float(dollars.sum()),
        'seeded_%': 100.0
        * float(cells.loc[cells['seeded'], 'dollars'].sum())
        / float(dollars.sum()),
        'reliability_$': _weighted(cells, 'reliability', dollars),
        'tc_$': _weighted(cells, 'tc', dollars),
        'reliability_N$': _weighted(cells, 'reliability', n_dollars),
        'tc_N$': _weighted(cells, 'tc', n_dollars),
    }
    return table


def pedigree_by_source(year: int = DEFAULT_YEAR) -> pd.DataFrame:
    """The same scores grouped by which source supplied the cell."""
    cells = pedigree_cells(year)
    total = float(cells['dollars'].sum())
    records = []
    for source, part in cells.groupby('source', dropna=False):
        records.append(
            {
                'source': str(source),
                'cells': float(len(part)),
                '$M': float(part['dollars'].sum()),
                '$M_%': 100.0 * float(part['dollars'].sum()) / total,
                'reliability': _weighted(part, 'reliability', part['dollars']),
                'tc_commodity': _weighted(part, 'tc_commodity', part['dollars']),
                'tc_industry': _weighted(part, 'tc_industry', part['dollars']),
                'tc': _weighted(part, 'tc', part['dollars']),
            }
        )
    table = pd.DataFrame(records).set_index('source')
    return table.sort_values('$M', ascending=False)


def best_year(years: tuple[int, ...] = STABLE_YEARS) -> pd.DataFrame:
    """Score every candidate year, best first -- which one to put on a slide.

    ``score`` is the mean of the two dollar-weighted indicators, both
    lower-is-better, with reliability breaking ties.

    ⚠️ **Coverage is the wrong ranking, and it points the other way.** 2020 and
    2021 observe ~1.9 points more of the block than 2022 does and so win on
    ``tc``, which barely separates the years at all (0.06 across four). What
    separates them is **reliability**, which moves 0.29 -- because
    ``materials_seed`` interpolates the mix between the 2017 and 2022 census
    vintages, so in any year that is not a census year the largest seeded block
    in the table is a calculation between two measurements rather than either of
    them. 2022 reads the 2022 census; 2020 and 2021 read a line drawn through it.

    ⚠️ **2022's lower coverage is a measured extract gap, not a property of the
    census.** ``Census_EC_Expenses`` 2022 carries **222 of the 232** BEA
    manufacturing industries that ``Census_ASM_Expenses`` 2021 carries, for all
    ten expense kinds, and that 4.3% shortfall is most of the 4.3-point drop in
    manufacturing coverage. Closing it would make 2022 win on both indicators.
    """
    records = []
    for year in years:
        total = pedigree_summary(year).loc['TOTAL']
        records.append(
            {
                'year': year,
                'seeded_%': float(total['seeded_%']),
                'reliability_$': float(total['reliability_$']),
                'tc_$': float(total['tc_$']),
                'score': (float(total['reliability_$']) + float(total['tc_$'])) / 2.0,
                'expense_source': expense_source_for(year),
            }
        )
    table = pd.DataFrame(records).set_index('year')
    return table.sort_values(['score', 'reliability_$'])


# --------------------------------------------------------------------------
# Colour
# --------------------------------------------------------------------------


def ramp_position(n: np.ndarray) -> np.ndarray:
    """``N`` -> 0..1 along the ramp, logarithmic and saturating at :data:`RAMP_TOP`."""
    clipped = np.clip(np.nan_to_num(n, nan=1.0), 1.0, RAMP_TOP)
    return np.log(clipped) / np.log(RAMP_TOP)


def seeded_rgb(position: np.ndarray) -> np.ndarray:
    lo, hi = _hex_to_rgb(SEEDED_RAMP[0]), _hex_to_rgb(SEEDED_RAMP[1])
    return lo + (hi - lo) * position[..., None]


def status_rgb(state: np.ndarray, n: np.ndarray) -> np.ndarray:
    out = np.empty((*state.shape, 3), dtype=float)
    out[...] = _hex_to_rgb(ABSENT_COLOR)
    out[state == CARRIED] = _hex_to_rgb(CARRIED_COLOR)
    seeded = state == SEEDED
    if seeded.any():
        out[seeded] = seeded_rgb(ramp_position(n[seeded]))
    return out


def palette_separation() -> pd.DataFrame:
    """CIE76 distance between every category pair, under four vision models.

    Ramp-against-ramp pairs are skipped -- shading inside ``SEEDED`` is a
    magnitude cue, not a category boundary.  Sort ascending and read the top
    row for the binding pair.
    """
    anchors: dict[str, np.ndarray] = {
        'absent': _hex_to_rgb(ABSENT_COLOR),
        'carried': _hex_to_rgb(CARRIED_COLOR),
    }
    for t in (0.0, 0.5, 1.0):
        anchors[f'seeded@{t:.1f}'] = seeded_rgb(np.array(t))

    records = []
    for vision, matrix in _CVD_MATRICES.items():
        seen = {
            name: _to_lab(_linear_to_srgb(matrix @ _srgb_to_linear(rgb)))
            for name, rgb in anchors.items()
        }
        for a, b in itertools.combinations(seen, 2):
            if a.startswith('seeded@') and b.startswith('seeded@'):
                continue
            records.append(
                {
                    'vision': vision,
                    'a': a,
                    'b': b,
                    'delta_e': float(np.linalg.norm(seen[a] - seen[b])),
                }
            )
    return pd.DataFrame(records).sort_values('delta_e').reset_index(drop=True)


# --------------------------------------------------------------------------
# The picture
# --------------------------------------------------------------------------


def _spread(centres: list[float], gap: float, hi: float) -> list[float]:
    """Push labels apart so thin bands stay legible, keeping their order.

    ``utilities`` is 3 rows of 402 -- roughly a twentieth of an inch -- so a
    label at the band's centre lands on top of its neighbours'.  Each label is
    nudged to at least *gap* from the one before, then the whole run is shifted
    back if it has overflowed the axis.  A leader line puts it back on its band.
    """
    placed: list[float] = []
    for centre in centres:
        position = centre if not placed else max(centre, placed[-1] + gap)
        placed.append(position)
    overflow = placed[-1] - hi if placed and placed[-1] > hi else 0.0
    return [p - overflow for p in placed]


def render(year: int = DEFAULT_YEAR, path: Path | None = None, dpi: int = 200) -> Path:
    """Draw the provenance map and write it to *path*."""
    from matplotlib.cm import ScalarMappable  # noqa: PLC0415
    from matplotlib.colors import LinearSegmentedColormap, LogNorm  # noqa: PLC0415
    from matplotlib.patches import Patch  # noqa: PLC0415

    block, n, state = _benchmark(), fanout(year), status(year)
    rows, row_spans = band_order(block.index)
    columns, column_spans = band_order(block.columns)
    n = n.reindex(index=rows, columns=columns)
    state = state.reindex(index=rows, columns=columns)

    rgb = status_rgb(state.to_numpy(), n.to_numpy())
    stats = coverage(year)
    total = stats.loc['TOTAL']

    figure, axes = plt.subplots(figsize=(14.0, 12.5))
    axes.imshow(rgb, interpolation='nearest', aspect='equal', origin='upper')

    for _, start, _ in row_spans[1:]:
        axes.axhline(start - 0.5, color='#1f2937', linewidth=0.6, alpha=0.55)
    for _, start, _ in column_spans[1:]:
        axes.axvline(start - 0.5, color='#1f2937', linewidth=0.6, alpha=0.55)

    # Band names sit off the axis with a leader back to the band, because the
    # thin bands -- utilities is 3 rows of 402 -- cannot hold a label in place.
    axes.set_xticks([])
    axes.set_yticks([])
    rows_n, columns_n = len(rows), len(columns)
    label_y = _spread(
        [(a + b) / 2 - 0.5 for _, a, b in row_spans], rows_n * 0.040, rows_n
    )
    for (name, a, b), y in zip(row_spans, label_y, strict=True):
        centre = (a + b) / 2 - 0.5
        axes.annotate(
            name,
            xy=(-0.004, centre),
            xytext=(-0.055, y),
            xycoords=('axes fraction', 'data'),
            textcoords=('axes fraction', 'data'),
            ha='right',
            va='center',
            fontsize=9.5,
            arrowprops={'arrowstyle': '-', 'color': '#9ca3af', 'linewidth': 0.7},
        )
    # Two rows, alternating: staggering halves the horizontal room each label
    # needs, which is what stops `construction` running into `manufacturing`.
    label_x = _spread(
        [(a + b) / 2 - 0.5 for _, a, b in column_spans], columns_n * 0.055, columns_n
    )
    for position, ((name, a, b), x) in enumerate(
        zip(column_spans, label_x, strict=True)
    ):
        centre = (a + b) / 2 - 0.5
        axes.annotate(
            name,
            xy=(centre, 1.004),
            xytext=(x, 1.035 if position % 2 == 0 else 1.075),
            xycoords=('data', 'axes fraction'),
            textcoords=('data', 'axes fraction'),
            ha='center',
            va='bottom',
            fontsize=9.5,
            arrowprops={'arrowstyle': '-', 'color': '#9ca3af', 'linewidth': 0.7},
        )
    for spine in axes.spines.values():
        spine.set_edgecolor('#9ca3af')

    axes.set_xlabel('industry that buys  →', labelpad=12)
    axes.set_ylabel('←  commodity that is bought', labelpad=12)
    figure.suptitle(
        f'Intermediate use block {year} — where each cell’s movement comes from',
        fontsize=15,
        y=0.975,
    )
    figure.text(
        0.5,
        0.947,
        f'{total["seeded_%"]:.1f}% of the block’s dollars are observed by an '
        f'annual survey; the rest holds its 2017 structure and moves only on '
        f'prices.  {int(total["cells_seeded"]):,} of {int(total["cells"]):,} '
        f'non-empty cells are seeded.',
        ha='center',
        fontsize=10,
        color='#374151',
    )

    handles = [
        Patch(facecolor=ABSENT_COLOR, edgecolor='#9ca3af', label='no cell (2017 = 0)'),
        Patch(facecolor=CARRIED_COLOR, label='carried — price carry only'),
    ]
    axes.legend(
        handles=handles,
        loc='upper left',
        bbox_to_anchor=(1.02, 1.0),
        frameon=False,
        fontsize=9.5,
        alignment='left',
    )

    # The ramp gets a bar rather than two swatches: N is a magnitude, and the
    # reader needs to be able to put a green somewhere on the scale.
    colormap = LinearSegmentedColormap.from_list('seeded', list(SEEDED_RAMP))
    bar_axes = figure.add_axes((0.800, 0.48, 0.016, 0.20))
    bar = figure.colorbar(
        ScalarMappable(norm=LogNorm(vmin=1.0, vmax=RAMP_TOP), cmap=colormap),
        cax=bar_axes,
    )
    bar.set_ticks([1, 2, 4, 8, 16, 32, 64])
    bar.set_ticklabels(['1', '2', '4', '8', '16', '32', '≥64'])
    bar.ax.tick_params(labelsize=8.5, length=2)
    bar.set_label('N — cells sharing one observation', fontsize=9, labelpad=8)
    bar.ax.text(
        1.0,
        -0.11,
        'dark = the datum is the cell',
        transform=bar.ax.transAxes,
        fontsize=8.5,
        color='#374151',
        ha='left',
    )

    figure.subplots_adjust(left=0.155, right=0.775, top=0.915, bottom=0.06)
    path = path or IMAGE_DIR / f'intermediate_seed_coverage_{year}.png'
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=dpi)
    plt.close(figure)
    return path


def year_stability(years: tuple[int, ...] = STABLE_YEARS) -> pd.DataFrame:
    """Seeded share per year -- the check on shipping **one** figure.

    The mappings behind ``N`` do not depend on the year, so the provenance map
    is nearly year-invariant and a figure per year would be a figure repeated.
    "Nearly" is the part worth measuring rather than assuming, because what
    *does* move is which items a survey published in a given year.

    ⚠️ **2018 and 2019 are a different picture and are excluded on purpose.**
    The SAS expense panel jumps straight from 2017 to 2020 -- there is no 2018
    or 2019 vintage in it -- so :func:`usable_items` returns nothing for those
    years and **services and transportation hold their 2017 columns entirely**.
    That is 19.5% of the block's dollars observed against 33.8% at 2022, and it
    is a gap in the source rather than in the method.  A figure for 2018 would
    be showing the missing SAS vintages, not the seeds.
    """
    records = []
    for year in years:
        total = coverage(year).loc['TOTAL']
        records.append(
            {
                'year': year,
                'seeded_%': float(total['seeded_%']),
                'cells_seeded': int(total['cells_seeded']),
                'median_N': float(total['median_N']),
            }
        )
    return pd.DataFrame(records).set_index('year')


def check(year: int = DEFAULT_YEAR, years: bool = False) -> int:
    """Assert the provenance map is well formed.  Returns a process exit code.

    ``years`` adds the :func:`year_stability` assertion, which re-runs every
    seed once per year and is slow enough to be opt-in.
    """
    from bedrock.transform.iot import nowcast_intermediate  # noqa: PLC0415

    failures: list[str] = []
    block, n, state = _benchmark(), fanout(year), status(year)

    if state.shape != block.shape:
        failures.append(f'shape {state.shape} != benchmark {block.shape}')

    counted = int((state == ABSENT).sum().sum() + (state == CARRIED).sum().sum())
    counted += int((state == SEEDED).sum().sum())
    if counted != block.size:
        failures.append(f'states cover {counted} cells, block has {block.size}')

    if bool((n.stack() < 1).any()):
        failures.append('a seeded cell has N < 1, which is not a count of cells')

    if bool(((block == 0.0) & (state == SEEDED)).any().any()):
        failures.append('a cell empty in 2017 is marked seeded; nothing can index it')

    # Every seeded cell must sit in a column composed_seed actually overlays --
    # the map is a claim about that function, so a cell outside its columns is
    # this module inventing coverage rather than reporting it.
    seeded_columns = {str(c) for c in state.columns if (state[c] == SEEDED).any()}
    overlaid = set(nowcast_intermediate.composed_seed(year).columns.astype(str))
    stray = sorted(seeded_columns - overlaid)
    if stray:
        failures.append(
            f'{len(stray)} columns seeded here are not overlaid: {stray[:6]}'
        )

    cells = pedigree_cells(year)
    live = int((_benchmark() != 0.0).sum().sum())
    if len(cells) != live:
        failures.append(f'pedigree scored {len(cells)} cells, block has {live}')
    for column in ('reliability', 'tc', 'tc_commodity', 'tc_industry'):
        outside = cells[(cells[column] < 1) | (cells[column] > WORST)]
        if not outside.empty:
            failures.append(
                f'{len(outside)} cells score outside 1..{WORST} on {column}'
            )
    carried_rows = cells[~cells['seeded']]
    if not carried_rows.empty and not bool(
        (carried_rows['reliability'] == WORST).all()
        and (carried_rows['tc'] == WORST).all()
    ):
        failures.append('a carried cell scores better than worst on an indicator')
    seeded_share = (
        100.0
        * float(cells.loc[cells['seeded'], 'dollars'].sum())
        / float(cells['dollars'].sum())
    )
    reported = float(coverage(year)['seeded_%'].astype(float).loc['TOTAL'])
    drift = abs(seeded_share - reported)
    if drift > 0.01:
        failures.append(
            f'pedigree and coverage disagree on the seeded share by {drift:.2f} points'
        )

    worst = palette_separation()
    floor = float(worst['delta_e'].min())
    if floor < 27.0:
        row = worst.iloc[0]
        failures.append(
            f'palette separation {floor:.1f} < 27 on {row["a"]} vs {row["b"]} '
            f'under {row["vision"]}'
        )

    table = coverage(year)
    print(table.round(1).to_string())
    print(f'\npalette worst-pair separation dE {floor:.1f}')

    print()
    print(pedigree_summary(year).round(2).to_string())
    print()
    print(pedigree_by_source(year).round(2).to_string())

    if years:
        spread = year_stability()
        print(f'\n{spread.round(1).to_string()}')
        width = float(spread['seeded_%'].max() - spread['seeded_%'].min())
        print(f'spread across {STABLE_YEARS}: {width:.1f} points')
        if width > STABILITY_BAND:
            failures.append(
                f'seeded share moves {width:.1f} points across {STABLE_YEARS}, '
                f'over the {STABILITY_BAND} band -- one figure no longer stands '
                f'for the span'
            )

    for failure in failures:
        print(f'FAIL {failure}')
    return 1 if failures else 0


@click.command()
@click.option('--year', default=DEFAULT_YEAR, show_default=True, type=int)
@click.option('--check', 'run_check', is_flag=True, help='Assert and print the stats.')
@click.option(
    '--check-years',
    is_flag=True,
    help='Also assert one figure stands for 2020-2023 (slow).',
)
@click.option('--check-palette', is_flag=True, help='Print the separation table.')
@click.option(
    '--best-year', 'best_year_', is_flag=True, help='Rank the years on the pedigree.'
)
@click.option('--dpi', default=200, show_default=True, type=int)
def main(
    year: int,
    run_check: bool,
    check_years: bool,
    check_palette: bool,
    best_year_: bool,
    dpi: int,
) -> None:
    """Render the intermediate block's provenance map."""
    if check_palette:
        print(palette_separation().to_string(index=False))
        return
    if best_year_:
        print(best_year().round(3).to_string())
        return
    if run_check or check_years:
        sys.exit(check(year, years=check_years))
    path = render(year, dpi=dpi)
    print(coverage(year).round(1).to_string())
    print(f'\nwrote {path}')


if __name__ == '__main__':
    main()
