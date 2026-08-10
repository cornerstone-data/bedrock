"""Annual transport margin by commodity, from the 2017 anchor and FAF ton-miles.

Step 4c of the nowcast build
(``bedrock/analysis/nowcasting/margins_estimation_plan.md``), phase 2
(`#611 <https://github.com/cornerstone-data/bedrock/issues/611>`_). Phase 1
(:mod:`bedrock.transform.iot.nowcast_margins`) produced the 2017 structure; this
module moves the transport column of it to an annual year. Transport is the one
margin with an external source that reaches every commodity, so it is settled
here rather than waiting for the trade levels in phase 3.

**Shape from ton-miles, level from a control total.** The construction is two
independent movements of the same 2017 column:

.. code-block:: text

    shape[c, t] = TRANS_2017[c] x ton-miles[group(c), t] / ton-miles[group(c), 2017]
    TRANS[c, t] = shape[c, t] x control_total[t] / sum_c shape[c, t]

Both factors are 1 in 2017, so the published 2017 column is reproduced exactly
and BEA's own commodity allocation is inherited rather than approximated.

**Why ton-miles move the allocation instead of generating it.** Allocating each
mode's margin across commodities on its share of ton-miles - BEA's own
construction - fits the published 2017 ``TRANS`` at a Pearson of only 0.370.
Volume does not predict the margin: furniture costs roughly eight times more per
ton-mile to deliver than coal, and a volume weighting discards exactly that. So
the cost structure is taken from the 2017 table, which knows it, and ton-miles
supply only the year-to-year movement. Ton-miles rather than tons all the same -
tons fit at 0.099, and the manual's constant-revenue-per-ton assumption is a
line-haul assumption where ``TRANS`` is the delivered margin.

**Why a control total is needed at all.** Ton-miles are a *volume* index and
``TRANS`` is nominal. Moving 2017 by ton-miles alone holds 2017 freight rates
fixed, which misses the 2021-22 rate surge entirely: the volume index reaches
1.04 by 2022 while the nominal control reaches 1.55. The level therefore comes
from a separate nominal source per mode - and that source is the whole of what
:func:`control_total_components` decides. Whichever treatment supplies it, the
result is also the *negative* side of the ``TRANS`` column, so the two sides stay
consistent by construction.

⚠️ **The choice of control total is the largest open uncertainty here.** The four
constructions in :func:`control_total_comparison` agree exactly in 2017 and
spread **12% by 2022**, from 575,587 million to 643,443 million. That is why
*method* is a parameter rather than a decision baked in, and why
`#620 <https://github.com/cornerstone-data/bedrock/issues/620>`_ exists to settle
it against BTS_TSA and Freight Facts & Figures rather than against itself.

**The level is not one construction but three, by mode.** A single treatment
cannot serve all five, because the modes differ in what fraction of their output
is margin at all - 0.026 for air against 0.862 for rail. :data:`MODE_CONTROL`
assigns each mode the only treatment that is conditioned for it:

*Residual, for truck and rail* - 84.3% of ``TRANS``. The mode's output less its
direct (non-margin) uses, where 2017 direct uses are moved by an annual
final-use index and the intermediate:final split is held at 2017. Margin is 77%
and 86% of output for these two, so direct uses are the small term: a 1% error
in them moves the margin 0.30% and 0.16%. This is what replaced a frozen give-up
ratio, and it lifts the pair 0.8% to 5.1% above that ratio over 2018-2024.

*Freight volume times price, for air and water* - 3.8%. Their margin is 2.6% and
17.3% of output, so the residual amplifies any error in direct uses 37x and 4.8x
and **goes negative** - air from 2019, reaching -121,719 million by 2024, because
air PCE grows 1.96x against output's 1.393x and direct uses overtake output
entirely. Neither can a frozen give-up ratio serve: it makes air *freight* margin
follow air *passenger* output, which is why the 2020 collapse to 0.536 of 2017
output would cut air freight margin 46% in a year when air freight ton-miles were
1.010. So these two move by their own FAF freight ton-miles - freight-only by
construction - repriced by their BEA mode price index.

*Output ratio, for pipeline* - 11.9%. Neither of the above is available:
pipeline's output is *less* than the margin it gives up (a 2017 ratio of 1.033),
so no residual exists, and it has no PCE at all, so there is no final-use index
to move direct uses by. Its output is very nearly all margin, which is what makes
the frozen ratio harmless here.

⚠️ The air and water price index is the whole industry's, passengers included, so
their repricing is still contaminated - much less than their *level* was under
the frozen ratio, but not cleanly. #620 covers validating all three drivers
against BTS_TSA and Freight Facts & Figures.

⚠️ **Three limitations of the construction as built.**

*The 2017/2018 seam is partly methodological.* FAF's 2017 is anchored on the
Commodity Flow Survey and 2018 onward are modelled. Aggregate ton-miles move
only 1.9% across that seam, but the composition moves much more: 21% of 2017
``TRANS`` sits in SCTG groups whose ton-miles jump by more than 25% in that one
year, and the ``TRANS``-weighted mean step is 1.124 against the aggregate's
1.019. The control total absorbs the level, so what survives is a shape shift.

*The residual freezes the intermediate:final split at 2017.* Direct uses are
moved wholly by a *final*-use index, so intermediate direct purchases of freight
are assumed to move with household ones. They are 46% of truck's direct uses and
62% of rail's. At those modes' leverage the exposure is small - a 10% error in
the frozen part moves truck margin 1.4% and rail 1.0% - but it is the reason the
residual is not extended to modes with thinner margins. Intermediate direct
purchases are a Use-table row, and taking them from a nowcast Use matrix would be
circular: Step 6b needs these margins to produce it.

*Pipeline gives up more margin than its gross output* - a 2017 ratio of 1.033.
That is carried as an index rather than treated as an error, but it means
pipeline margin tracks pipeline output including its crude-price component.

**On BEA codes and the FBS.** :func:`sctg_ton_miles` aggregates the
``Margins_Transport_<year>`` FBS back over its sector columns to the SCTG in
``Flowable``, then :func:`sctg_to_commodity` joins to BEA detail through the
crosswalk's ``Note`` column. That looks like undoing the FBS's work and is
deliberate: the FBS resolves sectors to NAICS, so an SCTG mapped to a 3-digit
NAICS group is spread across everything in it, while the ``Note`` column holds
the SCTG-to-BEA-detail correspondence that was ported and corrected in #611 and
covers all 258 ``TRANS``-receiving commodities at 100% of value. The mapping
machinery cannot read that column until
`#546 <https://github.com/cornerstone-data/bedrock/issues/546>`_ lands a BEA-code
target schema; when it does, this join moves into the FBS and
:func:`commodity_ton_miles` reads sectors instead of ``Flowable``.
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.extract.bea.BEA import map_detail_table
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.gdp import load_pi_detail
from bedrock.extract.iot.io_2017 import load_2017_pce_bridge_detail_usa
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.transform.iot.nowcast_margins import (
    COMMODITY_LEVEL,
    margins_by_commodity,
)
from bedrock.utils.config.settings import crosswalkpath
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

#: The benchmark the whole construction is anchored on - the year of the
#: published Margins table, and FAF 5.7.1's own base year.
ANCHOR_YEAR = 2017

#: The five BEA commodities that supply freight, and so give up the margin the
#: goods receive. They are 100% of the ``TRANS`` suppliers; the other three FAF
#: modes map to commodities with zero ``TRANS``.
TRANSPORT_MODE_COMMODITIES: tuple[str, ...] = (
    '481000',  # air
    '482000',  # rail
    '483000',  # water
    '484000',  # truck
    '486000',  # pipeline
)

#: FBS method carrying FAF ton-miles, one per year.
FBS_METHOD = 'Margins_Transport_{year}'

#: Annual nominal level source - gross output per BEA detail industry.
GROSS_OUTPUT_SOURCE = 'BEA_Detail_GrossOutput_IO'

#: Annual final-use source - NIPA table 2.4.5U, PCE by type of product.
FINAL_USE_SOURCE = 'BEA_NIPA'
FINAL_USE_TABLE = 'U20405'

#: The three treatments of a mode's give-up level; see the module docstring.
RESIDUAL = 'residual'
FREIGHT_VOLUME = 'freight_volume'
OUTPUT_RATIO = 'output_ratio'

#: Apply :data:`MODE_CONTROL` per mode rather than one treatment to all five.
#: The default, and the only one of the four that is conditioned for every mode.
MIXED = 'mixed'

#: Which mode gets which, and why it is the only one conditioned for it. The
#: ratio is each mode's 2017 margin-given-up over its gross output, and the
#: leverage is how far a 1% error in direct uses moves the margin - the number
#: that decides whether a residual is usable.
#:
#: ============ ====== ======== ================================================
#: mode         ratio  leverage treatment
#: ============ ====== ======== ================================================
#: truck        0.767  0.30     :data:`RESIDUAL`
#: rail         0.862  0.16     :data:`RESIDUAL`
#: water        0.173  4.8      :data:`FREIGHT_VOLUME` - residual turns negative
#: air          0.026  36.9     :data:`FREIGHT_VOLUME` - residual turns negative
#: pipeline     1.033  n/a      :data:`OUTPUT_RATIO` - no residual, and no PCE
#: ============ ====== ======== ================================================
MODE_CONTROL: dict[str, str] = {
    '481000': FREIGHT_VOLUME,  # air
    '482000': RESIDUAL,  # rail
    '483000': FREIGHT_VOLUME,  # water
    '484000': RESIDUAL,  # truck
    '486000': OUTPUT_RATIO,  # pipeline
}

#: The FBS column the SCTG commodity-moved name survives in, and the crosswalk
#: column holding its BEA 2017 detail code.
SCTG_COLUMN = 'Flowable'
_CROSSWALK = 'NAICS_Crosswalk_FAF_Mode_and_SCTG.csv'
_CROSSWALK_SOURCE = 'FAF_SCTG'
_CROSSWALK_BEA_COLUMN = 'Note'

#: Gross output is published in millions.
_MILLIONS = 1e6


def _as_years(years: tuple[int, ...] | list[int] | range) -> tuple[int, ...]:
    """Normalise a year argument, with the anchor year always included."""
    out = tuple(sorted({int(year) for year in years} | {ANCHOR_YEAR}))
    return out


@functools.cache
def sctg_ton_miles(years: tuple[int, ...]) -> pd.DataFrame:
    """
    Ton-miles per SCTG commodity group and year. Index SCTG, columns years.

    Aggregated over the FBS's mode and sector columns. Summing over modes is
    also what makes the FBS's redistribution of the two non-primary modes a
    no-op here - it moves ton-miles between modes of the same commodity, and
    this total is taken across all of them. The mode dimension is kept in the
    FBS for the mode-level refinement the plan records, not used here.
    """
    return pd.DataFrame(
        {
            year: getFlowBySector(
                FBS_METHOD.format(year=year),
                download_FBAs_if_missing=False,
                download_FBS_if_missing=False,
            )
            .groupby(SCTG_COLUMN)['FlowAmount']
            .sum()
            for year in _as_years(years)
        }
    )


@functools.cache
def sctg_to_commodity() -> pd.DataFrame:
    """
    The ``(SCTG, BEA 2017 detail commodity)`` pairs, from the ported crosswalk.

    Many-to-many, but barely: 254 of the 259 commodities sit in exactly one
    SCTG. The five that do not are ``324110`` refineries (gasoline and fuel
    oils), ``112300`` poultry (animal feed and meat/seafood), ``212310``
    stone/sand/gravel, ``336414`` guided missiles, and ``S00402`` used and
    secondhand goods, which spans eight. Together they are 13.5% of ``TRANS``.
    """
    crosswalk = pd.read_csv(crosswalkpath / _CROSSWALK, dtype=str).fillna('')
    pairs = crosswalk.loc[
        crosswalk['ActivitySourceName'] == _CROSSWALK_SOURCE,
        ['Activity', _CROSSWALK_BEA_COLUMN],
    ]
    pairs.columns = pd.Index(['sctg', COMMODITY_LEVEL])
    return pairs.drop_duplicates().reset_index(drop=True)


def commodity_ton_miles(years: tuple[int, ...]) -> pd.DataFrame:
    """
    Ton-miles per BEA commodity and year - the sum over the SCTGs it belongs to.

    A commodity in more than one SCTG takes the total of them, which weights
    each SCTG by its own ton-miles. Only the *ratio* of these figures is ever
    used, so no share of an SCTG is allocated to a commodity and none of this
    double-counts: two commodities in the same SCTG each carry the whole group's
    ton-miles, and both are moved by the same growth.
    """
    ton_miles = sctg_ton_miles(_as_years(years))
    missing = set(sctg_to_commodity()['sctg']) - set(ton_miles.index)
    if missing:
        raise ValueError(
            f'{_CROSSWALK} names SCTGs the FBS does not carry: {sorted(missing)}. '
            f'FAF has most likely renamed them; reconcile the crosswalk Activity '
            f'column against the metadata workbook.'
        )
    return (
        sctg_to_commodity()
        .merge(ton_miles, left_on='sctg', right_index=True)
        .groupby(COMMODITY_LEVEL)[list(ton_miles.columns)]
        .sum()
    )


def ton_mile_growth(years: tuple[int, ...]) -> pd.DataFrame:
    """
    Each commodity's ton-miles relative to :data:`ANCHOR_YEAR`. Anchor column 1.

    This is the whole cross-sectional content of the nowcast: how much more or
    less freight each commodity's group moves than it did in 2017. Coal falls to
    0.64 by 2024 and pharmaceuticals reach 1.74, which is the shape the 2017
    table cannot know and the volume data can.
    """
    ton_miles = commodity_ton_miles(years)
    return ton_miles.div(ton_miles[ANCHOR_YEAR], axis=0)


@functools.cache
def _gross_output(years: tuple[int, ...]) -> pd.DataFrame:
    """Gross output per BEA detail industry and year, USD."""
    return pd.DataFrame(
        {
            year: getFlowByActivity(
                GROSS_OUTPUT_SOURCE, year, download_FBA_if_missing=False
            )
            .set_index('ActivityProducedBy')['FlowAmount']
            .mul(_MILLIONS)
            for year in _as_years(years)
        }
    )


@functools.cache
def pce_final_use_index(years: tuple[int, ...]) -> pd.DataFrame:
    """
    Each mode's PCE relative to :data:`ANCHOR_YEAR`. Anchor column 1.

    The annual final-use driver of the :data:`RESIDUAL` treatment, built from
    two sources neither of which is annual on its own: NIPA table 2.4.5U, which
    is annual but by PCE category, and the 2017 PCE bridge, which maps category
    to commodity but exists for benchmark years only. Holding the bridge at 2017
    and moving the categories is the intended use of it - the bridge supplies
    structure, NIPA supplies the year.

    The join is on the bridge's ``NIPA Line`` column, so no name matching is
    involved. Thirteen bridge rows reach the five modes, and they reproduce the
    2017 SUT exactly: air 129,784 million against 129,785 published, water
    22,027, truck 11,700, rail 1,458, pipeline nil.

    Where a NIPA line splits across commodities its 2017 split is held fixed,
    the same assumption the bridge itself carries. Pipeline has no PCE at all,
    so its index is 1 throughout and unused - :data:`MODE_CONTROL` gives it
    :data:`OUTPUT_RATIO` for exactly that reason.
    """
    wanted = _as_years(years)
    pce = pd.DataFrame({year: _pce_by_nipa_line(year) for year in wanted}).fillna(0.0)

    bridge = load_2017_pce_bridge_detail_usa()
    line = pd.to_numeric(bridge['NIPA Line'], errors='coerce')
    bridge = bridge.assign(_line=line).dropna(subset=['_line'])
    line_total = bridge.groupby('_line')["Purchasers' Value"].transform('sum')
    bridge = bridge.assign(_share=bridge["Purchasers' Value"] / line_total)

    modes = list(TRANSPORT_MODE_COMMODITIES)
    rows = bridge[bridge[COMMODITY_LEVEL].isin(modes)].dropna(subset=['_share'])
    missing = set(rows['_line']) - set(pce.index)
    if missing:
        raise ValueError(
            f'the 2017 PCE bridge names NIPA lines that {FINAL_USE_TABLE} does '
            f'not carry: {sorted(missing)}. The bridge and the NIPA table have '
            f'come from different vintages.'
        )
    joined = rows.join(pce, on='_line')
    by_mode = (
        joined[list(wanted)]
        .mul(joined['_share'], axis=0)
        .groupby(joined[COMMODITY_LEVEL])
        .sum()
        .reindex(modes)
        .fillna(0.0)
    )
    anchor = by_mode[ANCHOR_YEAR].replace(0.0, float('nan'))
    return by_mode.div(anchor, axis=0).fillna(1.0)


@functools.cache
def _pce_by_nipa_line(year: int) -> pd.Series:
    """Personal consumption expenditure per NIPA line of :data:`FINAL_USE_TABLE`."""
    fba = getFlowByActivity(FINAL_USE_SOURCE, year, download_FBA_if_missing=False)
    parsed = (
        fba['Description']
        .astype(str)
        .str.extract(r'^(?P<table>[^:]+): \S+ - (?P<line>\d+)$')
    )
    parsed['FlowAmount'] = fba['FlowAmount'].to_numpy()
    rows = parsed[parsed['table'] == FINAL_USE_TABLE].dropna()
    return (
        rows.assign(line=pd.to_numeric(rows['line']))
        .groupby('line')['FlowAmount']
        .sum()
    )


@functools.cache
def mode_ton_miles(years: tuple[int, ...]) -> pd.DataFrame:
    """
    Freight ton-miles per mode and year, from the FBS's producing-sector column.

    The counterpart of :func:`sctg_ton_miles`, summed the other way. Freight-only
    by construction, which is the whole point for air and water: FAF knows
    nothing about passengers, so this moves air freight without air fares.
    """
    frames = {}
    for year in _as_years(years):
        fbs = getFlowBySector(
            FBS_METHOD.format(year=year),
            download_FBAs_if_missing=False,
            download_FBS_if_missing=False,
        )
        by_sector = fbs.groupby('SectorProducedBy')['FlowAmount'].sum()
        by_sector.index = pd.Index(
            [f'{sector}000' for sector in by_sector.index], name=COMMODITY_LEVEL
        )
        frames[year] = by_sector
    return pd.DataFrame(frames).reindex(list(TRANSPORT_MODE_COMMODITIES))


@functools.cache
def mode_price_index(years: tuple[int, ...]) -> pd.DataFrame:
    """
    Chain-type price index per mode and year, rebased so the anchor year is 1.

    ``UGO304-A``, the price companion of the ``UGO305-A`` gross output this
    module's other level source reads. Whole-industry, so air's carries fares -
    see the docstring warning.
    """
    detail = map_detail_table(load_pi_detail()).rename(
        columns={'sector_code': COMMODITY_LEVEL}
    )
    indexed = detail.set_index(COMMODITY_LEVEL)
    wanted = [str(year) for year in _as_years(years)]
    frame = indexed.loc[list(TRANSPORT_MODE_COMMODITIES), wanted].astype(float)
    frame.columns = pd.Index(_as_years(years))
    return frame.div(frame[ANCHOR_YEAR], axis=0)


def control_total_components(
    years: tuple[int, ...], method: str = MIXED
) -> pd.DataFrame:
    """
    The margin each transport mode gives up, per year. USD, index the five
    :data:`TRANSPORT_MODE_COMMODITIES`.

    *method* selects the treatment: :data:`MIXED` dispatches per mode on
    :data:`MODE_CONTROL` and is the default; the other three apply one treatment
    to all five, which is what :func:`control_total_comparison` uses and what
    #620 will judge against external sources.

    One of three treatments per mode, dispatched on :data:`MODE_CONTROL` - see
    the module docstring for why no single one serves all five:

    ``residual``
        ``output[t] - direct_uses[2017] x pce_index[t]``, where direct uses are
        output less the margin given up in 2017. Truck and rail.
    ``freight_volume``
        ``given_up[2017] x ton_miles[t] x price_index[t]``, all three relative
        to the anchor. Air and water.
    ``output_ratio``
        ``output[t] x (given_up / output)[2017]``. Pipeline.

    Every treatment is an identity in the anchor year, so all five reproduce the
    published 2017 give-up exactly and the choice affects only the movement.

    This is also the *negative* side of the ``TRANS`` column, commodity by
    commodity. Step 4a owns that side and derives it from commodity output; it
    is produced here so the two can be checked against each other, and because
    the positive side has to be controlled to something.
    """
    by_method = _give_up_by_method(_as_years(years))
    if method != MIXED:
        if method not in by_method:
            raise ValueError(
                f'unknown control method {method!r}; expected one of '
                f'{(MIXED, *by_method)}'
            )
        return by_method[method].rename_axis(COMMODITY_LEVEL)
    return pd.concat(
        [
            by_method[MODE_CONTROL[mode]].loc[[mode]]
            for mode in TRANSPORT_MODE_COMMODITIES
        ]
    ).rename_axis(COMMODITY_LEVEL)


def _give_up_by_method(years: tuple[int, ...]) -> dict[str, pd.DataFrame]:
    """Each single treatment applied to all five modes. USD."""
    modes = list(TRANSPORT_MODE_COMMODITIES)
    output = _gross_output(years).loc[modes]
    given_up = margins_by_commodity()['margin_given_up'].loc[modes]
    ton_miles = mode_ton_miles(years)
    return {
        RESIDUAL: output.sub(
            pce_final_use_index(years).mul(output[ANCHOR_YEAR] - given_up, axis=0)
        ),
        FREIGHT_VOLUME: ton_miles.div(ton_miles[ANCHOR_YEAR], axis=0)
        .mul(mode_price_index(years))
        .mul(given_up, axis=0),
        OUTPUT_RATIO: output.mul(given_up / output[ANCHOR_YEAR], axis=0),
    }


def control_total_comparison(years: tuple[int, ...]) -> pd.DataFrame:
    """
    Every control-total construction side by side, per year. USD, columns the
    methods plus :data:`MIXED`.

    The artifact for
    `#620 <https://github.com/cornerstone-data/bedrock/issues/620>`_, which
    settles the choice against BTS_TSA and Freight Facts & Figures rather than
    against itself. All four are identities in the anchor year, so they differ
    only in movement - and they differ by **12% at 2022**, from 575,587 million
    on :data:`FREIGHT_VOLUME` to 643,443 million on :data:`MIXED`. That spread
    is the single largest open uncertainty in the transport column, which is why
    the method is selectable rather than settled here.

    ⚠️ :data:`RESIDUAL` in this table is applied to *all five* modes, which is
    diagnostic only - it goes negative for air from 2019 and water from 2023.
    """
    by_method = _give_up_by_method(_as_years(years))
    columns = {name: frame.sum() for name, frame in by_method.items()}
    columns[MIXED] = control_total_components(years, method=MIXED).sum()
    return pd.DataFrame(columns).rename_axis('Year')


def transport_margin_control_total(
    years: tuple[int, ...], method: str = MIXED
) -> pd.Series:
    """
    Total transport margin per year. USD, indexed by year.

    The mode composite of :func:`control_total_components`, rescaled so the
    anchor year equals the published 2017 total exactly. The rescaling is
    0.24%: the composite reaches 415,548 million against a published 414,559
    million received, the gap being the handful of negative ``F03000``
    inventory-timing rows on the receiving side.
    """
    composite = control_total_components(years, method=method).sum()
    anchor = _anchor_margins().sum()
    return composite * (anchor / composite[ANCHOR_YEAR])


@functools.cache
def _anchor_margins() -> pd.Series:
    """Published 2017 transport margin per commodity, receiving side only."""
    transport = margins_by_commodity()['transport_margins']
    return transport[transport > 0]


def transport_margins(years: tuple[int, ...], method: str = MIXED) -> pd.DataFrame:
    """
    Transport margin per commodity and year. USD, commodities x years.

    The published 2017 column moved by each commodity's ton-mile growth and
    scaled to :func:`transport_margin_control_total`. Reindexed to all 402
    commodities, so one bearing no transport margin appears as zero rather than
    dropping out, and the anchor-year column reproduces the published one
    exactly.

    The five transport commodities themselves stay at zero here - they *give up*
    margin rather than receive it, and that side is
    :func:`control_total_components`.
    """
    anchor = _anchor_margins()
    growth = ton_mile_growth(years).reindex(anchor.index)
    if growth.isna().to_numpy().any():
        missing = sorted(growth.index[growth.isna().any(axis=1)])
        raise ValueError(
            f'{len(missing)} commodities receive 2017 transport margin but have '
            f'no ton-miles to move by: {missing[:10]}. Every one of the 258 is '
            f'covered by {_CROSSWALK}, so this means the anchor or the crosswalk '
            f'has changed.'
        )
    shape = growth.mul(anchor, axis=0)
    control = transport_margin_control_total(years, method=method)
    scaled = shape.mul(control / shape.sum(), axis=1)
    return scaled.reindex(list(USA_2017_COMMODITY_CODES), fill_value=0.0).rename_axis(
        COMMODITY_LEVEL
    )


def movement_summary(years: tuple[int, ...]) -> pd.DataFrame:
    """
    The two movements side by side, per year, both indexed to the anchor at 1.

    ``volume``
        what ton-miles alone would do to the total.
    ``level``
        what the control total does.
    ``repricing``
        ``level / volume`` - the part of the change that is freight rates rather
        than freight. It reaches 1.43 in 2022 and is the single largest thing
        this module does; a build that dropped the control total would lose it.
    """
    anchor_total = _anchor_margins().sum()
    shape = (
        ton_mile_growth(years)
        .reindex(_anchor_margins().index)
        .mul(_anchor_margins(), axis=0)
    )
    volume = shape.sum() / anchor_total
    level = transport_margin_control_total(years) / anchor_total
    return pd.DataFrame(
        {'volume': volume, 'level': level, 'repricing': level / volume}
    ).rename_axis('Year')
