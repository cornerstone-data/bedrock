"""Annual transport margin by commodity, from the 2017 anchor and FAF ton-miles.

Step 4c of the nowcast build
(``bedrock/analysis/nowcasting/margins_estimation_plan.md``), phase 2
(`#611 <https://github.com/cornerstone-data/bedrock/issues/611>`_). Phase 1
(:mod:`bedrock.transform.iot.nowcast_margins`) produced the 2017 structure; this
module moves the transport column of it to an annual year. Transport is the one
margin with an external source that reaches every commodity, so it is settled
here rather than waiting for the trade levels in phase 3.

**Shape from ton-miles, level from output.** The construction is two independent
movements of the same 2017 column:

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
1.04 by 2022 while the nominal control reaches 1.48. The level therefore comes
from the transport industries' own gross output, held at each mode's 2017 ratio
of margin given up to output - which is the same number the negative side of the
``TRANS`` column carries, so the two sides stay consistent by construction.

⚠️ **A better control total is designed and not yet built.** The give-up ratio
here is frozen at 2017, which is the weak point the limitations below all come
back to. The Supply table closes ``T016 = T013 + TRANS + TOP + SUB``, so the
margin can instead be taken as a residual against direct uses moved by an annual
final-use index - well conditioned for truck, rail and pipeline (leverage 0.19,
0.14, 0.01; 96.2% of ``TRANS``) and hopeless for air and water (43.5 and 3.2),
which would keep a freight-volume-times-price form. It is blocked on an annual
final-use index: ``NIPA_FD`` works for 2017 only, since the later years carry
2017 NIPA line numbers and drop most of their output. See
``margins_estimation_plan.md`` §Agreed refinement, and #620 for the external
validation of whichever driver wins.

⚠️ **Three limitations of the construction as built.**

*The 2017/2018 seam is partly methodological.* FAF's 2017 is anchored on the
Commodity Flow Survey and 2018 onward are modelled. Aggregate ton-miles move
only 1.9% across that seam, but the composition moves much more: 21% of 2017
``TRANS`` sits in SCTG groups whose ton-miles jump by more than 25% in that one
year, and the ``TRANS``-weighted mean step is 1.124 against the aggregate's
1.019. The control total absorbs the level, so what survives is a shape shift.

*Air and rail output are not freight-only.* :func:`control_total_components`
scales each mode's whole gross output, and air's includes passengers - which is
why air's 2017 give-up ratio is 0.026 where rail's is 0.862. Holding that ratio
fixed makes air freight margin follow passenger air output. Air is 1.5% of
``TRANS``, so the error is bounded; truck, at 68%, is the mode that matters and
is close to freight-pure.

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

from bedrock.extract.flowbyactivity import getFlowByActivity
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
    pairs.columns = ['sctg', COMMODITY_LEVEL]
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


def control_total_components(years: tuple[int, ...]) -> pd.DataFrame:
    """
    The margin each transport mode gives up, per year. USD, index the five
    :data:`TRANSPORT_MODE_COMMODITIES`.

    Each mode's gross output at its 2017 ratio of margin-given-up to output.
    That ratio is what carries the nominal freight rate: truck output runs 1.57x
    its 2017 level by 2022 against ton-miles that barely move, which is the
    2021-22 rate surge and the reason the volume index alone will not do.

    This is also the *negative* side of the ``TRANS`` column, commodity by
    commodity. Step 4a owns that side and derives it from commodity output; it
    is produced here so the two can be checked against each other, and because
    the positive side has to be controlled to something.
    """
    modes = list(TRANSPORT_MODE_COMMODITIES)
    output = _gross_output(years).loc[modes]
    given_up = margins_by_commodity()['margin_given_up'].loc[modes]
    return output.mul(given_up / output[ANCHOR_YEAR], axis=0)


def transport_margin_control_total(years: tuple[int, ...]) -> pd.Series:
    """
    Total transport margin per year. USD, indexed by year.

    The mode composite of :func:`control_total_components`, rescaled so the
    anchor year equals the published 2017 total exactly. The rescaling is
    0.24%: the composite reaches 415,548 million against a published 414,559
    million received, the gap being the handful of negative ``F03000``
    inventory-timing rows on the receiving side.
    """
    composite = control_total_components(years).sum()
    anchor = _anchor_margins().sum()
    return composite * (anchor / composite[ANCHOR_YEAR])


@functools.cache
def _anchor_margins() -> pd.Series:
    """Published 2017 transport margin per commodity, receiving side only."""
    transport = margins_by_commodity()['transport_margins']
    return transport[transport > 0]


def transport_margins(years: tuple[int, ...]) -> pd.DataFrame:
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
    control = transport_margin_control_total(years)
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
