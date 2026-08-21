"""Wholesale and retail trade margin allocation for the nowcast Margins dataset.

Step 4c of the nowcast build
(``bedrock/analysis/nowcasting/margins_estimation_plan.md``), phase 3
(`#612 <https://github.com/cornerstone-data/bedrock/issues/612>`_, closed) and
the trade half of phase 4
(`#613 <https://github.com/cornerstone-data/bedrock/issues/613>`_). It is the
sibling of :mod:`bedrock.transform.iot.nowcast_transport_margins` and is built
to the same shape: an observed annual level per giver, a commodity allocation
on the best available basis, and a column that sums to zero.

**Trade output essentially *is* margin.** Gross output of wholesale and retail
excludes the cost of goods purchased for resale, so the trade sectors give up
very nearly all of their own output - eight retail commodities give up exactly
100% of ``T013`` and the ten wholesale ones 90.8-99.4%. That is why the control
total is a *margin* series and never a sales one: sales carries COGS, which is
not the trade sector's revenue at all.

The 19 givers
-------------

============  ======  =====================================================
kind          count   BEA 2017 commodities
============  ======  =====================================================
wholesale     10      ``423100`` ``423400`` ``423600`` ``423800`` ``423A00``
                      ``424200`` ``424400`` ``424700`` ``424A00`` ``425000``
retail         9      ``441000`` ``444000`` ``445000`` ``446000`` ``447000``
                      ``448000`` ``452000`` ``454000`` ``4B0000``
============  ======  =====================================================

They give up **3,264,931 $M** in 2017 against 255 receiving commodities.

⚠️ **The give-up is not the published Wholesale and Retail margin columns.**
Those total 3,656,094 $M; the 391,163 $M difference is the **trade-level tax**
that ``TOP`` carries, so ``sum(W + R) = TRADE + TOP`` rather than ``= TRADE``.
Both controls are exposed - :func:`trade_control_total` for the Supply column
and :func:`gross_margin_control_total` for the Margins table's own columns - and
mixing them up double-counts or deletes that tax. See the plan's "The residual
decomposes ``TOP`` into producer-level and trade-level tax".

The annual level: anchor on BEA, move with Census
-------------------------------------------------

⚠️ **The Census margin is an index, not a level.** AWTS covers merchant
wholesalers only, while BEA's wholesale margin also carries manufacturers' sales
branches and offices and agents and brokers, so the 2017 Census figure is barely
half the BEA one. Substituting it would delete 42% of the wholesale margin.

======================  ==============  ==============  ==========
2017, $M                Census          BEA give-up     ratio
======================  ==============  ==============  ==========
wholesale, NAICS 42     1,100,925       1,718,990       **1.561**
retail, ``Total``       1,458,243       1,545,941       **1.061**
======================  ==============  ==============  ==========

:func:`trade_coverage_ratio` freezes that ratio at 2017 and
:func:`trade_control_total` multiplies it by the observed Census margin, so 2017
is an identity and a nowcast year moves with the source. This is the same
construction the transport side uses, for the same reason, and it is the whole
modelling content of the annual control.

⚠️ **Read the published total row, never the sum of sub-industries.**
Suppression varies by year, so the four-digit sum runs 1.000 of the NAICS 42 row
in 2012-16 and 2019-21 but 0.948 in 2017, 0.941 in 2018 and **0.839 in 2022**.
On the 4-digit sum, wholesale 2021->2022 reads -6.4% where the published row
moves +11.4% - a 17.8pp error in one year of the growth factor.
:func:`census_gross_margin` reads the published row;
:func:`census_margin_by_giver` recovers the suppressed detail *by subtraction
from it* rather than treating a suppressed cell as zero.

The AWTS/ARTS -> AIES splice
----------------------------

The annual economic surveys were consolidated into the Annual Integrated
Economic Survey from data year 2023, so ``Census_AWTS`` and ``Census_ARTS`` stop
at 2022 and ``Census_AIES`` carries 2023.

⚠️ **Type of operation decides which side is populated.** AIES publishes
wholesale margin under ``TYPOP`` ``1X`` and **zero** under ``00``; retail is the
other way round. Reading either at the wrong code returns a well-formed zero
rather than an error, silently deleting one side of the trade margin. The codes
are pinned in :data:`_AIES_TYPE_OF_OPERATION`.

✅ **The splice was tested and it holds.** ``1X`` is exactly what the AWTS
workbook is - its ``nomsbo`` table - and the wholesale margin *rate* moves 20.1%
to 20.4% across the seam. The retail rate steps harder, 31.3% to 34.2%, and that
was open until the Economic Census basis was ruled out: AIES 2023 wholesale sits
0.29pp from AWTS and 5.8pp from the Economic Census, so the consolidation did not
adopt the EC footing, and a survey that changed basis for retail but not
wholesale is not a coherent reading of one integrated instrument. The retail step
enters the index as a real move. See the plan's "Closed - AIES stayed on the
annual basis".

⚠️ **2024 has no source at all.** AIES returns 204 No Content for every year
except 2023 and 2024 is not published, while the nowcast window runs to 2024. So
:data:`TRADE_MARGIN_YEARS` stops at 2023 and :func:`census_gross_margin`
**raises** for 2024 unless ``allow_extrapolation=True`` is passed - a modelled
level should not arrive by default in a column every other year of which is
observed. The trailing-trend fill is available behind that flag, and the real fix
is step 4a's 2024 commodity output, which observes trade margin directly because
trade output is margin.

⚠️ **``425000`` has no annual source of its own.** Wholesale electronic markets,
agents and brokers never take title, so they book a *commission* rather than a
margin, and AWTS - the ``nomsbo`` table - excludes them by construction; AIES
publishes no ``425`` under ``1X`` either. The only source is Economic Census
``ecncomm``, which is quinquennial and so cannot supply annual movement. It
therefore carries the wholesale aggregate's growth, which is 0.7% of the
wholesale give-up and 0.37% of ``TRADE``.
"""

from __future__ import annotations

import functools
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot import nowcast_margins as nm
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

#: The margins anchor year, the only year with a published Supply table.
ANCHOR_YEAR = 2017

#: The two kinds of trade margin, which are two different problems: retail is
#: 87.5% consumed by PCE at a rate 2.2x its non-PCE one, wholesale is spread 8.8x
#: more widely at a rate that barely varies by buyer.
TRADE_KINDS: tuple[str, ...] = ('wholesale', 'retail')

#: The BEA 2017 commodities that give up each kind of margin.
GIVER_COMMODITIES: dict[str, tuple[str, ...]] = {
    'wholesale': (
        '423100',
        '423400',
        '423600',
        '423800',
        '423A00',
        '424200',
        '424400',
        '424700',
        '424A00',
        '425000',
    ),
    'retail': (
        '441000',
        '444000',
        '445000',
        '446000',
        '447000',
        '448000',
        '452000',
        '454000',
        '4B0000',
    ),
}

#: The published Margins table column each kind aggregates from.
MARGIN_COLUMN = {'wholesale': 'Wholesale', 'retail': 'Retail'}

#: Census trade code -> BEA 2017 trade commodity. A complete partition in both
#: directions: every AWTS four-digit and ARTS three-digit code maps to exactly
#: one giver, and every giver but ``425000`` is covered.
TRADE_CROSSWALK_PATH = (
    Path(__file__).parents[2]
    / 'utils'
    / 'mapping'
    / 'Crosswalk_Census_Trade_to_BEA_2017.csv'
)

#: The giver with no annual Census source - see the module docstring.
UNSOURCED_GIVER = '425000'

#: The published total row each survey carries the kind's control on.
_PUBLISHED_TOTAL_ROW = {
    ('wholesale', 'Census_AWTS'): '42',
    ('wholesale', 'Census_AIES'): '42',
    ('retail', 'Census_ARTS'): 'Total',
    ('retail', 'Census_AIES'): '44-45',
}

#: ⚠️ Not a detail. AIES publishes wholesale margin under 1X and zero under 00;
#: retail is the other way round, and the wrong code returns a well-formed zero.
_AIES_TYPE_OF_OPERATION = {'wholesale': '1X', 'retail': '00'}

#: The item name both standalone surveys and AIES publish the margin under.
GROSS_MARGIN_ITEM = 'Gross margins'

#: Last year of the standalone surveys, and the first year of AIES.
LAST_STANDALONE_YEAR = 2022
FIRST_AIES_YEAR = 2023

#: Years with an observed Census margin. 2024 is extrapolated - see
#: :func:`census_gross_margin`.
FIRST_OBSERVED_YEAR = 2012
LAST_OBSERVED_YEAR = FIRST_AIES_YEAR

#: How many trailing observed years the 2024 extrapolation takes its median
#: growth over. Five, so that 2021's post-pandemic 22-27% rebound is outvoted
#: rather than decisive - see :func:`_extrapolated_margin`.
EXTRAPOLATION_WINDOW = 5

#: The years TRADE is sourced for. Stops at the last observed Census year: 2024
#: has no source in any survey and is filled only on an explicit opt-in.
TRADE_MARGIN_YEARS: tuple[int, ...] = tuple(range(ANCHOR_YEAR, LAST_OBSERVED_YEAR + 1))

_SOURCE_FOR_KIND = {'wholesale': 'Census_AWTS', 'retail': 'Census_ARTS'}


def _check_kind(kind: str) -> str:
    if kind not in TRADE_KINDS:
        raise ValueError(
            f'kind must be one of {TRADE_KINDS}, not {kind!r}. It selects the '
            f'source, the type-of-operation code, the published total row and the '
            f'set of giving commodities, so a wrong value would allocate one kind '
            f"of trade margin on the other kind's basis."
        )
    return kind


@functools.cache
def load_trade_crosswalk() -> pd.DataFrame:
    """Census trade code -> BEA 2017 giver commodity, with the kind it belongs to."""
    crosswalk = pd.read_csv(TRADE_CROSSWALK_PATH, dtype=str).assign(
        census_code=lambda x: x['census_code'].str.strip(),
        bea_2017_commodity=lambda x: x['bea_2017_commodity'].str.strip(),
    )
    mapped = set(crosswalk['bea_2017_commodity'])
    expected = set(GIVER_COMMODITIES['wholesale']) | set(GIVER_COMMODITIES['retail'])
    unexpected = mapped - expected
    if unexpected:
        raise ValueError(
            f'{TRADE_CROSSWALK_PATH.name} maps to commodities that do not give up '
            f'trade margin: {sorted(unexpected)}. A giver that is not in '
            f'GIVER_COMMODITIES would take a share of the control total and never '
            f'appear on the negative side, breaking sum(TRADE) = 0.'
        )
    if crosswalk['census_code'].duplicated().any():
        duplicated = sorted(
            crosswalk.loc[crosswalk['census_code'].duplicated(), 'census_code']
        )
        raise ValueError(
            f'{TRADE_CROSSWALK_PATH.name} maps {duplicated} more than once. The '
            f'Census codes partition the published total, so a repeat double-counts '
            f'that code into two givers.'
        )
    return crosswalk


# --- the observed Census margin --------------------------------------------


def _census_fba(kind: str, year: int) -> tuple[pd.DataFrame, str]:
    """The FBA carrying *kind*'s margin in *year*, and the source it came from."""
    source = 'Census_AIES' if year >= FIRST_AIES_YEAR else _SOURCE_FOR_KIND[kind]
    fba = getFlowByActivity(source, year)
    rows = fba[fba['FlowName'].astype(str).str.strip() == GROSS_MARGIN_ITEM].copy()
    if source == 'Census_AIES':
        type_of_operation = _AIES_TYPE_OF_OPERATION[kind]
        rows = rows[rows['Description'].astype(str).str.strip() == type_of_operation]
        if rows.empty:
            raise ValueError(
                f'Census_AIES {year} has no {kind} gross margin at TYPOP '
                f'{type_of_operation!r}. AIES publishes wholesale only under 1X and '
                f'retail only under 00, and the wrong code returns a well-formed '
                f'zero - so an empty result here means the type-of-operation coding '
                f'moved, not that trade stopped earning a margin.'
            )
    rows['ActivityProducedBy'] = rows['ActivityProducedBy'].astype(str).str.strip()
    return rows, source


def census_gross_margin(
    kind: str, year: int, allow_extrapolation: bool = False
) -> float:
    """
    Observed Census gross margin for *kind* in *year*, USD.

    Read off the **published total row** - NAICS 42 for wholesale, ``Total`` (or
    AIES's ``44-45``) for retail - never the sum of the sub-industries, which
    suppression makes 16% short in 2022 alone.

    ⚠️ **2024 raises unless *allow_extrapolation* is passed.** It has no source
    in any survey and the extrapolation that fills it is a model, not an
    observation, so it is opt-in rather than a silent default - the same reason
    :data:`TRADE_MARGIN_YEARS` stops at 2023 and the transport side simply does
    not cover its own unsourced years.
    """
    _check_kind(kind)
    if year > LAST_OBSERVED_YEAR:
        if not allow_extrapolation:
            raise ValueError(
                f'{kind} gross margin is not published for {year}: AIES carries '
                f'{FIRST_AIES_YEAR} only and no later year has been released. Pass '
                f'allow_extrapolation=True to fill it from the trailing trend, and '
                f'be aware that produces a modelled level rather than an observed '
                f'one - see _extrapolated_margin.'
            )
        return _extrapolated_margin(kind, year)

    rows, source = _census_fba(kind, year)
    total_row = _PUBLISHED_TOTAL_ROW[kind, source]
    published = rows[rows['ActivityProducedBy'] == total_row]
    if published.empty:
        raise ValueError(
            f'{source} {year} has no published {total_row!r} row for {kind}. That '
            f'row is the control total; summing the sub-industries instead would '
            f'silently subtract the suppressed cells - 16% of wholesale in 2022.'
        )
    return float(published['FlowAmount'].sum())


def _extrapolated_margin(kind: str, year: int) -> float:
    """
    *kind*'s margin in an unpublished year, grown off the last observed one.

    ⚠️ **This is a model, not an observation**, and it is the only thing in step
    4c that is. AIES carries 2023 only, so the last year of the nowcast window
    has no trade control total at all.

    The growth applied is the **median** of the last
    :data:`EXTRAPOLATION_WINDOW` observed year-on-year changes. Median rather
    than mean, because the window necessarily spans 2021, when wholesale margin
    rose 26.7% and retail 22.4% as the post-pandemic restocking ran through the
    trade sectors. A three-year mean puts wholesale at +12.4%/yr - carried almost
    entirely by that one year, and against a 2023 that actually *fell* 1.0%. The
    median discards it as the outlier it is.

    **The better replacement is not a longer window.** Trade output essentially
    *is* margin, so once step 4a's commodity output covers the trade commodities
    for 2024 it observes this quantity directly and should displace the trend
    entirely. That is the fix to make when 4a's coverage is confirmed, rather
    than tuning the window here.
    """
    observed = pd.Series(
        {
            y: census_gross_margin(kind, y)
            for y in range(
                LAST_OBSERVED_YEAR - EXTRAPOLATION_WINDOW + 1, LAST_OBSERVED_YEAR + 1
            )
        }
    )
    growth = float(observed.pct_change().dropna().median())
    level = observed[LAST_OBSERVED_YEAR] * (1 + growth) ** (year - LAST_OBSERVED_YEAR)
    log.warning(
        f'Census {kind} {year} is not published: extrapolating from '
        f'{LAST_OBSERVED_YEAR} at {growth:.2%}/yr to {level:,.0f} USD. This is a '
        f'modelled level, not an observation.'
    )
    return float(level)


def _census_detail(kind: str, year: int) -> tuple[pd.Series, set[str], str]:
    """Crosswalk-level Census margin for *year*, and which of those are suppressed."""
    rows, source = _census_fba(kind, year)
    codes = set(load_trade_crosswalk().query('kind == @kind')['census_code'])

    detail = rows[rows['ActivityProducedBy'].isin(codes)]
    missing = codes - set(detail['ActivityProducedBy'])
    if missing:
        raise ValueError(
            f'{source} {year} is missing {kind} codes {sorted(missing)}. They are '
            f'part of a partition of the published total, so a missing one is not a '
            f'suppressed cell to be recovered - it means the kind-of-business '
            f'structure moved and the crosswalk needs revisiting.'
        )
    suppressed = (
        set(detail.loc[detail['Suppressed'].notna(), 'ActivityProducedBy'])
        if 'Suppressed' in detail.columns
        else set()
    )
    return (
        detail.groupby('ActivityProducedBy')['FlowAmount'].sum(),
        suppressed,
        source,
    )


def census_margin_by_giver(kind: str, year: int) -> pd.Series:
    """
    Census gross margin per BEA giver commodity for *year*, USD.

    ⚠️ **A suppressed cell is recovered by subtraction from the published total,
    never by scaling the survivors.** Zeroing a suppressed cell is harmless in a
    detail table and not harmless here, because the parts *are* the whole:
    Census's sub-industries partition the published total row exactly, so a cell
    that parses to zero is a cell whose margin has been handed to its siblings.

    It bites in real years, not hypothetical ones. Gasoline stations - **5.1% of
    retail** - are suppressed in ARTS 2022, and professional equipment and drugs
    are suppressed together in AWTS 2022, which is why the four-digit wholesale
    sum is 83.9% of its published row that year. Proportional scaling would have
    given gasoline stations a margin of exactly zero in 2022 and spread its 5%
    over every other kind of business.

    So the residual ``published total - unsuppressed detail`` is distributed over
    the suppressed codes on their shares in :data:`ANCHOR_YEAR` (or the nearest
    year in which all of them are published). That assumes only that the
    suppressed kinds of business hold their relative sizes *among themselves*,
    which is far weaker than assuming they grew like the rest - and it puts the
    right order of magnitude on the right code.

    Any residual left when nothing is suppressed is ordinary rounding and is
    spread proportionally.

    ``425000`` is not in the crosswalk and takes no share here; it is added by
    :func:`giver_allocation` on the aggregate's growth.
    """
    _check_kind(kind)
    detail, suppressed, source = _census_detail(kind, year)
    published = census_gross_margin(kind, year)

    if suppressed:
        reference = _suppression_reference_shares(kind, year, suppressed)
        residual = published - float(detail.drop(index=list(suppressed)).sum())
        if residual <= 0:
            raise ValueError(
                f'{source} {year} {kind}: the unsuppressed detail already covers '
                f'the published total, leaving {residual:,.0f} for the suppressed '
                f'codes {sorted(suppressed)}. Suppression can only subtract, so a '
                f'non-positive residual means the total row and the detail are not '
                f'the same partition.'
            )
        recovered = reference * residual
        detail = detail.copy()
        detail.loc[list(suppressed)] = recovered.reindex(list(suppressed)).to_numpy()
        log.info(
            f'Margins_Trade {kind} {year}: recovered {len(suppressed)} suppressed '
            f'codes {sorted(suppressed)} by subtraction, {residual:,.0f} USD '
            f'({residual / published:.1%} of the published total)'
        )

    coverage = float(detail.sum()) / published
    if coverage > 1.001:
        raise ValueError(
            f'{source} {year} {kind} sub-industries sum to {coverage:.3f} of the '
            f'published total. Detail above its own control is not suppression, '
            f'which can only subtract - check the total row and the crosswalk.'
        )
    if coverage < 0.95:
        raise ValueError(
            f'{source} {year} {kind} detail covers only {coverage:.1%} of the '
            f'published total after suppression recovery. A gap that large is not '
            f'rounding - a kind of business is missing from the crosswalk.'
        )

    crosswalk = load_trade_crosswalk().query('kind == @kind')
    by_giver = (
        detail.rename('FlowAmount')
        .rename_axis('census_code')
        .reset_index()
        .merge(crosswalk[['census_code', 'bea_2017_commodity']], on='census_code')
        .groupby('bea_2017_commodity')['FlowAmount']
        .sum()
    )
    return by_giver / coverage


def _suppression_reference_shares(
    kind: str, year: int, suppressed: set[str]
) -> pd.Series:
    """
    The suppressed codes' shares *among themselves*, from the nearest clean year.

    :data:`ANCHOR_YEAR` first, then outward by distance, because the anchor is
    where every other 2017-frozen quantity in step 4c is taken and using it keeps
    one reference rather than several.
    """
    candidates = sorted(
        (y for y in range(FIRST_OBSERVED_YEAR, LAST_OBSERVED_YEAR + 1) if y != year),
        key=lambda y: (y != ANCHOR_YEAR, abs(y - ANCHOR_YEAR)),
    )
    for candidate in candidates:
        detail, also_suppressed, _ = _census_detail(kind, candidate)
        if suppressed & also_suppressed:
            continue
        shares = detail.reindex(sorted(suppressed))
        if shares.notna().all() and shares.sum() > 0:
            return shares / shares.sum()

    raise ValueError(
        f'{kind}: no year between {FIRST_OBSERVED_YEAR} and {LAST_OBSERVED_YEAR} '
        f'publishes all of {sorted(suppressed)} unsuppressed, so their shares among '
        f'themselves cannot be referenced. Splitting the residual evenly would put '
        f'a made-up number on a named kind of business.'
    )


# --- the annual control totals ---------------------------------------------


def _kind_give_up_2017(kind: str, supply: pd.Series | None = None) -> float:
    """What *kind*'s commodities give up in the published 2017 ``TRADE`` column."""
    trade = published_trade_by_commodity() if supply is None else supply
    givers = list(GIVER_COMMODITIES[_check_kind(kind)])
    return float(-trade.reindex(givers).sum())


@functools.cache
def published_trade_by_commodity() -> pd.Series:
    """
    The published 2017 Supply ``TRADE`` column, USD, by BEA 2017 commodity.

    Negative on the 19 givers, positive on the 255 receivers, summing to zero.

    ⚠️ **This cannot be built from the Margins transaction table.** That table's
    ``Wholesale`` and ``Retail`` columns are the margin each transaction
    *receives*; the give-up never appears in it, so summing them by commodity
    gives an all-positive series and a give-up of zero. The netted column only
    exists in the Supply bridge, which phase 1 already reconciles to the
    published table at -0.003%.
    """
    supply = _load_2017_detail_supply_use_usa('Supply_detail').rename(
        columns=lambda column: column.strip()
    )
    commodities = [code for code in USA_2017_COMMODITY_CODES if code in supply.index]
    trade = (
        supply.loc[commodities, 'TRADE'].astype(float) * MILLION_CURRENCY_TO_CURRENCY
    )
    trade.index.name = nm.COMMODITY_LEVEL
    return trade.rename('TRADE')


@functools.cache
def published_trade_received_by_kind() -> pd.DataFrame:
    """
    The positive side of the published 2017 ``TRADE`` column, split by kind. USD.

    ⚠️ **The Margins table's own ``Wholesale`` and ``Retail`` columns cannot be
    used directly as the receiving weight.** They are gross of the trade-level
    tax - they total 3,656,094 $M against a ``TRADE`` column of 3,264,931 $M -
    and that tax is **not** a constant share across commodities, so scaling them
    to a ``TRADE`` control leaves a per-commodity error that reaches 59% on the
    worst commodity even in the anchor year.

    So the netted column is apportioned instead: each commodity's published
    ``TRADE`` is split between the two kinds in the ratio its own ``Wholesale``
    and ``Retail`` margins stand in. The two columns then **sum back to the
    published ``TRADE`` column exactly**, which is what makes 2017 an identity,
    while each kind keeps its own commodity profile - retail concentrated in the
    goods households buy, wholesale spread 8.8x more widely.
    """
    margins = nm.load_margins_transactions_2017()
    by_kind = margins.groupby(level=nm.COMMODITY_LEVEL)[
        [MARGIN_COLUMN['wholesale'], MARGIN_COLUMN['retail']]
    ].sum()
    by_kind.columns = ['wholesale', 'retail']

    trade = published_trade_by_commodity().reindex(by_kind.index).fillna(0.0)
    received = trade.clip(lower=0.0)

    gross = by_kind.sum(axis=1)
    share = by_kind.div(gross.where(gross > 0), axis=0).fillna(0.0)
    return share.mul(received, axis=0)


def trade_coverage_ratio(kind: str) -> float:
    """
    *kind*'s 2017 BEA give-up divided by its 2017 Census gross margin.

    **1.561 wholesale, 1.061 retail** - and the two mean different things.

    Retail's is near 1 because ARTS covers essentially the whole sector: the
    Census margin and BEA's are the same object measured twice.

    ⚠️ **Wholesale's is not a coverage rounding.** AWTS is the ``nomsbo`` table,
    merchant wholesalers only, while BEA's wholesale margin also carries
    manufacturers' sales branches and offices - 2,331,241 $M of sales whose
    margin is **published in no vintage of any source** - and agents and brokers.
    So 36% of the wholesale margin is outside the series that moves it, and this
    ratio is what carries it. It holds as long as MSBO margin grows roughly like
    merchant wholesale; imputing it instead would add a component driven entirely
    by MSBO *sales*, which is a modelling choice dressed as data.
    """
    return _kind_give_up_2017(kind) / census_gross_margin(kind, ANCHOR_YEAR)


def trade_control_total(
    kind: str, year: int, allow_extrapolation: bool = False
) -> float:
    """
    The margin *kind* gives up in *year*, USD - the level to allocate.

    ``coverage ratio (frozen at 2017) x observed Census margin in year``, so
    *year* = 2017 reproduces the published give-up exactly and a nowcast year
    moves with the source. Same construction as
    :func:`~bedrock.transform.iot.nowcast_transport_margins.mode_control_total`.

    This is the **Supply ``TRADE``** control, net of the trade-level tax. For the
    Margins table's own Wholesale and Retail columns use
    :func:`gross_margin_control_total` instead.
    """
    return trade_coverage_ratio(kind) * census_gross_margin(
        kind, year, allow_extrapolation
    )


def gross_margin_control_total(kind: str, year: int) -> float:
    """
    *kind*'s published margin column in *year*, USD - gross of the trade tax.

    ⚠️ **This is the larger of the two controls and they are not
    interchangeable.** The published Wholesale and Retail columns total
    3,656,094 $M in 2017 where the ``TRADE`` give-up is 3,264,931 $M; the
    391,163 $M between them is the trade-level tax sitting in ``TOP``, so
    ``sum(W + R) = TRADE + TOP``. Applying this control to the Supply column
    double-counts that tax, and applying :func:`trade_control_total` to the
    Margins table deletes it.
    """
    _check_kind(kind)
    margins = nm.load_margins_transactions_2017()
    published_2017 = float(margins[MARGIN_COLUMN[kind]].sum())
    ratio = published_2017 / census_gross_margin(kind, ANCHOR_YEAR)
    return ratio * census_gross_margin(kind, year)


# --- the commodity allocation ----------------------------------------------


def giver_allocation(
    kind: str, year: int, control_total: float | None = None
) -> pd.Series:
    """
    *control_total* split across *kind*'s giving commodities, USD, negative.

    **Anchored on BEA, moved by Census - not set by Census.** The shape is each
    giver's published 2017 give-up rescaled by *its own* Census margin relative
    to 2017:

    .. code-block:: text

        shape[g] = published_2017[g] x census[g, year] / census[g, 2017]

    So the split between motor-vehicle wholesaling and grocery wholesaling moves
    with what Census observes each year, while 2017 reproduces the published
    give-up **exactly**, commodity by commodity.

    ⚠️ **Taking the Census shape directly instead would not reproduce 2017.** The
    two disagree substantially at this level of detail - drugs and druggists'
    sundries is 40% low on the Census split and machinery 30% high - because
    AWTS's kind-of-business coverage is not BEA's commodity. Only the *change* in
    the Census series is used, which is the same anchor-and-move discipline the
    control total and the transport side both follow.

    ``425000`` has no annual source and so holds its 2017 share of the wholesale
    give-up, which means it moves with the aggregate. It is 0.7% of wholesale.
    """
    _check_kind(kind)
    if control_total is None:
        control_total = trade_control_total(kind, year)

    published = -published_trade_by_commodity().reindex(list(GIVER_COMMODITIES[kind]))
    if year == ANCHOR_YEAR:
        shape = published
    else:
        census_now = census_margin_by_giver(kind, year)
        census_2017 = census_margin_by_giver(kind, ANCHOR_YEAR)
        growth = (census_now / census_2017).reindex(published.index)
        # 425000 has no Census series of its own, so it holds the kind's average
        growth = growth.fillna(float((census_now.sum() / census_2017.sum())))
        shape = published * growth

    allocation = -control_total * shape / shape.sum()

    expected = set(GIVER_COMMODITIES[kind])
    if set(allocation.index) != expected:
        raise ValueError(
            f'{kind} {year} allocated to {sorted(set(allocation.index))} rather '
            f'than the {len(expected)} commodities that give up {kind} margin. A '
            f'giver missing here keeps its share of the column on the positive '
            f'side only, so sum(TRADE) would not be zero.'
        )
    return allocation.rename(f'{kind}_give_up')


def receiving_allocation(
    year: int,
    control_total: float | None = None,
    within_commodity_weight: pd.Series | None = None,
    kind: str | None = None,
) -> pd.Series:
    """
    *control_total* spread over the 255 commodities that **receive** trade margin.

    ⚠️ **Combined across both kinds by default, and that is deliberate.** The
    receiving side cannot be split by kind and still reproduce 2017. The
    wholesale commodities give up 1,718,990 $M but only 1,701,091 $M of the
    ``TRADE`` column is apportionable to wholesale on the published ``Wholesale``
    and ``Retail`` shares; retail is 17,900 $M the other way. That gap is the
    **trade-level tax falling unevenly between the two kinds** - it is part of
    ``TOP``, not of ``TRADE`` - so forcing each kind's receiving side onto its own
    give-up total scales one up 1.05% and the other down 1.14% and puts that error
    into every commodity of the anchor year.

    ``TRADE`` is a single Supply column, so nothing downstream needs the split.
    The kind distinction is kept exactly where it is real: which commodities give
    the margin up, and how each kind's level moves. Pass *kind* to get one kind's
    apportioned share anyway - the Margins table's own ``Wholesale`` and
    ``Retail`` columns do need it - but control that with
    :func:`gross_margin_control_total`, not with the ``TRADE`` control.

    ⚠️ **The weight is BEA's own published 2017 column, and it is the whole of
    the commodity detail.** Unlike the give-up side - where Census publishes an
    annual kind-of-business split - nothing annual says which *goods* the margin
    attaches to. BEA's own answer is the product-line method
    (`#615 <https://github.com/cornerstone-data/bedrock/issues/615>`_), which
    needs a NAPCS -> I-O concordance that does not exist yet and is deferred.

    Until it lands this freezes the 2017 commodity mix, exactly as the transport
    side's within-group weight does and with the same consequence: the method can
    only reproduce detail BEA has already published. The level and the
    kind-of-business split still move annually; the goods mix does not.

    Pass *within_commodity_weight* to substitute another - commodity output
    ``T013`` is the obvious alternative.
    """
    if control_total is None:
        control_total = sum(trade_control_total(k, year) for k in TRADE_KINDS)

    if within_commodity_weight is not None:
        weight = within_commodity_weight
    elif kind is None:
        weight = published_trade_received_by_kind().sum(axis=1)
    else:
        weight = published_trade_received_by_kind()[_check_kind(kind)]

    givers = set(GIVER_COMMODITIES['wholesale']) | set(GIVER_COMMODITIES['retail'])
    weight = weight[~weight.index.isin(givers)]
    weight = weight[weight > 0]
    if weight.empty:
        raise ValueError(
            'No commodity has a positive published trade margin, so there is '
            'nothing to spread the control total over.'
        )
    name = 'received' if kind is None else f'{kind}_received'
    return (control_total * weight / weight.sum()).rename(name)


# --- the Supply table's TRADE column ---------------------------------------


def trade_margin_column(
    year: int = ANCHOR_YEAR, allow_extrapolation: bool = False
) -> pd.Series:
    """
    The Supply table's ``TRADE`` column for *year*. USD, by BEA 2017 commodity.

    Positive on the 255 commodities that receive trade margin, negative on the 19
    trade commodities that give it up, and **summing to zero** - margin is a
    redistribution, not value created, which is target T16's identity and the
    only constraint the balance places on step 4c's output.

    The **give-up side is per kind** - wholesale and retail move on their own
    Census index and land on their own ten and nine commodities - while the
    **receiving side is combined**, because splitting it by kind cannot reproduce
    2017; see :func:`receiving_allocation` for why.
    """
    controls = {
        kind: trade_control_total(kind, year, allow_extrapolation)
        for kind in TRADE_KINDS
    }
    given_up = [giver_allocation(kind, year, controls[kind]) for kind in TRADE_KINDS]
    received = receiving_allocation(year, sum(controls.values()))

    column = pd.concat([received, *given_up]).groupby(level=0).sum().rename('TRADE')

    residual = float(column.sum())
    scale = float(column.abs().sum())
    if scale and abs(residual) / scale > 1e-9:
        raise ValueError(
            f'TRADE {year} sums to {residual:,.2f} on {scale:,.0f} of gross mass '
            f'rather than to zero. Margin is a redistribution, so the receiving '
            f'side and the give-up side are the same dollars counted twice - a '
            f'non-zero total means one kind was controlled differently from the '
            f'other.'
        )
    return column


def control_total_table(
    years: Iterable[int], allow_extrapolation: bool = False
) -> pd.DataFrame:
    """Annual control totals per kind, USD."""
    years = list(years)
    return pd.DataFrame(
        {
            kind: {
                year: trade_control_total(kind, year, allow_extrapolation)
                for year in years
            }
            for kind in TRADE_KINDS
        }
    ).rename_axis('year')
