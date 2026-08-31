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

The annual level: the published summary cells (#769)
-----------------------------------------------------

✅ **The give-up level is read from the published summary Supply ``Trade``
column, per giver group and year** (:func:`published_group_giveup`), 2017-2024.
``T007`` comes from the same block, so the two sides of
``T016 = T007 + TRADE`` share one source and the knife-edge that produced
negative total supply is closed by construction.  Census still supplies what it
is evidence for: the **within-group** commodity split and the tax index.

❌ **The prior control was ``frozen 2017 coverage ratio x Census margin``**
(kept as :func:`census_index_control_total`), and it is what #769 retired.
AWTS covers merchant wholesalers only - the 2017 ratio was 1.561 wholesale,
1.061 retail - and the published tables' own implied annual quotient ran
1.561 -> 1.435-1.489 by 2021-2023 for wholesale (a shrinking non-merchant
universe the frozen ratio cannot see) and 0.966 for retail in 2023 (the AIES
splice's 31.3% -> 34.2% rate step, which BEA did not take).  The overstatement
peaked at **-341bn of give-up in 2023** and 8-12 of the 19 givers insolvent;
:mod:`bedrock.analysis.nowcasting.trade_data.giveup_solvency` is the referee
analysis.

⚠️ **Within a group, the split is capped at each giver's own output**
(water-filling in :func:`giver_allocation`): the published group ``T016`` is
non-negative so the group always has capacity, but the census shape can put
more on one member than its ``T007`` carries - ``454000`` nonstore most of
all, the #724 e-commerce classification question.  Excess lands on the group's
solvent members; :func:`check_giveup_solvency` proves the result before it
reaches Step 5.

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
from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
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

#: The bracket and step count for the trade-level tax tilt solve. The bracket is
#: wide enough to span any split from all-retail to all-wholesale, and geometric
#: bisection over it reaches machine precision well inside the step count.
_TILT_BRACKET = (1e-9, 1e9)
_TILT_BISECTION_STEPS = 200

#: Slack when checking a commodity's tax against the margin it is levied on. The
#: published table is in whole millions, so an exact comparison trips on rounding.
_TILT_ROUNDING_TOLERANCE = 1.0 * MILLION_CURRENCY_TO_CURRENCY

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
    """The FBA carrying *kind*'s margin in *year*, and the source it came from.

    ⚠️ ``download_FBA_if_missing=True`` is deliberate.  The default is ``False``
    (:data:`settings.DEFAULT_DOWNLOAD_IF_MISSING`), which makes the load order
    *local, then rebuild from the live Census endpoint* -- so a cache miss goes
    straight to the network and a bad response surfaces as
    ``ValueError: Excel file format cannot be determined`` from deep inside
    ``pandas``, hundreds of frames from anything that names Census.  With this
    flag the order is *local, then the published GCS artifact, then rebuild*,
    which is the same choice ``EPA_GHGI`` already makes for its curated FBAs.
    """
    source = 'Census_AIES' if year >= FIRST_AIES_YEAR else _SOURCE_FOR_KIND[kind]
    fba = getFlowByActivity(source, year, download_FBA_if_missing=True)
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
def published_gross_margin_by_kind() -> pd.DataFrame:
    """
    Published 2017 ``Wholesale`` and ``Retail`` per receiving commodity. USD.

    Gross of the trade-level tax, which is what the Margins table publishes.
    The givers are excluded: they carry the negative side, which is not in these
    columns at all.
    """
    margins = nm.load_margins_transactions_2017()
    by_kind = margins.groupby(level=nm.COMMODITY_LEVEL)[
        [MARGIN_COLUMN['wholesale'], MARGIN_COLUMN['retail']]
    ].sum()
    by_kind.columns = list(TRADE_KINDS)

    givers = set(GIVER_COMMODITIES['wholesale']) | set(GIVER_COMMODITIES['retail'])
    return by_kind[~by_kind.index.isin(givers)]


@functools.cache
def trade_level_tax_2017() -> pd.Series:
    """
    The trade-level tax per receiving commodity in 2017. USD.

    Straight from the identity, with nothing modelled::

        trade_level_tax[c] = Wholesale[c] + Retail[c] - TRADE[c]

    **391,162 $M**, and it is the reason the Margins table's margin columns
    (3,656,094 $M) exceed the Supply ``TRADE`` column (3,264,932 $M). It is
    sales tax: levied on the trade transaction and therefore sitting *inside*
    the margin columns, where excise sits inside Producers' Value instead -
    *"The nonmargin taxes (excise taxes) are embedded in the Producers value
    field... Your distinction is correct about sales tax vs excise taxes."*
    (B. Jolliff, BEA, 2025-06-16).

    Both land in ``TOP``, so this is also one side of ``TOP``'s decomposition:
    the other, producer-level, side is ``TOP - trade_level_tax``.
    """
    gross = published_gross_margin_by_kind()
    trade = published_trade_by_commodity().reindex(gross.index).fillna(0.0)
    return (gross.sum(axis=1) - trade).rename('trade_level_tax')


@functools.cache
def trade_level_tax_by_kind_2017() -> pd.DataFrame:
    """
    The 2017 trade-level tax split between wholesale and retail. USD.

    ⚠️ **The tax does not split pro rata, and assuming it does is a real
    error.** Retail carries **55.2%** of the trade-level tax on only a 48.2%
    share of the margin - which is what sales tax levied at the counter looks
    like in the accounts. The fitted tilt is **0.796**, wholesale relative to
    retail, and a pro-rata split would be a tilt of exactly 1.

    **Both column totals are observed, not assumed.** A trade commodity's
    published give-up is its margin *net* of the tax it collected, so the give-up
    side pins them::

        tax[wholesale] = 1,894,329 - 1,718,990 = 175,339
        tax[retail]    = 1,761,765 - 1,545,941 = 215,824
                                                 -------
                                                 391,163  = the tax total

    Per commodity it is one equation in two unknowns. With only two columns the
    whole family of solutions is one-dimensional, so rather than fitting
    biproportionally the tax is **tilted on a single scalar** and that scalar
    solved by bisection::

        share[c] = tilt * s[c] / (tilt * s[c] + 1 - s[c])

    with ``s[c]`` the commodity's wholesale share of gross margin. The column
    total is monotone in *tilt*, so bisection hits it **exactly** - the residual
    is $0, not a tolerance - and each commodity's own tax closes by construction
    because the two shares sum to 1.

    ⚠️ **Feasible, and checked rather than assumed.** 67 commodities bear
    wholesale margin and no retail, so their whole tax is forced to wholesale;
    one is forced the other way. Those forced amounts are 12,765 and 1 $M against
    targets of 175,339 and 215,824, and 378,396 $M of the tax sits on the 187
    commodities carrying both margins, so the solve has ample freedom. The
    result is checked to put no commodity's tax above the margin it is levied on.
    """
    gross = published_gross_margin_by_kind()
    tax = trade_level_tax_2017()
    target = gross['wholesale'].sum() - _kind_give_up_2017('wholesale')

    total = gross.sum(axis=1)
    wholesale_share = (gross['wholesale'] / total.where(total > 0)).fillna(0.0)

    def wholesale_tax(tilt: float) -> pd.Series:
        tilted = tilt * wholesale_share
        denominator = tilted + (1.0 - wholesale_share)
        return tax * (tilted / denominator.where(denominator > 0)).fillna(0.0)

    low, high = _TILT_BRACKET
    if not (wholesale_tax(low).sum() <= target <= wholesale_tax(high).sum()):
        raise ValueError(
            f'The wholesale share of the trade-level tax, {target:,.0f} USD, is '
            f'outside what any tilt can reach ('
            f'{wholesale_tax(low).sum():,.0f} to {wholesale_tax(high).sum():,.0f}). '
            f'That means the tax forced onto commodities bearing only one kind of '
            f'margin already exceeds a column total, so no split exists.'
        )
    for _ in range(_TILT_BISECTION_STEPS):
        middle = (low * high) ** 0.5
        if wholesale_tax(middle).sum() < target:
            low = middle
        else:
            high = middle

    wholesale = wholesale_tax((low * high) ** 0.5)
    fitted = pd.DataFrame({'wholesale': wholesale, 'retail': tax - wholesale})

    above_margin = (fitted > gross + _TILT_ROUNDING_TOLERANCE).any(axis=1)
    if above_margin.any():
        raise ValueError(
            f'The tax split puts more tax on {sorted(fitted.index[above_margin])} '
            f'than the margin it is levied on, which would make that margin '
            f'negative net of tax. The tilt is a single scalar and cannot respect '
            f'a per-commodity ceiling, so this needs a bounded solve rather than '
            f'a wider bracket.'
        )
    return fitted


@functools.cache
def published_trade_received_by_kind() -> pd.DataFrame:
    """
    The positive side of the published 2017 ``TRADE`` column, split by kind. USD.

    ``Wholesale - tax[wholesale]`` and ``Retail - tax[retail]``, so the split is
    **doubly exact**: the two columns sum to the published ``TRADE`` column per
    commodity, *and* each column sums to that kind's own published give-up.

    ⚠️ **An earlier version apportioned ``TRADE`` on the raw
    ``Wholesale``:``Retail`` ratio and got the second of those wrong** - it put
    wholesale at 1,701,091 $M against a give-up of 1,718,990 $M, and the
    17,900 $M gap was read as evidence that the receiving side *could not* be
    split by kind. It could; the gap was the tax being spread pro rata when it
    is not pro rata. Carrying :func:`trade_level_tax_by_kind_2017` as its own
    term removes it.
    """
    return published_gross_margin_by_kind() - trade_level_tax_by_kind_2017()


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


#: Summary commodity group -> the detail givers inside it.  ``42`` is the whole
#: wholesale kind; retail splits across four published groups, three of them
#: singletons.  These are the cells :func:`published_group_giveup` reads and the
#: units :func:`giver_allocation` rescales to.
GIVER_GROUPS: dict[str, tuple[str, ...]] = {
    '42': (
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
    '441': ('441000',),
    '445': ('445000',),
    '452': ('452000',),
    '4A0': ('444000', '446000', '447000', '448000', '454000', '4B0000'),
}

#: Which kind each summary giver group belongs to.
_GROUP_KIND = {
    '42': 'wholesale',
    '441': 'retail',
    '445': 'retail',
    '452': 'retail',
    '4A0': 'retail',
}


@functools.cache
def published_group_giveup(year: int) -> pd.Series:
    """The published summary ``Trade`` give-up per giver group, USD, positive.

    Read from the same summary Supply workbook the ``Detail_Supply`` control
    comes from, so the two sides of ``T016 = T007 + TRADE`` share one source by
    construction -- which is the entire fix for #769.  At 2017 these cells sum
    to the published detail give-up exactly (3,264,931 $M), so the anchor is
    unchanged.

    ⚠️ The column label really is ``Trade`` at summary -- title case, no
    trailing space; the detail workbook's ``'TRADE '`` trap is a different
    file.
    """
    supply = _load_usa_summary_sut('Supply_summary', year)  # type: ignore[arg-type]
    supply.index = supply.index.astype(str).str.strip()
    supply.columns = supply.columns.astype(str).str.strip()
    values = {}
    for group in GIVER_GROUPS:
        cell = float(pd.to_numeric(supply.loc[group, 'Trade'], errors='raise'))
        if cell >= 0:
            raise ValueError(
                f'published summary Trade cell for {group} in {year} is '
                f'{cell:,.0f} $M; a giver group must be negative. The workbook '
                f'layout or the group list has changed.'
            )
        values[group] = -cell * MILLION_CURRENCY_TO_CURRENCY
    return pd.Series(values, name='published_giveup')


def census_index_control_total(
    kind: str, year: int, allow_extrapolation: bool = False
) -> float:
    """The pre-#769 control: ``frozen 2017 coverage ratio x Census margin``.

    ❌ **No longer the build's control** -- kept because the diagnostic that
    retired it (:mod:`bedrock.analysis.nowcasting.trade_data.giveup_solvency`)
    scores it, and because it is the only construction available for a year the
    summary tables have not reached.  What retired it: the implied annual
    coverage ratio in BEA's own published tables runs 1.561 -> 1.435-1.489 for
    wholesale by 2021-2023 (a shrinking non-merchant universe the frozen ratio
    cannot see) and 0.966 for retail in 2023 (the AIES splice's rate step,
    which BEA did not take).  The overstatement it produced was the #769
    insolvency.
    """
    return trade_coverage_ratio(kind) * census_gross_margin(
        kind, year, allow_extrapolation
    )


def trade_control_total(
    kind: str, year: int, allow_extrapolation: bool = False
) -> float:
    """
    The margin *kind* gives up in *year*, USD - the level to allocate.

    ✅ **The published summary ``Trade`` cells, summed over the kind's giver
    groups** (#769).  ``T007`` and the give-up now come from the same published
    block, so the knife-edge difference that produced negative total supply --
    two independently-moved series 90.8-100% of each other by construction --
    is closed at group level by sourcing.  2017 is the same anchor as before,
    to the dollar; the Census series still moves the *within-group* split in
    :func:`giver_allocation` and the within-kind tax index.

    ``allow_extrapolation`` is accepted for signature stability; the published
    workbook currently reaches 2024, one year past the Census margin series.

    This is the **Supply ``TRADE``** control, net of the trade-level tax. For the
    Margins table's own Wholesale and Retail columns use
    :func:`gross_margin_control_total` instead.
    """
    del allow_extrapolation
    groups = [g for g, k in _GROUP_KIND.items() if k == _check_kind(kind)]
    return float(published_group_giveup(int(year))[groups].sum())


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


def _cap_to_output(
    split: pd.Series, output: 'pd.Series[float]', group: str, year: int
) -> pd.Series:
    """Water-fill *split* (positive give-ups) under each member's own output.

    Members whose proportional share exceeds their output are fixed AT their
    output and the remainder is re-shared over the others on the original
    shape, repeating until no one is over -- at most one member fixes per
    sweep, so it terminates.  Feasible whenever the group total is within the
    group's output, which the published summary ``T016 >= 0`` guarantees; if
    the data ever breaks that, this raises rather than looping.
    """
    total = float(split.sum())
    ceiling = output.clip(lower=0.0).astype(float)
    # Scale-free dust bound: the published tables round to whole millions, so a
    # group can overshoot its capacity by a few $M on a ~$400bn total. 1e-5 of
    # the total covers that and stays meaningful at any unit.
    dust = 1e-5 * abs(total)
    if total > float(ceiling.sum()) + dust:
        raise ValueError(
            f'group {group} {year} give-up of {total:,.0f} USD exceeds the '
            f"group's own output of {ceiling.sum():,.0f} - the published "
            f'summary T016 for this group should make that impossible, so '
            f'either the output vector or the group list is wrong.'
        )
    shape = split.astype(float)
    fixed: dict[str, float] = {}
    free = list(shape.index)
    remaining = total
    for _ in range(len(shape)):
        if not free:
            # every member is at its ceiling and only published-rounding dust
            # remains (the feasibility check above bounds it); park the dust on
            # the largest member rather than losing mass - the guard's own
            # tolerance is two orders looser than this.
            largest = str(ceiling.idxmax())
            fixed[largest] = fixed[largest] + remaining
            return pd.Series(fixed, dtype=float).reindex(shape.index).fillna(0.0)
        weights = shape[free] / float(shape[free].sum())
        trial = remaining * weights
        over = trial.index[trial > ceiling[free] + dust]
        if len(over) == 0:
            result = pd.Series(fixed, dtype=float).reindex(shape.index).fillna(0.0)
            result[free] = trial
            return result
        for member in over:
            fixed[member] = float(ceiling[member])
            remaining -= float(ceiling[member])
            free.remove(member)
    raise AssertionError(
        f'group {group} {year}: water-filling failed to settle in '
        f'{len(shape)} sweeps, which its one-member-per-sweep bound forbids.'
    )


def giver_allocation(
    kind: str,
    year: int,
    control_total: float | None = None,
    commodity_output: pd.Series | None = None,
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

    # ⚠️ Rescale each summary giver GROUP to its own published Trade cell
    # (#769), not just the kind to its total. Retail is four published groups
    # and the census within-kind shape misallocated across them - 441 motor
    # vehicles ran +29% over BEA's own cell by 2023. Wholesale is one group, so
    # for it this is the kind rescale it always was. The census shape still
    # decides the split *inside* 4A0 and 42, which is all it is evidence for.
    #
    # ⚠️ And inside a group the split is CAPPED at each member's own output
    # (water-filling): the published summary T016 is non-negative, so the
    # group's output always covers its give-up, but the census shape can put
    # more on one member than that member's T007 - 454000 nonstore most of all,
    # which is the #724 e-commerce classification question. Excess redistributes
    # to the group's solvent members on the same shape; when the EC-2022 work
    # above this in the stack raises 454000's output, the cap relaxes by
    # itself. Without `commodity_output` (legacy callers, unit tests) the cap
    # is skipped and check_giveup_solvency still stands guard downstream.
    group_levels = published_group_giveup(int(year))
    allocation = shape.astype(float).copy()
    for group, members in GIVER_GROUPS.items():
        if _GROUP_KIND[group] != kind:
            continue
        inside = [m for m in members if m in allocation.index]
        mass = float(shape[inside].sum())
        if mass <= 0:
            raise ValueError(
                f'the {kind} {year} census shape carries no mass on group '
                f'{group} ({inside}); its published give-up of '
                f'{group_levels[group]:,.0f} USD has nothing to land on.'
            )
        split = group_levels[group] * shape[inside] / mass
        if commodity_output is not None:
            split = _cap_to_output(
                split, commodity_output.reindex(inside).fillna(0.0), group, year
            )
        allocation[inside] = -split

    expected_total = -control_total
    if abs(float(allocation.sum()) - expected_total) > _TILT_ROUNDING_TOLERANCE:
        raise ValueError(
            f'{kind} {year} group give-ups sum to {allocation.sum():,.0f} USD '
            f'against a kind control of {expected_total:,.0f}. Both come from '
            f'the same published cells, so a gap means the group list and the '
            f'kind partition disagree.'
        )

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

    Pass *kind* for one kind's receiving side, or leave it off for both together.
    Both reproduce 2017 exactly, because
    :func:`published_trade_received_by_kind` nets the trade-level tax out per
    kind rather than pro rata.

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


def trade_margin_components(
    year: int = ANCHOR_YEAR,
    allow_extrapolation: bool = False,
    commodity_output: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Trade margin for *year* decomposed into wholesale, retail and tax. USD.

    One row per BEA 2017 commodity, three columns::

        wholesale   net of the tax collected on it; sums to zero over commodities
        retail      likewise
        trade_tax   sales tax collected on the trade transaction; positive only

    and the identity that ties them to the published tables::

        TRADE                  = wholesale + retail
        Margins table Wholesale = wholesale + trade_tax[wholesale share]
        TOP                     = trade_tax + producer-level tax

    **This is the separation the Supply column alone cannot give you.** ``TRADE``
    nets wholesale against retail and excludes the tax entirely, so a consumer
    that needs the Margins table's own columns, or needs margins in basic prices,
    has to come here rather than to :func:`trade_margin_column`.

    Each kind's give-up side is negative on its own trade commodities and its
    receiving side positive on the 255 receivers, so **each kind's column sums to
    zero on its own** - a stronger statement than the combined column summing to
    zero, and one that only holds because the tax is carried separately.

    ⚠️ **The tax is not sourced annually.** Its 2017 level per commodity is
    observed, and it moves with its own kind's Census index - so the tax *rate*
    on a commodity's margin is frozen at 2017 even though the level moves. A
    sales-tax rate change is exactly what this will miss. ``TOP`` from 4d
    (`#580 <https://github.com/cornerstone-data/bedrock/issues/580>`_) is the
    source that would fix it, at the cost of a dependency the column does not
    otherwise have.
    """
    frames: dict[str, pd.Series] = {}
    tax_2017 = trade_level_tax_by_kind_2017()
    taxes: list[pd.Series] = []

    for kind in TRADE_KINDS:
        control = trade_control_total(kind, year, allow_extrapolation)
        received = receiving_allocation(year, control, kind=kind)
        given_up = giver_allocation(
            kind, year, control, commodity_output=commodity_output
        )
        frames[kind] = pd.concat([received, given_up]).groupby(level=0).sum()

        # the tax moves with the margin it is levied on, which is its own kind
        index = control / _kind_give_up_2017(kind)
        taxes.append(tax_2017[kind] * index)

    components = pd.DataFrame(frames)
    components['trade_tax'] = (
        pd.concat(taxes, axis=1).sum(axis=1).reindex(components.index).fillna(0.0)
    )

    for kind in TRADE_KINDS:
        residual = float(components[kind].sum())
        scale = float(components[kind].abs().sum())
        if scale and abs(residual) / scale > 1e-9:
            raise ValueError(
                f'The {kind} margin for {year} sums to {residual:,.2f} rather '
                f'than zero. Each kind is a redistribution in its own right - its '
                f'givers and its receivers are the same dollars - so a non-zero '
                f'total means the receiving side and the give-up side were '
                f'controlled differently.'
            )
    return components


#: A giver counts as insolvent when its total supply is below minus this share
#: of its own output.  The published tables round to whole millions and several
#: givers legitimately give up exactly 100.0%, so a strict zero flags dust.
#: With the water-filling cap in :func:`giver_allocation` this should never
#: fire at all -- the guard exists for the paths that skip the cap (a caller
#: that passes no ``commodity_output``) and for regressions.
SOLVENCY_TOLERANCE = 0.005


def check_giveup_solvency(
    commodity_output: pd.Series, trade: pd.Series, year: int
) -> pd.DataFrame:
    """Raise if any giver outside the known set has negative total supply.

    ``commodity_output`` is ``T007`` and ``trade`` the ``TRADE`` column, both
    in USD on the commodity axis.  Vectors are taken rather than fetched so
    this can sit here, upstream of the bridge, without importing it.

    Returns the giver table (output, give-up, partial ``T016``) so the caller
    can log it; raises ``ValueError`` on a **new** insolvency, because a
    negative supply row is infeasible for the balance -- the sign locks refuse
    the negative Use row ``T11`` would demand -- and silently emitting one was
    the whole #769 defect.
    """
    givers = sorted({c for kind in GIVER_COMMODITIES for c in GIVER_COMMODITIES[kind]})
    table = pd.DataFrame(
        {
            'T007': commodity_output.reindex(givers).fillna(0.0),
            'TRADE': trade.reindex(givers).fillna(0.0),
        }
    )
    table['T016_partial'] = table['T007'] + table['TRADE']
    floor = -SOLVENCY_TOLERANCE * table['T007'].abs()
    insolvent = list(table.index[table['T016_partial'] < floor])
    if insolvent:
        detail = ', '.join(
            f'{g}: {table.loc[g, "T016_partial"]:,.0f} USD' for g in insolvent
        )
        raise ValueError(
            f"trade give-up exceeds the giver's own output in {year} for "
            f'{detail} - negative total supply is infeasible for the balance, '
            f'and the water-filling cap should have made it impossible. Either '
            f'the caller built TRADE without commodity_output, or the group '
            f'give-up itself exceeds the group output, which the published '
            f'summary T016 forbids.'
        )
    return table


def trade_margin_column(
    year: int = ANCHOR_YEAR,
    allow_extrapolation: bool = False,
    commodity_output: pd.Series | None = None,
) -> pd.Series:
    """
    The Supply table's ``TRADE`` column for *year*. USD, by BEA 2017 commodity.

    Positive on the 255 commodities that receive trade margin, negative on the 19
    trade commodities that give it up, and **summing to zero** - margin is a
    redistribution, not value created, which is target T16's identity and the
    only constraint the balance places on step 4c's output.

    Both sides are built **per kind** and added - wholesale and retail move on
    their own Census index, land on their own ten and nine commodities, and
    receive on their own share of the netted column. Use
    :func:`trade_margin_components` to get them separately, together with the
    trade-level tax.
    """
    components = trade_margin_components(
        year, allow_extrapolation, commodity_output=commodity_output
    )
    column = components[list(TRADE_KINDS)].sum(axis=1).rename('TRADE')

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
