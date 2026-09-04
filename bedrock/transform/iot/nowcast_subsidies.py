"""Subsidies by commodity - the Supply table's ``SUB`` column.

The other half of Step 4d
(`#580 <https://github.com/cornerstone-data/bedrock/issues/580>`_), completing
the Supply bridge's ``T015`` block::

    T015 = MDTY + TOP + SUB          taxes less subsidies      695,565  (2017)

``TOP`` is :mod:`bedrock.transform.iot.nowcast_product_taxes`, and this module is
its sibling rather than its mirror image - the construction is genuinely
different, for reasons the first section gives.

⚠️ **Sign convention: ``SUB`` is stored NEGATIVE**, as BEA publishes it in the
Supply table, while NIPA publishes the same money positive and the Use table's
``T00SUB`` row positive as well. Every function here that returns a NIPA reading
returns it **positive**; :func:`sub_column` is the one that flips it. Getting
this wrong produces a plausible-looking table that fails ``T015`` by twice the
subsidy total, and comparing the two sides without it yields a tidy
"200.0017% error" that is nothing but the sign.

**The total is observed, and it is the published summary Supply column's**
(#784). This column holds subsidies on *products* — money that reduces a
commodity's purchaser price. NIPA T31300 is *total* subsidies, products plus
subsidies on *production* (payments to industries: PPP, Employee Retention
Credit, Provider Relief, ...), which belong on the industry side of the
accounts and on no commodity row at all. The two totals are identical
2017-2019 to the dollar (59,875 / 63,320 / 72,956 $M), and then diverge
hard: **5.9x in 2020, 6.0x in 2021** (698,507 against a published 118,366),
still +25% in 2022 and +5.5% in 2024. Leveling this column to T31300 —
the construction before #784 — booked every pandemic production subsidy
onto commodities, understating total supply by up to 580bn $M in exactly
the years the supply-equals-use gap spikes. The annual control is therefore
the published summary Supply ``SUB`` total, and the commodity allocation is
conditioned on that column's ~14 summary groups (annual, observed); NIPA
T31300 remains a diagnostic, and the gap between the two readings *is* the
production-subsidy mass (:func:`control_total_table` shows both).

Why this is not ``TOP`` with the sign flipped
----------------------------------------------

⚠️ **The 2017 share vector is safe for ``TOP`` and dangerous for ``SUB``.**
``TOP``'s composition moves slowly. ``SUB``'s does not: even products-only,
the published total is 2.0x its 2017 level by 2020 and the group mix moves
with programmes (transit ``485`` appears from nothing to 21% of the 2022
column). Holding the 2017 commodity shares constant misallocates whatever a
programme year adds, because the 2017 column is 66.2% housing (``531HST``
alone is 59.8%) and new programmes have no 2017 counterpart to attach to.

Anchor and move, per NIPA type
-------------------------------

Unlike ``TOP``, NIPA's own type lines **partition** the subsidy total rather than
covering a third of it, so every dollar is typed every year:

=================  ============  =========================================
NIPA T31300 line   series        BEA 2017 commodities
=================  ============  =========================================
agricultural       ``L31204``    ``1111B0`` ``111900``
housing            ``L31205``    ``531HST`` ``531HSO`` ``531ORE``
maritime           ``L31206``    ``483000``
air carriers       ``L31207``    ``481000``
other              ``L31208``    the remaining eight anchored commodities
state and local    ``B114RC``    folded into *other* - 0.9% of 2017
=================  ============  =========================================

Each commodity is **anchored** on its published 2017 value and **moved** by its
own type's annual NIPA growth - the same construction the two margin columns use,
and for the same reason: 2017 is then an identity, and a nowcast year moves with
the source rather than with a frozen aggregate. That already carries most of the
pandemic signal, because the type lines are where it lives: air carriers go 238
to 19,966 in 2020 (**84x**, the payroll support programme) and agricultural 11,532
to 46,457 (**4.0x**), while housing moves only 1.23x.

⚠️ **The type assignment is ours, not BEA's**, and the two readings do not agree
to the dollar - NIPA housing is 35,771 in 2017 where the three housing
commodities carry 39,636. Anchoring on the published column rather than on NIPA's
type totals is the deliberate choice: in the one year BEA publishes a commodity
split, BEA's split wins over our reading of which type a commodity belongs to.

The conditioning, and the one injected commodity
------------------------------------------------

The anchored-and-moved vector is a *shape*, and the published summary Supply
``SUB`` column is the annual answer for where product subsidies actually sit,
at summary-group resolution. So the column is finished the same way as the
final-demand block (#786): each summary group scaled to its published value,
the within-group split kept from the anchor. The fifteen anchored commodities
cover thirteen of the fourteen groups the published column ever touches — of
course they do, they were read off the published 2017 column — with one
exception:

✅ **Transit.** ``485`` carries no product subsidy in 2017 and −21,948 $M in
2022 (federal transit operating support, 21% of that year's column), so there
is nothing to scale. Its summary group holds exactly one detail commodity, so
the published group value lands on ``485000`` directly. Any *other* published
group the anchored shape cannot reach raises rather than being dropped — a new
programme year deserves a decision, not a silent hole.

⚠️ **What happened to the PPP machinery.** Before #784 this module carried
BEA's PPP-by-industry allocation to spread the 2020-21 NIPA *other* line
across ~387 commodities. That line is production subsidies; production
subsidies are no longer in this column, so the machinery is gone (git history
has it, and ``BEA_PPP_Subsidies`` remains cached on GCS should the industry
side of the accounts ever want it). #689's USAspending decomposition is
likewise an industry-side question now, not a ``SUB`` one.
"""

from __future__ import annotations

import functools
from collections.abc import Iterable, Mapping

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

#: The one year with a published detail Supply table.
ANCHOR_YEAR = 2017

#: Years ``SUB`` is built for - the years the summary Supply workbook covers.
SUB_YEARS = range(2017, 2025)

_NIPA_SOURCE = 'BEA_NIPA'
SUBSIDY_TABLE = 'T31300'

#: Total government subsidies (line 1). The column control.
SUBSIDY_TOTAL_SERIES = 'A107RC'

#: NIPA type lines and the BEA 2017 commodities each is anchored on. Together
#: with :data:`STATE_LOCAL_SERIES` these **partition** the subsidy total, which
#: is what makes this a per-type move rather than ``TOP``'s named-plus-residual.
SUBSIDY_TYPES: Mapping[str, tuple[str, tuple[str, ...]]] = {
    'agricultural': ('L31204', ('1111B0', '111900')),
    'housing': ('L31205', ('531HST', '531HSO', '531ORE')),
    'maritime': ('L31206', ('483000',)),
    'air carriers': ('L31207', ('481000',)),
    'other': (
        'L31208',
        (
            '5241XX',
            '482000',
            '52A000',
            '622000',
            '335912',
            '314900',
            '325414',
            '515200',
        ),
    ),
}

#: State and local subsidies (line 8), 555 in 2017 against a 59,875 total. NIPA
#: gives no type for them and the published column no separate home, so they ride
#: the *other* line's commodities via the conditioning.
STATE_LOCAL_SERIES = 'B114RC'

#: The one summary group the published column reaches that the 2017 anchor
#: cannot: federal transit operating support appears from 2020 (−15,617 $M)
#: and peaks at −21,948 in 2022, 21% of that year's column. The group holds
#: exactly this one detail commodity, so the published group value lands on it
#: directly.
INJECTED_COMMODITIES: Mapping[str, str] = {'485': '485000'}

#: The type label the injected commodities carry in :func:`sub_decomposition` —
#: they belong to no NIPA type line, and the industry-side conversion routes
#: this type to the state and local transit enterprise (the published summary
#: Use ``T00SUB`` row books the 2022 transit subsidy on ``GSLE``, none on
#: private ``485``).
INJECTED_TYPE = 'transit'

#: Slack on the identity checks, in USD - the published tables are in whole
#: millions, so an exact comparison trips on BEA's own rounding. The summary
#: total stacks ~14 rounded group cells.
_ROUNDING_TOLERANCE = 20.0 * MILLION_CURRENCY_TO_CURRENCY


# --- NIPA, the annual level ------------------------------------------------


@functools.cache
def nipa_subsidy_lines(year: int) -> pd.Series:
    """NIPA table 3.13 by series code for *year*, USD, **positive** as published."""
    fba = getFlowByActivity(_NIPA_SOURCE, int(year))
    description = fba['Description'].astype(str)
    rows = fba[description.str.startswith(f'{SUBSIDY_TABLE}:')].copy()
    if rows.empty:
        raise ValueError(
            f'{_NIPA_SOURCE} {year} carries no {SUBSIDY_TABLE} rows. That table is '
            f'listed in BEA_NIPA.yaml, so an empty result means the extract '
            f'changed rather than that subsidies stopped being paid.'
        )
    series = (
        rows['Description']
        .astype(str)
        .str.split(':')
        .str[1]
        .str.split(' - ')
        .str[0]
        .str.strip()
    )
    return (
        pd.to_numeric(rows['FlowAmount'], errors='coerce')
        .groupby(series)
        .sum()
        .rename_axis('nipa_series')
        .rename(f'{SUBSIDY_TABLE}_{year}')
    )


def nipa_line(code: str, year: int) -> float:
    """One NIPA T31300 series in *year*, USD positive, raising on a missing code."""
    lines = nipa_subsidy_lines(year)
    if code not in lines.index:
        raise ValueError(
            f'NIPA {SUBSIDY_TABLE} {year} has no series {code!r}. It was published '
            f'in {ANCHOR_YEAR}; if BEA has retired or renamed the line, '
            f'SUBSIDY_TYPES needs updating rather than defaulting to zero.'
        )
    return float(lines[code])


def sub_control_total(year: int) -> float:
    """NIPA **total** subsidies for *year*, USD, **positive** as NIPA publishes it.

    ⚠️ **A diagnostic, not the column control** (#784): this is products plus
    production subsidies, identical to the published products-only column
    2017-2019 and up to 6x it in 2020-21. The column control is
    :func:`published_sub_total`; the gap between the two readings is the
    production-subsidy mass that belongs on industries, not commodities.
    """
    total = nipa_line(SUBSIDY_TOTAL_SERIES, year)
    if total <= 0:
        raise ValueError(
            f'NIPA total subsidies is {total:,.0f} USD in {year}. This module reads '
            f'NIPA positive and flips the sign once, in sub_column; a non-positive '
            f'reading here means the source is already signed and would be flipped '
            f'twice.'
        )
    return total


@functools.cache
def published_sub_by_group(year: int) -> pd.Series:
    """The published summary Supply ``SUB`` column for *year*, USD, **negative**.

    Summary commodity groups only — the ``T017`` total row is dropped (it is
    checked against the group sum instead). Nonzero on ~14 groups.
    """
    supply = _load_usa_summary_sut('Supply_summary', year)  # type: ignore[arg-type]
    column = pd.to_numeric(supply['SUB'], errors='coerce').fillna(0.0)
    total = float(column.get('T017', 0.0)) * MILLION_CURRENCY_TO_CURRENCY
    groups = (
        column.drop(index=['T017'], errors='ignore').astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    if (groups > 0).any():
        raise ValueError(
            f'The {year} summary Supply SUB column is positive on '
            f'{sorted(groups.index[groups > 0])}. BEA stores this column negative; '
            f'a positive cell means the workbook was read on the Use table\'s sign '
            f'convention.'
        )
    if abs(float(groups.sum()) - total) > _ROUNDING_TOLERANCE:
        raise ValueError(
            f'The {year} summary Supply SUB groups sum to {groups.sum():,.0f} USD '
            f'against the workbook\'s own T017 of {total:,.0f}. A group row is '
            f'being dropped or double-read.'
        )
    return groups.rename('SUB')


def published_sub_total(year: int) -> float:
    """The ``SUB`` column control for *year*: the published summary total, USD,
    **positive** (the magnitude; :func:`sub_column` stores the column negative).
    """
    return float(-published_sub_by_group(year).sum())


def subsidy_type_totals(year: int) -> pd.Series:
    """Each NIPA type line for *year*, USD positive, in :data:`SUBSIDY_TYPES` order."""
    return pd.Series(
        {
            subsidy_type: nipa_line(series, year)
            for subsidy_type, (series, _) in SUBSIDY_TYPES.items()
        },
        dtype=float,
    ).rename_axis('subsidy_type')


# --- the 2017 anchor -------------------------------------------------------


@functools.cache
def published_sub_by_commodity() -> pd.Series:
    """The published 2017 Supply ``SUB`` column, USD, **negative**.

    Non-zero on 15 commodities of 402, and 66.2% of it is housing.
    """
    supply = _load_2017_detail_supply_use_usa('Supply_detail').rename(
        columns=lambda column: column.strip()
    )
    commodities = [code for code in USA_2017_COMMODITY_CODES if code in supply.index]
    sub = supply.loc[commodities, 'SUB'].astype(float) * MILLION_CURRENCY_TO_CURRENCY
    column = (
        sub.reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
        .rename_axis('commodity')
        .rename('SUB')
    )
    if (column > 0).any():
        raise ValueError(
            f'The published 2017 Supply SUB column is positive on '
            f'{sorted(column.index[column > 0])}. BEA stores this column negative; '
            f'a positive cell means the workbook was read on the Use table\'s sign '
            f'convention, which doubles the subsidy wedge in T015.'
        )
    return column


@functools.cache
def anchor_shares() -> pd.DataFrame:
    """Commodity x type share of each type's 2017 subsidy. Columns sum to 1.

    ⚠️ Every commodity carrying 2017 ``SUB`` must be typed, or its subsidy is
    dropped from every later year while the column total still ties to NIPA.
    """
    published = published_sub_by_commodity().abs()
    typed = [c for _, commodities in SUBSIDY_TYPES.values() for c in commodities]

    duplicated = sorted({c for c in typed if typed.count(c) > 1})
    if duplicated:
        raise ValueError(
            f'SUBSIDY_TYPES assigns {duplicated} to more than one type, so their '
            f'subsidy would be counted once per type.'
        )
    untyped = sorted(set(published.index[published > 0]) - set(typed))
    if untyped:
        raise ValueError(
            f'{untyped} carry {ANCHOR_YEAR} SUB but belong to no type in '
            f'SUBSIDY_TYPES, so they would be dropped from every year after the '
            f'anchor while the column total still tied to NIPA.'
        )

    shares = pd.DataFrame(
        0.0,
        index=published.index,
        columns=pd.Index(list(SUBSIDY_TYPES), name='subsidy_type'),
    )
    for subsidy_type, (_, commodities) in SUBSIDY_TYPES.items():
        selection = list(commodities)
        type_total = float(published[selection].sum())
        if type_total <= 0:
            raise ValueError(
                f'SUBSIDY_TYPES type {subsidy_type!r} maps to commodities carrying '
                f'no {ANCHOR_YEAR} subsidy, so its line has no basis to split on.'
            )
        shares.loc[selection, subsidy_type] = published[selection] / type_total
    return shares


def type_growth(year: int) -> pd.Series:
    """Each type's NIPA level in *year* over its 2017 level. 1.0 across the board in 2017.

    The pandemic signal is mostly here: air carriers 84x in 2020, agricultural
    4.0x, housing only 1.23x.
    """
    current = subsidy_type_totals(year)
    anchor = subsidy_type_totals(ANCHOR_YEAR)
    if (anchor <= 0).any():
        raise ValueError(
            f'NIPA type lines {sorted(anchor.index[anchor <= 0])} are zero in '
            f'{ANCHOR_YEAR}, so they have no anchor to move from.'
        )
    return (current / anchor).rename(f'growth_{year}')


# --- the column ------------------------------------------------------------


def anchored_shape(year: int) -> pd.Series:
    """The pre-conditioning shape: each type's 2017 mass moved by its NIPA line.

    USD **positive**. Unscaled — the conditioning in :func:`sub_decomposition`
    sets every group's level, so only the within-group proportions of this
    vector survive into the column.
    """
    published = published_sub_by_commodity().abs()
    shares = anchor_shares()
    growth = type_growth(year)
    anchored = pd.Series(
        {
            subsidy_type: float(published[list(commodities)].sum())
            * float(growth[subsidy_type])
            for subsidy_type, (_, commodities) in SUBSIDY_TYPES.items()
        }
    )
    return shares.mul(anchored, axis='columns').sum(axis='columns')


@functools.cache
def _commodity_to_summary() -> pd.Series:
    mapping = load_bea_v2017_commodity_to_bea_v2017_summary()
    return pd.Series({code: parents[0] for code, parents in mapping.items()})


def sub_decomposition(year: int) -> pd.DataFrame:
    """``SUB`` for *year* by type, USD **negative**, plus the ``SUB`` row margin.

    One column per :data:`SUBSIDY_TYPES` type plus :data:`INJECTED_TYPE`. The
    construction: each summary group of the anchored shape is scaled to the
    published summary Supply ``SUB`` value for that group and year; each
    :data:`INJECTED_COMMODITIES` group's published value lands on its single
    commodity directly. A published group the shape cannot reach any other
    way raises — a new programme year deserves a decision, not a silent hole.

    The typing survives the conditioning untouched because every commodity
    belongs to exactly one type; the type columns are what
    :func:`~bedrock.transform.iot.nowcast_va_taxes.t00sub_row` keys its
    government-enterprise routings on.
    """
    if int(year) not in SUB_YEARS:
        raise ValueError(
            f'SUB is built for {SUB_YEARS.start}-{SUB_YEARS.stop - 1}; {year} is '
            f'outside the years the summary Supply workbook covers.'
        )
    shape = anchored_shape(int(year))
    if int(year) == ANCHOR_YEAR:
        # At the anchor the published detail column IS the answer - conditioning
        # it onto whole-million summary group cells would only add rounding
        # dust, and downstream (t00sub_row) reproduces the published row to the
        # dollar on this exactness.
        conditioned = published_sub_by_commodity().abs()
    else:
        groups = _commodity_to_summary().reindex(shape.index)
        ours = shape.groupby(groups).sum()
        pub = published_sub_by_group(int(year)).abs()

        conditioned = pd.Series(0.0, index=shape.index)
        for group, target in pub[pub > 0].items():
            if group in INJECTED_COMMODITIES:
                conditioned.loc[INJECTED_COMMODITIES[str(group)]] = float(target)
                continue
            base = float(ours.get(group, 0.0))
            if base <= 0:
                raise ValueError(
                    f'The {year} published summary SUB column carries '
                    f'{target:,.0f} USD on group {group!r}, but the anchored '
                    f'shape reaches no commodity there and INJECTED_COMMODITIES '
                    f'names none. A new subsidy programme needs a home, not a '
                    f'silent drop.'
                )
            members = groups.index[groups == group]
            conditioned.loc[members] = shape[members] * (float(target) / base)

    type_of = pd.Series(
        {
            commodity: subsidy_type
            for subsidy_type, (_, commodities) in SUBSIDY_TYPES.items()
            for commodity in commodities
        }
    )
    for injected in INJECTED_COMMODITIES.values():
        type_of[injected] = INJECTED_TYPE

    out = pd.DataFrame(
        0.0,
        index=shape.index,
        columns=pd.Index([*SUBSIDY_TYPES, INJECTED_TYPE], name='subsidy_type'),
    )
    carrying = conditioned[conditioned != 0.0]
    for commodity, amount in carrying.items():
        out.loc[str(commodity), str(type_of[str(commodity)])] = -float(amount)
    out['SUB'] = out.sum(axis='columns')
    out.index.name = 'commodity'
    return out


def sub_column(year: int = ANCHOR_YEAR) -> pd.Series:
    """The Supply table's ``SUB`` column for *year*. USD, **negative**.

    Sums to −:func:`published_sub_total` exactly — the products-only concept
    (#784), not NIPA's combined total.

    ⚠️ **Negative is the Supply table's convention, not a bug and not ours.** The
    Use table's ``T00SUB`` row carries the same money positive. ``T015`` *adds*
    this column rather than subtracting it.
    """
    column = sub_decomposition(year)['SUB'].rename('SUB')

    control = -published_sub_total(year)
    if abs(float(column.sum()) - control) > _ROUNDING_TOLERANCE:
        raise ValueError(
            f'SUB {year} sums to {column.sum():,.0f} USD against the published '
            f'summary control of {control:,.0f}. Every group is scaled to its '
            f'published value by construction, so a gap means a group was '
            f'dropped or scaled twice.'
        )
    if (column > 0).any():
        raise ValueError(
            f'SUB {year} is positive on {sorted(column.index[column > 0])}. This '
            f'column is stored negative; a positive cell is a sign flip, and it '
            f'fails T015 by twice the amount rather than by the amount.'
        )
    return column


def control_total_table(years: Iterable[int] | None = None) -> pd.DataFrame:
    """The two annual totals and NIPA's type lines, $M positive, for diagnostics.

    ``SUB`` is the products-only column control (published summary);
    ``nipa_total`` is NIPA T31300; ``production_wedge`` is their difference —
    the production-subsidy mass that belongs on industries, 0 through 2019
    and ~580,000 $M at the 2020 peak.
    """
    years = list(SUB_YEARS if years is None else years)
    rows = {}
    for year in years:
        totals = subsidy_type_totals(year)
        nipa_total = sub_control_total(year)
        control = published_sub_total(year)
        rows[year] = {
            **totals.to_dict(),
            'state and local': nipa_line(STATE_LOCAL_SERIES, year),
            'SUB': control,
            'nipa_total': nipa_total,
            'production_wedge': nipa_total - control,
        }
    return (pd.DataFrame(rows).T / MILLION_CURRENCY_TO_CURRENCY).rename_axis('year')
