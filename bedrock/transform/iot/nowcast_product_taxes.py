"""Taxes on products by commodity - the Supply table's ``TOP`` column.

Step 4d of the nowcast build
(`#580 <https://github.com/cornerstone-data/bedrock/issues/580>`_), the tax half
of the Supply bridge's ``T015`` block::

    T015 = MDTY + TOP + SUB          taxes less subsidies      695,565  (2017)

``MDTY`` is :mod:`bedrock.transform.trade.duties`; ``SUB`` is not built yet.

**The total is not estimated at all.** Both columns this module family covers are
published annually in NIPA, independent of the Supply table::

    TOP = T30500 taxes on products - customs and other import duties

⚠️ **The residual is no longer frozen.** 70% of the column is general sales tax
with no named NIPA line. It sat on frozen 2017 shares until ``TRADE`` and
``TRANS`` were sourced (#611); it now moves on ``T013 + T014``, a
purchaser-price base by commodity - see :func:`residual_share_for_year`. 2024
holds 2023's shares, because the margin columns stop there.
        = (LA000236 + LA000238) - B235RC

2017 gives 716,925 against the published Supply column's 716,926 - a $1M
agreement on a $717bn column, which is BEA's own publication rounding. So the
annual level is **observed, every year, with no modelling content whatsoever**.

⚠️ **Customs duties have to come out.** NIPA's taxes on products is 755,438 in
2017 and the Supply ``TOP`` column is 716,926; the 38,513 difference is the
customs line (``B235RC``) exactly. Duties attach to *imports*, so they leave the
Supply table through ``MDTY`` instead, and reach ``TOP`` twice if not netted.

What this module actually estimates is therefore only the **commodity split**.

Two blocks, and the split between them is the whole method
-----------------------------------------------------------

===============================  ==========  =======  ===================================
block                            2017 $M     share    how it moves
===============================  ==========  =======  ===================================
named NIPA product lines            213,347   29.8%   **each line's own annual NIPA value**
residual: sales tax and
unnamed excise                      503,579   70.2%   frozen 2017 shares, NIPA-controlled
===============================  ==========  =======  ===================================

✅ **Block 1 beats the default outright, and the default is badly wrong there.**
NIPA publishes gasoline, diesel, alcohol, tobacco, air transport, pharmaceutical,
health insurance, insurance receipts, public utility and severance taxes as their
own lines, every year, and each names a commodity. Holding 2017 shares constant
instead - the issue's default proposal - grows every one of them at the *column's*
rate. It is not a small difference:

=========================  =========  =========  ==========  ==========
line                       2017       later      NIPA        default
=========================  =========  =========  ==========  ==========
federal tobacco excise        13,302   4,427 (2024)  **0.33x**     1.42x
state and local tobacco       19,150  14,968 (2024)  **0.78x**     1.42x
air transport                 18,337   6,063 (2020)  **0.33x**     1.06x
severance                     10,062  30,107 (2022)  **2.99x**     1.36x
=========================  =========  =========  ==========  ==========

Tobacco alone is **26,210 $M** of difference in 2024 - the default has tobacco
tax rising 42% over a period in which NIPA measures it falling 40%. Across the
whole column the two constructions differ by 3.7% in 2020 and 5.8% in 2024, and
the largest single cell either way is insurance in 2020, where the ACA fee puts
17,602 $M on ``5241XX`` that the default cannot see at all.

⚠️ **Block 2 is the default proposal, and it stays.** General sales tax is 56.5%
of ``TOP`` and is levied on the *purchaser* price - basic value plus trade and
transport margins plus other product taxes - so allocating it needs a base that
does not exist annually until Step 5 has run. Two movers were built and measured,
and **both were rejected**; see :func:`residual_share`. The frozen share vector
is defensible here for the reason the plan already records: the sales-tax share
of ``TOP`` moves only between 51.7% and 55.4% across 2017-2024, so the object
being frozen is stable even though the column around it grows 42%.

The named lines, and why each commodity set
--------------------------------------------

:data:`NAMED_TAX_LINES` maps ten NIPA line groups onto BEA 2017 commodities. Each
group's annual amount is spread across its commodity set in proportion to the
set's **2017 published** ``TOP``, so the assignment is anchored on the one year
BEA publishes and only the level moves.

⚠️ **Every group is checked against its set's 2017 ceiling** and the residual is
checked to be non-negative per commodity - a named line larger than the tax its
commodities actually carry would push the residual negative and silently invent
sales tax elsewhere. The measured ratios run 0.00 to 0.90 of the set's ``TOP``,
so every group fits with room to spare (:func:`named_line_feasibility`).

⚠️ **The commodity set is the tax's *product*, not the industry that remits it.**
Motor fuel tax goes to ``324110`` petroleum refineries because the taxed product
is refined fuel, even though it is collected at the pump by a retailer; that is
how the published table carries it, and it is why the trade-level share below is
98.8% rather than zero.

Producer-level and trade-level - the split Step 4c needs
---------------------------------------------------------

The plan requires ``TOP`` to carry its **producer-level vs trade-level split**,
not just the total: Step 4c's application phase rebuilds the margin base as
``T013 + MDTY + SUB + producer-level TOP``, and adding all of ``TOP`` there
double-counts the trade-level share already inside the margin rates - by 19% on
petroleum refineries and 36% on tobacco.

The split is observed for 2017 by
:func:`~bedrock.transform.iot.nowcast_trade_margins.trade_level_tax_2017`, which
gets it from an identity with nothing modelled in it::

    trade_level_tax[c] = Wholesale[c] + Retail[c] - TRADE[c]        391,162 $M

:func:`trade_level_share` freezes that share per commodity and
:func:`top_by_level` applies it. ⚠️ **The share is frozen per commodity, not per
tax line**, so a commodity whose named lines move differently from its residual
keeps its 2017 mix. Tobacco is the case where that shows: federal tobacco excise
is producer-level (13,302 in 2017, and ``312200``'s producer-level tax is 13,302
to the dollar) and state tobacco tax is trade-level, so as the federal line falls
to 4,427 by 2024 the true producer share falls with it. Carrying the split per
line instead would move about 9,000 $M on a 1,016,000 $M column - 0.9% - and
would need a per-group level judgement plus clipping wherever a group exceeds its
set's capacity at that level, which alcohol already does in 2017 by 771. Not
worth it while the consumer of the split is a *base* for a rate.
"""

from __future__ import annotations

import functools
from collections.abc import Iterable, Mapping

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.nowcast_trade_margins import trade_level_tax_2017
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

#: The one year with a published detail Supply table, and so the only year the
#: commodity split can be anchored on.
ANCHOR_YEAR = 2017

#: Years ``TOP`` is sourced for. Every input is NIPA, which publishes the whole
#: window, so unlike the margin columns this one has no source-driven end date.
TOP_YEARS = range(2017, 2025)

#: The FBA the NIPA tables are read from, and the table used.
_NIPA_SOURCE = 'BEA_NIPA'
PRODUCT_TAX_TABLE = 'T30500'

#: Taxes on products, federal (line 3) and state and local (line 19). Their sum
#: is NIPA's taxes-on-products total; the customs line below comes out of it.
PRODUCT_TAX_SUBTOTALS = ('LA000236', 'LA000238')

#: Customs and other import duties (line 15). Netted out because duties reach the
#: Supply table through ``MDTY``, not ``TOP``.
CUSTOMS_DUTIES_SERIES = 'B235RC'

#: NIPA product-tax lines that name a commodity, as
#: ``group -> (NIPA series codes, BEA 2017 commodities)``.
#:
#: The series are read at their published line values every year; the commodity
#: set fixes *where* the line lands and the 2017 ``TOP`` column fixes how it
#: splits within the set. Groups are disjoint on the series side by construction
#: - a series counted twice would double the tax - and may overlap on the
#: commodity side, which ``5241XX`` does: it takes both the ACA health insurance
#: fee and part of the state insurance receipts tax.
NAMED_TAX_LINES: Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    # Federal gasoline + federal diesel + state and local gasoline. All of it
    # lands on the refined product; it is collected downstream, which is why
    # 324110's tax is 99.8% trade-level.
    'motor fuel': (('B2000C', 'B2003C', 'LA000241'), ('324110',)),
    'alcohol': (('B2001C', 'LA000242'), ('312120', '312130', '312140')),
    'tobacco': (('B2002C', 'LA000243'), ('312200',)),
    'air transport': (('B2004C',), ('481000',)),
    # The ACA health insurance provider fee, which was legislated on and off:
    # 397 in 2017, 14,707 in 2018, 439 in 2019, 15,960 in 2020, then repealed
    # back to a few hundred. A real 15bn swing on one commodity, and the default
    # proposal cannot see it at all.
    'health insurance': (('Y700RC',), ('5241XX',)),
    # Zero in every published year, and kept because a line that returns to
    # non-zero should land somewhere rather than fall into the residual.
    'medical devices': (('Y701RC',), ('339112', '339113', '339114', '339115')),
    'pharmaceutical': (('Y702RC',), ('325412',)),
    'insurance receipts': (('LA000244',), ('5241XX', '524113')),
    # State and local "public utilities" excise, both levels. The tax base is the
    # regulated-utility bill, which in state tax codes has long covered telephone
    # alongside power, gas and water.
    'public utilities': (
        ('L30517', 'L30522'),
        ('221100', '221200', '221300', '517110', '517210', '517A00'),
    ),
    'severance': (
        ('S23043',),
        ('211000', '212100', '212230', '2122A0', '212310', '2123A0'),
    ),
}

#: Everything not in :data:`NAMED_TAX_LINES` - general sales taxes (``B248RC``,
#: 405,139 in 2017), the unnamed federal and state and local "other" excise
#: lines, and other taxes on goods and services. Named here for the diagnostics;
#: the residual is always computed by subtraction, so a NIPA line added or
#: renamed cannot fall out of the column silently.
RESIDUAL_LABEL = 'sales tax and unnamed excise'

#: Slack on the identity checks, in USD. The published table is in whole
#: millions, so a check has to tolerate one rounding step rather than demanding
#: an exact floor.
_ROUNDING_TOLERANCE = 1.0 * MILLION_CURRENCY_TO_CURRENCY


# --- NIPA, the annual level ------------------------------------------------


@functools.cache
def nipa_product_tax_lines(year: int) -> pd.Series:
    """NIPA table 3.5 by series code for *year*, USD.

    Read from the ``BEA_NIPA`` FBA, which extracts BEA's ``FlatFiles.ZIP``, so
    this and every other NIPA consumer in the build sit on one vintage.
    """
    fba = getFlowByActivity(_NIPA_SOURCE, int(year))
    description = fba['Description'].astype(str)
    rows = fba[description.str.startswith(f'{PRODUCT_TAX_TABLE}:')].copy()
    if rows.empty:
        raise ValueError(
            f'{_NIPA_SOURCE} {year} carries no {PRODUCT_TAX_TABLE} rows. That '
            f'table is listed in BEA_NIPA.yaml, so an empty result means the '
            f'extract changed rather than that the tax stopped being levied.'
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
        .rename(f'{PRODUCT_TAX_TABLE}_{year}')
    )


def nipa_line_total(codes: Iterable[str], year: int) -> float:
    """Sum of NIPA series *codes* in *year*, USD, raising on a missing code.

    A series that disappears from the flat files has to stop the build rather
    than sum to zero: silently dropping, say, the state gasoline line would move
    46bn out of ``324110`` and into the residual while the column total still
    tied to NIPA.
    """
    lines = nipa_product_tax_lines(year)
    codes = list(codes)
    missing = [code for code in codes if code not in lines.index]
    if missing:
        raise ValueError(
            f'NIPA {PRODUCT_TAX_TABLE} {year} has no series {missing}. The line '
            f'was published in {ANCHOR_YEAR}; if BEA has retired or renamed it, '
            f'NAMED_TAX_LINES needs updating rather than defaulting to zero.'
        )
    return float(lines[codes].sum())


def top_control_total(year: int) -> float:
    """The ``TOP`` column total for *year*, USD - taxes on products less duties.

    **Observed, not estimated.** 716,925 in 2017 against the published Supply
    column's 716,926.
    """
    total = nipa_line_total(PRODUCT_TAX_SUBTOTALS, year) - nipa_line_total(
        [CUSTOMS_DUTIES_SERIES], year
    )
    if total <= 0:
        raise ValueError(
            f'NIPA taxes on products net of customs duties is {total:,.0f} USD in '
            f'{year}, which cannot be a Supply TOP column total. Either the '
            f'customs line is being read at the wrong level or the two '
            f'taxes-on-products subtotals are not the ones BEA now publishes.'
        )
    return total


# --- the 2017 anchor -------------------------------------------------------


@functools.cache
def published_top_by_commodity() -> pd.Series:
    """The published 2017 Supply ``TOP`` column, USD, by BEA 2017 commodity.

    Non-negative on all 402 commodities and non-zero on 339 of them.
    """
    supply = _load_2017_detail_supply_use_usa('Supply_detail').rename(
        columns=lambda column: column.strip()
    )
    commodities = [code for code in USA_2017_COMMODITY_CODES if code in supply.index]
    top = supply.loc[commodities, 'TOP'].astype(float) * MILLION_CURRENCY_TO_CURRENCY
    return (
        top.reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
        .rename_axis('commodity')
        .rename('TOP')
    )


@functools.cache
def named_line_weights() -> pd.DataFrame:
    """Commodity x group share of each named line, from 2017 ``TOP``. Columns sum to 1.

    The within-set split is the only place the 2017 table is used as an allocator
    rather than as an anchor, and it only does work for the four multi-commodity
    groups: alcohol, insurance receipts, public utilities and severance. The
    other six name a single commodity, or a set whose line is zero.
    """
    top = published_top_by_commodity()
    weights = pd.DataFrame(
        0.0,
        index=top.index,
        columns=pd.Index(list(NAMED_TAX_LINES), name='tax_line_group'),
    )
    for group, (_, commodities) in NAMED_TAX_LINES.items():
        unknown = [code for code in commodities if code not in top.index]
        if unknown:
            raise ValueError(
                f'NAMED_TAX_LINES group {group!r} maps to {unknown}, which are not '
                f'BEA 2017 Detail commodities.'
            )
        set_top = float(top[list(commodities)].sum())
        if set_top <= 0:
            raise ValueError(
                f'NAMED_TAX_LINES group {group!r} maps to commodities carrying no '
                f'{ANCHOR_YEAR} TOP at all, so its line has no basis to split on.'
            )
        weights.loc[list(commodities), group] = top[list(commodities)] / set_top
    return weights


def named_line_feasibility() -> pd.DataFrame:
    """Each group's 2017 line against the 2017 ``TOP`` its commodities carry.

    ``ratio`` is what the group takes out of its set's published tax. It runs
    0.000 (medical devices, a zero line) to 0.902 (tobacco), so every group fits
    inside the tax its own commodities are recorded as bearing - which is the
    check that the commodity assignment is not merely plausible but *possible*.
    """
    top = published_top_by_commodity()
    rows = []
    for group, (series, commodities) in NAMED_TAX_LINES.items():
        amount = nipa_line_total(series, ANCHOR_YEAR)
        set_top = float(top[list(commodities)].sum())
        rows.append(
            {
                'tax_line_group': group,
                'nipa_series': ' + '.join(series),
                'commodities': len(commodities),
                'nipa_2017': amount,
                'set_top_2017': set_top,
                'ratio': amount / set_top if set_top else float('nan'),
                'fits': amount <= set_top,
            }
        )
    return pd.DataFrame(rows).set_index('tax_line_group')


def named_line_allocation(year: int) -> pd.Series:
    """The named NIPA product-tax lines for *year*, allocated to commodities. USD."""
    return (
        named_line_weights()
        .mul(_named_line_amounts(year), axis='columns')
        .sum(axis='columns')
        .rename('named')
    )


def _named_line_amounts(year: int) -> pd.Series:
    """Each named group's NIPA total for *year*, USD, in ``NAMED_TAX_LINES`` order."""
    return pd.Series(
        {
            group: nipa_line_total(series, year)
            for group, (series, _) in NAMED_TAX_LINES.items()
        },
        dtype=float,
    ).rename_axis('tax_line_group')


@functools.cache
def residual_2017() -> pd.Series:
    """2017 ``TOP`` less what the named lines account for, per commodity. USD.

    503,579 $M over 339 commodities - general sales tax above all, plus the
    unnamed federal and state and local "other excise" lines.

    ⚠️ **Non-negative by construction, and checked anyway.** Each group is spread
    in proportion to its set's own 2017 ``TOP``, so a group can only exceed a
    commodity's tax if its line exceeds the whole set's - which
    :func:`named_line_feasibility` rules out - or if two groups land on one
    commodity and jointly exceed it, which ``5241XX`` is the only candidate for,
    at 18,762 of 20,485.
    """
    residual = published_top_by_commodity() - named_line_allocation(ANCHOR_YEAR)
    negative = residual[residual < -_ROUNDING_TOLERANCE]
    if not negative.empty:
        raise ValueError(
            f'The named NIPA tax lines assign more tax than the {ANCHOR_YEAR} '
            f'Supply table records for {sorted(negative.index)} '
            f'(worst {negative.min():,.0f} USD). The residual carries general '
            f'sales tax and cannot be negative; a commodity set in '
            f'NAMED_TAX_LINES is too narrow for the line it receives.'
        )
    return residual.clip(lower=0.0).rename('residual')


@functools.cache
def residual_share() -> pd.Series:
    """:func:`residual_2017` as shares summing to 1 - the frozen part of the build.

    This is the issue's default proposal, kept deliberately and confined to the
    70% of ``TOP`` where nothing better is available. **Two movers were built and
    measured against it, and both were rejected**:

    ``move the service side by T007``
        Sales tax on a restaurant meal does move with restaurant output, and
        ``T007`` is sourced for all of 2017-2024, so this looked right - and it
        fixes exactly the pandemic sectors the issue worries about (accommodation
        2020 falls 7,511 $M against the frozen vector). ⚠️ **But moving one side
        only is not a mover, it is a transfer.** The goods side has no annual
        base, so it absorbs the whole renormalisation: the moved side reaches
        **1.86x** its 2017 level by 2024 against a control total growing
        **1.53x**, with the goods side pushed down to 1.25x to pay for it. The
        drift is an artefact of the construction, not a measurement.

    ``move every commodity by T007``
        Removes the drift - the rescale falls to 1.10 - but ``T007`` is **zero by
        definition** for ``S00402`` used and secondhand goods, ``S00300`` and
        ``4200ID``, which are not domestic output at all. ``S00402`` carries
        15,699 $M of 2017 ``TOP``, the eighth largest of any commodity, so the
        mover cannot move a top-ten position and falls back to 1.0 there. Domestic
        output is also the wrong base for an import-heavy good: apparel sells more
        each year while ``315000`` produces less.

    ✅ **The real fix landed, and it is :func:`purchaser_base`.** It was written
    here as "Step 5's output", but the supply bridge reaches it first: once
    ``TRADE`` and ``TRANS`` were sourced (#611), ``T013 + T014`` *is* a
    purchaser-price base by commodity - supply at basic value plus the margins
    that carry it to the purchaser - and it clears both objections above. See
    :func:`residual_share_for_year`. This function is what 2024 still falls back
    on, because the margin columns stop at 2023.
    """
    residual = residual_2017()
    return (residual / residual.sum()).rename('residual_share')


#: Last year the margin columns reach, so the last year the purchaser-price base
#: can be built. Kept local rather than imported from ``eeio.nowcast``, which
#: imports this module.
_LAST_MARGIN_YEAR = 2023


@functools.cache
def purchaser_base(year: int) -> pd.Series:
    """``T013 + T014`` by commodity for *year*, USD - supply at purchaser value
    less taxes.

    This is the annual base the residual moves on. It is assembled from the
    bridge's *components* rather than from
    :func:`~bedrock.transform.eeio.nowcast.derive_initial_supply_bridge`, which
    would be circular: the bridge calls :func:`top_column`, so ``TOP`` cannot
    read the finished bridge back.

    ⚠️ **Taxes are deliberately excluded from the base.** ``T015`` carries
    ``TOP`` itself, so including it would make the tax its own allocator.
    ``T013 + T014`` is the largest slice of purchaser value that ``TOP`` does not
    appear in.
    """
    from bedrock.transform.eeio.nowcast import (  # noqa: PLC0415
        _supply_fbs_commodity_vector,
        _trade_fbs_commodity_vector,
    )
    from bedrock.transform.iot.nowcast_trade_margins import (  # noqa: PLC0415
        trade_margin_column,
    )
    from bedrock.transform.iot.nowcast_transport_margins import (  # noqa: PLC0415
        transport_margin_column,
    )
    from bedrock.transform.trade.madj import madj_detail_usd  # noqa: PLC0415

    index = pd.Index(USA_2017_COMMODITY_CODES, name='commodity')

    def _v(series: pd.Series) -> pd.Series:
        return pd.Series(series).reindex(index).fillna(0.0).astype(float)

    # T013 = T007 + MCIF + MADJ
    base = (
        _v(_supply_fbs_commodity_vector(year, False))
        + _v(_trade_fbs_commodity_vector(f'Trade_Imports_{year}', False))
        + _v(madj_detail_usd(year, False))
        # T014 = TRADE + TRANS
        + _v(trade_margin_column(year))
        + _v(transport_margin_column(year))
    )
    # The margin columns are signed - the modes that give margin up carry it
    # negative - and a base must not be. A commodity whose margin give-up
    # exceeds its own supply is a giver, not a buyer, of the tax base.
    return base.clip(lower=0.0).rename('purchaser_base')


def residual_share_for_year(year: int) -> pd.Series:
    """The residual's commodity shares in *year*, summing to 1.

    The 2017 residual moved by each commodity's growth in
    :func:`purchaser_base`, then renormalised. This replaces the frozen 2017
    vector for every year the margin columns reach.

    **Why this base and not ``T007``.** Two ``T007`` movers were built and
    rejected (see :func:`residual_share`); this one clears both objections:

    ``S00402`` used and secondhand goods
        Carries 15,699 $M of 2017 ``TOP``, eighth largest of any commodity, and
        ``T007`` is **zero** for it by definition - it is not domestic output, so
        the ``T007`` mover could not move a top-ten position. Its purchaser base
        is 174,312 $M in 2017, because secondhand goods are almost entirely trade
        margin. The margin layer reaches exactly what domestic output cannot.

    **No renormalisation drift.** The one-sided ``T007`` mover reached 1.86x its
    2017 level by 2024 against a control growing 1.53x. This base tracks the
    control: the implied rescale stays within **0.939-1.008** across 2017-2023,
    because purchaser value is what sales tax is actually levied on.

    ⚠️ **The pandemic years are the check.** 2020 accommodation ``721000`` falls
    7,045 $M against the frozen vector, air transport ``481000`` roughly halves,
    and restaurants fall - the sectors #580 worried the frozen vector would get
    wrong. The rejected ``T007`` service-side mover put accommodation at
    -7,511 $M, so two independent bases agree on the size of the pandemic effect
    and only this one reaches it without breaking the control total.

    ⚠️ **2024 holds 2023's shares** rather than reverting to the frozen 2017
    vector. ``TRADE`` and ``TRANS`` stop at 2023, so 2024 has no base; carrying
    the last observed shares forward keeps the series continuous, where falling
    back to 2017 would undo six years of movement in one step. It is a hold, not
    a measurement.
    """
    year = int(year)
    if year > _LAST_MARGIN_YEAR:
        return residual_share_for_year(_LAST_MARGIN_YEAR)

    residual = residual_2017()
    base_now = purchaser_base(year).reindex(residual.index).fillna(0.0)
    base_2017 = purchaser_base(ANCHOR_YEAR).reindex(residual.index).fillna(0.0)

    ratio = (base_now / base_2017).replace([float('inf'), float('-inf')], 1.0)
    # A commodity with no 2017 base cannot be moved; it keeps its frozen share.
    ratio = ratio.where(base_2017 > 0, 1.0).fillna(1.0)

    moved = (residual * ratio).clip(lower=0.0)
    total = float(moved.sum())
    if total <= 0:
        raise ValueError(
            f'The TOP residual moved to a non-positive total in {year}. The '
            f'purchaser-price base is meant to be positive wherever the 2017 '
            f'residual is, so this means the base collapsed.'
        )
    return (moved / total).rename('residual_share')


# --- the column ------------------------------------------------------------


def top_decomposition(year: int) -> pd.DataFrame:
    """``TOP`` for *year* split into its named lines and the residual. USD.

    Columns: one per :data:`NAMED_TAX_LINES` group, then ``residual`` and
    ``TOP``. The row margin is the column :func:`top_column` returns.
    """
    if int(year) not in TOP_YEARS:
        raise ValueError(
            f'TOP is built for {TOP_YEARS.start}-{TOP_YEARS.stop - 1}; {year} is '
            f'outside the years the BEA_NIPA extract carries.'
        )
    amounts = _named_line_amounts(year)
    control = top_control_total(year)
    residual_total = control - float(amounts.sum())
    if residual_total <= 0:
        raise ValueError(
            f'The named NIPA lines are {amounts.sum():,.0f} USD in {year}, at or '
            f'above the whole taxes-on-products total of {control:,.0f}. That '
            f'leaves no general sales tax, so a line is being double counted or '
            f'read at the wrong level.'
        )

    out = named_line_weights().mul(amounts, axis='columns')
    out['residual'] = residual_share_for_year(year) * residual_total
    out['TOP'] = out.sum(axis='columns')
    return out


def top_column(year: int = ANCHOR_YEAR) -> pd.Series:
    """The Supply table's ``TOP`` column for *year*. USD, by BEA 2017 commodity.

    Non-negative everywhere, and summing to :func:`top_control_total` exactly.

    ⚠️ **Zero is a value here, not a gap.** 63 commodities bear no tax on products
    in 2017 and stay at zero every year: they carry no named line and no share of
    the residual, which is sourced information rather than an unfilled cell, and
    ``T015`` needs it to be a number.
    """
    column = top_decomposition(year)['TOP'].rename('TOP')

    control = top_control_total(year)
    residual = float(column.sum()) - control
    if abs(residual) > _ROUNDING_TOLERANCE:
        raise ValueError(
            f'TOP {year} sums to {column.sum():,.0f} USD against a NIPA control of '
            f'{control:,.0f}. The named lines and the residual are controlled to '
            f'that total by construction, so a gap means one of them was scaled '
            f'twice.'
        )
    if (column < 0).any():
        raise ValueError(
            f'TOP {year} is negative on {sorted(column.index[column < 0])}. A tax '
            f'on products is not a subsidy; SUB is a separate column.'
        )
    return column


# --- the producer-level / trade-level split --------------------------------


@functools.cache
def trade_level_share() -> pd.Series:
    """Each commodity's trade-level share of its 2017 ``TOP``. 0 to 1.

    1.00 on apparel, used goods and light trucks - all of whose product tax is
    sales tax collected by the retailer - and 0.00 on restaurants, electric
    power, air transport, telecoms and legal services, whose tax is levied on the
    seller directly and never passes through a margin column. Petroleum refineries
    are 0.998 and tobacco 0.63.

    Commodities with no ``TOP`` at all get 0.0; they have no tax to split.
    """
    top = published_top_by_commodity()
    trade_level = trade_level_tax_2017().reindex(top.index).fillna(0.0)
    share = (trade_level / top.where(top > 0)).fillna(0.0)
    return share.clip(lower=0.0, upper=1.0).rename('trade_level_share')


def top_by_level(year: int = ANCHOR_YEAR) -> pd.DataFrame:
    """``TOP`` for *year* split into ``producer_level`` and ``trade_level``. USD.

    ⚠️ **Step 4c wants ``producer_level``, not ``TOP``.** The margin base it
    rebuilds is ``T013 + MDTY + SUB + producer-level TOP``; the trade-level share
    is already inside the margin rates, and adding it twice overstates the base by
    19% on petroleum refineries and 36% on tobacco.
    """
    column = top_column(year)
    trade_level = (column * trade_level_share()).rename('trade_level')
    return pd.concat(
        [(column - trade_level).rename('producer_level'), trade_level, column],
        axis='columns',
    )


def control_total_table(years: Iterable[int] | None = None) -> pd.DataFrame:
    """The annual NIPA lines behind the column, in $M, for diagnostics."""
    years = list(TOP_YEARS if years is None else years)
    rows = {}
    for year in years:
        amounts = _named_line_amounts(year)
        control = top_control_total(year)
        rows[year] = {
            **amounts.to_dict(),
            'named total': float(amounts.sum()),
            RESIDUAL_LABEL: control - float(amounts.sum()),
            'TOP': control,
        }
    return (pd.DataFrame(rows).T / MILLION_CURRENCY_TO_CURRENCY).rename_axis('year')
