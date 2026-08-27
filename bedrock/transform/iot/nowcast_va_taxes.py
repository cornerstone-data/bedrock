"""The Use table's ``T00TOP`` and ``T00SUB`` rows - value added's tax wedge.

The last two rows of Step 2's value-added block, and the only two that are not
estimated from NIPA at all. They are the **same money** as the Supply table's
``TOP``/``MDTY``/``SUB`` columns, carried on the industry axis instead of the
commodity axis::

    VAPRO = VABAS + T00TOP - T00SUB

So this module **converts** rather than re-estimates, and its whole content is
the operator that does the conversion. ``TOP`` and ``SUB`` are built in Step 4d
(:mod:`~bedrock.transform.iot.nowcast_product_taxes`,
:mod:`~bedrock.transform.iot.nowcast_subsidies`); ``MDTY`` in
:mod:`~bedrock.transform.trade.duties`.

⚠️ **The obvious operator is wrong, and it was measured before this was built.**
:mod:`bedrock.analysis.nowcasting.tax_axis_conversion` scored the benchmark
market-share matrix ``D[c, i] = V[c, i] / T007[c]`` against the published 2017
rows and got **r = 0.202 at 114.6% error** on ``T00TOP``. It is not noisy, it is
pointed at the wrong stage of the chain: a tax on a product is remitted by
whoever *sells* it, so market shares send the entire petroleum tax from
wholesalers to refineries and the entire motor-vehicle tax from dealers to
assemblers. 55.7% of published ``T00TOP`` sits in wholesale and retail.

The two rows need two different operators
------------------------------------------

============  ====================================  ========  ==============
row           operator                              2017 r    2017 ``|err|``
============  ====================================  ========  ==============
``T00TOP``    level split + named routings           0.947            27.9%
``T00SUB``    code identity + two named routings     1.000             0.0%
============  ====================================  ========  ==============

``T00TOP`` is a seed and stays one. ``T00SUB`` reproduces the published row
**exactly** in the benchmark year - shape agreement to 7e-17, machine precision
- which is a stronger claim and is argued below.

⚠️ ``T00TOP`` scores 0.947 here against ``tax_axis_conversion``'s 0.948 on the
same operator, and the difference is the **make matrix**, not the method. The
analysis prototype reads the published 2017 Supply table; this module reads
``Detail_Supply_2017``, so that the same code serves every year. The FBS
reproduces published ``T007`` to rounding (33,772,550m against 33,772,566m) but
not cell by cell. 0.001 of correlation is the price of the operator being
annual.

``T00TOP`` - the level split, which Step 4c already computes
--------------------------------------------------------------

:func:`~bedrock.transform.iot.nowcast_product_taxes.top_by_level` splits ``TOP``
per commodity into **producer-level** and **trade-level** from an identity with
nothing modelled in it - excise sits in Producers' Value, sales tax sits inside
the margin columns. That split is exactly the producer-versus-seller distinction
market shares get wrong. Applied, with the progression measured on 2017:

===============================================  ======  ==========
operator                                           corr    ``|err|``
===============================================  ======  ==========
market share on all ``TOP + MDTY``                 0.204     114.6%
+ level split, trade-level by trade output         0.743      41.9%
+ motor fuel routed to ``424700`` by name          0.946      29.9%
+ government columns zeroed, renormalised          0.948      27.9%
===============================================  ======  ==========

Three pieces, and two of them are exact rather than estimated:

``4200ID`` customs duties
    A **lookup, not an allocation**. BEA books the whole of import duties to
    that one synthetic industry code - 38,513 published against a Supply
    ``MDTY`` of 38,510 in 2017. 5.1% of the row, free, in every year.

the ten government columns
    **Zero by an accounting rule**, not by a gap: a tax levied by government and
    remitted by a government producer nets out. Published ``T00TOP`` is zero on
    every one of them bar 538 on ``S00203``, while the columns themselves are
    populated (``V00100`` and ``VABAS`` are non-zero). The market-share leg
    violated this by 10,513, because government *does* produce taxed
    commodities. :func:`market_share_matrix` drops those columns and
    **renormalises each commodity over the producers that remain**, so the tax
    stays with its own commodity instead of being deleted or smeared.

motor fuel to ``424700``
    The one named routing wholesale needs. Output shares are worse than useless
    within wholesale (**r = -0.192**): ``424700`` petroleum wholesalers takes
    51.3% of wholesale product tax on 3.4% of wholesale output, because that tax
    is motor fuel excise rather than anything broad-based. Routing motor fuel's
    trade-level tax there by name takes the row from 0.743 to 0.946, and the
    remaining nine wholesale industries then score 0.825 on output shares - they
    behave like retail.

⚠️ **27.9% absolute error is a seed, not a target**, and the residual is 20
industries rather than 402: the top 20 carry 85.0% of it and 17 of them are
trade industries. Step 5's balance moves these cells under economy-wide soft
targets. See ``tax_axis_conversion`` for why probing the remaining sectors one
by one is not worth it - construction was the last block-shaped unknown and it
came back exact (r = 1.000, 1.7%).

``T00SUB`` - code identity, and two routings that close it
------------------------------------------------------------

✅ **Subsidies convert on code identity, and in 2017 the reconstruction is
exact.** 398 of the 402 commodity codes are also industry codes, and a subsidy
is paid to an *operator* rather than attaching to a product, so the money simply
stays on its own code. The four commodity-only codes (``S00300``, ``S00401``,
``S00402``, ``S00900``) carry no subsidy in any year, so nothing falls back.

Market shares score 0.676 at 79.8% error here and code identity alone scores
0.569 - **worse**, which is the finding, because identity's entire residual is
*two cells* rather than a smear:

===========  ===========  ===========  ==========================================
industry        identity    published   what it is
===========  ===========  ===========  ==========================================
``S00203``             0       19,471   public housing authorities
``531HST``        35,778       16,307   the same money, on the commodity it makes
``S00102``             0        6,339   federal insurance enterprises
``5241XX``         6,339            0   the same money, on the commodity it makes
===========  ===========  ===========  ==========================================

Those are the only four cells that differ by more than a dollar, and they are
two pairs - :func:`check` asserts that, so a third routing appearing is a test
failure rather than a silent drift. BEA books public-housing operating subsidies to ``S00203`` other
state and local government enterprises, and federal crop and flood insurance
subsidies to ``S00102`` - both government enterprises that *produce* a
subsidised commodity. :data:`GOVERNMENT_ENTERPRISE_ROUTINGS` moves them, and
with those two routings 2017 reproduces the published row to the dollar.

⚠️ **The residual 1 $M is a control gap, not a misallocation.** NIPA ``A107RC``
reads 59,875 where the published Supply column reads 59,876, and that one
million spreads proportionally across all twelve subsidised cells. :func:`check`
therefore tests **shape and level separately** - normalised shape must agree to
1e-6, and the level gap must not exceed the control gap. Scoring the raw levels
would report BEA's own rounding as error and could hide a real one behind it.

⚠️ **The routing shares are frozen at 2017 and that is the seed's whole
assumption.** NIPA ``T31300`` publishes eight lines - total, federal,
agricultural, housing, maritime, air carriers, other, state and local - and none
of them splits housing assistance between public authorities and private
landlords. So ``S00203``'s 54.4% of the housing line is an anchor moved by that
line's own annual growth, exactly as the commodity axis anchors and moves. The
levels are observed; this one split is not.

⚠️ **The routings do not fire on the PPP years' ``other`` column.** In 2020-2021
:func:`~bedrock.transform.iot.nowcast_subsidies.sub_decomposition` replaces the
``other`` type with BEA's PPP-by-industry allocation, so ``5241XX``'s subsidy in
those years is private-insurance PPP rather than the federal enterprise line.
Routing it to ``S00102`` would put 587bn of pandemic support programme on a
government enterprise. :data:`PANDEMIC_YEARS` is where that is decided, and it
is the upstream module's decision being carried, not a new one.

⚠️ **PPP is already industry-shaped, which is why identity is right for it.**
BEA publishes PPP *by industry*; the commodity axis gets it by spreading each
sector's amount over that sector's commodities by ``T007``. Converting back by
code identity very nearly recovers the original allocation, so the pandemic
years are the ones this operator is best on, not worst.

⚠️ **``S00102`` is over-subsidised from 2022, and the cause is upstream.** The
insurance routing is 100%, so the federal enterprise takes whatever the
commodity axis puts on ``5241XX`` - 36.6bn in 2022 against 6.3bn in 2017, a
5.8x rise. That is the NIPA ``other`` line still carrying pandemic-era
programmes that are not federal insurance subsidies, moved by
:func:`~bedrock.transform.iot.nowcast_subsidies.type_growth`. The conversion is
faithful: the *same* 36.6bn sits on commodity ``5241XX`` whether or not this
module runs, so the axis change neither causes the problem nor hides it. Fixing
it means splitting NIPA's ``other`` line, which ``T31300`` does not do. Read the
2022-2024 ``S00102`` cell as the ``other`` line's residue, not as a measurement.

===============  =======  =======  =======  =======  =======
``S00102``, $bn     2017     2019     2020     2022     2024
===============  =======  =======  =======  =======  =======
routed                6.3      5.1      0.0     36.6     13.2
===============  =======  =======  =======  =======  =======

Everything here is annual
--------------------------

The market-share matrix and the industry-output weights come from
``Detail_Supply_<year>`` rather than from the 2017 benchmark, so the operator
moves with the Supply block Step 4a builds. What is frozen is named and
enumerable: ``trade_level_share`` inside ``top_by_level``, the motor-fuel
routing, the government-column rule, and the two subsidy routing shares.

Sign convention
----------------

⚠️ ``T00SUB`` is returned **negative** here, matching
:data:`~bedrock.transform.eeio.nowcast._USE_VALUE_ADDED_SUBTOTALS` and
``nowcast_mask.published_2017_panel`` - the balance's convention, so ``VAPRO``
*adds* it. BEA publishes the Use row positive and subtracts it. Feeding a
BEA-signed row into the subtotals silently doubles the subsidy wedge.
:func:`t00sub_row` is the one place the flip happens.

Usage::

    uv run python -m bedrock.transform.iot.nowcast_va_taxes
    uv run python -m bedrock.transform.iot.nowcast_va_taxes --check
"""

from __future__ import annotations

import argparse
import functools
import sys
import typing as ta

import numpy as np
import pandas as pd

from bedrock.transform.flowbysector import getFlowBySector
from bedrock.transform.iot.nowcast_product_taxes import (
    NAMED_TAX_LINES,
    top_by_level,
)
from bedrock.transform.iot.nowcast_subsidies import (
    PANDEMIC_YEARS,
    sub_control_total,
    sub_decomposition,
)
from bedrock.transform.trade.duties import mdty_detail_usd
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: The benchmark, and the only year the published industry rows exist.
ANCHOR_YEAR = 2017

#: Years both rows are built for. Bounded by ``TOP``/``SUB`` upstream, which are
#: bounded by the ``BEA_NIPA`` extract, not by anything here.
VA_TAX_YEARS = range(2017, 2025)

#: Import duties land whole on this one synthetic industry code - a lookup, not
#: an allocation. 38,513 published against a Supply ``MDTY`` of 38,510 in 2017.
CUSTOMS_INDUSTRY = '4200ID'

#: Motor fuel excise is remitted by petroleum wholesalers, not spread across
#: wholesale by output. The one named routing the wholesale block needs.
PETROLEUM_WHOLESALERS = '424700'

#: BEA books no taxes on production to any industry code with these prefixes.
#: The single 2017 exception is 538 of ``T00TOP`` on ``S00203``.
GOVERNMENT_PREFIXES = ('S00', 'G')

#: Industry code prefixes of the wholesale and retail blocks the trade-level
#: leg lands on. ``4200ID`` starts ``42`` and is **not** one of them.
WHOLESALE_PREFIX = '42'
RETAIL_PREFIXES = ('44', '45', '4B')

#: Subsidies BEA books to a government enterprise rather than to the industry
#: whose code the commodity carries, as ``(subsidy type, commodity) -> industry``.
#: The share moved is derived from the published benchmark rather than written
#: down here - see :func:`government_enterprise_shares`.
#:
#: ⚠️ These two are the *entire* residual of the code-identity operator in 2017.
#: Adding a third means the identity has stopped closing, which
#: :func:`check` will say before this constant needs editing.
GOVERNMENT_ENTERPRISE_ROUTINGS: ta.Mapping[tuple[str, str], str] = {
    ('housing', '531HST'): 'S00203',
    ('other', '5241XX'): 'S00102',
}

#: Slack on the identity checks, in USD. The published tables are in whole
#: millions, so an exact comparison trips on BEA's own rounding.
_ROUNDING_TOLERANCE = 1.0 * MILLION_CURRENCY_TO_CURRENCY


# --- the annual axes -------------------------------------------------------


@functools.cache
def make_block(year: int) -> pd.DataFrame:
    """The ``Detail_Supply_<year>`` domestic-output block, commodity x industry, USD.

    ⚠️ Commodity is ``SectorConsumedBy`` and industry is ``SectorProducedBy`` -
    the Supply table's rows are commodities. Reading them the intuitive way
    round transposes the block, which still balances economy-wide and is
    therefore invisible in every total.
    """
    fbs = pd.DataFrame(getFlowBySector(f'Detail_Supply_{year}'))
    block = (
        fbs.groupby(['SectorConsumedBy', 'SectorProducedBy'])['FlowAmount']
        .sum()
        .unstack('SectorProducedBy')
        .astype(float)
    )
    return block.reindex(
        index=list(USA_2017_COMMODITY_CODES), columns=list(USA_2017_INDUSTRY_CODES)
    ).fillna(0.0)


def government_industries() -> list[str]:
    """The ten industry codes BEA books no taxes on production to."""
    return [
        i for i in USA_2017_INDUSTRY_CODES if str(i).startswith(GOVERNMENT_PREFIXES)
    ]


def trade_industries() -> list[str]:
    """Wholesale plus retail, the industries the trade-level leg lands on.

    ⚠️ ``4200ID`` is excluded by name. It starts ``42`` and a bare prefix rule
    files customs duties under wholesale trade, where they would then be spread
    by output instead of landing whole on their own code.
    """
    return [
        i
        for i in USA_2017_INDUSTRY_CODES
        if str(i).startswith((WHOLESALE_PREFIX, *RETAIL_PREFIXES))
        and i != CUSTOMS_INDUSTRY
    ]


@functools.cache
def industry_output(year: int) -> pd.Series:
    """Industry output at basic value, USD - the make block's column margin.

    Used as the within-trade weight. For a trade industry output very nearly
    *is* margin - eight retail commodities give up exactly 100% of ``T013`` and
    the ten wholesale ones 90.8-99.4% - so this is the margin-proportional case
    rather than a crude stand-in for it.
    """
    return make_block(year).sum(axis=0).rename('industry_output')


@functools.cache
def market_share_matrix(
    year: int, exclude_government: bool = False
) -> pd.DataFrame:
    """``D[c, i]``: industry ``i``'s share of commodity ``c``'s output in *year*.

    Normalised by the commodity's own output (the make block's **row** margin,
    ``T007``) rather than by industry output - that is the market-share matrix,
    not the commodity mix. A commodity-indexed quantity has to be spread over
    the industries that make that commodity, which is this one; the commodity
    mix answers the transposed question and gives an unrelated number.

    ``exclude_government`` drops the ten government columns and **renormalises
    each commodity over the producers that remain**, so the tax on a commodity
    stays with that commodity rather than being deleted or smeared economy-wide.
    A commodity with no remaining producer has nowhere to go, so its row is kept
    unrenormalised rather than losing the money; no commodity produced entirely
    by government carries any product tax, so the fallback never fires.
    """
    block = make_block(year)
    output = block.sum(axis=1)
    shares = block.div(output.replace(0.0, np.nan), axis=0).fillna(0.0)
    if not exclude_government:
        return shares

    excluded = set(government_industries())
    keep = [i for i in shares.columns if i not in excluded]
    retained = shares[keep].sum(axis=1)
    renormalised = pd.DataFrame(0.0, index=shares.index, columns=shares.columns)
    renormalised[keep] = shares[keep].div(retained.where(retained > 0), axis=0)
    stranded = retained <= 0
    renormalised.loc[stranded] = shares.loc[stranded]
    return renormalised.fillna(0.0)


# --- T00TOP ----------------------------------------------------------------


def t00top_row(year: int) -> pd.Series:
    """``T00TOP`` by industry for *year*, USD positive.

    Three legs, summed:

    1. **producer-level** ``TOP`` by market share, government columns dropped
       and each commodity renormalised over its remaining producers;
    2. **trade-level** ``TOP`` onto wholesale and retail - motor fuel routed to
       ``424700`` by name, the rest spread by industry output;
    3. **duties** whole onto ``4200ID``.

    The column total is ``TOP + MDTY`` by construction, which is the published
    row's total. What the operator estimates is only the split.
    """
    _guard_year(year)
    industries = list(USA_2017_INDUSTRY_CODES)
    commodities = list(USA_2017_COMMODITY_CODES)

    levels = top_by_level(year)
    producer = levels['producer_level'].reindex(commodities).fillna(0.0)
    trade = levels['trade_level'].reindex(commodities).fillna(0.0)
    duties = float(mdty_detail_usd(year, False).sum())

    shares = market_share_matrix(year, exclude_government=True)
    row = shares.mul(producer, axis=0).sum(axis=0).reindex(industries).fillna(0.0)

    fuel_commodities = [
        c for c in NAMED_TAX_LINES['motor fuel'][1] if c in trade.index
    ]
    fuel_tax = float(trade.loc[fuel_commodities].sum())
    others = [i for i in trade_industries() if i != PETROLEUM_WHOLESALERS]
    weight = industry_output(year).reindex(others).fillna(0.0).clip(lower=0.0)
    weight_total = float(weight.sum())
    if weight_total <= 0:
        raise ValueError(
            f'The non-petroleum trade industries carry no {year} output, so the '
            f'trade-level leg of T00TOP has no weight to spread on. That block '
            f'is 22.8% of the row; it cannot be empty.'
        )

    row[PETROLEUM_WHOLESALERS] += fuel_tax
    row[others] += (float(trade.sum()) - fuel_tax) * (weight / weight_total)
    row[CUSTOMS_INDUSTRY] += duties

    expected = float(producer.sum() + trade.sum() + duties)
    if abs(float(row.sum()) - expected) > _ROUNDING_TOLERANCE:
        raise ValueError(
            f'T00TOP {year} sums to {row.sum():,.0f} USD against a TOP + MDTY '
            f'total of {expected:,.0f}. The three legs partition that total, so '
            f'a gap means one of them was applied twice or dropped.'
        )
    return row.rename('T00TOP')


# --- T00SUB ----------------------------------------------------------------


@functools.cache
def government_enterprise_shares() -> pd.Series:
    """The share of each routed cell that goes to its government enterprise.

    Derived from the published 2017 tables rather than written down: the
    numerator is the enterprise's published ``T00SUB``, the denominator that
    commodity's published Supply ``SUB``. Housing comes out at 0.544 and
    insurance at 1.000.

    ⚠️ **This is the seed's whole assumption.** NIPA ``T31300`` has eight lines
    and none of them splits housing assistance between public authorities and
    private landlords, so the share is anchored here and moved by the housing
    line's own annual growth. The levels are observed; this split is not.
    """
    from bedrock.analysis.nowcasting.tax_axis_conversion import (  # noqa: PLC0415
        _frames,
        published_row,
    )

    supply, use = _frames()
    published_sub = -supply.loc[list(USA_2017_COMMODITY_CODES), 'SUB'].astype(float)
    published_t00sub = published_row(use, 'T00SUB')

    shares = {}
    for (subsidy_type, commodity), industry in GOVERNMENT_ENTERPRISE_ROUTINGS.items():
        cell = float(published_sub.get(commodity, 0.0))
        if cell <= 0:
            raise ValueError(
                f'{commodity} carries no {ANCHOR_YEAR} Supply SUB, so the '
                f'{subsidy_type} routing to {industry} has no base to take a '
                f'share of.'
            )
        share = float(published_t00sub.get(industry, 0.0)) / cell
        if not 0.0 <= share <= 1.0:
            raise ValueError(
                f'The {subsidy_type} routing would move {share:.1%} of '
                f'{commodity} to {industry}. A share outside 0-1 means the two '
                f'axes are not carrying the same money.'
            )
        shares[(subsidy_type, commodity)] = share
    return pd.Series(shares, name='government_enterprise_share')


def t00sub_row(year: int) -> pd.Series:
    """``T00SUB`` by industry for *year*, USD **negative**.

    Code identity on :func:`~bedrock.transform.iot.nowcast_subsidies.sub_decomposition`,
    then the two government-enterprise routings. In 2017 this reproduces the
    published row to the dollar.

    ⚠️ Negative is the balance's convention, matching
    ``nowcast._USE_VALUE_ADDED_SUBTOTALS`` - ``VAPRO`` adds this row. BEA
    publishes it positive and subtracts it.
    """
    _guard_year(year)
    industries = list(USA_2017_INDUSTRY_CODES)
    industry_set = set(industries)

    # sub_decomposition is negative; this row is built positive and flipped once
    # at the end, so the routing arithmetic reads the way the tables do.
    parts = -sub_decomposition(year).drop(columns=['SUB'])
    routed = government_enterprise_shares()

    row = pd.Series(0.0, index=industries)
    for subsidy_type in parts.columns:
        # In 2020-2021 the `other` column is BEA's PPP allocation rather than
        # the anchored line, and PPP on 5241XX is private-insurance support that
        # does not belong to a federal enterprise.
        ppp_replaced = (
            subsidy_type == 'other' and int(year) in PANDEMIC_YEARS
        )
        column = parts[subsidy_type]
        for raw_commodity, raw_amount in column[column != 0.0].items():
            commodity, amount = str(raw_commodity), float(raw_amount)
            key = (str(subsidy_type), commodity)
            share = 0.0 if ppp_replaced else float(routed.get(key, 0.0))
            if share > 0:
                row[GOVERNMENT_ENTERPRISE_ROUTINGS[key]] += amount * share
            remainder = amount * (1.0 - share)
            if commodity in industry_set:
                row[commodity] += remainder
            elif remainder != 0.0:
                raise ValueError(
                    f'{commodity} carries {remainder:,.0f} USD of {year} '
                    f'subsidy but is not an industry code, so code identity '
                    f'has nowhere to put it. The four commodity-only codes are '
                    f'meant to carry no subsidy in any year.'
                )

    expected = float(parts.to_numpy().sum())
    if abs(float(row.sum()) - expected) > _ROUNDING_TOLERANCE:
        raise ValueError(
            f'T00SUB {year} sums to {row.sum():,.0f} USD against a SUB total of '
            f'{expected:,.0f}. Identity and the routings both conserve mass, so '
            f'a gap means a commodity was placed twice.'
        )
    return (-row).rename('T00SUB')


# --- the block -------------------------------------------------------------


def va_tax_rows(year: int) -> pd.DataFrame:
    """``T00TOP`` and ``T00SUB`` for *year*, rows x industries, USD.

    ``T00SUB`` is negative; see the module docstring's sign convention note.
    """
    return pd.DataFrame([t00top_row(year), t00sub_row(year)]).rename_axis(
        index='value_added_code', columns='industry'
    )


def _guard_year(year: int) -> None:
    if int(year) not in VA_TAX_YEARS:
        raise ValueError(
            f'the value-added tax rows are built for {VA_TAX_YEARS.start}-'
            f'{VA_TAX_YEARS.stop - 1}; got {year}. The bound is TOP and SUB '
            f'upstream, which the BEA_NIPA extract sets.'
        )


def wedge_table(years: ta.Iterable[int] | None = None) -> pd.DataFrame:
    """Both rows' totals per year, in $M, for diagnostics.

    The row margin ``T00TOP + T00SUB`` is the producer-basic wedge the Supply
    table carries as ``T015``; the two must agree, and :func:`check` asserts it.
    """
    rows = {}
    for year in list(VA_TAX_YEARS if years is None else years):
        top, sub = t00top_row(year), t00sub_row(year)
        rows[year] = {
            'T00TOP': float(top.sum()),
            'T00SUB': float(sub.sum()),
            'wedge': float(top.sum() + sub.sum()),
        }
    return (pd.DataFrame(rows).T / MILLION_CURRENCY_TO_CURRENCY).rename_axis('year')


# --- report and check ------------------------------------------------------


def _score(estimate: pd.Series, published: pd.Series) -> dict[str, float]:
    industries = list(USA_2017_INDUSTRY_CODES)
    estimate = estimate.reindex(industries).astype(float).fillna(0.0)
    published = published.reindex(industries).astype(float).fillna(0.0)
    nonzero = (estimate.abs() + published.abs()) > 0
    absolute_error = float((estimate - published).abs().sum())
    return {
        'correlation': float(np.corrcoef(estimate[nonzero], published[nonzero])[0, 1]),
        'absolute_error': absolute_error,
        'error_share': absolute_error / float(published.abs().sum()),
    }


def benchmark_scores() -> dict[str, dict[str, float]]:
    """Both rows scored against the published 2017 Use table."""
    from bedrock.analysis.nowcasting.tax_axis_conversion import (  # noqa: PLC0415
        _frames,
        published_row,
    )

    _, use = _frames()
    million = MILLION_CURRENCY_TO_CURRENCY
    return {
        'T00TOP': _score(
            t00top_row(ANCHOR_YEAR) / million, published_row(use, 'T00TOP')
        ),
        # published positive, ours negative
        'T00SUB': _score(
            -t00sub_row(ANCHOR_YEAR) / million, published_row(use, 'T00SUB')
        ),
    }


def report() -> None:
    """Print the benchmark scores and the annual wedge."""
    print('Use value-added tax rows: commodity -> industry conversion\n')
    print(f'{"row":<10}{"corr":>9}{"|error|":>14}{"of row":>10}')
    for row, scores in benchmark_scores().items():
        print(
            f'{row:<10}{scores["correlation"]:>9.3f}'
            f'{scores["absolute_error"]:>14,.0f}'
            f'{scores["error_share"]:>9.1%}'
        )
    print(f'\ngovernment-enterprise routing shares ({ANCHOR_YEAR}):')
    shares = government_enterprise_shares()
    for key, industry in GOVERNMENT_ENTERPRISE_ROUTINGS.items():
        subsidy_type, commodity = key
        print(
            f'  {subsidy_type:<12} {commodity} -> {industry:<8} '
            f'{float(shares[key]):>7.1%}'
        )
    print('\nrow totals by year, $M:')
    print(wedge_table().round(0).to_string())


def check() -> int:
    """Assert every claim the docstring makes. Returns a process exit code."""
    from bedrock.analysis.nowcasting.tax_axis_conversion import (  # noqa: PLC0415
        _frames,
        published_row,
    )

    supply, use = _frames()
    failures: list[str] = []
    million = MILLION_CURRENCY_TO_CURRENCY

    scores = benchmark_scores()
    if scores['T00TOP']['correlation'] < 0.94:
        failures.append(
            f'T00TOP scores r = {scores["T00TOP"]["correlation"]:.3f} against the '
            f'published 2017 row; the measured operator gives 0.948.'
        )
    if scores['T00TOP']['error_share'] > 0.30:
        failures.append(
            f'T00TOP is {scores["T00TOP"]["error_share"]:.1%} off; the measured '
            f'operator gives 27.9%.'
        )

    # T00SUB is a reproduction, not a seed, so it is checked on **shape** and
    # level separately. The level carries a known 1 $M gap - NIPA's 59,875
    # against the workbook's 59,876 - which spreads proportionally across all
    # twelve subsidised cells and is not a misallocation. Comparing the raw
    # levels would report that rounding as error and hide a real one behind it.
    sub_error = scores['T00SUB']['absolute_error']
    published_sub_row = published_row(use, 'T00SUB')
    estimate_sub_row = (-t00sub_row(ANCHOR_YEAR) / million).reindex(
        published_sub_row.index
    ).fillna(0.0)
    shape_error = float(
        (
            estimate_sub_row / estimate_sub_row.sum()
            - published_sub_row / published_sub_row.sum()
        )
        .abs()
        .sum()
    )
    if shape_error > 1e-6:
        failures.append(
            f'T00SUB shape is {shape_error:.2e} off the published 2017 row. Code '
            f'identity plus the two routings is meant to reproduce it exactly, '
            f'so a gap means a third routing has appeared.'
        )
    control_gap = abs(
        float(published_sub_row.sum()) - sub_control_total(ANCHOR_YEAR) / million
    )
    if sub_error > control_gap + 1e-6:
        failures.append(
            f'T00SUB is {sub_error:,.3f} $M off the published 2017 row against a '
            f'NIPA-to-workbook control gap of {control_gap:,.3f} $M. Anything '
            f'above that gap is a misallocation rather than rounding.'
        )

    # the four commodity-only codes must stay empty in every year
    only_commodity = [
        c for c in USA_2017_COMMODITY_CODES if c not in set(USA_2017_INDUSTRY_CODES)
    ]
    for year in VA_TAX_YEARS:
        carried = -sub_decomposition(year)['SUB'].reindex(only_commodity).fillna(0.0)
        if float(carried.abs().sum()) > _ROUNDING_TOLERANCE:
            failures.append(
                f'{year} SUB puts {carried.abs().sum():,.0f} USD on commodity-only '
                f'codes {only_commodity}, which code identity cannot convert.'
            )

    # duties are a lookup and must land whole on 4200ID, every year
    for year in VA_TAX_YEARS:
        duties = float(mdty_detail_usd(year, False).sum())
        seeded = float(t00top_row(year)[CUSTOMS_INDUSTRY])
        if abs(seeded - duties) > _ROUNDING_TOLERANCE:
            failures.append(
                f'{year} puts {seeded:,.0f} USD on {CUSTOMS_INDUSTRY} against a '
                f'Supply MDTY of {duties:,.0f}. Duties are a lookup, not an '
                f'allocation, so the two are the same number.'
            )

    # government columns take no product tax, every year
    for year in VA_TAX_YEARS:
        on_government = float(t00top_row(year)[government_industries()].abs().sum())
        if on_government > _ROUNDING_TOLERANCE:
            failures.append(
                f'{year} seeds {on_government:,.0f} USD of T00TOP onto government '
                f'industries. BEA books zero there, and a wrong seed propagates '
                f'into the Step 7 reallocation.'
            )

    # the wedge must agree with the Supply table's own T015 block
    from bedrock.transform.iot.nowcast_product_taxes import top_column  # noqa: PLC0415
    from bedrock.transform.iot.nowcast_subsidies import (  # noqa: PLC0415
        sub_column,
    )

    for year in VA_TAX_YEARS:
        use_side = float(t00top_row(year).sum() + t00sub_row(year).sum())
        supply_side = float(
            top_column(year).sum()
            + sub_column(year).sum()
            + mdty_detail_usd(year, False).sum()
        )
        if abs(use_side - supply_side) > _ROUNDING_TOLERANCE:
            failures.append(
                f'{year} wedge is {use_side:,.0f} USD on the Use axis against a '
                f'Supply T015 of {supply_side:,.0f}. It is the same money; a gap '
                f'means one axis dropped a column.'
            )

    # the two routings really are the entire code-identity residual in 2017
    published_sub = -supply.loc[list(USA_2017_COMMODITY_CODES), 'SUB'].astype(float)
    identity = pd.Series(0.0, index=list(USA_2017_INDUSTRY_CODES))
    for raw_commodity, raw_amount in published_sub[published_sub != 0].items():
        commodity = str(raw_commodity)
        if commodity in identity.index:
            identity[commodity] += float(raw_amount)
    residual = identity - published_row(use, 'T00SUB')
    differing = sorted(residual.index[residual.abs() > 1.0])
    expected_cells = sorted(
        {c for _, c in GOVERNMENT_ENTERPRISE_ROUTINGS}
        | set(GOVERNMENT_ENTERPRISE_ROUTINGS.values())
    )
    if differing != expected_cells:
        failures.append(
            f'code identity leaves {differing} unexplained in {ANCHOR_YEAR}, not '
            f'{expected_cells}. GOVERNMENT_ENTERPRISE_ROUTINGS is meant to be the '
            f'whole residual.'
        )

    # T00SUB stays non-positive, every year, on every industry
    for year in VA_TAX_YEARS:
        row = t00sub_row(year)
        if (row > _ROUNDING_TOLERANCE).any():
            failures.append(
                f'{year} T00SUB is positive on {sorted(row.index[row > 0])}. This '
                f'row is stored negative; a sign flip fails VAPRO by twice the '
                f'subsidy rather than by the subsidy.'
            )

    if failures:
        print('FAILED:')
        for failure in failures:
            print(f'  - {failure}')
        return 1
    print(
        f'OK: all findings hold '
        f'(T00TOP r = {scores["T00TOP"]["correlation"]:.3f} at '
        f'{scores["T00TOP"]["error_share"]:.1%}; T00SUB shape exact to '
        f'{shape_error:.1e}, level {sub_error:,.2f} $M = the control gap)'
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the docstring findings instead of printing the report',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    report()
    return 0


if __name__ == '__main__':
    sys.exit(main())
