"""Can the product-tax rows be converted from commodity to industry, 2017?

`T00TOP` and `T00SUB` exist on both axes.  The Supply table carries them by
commodity (``TOP``, ``SUB``, ``MDTY``, built in Step 4d, #690); the Use table
carries them by industry as value-added rows.  It is the same money, so Step 2
should **transform** rather than re-estimate -- the question is only whether an
available operator reproduces the published industry row well enough to be worth
seeding the balance with.

This measures that for 2017, where the published answer exists.  The operator
tested is the obvious one: the benchmark **market-share matrix** from the Supply
table, ``D[c, i] = V[c, i] / T007[c]``, which distributes each commodity's tax to
the industries that *produce* that commodity.

The answer is no, and the reason is structural
----------------------------------------------

=========================  ===========  ===============================
row                        correlation  ``sum |est - published|``
=========================  ===========  ===============================
``T00TOP``  (TOP + MDTY)         0.202  865,515  = **114.6%** of the row
``T00TOP``  (TOP only)           0.211  827,322  = 109.5% of the row
``T00SUB``                       0.676   47,776  = 79.8% of the row
=========================  ===========  ===============================

The column totals are right by construction, and per industry the estimate is
worth nothing.  **55.7% of published ``T00TOP`` sits in wholesale and retail
industries**, and market shares put almost none of it there, because a tax on a
product is remitted by whoever *sells* it, not whoever makes it:

===========================  ==========  ==========  =========================
industry                       estimate   published   what it is
===========================  ==========  ==========  =========================
``424700``                           13      88,362   petroleum wholesalers
``324110``                       92,893         397   petroleum refineries
``441000``                        2,301      45,947   motor vehicle dealers
``336111`` + ``336112``          24,141          26   automobile manufacturing
``452000``                          635      37,990   general merchandise
``312200``                       35,559      13,345   tobacco manufacturing
===========================  ==========  ==========  =========================

Read the pairs: the entire petroleum tax moves from wholesalers to refineries,
and the entire motor-vehicle tax from dealers to assemblers.  The operator is
not noisy, it is pointed at the wrong stage of the chain.

⚠️ **This is not a "the benchmark year drifts" finding.**  It is measured *in*
the benchmark year, against the table the mix comes from, with nothing assumed
about later years at all.  A conversion that fails in 2017 cannot be rescued by
being applied closer to 2017.

``T00SUB`` is better but fails differently
------------------------------------------

Correlation 0.676, and the residual is two named structures rather than a
smear.  Government enterprises are under-attributed -- ``S00203`` other state and
local enterprises is 19,471 published against 1,964 estimated (public transit
operating subsidies), ``S00102`` 6,339 against 102 -- while tenant-occupied
housing ``531HST`` is over-attributed at 33,848 against 16,307.  Both are
subsidies paid to an *operator* rather than attaching to a product, so no
product-side operator can place them.  ``T30800`` and the housing tables already
carry those sectors directly (see `compensation_disaggregation_plan.md`).

The one piece that converts exactly
-----------------------------------

✅ **Import duties are a lookup, not an allocation.**  Published ``T00TOP`` on
``4200ID`` is **38,513** against a Supply ``MDTY`` total of 38,510 -- BEA books
the whole of customs duties to that one synthetic industry code.  That is 5.1%
of the row, free and exact, in every year.

What this means for Step 2 and Step 5
-------------------------------------

The plan's decision of 2026-08-17 -- leave the industry distribution of
``T00TOP``/``T00SUB`` to Step 5's balance under economy-wide soft targets --
**stands, and for a stronger reason than it was given.**  It was argued from
stability ("a fixed 2017 conversion ratio is exactly what we cannot assume").
The measured objection is sharper: the conversion is wrong in the benchmark year
itself.

But "leave it free" is not the same as "seed it with nothing", and a usable seed
does exist -- it just is not the Make matrix.

A usable operator, and how little it needs
------------------------------------------

Step 4c already computes what this needs, for its own reasons.
:func:`~bedrock.transform.iot.nowcast_product_taxes.top_by_level` splits ``TOP``
per commodity into **producer-level** (325,829) and **trade-level** (391,096)
from an identity with nothing modelled in it -- excise sits in Producers' Value,
sales tax sits inside the margin columns.  That split is exactly the
producer-versus-seller distinction the market-share operator gets wrong, and its
trade-level total lands within **+2.2%** of the published wholesale-plus-retail
``T00TOP`` (391,096 against 382,491).  Applying it:

===============================================  ======  ==========
operator                                           corr    ``|error|``
===============================================  ======  ==========
market share on all ``TOP + MDTY``                 0.204     114.6%
+ level split, trade-level by trade output         0.743      41.9%
+ motor fuel routed to ``424700`` by name          0.946      29.9%
+ government columns zeroed, renormalised          0.948      27.9%
===============================================  ======  ==========

**Do we need to differentiate trade industries within wholesale and within
retail?**  Measured, the answer differs by block, and only one block needs it:

- **Non-trade industries -- no, and no matrix at all.**  They are 44.3% of the
  row, and once the producer-level portion is separated, plain market shares
  give **correlation 0.987** on them.  That block is solved.
- **Within retail -- no.**  Output shares (which for a trade industry are very
  nearly margin shares) give correlation 0.744 there.  Retail product tax is
  general sales tax: broad-based, roughly proportional to sales, HHI 0.137 with
  an effective 7.3 of 9 industries carrying it.
- **Within wholesale -- yes, and output shares are worse than useless there:
  correlation −0.192.**  ``424700`` petroleum wholesalers takes **51.3% of
  wholesale product tax on 3.4% of wholesale output**, a 15x concentration,
  because wholesale tax is dominated by motor fuel excise rather than by a
  broad-based tax.  HHI 0.321, an effective 3.1 of 10.

⚠️ **But wholesale does not need a commodity x trade-industry matrix either.**
It needs one named routing.  ``NAMED_TAX_LINES`` already carries motor fuel as
``324110``, and ``trade_level_share`` already says that commodity's tax is 99.8%
trade-level; sending that 98,842 to ``424700`` lands against a published 88,362
and takes the whole row from 0.743 to **0.946**.  With petroleum pulled out by
name, the remaining nine wholesale industries score 0.825 on output shares --
they behave like retail.

So the general commodity-by-trade-industry margin matrix that the PRO:PUR
producer-price work will eventually need is **not required here**.  The tax
conversion is served by the level split, which exists, plus a handful of
named-line routings, which are enumerated.  That is a seed worth giving Step 5,
and it is still a seed: 27.9% absolute error is not a target.

Government industries take no product tax, and the seed has to know it
---------------------------------------------------------------------

✅ **BEA books zero taxes on production to all ten government industry codes** --
``T00OTOP`` and ``T00TOP`` are both zero on every one of them in 2017, the single
exception being 538 of ``T00TOP`` on ``S00203``.  The columns are real
(``V00100`` and ``VABAS`` are populated), so the zero is an accounting rule
rather than a gap: a tax levied by government and remitted by a government
producer nets out.

⚠️ **The market-share leg violated that rule, and it was not a rounding-sized
violation.**  It seeded 10,513 onto those ten columns against a published 538 --
``S00202`` state and local electric utilities 3,439 against zero, ``GSLGE``
educational services 1,698 against zero, ``S00203`` 2,428 against 538.  The cause
is direct: government *does* produce taxed commodities, so market shares hand it
a share of the tax on them.

The fix is to drop those columns from the producer-level leg and **renormalise
each commodity over the producers that remain**, so the tax stays with its own
commodity instead of being deleted or smeared.  It moves 15,692 of error, and
two thirds of that is the excess itself:

============================================  ==========  ==========
industry                                       ``|err|``  after
============================================  ==========  ==========
``221100`` electric power                          3,934        443
``S00202`` S&L electric utilities                  3,439          0
``721000`` accommodation                           6,592      5,088
``GSLGE`` S&L educational services                 1,698          0
``S00203`` other S&L enterprises                   1,890        538
``622000`` hospitals                                 763        296
============================================  ==========  ==========

Read the first two rows together: ``S00202``'s electricity tax lands on private
``221100``, which the previous operator was *under*-attributing by almost exactly
that amount.  The money was not merely misplaced, it was misplaced in a
recoverable direction.  Only the producer-level leg needs the exclusion -- the
trade-level leg lands on wholesale and retail, where no government code sits, and
duties land on ``4200ID``.  No commodity is stranded: in 2017 no commodity that
is produced entirely by government carries any product tax, so nothing has to
fall back, and the seed total is unchanged to the dollar.

⚠️ **This one is worth fixing before the build rather than after.**  The other
residuals are misallocations the balance can pull back; this one puts tax on
columns that the Step 7 government-enterprise reallocation later redistributes
into private industries, so a wrong seed here propagates into work that would
have to be unpicked.

Construction needs nothing at all
---------------------------------

✅ **The construction block converts on plain market shares: correlation 1.000,
absolute error 31 = 1.7% of the block.**  Commodity ``TOP`` on the twelve
construction commodities is 1,907 against a published ``T00TOP`` of 1,857 on the
twelve construction industries, and ``MDTY`` and ``SUB`` are *zero* on both axes
-- there is no duties question and no subsidy question in this block.

It was worth probing because construction is block-shaped in the same way the
trade industries are, and could have been a second petroleum.  It is the
opposite, and the reason is the Make matrix.  BEA defines the construction
industries *by type of structure*, so the block is diagonal by construction:
94.5% of construction commodity output is made inside the block and **100.0% of
that is on the diagonal**.  There is no producer-versus-seller distinction to get
wrong, because whoever builds the structure sells it.  The level split agrees --
:func:`~bedrock.transform.iot.nowcast_product_taxes.top_by_level` puts 100% of
construction ``TOP`` at producer level and nothing at trade level -- so the
routings that rescue wholesale are inert here, and the three operators of the
progression above all give the same number.

The tax sits on three of the twelve codes, none of them a ``NAMED_TAX_LINES``
entry (all three ride the residual, frozen-2017-share block of Step 4d):

=============================================  =======  ==========  ======
commodity                                        ``TOP``  published    diff
=============================================  =======  ==========  ======
``2334A0`` other residential structures            962         962       0
``230301`` nonresidential maintenance and repair   628         605     -23
``230302`` residential maintenance and repair      317         290     -27
=============================================  =======  ==========  ======

The residual 1.7% is a *leak*, and market shares get its direction right and its
size roughly half.  5.5% of construction commodity output is own-account or
secondary work by non-construction industries -- ``531HST`` tenant-occupied
housing 20,279, state and local government 19,991 -- so both the published row
and the operator move a little maintenance-and-repair tax off the block:
published moves 50, market shares move 30.4, mostly to durable-goods wholesalers
and building-material retailers.  Inflow the other way is 2.5, in one cell.

Do the remaining sectors need the same probe?  No
-------------------------------------------------

⚠️ **The residual error is 20 industries, not 402.**  Under the best operator,
the top 20 industries by absolute error carry **85.0%** of it and the top 5 carry
37.7%; 17 of those 20 are wholesale or retail.  By block:

==================  ==========  =============  ====================
block                published  share of row   share of the error
==================  ==========  =============  ====================
wholesale              172,194          22.8%                 45.0%
retail                 210,297          27.8%                 36.2%
non-trade              334,447          44.3%                 18.8%
``4200ID`` customs      38,513           5.1%                  0.0%
==================  ==========  =============  ====================

So what is left is the *within-trade allocation* already characterised above,
plus four named non-trade structures -- ``721000`` accommodation (lodging tax,
-5,088), ``517210`` wireless (-4,310), ``611A00`` colleges (+2,759) and
``517110`` wired telecom (-1,550).  With the government columns excluded, the
non-trade block is down to correlation **0.992** and 18.8% of the error, and no
government code appears in the ranking at all.  Construction was the last
block-shaped unknown worth a sweep of its own, and it came back exact.  **Build
against this seed and repair the named twenty later**, rather than probing the
remaining sectors one by one: Step 5's balance moves these cells under soft
targets anyway, so seed accuracy below the block level is not what the build is
waiting on.

Usage::

    uv run python -m bedrock.analysis.nowcasting.tax_axis_conversion
    uv run python -m bedrock.analysis.nowcasting.tax_axis_conversion --check
"""

from __future__ import annotations

import argparse
import functools
import sys

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

YEAR = 2017

#: Wholesale and retail industry code prefixes.  ``4200ID`` is BEA's customs
#: duties code, which sits inside the trade block and is counted with it.
TRADE_PREFIXES = ('42', '44', '45', '4B')

#: The naive operator reproduces so little of ``T00TOP`` that any bar worth
#: stating is one it fails.  Correlation above this would mean the conversion is
#: worth seeding with; it is 0.20.
USABLE_CORRELATION = 0.80

#: The Supply and Use workbooks are in millions; ``nowcast_product_taxes``
#: returns USD, so its output is divided by this to meet them.
MILLION = 1e6

#: BEA's customs-duties industry code.  It takes the whole of ``MDTY``.
CUSTOMS_INDUSTRY = '4200ID'

#: Petroleum and petroleum products wholesalers -- 51.3% of wholesale product tax
#: on 3.4% of wholesale output, and the single reason within-wholesale
#: differentiation is needed at all.
PETROLEUM_WHOLESALERS = '424700'

#: Government industry codes.  BEA books **no taxes on production** to any of
#: them -- ``T00OTOP`` and ``T00TOP`` are both zero on all ten in 2017, the sole
#: exception being 538 of ``T00TOP`` on ``S00203`` -- because a tax levied by
#: government and remitted by a government producer nets out.  The columns are
#: real (``V00100`` and ``VABAS`` are populated), so the zero is a rule, not a
#: gap, and any operator that puts product tax here is wrong by construction.
GOVERNMENT_PREFIXES = ('S00', 'G')

#: Construction codes share a prefix and are the same twelve on both axes.
CONSTRUCTION_PREFIX = '23'

#: The construction block converts on market shares alone; anything above this
#: means the diagonal Make block has stopped doing the work.  Measured: 1.7%.
CONSTRUCTION_ERROR_BAR = 0.10

#: How many industries carry 80% of the best operator's remaining error.  It is
#: 20, which is why the remaining sectors get repaired by name rather than
#: probed one by one.
ERROR_CONCENTRATION_RANK = 20


@functools.cache
def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    """The 2017 Supply and Use tables.

    Cached: every scoring function here needs both, the report and ``--check``
    call them a dozen times over, and the workbooks are only ever read from.
    """
    return (
        _load_2017_detail_supply_use_usa('Supply_detail'),
        _load_2017_detail_supply_use_usa('Use_SUT_detail'),
    )


def government_industries() -> list[str]:
    """The ten industry codes BEA books no taxes on production to."""
    return [
        i for i in USA_2017_INDUSTRY_CODES if str(i).startswith(GOVERNMENT_PREFIXES)
    ]


def market_share_matrix(
    exclude_industries: 'list[str] | None' = None,
) -> pd.DataFrame:
    """``D[c, i]``: industry ``i``'s share of commodity ``c``'s domestic output.

    Normalised by ``T007`` (commodity output, basic, domestic) rather than by
    industry output -- that is the *market share* matrix, not the commodity mix.
    A commodity-indexed quantity has to be spread over the industries that make
    that commodity, which is this one; the commodity mix answers the transposed
    question and would give an unrelated number.

    ``exclude_industries`` drops columns and **renormalises each commodity's row
    over the producers that remain**, so the tax on a commodity stays with that
    commodity rather than being deleted or smeared economy-wide.  A commodity
    with no remaining producer would have nowhere to go, so its row is kept
    unrenormalised rather than losing the money; in 2017 no such row carries any
    tax, so the fallback never fires.
    """
    supply, _ = _frames()
    commodities, industries = (
        list(USA_2017_COMMODITY_CODES),
        list(USA_2017_INDUSTRY_CODES),
    )
    make = supply.loc[commodities, industries].astype(float)
    output = supply.loc[commodities, 'T007'].astype(float)
    shares = make.div(output.replace(0, np.nan), axis=0).fillna(0.0)
    if not exclude_industries:
        return shares

    keep = [i for i in industries if i not in set(exclude_industries)]
    retained = shares[keep].sum(axis=1)
    renormalised = pd.DataFrame(0.0, index=shares.index, columns=industries)
    renormalised[keep] = shares[keep].div(retained.where(retained > 0), axis=0)
    stranded = retained <= 0
    renormalised.loc[stranded] = shares.loc[stranded]
    return renormalised.fillna(0.0)


def convert_to_industry(by_commodity: 'pd.Series[float]') -> 'pd.Series[float]':
    """Spread a commodity-indexed series over industries by market share."""
    return market_share_matrix().mul(by_commodity, axis=0).sum(axis=0)


def published_row(use: pd.DataFrame, row: str) -> 'pd.Series[float]':
    """One Use SUT value-added row, over the 402 industries, as floats."""
    industries = list(USA_2017_INDUSTRY_CODES)
    series = use.loc[row]
    assert isinstance(series, pd.Series)
    return series.reindex(industries).astype(float).fillna(0.0)


def _score(
    estimate: 'pd.Series[float]', published: 'pd.Series[float]'
) -> dict[str, float]:
    industries = list(USA_2017_INDUSTRY_CODES)
    estimate = estimate.reindex(industries).astype(float).fillna(0.0)
    published = published.reindex(industries).astype(float).fillna(0.0)
    nonzero = (estimate.abs() + published.abs()) > 0
    absolute_error = float((estimate - published).abs().sum())
    return {
        'estimate_total': float(estimate.sum()),
        'published_total': float(published.sum()),
        'correlation': float(np.corrcoef(estimate[nonzero], published[nonzero])[0, 1]),
        'absolute_error': absolute_error,
        'error_share': absolute_error / float(published.abs().sum()),
    }


def conversion_scores() -> dict[str, dict[str, float]]:
    """Score the market-share conversion against the published Use rows."""
    supply, use = _frames()
    commodities = list(USA_2017_COMMODITY_CODES)
    taxes = supply.loc[commodities, 'TOP'].astype(float)
    duties = supply.loc[commodities, 'MDTY'].astype(float)
    # SUB is stored negative on the Supply table and positive on the Use table
    subsidies = -supply.loc[commodities, 'SUB'].astype(float)

    return {
        'T00TOP (TOP + MDTY)': _score(
            convert_to_industry(taxes + duties), published_row(use, 'T00TOP')
        ),
        'T00TOP (TOP only)': _score(
            convert_to_industry(taxes), published_row(use, 'T00TOP')
        ),
        'T00SUB': _score(convert_to_industry(subsidies), published_row(use, 'T00SUB')),
    }


def _industry_output() -> 'pd.Series[float]':
    """Industry output at basic value, the Supply table's ``T017`` row.

    Used as the within-trade weight.  For a trade industry output very nearly
    *is* margin -- eight retail commodities give up exactly 100% of ``T013`` and
    the ten wholesale ones 90.8-99.4% (`nowcast_trade_margins`) -- so this is
    the margin-proportional case, not a crude stand-in for it.
    """
    supply, _ = _frames()
    industries = list(USA_2017_INDUSTRY_CODES)
    series = supply.loc['T017']
    assert isinstance(series, pd.Series)
    return series.reindex(industries).astype(float).fillna(0.0)


def trade_groups() -> dict[str, list[str]]:
    """The industry blocks the conversion behaves differently on."""
    industries = list(USA_2017_INDUSTRY_CODES)
    wholesale = [
        c for c in industries if str(c).startswith('42') and c != CUSTOMS_INDUSTRY
    ]
    retail = [c for c in industries if str(c).startswith(('44', '45', '4B'))]
    return {
        'wholesale': wholesale,
        'retail': retail,
        'non-trade': [
            c for c in industries if c not in wholesale + retail + [CUSTOMS_INDUSTRY]
        ],
    }


def level_split_estimates() -> dict[str, 'pd.Series[float]']:
    """The four operators of the docstring's progression, by industry.

    ``producer-level`` tax goes by market share -- that part the Make matrix has
    always had right.  ``trade-level`` goes to the trade industries, either
    spread by their output or with motor fuel routed to ``424700`` by name
    first.  Duties go to ``4200ID``.  The last step drops the government columns
    the market-share leg should never have filled and renormalises each
    commodity over its remaining producers.
    """
    from bedrock.transform.iot.nowcast_product_taxes import (  # noqa: PLC0415
        NAMED_TAX_LINES,
        top_by_level,
    )

    supply, _ = _frames()
    commodities = list(USA_2017_COMMODITY_CODES)
    industries = list(USA_2017_INDUSTRY_CODES)
    duties = supply.loc[commodities, 'MDTY'].astype(float)
    taxes = supply.loc[commodities, 'TOP'].astype(float)

    levels = top_by_level(YEAR) / MILLION
    producer = levels['producer_level'].reindex(commodities).fillna(0.0)
    trade = levels['trade_level'].reindex(commodities).fillna(0.0)

    groups = trade_groups()
    trade_industries = groups['wholesale'] + groups['retail']
    output = _industry_output()

    fuel = [c for c in NAMED_TAX_LINES['motor fuel'][1] if c in trade.index]
    fuel_tax = float(trade.loc[fuel].sum())
    others = [c for c in trade_industries if c != PETROLEUM_WHOLESALERS]

    def producer_leg(shares: pd.DataFrame) -> 'pd.Series[float]':
        """Producer-level tax by the given shares, plus duties on ``4200ID``."""
        leg = shares.mul(producer, axis=0).sum(axis=0).reindex(industries).fillna(0.0)
        leg[CUSTOMS_INDUSTRY] = leg.get(CUSTOMS_INDUSTRY, 0.0) + float(duties.sum())
        return leg

    def with_named_fuel(leg: 'pd.Series[float]') -> 'pd.Series[float]':
        """Add the trade-level leg, motor fuel routed to ``424700`` by name."""
        estimate = leg.copy()
        estimate[PETROLEUM_WHOLESALERS] += fuel_tax
        weight = output[others].clip(lower=0)
        estimate[others] += (float(trade.sum()) - fuel_tax) * (weight / weight.sum())
        return estimate

    base = producer_leg(market_share_matrix())

    by_output = base.copy()
    weight = output[trade_industries].clip(lower=0)
    by_output[trade_industries] += float(trade.sum()) * (weight / weight.sum())

    # Government industries take no product tax, so they take no share of the
    # producer-level leg either.  Only that leg needs the exclusion -- the
    # trade-level leg lands on wholesale and retail, where no government code
    # sits, and duties land on 4200ID.
    without_government = producer_leg(
        market_share_matrix(exclude_industries=government_industries())
    )

    return {
        'market share on all TOP + MDTY': convert_to_industry(taxes + duties)
        .reindex(industries)
        .fillna(0.0),
        '+ level split, trade-level by trade output': by_output,
        '+ motor fuel routed to 424700 by name': with_named_fuel(base),
        '+ government columns zeroed, renormalised': with_named_fuel(
            without_government
        ),
    }


def construction_codes() -> list[str]:
    """The twelve construction codes, identical on the commodity and industry axes."""
    commodities = [
        c for c in USA_2017_COMMODITY_CODES if str(c).startswith(CONSTRUCTION_PREFIX)
    ]
    industries = [
        i for i in USA_2017_INDUSTRY_CODES if str(i).startswith(CONSTRUCTION_PREFIX)
    ]
    assert set(commodities) == set(industries), 'construction axes have diverged'
    return commodities


def construction_scores() -> dict[str, float]:
    """How closely the two axes agree on construction, and why they can.

    Kept out of :func:`trade_groups` deliberately -- construction sits inside
    that function's ``non-trade`` block, and at 0.25% of the row it does not move
    the 0.987 measured there.  This is the block read on its own terms.
    """
    supply, use = _frames()
    commodities = list(USA_2017_COMMODITY_CODES)
    codes = construction_codes()

    top = supply.loc[commodities, 'TOP'].astype(float).reindex(codes)
    duties = supply.loc[commodities, 'MDTY'].astype(float).reindex(codes)
    subsidies = -supply.loc[commodities, 'SUB'].astype(float).reindex(codes)
    published = published_row(use, 'T00TOP').reindex(codes).fillna(0.0)
    estimate = (
        convert_to_industry(supply.loc[commodities, 'TOP'].astype(float))
        .reindex(codes)
        .fillna(0.0)
    )

    # Diagonality is the reason the conversion works: each construction
    # commodity is made by the industry of the same name, so market shares have
    # no producer-versus-seller decision to get wrong.
    make = supply.loc[commodities, list(USA_2017_INDUSTRY_CODES)].astype(float)
    block = make.loc[codes, codes]
    in_block = float(block.to_numpy().sum())
    absolute_error = float((estimate - published).abs().sum())

    return {
        'commodity_top': float(top.sum()),
        'commodity_mdty': float(duties.sum()),
        'commodity_sub': float(subsidies.sum()),
        'published_top': float(published.sum()),
        'estimate_top': float(estimate.sum()),
        'correlation': float(np.corrcoef(estimate, published)[0, 1]),
        'absolute_error': absolute_error,
        'error_share': absolute_error / float(published.sum()),
        'in_block_share': in_block / float(make.loc[codes].to_numpy().sum()),
        'diagonal_share': float(np.diag(block.to_numpy()).sum()) / in_block,
        'taxed_codes': float((top != 0).sum()),
    }


def government_scores() -> dict[str, float]:
    """The government rule, and what enforcing it is worth.

    ``published`` is what BEA books to the ten government industries -- 538, all
    of it on ``S00203``.  ``seeded_before`` is what the market-share leg gave
    them unaided.
    """
    _, use = _frames()
    published = published_row(use, 'T00TOP')
    other_taxes = published_row(use, 'T00OTOP')
    estimates = list(level_split_estimates().values())
    government = government_industries()
    before, after = estimates[-2], estimates[-1]
    return {
        'industries': float(len(government)),
        'published': float(published[government].sum()),
        'published_other_taxes': float(other_taxes[government].sum()),
        'seeded_before': float(before[government].sum()),
        'seeded_after': float(after[government].sum()),
        'error_before': float((before - published).abs().sum()),
        'error_after': float((after - published).abs().sum()),
        'total_before': float(before.sum()),
        'total_after': float(after.sum()),
    }


def error_concentration() -> dict[str, float]:
    """How few industries carry the best operator's remaining error.

    The answer to "do the other sectors each need their own probe": no, because
    the residual is not spread over 402 of them.
    """
    _, use = _frames()
    published = published_row(use, 'T00TOP')
    best = list(level_split_estimates().values())[-1]
    error = (best - published).abs().sort_values(ascending=False)
    total = float(error.sum())
    trade = set(trade_groups()['wholesale'] + trade_groups()['retail'])
    ranked = list(error.index[:ERROR_CONCENTRATION_RANK])
    return {
        'total_error': total,
        'top_5_share': float(error.iloc[:5].sum()) / total,
        f'top_{ERROR_CONCENTRATION_RANK}_share': float(
            error.iloc[:ERROR_CONCENTRATION_RANK].sum()
        )
        / total,
        'trade_in_top_rank': float(sum(1 for i in ranked if i in trade)),
    }


def trade_concentration() -> dict[str, float]:
    """How much of the published ``T00TOP`` row sits in trade industries."""
    _, use = _frames()
    published = published_row(use, 'T00TOP')
    trade = [c for c in published.index if str(c).startswith(TRADE_PREFIXES)]
    return {
        'total': float(published.sum()),
        'trade': float(published[trade].sum()),
        'trade_share': float(published[trade].sum() / published.sum()),
    }


def duties_lookup() -> dict[str, float]:
    """``4200ID`` carries the whole of ``MDTY``, exactly."""
    supply, use = _frames()
    commodities = list(USA_2017_COMMODITY_CODES)
    return {
        'supply_mdty': float(supply.loc[commodities, 'MDTY'].astype(float).sum()),
        'use_4200ID': float(published_row(use, 'T00TOP')['4200ID']),
    }


def report() -> None:
    print(f'Commodity -> industry conversion of the product-tax rows, {YEAR}\n')
    print(f"{'row':<22}{'corr':>7}{'|error|':>14}{'of row':>9}")
    for name, s in conversion_scores().items():
        print(
            f"{name:<22}{s['correlation']:>7.3f}"
            f"{s['absolute_error']:>14,.0f}{s['error_share']:>9.1%}"
        )
    concentration = trade_concentration()
    print(
        f"\npublished T00TOP in trade industries: "
        f"{concentration['trade']:,.0f} of {concentration['total']:,.0f} "
        f"= {concentration['trade_share']:.1%}"
    )
    duties = duties_lookup()
    print(
        f"import duties: Supply MDTY {duties['supply_mdty']:,.0f} vs "
        f"Use T00TOP on 4200ID {duties['use_4200ID']:,.0f}"
    )

    _, use = _frames()
    published = published_row(use, 'T00TOP')
    print()
    print(f"{'operator':<46}{'corr':>7}{'|error|':>13}{'of row':>9}")
    for name, estimate in level_split_estimates().items():
        error = float((estimate - published).abs().sum())
        correlation = float(np.corrcoef(estimate, published)[0, 1])
        print(
            f'{name:<46}{correlation:>7.3f}{error:>13,.0f}'
            f'{error / float(published.sum()):>9.1%}'
        )

    print()
    print(f"{'within-group, best operator':<46}{'corr':>7}{'|error|':>13}{'of err':>9}")
    best = list(level_split_estimates().values())[-1]
    total_error = float((best - published).abs().sum())
    for group, codes in trade_groups().items():
        error = float((best[codes] - published[codes]).abs().sum())
        correlation = float(np.corrcoef(best[codes], published[codes])[0, 1])
        print(
            f'{group:<46}{correlation:>7.3f}{error:>13,.0f}'
            f'{error / total_error:>9.1%}'
        )

    construction = construction_scores()
    print()
    print(
        f"construction: commodity TOP {construction['commodity_top']:,.0f} vs "
        f"published {construction['published_top']:,.0f}, market shares give "
        f"corr {construction['correlation']:.3f} and "
        f"|error| {construction['absolute_error']:,.0f} "
        f"= {construction['error_share']:.1%}"
    )
    print(
        f"  MDTY {construction['commodity_mdty']:,.0f}, "
        f"SUB {construction['commodity_sub']:,.0f}, "
        f"taxed on {construction['taxed_codes']:.0f} of "
        f'{len(construction_codes())} codes; Make block is '
        f"{construction['in_block_share']:.1%} in-block and "
        f"{construction['diagonal_share']:.1%} diagonal"
    )

    government = government_scores()
    print(
        f"  government columns: published "
        f"{government['published']:,.0f} across "
        f"{government['industries']:.0f} industries, seeded "
        f"{government['seeded_before']:,.0f} before the exclusion and "
        f"{government['seeded_after']:,.0f} after; "
        f"row error {government['error_before']:,.0f} -> "
        f"{government['error_after']:,.0f}"
    )

    concentration = error_concentration()
    print()
    print(
        f'remaining error is concentrated: top 5 industries carry '
        f"{concentration['top_5_share']:.1%}, top {ERROR_CONCENTRATION_RANK} carry "
        f"{concentration[f'top_{ERROR_CONCENTRATION_RANK}_share']:.1%}, of which "
        f"{concentration['trade_in_top_rank']:.0f} are trade industries"
    )


def check() -> int:
    """Assert the findings this module's docstring rests on.

    Analysis modules here carry their checks as a CLI flag rather than as unit
    tests.  The point is not to protect the conversion -- it is unusable -- but
    to fail if a later Supply or Use vintage makes the *argument* untrue, since
    Step 2 and Step 5 both lean on it.
    """
    failures = []
    scores = conversion_scores()
    for name, s in scores.items():
        if s['correlation'] >= USABLE_CORRELATION:
            failures.append(
                f'{name}: correlation {s["correlation"]:.3f} is now usable '
                f'(>= {USABLE_CORRELATION}); revisit the decision to leave the '
                f'industry split to the balance'
            )
    if scores['T00TOP (TOP + MDTY)']['error_share'] < 0.5:
        failures.append('T00TOP conversion error has fallen below 50% of the row')

    concentration = trade_concentration()
    if concentration['trade_share'] < 0.4:
        failures.append(
            f'trade industries now hold only '
            f'{concentration["trade_share"]:.1%} of T00TOP; the point-of-sale '
            f'argument no longer holds'
        )

    duties = duties_lookup()
    if abs(duties['supply_mdty'] - duties['use_4200ID']) > 25:
        failures.append(
            f'4200ID {duties["use_4200ID"]:,.0f} no longer equals Supply MDTY '
            f'{duties["supply_mdty"]:,.0f}; the duties lookup is not exact'
        )

    # The seed's argument: the level split does the work, and within-wholesale
    # resolution is needed for exactly one industry.
    _, use = _frames()
    published = published_row(use, 'T00TOP')
    estimates = level_split_estimates()
    best = list(estimates.values())[-1]
    if float(np.corrcoef(best, published)[0, 1]) < USABLE_CORRELATION:
        failures.append(
            'the level-split operator no longer reaches a usable correlation; '
            'the seed proposed for Step 5 does not hold'
        )
    wholesale = trade_groups()['wholesale']
    if PETROLEUM_WHOLESALERS not in wholesale:
        failures.append(f'{PETROLEUM_WHOLESALERS} is no longer a wholesale code')
    else:
        share = float(published[PETROLEUM_WHOLESALERS] / published[wholesale].sum())
        if share < 0.3:
            failures.append(
                f'{PETROLEUM_WHOLESALERS} now holds {share:.1%} of wholesale '
                f'T00TOP; within-wholesale resolution may no longer reduce to '
                f'one named routing'
            )

    # Construction: the block that needs no operator beyond market shares.
    construction = construction_scores()
    if construction['correlation'] < USABLE_CORRELATION:
        failures.append(
            f'construction now converts at correlation '
            f'{construction["correlation"]:.3f}; the diagonal Make block no '
            f'longer carries the conversion on its own'
        )
    if construction['error_share'] > CONSTRUCTION_ERROR_BAR:
        failures.append(
            f'construction conversion error is now '
            f'{construction["error_share"]:.1%} of the block, above '
            f'{CONSTRUCTION_ERROR_BAR:.0%}'
        )
    if construction['diagonal_share'] < 0.999:
        failures.append(
            f'the construction Make block is only '
            f'{construction["diagonal_share"]:.2%} diagonal; the argument that '
            f'construction has no producer-versus-seller problem rests on it'
        )
    if construction['commodity_mdty'] or construction['commodity_sub']:
        failures.append(
            f'construction now carries MDTY '
            f'{construction["commodity_mdty"]:,.0f} or SUB '
            f'{construction["commodity_sub"]:,.0f}; it is no longer a TOP-only block'
        )

    # Government industries take no product tax.  This is the rule the seed
    # was violating, and the rule itself is what has to keep holding.
    government = government_scores()
    if government['published'] > 1_000 or government['published_other_taxes']:
        failures.append(
            f'government industries now carry T00TOP '
            f'{government["published"]:,.0f} / T00OTOP '
            f'{government["published_other_taxes"]:,.0f}; BEA no longer books '
            f'zero taxes on production to them and the exclusion is wrong'
        )
    if government['seeded_after'] > 1.0:
        failures.append(
            f'the seed still puts {government["seeded_after"]:,.1f} of product '
            f'tax on government industries; the exclusion is not taking effect'
        )
    if government['error_after'] >= government['error_before']:
        failures.append(
            f'excluding government columns no longer reduces the row error '
            f'({government["error_before"]:,.0f} -> '
            f'{government["error_after"]:,.0f})'
        )
    if abs(government['total_after'] - government['total_before']) > 1.0:
        failures.append(
            f'the exclusion changed the seed total from '
            f'{government["total_before"]:,.0f} to '
            f'{government["total_after"]:,.0f}; renormalisation should move '
            f'money between industries, never create or destroy it'
        )

    # The reason the remaining sectors are repaired by name, not swept.
    concentration = error_concentration()
    if concentration[f'top_{ERROR_CONCENTRATION_RANK}_share'] < 0.6:
        failures.append(
            f'the top {ERROR_CONCENTRATION_RANK} industries now carry only '
            f'{concentration[f"top_{ERROR_CONCENTRATION_RANK}_share"]:.1%} of the '
            f'error; it has spread, and a broader sector sweep may be needed '
            f'after all'
        )

    for failure in failures:
        print(f'FAIL: {failure}')
    if not failures:
        print('OK: all findings hold')
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the findings rather than printing the report',
    )
    args = parser.parse_args()
    if args.check:
        return check()
    report()
    return 0


if __name__ == '__main__':
    sys.exit(main())
