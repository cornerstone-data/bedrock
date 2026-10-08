"""2017-anchored margin structure for the nowcast Margins dataset.

Step 4c of the nowcast build
(``bedrock/analysis/nowcasting/margins_estimation_plan.md``), phase 1
(`#610 <https://github.com/cornerstone-data/bedrock/issues/610>`_). What this
module produces is the *structure* the later phases carry forward: the three
receiving sets, a rate per (buyer, commodity, margin type), and the aggregation
back to the Supply table's margin columns. It sources nothing annual - phases 2
and 3 supply the annual levels, and phase 4 (#613) applies these rates to a
nowcast base.

**The object.** BEA's published Margins table is one row per (buyer, commodity)
with columns ``Producers' Value``, ``Transportation``, ``Wholesale``,
``Retail``, ``Purchasers' Value``. The buyer side spans industries *and*
final-demand codes, so a good bought by a household and the same good bought as
an intermediate input are separate rows - which is the point, since their retail
rates differ by more than 2x.

**Before redefinitions.** The margins anchor is the before-redefinitions table,
matching the MUT before-redefinitions tables and therefore the SUT: everything
upstream of Step 7 stays before redefinitions, and the published Supply table
this aggregates against is a before-redefinitions construct too. The
after-redefinitions table is the one ``derive_PRO_to_PUR_ratio`` reaches for via
``USAConfig`` - a different object for a different purpose.

**Cascading bases.** The margin rates are not shares of one common denominator.
Per the BEA IO manual (2009) chapter 8, margins stack: transportation applies to
producers' value, wholesale to producers' value plus transportation, retail to
all three. :data:`MARGIN_BASE_COLUMNS` encodes that order.

**A rate above 1 is not an error.** Margin is *added* to basic value
(``T016 = T013 + T014 + T015``), not carved out of it, so a rate on any of these
bases is unbounded - ``S00402`` used and secondhand goods runs to 16x its basic
value because used goods have no production. Bound-check against ``T016``
instead; see ``margins_2017_baseline.py``.

**Two anchors, and the final-demand one is BEA's.** A rate needs a side of the
valuation to sit on. Industry buyers keep :data:`PRODUCER_ANCHOR` - the
intermediate block is built at producer prices, so there is no independent
purchaser value to anchor on. Final-demand buyers take
:data:`PURCHASER_ANCHOR`, which is what BEA's own process does: *"we treat the
initial purchaser valuation as fixed and so we subtract off the distributed
margins and transportation to calculate a residual basic value"* (2026-09-04,
``analysis/nowcasting/bea_correspondence.md``). Graded 2017 -> 2012 on the
transactions it anchors, that cuts gross margin error from 714.5 to **408.7
billion USD** and halves the systematic over-prediction. See
:func:`purchaser_anchored_shares`.

**Two treatments, not one.** Most receiving transactions carry a *rate*. Two
kinds carry a *level* instead (:data:`LEVEL_BASIS`):

- ``F03000`` change in private inventories. Every negative margin in the 2017
  table is an ``F03000`` row: margin is booked when inventories build, so a
  drawdown carries the offsetting negative. That is a timing correction, not a
  rate, and dividing it by a change-in-inventories base produces a number that
  means nothing. Note this is the signal ``_margin_negatives_treatment``'s
  ``abs_negative_margin_columns`` flag destroys - do not route this table
  through it.
- Transactions whose cascading base is zero or negative, where a rate is
  undefined. 13 of them in 2017 - seven retail cells of exactly 1 million over a
  zero base, and six over a negative one, of which ``F02E00`` buying ``S00402``
  is 18.5 billion of the 18.5 billion involved.
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import load_benchmark_margins_before_redef_usa
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_BENCHMARK_DETAIL_SUT_YEARS,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

#: The three margin types, in the order they cascade.
MARGIN_TYPES: tuple[str, ...] = ('Transportation', 'Wholesale', 'Retail')

#: The base each margin type's rate is expressed on (BEA IO manual 2009, ch. 8).
#: Margins stack, so each type's base includes the ones applied before it.
MARGIN_BASE_COLUMNS: dict[str, tuple[str, ...]] = {
    'Transportation': ("Producers' Value",),
    'Wholesale': ("Producers' Value", 'Transportation'),
    'Retail': ("Producers' Value", 'Transportation', 'Wholesale'),
}

#: Index level names of the published Margins table.
BUYER_LEVEL = 'Industry Code'
COMMODITY_LEVEL = 'Commodity Code'
MARGIN_TYPE_LEVEL = 'margin_type'

#: Change in private inventories - carried as a level, never fitted as a rate.
INVENTORY_BUYER_CODE = 'F03000'

RATE_BASIS = 'rate'
LEVEL_BASIS = 'level'

#: Which side of the valuation a row's rate is anchored on.
PRODUCER_ANCHOR = 'producers'
PURCHASER_ANCHOR = 'purchasers'

#: The margins table's purchaser column.
PURCHASER_COLUMN = "Purchasers' Value"


def load_margins_transactions_2017() -> pd.DataFrame:
    """The 2017 margins transactions. See :func:`load_margins_transactions`."""
    return load_margins_transactions(2017)


@functools.cache
def load_margins_transactions(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> pd.DataFrame:
    """
    Published detail Margins table for a benchmark year (2007/2012/2017),
    before redefinitions, restricted to real commodities. USD.

    The workbook carries the value-added rows (``V00100``, ``V00200``,
    ``V00300``) in the commodity column as well, which the transaction object
    has no use for - they hold producers' value only and never any margin.
    """
    df = load_benchmark_margins_before_redef_usa(year)
    commodities = df.index.get_level_values(COMMODITY_LEVEL)
    return df.loc[commodities.isin(USA_2017_COMMODITY_CODES)]


def _margins_or_default(margins: pd.DataFrame | None) -> pd.DataFrame:
    return load_margins_transactions_2017() if margins is None else margins


def margin_bases(margins: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    The cascading base of each margin type, per transaction. USD.

    One column per :data:`MARGIN_TYPES`, on the same index as *margins*.
    """
    df = _margins_or_default(margins)
    return pd.DataFrame(
        {
            margin_type: df[list(base_columns)].sum(axis=1)
            for margin_type, base_columns in MARGIN_BASE_COLUMNS.items()
        },
        index=df.index,
    )


def margin_receiving_sets(
    margins: pd.DataFrame | None = None,
) -> dict[str, pd.MultiIndex]:
    """
    The (buyer, commodity) transactions that bear each margin, per margin type.

    A transaction receives a margin exactly when the published value is
    non-zero, which is BEA's own definition of the receiving set - the wholesale
    and retail steps distribute to a hand-selected set of transactions, and the
    zeros are that selection, not missing data. Retail's set is 8x sparser than
    wholesale's, as the manual's "only those transactions that move through
    retail establishments" implies.
    """
    df = _margins_or_default(margins)
    return {
        margin_type: pd.MultiIndex.from_frame(
            df.index[df[margin_type] != 0].to_frame(index=False)
        )
        for margin_type in MARGIN_TYPES
    }


def margin_rate_table(margins: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    One row per (buyer, commodity, margin type) that receives margin, with the
    rate or the level to carry it forward by.

    Index: ``Industry Code`` x ``Commodity Code`` x ``margin_type``. Columns:

    ``margin``
        the published margin, USD.
    ``base``
        its cascading base (:data:`MARGIN_BASE_COLUMNS`), USD.
    ``rate``
        ``margin / base``, or NaN where the row is carried as a level.
    ``basis``
        :data:`RATE_BASIS` or :data:`LEVEL_BASIS` - see the module docstring for
        why two treatments are needed.
    ``base_share``, ``margin_share``
        the row's share of its (commodity, margin type) group, over the
        rate-basis rows only, so each sums to 1 within a group. A nowcast base
        arrives per commodity (``T013``, from 4a and 4b) and has to be split
        across that commodity's receiving transactions before a per-transaction
        rate can be applied; these are the two candidate splits, and keeping
        both here is what makes the rate structure usable by #613 without going
        back to the published table.

    Rates are deliberately *not* collapsed to one per commodity. BEA computes
    one rate per *item*, uniform across the transactions receiving it, but the
    item is finer than the published commodity and is not recoverable: zero of
    the published commodities have a uniform rate on either trade margin.
    """
    df = _margins_or_default(margins)
    bases = margin_bases(df)
    buyers = df.index.get_level_values(BUYER_LEVEL)

    frames = []
    for margin_type in MARGIN_TYPES:
        receiving = df[margin_type] != 0
        frame = pd.DataFrame(
            {
                'margin': df.loc[receiving, margin_type],
                'base': bases.loc[receiving, margin_type],
            }
        )
        frame[MARGIN_TYPE_LEVEL] = margin_type
        frame['basis'] = np.where(
            (buyers[receiving] == INVENTORY_BUYER_CODE) | (frame['base'] <= 0),
            LEVEL_BASIS,
            RATE_BASIS,
        )
        frames.append(frame.set_index(MARGIN_TYPE_LEVEL, append=True))

    out = pd.concat(frames).sort_index()
    is_rate = out['basis'] == RATE_BASIS
    out['rate'] = (out['margin'] / out['base']).where(is_rate)

    group = [COMMODITY_LEVEL, MARGIN_TYPE_LEVEL]
    for column, share in (('base', 'base_share'), ('margin', 'margin_share')):
        totals = out[column].where(is_rate).groupby(level=group).transform('sum')
        out[share] = (out[column] / totals.replace(0.0, np.nan)).where(is_rate)

    return out[['margin', 'base', 'rate', 'basis', 'base_share', 'margin_share']]


def final_demand_buyers(margins: pd.DataFrame | None = None) -> pd.Index:
    """The final-demand buyer codes in the margins table.

    ⚠️ Asserted rather than assumed: no BEA 2017 *industry* code begins with
    ``F``, so the prefix separates the twenty final-demand buyers cleanly. If
    that ever stops being true the assert fires rather than silently
    reclassifying an industry.
    """
    buyers = _margins_or_default(margins).index.get_level_values(BUYER_LEVEL)
    unique = pd.Index(buyers.astype(str).unique())
    industries = {c for c in USA_2017_COMMODITY_CODES if str(c).startswith('F')}
    assert not industries, (
        f'{sorted(industries)} start with F, so the final-demand prefix no '
        f'longer identifies buyers'
    )
    return unique[unique.str.startswith('F')]


def purchaser_anchored_shares(
    margins: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Each final-demand transaction's margin as a share of its purchaser value.

    The parameter BEA's process actually holds: *"we treat the initial
    purchaser valuation as fixed and so we subtract off the distributed margins
    and transportation to calculate a residual basic value"* (2026-09-04, see
    ``analysis/nowcasting/bea_correspondence.md``).

    ✅ **Graded 2017 -> 2012 this beats the producer anchor by 40%.** Applying
    2017 parameters to the 2012 base and scoring on published 2012 margins over
    the 7,630 transactions usable in both benchmark years, gross margin error is
    **286.8 bn USD** anchored on the purchaser value against **476.1 bn**
    anchored on producers', and the systematic over-prediction halves from
    +392.3 bn to +196.8 bn. The derived side improves too: the purchaser anchor
    leaves 232.3 bn of basic-value error where the producer anchor leaves 444.0
    bn of purchaser-value error.

    The reason is stability, not accounting. Value-weighted across those rows,
    total margin **as a share of purchaser value** moves 0.2775 -> 0.3148
    between 2012 and 2017, a ratio of **1.134**; as a **rate on basic value** it
    moves 0.3840 -> 0.4870, a ratio of **1.268**. A margin is a wedge *inside*
    the purchaser price, so its share of that price is about twice as stable as
    its rate on the residual.

    ⚠️ **Final demand only, and that is where the value is.** 81% of the gain
    (153.9 of 189.3 bn) sits on final-demand buyers, which carry 2,836 bn of the
    4,071 bn of margin on twenty buyer codes - and which is the one block where
    we hold an independent purchaser value, from ``derive_initial_Y_pur``. The
    intermediate block is built at producer prices, so there is no purchaser
    value to anchor on and those rows keep :data:`PRODUCER_ANCHOR`.

    ❌ **Not the cascading-base question**, which is empty: the cascade
    telescopes and is algebraically identical to distributing all types
    simultaneously on basic value. This is the *direction of the anchor*, which
    is a real difference.

    Rows carried as levels keep that treatment - ``F03000`` change in
    inventories is a final-demand buyer, but a share of a *change* means
    nothing, the same reason it is not carried as a rate.
    """
    df = _margins_or_default(margins)
    table = margin_rate_table(df)
    buyers = table.index.get_level_values(BUYER_LEVEL).astype(str)
    purchaser = pd.Series(
        df[PURCHASER_COLUMN]
        .reindex(table.index.droplevel(MARGIN_TYPE_LEVEL))
        .to_numpy(),
        index=table.index,
        dtype=float,
    )
    eligible = (
        pd.Series(buyers.isin(final_demand_buyers(df)), index=table.index)
        & table['basis'].eq(RATE_BASIS)
        & purchaser.gt(0.0)
    )
    out = table.copy()
    out[PURCHASER_COLUMN] = purchaser
    out['anchor'] = np.where(eligible, PURCHASER_ANCHOR, PRODUCER_ANCHOR)
    out['purchaser_share'] = (out['margin'] / purchaser.where(purchaser > 0)).where(
        eligible
    )
    return out


def purchaser_anchored_final_demand(
    purchaser: pd.DataFrame,
    margins: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    """Basic value and margins for a purchaser-valued final-demand block.

    The construction BEA describes, run forwards: hold the purchaser value
    fixed, distribute the margins as shares of it, and take basic value as the
    residual - ``PUR - (Margin+TC) = BAS``.

    :param purchaser: commodity x final-demand-code, purchaser-valued, USD.
        ``derive_initial_Y_pur`` produces exactly this.
    :returns: ``(basic, margins_by_type)`` - the residual basic-value block on
        the same axes, and one commodity x buyer frame per margin type.

    ⚠️ **A share is only carried where 2017 had one.** A (buyer, commodity)
    cell with no published 2017 margin gets none here: the receiving set is
    BEA's own selection, not missing data, and inventing a margin on a cell BEA
    left empty is not something this parameterisation can justify. Those cells
    pass through with basic value equal to purchaser value.

    ⚠️ **Basic value can be driven negative if the shares overshoot**, which is
    the one risk the producer anchor does not have. On the 2012 holdout it
    happens on **0 of 7,630** transactions, but the guard is here rather than
    trusted to stay at zero - see :func:`negative_basic_value`.
    """
    shares = purchaser_anchored_shares(margins)
    shares = shares[shares['anchor'] == PURCHASER_ANCHOR]

    out: dict[str, pd.DataFrame] = {}
    total = pd.DataFrame(0.0, index=purchaser.index, columns=purchaser.columns)
    for margin_type in MARGIN_TYPES:
        share = (
            shares.xs(margin_type, level=MARGIN_TYPE_LEVEL)['purchaser_share']
            .unstack(BUYER_LEVEL)
            .reindex(index=purchaser.index, columns=purchaser.columns)
            .fillna(0.0)
        )
        booked = share * purchaser
        out[margin_type] = booked
        total = total + booked
    return purchaser - total, out


def negative_basic_value(basic: pd.DataFrame) -> pd.DataFrame:
    """The cells a purchaser-anchored split drove below zero.

    Empty is the expected result. A non-empty frame means the 2017 shares
    overshot that cell's purchaser value in the nowcast year, which is a signal
    about the share, not a cell to clip.
    """
    stacked = pd.Series(basic.stack(), dtype=float)
    return stacked[stacked.lt(0.0)].to_frame(name='basic_value')


def margin_levels(margins: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    The receiving transactions carried as levels rather than rates.

    The ``F03000`` inventory rows and the handful with a non-positive base, in
    the same shape as :func:`margin_rate_table`. Their margin moves with a
    price index, not with a base.
    """
    table = margin_rate_table(margins)
    return table.loc[table['basis'] == LEVEL_BASIS]


def margins_by_commodity(margins: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    The two Supply-table margin aggregates, per commodity. USD.

    ``trade_margins``
        ``sum_buyers (Wholesale + Retail)``, which equals Supply
        ``TRADE + TOP`` - **not** ``TRADE``. The Margins table is on the
        make-use framework and the Supply table on supply-use; wholesale and
        retail trade commodity tax sits inside the margin columns of the former
        and in the taxes-on-products column of the latter (B. Jolliff, BEA,
        2025-05-30). Dropping the tax term overstates ``TRADE`` by 11.8%.
    ``transport_margins``
        ``sum_buyers Transportation``, which equals Supply ``TRANS`` directly -
        no tax term, since transportation is not a trade margin.
    ``margin_given_up``
        the other side of the same money:
        ``sum_buyers (Producers' + Transportation + Wholesale + Retail -
        Purchasers')``. The published ``Purchasers' Value`` is *not* the sum of
        the four components on a trade or transport commodity's own rows - PCE
        buys 254 billion of ``441000`` motor vehicle dealers at producers' value
        and zero at purchasers' value, because that margin has been moved onto
        the goods. The difference is what the commodity gave up, and it lands
        entirely on the 24 commodities whose Supply margin column is negative:
        19 trade and 5 transport. Against ``-TRANS`` it reproduces all five
        transport commodities within 0.2%; against ``-TRADE`` it runs 1.0x to
        2.2x high, and the excess is trade-level tax, the same term the trade
        identity carries on the receiving side with the opposite sign.

    Reindexed to all 402 commodities, so a commodity that bears no margin
    appears as zero rather than dropping out.

    Validate these per commodity and never in aggregate: ``T014`` nets to about
    1 economy-wide against 7.4 trillion of gross mass, because a trade margin is
    added to the good and subtracted from the trade commodity that earned it. A
    totals check here passes on anything.
    """
    df = _margins_or_default(margins)
    given_up = (
        df[list(MARGIN_TYPES) + ["Producers' Value"]].sum(axis=1)
        - df["Purchasers' Value"]
    )
    by_commodity = (
        df.assign(margin_given_up=given_up)
        .groupby(level=COMMODITY_LEVEL)[list(MARGIN_TYPES) + ['margin_given_up']]
        .sum()
        .reindex(list(USA_2017_COMMODITY_CODES), fill_value=0.0)
    )
    out = pd.DataFrame(
        {
            'trade_margins': by_commodity['Wholesale'] + by_commodity['Retail'],
            'transport_margins': by_commodity['Transportation'],
            'margin_given_up': by_commodity['margin_given_up'],
        }
    )
    out.index.name = COMMODITY_LEVEL
    return out


def receiving_set_summary(margins: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Shape of each receiving set: how many transactions, buyers and commodities
    bear the margin, how much value it carries, and how it splits between the
    two treatments.

    ``transaction_share`` is against every row of the table, receiving or not,
    which is the figure that says how differently the three margins behave -
    wholesale reaches 30% of transactions, retail 4%.
    """
    df = _margins_or_default(margins)
    table = margin_rate_table(df)
    rows = []
    for margin_type in MARGIN_TYPES:
        received = table.xs(margin_type, level=MARGIN_TYPE_LEVEL)
        is_rate = received['basis'] == RATE_BASIS
        rows.append(
            {
                MARGIN_TYPE_LEVEL: margin_type,
                'transactions': len(received),
                'transaction_share': len(received) / len(df),
                'commodities': received.index.get_level_values(
                    COMMODITY_LEVEL
                ).nunique(),
                'buyers': received.index.get_level_values(BUYER_LEVEL).nunique(),
                'margin': received['margin'].sum(),
                'rate_transactions': int(is_rate.sum()),
                'rate_margin': received.loc[is_rate, 'margin'].sum(),
                'level_transactions': int((~is_rate).sum()),
                'level_margin': received.loc[~is_rate, 'margin'].sum(),
                'median_rate': received['rate'].median(),
            }
        )
    return pd.DataFrame(rows).set_index(MARGIN_TYPE_LEVEL)


def rate_dispersion_by_commodity(
    margins: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Within-commodity dispersion of the rates, per margin type and commodity.

    The evidence against collapsing to one rate per commodity, and the reason
    :func:`margin_rate_table` keeps the buyer dimension: BEA's uniform rate is
    at the item level, finer than the published commodity, so several item rates
    mix inside every commodity here. Restricted to commodities with more than
    one rate-basis transaction, since a single-transaction commodity has no
    dispersion to measure.

    Columns: ``transactions``, ``mean``, ``std``, ``cv``.
    """
    table = margin_rate_table(margins)
    rates = table.loc[table['basis'] == RATE_BASIS, 'rate']
    grouped = rates.groupby(level=[MARGIN_TYPE_LEVEL, COMMODITY_LEVEL])
    out = pd.DataFrame(
        {
            'transactions': grouped.size(),
            'mean': grouped.mean(),
            'std': grouped.std(ddof=0),
        }
    )
    out['cv'] = out['std'] / out['mean'].replace(0.0, np.nan)
    return out.loc[out['transactions'] > 1]
