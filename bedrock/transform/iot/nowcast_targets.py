"""Nowcast Supply/Use target set: what constrains the balance.

The generic machinery is :mod:`bedrock.utils.economic.balance.targets`; this
module is the *sourcing*. Paired with :mod:`bedrock.transform.iot.nowcast_mask`
- the mask and the target set are one decision seen from two sides, because a
source spent on a cell cannot also be spent on a margin.

What is real today, and what is not
-----------------------------------

The set is complete in **shape**. Values divide in two, and the division is
recorded on each target's ``source`` so it cannot be lost:

============================  =========================================
Real                          Placeholder (``PLACEHOLDER:`` prefixed)
============================  =========================================
T1 gross output               T2 final-demand column totals
T18 value added               T4 compensation by industry group
T11-T17 identities            T6 product-tax economy-wide totals
                              T7-T9 Supply column totals
============================  =========================================

T1 is real because ``BEA_Detail_GrossOutput_IO_<year>`` is extracted for
2017-2024, T18 is real because ``UVA205-A`` publishes over the same span, and
T11-T17 are real because an identity needs no source at all. The
placeholders are the ones waiting on upstream nowcasts and on NIPA reads that
are not yet wired; their **shapes, labels and aggregators are correct**, so an
engine built against this set does not change when the values arrive.

T1 and T18 are the two halves of one column
-------------------------------------------

T1 pins ``T005 + VAPRO``; **T18 pins ``VAPRO``**; together they pin ``T005``.

Without T18 the income side carries no per-industry constraint at all - T4 is
soft and aggregated to summary groups, T6 is a pair of economy-wide scalars,
and T5 is unimposed - so value-added estimation error lands in the intermediate
column total. That total is *the scale of a column of the technology matrix*,
and it multiplies through the Leontief inverse into every downstream result.
With T18 the same error lands inside value added instead, where the free row is
``V00300`` - gross operating surplus, which BEA itself largely computes as a
residual, and which nothing reads: it appears in no ``A``, no ``L`` and no
emission factor. **Moving the slack off ``T005`` and onto ``V00300`` is the
whole point of the target** (decided 2026-08-26;
``bedrock/analysis/nowcasting/compensation_disaggregation_plan.md`` carries the
measurement, including the 22 industries whose surplus is too thin to absorb
much and need a sign census).

✅ **The two sides come from one release, so they cannot disagree.** T1 reads
the extracted ``UGO305-A`` parquet and T18's series is derived against
``load_go_detail()`` - the two gross-output vectors are **identical to the
dollar for every industry in every year 2017-2024**. So the ``T005`` the pair
implies is exactly
:func:`~bedrock.transform.iot.derived_intermediate_and_value_added.derive_detail_intermediate_inputs`,
and no third quantity is smuggled in.

⚠️ **T18 is where the estimate is, and T1 is not.** Gross output is published
at 402 detail; value added is published on BEA's 191-row underlying frame and
*allocated* down. So T18 carries an allocation's error where T1 carries none,
which is the argument for T18 being the one that gives way first if this set
ever has to be relaxed.

⚠️ :func:`~bedrock.utils.economic.balance.feasibility.precheck` **refuses to
certify a set containing a placeholder** unless asked. A placeholder is
shape-correct, not an estimate, and must never be mistaken for one.

T1 binds the Use panel, not the Supply panel
--------------------------------------------

T1 is sometimes stated as "Supply + Use industry columns". Measured
on 2017, only the Use panel carries it:

===============================  ==============================
Use column sum (``T005+VAPRO``)  reproduces GO to **13** / industry
Supply column sum (``T007``)     misses GO by up to **88,363**
===============================  ==============================

which is the producer-versus-basic point restated: gross output is published at *producer* prices and
the Supply table's industry column is at *basic*. So T1 is imposed on the Use
column alone.

**The Supply industry column is constrained instead by T17**, the basic-to-
producer identity::

    BAS + TAX - SUB = PRO          BEA's statement
    supply.col + T00TOP + T00SUB = use.col      here, subsidies stored negative

⚠️ **The wedge is only available on the Use table.** The Supply panel carries
``TOP`` and ``SUB`` by *commodity*; nothing on it gives product taxes by
*industry*. That is what makes T17 cross-block, and it is the only constraint
the Supply industry columns have - without it that whole axis is free.

⚠️ ``4200ID`` is the sharpest case: its Use column is 38,513 of customs duties
and its Supply column is **zero**. See ``nowcast_mask.balance_industries``.
"""

from __future__ import annotations

import functools
import glob
from typing import cast

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    derive_detail_value_added,
)
from bedrock.transform.iot.nowcast_mask import (
    BLOCKS,
    EXCLUDED_COMMODITIES,
    ONE_TO_ONE_FD,
    SUPPLY_BRIDGE_COLUMNS,
    VA_ROWS,
    balance_commodities,
    balance_industries,
    panel_labels,
    published_2017_panel,
)
from bedrock.utils.economic.balance.targets import (
    PLACEHOLDER_PREFIX,
    Aggregator,
    Target,
    TargetSet,
    TargetTerm,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

GROSS_OUTPUT_GLOB = (
    'bedrock/extract/output_data/BEA_Detail_GrossOutput_IO_{year}_*.parquet'
)

#: The thirteen final-demand columns that stay targets. The other six are
#: imposed cell-wise by the mask, so they leave the target set - masking those
#: cells and targeting their column total are the same constraint written twice.
FD_TARGET_COLUMNS = tuple(c for c in SUT_FINAL_DEMAND_CODES if c not in ONE_TO_ONE_FD)

#: The rest-of-world adjustment commodity. BEA defines it as the values for
#: exports and imports that have offsetting adjustments to personal consumption
#: expenditures and government, so it is an accounting bridge rather than a
#: produced good. It leaves the balance's commodity axis under Tier 4 and is
#: re-derived from ``-F010 + Supply T016`` afterwards - but its Supply *make*
#: row is real production and does not go away with it.
REST_OF_WORLD_ADJUSTMENT = 'S00900'

#: ``F03000`` is -37,568 in 2020 and swings 1,248% year over year. It is the
#: only final-demand column that legitimately goes negative.
NEGATIVE_FD_COLUMNS = ('F03000',)

#: Starting weights. A weight says *who gives way
#: when the accounts disagree*, not *which number is more accurate*, and they
#: are meaningful only relative to each other. To be calibrated on the 2017
#: replay; the ordering is the part worth defending, not the values.
WEIGHTS = {
    'T2': 0.8,
    'T4': 0.6,
    'T6': 0.7,
    'T7': 0.8,
    'T8': 0.7,
    'T9': 0.7,
}


def _row(panel: pd.DataFrame, label: str) -> pd.Series:
    """One row of a panel, narrowed to a Series.

    ``.loc[label]`` is typed ``Series | DataFrame`` because a duplicated
    label would return a frame; the panels are built with unique labels, so
    this asserts that rather than spreading ignores.
    """
    row = panel.loc[label]
    if isinstance(row, pd.DataFrame):
        raise ValueError(f'{label} matches {len(row)} rows; expected one')
    return row


def _placeholder(what: str) -> str:
    return f'{PLACEHOLDER_PREFIX} {what}'


@functools.cache
def _use_labels() -> tuple[tuple[str, ...], tuple[str, ...]]:
    return panel_labels('use')


@functools.cache
def _supply_labels() -> tuple[tuple[str, ...], tuple[str, ...]]:
    return panel_labels('supply')


def _use_total(labels: list[str], name: str, axis: str) -> Aggregator:
    rows, columns = _use_labels()
    return Aggregator.total(labels, rows if axis == 'row' else columns, name)


def _supply_total(labels: list[str], name: str) -> Aggregator:
    _, columns = _supply_labels()
    return Aggregator.total(labels, columns, name)


# --------------------------------------------------------------------------
# T1 - gross output, the only sourced hard target
# --------------------------------------------------------------------------


def published_gross_output(year: int) -> pd.Series:
    """Detail gross output at producer prices, from BEA's UGO305-A.

    A straight read - no 2017 shares anywhere - and published for all 402
    detail industries in every year 2017-2024, which is why the industry
    constraint can be imposed at detail rather than at summary level.
    """
    paths = sorted(glob.glob(GROSS_OUTPUT_GLOB.format(year=year)))
    if not paths:
        raise FileNotFoundError(
            f'no extracted gross output for {year}: {GROSS_OUTPUT_GLOB.format(year=year)}'
        )
    series = (
        pd.read_parquet(paths[0])
        .set_index('ActivityProducedBy')['FlowAmount']
        .astype(float)
    )
    industries = list(balance_industries())
    aligned = series.reindex(industries)
    if aligned.isna().any():
        missing = list(aligned.index[aligned.isna()])
        raise KeyError(f'gross output for {year} is missing industries: {missing}')
    aligned.index.name = 'industry'
    return aligned


def industry_output_target(year: int, gross_output: pd.Series | None = None) -> Target:
    """T1. ``T005 + VAPRO = GO(producer)`` on the Use panel, per industry.

    The full Use column margin - intermediate inputs plus **all five**
    value-added rows - because the target is at producer prices and the balance
    solves the product-tax allocation rather than assuming a 2017 ratio.

    ⚠️ **Use only.** There is no Supply term: the Supply industry column is at
    basic prices and is constrained by T17 instead. Passing ``gross_output``
    injects the series rather than reading the extracted parquet, which is what
    makes this testable - the parquet is a pipeline artefact and is not in the
    repository.
    """
    values = published_gross_output(year) if gross_output is None else gross_output
    return Target.on_margin(
        'use',
        'column',
        values,
        'BEA UGO305-A detail gross output',
        name='T1',
        hard=True,
    )


# --------------------------------------------------------------------------
# T18 - value added, the other sourced hard target
# --------------------------------------------------------------------------


def published_value_added(year: int) -> pd.Series:
    """``VAPRO`` by detail industry, from BEA's ``UVA205-A``, million USD.

    The mirror of :func:`published_gross_output`, and the one asymmetry between
    them is worth stating plainly: gross output is **published** at 402 detail,
    while value added is published on BEA's 191-row underlying frame and
    *allocated* down by
    :mod:`~bedrock.transform.iot.derived_intermediate_and_value_added`. So this
    series carries an allocation where the gross-output one carries none.
    """
    values = derive_detail_value_added(year)
    aligned = values.reindex(list(balance_industries())).astype(float)
    if aligned.isna().any():
        missing = list(aligned.index[aligned.isna()])
        raise KeyError(f'value added for {year} is missing industries: {missing}')
    aligned.index.name = 'industry'
    return aligned


def industry_value_added_target(
    year: int, value_added: pd.Series | None = None
) -> Target:
    """T18. ``V00100 + T00OTOP + T00OSUB + V00300 + T00TOP + T00SUB = VAPRO``,
    per industry.

    The **income half of the Use column**, and the sibling of T1: T1 pins the
    whole column and this pins the value-added part of it, so the two together
    determine ``T005`` per industry. See the module docstring for why that
    matters more than either target does alone.

    The margin is a column margin **restricted to**
    :data:`~bedrock.transform.iot.nowcast_mask.VA_ROWS` - neither a row margin
    nor a plain column one, which is exactly what ``restrict_to`` exists for.

    ⚠️ **All six rows, and both subsidy rows enter negative.** ``VAPRO`` is
    ``VABAS + T00TOP - T00SUB`` on BEA's published tables, but
    :func:`~bedrock.transform.iot.nowcast_mask.published_2017_panel` normalises
    the Use ``T00SUB`` row negative (and the seed stores ``T00OSUB`` negative,
    #784), so here it is a plain sum of six rows.
    Measured on the published 2017 panel that sum reproduces the derived
    ``VAPRO`` series to **2 per industry**. Written as ``VABAS + T00TOP -
    T00SUB`` against the normalised panel it would be wrong by ``2 x T00SUB`` -
    the same sign trap T12 was caught on.

    ⚠️ ``allow_negative`` is set. ``S00201`` (state and local government
    passenger transit) has a published 2017 ``VAPRO`` of **-10,069** and the
    derived series is negative there in every year. That is BEA's number;
    clipping it would be a fabrication.

    Passing ``value_added`` injects the series instead of deriving it, which is
    what lets this be exercised without the extract workbooks - the same
    affordance :func:`industry_output_target` gives T1.
    """
    values = published_value_added(year) if value_added is None else value_added
    return Target.on_margin(
        'use',
        'column',
        values,
        'BEA UVA205-A value added, allocated to BEA 2017 detail',
        restrict_to=VA_ROWS,
        name='T18',
        hard=True,
        allow_negative=True,
    )


# --------------------------------------------------------------------------
# T11-T16 - identities, which spend no source
# --------------------------------------------------------------------------


def rest_of_world_adjustment_supply_make(year: int = 2017) -> pd.Series:
    """Domestic make of ``S00900``, the **rest-of-world adjustment**, by industry.

    This is the one row the balance drops from the Supply panel's commodity
    axis but cannot ignore. ``S00900`` is BEA's rest-of-world adjustment - the
    exports and imports carrying offsetting adjustments to PCE and government -
    and it is held out under Tier 4 because its Use row is 100% final demand
    against 0.9% joint freedom, then re-derived from ``-F010 + Supply T016``
    after the balance.

    **Holding the commodity out does not hold its production out.** The
    ``S00900`` Supply row carries **3,468** of domestic make spread across
    industries, so every industry column of the balance's Supply panel is short
    by its share of it. That is the entire gap in T17: with this row restored
    the basic-to-producer identity closes to 12 per industry on 2017, and
    without it to 3,464.

    So T17's right-hand side is **not zero** - it is minus this series. Carried
    explicitly rather than assumed away, because a 3,468 discrepancy spread
    over 402 industries looks exactly like ordinary rounding until it is
    named.
    """
    del year  # 2017 make until the nowcast Supply block exists
    if REST_OF_WORLD_ADJUSTMENT not in EXCLUDED_COMMODITIES:
        raise ValueError(
            f'{REST_OF_WORLD_ADJUSTMENT} is no longer held out of the commodity '
            f'axis, so its make row is already inside the Supply panel and T17 '
            f'must not offset for it - doing so would double-count 3,468'
        )
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    industries = list(balance_industries())
    row = _row(supply, REST_OF_WORLD_ADJUSTMENT)[industries]
    values = pd.to_numeric(row, errors='coerce').fillna(0.0).astype(float)
    values.index = pd.Index(industries, name='industry')
    return values


def identity_targets() -> list[Target]:
    """The six constraints that cost nothing because both sides are internal.

    T11 is per commodity; T12-T16 are economy-wide scalars. Each is expressed
    as a linear combination of margins, which is why they need no special
    casing.
    """
    commodities = list(balance_commodities())
    zero_per_commodity = pd.Series(0.0, index=pd.Index(commodities, name='commodity'))

    def scalar(name: str) -> pd.Series:
        return pd.Series([0.0], index=pd.Index([name], name='identity'))

    return [
        # T11 - total supply equals total use, per commodity. The one identity
        # that lets a frozen Use row be absorbed by its Supply row.
        Target(
            terms=(
                TargetTerm('supply', 'row', 1.0),
                TargetTerm('use', 'row', -1.0),
            ),
            values=zero_per_commodity,
            source='identity T016 = T019',
            name='T11',
            hard=True,
        ),
        # T12 - subsidies. Exact on 2017: -59,876 on both panels.
        #
        # ⚠️ A **difference**, not a sum. BEA stores the Use T00SUB row
        # positive and the Supply SUB column negative, so on the raw tables the
        # identity is `T00SUB + SUB = 0`. The balance normalises both negative
        # (``nowcast_mask.published_2017_panel``), which turns it into a plain
        # equality - and writing it as a sum here is wrong by exactly
        # ``2 x 59,876``. Caught by evaluating against the published 2017
        # tables; it is the single easiest sign error to make in this set.
        Target(
            terms=(
                TargetTerm(
                    'use', 'row', 1.0, _use_total(['T00SUB'], 'subsidies', 'row')
                ),
                TargetTerm(
                    'supply', 'column', -1.0, _supply_total(['SUB'], 'subsidies')
                ),
            ),
            values=scalar('subsidies'),
            source='identity T00SUB = SUB',
            name='T12',
            hard=True,
        ),
        # T13 - taxes on products. NOT T00TOP = TOP: customs duties are a
        # product tax the Supply table books in its own column while the Use
        # table folds it into T00TOP. Residual 18 on 755,451 in 2017.
        Target(
            terms=(
                TargetTerm(
                    'use', 'row', 1.0, _use_total(['T00TOP'], 'product_taxes', 'row')
                ),
                TargetTerm(
                    'supply',
                    'column',
                    -1.0,
                    _supply_total(['TOP', 'MDTY'], 'product_taxes'),
                ),
            ),
            values=scalar('product_taxes'),
            source='identity T00TOP = TOP + MDTY',
            name='T13',
            hard=True,
        ),
        # T14 - the hinge. 4200ID's Use column is the duty it collects, and it
        # is the Supply MDTY total. Doubles as a consistency check on the
        # annual MDTY nowcast, which is where a duty-rate error surfaces first.
        Target(
            terms=(
                TargetTerm(
                    'use',
                    'column',
                    1.0,
                    _use_total(['4200ID'], 'customs_duties', 'column'),
                    restrict_to=('T00TOP',),
                ),
                TargetTerm(
                    'supply', 'column', -1.0, _supply_total(['MDTY'], 'customs_duties')
                ),
            ),
            values=scalar('customs_duties'),
            source='identity T00TOP[4200ID] = MDTY',
            name='T14',
            hard=True,
        ),
        # T15/T16 - margins are a redistribution, not value created, so the
        # column sums to zero. The only constraint on Step 4c's own output, and
        # it leaves the distribution entirely free.
        Target(
            terms=(
                TargetTerm(
                    'supply', 'column', 1.0, _supply_total(['TRADE '], 'trade_margin')
                ),
            ),
            values=scalar('trade_margin'),
            source='identity sum(TRADE) = 0',
            name='T15',
            hard=True,
        ),
        Target(
            terms=(
                TargetTerm(
                    'supply',
                    'column',
                    1.0,
                    _supply_total(['TRANS'], 'transport_margin'),
                ),
            ),
            values=scalar('transport_margin'),
            source='identity sum(TRANS) = 0',
            name='T16',
            hard=True,
        ),
        # T17 - basic to producer, per industry. The Supply industry column
        # is industry output at *basic* prices; the Use industry column is the
        # same output at *producer* prices. The wedge is the product-tax rows,
        # and it is only available on the **Use** table - the Supply panel
        # carries TOP and SUB by commodity, never by industry. That is what
        # makes this identity the only constraint the Supply industry columns
        # have.
        #
        # BEA states it as BAS + TAX - SUB = PRO. Here it is a plain sum
        # because the balance stores subsidies negative - the same convention
        # flip that made T12 a difference rather than a sum.
        #
        # ⚠️ The right-hand side is not zero. S00900 is held out of the
        # commodity axis (Tier 4) but its Supply make row is not zero, so the
        # panel's column sum is short by exactly that row. Carried explicitly;
        # with it the identity holds to 12 per industry on 2017, without it to
        # 3,464.
        Target(
            terms=(
                TargetTerm('supply', 'column', 1.0),
                TargetTerm('use', 'column', -1.0),
                TargetTerm('use', 'column', 1.0, restrict_to=('T00TOP', 'T00SUB')),
            ),
            values=-rest_of_world_adjustment_supply_make(),
            source=(
                'identity BAS + TOP + SUB = PRO, less the held-out '
                'rest-of-world adjustment (S00900) make row'
            ),
            name='T17',
            hard=True,
            allow_negative=True,
        ),
    ]


# --------------------------------------------------------------------------
# T2, T4, T6-T9 - sourced targets, shapes real and values pending
# --------------------------------------------------------------------------


def fd_column_targets(year: int) -> Target:
    """T2. The thirteen final-demand column totals, one per code.

    ⚠️ Values are placeholders taken from the published 2017 columns, so the
    magnitudes are realistic and the shape is exact. The real source is one
    NIPA line per code, and ``F02E00``'s basis is still open.

    ``allow_negative`` is set because ``F03000`` is negative outright in 2020.
    """
    del year  # placeholder values are 2017's until the NIPA reads are wired
    panel = published_2017_panel('use')
    values = panel[list(FD_TARGET_COLUMNS)].sum(axis=0)
    values.index.name = 'final_demand'
    return Target.on_margin(
        'use',
        'column',
        values,
        _placeholder('NIPA column totals per FD code (§3)'),
        name='T2',
        weight=WEIGHTS['T2'],
        allow_negative=True,
    )


@functools.cache
def industry_group_aggregator() -> Aggregator:
    """Detail industries to BEA summary groups, for T4.

    ⚠️ **The grouping is BEA summary, not NIPA T60200D's.** Compensation is
    published by industry group rather than by 402 detail industries, and this
    is the closest published grouping bedrock already carries. Confirming that
    T60200D's groups match summary - or building the real mapping - is part of
    what makes T4 real rather than shape-only.
    """
    # Literal-keyed in the stubs; the balance works in plain codes.
    detail_to_summary = cast(
        'dict[str, list[str]]', load_bea_v2017_industry_to_bea_v2017_summary()
    )
    groups: dict[str, list[str]] = {}
    for detail in balance_industries():
        for summary in detail_to_summary.get(detail, []):
            groups.setdefault(str(summary), []).append(detail)
    _, columns = _use_labels()
    return Aggregator.from_mapping(groups, columns)


def va_row_targets(year: int) -> list[Target]:
    """T4 and T6: compensation by industry group, and the product-tax totals.

    T4 is the target that needs an aggregator - the truthful constraint is
    *"these N detail industries sum to the published group"*, and expressing it
    is the capability a row/column-vector API cannot provide.

    T5 (``T00OTOP``, ``V00300``) is deliberately absent: the income side is the
    hold-back that keeps GDP as out-of-sample evidence.
    Leaving it unimposed means those two value-added rows enter the balance
    as seed only, which is the price of the test being worth running.

    ⚠️ **T5's absence is now load-bearing rather than merely tolerated.** With
    T18 pinning ``VAPRO`` per industry the five value-added rows have a fixed
    column total, so leaving ``V00300`` unconstrained is precisely what makes it
    the row that absorbs the residual. Imposing T5 would push that slack back
    out into ``T005`` and undo T18. **Do not impose T5 without re-deciding
    T18.**
    """
    del year
    panel = published_2017_panel('use')
    aggregator = industry_group_aggregator()
    industries = list(balance_industries())

    # Over the whole row, not just the industries: the aggregator's detail is
    # every Use column, and the value-added by final-demand corner is zero.
    compensation = aggregator.apply(_row(panel, 'V00100'))
    compensation.index.name = 'industry_group'

    taxes = pd.Series(
        [
            float(_row(panel, 'T00TOP')[industries].sum()),
            float(_row(panel, 'T00SUB')[industries].sum()),
        ],
        index=pd.Index(['T00TOP', 'T00SUB'], name='va_row'),
        dtype=float,
    )

    return [
        Target(
            terms=(
                TargetTerm('use', 'column', 1.0, aggregator, restrict_to=('V00100',)),
            ),
            values=compensation,
            source=_placeholder('NIPA T60200D compensation by industry group'),
            name='T4',
            weight=WEIGHTS['T4'],
        ),
        Target(
            terms=(
                TargetTerm(
                    'use',
                    'row',
                    1.0,
                    Aggregator.from_mapping(
                        {'T00TOP': ['T00TOP'], 'T00SUB': ['T00SUB']}, _use_labels()[0]
                    ),
                ),
            ),
            values=taxes,
            source=_placeholder('NIPA T30500 / T31300 economy-wide totals'),
            name='T6',
            weight=WEIGHTS['T6'],
            allow_negative=True,  # T00SUB is stored negative
        ),
    ]


def supply_column_targets(year: int) -> list[Target]:
    """T7-T9: imports, customs duties, and product taxes on the Supply side.

    ⚠️ ``MADJ``, ``TRADE`` and ``TRANS`` are absent by design (T10). They are
    our own Step 4b/4c output, and a target we produced is a preference with
    extra steps. ``TRADE``/``TRANS`` are constrained by T15/T16 instead;
    ``MADJ`` is deliberately free (decided 2026-08-17).
    """
    del year
    panel = published_2017_panel('supply')
    specs = [
        ('T7', ['MCIF'], 'BEA ITA goods + services imports total'),
        ('T8', ['MDTY'], 'NIPA T30500 customs duties'),
        ('T9', ['TOP', 'SUB'], 'NIPA T30500 / T31300 product taxes and subsidies'),
    ]
    targets = []
    for name, columns, source in specs:
        values = panel[columns].sum(axis=0)
        values.index.name = 'supply_column'
        targets.append(
            Target.on_margin(
                'supply',
                'column',
                values,
                _placeholder(source),
                name=name,
                weight=WEIGHTS[name],
                allow_negative=True,  # SUB and MADJ are stored negative
            )
        )
    return targets


# --------------------------------------------------------------------------
# assembly
# --------------------------------------------------------------------------


def build_target_set(
    year: int,
    gross_output: pd.Series | None = None,
    value_added: pd.Series | None = None,
) -> TargetSet:
    """The full nowcast SUT target set for ``year``.

    Complete in shape; see the module docstring for which values are real.
    Feed it to :func:`~bedrock.utils.economic.balance.offset.offset_targets`
    together with the frozen blocks from
    :func:`~bedrock.utils.economic.balance.offset.split_fixed_blocks`.
    ``gross_output`` and ``value_added`` inject T1 and T18 the same way
    :func:`industry_output_target` and :func:`industry_value_added_target` do,
    so ``--check-engine`` can run without the extract parquet.
    """
    return TargetSet(
        (
            industry_output_target(year, gross_output=gross_output),
            industry_value_added_target(year, value_added=value_added),
            *identity_targets(),
            fd_column_targets(year),
            *va_row_targets(year),
            *supply_column_targets(year),
        )
    )


def hard_target_residuals(year: int = 2017) -> pd.DataFrame:
    """How far the seed already is from each hard constraint, per target.

    On the published 2017 tables every residual is BEA's $1M publication
    rounding - worst 21 on a $34 trillion table - which is what makes this a
    usable regression check on the target definitions themselves. It is how the
    T12 sign error was found: writing that identity as a sum rather than a
    difference is wrong by exactly ``2 x 59,876``, and nothing else in the
    pipeline would have said so.

    Belongs behind a ``--check`` flag rather than in a unit test, per the
    convention that real-data assertions live in the analysis scripts.
    """
    seeds: dict[str, pd.DataFrame] = {
        block: published_2017_panel(block) for block in BLOCKS
    }
    rows = []
    for target in build_target_set(year):
        if not target.hard:
            continue
        residual = (target.values - target.evaluate(seeds)).abs()
        rows.append(
            {
                'target': target.name,
                'margins': len(residual),
                'max_abs_residual': float(residual.max()),
                'total_abs_residual': float(residual.sum()),
                'source': target.source,
            }
        )
    return pd.DataFrame(rows).set_index('target')


def target_set_summary(year: int = 2017) -> pd.DataFrame:
    """One row per target: mode, size, provenance and whether it is real."""
    return build_target_set(year).summary()


__all__ = [
    'FD_TARGET_COLUMNS',
    'GROSS_OUTPUT_GLOB',
    'NEGATIVE_FD_COLUMNS',
    'SUPPLY_BRIDGE_COLUMNS',
    'WEIGHTS',
    'build_target_set',
    'fd_column_targets',
    'hard_target_residuals',
    'identity_targets',
    'industry_group_aggregator',
    'REST_OF_WORLD_ADJUSTMENT',
    'industry_output_target',
    'industry_value_added_target',
    'published_gross_output',
    'published_value_added',
    'rest_of_world_adjustment_supply_make',
    'supply_column_targets',
    'target_set_summary',
    'va_row_targets',
]
