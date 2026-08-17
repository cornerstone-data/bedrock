"""The Step 5 target set: what constrains the balance.

Step 5 Decision 3
(`#591 <https://github.com/cornerstone-data/bedrock/issues/591>`_). The generic
machinery is :mod:`bedrock.utils.economic.balance.targets`; this module is the
*sourcing*. Paired with :mod:`bedrock.transform.iot.nowcast_mask` - the mask and
the target set are one decision seen from two sides, because a source spent on a
cell cannot also be spent on a margin. The analysis is ``target_set_plan.md``.

What is real today, and what is not
-----------------------------------

The set is complete in **shape**. Values divide in two, and the division is
recorded on each target's ``source`` so it cannot be lost:

============================  =========================================
Real                          Placeholder (``PLACEHOLDER:`` prefixed)
============================  =========================================
T1 gross output               T2 final-demand column totals
T11-T16 identities            T4 compensation by industry group
                              T6 product-tax economy-wide totals
                              T7-T9 Supply column totals
============================  =========================================

T1 is real because ``BEA_Detail_GrossOutput_IO_<year>`` is extracted for
2017-2024, and T11-T16 are real because an identity needs no source at all. The
placeholders are the ones waiting on Steps 1-4 and on NIPA reads that are not
yet wired; their **shapes, labels and aggregators are correct**, so an engine
built against this set does not change when the values arrive.

⚠️ :func:`~bedrock.utils.economic.balance.feasibility.precheck` **refuses to
certify a set containing a placeholder** unless asked. A placeholder is
shape-correct, not an estimate, and must never be mistaken for one.

T1 binds the Use panel, not the Supply panel
--------------------------------------------

``target_set_plan.md`` §2 states T1 as "Supply + Use industry columns". Measured
on 2017, only the Use panel carries it:

===============================  ==============================
Use column sum (``T005+VAPRO``)  reproduces GO to **13** / industry
Supply column sum (``T007``)     misses GO by up to **88,363**
===============================  ==============================

which is §4's point restated: gross output is published at *producer* prices and
the Supply table's industry column is at *basic*. The two differ by
``T00TOP + T00SUB`` per industry - **a plain sum**, because the balance stores
subsidies negative. So T1 is imposed on the Use column alone, and the Supply
column is left for the commodity identity T11 to constrain.

⚠️ ``4200ID`` is the sharpest case: its Use column is 38,513 of customs duties
and its Supply column is **zero**. See ``nowcast_mask.balance_industries``.
"""

from __future__ import annotations

import functools
import glob
from typing import cast

import pandas as pd

from bedrock.transform.iot.nowcast_mask import (
    BLOCKS,
    ONE_TO_ONE_FD,
    SUPPLY_BRIDGE_COLUMNS,
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

#: ``F03000`` is -37,568 in 2020 and swings 1,248% year over year. It is the
#: only final-demand column that legitimately goes negative.
NEGATIVE_FD_COLUMNS = ('F03000',)

#: Starting weights from ``target_set_plan.md`` §2. A weight says *who gives way
#: when the accounts disagree*, not *which number is more accurate*, and they
#: are meaningful only relative to each other. To be calibrated on the 2017
#: replay (§8); the ordering is the part worth defending, not the values.
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


def industry_output_target(year: int) -> Target:
    """T1. ``T005 + VAPRO = GO(producer)`` on the Use panel, per industry.

    The full Use column margin - intermediate inputs plus **all five**
    value-added rows - because the target is at producer prices and the balance
    solves the product-tax allocation rather than assuming a 2017 ratio.
    """
    return Target.on_margin(
        'use',
        'column',
        published_gross_output(year),
        'BEA UGO305-A detail gross output',
        name='T1',
        hard=True,
    )


# --------------------------------------------------------------------------
# T11-T16 - identities, which spend no source
# --------------------------------------------------------------------------


def identity_targets() -> list[Target]:
    """The six constraints that cost nothing because both sides are internal.

    T11 is per commodity; T12-T16 are economy-wide scalars. Each is expressed
    as a linear combination of margins, which is why they need no special
    casing - see ``target_set_plan.md`` §2a and §2b.
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
    ]


# --------------------------------------------------------------------------
# T2, T4, T6-T9 - sourced targets, shapes real and values pending
# --------------------------------------------------------------------------


def fd_column_targets(year: int) -> Target:
    """T2. The thirteen final-demand column totals, one per code.

    ⚠️ Values are placeholders taken from the published 2017 columns, so the
    magnitudes are realistic and the shape is exact. The real source is one
    NIPA line per code (``target_set_plan.md`` §3), and ``F02E00``'s basis is
    still open on #547.

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
    hold-back that keeps GDP as out-of-sample evidence (``target_set_plan.md``
    §6). Leaving it unimposed means Step 2 enters Step 5 as a seed only for
    those two rows, which is the price of the test being worth running.
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


def build_target_set(year: int) -> TargetSet:
    """The full Step 5 target set for ``year``.

    Complete in shape; see the module docstring for which values are real.
    Feed it to :func:`~bedrock.utils.economic.balance.offset.offset_targets`
    together with the frozen blocks from
    :func:`~bedrock.utils.economic.balance.offset.split_fixed_blocks`.
    """
    return TargetSet(
        (
            industry_output_target(year),
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
    'identity_targets',
    'industry_group_aggregator',
    'industry_output_target',
    'published_gross_output',
    'supply_column_targets',
    'target_set_summary',
    'va_row_targets',
]
