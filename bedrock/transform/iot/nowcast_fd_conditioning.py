"""Condition the NIPA-built final-demand columns on the published annual
summary Use SUT.

Every one of the seventeen NIPA-built final-demand columns distributes an
observed annual total across commodities on some 2017-frozen shape: PCE and
equipment on the 2017 PCE/PEQ bridge mixes within each NIPA line, the
government investment columns on the entire 2017 Use-column shape moved by
one aggregate level. Measured against the published annual summary Use SUT
(the same purchasers'-price framework this block feeds, published 2017-2024),
those freezes rot at very different rates by 2023: PCE 2.3% of the column
misplaced, private equipment 17.9%, the government equipment columns 50-152%.
The levels are exact everywhere — the error is purely composition.

The conditioning is one biproportional step: scale each detail cell by
(published summary group value / our summary group value), per summary
commodity group, per column, per year. Group boundaries come from
:func:`load_bea_v2017_commodity_to_bea_v2017_summary` (one parent per detail
commodity). After it, our columns aggregate to BEA's own annual allocation at
the summary level; the within-group detail split — which the summary tables
cannot see — keeps our construction.

⚠️ **Do not expect this to move the supply-equals-use aggregate.** The
counterfactual was measured before wiring: T11 shifts less than a point in
either direction across 2018-2023, because the final-demand errors partially
offset interior-row errors of the same sign structure. What it buys is
correct *row targets* for the interior fit (each row's target is supply minus
final demand, so every misplaced final-demand dollar lands in the interior
targets otherwise) and large fixes on FD-owned rows — custom programming's
2023 residual fell 76 -> 30bn $M in the counterfactual.

⚠️ ``F03000`` and ``F04000`` are NOT conditioned. Inventories and exports are
their own sourced constructions (#746, #729/#771) with their own graders;
their summary-level standing is recorded in the About doc, and exports are
bl-young's lane.

Guards, in the order applied per (group, column):

- our group carries less than $1M absolute — nothing to scale; factor 1.0 and
  the published mass there is unreachable (measured at ~10-14bn/yr, mostly
  groups our crosswalks give no final demand at all);
- our group and the published group disagree in sign — a ratio would flip
  every cell's sign, which the downstream sign locks refuse; factor 1.0;
- otherwise factor = published / ours, unbounded — the government equipment
  columns legitimately need factors far from 1.
"""

from __future__ import annotations

import functools
import logging

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

logger = logging.getLogger(__name__)

#: The NIPA-built final-demand columns, i.e. every SUT final-demand code
#: except inventories and exports (see the module docstring).
CONDITIONED_COLUMNS: tuple[str, ...] = (
    'F01000',
    'F02E00',
    'F02N00',
    'F02R00',
    'F02S00',
    'F06C00',
    'F06E00',
    'F06N00',
    'F06S00',
    'F07C00',
    'F07E00',
    'F07N00',
    'F07S00',
    'F10C00',
    'F10E00',
    'F10N00',
    'F10S00',
)

#: Years the summary Use SUT is published for (the 1997-2024 workbook).
SUMMARY_SUT_YEARS: tuple[int, ...] = tuple(range(2017, 2025))

#: Below this absolute value ($) a group of ours is treated as empty: a ratio
#: against it is numerically meaningless and a zero can never be scaled up.
EMPTY_GROUP_USD = 1e6


@functools.cache
def _commodity_to_summary() -> pd.Series:
    mapping = load_bea_v2017_commodity_to_bea_v2017_summary()
    return pd.Series({code: parents[0] for code, parents in mapping.items()})


def summary_condition_factors(y: pd.DataFrame, year: int) -> pd.DataFrame:
    """Summary group x column scale factors for ``y`` at ``year``, guards applied.

    ``y`` is the assembled final-demand block in USD; published values come
    from the summary Use SUT workbook (million USD there, converted before
    dividing).
    """
    if year not in SUMMARY_SUT_YEARS:
        raise ValueError(
            f'the summary Use SUT covers {SUMMARY_SUT_YEARS[0]}-'
            f'{SUMMARY_SUT_YEARS[-1]}; got {year}'
        )
    groups = _commodity_to_summary().reindex(y.index)
    published = _load_usa_summary_sut('Use_SUT_summary', year)  # type: ignore[arg-type]
    published = published.apply(pd.to_numeric, errors='coerce').fillna(0.0) * 1e6

    factors = {}
    unreachable = 0.0
    sign_held = 0.0
    for column in CONDITIONED_COLUMNS:
        ours = y[column].groupby(groups).sum()
        # The summary workbooks truncate the codes to four characters
        # (F01000 -> F010).
        pub = published[column[:4]].reindex(ours.index).fillna(0.0)
        empty = ours.abs() < EMPTY_GROUP_USD
        flipped = ~empty & (np.sign(ours) != np.sign(pub)) & (pub.abs() >= 1.0)
        factor = (pub / ours.replace(0.0, np.nan)).fillna(1.0)
        factor[empty | flipped] = 1.0
        factors[column] = factor
        unreachable += float(pub[empty].abs().sum())
        sign_held += float((pub - ours)[flipped].abs().sum())
    if unreachable or sign_held:
        logger.info(
            'FD conditioning %s: %.0fM published unreachable (our group '
            'empty), %.0fM held on sign disagreement',
            year,
            unreachable / 1e6,
            sign_held / 1e6,
        )
    return pd.DataFrame(factors)


def condition_fd_on_summary(y: pd.DataFrame, year: int) -> pd.DataFrame:
    """Scale the NIPA-built columns of ``y`` to the published summary allocation.

    ``y`` is the assembled final-demand block, commodity x SUT final-demand
    codes, USD. Only :data:`CONDITIONED_COLUMNS` change; every other column is
    returned untouched. Zero cells stay zero (the factor multiplies), so the
    implicit mask survives.
    """
    factors = summary_condition_factors(y, year)
    groups = _commodity_to_summary().reindex(y.index)
    out = y.copy()
    for column in CONDITIONED_COLUMNS:
        per_row = groups.map(factors[column]).astype(float).fillna(1.0)
        per_row.index = y.index
        out[column] = y[column] * per_row
    return out
