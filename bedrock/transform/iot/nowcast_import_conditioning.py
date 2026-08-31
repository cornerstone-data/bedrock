"""Condition the detail imports column on the published annual summary Supply
table (#785).

The ``MCIF`` column is built from Census goods trade plus the IEA services
bridge, and graded against the published summary Supply table its composition
is 18-22% off in **every** year — including 20.6% at the 2017 anchor, because
unlike the margin and tax columns the imports vector was rebuilt from primary
sources rather than anchored on the published 2017 cells, and Census/IEA
classification is not BEA's. Imports are the second-largest component of
total supply, so that error transfers straight into the supply-equals-use row
targets the interior fit treats as truth.

The conditioning is the house instrument (#786 final demand, #784 subsidies):
scale each detail cell by (published summary group value / our group value),
per summary commodity group, per year. After it the column aggregates to
BEA's own annual allocation; the within-group detail split keeps our
construction.

⚠️ **2017 is conditioned too, deliberately.** The leave-2017-alone doctrine
protects anchors that reproduce the published table; the imports column never
did — its 20.6% summary-level gap at the anchor is Census-vs-BEA
classification error, not information. What conditioning cannot see is the
**within-group** classification error (the ``334220``/``334418``-style
reshuffles the supply-bridge diagnostic shows) — that remains #670's.

⚠️ **Every consumer of the imports vector must read the same conditioned
copy** (Wes's requirement, made structural): the Supply-bridge assembly, the
``purchaser_base`` tax base, and the customs-duty column all take these
factors. ``MDTY`` in particular computes duty = Census rate x goods MCIF and
used to derive that goods vector internally from Census — silently
disagreeing with a conditioned bridge by construction.

⚠️ The ``Other`` group re-levels ``S00300`` noncomparable imports from the
IEA-leaves sum (#766, restored on #783) to BEA's published group value — the
same answer-key doctrine applied uniformly; the leaves remain the *shape*
within the group alongside ``S00900`` (which carries no imports).
"""

from __future__ import annotations

import functools
import logging

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.transform.flowbysector import getFlowBySector
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

logger = logging.getLogger(__name__)

#: Years the summary Supply workbook covers — the same span as the trade
#: overlay, so every year with an imports vector has an answer to condition on.
CONDITIONED_YEARS: tuple[int, ...] = tuple(range(2017, 2025))

#: Below this absolute value ($) a group of ours is treated as empty: a ratio
#: against it is numerically meaningless and a zero can never be scaled up.
EMPTY_GROUP_USD = 1e6


@functools.cache
def _commodity_to_summary() -> pd.Series:
    mapping = load_bea_v2017_commodity_to_bea_v2017_summary()
    return pd.Series({code: parents[0] for code, parents in mapping.items()})


@functools.cache
def raw_import_vector(year: int, download_sources_ok: bool = True) -> pd.Series:
    """Commodity totals from ``Trade_Imports_<year>``, USD, BEA 2017 Detail.

    The same read the bridge assembly used to do inline; kept here so the
    factors and their consumers cannot drift apart. Industry-only crosswalk
    rows drop on the reindex.
    """
    fbs = getFlowBySector(
        f'Trade_Imports_{year}',
        download_FBAs_if_missing=download_sources_ok,
        download_FBS_if_missing=download_sources_ok,
    )
    return (
        pd.DataFrame(fbs)
        .groupby('SectorProducedBy')['FlowAmount']
        .sum()
        .reindex(USA_2017_COMMODITY_CODES)
        .fillna(0.0)
    )


@functools.cache
def mcif_condition_factors(year: int) -> pd.Series:
    """Per-commodity scale factor for *year*'s imports, guards applied.

    Constant within each summary group: (published summary ``MCIF`` for the
    group) / (our group total). Groups where we carry less than
    :data:`EMPTY_GROUP_USD` hold at 1.0 and the published mass there is
    logged as unreachable — a zero cannot be scaled into existence.
    """
    if int(year) not in CONDITIONED_YEARS:
        raise ValueError(
            f'the summary Supply workbook covers {CONDITIONED_YEARS[0]}-'
            f'{CONDITIONED_YEARS[-1]}; got {year}'
        )
    ours_detail = raw_import_vector(int(year))
    groups = _commodity_to_summary().reindex(ours_detail.index)
    ours = ours_detail.groupby(groups).sum()

    supply = _load_usa_summary_sut('Supply_summary', int(year))  # type: ignore[arg-type]
    published = (
        pd.to_numeric(pd.Series(supply['MCIF']), errors='coerce').fillna(0.0) * 1e6
    )
    published = published.reindex(ours.index).fillna(0.0)

    if (published < 0).any() or (ours < -EMPTY_GROUP_USD).any():
        raise ValueError(
            f'{year} imports carry a negative group total '
            f'(published: {sorted(published.index[published < 0])}; ours: '
            f'{sorted(ours.index[ours < -EMPTY_GROUP_USD])}). MCIF is a '
            f'nonnegative column on both sides; a negative group means a sign '
            f'convention broke upstream.'
        )
    empty = ours.abs() < EMPTY_GROUP_USD
    factor = (published / ours.replace(0.0, np.nan)).where(~empty, 1.0).fillna(1.0)
    unreachable = float(published[empty].sum())
    if unreachable:
        logger.info(
            'MCIF conditioning %s: %.0fM of published imports unreachable '
            '(our group empty)',
            year,
            unreachable / 1e6,
        )
    per_commodity = groups.map(factor).astype(float).fillna(1.0)
    per_commodity.index = ours_detail.index
    return per_commodity.rename('mcif_factor')


def conditioned_mcif(year: int, download_sources_ok: bool = True) -> pd.Series:
    """The imports column for *year*, conditioned, USD, BEA 2017 Detail.

    Zero cells stay zero (the factor multiplies), so the never-imported
    structure survives untouched.
    """
    raw = raw_import_vector(int(year), download_sources_ok)
    return (raw * mcif_condition_factors(int(year))).rename('MCIF')
