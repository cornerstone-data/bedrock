"""Route ``S00300`` noncomparable service imports on the Use table (#767).

Supply ``MCIF`` is built from IEA import leaves (#766). This module distributes
that mass across Detail industries on the Use intermediate block and sets the
``F02N00`` IP-investment slice from a license capitalization rate.

Travel abroad stays on NIPA-conditioned ``F01000`` (Wes, #767): IEA travel
leaves are not added to final demand here.

Routing policy (Phase 1):

- ``FinExplicitAndOth`` -> ``523A00``
- ``TransportAirPort`` -> ``481000``
- ``CipLicenses*`` -> ``cap_rate`` x mass to ``F02N00``; remainder to industries
  proportional to the published 2017 benchmark row
- ``GovtGoodsAndServicesNie`` -> government industry codes by published shares
- ``TransportSeaFreight`` / ``TransportSeaPort`` -> transport industries by
  published shares
- Residual leaves -> proportional tail on the benchmark row

At :data:`~bedrock.transform.iot.nowcast_intermediate.SEED_YEAR` the
intermediate overlay is skipped so Step 3's 2017 reproduction check stays exact;
off-anchor years scale the built row to the annual intermediate budget.
"""

from __future__ import annotations

import functools
from pathlib import Path
from typing import cast

import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.nowcast_intermediate import SEED_YEAR, benchmark_intermediate
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

IEA_IMPORTS_CROSSWALK = (
    Path(__file__).resolve().parents[2]
    / 'utils/mapping/activitytosectormapping/Sector_Crosswalk_BEA_IEA_imports.csv'
)

CIP_LICENSES_LEAVES: tuple[str, ...] = (
    'CipLicensesOutcomesResearchAndDev',
    'CipLicensesFranchiseFees',
    'CipLicensesTrademarks',
)

TRAVEL_LEAVES: tuple[str, ...] = (
    'TravelPersonalOth',
    'TravelBusinessOth',
    'TravelEducation',
    'TravelShortTermWork',
)

TRANSPORT_INDUSTRY_CODES: tuple[str, ...] = (
    '481000',
    '482000',
    '483000',
    '484000',
    '485000',
    '486000',
    '487000',
    '488000',
    '492000',
    '493000',
    '324110',
)

GOVT_INDUSTRY_CODES: tuple[str, ...] = ('S00500', 'S00600', 'S00102')

HEAD_LEAF_TO_INDUSTRY: dict[str, str] = {
    'FinExplicitAndOth': '523A00',
    'TransportAirPort': '481000',
}


@functools.cache
def _s00300_crosswalk_leaves() -> tuple[str, ...]:
    crosswalk = pd.read_csv(IEA_IMPORTS_CROSSWALK, dtype=str)
    leaves = sorted(
        set(crosswalk.loc[crosswalk['Sector'] == 'S00300', 'Activity'].astype(str))
    )
    if not leaves:
        raise ValueError(f'no S00300 leaves in {IEA_IMPORTS_CROSSWALK}')
    return tuple(leaves)


@functools.cache
def _benchmark_s00300_intermediate() -> pd.Series:
    row = (
        benchmark_intermediate()
        .loc['S00300']
        .reindex(USA_2017_INDUSTRY_CODES)
        .fillna(0.0)
    )
    row.index.name = 'industry'
    return cast(pd.Series, row.astype(float))


@functools.cache
def _benchmark_f02n00_usd() -> float:
    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    return (
        float(pd.to_numeric(use.loc['S00300', 'F02N00'], errors='raise'))
        * MILLION_CURRENCY_TO_CURRENCY
    )


@functools.cache
def license_cap_rate() -> float:
    """Published 2017 ``F02N00`` / sum of ``CipLicenses*`` import leaves."""
    leaves = iea_import_leaves_usd(SEED_YEAR, download_sources_ok=False)
    lic = float(leaves.reindex(CIP_LICENSES_LEAVES).fillna(0).sum())
    if lic <= 0:
        raise ValueError('2017 CipLicenses* leaves sum to zero')
    return _benchmark_f02n00_usd() / lic


def iea_import_leaves_usd(year: int, download_sources_ok: bool = False) -> pd.Series:
    """IEA import ``TypeOfService`` totals for ``S00300`` crosswalk leaves, USD."""
    fba = getFlowByActivity(
        'BEA_IEA',
        year=int(year),
        download_FBA_if_missing=download_sources_ok,
    )
    imports = fba.loc[fba['FlowName'].astype(str) == 'Imports']
    by_type = (
        imports.groupby(imports['ActivityProducedBy'].astype(str))['FlowAmount']
        .sum()
        .astype(float)
    )
    leaves = _s00300_crosswalk_leaves()
    out = by_type.reindex(leaves).fillna(0.0)
    out.index.name = 'type_of_service'
    return out


def s00300_f02n00_usd(year: int, download_sources_ok: bool = False) -> float:
    """``S00300`` x ``F02N00`` from cap rate x annual license import leaves."""
    leaves = iea_import_leaves_usd(year, download_sources_ok)
    return (
        float(leaves.reindex(CIP_LICENSES_LEAVES).fillna(0).sum()) * license_cap_rate()
    )


def _spread_proportional(
    ind: pd.Series,
    pub: pd.Series,
    mass_usd: float,
    *,
    exclude: set[str] | None = None,
) -> pd.Series:
    pool = pub[pub > 0].copy()
    if exclude:
        pool = pool.drop(
            labels=[c for c in exclude if c in pool.index], errors='ignore'
        )
    if mass_usd <= 0 or pool.sum() <= 0:
        return ind
    weights = pool / pool.sum()
    return ind.add(weights * mass_usd, fill_value=0.0)


def _spread_by_published_shares(
    ind: pd.Series,
    pub: pd.Series,
    codes: tuple[str, ...],
    mass_usd: float,
) -> pd.Series:
    subset = pub.reindex(codes).fillna(0.0)
    subset = subset[subset > 0]
    if mass_usd <= 0 or subset.sum() <= 0:
        return ind
    for code, val in (subset / subset.sum() * mass_usd).items():
        ind[code] = ind.get(code, 0.0) + float(val)
    return ind


def build_s00300_intermediate_row(
    year: int,
    intermediate_budget_usd: float,
    *,
    download_sources_ok: bool = False,
) -> pd.Series:
    """Build the ``S00300`` intermediate row for *year*, scaled to *intermediate_budget_usd*."""
    pub = _benchmark_s00300_intermediate()
    ind = pd.Series(0.0, index=list(USA_2017_INDUSTRY_CODES))
    leaves = iea_import_leaves_usd(year, download_sources_ok)

    assigned: set[str] = set()
    for leaf, industry in HEAD_LEAF_TO_INDUSTRY.items():
        mass = float(leaves.get(leaf, 0.0))
        if mass > 0:
            ind[industry] = ind.get(industry, 0.0) + mass
            assigned.add(industry)

    lic_all = float(leaves.reindex(CIP_LICENSES_LEAVES).fillna(0).sum())
    lic_int = lic_all * (1.0 - license_cap_rate())
    ind = _spread_proportional(ind, pub, lic_int, exclude=assigned)
    assigned.update(ind.index[ind > 0])

    govt_mass = float(leaves.get('GovtGoodsAndServicesNie', 0.0))
    ind = _spread_by_published_shares(ind, pub, GOVT_INDUSTRY_CODES, govt_mass)
    assigned.update(c for c in GOVT_INDUSTRY_CODES if ind.get(c, 0) > 0)

    sea_mass = float(leaves.get('TransportSeaFreight', 0.0)) + float(
        leaves.get('TransportSeaPort', 0.0)
    )
    ind = _spread_by_published_shares(ind, pub, TRANSPORT_INDUSTRY_CODES, sea_mass)
    assigned.update(c for c in TRANSPORT_INDUSTRY_CODES if ind.get(c, 0) > 0)

    assigned_leaves = {
        *HEAD_LEAF_TO_INDUSTRY.keys(),
        *CIP_LICENSES_LEAVES,
        *TRAVEL_LEAVES,
        'GovtGoodsAndServicesNie',
        'TransportSeaFreight',
        'TransportSeaPort',
    }
    residual = float(
        leaves.drop(
            labels=[leaf for leaf in leaves.index if leaf in assigned_leaves],
            errors='ignore',
        ).sum()
    )
    ind = _spread_proportional(ind, pub, residual, exclude=assigned)

    total = float(ind.sum())
    if total > 0 and intermediate_budget_usd > 0:
        ind *= intermediate_budget_usd / total
    ind.index.name = 'industry'
    return ind.astype(float)


def s00300_intermediate_budget_usd(
    mcif_s00300_usd: float,
    f01000_usd: float,
    f02n00_usd: float,
) -> float:
    """Intermediate slice of the ``S00300`` Use row given FD columns and ``MCIF``."""
    return float(mcif_s00300_usd) - float(f01000_usd) - float(f02n00_usd)


def s00300_intermediate_budget_from_sources(
    year: int, download_sources_ok: bool = False
) -> float:
    """Annual intermediate budget for ``S00300`` from ``MCIF`` and final demand."""
    from bedrock.transform.eeio.nowcast import derive_initial_Y_pur  # noqa: PLC0415
    from bedrock.transform.iot.nowcast_import_conditioning import (  # noqa: PLC0415
        conditioned_mcif,
    )

    y = derive_initial_Y_pur(year, download_sources_ok=download_sources_ok)
    mcif = float(conditioned_mcif(year, download_sources_ok)['S00300'])
    f01000 = float(pd.to_numeric(y.loc['S00300', 'F01000'], errors='raise'))
    f02n00 = float(s00300_f02n00_usd(year, download_sources_ok))
    return s00300_intermediate_budget_usd(mcif, f01000, f02n00)


def overlay_s00300_intermediate_block(
    block: pd.DataFrame,
    year: int,
    *,
    download_sources_ok: bool = False,
) -> pd.DataFrame:
    """Replace the ``S00300`` row when off the anchor year (#767)."""
    if year == SEED_YEAR:
        return block
    if 'S00300' not in block.index:
        raise KeyError('intermediate block missing S00300 row')
    budget = s00300_intermediate_budget_from_sources(year, download_sources_ok)
    out = block.copy()
    out.loc['S00300', :] = (
        build_s00300_intermediate_row(
            year,
            budget,
            download_sources_ok=download_sources_ok,
        )
        .reindex(out.columns)
        .fillna(0.0)
    )
    return out
