"""Floor each sector's emissions at what its own facilities report to GHGRP (#1060).

GHGRP covers only facilities above 25,000 t CO2e, so the CO2, CH4 and N2O the
FBS attributes to a sector should never fall below what that sector's
facilities report across all subparts (electric power, subpart D, excepted).
Attribution by fuel
shares can fall below it: kiln and furnace fuel reported inside process
subparts (cement H) is invisible to the shares, and the inventory's fuel
totals are spread on survey and Use weights that need not match the
facilities.

The floor is in CO2e (AR6), and each sector below it takes the deficit as
combustion CO2 **in its own fuel mix** across the shared industrial-combustion
fuels (:data:`FUEL_SETS`: coal, natural gas, petroleum): its own combustion
mix in the FBS, else its NAICS-4 then NAICS-3
parent's, else the whole pool's. Each fuel's deficit comes only from that
fuel's activity sets in other sectors whose median facility coverage clears
the attribution gate (0.8). Donors give in proportion to their
slack above their own floor, spread over their fuels, so no donor is pushed
below its floor. Within a fuel, recipients take the activity-set mix the
donors gave, so every activity set's national total is unchanged. Lease and
plant gas and still gas are attributed to specific sectors on purpose and do
not donate. Moved CO2 carries ``AttributionSources = 'GHGRP_sector_floor'``.

GHGRP facility NAICS are matched to the FBS's sector codes by longest prefix.
"""

from __future__ import annotations

import logging

import pandas as pd
import stewi

from bedrock.transform.ghg import facility_coverage as fc
from bedrock.transform.ghg.facility_coverage import _drop_outside_geography
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA

logger = logging.getLogger(__name__)

#: Shared industrial-combustion activity sets the floor moves CO2 within, by
#: fuel. Lease and plant gas and still gas go to specific sectors and stay put.
FUEL_SETS: dict[str, tuple[str, ...]] = {
    'Coal': ('UMD_GHGIA_T_3_11.coal',),
    'Natural Gas': (
        'UMD_GHGIA_T_3_11.ng_manufacturing',
        'UMD_GHGIA_T_3_11.natural_gas_nonmanufacturing',
    ),
    'Petroleum': (
        'UMD_GHGIA_T_3_11.petroleum_facility',
        'UMD_GHGIA_T_3_11.petroleum_use',
    ),
}
FUEL_OF_SET = {meta: fuel for fuel, metas in FUEL_SETS.items() for meta in metas}
FLOOR_ATTRIBUTION = 'GHGRP_sector_floor'
CO2 = 'Carbon dioxide'
#: CO2e weights (IPCC AR6, as in the model). GHGRP methane is fossil; FBS
#: methane is valued at the lower non-fossil weight so the FBS side is never
#: overstated against the floor.
GHGRP_GWP = {
    'Carbon Dioxide': 1.0,
    'Methane': float(GWP100_AR6_CEDA['CH4_fossil']),
    'Nitrous Oxide': float(GWP100_AR6_CEDA['N2O']),
}
FBS_GWP = {
    CO2: 1.0,
    'Methane': float(GWP100_AR6_CEDA['CH4_non_fossil']),
    'Nitrous oxide': float(GWP100_AR6_CEDA['N2O']),
}
#: GHGRP subparts left out: electric power generation.
EXCLUDED_SUBPARTS = frozenset({'D'})
#: Sector scope of the floor (as in the facility FBS); 221100 is out.
SCOPE_PREFIXES = ('21', '22', '31', '32', '33')


def ghgrp_co2e_by_naics(year: int) -> pd.Series:
    """CO2, CH4 and N2O reported to GHGRP by facility NAICS, kg CO2e (AR6;
    biogenic CO2 excluded; all subparts but D)."""
    flows = stewi.getInventory(
        'GHGRP', year=year, stewiformat='flowbyprocess', download_if_missing=True
    )
    flows = flows[
        flows['FlowName'].isin(GHGRP_GWP) & ~flows['Process'].isin(EXCLUDED_SUBPARTS)
    ].copy()
    flows['FlowAmount'] = flows['FlowAmount'] * flows['FlowName'].map(GHGRP_GWP)
    facilities = stewi.getInventoryFacilities('GHGRP', year, download_if_missing=True)[
        ['FacilityID', 'NAICS', 'State']
    ]
    flows = flows.merge(facilities, on='FacilityID', how='left')
    flows = _drop_outside_geography(flows, 'FlowAmount', 'GHGRP floor', year)
    naics = flows['NAICS'].astype(str).str.replace(r'\.0$', '', regex=True)
    return flows.groupby(naics)['FlowAmount'].sum()


def match_to_sectors(by_naics: pd.Series, sectors: pd.Index) -> pd.Series:
    """Sum NAICS-keyed values onto the longest FBS sector code each starts with."""
    codes = sorted({str(s) for s in sectors if s}, key=len, reverse=True)
    out: dict[str, float] = {}
    unmatched = 0.0
    for naics, value in by_naics.items():
        hit = next((c for c in codes if str(naics).startswith(c)), None)
        if hit is None:
            unmatched += float(value)
            continue
        out[hit] = out.get(hit, 0.0) + float(value)
    if unmatched:
        logger.info(
            'GHGRP floor: %.1f Mt CO2 on NAICS with no FBS sector', unmatched / 1e9
        )
    return pd.Series(out, dtype=float)


def fuel_mix(pool: pd.DataFrame, sector: str) -> pd.Series:
    """*sector*'s fuel mix in *pool* (sector x fuel CO2), summing to 1.

    Its own mix, else its NAICS-4 then NAICS-3 parent's, else the whole pool's.
    """
    index = pool.index.astype(str)
    for rows in (
        pool[index == sector],
        pool[index.str.startswith(sector[:4])],
        pool[index.str.startswith(sector[:3])],
        pool,
    ):
        total = rows.sum()
        if total.sum() > 0:
            return total / total.sum()
    raise ValueError('GHGRP floor: the combustion pool is empty')


def well_covered_sectors(codes: pd.Index) -> pd.Index:
    """FBS sector codes whose facility coverage clears the attribution gate.

    Coverage is the sector's median over :data:`fc.MODE_FREEZE_YEARS`, as the
    #1040 mode freeze uses it, against :data:`fc.ATTRIBUTION_MIN_COVERAGE`.
    Sectors with no facility combustion have no coverage and are not eligible.
    """
    panel = pd.concat(
        [
            fc.load_or_build_coverage_bands(y).set_index('sector')['coverage']
            for y in fc.MODE_FREEZE_YEARS
        ],
        axis=1,
    )
    median = panel.median(axis=1)
    bea = pd.Series([fc.bea_detail_for_naics(c) for c in codes], index=codes)
    return codes[bea.map(median).fillna(0.0).to_numpy() >= fc.ATTRIBUTION_MIN_COVERAGE]


def floor_moves(
    sector_co2: pd.Series,
    floor: pd.Series,
    pool: pd.DataFrame,
    eligible: pd.Index | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """CO2 to add to recipients and to take from donors, both sector x fuel, kg.

    *sector_co2* is each sector's FBS CO2, *floor* its GHGRP CO2, *pool* its CO2
    in the shared combustion fuels (sector x fuel). A recipient's deficit is
    split by :func:`fuel_mix`. A donor can give at most its slack above its own
    floor, spread over its fuels in its own mix; each fuel's need is taken
    from the donors in proportion to that capacity. Where a fuel's donors
    cannot cover its need (industrial coal sits almost entirely with GHGRP
    reporters already near their floors), the rest of each recipient's need
    in that fuel moves to the fuels with spare capacity, in proportion to the
    spare. Only sectors in *eligible* (all, if None) can donate. Raises only
    if all fuels together cannot cover the deficits.
    """
    pool = pool.reindex(columns=list(FUEL_SETS)).fillna(0.0)
    sectors = sector_co2.index.union(floor.index).union(pool.index)
    have = sector_co2.reindex(sectors).fillna(0.0)
    need = floor.reindex(sectors).fillna(0.0)
    deficit = (need - have).clip(lower=0.0)
    deficit = deficit[deficit > 0]
    if deficit.empty:
        empty = pd.DataFrame(columns=pool.columns, dtype=float)
        return empty, empty
    add = pd.DataFrame(
        {s: fuel_mix(pool, str(s)) * d for s, d in deficit.items()}
    ).T.reindex(columns=pool.columns)

    donors = pool.drop(index=deficit.index, errors='ignore')
    donors = donors[donors.sum(axis=1) > 0]
    if eligible is not None:
        donors = donors[donors.index.isin(eligible)]
    slack = (have - need).clip(lower=0.0).reindex(donors.index)
    share = (slack / donors.sum(axis=1)).clip(upper=1.0)
    capacity = donors.mul(share, axis=0)
    wanted = add.sum()
    available = capacity.sum().reindex(pool.columns).fillna(0.0)
    if wanted.sum() > available.sum():
        raise ValueError(
            f'GHGRP floor: deficits {wanted.sum() / 1e9:.2f} Mt exceed donor '
            f'capacity {available.sum() / 1e9:.2f} Mt across all fuels'
        )
    # Fuels whose donors fall short: keep what they can cover, move the rest.
    covered = (available / wanted.where(wanted > 0)).clip(upper=1.0).fillna(1.0)
    moved = add.mul(1.0 - covered, axis=1).sum(axis=1)
    add = add.mul(covered, axis=1)
    spare = (available - add.sum()).clip(lower=0.0)
    if moved.sum() > 0:
        logger.info(
            'GHGRP floor: fuel pools short by %s Mt; moved to fuels with spare '
            'capacity',
            ((wanted - add.sum()) / 1e9).round(2).to_dict(),
        )
        add = add + pd.DataFrame(
            {f: moved * (spare[f] / spare.sum()) for f in pool.columns}
        )
    give = capacity.mul(
        (add.sum() / available.where(available > 0)).fillna(0.0), axis=1
    )
    return add, give[give.sum(axis=1) > 0]


def apply_ghgrp_sector_floor(fbs: pd.DataFrame, year: int) -> pd.DataFrame:
    """Return *fbs* with every in-scope sector at or above its GHGRP CO2e."""
    sector = fbs['SectorProducedBy'].astype(str)
    co2 = fbs['Flowable'] == CO2
    in_scope = sector.str[:2].isin(SCOPE_PREFIXES) & ~sector.str.startswith('2211')
    fuel = fbs['MetaSources'].astype(str).map(FUEL_OF_SET)
    pool_rows = co2 & fuel.notna()

    # The floor is CO2e (CO2, CH4, N2O); the deficit is filled with CO2.
    weight = fbs['Flowable'].map(FBS_GWP)
    ghg = weight.notna() & in_scope
    sector_co2 = (fbs.loc[ghg, 'FlowAmount'] * weight[ghg]).groupby(sector[ghg]).sum()
    floor = match_to_sectors(ghgrp_co2e_by_naics(year), pd.Index(sector_co2.index))
    pool = (
        fbs.loc[pool_rows]
        .groupby([sector[pool_rows], fuel[pool_rows]])['FlowAmount']
        .sum()
        .unstack(fill_value=0.0)
    )
    # Donors: only sectors whose facility coverage clears the attribution
    # gate, where emissions well above GHGRP are the likeliest over-attribution.
    eligible = well_covered_sectors(pd.Index(pool.index.astype(str)))
    add, give = floor_moves(sector_co2, floor, pool, eligible)
    if add.empty:
        logger.info('GHGRP floor %d: no sector below its GHGRP CO2', year)
        return fbs

    # Remove from donors: scale each donor's rows of a fuel by one factor.
    out = fbs.copy()
    pool_full = pool.reindex(columns=list(FUEL_SETS)).fillna(0.0)
    factor = (1.0 - give / pool_full.reindex(index=give.index)).stack()
    donor = pool_rows & sector.isin(give.index)
    keys = pd.MultiIndex.from_arrays([sector[donor], fuel[donor]])
    scale = pd.Series(
        factor.reindex(keys).fillna(1.0).to_numpy(), index=fbs.index[donor]
    )
    removed = fbs.loc[donor, 'FlowAmount'] * (1.0 - scale)
    out.loc[donor, 'FlowAmount'] = fbs.loc[donor, 'FlowAmount'] * scale

    # Add to recipients: each fuel in the activity-set mix its donors gave.
    meta_mix = removed.groupby(fbs.loc[donor, 'MetaSources']).sum()
    template = (
        fbs.loc[pool_rows].drop_duplicates('MetaSources').set_index('MetaSources')
    )
    added = []
    for fuel_name, metas in FUEL_SETS.items():
        given = meta_mix.reindex(list(metas)).fillna(0.0)
        if given.sum() <= 0:
            continue
        parts = given / given.sum()
        for recipient, amount in add[fuel_name].items():
            if amount <= 0:
                continue
            for meta, part in parts.items():
                if part <= 0:
                    continue
                row = template.loc[meta].copy()
                row['MetaSources'] = meta
                row['SectorProducedBy'] = recipient
                row['FlowAmount'] = amount * part
                row['AttributionSources'] = FLOOR_ATTRIBUTION
                added.append(row)
    out = pd.concat([out, pd.DataFrame(added)], ignore_index=True)

    report = (add / 1e9).assign(total=lambda d: d.sum(axis=1))
    logger.info(
        'GHGRP floor %d: moved %.1f Mt CO2 into %d sectors from %d donors '
        '(Mt, by fuel):\n%s',
        year,
        report['total'].sum(),
        len(add),
        len(give),
        report.sort_values('total', ascending=False).round(2).to_string(),
    )
    return out
