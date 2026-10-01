"""Floor each sector's CO2 at what its own facilities report to GHGRP (#1060).

GHGRP covers only facilities above 25,000 t CO2e, so the CO2 the FBS attributes
to a sector should never fall below what that sector's facilities report
across all subparts (electric power, subpart D, excepted). Attribution by fuel
shares can fall below it: kiln and furnace fuel reported inside process
subparts (cement H) is invisible to the shares, and the inventory's fuel
totals are spread on survey and Use weights that need not match the
facilities.

For each sector below its floor, the deficit is moved into the sector from
the industrial stationary-combustion pool (:data:`POOL_PREFIX` activity sets)
of the other sectors. Each donor gives in proportion to its slack above its
own floor (its pool CO2 where it has no GHGRP reports), so no donor is pushed
below its floor. Recipients take the deficit across the pool's activity sets
in the pool's own proportions, so every activity set's national total is
unchanged. Moved CO2 carries ``AttributionSources = 'GHGRP_sector_floor'``.

GHGRP facility NAICS are matched to the FBS's sector codes by longest prefix.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import stewi

from bedrock.transform.ghg.facility_coverage import _drop_outside_geography

logger = logging.getLogger(__name__)

#: Activity sets the floor moves CO2 within: industrial stationary combustion.
POOL_PREFIX = 'UMD_GHGIA_T_3_11'
FLOOR_ATTRIBUTION = 'GHGRP_sector_floor'
CO2 = 'Carbon dioxide'
#: GHGRP subparts left out: electric power generation.
EXCLUDED_SUBPARTS = frozenset({'D'})
#: Sector scope of the floor (as in the facility FBS); 221100 is out.
SCOPE_PREFIXES = ('21', '22', '31', '32', '33')


def ghgrp_co2_by_naics(year: int) -> pd.Series:
    """Fossil CO2 reported to GHGRP by facility NAICS, kg (all subparts but D)."""
    flows = stewi.getInventory(
        'GHGRP', year=year, stewiformat='flowbyprocess', download_if_missing=True
    )
    flows = flows[
        (flows['FlowName'] == 'Carbon Dioxide')
        & ~flows['Process'].isin(EXCLUDED_SUBPARTS)
    ]
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


def floor_moves(
    sector_co2: pd.Series, floor: pd.Series, pool_co2: pd.Series
) -> tuple[pd.Series, pd.Series]:
    """Deficits to add and donor amounts to remove, both kg CO2 by sector.

    *sector_co2* is each sector's FBS CO2, *floor* its GHGRP CO2, *pool_co2* its
    CO2 in the pool. Donors give in proportion to min(pool CO2, slack above
    their own floor); raises if the pool cannot cover the deficits.
    """
    sectors = sector_co2.index.union(floor.index).union(pool_co2.index)
    have = sector_co2.reindex(sectors).fillna(0.0)
    need = floor.reindex(sectors).fillna(0.0)
    pool = pool_co2.reindex(sectors).fillna(0.0)
    deficit = (need - have).clip(lower=0.0)
    deficit = deficit[deficit > 0]
    slack = (have - need).clip(lower=0.0)
    capacity = np.minimum(pool, slack).drop(index=deficit.index, errors='ignore')
    capacity = capacity[capacity > 0]
    total = float(deficit.sum())
    if total == 0:
        return deficit, pd.Series(dtype=float)
    if capacity.sum() < total:
        raise ValueError(
            f'GHGRP floor: deficits {total / 1e9:.1f} Mt exceed donor capacity '
            f'{capacity.sum() / 1e9:.1f} Mt in the {POOL_PREFIX} pool'
        )
    return deficit, capacity / capacity.sum() * total


def apply_ghgrp_sector_floor(fbs: pd.DataFrame, year: int) -> pd.DataFrame:
    """Return *fbs* with every in-scope sector's CO2 at or above its GHGRP CO2."""
    sector = fbs['SectorProducedBy'].astype(str)
    co2 = fbs['Flowable'] == CO2
    in_scope = sector.str[:2].isin(SCOPE_PREFIXES) & ~sector.str.startswith('2211')
    pool_rows = co2 & fbs['MetaSources'].astype(str).str.startswith(POOL_PREFIX)

    sector_co2 = fbs.loc[co2 & in_scope].groupby(sector)['FlowAmount'].sum()
    floor = match_to_sectors(ghgrp_co2_by_naics(year), pd.Index(sector_co2.index))
    pool_co2 = fbs.loc[pool_rows].groupby(sector)['FlowAmount'].sum()
    deficit, give = floor_moves(sector_co2, floor, pool_co2)
    if deficit.empty:
        logger.info('GHGRP floor %d: no sector below its GHGRP CO2', year)
        return fbs

    # Remove from donors, scaling each donor's pool rows by the same factor.
    out = fbs.copy()
    scale = 1.0 - (give / pool_co2.reindex(give.index))
    donor = pool_rows & sector.isin(give.index)
    removed = fbs.loc[donor, 'FlowAmount'] * (1.0 - sector[donor].map(scale))
    out.loc[donor, 'FlowAmount'] = out.loc[donor, 'FlowAmount'] * sector[donor].map(
        scale
    )

    # Add to recipients in the activity-set mix the donors gave, so each
    # activity set's national total is unchanged.
    pool_mix = removed.groupby(fbs.loc[donor, 'MetaSources']).sum()
    pool_mix = pool_mix / pool_mix.sum()
    template = (
        fbs.loc[pool_rows].drop_duplicates('MetaSources').set_index('MetaSources')
    )
    added = []
    for recipient, amount in deficit.items():
        for meta, share in pool_mix.items():
            row = template.loc[meta].copy()
            row['MetaSources'] = meta
            row['SectorProducedBy'] = recipient
            row['FlowAmount'] = amount * share
            row['AttributionSources'] = FLOOR_ATTRIBUTION
            added.append(row)
    out = pd.concat([out, pd.DataFrame(added)], ignore_index=True)

    logger.info(
        'GHGRP floor %d: moved %.1f Mt CO2 into %d sectors from %d donors:\n%s',
        year,
        deficit.sum() / 1e9,
        len(deficit),
        len(give),
        (deficit / 1e9).sort_values(ascending=False).round(2).to_string(),
    )
    return out
