"""Bound each sector's emissions by what its own facilities report to GHGRP (#1060).

**Floor.** GHGRP covers only facilities above 25,000 t CO2e, so the CO2, CH4
and N2O the FBS attributes to a sector should never fall below what that
sector's facilities report across all subparts (electric power, subpart D,
excepted). Attribution by fuel shares can fall below it: kiln and furnace fuel
reported inside process subparts (cement H) is invisible to the shares, and
the inventory's fuel totals are spread on survey and Use weights that need
not match the facilities.

**Ceiling.** Where a sector's facility coverage is essentially complete (median
over 2017-2024 at least :data:`fc.VECTOR_COVERAGE_FLOOR`), GHGRP is the whole
sector, so its comparable CO2 should not exceed it either. Comparable CO2 is
the sector's FBS CO2 without Use-table petroleum (on-site vehicles and
equipment) and non-energy use, which GHGRP does not report; methane gets no
ceiling, since the inventory's oil and gas leak estimates legitimately exceed
GHGRP. Only the sector's share of the shared combustion pools is cut; the
inventory's process attributions stay where they are.

Both move CO2 within the shared industrial-combustion fuels
(:data:`FUEL_SETS`: coal, natural gas, petroleum), so every activity set's
national total is unchanged:

1. capped sectors above their ceiling give up the excess, in their own mix;
2. sectors below their floor take their deficit as combustion CO2 **in their
   own fuel mix** (own, else NAICS-4, NAICS-3, else the pool's): first from
   the CO2 the ceiling released, in the same fuel, then from donor sectors
   whose median coverage clears the attribution gate (0.8) and are not
   capped, each giving in proportion to its slack above its own floor. Where
   a fuel cannot cover its need, the rest moves to fuels with spare capacity.
   Capped sectors take the floor per gas: their comparable CO2 is pinned to
   their GHGRP CO2, and any CH4/N2O shortfall is reported, not filled with CO2;
3. released CO2 the floor did not use goes to in-scope sectors whose median
   coverage is below the attribution gate (mostly small facilities GHGRP
   cannot see), in proportion to their combustion in that fuel.

Within a fuel, recipients take the activity-set mix that was removed. Lease and
plant gas, still gas and hydrogen gas are attributed to specific sectors on
purpose and are not touched. Added CO2 carries ``AttributionSources`` of
``GHGRP_sector_floor`` or ``GHGRP_sector_ceiling``.

GHGRP facility NAICS are matched to the FBS's sector codes by longest prefix.
"""

from __future__ import annotations

import logging
from typing import cast

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
CEILING_ATTRIBUTION = 'GHGRP_sector_ceiling'
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
#: FBS CO2 left out of the ceiling comparison: GHGRP does not report on-site
#: vehicle and equipment fuel (Use-table petroleum) or non-energy use.
NOT_COMPARABLE_ATTRIBUTION = 'Nowcast_Detail_Use'
NOT_COMPARABLE_META = 'UMD_GHGIA_T_3_14'


def ghgrp_co2e_by_naics(year: int, gases: dict[str, float] | None = None) -> pd.Series:
    """GHGRP emissions by facility NAICS, kg CO2e (AR6; biogenic CO2 excluded;
    all subparts but D). *gases* maps GHGRP flow names to weights (default:
    CO2, CH4 and N2O)."""
    gases = gases or GHGRP_GWP
    flows = stewi.getInventory(
        'GHGRP', year=year, stewiformat='flowbyprocess', download_if_missing=True
    )
    flows = flows[
        flows['FlowName'].isin(gases) & ~flows['Process'].isin(EXCLUDED_SUBPARTS)
    ].copy()
    flows['FlowAmount'] = flows['FlowAmount'] * flows['FlowName'].map(gases)
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


def median_coverage(codes: pd.Index) -> pd.Series:
    """Each FBS sector code's facility coverage, median over the freeze span.

    As the #1040 mode freeze uses it. Sectors with no facility combustion have
    no coverage and get 0.
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
    # Guard (#1061 review): a code that does not resolve to a BEA sector, or
    # whose sector has no coverage row, gets 0 and so can neither be capped
    # nor donate. Say so rather than let it pass silently.
    unmapped = sorted(str(c) for c in bea.index[bea.isna()])
    no_row = sorted(str(c) for c in bea.index[bea.notna() & ~bea.isin(median.index)])
    if unmapped:
        logger.warning(
            'GHGRP floor: %d sector codes do not resolve to a BEA sector and get '
            'coverage 0: %s',
            len(unmapped),
            unmapped,
        )
    if no_row:
        logger.info(
            'GHGRP floor: %d sector codes have no facility coverage row (no '
            'facility combustion) and get coverage 0: %s',
            len(no_row),
            no_row,
        )
    return bea.map(median).fillna(0.0)


def floor_moves(
    sector_co2: pd.Series,
    floor: pd.Series,
    pool: pd.DataFrame,
    eligible: pd.Index | None = None,
    first: pd.Series | None = None,
    co2_slack: pd.Series | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    """CO2 to add to recipients and to take from donors, both sector x fuel, kg.

    *sector_co2* is each sector's FBS CO2, *floor* its GHGRP CO2, *pool* its CO2
    in the shared combustion fuels (sector x fuel). A recipient's deficit is
    split by :func:`fuel_mix`. *first* (by fuel) is CO2 already released, used
    before any donor. A donor can give at most its slack above its own floor,
    spread over its fuels in its own mix; each fuel's remaining need is taken
    from the donors in proportion to that capacity. Where a fuel cannot cover
    its need (industrial coal sits almost entirely with GHGRP reporters already
    near their floors), the rest of each recipient's need in that fuel moves to
    the fuels with spare capacity, in proportion to the spare. Only sectors in
    *eligible* (all, if None) can donate. *co2_slack* (by sector) further caps
    each donor at its CO2 above its own GHGRP CO2, so giving CO2 never takes a
    donor below its GHGRP CO2 while its methane carries the CO2e slack. Raises
    only if all fuels together cannot cover the deficits. Returns (add, give,
    used from *first*).
    """
    pool = pool.reindex(columns=list(FUEL_SETS)).fillna(0.0)
    first = (first if first is not None else pd.Series(dtype=float)).reindex(
        pool.columns
    )
    first = first.fillna(0.0)
    sectors = sector_co2.index.union(floor.index).union(pool.index)
    have = sector_co2.reindex(sectors).fillna(0.0)
    need = floor.reindex(sectors).fillna(0.0)
    deficit = (need - have).clip(lower=0.0)
    deficit = deficit[deficit > 0]
    if deficit.empty:
        empty = pd.DataFrame(columns=pool.columns, dtype=float)
        return empty, empty, first * 0.0
    add = pd.DataFrame(
        {s: fuel_mix(pool, str(s)) * d for s, d in deficit.items()}
    ).T.reindex(columns=pool.columns)

    donors = pool.drop(index=deficit.index, errors='ignore')
    donors = donors[donors.sum(axis=1) > 0]
    if eligible is not None:
        donors = donors[donors.index.isin(eligible)]
    slack = (have - need).clip(lower=0.0).reindex(donors.index)
    if co2_slack is not None:
        slack = pd.concat(
            [slack, co2_slack.reindex(donors.index).fillna(0.0).clip(lower=0.0)],
            axis=1,
        ).min(axis=1)
    share = (slack / donors.sum(axis=1)).clip(upper=1.0)
    capacity = donors.mul(share, axis=0)
    wanted = add.sum()
    available = first + capacity.sum().reindex(pool.columns).fillna(0.0)
    if wanted.sum() > available.sum():
        raise ValueError(
            f'GHGRP floor: deficits {wanted.sum() / 1e9:.2f} Mt exceed donor '
            f'capacity {available.sum() / 1e9:.2f} Mt across all fuels'
        )
    # Fuels that fall short: keep what they can cover, move the rest.
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
    used = pd.concat([add.sum(), first], axis=1).min(axis=1)
    from_donors = add.sum() - used
    donor_capacity = capacity.sum().reindex(pool.columns).fillna(0.0)
    give = capacity.mul(
        (from_donors / donor_capacity.where(donor_capacity > 0)).fillna(0.0), axis=1
    )
    return add, give[give.sum(axis=1) > 0], used


def _scale_rows(
    out: pd.DataFrame,
    rows: pd.Series,
    keys: pd.MultiIndex,
    factor: pd.Series,
) -> pd.Series:
    """Scale *rows* of *out* in place by *factor* (keyed like *keys*); return
    the amount removed from each row."""
    scale = pd.Series(
        factor.reindex(keys).fillna(1.0).to_numpy(), index=out.index[rows]
    )
    removed = out.loc[rows, 'FlowAmount'] * (1.0 - scale)
    out.loc[rows, 'FlowAmount'] = out.loc[rows, 'FlowAmount'] * scale
    return removed


def apply_ghgrp_sector_floor(fbs: pd.DataFrame, year: int) -> pd.DataFrame:
    """Return *fbs* with every in-scope sector at or above its GHGRP CO2e, and
    every fully covered sector's comparable CO2 at or below its GHGRP CO2."""
    out = fbs.copy()
    sector = out['SectorProducedBy'].astype(str)
    co2 = out['Flowable'] == CO2
    in_scope = sector.str[:2].isin(SCOPE_PREFIXES) & ~sector.str.startswith('2211')
    fuel = out['MetaSources'].astype(str).map(FUEL_OF_SET)
    pool_rows = co2 & fuel.notna()

    def pool_table() -> pd.DataFrame:
        return (
            out.loc[pool_rows]
            .groupby([sector[pool_rows], fuel[pool_rows]])['FlowAmount']
            .sum()
            .unstack(fill_value=0)
            .reindex(columns=list(FUEL_SETS))
            .fillna(0.0)
        )

    pool = pool_table()
    codes = pd.Index(sector[in_scope].unique())
    coverage = median_coverage(codes)
    capped = coverage.index[coverage >= fc.VECTOR_COVERAGE_FLOOR]
    eligible = coverage.index[
        (coverage >= fc.ATTRIBUTION_MIN_COVERAGE)
        & (coverage < fc.VECTOR_COVERAGE_FLOOR)
    ]
    low = coverage.index[coverage < fc.ATTRIBUTION_MIN_COVERAGE]

    # 1. Ceiling: trim capped sectors' comparable CO2 to their GHGRP CO2.
    comparable = (
        co2
        & in_scope
        & ~out['AttributionSources']
        .astype(str)
        .str.startswith(NOT_COMPARABLE_ATTRIBUTION)
        & ~out['MetaSources'].astype(str).str.startswith(NOT_COMPARABLE_META)
    )
    comparable_co2 = out.loc[comparable].groupby(sector[comparable])['FlowAmount'].sum()
    ceiling = match_to_sectors(
        ghgrp_co2e_by_naics(year, {'Carbon Dioxide': 1.0}),
        pd.Index(comparable_co2.index),
    )
    over = (comparable_co2 - ceiling.reindex(comparable_co2.index)).clip(lower=0.0)
    over = over[over.index.isin(capped) & ceiling.reindex(over.index).gt(0)]
    # Cut only comparable combustion: Use-table petroleum stays (GHGRP does
    # not see on-site vehicles and equipment).
    cut_rows = pool_rows & comparable
    cuttable = out.loc[cut_rows].groupby(sector[cut_rows])['FlowAmount'].sum()
    excess = pd.concat([over, cuttable.reindex(over.index)], axis=1).min(axis=1)
    excess = excess[excess > 0]
    keep = 1.0 - excess / cuttable.reindex(excess.index)
    trim = cast(
        pd.Series, pd.DataFrame({f: keep for f in FUEL_SETS}, index=keep.index).stack()
    )
    rows = cut_rows & sector.isin(excess.index)
    removed = [
        _scale_rows(
            out, rows, pd.MultiIndex.from_arrays([sector[rows], fuel[rows]]), trim
        )
    ]
    released = removed[0].groupby(fuel[rows]).sum().reindex(list(FUEL_SETS)).fillna(0.0)

    # 2. Floor: deficits filled from released CO2 first, then from donors.
    pool = pool_table()
    weight = out['Flowable'].map(FBS_GWP)
    ghg = weight.notna() & in_scope
    sector_co2e = (out.loc[ghg, 'FlowAmount'] * weight[ghg]).groupby(sector[ghg]).sum()
    floor = match_to_sectors(ghgrp_co2e_by_naics(year), pd.Index(sector_co2e.index))
    # Fully covered sectors take the floor per gas: their comparable CO2 is
    # pinned to their GHGRP CO2 from both sides, and a CH4/N2O shortfall (e.g.
    # biomass combustion at paper mills) is reported, not filled with CO2.
    pinned = ceiling.index.intersection(capped)
    comparable_after = (
        out.loc[comparable].groupby(sector[comparable])['FlowAmount'].sum()
    )
    have = sector_co2e.copy()
    have.loc[pinned] = comparable_after.reindex(pinned).fillna(0.0)
    floor.loc[pinned] = ceiling.reindex(pinned)
    # Donors give CO2, so they are also held at their GHGRP CO2 (comparable).
    co2_slack = comparable_after - ceiling.reindex(comparable_after.index).fillna(0.0)
    add, give, used = floor_moves(have, floor, pool, eligible, released, co2_slack)
    if not give.empty:
        factor = cast(pd.Series, (1.0 - give / pool.reindex(index=give.index)).stack())
        rows = pool_rows & sector.isin(give.index)
        removed.append(
            _scale_rows(
                out, rows, pd.MultiIndex.from_arrays([sector[rows], fuel[rows]]), factor
            )
        )

    # 3. Released CO2 the floor did not use: to low-coverage sectors.
    leftover = (released - used.reindex(released.index).fillna(0.0)).clip(lower=0.0)
    spread = pd.DataFrame(0.0, index=pd.Index([], dtype=str), columns=list(FUEL_SETS))
    if leftover.sum() > 0:
        takers = pool[pool.index.isin(low)]
        if takers.sum().sum() <= 0:
            takers = pool[~pool.index.isin(capped)]
        shares = takers.div(takers.sum().where(takers.sum() > 0), axis=1).fillna(0.0)
        spread = shares.mul(leftover, axis=1)
        missing = leftover[(takers.sum() <= 0) & (leftover > 0)]
        if missing.sum() > 0:
            raise ValueError(
                f'GHGRP ceiling: no low-coverage sector burns {list(missing.index)}'
            )

    # Add, per fuel, in the activity-set mix that was removed.
    meta = out['MetaSources']
    meta_mix = (
        pd.concat(removed).groupby(meta).sum() if removed else pd.Series(dtype=float)
    )
    template = (
        out.loc[pool_rows].drop_duplicates('MetaSources').set_index('MetaSources')
    )
    added = []
    for table, label in ((add, FLOOR_ATTRIBUTION), (spread, CEILING_ATTRIBUTION)):
        for fuel_name, metas in FUEL_SETS.items():
            given = meta_mix.reindex(list(metas)).fillna(0.0)
            if given.sum() <= 0 or fuel_name not in table:
                continue
            parts = given / given.sum()
            for recipient, amount in table[fuel_name].items():
                if amount <= 0:
                    continue
                for meta_name, part in parts.items():
                    if part <= 0:
                        continue
                    row = template.loc[meta_name].copy()
                    row['MetaSources'] = meta_name
                    row['SectorProducedBy'] = recipient
                    row['FlowAmount'] = amount * part
                    row['AttributionSources'] = label
                    added.append(row)
    if added:
        out = pd.concat([out, pd.DataFrame(added)], ignore_index=True)

    logger.info(
        'GHGRP ceiling %d: trimmed %.1f Mt CO2 from %d fully covered sectors:\n%s',
        year,
        excess.sum() / 1e9,
        len(excess),
        (excess / 1e9).sort_values(ascending=False).round(2).to_string(),
    )
    report = (add / 1e9).assign(total=lambda d: d.sum(axis=1))
    logger.info(
        'GHGRP floor %d: moved %.1f Mt CO2 into %d sectors (%.1f Mt from the '
        'ceiling, the rest from %d donors); %.1f Mt to low-coverage sectors '
        '(Mt, by fuel):\n%s',
        year,
        report['total'].sum() if not report.empty else 0.0,
        len(add),
        used.sum() / 1e9,
        len(give),
        spread.to_numpy().sum() / 1e9,
        report.sort_values('total', ascending=False).round(2).to_string(),
    )
    return out
