"""Sector emissions in a GHG FBS against what the sector's own facilities report to GHGRP.

GHGRP covers only facilities above 25,000 t CO2e, so the emissions the FBS
attributes to a sector should not fall below what that sector's facilities
report across all subparts. A sector-year below that bound is under-attributed:
#1060 found cement, lime, glass and iron and steel there in the v0.5 release
FBS, with their kiln and furnace fuel moved to other sectors.

Both sides are CO2, CH4 and N2O in CO2e (IPCC AR6, as in the model; biogenic
CO2 is excluded), on BEA detail sectors in the facility scope (21, 22 without
electric power, 31-33). The FBS side is split by attribution class: process
emissions the inventory names (``direct``), fuel combustion spread by the
energy survey or facility data, fuel spread by the Use table, and the rest.
The GHGRP side is split into general combustion (subpart C), oil and gas
(subpart W) and the source-category subparts (H, S, N, Q, Y, ...).

A sector's facilities can also report emissions the inventory attributes
elsewhere (a hydrogen plant inside a refinery, gathering and boosting under
an extraction NAICS), so a flag is a sector to read, not a verdict.

::

    uv run python -m bedrock.analysis.time_series_B_matrix.fbs_vs_ghgrp_totals \\
        --vintage v0.3.0_0e1b0f0 --label release_v0.5
    uv run python -m bedrock.analysis.time_series_B_matrix.fbs_vs_ghgrp_totals \\
        --vintage v0.3.0_99655e9 --stem GHG_national_Cornerstone_nowcast_{year} \\
        --config 2025_usa_cornerstone_v0_4_nowcast_{year} --label v0.4 --check
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import stewi

import bedrock.analysis.time_series_B_matrix.B_change_diagnostics as bcd
from bedrock.extract.stewifbs.facility_combustion import (
    FACILITY_SCOPE_PREFIXES,
    GHGRP_FLOW_MAP,
)
from bedrock.transform.ghg import facility_coverage as fc
from bedrock.utils.config.config_controllers import temp_usa_config
from bedrock.utils.emissions.gwp import GWP100_AR6_CEDA

logger = logging.getLogger(__name__)

YEARS = tuple(range(2017, 2025))
FACILITY_STEM = 'GHG_national_Cornerstone_nowcast_facilities_{year}'
V05_CONFIG = '2025_usa_cornerstone_v0_5_{year}'
#: GHGRP subparts not compared: D is electric power, outside the scope.
EXCLUDED_SUBPARTS = frozenset({'D'})
#: Model gases (after ``fbs_to_co2e``) that GHGRP also reports.
GHGRP_GASES = ('CO2', 'CH4_fossil', 'CH4_non_fossil', 'N2O')
#: FBS attribution classes grouped for the report.
FBS_GROUP = {
    'direct': 'fbs_process',
    'energy_survey': 'fbs_combustion',
    'facility_hybrid': 'fbs_combustion',
    'facility_direct': 'fbs_combustion',
    'io_use_table': 'fbs_use_table',
}
OUT_COLUMNS = [
    'sector',
    'year',
    'ghgrp_Mt',
    'ghgrp_C_Mt',
    'ghgrp_W_Mt',
    'ghgrp_process_subparts_Mt',
    'fbs_Mt',
    'fbs_process_Mt',
    'fbs_combustion_Mt',
    'fbs_use_table_Mt',
    'fbs_other_Mt',
    'fbs_over_ghgrp',
    'below_ghgrp',
]


def in_scope(sector: pd.Series) -> pd.Series:
    s = sector.astype(str)
    return s.str[:2].isin(FACILITY_SCOPE_PREFIXES) & (s != '221100')


def ghgrp_by_sector(year: int) -> pd.DataFrame:
    """GHGRP CO2e by BEA detail sector and subpart group, Mt (all but subpart D)."""
    gwp = {
        'CO2': 1.0,
        'CH4': GWP100_AR6_CEDA['CH4_fossil'],
        'N2O': GWP100_AR6_CEDA['N2O'],
    }
    flows = stewi.getInventory(
        'GHGRP', year=year, stewiformat='flowbyprocess', download_if_missing=True
    )
    flows = flows[
        ~flows['Process'].isin(EXCLUDED_SUBPARTS)
        & flows['FlowName'].isin(GHGRP_FLOW_MAP)
    ].copy()
    flows['CO2e'] = flows['FlowAmount'] * flows['FlowName'].map(GHGRP_FLOW_MAP).map(gwp)
    facilities = stewi.getInventoryFacilities('GHGRP', year, download_if_missing=True)[
        ['FacilityID', 'NAICS', 'State']
    ]
    flows = flows.merge(facilities, on='FacilityID', how='left')
    flows = fc._drop_outside_geography(flows, 'CO2e', 'GHGRP all subparts', year)
    to_bea = {n: fc.bea_detail_for_naics(n) for n in flows['NAICS'].dropna().unique()}
    flows['sector'] = flows['NAICS'].map(to_bea)
    flows['group'] = (
        flows['Process']
        .map({'C': 'ghgrp_C_Mt', 'W': 'ghgrp_W_Mt'})
        .fillna('ghgrp_process_subparts_Mt')
    )
    resolved = flows.dropna(subset=['sector'])
    unresolved = flows.loc[flows['sector'].isna(), 'CO2e'].sum() / 1e9
    if unresolved > 0:
        logger.info(
            'GHGRP %d: %.1f Mt on NAICS with no single BEA sector', year, unresolved
        )
    out = (
        resolved.pivot_table(
            index='sector',
            columns='group',
            values='CO2e',
            aggfunc='sum',
            fill_value=0.0,
        )
        / 1e9
    )
    for col in ('ghgrp_C_Mt', 'ghgrp_W_Mt', 'ghgrp_process_subparts_Mt'):
        if col not in out:
            out[col] = 0.0
    out['ghgrp_Mt'] = out.sum(axis=1)
    return out


def fbs_by_sector(year: int, vintage: str, config: str) -> pd.DataFrame:
    """FBS CO2e (GHGRP gases) by sector and attribution group, Mt."""
    with temp_usa_config(config.format(year=year)):
        co2e = bcd.fbs_to_co2e(bcd.load_fbs(year, vintage))
    co2e = co2e[co2e['Flowable'].isin(GHGRP_GASES)]
    group = (
        co2e['AttributionSources']
        .map(bcd.classify_attribution)
        .map(FBS_GROUP)
        .fillna('fbs_other')
    )
    out = (
        co2e.assign(
            group=group + '_Mt', sector=co2e['SectorProducedBy'].astype(str)
        ).pivot_table(
            index='sector',
            columns='group',
            values='CO2e',
            aggfunc='sum',
            fill_value=0.0,
        )
        / 1e9
    )
    for col in (
        'fbs_process_Mt',
        'fbs_combustion_Mt',
        'fbs_use_table_Mt',
        'fbs_other_Mt',
    ):
        if col not in out:
            out[col] = 0.0
    out['fbs_Mt'] = out.sum(axis=1)
    return out


def compare(
    years: tuple[int, ...], vintage: str, stem: str, config: str
) -> pd.DataFrame:
    bcd.FBS_STEM = stem
    rows = []
    for year in years:
        g = ghgrp_by_sector(year)
        f = fbs_by_sector(year, vintage, config)
        d = g.join(f, how='outer').fillna(0.0)
        d = d[in_scope(pd.Series(d.index, index=d.index)) & (d['ghgrp_Mt'] > 0)]
        d['year'] = year
        rows.append(d.rename_axis('sector').reset_index())
    out = pd.concat(rows, ignore_index=True)
    out['fbs_over_ghgrp'] = out['fbs_Mt'] / out['ghgrp_Mt']
    out['below_ghgrp'] = out['fbs_Mt'] < out['ghgrp_Mt']
    return out[OUT_COLUMNS]


def check(df: pd.DataFrame) -> None:
    """Structural checks; the comparison itself is reported, not asserted."""
    assert not df.duplicated(['sector', 'year']).any(), 'duplicate sector-years'
    assert (df['ghgrp_Mt'] > 0).all(), 'a compared sector has no GHGRP emissions'
    parts = df[['ghgrp_C_Mt', 'ghgrp_W_Mt', 'ghgrp_process_subparts_Mt']].sum(axis=1)
    assert ((parts - df['ghgrp_Mt']).abs() < 1e-9).all(), 'GHGRP parts do not add up'
    fparts = df[
        ['fbs_process_Mt', 'fbs_combustion_Mt', 'fbs_use_table_Mt', 'fbs_other_Mt']
    ]
    assert (
        (fparts.sum(axis=1) - df['fbs_Mt']).abs() < 1e-9
    ).all(), 'FBS parts do not add up'
    assert (fparts >= -1e-9).all().all(), 'negative FBS emissions in scope'
    assert in_scope(df['sector']).all(), 'a sector outside the facility scope'
    logger.info('check passed: %d sector-years', len(df))


def report(df: pd.DataFrame) -> pd.DataFrame:
    by_year = df.groupby('year').agg(
        sectors=('sector', 'size'),
        below=('below_ghgrp', 'sum'),
        ghgrp_Mt=('ghgrp_Mt', 'sum'),
        fbs_Mt=('fbs_Mt', 'sum'),
        shortfall_Mt=(
            'fbs_Mt',
            lambda s: (df.loc[s.index, 'ghgrp_Mt'] - s).clip(lower=0).sum(),
        ),
    )
    logger.info(
        'Sector-years whose FBS emissions fall below their own GHGRP total:\n%s',
        by_year.round(1).to_string(),
    )
    worst = (
        df.assign(shortfall_Mt=(df['ghgrp_Mt'] - df['fbs_Mt']).clip(lower=0))
        .groupby('sector')[['shortfall_Mt', 'ghgrp_Mt', 'fbs_Mt']]
        .median()
        .sort_values('shortfall_Mt', ascending=False)
    )
    logger.info(
        'Largest median shortfalls (Mt, median over the years):\n%s',
        worst[worst['shortfall_Mt'] > 0].head(25).round(2).to_string(),
    )
    return by_year


def main(
    years: tuple[int, ...],
    vintage: str,
    stem: str,
    config: str,
    label: str,
    do_check: bool,
) -> pd.DataFrame:
    df = compare(years, vintage, stem, config)
    if do_check:
        check(df)
    out = Path(bcd.OUTPUT_DIR) / f'fbs_vs_ghgrp_{label}.csv'
    df.to_csv(out, index=False)
    report(df)
    logger.info('Wrote %s', out)
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument(
        '--vintage', required=True, help='FBS vintage, e.g. v0.3.0_0e1b0f0'
    )
    parser.add_argument('--stem', default=FACILITY_STEM)
    parser.add_argument('--config', default=V05_CONFIG)
    parser.add_argument('--label', required=True)
    parser.add_argument('--years', default=','.join(map(str, YEARS)))
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    main(
        tuple(int(y) for y in args.years.split(',')),
        args.vintage,
        args.stem,
        args.config,
        args.label,
        args.check,
    )
