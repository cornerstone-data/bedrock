"""Data coverage indicators for the facility-based GHG FBS, per sector and year.

The facility FBS (#965, #1023) is meant to be **better data**, not only smoother
data: a sector's share of industrial fuel combustion comes from the facilities
that reported it rather than from a survey or a Use-table row. Scoring it only
on how much ``B`` moves misses that, and a real change in reported emissions is
movement the model should keep. This module reports the data behind each
sector's ``B`` instead:

- **facility counts** -- GHGRP and NEI facilities in the sector's combustion
  union. Zero under the MECS/Use-table basis, which rests on no facility at all;
- **facility-reported Mt** -- what those facilities reported;
- **facility-backed share of E** -- the part of the sector's emissions in the
  FBS whose sector split rests on facility reports: the ``GHGRP_NEI_Facilities``
  routes, plus ``Hybrid_Facility_MECS`` in the years #1023's gate puts the
  sector on a facility mode;
- **gate mode and coverage** from ``facility_coverage`` (#928, #1023);
- **facility-backed over facility-reported Mt** -- the allocation against what
  the facilities themselves reported;
- **year-on-year changes** in all of these, with flags. A facility count that
  jumps, a share that swings, or a gate mode that flips is where a data or
  method error shows first, and where a ``B`` move should be checked before it
  is believed.

Counts are also kept **by fuel** (``by_fuel_counts``). A sector-fuel whose
facility count keeps failing the year-on-year screen is unstable reporting,
and a candidate to exclude from the facility method at the cut, next to the
coverage test #1023 already applies.

These describe how much of each sector rests on facility reports and how
stable that is. They are not a data quality assessment and are not scored on
any formal scheme.

::

    uv run python -m bedrock.analysis.time_series_B_matrix.facility_data_coverage \\
        --cache-dir output/cache_v05_facilities
    uv run python -m bedrock.analysis.time_series_B_matrix.facility_data_coverage \\
        --cache-dir output/cache_v05_facilities --check
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

import bedrock.analysis.time_series_B_matrix.B_change_diagnostics as bcd

logger = logging.getLogger(__name__)

#: AttributionSources values on the facility routes (year suffix stripped).
FACILITY_ROUTE = 'GHGRP_NEI_Facilities'
HYBRID_ROUTE = 'Hybrid_Facility_MECS'
#: #1023 modes that take facility shares; ``keep_prior`` is MECS.
FACILITY_MODES = ('facility_vector', 'facility_floor')
#: Flowable of the lease/plant share-weight rows ``build_facility_combustion``
#: appends; they duplicate GHGRP tonnes, so they are counted as facilities but
#: kept out of reported Mt.
LEASE_PLANT_WEIGHT_FLOWABLE = 'Natural Gas - lease and plant'
#: NEI carries no CO2 after 2022; #1023 holds the 2022 NEI year for 2023-24.
NEI_LAST_YEAR = 2022

#: Flag thresholds. Screens, not verdicts: a flagged sector-year is where to look.
FLAG_COUNT_PCT = (
    25.0  # facility count change, %, with at least FLAG_COUNT_MIN facilities
)
FLAG_COUNT_MIN = 3
FLAG_SHARE_PP = 10.0  # facility-backed share change, percentage points
FLAG_MIN_MT = 0.5  # ignore sectors with less facility-backed E than this
#: The allocation moved but the reports did not: facility-backed Mt changes by
#: more than FLAG_ALLOC_PCT while facility-reported Mt changes by less than
#: FLAG_REPORTED_STEADY_PCT.
FLAG_ALLOC_PCT = 50.0
FLAG_REPORTED_STEADY_PCT = 15.0

OUT_NAME = 'facility_data_coverage.csv'


def facility_union(year: int) -> pd.DataFrame:
    """The combustion union the facility FBS is built from, for *year*."""
    from bedrock.extract.stewifbs.facility_combustion import (  # noqa: PLC0415
        build_facility_combustion,
    )
    from bedrock.transform.ghg.facility_coverage import (  # noqa: PLC0415
        FACILITY_SCOPE_PREFIXES,
    )

    return build_facility_combustion(
        year,
        nei_year=min(year, NEI_LAST_YEAR),
        sector_prefixes=FACILITY_SCOPE_PREFIXES,
        exclude_sectors=('221100',),
    )


def gate_modes(year: int) -> pd.DataFrame:
    """#1023's coverage and mode per sector for *year*."""
    from bedrock.transform.ghg.facility_coverage import (  # noqa: PLC0415
        facility_coverage_bands,
        modes_by_sector,
    )

    bands = facility_coverage_bands(year, nei_year=min(year, NEI_LAST_YEAR))
    modes = pd.Series(modes_by_sector(bands), name='gate_mode')
    return bands.set_index('sector')[['coverage']].join(modes, how='outer')


def sector_year_indicators(span: bcd.Span, years: tuple[int, ...]) -> pd.DataFrame:
    """One row per sector-year: facility counts, reported Mt, facility-backed share."""
    E = span.E.copy()
    E['route'] = E['AttributionSources'].map(bcd.canonical_attribution)
    rows = []
    for year in years:
        u = facility_union(year)
        mass = u[u['Flowable'] != LEASE_PLANT_WEIGHT_FLOWABLE]
        counts = (
            u.groupby(['sector', 'source'])['FacilityID']
            .nunique()
            .unstack(fill_value=0)
        )
        reported = mass.groupby('sector')['CO2e'].sum() / 1e9
        gate = gate_modes(year)

        e = E[E['year'] == year]
        total = e.groupby('sector')['CO2e'].sum() / 1e9
        fac = e[e['route'] == FACILITY_ROUTE].groupby('sector')['CO2e'].sum() / 1e9
        hyb = e[e['route'] == HYBRID_ROUTE].groupby('sector')['CO2e'].sum() / 1e9
        in_facility_mode = gate['gate_mode'].isin(FACILITY_MODES)
        hyb_backed = hyb.where(hyb.index.isin(gate.index[in_facility_mode]), 0.0)

        sectors = total.index.union(counts.index)
        frame = pd.DataFrame(index=pd.Index(sectors, name='sector'))
        frame['year'] = year
        frame['n_ghgrp'] = (
            counts.get('GHGRP', pd.Series(dtype=int))
            .reindex(sectors)
            .fillna(0)
            .astype(int)
        )
        frame['n_nei'] = (
            counts.get('NEI', pd.Series(dtype=int))
            .reindex(sectors)
            .fillna(0)
            .astype(int)
        )
        frame['n_facilities'] = frame['n_ghgrp'] + frame['n_nei']
        frame['reported_Mt'] = reported.reindex(sectors).fillna(0.0)
        frame['E_Mt'] = total.reindex(sectors).fillna(0.0)
        frame['facility_backed_Mt'] = fac.reindex(sectors).fillna(
            0.0
        ) + hyb_backed.reindex(sectors).fillna(0.0)
        frame['facility_backed_share'] = (
            frame['facility_backed_Mt'] / frame['E_Mt'].where(frame['E_Mt'] > 0)
        ).fillna(0.0)
        frame['coverage'] = gate['coverage'].reindex(sectors)
        frame['gate_mode'] = gate['gate_mode'].reindex(sectors).fillna('none')
        rows.append(frame.reset_index())
        logger.info(
            '%d: %d facilities (%d GHGRP, %d NEI); facility-backed %.0f of %.0f Mt',
            year,
            int(frame['n_facilities'].sum()),
            int(frame['n_ghgrp'].sum()),
            int(frame['n_nei'].sum()),
            frame['facility_backed_Mt'].sum(),
            frame['E_Mt'].sum(),
        )
    return pd.concat(rows, ignore_index=True)


def add_changes_and_flags(df: pd.DataFrame, span: bcd.Span) -> pd.DataFrame:
    """Year-on-year changes, the sector's real ``B`` move, and screening flags."""
    df = df.sort_values(['sector', 'year']).copy()
    g = df.groupby('sector')
    df['d_n_facilities'] = g['n_facilities'].diff()
    prev_n = g['n_facilities'].shift()
    df['d_n_facilities_pct'] = 100 * df['d_n_facilities'] / prev_n.where(prev_n > 0)
    df['d_facility_backed_share_pp'] = 100 * g['facility_backed_share'].diff()
    df['backed_to_reported'] = df['facility_backed_Mt'] / df['reported_Mt'].where(
        df['reported_Mt'] > 0
    )
    prev_b = g['facility_backed_Mt'].shift()
    prev_r = g['reported_Mt'].shift()
    d_backed_pct = 100 * (df['facility_backed_Mt'] - prev_b) / prev_b.where(prev_b > 0)
    d_reported_pct = 100 * (df['reported_Mt'] - prev_r) / prev_r.where(prev_r > 0)
    df['d_facility_backed_pct'] = d_backed_pct
    df['d_reported_pct'] = d_reported_pct
    # allocation moved, reports steady: a backed share falling to or rising from
    # zero counts as a move even though the percentage is undefined
    alloc_moved = (d_backed_pct.abs() > FLAG_ALLOC_PCT) | (
        ((df['facility_backed_Mt'] == 0) ^ (prev_b == 0)) & prev_b.notna()
    )
    df['_alloc_flag'] = alloc_moved & (d_reported_pct.abs() < FLAG_REPORTED_STEADY_PCT)
    df['gate_mode_changed'] = (g['gate_mode'].shift() != df['gate_mode']) & g[
        'gate_mode'
    ].shift().notna()

    # the commodity factor of the same code (Make is near-diagonal at detail)
    B = bcd.B_total(span, real=True)
    b_long = pd.Series(B.stack(), name='B_real').reset_index()
    b_long.columns = pd.Index(['sector', 'year', 'B_real'])
    b_long['year'] = b_long['year'].astype(int)
    df = df.merge(b_long, on=['sector', 'year'], how='left')
    df['d_B_real_pct'] = 100 * df.groupby('sector')['B_real'].pct_change(
        fill_method=None
    )

    material = (
        df['facility_backed_Mt'].groupby(df['sector']).transform('max') >= FLAG_MIN_MT
    )
    count_flag = (df['d_n_facilities_pct'].abs() > FLAG_COUNT_PCT) & (
        prev_n.reindex(df.index) >= FLAG_COUNT_MIN
    )
    share_flag = df['d_facility_backed_share_pp'].abs() > FLAG_SHARE_PP
    df['flag_count'] = (count_flag & material).fillna(False)
    df['flag_share'] = (share_flag & material).fillna(False)
    df['flag_mode'] = (df['gate_mode_changed'] & material).fillna(False)
    df['flag_alloc_vs_reported'] = (df.pop('_alloc_flag') & material).fillna(False)
    df['n_flags'] = df[
        ['flag_count', 'flag_share', 'flag_mode', 'flag_alloc_vs_reported']
    ].sum(axis=1)
    return df


def by_fuel_counts(years: tuple[int, ...]) -> pd.DataFrame:
    """Facilities per sector x fuel x year, with year-on-year count changes.

    The fuel is the union's ``Flowable`` with ``fuel_class`` (purchased,
    self-supplied, process); lease and plant share-weight rows are dropped.
    """
    parts = []
    for year in years:
        u = facility_union(year)
        u = u[u['Flowable'] != LEASE_PLANT_WEIGHT_FLOWABLE]
        c = (
            u.groupby(['sector', 'Flowable', 'fuel_class', 'source'])['FacilityID']
            .nunique()
            .unstack('source', fill_value=0)
        )
        m = u.groupby(['sector', 'Flowable', 'fuel_class'])['CO2e'].sum() / 1e9
        f = pd.DataFrame(
            {
                'n_ghgrp': c.get('GHGRP', 0),
                'n_nei': c.get('NEI', 0),
                'reported_Mt': m,
            }
        ).fillna(0.0)
        f['year'] = year
        parts.append(f.reset_index())
    out = pd.concat(parts, ignore_index=True)
    out['n_facilities'] = out['n_ghgrp'] + out['n_nei']
    key = ['sector', 'Flowable', 'fuel_class']
    out = out.sort_values(key + ['year'])
    prev = out.groupby(key)['n_facilities'].shift()
    out['d_n_facilities_pct'] = (
        100 * (out['n_facilities'] - prev) / prev.where(prev > 0)
    )
    out['flag_count'] = (
        (out['d_n_facilities_pct'].abs() > FLAG_COUNT_PCT) & (prev >= FLAG_COUNT_MIN)
    ).fillna(False)
    return out


def instability_summary(fuel: pd.DataFrame) -> pd.DataFrame:
    """Per sector x fuel: how many year-on-year count changes fail the screen.

    A sector-fuel failing in several of the seven transitions is where
    reporting is unstable enough to consider excluding it from the facility
    method, the way coverage below 0.8 already does.
    """
    key = ['sector', 'Flowable', 'fuel_class']
    g = fuel.groupby(key)
    out = pd.DataFrame(
        {
            'transitions': g['d_n_facilities_pct'].count(),
            'failed': g['flag_count'].sum(),
            'mean_facilities': g['n_facilities'].mean(),
            'mean_reported_Mt': g['reported_Mt'].mean(),
        }
    )
    out['failed_share'] = out['failed'] / out['transitions'].where(
        out['transitions'] > 0
    )
    return out.sort_values(['failed', 'mean_reported_Mt'], ascending=False)


def check(df: pd.DataFrame, span: bcd.Span) -> None:
    """The identities the table rests on."""
    assert (
        df['facility_backed_share'].between(0, 1 + 1e-9)
    ).all(), 'share outside [0, 1]'
    assert (df[['n_ghgrp', 'n_nei']] >= 0).all().all(), 'negative facility count'
    for year, g in df.groupby('year'):
        want = float(span.E.loc[span.E['year'] == year, 'CO2e'].sum()) / 1e9
        got = float(g['E_Mt'].sum())
        assert abs(got - want) < 1e-6 * max(
            want, 1.0
        ), f'{year}: E {got} != span {want}'
    logger.info(
        'OK  shares in [0, 1], counts non-negative, sector E sums to the span every year'
    )


def report(df: pd.DataFrame) -> None:
    by_year = df.groupby('year').agg(
        facilities=('n_facilities', 'sum'),
        ghgrp=('n_ghgrp', 'sum'),
        nei=('n_nei', 'sum'),
        facility_backed_Mt=('facility_backed_Mt', 'sum'),
        E_Mt=('E_Mt', 'sum'),
        sectors_with_facilities=('n_facilities', lambda s: int((s > 0).sum())),
        flags=('n_flags', 'sum'),
    )
    by_year['facility_backed_%'] = 100 * by_year['facility_backed_Mt'] / by_year['E_Mt']
    logger.info(
        'National, by year (the MECS/Use-table basis has 0 facilities):\n%s',
        by_year.round(1).to_string(),
    )
    top = df[df['n_flags'] > 0].sort_values(
        ['n_flags', 'facility_backed_Mt'], ascending=False
    )
    alloc = df[df['flag_alloc_vs_reported']].sort_values(
        'facility_backed_Mt', ascending=False
    )
    logger.info(
        'Allocation moved while facility reports held steady (likely build errors):\n%s',
        alloc[
            [
                'sector',
                'year',
                'n_facilities',
                'reported_Mt',
                'd_reported_pct',
                'facility_backed_Mt',
                'd_facility_backed_pct',
                'gate_mode',
                'd_B_real_pct',
            ]
        ]
        .head(25)
        .round(2)
        .to_string(index=False),
    )
    cols = [
        'sector',
        'year',
        'n_facilities',
        'd_n_facilities_pct',
        'facility_backed_share',
        'd_facility_backed_share_pp',
        'gate_mode',
        'coverage',
        'd_B_real_pct',
        'n_flags',
    ]
    logger.info(
        'Flagged sector-years (largest first):\n%s',
        top[cols].head(30).round(2).to_string(index=False),
    )


def main(cache_dir: str, do_check: bool) -> pd.DataFrame:
    bcd.CACHE_DIR = (
        Path(cache_dir)
        if Path(cache_dir).is_absolute()
        else Path(bcd.OUTPUT_DIR) / cache_dir
    )
    span = bcd.load_span()
    logger.info('span FBS %s, MUT %s', span.vintages.fbs, span.vintages.mut)
    years = tuple(sorted(span.L))
    df = add_changes_and_flags(sector_year_indicators(span, years), span)
    if do_check:
        check(df, span)
    out = bcd.CACHE_DIR.parent / f'{bcd.CACHE_DIR.name}_{OUT_NAME}'
    df.to_csv(out, index=False)
    report(df)
    fuel = by_fuel_counts(years)
    unstable = instability_summary(fuel)
    fuel.to_csv(out.with_name(out.stem + '_by_fuel.csv'), index=False)
    unstable.to_csv(out.with_name(out.stem + '_fuel_instability.csv'))
    logger.info(
        'Sector-fuels failing the count screen in 3+ of 7 transitions (candidates '
        'to exclude at the cut):\n%s',
        unstable[unstable['failed'] >= 3].head(30).round(2).to_string(),
    )
    logger.info('Wrote %s and the by-fuel tables beside it', out)
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument(
        '--cache-dir', required=True, help='span cache built on a facility FBS'
    )
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    main(args.cache_dir, args.check)
