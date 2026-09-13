"""Three independent claims about who buys manufacturing's electricity, 2017-2024.

The G/T/D purchaser allocation splits the Industrial end-use class across
manufacturing by MECS Table 7.7 purchased-kWh shares
(``_industrial_mecs_class_mwh``).  The Use table's own electricity row is never
consulted for that split, so MECS and the IO model are two separate claims about
the same allocation and nothing in the pipeline reconciles them.

Census publishes a third: cost of purchased electricity by NAICS, annually, in
dollars -- ``CSTELEC`` in the Economic Census (2017, 2022) and ASM (2018-2021),
``EXPS_ELEC_VAL`` in AIES (2023-2024).  It is the only one of the three that is
both annual and in the same units as the Use table.

Everything here is compared as **share of the manufacturing total**, never as a
level.  ``Census_EC_Expenses`` warns that Census and BEA disagree about what
these cells contain, by large factors, at the 2017 base itself; the shares are
comparable where the levels are not.  A share also makes the kWh source
commensurate with the two dollar sources without asserting a price.

::

    python -m bedrock.analysis.electricity.current.eia_gtd.annual_electricity_shares
    # options: --years 2017-2024  --top 15  --csv  --check
"""

from __future__ import annotations

import argparse
import functools
import logging
from pathlib import Path

import pandas as pd

from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent
ELECTRICITY_ROW = '221100'
DEFAULT_YEARS = tuple(range(2017, 2025))

#: Cost of purchased electricity, by year, as (FBA source, FlowName).  ASM does
#: not run in an Economic Census year and AIES replaced it from 2023; the three
#: publish the same cell, and EC/ASM even share the variable name.
CENSUS_ELECTRICITY_SOURCE: dict[int, tuple[str, str]] = {
    2017: ('Census_EC_Expenses', 'CSTELEC'),
    2018: ('Census_ASM_Expenses', 'CSTELEC'),
    2019: ('Census_ASM_Expenses', 'CSTELEC'),
    2020: ('Census_ASM_Expenses', 'CSTELEC'),
    2021: ('Census_ASM_Expenses', 'CSTELEC'),
    2022: ('Census_EC_Expenses', 'CSTELEC'),
    2023: ('Census_AIES_Expenses', 'EXPS_ELEC_VAL'),
    2024: ('Census_AIES_Expenses', 'EXPS_ELEC_VAL'),
}

_NAICS_TO_BEA = (
    Path(__file__).resolve().parents[4]
    / 'utils'
    / 'mapping'
    / 'naics'
    / 'NAICS_to_BEA_Crosswalk_2017.csv'
)

DESC: dict[str, str] = {str(k): str(v) for k, v in COMMODITY_DESC.items()}


# --------------------------------------------------------------- the three sources


@functools.cache
def _naics6_to_io() -> dict[str, str]:
    """Six-digit manufacturing NAICS -> BEA 2017 detail code.

    Many-to-one on this slice: 360 NAICS onto 232 IO codes, no NAICS carrying
    more than one, so no splitting weight is needed here (unlike Table 7.7,
    whose rows do straddle IO codes).
    """
    frame = pd.read_csv(_NAICS_TO_BEA, dtype=str)
    naics = frame['NAICS_2017_Code'].astype(str).str.strip()
    io = frame['BEA_2017_Detail_Code'].astype(str).str.strip()
    keep = naics.str.fullmatch(r'3[123]\d{4}') & io.str.len().gt(0)
    pairs = pd.DataFrame({'naics': naics[keep], 'io': io[keep]}).drop_duplicates()
    multi = pairs.groupby('naics')['io'].nunique()
    if int((multi > 1).sum()):
        raise ValueError(
            'NAICS_to_BEA_Crosswalk_2017 now maps a 6-digit manufacturing NAICS '
            f'to several IO codes ({sorted(multi[multi > 1].index)[:5]}); this '
            'module assumes many-to-one and would need a splitting weight'
        )
    return dict(zip(pairs['naics'], pairs['io'], strict=True))


def io_use_electricity(year: int) -> pd.Series:
    """Electricity row of the nowcast after-redef Use table, by industry, USD."""
    from bedrock.analysis.nowcasting.results._ef_smoke_lib import (  # noqa: PLC0415
        activate_live_config,
    )
    from bedrock.extract.iot.nowcast_mut_storage import (  # noqa: PLC0415
        load_nowcast_detail_Utot_usa,
    )

    activate_live_config(year)
    use = load_nowcast_detail_Utot_usa()
    row = pd.Series(use.loc[ELECTRICITY_ROW], dtype=float)
    row.index = pd.Index([str(c) for c in row.index])
    return row


def mecs_electricity(year: int, electricity_purchases: pd.Series) -> pd.Series:
    """Table 7.7 purchased kWh mapped onto IO manufacturing columns.

    ``electricity_purchases`` splits 7.7 rows that straddle several IO codes,
    exactly as the allocator does, so this reproduces the weights the model
    actually uses rather than an independent reading of the table.
    """
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        io_manufacturing_purchased_kwh,
        mecs_year_for_eia_year,
    )

    survey = mecs_year_for_eia_year(year)
    return io_manufacturing_purchased_kwh(electricity_purchases, survey).astype(float)


def census_electricity(year: int) -> pd.Series:
    """Census cost of purchased electricity by IO code, USD.

    Six-digit manufacturing NAICS only.  Every level of the Census frame covers
    all of manufacturing, so summing it unfiltered multiplies the total several
    times over -- and ``'31-33'`` is five characters, which survives a naive
    length filter.
    """
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    source, flow_name = CENSUS_ELECTRICITY_SOURCE[int(year)]
    frame = getFlowByActivity(source, int(year))
    sub = frame.loc[frame['FlowName'].astype(str) == flow_name].copy()
    if sub.empty:
        raise ValueError(f'{source} {year} has no {flow_name!r} rows')
    naics = sub['ActivityConsumedBy'].astype(str).str.strip()
    sub = sub.loc[naics.str.fullmatch(r'3[123]\d{4}')].copy()
    sub['naics'] = naics.loc[sub.index]
    # Census amounts are Thousand USD everywhere in the API.
    sub['usd'] = pd.to_numeric(sub['FlowAmount'], errors='coerce').fillna(0.0) * 1e3
    mapping = _naics6_to_io()
    sub['io'] = sub['naics'].map(mapping)
    unmapped = sub.loc[sub['io'].isna(), 'naics'].nunique()
    if unmapped:
        logger.info(
            '%s %s: %s six-digit NAICS not in the crosswalk', source, year, unmapped
        )
    return sub.dropna(subset=['io']).groupby('io')['usd'].sum().astype(float)


# ------------------------------------------------------------------- the comparison


def annual_shares(years: tuple[int, ...]) -> pd.DataFrame:
    """Long frame: one row per (year, sector), each source as % of its own total.

    Shares are taken over the sectors all three sources cover in that year, so
    the three columns are normalised on one axis and sum to 100 each.
    """
    rows: list[pd.DataFrame] = []
    for year in years:
        use_row = io_use_electricity(year)
        mecs = mecs_electricity(year, use_row)
        census = census_electricity(year)
        common = sorted(set(use_row.index) & set(mecs.index) & set(census.index))
        frame = pd.DataFrame(
            {
                'io': use_row.reindex(common).clip(lower=0.0).fillna(0.0),
                'mecs': mecs.reindex(common).clip(lower=0.0).fillna(0.0),
                'census': census.reindex(common).clip(lower=0.0).fillna(0.0),
            }
        )
        frame = frame.loc[frame.sum(axis=1) > 0]
        shares = frame.div(frame.sum(axis=0), axis=1) * 100.0
        shares.columns = ['io_share', 'mecs_share', 'census_share']
        shares['year'] = int(year)
        shares['sector'] = shares.index
        shares['name'] = [DESC.get(str(c), '') for c in shares.index]
        shares['io_usd'] = frame['io']
        shares['census_usd'] = frame['census']
        shares['mecs_kwh'] = frame['mecs']
        rows.append(shares.reset_index(drop=True))
    out = pd.concat(rows, ignore_index=True)
    out['io_minus_mecs'] = out['io_share'] - out['mecs_share']
    out['io_minus_census'] = out['io_share'] - out['census_share']
    out['census_minus_mecs'] = out['census_share'] - out['mecs_share']
    return out


def divergence_by_year(shares: pd.DataFrame) -> pd.DataFrame:
    """Per year, how much share separates each pair of sources.

    Half the summed absolute gap is the share of the manufacturing total that
    one source assigns to different sectors than the other -- the fraction that
    would have to be moved to reconcile them.
    """
    rows: list[dict[str, float]] = []
    for year, grp in shares.groupby('year'):
        row: dict[str, float] = {
            'year': float(year),  # type: ignore[arg-type]
            'sectors': float(len(grp)),
        }
        for label, col in (
            ('IO vs MECS', 'io_minus_mecs'),
            ('IO vs Census', 'io_minus_census'),
            ('Census vs MECS', 'census_minus_mecs'),
        ):
            row[f'{label} (pp)'] = float(grp[col].abs().sum() / 2.0)
        row['sectors >1pp off MECS'] = float((grp['io_minus_mecs'].abs() > 1.0).sum())
        rows.append(row)
    frame = pd.DataFrame(rows)
    frame['year'] = frame['year'].astype(int)
    frame['sectors'] = frame['sectors'].astype(int)
    frame['sectors >1pp off MECS'] = frame['sectors >1pp off MECS'].astype(int)
    return frame.set_index('year')


def top_departures(shares: pd.DataFrame, column: str, top: int) -> pd.DataFrame:
    """Sectors whose share gap is largest on average across the span."""
    rank = (
        shares.groupby('sector')[column]
        .apply(lambda s: s.abs().mean())
        .sort_values(ascending=False)
    )
    return shares.loc[shares['sector'].isin(rank.head(top).index)]


def _series_table(shares: pd.DataFrame, sectors: list[str], col: str) -> pd.DataFrame:
    wide = shares.pivot_table(index='sector', columns='year', values=col)
    wide = wide.reindex(sectors)
    wide.insert(0, 'name', [DESC.get(s, '')[:38] for s in wide.index])
    return wide


def check(shares: pd.DataFrame) -> int:
    """Identities that must hold; returns the number that failed."""
    failures = 0
    for year, grp in shares.groupby('year'):
        for col in ('io_share', 'mecs_share', 'census_share'):
            total = float(grp[col].sum())
            if abs(total - 100.0) > 1e-6:
                print(f'  FAIL {year} {col} sums to {total:.6f}, not 100')
                failures += 1
    covered = shares.groupby('year')['sector'].nunique()
    if int(covered.min()) < 150:
        print(f'  FAIL thin year: {int(covered.min())} sectors in common')
        failures += 1
    print(f'check: {failures} failure(s) over {shares["year"].nunique()} years')
    return failures


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--years', default='2017-2024', help='e.g. 2017-2024')
    parser.add_argument('--top', type=int, default=12)
    parser.add_argument('--csv', action='store_true', help='write the long frame')
    parser.add_argument('--check', action='store_true', help='run identities only')
    args = parser.parse_args(argv)

    start, _, end = args.years.partition('-')
    years = tuple(range(int(start), int(end or start) + 1))
    shares = annual_shares(years)

    if args.check:
        raise SystemExit(1 if check(shares) else 0)

    print()
    print('=== share of manufacturing electricity that separates each pair ===')
    print('(half the summed absolute gap: the share one source would have to move)')
    print(divergence_by_year(shares).to_string(float_format=lambda v: f'{v:,.2f}'))

    for col, label in (
        ('io_minus_mecs', 'IO minus MECS'),
        ('io_minus_census', 'IO minus Census'),
    ):
        top = top_departures(shares, col, args.top)
        sectors = list(
            top.groupby('sector')[col]
            .apply(lambda s: s.abs().mean())
            .sort_values(ascending=False)
            .index
        )
        print()
        print(f'=== largest departures, {label} (pp of manufacturing total) ===')
        print(
            _series_table(top, sectors, col).to_string(
                float_format=lambda v: f'{v:+.2f}'
            )
        )

    sectors = list(
        top_departures(shares, 'io_minus_mecs', args.top)
        .groupby('sector')['io_minus_mecs']
        .apply(lambda s: s.abs().mean())
        .sort_values(ascending=False)
        .index
    )
    for src, label in (
        ('io_share', 'IO (Use table)'),
        ('census_share', 'Census (cost of purchased electricity)'),
        ('mecs_share', 'MECS (Table 7.7 kWh)'),
    ):
        print()
        print(f'=== {label}: share of manufacturing total, % ===')
        print(
            _series_table(shares, sectors, src).to_string(
                float_format=lambda v: f'{v:,.2f}'
            )
        )

    if args.csv:
        path = OUT_DIR / 'annual_electricity_shares.csv'
        shares.to_csv(path, index=False)
        print(f'\nwrote {path}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    main()
