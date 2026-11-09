"""Gas distribution (``221200``): output and the row by buyer class against EIA (#1004).

Two comparisons, 2017-2024:

1. **Output.** BEA gross output (``UGO305-A``), the conditioned panel the
   nowcast uses, census receipts for NAICS ``221210`` (Economic Census 2017 and
   2022, AIES 2023-24), and EIA natural gas sales.
2. **The row.** Purchases of commodity ``221200`` in a stored nowcast build, by
   buyer class, against EIA sales to the same class. Residential is ``F01000``;
   commercial is buyers in NAICS 42-81, government and special industries;
   industrial is NAICS 11-33 and 22. The buckets are rough, so read the index,
   not the level ratio.

EIA sales are volume times price from ``natural-gas/cons/sum`` and
``natural-gas/pri/sum``. Commercial and industrial take only the share of
deliveries the price represents (LDC sales, not gas transported for others);
residential uses the delivered price, which covers the whole class.

⚠️ **Residential tracking EIA is not independent evidence.** BEA builds natural
gas PCE from EIA, so it shows the units and series are right, not that the row
is.

Run::

    uv run python -m bedrock.analysis.nowcasting.gas_distribution_row_check --vintage v1.0.0_b17908f
    uv run python -m bedrock.analysis.nowcasting.gas_distribution_row_check --vintage v1.0.0_b17908f --check
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable

import numpy as np
import pandas as pd
import requests

YEARS = list(range(2017, 2025))
INDUSTRY = '221200'
NAICS = '221210'

#: EIA series, by buyer class: (volume, price, share of deliveries the price
#: represents or ``None``). ⚠️ Match commercial volume on the full name: the
#: substring ``'Vehicle Fuel'`` also hits it.
EIA_SERIES: dict[str, tuple[str, str, str | None]] = {
    'residential': (
        'U.S. Natural Gas Residential Consumption (MMcf)',
        'U.S. Price of Natural Gas Delivered to Residential Consumers',
        None,
    ),
    'commercial': (
        'Natural Gas Deliveries to Commercial Consumers (Including Vehicle Fuel '
        'through 1996) in the U.S. (MMcf)',
        'U.S. Price of Natural Gas Sold to Commercial Consumers',
        'Percent of Commercial Natural Gas Deliveries',
    ),
    'industrial': (
        'U.S. Natural Gas Industrial Consumption (MMcf)',
        'United States Natural Gas Industrial Price',
        'Percent of Industrial Natural Gas Deliveries',
    ),
}

_COMMERCIAL = (
    '42',
    '44',
    '45',
    '51',
    '52',
    '53',
    '54',
    '55',
    '56',
    '61',
    '62',
    '71',
    '72',
    '81',
)
_INDUSTRIAL = ('11', '21', '22', '23', '31', '32', '33')

BUYER_CLASSES: dict[str, Callable[[str], bool]] = {
    'commercial': lambda c: c[:2] in _COMMERCIAL or c[:1] in ('G', 'S'),
    'industrial': lambda c: c[:2] in _INDUSTRIAL,
}


def _eia(route: str) -> pd.DataFrame:
    from dotenv import load_dotenv  # noqa: PLC0415

    load_dotenv()
    response = requests.get(
        f'https://api.eia.gov/v2/natural-gas/{route}/data/',
        params={
            'api_key': os.environ['EIA_API_KEY'],
            'frequency': 'annual',
            'data[0]': 'value',
            'facets[duoarea][]': 'NUS',
            'start': str(YEARS[0]),
            'end': str(YEARS[-1]),
            'length': '5000',
        },
        timeout=120,
    )
    response.raise_for_status()
    frame = pd.DataFrame(response.json()['response']['data'])
    frame['value'] = pd.to_numeric(frame['value'], errors='coerce')
    frame['period'] = frame['period'].astype(int)
    return frame


def _series(frame: pd.DataFrame, name: str) -> pd.Series:
    rows = frame[frame['series-description'].str.contains(name, regex=False)]
    if rows.empty:
        raise KeyError(f'EIA series not found: {name!r}')
    return rows.groupby('period')['value'].first().reindex(YEARS)


def eia_sales() -> pd.DataFrame:
    """EIA natural gas sales by buyer class, $M, years x class."""
    cons, pri = _eia('cons/sum'), _eia('pri/sum')
    out = {}
    for label, (volume, price, share) in EIA_SERIES.items():
        sales = _series(cons, volume) * _series(pri, price) / 1e3  # MMcf x $/Mcf
        if share is not None:
            sales = sales * _series(pri, share) / 100.0
        out[label] = sales
    return pd.DataFrame(out)


def output_panel() -> pd.DataFrame:
    """BEA, conditioned panel, census and EIA total sales for ``221200``, $M."""
    from bedrock.transform.iot.aies_go_chaining import (  # noqa: PLC0415
        _receipts_all_levels,
    )
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415, E501
        detail_gross_output_panel,
    )

    census = {}
    for year in (2017, 2022, 2023, 2024):
        census[year] = float(_receipts_all_levels(year).get(NAICS, np.nan))
    raw = detail_gross_output_panel(ec_adjusted=False).loc[INDUSTRY]
    conditioned = detail_gross_output_panel().loc[INDUSTRY]
    return pd.DataFrame(
        {
            'BEA gross output': raw.reindex(YEARS),
            'conditioned panel': conditioned.reindex(YEARS),
            'census receipts': pd.Series(census).reindex(YEARS),
            'EIA sales': eia_sales().sum(axis=1),
        }
    ).astype(float)


def row_by_class(vintage: str) -> pd.DataFrame:
    """Purchases of ``221200`` in a stored build, by buyer class, $M."""
    from bedrock.extract.iot.nowcast_mut_storage import (  # noqa: PLC0415
        _load_stored_table,
    )

    out = {}
    for year in YEARS:
        use = _load_stored_table('Use', vintage=vintage, year=year, stage='before')
        row = use.loc[INDUSTRY].astype(float) / 1e6  # USD -> $M
        buyers = [
            str(c) for c in row.index if str(c)[:1].isdigit() or str(c)[:1] in 'GS'
        ]
        values = {'residential': float(row.get('F01000', np.nan))}
        for label, member in BUYER_CLASSES.items():
            values[label] = float(row[[c for c in buyers if member(c)]].sum())
        out[year] = values
    return pd.DataFrame(out).T


def check(vintage: str) -> int:
    """The units and series hold: residential tracks EIA within 10% every year."""
    eia = eia_sales()
    ours = row_by_class(vintage)
    failures = []
    ratio = ours['residential'] / eia['residential']
    for year, value in ratio.items():
        if not 0.9 <= value <= 1.1:
            failures.append(f'{year} residential ours/EIA {value:.3f}')
    if eia.isna().any().any():
        failures.append('an EIA series has a missing year')
    for line in failures:
        print(f'FAIL {line}')
    if failures:
        return 1
    print(f'ok: residential within 10% of EIA in every year on {vintage}')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--vintage', required=True, help='stored build, e.g. v1.0.0_b17908f'
    )
    parser.add_argument('--check', action='store_true', help='assert units and series')
    args = parser.parse_args()
    if args.check:
        return check(args.vintage)
    pd.set_option('display.width', 200)
    output = output_panel()
    print('Gas distribution output, $bn')
    print((output / 1e3).T.round(1).to_string())
    print('\nindex, 2017 = 1')
    print((output / output.loc[2017]).T.round(2).to_string())
    eia, ours = eia_sales(), row_by_class(args.vintage)
    print(f'\nPurchases of 221200 by buyer class, index 2017 = 1 ({args.vintage})')
    print(
        pd.concat(
            {'EIA': eia / eia.loc[2017], 'ours': ours[eia.columns] / ours.loc[2017]},
            axis=1,
        )
        .T.round(2)
        .to_string()
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
