"""Write ``ec_alt_measures.csv`` — the C1 alternative census measures (#724).

The RCPTOT conditioners in :mod:`~bedrock.transform.iot.ec_go_adjustment` read
the ``Census_EC_Expenses`` FBA, but two of C1's sectors are benchmarked on a
*different* census measure, from datasets this repo does not extract:

- **wholesale trade**: gross margin (``GRMARG``), from ``ecnmargin`` (2017)
  and ``ecngrmargprof`` (2022) at four-digit NAICS — BEA's wholesale output
  *is* margin, so receipts (coverage 3.9) were never the instrument;
- **other services (81)**: C1 says *"taxable revenue and tax-exempt
  expenses"* — taxable ``RCPTOT`` plus tax-exempt ``OPEX`` from ``ecnbasic``'s
  ``TAXSTAT`` dimension, at six-digit NAICS.

⚠️ **Writing this file is not enough** on its own machine-to-machine — the CSV
is committed so the build never calls the Census API (same pattern as
``write_supply_mix_update.py``).  Rerun this script only to refresh from
Census, then commit the diff.

Run::

    uv run python -m bedrock.utils.mapping.write_ec_alt_measures
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from pathlib import Path

import pandas as pd

#: The committed output, consumed by ``ec_go_adjustment``.
OUTPUT = Path('bedrock/analysis/nowcasting/census_alt/ec_alt_measures.csv')

#: (measure, year) -> (dataset, variable list).  The margin dataset was
#: renamed between vintages; the variables were not.
SOURCES: dict[tuple[str, int], tuple[str, str]] = {
    ('wholesale_margin', 2017): ('ecnmargin', 'GRMARG'),
    ('wholesale_margin', 2022): ('ecngrmargprof', 'GRMARG'),
    ('services81_two_part', 2017): ('ecnbasic', 'RCPTOT,OPEX,TAXSTAT'),
    ('services81_two_part', 2022): ('ecnbasic', 'RCPTOT,OPEX,TAXSTAT'),
}


def _api_key() -> str:
    key = os.environ.get('CENSUS_API_KEY', '')
    if not key:
        for line in Path('.env').read_text(encoding='utf-8').splitlines():
            if line.startswith('CENSUS_API_KEY'):
                key = line.split('=', 1)[1].strip().strip('"').strip("'")
    if not key:
        raise RuntimeError('CENSUS_API_KEY not set and not found in .env')
    return key


def _query(dataset: str, year: int, get: str) -> pd.DataFrame:
    ncol = f'NAICS{year}'
    url = (
        f'https://api.census.gov/data/{year}/{dataset}'
        f'?get={ncol},{get}&for=us:*&key={_api_key()}'
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=180) as response:
                data = json.loads(response.read())
            break
        except Exception:
            if attempt == 2:
                raise
            time.sleep(5)
    frame = pd.DataFrame(data[1:], columns=data[0])
    frame = frame.loc[:, ~frame.columns.duplicated()]
    return frame.rename(columns={ncol: 'naics'})


def build() -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    for year in (2017, 2022):
        dataset, variables = SOURCES[('wholesale_margin', year)]
        frame = _query(dataset, year, variables)
        frame = frame[
            frame['naics'].str.startswith('42') & (frame['naics'].str.len() == 4)
        ].copy()
        frame['value'] = pd.to_numeric(frame['GRMARG'], errors='coerce')
        rows.append(
            frame[['naics', 'value']].assign(measure='wholesale_margin', year=year)
        )

        dataset, variables = SOURCES[('services81_two_part', year)]
        frame = _query(dataset, year, variables)
        frame = frame[
            frame['naics'].str.startswith('81') & (frame['naics'].str.len() == 6)
        ].copy()
        for column in ('RCPTOT', 'OPEX'):
            frame[column] = pd.to_numeric(frame[column], errors='coerce')
        taxable = frame[frame['TAXSTAT'] == 'T'].set_index('naics')['RCPTOT']
        exempt = frame[frame['TAXSTAT'] == 'Y'].set_index('naics')['OPEX']
        combined = taxable.add(exempt, fill_value=0.0).rename('value').reset_index()
        rows.append(combined.assign(measure='services81_two_part', year=year))

    table = pd.concat(rows, ignore_index=True)[['measure', 'year', 'naics', 'value']]
    return table.dropna(subset=['value']).sort_values(['measure', 'year', 'naics'])


def main() -> int:
    table = build()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(OUTPUT, index=False)
    totals = table.groupby(['measure', 'year'])['value'].sum() / 1e6
    print(f'wrote {len(table)} rows to {OUTPUT}')
    print(totals.round(1).rename('total_$B').to_string())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
