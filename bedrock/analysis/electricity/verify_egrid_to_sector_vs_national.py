"""Check ``egrid_to_sector`` FBS totals vs stewi eGRID US national tab.

    uv run python -m bedrock.analysis.electricity.verify_egrid_to_sector_vs_national
    uv run python -m bedrock.analysis.electricity.verify_egrid_to_sector_vs_national 2022

Optional argv[1] is stewi eGRID inventory year (default 2023). Requires local stewi
build for that year. Reference totals on GitHub (standardizedinventories stewi/data).
"""

from __future__ import annotations

import sys

import pandas as pd

from bedrock.extract.stewifbs.stewiFBS import egrid_to_sector

DEFAULT_EGRID_YEAR = '2023'
TOLERANCE_PCT = 0.1
_GHG = ('Carbon dioxide', 'Methane', 'Nitrous oxide')
_NAT_TOTALS_URL = (
    'https://raw.githubusercontent.com/cornerstone-data/standardizedinventories/'
    'refs/heads/main/stewi/data/eGRID_{year}_NationalTotals.csv'
)

# Minimal ``egrid_to_sector`` config (not a full FBS method stub).
_EGRID_TO_SECTOR_CONFIG: dict = {
    'data_format': 'FBS_outside_flowsa',
    'geoscale': 'national',
    'target_naics_year': 2017,
    'activity_schema': 'NAICS_2017_Code',
    'activity_to_sector_mapping': 'EPA_eGRID',
    'industry_spec': {
        'default': 'NAICS_6',
        'NAICS_6': [
            '221111',
            '221112',
            '221113',
            '221114',
            '221115',
            '221116',
            '221117',
            '221118',
            '221121',
            '221122',
        ],
    },
    'selection_fields': {
        'Compartment': 'air',
        'FlowName': list(_GHG),
    },
}


def _flow_kg(amount: float, unit: str) -> float:
    if unit == 'tons':
        return amount * 907.18474
    if unit == 'lbs':
        return amount * 0.4535924
    return amount


def main() -> int:
    year = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_EGRID_YEAR
    nat_url = _NAT_TOTALS_URL.format(year=year)
    config = {
        **_EGRID_TO_SECTOR_CONFIG,
        'inventory_dict': {'eGRID': year},
        'year': int(year),
    }

    fbs_kg = (
        pd.DataFrame(egrid_to_sector(config, full_name='egrid_national_verify'))
        .groupby('Flowable')['FlowAmount']
        .sum()
    )

    nat = pd.read_csv(nat_url).loc[lambda d: d['FlowName'].isin(_GHG)]
    ref_kg = nat.apply(lambda r: _flow_kg(float(r['FlowAmount']), r['Unit']), axis=1)
    ref_kg.index = nat['FlowName']

    print(f'eGRID {year}  reference: {nat_url}')
    print(f'tolerance: {TOLERANCE_PCT}%')
    print()
    ok = True
    for flow in _GHG:
        ref = float(ref_kg[flow])
        got = float(fbs_kg.get(flow, 0.0))
        pct = abs(got - ref) / ref * 100.0
        print(
            f'  {flow:16}  national={ref:,.0f} kg  fbs={got:,.0f} kg  diff={pct:.3f}%'
        )
        ok = ok and pct <= TOLERANCE_PCT

    print()
    print('PASS' if ok else 'FAIL')
    return 0 if ok else 1


if __name__ == '__main__':
    sys.exit(main())
