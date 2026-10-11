"""EIA Fuel Oil and Kerosene Sales: adjusted distillate sales by end use"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS

_DATA_PATH = (
    Path(__file__).resolve().parent / 'data' / 'EIA_FOKS_adjusted_distillate_sales.csv'
)


def eia_foks_parse(*, source: str, year: str, **_kwargs: Any) -> pd.DataFrame:
    """Read the stored U.S. adjusted distillate sales for one year, in gallons"""
    raw = pd.read_csv(_DATA_PATH)
    raw = raw.loc[raw['Year'] == int(year)]
    if raw.empty:
        raise ValueError(f'No EIA_FOKS adjusted distillate sales stored for {year}')

    df = pd.DataFrame(
        {
            'FlowName': 'Distillate Fuel Oil',
            'FlowAmount': raw['ThousandGallons'].astype(float) * 1000,
            'ActivityConsumedBy': raw['EndUse'].astype(str),
        }
    ).reset_index(drop=True)
    df['ActivityProducedBy'] = None
    df['Description'] = 'Adjusted Sales of Distillate Fuel Oil by End Use'
    df['Unit'] = 'gal'
    df['Class'] = 'Energy'
    df['SourceName'] = source
    df['Year'] = int(year)
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['DataReliability'] = 3
    df['DataCollection'] = 1
    return df
