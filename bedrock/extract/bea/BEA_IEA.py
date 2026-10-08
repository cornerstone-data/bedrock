"""BEA IntlServTrade national services trade (AllCountries)."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
from requests import Response

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_sub_bucket_from_kwargs
from bedrock.utils.io.local_extract_input_data import load_local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS

_MILLION_TO_USD = 1_000_000.0


def _bea_iea_direction(url: str) -> str:
    query = parse_qs(urlparse(url).query)
    direction = (query.get('TradeDirection') or query.get('tradedirection') or [''])[0]
    if direction not in {'Imports', 'Exports'}:
        raise ValueError(f'BEA IntlServTrade url missing TradeDirection: {url}')
    return direction


def _bea_iea_filename(url: str, year: str | int) -> str:
    return f"BEA_IEA_{year}_{_bea_iea_direction(url)}.csv"


def bea_iea_url_helper(
    *, build_url: str, config: dict[str, Any], **_kwargs: Any
) -> list[str]:
    """National AllCountries urls for each TradeDirection (no partner loop)."""
    return [
        build_url.replace('__direction__', direction)
        for direction in config.get('trade_directions', ['Imports', 'Exports'])
    ]


def bea_iea_call(*, resp: Response, **kwargs: Any) -> pd.DataFrame:
    """Parse BEA JSON Results.Data and write the raw table under extract/input_data/."""
    raw = json.loads(resp.text)
    results = raw.get('BEAAPI', {}).get('Results', {})
    if isinstance(results, dict) and results.get('Error'):
        raise RuntimeError(f"BEA IntlServTrade error: {results['Error']}")
    data = results.get('Data') if isinstance(results, dict) else None
    if not data:
        raise ValueError(f'BEA IntlServTrade returned no Data for {resp.url}')
    df = pd.DataFrame(data)
    filename = _bea_iea_filename(resp.url, kwargs['year'])
    out_dir = load_local_extract_input_dir(kwargs)
    df.to_csv(os.path.join(out_dir, filename), index=False)
    return df


def bea_iea_load_gcs(**kwargs: Any) -> pd.DataFrame:
    """Load a cached IntlServTrade dump from local input_data (GCS later, if staged)."""
    filename = _bea_iea_filename(str(kwargs['url']), kwargs['year'])
    return load_from_gcs(
        name=filename,
        sub_bucket=gcs_extract_input_sub_bucket_from_kwargs(kwargs),
        local_dir=load_local_extract_input_dir(kwargs),
        loader=pd.read_csv,
    )


def bea_iea_parse(
    *, df_list: list[pd.DataFrame], year: str, **_kwargs: Any
) -> pd.DataFrame:
    """Format TypeOfService rows; convert million-USD DataValue to USD."""
    df = pd.concat(df_list, ignore_index=True)
    if 'TypeOfService' not in df.columns or 'DataValue' not in df.columns:
        raise ValueError('BEA IntlServTrade dump missing TypeOfService or DataValue')
    direction_col = 'TradeDirection' if 'TradeDirection' in df.columns else None
    if direction_col is None:
        raise ValueError('BEA IntlServTrade dump missing TradeDirection')

    out = pd.DataFrame(
        {
            'ActivityProducedBy': df['TypeOfService'].astype(str),
            'FlowName': df[direction_col].astype(str),
            'FlowAmount': pd.to_numeric(df['DataValue'], errors='coerce').fillna(0.0)
            * _MILLION_TO_USD,
            'Description': df[direction_col].astype(str).str.lower(),
        }
    )
    out['ActivityConsumedBy'] = ''
    out['SourceName'] = 'BEA_IEA'
    out['Class'] = 'Money'
    out['FlowType'] = 'TECHNOSPHERE_FLOW'
    out['Compartment'] = ''
    out['Unit'] = 'USD'
    out['Year'] = int(year)
    out['Location'] = US_FIPS
    out['DataReliability'] = 5  # tmp
    out['DataCollection'] = 5  # tmp
    return assign_fips_location_system(out, year)
