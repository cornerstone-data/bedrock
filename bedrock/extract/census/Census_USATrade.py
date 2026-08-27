"""Census USA Trade NAICS-6 merchandise trade (national annual YTD)."""

from __future__ import annotations

import json
import os
from typing import Any
from urllib.parse import urlparse

import pandas as pd
from requests import Response

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_sub_bucket_from_kwargs
from bedrock.utils.io.local_extract_input_data import load_local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS

_IMPORT_FLOW = 'imports'
_EXPORT_FLOW = 'exports'


def _census_flow_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    if f'/{_IMPORT_FLOW}/' in path:
        return _IMPORT_FLOW
    if f'/{_EXPORT_FLOW}/' in path:
        return _EXPORT_FLOW
    raise ValueError(f'Census USA Trade url missing imports/exports path: {url}')


def _census_usatrade_filename(url: str, year: str | int) -> str:
    return f"Census_USATrade_{year}_{_census_flow_from_url(url)}.csv"


def census_usatrade_url_helper(
    *, build_url: str, config: dict[str, Any], **_kwargs: Any
) -> list[str]:
    """National NAICS-6 import and export urls (no partner-country loop)."""
    urls = []
    for flow, get_key in (
        (_IMPORT_FLOW, 'import_get_fields'),
        (_EXPORT_FLOW, 'export_get_fields'),
    ):
        url = build_url.replace('__flow__', flow).replace(
            '__get_fields__', str(config[get_key])
        )
        urls.append(url)
    return urls


def census_usatrade_call(*, resp: Response, **kwargs: Any) -> pd.DataFrame:
    """Parse Census JSON and write the raw table under extract/input_data/."""
    payload = json.loads(resp.text)
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError(f'Census USA Trade returned no data rows for {resp.url}')
    df = pd.DataFrame(payload[1:], columns=payload[0])
    filename = _census_usatrade_filename(resp.url, kwargs['year'])
    out_dir = load_local_extract_input_dir(kwargs)
    df.to_csv(os.path.join(out_dir, filename), index=False)
    return df


def census_usatrade_load_gcs(**kwargs: Any) -> pd.DataFrame:
    """Load a cached Census dump from local input_data (GCS later, if staged)."""
    filename = _census_usatrade_filename(str(kwargs['url']), kwargs['year'])
    return load_from_gcs(
        name=filename,
        sub_bucket=gcs_extract_input_sub_bucket_from_kwargs(kwargs),
        local_dir=load_local_extract_input_dir(kwargs),
        loader=pd.read_csv,
    )


def census_usatrade_parse(
    *, df_list: list[pd.DataFrame], year: str, config: dict[str, Any], **_kwargs: Any
) -> pd.DataFrame:
    """Melt Census value fields to FlowName / FlowAmount in USD."""
    frames = []
    for raw in df_list:
        df = raw.copy()
        cols = {str(c).upper(): c for c in df.columns}
        if 'NAICS' not in cols:
            raise ValueError('Census USA Trade dump missing NAICS column')
        naics_col = cols['NAICS']
        present = [name for name in _flow_names_in_frame(df, config) if name in cols]
        if not present:
            continue
        keep = df[[naics_col, *[cols[n] for n in present]]].copy()
        keep = keep.rename(
            columns={naics_col: 'NAICS', **{cols[n]: n for n in present}}
        )
        keep['NAICS'] = (
            keep['NAICS']
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
            .str.zfill(6)
        )
        # Keep digit-6 NAICS and Census residual codes (trailing X / XX), e.g.
        # 33641X, 31181X, 11211X, 1123XX. Residuals carry suppressed detail mass
        # that Sector_Crosswalk_Census_USATrade maps 1:m onto BEA Detail.
        keep = keep.loc[keep['NAICS'].str.fullmatch(r'\d{6}|\d{5}X|\d{4}XX', na=False)]
        melted = keep.melt(
            id_vars=['NAICS'],
            value_vars=present,
            var_name='FlowName',
            value_name='FlowAmount',
        )
        melted['FlowAmount'] = pd.to_numeric(
            melted['FlowAmount'], errors='coerce'
        ).fillna(0.0)
        melted['Description'] = melted['FlowName'].map(
            lambda n: 'imports' if n != 'ALL_VAL_YR' else 'exports'
        )
        frames.append(melted)

    if not frames:
        raise ValueError(f'Census USA Trade parse produced no rows for {year}')

    df = pd.concat(frames, ignore_index=True)
    df['ActivityProducedBy'] = df['NAICS']
    df['ActivityConsumedBy'] = ''
    df['SourceName'] = 'Census_USATrade'
    df['Class'] = 'Money'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Compartment'] = ''
    df['Unit'] = 'USD'
    df['Year'] = int(year)
    df['Location'] = US_FIPS
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    df = assign_fips_location_system(df, year)
    return df.drop(columns=['NAICS'])


def _flow_names_in_frame(df: pd.DataFrame, config: dict[str, Any]) -> list[str]:
    names = list(config.get('import_flow_names') or []) + list(
        config.get('export_flow_names') or []
    )
    upper_cols = {str(c).upper() for c in df.columns}
    return [n for n in names if n in upper_cols]
