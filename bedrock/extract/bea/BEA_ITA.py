"""BEA ITA goods+services national control totals (Tables 2.1 / 3.1 family)."""

from __future__ import annotations

from typing import Any, Literal

import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS

_MILLION_TO_USD = 1_000_000.0
_ITA_INPUT_FILENAME = 'ita_gs_totals.csv'
_ITA_SOURCE = 'BEA_ITA'


def load_ita_gs_table() -> pd.DataFrame:
    """Year-indexed ITA G+S export/import totals in million USD."""
    df = load_from_gcs(
        name=_ITA_INPUT_FILENAME,
        sub_bucket=gcs_extract_input_path(_ITA_SOURCE, year=None),
        local_dir=local_extract_input_dir(_ITA_SOURCE, year=None),
        loader=pd.read_csv,
    )
    required = {'year', 'exports_gs_million_usd', 'imports_gs_million_usd'}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f'ITA totals CSV missing columns: {sorted(missing)}')
    df['year'] = df['year'].astype(int)
    for col in ('exports_gs_million_usd', 'imports_gs_million_usd'):
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def ita_gs_totals_usd(year: int | str) -> dict[Literal['exports', 'imports'], float]:
    """Calendar-year ITA goods+services totals in USD, from the BEA_ITA FBA."""
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    fba = getFlowByActivity(_ITA_SOURCE, int(year))
    out: dict[Literal['exports', 'imports'], float] = {}
    for direction, flow_name in (
        ('exports', 'exports_gs'),
        ('imports', 'imports_gs'),
    ):
        rows = fba.loc[fba['FlowName'] == flow_name]
        if rows.empty:
            raise ValueError(f'BEA_ITA FBA missing {flow_name} for {year}')
        out[direction] = float(pd.to_numeric(rows['FlowAmount'], errors='coerce').sum())
    return out


def bea_ita_load(**_kwargs: Any) -> pd.DataFrame:
    """Load staged ITA totals from extract/input_data/BEA_ITA/ (GCS later, if staged)."""
    return load_ita_gs_table()


def bea_ita_parse(
    *, df_list: list[pd.DataFrame], year: str, **_kwargs: Any
) -> pd.DataFrame:
    """One exports_gs row and one imports_gs row for the requested year, in USD."""
    table = pd.concat(df_list, ignore_index=True) if df_list else load_ita_gs_table()
    year_i = int(year)
    row = table.loc[table['year'].astype(int) == year_i]
    if row.empty:
        raise ValueError(f'ITA G+S totals missing year {year_i}')
    records = [
        {
            'FlowName': 'exports_gs',
            'FlowAmount': float(row['exports_gs_million_usd'].iloc[0])
            * _MILLION_TO_USD,
            'Description': 'ITA goods+services exports',
        },
        {
            'FlowName': 'imports_gs',
            'FlowAmount': float(row['imports_gs_million_usd'].iloc[0])
            * _MILLION_TO_USD,
            'Description': 'ITA goods+services imports',
        },
    ]
    df = pd.DataFrame(records)
    df['ActivityProducedBy'] = 'All'
    df['ActivityConsumedBy'] = ''
    df['SourceName'] = _ITA_SOURCE
    df['Class'] = 'Money'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Compartment'] = ''
    df['Unit'] = 'USD'
    df['Year'] = year_i
    df['Location'] = US_FIPS
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    return assign_fips_location_system(df, year)
