"""Census USA Trade NAICS-6 merchandise trade (national annual YTD).

Also provides ``refresh_hs2716_electricity_unit_value_csv`` for HS10
``2716000000`` unit values used by Trade electricity dollarization (#668) —
a shortcut until a full Census HS FBA extract exists.

Special-classification NAICS codes, and why one of them is dropped
-----------------------------------------------------------------
Census publishes four ``9xxxxx`` special-classification codes beside its
industry NAICS.  They carry real mass — 111,288 $M of 2017 imports (4.6% of
the direction) and 80,673 $M of exports (5.2%) — so what happens to them is a
methods decision, not a rounding question.  ``Sector_Crosswalk_Census_USATrade``
maps three and deliberately omits the fourth:

=========  ======================================  ====================
code       Census description                      BEA Detail target
=========  ======================================  ====================
``910000`` waste and scrap                         ``S00401`` scrap
``930000`` used or second-hand merchandise         ``S00402`` used goods
``990000`` other special classification provisions ``S00402`` used goods
``980000`` goods returned                          **none — dropped**
=========  ======================================  ====================

⚠️ **The ``980000`` drop is intentional.**  "Goods returned" is re-imported
U.S. merchandise (71,503 $M c.i.f. in 2017; the export direction is a rounding
error at 138 $M).  BEA's I-O import column is **net of re-imports** — the I-O
accounts remove re-exports and re-imports so gross trade matches domestic
supply (Concepts and Methods ch. 7) — so routing ``980000`` to a commodity row
would put mass in ``MCIF`` that the target column does not contain.  Omitting
it from the Crosswalk is how that exclusion is implemented.  ✅ Verified: our
mapped goods rows carry none of it.

⚠️ **``990000`` is a catch-all and is the weaker of the three mappings.**
"Other special classification provisions" is a grab-bag, not used merchandise,
and sending it to ``S00402`` overstates that row — 2017 exports land at
62,219 $M against 15,515 $M published, and 42,788 $M of the excess is
``990000`` alone.  Tracked as part of #703.
"""

from __future__ import annotations

import json
import os
import ssl
import time
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import pandas as pd
from requests import Response

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.config.common import load_env_file_key
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_sub_bucket_from_kwargs
from bedrock.utils.io.local_extract_input_data import load_local_extract_input_dir
from bedrock.utils.mapping.location import US_FIPS

_IMPORT_FLOW = 'imports'
_EXPORT_FLOW = 'exports'

TradeDirection = Literal['exports', 'imports']

_HS2716 = '2716000000'
HS2716_ELECTRICAL_ENERGY = _HS2716
_HS2716_UNIT_VALUE_CSV = (
    Path(__file__).resolve().parent / 'data' / 'hs2716_electricity_unit_value.csv'
)
_HS2716_DEFAULT_YEARS = tuple(range(2017, 2025))


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


def hs2716_unit_value_csv_path() -> Path:
    """Checked-in Census HS 2716 unit-value table (year × direction)."""
    return _HS2716_UNIT_VALUE_CSV


def load_hs2716_unit_value_table() -> pd.DataFrame:
    """Read the committed year×direction Census HS 2716 unit-value CSV."""
    path = hs2716_unit_value_csv_path()
    if not path.is_file():
        raise FileNotFoundError(
            f'Missing {path.name}; run refresh_hs2716_electricity_unit_value_csv()'
        )
    df = pd.read_csv(path)
    required = {
        'year',
        'direction',
        'value_usd',
        'qty_mwh',
        'unit_value_usd_per_mwh',
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f'{path.name} missing columns {sorted(missing)}')
    return df


def hs2716_unit_value_usd_per_mwh(year: int, direction: TradeDirection) -> float:
    """Census HS 2716 unit value (USD/MWh) for *year* and *direction*."""
    if direction not in ('exports', 'imports'):
        raise ValueError(f'direction must be exports or imports, got {direction!r}')
    table = load_hs2716_unit_value_table()
    hit = table.loc[(table['year'] == int(year)) & (table['direction'] == direction)]
    if hit.empty:
        raise ValueError(
            f'No HS 2716 unit value for year={year} direction={direction} in '
            f'{hs2716_unit_value_csv_path().name}'
        )
    return float(hit['unit_value_usd_per_mwh'].iloc[0])


def _census_key() -> str:
    return load_env_file_key('api_key', 'Census')


def _fetch_hs2716_national_row(
    year: int, direction: TradeDirection, key: str
) -> dict[str, Any]:
    """National HS10 ``2716000000`` Dec YTD row (all countries / Canada total).

    Partner breakouts are not useful for electricity: Census lists Canada and
    aggregate regions at the same national total; Mexico is absent (BOP coverage
    adds MX outside this HS extract). Keep the table national × direction.
    """
    if direction == 'exports':
        path = 'exports/hs'
        get = 'E_COMMODITY,ALL_VAL_YR,QTY_1_YR,UNIT_QY1'
        code_col = 'E_COMMODITY'
        value_col = 'ALL_VAL_YR'
        qty_col = 'QTY_1_YR'
    else:
        path = 'imports/hs'
        get = 'I_COMMODITY,GEN_VAL_YR,GEN_QY1_YR,UNIT_QY1'
        code_col = 'I_COMMODITY'
        value_col = 'GEN_VAL_YR'
        qty_col = 'GEN_QY1_YR'
    params = {
        'get': get,
        'COMM_LVL': 'HS10',
        'YEAR': str(year),
        'MONTH': '12',
        code_col: _HS2716,
        'key': key,
    }
    url = f'https://api.census.gov/data/timeseries/intltrade/{path}?{urlencode(params)}'
    ctx = ssl._create_unverified_context()
    with urlopen(url, context=ctx, timeout=120) as resp:
        rows = json.loads(resp.read().decode())
    if len(rows) < 2:
        raise ValueError(f'Census HS {_HS2716} empty for {year} {direction}')
    header, body = rows[0], rows[1]
    record = dict(zip(header, body, strict=True))
    unit = str(record.get('UNIT_QY1', '')).strip().upper()
    if unit != 'MWH':
        raise ValueError(
            f'Census HS {_HS2716} {year} {direction} UNIT_QY1={unit!r}; expected MWH'
        )
    value_usd = float(record[value_col])
    qty_mwh = float(record[qty_col])
    if qty_mwh <= 0:
        raise ValueError(f'Census HS {_HS2716} {year} {direction} qty_mwh={qty_mwh}')
    return {
        'year': int(year),
        'direction': direction,
        'hs_commodity': _HS2716,
        'value_usd': value_usd,
        'qty_mwh': qty_mwh,
        'unit_qy1': unit,
        'unit_value_usd_per_mwh': value_usd / qty_mwh,
    }


def refresh_hs2716_electricity_unit_value_csv(
    years: tuple[int, ...] | list[int] = _HS2716_DEFAULT_YEARS,
    *,
    path: Path | None = None,
    pause_s: float = 0.25,
) -> Path:
    """Rewrite the HS 2716 unit-value CSV from Census intltrade HS API.

    Shortcut until a full Census HS FBA extract exists: pulls only HS10
    ``2716000000`` for each year×direction. When a durable HS extract lands,
    replace this with a filter on that FBA and keep the same CSV schema (or read
    unit values from the FBA directly).

    Requires a Census API key (``api_key`` / Census env).
    """
    out = path or hs2716_unit_value_csv_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    key = _census_key()
    rows: list[dict[str, Any]] = []
    for year in years:
        for direction in ('exports', 'imports'):
            rows.append(_fetch_hs2716_national_row(int(year), direction, key))
            time.sleep(pause_s)
    df = pd.DataFrame(rows).sort_values(['year', 'direction']).reset_index(drop=True)
    df.to_csv(out, index=False)
    return out


if __name__ == '__main__':
    written = refresh_hs2716_electricity_unit_value_csv()
    print(f'Wrote {written}')
    print(pd.read_csv(written).to_string(index=False))
