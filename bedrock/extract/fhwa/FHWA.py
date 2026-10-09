"""FHWA Highway Statistics extracts (MF-21, VM-1, MV-7) and highway fuel shares."""

from __future__ import annotations

import os
from io import BytesIO
from typing import Any, Callable
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS

# ---------------------------------------------------------------------------
# Multi-table load (MECS-style)
# ---------------------------------------------------------------------------


def fhwa_url_helper(
    *, build_url: str, config: dict[str, Any], year: str, **_kwargs: Any
) -> list[str]:
    return [
        build_url.replace('__year__', str(year)).replace('__table__', str(table))
        for table in config['tables']
    ]


def fhwa_call(
    *, resp: Any, url: str | None = None, **_kwargs: Any
) -> list[pd.DataFrame]:
    df = pd.read_excel(BytesIO(resp.content), header=None)
    df.attrs['fhwa_table'] = (
        os.path.basename(urlparse(url or '').path).lower().rsplit('.', 1)[0]
    )
    return [df]


def fhwa_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **kwargs: Any
) -> pd.DataFrame:
    parsers: dict[str, Callable[..., pd.DataFrame]] = {
        'mf21': fhwa_mf21_parse,
        'mv7': fhwa_mv7_parse,
        'vm1': fhwa_vm1_parse,
    }
    return pd.concat(
        [
            parsers[df.attrs['fhwa_table']](
                df_list=[df], source=source, year=year, **kwargs
            )
            for df in df_list
        ],
        ignore_index=True,
    )


def _attach_fba_meta(
    df: pd.DataFrame,
    *,
    source: str,
    year: str | int,
    description: str,
    cls: str,
    unit: str | None = None,
    reliability: int = 3,
) -> pd.DataFrame:
    df['SourceName'] = source
    df['Year'] = int(year)
    df['Description'] = description
    df['Class'] = cls
    if unit is not None:
        df['Unit'] = unit
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['DataReliability'] = reliability
    df['DataCollection'] = 5
    return df


# ---------------------------------------------------------------------------
# MF-21 — motor-fuel use by ownership (national Total)
# ---------------------------------------------------------------------------

# (column, FlowName, ActivityConsumedBy owner).
# Description = Table MF-21 only. Highway / nonhighway live in FlowName
# (same table, not separate tables; not ActivityProducedBy).
_MF21_COLUMNS_LEGACY: list[tuple[int, str, str]] = [
    (1, 'Gasoline highway', 'Private and Commercial'),
    (2, 'Gasoline highway', 'Federal Civilian'),
    (3, 'Gasoline highway', 'State, County and Municipal'),
    (4, 'Gasoline highway', 'Public Total'),
    (5, 'Gasoline highway', 'Total'),
    (6, 'Gasoline nonhighway', 'Private and Commercial'),
    (7, 'Gasoline nonhighway', 'State, County and Municipal'),
    (8, 'Gasoline nonhighway', 'Total'),
    (9, 'Gasoline total use', 'Total'),
    (11, 'Gasoline total consumption', 'Total'),
    (12, 'Special fuel highway', 'Private and Commercial'),
    (13, 'Gasoline plus special fuel highway', 'Total'),
]

# 2024+ MF-21: under each owner, GASOLINE then SPECIAL FUEL (interleaved).
_MF21_COLUMNS_2024: list[tuple[int, str, str]] = [
    (1, 'Gasoline highway', 'Private and Commercial'),
    (2, 'Special fuel highway', 'Private and Commercial'),
    (3, 'Gasoline highway', 'Federal Civilian'),
    (4, 'Special fuel highway', 'Federal Civilian'),
    (5, 'Gasoline highway', 'State, County and Municipal'),
    (6, 'Special fuel highway', 'State, County and Municipal'),
    (7, 'Gasoline highway', 'Public Total'),
    (8, 'Special fuel highway', 'Public Total'),
    (9, 'Gasoline highway', 'Total'),
    (10, 'Special fuel highway', 'Total'),
    (11, 'Gasoline nonhighway', 'Private and Commercial'),
    (12, 'Special fuel nonhighway', 'Private and Commercial'),
    (13, 'Gasoline nonhighway', 'State, County and Municipal'),
    (14, 'Special fuel nonhighway', 'State, County and Municipal'),
    (15, 'Gasoline nonhighway', 'Total'),
    (16, 'Special fuel nonhighway', 'Total'),
    (17, 'Gasoline total use', 'Total'),
    (18, 'Special fuel total use', 'Total'),
    (21, 'Gasoline total consumption', 'Total'),
    (22, 'Special fuel total consumption', 'Total'),
]


def fhwa_mf21_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **_kwargs: Any
) -> pd.DataFrame:
    raw = df_list[0]
    y = int(year)
    columns = _MF21_COLUMNS_2024 if y >= 2024 else _MF21_COLUMNS_LEGACY
    total_row = None
    for _, series in raw.iterrows():
        row = list(series.values)
        if (
            row
            and row[0] is not None
            and str(row[0]).strip() in ('Total', '     Total')
        ):
            total_row = row
            break
    if total_row is None:
        raise ValueError(f'FHWA MF-21 Total row not found for {year}')

    records = []
    for col, flow_name, owner in columns:
        if col >= len(total_row) or total_row[col] in (None, ''):
            continue
        records.append(
            {
                'ActivityConsumedBy': owner,
                'FlowName': flow_name,
                'FlowAmount': float(total_row[col]) * 1000.0,
            }
        )
    df = pd.DataFrame.from_records(records)
    df['ActivityProducedBy'] = np.nan
    return _attach_fba_meta(
        df,
        source=source,
        year=year,
        description='Table MF-21',
        unit='gal',
        cls='Energy',
    )


# ---------------------------------------------------------------------------
# VM-1 — national VMT, fuel, and mpg by vehicle type
# ---------------------------------------------------------------------------

_VEHICLE_TYPES: list[tuple[int, str]] = [
    (2, 'Light Duty Vehicles Short WB'),
    (3, 'Motorcycles'),
    (4, 'Buses'),
    (5, 'Light Duty Vehicles Long WB'),
    (6, 'Single-Unit Trucks'),
    (7, 'Combination Trucks'),
    (8, 'All Light Duty Vehicles'),
    (9, 'Single-Unit 2-Axle 6-Tire or More and Combination Trucks'),
    (10, 'All Motor Vehicles'),
]

_METRICS: list[tuple[str, str, str]] = [
    ('Total Rural and Urban', 'Vehicle miles', 'millions'),
    ('Fuel consumed', 'Fuel consumed', 'thousand gallons'),
    ('Average miles traveled per', 'Average miles per gallon', 'miles per gallon'),
    ('Number of motor vehicles', 'Number of motor vehicles', 'vehicles'),
]


def fhwa_vm1_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **_kwargs: Any
) -> pd.DataFrame:
    raw = df_list[0]
    y = int(year)
    records: list[dict[str, Any]] = []
    for _, series in raw.iterrows():
        row = list(series.values)
        if not row or row[0] != y:
            continue
        item = str(row[1] or '')
        metric = next(((fn, u) for key, fn, u in _METRICS if key in item), None)
        if metric is None:
            continue
        flow_name, unit = metric
        for col, vehicle in _VEHICLE_TYPES:
            if col < len(row) and row[col] not in (None, ''):
                records.append(
                    {
                        'ActivityProducedBy': vehicle,
                        'FlowName': flow_name,
                        'FlowAmount': float(row[col]),
                        'Unit': unit,
                    }
                )
    df = pd.DataFrame.from_records(records)
    df['ActivityConsumedBy'] = np.nan
    return _attach_fba_meta(
        df, source=source, year=year, description='Table VM-1', cls='Other'
    )


# ---------------------------------------------------------------------------
# MV-7 — publicly owned vehicles (federal vs SCM truck stock)
# ---------------------------------------------------------------------------


def fhwa_mv7_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **_kwargs: Any
) -> pd.DataFrame:
    raw = df_list[0]
    total_row = None
    for _, series in raw.iterrows():
        row = list(series.values)
        if row and row[0] is not None and str(row[0]).strip() == 'Total':
            total_row = row
            break
    if total_row is None:
        raise ValueError(f'FHWA MV-7 Total row not found for {year}')

    records = [
        {
            'ActivityConsumedBy': owner,
            'FlowName': 'Trucks',
            'FlowAmount': float(total_row[col]),
        }
        for col, owner in (
            (3, 'Federal'),
            (9, 'State, County and Municipal'),
        )
        if col < len(total_row) and total_row[col] not in (None, '')
    ]
    df = pd.DataFrame.from_records(records)
    df['ActivityProducedBy'] = np.nan
    return _attach_fba_meta(
        df,
        source=source,
        year=year,
        description='Table MV-7',
        unit='vehicles',
        cls='Other',
        reliability=5,
    )


# ---------------------------------------------------------------------------
# Highway fuel sector shares (Energy_highway_fuel_shares_national_*.yaml)
# ---------------------------------------------------------------------------
# Federal nest: FFR Total Civilian + Total USPS.
# State, County and Municipal nest: Nowcast Use of 324110 (YAML clean_source /
# proportional attribution — never load_bea_use_table).
# Full priv weight on each private landing (Approach A).

_FFR_CIV = 'Total Civilian Agencies'
_FFR_USPS = 'Total U.S. Postal Service'
_FFR_TO_SECTOR = {_FFR_CIV: 'S00600', _FFR_USPS: '491'}
_STATE_COUNTY_MUNICIPAL_CODES = ('GSLGE', 'GSLGH', 'GSLGO', 'S00203')
_STATE_COUNTY_MUNICIPAL = 'State, County and Municipal'


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    total = sum(weights.values())
    if total <= 0:
        raise ValueError('Cannot normalize empty or zero highway-share weights')
    return {k: v / total for k, v in weights.items()}


def _ffr_civ_usps_fuel_shares(
    ffr: pd.DataFrame, *, fuels: set[str]
) -> dict[str, float]:
    sub = ffr.loc[
        ffr['ActivityConsumedBy'].isin(_FFR_TO_SECTOR) & ffr['FlowName'].isin(fuels)
    ].assign(sector=lambda d: d['ActivityConsumedBy'].map(_FFR_TO_SECTOR))
    buckets = sub.groupby('sector', sort=False)['FlowAmount'].sum().to_dict()
    return _normalize(
        {
            'S00600': float(buckets.get('S00600', 0.0)),
            '491': float(buckets.get('491', 0.0)),
        }
    )


def _state_county_municipal_use_weights(use: pd.DataFrame) -> dict[str, float]:
    """Normalize Use of 324110 among State/County/Municipal landing sectors.

    ``use`` must come from YAML ``clean_source.Nowcast_Detail_Use_AfterRedef``
    (or equivalent), not from ``load_bea_use_table``.
    """
    produced = use['ActivityProducedBy'].astype(str)
    consumed = use['ActivityConsumedBy'].astype(str)
    sub = use.loc[
        produced.eq('324110') & consumed.isin(_STATE_COUNTY_MUNICIPAL_CODES),
        ['ActivityConsumedBy', 'FlowAmount'],
    ]
    buckets = {
        code: float(
            pd.to_numeric(
                sub.loc[sub['ActivityConsumedBy'].astype(str).eq(code), 'FlowAmount'],
                errors='coerce',
            )
            .fillna(0.0)
            .sum()
        )
        for code in _STATE_COUNTY_MUNICIPAL_CODES
    }
    return _normalize(buckets)


def _emit_highway_shares(
    fba: pd.DataFrame,
    *,
    flowable: str,
    fed: float,
    state_county_municipal: float,
    priv: float,
    fed_agency: dict[str, float],
    state_county_municipal_weights: dict[str, float] | None,
    include_state_county_municipal: bool,
) -> pd.DataFrame:
    """Share rows: full priv on each private sector; no union-wide renorm."""
    sec_col = 'PrimarySector' if 'PrimarySector' in fba.columns else 'SectorConsumedBy'
    ssn_col = (
        'PrimarySectorSourceName'
        if 'PrimarySectorSourceName' in fba.columns
        else 'SectorSourceName'
    )
    priv_pairs = list(
        fba.loc[
            fba['ActivityConsumedBy'] == 'Private and Commercial', [ssn_col, sec_col]
        ]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    template = fba.iloc[0].to_dict()
    rows: list[dict[str, Any]] = []

    def add(ssn: str, sec: str, weight: float, activity: str) -> None:
        if weight <= 0:
            return
        row = dict(template)
        row.update(
            {
                'FlowAmount': float(weight),
                'Unit': 'share',
                'Class': 'Energy',
                'FlowName': flowable,
                'ActivityConsumedBy': activity,
                sec_col: sec,
                ssn_col: ssn,
            }
        )
        if 'Flowable' in row:
            row['Flowable'] = flowable
        for col, val in (
            ('SectorConsumedBy', sec),
            ('SectorSourceName', ssn),
            ('PrimarySector', sec),
            ('PrimarySectorSourceName', ssn),
        ):
            if col in row:
                row[col] = val
        rows.append(row)

    for sec, w in fed_agency.items():
        add(
            'NAICS_2017_Code' if sec == '491' else 'BEA_2017_Code',
            sec,
            fed * w,
            'Federal Civilian',
        )
    if include_state_county_municipal:
        if not state_county_municipal_weights:
            raise ValueError(
                'State, County and Municipal nest requires Use weights from '
                'clean_source Nowcast_Detail_Use_AfterRedef'
            )
        for sec, w in state_county_municipal_weights.items():
            add(
                'BEA_2017_Code',
                sec,
                state_county_municipal * w,
                _STATE_COUNTY_MUNICIPAL,
            )
    for ssn, sec in priv_pairs:
        add(str(ssn), str(sec), priv, 'Private and Commercial')

    return pd.DataFrame.from_records(rows).reset_index(drop=True)


def _load_fba(name: str, year: int, download: bool) -> pd.DataFrame:
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415

    return pd.DataFrame(getFlowByActivity(name, year, download_FBA_if_missing=download))


def _mf21_owner_totals(
    mf21: pd.DataFrame,
) -> tuple[float, float, float, float]:
    owners = {
        o: float(mf21.loc[mf21['ActivityConsumedBy'] == o, 'FlowAmount'].sum())
        for o in (
            'Federal Civilian',
            _STATE_COUNTY_MUNICIPAL,
            'Private and Commercial',
        )
    }
    total = sum(owners.values())
    return (
        owners['Federal Civilian'],
        owners[_STATE_COUNTY_MUNICIPAL],
        owners['Private and Commercial'],
        total,
    )


def scale_attributed_to_owner_share(
    fba: pd.DataFrame, download_sources_ok: bool = True, **_kwargs: Any
) -> pd.DataFrame:
    """After proportional Use peel, scale FlowAmounts to the owner share of MF-21."""
    from bedrock.extract.flowbyactivity import FlowByActivity  # noqa: PLC0415

    year = int(fba.config.get('year', fba['Year'].iloc[0]))
    clean = fba.config.get('clean_source') or {}
    fhwa_cfg = clean.get('FHWA_Highway_Statistics') or {}
    mf21 = _load_fba(
        'FHWA_Highway_Statistics',
        int(fhwa_cfg.get('year', year)),
        download_sources_ok,
    )
    for col, wanted in (fhwa_cfg.get('selection_fields') or {}).items():
        values = wanted if isinstance(wanted, list) else [wanted]
        mf21 = mf21[mf21[col].isin(values)]
    _fed, state_county_municipal, _priv, total = _mf21_owner_totals(mf21)
    if total <= 0:
        raise ValueError('MF-21 owner total is zero; cannot scale to owner share')
    owner_share = state_county_municipal / total
    attributed = float(
        pd.to_numeric(fba['FlowAmount'], errors='coerce').fillna(0).sum()
    )
    out = fba.copy()
    if attributed > 0:
        out['FlowAmount'] = (
            pd.to_numeric(out['FlowAmount'], errors='coerce').fillna(0.0)
            / attributed
            * owner_share
        )
    out['Unit'] = 'share'
    if 'Flowable' in out.columns:
        flow = str(fhwa_cfg.get('selection_fields', {}).get('FlowName', 'Gasoline'))
        out['Flowable'] = 'Gasoline' if flow.startswith('Gasoline') else 'Diesel'
    return FlowByActivity(out, full_name=fba.full_name, config=fba.config)


def gasoline_highway_fuel_shares(
    fba: pd.DataFrame, download_sources_ok: bool = True, **_kwargs: Any
) -> pd.DataFrame:
    """Federal + private gasoline shares; federal nested with FFR Civ+USPS.

    State, County and Municipal is a separate proportional Use activity set.
    """
    from bedrock.extract.flowbyactivity import FlowByActivity  # noqa: PLC0415

    year = int(fba.config.get('year', fba['Year'].iloc[0]))
    clean = fba.config.get('clean_source') or {}
    ffr = _load_fba(
        'GSA_FFR',
        int(clean.get('GSA_FFR', {}).get('year', year)),
        download_sources_ok,
    )
    fhwa_cfg = clean.get('FHWA_Highway_Statistics') or {}
    mf21 = _load_fba(
        'FHWA_Highway_Statistics',
        int(fhwa_cfg.get('year', year)),
        download_sources_ok,
    )
    for col, wanted in (fhwa_cfg.get('selection_fields') or {}).items():
        values = wanted if isinstance(wanted, list) else [wanted]
        mf21 = mf21[mf21[col].isin(values)]
    fed, state_county_municipal, priv, _total = _mf21_owner_totals(mf21)
    shares = _normalize(
        {
            'fed': fed,
            'state_county_municipal': state_county_municipal,
            'priv': priv,
        }
    )
    return FlowByActivity(
        _emit_highway_shares(
            fba,
            flowable='Gasoline',
            fed=shares['fed'],
            state_county_municipal=shares['state_county_municipal'],
            priv=shares['priv'],
            fed_agency=_ffr_civ_usps_fuel_shares(ffr, fuels={'Gasoline'}),
            state_county_municipal_weights=None,
            include_state_county_municipal=False,
        ),
        full_name=fba.full_name,
        config=fba.config,
    )


def diesel_highway_fuel_shares(
    fba: pd.DataFrame, download_sources_ok: bool = True, **_kwargs: Any
) -> pd.DataFrame:
    """Method C diesel: FFR Civ+USPS federal + MV-7 State/County/Municipal + SF private.

    State/County/Municipal sector nest uses Nowcast Use from YAML clean_source.
    """
    from bedrock.extract.flowbyactivity import FlowByActivity  # noqa: PLC0415

    year = int(fba.config.get('year', fba['Year'].iloc[0]))
    clean = fba.config.get('clean_source') or {}
    if 'Nowcast_Detail_Use_AfterRedef' not in clean:
        raise ValueError(
            'diesel_highway_fuel_shares requires clean_source.'
            'Nowcast_Detail_Use_AfterRedef for State, County and Municipal nest'
        )
    fhwa_cfg = clean.get('FHWA_Highway_Statistics') or {}
    use_cfg = clean['Nowcast_Detail_Use_AfterRedef']
    ffr = _load_fba(
        'GSA_FFR',
        int(clean.get('GSA_FFR', {}).get('year', year)),
        download_sources_ok,
    )
    mv7 = _load_fba(
        'FHWA_Highway_Statistics',
        int(fhwa_cfg.get('year', year)),
        download_sources_ok,
    )
    for col, wanted in (fhwa_cfg.get('selection_fields') or {}).items():
        values = wanted if isinstance(wanted, list) else [wanted]
        mv7 = mv7[mv7[col].isin(values)]
    use = _load_fba(
        'Nowcast_Detail_Use_AfterRedef',
        int(use_cfg.get('year', year)),
        download_sources_ok,
    )

    diesel_fuels = {
        'Diesel',
        'Biodiesel (B20)',
        'Biodiesel (B100)',
        'Renewable Diesel',
    }
    civ_usps = set(_FFR_TO_SECTOR)
    ffr_diesel = float(
        ffr.loc[
            ffr['ActivityConsumedBy'].isin(civ_usps)
            & ffr['FlowName'].isin(diesel_fuels),
            'FlowAmount',
        ].sum()
    )
    civ_diesel = float(
        ffr.loc[
            (ffr['ActivityConsumedBy'] == _FFR_CIV)
            & ffr['FlowName'].isin(diesel_fuels),
            'FlowAmount',
        ].sum()
    )
    mv_fed = float(mv7.loc[mv7['ActivityConsumedBy'] == 'Federal', 'FlowAmount'].sum())
    mv_state = float(
        mv7.loc[
            mv7['ActivityConsumedBy'] == _STATE_COUNTY_MUNICIPAL, 'FlowAmount'
        ].sum()
    )
    shares = _normalize(
        {
            'fed': ffr_diesel,
            'state_county_municipal': civ_diesel / mv_fed * mv_state,
            'priv': float(
                fba.loc[
                    fba['ActivityConsumedBy'] == 'Private and Commercial', 'FlowAmount'
                ].sum()
            ),
        }
    )
    return FlowByActivity(
        _emit_highway_shares(
            fba,
            flowable='Diesel',
            fed=shares['fed'],
            state_county_municipal=shares['state_county_municipal'],
            priv=shares['priv'],
            fed_agency=_ffr_civ_usps_fuel_shares(ffr, fuels=diesel_fuels),
            state_county_municipal_weights=_state_county_municipal_use_weights(use),
            include_state_county_municipal=True,
        ),
        full_name=fba.full_name,
        config=fba.config,
    )
