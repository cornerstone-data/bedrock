"""Parse FHWA Highway Statistics tables and build highway fuel sector shares"""

from __future__ import annotations

import os
from io import BytesIO
from typing import Any, Callable
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import US_FIPS


def fhwa_url_helper(
    *, build_url: str, config: dict[str, Any], year: str, **_kwargs: Any
) -> list[str]:
    """Build download URLs for each configured FHWA table"""
    return [
        build_url.replace('__year__', str(year)).replace('__table__', str(table))
        for table in config['tables']
    ]


def fhwa_call(
    *, resp: Any, url: str | None = None, **_kwargs: Any
) -> list[pd.DataFrame]:
    """Read an Excel response and tag it with the table name"""
    df = pd.read_excel(BytesIO(resp.content), header=None)
    df.attrs['fhwa_table'] = (
        os.path.basename(urlparse(url or '').path).lower().rsplit('.', 1)[0]
    )
    return [df]


def fhwa_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **kwargs: Any
) -> pd.DataFrame:
    """Concat data"""
    parsers: dict[str, Callable[..., pd.DataFrame]] = {
        'mf21': fhwa_mf21_parse,
        'mv7': fhwa_mv7_parse,
        'mv10': fhwa_mv10_parse,
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
    """Add standard Flow-By-Activity metadata columns"""
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


# MF-21 — motor fuel use by owner
# (column index, FlowName, owner) — highway vs nonhighway is in FlowName
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

# 2024 onward: gasoline and special fuel columns alternate under each owner
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
    """Motor fuel use by owner"""
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


# VM-1 — national miles, fuel, and mpg by vehicle type
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
    """Vehicle miles, fuel, and mpg by vehicle type"""
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


# MV-7 — publicly owned autos, buses, and trucks
# Federal in cols 1–3; state/county/municipal in 7–9
_MV7_COLUMNS: list[tuple[int, str, str]] = [
    (1, 'Automobiles', 'Federal'),
    (2, 'Buses', 'Federal'),
    (3, 'Trucks', 'Federal'),
    (7, 'Automobiles', 'State, County and Municipal'),
    (8, 'Buses', 'State, County and Municipal'),
    (9, 'Trucks', 'State, County and Municipal'),
]


def fhwa_mv7_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **_kwargs: Any
) -> pd.DataFrame:
    """Publicly owned cars, buses, and trucks"""
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
            'FlowName': flow_name,
            'FlowAmount': float(total_row[col]),
        }
        for col, flow_name, owner in _MV7_COLUMNS
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


# MV-10 — bus registrations by owner
# https://www.fhwa.dot.gov/policyinformation/statistics/2024/mv10.cfm
_MV10_COLUMNS: list[tuple[int, str]] = [
    (3, 'Private and Commercial'),
    (4, 'Federal'),
    (5, 'State, County and Municipal'),
]


def fhwa_mv10_parse(
    *, df_list: list[pd.DataFrame], source: str, year: str, **_kwargs: Any
) -> pd.DataFrame:
    """Bus registrations by owner"""
    raw = df_list[0]
    total_row = None
    for _, series in raw.iterrows():
        row = list(series.values)
        if row and row[0] is not None and str(row[0]).strip() == 'Total':
            total_row = row
            break
    if total_row is None:
        raise ValueError(f'FHWA MV-10 Total row not found for {year}')

    records = [
        {
            'ActivityConsumedBy': owner,
            'FlowName': 'Buses',
            'FlowAmount': float(total_row[col]),
        }
        for col, owner in _MV10_COLUMNS
        if col < len(total_row) and total_row[col] not in (None, '')
    ]
    df = pd.DataFrame.from_records(records)
    df['ActivityProducedBy'] = np.nan
    return _attach_fba_meta(
        df,
        source=source,
        year=year,
        description='Table MV-10',
        unit='vehicles',
        cls='Other',
        reliability=5,
    )


# Highway fuel sector shares (Energy_highway_fuel_shares_national_*.yaml)


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    """Scale weights so they sum to 1"""
    total = sum(weights.values())
    if total <= 0:
        raise ValueError('Cannot normalize empty or zero highway-share weights')
    return {k: v / total for k, v in weights.items()}


def _ffr_civ_usps_fuel_shares(
    ffr: pd.DataFrame, *, fuels: set[str], gsa: pd.DataFrame
) -> list[tuple[str, str, float]]:
    """Split federal fuel across GSA-mapped civilian and Postal Service sectors"""
    act_to = {
        str(r.Activity): (str(r.SectorSourceName), str(r.Sector))
        for r in gsa.itertuples(index=False)
    }
    sub = ffr.loc[
        ffr['ActivityConsumedBy'].astype(str).isin(act_to) & ffr['FlowName'].isin(fuels)
    ].copy()
    if sub.empty:
        raise ValueError('No GSA FFR fuel for crosswalked civilian/Postal activities')
    mapped = sub['ActivityConsumedBy'].astype(str).map(act_to)
    sub['SectorSourceName'] = mapped.map(lambda p: p[0])
    sub['Sector'] = mapped.map(lambda p: p[1])
    grouped = (
        sub.groupby(['SectorSourceName', 'Sector'], sort=False)['FlowAmount']
        .sum()
        .reset_index(name='FlowAmount')
    )
    buckets: dict[tuple[str, str], float] = {}
    for _, row in grouped.iterrows():
        buckets[(str(row['SectorSourceName']), str(row['Sector']))] = float(
            row['FlowAmount']
        )
    total = sum(buckets.values())
    if total <= 0:
        raise ValueError('GSA FFR civilian/Postal fuel total is zero')
    return [
        (sector_source_name, sec, amt / total)
        for (sector_source_name, sec), amt in buckets.items()
    ]


def _state_county_municipal_use_weights(
    use: pd.DataFrame,
    *,
    use_commodity_for_sector_weights: str,
) -> list[tuple[str, str, float]]:
    """How to split state/county/municipal fuel across FHWA-mapped sectors"""
    from bedrock.utils.mapping.sectormapping import (  # noqa: PLC0415
        get_activitytosector_mapping,
    )

    cw = get_activitytosector_mapping('FHWA')
    rows = cw.loc[cw['Activity'].astype(str).eq('State, County and Municipal')]
    pairs = [
        (str(r.SectorSourceName), str(r.Sector)) for r in rows.itertuples(index=False)
    ]
    codes = [sec for _sector_source_name, sec in pairs]
    produced = use['ActivityProducedBy'].astype(str)
    consumed = use['ActivityConsumedBy'].astype(str)
    sub = use.loc[
        produced.eq(use_commodity_for_sector_weights) & consumed.isin(codes),
        ['ActivityConsumedBy', 'FlowAmount'],
    ]
    amounts = {
        sec: float(
            pd.to_numeric(
                sub.loc[sub['ActivityConsumedBy'].astype(str).eq(sec), 'FlowAmount'],
                errors='coerce',
            )
            .fillna(0.0)
            .sum()
        )
        for _sector_source_name, sec in pairs
    }
    normed = _normalize(amounts)
    return [(sector_source_name, sec, normed[sec]) for sector_source_name, sec in pairs]


def _emit_highway_shares(
    fba: pd.DataFrame,
    *,
    flowable: str,
    vehicle_class: str,
    fed: float,
    state_county_municipal: float,
    nonpublic: float,
    fed_agency: list[tuple[str, str, float]],
    state_county_municipal_weights: list[tuple[str, str, float]],
    nonpublic_weights: list[tuple[str, str, float]],
) -> pd.DataFrame:
    """Build share rows for one fuel and vehicle class"""
    sec_col = 'PrimarySector' if 'PrimarySector' in fba.columns else 'SectorConsumedBy'
    sec_source_name_col = (
        'PrimarySectorSourceName'
        if 'PrimarySectorSourceName' in fba.columns
        else 'SectorSourceName'
    )
    template = fba.iloc[0].to_dict()
    flowable_class = f'{flowable}; {vehicle_class}'
    rows: list[dict[str, Any]] = []

    def add(sec_source_name: str, sec: str, weight: float, activity: str) -> None:
        if weight <= 0:
            return
        row = dict(template)
        row.update(
            {
                'FlowAmount': float(weight),
                'Unit': 'share',
                'Class': 'Energy',
                'Flowable': flowable_class,
                'ActivityConsumedBy': activity,
                sec_col: sec,
                sec_source_name_col: sec_source_name,
            }
        )
        row.pop('FlowName', None)
        for col, val in (
            ('SectorConsumedBy', sec),
            ('SectorSourceName', sec_source_name),
            ('PrimarySector', sec),
            ('PrimarySectorSourceName', sec_source_name),
        ):
            if col in row:
                row[col] = val
        rows.append(row)

    for sec_source_name, sec, w in fed_agency:
        add(sec_source_name, sec, fed * w, 'Federal Civilian')
    for sec_source_name, sec, w in state_county_municipal_weights:
        add(
            sec_source_name,
            sec,
            state_county_municipal * w,
            'State, County and Municipal',
        )
    for sec_source_name, sec, w in nonpublic_weights:
        add(sec_source_name, sec, nonpublic * w, 'Private and Commercial')

    out = pd.DataFrame.from_records(rows).reset_index(drop=True)
    if 'FlowName' in out.columns:
        out = out.drop(columns=['FlowName'])
    return out


def _mf21_owner_totals(
    mf21: pd.DataFrame,
) -> tuple[float, float, float, float]:
    """Federal, state/county/municipal, nonpublic, and total fuel from MF-21"""
    owners = {
        o: float(mf21.loc[mf21['ActivityConsumedBy'] == o, 'FlowAmount'].sum())
        for o in (
            'Federal Civilian',
            'State, County and Municipal',
            'Private and Commercial',
        )
    }
    total = sum(owners.values())
    return (
        owners['Federal Civilian'],
        owners['State, County and Municipal'],
        owners['Private and Commercial'],
        total,
    )


def _class_owner_shares(
    *,
    vehicle_class: str,
    mv7: pd.DataFrame,
    mv10: pd.DataFrame,
    fed_fuel: float,
    civ_fuel: float,
    nonpublic_fuel: float,
    gsa: pd.DataFrame,
) -> tuple[dict[str, float], list[tuple[str, str, float]]]:
    """Owner shares and federal sector weights for one vehicle class"""
    if vehicle_class == 'Buses':
        # Buses use registration counts. Postal Service has no buses, so all
        # federal bus fuel goes to GSA civilian (BEA) landings.
        civ = gsa.loc[gsa['SectorSourceName'].eq('BEA_2017_Code')]
        n = len(civ)
        fed_agency = [
            (str(r.SectorSourceName), str(r.Sector), 1.0 / n)
            for r in civ.itertuples(index=False)
        ]
        return (
            _normalize(
                {
                    'fed': float(
                        mv10.loc[
                            (mv10['FlowName'] == 'Buses')
                            & (mv10['ActivityConsumedBy'] == 'Federal'),
                            'FlowAmount',
                        ].sum()
                    ),
                    'state_county_municipal': float(
                        mv10.loc[
                            (mv10['FlowName'] == 'Buses')
                            & (
                                mv10['ActivityConsumedBy']
                                == 'State, County and Municipal'
                            ),
                            'FlowAmount',
                        ].sum()
                    ),
                    'nonpublic': float(
                        mv10.loc[
                            (mv10['FlowName'] == 'Buses')
                            & (mv10['ActivityConsumedBy'] == 'Private and Commercial'),
                            'FlowAmount',
                        ].sum()
                    ),
                }
            ),
            fed_agency,
        )
    mv_fed = float(
        mv7.loc[
            (mv7['FlowName'] == vehicle_class)
            & (mv7['ActivityConsumedBy'] == 'Federal'),
            'FlowAmount',
        ].sum()
    )
    if mv_fed <= 0:
        raise ValueError('MV-7 federal vehicle stock is zero')
    mv_scm = float(
        mv7.loc[
            (mv7['FlowName'] == vehicle_class)
            & (mv7['ActivityConsumedBy'] == 'State, County and Municipal'),
            'FlowAmount',
        ].sum()
    )
    return (
        _normalize(
            {
                'fed': fed_fuel,
                'state_county_municipal': civ_fuel / mv_fed * mv_scm,
                'nonpublic': nonpublic_fuel,
            }
        ),
        [],  # caller fills with GSA civilian vs Postal Service fuel shares
    )


def highway_fuel_shares(
    fba: pd.DataFrame, download_sources_ok: bool = True, **_kwargs: Any
) -> pd.DataFrame:
    """Split highway fuel among sectors by vehicle class and owner"""
    from bedrock.extract.flowbyactivity import (  # noqa: PLC0415
        FlowByActivity,
        getFlowByActivity,
    )
    from bedrock.utils.mapping.sectormapping import (  # noqa: PLC0415
        get_activitytosector_mapping,
    )

    params = fba.config['clean_parameter']
    flowable = params['flowable']
    ffr_fuels_set = set(params['ffr_fuels'])
    nonpublic_source = params['nonpublic_source']
    nonpublic_sectors = params['nonpublic_sectors']
    use_commodity_for_sector_weights = str(params['use_commodity_for_sector_weights'])
    ffr_cfg = params['ffr']
    use_cfg = params['use']
    mv7_cfg = params['mv7']
    mv10_cfg = params['mv10']

    ffr = pd.DataFrame(
        getFlowByActivity(
            ffr_cfg['source_name'],
            int(ffr_cfg['year']),
            download_FBA_if_missing=download_sources_ok,
        )
    )

    mv7 = pd.DataFrame(
        getFlowByActivity(
            mv7_cfg['source_name'],
            int(mv7_cfg['year']),
            download_FBA_if_missing=download_sources_ok,
        )
    )
    for col, wanted in mv7_cfg['selection_fields'].items():
        values = wanted if isinstance(wanted, list) else [wanted]
        mv7 = mv7[mv7[col].isin(values)]

    mv10 = pd.DataFrame(
        getFlowByActivity(
            mv10_cfg['source_name'],
            int(mv10_cfg['year']),
            download_FBA_if_missing=download_sources_ok,
        )
    )
    for col, wanted in mv10_cfg['selection_fields'].items():
        values = wanted if isinstance(wanted, list) else [wanted]
        mv10 = mv10[mv10[col].isin(values)]

    use = pd.DataFrame(
        getFlowByActivity(
            use_cfg['source_name'],
            int(use_cfg['year']),
            download_FBA_if_missing=download_sources_ok,
        )
    )

    if nonpublic_source == 'mf21':
        mf21_cfg = params['mf21']
        mf21 = pd.DataFrame(
            getFlowByActivity(
                mf21_cfg['source_name'],
                int(mf21_cfg['year']),
                download_FBA_if_missing=download_sources_ok,
            )
        )
        for col, wanted in mf21_cfg['selection_fields'].items():
            values = wanted if isinstance(wanted, list) else [wanted]
            mf21 = mf21[mf21[col].isin(values)]
        _fed, _scm, nonpublic, _total = _mf21_owner_totals(mf21)
    elif nonpublic_source == 'attributed_nonpublic':
        nonpublic = float(
            fba.loc[
                fba['ActivityConsumedBy'] == 'Private and Commercial', 'FlowAmount'
            ].sum()
        )
    else:
        raise ValueError(
            f"clean_parameter nonpublic_source must be 'mf21' or "
            f"'attributed_nonpublic', got {nonpublic_source!r}"
        )

    gsa = (
        get_activitytosector_mapping('GSA')[['Activity', 'SectorSourceName', 'Sector']]
        .astype(str)
        .drop_duplicates()
    )
    fhwa_cw = get_activitytosector_mapping('FHWA')
    nonpublic_cw = fhwa_cw.loc[
        fhwa_cw['Activity'].astype(str).eq('Private and Commercial')
    ]
    sector_to_sec_source_name = {
        str(r.Sector): str(r.SectorSourceName)
        for r in nonpublic_cw.itertuples(index=False)
    }
    gsa_acts = set(gsa['Activity'])
    civ_acts = set(gsa.loc[gsa['SectorSourceName'].eq('BEA_2017_Code'), 'Activity'])
    ffr_fed = float(
        ffr.loc[
            ffr['ActivityConsumedBy'].astype(str).isin(gsa_acts)
            & ffr['FlowName'].isin(ffr_fuels_set),
            'FlowAmount',
        ].sum()
    )
    civ_fuel = float(
        ffr.loc[
            ffr['ActivityConsumedBy'].astype(str).isin(civ_acts)
            & ffr['FlowName'].isin(ffr_fuels_set),
            'FlowAmount',
        ].sum()
    )
    ffr_fed_agency = _ffr_civ_usps_fuel_shares(ffr, fuels=ffr_fuels_set, gsa=gsa)
    scm_weights = _state_county_municipal_use_weights(
        use, use_commodity_for_sector_weights=use_commodity_for_sector_weights
    )
    vehicle_classes = mv7_cfg['selection_fields']['FlowName']

    parts: list[pd.DataFrame] = []
    for vehicle_class in vehicle_classes:
        shares, fed_agency = _class_owner_shares(
            vehicle_class=vehicle_class,
            mv7=mv7,
            mv10=mv10,
            fed_fuel=ffr_fed,
            civ_fuel=civ_fuel,
            nonpublic_fuel=nonpublic,
            gsa=gsa,
        )
        if not fed_agency:
            fed_agency = ffr_fed_agency
        codes = [str(sec) for sec in nonpublic_sectors[vehicle_class]]
        pairs = [(sector_to_sec_source_name[sec], sec) for sec in codes]
        if len(pairs) == 1:
            nonpublic_weights = [(pairs[0][0], pairs[0][1], 1.0)]
        else:
            code_set = set(codes)
            produced = use['ActivityProducedBy'].astype(str)
            consumed = use['ActivityConsumedBy'].astype(str)
            sub = use.loc[
                produced.eq(use_commodity_for_sector_weights) & consumed.isin(code_set),
                ['ActivityConsumedBy', 'FlowAmount'],
            ]
            amounts = {
                sec: float(
                    pd.to_numeric(
                        sub.loc[
                            sub['ActivityConsumedBy'].astype(str).eq(sec), 'FlowAmount'
                        ],
                        errors='coerce',
                    )
                    .fillna(0.0)
                    .sum()
                )
                for _sec_source_name, sec in pairs
            }
            normed = _normalize(amounts)
            nonpublic_weights = [
                (sec_source_name, sec, normed[sec]) for sec_source_name, sec in pairs
            ]
        parts.append(
            _emit_highway_shares(
                fba,
                flowable=str(flowable),
                vehicle_class=str(vehicle_class),
                fed=shares['fed'],
                state_county_municipal=shares['state_county_municipal'],
                nonpublic=shares['nonpublic'],
                fed_agency=fed_agency,
                state_county_municipal_weights=scm_weights,
                nonpublic_weights=nonpublic_weights,
            )
        )
    return FlowByActivity(
        pd.concat(parts, ignore_index=True),
        full_name=fba.full_name,
        config=fba.config,
    )
