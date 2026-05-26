# UMD_GHGIA.py — University of Maryland GHG Inventory and Analysis
# !/usr/bin/env python3
# coding=utf-8
"""
Greenhouse Gas Inventory and Analysis (GHGIA) for the U.S.
https://ghgi.cgs.umd.edu/data.html
"""

import os
import posixpath
import re
from typing import Any, List, cast

import numpy as np
import pandas as pd

from bedrock.extract.flowbyactivity import FlowByActivity
from bedrock.transform.flowbyfunctions import (
    assign_fips_location_system,
    load_fba_w_standardized_units,
)
from bedrock.utils.config.schema import flow_by_activity_fields
from bedrock.utils.io.gcp import download_gcs_file
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_dir_for_gcs_sub_bucket
from bedrock.utils.logging.flowsa_log import log

UMD_SOURCE_PREFIX = 'UMD_GHGIA_T_'

# Applied when a table omits these keys in UMD_GHGIA.yaml; explicit yaml entries override.
DEFAULT_UMD_TABLE_CLASS = 'Chemicals'
DEFAULT_UMD_TABLE_UNIT = 'MMT CO2e'
DEFAULT_UMD_TABLE_COMPARTMENT = 'air'

# Inventory-year columns share the same staged CSVs; GCS and local cache use this folder
# (``extract/input-data/UMD_GHGIA/{year}/`` on ``cornerstone-default``).
UMD_GHGIA_INPUT_LAYOUT_YEAR = '2024'


SECTOR_DICT = {
    'Res.': 'Residential',
    'Comm.': 'Commercial',
    'Ind.': 'Industrial',
    'Trans.': 'Transportation',
    'Elec.': 'Electricity Power',
    'Terr.': 'U.S. Territory',
}

ANNEX_HEADERS = {
    'Total Consumption (TBtu) a': 'Total Consumption (TBtu)',
    'Total Consumption (TBtu)a': 'Total Consumption (TBtu)',
    'Adjustments (TBtu) b': 'Adjustments (TBtu)',
    'Adjusted Consumption (TBtu) a': 'Adjusted Consumption (TBtu)',
    'Adjusted Consumption (TBtu)a': 'Adjusted Consumption (TBtu)',
    'Emissions b (MMT CO2 Eq.) from Energy Use': 'Emissions (MMT CO2 Eq.) from Energy Use',
    'Emissionsb (MMT CO2 Eq.) from Energy Use': 'Emissions (MMT CO2 Eq.) from Energy Use',
}

# Tables for annual CO2 emissions from fossil fuel combustion
ANNEX_ENERGY_TABLES = ['A-' + str(x) for x in list(range(4, 16))]

# UMD tables that use a two-row CSV header (Annex A-style + NEU / petroleum tables per UMD_GHGIA.yaml).
# TODO: verify staged CSV layout for 3-14/3-15 (NEU)
UMD_TWO_ROW_HEADER_TABLES = frozenset(
    [*ANNEX_ENERGY_TABLES]
)  # frozenset([*ANNEX_ENERGY_TABLES, '3-14', '3-15'])

DROP_COLS = ['Unnamed: 0'] + list(
    pd.date_range(start='1990', end='2010', freq='YE').year.astype(str)
)

YEARS = list(pd.date_range(start='2010', end='2024', freq='YE').year.astype(str))


def _cell_get_name(value: str, default_flow_name: str) -> str:
    """
    Given a single string value (cell), separate the name and units.
    :param value: str
    :param default_flow_name: indicate return flow name string subset
    :return: flow name for row
    """
    if '(' not in value:
        return default_flow_name.replace('__type__', value.strip())

    spl = value.split(' ')
    name = ''
    found_units = False
    for sub in spl:
        if '(' not in sub and not found_units:
            name = f'{name.strip()} {sub}'
        else:
            found_units = True
    return name.strip()


def _cell_get_units(value: str, default_units: str) -> str:
    """
    Given a single string value (cell), separate the name and units.
    :param value: str
    :param default_units: indicate return units string subset
    :return: unit for row
    """
    if '(' not in value:
        return default_units

    spl = value.split(' ')
    name = ''
    found_units = False
    for sub in spl:
        if ')' in sub:
            found_units = False
        if '(' in sub or found_units:
            name = f'{name} {sub.replace("(", "").replace(")", "")} '
            found_units = True
    return name.strip()


def series_separate_name_and_units(
    series: pd.Series, default_flow_name: str, default_units: str
) -> dict[str, pd.Series]:
    """
    Given a series (such as a df column), split the contents' strings into a name and units.
    An example might be converting "Carbon Stored (MMT C)" into ["Carbon Stored", "MMT C"].

    :param series: df column
    :param default_flow_name: df column for flow name to be modified
    :param default_units: df column for units to be modified
    :return: str, flowname and units for each row in df
    """
    names = series.apply(lambda x: _cell_get_name(x, default_flow_name))
    units = series.apply(lambda x: _cell_get_units(x, default_units))
    return {'names': names, 'units': units}


def _read_yearly_annex_tables(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Special handling of ANNEX Energy Tables"""
    if table == 'A-4':
        # Table "Energy Consumption Data by Fuel Type (TBtu) and Adjusted
        # Energy Consumption Data"
        # Extra row to drop in this table
        df = df.drop([0])
    header_name = ''
    newcols = []  # empty list to have new column names
    dropcols = []
    for i in range(len(df.columns)):
        fuel_type = str(df.iloc[0, i])
        for abbrev, full_name in SECTOR_DICT.items():
            fuel_type = fuel_type.replace(abbrev, full_name)
        fuel_type = fuel_type.strip()

        col_name = df.columns[i][1]
        if df.iloc[:, i].isnull().all():
            # skip over mis aligned columns
            dropcols.append(i)
            continue
        if 'Unnamed' in col_name:
            column_name = header_name
        elif col_name in ANNEX_HEADERS.keys():
            column_name = ANNEX_HEADERS[col_name]
            header_name = ANNEX_HEADERS[col_name]
        else:
            # Fallback assignment if col_name is not in ANNEX_HEADERS
            column_name = col_name.strip()
            header_name = column_name  # update the header_name for potential Unnamed columns later

        newcols.append(f'{column_name} - {fuel_type}')
        if 'Unnamed' not in col_name and col_name not in ANNEX_HEADERS:
            log.warning(f'Unknown column header encountered: {col_name}')

    df = df.drop(columns=df.columns[dropcols])
    df.columns = newcols  # assign column names
    df = df.iloc[1:, :]  # exclude first row
    df.dropna(how='all', inplace=True)
    df = df.reset_index(drop=True)
    return df


def umd_ghgia_load(**kwargs: dict[str, Any]) -> List[pd.DataFrame]:
    """Load UMD GHGIA tables from GCS under extract/input-data/UMD_GHGIA/2024/; assign UMD_GHGIA_T_* SourceNames."""
    df_list = []
    table_dict = kwargs['config']['Tables'] | kwargs['config'].get('Annex', {})
    year = str(kwargs['year'])
    for chapter, tables in table_dict.items():
        for table, data in tables.items():
            if data.get('year') not in (None, year):
                # Skip tables when the year does not align with target year
                continue
            # TODO: confirm whether UMD extract uses 3-25b (EPA-only alternate layout).
            # if year in ('2023', '2024') and table == '3-25b':
            #     continue
            df = _load_umd_ghgia_table(table)
            # for some of the imported tables, cell A2 is blank, where in the EPA GHGI tables, the column is labeled
            # "Activity". We want to retain the activities, so give the column a header to prevent being dropped
            if (
                df is not None
                and table in kwargs['config'].get('source_activity_2', [])
                and 'Unnamed' in str(df.columns[0])
            ):
                df = df.rename(columns={df.columns[0]: 'Activity'})
            if df is not None and len(df.columns) > 1:
                years = YEARS.copy()
                years.remove(year)
                df = df.drop(columns=(DROP_COLS + years), errors='ignore')
                df['SourceName'] = f'{UMD_SOURCE_PREFIX}{table.replace("-", "_")}'
                df_list.append(df)

    return df_list


def _load_umd_ghgia_table(table: str) -> pd.DataFrame:
    """Load one UMD GHGIA CSV from GCS and apply table-specific reshape."""
    # if table == '3-25b':
    #     return pd.read_csv(
    #         externaldatapath / f'GHGI_Table_{table}.csv',
    #         skiprows=2,
    #         encoding='ISO-8859-1',
    #         thousands=',',
    #     )

    chapter_dir = {
        '1': 'Chapter 1 - Introduction',
        '2': 'Chapter 2 - Trends',
        '3': 'Chapter 3 - Energy',
        '4': 'Chapter 4 - IPPU',
        '5': 'Chapter 5 - Agriculture',
        '6': 'Chapter 6 - Land Use, Land Use-Change, and Forestry',
        '7': 'Chapter 7 - Waste',
        '9': 'Chapter 9 - Recalculations',
    }
    section = table.split('-')[0]
    if section == 'A':
        rel = posixpath.join('Annex', f'Table {table}.csv')
    else:
        rel = posixpath.join(chapter_dir[section], f'Table {table}.csv')
    full = posixpath.join(
        gcs_extract_input_path('UMD_GHGIA', UMD_GHGIA_INPUT_LAYOUT_YEAR),
        rel,
    )
    gcs_sub_bucket, blob_name = posixpath.split(full)
    local_dir = local_dir_for_gcs_sub_bucket(gcs_sub_bucket)
    pth = os.path.join(local_dir, blob_name)
    if not os.path.isfile(pth):
        download_gcs_file(blob_name, gcs_sub_bucket, pth)

    use_two_row = table in UMD_TWO_ROW_HEADER_TABLES
    df = pd.read_csv(
        pth,
        skiprows=1,
        encoding='ISO-8859-1',
        thousands=',',
        header=[0, 1] if use_two_row else 0,
    )
    cols = list(df.columns)
    years = [int(c) for c in cols if str(c).isdigit()]
    if years and 'Unnamed' in str(cols[-1]):
        df = df.rename(columns={cols[-1]: str(years[-1] + 1)})
    if table in ANNEX_ENERGY_TABLES:
        return _read_yearly_annex_tables(df, table)
    elif table == '3-8':  # todo - check if necessary
        # remove notes from column headers in some years (GHGI Table 3-13 analogue)
        cols = [c[:4] for c in list(df.columns[1:])]
        return df.rename(columns=dict(zip(df.columns[1:], cols)))
    elif table in ('3-14', '3-15'):  # todo - check if necessary
        # Row 0 is header, row 1 is unit (GHGI Table 3-25 / UMD NEU & petroleum layouts).
        new_headers = []
        for col in df.columns:
            new_header = 'Unnamed: 0'
            if 'Unnamed' not in col[0]:
                if 'Unnamed' not in col[1]:
                    new_header = f'{col[0]} {col[1]}'
                else:
                    new_header = col[0]
            else:
                new_header = col[1]
            new_headers.append(new_header)
        df.columns = new_headers
        return df
    elif table == '5-15':
        # only keep string after last slash, so update activities like Cereals/Wheat and Pulses/Other/Soybeans
        col = df.columns[0]
        df[col] = df[col].str.split('/').str[-1].str.strip()
        return df
    else:
        return df


def _get_unnamed_cols(df: pd.DataFrame) -> List[str]:
    """
    Get a list of all unnamed columns, used to drop them.
    :param df: df being formatted
    :return: list, unnamed columns
    """
    return [col for col in df.columns if 'Unnamed' in col]


def get_table_meta(source_name: str, config: dict[str, Any]) -> dict[str, Any]:
    """Find and return table meta from source_name."""
    td = config.get('Annex', {}) if '_A_' in source_name else config['Tables']
    for chapter in td.keys():
        for k, v in td[chapter].items():
            if source_name.endswith(k.replace('-', '_')):
                meta = dict(v)
                if 'class' not in meta:
                    meta['class'] = DEFAULT_UMD_TABLE_CLASS
                if 'unit' not in meta:
                    meta['unit'] = DEFAULT_UMD_TABLE_UNIT
                if 'compartment' not in meta:
                    meta['compartment'] = DEFAULT_UMD_TABLE_COMPARTMENT
                return meta
    else:
        raise KeyError(f'Table meta not found for {source_name}')


def _is_consumption(source_name: str, config: dict[str, Any]) -> bool:
    """
    Determine whether the given source contains consumption or production data.
    :param source_name: df
    :return: True or False
    """
    if (
        'consum' in get_table_meta(source_name, config)['desc'].lower()
        and get_table_meta(source_name, config)['class'] != 'Chemicals'
    ):
        return True
    return False


def strip_char(text: str) -> str:
    """
    Removes the footnote chars from the text
    """
    text = text + ' '
    notes = [
        'f, g',
        ' a ',
        ' b ',
        ' c ',
        ' d ',
        ' e ',
        ' f ',
        ' g ',
        ' h ',
        ' i ',
        ' j ',
        ' k ',
        ' l ',
        ' b,c ',
        ' h,i ',
        ' f,g ',
        ')a',
        ')b',
        ')f',
        ')k',
        'b,c',
        'h,i',
        'a,b',
    ]
    for i in notes:
        if i in text:
            text_split = text.split(i)
            text = text_split[0]

    footnotes = {
        'Gasolineb': 'Gasoline',
        'Trucksa': 'Trucks',  # UMD 3-8
        'Trucksc': 'Trucks',
        'Boatsd': 'Boats',
        'Boatse': 'Boats',
        'Fuelsb': 'Fuels',
        'Fuelsf': 'Fuels',
        'Consumptiona': 'Consumption',
        'Aircraftb': 'Aircraft',  # UMD 3-8
        'Aircraftg': 'Aircraft',
        'Pipelinec': 'Pipeline',  # UMD 3-8
        'Pipelineh': 'Pipeline',
        'Pipelineg': 'Pipeline',
        'Electricityh': 'Electricity',
        'Electricityl': 'Electricity',
        'Ethanoli': 'Ethanol',
        'Biodieseli': 'Biodiesel',
        'Changee': 'Change',
        'Emissionsc': 'Emissions',
        'Equipmentc': 'Equipment',
        'Equipmentd': 'Equipment',
        'Equipmente': 'Equipment',
        'Totalf': 'Total',
        'Roadg': 'Road',
        'Otherf': 'Other',
        'Othere': 'Other',  # UMD 3-9
        'Railc': 'Rail',
        'Railb': 'Rail',  # UMD 3-9
        'Usesb': 'Uses',
        'Substancesd': 'Substances',
        'Territoriesa': 'Territories',
        'Roadb': 'Road',
        'Raile': 'Rail',
        'LPGf': 'LPG',
        'Gasf': 'Gas',
        'Gasolinec': 'Gasoline',
        'Gasolinef': 'Gasoline',
        'Fuelf': 'Fuel',
        'Amendmenta': 'Amendment',
        'Residue Nb': 'Residue N',
        'Residue Nd': 'Residue N',
        'Landa': 'Land',
        'Landb': 'Land',
        'landb': 'land',
        'landc': 'land',
        'landd': 'land',
        'Settlementsc': 'Settlements',
        'Wetlandse': 'Wetlands',
        'Settlementsf': 'Settlements',
        'Totali': 'Total',
        'Othersa': 'Others',
        'N?O': 'N2O',
        'N2Oc': 'N2O',
        'Distillate Fuel Oil (Diesel)': 'Distillate Fuel Oil',
        'Distillate Fuel Oil (Diesel': 'Distillate Fuel Oil',
        'Natural gas': 'Natural Gas',  # Fix capitalization inconsistency
        'HGLb': 'HGL',
        'Biofuels-Biodieselh': 'Biofuels-Biodiesel',
        'Biofuels-Ethanolh': 'Biofuels-Ethanol',
        'Commercial Aircraftb': 'Commercial Aircraft',  # new with UMD 3-8
        'Commercial Aircraftf': 'Commercial Aircraft',
        'Electricityk': 'Electricity',
        'Gasolinea': 'Gasoline',
        'International Bunker Fuelse': 'International Bunker Fuel',
        'Medium- and Heavy-Duty Trucksa': 'Medium- and Heavy-Duty Trucks',  # new with UMD 3-8
        'Medium- and Heavy-Duty Trucksb': 'Medium- and Heavy-Duty Trucks',
        'Recreational Boatsc': 'Recreational Boats',
        'Construction/Mining Equipmentf': 'Construction/Mining Equipment',
        'Non-Roadc': 'Non-Road',
        'Non-Roada': 'Non-Road',  # UMD 3-9
        'HFCsa': 'HFCs',
        'HFOsb': 'HFOs',
        'CO_{2}': 'CO2',
        'CH?^{c}': 'CH4',
        'CH4c': 'CH4',  # UMD 2-1
        'N_{2} O^{c}': 'N2O',
        'N_{2} O': 'N2O',
        'SF?': 'SF6',
        'NF?': 'NF3',
        'CH_{4}': 'CH4',
        'Total e,j': 'Total',
        'Naphtha (<401Â° F)': 'Naphtha (<401° F)',
        'Other Oil (>401Â° F)': 'Other Oil (>401° F)',
    }
    text = re.sub(r'\^\{[a-zA-Z]\}', '', text)

    for key in footnotes:
        text = text.replace(key, footnotes[key])

    return ' '.join(text.split())  # remove extra spaces between words


def umd_ghgia_parse(
    *, df_list: List[pd.DataFrame], year: str, config: dict[str, Any], **_kwargs: Any
) -> List[pd.DataFrame]:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :param config: dictionary, items in FBA method yaml
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    cleaned_list = []
    for df in df_list:
        source_name = df['SourceName'][0]
        table_name = source_name.removeprefix(UMD_SOURCE_PREFIX).replace('_', '-')
        log.info(f'Processing {source_name}')

        # Specify to ignore errors in case one of the drop_cols is missing.
        df = df.drop(columns=_get_unnamed_cols(df), errors='ignore')

        # Rename to "ActivityProducedBy" or "ActivityConsumedBy":
        if _is_consumption(source_name, config):
            df = df.rename(columns={df.columns[0]: 'ActivityConsumedBy'})
            df['ActivityProducedBy'] = 'None'
        else:
            df = df.rename(columns={df.columns[0]: 'ActivityProducedBy'})
            df['ActivityConsumedBy'] = 'None'

        df['FlowType'] = 'ELEMENTARY_FLOW'
        df['Location'] = '00000'

        id_vars = [
            'SourceName',
            'ActivityConsumedBy',
            'ActivityProducedBy',
            'FlowType',
            'Location',
        ]

        df.set_index(id_vars)

        meta = get_table_meta(source_name, config)

        if table_name in ['3-14']:  # todo - check if necessary
            df = df.melt(
                id_vars=id_vars, var_name=meta.get('melt_var'), value_name='FlowAmount'
            )
            act_template = meta['activity']
            if isinstance(act_template, str):
                act_template = act_template.replace('__year__', year)
            name_unit = series_separate_name_and_units(
                df['FlowName'], act_template, meta['unit']
            )
            df['FlowName'] = name_unit['names']
            df['Unit'] = name_unit['units']
            df['Year'] = year

        elif table_name in ANNEX_ENERGY_TABLES:
            df = df.melt(id_vars=id_vars, var_name='FlowName', value_name='FlowAmount')
            df['Year'] = year
            for index, row in df.iterrows():
                col_name = row['FlowName']
                acb = row['ActivityConsumedBy'].strip()
                name_split = col_name.split(' (')
                source = name_split[1].split('- ')[1]
                # Append column name after dash to activity
                activity = f'{acb.strip()} {name_split[1].split("- ")[1]}'

                df.at[index, 'Description'] = meta['desc']  # type: ignore[index]
                if name_split[0] == 'Emissions':
                    df.at[index, 'FlowName'] = meta['emission']  # type: ignore[index]
                    df.at[index, 'Unit'] = meta['emission_unit']  # type: ignore[index]
                    df.at[index, 'Class'] = meta['emission_class']  # type: ignore[index]
                    df.at[index, 'Compartment'] = meta['emission_compartment']  # type: ignore[index]
                    df.at[index, 'ActivityProducedBy'] = activity  # type: ignore[index]
                    df.at[index, 'ActivityConsumedBy'] = 'None'  # type: ignore[index]
                else:  # "Consumption"
                    df.at[index, 'FlowName'] = acb  # type: ignore[index]
                    df.at[index, 'FlowType'] = 'TECHNOSPHERE_FLOW'  # type: ignore[index]
                    df.at[index, 'Unit'] = meta['unit']  # type: ignore[index]
                    df.at[index, 'Class'] = meta['class']  # type: ignore[index]
                    df.at[index, 'ActivityProducedBy'] = 'None'  # type: ignore[index]
                    df.at[index, 'ActivityConsumedBy'] = source  # type: ignore[index]

        else:
            # Standard years (one or more) as column headers
            df = df.melt(id_vars=id_vars, var_name='Year', value_name='FlowAmount')

        # set suppressed values to 0 but mark as suppressed
        # otherwise set non-numeric to nan
        try:
            flow_stripped = df['FlowAmount'].astype(str).str.strip()
            # mark '+' as suppressed, everything else NaN
            df['Suppressed'] = flow_stripped.where(flow_stripped == '+', np.nan)
            df['FlowAmount'] = (
                df['FlowAmount']
                .astype(str)
                .str.strip()
                .str.replace(',', '', regex=False)
                .infer_objects(copy=False)
            )
            df['FlowAmount'] = (
                df['FlowAmount']
                .replace('+', '0', regex=False)
                .infer_objects(copy=False)
            )
            df['FlowAmount'] = pd.to_numeric(df['FlowAmount'], errors='coerce')
            df = df.dropna(subset='FlowAmount')
        except AttributeError:
            # if no string in FlowAmount, then proceed
            df = df.dropna(subset='FlowAmount')

        if table_name not in ANNEX_ENERGY_TABLES:
            if 'Unit' not in df:
                df['Unit'] = meta.get('unit')
            if 'FlowName' not in df:
                df['FlowName'] = meta.get('flow')

            df['Class'] = meta.get('class')
            df['Description'] = meta.get('desc')
            df['Compartment'] = meta.get('compartment')

        if 'Year' not in df.columns:
            df['Year'] = year
        else:
            df = df[df['Year'].astype(str).isin([year])]

        # Add DQ scores
        df['DataReliability'] = meta.get('data_reliability', 5)
        df['DataCollection'] = 1
        # Fill in the rest of the Flow by fields so they show "None" instead of nan
        df['MeasureofSpread'] = 'None'
        df['DistributionType'] = 'None'
        df['LocationSystem'] = 'None'
        df = assign_fips_location_system(df, str(year))

        # Define special table lists from config
        multi_chem_names: list[str] = config.get('multi_chem_names') or []
        source_No_activity: list[str] = config.get('source_No_activity') or []
        source_activity_1: list[str] = config.get('source_activity_1') or []
        source_activity_1_fuel: list[str] = config.get('source_activity_1_fuel') or []
        source_activity_2: list[str] = config.get('source_activity_2') or []
        rows_as_flows: list[str] = config.get('rows_as_flows') or []

        if table_name in multi_chem_names:
            bool_apb = False
            bool_LULUCF = False
            apbe_value = ''
            flow_name_list = [
                'CO2',
                'CH4',
                'N2O',
                'NF3',
                'HFCs',
                'PFCs',
                'SF6',
                'NF3',
                'CH4 a',
                'N2O b',
                'CO',
                'NOx',
            ]
            for index, row in df.iterrows():
                apb_value = strip_char(row['ActivityProducedBy'])
                if 'CH4' in apb_value:
                    apb_value = 'CH4'
                elif 'N2O' in apb_value and apb_value != 'N2O from Product Uses':
                    apb_value = 'N2O'
                elif 'CO2' in apb_value:
                    apb_value = 'CO2'

                if apb_value in flow_name_list:
                    if bool_LULUCF:
                        df = df.drop(index)
                    else:
                        apbe_value = apb_value
                        df.loc[index, 'FlowName'] = apbe_value  # type: ignore[index]
                        df.loc[index, 'ActivityProducedBy'] = 'All activities'  # type: ignore[index]
                        bool_apb = True
                elif apb_value.startswith('LULUCF'):
                    df.loc[index, 'FlowName'] = 'CO2e'  # type: ignore[index]
                    df.loc[index, 'ActivityProducedBy'] = strip_char(apb_value)  # type: ignore[index]
                    bool_LULUCF = True
                elif apb_value.startswith(('Total', 'Net')):
                    df = df.drop(index)
                else:
                    apb_txt = cast(str, df.loc[index, 'ActivityProducedBy'])  # type: ignore[index]
                    apb_txt = strip_char(apb_txt)
                    df.loc[index, 'ActivityProducedBy'] = apb_txt  # type: ignore[index]
                    if bool_apb:
                        df.loc[index, 'FlowName'] = apbe_value  # type: ignore[index]

        elif table_name in source_No_activity:
            apbe_value = ''
            flow_name_list = ['Industry', 'Transportation', 'U.S. Territories']
            for index, row in df.iterrows():
                unit = row['Unit']
                if unit.strip() == 'MMT  CO2':
                    df.loc[index, 'Unit'] = 'MMT CO2e'  # type: ignore[index]
                if df.loc[index, 'Unit'] != 'MMT CO2e':  # type: ignore[index]
                    df = df.drop(index)
                else:
                    df.loc[index, 'FlowName'] = meta.get('flow')  # type: ignore[index]
                    # use .join and split to remove interior spaces
                    apb_value = ' '.join(row['ActivityProducedBy'].split())
                    apb_value = apb_value.replace('°', '')
                    if apb_value in flow_name_list:
                        # set header
                        apbe_value = apb_value
                        df.loc[index, 'ActivityProducedBy'] = (  # type: ignore[index]
                            f'{apbe_value} All activities'
                        )
                    else:
                        # apply header
                        apb_txt = strip_char(apb_value)
                        df.loc[index, 'ActivityProducedBy'] = f'{apbe_value} {apb_txt}'  # type: ignore[index]
                    if 'Total' == apb_value or 'Total ' == apb_value:
                        df = df.drop(index)

        elif table_name in (source_activity_1 + source_activity_1_fuel):
            apbe_value = ''
            activity_subtotal_sector = [
                'Electric Power',
                'Industrial',
                'Commercial',
                'Residential',
                'U.S. Territories',
                'Transportation',
                'Exploration',
                'Production (Total)',
                'Refining',
                'Crude Oil Transportation',
                'Cropland',
                'Grassland',
            ]
            activity_subtotal_fuel = [
                'Gasoline',  # in UMD (3-8)
                'Distillate Fuel Oil',
                'Diesel Fuel',  # new for UMD (3-8)
                'Jet Fuel',  # in UMD (3-8)
                'Aviation Gasoline',
                'Residual Fuel Oil',
                'Natural Gas',
                'LPG',
                'Electricity',
                'Fuel Type/Vehicle Type',
                'Diesel On-Road',  # UMD 3-9
                'Alternative Fuel On-Road',  # UMD 3-10
                'Non-Road',
                'Gasoline On-Road',  # UMD 3-9
                'Distillate Fuel Oil',
            ]
            if table_name in source_activity_1:
                activity_subtotal = activity_subtotal_sector
            else:
                activity_subtotal = activity_subtotal_fuel
            after_Total = False
            for index, row in df.iterrows():
                apb_value = strip_char(row['ActivityProducedBy'])
                if apb_value in activity_subtotal or after_Total:
                    # set the header
                    apbe_value = apb_value
                    df.loc[index, 'ActivityProducedBy'] = f'All activities {apbe_value}'  # type: ignore[index]
                else:
                    # apply the header
                    apb_txt = apb_value
                    if table_name == 'X-X':  # was EPA  3-10
                        # Separate Flows and activities for this table
                        df.loc[index, 'ActivityProducedBy'] = apbe_value  # type: ignore[index]
                        df.loc[index, 'FlowName'] = apb_txt  # type: ignore[index]
                    else:
                        df.loc[index, 'ActivityProducedBy'] = f'{apb_txt} {apbe_value}'  # type: ignore[index]
                if apb_value.startswith('Total'):
                    df = df.drop(index)
                    after_Total = True

        elif table_name in source_activity_2:
            bool_apb = False
            apbe_value = ''
            flow_name_list = [
                'Explorationb',
                'Production',  # UMD 3-25
                'Processing',
                'Transmission and Storage',
                'Transportation',  # New: UMD 3-25
                'Distribution',
                'Post-Meter',
                'Crude Oil Transportation',
                'Refining',
                'Refineries',  # New: UMD 3-25
                'Exploration',  # UMD 3-25
                'Mobile AC',
                'Refrigerated Transport',
                'Comfort Cooling for Trains and Buses',
            ]
            for index, row in df.iterrows():
                apb_value = row['ActivityProducedBy']
                start_activity = row['FlowName']
                if apb_value.strip() in flow_name_list:
                    apbe_value = apb_value
                    if apbe_value == 'Explorationb':
                        apbe_value = 'Exploration'
                    df.loc[index, 'FlowName'] = start_activity  # type: ignore[index]
                    df.loc[index, 'ActivityProducedBy'] = apbe_value  # type: ignore[index]
                    bool_apb = True
                else:
                    if bool_apb:
                        df.loc[index, 'FlowName'] = start_activity  # type: ignore[index]
                        apb_txt = cast(str, df.loc[index, 'ActivityProducedBy'])  # type: ignore[index]
                        apb_txt = strip_char(apb_txt)
                        if apb_txt == 'Gathering and Boostingc':
                            apb_txt = 'Gathering and Boosting'
                        df.loc[index, 'ActivityProducedBy'] = (  # type: ignore[index]
                            f'{apbe_value} - {apb_txt}'
                        )
                    else:
                        apb_txt = cast(str, df.loc[index, 'ActivityProducedBy'])  # type: ignore[index]
                        apb_txt = strip_char(apb_txt)
                        df.loc[index, 'ActivityProducedBy'] = f'{apb_txt} {apbe_value}'  # type: ignore[index]
                if 'Total' == apb_value or 'Total ' == apb_value:
                    df = df.drop(index)

        elif table_name == 'A-69':  # TODO: EPA table, update for umd
            fuel_name = ''
            A_79_unit_dict = {
                'Natural Gas': 'trillion cubic feet',
                'Electricity': 'million kilowatt-hours',
            }
            df.loc[:, 'FlowType'] = 'TECHNOSPHERE_FLOW'
            for index, row in df.iterrows():
                if row['ActivityConsumedBy'].startswith(' '):
                    # indicates subcategory
                    df.loc[index, 'ActivityConsumedBy'] = strip_char(  # type: ignore[index]
                        cast(str, df.loc[index, 'ActivityConsumedBy'])  # type: ignore[index]
                    )
                    df.loc[index, 'FlowName'] = fuel_name  # type: ignore[index]
                else:
                    # fuel header
                    fuel_name = cast(str, df.loc[index, 'ActivityConsumedBy'])  # type: ignore[index]
                    fuel_name = strip_char(fuel_name.split('(')[0])
                    df.loc[index, 'ActivityConsumedBy'] = 'All activities'  # type: ignore[index]
                    df.loc[index, 'FlowName'] = fuel_name  # type: ignore[index]
                if fuel_name in A_79_unit_dict.keys():
                    df.loc[index, 'Unit'] = A_79_unit_dict[fuel_name]  # type: ignore[index]

        else:
            if table_name in ['4-31']:  # TODO: EPA code, update for umd?
                # Assign activity as flow for technosphere flows (GHGI Table 4-55 → UMD 4-31).
                df.loc[:, 'FlowType'] = 'TECHNOSPHERE_FLOW'
                df.loc[:, 'FlowName'] = df.loc[:, 'ActivityProducedBy']

            elif table_name in ['4-57', '4-62']:
                df = df.iloc[::-1]  # reverse the order for assigning APB
                for index, row in df.iterrows():
                    apb_value = strip_char(row['ActivityProducedBy'])
                    if apb_value.startswith('Total'):
                        # set the header
                        apbe_value = apb_value.replace('Total ', '')
                        df = df.drop(index)
                    else:
                        if apbe_value == 'N2O':
                            match = re.findall(r'\(.*?\)', apb_value)[0][1:-1]
                            df.loc[index, 'ActivityProducedBy'] = match  # type: ignore[index]
                            df.loc[index, 'FlowName'] = 'N2O'  # type: ignore[index]
                        else:
                            df.loc[index, 'ActivityProducedBy'] = apbe_value  # type: ignore[index]
                            df.loc[index, 'FlowName'] = apb_value  # type: ignore[index]
                df = df.iloc[::-1]  # revert the order

            elif table_name in rows_as_flows:
                # Table with flow names as Rows
                df.loc[:, 'FlowName'] = df.loc[:, 'ActivityProducedBy'].apply(
                    lambda x: strip_char(x)
                )
                df = df[~df['FlowName'].str.contains('Total')]
                df.loc[:, 'ActivityProducedBy'] = meta.get('activity')

            elif table_name in ['4-16']:  # TODO: EPA code, update for umd?
                # TODO: 4-16 not in UMD_GHGIA.yaml; confirm drop or map if this branch runs.
                # Remove notes from activity names (GHGI 4-124 → UMD 4-60).
                for index, row in df.iterrows():
                    apb_value = strip_char(row['ActivityProducedBy'].split('(')[0])
                    df.loc[index, 'ActivityProducedBy'] = apb_value  # type: ignore[index]

        df['ActivityProducedBy'] = df['ActivityProducedBy'].str.strip()
        df['ActivityConsumedBy'] = df['ActivityConsumedBy'].str.strip()
        df['FlowName'] = df['FlowName'].str.strip()

        # Update location for terriory-based activities
        df.loc[
            (df['ActivityProducedBy'].str.contains('U.S. Territor'))
            | (df['ActivityConsumedBy'].str.contains('U.S. Territor')),
            'Location',
        ] = '99000'

        df.drop(df.loc[df['ActivityProducedBy'] == 'Total'].index, inplace=True)
        df.drop(df.loc[df['FlowName'] == 'Total'].index, inplace=True)

        df = df.loc[:, ~df.columns.duplicated()]
        # Remove commas from numbers again in case any were missed:
        df['FlowAmount'] = df['FlowAmount'].replace(',', '', regex=True)
        if len(df) == 0:
            log.warning(f'Error processing {table_name}')
        else:
            cleaned_list.append(df)

    return cleaned_list


def get_manufacturing_energy_ratios(parameter_dict: dict[str, Any]) -> dict[str, float]:
    """Calculate energy ratio by fuel between GHGI and EIA MECS."""
    # flow correspondence between GHGI and MECS
    flow_corr = {
        'Industrial Other Coal': 'Coal',
        'Natural Gas': 'Natural Gas',
        # 'Total Petroleum': (
        #     'Petroleum', ['Residual Fuel Oil',
        #                   'Distillate Fuel Oil',
        #                   'Hydrocarbon Gas Liquids, excluding natural gasoline',
        #                   ])
    }
    mecs_year = parameter_dict.get('year')

    # Filter MECS for total national energy consumption for manufacturing sectors
    mecs = load_fba_w_standardized_units(
        datasource=cast(str, parameter_dict.get('energy_fba')),
        year=cast(int, mecs_year),
        flowclass='Energy',
        download_FBA_if_missing=True,
    )
    mecs = mecs.loc[
        (mecs['ActivityConsumedBy'] == '31-33')
        & (mecs['Location'] == '00000')
        & (mecs['Description'].isin(['Table 3.2', 'Table 2.2']))
        & (mecs['Unit'] == 'MJ')
    ].reset_index(drop=True)

    # Load energy consumption data by fuel from GHGI
    ghgi = load_fba_w_standardized_units(
        datasource=cast(str, parameter_dict.get('ghg_fba')),
        year=cast(int, parameter_dict.get('ghgi_year', mecs_year)),
        flowclass='Energy',
        download_FBA_if_missing=True,
    )
    ghgi = ghgi[ghgi['ActivityConsumedBy'] == 'Industrial'].reset_index(drop=True)

    pct_dict = {}
    for ghgi_flow, v in flow_corr.items():
        if type(v) is tuple:
            label = v[0]
            mecs_flows = v[1]
        else:
            label = v
            mecs_flows = [v]
        # Calculate percent energy contribution from MECS based on v
        mecs_energy = sum(
            mecs.loc[mecs['FlowName'].isin(mecs_flows), 'FlowAmount'].values
        )
        ghgi_energy = ghgi.loc[ghgi['FlowName'] == ghgi_flow, 'FlowAmount'].values[0]
        pct = np.minimum(mecs_energy / ghgi_energy, 1)
        pct_dict[label] = pct

    # based on 2018 datasets
    # {'Coal': np.float64(0.7599),
    #  'Natural Gas': np.float64(0.6822)}

    # based on 2018 MECS / 2023 GHGI
    # {'Coal': np.float64(1.0),
    #  'Natural Gas': np.float64(0.6540)}
    return pct_dict


def allocate_industrial_combustion(
    fba: FlowByActivity, **_kwargs: Any
) -> FlowByActivity:
    """
    Split industrial combustion emissions into two buckets to be further allocated.

    clean_fba_before_activity_sets. Calculate the percentage of fuel consumption captured in
    EIA MECS relative to GHGI/UMD GHGIA. Create new activities to distinguish those
    which use EIA MECS as allocation source and those that use alternate source.
    """
    clean_parameter = fba.config.get('clean_parameter')
    if clean_parameter is None:
        raise ValueError('clean_parameter is required in config')
    pct_dict = get_manufacturing_energy_ratios(clean_parameter)

    # TODO: stationary CH4/N2O in UMD come from 3-11/3-12 (GHGI 3-8/3-9 analogues); industrial split
    # still keys off Annex A-14-style consumption where available (UMD partial: 3-4 per GHGI A-5 mapping).
    # activities reflect flows in A_14 and UMD 3-11 / 3-12
    activities_to_split = {
        'Industrial Other Coal Industrial': 'Coal',
        'Natural Gas Industrial': 'Natural Gas',
        'Coal Industrial': 'Coal',
        # 'Total Petroleum Industrial': 'Petroleum',
        # 'Fuel Oil Industrial': 'Petroleum',
    }

    for activity, fuel in activities_to_split.items():
        df_subset = fba.loc[fba['ActivityProducedBy'] == activity].reset_index(
            drop=True
        )
        if len(df_subset) == 0:
            continue
        df_subset['FlowAmount'] = df_subset['FlowAmount'] * pct_dict[fuel]
        df_subset['ActivityProducedBy'] = f'{activity} - Manufacturing'
        fba.loc[fba['ActivityProducedBy'] == activity, 'FlowAmount'] = fba[
            'FlowAmount'
        ] * (1 - pct_dict[fuel])
        fba = FlowByActivity(pd.concat([fba, df_subset], ignore_index=True))

    return fba


def split_HFCs_by_type(fba: FlowByActivity, **_kwargs: Any) -> FlowByActivity:
    """Speciates HFCs and PFCs using shares from ODS substitute table (GHGI 4-122 → UMD `UMD_GHGIA_T_4_59`).

    `clean_parameter['flow_fba']` should name that speciation FBA (not GHGI Table 4-125).
    clean_fba_before_mapping_df_fxn
    """

    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }
    original_sum = fba.FlowAmount.sum()
    clean_parameter = fba.config.get('clean_parameter')
    if clean_parameter is None:
        raise ValueError('clean_parameter is required in config')
    tbl = clean_parameter['flow_fba']  # e.g. UMD_GHGIA_T_4_59
    splits = load_fba_w_standardized_units(
        datasource=tbl, year=fba['Year'][0], download_FBA_if_missing=True
    )
    splits['pct'] = splits['FlowAmount'] / splits['FlowAmount'].sum()
    splits = splits[['FlowName', 'pct']]

    speciated_df = fba.apply(
        lambda x: [p * x['FlowAmount'] for p in splits['pct']],
        axis=1,
        result_type='expand',
    )
    speciated_df.columns = splits['FlowName']
    combined_df = pd.concat([fba, speciated_df], axis=1)
    melted_df = (
        combined_df.melt(
            id_vars=[c for c in flow_by_activity_fields.keys() if c in combined_df],
            var_name='Flow',
        )
        .drop(columns=['FlowName', 'FlowAmount'])
        .rename(columns={'Flow': 'FlowName', 'value': 'FlowAmount'})
    )
    new_sum = melted_df.FlowAmount.sum()
    if round(new_sum, 6) != round(original_sum, 6):
        log.warning('Error: totals do not match when splitting HFCs')
    new_fba = FlowByActivity(melted_df)
    for attr in attributes_to_save:
        setattr(new_fba, attr, attributes_to_save[attr])

    return new_fba


def clean_UMD_GHGIA_T_4_60(fba: FlowByActivity, **_kwargs: Any) -> FlowByActivity:
    """Subtract out refrigeration transport emissions from the
    Refrigeration/Air Conditioning activity (GHGI 4-124 → UMD 4-60)."""

    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }

    # TODO: GHGI Table A-90 has no UMD equivalent—verify subtraction (EPA FBA, derived values, or omit).
    tbl = load_fba_w_standardized_units(
        datasource='EPA_GHGI_T_A_90', year=fba['Year'][0], download_FBA_if_missing=True
    )

    activities = [
        "Comfort Cooling for Trains and Buses",
        "Mobile AC",
        "Refrigerated Transport",
    ]

    total = tbl.loc[tbl["ActivityProducedBy"].isin(activities), "FlowAmount"].sum()

    fba.loc[
        fba["ActivityProducedBy"] == "Refrigeration/Air Conditioning", "FlowAmount"
    ] -= total

    for attr in attributes_to_save:
        setattr(fba, attr, attributes_to_save[attr])

    fba2 = split_HFCs_by_type(fba)

    return fba2
