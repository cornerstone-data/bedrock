# EPA_GHGI.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Inventory of US EPA GHG
https://www.epa.gov/ghgemissions/inventory-us-greenhouse-gas-emissions-and-sinks
"""

import re
from typing import Any, List, cast

import numpy as np
import pandas as pd

from bedrock.extract.allocation.epa_constants import TBL_NUMBERS
from bedrock.extract.flowbyactivity import FlowByActivity, getFlowByActivity
from bedrock.extract.generateflowbyactivity import generateFlowByActivity
from bedrock.transform.flowbyfunctions import (
    assign_fips_location_system,
    load_fba_w_standardized_units,
)
from bedrock.utils.config.schema import flow_by_activity_fields
from bedrock.utils.config.settings import externaldatapath
from bedrock.utils.logging.flowsa_log import log

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


def ghg_load_gcs(**kwargs: dict[str, Any]) -> List[pd.DataFrame]:
    """For each url the file gets download and stored locally from gcs"""
    df_list = []
    table_dict = kwargs['config']['Tables'] | kwargs['config']['Annex']
    year = str(kwargs['year'])
    for chapter, tables in table_dict.items():
        for table, data in tables.items():
            if data.get('year') not in (None, year):
                # Skip tables when the year does not align with target year
                continue
            if year == '2023' and table == '3-25b':
                # Skip 3-25b for current year (use 3-25 instead)
                continue
            df = _load_ghg_table(table)
            if df is not None and len(df.columns) > 1:
                years = YEARS.copy()
                years.remove(year)
                df = df.drop(columns=(DROP_COLS + years), errors='ignore')
                df['SourceName'] = f'EPA_GHGI_T_{table.replace("-", "_")}'
                df_list.append(df)

    return df_list


def _load_ghg_table(table: str) -> pd.DataFrame:
    """Applies branching logic to load the table correctly and returns a dataframe"""
    from bedrock.extract.allocation.epa import _load_epa_tbl_from_gcs  # noqa:PLC0415

    if table == '3-25b':
        return pd.read_csv(
            externaldatapath / f'GHGI_Table_{table}.csv',
            skiprows=2,
            encoding='ISO-8859-1',
            thousands=',',
        )

    df = _load_epa_tbl_from_gcs(
        cast(TBL_NUMBERS, table),
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=2 if table == '4-118' else 1,
            encoding='ISO-8859-1',
            thousands=',',
            header=[0, 1] if table in (ANNEX_ENERGY_TABLES + ['3-25']) else 0,
        ),
    )
    if table in ANNEX_ENERGY_TABLES:
        return _read_yearly_annex_tables(df, table)
    elif table == '3-13':
        # remove notes from column headers in some years
        cols = [c[:4] for c in list(df.columns[1:])]
        return df.rename(columns=dict(zip(df.columns[1:], cols)))
    elif table == '3-25':
        # Row 0 is header, row 1 is unit
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
    td = config['Annex'] if '_A_' in source_name else config['Tables']
    for chapter in td.keys():
        for k, v in td[chapter].items():
            if source_name.endswith(k.replace('-', '_')):
                return v
    else:
        raise KeyError(f'Table meta nto found for {source_name}')


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
    ]
    for i in notes:
        if i in text:
            text_split = text.split(i)
            text = text_split[0]

    footnotes = {
        'Gasolineb': 'Gasoline',
        'Trucksc': 'Trucks',
        'Boatsd': 'Boats',
        'Boatse': 'Boats',
        'Fuelsb': 'Fuels',
        'Fuelsf': 'Fuels',
        'Consumptiona': 'Consumption',
        'Aircraftg': 'Aircraft',
        'Pipelineh': 'Pipeline',
        'Electricityh': 'Electricity',
        'Electricityl': 'Electricity',
        'Ethanoli': 'Ethanol',
        'Biodieseli': 'Biodiesel',
        'Changee': 'Change',
        'Emissionsc': 'Emissions',
        'Equipmentd': 'Equipment',
        'Equipmente': 'Equipment',
        'Totalf': 'Total',
        'Roadg': 'Road',
        'Otherf': 'Other',
        'Railc': 'Rail',
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
        'Distillate Fuel Oil (Diesel)': 'Distillate Fuel Oil',
        'Distillate Fuel Oil (Diesel': 'Distillate Fuel Oil',
        'Natural gas': 'Natural Gas',  # Fix capitalization inconsistency
        'HGLb': 'HGL',
        'Biofuels-Biodieselh': 'Biofuels-Biodiesel',
        'Biofuels-Ethanolh': 'Biofuels-Ethanol',
        'Commercial Aircraftf': 'Commercial Aircraft',
        'Electricityk': 'Electricity',
        'Gasolinea': 'Gasoline',
        'International Bunker Fuelse': 'International Bunker Fuel',
        'Medium- and Heavy-Duty Trucksb': 'Medium- and Heavy-Duty Trucks',
        'Pipelineg': 'Pipeline',
        'Recreational Boatsc': 'Recreational Boats',
        'Construction/Mining Equipmentf': 'Construction/Mining Equipment',
        'Non-Roadc': 'Non-Road',
        'HFCsa': 'HFCs',
        'HFOsb': 'HFOs',
        'CO_{2}': 'CO2',
        'CH?^{c}': 'CH4',
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


def ghg_parse(
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
        table_name = source_name[11:].replace('_', '-')
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

        if table_name in ['3-25']:
            df = df.melt(
                id_vars=id_vars, var_name=meta.get('melt_var'), value_name='FlowAmount'
            )
            name_unit = series_separate_name_and_units(
                df['FlowName'], meta['activity'], meta['unit']
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
            df['Suppressed'] = (
                df['FlowAmount']
                .astype(str)
                .str.strip()
                .eq('+')
                .replace({True: '+', False: np.nan})
                .infer_objects(copy=False)
            )
            df['FlowAmount'] = (
                df['FlowAmount']
                .astype(str)
                .str.replace(',', '')
                .infer_objects(copy=False)
            )
            df['FlowAmount'] = (
                df['FlowAmount'].replace('+', '0').infer_objects(copy=False)
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
                'Gasoline',
                'Distillate Fuel Oil',
                'Jet Fuel',
                'Aviation Gasoline',
                'Residual Fuel Oil',
                'Natural Gas',
                'LPG',
                'Electricity',
                'Fuel Type/Vehicle Type',
                'Diesel On-Road',
                'Alternative Fuel On-Road',
                'Non-Road',
                'Gasoline On-Road',
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
                    if table_name == '3-10':
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
                'Production',
                'Processing',
                'Transmission and Storage',
                'Distribution',
                'Post-Meter',
                'Crude Oil Transportation',
                'Refining',
                'Exploration',
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

        elif table_name == 'A-69':
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
            if table_name in ['4-55']:
                # Assign activity as flow for technosphere flows
                df.loc[:, 'FlowType'] = 'TECHNOSPHERE_FLOW'
                df.loc[:, 'FlowName'] = df.loc[:, 'ActivityProducedBy']

            elif table_name in ['4-118', '4-132']:
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

            elif table_name in ['4-16', '4-124']:
                # Remove notes from activity names
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
        year=cast(int, mecs_year),
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

    return pct_dict


def allocate_industrial_combustion(
    fba: FlowByActivity, **_kwargs: Any
) -> FlowByActivity:
    """
    Split industrial combustion emissions into two buckets to be further allocated.

    clean_fba_before_activity_sets. Calculate the percentage of fuel consumption captured in
    EIA MECS relative to EPA GHGI. Create new activities to distinguish those
    which use EIA MECS as allocation source and those that use alternate source.
    """
    clean_parameter = fba.config.get('clean_parameter')
    if clean_parameter is None:
        raise ValueError('clean_parameter is required in config')
    pct_dict = get_manufacturing_energy_ratios(clean_parameter)

    # activities reflect flows in A_14 and 3_8 and 3_9
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
    """Speciates HFCs and PFCs for all activities based on T_4_125.
    clean_fba_before_mapping_df_fxn"""

    attributes_to_save = {
        attr: getattr(fba, attr) for attr in fba._metadata + ['_metadata']
    }
    original_sum = fba.FlowAmount.sum()
    clean_parameter = fba.config.get('clean_parameter')
    if clean_parameter is None:
        raise ValueError('clean_parameter is required in config')
    tbl = clean_parameter['flow_fba']  # 4-125
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


if __name__ == '__main__':
    # fba = bedrock.return_FBA('EPA_GHGI_T_4_101', 2016)
    # df = clean_HFC_fba(fba)
    tbl_list = [
        '2-1',
        '3-7',
        '3-8',
        '3-9',
        '3-13',
        '3-14',
        '3-15',  # "3-25","3-25b",
        '3-106',
        '3-45',
        '3-47',
        '3-49',
        '3-64',
        '3-66',
        '3-68',
        '3-102',
        '4-16',
        '4-39',
        '4-59',
        '4-100',
        '4-55',
        '4-57',
        '4-63',
        '4-64',
        '4-106',
        '4-118',
        '4-122',
        '4-124',
        '4-132',
        '5-3',
        '5-7',
        '5-18',
        '5-19',
        '5-29',
        # "A-5"
    ]
    fba_list = []
    for y in range(2020, 2024):
        generateFlowByActivity(year=y, source='EPA_GHGI')
        if y == 2023:
            ls = tbl_list + ['3-25', 'A-5']
        else:
            ls = tbl_list + ['3-25b'] + [f'A-{2028 - y}']
        fba = pd.concat(
            [getFlowByActivity(f'EPA_GHGI_T_{str(t).replace("-", "_")}', y) for t in ls]
        )
        fba_list.append(fba)
    fba_all = pd.concat(fba_list, ignore_index=True)
