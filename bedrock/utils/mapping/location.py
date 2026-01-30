# location.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
Functions related to accessing and modifying location codes
"""

import io
import urllib.error

import numpy as np
import pandas as pd
import pycountry
from esupy.remote import make_url_request

from bedrock.utils.config.common import clean_str_and_capitalize
from bedrock.utils.config.settings import mappingpath
from bedrock.utils.logging.flowsa_log import log
from bedrock.utils.mapping.geo import get_all_fips

# =============================================================================
# Constants
# =============================================================================

US_FIPS = "00000"

# see geo.py for assignments
fips_number_key: dict[str, int] = {"national": 5, "state": 2, "county": 1}

# From https://gist.github.com/rogerallen/1583593
# removed non US states, PR, MP, VI
us_state_abbrev: dict[str, str] = {
    'Alabama': 'AL',
    'Alaska': 'AK',
    'Arizona': 'AZ',
    'Arkansas': 'AR',
    'California': 'CA',
    'Colorado': 'CO',
    'Connecticut': 'CT',
    'Delaware': 'DE',
    'District of Columbia': 'DC',
    'Florida': 'FL',
    'Georgia': 'GA',
    'Hawaii': 'HI',
    'Idaho': 'ID',
    'Illinois': 'IL',
    'Indiana': 'IN',
    'Iowa': 'IA',
    'Kansas': 'KS',
    'Kentucky': 'KY',
    'Louisiana': 'LA',
    'Maine': 'ME',
    'Maryland': 'MD',
    'Massachusetts': 'MA',
    'Michigan': 'MI',
    'Minnesota': 'MN',
    'Mississippi': 'MS',
    'Missouri': 'MO',
    'Montana': 'MT',
    'Nebraska': 'NE',
    'Nevada': 'NV',
    'New Hampshire': 'NH',
    'New Jersey': 'NJ',
    'New Mexico': 'NM',
    'New York': 'NY',
    'North Carolina': 'NC',
    'North Dakota': 'ND',
    'Ohio': 'OH',
    'Oklahoma': 'OK',
    'Oregon': 'OR',
    'Pennsylvania': 'PA',
    'Rhode Island': 'RI',
    'South Carolina': 'SC',
    'South Dakota': 'SD',
    'Tennessee': 'TN',
    'Texas': 'TX',
    'Utah': 'UT',
    'Vermont': 'VT',
    'Virginia': 'VA',
    'Washington': 'WA',
    'West Virginia': 'WV',
    'Wisconsin': 'WI',
    'Wyoming': 'WY',
}

# Reverse mapping: abbreviation -> state name
# thank you to @kinghelix and @trevormarburger for this idea
abbrev_us_state: dict[str, str] = {
    abbr: state for state, abbr in us_state_abbrev.items()
}


# =============================================================================
# FIPS Code Functions
# =============================================================================


def get_state_FIPS(year: str = '2015', abbrev: bool = False) -> pd.DataFrame:
    """
    Filters FIPS df for state codes only
    :param year: str, year of FIPS, defaults to 2015
    :param abbrev: bool, if True return state abbreviations instead of names
    :return: FIPS df with only state level records
    """
    fips = get_all_fips(int(year))  # type: ignore[arg-type]
    fips = fips.drop_duplicates(subset='State')
    fips = fips[fips['State'].notnull()]
    if abbrev:
        fips['State'] = (
            fips['State']
            .str.title()
            .replace(us_state_abbrev)
            .replace({'District Of Columbia': 'DC'})
        )
    return fips.drop(columns='FIPS_Scale')


def get_county_FIPS(year: str = '2015') -> pd.DataFrame:
    """
    Filters FIPS df for county codes only
    :param year: str, year of FIPS, defaults to 2015
    :return: FIPS df with only county level records
    """
    fips = get_all_fips(int(year))  # type: ignore[arg-type]
    fips = fips.drop_duplicates(subset='FIPS')
    fips = fips[fips['County'].notnull()]
    return fips.drop(columns='FIPS_Scale')


def get_all_state_FIPS_2(year: str = '2015') -> pd.DataFrame:
    """
    Gets a subset of all FIPS 2 digit codes for states
    :param year: str, year of FIPS, defaults to 2015
    :return: df with 'State' and 'FIPS_2' cols
    """
    state_fips = get_state_FIPS(year)
    state_fips.loc[:, 'FIPS_2'] = state_fips['FIPS'].apply(lambda x: str(x)[0:2])
    state_fips = state_fips[['State', 'FIPS_2']]
    return state_fips


def apply_county_FIPS(
    df: pd.DataFrame, year: str = '2015', source_state_abbrev: bool = True
) -> pd.DataFrame:
    """
    Applies FIPS codes by county to dataframe containing columns with State
    and County
    :param df: dataframe must contain columns with 'State' and 'County', but
        not 'Location'
    :param year: str, FIPS year, defaults to 2015
    :param source_state_abbrev: True or False, the state column uses
        abbreviations
    :return dataframe with new column 'FIPS', blanks not removed
    """
    # If using 2 letter abbrevations, map to state names
    if source_state_abbrev:
        df['State'] = df['State'].map(abbrev_us_state).fillna(df['State'])
    df['State'] = df['State'].apply(lambda x: clean_str_and_capitalize(str(x)))
    if 'County' not in df:
        df['County'] = ''
    df['County'] = df['County'].apply(lambda x: clean_str_and_capitalize(str(x)))

    # Pull and merge FIPS on state and county
    mapping_FIPS = get_county_FIPS(year)
    df = df.merge(mapping_FIPS, how='left')

    # Where no county match occurs, assign state FIPS instead
    mapping_FIPS = get_state_FIPS()
    mapping_FIPS = mapping_FIPS.drop(columns=['County'])
    df = df.merge(mapping_FIPS, left_on='State', right_on='State', how='left')
    df['Location'] = df['FIPS_x'].where(df['FIPS_x'].notnull(), df['FIPS_y'])
    df = df.drop(columns=['FIPS_x', 'FIPS_y'])

    return df


def update_geoscale(df: pd.DataFrame, to_scale: str) -> pd.DataFrame:
    """
    Updates df['Location'] based on specified to_scale
    :param df: df, requires Location column
    :param to_scale: str, target geoscale
    :return: df, with 5 digit geo
    """
    # code for when the "Location" is a FIPS based system
    if to_scale == 'state':
        df = df.assign(
            Location=(
                df['Location']
                .apply(lambda x: str(x)[0:2])
                .apply(lambda x: x.ljust(3 + len(x), '0') if len(x) < 5 else x)
            )
        )
    elif to_scale == 'national':
        df = df.assign(Location=US_FIPS)
    return df


def extract_fips_years(pd_series: pd.Series | pd.Index) -> np.ndarray:
    """
    Extract np.array of unique year ints from pd.series of 'FIPS_yyyy' strings
    :param pd_series: pandas series or index
    :return: numpy array of year integers
    """
    if isinstance(pd_series, pd.Index):
        pd_series = pd.Series(pd_series)
    extracted = pd_series.str.extract(r'FIPS_(\d{4})').dropna()
    squeezed = extracted.squeeze()
    if isinstance(squeezed, pd.Series):
        years = squeezed.unique().astype(int)
    else:
        years = np.array([squeezed]).astype(int)
    return years


# =============================================================================
# Census Region Functions
# =============================================================================


def get_region_and_division_codes() -> pd.DataFrame:
    """
    Load the Census Regions csv
    :return: pandas df of census regions
    """
    df = pd.read_csv(
        mappingpath / 'geo' / "Census_Regions_and_Divisions.csv", dtype="str"
    )
    return df


def assign_census_regions(df_load: pd.DataFrame) -> pd.DataFrame:
    """
    Assign census regions as a LocationSystem
    :param df_load: fba or fbs
    :return: df with census regions as LocationSystem
    """
    # load census codes
    census_codes_load = get_region_and_division_codes()
    census_codes = census_codes_load[
        census_codes_load['LocationSystem'] == 'Census_Region'
    ]

    # merge df with census codes
    df = df_load.merge(
        census_codes[['Name', 'Region']],
        left_on=['Location'],
        right_on=['Name'],
        how='left',
    )
    # replace Location value
    df['Location'] = np.where(~df['Region'].isnull(), df['Region'], df['Location'])

    # modify LocationSystem
    # merge df with census codes
    df = df.merge(
        census_codes[['Region', 'LocationSystem']],
        left_on=['Region'],
        right_on=['Region'],
        how='left',
    )
    # replace Location value
    df['LocationSystem_x'] = np.where(
        ~df['LocationSystem_y'].isnull(), df['LocationSystem_y'], df['LocationSystem_x']
    )

    # drop census columns
    df = df.drop(columns=['Name', 'Region', 'LocationSystem_y'])
    df = df.rename(columns={'LocationSystem_x': 'LocationSystem'})

    return df


# =============================================================================
# Country Code Functions
# =============================================================================


def call_country_code(country: str) -> str:
    """
    use pycountry to call on 3 digit iso country code
    :param country: str, name of country
    :return: str, ISO code
    """
    result = pycountry.countries.get(name=country)
    if result is None:
        raise ValueError(f"Country not found: {country}")
    return result.numeric


# =============================================================================
# Urban/Rural Population Functions
# =============================================================================


def merge_urb_cnty_pct(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Merge a %-urban-population column onto a df with existing LocationSystem
    and Location columns, where the latter contains FIPS county codes.
    Note: post-merge 0% values are valid and not equal to nan's.
    :param df: pandas dataframe
    :return: pandas dataframe with new column of urban population percentages
    """
    if any(i is None for i in df['LocationSystem'].tolist()):
        log.error('LocationSystem column contains one or more None values')
        return None
    elif any('FIPS' not in str(i) for i in set(df['LocationSystem'].tolist())):
        log.error('LocationSystem column contains non-FIPS labels')
        return None  # check derived from bedrock.flowsa FBA format specs
    elif any(df['Location'].str.len() != 5):
        log.error('One or more FIPS codes are not expressed as 5-digits;' 'review data')
        return None

    # expects uniform LocationSystem values (i.e., single FIPS_yyyy year code)
    years = extract_fips_years(df['LocationSystem'])
    try:
        year = years.item()  # extract year element from length-1 array
    except ValueError:
        log.error('LocationSystem contains >1 "FIPS_yyyy" code')
        return None

    years_xwalk = extract_fips_years(  # from xwalk headers
        pd.read_csv(mappingpath / 'geo' / 'FIPS_Crosswalk.csv', nrows=0).columns
    )

    if not {year} <= set(years_xwalk):  # compare data years as sets
        log.error('LocationSystem incompatible with FIPS_Crosswalk.csv')
        return None

    pct_urb = get_census_cnty_tbl(year)
    if pct_urb is None:
        return None
    df = pd.merge(df, pct_urb, how='left', on='Location')

    # find unmerged nan pct_pop_urb values
    pct_na = sum(df['pct_pop_urb'].isna())
    if pct_na != 0:
        log.error(
            f'WARNING {pct_na} records did not merge successfully.\n'
            'In pct_pop_urb, "nan" values are not equal to 0%.'
        )

    return reshape_urb_rur_df(df)


def get_census_cnty_tbl(year: int) -> pd.DataFrame | None:
    """
    Read table of Census county-equivalent-level (FIPS) urban and rural
    population counts (detail in esupy/data_census/README.md), and
    calculate each area's urban population percentage.
    :param year: integer data year from a LocationSystem column (FIPS_yyyy)
    """
    cnty_url = {
        2010: (
            'https://www2.census.gov/geo/docs/' 'reference/ua/PctUrbanRural_County.txt'
        ),
        2020: ('https://www2.census.gov/geo/docs/reference/' 'ua/2020_UA_COUNTY.xlsx'),
    }

    decade = year - (year % 10)
    # screen for data availability; limited to 2010-2029 for now
    if decade not in (2010, 2020):
        log.error('County-level data year not yet available')
        return None

    try:
        if decade == 2010:
            resp = make_url_request(cnty_url[2010])
            df = pd.read_csv(
                io.StringIO(resp.content.decode('iso-8859-1')),
                usecols=['STATE', 'COUNTY', 'POP_COU', 'POP_URBAN'],
            )
        elif decade == 2020:
            resp = make_url_request(cnty_url[2020])
            df = pd.read_excel(
                resp.content,
                sheet_name='2020_UA_COUNTY',
                usecols=['STATE', 'COUNTY', 'POP_COU', 'POP_URB'],
            ).rename(columns={'POP_URB': 'POP_URBAN'})
    except urllib.error.HTTPError:
        log.error(f'File unavailable, check Census domain status: ' f'\n{cnty_url}')
        return None

    df['STATE'] = df['STATE'].apply(lambda x: f'{x:0>2}')
    df['COUNTY'] = df['COUNTY'].apply(lambda x: f'{x:0>3}')
    df[f'FIPS_{decade}'] = df['STATE'] + df['COUNTY']  # 5-digit county codes
    # Note: {total = urban + rural} population, for all FIPS areas
    df['pct_pop_urb'] = df['POP_URBAN'] / df['POP_COU']

    df = shift_census_cnty_tbl(df, year)  # adjust to match data year
    return df


def shift_census_cnty_tbl(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Transform a table of Census pct_pop_urb values (by FIPS area code)
    to a specified data year via FIPS_Crosswalk.csv.
    FIPS area splits are assumed to inherit their parent's pct_pop_urb value.
    FIPS area merges assume the child inherits the sum of its parents'
    total and urban population values (both assumed constant over time),
    requiring a recalculation of pct_pop_urb.
    :param df: pandas df from get_census_cnty_tbl
    :param year: integer data year from a LocationSystem column (FIPS_yyyy)
    """
    decade = year - year % 10  # previous decennial census year
    if year == decade:  # if already a decennial census year
        df['Location'] = df[f'FIPS_{decade}']
        df = df[['Location', 'pct_pop_urb']]  # keep only necessary cols
        return df
    # splits identified by duplicated 'FIPS_{decade}' codes (A-->B, A-->C)
    # i.e., join by ['FIPS_{decade}'] field ensures pct_pop_urb inheritance
    # merges identified by duplicated 'FIPS_{year}' codes (A-->C, B-->C)
    # e.g., 51019 & 51515 --> 51019
    fips_xwalk = pd.read_csv(
        mappingpath / 'geo' / 'FIPS_Crosswalk.csv',
        dtype=str,
        usecols=[f'FIPS_{decade}', f'FIPS_{year}'],
    )
    df = pd.merge(df, fips_xwalk, how='left', on=f'FIPS_{decade}')

    # sums population counts where code-year values are duplicated
    df[f'POP_COU_{year}'] = df.groupby(f'FIPS_{year}')['POP_COU'].transform('sum')
    df[f'POP_URBAN_{year}'] = df.groupby(f'FIPS_{year}')['POP_URBAN'].transform('sum')
    df['pct_pop_urb'] = df[f'POP_URBAN_{year}'] / df[f'POP_COU_{year}']
    df['Location'] = df[f'FIPS_{year}']
    df = df[['Location', 'pct_pop_urb']].drop_duplicates()
    return df


def reshape_urb_rur_df(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Pivot a df with urban (and thereby rural) population percentage values
    to long format, such that each row is purely urban or rural. Then scale
    FlowAmount quantities by these percentages and append '/urban' and '/rural'
    to Compartment labels.
    :param df: pandas dataframe
    :return: pandas dataframe with rows disaggregated by urban/rural population
    """
    try:
        df['pct_pop_rur'] = 1 - df['pct_pop_urb']
    except KeyError:
        log.error('Must pass a df with pct_pop_urb values')
        return None
    # grab list of "id_vars" we want to preserve
    df_id_vars = df.columns[~df.columns.str.match(r'pct_pop_\w{3}')].tolist()
    # rename pct_pop_X columns to pre-emptively label SubCompartment
    df = df.rename(columns={'pct_pop_urb': 'urban', 'pct_pop_rur': 'rural'})
    df = pd.melt(
        df,
        id_vars=df_id_vars,
        value_vars=['urban', 'rural'],
        var_name='SubCompartment',
        value_name='pct_pop',
    )
    # drop 0-value rows + duplicates (i.e., nan vals passed in pct_pop_urb col)
    df = df[df['pct_pop'] != 0].drop_duplicates()
    df['FlowAmount'] = df['FlowAmount'] * df['pct_pop']
    # append
    df['Compartment'] = df['Compartment'] + '/' + df['SubCompartment']
    df = df.drop(columns=['SubCompartment', 'pct_pop'])
    return df


def to_ndigit_str(
    s: pd.Series,
    digits: int,
    fill_value: int | float | str = 0,
    strict: bool = False,
) -> pd.Series:
    """
    Convert a Series to zero-padded, fixed-width digit strings with no decimals and no NaNs.
    Decimals are truncated toward zero.

    Parameters
    ----------
    s : pd.Series
        Input series containing numbers or strings (possibly with decimals or NaNs).
    digits : int
        Target string width; values are zero-padded to this length.
    fill_value : int | float | str, default 0
        Replacement for values that cannot be parsed to numeric (NaNs after coercion).
        If str, it will be parsed numerically; non-parsable falls back to 0.
    strict : bool, default False
        If True, raise ValueError when any value falls outside [0, 10**digits - 1].
        If False, values are clamped to that range.

    Returns
    -------
    pd.Series
        String series where each value is a zero-padded, `digits`-wide non-negative integer.

    Examples
    --------
    >>> s = pd.Series(['1', 23, 7.9, None, 'abc'])
    >>> to_ndigit_str(s, digits=3)
    0    001
    1    023
    2    007
    3    000
    4    000
    dtype: object
    """
    # Coerce to numeric; non-parsable -> NaN
    s_num = pd.to_numeric(s, errors="coerce")

    # If fill_value is a string, try to parse; fallback to 0 if not parsable
    if isinstance(fill_value, str):
        fv = pd.to_numeric(pd.Series([fill_value]), errors="coerce").iloc[0]
        fill_value = 0 if pd.isna(fv) else fv

    # Replace NaNs from coercion
    s_num = s_num.fillna(fill_value)

    # Truncate toward zero and cast to integer
    s_int = s_num.astype(int)

    # Range handling
    lo, hi = 0, 10**digits - 1
    if strict:
        bad = s_int[(s_int < lo) | (s_int > hi)]
        if not bad.empty:
            raise ValueError(
                f"Values out of range [{lo}, {hi}]: {bad.unique()[:10].tolist()}"
            )
    else:
        s_int = s_int.clip(lo, hi)

    # Zero-pad to fixed width
    return s_int.astype(str).str.zfill(digits)


if __name__ == "__main__":
    df = pd.DataFrame()
    for year in [2010, 2020]:
        pct_urb_result = get_census_cnty_tbl(year)
        if pct_urb_result is None:
            continue
        pct_urb = pct_urb_result.rename(columns={'pct_pop_urb': str(year)})
        if len(df) == 0:
            df = pct_urb.copy()
        else:
            df = df.merge(pct_urb, how='outer', on='Location')
    df.sort_values(by='Location').to_csv('percent_urban.csv', index=False)
