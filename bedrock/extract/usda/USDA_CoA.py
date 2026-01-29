# USDA_CoA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Functions used to import and parse USDA Census of Ag Cropland data,
Livestock data, and Cropland data in NAICS format
"""

import json
import os
import posixpath
from typing import Any, List
from urllib.parse import parse_qs, urlparse

import numpy as np
import pandas as pd
from requests import Response

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.config.common import WITHDRAWN_KEYWORD
from bedrock.utils.config.schema import flow_by_activity_fields
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import GCS_CEDA_INPUT_DIR
from bedrock.utils.mapping.location import US_FIPS, abbrev_us_state, to_ndigit_str

IN_DIR = os.path.join(os.path.dirname(__file__), "..", "input_data")


def CoA_Cropland_URL_helper(
    *, build_url: str, config: dict[str, Any], **_kwargs: Any
) -> List[str]:
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    Used for CoA_Cropland
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        for y in config['sector_levels']:
            # at national level, remove the text string calling for
            # state acronyms
            if x == 'NATIONAL':
                url = build_url
                url = url.replace("__aggLevel__", x)
                url = url.replace("__secLevel__", y)
                url = url.replace("&state_alpha=__stateAlpha__", "")
                if y == "ECONOMICS":
                    url = url.replace(
                        "AREA%20HARVESTED&statisticcat_desc=AREA%20IN%20"
                        "PRODUCTION&statisticcat_desc=TOTAL&statisticcat_desc="
                        "AREA%20BEARING%20%26%20NON-BEARING",
                        "AREA&statisticcat_desc=AREA%20OPERATED",
                    )
                else:
                    url = url.replace(
                        "&commodity_desc=AG%20LAND&" "commodity_desc=FARM%20OPERATIONS",
                        "",
                    )
                urls.append(url)
            else:
                # substitute in state acronyms for state and county url calls
                for z in state_abbrevs:
                    url = build_url
                    url = url.replace("__aggLevel__", x)
                    url = url.replace("__secLevel__", y)
                    url = url.replace("__stateAlpha__", z)
                    if y == "ECONOMICS":
                        url = url.replace(
                            "AREA%20HARVESTED&statisticcat_desc=AREA%20IN%20"
                            "PRODUCTION&statisticcat_desc=TOTAL&"
                            "statisticcat_desc=AREA%20BEARING%20%26%20NON-BEARING",
                            "AREA&statisticcat_desc=AREA%20OPERATED",
                        )
                    else:
                        url = url.replace(
                            "&commodity_desc=AG%20LAND&commodity_"
                            "desc=FARM%20OPERATIONS",
                            "",
                        )
                    urls.append(url)
    return urls


def CoA_URL_helper(
    *, build_url: str, config: dict[str, Any], **_kwargs: Any
) -> List[str]:
    """
    This helper function uses the "build_url" input from generateflowbyactivity.py,
    which is a base url for data imports that requires parts of the url text
    string to be replaced with info specific to the data year. This function
    does not parse the data, only modifies the urls from which data is
    obtained.
    Used for CoA_Cropland_NAICS and CoA_Livestock
    :param build_url: string, base url
    :param config: dictionary, items in FBA method yaml
    :return: list, urls to call, concat, parse, format into Flow-By-Activity
        format
    """
    # initiate url list for coa cropland data
    urls = []

    # call on state acronyms from common.py (and remove entry for DC)
    state_abbrevs = abbrev_us_state
    state_abbrevs = {k: v for (k, v) in state_abbrevs.items() if k != "DC"}

    # replace "__aggLevel__" in build_url to create three urls
    for x in config['agg_levels']:
        # at national level, remove the text string calling for state acronyms
        if x == 'NATIONAL':
            url = build_url
            url = url.replace("__aggLevel__", x)
            url = url.replace("&state_alpha=__stateAlpha__", "")
            urls.append(url)
        else:
            # substitute in state acronyms for state and county url calls
            for z in state_abbrevs:
                url = build_url
                url = url.replace("__aggLevel__", x)
                url = url.replace("__stateAlpha__", z)
                urls.append(url)
    return urls


def coa_call(*, resp: Response, **_kwargs: Any) -> pd.DataFrame:
    """
    Convert response for calling url to pandas dataframe, begin parsing df
    into FBA format
    :param url: string, url
    :param resp: df, response from url call
    :param args: dictionary, arguments specified when running
        generateflowbyactivity.py ('year' and 'source')
    :return: pandas dataframe of original source data
    """
    cropland_json = json.loads(resp.text)
    df_cropland = pd.DataFrame(data=cropland_json["data"])

    # During API call, save a copy to csv
    filename = f"{_kwargs['source']}_{_kwargs['year']}_{_define_filename(resp.url)}"
    df_cropland.to_csv(posixpath.join(IN_DIR, f"{filename}.csv"))

    return df_cropland


def _define_filename(url: str) -> str:
    """Parse the query to set the filename"""
    query_params = parse_qs(urlparse(url).query)
    agg_level = query_params['agg_level_desc'][0]
    desc = (query_params['sector_desc'][0]).replace('&', '')
    loc = ('US' if agg_level == "NATIONAL" else query_params['state_alpha'][0]) + (
        "_County" if agg_level == "COUNTY" else ""
    )
    return f"{loc}_{''.join(desc.split())}"


def coa_load_gcs(**kwargs: Any) -> pd.DataFrame:
    """For each url the file gets download and stored locally from gcs"""
    GCS_COA_DIR = posixpath.join(GCS_CEDA_INPUT_DIR, f"USDA_{kwargs.get('year')}")
    filename = f"{kwargs['source']}_{kwargs['year']}_{_define_filename(kwargs['url'])}"
    return load_from_gcs(
        name=f"{filename}.csv",
        sub_bucket=GCS_COA_DIR,
        local_dir=IN_DIR,
        loader=pd.read_csv,
    )


def coa_cropland_parse(
    *, df_list: List[pd.DataFrame], year: str, **_kwargs: Any
) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)
    # specify desired data based on domain_desc
    df = df[
        ~df['domain_desc'].isin(
            [
                'ECONOMIC CLASS',
                'FARM SALES',
                'IRRIGATION STATUS',
                'CONCENTRATION',
                'ORGANIC STATUS',
                'NAICS CLASSIFICATION',
                'PRODUCERS',
                'TYPOLOGY',
            ]
        )
    ]
    df = df[
        df['statisticcat_desc'].isin(
            [
                'AREA HARVESTED',
                'AREA IN PRODUCTION',
                'AREA BEARING & NON-BEARING',
                'AREA',
                'AREA OPERATED',
                'AREA GROWN',
            ]
        )
    ]
    # drop rows that subset data into farm sizes (ex. 'area harvested:
    # (1,000 to 1,999 acres)
    df = df[~df['domaincat_desc'].str.contains(' ACRES')].reset_index(drop=True)
    # drop Descriptions that contain certain phrases, as these data are
    # included in other categories
    df = df[
        ~df['short_desc'].str.contains(
            'FRESH MARKET|PROCESSING|ENTIRE CROP|NONE OF CROP|PART OF CROP'
        )
    ]
    # drop Descriptions that contain certain phrases - only occur in
    # AG LAND data
    df = df[
        ~df['short_desc'].str.contains('INSURANCE|OWNED|RENTED|FAILED|FALLOW|IDLE')
    ].reset_index(drop=True)
    # Many crops are listed as their own commodities as well as grouped
    # within a broader category (for example, orange
    # trees are also part of orchards). As this dta is not needed,
    # takes up space, and can lead to double counting if
    # included, want to drop these unused columns
    # subset dataframe into the 5 crop types and land in farms and drop rows
    # crop totals: drop all data
    # field crops: don't want certain commodities and don't
    # want detailed types of wheat, cotton, or sunflower
    df_fc = df[df['group_desc'] == 'FIELD CROPS']
    df_fc = df_fc[
        ~df_fc['commodity_desc'].isin(
            ['GRASSES', 'GRASSES & LEGUMES, OTHER', 'LEGUMES', 'HAY', 'HAYLAGE']
        )
    ]
    df_fc = df_fc[
        ~df_fc['class_desc'].str.contains(
            'SPRING|WINTER|TRADITIONAL|OIL|PIMA|UPLAND', regex=True
        )
    ]
    # fruit and tree nuts: only want a few commodities
    df_ftn = df[df['group_desc'] == 'FRUIT & TREE NUTS']
    df_ftn = df_ftn[df_ftn['commodity_desc'].isin(['BERRY TOTALS', 'ORCHARDS'])]
    df_ftn = df_ftn[df_ftn['class_desc'].isin(['ALL CLASSES'])]
    # horticulture: only want a few commodities
    df_h = df[df['group_desc'] == 'HORTICULTURE']
    df_h = df_h[
        df_h['commodity_desc'].isin(['CUT CHRISTMAS TREES', 'SHORT TERM WOODY CROPS'])
    ]
    # vegetables: only want a few commodities
    df_v = df[df['group_desc'] == 'VEGETABLES']
    df_v = df_v[df_v['commodity_desc'].isin(['VEGETABLE TOTALS'])]
    # only want ag land and farm operations in farms & land & assets
    df_fla = df[df['group_desc'] == 'FARMS & LAND & ASSETS']
    df_fla = df_fla[df_fla['short_desc'].str.contains("AG LAND|FARM OPERATIONS")]
    # drop the irrigated acreage in farms (want the irrigated harvested acres)
    df_fla = df_fla[
        ~(
            (df_fla['domaincat_desc'] == 'AREA CROPLAND, HARVESTED: (ANY)')
            & (df_fla['domain_desc'] == 'AREA CROPLAND, HARVESTED')
            & (df_fla['short_desc'] == 'AG LAND, IRRIGATED - ACRES')
        )
    ]
    # concat data frames
    df = pd.concat([df_fc, df_ftn, df_h, df_v, df_fla], sort=False).reset_index(
        drop=True
    )

    # address non-NAICS classification data
    # use info from other columns to determine flow name
    df.loc[:, 'FlowName'] = df['statisticcat_desc'] + ', ' + df['prodn_practice_desc']
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(
        ", ALL PRODUCTION PRACTICES", "", regex=True
    )
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(", IN THE OPEN", "", regex=True)
    # want to included "harvested" in the flowname when "harvested" is
    # included in the class_desc
    df['FlowName'] = np.where(
        df['class_desc'].str.contains(', HARVESTED'),
        df['FlowName'] + " HARVESTED",
        df['FlowName'],
    )
    # reorder
    df['FlowName'] = np.where(
        df['FlowName'] == 'AREA, IRRIGATED HARVESTED',
        'AREA HARVESTED, IRRIGATED',
        df['FlowName'],
    )
    # combine column information to create activity
    # information, and create two new columns for activities
    df['Activity'] = (
        df['commodity_desc'] + ', ' + df['class_desc'] + ', ' + df['util_practice_desc']
    )  # drop this column later
    # not interested in all data from class_desc
    df['Activity'] = df['Activity'].str.replace(", ALL CLASSES", "", regex=True)
    # not interested in all data from class_desc
    df['Activity'] = df['Activity'].str.replace(
        ", ALL UTILIZATION PRACTICES", "", regex=True
    )
    df["ActivityProducedBy"] = (
        df["Activity"].astype("string").where(df["unit_desc"].eq("OPERATIONS"), pd.NA)
    )

    df["ActivityConsumedBy"] = (
        df["Activity"].astype("string").where(df["unit_desc"].eq("ACRES"), pd.NA)
    )

    # rename columns to match flowbyactivity format
    df = df.rename(
        columns={
            "Value": "FlowAmount",
            "unit_desc": "Unit",
            "year": "Year",
            "CV (%)": "Spread",
            "short_desc": "Description",
        }
    )

    # modify contents of units column
    df.loc[df['Unit'] == 'OPERATIONS', 'Unit'] = 'p'
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # Add hardcoded data
    df['Class'] = np.where(df["Unit"] == 'ACRES', "Land", "Other")
    df['SourceName'] = "USDA_CoA_Cropland"
    df['FlowType'] = 'ELEMENTARY_FLOW'
    df = coa_common_parse(df)

    return df


def coa_livestock_parse(
    *, df_list: List[pd.DataFrame], year: str, **_kwargs: Any
) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)
    # # specify desired data based on domain_desc
    df = df[df['domain_desc'].str.contains("INVENTORY|TOTAL")]
    df = df[
        ~df['domain_desc'].str.contains("ECONOMIC CLASS|NAICS|FARM SALES|AREA OPERATED")
    ]
    # drop any specialized production practices
    df = df[df['prodn_practice_desc'] == 'ALL PRODUCTION PRACTICES']
    # drop specialized class descriptions
    df = df[~df['class_desc'].str.contains("BREEDING|MARKET")]

    # combine column information to create activity information,
    # and create two new columns for activities
    # drop this column later
    df['ActivityProducedBy'] = df['commodity_desc'] + ', ' + df['class_desc']
    # not interested in all class_desc data
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(
        ", ALL CLASSES", "", regex=True
    )
    # rename columns to match flowbyactivity format
    df = df.rename(
        columns={
            "Value": "FlowAmount",
            "unit_desc": "FlowName",
            "year": "Year",
            "CV (%)": "Spread",
            "domaincat_desc": "Compartment",
            "short_desc": "Description",
        }
    )

    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # # Add hardcoded data
    df['Class'] = "Other"
    df['SourceName'] = "USDA_CoA_Livestock"
    df['Unit'] = "p"
    df = coa_common_parse(df)

    return df


def coa_cropland_NAICS_parse(
    *, df_list: List[pd.DataFrame], year: str, **_kwargs: Any
) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param year: year
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    df = pd.concat(df_list, sort=False)
    # specify desired data based on domain_desc
    df = df[df['domain_desc'] == 'NAICS CLASSIFICATION']
    # only want ag land and farm operations
    df = df[df['short_desc'].str.contains("AG LAND|FARM OPERATIONS")]

    # NAICS classification data
    # flowname
    df.loc[:, 'FlowName'] = (
        df['commodity_desc']
        + ', '
        + df['class_desc']
        + ', '
        + df['prodn_practice_desc']
    )
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(
        ", ALL PRODUCTION PRACTICES", "", regex=True
    )
    df.loc[:, 'FlowName'] = df['FlowName'].str.replace(", ALL CLASSES", "", regex=True)
    # activity consumed/produced by
    df.loc[:, 'Activity'] = df['domaincat_desc']
    df.loc[:, 'Activity'] = df['Activity'].str.replace(
        "NAICS CLASSIFICATION: ", "", regex=True
    )
    df.loc[:, 'Activity'] = df['Activity'].str.replace('[()]+', '', regex=True)
    df['ActivityProducedBy'] = np.where(
        df["unit_desc"] == 'OPERATIONS', df["Activity"], ''
    )
    df['ActivityConsumedBy'] = np.where(df["unit_desc"] == 'ACRES', df["Activity"], '')

    # rename columns to match flowbyactivity format
    df = df.rename(
        columns={
            "Value": "FlowAmount",
            "unit_desc": "Unit",
            "year": "Year",
            "CV (%)": "Spread",
            "short_desc": "Description",
        }
    )

    # modify contents of units column
    df.loc[df['Unit'] == 'OPERATIONS', 'Unit'] = 'p'

    # drop Descriptions that contain certain phrases, as these
    # data are included in other categories
    df = df[
        ~df['Description'].str.contains(
            'FRESH MARKET|PROCESSING|ENTIRE CROP|NONE OF CROP|PART OF CROP'
        )
    ]
    # drop Descriptions that contain certain phrases -
    # only occur in AG LAND data
    df = df[
        ~df['Description'].str.contains('INSURANCE|OWNED|RENTED|FAILED|FALLOW|IDLE')
    ].reset_index(drop=True)
    # add location system based on year of data
    df = assign_fips_location_system(df, year)
    # Add hardcoded data
    df['Class'] = np.where(df["Unit"] == 'ACRES', "Land", "Other")
    df['SourceName'] = "USDA_CoA_Cropland_NAICS"
    df = coa_common_parse(df)

    return df


def coa_common_parse(df: pd.DataFrame) -> pd.DataFrame:

    # create FIPS column by combining existing columns
    df['Location'] = to_ndigit_str(df['state_fips_code'], 2) + to_ndigit_str(
        df['county_code'], 3
    )
    df.loc[df['Location'] == '99000', 'Location'] = US_FIPS

    # modify contents of flowamount column, "D" is supressed data,
    # "z" means less than half the unit is shown
    df['FlowAmount'] = df['FlowAmount'].str.strip()  # trim whitespace
    df.loc[df['FlowAmount'] == "(D)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df.loc[df['FlowAmount'] == "(Z)", 'FlowAmount'] = WITHDRAWN_KEYWORD
    df['FlowAmount'] = df['FlowAmount'].str.replace(",", "", regex=True)

    # USDA CoA 2017 states that (H) means CV >= 99.95,
    # therefore replacing with 99.95 so can convert column to int
    # (L) is a CV of <= 0.05
    df['Spread'] = df['Spread'].str.strip()  # trim whitespace
    df.loc[df['Spread'] == "(H)", 'Spread'] = 99.95
    df.loc[df['Spread'] == "(L)", 'Spread'] = 0.05
    df.loc[df['Spread'] == "", 'Spread'] = None
    df.loc[df['Spread'] == "(D)", 'Spread'] = WITHDRAWN_KEYWORD
    df['MeasureofSpread'] = "RSD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 2
    # Keep only necessary columns
    df = df[[c for c in flow_by_activity_fields.keys() if c in df.columns]]
    return df


if __name__ == "__main__":
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity

    generateFlowByActivity(year=2022, source='USDA_CoA_Cropland')
    fba = getFlowByActivity('USDA_CoA_Cropland', 2022)
