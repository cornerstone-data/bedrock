# USDA_ERS_FIWS.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8
"""
USDA Economic Research Service (ERS) Farm Income and Wealth Statistics (FIWS)
https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/

Downloads the February 2025 update (the zip carries 1910-2025).

Carries three concepts, per KEPT_CONCEPTS: cash receipts; intermediate product
expenses -- the concept BEA itself uses to build the agriculture input column
(Table C2), and what #577 is built on; and inventory change, the farm branch of
``F03000`` (#529).
"""

import io
import zipfile
from typing import Any

import pandas as pd
from requests import Response

from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.mapping.location import (
    US_FIPS,
    get_all_state_FIPS_2,
    us_state_abbrev,
)

#: The published concepts this FBA carries, matched as **prefixes** of
#: ``FlowName`` (the text before the first comma of
#: ``VariableDescriptionTotal``).
#:
#: ⚠️ **Adding a concept here is the only way to surface it** -- the parse drops
#: everything else, so a variable missing from this list looks like a variable
#: missing from the source.  ``Cash receipts`` was the sole entry until
#: 2026-08-25, which is why ``Intermediate product expenses`` appeared to be
#: absent from a source that has published it since 1910.
#:
#: ⚠️ **``Cash receipt`` is singular deliberately.**  ERS labels the concept
#: ``Cash receipts value`` everywhere except bell peppers, which are
#: ``Cash receipt value`` -- 129 rows over 2008-2023.  The previous
#: ``contains('Cash receipts')`` test dropped them: $626M of $1,470,607M in
#: 2017, 0.04%.  Small, and a source typo rather than a real distinction.
#: ⚠️ **``Inventory change value`` is a CHANGE, and it is the farm branch of
#: ``F03000``.** ``inventories_estimation_plan.md`` used to prescribe
#: differencing the ``Dec. 31 value of ... inventory`` **stock** series and
#: warned that doing so imports holding gains -- measured at -887 against a true
#: farm CIPI of -5,679, out by roughly six times. That warning is right and the
#: prescription was unnecessary: ERS publishes the change concept directly, 1910
#: -2025, and it was invisible only because this filter dropped it.
#:
#: Graded against NIPA's published farm line (``T50705B`` ``B018RC``) over
#: 2012-2023: **correlation 0.936, sign agreement 12 of 13 years**, mean
#: absolute difference 1,736 $M on a mean absolute level of 7,409 $M. Against
#: the stock-differencing route's 6x error, this is the better instrument.
#:
#: ⚠️ **2024 does not fit and must not be used.** NIPA reads +552 $M against
#: FIWS's -9,744 $M -- the only sign disagreement in the span. The February 2025
#: vintage's latest years are ERS forecasts rather than realized estimates, and
#: the file carries no flag distinguishing them, so the span has to be bounded
#: here rather than detected. Same caveat as #577.
#:
#: ⚠️ **The three inventory-change rows NEST.** ``All commodities`` is the parent
#: of ``All crops`` and ``Animals and products`` and equals their sum exactly
#: (2017: -7,108,016 + 1,056,304 = -6,051,712 $1,000). Summing this FBA on the
#: concept without filtering double-counts the farm column.
#:
#: ⚠️ **The change series has no purchased-inputs component**, though the Dec. 31
#: stock series does. It is crops and livestock only, which is what the farm
#: split needs; the level comes from NIPA either way.
KEPT_CONCEPTS = (
    'Cash receipt',
    'Intermediate product expenses',
    'Inventory change value',
)

#: ⚠️ Marks the operator-dwellings variant of an expense series.  See the parse.
DWELLINGS_INCLUDED = 'incl. operator dwellings'


def fiws_call(*, resp: Response, **_: Any) -> pd.DataFrame:
    """
    Convert response for calling url to pandas dataframe, begin parsing
    df into FBA format
    :param resp: df, response from url call
    :return: pandas dataframe of original source data
    """
    # extract data from zip file (only one csv)
    with zipfile.ZipFile(io.BytesIO(resp.content), "r") as f:
        # read in file names
        for name in f.namelist():
            data = f.open(name)
            df = pd.read_csv(data, encoding="ISO-8859-1")
        return df


def fiws_parse(*, df_list: list[pd.DataFrame], year: str, **_: Any) -> pd.DataFrame:
    """
    Combine, parse, and format the provided dataframes
    :param df_list: list of dataframes to concat and format
    :param args: dictionary, used to run generateflowbyactivity.py
        ('year' and 'source')
    :return: df, parsed and partially formatted to flowbyactivity
        specifications
    """
    # concat dataframes
    df = pd.concat(df_list, sort=False)
    # select data for chosen year, cast year as string to match argument
    df['Year'] = df['Year'].astype(str)
    df = df[df['Year'] == year].reset_index(drop=True)
    # add state geo codes, reading in datasets from common.py
    fips = get_all_state_FIPS_2().reset_index(drop=True)
    # ensure capitalization of state names
    fips['State'] = fips['State'].apply(lambda x: x.title())
    fips['StateAbbrev'] = fips['State'].map(us_state_abbrev)
    # pad zeroes
    fips['FIPS_2'] = fips['FIPS_2'].apply(lambda x: x.ljust(3 + len(x), '0'))
    df = pd.merge(df, fips, how='left', left_on='State', right_on='StateAbbrev')
    # set us location code
    df.loc[df['State_x'] == 'US', 'FIPS_2'] = US_FIPS
    # drop "All" in variabledescription2
    df.loc[df['VariableDescriptionPart2'] == 'All', 'VariableDescriptionPart2'] = 'drop'
    # combine variable descriptions to create Activity name and remove ", drop"
    df['ActivityProducedBy'] = (
        df['VariableDescriptionPart1'] + ', ' + df['VariableDescriptionPart2']
    )
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.replace(
        ", drop", "", regex=True
    )
    # trim whitespace
    df['ActivityProducedBy'] = df['ActivityProducedBy'].str.strip()
    # drop columns
    df = df.drop(
        columns=[
            'artificialKey',
            'PublicationDate',
            'Source',
            'ChainType_GDP_Deflator',
            'VariableDescriptionPart1',
            'VariableDescriptionPart2',
            'State_x',
            'State_y',
            'StateAbbrev',
            'unit_desc',
        ]
    )
    # rename columns
    df = df.rename(
        columns={
            "VariableDescriptionTotal": "Description",
            "Amount": "FlowAmount",
            "FIPS_2": "Location",
        }
    )
    # assign flowname, based on comma placement
    df['FlowName'] = df['Description'].str.split(',').str[0]
    # add location system based on year of data
    df['Year'] = df['Year'].astype(int)
    df = assign_fips_location_system(df, year)
    # Keep the published concepts downstream work needs. ⚠️ This filter used to
    # be `Cash receipts` alone, which silently dropped intermediate expenses --
    # the concept #577 is built on, and the one BEA itself uses for the
    # agriculture input column (Table C2). Widening rather than forking: no FBS
    # method or Python module consumed this FBA.
    df = df[df['FlowName'].str.startswith(KEPT_CONCEPTS)]
    # ⚠️ Several expense categories publish BOTH an `excl. operator dwellings`
    # and an `incl. operator dwellings` variant, and the two are identical in
    # VariableDescriptionPart2 -- so they collide in ActivityProducedBy and
    # double-count on any groupby. Farm dwellings are not an intermediate input
    # to farming, and the `excl.` series is the one that reconciles: for 2017 it
    # sums to the published 226,610,581 exactly across farm origin, manufactured
    # inputs and other intermediate.
    df = df[~df['Description'].str.contains(DWELLINGS_INCLUDED, regex=False)]
    # ⚠️ `Miscellaneous` still collides after that: four expense series share
    # VariableDescriptionPart2 = 'Miscellaneous' -- the group total, insurance
    # premiums, federal insurance premiums and irrigation -- so the Part1+Part2
    # activity name is not unique for expenses and a groupby would silently add
    # a group to its own members. The full published description is unique, so
    # expenses take that as their activity instead.
    #
    # ⚠️ Cash receipts keep the Part1+Part2 name: those are the activities
    # Sector_Crosswalk_USDA_ERS_FIWS.csv is keyed on, and renaming them would
    # break every existing mapping.
    expenses = df['FlowName'] == 'Intermediate product expenses'
    df.loc[expenses, 'ActivityProducedBy'] = (
        df.loc[expenses, 'Description'].str.replace(r'\s+', ' ', regex=True).str.strip()
    )
    # the unit is $1000 USD, so multiply FlowAmount by 1000 and
    # set unit as 'USD'
    df['FlowAmount'] = df['FlowAmount'].astype(float)
    df['FlowAmount'] = df['FlowAmount'] * 1000
    # hard code data
    df['Class'] = 'Money'
    df['SourceName'] = 'USDA_ERS_FIWS'
    df['Unit'] = 'USD'
    # Add DQ scores
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    # sort df
    df = df.sort_values(['Location', 'FlowName'])
    # reset index
    df.reset_index(drop=True, inplace=True)

    return df


if __name__ == "__main__":
    from bedrock.extract.flowbyactivity import getFlowByActivity
    from bedrock.extract.generateflowbyactivity import generateFlowByActivity

    generateFlowByActivity(year='2012-2025', source='USDA_ERS_FIWS')
    fba = getFlowByActivity('USDA_ERS_FIWS', year=2025)
