# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

Generation of BEA Gross Output data and industry transaction data as FBA,
Source csv files for BEA data are documented
in scripts/write_BEA_data_from_useeior.py
"""

import numpy as np
import pandas as pd

from bedrock.ceda_usa.extract.iot.io_2017 import (
    # _load_2017_detail_make_use_usa,
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.generateflowbyactivity import generateFlowByActivity
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.config.settings import externaldatapath
from bedrock.utils.mapping.location import US_FIPS


# %%
def bea_parse(*, source, year, **_):
    """
    Parse BEA data for GrossOutput, Make, and Use tables
    :param source:
    :param year:
    :return:
    """
    ## Case Detail_Supply
    if "Detail_Use_SUT" in source:
        filename = 'Use_SUT_detail'
        df = _load_2017_detail_supply_use_usa(filename)
        df = df.iloc[:, 1:]  # drop first column
        df = df.reset_index()
        df = df.rename(columns={'Code': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(
            id_vars=["ActivityProducedBy"],
            var_name="ActivityConsumedBy",
            value_name="FlowAmount",
        )
    elif "Detail_Supply" in source:
        filename = 'Supply_detail'
        df = _load_2017_detail_supply_use_usa(filename)
        df = df.iloc[1:, 1:]  # drop first row and column
        df = np.transpose(df)
        df = df.reset_index().rename(columns={'index': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(
            id_vars=["ActivityProducedBy"],
            var_name="ActivityConsumedBy",
            value_name="FlowAmount",
        )
    elif "Summary_Supply" in source:
        filename = 'Supply_summary'
        df = _load_usa_summary_sut(filename, year)
        df = df.iloc[1:, 1:]  # drop first row and column
        df = np.transpose(df)
        df = df.reset_index().rename(columns={'index': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(
            id_vars=["ActivityProducedBy"],
            var_name="ActivityConsumedBy",
            value_name="FlowAmount",
        )
    elif "Summary_Use_SUT" in source:
        filename = 'Use_SUT_summary'
        df = _load_usa_summary_sut(filename, year)
        df = df.iloc[1:, 1:]  # drop first row and column
        df = df.reset_index()
        df = df.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(
            id_vars=["ActivityProducedBy"],
            var_name="ActivityConsumedBy",
            value_name="FlowAmount",
        )
    elif "GrossOutput" in source:
        filename = f'{source}_17sch'
        df = pd.read_csv(externaldatapath / f"{filename}.csv")
        df = df.rename(columns={'Unnamed: 0': 'ActivityProducedBy'})
        df = df.melt(
            id_vars=["ActivityProducedBy"], var_name="Year", value_name="FlowAmount"
        )
        df = df[df['Year'] == year]
    else:
        raise KeyError(f'{source} not available')

    df = df.reset_index(drop=True)

    # columns relevant to all BEA data
    df["SourceName"] = source
    df['Year'] = str(year)
    df['FlowName'] = f"USD{str(year)}"
    df["Class"] = "Money"
    df["FlowType"] = "TECHNOSPHERE_FLOW"
    df["Location"] = US_FIPS
    df = assign_fips_location_system(df, year)
    df['FlowAmount'] = df['FlowAmount']
    df["Unit"] = "Million USD"
    df['DataReliability'] = 5  # tmp
    df['DataCollection'] = 5  # tmp
    df['Description'] = filename

    return df


if __name__ == "__main__":
    for y in range(2022, 2023):
        generateFlowByActivity(year=y, source='BEA_Summary_Supply')
        generateFlowByActivity(year=y, source='BEA_Summary_Use_SUT')
        fba = getFlowByActivity('BEA_Summary_Supply', y)
        fba2 = getFlowByActivity('BEA_Summary_Use_SUT', y)
    generateFlowByActivity(year=2017, source="BEA_Detail_Use_SUT")
    generateFlowByActivity(year=2017, source="BEA_Detail_Supply")
    generateFlowByActivity(year=2018, source="BEA_Detail_GrossOutput_IO")
