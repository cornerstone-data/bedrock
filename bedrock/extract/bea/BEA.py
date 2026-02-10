# BEA.py (flowsa)
# !/usr/bin/env python3
# coding=utf-8

"""
Supporting functions for BEA data.

Generation of BEA Gross Output data and industry transaction data as FBA,
Source csv files for BEA data are documented
in scripts/write_BEA_data_from_useeior.py
"""

from typing import Any, cast

import numpy as np
import pandas as pd
from esupy.processed_data_mgmt import download_from_remote

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.generateflowbyactivity import generateFlowByActivity
from bedrock.extract.iot.io_2017 import (  # _load_2017_detail_make_use_usa,
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
from bedrock.extract.iot.io_price_index import load_go_detail
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.transform.iot.derived_price_index import _map_detail_table
from bedrock.utils.config.settings import PATHS
from bedrock.utils.mapping.location import US_FIPS
from bedrock.utils.metadata.metadata import set_fb_meta
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_MUT_YEARS


# %%
def bea_parse(*, source: str, year: int, **_: Any) -> pd.DataFrame:
    """
    Parse BEA data for GrossOutput, Make, and Use tables
    :param source:
    :param year:
    :return:
    """
    ## Case Detail_Supply
    if "Detail_Use_SUT" in source:
        df = _load_2017_detail_supply_use_usa('Use_SUT_detail')
        df = df.iloc[:, 1:]  # drop first column
        loc = df.index.get_loc('VAPRO')
        assert isinstance(loc, int)
        df = df.iloc[: loc + 1]  # drop everything after last row
        df = df.reset_index()
        df = df.rename(columns={'Code': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(
            id_vars=["ActivityProducedBy"],
            var_name="ActivityConsumedBy",
            value_name="FlowAmount",
        )
    elif "Detail_Supply" in source:
        df = _load_2017_detail_supply_use_usa('Supply_detail')
        df = df.iloc[:, 1:]  # drop first column
        loc = df.index.get_loc('T017')
        assert isinstance(loc, int)
        df = df.iloc[: loc + 1]  # drop everything after the total
        df = pd.DataFrame(np.transpose(df))
        df = df.reset_index().rename(columns={'index': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(
            id_vars=["ActivityProducedBy"],
            var_name="ActivityConsumedBy",
            value_name="FlowAmount",
        )
    elif "Summary_Supply" in source:
        df = _load_usa_summary_sut('Supply_summary', cast(USA_SUMMARY_MUT_YEARS, year))
        df = df.iloc[1:, 1:]  # drop first row and column
        df = pd.DataFrame(np.transpose(df))
        df = df.reset_index().rename(columns={'index': 'ActivityProducedBy'})
        # use "melt" fxn to convert colummns into rows
        df = df.melt(
            id_vars=["ActivityProducedBy"],
            var_name="ActivityConsumedBy",
            value_name="FlowAmount",
        )
    elif "Summary_Use_SUT" in source:
        df = _load_usa_summary_sut('Use_SUT_summary', cast(USA_SUMMARY_MUT_YEARS, year))
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
        df = _map_detail_table(load_go_detail())
        df = df.iloc[:, 1:]  # drop first column
        df = df.rename(columns={'sector_code': 'ActivityProducedBy'})
        df = (
            df.melt(
                id_vars=["ActivityProducedBy"], var_name="Year", value_name="FlowAmount"
            )
            .groupby(['ActivityProducedBy', 'Year'])['FlowAmount']
            .sum()
            .reset_index()
        )
        df = df[df['Year'] == str(year)]
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
    df['Description'] = (
        f'{source}_17sch' if 'GrossOutput' in source else f'{source}_{year}_17sch'
    )

    # Trim all string columns but avoid errors
    obj = df.select_dtypes(include="object")
    df[obj.columns] = obj.apply(
        lambda s: s.map(lambda x: x.strip() if isinstance(x, str) else x)
    )

    return df


# %%
if __name__ == "__main__":

    methods = [
        'BEA_Detail_Supply',  # Success
        'BEA_Detail_GrossOutput_IO',  # Fails due to flow amount
        'BEA_Detail_Use_SUT',  # Success
        'BEA_Summary_Supply',  # Fails due to flow amount - GCS has older data
        'BEA_Summary_Use_SUT',  # Fails due to flow amount - GCS has older data
    ]
    ## COMPARISON requires that the newly generated FBA does not yet exist or it
    ## will pull itself and compare to itself

    ## see flowsa PR https://github.com/USEPA/flowsa/pull/456/changes where
    # summary data were updated for v2.1

    for method in methods:
        print(f'\n\n{method}')
        # method = 'BEA_Detail_Supply'
        # year = 2017
        year = 2022 if any(st in method for st in ['Summary', 'Gross']) else 2017
        f = set_fb_meta(f'{method}_{year}', 'FlowByActivity')
        download_from_remote(f, PATHS)
        fba1 = getFlowByActivity(method, year)
        generateFlowByActivity(year=year, source=method)
        fba2 = getFlowByActivity(method, year)

        set(fba1['ActivityProducedBy']).difference(set(fba2['ActivityProducedBy']))
        set(fba2['ActivityProducedBy']).difference(set(fba1['ActivityProducedBy']))

        set(fba1['ActivityConsumedBy']).difference(set(fba2['ActivityConsumedBy']))
        set(fba2['ActivityConsumedBy']).difference(set(fba1['ActivityConsumedBy']))
        if 'Summary' in method or 'GrossOutput' in method:
            pd.testing.assert_frame_equal(
                fba1.drop(columns='FlowAmount'), fba2.drop(columns='FlowAmount')
            )
        else:
            pd.testing.assert_frame_equal(fba1, fba2)

    # for y in range(2022, 2023):
    #     generateFlowByActivity(year=y, source='BEA_Summary_Supply')
    #     generateFlowByActivity(year=y, source='BEA_Summary_Use_SUT')
    #     fba = getFlowByActivity('BEA_Summary_Supply', y)
    #     fba2 = getFlowByActivity('BEA_Summary_Use_SUT', y)
