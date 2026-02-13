import functools

import pandas as pd

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.utils.taxonomy.bea.v2012_commodity import (
    USA_2012_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2012_industry import (
    USA_2012_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import (
    USA_2017_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    USA_2017_SUMMARY_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry import (
    USA_2017_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_summary_final_demand import (
    USA_2017_SUMMARY_FINAL_DEMAND_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_value_added import (
    USA_2017_VALUE_ADDED_CODES,
)
from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix
from bedrock.utils.taxonomy.mappings.bea_v2012_commodity__bea_v2017_commodity import (
    load_bea_v2012_commodity_to_bea_v2017_commodity,
)
from bedrock.utils.taxonomy.mappings.bea_v2012_industry__bea_v2017_industry import (
    load_bea_v2012_industry_to_bea_v2017_industry,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_ceda_v7 import (
    load_bea_v2017_commodity_to_bea_ceda_v7,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_commodity import (
    load_bea_v2017_industry_to_bea_v2017_commodity,
)
from bedrock.utils.taxonomy.utils import traverse

USA_2017_INDUSTRY_INDEX = pd.Index(USA_2017_INDUSTRY_CODES, name="industry")
USA_2017_COMMODITY_INDEX = pd.Index(USA_2017_COMMODITY_CODES, name="commodity")
USA_2017_FINAL_DEMAND_INDEX = pd.Index(USA_2017_FINAL_DEMAND_CODES, name="final_demand")
USA_2017_VALUE_ADDED_INDEX = pd.Index(USA_2017_VALUE_ADDED_CODES, name="value_added")
CEDA_V7_SECTOR_INDEX = pd.Index(CEDA_V7_SECTORS, name="industry")

USA_2017_SUMMARY_INDUSTRY_INDEX = pd.Index(
    USA_2017_SUMMARY_INDUSTRY_CODES, name="industry"
)
USA_2017_SUMMARY_COMMODITY_INDEX = pd.Index(
    USA_2017_SUMMARY_COMMODITY_CODES, name="commodity"
)
USA_2017_SUMMARY_FINAL_DEMAND_INDEX = pd.Index(
    USA_2017_SUMMARY_FINAL_DEMAND_CODES, name="final_demand"
)


@functools.cache
def load_usa_2017_commodity__ceda_v7_correspondence() -> pd.DataFrame:
    return create_correspondence_matrix(
        load_bea_v2017_commodity_to_bea_ceda_v7(),  # type: ignore
        domain=USA_2017_COMMODITY_CODES,
        range=CEDA_V7_SECTORS,
        is_injective=False,
        is_surjective=False,
        is_complete=False,
    ).astype(float)


@functools.cache
def load_usa_2017_industry__ceda_v7_correspondence() -> pd.DataFrame:
    return create_correspondence_matrix(
        traverse(
            load_bea_v2017_industry_to_bea_v2017_commodity(),
            load_bea_v2017_commodity_to_bea_ceda_v7(),
        ),  # type: ignore
        domain=USA_2017_INDUSTRY_CODES,
        range=CEDA_V7_SECTORS,
        is_injective=False,
        is_surjective=False,
        is_complete=False,
    ).astype(float)


@functools.cache
def load_usa_2012_industry__usa_2017_industry_correspondence() -> pd.DataFrame:
    return create_correspondence_matrix(
        load_bea_v2012_industry_to_bea_v2017_industry(),  # type: ignore
        domain=USA_2012_INDUSTRY_CODES,
        range=USA_2017_INDUSTRY_CODES,
        is_injective=False,
        is_surjective=False,
        is_complete=False,
    ).astype(float)


@functools.cache
def load_usa_2012_commodity__usa_2017_commodity_correspondence() -> pd.DataFrame:
    return create_correspondence_matrix(
        load_bea_v2012_commodity_to_bea_v2017_commodity(),  # type: ignore
        domain=USA_2012_COMMODITY_CODES,
        range=USA_2017_COMMODITY_CODES,
        is_injective=False,
        is_surjective=False,
        is_complete=False,
    ).astype(float)


@functools.cache
def load_usa_2012_commodity__ceda_v7_correspondence() -> pd.DataFrame:
    return create_correspondence_matrix(
        traverse(load_bea_v2012_commodity_to_bea_v2017_commodity(), load_bea_v2017_commodity_to_bea_ceda_v7()),  # type: ignore
        domain=USA_2012_COMMODITY_CODES,
        range=CEDA_V7_SECTORS,
        is_injective=False,
        is_surjective=False,
        is_complete=False,
    ).astype(float)


@functools.cache
def load_usa_2012_industry__ceda_v7_correspondence() -> pd.DataFrame:
    return create_correspondence_matrix(
        traverse(
            load_bea_v2012_industry_to_bea_v2017_industry(),
            traverse(
                load_bea_v2017_industry_to_bea_v2017_commodity(),
                load_bea_v2017_commodity_to_bea_ceda_v7(),
            ),
        ),  # type: ignore
        domain=USA_2012_INDUSTRY_CODES,
        range=CEDA_V7_SECTORS,
        is_injective=False,
        is_surjective=False,
        is_complete=False,
    ).astype(float)
