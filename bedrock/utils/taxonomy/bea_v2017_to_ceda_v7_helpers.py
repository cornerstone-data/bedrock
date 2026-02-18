import pandas as pd

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.utils.taxonomy.bea.v2012_summary import BEA_2012_SUMMARY_CODES
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    BEA_2017_COMMODITY_SUMMARY_CODE,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.correspondence import create_correspondence_matrix
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_ceda_v7 import (
    load_bea_v2017_commodity_to_bea_ceda_v7,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__cornerstone_commodity import (
    load_bea_v2017_commodity_to_cornerstone_commodity,
)
from bedrock.utils.taxonomy.utils import reverse, traverse


def load_bea_v2017_summary_to_ceda_v7() -> (
    dict[BEA_2017_COMMODITY_SUMMARY_CODE, list[CEDA_V7_SECTOR]]
):
    commodity_to_ceda_v7 = load_bea_v2017_commodity_to_bea_ceda_v7()
    commodity_to_summary = load_bea_v2017_commodity_to_bea_v2017_summary()

    ceda_v7_to_commodity = reverse(
        commodity_to_ceda_v7, new_domain=set(CEDA_V7_SECTORS)
    )
    ceda_v7_to_summary = traverse(ceda_v7_to_commodity, commodity_to_summary)
    return reverse(ceda_v7_to_summary, new_domain=set(BEA_2012_SUMMARY_CODES))


def load_bea_v2017_summary_to_cornerstone() -> (
    dict[BEA_2017_COMMODITY_SUMMARY_CODE, list[str]]
):
    commodity_to_cornerstone = load_bea_v2017_commodity_to_cornerstone_commodity()
    commodity_to_summary = load_bea_v2017_commodity_to_bea_v2017_summary()

    cornerstone_to_commodity = reverse(
        commodity_to_cornerstone, new_domain=set(COMMODITIES)
    )
    cornerstone_to_summary = traverse(cornerstone_to_commodity, commodity_to_summary)
    return reverse(cornerstone_to_summary, new_domain=set(BEA_2012_SUMMARY_CODES))  # type: ignore[arg-type]


def get_bea_v2017_summary_to_ceda_corresp_df() -> pd.DataFrame:
    summary_to_ceda_v7 = load_bea_v2017_summary_to_ceda_v7()
    summary_to_ceda_v7_corresp_df = create_correspondence_matrix(
        summary_to_ceda_v7
    ).loc[CEDA_V7_SECTORS, :]

    summary_to_ceda_v7_corresp_df.index.name = 'sector'

    return summary_to_ceda_v7_corresp_df


def get_bea_v2017_summary_to_cornerstone_corresp_df() -> pd.DataFrame:
    summary_to_cornerstone = load_bea_v2017_summary_to_cornerstone()
    corresp_df = create_correspondence_matrix(summary_to_cornerstone).reindex(
        index=COMMODITIES,
        columns=USA_2017_SUMMARY_INDUSTRY_CODES,
        fill_value=0,
    )
    corresp_df.index.name = 'sector'
    return corresp_df
