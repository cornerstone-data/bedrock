import pandas as pd

from ceda_usa.utils.correspondence import create_correspondence_matrix
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from ceda_usa.utils.taxonomy.bea.v2012_summary import BEA_2012_SUMMARY_CODES
from ceda_usa.utils.taxonomy.bea.v2017_commodity_summary import (
    BEA_2017_COMMODITY_SUMMARY_CODE,
)
from ceda_usa.utils.taxonomy.mappings.bea_v2017_commodity__bea_ceda_v7 import (
    load_bea_v2017_commodity_to_bea_ceda_v7,
)
from ceda_usa.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from ceda_usa.utils.taxonomy.utils import reverse, traverse


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


def get_bea_v2017_summary_to_ceda_corresp_df() -> pd.DataFrame:
    summary_to_ceda_v7 = load_bea_v2017_summary_to_ceda_v7()
    summary_to_ceda_v7_corresp_df = create_correspondence_matrix(
        summary_to_ceda_v7
    ).loc[CEDA_V7_SECTORS, :]

    summary_to_ceda_v7_corresp_df.index.name = "sector"

    return summary_to_ceda_v7_corresp_df
