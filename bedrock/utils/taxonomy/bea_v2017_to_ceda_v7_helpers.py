import typing as ta
from typing import cast

import pandas as pd

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.utils.taxonomy.bea.v2012_summary import BEA_2012_SUMMARY_CODES
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    BEA_2017_COMMODITY_SUMMARY_CODE,
    BEA_2017_COMMODITY_SUMMARY_CODES,
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

# USEEIO / Cornerstone splits of a single BEA detail commodity (not separate rows
# in ``load_bea_v2017_commodity_to_bea_v2017_summary()``).
_APPLIANCE_DETAIL_TO_PARENT: dict[str, str] = {
    '335221': '335220',
    '335222': '335220',
    '335224': '335220',
    '335228': '335220',
}


def _useeio_split_detail_to_parent_commodity() -> dict[str, str]:
    """Map USEEIO detail codes that are BEA split children -> parent BEA commodity."""
    out: dict[str, str] = dict(_APPLIANCE_DETAIL_TO_PARENT)
    for parent, children in load_bea_v2017_commodity_to_cornerstone_commodity().items():
        for ch in children:
            if ch != parent:
                out[str(ch)] = str(parent)
    return out


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


def get_bea_v2017_summary_to_useeio_corresp_df(
    useeio_detail_index: pd.Index,
) -> pd.DataFrame:
    """Build BEA summary->USEEIO-detail correspondence.

    Rows are BEA 2017 detail commodity codes (USEEIO's industry/commodity axis).
    ``create_correspondence_matrix(load_bea_v2017_summary_to_cornerstone())``
    indexes rows by cornerstone split targets instead, so reindexing that matrix
    onto ``useeio_detail_index`` zero-fills every USEEIO row — use the official
    commodity->summary map reversed instead.

    Note:
    This correspondence is retained for USEEIO diagnostics/support tooling and
    auxiliary exports. Current USEEIO BLy ``y_old`` no longer uses this
    disaggregation path; it uses Cornerstone ``y_nab`` reindexed to USEEIO axis.
    """
    commodity_to_summary = load_bea_v2017_commodity_to_bea_v2017_summary()
    summary_codomain = cast(
        ta.AbstractSet[BEA_2017_COMMODITY_SUMMARY_CODE],
        frozenset(BEA_2017_COMMODITY_SUMMARY_CODES),
    )
    summary_to_commodities = reverse(
        commodity_to_summary,
        new_domain=summary_codomain,
    )
    corresp_full = create_correspondence_matrix(summary_to_commodities).reindex(
        columns=USA_2017_SUMMARY_INDUSTRY_CODES,
        fill_value=0,
    )
    idx = useeio_detail_index.sort_values()
    corresp_df = corresp_full.reindex(index=idx, fill_value=0)
    split_to_parent = _useeio_split_detail_to_parent_commodity()
    for sector in idx:
        if corresp_df.loc[sector].sum() != 0:
            continue
        parent = split_to_parent.get(str(sector))
        if parent is None or parent not in corresp_full.index:
            continue
        if corresp_full.loc[parent].sum() == 0:
            continue
        corresp_df.loc[sector] = corresp_full.loc[parent]
    corresp_df.index.name = 'sector'
    return corresp_df
