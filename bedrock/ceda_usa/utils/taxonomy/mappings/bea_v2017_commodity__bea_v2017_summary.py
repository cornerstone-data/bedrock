import typing as ta

from bedrock.ceda_usa.utils.taxonomy.bea.v2017_commodity import (
    BEA_2017_COMMODITY_CODE,
    BEA_2017_COMMODITY_CODES,
)
from bedrock.ceda_usa.utils.taxonomy.bea.v2017_commodity_summary import (
    BEA_2017_COMMODITY_SUMMARY_CODE,
    BEA_2017_COMMODITY_SUMMARY_CODES,
)
from bedrock.ceda_usa.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_commodity import (
    load_bea_v2017_industry_to_bea_v2017_commodity,
)
from bedrock.ceda_usa.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)
from bedrock.ceda_usa.utils.taxonomy.utils import reverse, traverse, validate_mapping


def load_bea_v2017_commodity_to_bea_v2017_summary() -> (
    ta.Dict[BEA_2017_COMMODITY_CODE, ta.List[BEA_2017_COMMODITY_SUMMARY_CODE]]
):
    mapping = traverse(
        reverse(
            load_bea_v2017_industry_to_bea_v2017_commodity(),
            new_domain=set(BEA_2017_COMMODITY_CODES),
        ),
        load_bea_v2017_industry_to_bea_v2017_summary(),
    )
    # intentionally make these two detail sector to only map to their native parent sector
    mapping["221100"] = ["22"]
    mapping["485000"] = ["485"]
    # add missing commodities
    mapping["S00401"] = ["Used"]
    mapping["S00402"] = ["Used"]
    mapping["S00300"] = ["Other"]
    mapping["S00900"] = ["Other"]

    validate_mapping(
        mapping,
        domain=set(BEA_2017_COMMODITY_CODES),
        codomain=set(BEA_2017_COMMODITY_SUMMARY_CODES),
    )
    return mapping
