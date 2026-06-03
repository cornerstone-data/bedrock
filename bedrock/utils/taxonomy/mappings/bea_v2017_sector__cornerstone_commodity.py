import typing as ta

from bedrock.utils.taxonomy.bea.v2017_commodity_sector import (
    BEA_2017_SECTOR_COMMODITY_CODE,
    BEA_2017_SECTOR_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES, COMMODITY
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__cornerstone_commodity import (
    load_bea_v2017_commodity_to_cornerstone_commodity,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_sector__bea_v2017_commodity import (
    load_bea_v2017_sector_commodity_to_bea_v2017_commodity,
)
from bedrock.utils.taxonomy.utils import traverse, validate_mapping


def load_bea_v2017_sector_commodity_to_cornerstone_commodity() -> (
    ta.Dict[BEA_2017_SECTOR_COMMODITY_CODE, ta.List[COMMODITY]]
):
    mapping = traverse(
        load_bea_v2017_sector_commodity_to_bea_v2017_commodity(),
        load_bea_v2017_commodity_to_cornerstone_commodity(),
    )
    validate_mapping(
        mapping,
        domain=set(BEA_2017_SECTOR_COMMODITY_CODES),
        codomain=set(COMMODITIES),
        dangerously_skip_empty_mapping_check=True,
    )
    return mapping
