import typing as ta
from typing import cast

from bedrock.utils.taxonomy.bea.v2017_commodity import (
    BEA_2017_COMMODITY_CODE,
    BEA_2017_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES, COMMODITY
from bedrock.utils.taxonomy.cornerstone.disagg_sectors import DISAGG_SECTORS
from bedrock.utils.taxonomy.utils import validate_mapping


def load_bea_v2017_commodity_to_cornerstone_commodity() -> (
    ta.Dict[BEA_2017_COMMODITY_CODE, ta.List[COMMODITY]]
):
    def _map_v2017_commodity_to_cornerstone_commodity(
        bea: BEA_2017_COMMODITY_CODE,
    ) -> ta.List[COMMODITY]:
        for sector in DISAGG_SECTORS.values():
            if bea == sector.commodity_aggregate_code:
                return cast(ta.List[COMMODITY], [*sector.commodity_new_codes])
        if bea in {  # Drop these commodities
            'S00401',  # Scrap
            'S00300',  # Noncomparable imports
            'S00900',  # Rest of the world adjustment
        }:
            return []
        if bea in COMMODITIES:
            return [bea]  # type: ignore
        raise RuntimeError(f"Unexpected BEA 2017 commodity code: {bea}")

    mapping: ta.Dict[BEA_2017_COMMODITY_CODE, ta.List[COMMODITY]] = {
        bea: _map_v2017_commodity_to_cornerstone_commodity(bea)  # type: ignore[misc, arg-type]
        for bea in BEA_2017_COMMODITY_CODES
    }
    validate_mapping(  # type: ignore[misc]
        mapping,
        domain=set(BEA_2017_COMMODITY_CODES),
        codomain=set(COMMODITIES),
        dangerously_skip_empty_mapping_check=True,
    )
    return mapping
