import typing as ta

from bedrock.utils.taxonomy.bea.v2017_commodity import (
    BEA_2017_COMMODITY_CODE,
    BEA_2017_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.cornerstone.commodities import (
    COMMODITIES,
    COMMODITY,
    WASTE_DISAGG_COMMODITIES,
)
from bedrock.utils.taxonomy.utils import validate_mapping


def load_bea_v2017_commodity_to_bea_cornerstone_commodity() -> (
    ta.Dict[BEA_2017_COMMODITY_CODE, ta.List[COMMODITY]]
):
    def _map_v2017_commodity_to_cornerstone_commodity(
        bea: BEA_2017_COMMODITY_CODE,
    ) -> ta.List[COMMODITY]:
        if bea == '562000':  # Waste disaggregation
            return WASTE_DISAGG_COMMODITIES['562000']
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
