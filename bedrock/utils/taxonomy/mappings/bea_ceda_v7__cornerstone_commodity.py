import typing as ta

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.utils.taxonomy.cornerstone.commodities import (
    COMMODITIES,
    COMMODITY,
    WASTE_DISAGG_COMMODITIES,
)
from bedrock.utils.taxonomy.utils import validate_mapping


def load_ceda_v7_commodity_to_cornerstone_commodity() -> (
    ta.Dict[CEDA_V7_SECTOR, ta.List[COMMODITY]]
):
    def _map_ceda_v7_to_cornerstone(
        ceda: CEDA_V7_SECTOR,
    ) -> ta.List[COMMODITY]:
        # Aluminum: CEDA v7 uses 331313, Cornerstone uses 33131B
        if ceda == '331313':
            return ['33131B']
        # Appliances: CEDA v7 splits 335220 into 4; Cornerstone keeps 335220
        if ceda in {'335221', '335222', '335224', '335228'}:
            return ['335220']
        # Waste: CEDA v7 keeps 562000; Cornerstone disaggregates into 7
        if ceda == '562000':
            return WASTE_DISAGG_COMMODITIES['562000']
        # 1:1 codes present in both taxonomies
        if ceda in COMMODITIES:
            return [ceda]  # type: ignore
        raise RuntimeError(f"Unexpected CEDA v7 sector code: {ceda}")

    mapping: ta.Dict[CEDA_V7_SECTOR, ta.List[COMMODITY]] = {
        ceda: _map_ceda_v7_to_cornerstone(ceda) for ceda in CEDA_V7_SECTORS
    }
    validate_mapping(
        mapping,
        domain=set(CEDA_V7_SECTORS),
        codomain=set(COMMODITIES),
        dangerously_skip_empty_mapping_check=True,
    )
    return mapping
