import typing as ta

from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from ceda_usa.utils.taxonomy.bea.v2017_commodity import (
    BEA_2017_COMMODITY_CODE,
    BEA_2017_COMMODITY_CODES,
)
from ceda_usa.utils.taxonomy.utils import validate_mapping


def load_bea_v2017_commodity_to_bea_ceda_v7() -> (
    ta.Dict[BEA_2017_COMMODITY_CODE, ta.List[CEDA_V7_SECTOR]]
):
    def _map_v2017_industry_to_ceda_v7(
        bea: BEA_2017_COMMODITY_CODE,
    ) -> ta.List[CEDA_V7_SECTOR]:
        if bea not in {"33131B", "335220", "S00402", "S00401", "S00300", "S00900"}:
            return [bea]  # type: ignore
        if bea == "33131B":
            return ["331313"]
        if bea == "335220":
            return ["335221", "335222", "335224", "335228"]
        if bea in {"S00402", "S00401", "S00300", "S00900"}:
            return []
        raise RuntimeError(f"Unexpected BEA 2012 industry code: {bea}")

    mapping = {
        bea: _map_v2017_industry_to_ceda_v7(bea) for bea in BEA_2017_COMMODITY_CODES
    }
    validate_mapping(
        mapping,
        domain=set(BEA_2017_COMMODITY_CODES),
        codomain=set(CEDA_V7_SECTORS),
        dangerously_skip_empty_mapping_check=True,
    )
    return mapping
