import typing as ta

from ceda_usa.utils.taxonomy.bea.v2017_commodity import (
    BEA_2017_COMMODITY_CODE,
    BEA_2017_COMMODITY_CODES,
)
from ceda_usa.utils.taxonomy.bea.v2017_industry import (
    BEA_2017_INDUSTRY_CODE,
    BEA_2017_INDUSTRY_CODES,
)
from ceda_usa.utils.taxonomy.utils import validate_mapping


def load_bea_v2017_industry_to_bea_v2017_commodity() -> (
    ta.Dict[BEA_2017_INDUSTRY_CODE, ta.List[BEA_2017_COMMODITY_CODE]]
):
    def _map_v2017_industry_to_v2017_commodity(
        bea: BEA_2017_INDUSTRY_CODE,
    ) -> ta.List[BEA_2017_COMMODITY_CODE]:
        if bea not in {"331314", "S00101", "S00201", "S00202"}:
            return [bea]  # type: ignore
        if bea == "331314":
            return ["331313"]
        if bea in {"S00101", "S00202"}:
            return ["221100"]
        if bea == "S00201":
            return ["485000"]
        raise RuntimeError(f"Unexpected BEA 2012 industry code: {bea}")

    mapping = {
        bea: _map_v2017_industry_to_v2017_commodity(bea)
        for bea in BEA_2017_INDUSTRY_CODES
    }
    validate_mapping(
        mapping,
        domain=set(BEA_2017_INDUSTRY_CODES),
        codomain=set(BEA_2017_COMMODITY_CODES),
    )
    return mapping
