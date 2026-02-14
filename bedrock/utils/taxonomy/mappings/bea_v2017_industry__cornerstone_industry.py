import typing as ta

from bedrock.utils.taxonomy.bea.v2017_industry import (
    BEA_2017_INDUSTRY_CODE,
    BEA_2017_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.cornerstone.industries import (
    INDUSTRIES,
    INDUSTRY,
    WASTE_DISAGG_INDUSTRIES,
)
from bedrock.utils.taxonomy.utils import validate_mapping


def load_bea_v2017_industry_to_bea_cornerstone_industry() -> (
    ta.Dict[BEA_2017_INDUSTRY_CODE, ta.List[INDUSTRY]]
):
    def _map_v2017_industry_to_cornerstone_industry(
        bea: BEA_2017_INDUSTRY_CODE,
    ) -> ta.List[INDUSTRY]:
        if bea in {"S00101", "S00202"}:  # Aggregate electric power generation
            return ["221100"]
        if bea == "S00201":  # Aggregate passenger transportation
            return ["485000"]
        if bea == '562000':  # Waste disaggregation
            return WASTE_DISAGG_INDUSTRIES['562000']
        if bea in INDUSTRIES:
            return [bea]  # type: ignore
        raise RuntimeError(f"Unexpected BEA 2017 industry code: {bea}")

    mapping: ta.Dict[BEA_2017_INDUSTRY_CODE, ta.List[INDUSTRY]] = {
        bea: _map_v2017_industry_to_cornerstone_industry(bea)  # type: ignore[misc, arg-type]
        for bea in BEA_2017_INDUSTRY_CODES
    }
    validate_mapping(  # type: ignore[misc]
        mapping,
        domain=set(BEA_2017_INDUSTRY_CODES),
        codomain=set(INDUSTRIES),
        dangerously_skip_empty_mapping_check=True,
    )
    return mapping
