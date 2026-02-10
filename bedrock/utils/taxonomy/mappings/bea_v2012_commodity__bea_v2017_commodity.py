import typing as ta

from bedrock.utils.taxonomy.bea.v2012_commodity import (
    BEA_2012_COMMODITY_CODE,
    BEA_2012_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import (
    BEA_2017_COMMODITY_CODE,
    BEA_2017_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.utils import validate_mapping


def load_bea_v2012_commodity_to_bea_v2017_commodity() -> (
    ta.Dict[BEA_2012_COMMODITY_CODE, ta.List[BEA_2017_COMMODITY_CODE]]
):
    def _map_v2012_commodity_to_v2017_commodity(
        bea: BEA_2012_COMMODITY_CODE,
    ) -> ta.List[BEA_2017_COMMODITY_CODE]:
        if bea not in {"33391A", "335221", "335222", "335224", "335228"}:
            return [bea]  # type: ignore
        if bea in {"335221", "335222", "335224", "335228"}:
            return ["335220"]
        if bea == "33391A":
            return ["333914"]
        raise RuntimeError(f"Unexpected BEA 2012 commodity code: {bea}")

    mapping: ta.Dict[BEA_2012_COMMODITY_CODE, ta.List[BEA_2017_COMMODITY_CODE]] = {
        bea: _map_v2012_commodity_to_v2017_commodity(bea)  # type: ignore[misc, arg-type]
        for bea in BEA_2012_COMMODITY_CODES
    }
    validate_mapping(  # type: ignore[misc]
        mapping,
        domain=set(BEA_2012_COMMODITY_CODES),
        codomain=set(BEA_2017_COMMODITY_CODES),
    )
    return mapping
