import typing as ta

from ceda_usa.utils.taxonomy.bea.v2012_industry import (
    BEA_2012_INDUSTRY_CODE,
    BEA_2012_INDUSTRY_CODES,
)
from ceda_usa.utils.taxonomy.bea.v2017_industry import (
    BEA_2017_INDUSTRY_CODE,
    BEA_2017_INDUSTRY_CODES,
)
from ceda_usa.utils.taxonomy.utils import validate_mapping


def load_bea_v2012_industry_to_bea_v2017_industry() -> (
    ta.Dict[BEA_2012_INDUSTRY_CODE, ta.List[BEA_2017_INDUSTRY_CODE]]
):
    def _map_v2012_industry_to_v2017_commodity(
        bea: BEA_2012_INDUSTRY_CODE,
    ) -> ta.List[BEA_2017_INDUSTRY_CODE]:
        if bea not in {"33391A", "335221", "335222", "335224", "335228"}:
            return [bea]  # type: ignore
        if bea in {"335221", "335222", "335224", "335228"}:
            return ["335220"]
        if bea == "33391A":
            return ["333914"]
        raise RuntimeError(f"Unexpected BEA 2012 industry code: {bea}")

    mapping = {
        bea: _map_v2012_industry_to_v2017_commodity(bea)
        for bea in BEA_2012_INDUSTRY_CODES
    }
    validate_mapping(
        mapping,
        domain=set(BEA_2012_INDUSTRY_CODES),
        codomain=set(BEA_2017_INDUSTRY_CODES),
    )
    return mapping
