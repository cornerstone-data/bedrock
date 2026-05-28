from __future__ import annotations

import functools
import typing as ta

from bedrock.utils.config.common import load_crosswalk
from bedrock.utils.taxonomy.bea.v2017_commodity import (
    BEA_2017_COMMODITY_CODE,
    BEA_2017_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_sector import (
    BEA_2017_SECTOR_CODE,
    BEA_2017_SECTOR_CODES,
)
from bedrock.utils.taxonomy.utils import validate_mapping


@functools.cache
def _sector_to_detail_from_crosswalk() -> dict[str, list[str]]:
    df = load_crosswalk("NAICS_to_BEA_Crosswalk_2017")
    commodity_codes = set(BEA_2017_COMMODITY_CODES)
    grouped = (
        df.groupby("BEA_2017_Sector_Code", sort=False)["BEA_2017_Detail_Code"]
        .apply(
            lambda codes: sorted({str(c) for c in codes if str(c) in commodity_codes})
        )
        .to_dict()
    )
    return grouped


def load_bea_v2017_sector_to_bea_v2017_commodity() -> (
    ta.Dict[BEA_2017_SECTOR_CODE, ta.List[BEA_2017_COMMODITY_CODE]]
):
    grouped = _sector_to_detail_from_crosswalk()
    mapping: ta.Dict[BEA_2017_SECTOR_CODE, ta.List[BEA_2017_COMMODITY_CODE]] = {
        sector: grouped[sector]  # type: ignore[misc]
        for sector in BEA_2017_SECTOR_CODES
    }
    validate_mapping(  # type: ignore[misc]
        mapping,
        domain=set(BEA_2017_SECTOR_CODES),
        codomain=set(BEA_2017_COMMODITY_CODES),
        dangerously_skip_empty_mapping_check=True,
    )
    return mapping
