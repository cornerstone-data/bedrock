import pandas as pd

from bedrock.extract.iot.constants import (
    PRICE_INDEX_DETAIL_NAME_TO_BEA_2017_INDUSTRY_MAPPING,
)
from bedrock.extract.iot.gdp import SECTOR_NAME_COL, SECTOR_SUMMARY_CODE_COL
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

SECTOR_CODE_COL = "sector_code"


def map_pi_summary__detail(pi_summary: pd.DataFrame) -> pd.DataFrame:
    """Map BEA summary-level PI rows onto their corresponding detail sectors."""
    summary__detail_mapping = pd.DataFrame.from_dict(
        load_bea_v2017_industry_to_bea_v2017_summary(),
        orient="index",
        columns=[SECTOR_SUMMARY_CODE_COL],
    )
    summary__detail_mapping[SECTOR_CODE_COL] = summary__detail_mapping.index

    return pi_summary.merge(
        summary__detail_mapping,
        on=SECTOR_SUMMARY_CODE_COL,
        how="left",
    ).set_index(SECTOR_NAME_COL)


def map_detail_table(df: pd.DataFrame) -> pd.DataFrame:
    """Attach BEA detail sector codes to the raw PI/GO tables via name mapping."""
    mapping = pd.DataFrame(
        list(PRICE_INDEX_DETAIL_NAME_TO_BEA_2017_INDUSTRY_MAPPING.items()),
        columns=[SECTOR_NAME_COL, SECTOR_CODE_COL],
    ).explode(SECTOR_CODE_COL)
    return df.merge(mapping, on=SECTOR_NAME_COL, how="left")
