import os
import posixpath

import pandas as pd

from bedrock.ceda_usa.utils.gcp import (
    GCS_CEDA_INPUT_DIR,
    download_gcs_file_if_not_exists,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v5 import CEDA_V5_SECTORS
from bedrock.ceda_usa.utils.taxonomy.mappings.ceda_v7__ceda_v5 import (
    CEDA_V5_TO_CEDA_V7_CODES,
)

# Obtained from Watershed price index source (rds_2Au4cfUuGHgFFLG37rdR),
# which is derived from BEA price index:
# TODO: migrate to publicly available BEA price index
INFLATION_FACTOR_DATA_GCS_PATH = posixpath.join(
    GCS_CEDA_INPUT_DIR, "BEA_PriceIndex", "bea_price_index_2025_10_01.parquet"
)


def obtain_inflation_factors_from_reference_data() -> pd.DataFrame:
    local_inflation_factor_path = os.path.join(
        os.path.dirname(__file__), "input_data/inflation_factors.parquet"
    )
    download_gcs_file_if_not_exists(
        INFLATION_FACTOR_DATA_GCS_PATH, local_inflation_factor_path
    )

    price_index = (
        pd.read_parquet(local_inflation_factor_path)[
            ["year", "sector_code", "price_index"]
        ]
        .assign(
            sector_code=lambda df: df["sector_code"]
            .str.replace("naics_", "", regex=True)
            .str.upper()
        )
        .set_index(["sector_code", "year"])
        .unstack()
        .loc[CEDA_V5_SECTORS]
        .rename(index=CEDA_V5_TO_CEDA_V7_CODES)
    )
    price_index.columns = price_index.columns.droplevel()
    assert isinstance(price_index, pd.DataFrame), "price_index must be a DataFrame"

    return price_index
