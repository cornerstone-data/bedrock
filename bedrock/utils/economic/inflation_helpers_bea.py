"""BEA industry price index for the configured ``bea_price_index_vintage``.

2025Q2 reads the Watershed-derived parquet; later vintages read the index
bedrock publishes (publish_price_index), the same object CEDA pins.
"""

from __future__ import annotations

import os

import pandas as pd

from bedrock.extract.iot.gdp import BeaDataVersion
from bedrock.transform.iot.publish_price_index import (
    ARTIFACT_NAME,
    GCS_EXTRACT_OUTPUT_DIR,
)
from bedrock.utils.config.settings import FBA_DIR
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import download_gcs_file, download_gcs_file_if_not_exists
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir

# Obtained from Watershed price index source (rds_2Au4cfUuGHgFFLG37rdR),
# which is derived from BEA price index:
# TODO: migrate to publicly available BEA price index
# BEA price index on GCS: ``extract/input-data/BEA_PriceIndex/`` (no year subfolder).
INFLATION_FACTOR_DATA_GCS_PATH = gcs_extract_input_path("BEA_PriceIndex")

#: The vintage the Watershed-derived parquet stands in for. It is not
#: ``derive_industry_price_index('2025Q2')``, and it stays the default so v0.4
#: and v0.5 margins do not move.
REFERENCE_PARQUET_VINTAGE: BeaDataVersion = "2025Q2"

#: Published stem per later vintage, under ``extract/output-data``. Pinned, and
#: the same stem CEDA pins in ``ceda/utils/inflation.py``, so both read one file.
PUBLISHED_PRICE_INDEX_STEMS: dict[BeaDataVersion, str] = {
    "2026Q2": "BEA_PriceIndex_2026Q2_v0.5.0_a166975",
}


def obtain_inflation_factors_from_reference_data(
    vintage: BeaDataVersion | None = None,
) -> pd.DataFrame:
    """Wide sector x year price index; ``vintage`` defaults to the config's."""
    if vintage is None:
        vintage = get_usa_config().bea_price_index_vintage
    if vintage == REFERENCE_PARQUET_VINTAGE:
        return _load_reference_parquet()
    return load_published_price_index(vintage)


def _load_reference_parquet() -> pd.DataFrame:
    local_inflation_factor_path = os.path.join(
        local_extract_input_dir("BEA_PriceIndex"), "inflation_factors.parquet"
    )
    download_gcs_file_if_not_exists(
        "bea_price_index_2025_10_01.parquet",
        INFLATION_FACTOR_DATA_GCS_PATH,
        local_inflation_factor_path,
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
    )
    price_index.columns = price_index.columns.droplevel()
    assert isinstance(price_index, pd.DataFrame), "price_index must be a DataFrame"

    return price_index


def load_published_price_index(vintage: BeaDataVersion) -> pd.DataFrame:
    """Wide sector x year index from the published ``vintage`` (publish_price_index)."""
    stem = PUBLISHED_PRICE_INDEX_STEMS.get(vintage)
    if stem is None:
        raise KeyError(
            f"No published BEA price index pinned for {vintage}. Publish it "
            "with bedrock.transform.iot.publish_price_index --upload and add "
            "its stem to PUBLISHED_PRICE_INDEX_STEMS."
        )
    if not stem.startswith(f"{ARTIFACT_NAME}_{vintage}_"):
        raise ValueError(f"{stem} is pinned as {vintage} but names another")
    path = os.path.join(FBA_DIR, f"{stem}.parquet")
    if not os.path.exists(path):
        download_gcs_file(f"{stem}.parquet", GCS_EXTRACT_OUTPUT_DIR, path)
    price_index = pd.read_parquet(path).pivot(
        index="sector_code", columns="year", values="price_index"
    )
    price_index.columns = price_index.columns.astype(int)
    return price_index
