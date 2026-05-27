from __future__ import annotations

import functools

import pandas as pd

from bedrock.extract.iot.constants import GCS_USA_MAKE_USE_DIR
from bedrock.extract.iot.io_2017 import LOCAL_USA_MAKE_USE_DIR
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_2017_DETAIL_IO_MATRIX_MAPPING
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_COMMODITY_INDEX,
    USA_2017_INDUSTRY_INDEX,
)

MARGINS_SECTORS = ["Transportation", "Wholesale", "Retail"]


@functools.cache
def load_2017_margins_usa() -> pd.DataFrame:
    """
    Margins table, (commodity x industry) x margin component, after redefinitions,
    in producer price. Columns are Producer's Price, Transportation, Wholesale,
    Retail, and Purchaser's Price. unit is USD, original unit is million USD.

    https://www.bea.gov/products/industry-economic-accounts/underlying-estimates
        > Use Tables / After Redefinitions / Margin Details
        > 2017: 402 Industries XLSX
    Note: the margins table contains industries and final demand as columns.
    """
    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_MATRIX_MAPPING["Margins"],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth,
                sheet_name="2017",
                skiprows=4,
                dtype={"Commodity Code": str, "Industry Code": str},
            ),
        )
        .rename(
            columns={
                "Unnamed: 4": "Producer's Price",
                "Unnamed: 5": "Transportation",
                "Unnamed: 8": "Purchaser's Price",
            }
        )
        .fillna(0)
        .set_index(["Commodity Code", "Industry Code"])
        .loc[:, MARGINS_SECTORS + ["Producer's Price", "Purchaser's Price"]]
        .reindex(  # drop final demand columns and value added rows
            index=pd.MultiIndex.from_product(
                [USA_2017_COMMODITY_INDEX, USA_2017_INDUSTRY_INDEX]
            )
        )
        .fillna(0.0)
    )

    return df * MILLION_CURRENCY_TO_CURRENCY
