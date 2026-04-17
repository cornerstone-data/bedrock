import functools

import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir


@functools.cache
def load_propane_annual_avg_residential_price() -> float:
    """
    monthly price in $ per gallon from https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=M_EPLLPA_PRS_NUS_DPG&f=M
    """
    tbl = load_from_gcs(
        name="U.S._Propane_Residential_Price.csv",
        sub_bucket=gcs_extract_input_path("EIA_EnergyPrice"),
        local_dir=local_extract_input_dir("EIA_EnergyPrice"),
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=4,
        ),
    )
    tbl["Year"] = tbl["Month"].str.extract(r"(\d{4})").astype(int)

    annual_avg_price = tbl.groupby("Year")[
        "U.S. Propane Residential Price Dollars per Gallon"
    ].mean()[get_usa_config().usa_ghg_data_year]

    return annual_avg_price


@functools.cache
def load_heating_oil_annual_avg_residential_price() -> float:
    """
    monthly price in $ per gallon from https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=M_EPD2F_PRS_NUS_DPG&f=M
    """
    tbl = load_from_gcs(
        name="U.S._No._2_Heating_Oil_Residential_Price.csv",
        sub_bucket=gcs_extract_input_path("EIA_EnergyPrice"),
        local_dir=local_extract_input_dir("EIA_EnergyPrice"),
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=4,
        ),
    )
    tbl["Year"] = tbl["Month"].str.extract(r"(\d{4})").astype(int)

    annual_avg_price = tbl.groupby("Year")[
        "U.S. No. 2 Heating Oil Residential Price Dollars per Gallon"
    ].mean()[get_usa_config().usa_ghg_data_year]

    return annual_avg_price
