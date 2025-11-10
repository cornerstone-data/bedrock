import os
import posixpath

import pandas as pd

from ceda_usa.config.usa_config import get_usa_config
from ceda_usa.utils.gcp import GCS_CEDA_INPUT_DIR, load_from_gcs

GCS_EIA_DIR = posixpath.join(GCS_CEDA_INPUT_DIR, "EIA_EnergyPrice")
IN_DIR = os.path.join(os.path.dirname(__file__), "..", "input_data")


def load_propane_annual_avg_residential_price() -> float:
    """
    monthly price in $ per gallon from https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=M_EPLLPA_PRS_NUS_DPG&f=M
    """
    tbl = load_from_gcs(
        posixpath.join(GCS_EIA_DIR, "U.S._Propane_Residential_Price.csv"),
        local_dir=IN_DIR,
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


def load_heating_oil_annual_avg_residential_price() -> float:
    """
    monthly price in $ per gallon from https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?n=PET&s=M_EPD2F_PRS_NUS_DPG&f=M
    """
    tbl = load_from_gcs(
        posixpath.join(GCS_EIA_DIR, "U.S._No._2_Heating_Oil_Residential_Price.csv"),
        local_dir=IN_DIR,
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
