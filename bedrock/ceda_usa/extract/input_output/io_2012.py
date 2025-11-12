from __future__ import annotations

import os
import posixpath
import typing as ta

import pandas as pd
from typing_extensions import deprecated

from bedrock.ceda_usa.utils.ceda_sector_index import get_ceda_sector_index
from bedrock.ceda_usa.utils.constants import (
    USA_2012_COMMODITY_CODES,
    USA_2012_FINAL_DEMAND_CODES,
    USA_2012_INDUSTRY_CODES,
)
from bedrock.ceda_usa.utils.gcp import (
    GCS_CEDA_V5_INPUT_DIR,
    download_gcs_file_if_not_exists,
)
from bedrock.ceda_usa.utils.ghg import GHG_DETAILED
from bedrock.ceda_usa.utils.units import MILLION_CURRENCY_TO_CURRENCY

USA_IO_VECTOR_NAMES = ta.Literal[
    "q0",
    "gR",
    "pR",
    "qR",
]

USA_IO_MATRIX_NAMES = ta.Literal[
    "VR",
    "UR",
    "URdom",
    "PI",
    "PC",
    "YR",
]

IN_DIR = os.path.join(os.path.dirname(__file__), "input_data")

USA_INDUSTRY_INDEX = pd.Index(USA_2012_INDUSTRY_CODES, name="industry")
USA_COMMODITY_INDEX = pd.Index(USA_2012_COMMODITY_CODES, name="commodity")
USA_FINAL_DEMAND_INDEX = pd.Index(USA_2012_FINAL_DEMAND_CODES, name="final_demand")
GHG_DETAILED_INDEX = pd.Index(GHG_DETAILED)


def load_2012_VR_usa() -> pd.DataFrame:
    """
    Redefined Make matrix in producer's price
    unit is USD, original unit is million USD
    """
    df = _load_usa_io_matrix("VR") * MILLION_CURRENCY_TO_CURRENCY
    df.index = USA_INDUSTRY_INDEX
    df.columns = USA_COMMODITY_INDEX
    return df


def load_2012_UR_usa() -> pd.DataFrame:
    """
    Redefined Use matrix in producer's price
    unit is USD, original unit is million USD
    """
    df = _load_usa_io_matrix("UR") * MILLION_CURRENCY_TO_CURRENCY
    df.index = USA_COMMODITY_INDEX
    df.columns = USA_INDUSTRY_INDEX
    return df


def load_2012_URdom_usa() -> pd.DataFrame:
    """
    Redefined Domestic portion of Use matrix in producer's price
    unit is USD, original unit is million USD
    """
    df = _load_usa_io_matrix("URdom") * MILLION_CURRENCY_TO_CURRENCY
    df.index = USA_COMMODITY_INDEX
    df.columns = USA_INDUSTRY_INDEX
    return df


def load_2012_PI_usa() -> pd.DataFrame:
    """
    400 industry X 405 industry matrix that reduces # of industry
    """
    df = _load_usa_io_matrix("PI")
    df.columns = USA_INDUSTRY_INDEX
    df.index = get_ceda_sector_index()
    return df


def load_2012_PC_usa() -> pd.DataFrame:
    """
    401 commodity X 400 commodity matrix that reduces # of commodity
    """
    df = _load_usa_io_matrix("PC")
    df.index = USA_COMMODITY_INDEX
    df.columns = get_ceda_sector_index()
    return df


def load_2012_gR_usa() -> pd.Series[float]:
    """
    Redefined industry output vector
    unit is USD, original unit is million USD
    """
    ser = _load_usa_io_vector("gR") * MILLION_CURRENCY_TO_CURRENCY
    ser.index = USA_INDUSTRY_INDEX
    return ser


def load_2012_pR_usa() -> pd.Series[float]:
    """
    Redefined scrap fraction vector
    """
    ser = _load_usa_io_vector("pR")
    ser.index = USA_INDUSTRY_INDEX
    return ser


def load_2012_qR_usa() -> pd.Series[float]:
    """
    Redefined commodity output vector
    unit is USD, original unit is million USD
    """
    ser = _load_usa_io_vector("qR") * MILLION_CURRENCY_TO_CURRENCY
    ser.index = USA_COMMODITY_INDEX
    return ser


def load_2012_YR_usa() -> pd.DataFrame:
    """
    Redefined Final Demand matrix in producer's price
    unit is USD, original unit is million USD
    """
    df = _load_usa_io_matrix("YR") * MILLION_CURRENCY_TO_CURRENCY
    df.index = USA_COMMODITY_INDEX
    df.columns = USA_FINAL_DEMAND_INDEX
    return df


@deprecated("CEDAv7 update")
def _load_usa_io_matrix(matrix_name: USA_IO_MATRIX_NAMES) -> pd.DataFrame:
    return _load_usa_xlsx(matrix_name)


@deprecated("CEDAv7 update")
def _load_usa_io_vector(vector_name: USA_IO_VECTOR_NAMES) -> pd.Series[float]:
    df = _load_usa_xlsx(vector_name)
    squeezed = df.squeeze()
    assert isinstance(
        squeezed, pd.Series
    ), f"Expected Series after squeeze, got {type(squeezed)}"
    ser = squeezed.astype(float)
    return ser


@deprecated("CEDAv7 update")
def _load_usa_xlsx(
    vector_name: USA_IO_VECTOR_NAMES | USA_IO_MATRIX_NAMES,
) -> pd.DataFrame:
    """
    load US data from CEDA6IO.xlsx available at
    https://docs.google.com/spreadsheets/d/1PREVLdN9k1LnXuJSmq-zJ5rhlmwvJbTm/edit?usp=drive_link&ouid=108994017865296281234&rtpof=true&sd=true
    """
    fname = "CEDA6IO.xlsx"
    pth = os.path.join(IN_DIR, fname)
    download_gcs_file_if_not_exists(posixpath.join(GCS_CEDA_V5_INPUT_DIR, fname), pth)

    df = pd.read_excel(
        pth,
        sheet_name=vector_name,
        header=None,
    ).astype(float)
    assert isinstance(df, pd.DataFrame), f"expected a DataFrame, got a {type(df)}"
    assert (
        len(df.shape) == 2
    ), f"expected a 2D DataFrame, got a {len(df.shape)}D DataFrame"
    return df
