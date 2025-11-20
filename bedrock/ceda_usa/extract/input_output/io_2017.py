from __future__ import annotations

import os

import pandas as pd
from typing_extensions import deprecated

from bedrock.ceda_usa.config.usa_config import get_usa_config
from bedrock.ceda_usa.extract.input_output.constants import GCS_USA_DIR
from bedrock.ceda_usa.utils.constants import (
    USA_2017_COMMODITY_CODES,
    USA_2017_DETAIL_IO_MATRIX_MAPPING,
    USA_2017_DETAIL_IO_MATRIX_NAMES,
    USA_2017_FINAL_DEMAND_CODES,
    USA_2017_INDUSTRY_CODES,
    USA_2017_SUMMARY_COMMODITY_CODES,
    USA_2017_SUMMARY_FINAL_DEMAND_CODES,
    USA_2017_SUMMARY_INDUSTRY_CODES,
    USA_SUMMARY_MUT_MAPPING_1997_2022,
    USA_SUMMARY_MUT_MAPPING_1997_2023,
    USA_SUMMARY_MUT_NAMES,
    USA_SUMMARY_MUT_YEARS,
)
from bedrock.ceda_usa.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_COMMODITY_INDEX,
    USA_2017_FINAL_DEMAND_INDEX,
    USA_2017_INDUSTRY_INDEX,
    USA_2017_SUMMARY_COMMODITY_INDEX,
    USA_2017_SUMMARY_FINAL_DEMAND_INDEX,
    USA_2017_SUMMARY_INDUSTRY_INDEX,
)
from bedrock.ceda_usa.utils.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.gcp import load_from_gcs

IN_DIR = os.path.join(os.path.dirname(__file__), "input_data")

# ----- Documentation ----- #
# MUTs (Detail and Summary, After Redefinitions) are downloaded from:
# https://apps.bea.gov/iTable/?isuri=1&reqid=151&step=1
#   > Make-Use
#     > All Tables
# Import matrices (Detail and Summary, After Redefinitions) are downloaded from:
# https://www.bea.gov/industry/input-output-accounts-data
#   > Supplemental Estimate Tables
#     > Requirements Tables
#       > After Redefinitions
#         > Import Matrices/After Redefinitions


def load_2017_V_usa() -> pd.DataFrame:
    """
    Make table, industry x commodity, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_2017_detail_make_use_usa("Make_detail")
        .loc[USA_2017_INDUSTRY_CODES, USA_2017_COMMODITY_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_INDUSTRY_INDEX
    df.columns = USA_2017_COMMODITY_INDEX
    return df


def load_2017_Utot_usa() -> pd.DataFrame:
    """
    Use table, commodity x industry, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_2017_detail_make_use_usa("Use_detail")
        .loc[USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX
    df.columns = USA_2017_INDUSTRY_INDEX

    return df


def load_2017_Uimp_usa() -> pd.DataFrame:
    """
    Import table, commodity x industry, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_2017_detail_make_use_usa("Import_detail")
        .loc[USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX
    df.columns = USA_2017_INDUSTRY_INDEX

    return df


def load_2017_Ytot_usa() -> pd.DataFrame:
    """
    Final Demand (total), commodity x final demand category, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = _load_2017_detail_make_use_usa("Use_detail")
    df = (
        df.loc[USA_2017_COMMODITY_CODES, USA_2017_FINAL_DEMAND_CODES].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX.copy()
    df.columns = USA_2017_FINAL_DEMAND_INDEX.copy()

    return df


def load_2017_Yimp_usa() -> pd.DataFrame:
    """
    Final Demand (from Import matrix), commodity x final demand category, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = _load_2017_detail_make_use_usa("Import_detail")
    df = (
        df.loc[USA_2017_COMMODITY_CODES, USA_2017_FINAL_DEMAND_CODES].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX.copy()
    df.columns = USA_2017_FINAL_DEMAND_INDEX.copy()

    return df


def _load_2017_detail_make_use_usa(
    matrix_name: USA_2017_DETAIL_IO_MATRIX_NAMES,
) -> pd.DataFrame:
    """
    Load 2017 USA Detail Make, Use and Import matrices
    """
    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_MATRIX_MAPPING[matrix_name],
            sub_bucket=GCS_USA_DIR,
            local_dir=IN_DIR,
            loader=lambda pth: pd.read_excel(
                pth,
                sheet_name="2017",
                skiprows=5,
                dtype={"Code": str},
            ),
        )
        .set_index("Code")
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f"expected a DataFrame, got a {type(df)}"
    assert (
        len(df.shape) == 2
    ), f"expected a 2D DataFrame, got a {len(df.shape)}D DataFrame"

    return df


def load_summary_V_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    """
    Make table, industry x commodity, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_usa_summary_mut("Make_summary", year)
        .loc[
            USA_2017_SUMMARY_INDUSTRY_CODES,
            USA_2017_SUMMARY_COMMODITY_CODES,
        ]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_SUMMARY_INDUSTRY_INDEX.copy()
    df.columns = USA_2017_SUMMARY_COMMODITY_INDEX.copy()
    return df


def load_summary_Utot_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    """
    Use table, commodity x industry, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_usa_summary_mut("Use_summary", year)
        .loc[
            USA_2017_SUMMARY_COMMODITY_CODES,
            USA_2017_SUMMARY_INDUSTRY_CODES,
        ]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_SUMMARY_COMMODITY_INDEX.copy()
    df.columns = USA_2017_SUMMARY_INDUSTRY_INDEX.copy()

    return df


def load_summary_Uimp_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    """
    Use table, commodity x industry, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_usa_summary_mut("Import_summary", year)
        .loc[
            USA_2017_SUMMARY_COMMODITY_CODES,
            USA_2017_SUMMARY_INDUSTRY_CODES,
        ]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_SUMMARY_COMMODITY_INDEX.copy()
    df.columns = USA_2017_SUMMARY_INDUSTRY_INDEX.copy()

    return df


def load_summary_Ytot_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    """
    Final demand, commodity x final demand category, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_usa_summary_mut("Use_summary", year)
        .loc[
            USA_2017_SUMMARY_INDUSTRY_CODES,  # use industry index instead of commodity index as hacky way to exclude Used and Other
            USA_2017_SUMMARY_FINAL_DEMAND_CODES,
        ]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = (
        USA_2017_SUMMARY_INDUSTRY_INDEX.copy()
    )  # use industry index instead of commodity index as hacky way to exclude Used and Other
    df.columns = USA_2017_SUMMARY_FINAL_DEMAND_INDEX.copy()

    return df


def load_summary_Yimp_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    """
    Final demand from imports, commodity x final demand category, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_usa_summary_mut("Import_summary", year)
        .loc[
            USA_2017_SUMMARY_INDUSTRY_CODES,  # use industry index instead of commodity index as hacky way to exclude Used and Other
            USA_2017_SUMMARY_FINAL_DEMAND_CODES,
        ]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = (
        USA_2017_SUMMARY_INDUSTRY_INDEX.copy()
    )  # use industry index instead of commodity index as hacky way to exclude Used and Other
    df.columns = USA_2017_SUMMARY_FINAL_DEMAND_INDEX.copy()

    return df


def _load_usa_summary_mut(
    matrix_name: USA_SUMMARY_MUT_NAMES, year: USA_SUMMARY_MUT_YEARS
) -> pd.DataFrame:
    """
    Load USA Summary SUT matrix
    """

    usa_summary_mut_mapping = (
        USA_SUMMARY_MUT_MAPPING_1997_2022
        if get_usa_config().usa_io_data_year == 2022
        else USA_SUMMARY_MUT_MAPPING_1997_2023
    )

    df = (
        load_from_gcs(
            name=usa_summary_mut_mapping[matrix_name],
            sub_bucket=GCS_USA_DIR,
            local_dir=IN_DIR,
            loader=lambda pth: pd.read_excel(
                pth,
                sheet_name=str(year),
                skiprows=5,
                dtype={"Unnamed: 0": str},
            ),
        )
        .set_index("Unnamed: 0")
        .replace("...", 0)
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f"expected a DataFrame, got a {type(df)}"
    assert (
        len(df.shape) == 2
    ), f"expected a 2D DataFrame, got a {len(df.shape)}D DataFrame"

    return df


@deprecated("Use load_detail_Ytot_usa instead, which reads from MUT.")
def load_2017_Ytot_sut_usa() -> pd.DataFrame:
    """
    Final Demand (total), commodity x final demand, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_2017_detail_sut_usa("Use_detail")
        .loc[USA_2017_COMMODITY_CODES, USA_2017_FINAL_DEMAND_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX
    df.columns = USA_2017_FINAL_DEMAND_INDEX

    return df


def _load_2017_detail_sut_usa(
    matrix_name: USA_2017_DETAIL_IO_MATRIX_NAMES,
) -> pd.DataFrame:
    """
    Load 2017 USA Detail SUT and import matrix
    """

    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_MATRIX_MAPPING[matrix_name],
            sub_bucket=GCS_USA_DIR,
            local_dir=IN_DIR,
            loader=lambda pth: pd.read_excel(
                pth,
                sheet_name="2017",
                skiprows=5,
                dtype={"Code": str},
            ),
        )
        .set_index("Code")
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f"expected a DataFrame, got a {type(df)}"
    assert (
        len(df.shape) == 2
    ), f"expected a 2D DataFrame, got a {len(df.shape)}D DataFrame"

    return df
