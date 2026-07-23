from __future__ import annotations

import functools
import warnings

import pandas as pd
from typing_extensions import deprecated

from bedrock.extract.iot.constants import (
    GCS_BEA_NIPA_IOT_BRIDGES_DIR,
    GCS_USA_MAKE_USE_DIR,
    GCS_USA_SUP_DIR,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.local_extract_input_data import local_dir_for_gcs_sub_bucket
from bedrock.utils.taxonomy.bea.matrix_mappings import (
    USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING,
    USA_2017_DETAIL_IO_MATRIX_MAPPING,
    USA_2017_DETAIL_IO_MATRIX_NAMES,
    USA_2017_DETAIL_IO_SUT_MATRIX_MAPPING,
    USA_2017_DETAIL_IO_SUT_MATRIX_NAMES,
    USA_SUMMARY_MUT_MAPPING_1997_2022,
    USA_SUMMARY_MUT_MAPPING_1997_2023,
    USA_SUMMARY_MUT_MAPPING_1997_2024,
    USA_SUMMARY_MUT_NAMES,
    USA_SUMMARY_MUT_YEARS,
    USA_SUMMARY_SUT_MAPPING_2017_2022,
    USA_SUMMARY_SUT_NAMES,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import (
    USA_2017_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    USA_2017_SUMMARY_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_final_demand import (
    USA_2017_FINAL_DEMAND_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry import (
    USA_2017_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_summary_final_demand import (
    USA_2017_SUMMARY_FINAL_DEMAND_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_value_added import (
    USA_2017_VALUE_ADDED_CODES,
)
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_COMMODITY_INDEX,
    USA_2017_FINAL_DEMAND_INDEX,
    USA_2017_INDUSTRY_INDEX,
    USA_2017_SUMMARY_COMMODITY_INDEX,
    USA_2017_SUMMARY_FINAL_DEMAND_INDEX,
    USA_2017_SUMMARY_INDUSTRY_INDEX,
    USA_2017_VALUE_ADDED_INDEX,
)

LOCAL_USA_MAKE_USE_DIR = local_dir_for_gcs_sub_bucket(GCS_USA_MAKE_USE_DIR)
LOCAL_USA_SUP_DIR = local_dir_for_gcs_sub_bucket(GCS_USA_SUP_DIR)
LOCAL_BEA_NIPA_IOT_BRIDGES_DIR = local_dir_for_gcs_sub_bucket(GCS_BEA_NIPA_IOT_BRIDGES_DIR)


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
#
# PCE Bridge (detail, 403 commodities) is downloaded from:
# https://apps.bea.gov/industry/release/xlsx/PCEBridge_Detail.xlsx
# PEQ Bridge (private equipment investment, detail) is downloaded from:
# https://apps.bea.gov/industry/release/xlsx/PEQBridge_Detail.xlsx
# Both bridge workbooks live together under GCS_BEA_NIPA_IOT_BRIDGES_DIR.
#
# ``load_2017_V_usa``, ``load_2017_Utot_usa``, and ``load_2017_Uimp_usa`` branch on
# ``USAConfig.iot_before_or_after_redefinition`` and are not cached. Pipelines that
# must always use after-redefinition BEA detail tables (e.g. CEDA mapping) should
# call ``load_*_after_redef_usa`` explicitly.


@functools.cache
def load_2017_V_after_redef_usa() -> pd.DataFrame:
    """
    Make table, industry x commodity, after redefinition, in producer price.
    unit is USD, original unit is million USD.
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


def load_2017_V_usa() -> pd.DataFrame:
    """2017 USA Make (V); before vs after BEA redefinitions from ``USAConfig``."""
    stage = get_usa_config().iot_before_or_after_redefinition
    if stage == "before":
        return load_2017_V_before_redef_usa()
    if stage == "after":
        return load_2017_V_after_redef_usa()
    raise ValueError(
        "Invalid iot_before_or_after_redefinition; expected 'before' or 'after'."
    )


@functools.cache
def load_2017_V_before_redef_usa() -> pd.DataFrame:
    """
    Make table, industry x commodity, before redefinition, in producer price.
    unit is USD, original unit is million USD.

    This table contains co-production (off-diagonal) entries that represent
    secondary products — i.e., commodities produced by industries other than
    the industry that primarily produces them.
    """
    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING[
                "Make_detail_before_redef"
            ],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name="2017", skiprows=5, dtype={"Code": str}
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

    df = (
        df.loc[USA_2017_INDUSTRY_CODES, USA_2017_COMMODITY_CODES].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_INDUSTRY_INDEX
    df.columns = USA_2017_COMMODITY_INDEX
    return df


@functools.cache
def load_2017_Utot_after_redef_usa() -> pd.DataFrame:
    """
    Use table, commodity x industry, after redefinition, in producer price.
    unit is USD, original unit is million USD.
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


def load_2017_Utot_usa() -> pd.DataFrame:
    """2017 USA total Use (Utot); before vs after BEA redefinitions from ``USAConfig``."""
    stage = get_usa_config().iot_before_or_after_redefinition
    if stage == "before":
        return load_2017_Utot_before_redef_usa()
    if stage == "after":
        return load_2017_Utot_after_redef_usa()
    raise ValueError(
        "Invalid iot_before_or_after_redefinition; expected 'before' or 'after'."
    )


@functools.cache
def load_2017_Utot_before_redef_usa() -> pd.DataFrame:
    """
    Use table, commodity x industry, before redefinition, in producer price.
    unit is USD, original unit is million USD.
    """
    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING[
                "Use_detail_before_redef"
            ],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name="2017", skiprows=5, dtype={"Code": str}
            ),
        )
        .set_index("Code")
        .fillna(0)
    )
    df.columns = df.columns.astype(str)
    df = (
        df.loc[USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX
    df.columns = USA_2017_INDUSTRY_INDEX
    return df


@functools.cache
def load_2017_Uimp_after_redef_usa() -> pd.DataFrame:
    """
    Import table, commodity x industry, after redefinition, in producer price.
    unit is USD, original unit is million USD.
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


def load_2017_Uimp_usa() -> pd.DataFrame:
    """2017 USA import Use (Uimp); before vs after BEA redefinitions from ``USAConfig``."""
    stage = get_usa_config().iot_before_or_after_redefinition
    if stage == "before":
        return load_2017_Uimp_before_redef_usa()
    if stage == "after":
        return load_2017_Uimp_after_redef_usa()
    raise ValueError(
        "Invalid iot_before_or_after_redefinition; expected 'before' or 'after'."
    )


@functools.cache
def load_2017_Uimp_before_redef_usa() -> pd.DataFrame:
    """
    Import table, commodity x industry, before redefinition, in producer price.
    unit is USD, original unit is million USD.
    """
    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING[
                "Import_detail_before_redef"
            ],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name="2017", skiprows=5, dtype={"Code": str}
            ),
        )
        .set_index("Code")
        .fillna(0)
    )
    df.columns = df.columns.astype(str)
    df = (
        df.loc[USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX
    df.columns = USA_2017_INDUSTRY_INDEX
    return df


_MARGINS_COLUMNS = [
    "Industry Code",
    "Industry Description",
    "Commodity Code",
    "Commodity Description",
    "Producers' Value",
    "Transportation",
    "Wholesale",
    "Retail",
    "Purchasers' Value",
]
_MARGINS_VALUE_COLUMNS = [
    "Producers' Value",
    "Transportation",
    "Wholesale",
    "Retail",
    "Purchasers' Value",
]


def load_2017_margins_usa() -> pd.DataFrame:
    """2017 Margins before vs after BEA redefinitions from ``USAConfig``."""
    stage = get_usa_config().iot_before_or_after_redefinition
    if stage == "before":
        return load_2017_margins_before_redef_usa()
    if stage == "after":
        return load_2017_margins_after_redef_usa()
    raise ValueError(
        "Invalid iot_before_or_after_redefinition; expected 'before' or 'after'."
    )


def _load_margins_excel(pth: str) -> pd.DataFrame:
    """Read the Margins Excel file, suppressing the openpyxl header/footer warning."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Cannot parse header or footer so it will be ignored",
            category=UserWarning,
        )
        return pd.read_excel(
            pth,
            sheet_name="2017",
            skiprows=5,
            header=None,
            names=_MARGINS_COLUMNS,
            dtype={"Industry Code": str, "Commodity Code": str},
        )


def _load_2017_margins_from_file(filename: str) -> pd.DataFrame:
    """Shared loader for margins tables; applies index filtering and unit scaling."""
    df = load_from_gcs(
        name=filename,
        sub_bucket=GCS_USA_MAKE_USE_DIR,
        local_dir=LOCAL_USA_MAKE_USE_DIR,
        loader=_load_margins_excel,
    ).set_index(["Industry Code", "Commodity Code"])
    valid_industry = set(USA_2017_INDUSTRY_CODES) | set(USA_2017_FINAL_DEMAND_CODES)
    valid_commodity = set(USA_2017_COMMODITY_CODES) | set(USA_2017_VALUE_ADDED_CODES)
    mask = df.index.get_level_values("Industry Code").isin(
        valid_industry
    ) & df.index.get_level_values("Commodity Code").isin(valid_commodity)
    return (
        df.loc[mask, _MARGINS_VALUE_COLUMNS].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )


@functools.cache
def load_2017_margins_after_redef_usa() -> pd.DataFrame:
    """
    Margins table, (industry, commodity) x margin type, after redefinition, in producer price.
    Columns: Producers' Value, Transportation, Wholesale, Retail, Purchasers' Value.
    unit is USD, original unit is million USD.
    """
    return _load_2017_margins_from_file(USA_2017_DETAIL_IO_MATRIX_MAPPING["Margins"])


@functools.cache
def load_2017_margins_before_redef_usa() -> pd.DataFrame:
    """
    Margins table, (industry, commodity) x margin type, before redefinition, in producer price.
    Columns: Producers' Value, Transportation, Wholesale, Retail, Purchasers' Value.
    unit is USD, original unit is million USD.
    """
    return _load_2017_margins_from_file(
        USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING["Margins"]
    )


_PCE_BRIDGE_DETAIL_COLUMNS = [
    "NIPA Line",
    "PCE Category",
    "Commodity Code",
    "Commodity Description",
    "Producers' Value",
    "Transportation",
    "Wholesale",
    "Retail",
    "Purchasers' Value",
    "Year",
]
_PCE_BRIDGE_DETAIL_VALUE_COLUMNS = [
    "Producers' Value",
    "Transportation",
    "Wholesale",
    "Retail",
    "Purchasers' Value",
]


def _load_pce_bridge_detail_excel(pth: str) -> pd.DataFrame:
    """Read the PCE Bridge Detail Excel file, suppressing the openpyxl header/footer warning."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Cannot parse header or footer so it will be ignored",
            category=UserWarning,
        )
        return pd.read_excel(
            pth,
            sheet_name="2017",
            skiprows=5,
            header=None,
            names=_PCE_BRIDGE_DETAIL_COLUMNS,
            dtype={"Commodity Code": str},
        )


@functools.cache
def _load_pce_bridge_detail_raw_usa() -> pd.DataFrame:
    """
    PCE Bridge table (detail, 403 commodities), long format, as published: one
    row per (NIPA PCE line, commodity) pair, after redefinition, in producer
    price. Columns: NIPA Line, PCE Category, Commodity Code, Commodity
    Description, Producers' Value, Transportation Costs, Wholesale, Retail,
    Purchasers' Value, Year. unit is million USD, matching the source file.
    """
    df = load_from_gcs(
        name="PCEBridge_Detail.xlsx",
        sub_bucket=GCS_BEA_NIPA_IOT_BRIDGES_DIR,
        local_dir=LOCAL_BEA_NIPA_IOT_BRIDGES_DIR,
        loader=_load_pce_bridge_detail_excel,
    )
    assert set(df["Commodity Code"]).issubset(USA_2017_COMMODITY_CODES), (
        "PCE Bridge Detail has commodity codes outside the 2017 taxonomy: "
        f"{set(df['Commodity Code']) - set(USA_2017_COMMODITY_CODES)}"
    )
    return df


def load_2017_pce_bridge_detail_usa() -> pd.DataFrame:
    """
    PCE Bridge table (detail, 403 commodities); see `_load_pce_bridge_detail_raw_usa`
    for column layout. unit is USD, original unit is million USD.
    """
    df = _load_pce_bridge_detail_raw_usa().copy()
    df[_PCE_BRIDGE_DETAIL_VALUE_COLUMNS] = (
        df[_PCE_BRIDGE_DETAIL_VALUE_COLUMNS].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    return df


# PEQ Bridge shares the exact same layout as PCE Bridge (skiprows=5, same 10 columns
# in the same order - BEA reuses "PCE Category" as the category-column header even
# though the values are equipment categories, e.g. "Computers and peripheral
# equipment"), so the same column lists/parsing logic apply unchanged.
_PEQ_BRIDGE_DETAIL_COLUMNS = _PCE_BRIDGE_DETAIL_COLUMNS
_PEQ_BRIDGE_DETAIL_VALUE_COLUMNS = _PCE_BRIDGE_DETAIL_VALUE_COLUMNS


def _load_peq_bridge_detail_excel(pth: str) -> pd.DataFrame:
    """Read the PEQ Bridge Detail Excel file, suppressing the openpyxl header/footer warning."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Cannot parse header or footer so it will be ignored",
            category=UserWarning,
        )
        return pd.read_excel(
            pth,
            sheet_name="2017",
            skiprows=5,
            header=None,
            names=_PEQ_BRIDGE_DETAIL_COLUMNS,
            dtype={"Commodity Code": str},
        )


@functools.cache
def _load_peq_bridge_detail_raw_usa() -> pd.DataFrame:
    """
    PEQ Bridge table (private equipment investment, detail), long format, as
    published: one row per (NIPA equipment line, commodity) pair, after
    redefinition, in producer price. Columns: NIPA Line, PCE Category (equipment
    category, e.g. "Autos"), Commodity Code, Commodity Description, Producers'
    Value, Transportation Costs, Wholesale, Retail, Purchasers' Value, Year.
    unit is million USD, matching the source file.
    """
    df = load_from_gcs(
        name="PEQBridge_Detail.xlsx",
        sub_bucket=GCS_BEA_NIPA_IOT_BRIDGES_DIR,
        local_dir=LOCAL_BEA_NIPA_IOT_BRIDGES_DIR,
        loader=_load_peq_bridge_detail_excel,
    )
    assert set(df["Commodity Code"]).issubset(USA_2017_COMMODITY_CODES), (
        "PEQ Bridge Detail has commodity codes outside the 2017 taxonomy: "
        f"{set(df['Commodity Code']) - set(USA_2017_COMMODITY_CODES)}"
    )
    return df


def load_2017_peq_bridge_detail_usa() -> pd.DataFrame:
    """
    PEQ Bridge table (private equipment investment, detail); see
    `_load_peq_bridge_detail_raw_usa` for column layout. unit is USD, original
    unit is million USD.
    """
    df = _load_peq_bridge_detail_raw_usa().copy()
    df[_PEQ_BRIDGE_DETAIL_VALUE_COLUMNS] = (
        df[_PEQ_BRIDGE_DETAIL_VALUE_COLUMNS].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
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


def load_2017_value_added_usa() -> pd.DataFrame:
    """
    Value added (total), VA category x industry, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = _load_2017_detail_make_use_usa("Use_detail")
    df = (
        df.loc[USA_2017_VALUE_ADDED_CODES, USA_2017_INDUSTRY_CODES].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_VALUE_ADDED_INDEX.copy()
    df.columns = USA_2017_INDUSTRY_INDEX.copy()

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
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name="2017", skiprows=5, dtype={"Code": str}
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


def _load_2017_detail_supply_use_usa(
    matrix_name: USA_2017_DETAIL_IO_SUT_MATRIX_NAMES,
) -> pd.DataFrame:
    """
    Load 2017 USA Detail Supply and Use_SUT matrices
    """
    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_SUT_MATRIX_MAPPING[matrix_name],
            sub_bucket=GCS_USA_SUP_DIR,
            local_dir=LOCAL_USA_SUP_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name="2017", skiprows=5, dtype={"Code": str}
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


@functools.cache
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


@functools.cache
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


@functools.cache
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


@functools.cache
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


@functools.cache
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

    # BEA revises historical data in each new release. We pin older years to the
    # oldest file containing them so values stay stable across releases (e.g.
    # scale_cornerstone_B uses years 2017 and 2022, which must not change as new
    # vintages add years on the right).
    if year > 2023:
        mapping = USA_SUMMARY_MUT_MAPPING_1997_2024
    elif year > 2022:
        mapping = USA_SUMMARY_MUT_MAPPING_1997_2023
    else:
        mapping = USA_SUMMARY_MUT_MAPPING_1997_2022
    df = (
        load_from_gcs(
            name=mapping[matrix_name],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
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


def _load_usa_summary_sut(
    matrix_name: USA_SUMMARY_SUT_NAMES, year: USA_SUMMARY_MUT_YEARS
) -> pd.DataFrame:
    """
    Load USA Summary tables in Supply-use format
    """
    df = (
        load_from_gcs(
            name=USA_SUMMARY_SUT_MAPPING_2017_2022[matrix_name],
            sub_bucket=GCS_USA_SUP_DIR,
            local_dir=LOCAL_USA_SUP_DIR,
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
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name="2017", skiprows=5, dtype={"Code": str}
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
