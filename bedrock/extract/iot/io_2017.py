from __future__ import annotations

import functools
import typing as ta
import warnings
import zipfile

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
    USA_BENCHMARK_DETAIL_SUT_ARCHIVE,
    USA_BENCHMARK_DETAIL_SUT_MEMBER_MAPPING,
    USA_BENCHMARK_DETAIL_SUT_YEARS,
    USA_SUMMARY_MUT_BEFORE_REDEF_MAPPING,
    USA_SUMMARY_MUT_BEFORE_REDEF_NAMES,
    USA_SUMMARY_MUT_MAPPING_1997_2022,
    USA_SUMMARY_MUT_MAPPING_1997_2023,
    USA_SUMMARY_MUT_MAPPING_1997_2024,
    USA_SUMMARY_MUT_NAMES,
    USA_SUMMARY_MUT_YEARS,
    USA_SUMMARY_SPAN_MUT_YEARS,
    USA_SUMMARY_SUT_MAPPING_1997_2024,
    USA_SUMMARY_SUT_MAPPING_2017_2022,
    USA_SUMMARY_SUT_NAMES,
    USA_SUMMARY_SUT_YEARS,
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
    SUMMARY_VA_CODES,
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
LOCAL_BEA_NIPA_IOT_BRIDGES_DIR = local_dir_for_gcs_sub_bucket(
    GCS_BEA_NIPA_IOT_BRIDGES_DIR
)


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
        _load_2017_detail_make_use_usa('Make_detail')
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
    if stage == 'before':
        return load_2017_V_before_redef_usa()
    if stage == 'after':
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
                'Make_detail_before_redef'
            ],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
            ),
        )
        .set_index('Code')
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f'expected a DataFrame, got a {type(df)}'
    assert (
        len(df.shape) == 2
    ), f'expected a 2D DataFrame, got a {len(df.shape)}D DataFrame'

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
        _load_2017_detail_make_use_usa('Use_detail')
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
    if stage == 'before':
        return load_2017_Utot_before_redef_usa()
    if stage == 'after':
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
                'Use_detail_before_redef'
            ],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
            ),
        )
        .set_index('Code')
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
        _load_2017_detail_make_use_usa('Import_detail')
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
    if stage == 'before':
        return load_2017_Uimp_before_redef_usa()
    if stage == 'after':
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
                'Import_detail_before_redef'
            ],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
            ),
        )
        .set_index('Code')
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
    'Industry Code',
    'Industry Description',
    'Commodity Code',
    'Commodity Description',
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]
_MARGINS_VALUE_COLUMNS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def load_2017_margins_usa() -> pd.DataFrame:
    """2017 Margins before vs after BEA redefinitions from ``USAConfig``."""
    stage = get_usa_config().iot_before_or_after_redefinition
    if stage == 'before':
        return load_2017_margins_before_redef_usa()
    if stage == 'after':
        return load_2017_margins_after_redef_usa()
    raise ValueError(
        "Invalid iot_before_or_after_redefinition; expected 'before' or 'after'."
    )


def _load_margins_excel(pth: str) -> pd.DataFrame:
    """Read the Margins Excel file, suppressing the openpyxl header/footer warning."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore',
            message='Cannot parse header or footer so it will be ignored',
            category=UserWarning,
        )
        return pd.read_excel(
            pth,
            sheet_name='2017',
            skiprows=5,
            header=None,
            names=_MARGINS_COLUMNS,
            dtype={'Industry Code': str, 'Commodity Code': str},
        )


def _load_2017_margins_from_file(filename: str) -> pd.DataFrame:
    """Shared loader for margins tables; applies index filtering and unit scaling."""
    df = load_from_gcs(
        name=filename,
        sub_bucket=GCS_USA_MAKE_USE_DIR,
        local_dir=LOCAL_USA_MAKE_USE_DIR,
        loader=_load_margins_excel,
    ).set_index(['Industry Code', 'Commodity Code'])
    valid_industry = set(USA_2017_INDUSTRY_CODES) | set(USA_2017_FINAL_DEMAND_CODES)
    valid_commodity = set(USA_2017_COMMODITY_CODES) | set(USA_2017_VALUE_ADDED_CODES)
    mask = df.index.get_level_values('Industry Code').isin(
        valid_industry
    ) & df.index.get_level_values('Commodity Code').isin(valid_commodity)
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
    return _load_2017_margins_from_file(USA_2017_DETAIL_IO_MATRIX_MAPPING['Margins'])


@functools.cache
def load_2017_margins_before_redef_usa() -> pd.DataFrame:
    """
    Margins table, (industry, commodity) x margin type, before redefinition, in producer price.
    Columns: Producers' Value, Transportation, Wholesale, Retail, Purchasers' Value.
    unit is USD, original unit is million USD.
    """
    return _load_2017_margins_from_file(
        USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING['Margins']
    )


_PCE_BRIDGE_DETAIL_COLUMNS = [
    'NIPA Line',
    'PCE Category',
    'Commodity Code',
    'Commodity Description',
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
    'Year',
]
_PCE_BRIDGE_DETAIL_VALUE_COLUMNS = [
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
]


def _load_pce_bridge_detail_excel(pth: str) -> pd.DataFrame:
    """Read the PCE Bridge Detail Excel file, suppressing the openpyxl header/footer warning."""
    with warnings.catch_warnings():
        warnings.filterwarnings(
            'ignore',
            message='Cannot parse header or footer so it will be ignored',
            category=UserWarning,
        )
        return pd.read_excel(
            pth,
            sheet_name='2017',
            skiprows=5,
            header=None,
            names=_PCE_BRIDGE_DETAIL_COLUMNS,
            dtype={'Commodity Code': str},
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
        name='PCEBridge_Detail.xlsx',
        sub_bucket=GCS_BEA_NIPA_IOT_BRIDGES_DIR,
        local_dir=LOCAL_BEA_NIPA_IOT_BRIDGES_DIR,
        loader=_load_pce_bridge_detail_excel,
    )
    assert set(df['Commodity Code']).issubset(USA_2017_COMMODITY_CODES), (
        'PCE Bridge Detail has commodity codes outside the 2017 taxonomy: '
        f'{set(df["Commodity Code"]) - set(USA_2017_COMMODITY_CODES)}'
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
            'ignore',
            message='Cannot parse header or footer so it will be ignored',
            category=UserWarning,
        )
        return pd.read_excel(
            pth,
            sheet_name='2017',
            skiprows=5,
            header=None,
            names=_PEQ_BRIDGE_DETAIL_COLUMNS,
            dtype={'Commodity Code': str},
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
        name='PEQBridge_Detail.xlsx',
        sub_bucket=GCS_BEA_NIPA_IOT_BRIDGES_DIR,
        local_dir=LOCAL_BEA_NIPA_IOT_BRIDGES_DIR,
        loader=_load_peq_bridge_detail_excel,
    )
    assert set(df['Commodity Code']).issubset(USA_2017_COMMODITY_CODES), (
        'PEQ Bridge Detail has commodity codes outside the 2017 taxonomy: '
        f'{set(df["Commodity Code"]) - set(USA_2017_COMMODITY_CODES)}'
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
    df = _load_2017_detail_make_use_usa('Use_detail')
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
    df = _load_2017_detail_make_use_usa('Use_detail')
    df = (
        df.loc[USA_2017_VALUE_ADDED_CODES, USA_2017_INDUSTRY_CODES].astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_VALUE_ADDED_INDEX.copy()
    df.columns = USA_2017_INDUSTRY_INDEX.copy()

    return df


@functools.cache
def load_2017_value_added_before_redef_usa() -> pd.DataFrame:
    """
    Value added, VA category x industry, before redefinition, in producer price.
    unit is USD, original unit is million USD.

    Slices ``V00100`` / ``V00200`` / ``V00300`` from the same before-redef Use
    workbook ``load_2017_Utot_before_redef_usa`` reads. Those three rows must
    exist in the file; this loader does not switch
    ``load_2017_value_added_usa``.
    """
    df = (
        load_from_gcs(
            name=USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING[
                'Use_detail_before_redef'
            ],
            sub_bucket=GCS_USA_MAKE_USE_DIR,
            local_dir=LOCAL_USA_MAKE_USE_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
            ),
        )
        .set_index('Code')
        .fillna(0)
    )
    df.columns = df.columns.astype(str)
    missing = [code for code in USA_2017_VALUE_ADDED_CODES if code not in df.index]
    assert (
        not missing
    ), f'before-redef Use workbook is missing value-added rows: {missing}'
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
    df = _load_2017_detail_make_use_usa('Import_detail')
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
                pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
            ),
        )
        .set_index('Code')
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f'expected a DataFrame, got a {type(df)}'
    assert (
        len(df.shape) == 2
    ), f'expected a 2D DataFrame, got a {len(df.shape)}D DataFrame'

    return df


def _assert_bea_subsidy_signs(
    df: pd.DataFrame, matrix_name: USA_2017_DETAIL_IO_SUT_MATRIX_NAMES
) -> None:
    """Check BEA's subsidy sign convention has not changed under us.

    BEA publishes subsidies with **opposite signs on the two tables**: the Use
    table's ``T00SUB`` row is stored positive and the Supply table's ``SUB``
    column negative, both totalling 59,876 in 2017. Anything consuming both
    tables has to reconcile that, and a producer-price industry column margin
    is wrong by ``2 x T00SUB`` if it goes unnoticed - up to 38,943 on a single
    industry.

    This asserts the published convention rather than changing it, because the
    frame this returns is also what ``bea_parse`` emits as the
    ``BEA_Detail_Use_SUT`` / ``BEA_Detail_Supply`` FBAs, which must stay
    faithful to the workbook. Normalisation to one convention belongs at SUT
    panel assembly, where
    ``bedrock.utils.economic.balance.mask.assert_subsidies_negative`` checks it
    once for the balance.
    """
    if matrix_name == 'Use_SUT_detail' and 'T00SUB' in df.index:
        row = df.loc['T00SUB']
        if isinstance(row, pd.DataFrame):
            raise AssertionError(
                f'BEA Use table has {len(row)} T00SUB rows; expected exactly one'
            )
        values = pd.to_numeric(row, errors='coerce').dropna()
        if (values < 0).any():
            raise AssertionError(
                f'BEA Use T00SUB is stored positive; found '
                f'{int((values < 0).sum())} negative cells. The subsidy sign '
                f'convention has changed - see the balance layer, which '
                f'assumes the Use row needs negating and the Supply column '
                f'does not'
            )
    if matrix_name == 'Supply_detail' and 'SUB' in df.columns:
        values = pd.to_numeric(df['SUB'], errors='coerce').dropna()
        if (values > 0).any():
            raise AssertionError(
                f'BEA Supply SUB is stored negative; found '
                f'{int((values > 0).sum())} positive cells. The subsidy sign '
                f'convention has changed - see the balance layer'
            )


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
                pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
            ),
        )
        .set_index('Code')
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f'expected a DataFrame, got a {type(df)}'
    assert (
        len(df.shape) == 2
    ), f'expected a 2D DataFrame, got a {len(df.shape)}D DataFrame'

    _assert_bea_subsidy_signs(df, matrix_name)

    return df


@functools.cache
def _load_benchmark_detail_supply_use_usa(
    matrix_name: USA_2017_DETAIL_IO_SUT_MATRIX_NAMES,
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> pd.DataFrame:
    """
    Load a USA Detail Supply or Use_SUT matrix for a *benchmark* year.

    ``_load_2017_detail_supply_use_usa`` reads the single-year workbooks and can
    only ever return 2017.  BEA also publishes the three benchmark years as one
    zip -- ``Supply_Detail.xlsx`` and ``Use_SUT_Detail.xlsx``, each with a sheet
    per year -- and **all three sheets are already on the 2017 code basis in one
    413 x 424 (Use) / 405 x 415 (Supply) frame**, so 2007, 2012 and 2017 can be
    differenced against each other without a crosswalk.

    The 2017 sheets are **identical, cell for cell**, to the single-year
    workbooks; :func:`assert_benchmark_panel_matches_2017` checks that.  The two
    loaders are kept separate anyway because ``_load_2017_detail_supply_use_usa``
    is what ``bea_parse`` emits as the ``BEA_Detail_Use_SUT`` /
    ``BEA_Detail_Supply`` FBAs, which are pinned to their published workbook.

    Returns the frame as published: ``Code`` index, description column kept,
    million USD, purchaser value, before redefinitions.
    """
    member = USA_BENCHMARK_DETAIL_SUT_MEMBER_MAPPING[matrix_name]

    def _read_member(pth: str) -> pd.DataFrame:
        with (
            zipfile.ZipFile(pth) as bundle,
            bundle.open(member) as sheet,
        ):
            return pd.read_excel(
                sheet, sheet_name=str(year), skiprows=5, dtype={'Code': str}
            )

    df = (
        load_from_gcs(
            name=USA_BENCHMARK_DETAIL_SUT_ARCHIVE,
            sub_bucket=GCS_USA_SUP_DIR,
            local_dir=LOCAL_USA_SUP_DIR,
            loader=_read_member,
        )
        .set_index('Code')
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f'expected a DataFrame, got a {type(df)}'
    assert (
        len(df.shape) == 2
    ), f'expected a 2D DataFrame, got a {len(df.shape)}D DataFrame'

    _assert_bea_subsidy_signs(df, matrix_name)

    return df


@functools.cache
def load_benchmark_detail_U_intermediate_usa(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> pd.DataFrame:
    """
    Intermediate block of the benchmark detail Use SUT, commodity x industry.

    Purchaser value, before redefinitions, unit is USD, original unit is
    million USD.  This is the interior only -- no final demand columns and no
    value-added rows.
    """
    df = (
        _load_benchmark_detail_supply_use_usa('Use_SUT_detail', year)
        .loc[USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX.copy()
    df.columns = USA_2017_INDUSTRY_INDEX.copy()
    return df


@functools.cache
def load_benchmark_detail_supply_usa(
    year: USA_BENCHMARK_DETAIL_SUT_YEARS,
) -> pd.DataFrame:
    """
    Production block of the benchmark detail Supply table, commodity x industry.

    Basic value, before redefinitions, unit is USD, original unit is million
    USD.  The valuation columns BEA carries beside it -- ``T013`` basic,
    ``T014`` margins, ``T015`` net taxes, ``T016`` purchaser -- are dropped
    here; read them off :func:`_load_benchmark_detail_supply_use_usa`.
    """
    df = (
        _load_benchmark_detail_supply_use_usa('Supply_detail', year)
        .loc[USA_2017_COMMODITY_CODES, USA_2017_INDUSTRY_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = USA_2017_COMMODITY_INDEX.copy()
    df.columns = USA_2017_INDUSTRY_INDEX.copy()
    return df


def assert_benchmark_panel_matches_2017() -> None:
    """Check the panel's 2017 sheets against the single-year workbooks.

    The two are separate GCS objects that BEA published separately, and the
    benchmark panel is only usable as a second and third observation of the
    single-year 2017 table if its 2017 sheet *is* that table.  Today they agree
    on every cell of both matrices; this raises if a re-release breaks that.
    """
    for matrix_name in ta.get_args(USA_2017_DETAIL_IO_SUT_MATRIX_NAMES):
        single = _load_2017_detail_supply_use_usa(matrix_name)
        panel = _load_benchmark_detail_supply_use_usa(matrix_name, 2017)
        if list(single.index) != list(panel.index):
            raise AssertionError(
                f'{matrix_name}: benchmark panel 2017 rows differ from the '
                f'single-year workbook'
            )
        if list(single.columns) != list(panel.columns):
            raise AssertionError(
                f'{matrix_name}: benchmark panel 2017 columns differ from the '
                f'single-year workbook'
            )
        left = single.apply(pd.to_numeric, errors='coerce')
        right = panel.apply(pd.to_numeric, errors='coerce')
        gap = (left - right).abs().max().max()
        if not (pd.isna(gap) or gap == 0):
            raise AssertionError(
                f'{matrix_name}: benchmark panel 2017 differs from the '
                f'single-year workbook by up to {gap} million USD'
            )


@functools.cache
def load_summary_V_usa(year: USA_SUMMARY_MUT_YEARS) -> pd.DataFrame:
    """
    Make table, industry x commodity, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_usa_summary_mut('Make_summary', year)
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
        _load_usa_summary_mut('Use_summary', year)
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
        _load_usa_summary_mut('Import_summary', year)
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
        _load_usa_summary_mut('Use_summary', year)
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
        _load_usa_summary_mut('Import_summary', year)
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


def _require_summary_span_year(year: int | str) -> int:
    """Coerce and validate years for before-redef / 2024-vintage span loaders."""
    year_int = int(year)
    if year_int < 2017 or year_int > 2024:
        raise ValueError(f'year {year_int} out of span domain 2017–2024')
    return year_int


def _assert_summary_va_rows(df: pd.DataFrame, *, context: str) -> None:
    missing = [code for code in SUMMARY_VA_CODES if code not in df.index]
    if missing:
        raise ValueError(f'{context}: missing VA rows {missing}')


def _load_usa_summary_mut_from_mapping(
    mapping: ta.Mapping[str, str],
    matrix_name: str,
    year: int,
) -> pd.DataFrame:
    """Load one sheet from a summary MUT workbook dict (million USD, raw)."""
    filename = mapping[matrix_name]
    try:
        df = (
            load_from_gcs(
                name=filename,
                sub_bucket=GCS_USA_MAKE_USE_DIR,
                local_dir=LOCAL_USA_MAKE_USE_DIR,
                loader=lambda pth: pd.read_excel(
                    pth,
                    sheet_name=str(year),
                    skiprows=5,
                    dtype={'Unnamed: 0': str},
                ),
            )
            .set_index('Unnamed: 0')
            .replace('...', 0)
            .fillna(0)
        )
    except Exception as exc:
        raise RuntimeError(
            f'failed loading summary MUT year={year} file={filename}: {exc}'
        ) from exc
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f'expected a DataFrame, got a {type(df)}'
    assert (
        len(df.shape) == 2
    ), f'expected a 2D DataFrame, got a {len(df.shape)}D DataFrame'

    return df


def _load_usa_summary_mut(
    matrix_name: USA_SUMMARY_MUT_NAMES, year: USA_SUMMARY_MUT_YEARS
) -> pd.DataFrame:
    """
    Load USA Summary SUT matrix
    """

    # BEA revises historical data in each new release. We pin older years to the
    # oldest file containing them so values stay stable across releases (e.g.
    # scale_cornerstone_A uses years 2017 and 2022, which must not change as new
    # vintages add years on the right).
    # year arrives as a str from the FBA generation path, and typing.cast at the
    # call site is a no-op at runtime. Coerce before the vintage comparisons: they
    # raise on a str today, but sheet_name=str(year) accepts either type, so if
    # they ever moved a str year would silently select the wrong vintage mapping.
    year_int = int(year)
    if year_int > 2023:
        mapping = USA_SUMMARY_MUT_MAPPING_1997_2024
    elif year_int > 2022:
        mapping = USA_SUMMARY_MUT_MAPPING_1997_2023
    else:
        mapping = USA_SUMMARY_MUT_MAPPING_1997_2022
    return _load_usa_summary_mut_from_mapping(mapping, matrix_name, year_int)


def _load_usa_summary_mut_before_redef(
    matrix_name: USA_SUMMARY_MUT_BEFORE_REDEF_NAMES,
    year: USA_SUMMARY_SPAN_MUT_YEARS,
) -> pd.DataFrame:
    """Load before-redef summary MUT sheet (million USD, raw)."""
    year_int = _require_summary_span_year(year)
    return _load_usa_summary_mut_from_mapping(
        USA_SUMMARY_MUT_BEFORE_REDEF_MAPPING, matrix_name, year_int
    )


@functools.cache
def load_summary_V_before_redef_usa(year: USA_SUMMARY_SPAN_MUT_YEARS) -> pd.DataFrame:
    """Make table before redefinitions, industry x commodity, USD."""
    df = (
        _load_usa_summary_mut_before_redef('Make_summary_before_redef', year)
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
def load_summary_Utot_before_redef_usa(
    year: USA_SUMMARY_SPAN_MUT_YEARS,
) -> pd.DataFrame:
    """Use intermediate before redefinitions, commodity x industry, USD."""
    df = (
        _load_usa_summary_mut_before_redef('Use_summary_before_redef', year)
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
def load_summary_Uimp_before_redef_usa(
    year: USA_SUMMARY_SPAN_MUT_YEARS,
) -> pd.DataFrame:
    """Import matrix before redefinitions, commodity x industry, USD."""
    df = (
        _load_usa_summary_mut_before_redef('Import_summary_before_redef', year)
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
def load_summary_value_added_before_redef_usa(
    year: USA_SUMMARY_SPAN_MUT_YEARS,
) -> pd.DataFrame:
    """Value added before redefinitions, V001/V002/V003 x summary industry, USD."""
    raw = _load_usa_summary_mut_before_redef('Use_summary_before_redef', year)
    _assert_summary_va_rows(raw, context='before-redef summary Use')
    df = (
        raw.loc[list(SUMMARY_VA_CODES), USA_2017_SUMMARY_INDUSTRY_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = pd.Index(list(SUMMARY_VA_CODES), name='value_added')
    df.columns = USA_2017_SUMMARY_INDUSTRY_INDEX.copy()
    return df


@functools.cache
def load_summary_V_usa_2024_vintage(year: USA_SUMMARY_SPAN_MUT_YEARS) -> pd.DataFrame:
    """Make after redefinitions from the 1997–2024-named workbook, USD."""
    year_int = _require_summary_span_year(year)
    df = (
        _load_usa_summary_mut_from_mapping(
            USA_SUMMARY_MUT_MAPPING_1997_2024, 'Make_summary', year_int
        )
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
def load_summary_Utot_usa_2024_vintage(
    year: USA_SUMMARY_SPAN_MUT_YEARS,
) -> pd.DataFrame:
    """Use intermediate after redefinitions from the 1997–2024-named workbook, USD."""
    year_int = _require_summary_span_year(year)
    df = (
        _load_usa_summary_mut_from_mapping(
            USA_SUMMARY_MUT_MAPPING_1997_2024, 'Use_summary', year_int
        )
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
def load_summary_Uimp_usa_2024_vintage(
    year: USA_SUMMARY_SPAN_MUT_YEARS,
) -> pd.DataFrame:
    """Import matrix after redefinitions from the 1997–2024-named workbook, USD."""
    year_int = _require_summary_span_year(year)
    df = (
        _load_usa_summary_mut_from_mapping(
            USA_SUMMARY_MUT_MAPPING_1997_2024, 'Import_summary', year_int
        )
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
def load_summary_value_added_usa_2024_vintage(
    year: USA_SUMMARY_SPAN_MUT_YEARS,
) -> pd.DataFrame:
    """VA after redefinitions from the 1997–2024 Use workbook, USD."""
    year_int = _require_summary_span_year(year)
    raw = _load_usa_summary_mut_from_mapping(
        USA_SUMMARY_MUT_MAPPING_1997_2024, 'Use_summary', year_int
    )
    _assert_summary_va_rows(raw, context='2024-vintage after-redef summary Use')
    df = (
        raw.loc[list(SUMMARY_VA_CODES), USA_2017_SUMMARY_INDUSTRY_CODES]
        .astype(float)
        * MILLION_CURRENCY_TO_CURRENCY
    )
    df.index = pd.Index(list(SUMMARY_VA_CODES), name='value_added')
    df.columns = USA_2017_SUMMARY_INDUSTRY_INDEX.copy()
    return df


def _load_usa_summary_sut(
    matrix_name: USA_SUMMARY_SUT_NAMES, year: USA_SUMMARY_SUT_YEARS
) -> pd.DataFrame:
    """
    Load USA Summary tables in Supply-use format
    """

    # Vintage pinning, as in _load_usa_summary_mut above: the workbook is chosen by
    # year, not by recency, so BEA's historical revisions do not move values under
    # existing consumers as new vintages add years on the right. 2017-2022 stays on
    # the workbook its published FBAs were built from; a 2025 vintage would get its
    # own `year > 2024` arm, leaving 2023-2024 where they are.
    # year arrives as a str from the FBA generation path, and typing.cast at the
    # call site is a no-op at runtime. Coerce before the vintage comparisons: they
    # raise on a str today, but sheet_name=str(year) accepts either type, so if
    # they ever moved a str year would silently select the wrong vintage mapping.
    year_int = int(year)
    if year_int > 2022:
        mapping = USA_SUMMARY_SUT_MAPPING_1997_2024
    else:
        mapping = USA_SUMMARY_SUT_MAPPING_2017_2022
    df = (
        load_from_gcs(
            name=mapping[matrix_name],
            sub_bucket=GCS_USA_SUP_DIR,
            local_dir=LOCAL_USA_SUP_DIR,
            loader=lambda pth: pd.read_excel(
                pth,
                sheet_name=str(year_int),
                skiprows=5,
                dtype={'Unnamed: 0': str},
            ),
        )
        .set_index('Unnamed: 0')
        .replace('...', 0)
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f'expected a DataFrame, got a {type(df)}'
    assert (
        len(df.shape) == 2
    ), f'expected a 2D DataFrame, got a {len(df.shape)}D DataFrame'

    return df


@deprecated('Use load_detail_Ytot_usa instead, which reads from MUT.')
def load_2017_Ytot_sut_usa() -> pd.DataFrame:
    """
    Final Demand (total), commodity x final demand, after redefintion, in producer price
    unit is USD, original unit is million USD
    """
    df = (
        _load_2017_detail_sut_usa('Use_detail')
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
                pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
            ),
        )
        .set_index('Code')
        .fillna(0)
    )
    df.columns = df.columns.astype(str)

    assert isinstance(df, pd.DataFrame), f'expected a DataFrame, got a {type(df)}'
    assert (
        len(df.shape) == 2
    ), f'expected a 2D DataFrame, got a {len(df.shape)}D DataFrame'

    return df
