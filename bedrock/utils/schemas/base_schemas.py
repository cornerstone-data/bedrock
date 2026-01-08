from __future__ import annotations

import typing as ta

import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.utils.emissions.ghg import GHG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR as CEDA_SECTOR
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS as CEDA_SECTORS
from bedrock.utils.taxonomy.countries import CEDA_COUNTRIES, CEDA_COUNTRY


def validate_country_index(
    index: pd.Index[str],
    valid_countries: list[str] = list(CEDA_COUNTRIES),
) -> bool:
    # need to try/catch in case the index length does not match
    # so we return False and the schema validation produces a more helpful error
    try:
        return valid_countries == index.get_level_values("country").to_list()
    except Exception:
        return False


def validate_sector_index(
    index: pd.Index[str],
    valid_sectors: list[CEDA_SECTOR] = CEDA_SECTORS,
) -> bool:
    # need to try/catch in case the index length does not match
    # so we return False and the schema validation produces a more helpful error
    try:
        return valid_sectors == index.get_level_values("sector").to_list()
    except Exception:
        return False


def validate_multi_region_index(
    index: pd.Index[ta.Any],
    valid_countries: list[CEDA_COUNTRY] = CEDA_COUNTRIES,
    valid_sectors: list[CEDA_SECTOR] = CEDA_SECTORS,
) -> bool:
    expected_index = pd.MultiIndex.from_product(
        [valid_countries, valid_sectors], names=["country", "sector"]
    )
    # need to try/catch in case the index length does not match
    # so we return False and the schema valtest_validate_multi_region_indexidation produces a more helpful error
    try:
        return (
            bool((index == expected_index).all())
            and index.names == expected_index.names
        )
    except Exception:
        return False


class CedaMultiRegionSymmetricMatrixBase(pa.DataFrameModel):
    """Base schema for CEDA matrices that are symmetric with country x sector indices."""

    country: pt.Index[str] = pa.Field(
        isin=CEDA_COUNTRIES, unique_values_eq=CEDA_COUNTRIES
    )
    sector: pt.Index[str] = pa.Field(isin=CEDA_SECTORS, unique_values_eq=CEDA_SECTORS)

    @pa.dataframe_check
    @classmethod
    def check_dataframe(cls, df: pd.DataFrame) -> bool:
        return validate_multi_region_index(df.index) and validate_multi_region_index(
            df.columns
        )


class CedaSingleRegionSymmetricMatrixBase(pa.DataFrameModel):
    """Base schema for CEDA matrices that are single-region, with sector indices."""

    sector: pt.Index[str] = pa.Field(
        isin=CEDA_SECTORS, unique=True, unique_values_eq=CEDA_SECTORS, nullable=False
    )

    @pa.dataframe_check
    @classmethod
    def check_dataframe(cls, df: pd.DataFrame) -> bool:
        return validate_sector_index(df.index) and validate_sector_index(df.columns)


class CedaMultiRegionEmissionsMatrixBase(pa.DataFrameModel):
    """
    Base schema for CEDA emissions matrices that are multi-region,
    with country x sector indices.
    """

    ghg: pt.Index[str] = pa.Field(isin=GHG, unique=True, unique_values_eq=GHG)

    @pa.dataframe_check
    @classmethod
    def check_columns(cls, df: pd.DataFrame) -> bool:
        return validate_multi_region_index(df.columns)


class CedaSingleRegionEmissionsMatrixBase(pa.DataFrameModel):
    """Base schema for CEDA emissions matrices that are single-region, with sector indices."""

    ghg: pt.Index[str] = pa.Field(isin=GHG, unique=True, unique_values_eq=GHG)

    @pa.dataframe_check
    @classmethod
    def check_columns(cls, df: pd.DataFrame) -> bool:
        return validate_sector_index(df.columns)


def get_ceda_single_region_economic_vector_schema(
    is_non_negative: bool,
) -> pa.SeriesSchema:
    series_idx = pa.Index(
        str,
        name="sector",
        checks=[
            pa.Check.isin(CEDA_SECTORS),
            pa.Check.unique_values_eq(CEDA_SECTORS),
        ],
    )
    series_checks = [
        pa.Check(lambda x: validate_sector_index(x.index)),
    ]

    if is_non_negative:
        series_checks.append(pa.Check(lambda x: x >= 0, element_wise=True))

    return pa.SeriesSchema(
        float,
        index=series_idx,
        nullable=False,
        unique=False,
        checks=series_checks,
    )


"""
Base schema for CEDA economic vectors that are single-region,
with sector indices.
"""
CedaSingleRegionEconomicVectorBaseSchema = (
    get_ceda_single_region_economic_vector_schema(
        is_non_negative=False,
    )
)

"""
Base schema for CEDA economic vectors that are single-region,
non-negative, with sector indices.
"""
CedaSingleRegionNonNegativeEconomicVectorBaseSchema = (
    get_ceda_single_region_economic_vector_schema(
        is_non_negative=True,
    )
)
