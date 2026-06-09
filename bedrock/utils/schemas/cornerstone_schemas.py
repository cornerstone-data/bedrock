"""Pandera schemas for cornerstone single-region IO matrices and vectors.

Mirrors the structure of single_region_schemas.py / base_schemas.py but
validates against cornerstone commodity/industry sector codes instead of
CEDA v7 sectors.

Key difference from CEDA v7: industry and commodity indices are tracked
separately. V is (industry × commodity), U is (commodity × industry),
while A and L are (commodity × commodity).

Will rename/replace single_region_schemas.py / base_schemas.py after fully merged model.
"""

from __future__ import annotations

import typing as ta

import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.emissions.ghg import GHG
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

CORNERSTONE_COMMODITIES: list[str] = list(COMMODITIES)
CORNERSTONE_INDUSTRIES: list[str] = list(INDUSTRIES)

ELECTRICITY_AGGREGATE_SECTOR = "221100"
ELECTRICITY_DISAGG_SECTORS: list[str] = ["221110", "221121", "221122"]


def _replace_sector_in_list(
    sectors: list[str],
    aggregate: str,
    replacements: list[str],
) -> list[str]:
    idx = sectors.index(aggregate)
    return sectors[:idx] + replacements + sectors[idx + 1 :]


CORNERSTONE_COMMODITIES_ELEC: list[str] = _replace_sector_in_list(
    CORNERSTONE_COMMODITIES,
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)
CORNERSTONE_INDUSTRIES_ELEC: list[str] = _replace_sector_in_list(
    CORNERSTONE_INDUSTRIES,
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)

CornerstoneMatrixKind = ta.Literal["V", "U", "A", "B", "X", "Q"]


def _electricity_disaggregation_active() -> bool:
    return get_usa_config().implement_electricity_disaggregation


def active_cornerstone_commodities() -> list[str]:
    if _electricity_disaggregation_active():
        return CORNERSTONE_COMMODITIES_ELEC
    return CORNERSTONE_COMMODITIES


def active_cornerstone_industries() -> list[str]:
    if _electricity_disaggregation_active():
        return CORNERSTONE_INDUSTRIES_ELEC
    return CORNERSTONE_INDUSTRIES


def validate_cornerstone(df: pd.DataFrame | pd.Series, kind: CornerstoneMatrixKind) -> None:
    """Flag-aware pandera validation for Cornerstone outputs (405 vs 407)."""
    commodities = active_cornerstone_commodities()
    industries = active_cornerstone_industries()
    if kind == "V":
        if not validate_industry_index(df.index, industries):
            raise pa.errors.SchemaError(
                schema=CornerstoneVMatrix,
                data=df,
                message="V index does not match active Cornerstone industries",
            )
        if not validate_commodity_index(df.columns, commodities):
            raise pa.errors.SchemaError(
                schema=CornerstoneVMatrix,
                data=df,
                message="V columns do not match active Cornerstone commodities",
            )
    elif kind == "U":
        if not validate_commodity_index(df.index, commodities):
            raise pa.errors.SchemaError(
                schema=CornerstoneUMatrix,
                data=df,
                message="U index does not match active Cornerstone commodities",
            )
        if not validate_industry_index(df.columns, industries):
            raise pa.errors.SchemaError(
                schema=CornerstoneUMatrix,
                data=df,
                message="U columns do not match active Cornerstone industries",
            )
    elif kind == "A":
        if not validate_commodity_index(df.index, commodities):
            raise pa.errors.SchemaError(
                schema=CornerstoneAMatrix,
                data=df,
                message="A index does not match active Cornerstone commodities",
            )
        if not validate_commodity_index(df.columns, commodities):
            raise pa.errors.SchemaError(
                schema=CornerstoneAMatrix,
                data=df,
                message="A columns do not match active Cornerstone commodities",
            )
    elif kind == "B":
        if not validate_commodity_index(df.columns, commodities):
            raise pa.errors.SchemaError(
                schema=CornerstoneBMatrix,
                data=df,
                message="B columns do not match active Cornerstone commodities",
            )
    elif kind == "X":
        if not validate_industry_index(df.index, industries):
            raise pa.errors.SchemaError(
                schema=CornerstoneXVectorSchema,
                data=df,
                message="x index does not match active Cornerstone industries",
            )
    elif kind == "Q":
        if not validate_commodity_index(df.index, commodities):
            raise pa.errors.SchemaError(
                schema=CornerstoneQVectorSchema,
                data=df,
                message="q index does not match active Cornerstone commodities",
            )


def validate_commodity_index(
    index: pd.Index[str],
    valid_sectors: list[str] = CORNERSTONE_COMMODITIES,
) -> bool:
    try:
        return valid_sectors == index.get_level_values('sector').to_list()
    except Exception:
        return False


def validate_industry_index(
    index: pd.Index[str],
    valid_sectors: list[str] = CORNERSTONE_INDUSTRIES,
) -> bool:
    try:
        return valid_sectors == index.get_level_values('sector').to_list()
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Base matrix schemas
# ---------------------------------------------------------------------------


class CornerstoneCommoditySymmetricMatrixBase(pa.DataFrameModel):
    """Commodity × commodity matrix (e.g. A, L)."""

    sector: pt.Index[str] = pa.Field(
        isin=CORNERSTONE_COMMODITIES,
        unique=True,
        unique_values_eq=CORNERSTONE_COMMODITIES,
        nullable=False,
    )

    @pa.dataframe_check
    @classmethod
    def check_dataframe(cls, df: pd.DataFrame) -> bool:
        return validate_commodity_index(df.index) and validate_commodity_index(
            df.columns
        )


class CornerstoneVMatrixBase(pa.DataFrameModel):
    """Industry × commodity matrix (make / V matrix)."""

    sector: pt.Index[str] = pa.Field(
        isin=CORNERSTONE_INDUSTRIES,
        unique=True,
        unique_values_eq=CORNERSTONE_INDUSTRIES,
        nullable=False,
    )

    @pa.dataframe_check
    @classmethod
    def check_dataframe(cls, df: pd.DataFrame) -> bool:
        return validate_industry_index(df.index) and validate_commodity_index(
            df.columns
        )


class CornerstoneUMatrixBase(pa.DataFrameModel):
    """Commodity × industry matrix (use / U matrix)."""

    sector: pt.Index[str] = pa.Field(
        isin=CORNERSTONE_COMMODITIES,
        unique=True,
        unique_values_eq=CORNERSTONE_COMMODITIES,
        nullable=False,
    )

    @pa.dataframe_check
    @classmethod
    def check_dataframe(cls, df: pd.DataFrame) -> bool:
        return validate_commodity_index(df.index) and validate_industry_index(
            df.columns
        )


class _GhgIndexBase(pa.DataFrameModel):
    """Base for matrices with a GHG row index."""

    ghg: pt.Index[str] = pa.Field(isin=GHG, unique=True, unique_values_eq=GHG)


class CornerstoneSingleRegionEmissionsMatrixBase(_GhgIndexBase):
    """Emissions-like matrix with commodity columns (ghg × commodity), e.g. B."""

    @pa.dataframe_check
    @classmethod
    def check_columns(cls, df: pd.DataFrame) -> bool:
        return validate_commodity_index(df.columns)


class CornerstoneSingleRegionIndustryEmissionsBase(_GhgIndexBase):
    """Emissions matrix with industry columns (ghg × industry), e.g. E."""

    @pa.dataframe_check
    @classmethod
    def check_columns(cls, df: pd.DataFrame) -> bool:
        return validate_industry_index(df.columns)


# ---------------------------------------------------------------------------
# Vector schema helpers
# ---------------------------------------------------------------------------


def _get_commodity_vector_schema(is_non_negative: bool) -> pa.SeriesSchema:
    series_idx = pa.Index(
        str,
        name='sector',
        checks=[
            pa.Check.isin(CORNERSTONE_COMMODITIES),
            pa.Check.unique_values_eq(CORNERSTONE_COMMODITIES),
        ],
    )
    series_checks: list[pa.Check] = [
        pa.Check(lambda x: validate_commodity_index(x.index)),
    ]
    if is_non_negative:
        series_checks.append(pa.Check(lambda x: x >= 0, element_wise=True))

    return pa.SeriesSchema(
        float, index=series_idx, nullable=False, unique=False, checks=series_checks
    )


def _get_industry_vector_schema(is_non_negative: bool) -> pa.SeriesSchema:
    series_idx = pa.Index(
        str,
        name='sector',
        checks=[
            pa.Check.isin(CORNERSTONE_INDUSTRIES),
            pa.Check.unique_values_eq(CORNERSTONE_INDUSTRIES),
        ],
    )
    series_checks: list[pa.Check] = [
        pa.Check(lambda x: validate_industry_index(x.index)),
    ]
    if is_non_negative:
        series_checks.append(pa.Check(lambda x: x >= 0, element_wise=True))

    return pa.SeriesSchema(
        float, index=series_idx, nullable=False, unique=False, checks=series_checks
    )


# ---------------------------------------------------------------------------
# Matrix schemas
# ---------------------------------------------------------------------------


class CornerstoneUMatrix(CornerstoneUMatrixBase):
    """Use matrix (commodity × industry)."""


class CornerstoneVMatrix(CornerstoneVMatrixBase):
    """Make matrix (industry × commodity)."""


class CornerstoneAMatrix(CornerstoneCommoditySymmetricMatrixBase):
    """Technical coefficients matrix (commodity × commodity)."""


class CornerstoneBMatrix(CornerstoneSingleRegionEmissionsMatrixBase):
    """Emission coefficients matrix (ghg × commodity)."""


class CornerstoneEMatrix(CornerstoneSingleRegionIndustryEmissionsBase):
    """Emissions matrix (ghg × industry)."""


# ---------------------------------------------------------------------------
# Vector schemas
# ---------------------------------------------------------------------------

CornerstoneXVectorSchema = _get_industry_vector_schema(is_non_negative=False)
CornerstoneQVectorSchema = _get_commodity_vector_schema(is_non_negative=False)
CornerstoneYVectorSchema = _get_commodity_vector_schema(is_non_negative=True)
CornerstoneExportsVectorSchema = _get_commodity_vector_schema(is_non_negative=True)
CornerstoneImportsVectorSchema = _get_commodity_vector_schema(is_non_negative=False)
