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

import pandas as pd
import pandera.pandas as pa
import pandera.typing as pt

from bedrock.utils.emissions.ghg import GHG
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

CORNERSTONE_COMMODITIES: list[str] = list(COMMODITIES)
CORNERSTONE_INDUSTRIES: list[str] = list(INDUSTRIES)


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

CornerstoneGVectorSchema = _get_industry_vector_schema(is_non_negative=False)
CornerstoneQVectorSchema = _get_commodity_vector_schema(is_non_negative=False)
CornerstoneYVectorSchema = _get_commodity_vector_schema(is_non_negative=True)
CornerstoneExportsVectorSchema = _get_commodity_vector_schema(is_non_negative=True)
CornerstoneImportsVectorSchema = _get_commodity_vector_schema(is_non_negative=False)
