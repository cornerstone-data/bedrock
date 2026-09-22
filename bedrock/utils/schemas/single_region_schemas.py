from __future__ import annotations

from bedrock.utils.schemas.base_schemas import (
    CedaSingleRegionEconomicVectorBaseSchema,
    CedaSingleRegionEmissionsMatrixBase,
    CedaSingleRegionNonNegativeEconomicVectorBaseSchema,
    CedaSingleRegionSymmetricMatrixBase,
)


class UMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Legacy single-region U schema (sector × sector).

    Live Cornerstone U uses ``CornerstoneUMatrix`` (commodity × industry).
    """

    # TODO: add a check that values are within expectations


class VMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Legacy single-region V schema (sector × sector).

    Live Cornerstone V uses ``CornerstoneVMatrix`` (industry × commodity).
    """

    # TODO: add a check that values are within expectations


class AMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Legacy single-region A schema (square sector × sector).

    Live Cornerstone A uses ``CornerstoneAMatrix``; callers that hold Cornerstone
    frames cast around this type rather than running this Pandera validation.
    """

    # TODO: add a check that values are between 0 and 1
    # TODO: add a check that column sums are <= 1


class LMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Schema for CEDA v7 L (Leontief inverse) matrices.

    Values should be >= 1 on diagonal and >= 0 elsewhere, but we don't yet
    validate for this.
    """

    # TODO: add a check that values are within expectations


class EMatrix(CedaSingleRegionEmissionsMatrixBase):
    """Schema for CEDA v7 E (emissions) matrices."""


class BMatrix(CedaSingleRegionEmissionsMatrixBase):
    """Schema for CEDA v7 B (emission coefficients) matrices."""


"""Schema for CEDA v7 x (industry output) vectors."""
XVectorSchema = CedaSingleRegionEconomicVectorBaseSchema


"""Schema for CEDA v7 q (commodity output) vectors."""
QVectorSchema = CedaSingleRegionEconomicVectorBaseSchema


"""Schema for CEDA v7 y (final demand) vectors."""
YVectorSchema = CedaSingleRegionNonNegativeEconomicVectorBaseSchema


"""Schema for CEDA v7 exports vectors."""
ExportsVectorSchema = CedaSingleRegionNonNegativeEconomicVectorBaseSchema


"""Schema for CEDA v7 imports vectors."""
ImportsVectorSchema = CedaSingleRegionEconomicVectorBaseSchema
