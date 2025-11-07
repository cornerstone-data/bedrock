from __future__ import annotations

from ceda_usa.utils.schemas.base_schemas import (
    CedaSingleRegionEconomicVectorBaseSchema,
    CedaSingleRegionEmissionsMatrixBase,
    CedaSingleRegionNonNegativeEconomicVectorBaseSchema,
    CedaSingleRegionSymmetricMatrixBase,
)


class UMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Schema for the CEDA U matrix (use matrix, sector x sector).

    The U matrix represents the total amount of each commodity used to produce
    the total output of each commodity.
    """

    # TODO: add a check that values are within expectations


class VMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Schema for the CEDA U matrix (use matrix, sector x sector).

    The U matrix represents the total amount of each commodity used to produce
    the total output of each commodity.
    """

    # TODO: add a check that values are within expectations


class AMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Schema for the CEDA A matrix (technical coefficients matrix).

    The A matrix represents the amount of input from sector i required to produce
    one unit of output in sector j.

    Values should be between 0 and 1, but we don't yet validate for this.
    """

    # TODO: add a check that values are between 0 and 1
    # TODO: add a check that column sums are <= 1


class LMatrix(CedaSingleRegionSymmetricMatrixBase):
    """Schema for the CEDA L matrix (Leontief inverse matrix).

    The L matrix represents the total (direct + indirect) requirements
    to produce one unit of output. Values should be >= 1 on diagonal
    and >= 0 elsewhere, but we don't yet validate for this.
    """

    # TODO: add a check that values are within expectations


class EMatrix(CedaSingleRegionEmissionsMatrixBase):
    """Schema for the CEDA E matrix (emissions matrix).

    The E matrix represents the total emissions produced by each sector,
    split by GHG.
    """


class BMatrix(CedaSingleRegionEmissionsMatrixBase):
    """Schema for the CEDA B matrix (emission coefficients matrix).

    The B matrix represents the amount of each GHG emitted to produce
    one unit of each sector.
    """


"""Schema for the CEDA g vector (industry output vector).

The g vector represents the total industry output of each sector.
"""
GVectorSchema = CedaSingleRegionEconomicVectorBaseSchema


"""Schema for the CEDA q vector (commodity output vector).

The q vector represents the total commodity output of each sector.
"""
QVectorSchema = CedaSingleRegionEconomicVectorBaseSchema


"""Schema for the CEDA y vector (final demand vector).

The y vector represents the final demand for each sector.
"""
YVectorSchema = CedaSingleRegionNonNegativeEconomicVectorBaseSchema


"""Schema for the CEDA exports vector (exports vector).

The exports vector represents the exports of each sector.
"""
ExportsVectorSchema = CedaSingleRegionNonNegativeEconomicVectorBaseSchema


"""Schema for the CEDA imports vector (imports vector).

The imports vector represents the imports of each sector.
"""
ImportsVectorSchema = CedaSingleRegionEconomicVectorBaseSchema
