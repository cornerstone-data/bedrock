"""Unit tests for waste weight loader and matrix/vector builders.

Tests cover:
- Raw CSV loaders (structure, normalization, filtering)
- Weight matrix builders (dimensions, indices, specific values)
- Fallback behavior (equal split when weight is missing/zero)
- Compatibility with structural_reflect_matrix assertions
"""

from __future__ import annotations

from typing import cast

import pandas as pd
import pytest

from bedrock.extract.disaggregation.load_waste_weights import (
    EQUAL_SPLIT_WEIGHT,
    WASTE_SUB_SECTORS,
    _normalize_sector_code,
    build_make_weight_matrix_for_cornerstone,
    build_U_weight_matrix_for_cornerstone,
    build_use_weight_matrix_for_cornerstone,
    build_V_weight_matrix_for_cornerstone,
    build_Y_weight_vector_for_cornerstone,
    load_waste_make_weights,
    load_waste_use_weights,
)
from bedrock.transform.eeio.cornerstone_expansion import (
    commodity_corresp_raw,
    industry_corresp_raw,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

WASTE_LIST: list[str] = list(WASTE_SUB_SECTORS)


# ---------------------------------------------------------------------------
# Sector code normalization
# ---------------------------------------------------------------------------


class TestNormalizeSectorCode:
    def test_strip_us_suffix(self) -> None:
        assert _normalize_sector_code("562111/US") == "562111"

    def test_no_suffix_unchanged(self) -> None:
        assert _normalize_sector_code("562111") == "562111"

    def test_other_country_suffix(self) -> None:
        assert _normalize_sector_code("562111/CA") == "562111"

    def test_multi_part_code(self) -> None:
        assert _normalize_sector_code("3363A0/US") == "3363A0"


# ---------------------------------------------------------------------------
# Raw CSV loaders
# ---------------------------------------------------------------------------


class TestLoadWasteMakeWeights:
    @pytest.fixture(scope="class")
    def make_df(self) -> pd.DataFrame:
        return load_waste_make_weights()

    def test_returns_dataframe(self, make_df: pd.DataFrame) -> None:
        assert make_df is not None
        assert len(make_df) > 0

    def test_has_multiindex(self, make_df: pd.DataFrame) -> None:
        assert make_df.index.names == ["industry", "commodity"]

    def test_has_weight_column(self, make_df: pd.DataFrame) -> None:
        assert "weight" in make_df.columns

    def test_has_note_column(self, make_df: pd.DataFrame) -> None:
        assert "Note" in make_df.columns

    def test_no_slash_in_codes(self, make_df: pd.DataFrame) -> None:
        for ind, com in make_df.index:
            assert "/" not in str(ind), f"Industry code {ind} contains /"
            assert "/" not in str(com), f"Commodity code {com} contains /"


class TestLoadWasteUseWeights:
    @pytest.fixture(scope="class")
    def use_df(self) -> pd.DataFrame:
        return load_waste_use_weights()

    def test_returns_dataframe(self, use_df: pd.DataFrame) -> None:
        assert use_df is not None
        assert len(use_df) > 0

    def test_has_multiindex(self, use_df: pd.DataFrame) -> None:
        assert use_df.index.names == ["industry", "commodity"]

    def test_has_weight_column(self, use_df: pd.DataFrame) -> None:
        assert "weight" in use_df.columns

    def test_no_slash_in_codes(self, use_df: pd.DataFrame) -> None:
        for ind, com in use_df.index:
            assert "/" not in str(ind), f"Industry code {ind} contains /"
            assert "/" not in str(com), f"Commodity code {com} contains /"


# ---------------------------------------------------------------------------
# A-matrix weight builders (commodity × commodity)
# ---------------------------------------------------------------------------


def _waste_row(df: pd.DataFrame, sector: str) -> pd.Series[float]:
    """Extract the waste sub-sector slice for a given row."""
    row = cast("pd.Series[float]", df.loc[sector])
    return row[WASTE_LIST]


class TestBuildMakeWeightMatrix:
    @pytest.fixture(scope="class")
    def make_weights(self) -> pd.DataFrame:
        return build_make_weight_matrix_for_cornerstone()

    def test_shape(self, make_weights: pd.DataFrame) -> None:
        assert make_weights.shape == (len(COMMODITIES), len(COMMODITIES))

    def test_index_equals_commodities(self, make_weights: pd.DataFrame) -> None:
        assert list(make_weights.index) == list(COMMODITIES)

    def test_columns_equals_commodities(self, make_weights: pd.DataFrame) -> None:
        assert list(make_weights.columns) == list(COMMODITIES)

    def test_non_waste_diagonal_is_one(self, make_weights: pd.DataFrame) -> None:
        waste_set = set(WASTE_SUB_SECTORS)
        for s in COMMODITIES:
            if s not in waste_set:
                assert make_weights.loc[s, s] == pytest.approx(
                    1.0
                ), f"Diagonal for non-waste sector {s} should be 1.0"

    def test_221300_has_only_4_nonzero_waste_columns(
        self, make_weights: pd.DataFrame
    ) -> None:
        """CSV specifies commodity disaggregation for 221300 to only 4 waste sub-sectors."""
        row = _waste_row(make_weights, "221300")
        nonzero_cols = set(row[row > 0].index)
        expected = {"562111", "562212", "562213", "562920"}
        assert (
            nonzero_cols == expected
        ), f"221300 should have exactly {expected} non-zero, got {nonzero_cols}"

    def test_484000_has_only_562111_nonzero(self, make_weights: pd.DataFrame) -> None:
        """CSV specifies 484000 → 562111 at 1.0."""
        row = _waste_row(make_weights, "484000")
        nonzero_cols = set(row[row > 0].index)
        assert nonzero_cols == {"562111"}

    def test_561700_has_only_562910_nonzero(self, make_weights: pd.DataFrame) -> None:
        """CSV specifies 561700 → 562910 at 1.0."""
        row = _waste_row(make_weights, "561700")
        nonzero_cols = set(row[row > 0].index)
        assert nonzero_cols == {"562910"}


class TestBuildUseWeightMatrix:
    @pytest.fixture(scope="class")
    def use_weights(self) -> pd.DataFrame:
        return build_use_weight_matrix_for_cornerstone()

    def test_shape(self, use_weights: pd.DataFrame) -> None:
        assert use_weights.shape == (len(COMMODITIES), len(COMMODITIES))

    def test_index_equals_commodities(self, use_weights: pd.DataFrame) -> None:
        assert list(use_weights.index) == list(COMMODITIES)

    def test_columns_equals_commodities(self, use_weights: pd.DataFrame) -> None:
        assert list(use_weights.columns) == list(COMMODITIES)


# ---------------------------------------------------------------------------
# V weight matrix (industry × commodity)
# ---------------------------------------------------------------------------


class TestBuildVWeightMatrix:
    @pytest.fixture(scope="class")
    def v_weights(self) -> pd.DataFrame:
        return build_V_weight_matrix_for_cornerstone()

    def test_shape(self, v_weights: pd.DataFrame) -> None:
        assert v_weights.shape == (len(INDUSTRIES), len(COMMODITIES))

    def test_index_equals_industries(self, v_weights: pd.DataFrame) -> None:
        assert list(v_weights.index) == list(INDUSTRIES)

    def test_columns_equals_commodities(self, v_weights: pd.DataFrame) -> None:
        assert list(v_weights.columns) == list(COMMODITIES)

    def test_non_waste_diagonal_is_one(self, v_weights: pd.DataFrame) -> None:
        common = set(str(s) for s in INDUSTRIES) & set(
            str(s) for s in COMMODITIES
        ) - set(WASTE_SUB_SECTORS)
        for s in list(common)[:20]:
            assert v_weights.loc[s, s] == pytest.approx(
                1.0
            ), f"Diagonal for non-waste sector {s} should be 1.0"


# ---------------------------------------------------------------------------
# U weight matrix (commodity × industry)
# ---------------------------------------------------------------------------


class TestBuildUWeightMatrix:
    @pytest.fixture(scope="class")
    def u_weights(self) -> pd.DataFrame:
        return build_U_weight_matrix_for_cornerstone()

    def test_shape(self, u_weights: pd.DataFrame) -> None:
        assert u_weights.shape == (len(COMMODITIES), len(INDUSTRIES))

    def test_index_equals_commodities(self, u_weights: pd.DataFrame) -> None:
        assert list(u_weights.index) == list(COMMODITIES)

    def test_columns_equals_industries(self, u_weights: pd.DataFrame) -> None:
        assert list(u_weights.columns) == list(INDUSTRIES)

    def test_transpose_csv_entry(self, u_weights: pd.DataFrame) -> None:
        """Use CSV has (813100, 562111, 0.677). U weight matrix is commodity × industry,
        so this should appear at loc['562111', '813100']."""
        val = u_weights.loc["562111", "813100"]
        assert val == pytest.approx(0.677, abs=0.001)

    def test_non_waste_diagonal_is_one(self, u_weights: pd.DataFrame) -> None:
        common = set(str(s) for s in COMMODITIES) & set(
            str(s) for s in INDUSTRIES
        ) - set(WASTE_SUB_SECTORS)
        for s in list(common)[:20]:
            assert u_weights.loc[s, s] == pytest.approx(
                1.0
            ), f"Diagonal for non-waste sector {s} should be 1.0"


# ---------------------------------------------------------------------------
# Y weight vector (commodity)
# ---------------------------------------------------------------------------


class TestBuildYWeightVector:
    @pytest.fixture(scope="class")
    def y_weights(self) -> pd.Series[float]:
        return build_Y_weight_vector_for_cornerstone()

    def test_length(self, y_weights: pd.Series[float]) -> None:
        assert len(y_weights) == len(COMMODITIES)

    def test_index_equals_commodities(self, y_weights: pd.Series[float]) -> None:
        assert list(y_weights.index) == list(COMMODITIES)

    def test_non_waste_sectors_are_one(self, y_weights: pd.Series[float]) -> None:
        waste_set = set(WASTE_SUB_SECTORS)
        for s in COMMODITIES:
            if s not in waste_set:
                assert y_weights.loc[s] == pytest.approx(1.0)

    def test_waste_sub_sectors_sum_to_one(self, y_weights: pd.Series[float]) -> None:
        waste_sum = sum(float(y_weights.loc[s]) for s in WASTE_SUB_SECTORS)
        assert waste_sum == pytest.approx(1.0, abs=0.001)

    def test_waste_sub_sectors_positive(self, y_weights: pd.Series[float]) -> None:
        for s in WASTE_SUB_SECTORS:
            assert y_weights.loc[s] > 0


# ---------------------------------------------------------------------------
# Fallback behavior
# ---------------------------------------------------------------------------


class TestFallbackBehavior:
    @pytest.fixture(scope="class")
    def make_weights(self) -> pd.DataFrame:
        return build_make_weight_matrix_for_cornerstone()

    def test_sectors_without_explicit_disagg_use_col_sum_proportions(
        self, make_weights: pd.DataFrame
    ) -> None:
        """Sectors without explicit commodity disaggregation should get
        Make column sum proportions (not uniform 1/7)."""
        row = _waste_row(make_weights, "211000")
        vals = [float(v) for v in row.values]
        assert not all(v == pytest.approx(EQUAL_SPLIT_WEIGHT) for v in vals), (
            "211000 has explicit industry disaggregation (562212→211000), "
            "remaining waste cols should use Make column sum proportions"
        )

    def test_7x7_waste_block_sums_to_one(self) -> None:
        """The 7×7 waste intersection block should sum to 1.0."""
        make_weights = build_make_weight_matrix_for_cornerstone()
        block = make_weights.loc[WASTE_LIST, WASTE_LIST]
        assert block.sum().sum() == pytest.approx(1.0, abs=0.01)


# ---------------------------------------------------------------------------
# Use col_sum/row_sum extraction
# ---------------------------------------------------------------------------


class TestUseAllocationProportions:
    @pytest.fixture(scope="class")
    def u_weights(self) -> pd.DataFrame:
        return build_U_weight_matrix_for_cornerstone()

    def test_u_column_fallback_not_equal_split(self, u_weights: pd.DataFrame) -> None:
        """U column fallback (industry axis) should use 'Use column sum' proportions,
        not equal split."""
        waste_set = set(WASTE_SUB_SECTORS)
        waste_row = cast("pd.Series[float]", u_weights.loc["562111"])
        col_vals = [float(waste_row[ind]) for ind in INDUSTRIES if ind not in waste_set]
        nonzero = [v for v in col_vals if v > 0]
        if nonzero:
            assert not all(
                v == pytest.approx(nonzero[0]) for v in nonzero
            ), "U weight industry fallback values should vary (not uniform)"

    def test_u_col_fallback_uses_industry_proportions(
        self, u_weights: pd.DataFrame
    ) -> None:
        """Non-waste commodity rows at waste industry columns use 'Use column sum'
        proportions (industry proportions ≈ 0.476 for 562111), not equal split 1/7."""
        waste_set = set(WASTE_SUB_SECTORS)
        sample_com = next(c for c in COMMODITIES if c not in waste_set)
        col_562111: pd.Series[float] = u_weights["562111"]
        val = float(col_562111[sample_com])
        assert val != pytest.approx(EQUAL_SPLIT_WEIGHT, abs=0.01), (
            f"U col fallback at ({sample_com}, 562111) = {val:.4f} "
            f"should not be equal split {EQUAL_SPLIT_WEIGHT:.4f}"
        )
        assert val == pytest.approx(
            0.476, abs=0.02
        ), f"Expected Use column sum proportion for 562111 ≈ 0.476, got {val:.4f}"


# ---------------------------------------------------------------------------
# structural_reflect_matrix compatibility
# ---------------------------------------------------------------------------


class TestStructuralReflectCompatibility:
    """Verify weight matrix indices match what structural_reflect_matrix expects."""

    def test_make_weight_compatible_with_commodity_corresp(self) -> None:
        corresp = commodity_corresp_raw()
        weights = build_make_weight_matrix_for_cornerstone()
        assert (weights.index == corresp.index).all()
        assert (weights.columns == corresp.index).all()

    def test_v_weight_compatible_with_industry_and_commodity_corresp(self) -> None:
        ind_corresp = industry_corresp_raw()
        com_corresp = commodity_corresp_raw()
        weights = build_V_weight_matrix_for_cornerstone()
        assert (weights.index == ind_corresp.index).all()
        assert (weights.columns == com_corresp.index).all()

    def test_u_weight_compatible_with_commodity_and_industry_corresp(self) -> None:
        com_corresp = commodity_corresp_raw()
        ind_corresp = industry_corresp_raw()
        weights = build_U_weight_matrix_for_cornerstone()
        assert (weights.index == com_corresp.index).all()
        assert (weights.columns == ind_corresp.index).all()

    def test_use_weight_compatible_with_commodity_corresp(self) -> None:
        corresp = commodity_corresp_raw()
        weights = build_use_weight_matrix_for_cornerstone()
        assert (weights.index == corresp.index).all()
        assert (weights.columns == corresp.index).all()
