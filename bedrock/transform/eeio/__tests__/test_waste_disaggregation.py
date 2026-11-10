"""Waste-specific disaggregation tests (i.e., 562111, 562212, etc). NOT sector agnostic."""

from __future__ import annotations

import pathlib
from typing import cast

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.disaggregation.disagg_weights import (
    DisaggWeights,
    _empty_weight_table,
    load_disagg_weights,
)
from bedrock.extract.disaggregation.waste_static_rules import (
    GSLGO_MEMBERSHIP,
    MAKE_COLUMN_GSLGO_INDUSTRIES,
    MAKE_COLUMN_ONEHOT_INDUSTRIES,
    MAKE_ROW_ONEHOT_COMMODITIES,
)
from bedrock.extract.disaggregation.waste_weight_config import EEIOWasteDisaggConfig
from bedrock.extract.iot.io_2017 import load_2017_value_added_usa
from bedrock.transform.eeio.sector_disagg_pipeline import (
    derive_disagg_Ytot_with_trade,
)
from bedrock.transform.eeio.usa_expansion import industry_corresp
from bedrock.transform.eeio.usa_matrices import (
    derive_U_with_negatives,
    derive_V,
)
from bedrock.transform.eeio.waste_disaggregation import (
    DISAGG_BALANCE_ATOL,
    _make_column_default_shares,
    _make_column_gslgo_shares,
    apply_waste_disagg_to_U,
    apply_waste_disagg_to_V,
    apply_waste_disagg_to_VA,
    apply_waste_disagg_to_Ytot,
    compute_waste_q_targets,
    reconcile_waste_make_and_va_to_use,
)
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES

WASTE = ["562111", "562212", "562910"]
ORIG = "562000"


def _wt(index: list[str], columns: list[str], vals: list[list[float]]) -> pd.DataFrame:
    return pd.DataFrame(vals, index=index, columns=columns, dtype=float)


def _uniform_row(columns: list[str]) -> list[float]:
    n = len(columns)
    return [1.0 / n] * n


@pytest.fixture()
def simple_weights() -> DisaggWeights:
    """Weights fixture with 3 waste subsectors and simple known distributions."""
    make_intersection = _wt(
        WASTE,
        WASTE,
        [
            [0.6, 0.0, 0.0],
            [0.0, 0.3, 0.0],
            [0.0, 0.0, 0.1],
        ],
    )
    make_col_all = _wt(
        [ORIG],
        WASTE,
        [[0.5, 0.3, 0.2]],
    )
    make_col_specific = _empty_weight_table()
    make_row_specific = _wt(
        ["COM_A"],
        WASTE,
        [[0.4, 0.4, 0.2]],
    )

    use_intersection = _wt(
        WASTE,
        WASTE,
        [
            [0.5, 0.0, 0.0],
            [0.0, 0.3, 0.0],
            [0.0, 0.0, 0.2],
        ],
    )
    use_col_all = _wt(
        [ORIG],
        WASTE,
        [[0.4, 0.35, 0.25]],
    )
    use_row_all = _wt(
        [ORIG],
        WASTE,
        [[0.5, 0.3, 0.2]],
    )
    use_rows_specific = _empty_weight_table()
    use_va = _wt(
        ["V00100", "V00200"],
        WASTE,
        [
            [0.5, 0.3, 0.2],
            [0.6, 0.2, 0.2],
        ],
    )
    use_fd = _wt(
        ["F01000", "F06C00"],
        WASTE,
        [
            [0.7, 0.2, 0.1],
            [0.3, 0.4, 0.3],
        ],
    )

    return DisaggWeights(
        use_intersection=use_intersection,
        use_disagg_industry_columns_all_rows=use_col_all,
        use_disagg_commodity_rows_all_columns=use_row_all,
        use_disagg_rows_specific_columns=use_rows_specific,
        use_va_rows_for_disagg_industry_columns=use_va,
        use_fd_columns_for_disagg_commodity_rows=use_fd,
        make_intersection=make_intersection,
        make_disagg_commodity_columns_all_rows=make_col_all,
        make_disagg_commodity_columns_specific_rows=make_col_specific,
        make_disagg_industry_rows_specific_columns=make_row_specific,
        year=2017,
        source_name="test",
    )


@pytest.fixture()
def simple_weights_with_ind_x_row_specific() -> DisaggWeights:
    """Like simple_weights but use_disagg_rows_specific_columns has IND_X with 0.6, 0.25, 0.15."""
    make_intersection = _wt(
        WASTE,
        WASTE,
        [
            [0.6, 0.0, 0.0],
            [0.0, 0.3, 0.0],
            [0.0, 0.0, 0.1],
        ],
    )
    make_col_all = _wt([ORIG], WASTE, [[0.5, 0.3, 0.2]])
    make_col_specific = _empty_weight_table()
    make_row_specific = _wt(["COM_A"], WASTE, [[0.4, 0.4, 0.2]])
    use_intersection = _wt(
        WASTE,
        WASTE,
        [
            [0.5, 0.0, 0.0],
            [0.0, 0.3, 0.0],
            [0.0, 0.0, 0.2],
        ],
    )
    use_col_all = _wt([ORIG], WASTE, [[0.4, 0.35, 0.25]])
    use_row_all = _wt([ORIG], WASTE, [[0.5, 0.3, 0.2]])
    use_rows_specific = _wt(["IND_X"], WASTE, [[0.6, 0.25, 0.15]])
    use_va = _wt(
        ["V00100", "V00200"],
        WASTE,
        [[0.5, 0.3, 0.2], [0.6, 0.2, 0.2]],
    )
    use_fd = _wt(
        ["F01000", "F06C00"],
        WASTE,
        [[0.7, 0.2, 0.1], [0.3, 0.4, 0.3]],
    )
    return DisaggWeights(
        use_intersection=use_intersection,
        use_disagg_industry_columns_all_rows=use_col_all,
        use_disagg_commodity_rows_all_columns=use_row_all,
        use_disagg_rows_specific_columns=use_rows_specific,
        use_va_rows_for_disagg_industry_columns=use_va,
        use_fd_columns_for_disagg_commodity_rows=use_fd,
        make_intersection=make_intersection,
        make_disagg_commodity_columns_all_rows=make_col_all,
        make_disagg_commodity_columns_specific_rows=make_col_specific,
        make_disagg_industry_rows_specific_columns=make_row_specific,
        year=2017,
        source_name="test",
    )


# ---------------------------------------------------------------------------
# apply_waste_disagg_to_V
# ---------------------------------------------------------------------------


class TestApplyWasteDisaggToV:
    def test_intersection_block_matches_weights(
        self, simple_weights: DisaggWeights
    ) -> None:
        industries = WASTE + ["IND_A", ORIG]
        commodities = WASTE + ["COM_A", ORIG]
        V = pd.DataFrame(0.0, index=industries, columns=commodities)
        V.loc[ORIG, ORIG] = 100.0
        V.loc["IND_A", ORIG] = 50.0
        V.loc[ORIG, "COM_A"] = 30.0
        V.loc["IND_A", "COM_A"] = 10.0

        result = apply_waste_disagg_to_V(V, simple_weights)

        assert ORIG not in result.index
        assert ORIG not in result.columns

        assert result.loc["562111", "562111"] == pytest.approx(60.0)
        assert result.loc["562212", "562212"] == pytest.approx(30.0)
        assert result.loc["562910", "562910"] == pytest.approx(10.0)
        assert result.loc["562111", "562212"] == pytest.approx(0.0, abs=1e-12)

    def test_intersection_total_preserved(self, simple_weights: DisaggWeights) -> None:
        industries = WASTE + ["IND_A", ORIG]
        commodities = WASTE + ["COM_A", ORIG]
        V = pd.DataFrame(0.0, index=industries, columns=commodities)
        V.loc[ORIG, ORIG] = 100.0
        V.loc["IND_A", ORIG] = 50.0
        V.loc[ORIG, "COM_A"] = 30.0

        result = apply_waste_disagg_to_V(V, simple_weights)

        intersection_sum = sum(
            cast(float, result.loc[i, j]) for i in WASTE for j in WASTE
        )
        assert intersection_sum == pytest.approx(100.0)

    def test_column_disaggregation(self, simple_weights: DisaggWeights) -> None:
        industries = WASTE + ["IND_A", ORIG]
        commodities = WASTE + ["COM_A", ORIG]
        V = pd.DataFrame(0.0, index=industries, columns=commodities)
        V.loc["IND_A", ORIG] = 50.0

        result = apply_waste_disagg_to_V(V, simple_weights)

        assert result.loc["IND_A", "562111"] == pytest.approx(25.0)
        assert result.loc["IND_A", "562212"] == pytest.approx(15.0)
        assert result.loc["IND_A", "562910"] == pytest.approx(10.0)
        col_sum = sum(cast(float, result.loc["IND_A", c]) for c in WASTE)
        assert col_sum == pytest.approx(50.0)

    def test_row_disaggregation_specific(self, simple_weights: DisaggWeights) -> None:
        industries = WASTE + ["IND_A", ORIG]
        commodities = WASTE + ["COM_A", ORIG]
        V = pd.DataFrame(0.0, index=industries, columns=commodities)
        V.loc[ORIG, "COM_A"] = 30.0

        result = apply_waste_disagg_to_V(V, simple_weights)

        assert result.loc["562111", "COM_A"] == pytest.approx(12.0)
        assert result.loc["562212", "COM_A"] == pytest.approx(12.0)
        assert result.loc["562910", "COM_A"] == pytest.approx(6.0)
        row_sum = sum(cast(float, result.loc[r, "COM_A"]) for r in WASTE)
        assert row_sum == pytest.approx(30.0)

    def test_non_waste_cells_unchanged(self, simple_weights: DisaggWeights) -> None:
        industries = WASTE + ["IND_A", ORIG]
        commodities = WASTE + ["COM_A", ORIG]
        V = pd.DataFrame(0.0, index=industries, columns=commodities)
        V.loc["IND_A", "COM_A"] = 10.0
        V.loc[ORIG, ORIG] = 100.0

        result = apply_waste_disagg_to_V(V, simple_weights)

        assert result.loc["IND_A", "COM_A"] == pytest.approx(10.0)

    def test_no_original_code_returns_unchanged(
        self, simple_weights: DisaggWeights
    ) -> None:
        V = pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]}, index=["X", "Y"])
        result = apply_waste_disagg_to_V(V, simple_weights)
        pd.testing.assert_frame_equal(result, V)


# ---------------------------------------------------------------------------
# apply_waste_disagg_to_U
# ---------------------------------------------------------------------------


class TestApplyWasteDisaggToU:
    def _make_U(self) -> pd.DataFrame:
        commodities = WASTE + ["COM_X", ORIG]
        industries = WASTE + ["IND_X", ORIG]
        U = pd.DataFrame(0.0, index=commodities, columns=industries)
        U.loc[ORIG, ORIG] = 200.0
        U.loc["COM_X", ORIG] = 80.0
        U.loc[ORIG, "IND_X"] = 60.0
        U.loc["COM_X", "IND_X"] = 15.0
        return U

    def test_intersection_preserved(self, simple_weights: DisaggWeights) -> None:
        U = self._make_U()
        Udom, _ = apply_waste_disagg_to_U(U, U.copy(), simple_weights)

        intersection_sum = sum(
            cast(float, Udom.loc[c, i]) for c in WASTE for i in WASTE
        )
        assert intersection_sum == pytest.approx(200.0)

    def test_intersection_values(self, simple_weights: DisaggWeights) -> None:
        U = self._make_U()
        Udom, _ = apply_waste_disagg_to_U(U, U.copy(), simple_weights)

        assert Udom.loc["562111", "562111"] == pytest.approx(100.0)
        assert Udom.loc["562212", "562212"] == pytest.approx(60.0)
        assert Udom.loc["562910", "562910"] == pytest.approx(40.0)

    def test_column_disagg_total_preserved(self, simple_weights: DisaggWeights) -> None:
        U = self._make_U()
        Udom, _ = apply_waste_disagg_to_U(U, U.copy(), simple_weights)

        col_sum = sum(cast(float, Udom.loc["COM_X", i]) for i in WASTE)
        assert col_sum == pytest.approx(80.0)

    def test_row_disagg_total_preserved(self, simple_weights: DisaggWeights) -> None:
        U = self._make_U()
        Udom, _ = apply_waste_disagg_to_U(U, U.copy(), simple_weights)

        row_sum = sum(cast(float, Udom.loc[c, "IND_X"]) for c in WASTE)
        assert row_sum == pytest.approx(60.0)

    def test_row_disagg_uses_default_weights(
        self, simple_weights: DisaggWeights
    ) -> None:
        """With no industry-specific row weights, IND_X uses default row [0.5, 0.3, 0.2]."""
        U = self._make_U()
        Udom, _ = apply_waste_disagg_to_U(U, U.copy(), simple_weights)
        # U[ORIG, IND_X]=60 split by use_disagg_commodity_rows_all_columns (ORIG row)
        assert Udom.loc["562111", "IND_X"] == pytest.approx(30.0)
        assert Udom.loc["562212", "IND_X"] == pytest.approx(18.0)
        assert Udom.loc["562910", "IND_X"] == pytest.approx(12.0)

    def test_row_disagg_uses_specific_weights_when_present(
        self, simple_weights_with_ind_x_row_specific: DisaggWeights
    ) -> None:
        """When use_disagg_rows_specific_columns has a row for the industry, that row is used."""
        U = self._make_U()
        Udom, _ = apply_waste_disagg_to_U(
            U, U.copy(), simple_weights_with_ind_x_row_specific
        )
        # U[ORIG, IND_X]=60 split by use_disagg_rows_specific_columns IND_X row (0.6, 0.25, 0.15)
        assert Udom.loc["562111", "IND_X"] == pytest.approx(36.0)
        assert Udom.loc["562212", "IND_X"] == pytest.approx(15.0)
        assert Udom.loc["562910", "IND_X"] == pytest.approx(9.0)

    def test_original_code_removed(self, simple_weights: DisaggWeights) -> None:
        U = self._make_U()
        Udom, Uimp = apply_waste_disagg_to_U(U, U.copy(), simple_weights)

        assert ORIG not in Udom.index
        assert ORIG not in Udom.columns
        assert ORIG not in Uimp.index
        assert ORIG not in Uimp.columns

    def test_non_waste_unchanged(self, simple_weights: DisaggWeights) -> None:
        U = self._make_U()
        Udom, _ = apply_waste_disagg_to_U(U, U.copy(), simple_weights)

        assert Udom.loc["COM_X", "IND_X"] == pytest.approx(15.0)

    def test_both_matrices_processed(self, simple_weights: DisaggWeights) -> None:
        U1 = self._make_U()
        U2 = self._make_U()
        U2.loc[ORIG, ORIG] = 300.0
        Udom, Uimp = apply_waste_disagg_to_U(U1, U2, simple_weights)

        dom_int = sum(cast(float, Udom.loc[c, i]) for c in WASTE for i in WASTE)
        imp_int = sum(cast(float, Uimp.loc[c, i]) for c in WASTE for i in WASTE)
        assert dom_int == pytest.approx(200.0)
        assert imp_int == pytest.approx(300.0)


# ---------------------------------------------------------------------------
# apply_waste_disagg_to_VA
# ---------------------------------------------------------------------------


class TestApplyWasteDisaggToVA:
    def test_va_total_preserved(self, simple_weights: DisaggWeights) -> None:
        va = pd.DataFrame(
            {ORIG: [100.0, 200.0], "IND_A": [50.0, 60.0]},
            index=["V00100", "V00200"],
        )
        result = apply_waste_disagg_to_VA(va, simple_weights)

        assert ORIG not in result.columns
        for va_row in ["V00100", "V00200"]:
            row_sum = sum(cast(float, result.loc[va_row, c]) for c in WASTE)
            assert row_sum == pytest.approx(
                va.loc[va_row, ORIG]
            ), f"VA total not preserved for {va_row}"

    def test_va_distribution_matches_weights(
        self, simple_weights: DisaggWeights
    ) -> None:
        va = pd.DataFrame(
            {ORIG: [100.0], "IND_A": [50.0]},
            index=["V00100"],
        )
        result = apply_waste_disagg_to_VA(va, simple_weights)

        assert result.loc["V00100", "562111"] == pytest.approx(50.0)
        assert result.loc["V00100", "562212"] == pytest.approx(30.0)
        assert result.loc["V00100", "562910"] == pytest.approx(20.0)

    def test_non_waste_columns_unchanged(self, simple_weights: DisaggWeights) -> None:
        va = pd.DataFrame(
            {ORIG: [100.0], "IND_A": [50.0]},
            index=["V00100"],
        )
        result = apply_waste_disagg_to_VA(va, simple_weights)
        assert result.loc["V00100", "IND_A"] == pytest.approx(50.0)

    def test_different_va_rows_use_different_weights(
        self, simple_weights: DisaggWeights
    ) -> None:
        va = pd.DataFrame(
            {ORIG: [100.0, 100.0], "IND_A": [50.0, 60.0]},
            index=["V00100", "V00200"],
        )
        result = apply_waste_disagg_to_VA(va, simple_weights)

        assert result.loc["V00100", "562111"] == pytest.approx(50.0)
        assert result.loc["V00200", "562111"] == pytest.approx(60.0)

    def test_no_original_code_returns_unchanged(
        self, simple_weights: DisaggWeights
    ) -> None:
        va = pd.DataFrame(
            {"IND_A": [50.0]},
            index=["V00100"],
        )
        result = apply_waste_disagg_to_VA(va, simple_weights)
        pd.testing.assert_frame_equal(result, va)


# ---------------------------------------------------------------------------
# apply_waste_disagg_to_Ytot
# ---------------------------------------------------------------------------


class TestApplyWasteDisaggToYtot:
    def test_fd_total_preserved_per_column(self, simple_weights: DisaggWeights) -> None:
        Ytot = pd.DataFrame(
            {"F01000": [100.0, 20.0], "F06C00": [80.0, 10.0]},
            index=[ORIG, "COM_X"],
        )
        result = apply_waste_disagg_to_Ytot(Ytot, simple_weights)

        assert ORIG not in result.index
        for fd_col in ["F01000", "F06C00"]:
            col_sum = sum(cast(float, result.loc[c, fd_col]) for c in WASTE)
            assert col_sum == pytest.approx(
                Ytot.loc[ORIG, fd_col]
            ), f"FD total not preserved for {fd_col}"

    def test_fd_distribution_matches_weights(
        self, simple_weights: DisaggWeights
    ) -> None:
        Ytot = pd.DataFrame(
            {"F01000": [100.0], "F06C00": [80.0]},
            index=[ORIG],
        )
        result = apply_waste_disagg_to_Ytot(Ytot, simple_weights)

        assert result.loc["562111", "F01000"] == pytest.approx(70.0)
        assert result.loc["562212", "F01000"] == pytest.approx(20.0)
        assert result.loc["562910", "F01000"] == pytest.approx(10.0)

        assert result.loc["562111", "F06C00"] == pytest.approx(24.0)
        assert result.loc["562212", "F06C00"] == pytest.approx(32.0)
        assert result.loc["562910", "F06C00"] == pytest.approx(24.0)

    def test_non_waste_rows_unchanged(self, simple_weights: DisaggWeights) -> None:
        Ytot = pd.DataFrame(
            {"F01000": [100.0, 20.0]},
            index=[ORIG, "COM_X"],
        )
        result = apply_waste_disagg_to_Ytot(Ytot, simple_weights)
        assert result.loc["COM_X", "F01000"] == pytest.approx(20.0)

    def test_fallback_to_commodity_rows_all_columns_for_unknown_fd_col(
        self, simple_weights: DisaggWeights
    ) -> None:
        Ytot = pd.DataFrame(
            {"F99999": [100.0]},
            index=[ORIG],
        )
        result = apply_waste_disagg_to_Ytot(Ytot, simple_weights)

        col_sum = sum(cast(float, result.loc[c, "F99999"]) for c in WASTE)
        assert col_sum == pytest.approx(100.0)
        assert result.loc["562111", "F99999"] == pytest.approx(50.0)
        assert result.loc["562212", "F99999"] == pytest.approx(30.0)
        assert result.loc["562910", "F99999"] == pytest.approx(20.0)

    def test_no_original_code_returns_unchanged(
        self, simple_weights: DisaggWeights
    ) -> None:
        Ytot = pd.DataFrame({"F01000": [1.0, 2.0]}, index=["A", "B"])
        result = apply_waste_disagg_to_Ytot(Ytot, simple_weights)
        pd.testing.assert_frame_equal(result, Ytot)

    def test_zero_value_produces_zero_rows(self, simple_weights: DisaggWeights) -> None:
        Ytot = pd.DataFrame(
            {"F01000": [0.0]},
            index=[ORIG],
        )
        result = apply_waste_disagg_to_Ytot(Ytot, simple_weights)
        for c in WASTE:
            assert result.loc[c, "F01000"] == pytest.approx(0.0, abs=1e-12)


# ===========================================================================
# Integration tests using real 2017 CSV weights
# ===========================================================================

_DATA_DIR = (
    pathlib.Path(__file__).resolve().parents[3]
    / "extract"
    / "disaggregation"
    / "waste_disagg_inputs"
)
_USE_PATH = _DATA_DIR / "WasteDisaggregationDetail2017_Use.csv"
_MAKE_PATH = _DATA_DIR / "WasteDisaggregationDetail2017_Make.csv"
_WASTE_CODES_2017 = cast(list[str], list(WASTE_DISAGG_COMMODITIES["562000"]))
_VA_ROWS = ["V00100", "V00200", "V00300"]
_ORIG = "562000"


@pytest.fixture(scope="module")
def weights_2017() -> DisaggWeights:
    cfg = EEIOWasteDisaggConfig(
        use_weights_file=str(_USE_PATH),
        make_weights_file=str(_MAKE_PATH),
        year=2017,
        source_name="WasteDisaggregationDetail2017",
    )
    return load_disagg_weights(
        cfg,
        original_code=_ORIG,
        new_codes=_WASTE_CODES_2017,
        disagg_sectors=_WASTE_CODES_2017,
        va_row_codes=_VA_ROWS,
    )


@pytest.fixture(scope="module")
def real_V() -> pd.DataFrame:
    """Cornerstone Make matrix (industry x commodity) from usa_matrices.
    May already have waste disaggregated (7 subsectors, no 562000) via correspondence.
    """
    return derive_V()


@pytest.fixture(scope="module")
def real_U() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cornerstone Use (Udom, Uimp), commodity x industry, from usa_matrices.
    May already have waste disaggregated (7 subsectors, no 562000).
    """
    uset = derive_U_with_negatives()
    return pd.DataFrame(uset.Udom), pd.DataFrame(uset.Uimp)


@pytest.fixture(scope="module")
def real_va() -> pd.DataFrame:
    """Cornerstone Value Added (VA rows x industry columns).
    Built as BEA VA @ industry_corresp().T to match usa_matrices sector space.
    May already have waste as 7 subsector columns (no 562000).
    """
    va_bea = load_2017_value_added_usa()
    return va_bea @ industry_corresp().T


@pytest.fixture(scope="module")
def real_Ytot() -> pd.DataFrame:
    """Cornerstone Final Demand (commodity x FD columns) from usa_matrices.
    May already have waste disaggregated (7 subsectors, no 562000 in index).
    """
    return derive_disagg_Ytot_with_trade()


def _build_V(waste_codes: list[str]) -> pd.DataFrame:
    """Build a toy Make matrix with realistic structure for 2017 integration tests."""
    other_industries = ["111CA0", "221300", "2332D0", "484000"]
    other_commodities = ["111CA0", "211000", "2332D0", "444000"]
    industries = other_industries + [_ORIG] + waste_codes
    commodities = other_commodities + [_ORIG] + waste_codes

    rng = np.random.default_rng(42)
    V = pd.DataFrame(0.0, index=industries, columns=commodities)
    for ind in other_industries:
        for com in other_commodities:
            V.loc[ind, com] = rng.uniform(10, 500)
    V.loc[_ORIG, _ORIG] = 1000.0
    for ind in other_industries:
        V.loc[ind, _ORIG] = rng.uniform(5, 100)
    for com in other_commodities:
        V.loc[_ORIG, com] = rng.uniform(5, 100)
    return V


def _build_U(waste_codes: list[str]) -> pd.DataFrame:
    """Build a toy Use matrix (index=commodities, columns=industries)."""
    other_commodities = ["111CA0", "211000", "325000"]
    other_industries = ["111CA0", "221300", "325000"]
    va_rows = _VA_ROWS
    fd_cols = ["F01000"]

    commodities = other_commodities + [_ORIG] + waste_codes + va_rows
    industries = other_industries + [_ORIG] + waste_codes + fd_cols

    rng = np.random.default_rng(99)
    U = pd.DataFrame(0.0, index=commodities, columns=industries)
    for com in other_commodities:
        for ind in other_industries:
            U.loc[com, ind] = rng.uniform(10, 200)
    U.loc[_ORIG, _ORIG] = 800.0
    for com in other_commodities:
        U.loc[com, _ORIG] = rng.uniform(5, 80)
    for ind in other_industries:
        U.loc[_ORIG, ind] = rng.uniform(5, 80)
    for va in va_rows:
        U.loc[va, _ORIG] = rng.uniform(10, 100)
        for ind in other_industries:
            U.loc[va, ind] = rng.uniform(10, 100)
    for fd in fd_cols:
        U.loc[_ORIG, fd] = rng.uniform(20, 200)
    return U


@pytest.mark.eeio_integration
class TestIntegrationV:
    def test_original_code_removed(
        self, weights_2017: DisaggWeights, real_V: pd.DataFrame
    ) -> None:
        result = apply_waste_disagg_to_V(real_V, weights_2017)
        assert _ORIG not in result.index
        assert _ORIG not in result.columns

    def test_waste_subsectors_present(
        self, weights_2017: DisaggWeights, real_V: pd.DataFrame
    ) -> None:
        result = apply_waste_disagg_to_V(real_V, weights_2017)
        for code in _WASTE_CODES_2017:
            assert code in result.index, f"{code} missing from index"
            assert code in result.columns, f"{code} missing from columns"

    def test_intersection_mass_preserved(
        self, weights_2017: DisaggWeights, real_V: pd.DataFrame
    ) -> None:
        result = apply_waste_disagg_to_V(real_V, weights_2017)
        if _ORIG in real_V.index and _ORIG in real_V.columns:
            orig_val = cast(float, real_V.loc[_ORIG, _ORIG])
        else:
            orig_val = float(
                real_V.loc[_WASTE_CODES_2017, _WASTE_CODES_2017].sum().sum()
            )
        intersection_sum = sum(
            cast(float, result.loc[i, j])
            for i in _WASTE_CODES_2017
            for j in _WASTE_CODES_2017
        )
        assert intersection_sum == pytest.approx(orig_val, rel=1e-6)

    def test_column_mass_preserved_per_industry(
        self, weights_2017: DisaggWeights, real_V: pd.DataFrame
    ) -> None:
        result = apply_waste_disagg_to_V(real_V, weights_2017)
        waste_set = set(_WASTE_CODES_2017)
        sample: list[tuple[str, float]] = []
        for ind in real_V.index:
            if ind == _ORIG or ind in waste_set:
                continue
            if _ORIG in real_V.columns:
                orig_val = cast(float, real_V.loc[ind, _ORIG])
            else:
                orig_val = float(real_V.loc[ind, _WASTE_CODES_2017].sum())
            if orig_val == 0.0:
                continue
            sample.append((ind, orig_val))
            if len(sample) >= 6:
                break
        for ind, orig_val in sample:
            disagg_sum = sum(cast(float, result.loc[ind, c]) for c in _WASTE_CODES_2017)
            assert disagg_sum == pytest.approx(
                orig_val, rel=1e-6
            ), f"Column mass not preserved for industry {ind}"

    def test_row_mass_preserved_per_commodity(
        self, weights_2017: DisaggWeights, real_V: pd.DataFrame
    ) -> None:
        result = apply_waste_disagg_to_V(real_V, weights_2017)
        waste_set = set(_WASTE_CODES_2017)
        sample: list[tuple[str, float]] = []
        for com in real_V.columns:
            if com == _ORIG or com in waste_set:
                continue
            if _ORIG in real_V.index:
                orig_val = cast(float, real_V.loc[_ORIG, com])
            else:
                orig_val = float(real_V.loc[_WASTE_CODES_2017, com].sum())
            if orig_val == 0.0:
                continue
            sample.append((com, orig_val))
            if len(sample) >= 6:
                break
        for com, orig_val in sample:
            disagg_sum = sum(cast(float, result.loc[i, com]) for i in _WASTE_CODES_2017)
            assert disagg_sum == pytest.approx(
                orig_val, rel=1e-6
            ), f"Row mass not preserved for commodity {com}"

    def test_non_waste_cells_unchanged(
        self, weights_2017: DisaggWeights, real_V: pd.DataFrame
    ) -> None:
        result = apply_waste_disagg_to_V(real_V, weights_2017)
        waste_set = set(_WASTE_CODES_2017)
        other = [i for i in real_V.index if i != _ORIG and i not in waste_set]
        if len(other) >= 1:
            i, j = other[0], other[0]
            assert cast(float, result.loc[i, j]) == pytest.approx(
                cast(float, real_V.loc[i, j])
            )

    def test_make_intersection_diagonal_dominant(
        self, weights_2017: DisaggWeights, real_V: pd.DataFrame
    ) -> None:
        """In the 2017 Make data, intersection is diagonal-only when disaggregating from 562000."""
        if _ORIG not in real_V.index or _ORIG not in real_V.columns:
            pytest.skip(
                "Cornerstone V already disaggregated; diagonal check applies to disaggregation output"
            )
        result = apply_waste_disagg_to_V(real_V, weights_2017)
        for i in _WASTE_CODES_2017:
            for j in _WASTE_CODES_2017:
                if i != j:
                    assert cast(float, result.loc[i, j]) == pytest.approx(
                        0.0, abs=1e-10
                    ), f"Off-diagonal ({i},{j}) should be ~0"


class TestIntegrationU:
    def test_original_code_removed(
        self,
        weights_2017: DisaggWeights,
        real_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        Udom_src, Uimp_src = real_U
        Udom, Uimp = apply_waste_disagg_to_U(Udom_src, Uimp_src, weights_2017)
        assert _ORIG not in Udom.index
        assert _ORIG not in Udom.columns

    def test_intersection_mass_preserved(
        self,
        weights_2017: DisaggWeights,
        real_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        Udom_src, Uimp_src = real_U
        Udom, _ = apply_waste_disagg_to_U(Udom_src, Uimp_src, weights_2017)
        if _ORIG in Udom_src.index and _ORIG in Udom_src.columns:
            orig_val = cast(float, Udom_src.loc[_ORIG, _ORIG])
        else:
            orig_val = float(
                Udom_src.loc[_WASTE_CODES_2017, _WASTE_CODES_2017].sum().sum()
            )
        intersection_sum = sum(
            cast(float, Udom.loc[c, i])
            for c in _WASTE_CODES_2017
            for i in _WASTE_CODES_2017
        )
        assert intersection_sum == pytest.approx(orig_val, rel=1e-6)

    def test_column_mass_preserved(
        self,
        weights_2017: DisaggWeights,
        real_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        Udom_src, Uimp_src = real_U
        Udom, _ = apply_waste_disagg_to_U(Udom_src, Uimp_src, weights_2017)
        waste_set = set(_WASTE_CODES_2017)
        sample: list[tuple[str, float]] = []
        for com in Udom_src.index:
            if com == _ORIG or com in waste_set:
                continue
            if _ORIG in Udom_src.columns:
                orig_val = cast(float, Udom_src.loc[com, _ORIG])
            else:
                orig_val = float(Udom_src.loc[com, _WASTE_CODES_2017].sum())
            if orig_val == 0.0:
                continue
            sample.append((com, orig_val))
            if len(sample) >= 6:
                break
        for com, orig_val in sample:
            disagg_sum = sum(cast(float, Udom.loc[com, i]) for i in _WASTE_CODES_2017)
            assert disagg_sum == pytest.approx(
                orig_val, rel=1e-6
            ), f"Column mass not preserved for commodity {com}"

    def test_row_mass_preserved(
        self,
        weights_2017: DisaggWeights,
        real_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        Udom_src, Uimp_src = real_U
        Udom, _ = apply_waste_disagg_to_U(Udom_src, Uimp_src, weights_2017)
        waste_set = set(_WASTE_CODES_2017)
        sample: list[tuple[str, float]] = []
        for ind in Udom_src.columns:
            if ind == _ORIG or ind in waste_set:
                continue
            if _ORIG in Udom_src.index:
                orig_val = cast(float, Udom_src.loc[_ORIG, ind])
            else:
                orig_val = float(Udom_src.loc[_WASTE_CODES_2017, ind].sum())
            if orig_val == 0.0:
                continue
            sample.append((ind, orig_val))
            if len(sample) >= 6:
                break
        for ind, orig_val in sample:
            disagg_sum = sum(cast(float, Udom.loc[c, ind]) for c in _WASTE_CODES_2017)
            assert disagg_sum == pytest.approx(
                orig_val, rel=1e-6
            ), f"Row mass not preserved for industry {ind}"

    def test_non_waste_unchanged(
        self,
        weights_2017: DisaggWeights,
        real_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        Udom_src, Uimp_src = real_U
        Udom, _ = apply_waste_disagg_to_U(Udom_src, Uimp_src, weights_2017)
        waste_set = set(_WASTE_CODES_2017)
        other_com = [c for c in Udom_src.index if c != _ORIG and c not in waste_set]
        other_ind = [i for i in Udom_src.columns if i != _ORIG and i not in waste_set]
        if other_com and other_ind:
            c, i = other_com[0], other_ind[0]
            assert cast(float, Udom.loc[c, i]) == pytest.approx(
                cast(float, Udom_src.loc[c, i])
            )

    def test_va_rows_not_disaggregated_by_U_helper(
        self,
        weights_2017: DisaggWeights,
        real_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        """VA rows are excluded from Use column disaggregation (handled by VA helper)."""
        Udom_src, Uimp_src = real_U
        if not all(va in Udom_src.index for va in _VA_ROWS):
            pytest.skip("Cornerstone Use table has no VA rows (VA is separate)")
        Udom, _ = apply_waste_disagg_to_U(Udom_src, Uimp_src, weights_2017)
        for va_row in _VA_ROWS:
            for ind in _WASTE_CODES_2017:
                assert cast(float, Udom.loc[va_row, ind]) == pytest.approx(
                    0.0, abs=1e-12
                ), f"VA row {va_row} should not be split by U helper for industry {ind}"


class TestIntegrationVA:
    def test_va_mass_preserved_per_row(
        self, weights_2017: DisaggWeights, real_va: pd.DataFrame
    ) -> None:
        va_rows = [r for r in _VA_ROWS if r in real_va.index]
        if not va_rows:
            pytest.skip("Cornerstone VA table has none of V00100/V00200/V00300")
        result = apply_waste_disagg_to_VA(real_va, weights_2017)

        assert _ORIG not in result.columns
        for va_row in va_rows:
            if _ORIG in real_va.columns:
                orig_val = cast(float, real_va.loc[va_row, _ORIG])
            else:
                row_series = real_va.loc[va_row]
                orig_val = float(row_series.loc[_WASTE_CODES_2017].sum())
            disagg_sum = sum(
                cast(float, result.loc[va_row, ind]) for ind in _WASTE_CODES_2017
            )
            assert disagg_sum == pytest.approx(
                orig_val, rel=1e-6
            ), f"VA mass not preserved for {va_row}"

    def test_va_non_waste_unchanged(
        self, weights_2017: DisaggWeights, real_va: pd.DataFrame
    ) -> None:
        va_rows = [r for r in _VA_ROWS if r in real_va.index]
        if not va_rows:
            pytest.skip("Cornerstone VA table has none of V00100/V00200/V00300")
        result = apply_waste_disagg_to_VA(real_va, weights_2017)
        waste_set = set(_WASTE_CODES_2017)
        sample_industries = [
            ind for ind in real_va.columns if ind != _ORIG and ind not in waste_set
        ][:5]
        for va_row in va_rows:
            for ind in sample_industries:
                if ind in result.columns:
                    assert cast(float, result.loc[va_row, ind]) == pytest.approx(
                        cast(float, real_va.loc[va_row, ind])
                    )

    def test_va_all_three_rows_get_same_proportions(
        self, weights_2017: DisaggWeights
    ) -> None:
        """V00100/V00200/V00300 have identical raw VA weights, so all three rows
        should get the same proportions (though different absolute values)."""
        va = pd.DataFrame(
            {_ORIG: [100.0, 200.0, 300.0], "IND_A": [10.0, 20.0, 30.0]},
            index=_VA_ROWS,
        )
        result = apply_waste_disagg_to_VA(va, weights_2017)

        proportions = []
        for va_row in _VA_ROWS:
            row_total = sum(
                cast(float, result.loc[va_row, c]) for c in _WASTE_CODES_2017
            )
            props = [
                cast(float, result.loc[va_row, c]) / row_total
                for c in _WASTE_CODES_2017
            ]
            proportions.append(props)

        for i in range(1, len(proportions)):
            assert proportions[i] == pytest.approx(proportions[0], rel=1e-6)


class TestIntegrationYtot:
    def test_fd_mass_preserved_per_column(
        self, weights_2017: DisaggWeights, real_Ytot: pd.DataFrame
    ) -> None:
        result = apply_waste_disagg_to_Ytot(real_Ytot, weights_2017)
        assert _ORIG not in result.index
        sample_fd_cols = [
            fd
            for fd in real_Ytot.columns
            if (_ORIG in real_Ytot.index and cast(float, real_Ytot.loc[_ORIG, fd]) != 0)
            or (
                _ORIG not in real_Ytot.index
                and float(real_Ytot.loc[_WASTE_CODES_2017, fd].sum()) != 0.0
            )
        ][:6]
        for fd_col in sample_fd_cols:
            if _ORIG in real_Ytot.index:
                orig_val = cast(float, real_Ytot.loc[_ORIG, fd_col])
            else:
                orig_val = float(real_Ytot.loc[_WASTE_CODES_2017, fd_col].sum())
            disagg_sum = sum(
                cast(float, result.loc[c, fd_col]) for c in _WASTE_CODES_2017
            )
            assert disagg_sum == pytest.approx(
                orig_val, rel=1e-6
            ), f"FD mass not preserved for {fd_col}"

    def test_fd_non_waste_rows_unchanged(
        self, weights_2017: DisaggWeights, real_Ytot: pd.DataFrame
    ) -> None:
        waste_set = set(_WASTE_CODES_2017)
        other_com = [c for c in real_Ytot.index if c != _ORIG and c not in waste_set]
        if not other_com or not real_Ytot.columns.size:
            pytest.skip("Need another commodity and at least one FD column")
        result = apply_waste_disagg_to_Ytot(real_Ytot, weights_2017)
        fd_col = real_Ytot.columns[0]
        com = other_com[0]
        assert cast(float, result.loc[com, fd_col]) == pytest.approx(
            cast(float, real_Ytot.loc[com, fd_col])
        )

    def test_known_fd_col_uses_specific_weights(
        self, weights_2017: DisaggWeights, real_Ytot: pd.DataFrame
    ) -> None:
        """F01000 has specific weights in the Use CSV; verify they are applied."""
        fd_w = weights_2017.use_fd_columns_for_disagg_commodity_rows
        if "F01000" not in fd_w.index:
            pytest.skip("F01000 not in waste FD weights")
        if "F01000" not in real_Ytot.columns:
            pytest.skip("Cornerstone Ytot has no F01000 column")
        Ytot = real_Ytot[["F01000"]].copy()
        if _ORIG in Ytot.index:
            orig_val = cast(float, Ytot.loc[_ORIG, "F01000"])
        else:
            orig_val = float(Ytot.loc[_WASTE_CODES_2017, "F01000"].sum())
        if orig_val == 0.0:
            pytest.skip("F01000 has zero mass for waste group in real Ytot")
        result = apply_waste_disagg_to_Ytot(Ytot, weights_2017)
        for com in _WASTE_CODES_2017:
            expected = orig_val * cast(float, fd_w.loc["F01000", com])
            assert cast(float, result.loc[com, "F01000"]) == pytest.approx(
                expected, rel=1e-6
            ), f"F01000 weight mismatch for {com}"

    def test_unknown_fd_col_uses_fallback(self, weights_2017: DisaggWeights) -> None:
        """FD columns not in the weights CSV should fall back to intersection marginals."""
        Ytot = pd.DataFrame({"F99999": [1000.0]}, index=[_ORIG])
        result = apply_waste_disagg_to_Ytot(Ytot, weights_2017)

        col_sum = sum(cast(float, result.loc[c, "F99999"]) for c in _WASTE_CODES_2017)
        assert col_sum == pytest.approx(1000.0, rel=1e-6)


# ---------------------------------------------------------------------------
# Make/VA reconcile to Use commodity identity
# ---------------------------------------------------------------------------

_TOY_WASTE = ["562111", "562HAZ", "562212"]


def _toy_balanced_inputs() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    """Tiny V/U/Y/VA with parent 562000 present for reconcile tests.

    Parent Make column mass (60) is below sum(q_target)=120 so diagonal residuals
    stay positive; Use waste-industry columns stay below Make rows for VA balance.
    """
    inds = ["111000", "484000", "221300", *_TOY_WASTE, "562000"]
    coms = ["111000", "211000", *_TOY_WASTE, "562000"]
    V = pd.DataFrame(0.0, index=inds, columns=coms)
    # Parent Make column mass (non-waste industries × waste commodities)
    V.loc["111000", "562000"] = 30.0
    V.loc["484000", "562000"] = 10.0
    V.loc["221300", "562000"] = 20.0
    # Parent Make row mass (waste industries × non-waste commodities)
    V.loc["562000", "111000"] = 40.0
    V.loc["562000", "211000"] = 20.0
    V.loc["562000", "562000"] = 10.0
    # Non-waste block
    V.loc["111000", "111000"] = 500.0
    V.loc["484000", "111000"] = 50.0

    Udom = pd.DataFrame(0.0, index=coms, columns=inds)
    # Waste commodity rows → industry purchases (build q_target = 60/30/30)
    Udom.loc["562111", "111000"] = 40.0
    Udom.loc["562HAZ", "111000"] = 20.0
    Udom.loc["562212", "111000"] = 20.0
    # Waste industry columns (Use of waste industries)
    Udom.loc["111000", "562111"] = 10.0
    Udom.loc["111000", "562HAZ"] = 5.0
    Udom.loc["111000", "562212"] = 5.0
    Udom.loc["562111", "562111"] = 2.0
    Uimp = pd.DataFrame(0.0, index=coms, columns=inds)

    Y = pd.DataFrame(0.0, index=coms, columns=["F01000"])
    Y.loc["562111", "F01000"] = 20.0
    Y.loc["562HAZ", "F01000"] = 10.0
    Y.loc["562212", "F01000"] = 10.0

    VA = pd.DataFrame(0.0, index=["V00100", "V00300"], columns=inds)
    for w in _TOY_WASTE:
        VA.loc["V00100", w] = 3.0
        VA.loc["V00300", w] = 1.0
    VA.loc["V00100", "111000"] = 100.0
    return V, Udom, Uimp, VA, Y


class TestComputeWasteQTargets:
    def test_algebra_matches_udom_y_uimp(self) -> None:
        _V, Udom, Uimp, _VA, Y = _toy_balanced_inputs()
        q = compute_waste_q_targets(Udom, Uimp, Y, waste_codes=_TOY_WASTE)
        for c in _TOY_WASTE:
            expected = (
                float(Udom.loc[c].sum())
                + float(Y.loc[c].sum())
                + float(Uimp.loc[c].sum())
            )
            assert float(q.loc[c]) == pytest.approx(expected)


class TestMakeColumnDefaultShares:
    def test_negative_q_target_clipped_before_normalize(self) -> None:
        """Review #1253 (6): negative targets must not create >100% / neg shares."""
        codes = ["562111", "562HAZ", "562212"]
        q = pd.Series({"562111": 100.0, "562HAZ": -10.0, "562212": 0.0})
        with pytest.warns(UserWarning, match="negative q_target clipped"):
            shares = _make_column_default_shares(q, codes)
        assert float(shares.sum()) == pytest.approx(1.0)
        assert float(shares.loc["562111"]) == pytest.approx(1.0)
        assert float(shares.loc["562HAZ"]) == pytest.approx(0.0)
        assert float(shares.loc["562212"]) == pytest.approx(0.0)
        # GSLGO inherits the clipped defaults (no negative mass on members).
        gslgo = _make_column_gslgo_shares(shares, codes)
        assert float(gslgo.loc["562HAZ"]) == pytest.approx(0.0)
        assert float(gslgo.sum()) == pytest.approx(1.0)
        assert all(float(v) >= 0.0 for v in gslgo)


class TestReconcileWasteMakeAndVa:
    def test_columns_match_q_target_and_offdiag_zero(self) -> None:
        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        V_out, VA_out = reconcile_waste_make_and_va_to_use(
            V, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
        )
        q = compute_waste_q_targets(Udom, Uimp, Y, waste_codes=_TOY_WASTE)
        for c in _TOY_WASTE:
            assert float(V_out[c].sum()) == pytest.approx(
                float(cast(float, q.loc[c])), abs=1e-9
            )
        for i in _TOY_WASTE:
            for j in _TOY_WASTE:
                if i != j:
                    assert float(cast(float, V_out.loc[i, j])) == pytest.approx(0.0)

    def test_onehot_and_gslgo_assignment_rules(self) -> None:
        assert MAKE_COLUMN_ONEHOT_INDUSTRIES["484000"] == "562111"
        assert "221300" in MAKE_COLUMN_GSLGO_INDUSTRIES
        assert GSLGO_MEMBERSHIP == frozenset({"562111", "562212", "562213", "562920"})
        assert MAKE_ROW_ONEHOT_COMMODITIES["211000"] == "562212"

        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        V_out, _ = reconcile_waste_make_and_va_to_use(
            V, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
        )
        # 484000 → one-hot 562111
        assert float(cast(float, V_out.loc["484000", "562111"])) == pytest.approx(10.0)
        assert float(cast(float, V_out.loc["484000", "562HAZ"])) == pytest.approx(0.0)
        # 211000 commodity → one-hot 562212 industry row
        assert float(cast(float, V_out.loc["562212", "211000"])) == pytest.approx(20.0)
        assert float(cast(float, V_out.loc["562111", "211000"])) == pytest.approx(0.0)

    def test_parent_sum_collapse_when_562000_absent(self) -> None:
        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        # Expand parent into equal children first (simulates correspondence)
        V_exp = V.drop(index="562000", columns="562000", errors="ignore").copy()
        for w in _TOY_WASTE:
            if w not in V_exp.index:
                V_exp.loc[w] = 0.0
            if w not in V_exp.columns:
                V_exp[w] = 0.0
        # Distribute parent border equally across children
        for ind in ["111000", "484000", "221300"]:
            for w in _TOY_WASTE:
                V_exp.loc[ind, w] = float(cast(float, V.loc[ind, "562000"])) / 3.0
        for com in ["111000", "211000"]:
            for w in _TOY_WASTE:
                V_exp.loc[w, com] = float(cast(float, V.loc["562000", com])) / 3.0

        V_out, _ = reconcile_waste_make_and_va_to_use(
            V_exp, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
        )
        # Parent column mass for 111000 recovered as sum of children (=30)
        border = pd.Series(V_out.loc["111000"].reindex(_TOY_WASTE), dtype=float)
        assert float(border.sum()) == pytest.approx(30.0)
        # Not a single-child understatement (would be 10)
        assert float(border.max()) < 30.0

    def test_partial_children_without_parent_raises_clear_error(self) -> None:
        """Review #1253 (5): partial child set must name which codes are missing."""
        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        V = V.drop(
            index=["562000", "562HAZ", "562212"],
            columns=["562000", "562HAZ", "562212"],
            errors="ignore",
        )
        # Only 562111 remains — incomplete correspondence / IO vintage vs weights.
        with pytest.raises(ValueError, match="index missing=.*562HAZ"):
            reconcile_waste_make_and_va_to_use(
                V, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
            )

    def test_onehot_overflow_scales_nonwaste_diagonal_zero(self) -> None:
        """Review #1253 (4): one-hot mass > q_target scales down; no neg diagonal."""
        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        # 484000 → one-hot 562111; dump more parent mass than q_target[562111].
        V.loc["484000", "562000"] = 100.0
        V.loc["562000", "111000"] = 500.0
        for w in _TOY_WASTE:
            Udom.loc[:, w] = 0.0
            Uimp.loc[:, w] = 0.0
        q = compute_waste_q_targets(Udom, Uimp, Y, waste_codes=_TOY_WASTE)
        qt_111 = float(cast(float, q.loc["562111"]))
        with pytest.warns(UserWarning, match="scaled non-waste"):
            V_out, _ = reconcile_waste_make_and_va_to_use(
                V, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
            )
        assert float(cast(float, V_out.loc["562111", "562111"])) == pytest.approx(0.0)
        assert float(V_out["562111"].sum()) == pytest.approx(qt_111, abs=1e-9)
        # One-hot cell was scaled down from 100, not deleted via whole-column wipe.
        assert 0.0 < float(cast(float, V_out.loc["484000", "562111"])) < 100.0

    def test_nonpositive_q_target_redistributes_nonwaste_mass(self) -> None:
        """Review #1253 (3): qt<=0 must not wipe one-hot mass; move it to siblings."""
        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        # Force q_target[562111] == 0 while 484000 still one-hots onto 562111.
        Udom.loc["562111", :] = 0.0
        Uimp.loc["562111", :] = 0.0
        Y.loc["562111", :] = 0.0
        # Only the one-hot industry carries parent column mass so siblings have
        # room after redistribution (no secondary scale-down of the moved cells).
        V.loc["111000", "562000"] = 0.0
        V.loc["221300", "562000"] = 0.0
        V.loc["484000", "562000"] = 10.0
        with pytest.warns(UserWarning, match="redistributed non-waste Make"):
            V_out, _ = reconcile_waste_make_and_va_to_use(
                V, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
            )
        assert float(cast(float, V_out.loc["484000", "562111"])) == pytest.approx(0.0)
        # Parent one-hot mass preserved on the other waste commodities.
        moved = float(V_out.loc["484000", ["562HAZ", "562212"]].sum())
        assert moved == pytest.approx(10.0, abs=1e-9)
        assert float(V_out["562111"].sum()) == pytest.approx(0.0, abs=1e-9)

    def test_nonwaste_block_and_va_unchanged(self) -> None:
        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        V_out, VA_out = reconcile_waste_make_and_va_to_use(
            V, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
        )
        assert float(cast(float, V_out.loc["111000", "111000"])) == pytest.approx(500.0)
        assert float(cast(float, VA_out.loc["V00100", "111000"])) == pytest.approx(
            100.0
        )

    def test_waste_va_balances_make_row(self) -> None:
        V, Udom, Uimp, VA, Y = _toy_balanced_inputs()
        V_out, VA_out = reconcile_waste_make_and_va_to_use(
            V, Udom, Uimp, VA, Y, waste_codes=_TOY_WASTE
        )
        for ind in _TOY_WASTE:
            make_row = float(V_out.loc[ind].sum())
            use_col = float(Udom.loc[:, ind].sum()) + float(Uimp.loc[:, ind].sum())
            va = float(VA_out[ind].sum())
            tol = max(DISAGG_BALANCE_ATOL, 0.01 * abs(make_row))
            assert abs(make_row - use_col - va) <= tol
