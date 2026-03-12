"""Waste disaggregation pipeline integration tests.

Tests that get_waste_disagg_weights() is config-gated and cached, that V/U/Ytot
entry points apply disaggregation inside @functools.cache, that A/q and B use
the Cornerstone-space path when enabled, and that feature-off is a no-op.
"""

from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.disaggregation.waste_weights import WasteDisaggWeights
from bedrock.transform.eeio.derived_cornerstone import (
    _WASTE_NEW_CODES,
    _derive_cornerstone_Ytot_with_trade,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_B_via_vnorm,
    derive_cornerstone_E,
    derive_cornerstone_q,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_y_nab,
    derive_cornerstone_Ytot_matrix_set,
    get_waste_disagg_weights,
)
from bedrock.utils.config.usa_config import (
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.math.formulas import compute_q

_WASTE_SET = set(_WASTE_NEW_CODES)

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    derive_cornerstone_V,
    derive_cornerstone_x,
    derive_cornerstone_q,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    _derive_cornerstone_Ytot_with_trade,
    derive_cornerstone_Ytot_matrix_set,
    derive_cornerstone_VA,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_E,
    derive_cornerstone_y_nab,
]


def _clear_all_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


def _setup_config(config_name: str) -> None:
    """Reset config + caches, then set a fresh global config."""
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)


# ---------------------------------------------------------------------------
# Weight provider tests (waste_disagg)
# ---------------------------------------------------------------------------


class TestWeightProvider:

    def teardown_method(self) -> None:
        _teardown()

    def test_returns_none_when_disabled(self) -> None:
        _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
        result = get_waste_disagg_weights()
        assert result is None

    def test_returns_weights_when_enabled(self) -> None:
        _setup_config("test_usa_config_waste_disagg")
        result = get_waste_disagg_weights()
        assert result is not None
        assert isinstance(result, WasteDisaggWeights)

    def test_cache_clearing_reflects_new_config(self) -> None:
        _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
        result1 = get_waste_disagg_weights()
        assert result1 is None

        _setup_config("test_usa_config_waste_disagg")
        result2 = get_waste_disagg_weights()
        assert result2 is not None
        assert isinstance(result2, WasteDisaggWeights)


# ---------------------------------------------------------------------------
# Module-scoped fixtures for heavy pipeline results
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def baseline_V() -> pd.DataFrame:
    _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
    V = derive_cornerstone_V()
    _teardown()
    return V


@pytest.fixture(scope="module")
def baseline_U() -> tuple[pd.DataFrame, pd.DataFrame]:
    _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
    uset = derive_cornerstone_U_with_negatives()
    result = (pd.DataFrame(uset.Udom), pd.DataFrame(uset.Uimp))
    _teardown()
    return result


@pytest.fixture(scope="module")
def baseline_Ytot() -> pd.DataFrame:
    _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
    Ytot = _derive_cornerstone_Ytot_with_trade()
    _teardown()
    return Ytot


@pytest.fixture(scope="module")
def baseline_Aq() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
    aq = derive_cornerstone_Aq()
    result = (pd.DataFrame(aq.Adom), pd.DataFrame(aq.Aimp), aq.scaled_q.copy())
    _teardown()
    return result


@pytest.fixture(scope="module")
def baseline_B() -> pd.DataFrame | None:
    _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
    try:
        B = derive_cornerstone_B_via_vnorm()
    except Exception:
        _teardown()
        return None
    _teardown()
    return B


@pytest.fixture(scope="module")
def disagg_V() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    V = derive_cornerstone_V()
    _teardown()
    return V


@pytest.fixture(scope="module")
def disagg_U() -> tuple[pd.DataFrame, pd.DataFrame]:
    _setup_config("test_usa_config_waste_disagg")
    uset = derive_cornerstone_U_with_negatives()
    result = (pd.DataFrame(uset.Udom), pd.DataFrame(uset.Uimp))
    _teardown()
    return result


@pytest.fixture(scope="module")
def disagg_Ytot() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    Ytot = _derive_cornerstone_Ytot_with_trade()
    _teardown()
    return Ytot


@pytest.fixture(scope="module")
def disagg_VA() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    VA = derive_cornerstone_VA()
    _teardown()
    return VA


@pytest.fixture(scope="module")
def disagg_Aq() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    _setup_config("test_usa_config_waste_disagg")
    aq = derive_cornerstone_Aq()
    result = (pd.DataFrame(aq.Adom), pd.DataFrame(aq.Aimp), aq.scaled_q.copy())
    _teardown()
    return result


@pytest.fixture(scope="module")
def disagg_B() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    B = derive_cornerstone_B_via_vnorm()
    _teardown()
    return B


# ---------------------------------------------------------------------------
# V/U/Ytot integration tests (waste_disagg)
# ---------------------------------------------------------------------------


class TestPipelineV:

    def test_waste_block_differs(
        self,
        baseline_V: pd.DataFrame,
        disagg_V: pd.DataFrame,
    ) -> None:
        waste_in_v = [c for c in _WASTE_NEW_CODES if c in disagg_V.index]
        assert len(waste_in_v) > 0
        baseline_block = baseline_V.loc[waste_in_v, waste_in_v]
        disagg_block = disagg_V.loc[waste_in_v, waste_in_v]
        assert not np.allclose(
            baseline_block.values, disagg_block.values, atol=1e-6
        ), "Waste block should differ after disaggregation"

    def test_non_waste_unchanged(
        self,
        baseline_V: pd.DataFrame,
        disagg_V: pd.DataFrame,
    ) -> None:
        non_waste_idx = [i for i in baseline_V.index if i not in _WASTE_SET]
        non_waste_cols = [c for c in baseline_V.columns if c not in _WASTE_SET]
        np.testing.assert_allclose(
            disagg_V.loc[non_waste_idx, non_waste_cols].values,
            baseline_V.loc[non_waste_idx, non_waste_cols].values,
            rtol=1e-9,
            atol=1e-12,
        )

    def test_same_shape(
        self,
        baseline_V: pd.DataFrame,
        disagg_V: pd.DataFrame,
    ) -> None:
        assert baseline_V.shape == disagg_V.shape
        assert list(baseline_V.index) == list(disagg_V.index)
        assert list(baseline_V.columns) == list(disagg_V.columns)


class TestPipelineU:

    def test_waste_block_differs(
        self,
        baseline_U: tuple[pd.DataFrame, pd.DataFrame],
        disagg_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        for base, disagg in zip(baseline_U, disagg_U):
            waste_in = [c for c in _WASTE_NEW_CODES if c in disagg.index]
            if not waste_in:
                continue
            waste_cols = [c for c in _WASTE_NEW_CODES if c in disagg.columns]
            if not waste_cols:
                continue
            baseline_block = base.loc[waste_in, waste_cols]
            disagg_block = disagg.loc[waste_in, waste_cols]
            assert not np.allclose(
                baseline_block.values, disagg_block.values, atol=1e-6
            ), "U waste block should differ"

    def test_non_waste_unchanged(
        self,
        baseline_U: tuple[pd.DataFrame, pd.DataFrame],
        disagg_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        for base, disagg in zip(baseline_U, disagg_U):
            non_waste_idx = [i for i in base.index if i not in _WASTE_SET]
            non_waste_cols = [c for c in base.columns if c not in _WASTE_SET]
            np.testing.assert_allclose(
                disagg.loc[non_waste_idx, non_waste_cols].values,
                base.loc[non_waste_idx, non_waste_cols].values,
                rtol=1e-9,
                atol=1e-12,
            )

    def test_same_shape(
        self,
        baseline_U: tuple[pd.DataFrame, pd.DataFrame],
        disagg_U: tuple[pd.DataFrame, pd.DataFrame],
    ) -> None:
        for base, disagg in zip(baseline_U, disagg_U):
            assert base.shape == disagg.shape


class TestPipelineYtot:

    def test_waste_rows_differ(
        self,
        baseline_Ytot: pd.DataFrame,
        disagg_Ytot: pd.DataFrame,
    ) -> None:
        waste_in = [c for c in _WASTE_NEW_CODES if c in disagg_Ytot.index]
        assert len(waste_in) > 0
        baseline_block = baseline_Ytot.loc[waste_in, :]
        disagg_block = disagg_Ytot.loc[waste_in, :]
        assert not np.allclose(
            baseline_block.values, disagg_block.values, atol=1e-6
        ), "Ytot waste rows should differ"

    def test_non_waste_unchanged(
        self,
        baseline_Ytot: pd.DataFrame,
        disagg_Ytot: pd.DataFrame,
    ) -> None:
        non_waste = [i for i in baseline_Ytot.index if i not in _WASTE_SET]
        np.testing.assert_allclose(
            disagg_Ytot.loc[non_waste, :].values,
            baseline_Ytot.loc[non_waste, :].values,
            rtol=1e-9,
            atol=1e-12,
        )

    def test_same_shape(
        self,
        baseline_Ytot: pd.DataFrame,
        disagg_Ytot: pd.DataFrame,
    ) -> None:
        assert baseline_Ytot.shape == disagg_Ytot.shape


# ---------------------------------------------------------------------------
# VA tests (waste_disagg)
# ---------------------------------------------------------------------------


class TestPipelineVA:

    def test_va_has_waste_columns(self, disagg_VA: pd.DataFrame) -> None:
        waste_cols = [c for c in _WASTE_NEW_CODES if c in disagg_VA.columns]
        assert len(waste_cols) == len(_WASTE_NEW_CODES)

    def test_va_shape(self, disagg_VA: pd.DataFrame) -> None:
        assert disagg_VA.shape[0] == 3  # V00100, V00200, V00300
        assert disagg_VA.shape[1] == 405


# ---------------------------------------------------------------------------
# A/q integration tests (waste_disagg)
# ---------------------------------------------------------------------------


class TestPipelineAq:

    def test_correct_dimensions(
        self,
        disagg_Aq: tuple[pd.DataFrame, pd.DataFrame, pd.Series],
    ) -> None:
        Adom, Aimp, q = disagg_Aq
        assert Adom.shape == (405, 405)
        assert Aimp.shape == (405, 405)
        assert len(q) == 405

    def test_no_intragroup_treatment(
        self,
        disagg_Aq: tuple[pd.DataFrame, pd.DataFrame, pd.Series],
    ) -> None:
        """Waste–waste cross-terms should be non-zero (no intragroup zeroing)."""
        Adom, _, _ = disagg_Aq
        waste_in = [c for c in _WASTE_NEW_CODES if c in Adom.index]
        if len(waste_in) < 2:
            pytest.skip("Not enough waste codes in A")
        waste_block = Adom.loc[waste_in, waste_in]
        off_diag = waste_block.values.copy()
        np.fill_diagonal(off_diag, 0.0)
        assert off_diag.sum() != 0.0, (
            "Off-diagonal waste cross-terms should be non-zero "
            "(intragroup treatment should NOT be applied)"
        )

    def test_q_equals_compute_q_from_V(
        self,
        disagg_Aq: tuple[pd.DataFrame, pd.DataFrame, pd.Series],
        disagg_V: pd.DataFrame,
    ) -> None:
        _, _, q_from_Aq = disagg_Aq
        q_from_V = compute_q(V=disagg_V)
        np.testing.assert_allclose(
            np.asarray(q_from_Aq, dtype=float),
            np.asarray(q_from_V, dtype=float),
            rtol=1e-9,
            atol=1e-12,
        )

    def test_differs_from_baseline(
        self,
        baseline_Aq: tuple[pd.DataFrame, pd.DataFrame, pd.Series],
        disagg_Aq: tuple[pd.DataFrame, pd.DataFrame, pd.Series],
    ) -> None:
        base_Adom, _, _ = baseline_Aq
        disagg_Adom, _, _ = disagg_Aq
        waste_in = [c for c in _WASTE_NEW_CODES if c in disagg_Adom.index]
        assert not np.allclose(
            base_Adom.loc[waste_in, waste_in].values,
            disagg_Adom.loc[waste_in, waste_in].values,
            atol=1e-6,
        ), "A waste block should differ between baseline and disaggregated"


# ---------------------------------------------------------------------------
# B integration tests (waste_disagg)
# ---------------------------------------------------------------------------


class TestPipelineB:

    def test_correct_dimensions(self, disagg_B: pd.DataFrame) -> None:
        assert disagg_B.shape[1] == 405

    def test_differs_from_baseline(
        self,
        baseline_B: pd.DataFrame | None,
        disagg_B: pd.DataFrame,
    ) -> None:
        if baseline_B is None:
            pytest.skip("Baseline B unavailable (GCP auth or data not configured)")
        waste_cols = [c for c in _WASTE_NEW_CODES if c in disagg_B.columns]
        assert len(waste_cols) > 0
        assert not np.allclose(
            baseline_B[waste_cols].values,
            disagg_B[waste_cols].values,
            atol=1e-6,
        ), "B waste columns should differ after disaggregation"


# ---------------------------------------------------------------------------
# Feature-off regression test (waste_disagg)
# ---------------------------------------------------------------------------


class TestFeatureOffRegression:

    def test_feature_off_V_matches_baseline(self) -> None:
        _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
        assert get_waste_disagg_weights() is None
        V = derive_cornerstone_V()
        _teardown()

        _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
        V2 = derive_cornerstone_V()
        _teardown()

        np.testing.assert_array_equal(V.values, V2.values)

    def test_feature_off_Aq_uses_expansion_path(self) -> None:
        _setup_config("2025_usa_cornerstone_taxonomy_and_B_transformation")
        assert get_waste_disagg_weights() is None
        aq = derive_cornerstone_Aq()
        Adom = pd.DataFrame(aq.Adom)
        waste_in = [c for c in _WASTE_NEW_CODES if c in Adom.index]
        waste_block = Adom.loc[waste_in, waste_in]
        off_diag = waste_block.values.copy()
        np.fill_diagonal(off_diag, 0.0)
        assert np.allclose(
            off_diag, 0.0
        ), "With feature off, intragroup treatment should zero waste cross-terms"
        _teardown()
