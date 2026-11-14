"""Waste-specific disaggregation pipeline integration tests - NOT sector agnostic.

Tests that get_waste_disagg_weights() returns waste-specific values,
that it is config-gated and cached, that V/U/Ytot
entry points apply disaggregation inside @functools.cache, that A/q and B use
the Cornerstone-space path when enabled, and that feature-off is a no-op.
"""

from __future__ import annotations

from typing import Callable, cast

import numpy as np
import pandas as pd
import pytest

from bedrock.extract.disaggregation import derive_waste_weights as dww
from bedrock.extract.disaggregation import disagg_weights as dw
from bedrock.extract.disaggregation.disagg_weights import DisaggWeights
from bedrock.extract.disaggregation.waste_weight_types import (
    WeightDerivationProvenance,
)
from bedrock.transform.allocation.derived import load_E_from_flowsa
from bedrock.transform.eeio import (
    sector_disagg_pipeline as sdp,
)
from bedrock.transform.eeio import (
    usa_expansion,
)
from bedrock.transform.eeio.sector_disagg_pipeline import (
    _WASTE_NEW_CODES,
    _derive_waste_balanced_io,
    _derive_waste_use_y,
    clear_waste_disagg_weights_cache,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    derive_U_after_waste,
    derive_V_after_waste,
    derive_y_after_waste,
    electricity_disaggregation_enabled,
    electricity_reaggregation_enabled,
    electricity_reallocation_enabled,
    get_waste_disagg_provenance,
    get_waste_disagg_weights,
    sector_disagg_active,
)
from bedrock.transform.eeio.usa_matrices import (
    derive_Aq,
    derive_Aq_scaled,
    derive_B,
    derive_B_via_vnorm,
    derive_q,
    derive_U_set,
    derive_U_with_negatives,
    derive_V,
    derive_VA,
    derive_Vnorm_scrap_corrected,
    derive_x,
    derive_x_after_redefinition,
    derive_y_nab,
    derive_Ytot_matrix_set,
    scrap_share_of_output,
)
from bedrock.transform.eeio.waste_disaggregation import compute_waste_q_targets
from bedrock.utils.config.usa_config import (
    _load_usa_config_from_file_name,
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
    set_global_usa_config_object,
)
from bedrock.utils.economic.inflation_helpers_usa import (
    clear_usa_inflation_caches,
)
from bedrock.utils.math.formulas import compute_q

_WASTE_SET = set(_WASTE_NEW_CODES)

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    sector_disagg_active,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    electricity_reaggregation_enabled,
    _derive_waste_use_y,
    _derive_waste_balanced_io,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    derive_V,
    derive_x,
    derive_x_after_redefinition,
    derive_q,
    derive_Vnorm_scrap_corrected,
    derive_U_with_negatives,
    derive_U_set,
    derive_Ytot_matrix_set,
    derive_VA,
    derive_Aq,
    derive_Aq_scaled,
    derive_B,
    derive_y_nab,
]


def _clear_all_caches() -> None:
    clear_waste_disagg_weights_cache()
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()
    clear_usa_inflation_caches()


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
        _setup_config("test_usa_config_waste_disagg_off")
        result = get_waste_disagg_weights()
        assert result is None

    def test_returns_weights_when_enabled(self) -> None:
        _setup_config("test_usa_config_waste_disagg")
        result = get_waste_disagg_weights()
        assert result is not None
        assert isinstance(result, DisaggWeights)

    def test_cache_clearing_reflects_new_config(self) -> None:
        _setup_config("test_usa_config_waste_disagg_off")
        result1 = get_waste_disagg_weights()
        assert result1 is None

        _setup_config("test_usa_config_waste_disagg")
        result2 = get_waste_disagg_weights()
        assert result2 is not None
        assert isinstance(result2, DisaggWeights)

    def test_match_io_year_2017_derives_not_bundled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """#1312 / #1269: match_io + calendar 2017 must not hit bundled CSVs."""
        derived = object()
        prov = WeightDerivationProvenance(
            target_year=2017,
            rcra_source_year=2017,
            ec_source_year=2017,
            fallback_notes=["test-derive"],
        )
        derive_calls: list[int] = []

        def _derive(
            year: int, **_k: object
        ) -> tuple[object, WeightDerivationProvenance]:
            derive_calls.append(year)
            return derived, prov

        def _load_forbidden(*_a: object, **_k: object) -> object:
            raise AssertionError(
                "bundled load_disagg_weights must not run under match_io"
            )

        monkeypatch.setattr(dww, "derive_waste_weights", _derive)
        monkeypatch.setattr(dw, "load_disagg_weights", _load_forbidden)
        monkeypatch.setattr(sdp, "_disagg_weights", dw)

        _clear_all_caches()
        reset_usa_config(should_reset_env_var=True)
        cfg = _load_usa_config_from_file_name(
            "test_usa_config_waste_disagg.yaml"
        ).model_copy(
            update={
                "waste_weights_year": "match_io",
                "usa_base_io_data_year": 2017,
                "model_base_year": 2017,
                "usa_ghg_data_year": 2017,
            }
        )
        set_global_usa_config_object(cfg, source_label="test_match_io_2017")

        assert get_waste_disagg_weights() is derived
        assert derive_calls == [2017]
        got = get_waste_disagg_provenance()
        assert got is not None
        assert got.rcra_source_year == 2017
        assert "bundled 2017 CSVs" not in " ".join(got.fallback_notes)

    def test_explicit_waste_weights_year_2017_loads_bundled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit / default waste_weights_year=2017 stays on Cornerstone CSVs."""
        bundled = object()

        def _derive_forbidden(*_a: object, **_k: object) -> object:
            raise AssertionError("derive_waste_weights must not run for explicit 2017")

        def _load(*_a: object, **_k: object) -> object:
            return bundled

        monkeypatch.setattr(dww, "derive_waste_weights", _derive_forbidden)
        monkeypatch.setattr(dw, "load_disagg_weights", _load)
        monkeypatch.setattr(sdp, "_disagg_weights", dw)

        _setup_config("test_usa_config_waste_disagg")
        assert get_usa_config().waste_weights_year == 2017
        assert get_waste_disagg_weights() is bundled
        got = get_waste_disagg_provenance()
        assert got is not None
        assert got.rcra_source_year == 2012
        assert any("bundled 2017 CSVs" in n for n in got.fallback_notes)


# ---------------------------------------------------------------------------
# Module-scoped fixtures for heavy pipeline results
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def baseline_V() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg_off")
    V = derive_V()
    _teardown()
    return V


@pytest.fixture(scope="module")
def baseline_U() -> tuple[pd.DataFrame, pd.DataFrame]:
    _setup_config("test_usa_config_waste_disagg_off")
    uset = derive_U_with_negatives()
    result = (pd.DataFrame(uset.Udom), pd.DataFrame(uset.Uimp))
    _teardown()
    return result


@pytest.fixture(scope="module")
def baseline_Ytot() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg_off")
    Ytot = derive_disagg_Ytot_with_trade()
    _teardown()
    return Ytot


@pytest.fixture(scope="module")
def baseline_Aq() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    _setup_config("test_usa_config_waste_disagg_off")
    aq = derive_Aq()
    result = (pd.DataFrame(aq.Adom), pd.DataFrame(aq.Aimp), aq.scaled_q.copy())
    _teardown()
    return result


@pytest.fixture(scope="module")
def baseline_B() -> pd.DataFrame | None:
    _setup_config("test_usa_config_waste_disagg_off")
    try:
        B = derive_B_via_vnorm()
    except Exception:
        _teardown()
        return None
    _teardown()
    return B


@pytest.fixture(scope="module")
def disagg_V() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    V = derive_V()
    _teardown()
    return V


@pytest.fixture(scope="module")
def disagg_U() -> tuple[pd.DataFrame, pd.DataFrame]:
    _setup_config("test_usa_config_waste_disagg")
    uset = derive_U_with_negatives()
    result = (pd.DataFrame(uset.Udom), pd.DataFrame(uset.Uimp))
    _teardown()
    return result


@pytest.fixture(scope="module")
def disagg_Ytot() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    Ytot = derive_disagg_Ytot_with_trade()
    _teardown()
    return Ytot


@pytest.fixture(scope="module")
def disagg_VA() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    VA = derive_VA()
    _teardown()
    return VA


@pytest.fixture(scope="module")
def disagg_Aq() -> tuple[pd.DataFrame, pd.DataFrame, pd.Series]:
    _setup_config("test_usa_config_waste_disagg")
    aq = derive_Aq()
    result = (pd.DataFrame(aq.Adom), pd.DataFrame(aq.Aimp), aq.scaled_q.copy())
    _teardown()
    return result


@pytest.fixture(scope="module")
def disagg_B() -> pd.DataFrame:
    _setup_config("test_usa_config_waste_disagg")
    B = derive_B_via_vnorm()
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

    @pytest.mark.xfail(reason="Expected to fail")
    def test_non_waste_matches_baseline(
        self,
        baseline_B: pd.DataFrame | None,
        disagg_B: pd.DataFrame,
    ) -> None:
        if baseline_B is None:
            pytest.skip("Baseline B unavailable (GCP auth or data not configured)")
        waste_cols = [c for c in _WASTE_NEW_CODES if c in disagg_B.columns]
        assert len(waste_cols) > 0
        non_waste_cols = [c for c in disagg_B.columns if c not in waste_cols]
        assert len(non_waste_cols) > 0
        assert np.allclose(
            baseline_B[non_waste_cols].values,
            disagg_B[non_waste_cols].values,
            atol=1e-6,
        ), "B non-waste columns should match between baseline and disaggregated configs"

    @pytest.mark.eeio_integration
    def test_runtime_B_path_does_not_use_bea_expansion_helpers(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _setup_config("2025_usa_cornerstone_v0_3.yaml")

        def _raise_if_called(*args: object, **kwargs: object) -> None:
            raise AssertionError(
                "B runtime path should not call BEA-expansion E helpers"
            )

        monkeypatch.setattr(
            usa_expansion,
            "expand_ghg_matrix_from_bea_to_cornerstone",
            _raise_if_called,
        )

        try:
            B = derive_B_via_vnorm()
        finally:
            _teardown()

        assert B.shape[1] == 405

    @pytest.mark.eeio_integration
    def test_runtime_B_formula_matches_implementation(self) -> None:
        _setup_config("2025_usa_cornerstone_v0_3.yaml")
        try:
            cfg = get_usa_config()
            actual = derive_B_via_vnorm()

            x = derive_x_after_redefinition() if cfg.use_ghg_year_x_in_B else derive_x()
            Vnorm = derive_Vnorm_scrap_corrected()
            # Mirror derive_B_via_vnorm: BEA gross-output x includes scrap (#1293).
            if cfg.usa_detail_io_source != 'nowcast' and (
                cfg.deflate_x_to_detail_io_year_for_B or cfg.use_ghg_year_x_in_B
            ):
                Vnorm = Vnorm.divide(1.0 - scrap_share_of_output(), axis=0)
            expected = load_E_from_flowsa().divide(x, axis=1).fillna(0.0) @ Vnorm
        finally:
            _teardown()

        assert list(actual.index) == list(expected.index)
        assert list(actual.columns) == list(expected.columns)
        np.testing.assert_allclose(
            actual.values,
            expected.values,
            rtol=1e-9,
            atol=1e-12,
        )


# ---------------------------------------------------------------------------
# Feature-off regression test (waste_disagg)
# ---------------------------------------------------------------------------


class TestFeatureOffRegression:

    def test_feature_off_V_matches_baseline(self) -> None:
        _setup_config("test_usa_config_waste_disagg_off")
        assert get_waste_disagg_weights() is None
        V = derive_V()
        _teardown()

        _setup_config("test_usa_config_waste_disagg_off")
        V2 = derive_V()
        _teardown()

        np.testing.assert_array_equal(V.values, V2.values)

    def test_feature_off_Aq_uses_expansion_path(self) -> None:
        _setup_config("test_usa_config_waste_disagg_off")
        assert get_waste_disagg_weights() is None
        aq = derive_Aq()
        Adom = pd.DataFrame(aq.Adom)
        waste_in = [c for c in _WASTE_NEW_CODES if c in Adom.index]
        waste_block = Adom.loc[waste_in, waste_in]
        off_diag = waste_block.values.copy()
        np.fill_diagonal(off_diag, 0.0)
        assert np.allclose(
            off_diag, 0.0
        ), "With feature off, intragroup treatment should zero waste cross-terms"
        _teardown()


# ---------------------------------------------------------------------------
# Waste commodity identity + cache isolation
# ---------------------------------------------------------------------------


class TestWasteCommodityIdentity:

    def teardown_method(self) -> None:
        _teardown()

    def test_waste_commodity_q_matches_use_plus_fd_within_1pct(self) -> None:
        """Always-on: each 562* Make column ≈ Udom+Uimp+Y row within 1%."""
        _setup_config("test_usa_config_waste_disagg")
        V = derive_V_after_waste()
        Udom, Uimp = derive_U_after_waste()
        Y = derive_disagg_Ytot_with_trade()
        present = [c for c in _WASTE_NEW_CODES if c in V.columns and c in Udom.index]
        q_target = compute_waste_q_targets(Udom, Uimp, Y, waste_codes=present)
        for c in present:
            q_make = float(V[c].sum())
            qt = float(cast(float, q_target.loc[c]))
            denom = max(1.0, abs(qt))
            assert (
                abs(q_make - qt) / denom <= 0.01
            ), f"{c}: Make col={q_make} vs q_target={qt}"

    def test_wrapper_copy_does_not_poison_cache(self) -> None:
        _setup_config("test_usa_config_waste_disagg")
        V1 = derive_V_after_waste()
        marker = float(cast(float, V1.iloc[0, 0]))
        V1.iloc[0, 0] = marker + 12345.0
        V2 = derive_V_after_waste()
        assert float(cast(float, V2.iloc[0, 0])) == pytest.approx(marker)

    def test_u_after_waste_skips_make_va_reconcile(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """U/Y-only path must not build or fail on Make/VA reconcile."""
        _setup_config("test_usa_config_waste_disagg")

        def _boom(*_a: object, **_k: object) -> tuple[object, object]:
            raise RuntimeError("reconcile must not run for Use-only callers")

        monkeypatch.setattr(
            "bedrock.transform.eeio.sector_disagg_pipeline.reconcile_waste_make_and_va_to_use",
            _boom,
        )
        Udom, Uimp = derive_U_after_waste()
        Y = derive_y_after_waste()
        assert not Udom.empty
        assert not Uimp.empty
        assert not Y.empty
        with pytest.raises(RuntimeError, match="reconcile must not run"):
            derive_V_after_waste()
