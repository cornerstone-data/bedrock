"""Parity tests for cornerstone sector-disaggregation refactor."""

from __future__ import annotations

from typing import Callable, Generator

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from bedrock.extract.disaggregation import disagg_weights as disagg_weights_module
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_cornerstone_U_after_waste,
    derive_cornerstone_V_after_waste,
    derive_cornerstone_VA_after_waste,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_q,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_Y_personal_consumption_expenditure,
    derive_cornerstone_Ytot_full_cs_matrix,
    derive_cornerstone_Ytot_matrix_set,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config

_RTOL = 1e-9
_ATOL = 1e-6

_PARITY_CONFIGS = [
    '2025_usa_cornerstone_taxonomy_and_B_transformation',
    'test_usa_config_waste_disagg',
    'test_usa_config_waste_disagg_electricity',
]

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    cornerstone_sector_disagg_active,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    derive_cornerstone_V,
    derive_cornerstone_q,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_VA,
    derive_cornerstone_Ytot_matrix_set,
    derive_cornerstone_Aq,
]


def _clear_all_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()


def _setup_config(config_name: str) -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)


@pytest.fixture(params=_PARITY_CONFIGS)
def parity_config(request: pytest.FixtureRequest) -> Generator[str, None, None]:
    config_name = str(request.param)
    _setup_config(config_name)
    yield config_name
    _teardown()


class TestRouterParity:
    def test_public_io_getters(self, parity_config: str) -> None:
        V = derive_cornerstone_V()
        uset = derive_cornerstone_U_with_negatives()
        VA = derive_cornerstone_VA()
        Ytot_set = derive_cornerstone_Ytot_matrix_set()
        Y_full = derive_cornerstone_Ytot_full_cs_matrix()
        Y_pce = derive_cornerstone_Y_personal_consumption_expenditure()
        Aq = derive_cornerstone_Aq()

        assert V.shape[0] == V.shape[1]
        assert uset.Udom.shape == uset.Uimp.shape
        assert VA.shape[1] == V.shape[0]
        assert len(Ytot_set.ytot) == V.shape[0]
        assert Y_full.shape[0] == V.shape[0]
        assert len(Y_pce) == V.shape[0]
        assert Aq.Adom.shape == V.shape

    @pytest.mark.parametrize('config_name', _PARITY_CONFIGS)
    def test_waste_only_bundle_matches_after_waste(self, config_name: str) -> None:
        _setup_config(config_name)
        try:
            if not get_waste_disagg_weights():
                pytest.skip('waste disagg not enabled')
            bundle = derive_disagg_io_bundle()
            V_w = derive_cornerstone_V_after_waste()
            Udom_w, Uimp_w = derive_cornerstone_U_after_waste()
            VA_w = derive_cornerstone_VA_after_waste()

            if electricity_reallocation_enabled():
                assert_frame_equal(
                    derive_cornerstone_V(), bundle.V, rtol=_RTOL, atol=_ATOL
                )
                uset = derive_cornerstone_U_with_negatives()
                assert_frame_equal(
                    pd.DataFrame(uset.Udom), bundle.Udom, rtol=_RTOL, atol=_ATOL
                )
                assert_frame_equal(
                    pd.DataFrame(uset.Uimp), bundle.Uimp, rtol=_RTOL, atol=_ATOL
                )
                assert_frame_equal(
                    derive_cornerstone_VA(), bundle.VA, rtol=_RTOL, atol=_ATOL
                )
                assert not bundle.V.equals(V_w)
            else:
                assert_frame_equal(derive_cornerstone_V(), V_w, rtol=_RTOL, atol=_ATOL)
                uset = derive_cornerstone_U_with_negatives()
                assert_frame_equal(
                    pd.DataFrame(uset.Udom), Udom_w, rtol=_RTOL, atol=_ATOL
                )
                assert_frame_equal(
                    pd.DataFrame(uset.Uimp), Uimp_w, rtol=_RTOL, atol=_ATOL
                )
                assert_frame_equal(
                    derive_cornerstone_VA(), VA_w, rtol=_RTOL, atol=_ATOL
                )
                assert_frame_equal(bundle.V, V_w, rtol=_RTOL, atol=_ATOL)
                assert_frame_equal(bundle.Udom, Udom_w, rtol=_RTOL, atol=_ATOL)
                assert_frame_equal(bundle.Uimp, Uimp_w, rtol=_RTOL, atol=_ATOL)
                assert_frame_equal(bundle.VA, VA_w, rtol=_RTOL, atol=_ATOL)
        finally:
            _teardown()

    @pytest.mark.parametrize('config_name', _PARITY_CONFIGS)
    def test_y_public_router_matches_disagg_when_active(self, config_name: str) -> None:
        _setup_config(config_name)
        try:
            Y_public = derive_cornerstone_Ytot_full_cs_matrix()
            if cornerstone_sector_disagg_active():
                Y_disagg = derive_disagg_Ytot_with_trade()
                assert_frame_equal(Y_public, Y_disagg, rtol=_RTOL, atol=_ATOL)
            else:
                assert Y_public.shape[0] > 0
        finally:
            _teardown()


class TestLazyImportMonkeypatch:
    def test_load_disagg_weights_monkeypatch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        called = {'n': 0}

        def _fake(*args: object, **kwargs: object) -> object:
            called['n'] += 1
            return object()

        monkeypatch.setattr(disagg_weights_module, 'load_disagg_weights', _fake)
        _setup_config('test_usa_config_waste_disagg')
        try:
            get_waste_disagg_weights()
            assert called['n'] == 1
        finally:
            _teardown()


class TestInflationParity:
    def test_q_and_vnorm_inflation(self) -> None:
        _setup_config('2025_usa_cornerstone_full_model_A_commodity_price_index')
        try:
            q = derive_cornerstone_q()
            Vnorm = derive_cornerstone_Vnorm_scrap_corrected(apply_inflation=True)
            assert len(q) > 0
            assert Vnorm.shape[0] > 0
        finally:
            _teardown()
