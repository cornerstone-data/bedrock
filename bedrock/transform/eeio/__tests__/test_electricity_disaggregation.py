"""Tests for 221100 electricity sector disaggregation (PR3)."""

from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_disaggregation_enabled,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    ELECTRICITY_DISAGG_SECTORS,
    _compute_w_row,
    _derive_post_reallocation_checkpoint_for_disagg,
    _float_ndarray,
    _table83_purchased_power_expenses,
    _weights_from_table83_expenses,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    build_electricity_ugo305_scaling_ratios,
    disaggregate_use_industry_columns,
    get_electricity_commodity_row_weights,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES_ELEC,
    CORNERSTONE_INDUSTRIES_ELEC,
)
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS
from bedrock.utils.validation.diagnostics_helpers import pull_efs_for_diagnostics

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    derive_disagg_io_bundle,
    cornerstone_sector_disagg_active,
    derive_disagg_Ytot_with_trade,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    build_electricity_ugo305_scaling_ratios,
    get_electricity_commodity_row_weights,
    _derive_post_reallocation_checkpoint_for_disagg,
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    derive_cornerstone_VA,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
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


@pytest.fixture
def electricity_disagg_config() -> str:
    return 'test_usa_config_waste_disagg_electricity_disaggregation.yaml'


class TestGoWeights:
    def test_go_weights_sum_to_one(self) -> None:
        _setup_config('test_usa_config_waste_disagg_electricity_disaggregation.yaml')
        try:
            w = build_electricity_disagg_go_weights()
            assert set(w.index) == set(ELECTRICITY_DISAGG_SECTORS)
            np.testing.assert_allclose(float(w.sum()), 1.0, rtol=1e-9, atol=1e-12)
        finally:
            _teardown()


class TestStep3WorkedExample:
    def test_va_balancing_worked_example(self) -> None:
        w = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
        codes = list(ELECTRICITY_DISAGG_SECTORS)
        agg = ELECTRICITY_AGGREGATE
        extra_rows = ['212100', '541000']
        rows = codes + [agg, *extra_rows, *VALUE_ADDEDS]
        cols = codes + [agg, *extra_rows]
        Udom = pd.DataFrame(0.0, index=rows, columns=cols)
        Uimp = pd.DataFrame(0.0, index=rows, columns=cols)
        Udom.at[agg, agg] = 100.0
        Udom.at['212100', agg] = 50.0
        Udom.at['541000', agg] = 40.0
        for code in codes:
            Udom.at[code, code] = 100.0 * float(w[code])
        Udom.at[agg, agg] = 0.0
        VA = pd.DataFrame(0.0, index=list(VALUE_ADDEDS), columns=[agg])
        VA.at['V00100', agg] = 70.0
        VA.at['V00200', agg] = 30.0
        VA.at['V00300', agg] = 60.0
        x_agg = 350.0

        Udom, Uimp, VA = disaggregate_use_industry_columns(x_agg, Udom, Uimp, VA, w)

        use_sub = pd.Series(
            {code: float(Udom[code].sum()) + float(Uimp[code].sum()) for code in codes}
        )
        np.testing.assert_allclose(
            _float_ndarray(use_sub.to_numpy()),
            np.array([97.6, 5.6, 86.8]),
            rtol=1e-9,
            atol=1e-6,
        )
        va_col_totals = VA[codes].sum(axis=0)
        np.testing.assert_allclose(
            _float_ndarray(va_col_totals.to_numpy()),
            np.array([21.4, 8.4, 130.2]),
            rtol=1e-9,
            atol=1e-6,
        )
        np.testing.assert_allclose(
            _float_ndarray(VA.sum(axis=1).to_numpy()),
            np.array([70.0, 30.0, 60.0]),
            rtol=1e-9,
            atol=1e-6,
        )
        col_totals = use_sub + va_col_totals
        np.testing.assert_allclose(
            _float_ndarray(col_totals.to_numpy()),
            np.array([119.0, 14.0, 217.0]),
            rtol=1e-9,
            atol=1e-6,
        )


@pytest.mark.eeio_integration
class TestElectricityDisaggregationPipeline:
    def test_schema_is_407_sectors(self, electricity_disagg_config: str) -> None:
        _setup_config(electricity_disagg_config)
        try:
            V = derive_cornerstone_V()
            assert list(V.index) == CORNERSTONE_INDUSTRIES_ELEC
            assert list(V.columns) == CORNERSTONE_COMMODITIES_ELEC
            assert ELECTRICITY_AGGREGATE not in V.index
        finally:
            _teardown()

    def test_make_and_use_balance(self, electricity_disagg_config: str) -> None:
        _setup_config(electricity_disagg_config)
        try:
            bundle = derive_disagg_io_bundle()
            V, Udom, Uimp, VA = bundle.V, bundle.Udom, bundle.Uimp, bundle.VA
            for frame in (V, Udom, Uimp, VA):
                assert ELECTRICITY_AGGREGATE not in frame.index
                assert ELECTRICITY_AGGREGATE not in frame.columns
        finally:
            _teardown()

    def test_pipeline_aq_and_diagnostics(self, electricity_disagg_config: str) -> None:
        _setup_config(electricity_disagg_config)
        try:
            aq = derive_cornerstone_Aq_scaled()
            assert aq.Adom.shape[0] == len(CORNERSTONE_COMMODITIES_ELEC)
            elec_col_sums = aq.Adom[ELECTRICITY_DISAGG_SECTORS].sum(axis=0)
            assert (elec_col_sums <= 1.0 + 1e-6).all()
            pull_efs_for_diagnostics()
        finally:
            _teardown()

    def test_e_attribution(self, electricity_disagg_config: str) -> None:
        _setup_config('2025_usa_cornerstone_full_model_electricity_disaggregation.yaml')
        try:
            E = derive_E_usa()
            elec_cols = [c for c in ELECTRICITY_DISAGG_SECTORS if c in E.columns]
            assert len(elec_cols) == 3
            assert float(E[elec_cols].sum().sum()) > 0
        finally:
            _teardown()


class TestTable83UseIntersectionWeights:
    def test_purchased_power_weights_sum_to_one(self) -> None:
        from bedrock.analysis.electricity.d_85.__tests__.conftest import (  # noqa: PLC0415
            mock_fba_table83_table24,
        )

        expenses = _table83_purchased_power_expenses(
            2017, fba=mock_fba_table83_table24()
        )
        w = _weights_from_table83_expenses(expenses)
        assert set(w.index) == set(ELECTRICITY_DISAGG_SECTORS)
        np.testing.assert_allclose(float(w.sum()), 1.0, rtol=1e-9, atol=1e-12)
        assert w['221110'] == pytest.approx(49030.0 / (49030.0 + 10804.0 + 4358.0))


class TestCompensatingRowWeights:
    def test_compute_w_row_closes_market_clearing(self) -> None:
        w_go = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
        w_int = w_go.copy()
        t = 100.0
        p = 200.0
        w_row = _compute_w_row(w_go, w_int, t, p)
        np.testing.assert_allclose(w_row, w_go, rtol=1e-9, atol=1e-12)
        q_total = t + p
        for code in ELECTRICITY_DISAGG_SECTORS:
            q_k = float(w_go[code]) * q_total
            i_k = float(w_int[code]) * t
            non_int = float(w_row[code]) * p
            np.testing.assert_allclose(q_k, i_k + non_int, rtol=1e-9, atol=1e-6)

    def test_getter_does_not_call_io_bundle(
        self, electricity_disagg_config: str
    ) -> None:
        from unittest import mock  # noqa: PLC0415

        _setup_config(electricity_disagg_config)
        try:
            with mock.patch(
                'bedrock.transform.eeio.cornerstone_disagg_pipeline.derive_disagg_io_bundle'
            ) as bundle_mock:
                with mock.patch(
                    'bedrock.transform.eeio.cornerstone_disagg_pipeline.derive_disagg_Ytot_with_trade'
                ) as y_mock:
                    get_electricity_commodity_row_weights.cache_clear()
                    _derive_post_reallocation_checkpoint_for_disagg.cache_clear()
                    w = get_electricity_commodity_row_weights()
                    assert set(w.index) == set(ELECTRICITY_DISAGG_SECTORS)
                    bundle_mock.assert_not_called()
                    y_mock.assert_not_called()
        finally:
            _teardown()


@pytest.mark.eeio_integration
class TestD7PureScaling:
    def test_differentiated_child_q_scaling(
        self, electricity_disagg_config: str
    ) -> None:
        _setup_config(electricity_disagg_config)
        try:
            aq = derive_cornerstone_Aq_scaled()
            ratios = build_electricity_ugo305_scaling_ratios(2017, 2022)
            q_vals = [float(aq.scaled_q[c]) for c in ELECTRICITY_DISAGG_SECTORS]
            assert len(set(round(v, 6) for v in q_vals)) == 3
            assert ratios['221110'] != pytest.approx(ratios['221121'])
        finally:
            _teardown()
