"""Tests for 221100 electricity co-production reallocation."""

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
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Ytot_matrix_set,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    BALANCE_TOLERANCE,
    ELECTRICITY_AGGREGATE,
    CoprodTransfer,
    _float_ndarray,
    _frame_cell_float,
    _make_diagonal,
    apply_single_coproduction_transfer,
    build_coproduction_transfer_schedule,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.validation.diagnostics_helpers import pull_efs_for_diagnostics

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    electricity_reallocation_enabled,
    derive_disagg_io_bundle,
    cornerstone_sector_disagg_active,
    derive_cornerstone_V,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    derive_cornerstone_VA,
    derive_disagg_Ytot_with_trade,
    derive_cornerstone_Ytot_matrix_set,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
]


def _clear_all_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


def _setup_config(config_name: str) -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_all_caches()
    reset_usa_config(should_reset_env_var=True)


def _expected_post_reallocation_diagonals(
    V_pre: pd.DataFrame,
    aggregate: str = ELECTRICITY_AGGREGATE,
) -> pd.Series[float]:
    """Expected diagonal Make values after full reallocation (static schedule)."""
    expected = pd.Series({i: _make_diagonal(V_pre, i) for i in V_pre.index})
    for s in V_pre.index:
        if s != aggregate:
            t = _frame_cell_float(V_pre, str(s), aggregate)
            if t > 0:
                expected[aggregate] += t
    for d in V_pre.columns:
        if d != aggregate:
            t = _frame_cell_float(V_pre, aggregate, str(d))
            if t > 0:
                expected[d] += t
    return expected


class TestTransferSchedule:
    def test_transfer_schedule_order(self) -> None:
        codes = ["221200", ELECTRICITY_AGGREGATE, "531ORE"]
        V = pd.DataFrame(
            [
                [100.0, 50.0, 0.0],
                [0.0, 200.0, 80.0],
                [0.0, 0.0, 300.0],
            ],
            index=codes,
            columns=codes,
        )
        schedule = build_coproduction_transfer_schedule(V)
        assert len(schedule) == 2
        assert schedule[0] == CoprodTransfer(
            source="221200", target=ELECTRICITY_AGGREGATE, amount=50.0
        )
        assert schedule[1] == CoprodTransfer(
            source=ELECTRICITY_AGGREGATE, target="531ORE", amount=80.0
        )

    def test_row_sum_s_zero_raises(self) -> None:
        codes = ["A", "B"]
        V = pd.DataFrame([[0.0, 0.0], [0.0, 10.0]], index=codes, columns=codes)
        U = pd.DataFrame(1.0, index=codes, columns=codes)
        VA = pd.DataFrame(1.0, index=["va1"], columns=codes)
        transfer = CoprodTransfer(source="A", target="B", amount=5.0)
        with pytest.raises(ValueError, match="row sum is zero"):
            apply_single_coproduction_transfer(V, U, U, VA, transfer)

    def test_single_transfer_preserves_row_totals(self) -> None:
        rng = np.random.default_rng(0)
        codes = ["s", "d", "x"]
        V = pd.DataFrame(
            [[10.0, 5.0, 0.0], [0.0, 20.0, 0.0], [0.0, 0.0, 1.0]],
            index=codes,
            columns=codes,
        )
        Udom = pd.DataFrame(rng.random((3, 3)), index=codes, columns=codes)
        Uimp = pd.DataFrame(rng.random((3, 3)), index=codes, columns=codes)
        VA = pd.DataFrame(rng.random((2, 3)), index=["va1", "va2"], columns=codes)
        transfer = CoprodTransfer(source="s", target="d", amount=5.0)
        rows_before = pd.concat([Udom, Uimp, VA]).sum(axis=1)
        _, Udom2, Uimp2, VA2 = apply_single_coproduction_transfer(
            V, Udom, Uimp, VA, transfer
        )
        rows_after = pd.concat([Udom2, Uimp2, VA2]).sum(axis=1)
        np.testing.assert_allclose(
            _float_ndarray(rows_after.to_numpy()),
            _float_ndarray(rows_before.to_numpy()),
            atol=1e-6,
        )


@pytest.mark.eeio_integration
class TestElectricityReallocationIntegration:
    def teardown_method(self) -> None:
        _teardown()

    def test_post_reallocation_diagonal_values(self) -> None:
        _setup_config("test_usa_config_waste_disagg.yaml")
        V_pre = derive_cornerstone_V_after_waste()
        expected = _expected_post_reallocation_diagonals(V_pre)
        touched = {t.source for t in build_coproduction_transfer_schedule(V_pre)} | {
            t.target for t in build_coproduction_transfer_schedule(V_pre)
        }

        _setup_config("test_usa_config_waste_disagg_electricity.yaml")
        V_post = derive_cornerstone_V()
        for industry in touched:
            np.testing.assert_allclose(
                _make_diagonal(V_post, industry),
                expected.loc[industry],
                rtol=0,
                atol=BALANCE_TOLERANCE,
            )

    def test_make_use_commodity_balance(self) -> None:
        _setup_config("test_usa_config_waste_disagg.yaml")
        V_pre = derive_cornerstone_V_after_waste()
        uset_pre = derive_cornerstone_U_with_negatives()
        y_pre = derive_cornerstone_Ytot_matrix_set().ytot
        q_make_pre = float(V_pre[ELECTRICITY_AGGREGATE].sum())
        q_use_pre = float(
            (uset_pre.Udom + uset_pre.Uimp).loc[ELECTRICITY_AGGREGATE].sum()
            + y_pre.get(ELECTRICITY_AGGREGATE, 0.0)
        )

        _setup_config("test_usa_config_waste_disagg_electricity.yaml")
        V = derive_cornerstone_V()
        uset = derive_cornerstone_U_with_negatives()
        y = derive_cornerstone_Ytot_matrix_set().ytot
        q_make = float(V[ELECTRICITY_AGGREGATE].sum())
        q_use = float(
            (uset.Udom + uset.Uimp).loc[ELECTRICITY_AGGREGATE].sum()
            + y.get(ELECTRICITY_AGGREGATE, 0.0)
        )
        assert q_make == pytest.approx(q_make_pre, abs=1.0)
        assert q_use == pytest.approx(q_use_pre, abs=1.0)

    def test_y_221100_unchanged(self) -> None:
        _setup_config("test_usa_config_waste_disagg.yaml")
        y_waste = derive_disagg_Ytot_with_trade().loc[ELECTRICITY_AGGREGATE]

        _setup_config("test_usa_config_waste_disagg_electricity.yaml")
        y_elec = derive_disagg_Ytot_with_trade().loc[ELECTRICITY_AGGREGATE]
        np.testing.assert_allclose(
            _float_ndarray(y_waste.to_numpy()),
            _float_ndarray(y_elec.to_numpy()),
            rtol=0,
            atol=1e-6,
        )

    def test_e_unchanged_with_electricity_reallocation(self) -> None:
        _setup_config("test_usa_config_waste_disagg.yaml")
        E_waste = derive_E_usa()

        _setup_config("test_usa_config_waste_disagg_electricity.yaml")
        E_elec = derive_E_usa()
        pd.testing.assert_frame_equal(E_waste, E_elec)

    def test_pipeline_aq_dimensions(self) -> None:
        _setup_config("test_usa_config_waste_disagg_electricity.yaml")
        aq = derive_cornerstone_Aq_scaled()
        assert aq.Adom.shape == (405, 405)
        assert aq.Aimp.shape == (405, 405)

    def test_feature_off_regression(self) -> None:
        _setup_config("2025_usa_cornerstone_full_model.yaml")
        assert electricity_reallocation_enabled() is False
        V_full = derive_cornerstone_V()
        _setup_config("test_usa_config_waste_disagg.yaml")
        V_waste = derive_cornerstone_V()
        square_labels = V_full.index.intersection(V_full.columns)
        unrelated = [
            c
            for c in square_labels
            if c not in (ELECTRICITY_AGGREGATE, "221200", "531ORE", "S00203")
        ]
        np.testing.assert_allclose(
            _float_ndarray(V_full.loc[unrelated, unrelated].to_numpy()),
            _float_ndarray(V_waste.loc[unrelated, unrelated].to_numpy()),
            rtol=1e-9,
            atol=1e-6,
        )

    def test_diagnostics_helpers_run(self) -> None:
        _setup_config("2025_usa_cornerstone_full_model_electricity_reallocation.yaml")
        result = pull_efs_for_diagnostics()
        assert result is not None
