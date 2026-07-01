"""Tests for 221110 mixed-unit (MWh) conversion (PR4)."""

from __future__ import annotations

from typing import Callable, cast
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    build_end_use_map,
    compute_mixed_unit_ef_vectors,
    electricity_mixed_units_enabled,
    table_2_4_prices_cents_kwh,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_mixed_units,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    apply_electricity_unit_conversion_to_A,
    apply_electricity_unit_conversion_to_B,
    electricity_class_row_factors,
    electricity_output_factor,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.math.formulas import backcompute_y_from_A_and_q, compute_d
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet
from bedrock.utils.validation.diagnostics_helpers import (
    apply_mixed_units_bly_diff_exemptions,
    apply_mixed_units_ef_diff_exemptions,
    pull_efs_for_diagnostics,
)

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    electricity_mixed_units_enabled,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_mixed_units,
    build_end_use_map,
    table_2_4_prices_cents_kwh,
]


def _clear_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()


def _setup(config_name: str) -> None:
    _clear_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_caches()
    reset_usa_config(should_reset_env_var=True)


@pytest.fixture
def mixed_units_config() -> str:
    return 'test_usa_config_waste_disagg_electricity_mixed_units.yaml'


def test_electricity_output_factor_raises_on_bad_inputs() -> None:
    with pytest.raises(ValueError, match='q\\$_221110'):
        electricity_output_factor(0.0, 1e9)
    with pytest.raises(ValueError, match='mwh_221110'):
        electricity_output_factor(1e11, 0.0)


def test_class_row_factors_monotonicity() -> None:
    prices = {
        'Residential': 12.0,
        'Commercial': 10.0,
        'Industrial': 7.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
    end_use_map = {'col_ind': 'Industrial', 'col_res': 'Residential'}
    scaled_q = pd.Series({'col_ind': 100.0, 'col_res': 100.0})
    adom_row = pd.Series({'col_ind': 0.01, 'col_res': 0.01})
    y_row = pd.Series(dtype=float)
    mwh = 1e9
    c_row = electricity_class_row_factors(
        adom_row, scaled_q, y_row, prices, end_use_map, mwh
    )
    assert c_row['col_ind'] > c_row['col_res']


def test_uniform_prices_similarity_transform() -> None:
    cols = [GENERATION_SECTOR, 'c0', 'c1', 'c2']
    A = pd.DataFrame(0.0, index=cols, columns=cols)
    for c in cols:
        A.loc[GENERATION_SECTOR, c] = 0.02
    q = pd.Series({c: 100.0 for c in cols}, dtype=float)
    q[GENERATION_SECTOR] = 500.0
    mwh = float(q[GENERATION_SECTOR]) * 0.01
    prices = {
        k: 10.0
        for k in ('Residential', 'Commercial', 'Industrial', 'Transportation', 'Total')
    }
    end_use_map = {c: 'Commercial' for c in cols}
    adom_row = cast(pd.Series, A.loc[GENERATION_SECTOR])
    y_row = pd.Series(dtype=float)
    c_col = electricity_output_factor(float(q[GENERATION_SECTOR]), mwh)
    c_row = electricity_class_row_factors(adom_row, q, y_row, prices, end_use_map, mwh)
    A_m = apply_electricity_unit_conversion_to_A(A, c_col=c_col, c_row=c_row)
    for c in cols:
        if c == GENERATION_SECTOR:
            continue
        expected = cast(float, A.loc[GENERATION_SECTOR, c]) * float(c_row[c])
        assert A_m.loc[GENERATION_SECTOR, c] == pytest.approx(expected)


def test_build_end_use_map_includes_electricity_children() -> None:
    mapping = build_end_use_map()
    for code in ELECTRICITY_DISAGG_SECTORS:
        assert mapping[code] == 'Industrial'


def test_mixed_units_flag_off_is_noop() -> None:
    _setup('test_usa_config_waste_disagg_electricity_disaggregation.yaml')
    try:
        aq_mon = derive_cornerstone_Aq_scaled()
        aq_mixed = derive_cornerstone_Aq_mixed_units()
        pd.testing.assert_frame_equal(aq_mon.Adom, aq_mixed.Adom)
        pd.testing.assert_series_equal(aq_mon.scaled_q, aq_mixed.scaled_q)
    finally:
        _teardown()


@patch(
    'bedrock.extract.disaggregation.egrid_generation.us_total_net_generation_mwh',
    return_value=4_000_000_000.0,
)
@patch(
    'bedrock.transform.eeio.cornerstone_disagg_pipeline.table_2_4_prices_cents_kwh',
)
def test_output_mwh_anchor(
    mock_prices: Mock,
    mock_mwh: Mock,
    mixed_units_config: str,
) -> None:
    mock_prices.return_value = {
        'Residential': 12.0,
        'Commercial': 10.0,
        'Industrial': 7.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
    _setup(mixed_units_config)
    try:
        aq = derive_cornerstone_Aq_mixed_units()
        assert aq.scaled_q[GENERATION_SECTOR] == pytest.approx(4_000_000_000.0)
    finally:
        _teardown()


def test_apply_mixed_units_ef_diff_exemptions() -> None:
    _setup('test_usa_config_waste_disagg_electricity_mixed_units.yaml')
    try:
        idx = pd.Index(['221110', '1111A0'], name='sector')
        comp = pd.DataFrame(
            {
                'D_new': [0.5, 1.0],
                'D_old_inflated': [50.0, 1.0],
                'D_old': [50.0, 1.0],
                'D_perc_diff': [0.99, 0.1],
            },
            index=idx,
        )
        out = apply_mixed_units_ef_diff_exemptions(comp, 'D')
        assert bool(np.isnan(cast(float, out.loc['221110', 'D_perc_diff'])))
        assert (
            out.loc['221110', 'exemption_reason'] == 'unit_incommensurate_mixed_units'
        )
        assert out.loc['1111A0', 'D_perc_diff'] == pytest.approx(0.1)
    finally:
        _teardown()


def test_apply_mixed_units_bly_diff_exemptions() -> None:
    _setup('test_usa_config_waste_disagg_electricity_mixed_units.yaml')
    try:
        df = pd.DataFrame(
            {
                'index': ['221110', '1111A0'],
                'BLy_new (MtCO2e)': [1.0, 2.0],
                'BLy_old (MtCO2e)': [0.5, 2.0],
                '(BLy_new - BLy_old) / BLy_old (%)': [1.0, 0.0],
            }
        )
        out = apply_mixed_units_bly_diff_exemptions(df)
        row = out.loc[out['index'] == '221110'].iloc[0]
        assert bool(np.isnan(cast(float, row['(BLy_new - BLy_old) / BLy_old (%)'])))
        assert row['exemption_reason'] == 'baseline_monetary_vs_live_mixed'
        other = out.loc[out['index'] == '1111A0'].iloc[0]
        assert other['(BLy_new - BLy_old) / BLy_old (%)'] == pytest.approx(0.0)
        assert other['exemption_reason'] == ''
    finally:
        _teardown()


def test_y_nab_stays_monetary_under_mixed_gate(mixed_units_config: str) -> None:
    _setup('test_usa_config_waste_disagg_electricity_disaggregation.yaml')
    try:
        y_off = derive_cornerstone_y_nab().copy()
    finally:
        _teardown()
    _setup(mixed_units_config)
    try:
        y_on = derive_cornerstone_y_nab()
        pd.testing.assert_series_equal(y_off, y_on)
    finally:
        _teardown()


@patch(
    'bedrock.extract.disaggregation.egrid_generation.us_total_net_generation_mwh',
    return_value=4_000_000_000.0,
)
@patch(
    'bedrock.transform.eeio.cornerstone_disagg_pipeline.table_2_4_prices_cents_kwh',
)
def test_y_nab_mixed_differs_from_monetary_under_gate(
    mock_prices: Mock,
    mock_mwh: Mock,
    mixed_units_config: str,
) -> None:
    mock_prices.return_value = {
        'Residential': 12.0,
        'Commercial': 10.0,
        'Industrial': 7.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
    _setup(mixed_units_config)
    try:
        y_mon = derive_cornerstone_y_nab()
        y_mix = derive_cornerstone_y_nab_mixed_units()
        aq = derive_cornerstone_Aq_mixed_units()
        assert y_mix[GENERATION_SECTOR] != pytest.approx(y_mon[GENERATION_SECTOR])
        y_back = backcompute_y_from_A_and_q(A=aq.Adom, q=aq.scaled_q)
        pd.testing.assert_series_equal(y_mix, y_back)
        u = aq.Adom.multiply(aq.scaled_q, axis=1).sum(axis=1) + y_mix
        pd.testing.assert_series_equal(aq.scaled_q, u, rtol=1e-6, check_names=False)
    finally:
        _teardown()


@patch(
    'bedrock.extract.disaggregation.egrid_generation.us_total_net_generation_mwh',
    return_value=4_000_000_000.0,
)
@patch(
    'bedrock.transform.eeio.cornerstone_disagg_pipeline.table_2_4_prices_cents_kwh',
)
def test_d_scalar_bridge_under_gate(
    mock_prices: Mock,
    mock_mwh: Mock,
    mixed_units_config: str,
) -> None:
    mock_prices.return_value = {
        'Residential': 12.0,
        'Commercial': 10.0,
        'Industrial': 7.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
    _setup(mixed_units_config)
    try:
        gen = GENERATION_SECTOR
        cols = [gen, '1111A0', '1111B0']
        b_mon = pd.DataFrame(np.eye(len(cols)) * 10.0, index=cols, columns=cols)
        c_col = 0.02
        b_mix = apply_electricity_unit_conversion_to_B(b_mon, c_col)
        d_mon = compute_d(B=b_mon)
        d_mix = compute_d(B=b_mix)
        assert float(d_mix[gen]) == pytest.approx(float(d_mon[gen]) / c_col)
    finally:
        _teardown()


def test_compute_mixed_unit_ef_vectors_not_cached() -> None:
    gen = GENERATION_SECTOR
    cols = [gen, '1111A0']
    adom = pd.DataFrame([[0.0, 0.01], [0.0, 0.0]], index=cols, columns=cols)
    aimp = pd.DataFrame(0.0, index=cols, columns=cols)
    q = pd.Series({gen: 100.0, '1111A0': 50.0}, dtype=float)
    aq = SingleRegionAqMatrixSet(Adom=adom, Aimp=aimp, scaled_q=q)  # type: ignore[arg-type]
    b = pd.DataFrame(10.0, index=cols, columns=cols)
    c_row_a = pd.Series({gen: 0.5, '1111A0': 0.5})
    c_row_b = pd.Series({gen: 0.8, '1111A0': 0.8})
    with patch(
        'bedrock.transform.eeio.cornerstone_disagg_pipeline.electricity_conversion_factors',
        side_effect=[(0.5, c_row_a), (0.5, c_row_b)],
    ):
        r_class = compute_mixed_unit_ef_vectors(aq, b, prices_by_class=None)
        r_uniform = compute_mixed_unit_ef_vectors(
            aq, b, prices_by_class={'Industrial': 10.0, 'Total': 10.0}
        )
    assert not r_class.N.equals(r_uniform.N)


@pytest.mark.eeio_integration
def test_pull_efs_mixed_units_config(mixed_units_config: str) -> None:
    _setup(mixed_units_config)
    try:
        result = pull_efs_for_diagnostics()
        assert result.D_new is not None
        assert result.N_new is not None
    finally:
        _teardown()


def test_electricity_class_row_factors_missing_column_raises() -> None:
    prices = {
        'Industrial': 7.0,
        'Commercial': 10.0,
        'Residential': 12.0,
        'Transportation': 9.0,
        'Total': 10.0,
    }
    with pytest.raises(ValueError, match='absent from end_use_map'):
        electricity_class_row_factors(
            pd.Series({'unknown_col': 0.1}),
            pd.Series({'unknown_col': 1.0}),
            pd.Series(dtype=float),
            prices,
            {},
            1e9,
        )
