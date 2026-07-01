"""Tests for 221110 mixed-unit (MWh) conversion (PR4)."""

from __future__ import annotations

from typing import Callable, cast
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    build_end_use_map,
    electricity_mixed_units_enabled,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_y_nab,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    apply_electricity_unit_conversion_to_A,
    electricity_class_row_factors,
    electricity_output_factor,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.validation.diagnostics_helpers import (
    apply_mixed_units_ef_diff_exemptions,
)

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    electricity_mixed_units_enabled,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_y_nab,
    build_end_use_map,
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
