"""Tests for step-wise disaggregation scenarios."""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from bedrock.analysis.electricity.d_85.__tests__.conftest import (
    minimal_balanced_checkpoint,
    mock_fba_table83_table24,
)
from bedrock.analysis.electricity.d_85.disagg_scenarios import (
    apply_use_intersection_custom,
    run_scenario,
    split_y_commodity_row_by_column,
)
from bedrock.analysis.electricity.d_85.disagg_weights import (
    build_ugo_col_table83_row_intersection_matrix,
)
from bedrock.analysis.electricity.d_85.eia_inputs import table_2_4_prices_cents_kwh
from bedrock.transform.eeio.electricity_disaggregation import ELECTRICITY_AGGREGATE
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS


def test_apply_use_intersection_custom_preserves_total() -> None:
    idx = list(ELECTRICITY_DISAGG_SECTORS)
    Udom = pd.DataFrame(
        0.0, index=idx + [ELECTRICITY_AGGREGATE], columns=idx + [ELECTRICITY_AGGREGATE]
    )
    Uimp = Udom.copy()
    Udom.at[ELECTRICITY_AGGREGATE, ELECTRICITY_AGGREGATE] = 500.0
    w_ugo = pd.Series({'221110': 0.5, '221121': 0.3, '221122': 0.2})
    w_83 = pd.Series({'221110': 0.86, '221121': 0.09, '221122': 0.05})
    matrix = build_ugo_col_table83_row_intersection_matrix(w_ugo, w_83, 500.0)
    Udom, Uimp = apply_use_intersection_custom(Udom, Uimp, matrix)
    assert Udom.at[ELECTRICITY_AGGREGATE, ELECTRICITY_AGGREGATE] == 0.0
    assert pytest.approx(float(matrix.values.sum())) == 500.0


def test_split_y_removes_aggregate_row() -> None:
    Y = pd.DataFrame({'F01000': [100.0]}, index=[ELECTRICITY_AGGREGATE])
    w_by_col = pd.DataFrame(
        {'F01000': [0.5, 0.3, 0.2]},
        index=list(ELECTRICITY_DISAGG_SECTORS),
    )
    out = split_y_commodity_row_by_column(Y, w_by_col)
    assert ELECTRICITY_AGGREGATE not in out.index


@mock.patch('bedrock.analysis.electricity.d_85.disagg_scenarios.table83_go_weights')
@mock.patch('bedrock.analysis.electricity.d_85.disagg_scenarios.ugo305_go_weights')
@mock.patch(
    'bedrock.analysis.electricity.d_85.disagg_scenarios.derive_post_reallocation_checkpoint'
)
def test_d8_mixed_step2_uses_table83_weights(
    checkpoint_mock: mock.Mock,
    ugo_mock: mock.Mock,
    t83_mock: mock.Mock,
) -> None:
    checkpoint_mock.return_value = minimal_balanced_checkpoint()
    ugo_mock.return_value = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
    t83_mock.return_value = pd.Series({'221110': 0.86, '221121': 0.09, '221122': 0.05})

    with mock.patch(
        'bedrock.analysis.electricity.d_85.disagg_scenarios.disaggregate_use_intersection'
    ) as use_int:
        use_int.side_effect = lambda udom, uimp, w: (udom, uimp)
        with mock.patch(
            'bedrock.analysis.electricity.d_85.disagg_scenarios.disaggregate_make_intersection',
            side_effect=lambda v, _w: v,
        ):
            with mock.patch(
                'bedrock.analysis.electricity.d_85.disagg_scenarios.disaggregate_use_industry_columns',
                side_effect=lambda _x, udom, uimp, va, _w: (udom, uimp, va),
            ):
                with mock.patch(
                    'bedrock.analysis.electricity.d_85.disagg_scenarios._split_uniform_row',
                    side_effect=lambda udom, uimp, _w: (udom, uimp),
                ):
                    with mock.patch(
                        'bedrock.analysis.electricity.d_85.disagg_scenarios.disaggregate_electricity_commodity_row_in_y',
                        side_effect=lambda y, _w: y,
                    ):
                        with mock.patch(
                            'bedrock.analysis.electricity.d_85.disagg_scenarios.validate_cornerstone'
                        ):
                            run_scenario('d8_mixed')
    _, _, w_passed = use_int.call_args[0]
    assert pytest.approx(float(w_passed['221110'])) == 0.86


@mock.patch(
    'bedrock.analysis.electricity.d_85.disagg_scenarios.table_2_4_prices_cents_kwh'
)
@mock.patch('bedrock.analysis.electricity.d_85.disagg_scenarios.build_end_use_map')
@mock.patch('bedrock.analysis.electricity.d_85.disagg_scenarios.ugo305_go_weights')
@mock.patch(
    'bedrock.analysis.electricity.d_85.disagg_scenarios.derive_post_reallocation_checkpoint'
)
def test_p24_weights_sum_to_one_per_column(
    checkpoint_mock: mock.Mock,
    ugo_mock: mock.Mock,
    map_mock: mock.Mock,
    prices_mock: mock.Mock,
) -> None:
    V, Udom, Uimp, VA, Y = minimal_balanced_checkpoint()
    checkpoint_mock.return_value = (V, Udom, Uimp, VA, Y)
    ugo_mock.return_value = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
    map_mock.return_value = {'541000': 'Commercial', 'F01000': 'Residential'}
    prices_mock.return_value = table_2_4_prices_cents_kwh(
        2017, fba=mock_fba_table83_table24()
    )

    with mock.patch(
        'bedrock.analysis.electricity.d_85.disagg_scenarios.disaggregate_make_intersection',
        side_effect=lambda v, _w: v,
    ):
        with mock.patch(
            'bedrock.analysis.electricity.d_85.disagg_scenarios.disaggregate_use_intersection',
            side_effect=lambda udom, uimp, _w: (udom, uimp),
        ):
            with mock.patch(
                'bedrock.analysis.electricity.d_85.disagg_scenarios.disaggregate_use_industry_columns',
                side_effect=lambda _x, udom, uimp, va, _w: (udom, uimp, va),
            ):
                with mock.patch(
                    'bedrock.analysis.electricity.d_85.disagg_scenarios.split_commodity_row_by_column',
                    side_effect=lambda udom, uimp, w: (udom, uimp),
                ) as row_split:
                    with mock.patch(
                        'bedrock.analysis.electricity.d_85.disagg_scenarios.split_y_commodity_row_by_column',
                        side_effect=lambda y, _w: y,
                    ):
                        with mock.patch(
                            'bedrock.analysis.electricity.d_85.disagg_scenarios.validate_cornerstone'
                        ):
                            run_scenario('p24_2017')
    w_by_col = row_split.call_args[0][2]
    for col in w_by_col.columns:
        assert pytest.approx(float(w_by_col[col].sum())) == 1.0
