"""Production path parity vs compensated analysis scenario (PR 3.1)."""

from __future__ import annotations

from unittest import mock

import numpy as np
import pandas as pd

from bedrock.analysis.electricity.d_85.__tests__.conftest import (
    minimal_balanced_checkpoint,
)
from bedrock.analysis.electricity.d_85.disagg_scenarios import run_scenario
from bedrock.transform.eeio.electricity_disaggregation import (
    DISAGG_BALANCE_ATOL,
    _capture_intersection_total,
    _capture_purchases_total,
    _compute_w_row,
    _derive_post_reallocation_checkpoint_for_disagg,
    disaggregate_electricity_commodity_row_in_y,
    disaggregate_electricity_make_use_va,
    get_electricity_commodity_row_weights,
)
from bedrock.utils.math.formulas import compute_q, compute_x


@mock.patch('bedrock.analysis.electricity.d_85.disagg_scenarios.validate_cornerstone')
@mock.patch(
    'bedrock.transform.eeio.electricity_disaggregation.build_electricity_disagg_use_intersection_weights'
)
@mock.patch(
    'bedrock.transform.eeio.electricity_disaggregation.export_electricity_disagg_weights_to_csv'
)
@mock.patch(
    'bedrock.transform.eeio.electricity_disaggregation._derive_y_before_electricity_disagg_lazy'
)
@mock.patch(
    'bedrock.transform.eeio.electricity_disaggregation._derive_post_reallocation_checkpoint_for_disagg'
)
@mock.patch(
    'bedrock.analysis.electricity.d_85.disagg_scenarios.derive_post_reallocation_checkpoint'
)
@mock.patch(
    'bedrock.analysis.electricity.d_85.disagg_scenarios.table83_purchased_power_weights'
)
@mock.patch('bedrock.analysis.electricity.d_85.disagg_scenarios.ugo305_go_weights')
@mock.patch(
    'bedrock.analysis.electricity.d_85.disagg_scenarios.get_electricity_commodity_row_weights'
)
def test_production_matches_compensated_scenario(
    getter_mock: mock.Mock,
    ugo_mock: mock.Mock,
    t83_mock: mock.Mock,
    checkpoint_scenarios: mock.Mock,
    checkpoint_prod: mock.Mock,
    y_lazy_mock: mock.Mock,
    export_mock: mock.Mock,
    w_int_prod_mock: mock.Mock,
    validate_mock: mock.Mock,
) -> None:
    get_electricity_commodity_row_weights.cache_clear()
    _derive_post_reallocation_checkpoint_for_disagg.cache_clear()

    checkpoint = minimal_balanced_checkpoint()
    checkpoint_scenarios.return_value = checkpoint
    v, udom, uimp, va, y = checkpoint
    checkpoint_prod.return_value = (v, udom, uimp, va)
    y_lazy_mock.return_value = y

    w_go = pd.Series({'221110': 0.34, '221121': 0.04, '221122': 0.62})
    w_int = pd.Series({'221110': 0.40, '221121': 0.04, '221122': 0.56})
    ugo_mock.return_value = w_go
    t83_mock.return_value = w_int
    w_int_prod_mock.return_value = w_int
    t = _capture_intersection_total(udom, uimp)
    p = _capture_purchases_total(udom, uimp, y)
    w_row = _compute_w_row(w_go, w_int, t, p)
    getter_mock.return_value = w_row

    prod_v, prod_udom, prod_uimp, prod_va = disaggregate_electricity_make_use_va(
        v.copy(), udom.copy(), uimp.copy(), va.copy()
    )
    prod_y = disaggregate_electricity_commodity_row_in_y(y.copy(), w_row)

    analysis = run_scenario('t8.3_purchased_power_diag_compensated')

    for label, prod, anal in (
        ('V', prod_v, analysis.V),
        ('Udom', prod_udom, analysis.Udom),
        ('Uimp', prod_uimp, analysis.Uimp),
        ('VA', prod_va, analysis.VA),
        ('Y', prod_y, analysis.Y),
    ):
        common_idx = prod.index.intersection(anal.index)
        common_cols = prod.columns.intersection(anal.columns)
        prod_sub = prod.loc[common_idx, common_cols].astype(float)
        anal_sub = anal.loc[common_idx, common_cols].astype(float)
        np.testing.assert_allclose(
            prod_sub.to_numpy(),
            anal_sub.to_numpy(),
            rtol=1e-9,
            atol=DISAGG_BALANCE_ATOL,
            err_msg=f'{label} mismatch vs compensated scenario',
        )

    np.testing.assert_allclose(
        analysis.q.reindex(prod_v.index, fill_value=0.0).to_numpy(),
        compute_q(V=prod_v).reindex(prod_v.index, fill_value=0.0).to_numpy(),
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
    )
    np.testing.assert_allclose(
        analysis.x.reindex(prod_v.index, fill_value=0.0).to_numpy(),
        compute_x(V=prod_v).reindex(prod_v.index, fill_value=0.0).to_numpy(),
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
    )
