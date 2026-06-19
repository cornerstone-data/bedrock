"""Baseline scenario parity vs production PR3 path."""

from __future__ import annotations

from unittest import mock

import numpy as np

from bedrock.analysis.electricity.d_85.__tests__.conftest import (
    minimal_balanced_checkpoint,
)
from bedrock.analysis.electricity.d_85.disagg_scenarios import _run_baseline
from bedrock.analysis.electricity.d_85.disagg_weights import ugo305_go_weights
from bedrock.transform.eeio.electricity_disaggregation import (
    DISAGG_BALANCE_ATOL,
    disaggregate_electricity_commodity_row_in_y,
    disaggregate_electricity_make_use_va,
)
from bedrock.utils.math.formulas import compute_q, compute_x


@mock.patch(
    'bedrock.transform.eeio.electricity_disaggregation.export_electricity_disagg_weights_to_csv'
)
@mock.patch(
    'bedrock.analysis.electricity.d_85.disagg_scenarios.derive_post_reallocation_checkpoint'
)
def test_baseline_matches_production(
    checkpoint_scenarios: mock.Mock,
    export_mock: mock.Mock,
) -> None:
    checkpoint = minimal_balanced_checkpoint()
    checkpoint_scenarios.return_value = checkpoint
    V, Udom, Uimp, VA, Y = checkpoint

    prod_V, prod_Udom, prod_Uimp, prod_VA = disaggregate_electricity_make_use_va(
        V.copy(), Udom.copy(), Uimp.copy(), VA.copy()
    )
    prod_Y = disaggregate_electricity_commodity_row_in_y(Y.copy(), ugo305_go_weights())

    analysis = _run_baseline()

    for label, prod, anal in (
        ('V', prod_V, analysis.V),
        ('Udom', prod_Udom, analysis.Udom),
        ('Uimp', prod_Uimp, analysis.Uimp),
        ('VA', prod_VA, analysis.VA),
        ('Y', prod_Y, analysis.Y),
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
            err_msg=f'{label} mismatch vs production baseline',
        )

    np.testing.assert_allclose(
        analysis.q.reindex(prod_V.index, fill_value=0.0).to_numpy(),
        compute_q(V=prod_V).reindex(prod_V.index, fill_value=0.0).to_numpy(),
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
    )
    np.testing.assert_allclose(
        analysis.x.reindex(prod_V.index, fill_value=0.0).to_numpy(),
        compute_x(V=prod_V).reindex(prod_V.index, fill_value=0.0).to_numpy(),
        rtol=1e-9,
        atol=DISAGG_BALANCE_ATOL,
    )
