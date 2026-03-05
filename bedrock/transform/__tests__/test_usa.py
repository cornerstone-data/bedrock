from __future__ import annotations

import pandas as pd
import pytest

from bedrock.transform.eeio.derived import (
    derive_Aq_usa,
    derive_B_usa_non_finetuned,
    derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection,
    derive_y_for_national_accounting_balance_usa,
    derive_ydom_and_yimp_usa,
)
from bedrock.utils.validation.test_helpers import (
    assert_snapshot_frame_equal,
    assert_snapshot_series_equal,
)


@pytest.mark.eeio_integration
def test_b_usa_non_finetuned_snapshot(
    b_usa_non_finetuned_snapshot: pd.DataFrame,
) -> None:
    assert_snapshot_frame_equal(
        actual=derive_B_usa_non_finetuned(),
        expected=b_usa_non_finetuned_snapshot,
        msg="B_USA_non_finetuned",
    )


@pytest.mark.eeio_integration
def test_Adom_usa_snapshot(adom_usa_snapshot: pd.DataFrame) -> None:
    assert_snapshot_frame_equal(
        actual=derive_Aq_usa().Adom,
        expected=adom_usa_snapshot,
        msg="Adom_USA",
    )


@pytest.mark.eeio_integration
def test_Aimp_usa_snapshot(aimp_usa_snapshot: pd.DataFrame) -> None:
    assert_snapshot_frame_equal(
        actual=derive_Aq_usa().Aimp,
        expected=aimp_usa_snapshot,
        msg="Aimp_USA",
    )


@pytest.mark.eeio_integration
def test_scaled_q_usa_snapshot(scaled_q_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_Aq_usa().scaled_q,
        expected=scaled_q_usa_snapshot,
        msg="scaled_q_USA",
    )


@pytest.mark.eeio_integration
def test_y_nab_usa_snapshot(y_nab_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_y_for_national_accounting_balance_usa(),
        expected=y_nab_usa_snapshot,
        msg="y_nab_USA",
    )


@pytest.mark.eeio_integration
def test_ytot_usa_snapshot(ytot_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection().ytot,
        expected=ytot_usa_snapshot,
        msg="ytot_USA",
    )


@pytest.mark.eeio_integration
def test_exports_usa_snapshot(exports_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection().exports,
        expected=exports_usa_snapshot,
        msg="exports_USA",
    )


@pytest.mark.eeio_integration
def test_ydom_usa_snapshot(ydom_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_ydom_and_yimp_usa().ydom,
        expected=ydom_usa_snapshot,
        msg="ydom_USA",
    )


@pytest.mark.eeio_integration
def test_yimp_usa_snapshot(yimp_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_ydom_and_yimp_usa().yimp,
        expected=yimp_usa_snapshot,
        msg="yimp_USA",
    )
