from __future__ import annotations

import pandas as pd
import pytest

from ceda_usa.transform.eeio.derived import (
    derive_Aq_usa,
    derive_B_usa_non_finetuned,
    derive_y_for_national_accounting_balance_usa,
)
from ceda_usa.utils.testing import (
    assert_snapshot_frame_equal,
    assert_snapshot_series_equal,
)


@pytest.mark.ceda_integration
def test_b_usa_non_finetuned_snapshot(
    b_usa_non_finetuned_snapshot: pd.DataFrame,
) -> None:
    assert_snapshot_frame_equal(
        actual=derive_B_usa_non_finetuned(),
        expected=b_usa_non_finetuned_snapshot,
        msg="B_USA_non_finetuned",
    )


@pytest.mark.ceda_integration
def test_Adom_usa_snapshot(adom_usa_snapshot: pd.DataFrame) -> None:
    assert_snapshot_frame_equal(
        actual=derive_Aq_usa().Adom,
        expected=adom_usa_snapshot,
        msg="Adom_USA",
    )


@pytest.mark.ceda_integration
def test_Aimp_usa_snapshot(aimp_usa_snapshot: pd.DataFrame) -> None:
    assert_snapshot_frame_equal(
        actual=derive_Aq_usa().Aimp,
        expected=aimp_usa_snapshot,
        msg="Aimp_USA",
    )


@pytest.mark.ceda_integration
def test_scaled_q_usa_snapshot(scaled_q_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_Aq_usa().scaled_q,
        expected=scaled_q_usa_snapshot,
        msg="scaled_q_USA",
    )


@pytest.mark.ceda_integration
def test_y_nab_usa_snapshot(y_nab_usa_snapshot: pd.Series[float]) -> None:
    assert_snapshot_series_equal(
        actual=derive_y_for_national_accounting_balance_usa(),
        expected=y_nab_usa_snapshot,
        msg="y_nab_USA",
    )
