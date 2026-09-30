"""Unit tests for SAS Table 2/3 waste suppression recovery (equal residual)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.disaggregation.sas_waste_metrics import (
    recover_suppressed_sas_table2_waste_revenue,
    recover_suppressed_sas_table3_waste_expenses,
)


def _table3_frame() -> pd.DataFrame:
    """Minimal Table 3: parent 562 + published and suppressed waste detail."""
    rows = [
        ("562", 100.0, None),
        ("562111", 40.0, None),
        ("562212", 20.0, None),
        ("562112", 0.0, "S"),  # → 562HAZ
        ("562211", 0.0, "S"),  # → 562HAZ
        ("562213", 0.0, "S"),  # → 562213
    ]
    return pd.DataFrame(
        rows, columns=["ActivityConsumedBy", "FlowAmount", "Suppressed"]
    )


def _table2_frame_single_suppressed() -> pd.DataFrame:
    """Table 2-like: NAICS on ActivityProducedBy; one suppressed cell (2021-like)."""
    rows = [
        ("562", 100.0, None),
        ("562111", 50.0, None),
        ("562212", 30.0, None),
        ("562213", 0.0, "S"),
    ]
    return pd.DataFrame(
        rows, columns=["ActivityProducedBy", "FlowAmount", "Suppressed"]
    )


def test_equal_residual_recovery_fills_suppressed() -> None:
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(_table3_frame())
    fill = 40.0 / 3.0
    supp = recovered[recovered["Suppressed"] == "S"]
    assert len(supp) == 3
    assert pytest.approx(supp["FlowAmount"].tolist()) == [fill, fill, fill]
    assert (supp["SuppressionRecovery"] == "equal_residual").all()
    assert any("equal_residual" in n for n in notes)
    haz = recovered[recovered["ActivityConsumedBy"].isin(["562112", "562211"])]
    assert pytest.approx(float(haz["FlowAmount"].sum())) == 2 * fill


def test_table2_single_suppressed_equals_exact_residual() -> None:
    recovered, notes = recover_suppressed_sas_table2_waste_revenue(
        _table2_frame_single_suppressed()
    )
    # residual = 100 - 80 = 20; n=1 → fill 20 (same as single-cell subtraction)
    row = recovered[recovered["ActivityProducedBy"] == "562213"].iloc[0]
    assert float(row["FlowAmount"]) == pytest.approx(20.0)
    assert row["SuppressionRecovery"] == "equal_residual"
    assert any("Table 2 revenue" in n and "equal_residual" in n for n in notes)


def test_no_suppressed_is_noop() -> None:
    df = pd.DataFrame(
        {
            "ActivityConsumedBy": ["562", "562111", "562212"],
            "FlowAmount": [100.0, 60.0, 40.0],
            "Suppressed": [None, None, None],
        }
    )
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(df)
    assert list(recovered["FlowAmount"]) == [100.0, 60.0, 40.0]
    assert any("no suppressed" in n for n in notes)


def test_missing_parent_skips() -> None:
    df = pd.DataFrame(
        {
            "ActivityConsumedBy": ["562111", "562112"],
            "FlowAmount": [50.0, 0.0],
            "Suppressed": [None, "S"],
        }
    )
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(df)
    assert float(recovered.loc[1, "FlowAmount"]) == 0.0
    assert any("skipped" in n and "parent" in n for n in notes)


def test_nonpositive_residual_skips() -> None:
    df = pd.DataFrame(
        {
            "ActivityConsumedBy": ["562", "562111", "562112"],
            "FlowAmount": [50.0, 60.0, 0.0],
            "Suppressed": [None, None, "S"],
        }
    )
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(df)
    assert float(recovered.loc[2, "FlowAmount"]) == 0.0
    assert any("residual" in n and "skipped" in n for n in notes)
