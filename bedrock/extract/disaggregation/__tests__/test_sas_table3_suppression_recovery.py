"""Unit tests for SAS Table 2/3 waste suppression recovery."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.disaggregation.sas_waste_metrics import (
    _suppressed_mask,
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
    assert float(recovered.loc[1, "FlowAmount"]) == 0.0  # type: ignore[arg-type]
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
    assert float(recovered.loc[2, "FlowAmount"]) == 0.0  # type: ignore[arg-type]
    assert any("residual" in n and "skipped" in n for n in notes)


def test_sampling_error_s_not_suppressed_for_recovery() -> None:
    """``(s)`` is sampling-error markup; keep published amount in residual math."""
    df = pd.DataFrame(
        {
            "ActivityConsumedBy": ["562", "562111", "562212", "562112"],
            "FlowAmount": [100.0, 40.0, 20.0, 15.0],
            "Suppressed": [None, None, None, "(s)"],
        }
    )
    mask = _suppressed_mask(df)
    assert not bool(mask.iloc[3])
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(df)
    assert float(recovered.loc[3, "FlowAmount"]) == pytest.approx(15.0)
    assert any("no suppressed" in n for n in notes)


def test_z_not_in_suppressed_set() -> None:
    """``Z`` (rounds to zero) does not claim residual."""
    df = pd.DataFrame(
        {
            "ActivityConsumedBy": ["562", "562111", "562212", "562112"],
            "FlowAmount": [100.0, 50.0, 50.0, 0.0],
            "Suppressed": [None, None, None, "Z"],
        }
    )
    mask = _suppressed_mask(df)
    assert not bool(mask.iloc[3])
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(df)
    assert float(recovered.loc[3, "FlowAmount"]) == pytest.approx(0.0)
    assert any("no suppressed" in n for n in notes)


def test_table3_prior_weighted_unequal_fills() -> None:
    priors = {"562112": 10.0, "562211": 20.0, "562213": 70.0}
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(
        _table3_frame(), prior_by_naics=priors
    )
    # residual = 40; fills 4, 8, 28
    by_code = recovered.set_index("ActivityConsumedBy")["FlowAmount"]
    assert float(by_code.loc["562112"]) == pytest.approx(4.0)
    assert float(by_code.loc["562211"]) == pytest.approx(8.0)
    assert float(by_code.loc["562213"]) == pytest.approx(28.0)
    supp = recovered[recovered["Suppressed"] == "S"]
    assert (supp["SuppressionRecovery"] == "prior_weighted_residual").all()
    assert pytest.approx(float(supp["FlowAmount"].sum())) == 40.0
    assert any("prior_weighted_residual" in n for n in notes)


def test_table2_prior_weighted_multi_suppressed() -> None:
    rows = [
        ("562", 100.0, None),
        ("562111", 50.0, None),
        ("562212", 20.0, None),
        ("562112", 0.0, "S"),
        ("562213", 0.0, "S"),
    ]
    df = pd.DataFrame(rows, columns=["ActivityProducedBy", "FlowAmount", "Suppressed"])
    priors = {"562112": 5.0, "562213": 15.0}
    recovered, notes = recover_suppressed_sas_table2_waste_revenue(
        df, prior_by_naics=priors
    )
    # residual = 30; fills 7.5 and 22.5
    by_code = recovered.set_index("ActivityProducedBy")["FlowAmount"]
    assert float(by_code.loc["562112"]) == pytest.approx(7.5)
    assert float(by_code.loc["562213"]) == pytest.approx(22.5)
    supp = recovered[recovered["Suppressed"] == "S"]
    assert (supp["SuppressionRecovery"] == "prior_weighted_residual").all()
    assert any("prior_weighted_residual" in n for n in notes)


def test_partial_priors_two_pool() -> None:
    priors = {"562112": 10.0, "562211": 30.0}  # 562213 missing
    recovered, notes = recover_suppressed_sas_table3_waste_expenses(
        _table3_frame(), prior_by_naics=priors
    )
    # n=3, n_p=2, n_m=1; residual=40
    # residual_prior = 40 * 2/3; residual_equal = 40/3
    # 562112: (80/3)*(10/40)=20/3; 562211: (80/3)*(30/40)=20; 562213: 40/3
    by_code = recovered.set_index("ActivityConsumedBy")["FlowAmount"]
    assert float(by_code.loc["562112"]) == pytest.approx(20.0 / 3.0)
    assert float(by_code.loc["562211"]) == pytest.approx(20.0)
    assert float(by_code.loc["562213"]) == pytest.approx(40.0 / 3.0)
    assert (
        pytest.approx(float(by_code.loc[["562112", "562211", "562213"]].sum())) == 40.0
    )
    assert any("two-pool" in n and "missing_priors" in n for n in notes)


def test_require_complete_priors_raises_when_missing() -> None:
    with pytest.raises(ValueError, match="require_complete_priors"):
        recover_suppressed_sas_table3_waste_expenses(
            _table3_frame(),
            prior_by_naics={"562112": 10.0},
            require_complete_priors=True,
        )


def test_require_complete_priors_ok_with_full_map() -> None:
    priors = {"562112": 1.0, "562211": 1.0, "562213": 1.0}
    recovered, _ = recover_suppressed_sas_table3_waste_expenses(
        _table3_frame(),
        prior_by_naics=priors,
        require_complete_priors=True,
    )
    supp = recovered[recovered["Suppressed"] == "S"]
    assert (supp["SuppressionRecovery"] == "prior_weighted_residual").all()
    assert pytest.approx(float(supp["FlowAmount"].sum())) == 40.0
