"""Hermetic tests for industry-mix fail-loud AIES and SAS→AIES chain."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from bedrock.extract.disaggregation import derive_waste_weights as m
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN
from bedrock.extract.disaggregation.waste_weight_types import WeightDerivationProvenance


def _prov(year: int = 2023) -> WeightDerivationProvenance:
    return WeightDerivationProvenance(
        target_year=year,
        rcra_source_year=2021,
        ec_source_year=2022,
    )


def _uniform_shares() -> pd.Series:
    return pd.Series(1.0 / len(WASTE_CHILDREN), index=WASTE_CHILDREN)


def test_aies_failure_raises_under_match_io(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(m, "_match_io_active", lambda: True)
    monkeypatch.setattr(m, "_chain_waste_industry_mix", lambda: False)
    monkeypatch.setattr(
        m, "resolve_industry_mix_source", lambda _y: ("aies_exp01", 2023)
    )

    def _boom(*_a: object, **_k: object) -> pd.Series:
        raise RuntimeError("injected AIES failure")

    monkeypatch.setattr(m, "load_aies_child_expense_shares", _boom)
    with pytest.raises(RuntimeError, match="match_io"):
        m._industry_mix_shares(2023, _prov())


def test_aies_failure_falls_back_when_not_match_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(m, "_match_io_active", lambda: False)
    monkeypatch.setattr(m, "_chain_waste_industry_mix", lambda: False)
    monkeypatch.setattr(
        m, "resolve_industry_mix_source", lambda _y: ("aies_exp01", 2023)
    )

    def _boom(*_a: object, **_k: object) -> pd.Series:
        raise RuntimeError("injected AIES failure")

    monkeypatch.setattr(m, "load_aies_child_expense_shares", _boom)
    fallback = _uniform_shares()
    monkeypatch.setattr(
        m,
        "load_sas_table3_expense_shares",
        lambda *_a, **_k: (fallback, ["sas note"]),
    )
    prov = _prov()
    shares = m._industry_mix_shares(2023, prov)
    assert shares.equals(fallback)
    assert any("falling back to 2022 SAS" in n for n in prov.fallback_notes)


def test_chain_2023_holds_post_fill_sas_2022(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(m, "_match_io_active", lambda: True)
    monkeypatch.setattr(m, "_chain_waste_industry_mix", lambda: True)
    held = pd.Series([0.40, 0.10, 0.10, 0.10, 0.10, 0.10, 0.10], index=WASTE_CHILDREN)
    held = held / float(held.sum())
    monkeypatch.setattr(
        m,
        "load_sas_table3_expense_shares",
        lambda *_a, **_k: (held, ["prior_weighted note"]),
    )
    aies = MagicMock(side_effect=AssertionError("AIES must not load for 2023 chain"))
    monkeypatch.setattr(m, "load_aies_child_expense_shares", aies)
    prov = _prov(2023)
    shares = m._industry_mix_shares(2023, prov)
    pd.testing.assert_series_equal(shares, held, check_names=False)
    assert any("holds post-fill SAS 2022" in n for n in prov.fallback_notes)


def test_chain_2024_applies_aies_ratios_then_renormalises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(m, "_match_io_active", lambda: True)
    monkeypatch.setattr(m, "_chain_waste_industry_mix", lambda: True)
    held = pd.Series(1.0 / len(WASTE_CHILDREN), index=WASTE_CHILDREN)
    aies_2023 = held.copy()
    aies_2024 = held.copy()
    aies_2024.iloc[0] = 0.25
    aies_2024.iloc[1:] = 0.75 / (len(WASTE_CHILDREN) - 1)
    monkeypatch.setattr(
        m,
        "load_sas_table3_expense_shares",
        lambda *_a, **_k: (held, []),
    )

    def _aies(year: int, *, table: str) -> pd.Series:
        del table
        return aies_2023 if year == 2023 else aies_2024

    monkeypatch.setattr(m, "load_aies_child_expense_shares", _aies)
    prov = _prov(2024)
    shares = m._industry_mix_shares(2024, prov)
    assert pytest.approx(float(shares.sum())) == 1.0
    # First child grows relative to held; others shrink after renormalise.
    assert float(shares.iloc[0]) > float(held.iloc[0])
    assert any("AIES 2024 / AIES 2023" in n for n in prov.fallback_notes)
