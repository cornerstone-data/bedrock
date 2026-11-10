"""EC Business firms Use row-sum replaces SAS Table 2 on the derive path."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bedrock.extract.disaggregation import derive_waste_weights as m
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN


def _uniform() -> pd.Series:
    return pd.Series(1.0 / len(WASTE_CHILDREN), index=WASTE_CHILDREN)


def _stub_common_derive(monkeypatch: pytest.MonkeyPatch) -> None:
    use = pd.DataFrame(
        {
            "IndustryCode": ["562000"] * len(WASTE_CHILDREN),
            "CommodityCode": list(WASTE_CHILDREN),
            "PercentUsed": list(_uniform().values),
            "Note": ["Use row sum, commodity output"] * len(WASTE_CHILDREN),
        }
    )
    make = pd.DataFrame(
        {
            "IndustryCode": ["562000"] * len(WASTE_CHILDREN),
            "CommodityCode": list(WASTE_CHILDREN),
            "PercentMake": list(_uniform().values),
            "Note": ["Make column sum"] * len(WASTE_CHILDREN),
        }
    )
    monkeypatch.setattr(m, "_load_bundled_long", lambda: (use.copy(), make.copy()))

    def _industry_mix(
        target_year: int, prov: object, *_a: object, **_k: object
    ) -> pd.Series:
        # Mirror production: industry mix still sets sas_source_year when SAS feeds.
        setattr(prov, "sas_source_year", min(int(target_year), 2022))
        return _uniform()

    monkeypatch.setattr(m, "_industry_mix_shares", _industry_mix)
    monkeypatch.setattr(
        m,
        "load_rcra_intersection_shares",
        lambda *_a, **_k: (
            pd.DataFrame(
                1.0 / len(WASTE_CHILDREN),
                index=WASTE_CHILDREN,
                columns=WASTE_CHILDREN,
            ),
            [],
        ),
    )
    monkeypatch.setattr(m, "customer_class_shares_from_ec_fba", lambda *_a, **_k: None)
    monkeypatch.setattr(
        m,
        "load_bundled_customer_class_rows",
        lambda *_a, **_k: pd.DataFrame(),
    )
    monkeypatch.setattr(
        m,
        "build_disagg_weights_from_long_format",
        lambda use, make, **_k: MagicMock(name="weights", use=use, make=make),
    )


def test_match_io_hard_raises_on_business_firms_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_common_derive(monkeypatch)
    monkeypatch.setattr(m, "_match_io_active", lambda: True)
    monkeypatch.setattr(m, "resolve_ec_year", lambda *_a, **_k: 2022)

    def _boom(*_a: object, **_k: object) -> pd.DataFrame:
        raise RuntimeError("injected EC failure")

    monkeypatch.setattr(m, "load_census_ec_fba", _boom)
    with pytest.raises(RuntimeError, match="match_io"):
        m.derive_waste_weights(2022)


def test_non_match_io_keeps_bundled_row_sum_not_sas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_common_derive(monkeypatch)
    monkeypatch.setattr(m, "_match_io_active", lambda: False)
    monkeypatch.setattr(m, "resolve_ec_year", lambda *_a, **_k: 2022)

    def _boom(*_a: object, **_k: object) -> pd.DataFrame:
        raise RuntimeError("injected EC failure")

    monkeypatch.setattr(m, "load_census_ec_fba", _boom)
    weights, prov = m.derive_waste_weights(2022)
    assert any("bundled 2017 Use row-sum" in n for n in prov.fallback_notes)
    assert any("not SAS Table 2" in n for n in prov.fallback_notes)
    del weights


def test_business_firms_shares_replace_row_sum(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_common_derive(monkeypatch)
    monkeypatch.setattr(m, "_match_io_active", lambda: False)
    monkeypatch.setattr(m, "resolve_ec_year", lambda *_a, **_k: 2017)
    shares = pd.Series(
        [0.5, 0.1, 0.1, 0.05, 0.1, 0.1, 0.05], index=WASTE_CHILDREN, dtype=float
    )
    fake_fba = pd.DataFrame({"_": [1]})
    load_calls = {"n": 0}

    def _load(_year: int) -> pd.DataFrame:
        load_calls["n"] += 1
        return fake_fba

    monkeypatch.setattr(m, "load_census_ec_fba", _load)
    monkeypatch.setattr(
        m,
        "build_business_firms_row_sum_shares_from_ec_fba",
        lambda _fba: shares,
    )

    captured: dict[str, pd.DataFrame] = {}

    def _capture(use: pd.DataFrame, make: pd.DataFrame, **_k: object) -> MagicMock:
        captured["use"] = use
        return MagicMock(name="weights")

    monkeypatch.setattr(m, "build_disagg_weights_from_long_format", _capture)
    _weights, prov = m.derive_waste_weights(2017)
    row = captured["use"]
    row = row[row["Note"] == "Use row sum, commodity output"].set_index("CommodityCode")
    assert float(cast(float, row.loc["562111", "PercentUsed"])) == pytest.approx(0.5)
    assert prov.ec_source_year == 2017
    assert prov.sas_source_year == 2017
    assert not any("Business firms" in n for n in prov.fallback_notes)
    # One FBA load shared with customer-class path.
    assert load_calls["n"] == 1


def test_single_ec_fba_reused_for_customer_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_common_derive(monkeypatch)
    monkeypatch.setattr(m, "_match_io_active", lambda: False)
    monkeypatch.setattr(m, "resolve_ec_year", lambda *_a, **_k: 2022)
    fake_fba = pd.DataFrame({"marker": [1]})
    load_calls = {"n": 0}
    seen: dict[str, pd.DataFrame] = {}

    def _load(_year: int) -> pd.DataFrame:
        load_calls["n"] += 1
        return fake_fba

    def _customer(fba: pd.DataFrame) -> pd.DataFrame:
        seen["fba"] = fba
        return pd.DataFrame(
            {
                "IndustryCode": ["F01000"],
                "CommodityCode": ["562111"],
                "PercentUsed": [1.0],
                "Note": ["Use table, personal consumption"],
            }
        )

    monkeypatch.setattr(m, "load_census_ec_fba", _load)
    monkeypatch.setattr(
        m,
        "build_business_firms_row_sum_shares_from_ec_fba",
        lambda _fba: _uniform(),
    )
    monkeypatch.setattr(m, "customer_class_shares_from_ec_fba", _customer)
    m.derive_waste_weights(2022)
    assert load_calls["n"] == 1
    assert seen["fba"] is fake_fba
