"""Unit tests for BR shipper→receiver RCRA intersection builder."""

from __future__ import annotations

from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pandas as pd

from bedrock.extract.disaggregation import rcra_waste_flows as rcra_waste_flows_mod
from bedrock.extract.disaggregation.rcra_waste_flows import (
    build_facility_naics_map,
    build_intersection_mass_from_br,
)
from bedrock.extract.disaggregation.waste_static_rules import WASTE_CHILDREN


def _fake_br() -> pd.DataFrame:
    """Minimal BR-like rows: two waste facilities + one non-waste generator."""
    return pd.DataFrame(
        [
            # Facility NAICS map rows (handler appearances)
            {
                "Handler ID": "S1",
                "Primary NAICS": "562111",
                "Shipper ID": None,
                "Receiver ID": None,
                "Received Tons": 0,
                "Shipped Tons": 0,
                "Receiver Waste Stream Included in NBR": None,
                "Shipper Waste Stream Included in NBR": None,
            },
            {
                "Handler ID": "R1",
                "Primary NAICS": "562212",
                "Shipper ID": None,
                "Receiver ID": None,
                "Received Tons": 0,
                "Shipped Tons": 0,
                "Receiver Waste Stream Included in NBR": None,
                "Shipper Waste Stream Included in NBR": None,
            },
            {
                "Handler ID": "G1",
                "Primary NAICS": "325199",
                "Shipper ID": None,
                "Receiver ID": None,
                "Received Tons": 0,
                "Shipped Tons": 0,
                "Receiver Waste Stream Included in NBR": None,
                "Shipper Waste Stream Included in NBR": None,
            },
            # Receiver-reported waste→waste shipment (preferred)
            {
                "Handler ID": "R1",
                "Primary NAICS": "562212",
                "Shipper ID": "S1",
                "Receiver ID": "R1",
                "Received Tons": 80.0,
                "Shipped Tons": 0.0,
                "Receiver Waste Stream Included in NBR": "Y",
                "Shipper Waste Stream Included in NBR": "Y",
            },
            # Shipper-reported only (fallback) waste→waste
            {
                "Handler ID": "S1",
                "Primary NAICS": "562111",
                "Shipper ID": "S1",
                "Receiver ID": "R1",
                "Received Tons": 0.0,
                "Shipped Tons": 20.0,
                "Receiver Waste Stream Included in NBR": "Y",
                "Shipper Waste Stream Included in NBR": "Y",
            },
            # Non-waste shipper → waste receiver (excluded from intersection)
            {
                "Handler ID": "R1",
                "Primary NAICS": "562212",
                "Shipper ID": "G1",
                "Receiver ID": "R1",
                "Received Tons": 500.0,
                "Shipped Tons": 0.0,
                "Receiver Waste Stream Included in NBR": "Y",
                "Shipper Waste Stream Included in NBR": "Y",
            },
        ]
    )


def test_build_facility_naics_map() -> None:
    m = build_facility_naics_map(_fake_br())
    assert m["S1"] == "562111"
    assert m["R1"] == "562212"
    assert m["G1"] == "325199"


def test_intersection_prefers_received_and_filters_non_waste() -> None:
    mat, stats = build_intersection_mass_from_br(_fake_br())
    assert list(mat.index) == WASTE_CHILDREN
    assert list(mat.columns) == WASTE_CHILDREN
    # recv=562212, ship=562111: 80 (received) + 20 (shipped fallback) = 100
    assert abs(float(cast(float, mat.loc["562212", "562111"])) - 100.0) < 1e-9
    assert float(mat.to_numpy().sum()) == 100.0
    assert stats["rows_used_received"] >= 1
    assert stats["rows_used_waste_intersection"] == 2


def test_read_br_csv_logs_nrows_and_tons_on_python_fallback(tmp_path: Path) -> None:
    path = tmp_path / "br.csv"
    path.write_text(
        "Received Tons,Shipped Tons\n10.0,1.0\n20.0,2.0\n",
        encoding="utf-8",
    )
    good = pd.DataFrame({"Received Tons": [10.0, 20.0], "Shipped Tons": [1.0, 2.0]})

    def _read_csv(*args: object, **kwargs: object) -> pd.DataFrame:
        if kwargs.get("engine") == "python":
            return good
        raise pd.errors.ParserError("Buffer overflow")

    mock_log = MagicMock()
    with (
        patch.object(rcra_waste_flows_mod.pd, "read_csv", side_effect=_read_csv),
        patch.object(rcra_waste_flows_mod, "log", mock_log),
    ):
        out = rcra_waste_flows_mod._read_br_csv(path)

    assert list(out["Received Tons"]) == [10.0, 20.0]
    assert mock_log.warning.call_count == 2
    first = " ".join(str(a) for a in mock_log.warning.call_args_list[0].args)
    second = " ".join(str(a) for a in mock_log.warning.call_args_list[1].args)
    assert "C-engine BR parse failed" in first
    assert "nrows=%s" in second or "nrows=2" in second
    assert mock_log.warning.call_args_list[1].args[2] == 2
    assert mock_log.warning.call_args_list[1].args[3] == 30.0
    assert mock_log.warning.call_args_list[1].args[4] == 3.0
