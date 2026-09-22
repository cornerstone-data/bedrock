"""Unit tests for BR shipper→receiver RCRA intersection builder."""

from __future__ import annotations

from typing import cast

import pandas as pd

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
