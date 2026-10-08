"""RCRA path records ``rcra_path=br_bypass`` (hermetic).

Full ``derive_waste_weights`` against live BR CSVs is not CI-safe: consolidated
``br_reporting_*.csv`` files can trip pandas' C tokenizer on the runner. The
token is stamped in ``load_rcra_intersection_shares``; assert it there with a
minimal fake BR frame.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from bedrock.extract.disaggregation.rcra_waste_flows import (
    load_rcra_intersection_shares,
)


def _minimal_br() -> pd.DataFrame:
    """One waste→waste received-ton row so the share matrix is non-zero."""
    return pd.DataFrame(
        [
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
                "Handler ID": "R1",
                "Primary NAICS": "562212",
                "Shipper ID": "S1",
                "Receiver ID": "R1",
                "Received Tons": 100.0,
                "Shipped Tons": 0.0,
                "Receiver Waste Stream Included in NBR": "Y",
                "Shipper Waste Stream Included in NBR": "Y",
            },
        ]
    )


def test_load_rcra_intersection_shares_records_br_bypass() -> None:
    with patch(
        "bedrock.extract.disaggregation.rcra_waste_flows.load_br_reporting",
        return_value=_minimal_br(),
    ):
        mat, notes = load_rcra_intersection_shares(2021)
    joined = " | ".join(notes)
    assert "rcra_path=br_bypass" in joined
    assert "bypasses CRHW FBS" in joined
    assert abs(float(mat.to_numpy().sum()) - 1.0) < 1e-9
