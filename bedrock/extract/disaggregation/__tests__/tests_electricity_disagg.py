"""Tests for the electricity disaggregation workstream."""

from __future__ import annotations

import math
from unittest.mock import patch

import pandas as pd
import pytest

import bedrock.transform.eeio.derived_cornerstone as derived_cornerstone
from bedrock.extract.disaggregation.egrid_generation import (
    us_total_net_generation_mwh,
)

# stewi eGRID_2022_v1.2.0_7562dc3 flowbyfacility, US plant net generation sum
_EGRID_2022_NET_GENERATION_MWH = 4_247_477_433


def test_derive_cornerstone_ytot_full_cs_matrix_is_copy_of_underlying() -> None:
    # Electricity disaggregation step 1 (PR1): export of waste-disaggregated matrices.
    # This test guards the thin wrapper used to produce cornerstone_Ytot_full_cs.csv
    # (full commodity x FD Y matrix in cornerstone space, including trade columns).
    fake = pd.DataFrame({"F1": [1.0, 2.0]}, index=["c1", "c2"])
    with patch.object(
        derived_cornerstone,
        "_derive_cornerstone_Ytot_with_trade",
        return_value=fake,
    ):
        out = derived_cornerstone.derive_cornerstone_Ytot_full_cs_matrix()
    pd.testing.assert_frame_equal(out, fake)
    assert out is not fake


@pytest.mark.eeio_integration
def test_us_total_net_generation_mwh_2022_matches_stewi_egrid() -> None:
    """Load stewi eGRID 2022 and confirm US net generation matches published inventory."""
    actual = us_total_net_generation_mwh(2022, download_if_missing=True)
    assert math.isclose(
        actual,
        _EGRID_2022_NET_GENERATION_MWH,
        rel_tol=0,
        abs_tol=1.0,
    )
