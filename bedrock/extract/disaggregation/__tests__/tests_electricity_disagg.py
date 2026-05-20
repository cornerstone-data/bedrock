"""Tests for the electricity disaggregation workstream."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

import bedrock.transform.eeio.derived_cornerstone as derived_cornerstone


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
