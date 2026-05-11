"""Tests for waste-disaggregated cornerstone matrix CSV export and Y full-matrix wrapper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import bedrock.transform.eeio.derived_cornerstone as derived_cornerstone
from bedrock.extract.disaggregation import (
    export_cornerstone_waste_disagg_matrices as exp,
)


def _canonical_full_model_config_mock() -> MagicMock:
    m = MagicMock()
    for key, val in exp._FULL_MODEL_MATRIX_EXPORT_FLAGS.items():
        setattr(m, key, val)
    return m


def test_assert_cornerstone_matrix_export_preconditions_passes() -> None:
    with patch.object(
        exp, "get_usa_config", return_value=_canonical_full_model_config_mock()
    ):
        with patch.object(exp, "get_waste_disagg_weights", return_value=MagicMock()):
            exp.assert_cornerstone_matrix_export_preconditions()


def test_assert_cornerstone_matrix_export_preconditions_raises_without_waste_weights() -> (
    None
):
    with patch.object(
        exp, "get_usa_config", return_value=_canonical_full_model_config_mock()
    ):
        with patch.object(exp, "get_waste_disagg_weights", return_value=None):
            with pytest.raises(RuntimeError, match="waste disaggregation"):
                exp.assert_cornerstone_matrix_export_preconditions()


def test_assert_cornerstone_matrix_export_preconditions_raises_on_flag_mismatch() -> (
    None
):
    cfg = _canonical_full_model_config_mock()
    cfg.load_E_from_flowsa = False
    with patch.object(exp, "get_usa_config", return_value=cfg):
        with patch.object(exp, "get_waste_disagg_weights", return_value=MagicMock()):
            with pytest.raises(RuntimeError, match="load_E_from_flowsa"):
                exp.assert_cornerstone_matrix_export_preconditions()


def test_export_cornerstone_matrices_to_csv_writes_expected_csvs(
    tmp_path: Path,
) -> None:
    V = pd.DataFrame([[1.0]], index=["i0"], columns=["c0"])
    Udom = pd.DataFrame([[2.0]], index=["c0"], columns=["i0"])
    Uimp = pd.DataFrame([[3.0]], index=["c0"], columns=["i0"])
    VA = pd.DataFrame([[4.0]], index=["V0"], columns=["i0"])
    Y = pd.DataFrame([[5.0]], index=["c0"], columns=["F0"])
    E = pd.DataFrame([[6.0]], index=["GHG0"], columns=["i0"])
    uset = MagicMock()
    uset.Udom = Udom
    uset.Uimp = Uimp

    with patch.object(
        exp, "get_usa_config", return_value=_canonical_full_model_config_mock()
    ):
        with patch.object(exp, "get_waste_disagg_weights", return_value=MagicMock()):
            with patch.object(exp, "derive_cornerstone_V", return_value=V):
                with patch.object(exp, "derive_cornerstone_U_set", return_value=uset):
                    with patch.object(exp, "derive_cornerstone_VA", return_value=VA):
                        with patch.object(
                            exp,
                            "derive_cornerstone_Ytot_full_cs_matrix",
                            return_value=Y,
                        ):
                            with patch.object(exp, "derive_E_usa", return_value=E):
                                out = exp.export_cornerstone_matrices_to_csv(tmp_path)

    assert out.resolve() == tmp_path.resolve()
    names = {p.name for p in tmp_path.iterdir()}
    assert names == {
        "cornerstone_V.csv",
        "cornerstone_Udom.csv",
        "cornerstone_Uimp.csv",
        "cornerstone_VA.csv",
        "cornerstone_Ytot_full_cs.csv",
        "cornerstone_E.csv",
    }
    round_trip = pd.read_csv(tmp_path / "cornerstone_V.csv", index_col=0)
    pd.testing.assert_frame_equal(round_trip, V)


def test_derive_cornerstone_ytot_full_cs_matrix_is_copy_of_underlying() -> None:
    fake = pd.DataFrame({"F1": [1.0, 2.0]}, index=["c1", "c2"])
    with patch.object(
        derived_cornerstone,
        "_derive_cornerstone_Ytot_with_trade",
        return_value=fake,
    ):
        out = derived_cornerstone.derive_cornerstone_Ytot_full_cs_matrix()
    pd.testing.assert_frame_equal(out, fake)
    assert out is not fake
