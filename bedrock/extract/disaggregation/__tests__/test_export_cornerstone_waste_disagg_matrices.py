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
from bedrock.utils.config.usa_config import USAConfig


def _canonical_pins() -> dict[str, object]:
    """Mirror of the YAML pins; kept in the test so a stray YAML edit fails here too."""
    return {
        "use_cornerstone_2026_model_schema": True,
        "load_E_from_flowsa": True,
        "new_ghg_method": True,
        "use_E_data_year_for_x_in_B": True,
        "implement_waste_disaggregation": True,
    }


def _config_mock_for(pins: dict[str, object]) -> MagicMock:
    """Build a ``MagicMock(spec=USAConfig)`` carrying *pins* as attributes.

    Using ``spec=USAConfig`` makes unknown-attribute access raise
    ``AttributeError`` (so ``getattr(cfg, "nonexistent_flag", None)`` returns
    ``None``, as the precondition expects), and also catches typos in pin
    names at test-construction time.
    """
    m = MagicMock(spec=USAConfig)
    for key, val in pins.items():
        setattr(m, key, val)
    return m


def test_load_required_yaml_pins_returns_expected_keys() -> None:
    """Drift gate: reads the on-disk
    ``bedrock/utils/config/configs/2025_usa_cornerstone_full_model.yaml`` to ensure
    its pinned keys match the canonical pin set the precondition expects. Do NOT
    "fix" this test by injecting a ``tmp_path`` YAML — that would silently remove
    the drift detector.
    """
    pins = exp._load_required_yaml_pins()
    assert pins == _canonical_pins()


def test_assert_cornerstone_matrix_export_preconditions_passes() -> None:
    with patch.object(
        exp, "get_usa_config", return_value=_config_mock_for(_canonical_pins())
    ):
        with patch.object(exp, "get_waste_disagg_weights", return_value=MagicMock()):
            exp.assert_cornerstone_matrix_export_preconditions()


def test_assert_cornerstone_matrix_export_preconditions_raises_without_waste_weights() -> (
    None
):
    with patch.object(
        exp, "get_usa_config", return_value=_config_mock_for(_canonical_pins())
    ):
        with patch.object(exp, "get_waste_disagg_weights", return_value=None):
            with pytest.raises(RuntimeError, match=r"waste disaggregation"):
                exp.assert_cornerstone_matrix_export_preconditions()


def test_assert_cornerstone_matrix_export_preconditions_raises_on_flag_mismatch() -> (
    None
):
    pins = _canonical_pins()
    pins["load_E_from_flowsa"] = False
    cfg = _config_mock_for(pins)
    with patch.object(exp, "get_usa_config", return_value=cfg):
        with patch.object(exp, "get_waste_disagg_weights", return_value=MagicMock()):
            with pytest.raises(RuntimeError, match=r"load_E_from_flowsa") as excinfo:
                exp.assert_cornerstone_matrix_export_preconditions()
    assert exp._REQUIRED_CONFIG_FILE in str(excinfo.value)


def test_assert_cornerstone_matrix_export_preconditions_raises_on_missing_attr() -> (
    None
):
    cfg = _config_mock_for(_canonical_pins())
    with patch.object(
        exp, "_load_required_yaml_pins", return_value={"nonexistent_flag": True}
    ):
        with patch.object(exp, "get_usa_config", return_value=cfg):
            with patch.object(
                exp, "get_waste_disagg_weights", return_value=MagicMock()
            ):
                with pytest.raises(RuntimeError, match=r"nonexistent_flag"):
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
        exp, "get_usa_config", return_value=_config_mock_for(_canonical_pins())
    ):
        with patch.object(exp, "get_waste_disagg_weights", return_value=MagicMock()):
            with patch.object(exp, "clear_publish_caches"):
                with patch.object(exp, "derive_cornerstone_V", return_value=V):
                    with patch.object(
                        exp, "derive_cornerstone_U_set", return_value=uset
                    ):
                        with patch.object(
                            exp, "derive_cornerstone_VA", return_value=VA
                        ):
                            with patch.object(
                                exp,
                                "derive_cornerstone_Ytot_full_cs_matrix",
                                return_value=Y,
                            ):
                                with patch.object(exp, "derive_E_usa", return_value=E):
                                    out = exp.export_cornerstone_matrices_to_csv(
                                        tmp_path
                                    )

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


def test_export_cornerstone_matrices_to_csv_invokes_clear_publish_caches_before_derives(
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

    manager = MagicMock()
    with (
        patch.object(exp, "clear_publish_caches") as m_clear,
        patch.object(exp, "derive_cornerstone_V", return_value=V) as m_V,
        patch.object(exp, "derive_cornerstone_U_set", return_value=uset) as m_uset,
        patch.object(exp, "derive_cornerstone_VA", return_value=VA) as m_VA,
        patch.object(
            exp, "derive_cornerstone_Ytot_full_cs_matrix", return_value=Y
        ) as m_Y,
        patch.object(exp, "derive_E_usa", return_value=E) as m_E,
        patch.object(
            exp, "get_usa_config", return_value=_config_mock_for(_canonical_pins())
        ),
        patch.object(exp, "get_waste_disagg_weights", return_value=MagicMock()),
    ):
        manager.attach_mock(m_clear, "clear")
        manager.attach_mock(m_V, "derive_V")
        manager.attach_mock(m_uset, "derive_U_set")
        manager.attach_mock(m_VA, "derive_VA")
        manager.attach_mock(m_Y, "derive_Y")
        manager.attach_mock(m_E, "derive_E")
        exp.export_cornerstone_matrices_to_csv(tmp_path)

    call_names = [c[0] for c in manager.mock_calls]
    m_clear.assert_called_once()
    derive_names = {"derive_V", "derive_U_set", "derive_VA", "derive_Y", "derive_E"}
    assert call_names.index("clear") < min(
        call_names.index(name) for name in derive_names
    )


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
