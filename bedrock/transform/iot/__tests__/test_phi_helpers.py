"""Tests for PRO:PUR (Phi) helper functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from bedrock.transform.iot.derive_PRO_to_PUR_ratio import (
    apply_phi_to_ef_vector,
    margins_phi_active,
    phi_for_sectors,
)
from bedrock.utils.config.usa_config import USAConfig


class TestMarginsPhiActive:
    def test_active_when_useeio_margins(self) -> None:
        cfg = USAConfig(useeio_margins=True)
        assert margins_phi_active(cfg) is True

    def test_inactive_when_no_margins_flag(self) -> None:
        cfg = USAConfig()
        assert margins_phi_active(cfg) is False


@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.margins_phi_active',
    return_value=True,
)
@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.derive_phi_cornerstone_usa_at_year'
)
def test_apply_phi_to_ef_vector(mock_phi: MagicMock, _mock_active: MagicMock) -> None:
    mock_phi.return_value = pd.Series({'1111A0': 0.5, '221100': 0.8})
    ef = pd.Series({'1111A0': 10.0, '221100': 20.0, '311111': 2.0})
    got = apply_phi_to_ef_vector(ef, year=2024)
    mock_phi.assert_called_once_with(2024)
    assert got['1111A0'] == 5.0
    assert got['221100'] == 16.0
    assert got['311111'] == 2.0


@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.margins_phi_active',
    return_value=False,
)
def test_phi_for_sectors_identity_when_inactive(_mock_active: MagicMock) -> None:
    idx = pd.Index(['1111A0', '221100'], name='sector')
    got = phi_for_sectors(idx)
    pd.testing.assert_series_equal(got, pd.Series(1.0, index=idx, dtype=float))
