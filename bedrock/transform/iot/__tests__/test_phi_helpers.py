"""Tests for PRO:PUR (Phi) helper functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from bedrock.transform.iot.derive_PRO_to_PUR_ratio import (
    apply_phi_to_ef_vector,
    derive_phi_cornerstone_usa_panel_published,
    margins_phi_active,
    phi_for_sectors,
)
from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_COMMODITIES,
    ELECTRICITY_DISAGG_SECTORS,
)


class TestMarginsPhiActive:
    def test_active_when_useeio_margins(self) -> None:
        cfg = USAConfig(useeio_margins=True, cornerstone_industry_avg_margins=False)
        assert margins_phi_active(cfg) is True

    def test_active_when_cornerstone_margins(self) -> None:
        cfg = USAConfig(cornerstone_industry_avg_margins=True)
        assert margins_phi_active(cfg) is True

    def test_inactive_when_no_margins_flag(self) -> None:
        cfg = USAConfig(cornerstone_industry_avg_margins=False)
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


@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.get_usa_config',
)
@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.margins_phi_active',
    return_value=True,
)
@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.derive_phi_cornerstone_usa_at_year'
)
def test_phi_electricity_children_forced_to_one_when_disaggregation_on(
    mock_phi: MagicMock,
    _mock_active: MagicMock,
    mock_cfg: MagicMock,
) -> None:
    mock_cfg.return_value.implement_electricity_disaggregation = True
    mock_cfg.return_value.model_base_year = 2024
    mock_phi.return_value = pd.Series(
        {'1111A0': 0.5, '221110': 0.8, '221121': 0.7, '221122': 0.6}
    )
    idx = pd.Index(['1111A0', '221110', '221121', '221122', '311111'], name='sector')
    got = phi_for_sectors(idx)
    assert got['221110'] == 1.0
    assert got['221121'] == 1.0
    assert got['221122'] == 1.0
    assert got['1111A0'] == 0.5
    assert got['311111'] == 1.0


@patch(
    'bedrock.transform.eeio.cornerstone_disagg_pipeline.electricity_reaggregation_enabled',
    return_value=True,
)
@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.derive_phi_cornerstone_usa_panel',
)
def test_published_phi_reagg_drops_children_and_sets_parent(
    mock_panel: MagicMock,
    _mock_reagg: MagicMock,
) -> None:
    years = (2023, 2024)
    mock_panel.return_value = pd.DataFrame(
        {
            '2023': {
                '1111A0': 0.5,
                '221100': 0.9,
                '221110': 0.8,
                '221121': 0.7,
                '221122': 0.6,
            },
            '2024': {
                '1111A0': 0.4,
                '221100': 0.85,
                '221110': 0.75,
                '221121': 0.65,
                '221122': 0.55,
            },
        }
    )
    try:
        out = derive_phi_cornerstone_usa_panel_published(years)
    finally:
        derive_phi_cornerstone_usa_panel_published.cache_clear()

    assert list(out.index) == list(CORNERSTONE_COMMODITIES)
    assert all(c not in out.index for c in ELECTRICITY_DISAGG_SECTORS)
    assert out.loc['221100', '2023'] == 1.0
    assert out.loc['221100', '2024'] == 1.0
    assert out.loc['1111A0', '2023'] == 0.5


@patch(
    'bedrock.transform.eeio.cornerstone_disagg_pipeline.electricity_reaggregation_enabled',
    return_value=False,
)
@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.get_usa_config',
)
@patch(
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio.derive_phi_cornerstone_usa_panel',
)
def test_published_phi_disagg_sets_children_to_one(
    mock_panel: MagicMock,
    mock_cfg: MagicMock,
    _mock_reagg: MagicMock,
) -> None:
    mock_cfg.return_value.implement_electricity_disaggregation = True
    years = (2024,)
    mock_panel.return_value = pd.DataFrame(
        {
            '2024': {
                '1111A0': 0.5,
                '221110': 0.8,
                '221121': 0.7,
                '221122': 0.6,
            },
        }
    )
    try:
        out = derive_phi_cornerstone_usa_panel_published(years)
    finally:
        derive_phi_cornerstone_usa_panel_published.cache_clear()

    assert out.loc['221110', '2024'] == 1.0
    assert out.loc['221121', '2024'] == 1.0
    assert out.loc['221122', '2024'] == 1.0
    assert out.loc['1111A0', '2024'] == 0.5
