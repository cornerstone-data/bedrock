"""Unit tests for published vs internal E under electricity reaggregation."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.utils.schemas.cornerstone_schemas import (
    CORNERSTONE_INDUSTRIES,
    ELECTRICITY_DISAGG_SECTORS,
)


@patch(
    'bedrock.transform.eeio.cornerstone_disagg_pipeline.electricity_reaggregation_enabled',
    return_value=True,
)
@patch('bedrock.transform.allocation.derived.load_E_from_flowsa')
def test_derive_E_usa_collapses_children_when_reagg(
    mock_load: MagicMock,
    _mock_reagg: MagicMock,
) -> None:
    cols = ['1111A0', '221100', *ELECTRICITY_DISAGG_SECTORS, '311111']
    mock_load.return_value = pd.DataFrame(
        [[1.0, 0.0, 10.0, 20.0, 30.0, 2.0]],
        index=['CO2'],
        columns=cols,
    )
    out = derive_E_usa()
    assert list(out.columns) == list(CORNERSTONE_INDUSTRIES)
    assert all(c not in out.columns for c in ELECTRICITY_DISAGG_SECTORS)
    assert out.loc['CO2', '221100'] == 60.0
    assert out.loc['CO2', '1111A0'] == 1.0


@patch(
    'bedrock.transform.eeio.cornerstone_disagg_pipeline.electricity_reaggregation_enabled',
    return_value=False,
)
@patch('bedrock.transform.allocation.derived.load_E_from_flowsa')
def test_derive_E_usa_passthrough_when_reagg_off(
    mock_load: MagicMock,
    _mock_reagg: MagicMock,
) -> None:
    e = pd.DataFrame([[1.0, 2.0]], index=['CO2'], columns=['1111A0', '221110'])
    mock_load.return_value = e
    out = derive_E_usa()
    pd.testing.assert_frame_equal(out, e)
