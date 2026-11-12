from __future__ import annotations

import pandas as pd
import pytest
from click.testing import CliRunner

from bedrock.utils.snapshots import compare_snapshots as cs

FRAME = pd.DataFrame({'221100': [1.0, 2.0], '42ELEC': [0.5, 0.0]}, index=['CO2', 'SF6'])


def _patch(monkeypatch: pytest.MonkeyPatch, frames: dict[str, pd.DataFrame]) -> None:
    monkeypatch.setattr(cs, 'load_snapshot', lambda name, key: frames[key])


def test_matching_prefixes_report_no_difference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch(monkeypatch, {'a': FRAME, 'b': FRAME.copy()})
    assert cs.compare_snapshots('a', 'b') == {}


def test_last_bit_noise_is_not_a_difference(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(monkeypatch, {'a': FRAME, 'b': FRAME * (1 + 1e-13)})
    assert cs.compare_snapshots('a', 'b', names=['B_USA']) == {}


@pytest.mark.parametrize(
    'changed',
    [
        FRAME * 1.001,
        FRAME.drop(columns=['42ELEC']),
        FRAME.rename(index={'SF6': 'NF3'}),
        FRAME.astype('float32'),
    ],
)
def test_any_change_is_a_difference(
    monkeypatch: pytest.MonkeyPatch, changed: pd.DataFrame
) -> None:
    _patch(monkeypatch, {'a': FRAME, 'b': changed})
    assert list(cs.compare_snapshots('a', 'b', names=['B_USA'])) == ['B_USA']


def test_the_command_fails_when_an_object_differs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch(monkeypatch, {'a': FRAME, 'b': FRAME * 2})
    result = CliRunner().invoke(cs.main, ['a', 'b'])
    assert result.exit_code == 1
    assert f'{len(cs.SNAPSHOT_NAMES)} of {len(cs.SNAPSHOT_NAMES)} objects differ' in (
        result.output
    )


def test_the_command_passes_when_every_object_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch(monkeypatch, {'a': FRAME, 'b': FRAME.copy()})
    result = CliRunner().invoke(cs.main, ['a', 'b'])
    assert result.exit_code == 0
    assert 'objects match' in result.output
