"""Unit tests for `bedrock.publish.excel.writer`.

These tests monkeypatch the registry-building function with small
synthetic getters so we can exercise the writer's behavior (skip-if-None,
empty-data-sheets guard, location-suffix application, metadata content,
round-trip correctness) without standing up the real EEIO pipeline.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from bedrock.publish.excel import writer as writer_module


def _empty_registry(config_name: str) -> list[writer_module.SheetSpec]:
    """All sheets resolve to None -- exercises the empty-data-sheets guard."""
    return [
        writer_module.SheetSpec('B', lambda: None),
        writer_module.SheetSpec('C', lambda: None),
        writer_module.SheetSpec(
            'model_info',
            lambda: pd.DataFrame([{'field': 'config_name', 'value': config_name}]),
        ),
    ]


def _synthetic_registry(config_name: str) -> list[writer_module.SheetSpec]:
    """Two sectors and three GHGs; covers DataFrame and Series paths."""
    sector_idx = pd.Index(['100000', '200000'], name='sector')
    ghg_idx = pd.Index(['CO2', 'CH4', 'N2O'], name='ghg')
    indicator_idx = pd.Index(['Greenhouse Gases'], name='indicator')

    B = pd.DataFrame(
        [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        index=ghg_idx,
        columns=sector_idx,
    )
    C = pd.DataFrame([[1.0, 1.0, 1.0]], index=indicator_idx, columns=ghg_idx)
    q = pd.Series([100.0, 200.0], index=sector_idx, name='q')

    return [
        writer_module.SheetSpec('B', lambda: B),
        writer_module.SheetSpec('C', lambda: C),
        writer_module.SheetSpec('q', lambda: q),
        writer_module.SheetSpec('A', lambda: None),
        writer_module.SheetSpec(
            'model_info',
            lambda: pd.DataFrame(
                [
                    {'field': 'config_name', 'value': config_name},
                    {'field': 'b_units', 'value': 'kg CO2e / USD'},
                ]
            ),
        ),
    ]


def test_raises_when_no_data_sheets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = os.fspath(tmp_path / 'empty.xlsx')
    monkeypatch.setattr(writer_module, '_build_matrix_registry', _empty_registry)
    with pytest.raises(RuntimeError, match='no data sheets'):
        writer_module.write_model_to_xlsx(out, config_name='dummy')
    assert not os.path.exists(
        out
    ), 'workbook should not be written when no data sheets materialize'


def test_writes_only_materialized_sheets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = os.fspath(tmp_path / 'model.xlsx')
    monkeypatch.setattr(writer_module, '_build_matrix_registry', _synthetic_registry)
    writer_module.write_model_to_xlsx(out, config_name='dummy')

    book = pd.read_excel(out, sheet_name=None, index_col=0, engine='openpyxl')
    assert set(book) == {
        'B',
        'C',
        'q',
        'model_info',
    }, f'expected B/C/q/model_info; got {sorted(book)}'


def test_loc_suffix_applied_only_to_sector_axis(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = os.fspath(tmp_path / 'model.xlsx')
    monkeypatch.setattr(writer_module, '_build_matrix_registry', _synthetic_registry)
    writer_module.write_model_to_xlsx(out, config_name='dummy')

    book = pd.read_excel(out, sheet_name=None, index_col=0, engine='openpyxl')

    B = book['B']
    assert list(B.index) == [
        'CO2',
        'CH4',
        'N2O',
    ], f'B ghg index should be unsuffixed; got {list(B.index)}'
    assert list(B.columns) == [
        '100000/US',
        '200000/US',
    ], f'B sector columns should carry /US suffix; got {list(B.columns)}'

    C = book['C']
    assert list(C.index) == ['Greenhouse Gases']
    assert list(C.columns) == ['CO2', 'CH4', 'N2O']

    q = book['q']
    assert list(q.index) == ['100000/US', '200000/US']


def test_round_trip_values_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = os.fspath(tmp_path / 'model.xlsx')
    monkeypatch.setattr(writer_module, '_build_matrix_registry', _synthetic_registry)
    writer_module.write_model_to_xlsx(out, config_name='dummy')

    book = pd.read_excel(out, sheet_name=None, index_col=0, engine='openpyxl')
    B = book['B']
    assert B.shape == (3, 2)
    assert B.loc['CO2', '100000/US'] == pytest.approx(1.0)
    assert B.loc['N2O', '200000/US'] == pytest.approx(6.0)

    q = book['q']
    assert q.shape == (2, 1)
    assert q.loc['100000/US', 'q'] == pytest.approx(100.0)
