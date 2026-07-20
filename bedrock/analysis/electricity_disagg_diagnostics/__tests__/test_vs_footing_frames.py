"""Unit tests for vs-footing EF frames and drop footnotes."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.analysis.electricity_disagg_diagnostics import vs_footing_frames as mod
from bedrock.analysis.electricity_disagg_diagnostics.vs_footing_frames import (
    DroppedSector,
    collect_electricity_drops,
    format_drop_footnote,
    humanize_exemption_reason,
)


def test_humanize_mixed_units_exemption() -> None:
    text = humanize_exemption_reason('unit_incommensurate_mixed_units')
    assert 'mixed units' in text
    assert 'kg/MWh' in text


def test_collect_drops_ordered_exemption_before_presence() -> None:
    step_n = pd.DataFrame(
        {
            'sector': ['221110', '221121'],
            'exemption_reason': ['unit_incommensurate_mixed_units', None],
        }
    )
    step_d = step_n.copy()
    drops = collect_electricity_drops(
        step_n,
        step_d,
        step_live_sectors={'221110', '221121'},
        footing_live_sectors={'221100'},
    )
    by_sector = {d.sector: d.reason for d in drops}
    assert '221110' in by_sector
    assert 'mixed units' in by_sector['221110']
    assert '221121' in by_sector
    assert 'not present in v0.2 baseline' in by_sector['221121']
    assert '221100' in by_sector
    assert 'present only in v0.2 baseline' in by_sector['221100']


def test_format_drop_footnote() -> None:
    text = format_drop_footnote(
        [
            DroppedSector('221110', 'mixed units are incompatible for plotting'),
            DroppedSector('221100', 'present only in v0.2 baseline'),
        ]
    )
    assert '221110 dropped:' in text
    assert '221100 dropped:' in text


def test_vs_footing_perc_formula_with_mocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``(step - footing) / |footing|`` on an overlapping monetary sector."""
    footing_n = pd.DataFrame(
        {
            'sector': ['1111A0', '221100'],
            'sector_name': ['Oilseeds', 'Electric power'],
            'N_new': [1.0, 3.0],
            'comparison_type': ['direct', 'direct'],
        }
    )
    footing_d = footing_n.rename(columns={'N_new': 'D_new'})
    step_n = pd.DataFrame(
        {
            'sector': ['1111A0', '221100'],
            'sector_name': ['Oilseeds', 'Electric power'],
            'N_new': [1.1, 3.3],
            'comparison_type': ['direct', 'direct'],
            'exemption_reason': [None, None],
        }
    )
    step_d = step_n.rename(columns={'N_new': 'D_new'})
    sig = pd.DataFrame({'sector': ['1111A0'], 'sector_name': ['Oilseeds']})

    def fake_load(sheet_id: str, tab: str, refresh: bool = False) -> pd.DataFrame:
        del refresh
        if sheet_id == 'foot':
            return footing_n if tab.startswith('N') else footing_d
        if tab == 'D_and_N_significant_sectors':
            return sig
        return step_n if tab.startswith('N') else step_d

    monkeypatch.setattr(mod, 'load_tab', fake_load)
    frames = mod.vs_footing_ef_frames('step', 'foot')
    row = frames.df_n.loc[frames.df_n['sector'] == '1111A0'].iloc[0]
    assert float(row['N_perc_diff']) == pytest.approx(0.1)
    assert list(frames.df_scatter.columns) == [
        'ef_new',
        'ef_old',
        'ef_change',
        'ef_pct_change',
        'sector_name',
    ]
    assert frames.df_scatter.loc['1111A0', 'ef_old'] == pytest.approx(1.0)
    assert frames.df_scatter.loc['1111A0', 'ef_change'] == pytest.approx(0.1)
