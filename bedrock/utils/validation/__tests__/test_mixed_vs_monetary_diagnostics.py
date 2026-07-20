"""Unit tests for mixed vs monetary EF comparison helpers (Track C)."""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.utils.validation.diagnostics_helpers import (
    MIXED_VS_MONETARY_TAB_COLUMNS,
    build_mixed_vs_monetary_comparison_df,
    sectors_for_mixed_vs_monetary_tab,
)


def test_sectors_for_mixed_vs_monetary_tab_includes_electricity_and_spillover() -> None:
    idx = ['221110', '221121', '221122', '1111A0', '1111B0', '221100']
    n_mix = pd.Series([1.0, 2.0, 3.0, 10.0, 1.0, 0.5], index=idx)
    n_mon = pd.Series([0.5, 2.0, 3.0, 1.0, 1.0, 0.5], index=idx)
    sectors = sectors_for_mixed_vs_monetary_tab(n_mix, n_mon)
    assert '221110' in sectors
    assert '221121' in sectors
    assert '221122' in sectors
    assert '1111A0' in sectors
    assert sectors.count('221110') == 1


def test_mixed_vs_monetary_tab_columns() -> None:
    sectors = ['221110', '1111A0']
    d_mon = pd.Series({'221110': 100.0, '1111A0': 1.0})
    n_mon = pd.Series({'221110': 200.0, '1111A0': 2.0})
    d_mix = pd.Series({'221110': 50.0, '1111A0': 1.0})
    n_mix = pd.Series({'221110': 80.0, '1111A0': 2.0})
    n_uniform = pd.Series({'221110': 75.0, '1111A0': 2.0})
    c_col = 0.5
    df = build_mixed_vs_monetary_comparison_df(
        sectors=sectors,
        D_mon=d_mon,
        N_mon=n_mon,
        D_mix=d_mix,
        N_mix=n_mix,
        N_uniform=n_uniform,
        c_col=c_col,
        sector_desc_lookup={'221110': 'Generation'},
    )
    assert list(df.columns) == list(MIXED_VS_MONETARY_TAB_COLUMNS)
    row = df.loc[df['index'] == '221110'].iloc[0]
    assert row['D_mon_over_c_col'] == pytest.approx(200.0)
    assert row['D_mix_minus_D_mon_over_c_col'] == pytest.approx(-150.0)
    assert row['sector_desc'] == 'Generation'
