"""Load and normalize CEDA EF diagnostics tabs for the two decks."""

from __future__ import annotations

from typing import Literal

import pandas as pd

from bedrock.analysis.electricity.current.diagnostics.ceda_electricity.sheets import (
    BLY_COL,
    D_NEW,
    D_OLD_INFL,
    D_PERC_NO_MA,
    N_A_EFFECT,
    N_D_EFFECT,
    N_NEW,
    N_OLD_INFL,
    N_PERC_NO_MA,
    N_PERC_WITH_MA,
)
from bedrock.utils.validation.analysis.fetch import load_tab

Scope = Literal['global', 'USA']


def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce')


def load_n(sheet_id: str, *, refresh: bool = False) -> pd.DataFrame:
    return load_tab(sheet_id, 'N_and_diffs', refresh=refresh)


def load_d(sheet_id: str, *, refresh: bool = False) -> pd.DataFrame:
    return load_tab(sheet_id, 'D_and_diffs', refresh=refresh)


def load_bly_cs(sheet_id: str, *, refresh: bool = False) -> pd.DataFrame:
    return load_tab(sheet_id, 'BLy_by_country_sector', refresh=refresh)


def load_config_summary(sheet_id: str, *, refresh: bool = False) -> dict[str, str]:
    df = load_tab(sheet_id, 'config_summary', refresh=refresh)
    if df.shape[1] < 2:
        return {}
    keys = df.iloc[:, 0].astype(str)
    vals = df.iloc[:, 1].astype(str)
    return dict(zip(keys, vals, strict=False))


def filter_scope(df: pd.DataFrame, scope: Scope) -> pd.DataFrame:
    if scope == 'global':
        return df
    if 'country' not in df.columns:
        raise KeyError('expected country column for USA filter')
    return df.loc[df['country'].astype(str) == 'USA'].copy()


def perc_series(
    df: pd.DataFrame,
    col: str,
    *,
    as_percent: bool = True,
) -> pd.Series:
    """Return finite percent-diff values (percent units if ``as_percent``)."""
    s = _to_float(df[col]).dropna()
    s = s[s.map(lambda x: abs(x) < 1e12)]
    if as_percent:
        # CEDA sheets store fractions; bedrock hist xlim is in percent points.
        if s.abs().median(skipna=True) <= 2.0:
            s = s * 100.0
    return s


def n_perc_no_ma(df: pd.DataFrame, *, as_percent: bool = True) -> pd.Series:
    return perc_series(df, N_PERC_NO_MA, as_percent=as_percent)


def d_perc_no_ma(df: pd.DataFrame, *, as_percent: bool = True) -> pd.Series:
    return perc_series(df, D_PERC_NO_MA, as_percent=as_percent)


def n_perc_with_ma(df: pd.DataFrame, *, as_percent: bool = True) -> pd.Series:
    return perc_series(df, N_PERC_WITH_MA, as_percent=as_percent)


def effect_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df[['country', 'sector', 'sector_name']].copy()
    out['d_effect'] = _to_float(df[N_D_EFFECT]) * 100.0
    out['a_effect'] = _to_float(df[N_A_EFFECT]) * 100.0
    out['n_perc'] = _to_float(df[N_PERC_NO_MA]) * 100.0
    return out.dropna(subset=['d_effect', 'a_effect'])


def sector_row(
    df: pd.DataFrame,
    *,
    country: str,
    sector: str,
) -> pd.Series | None:
    hit = df.loc[
        (df['country'].astype(str) == country) & (df['sector'].astype(str) == sector)
    ]
    if hit.empty:
        return None
    return hit.iloc[0]


def ef_focus_row(
    n: pd.DataFrame,
    d: pd.DataFrame,
    *,
    country: str,
    sector: str,
) -> dict[str, float | str | None]:
    nr = sector_row(n, country=country, sector=sector)
    dr = sector_row(d, country=country, sector=sector)
    name = None if nr is None else str(nr.get('sector_name', sector))
    return {
        'country': country,
        'sector': sector,
        'sector_name': name,
        'N_new': None if nr is None else float(_to_float(pd.Series([nr[N_NEW]])).iloc[0]),
        'N_old_infl': (
            None
            if nr is None
            else float(_to_float(pd.Series([nr[N_OLD_INFL]])).iloc[0])
        ),
        'N_perc': (
            None
            if nr is None
            else float(_to_float(pd.Series([nr[N_PERC_NO_MA]])).iloc[0]) * 100.0
        ),
        'N_d_effect': (
            None
            if nr is None
            else float(_to_float(pd.Series([nr[N_D_EFFECT]])).iloc[0]) * 100.0
        ),
        'N_a_effect': (
            None
            if nr is None
            else float(_to_float(pd.Series([nr[N_A_EFFECT]])).iloc[0]) * 100.0
        ),
        'D_new': None if dr is None else float(_to_float(pd.Series([dr[D_NEW]])).iloc[0]),
        'D_old_infl': (
            None
            if dr is None
            else float(_to_float(pd.Series([dr[D_OLD_INFL]])).iloc[0])
        ),
        'D_perc': (
            None
            if dr is None
            else float(_to_float(pd.Series([dr[D_PERC_NO_MA]])).iloc[0]) * 100.0
        ),
    }


def total_bly(df: pd.DataFrame, scope: Scope) -> float:
    scoped = filter_scope(df, scope)
    return float(_to_float(scoped[BLY_COL]).sum(skipna=True))
