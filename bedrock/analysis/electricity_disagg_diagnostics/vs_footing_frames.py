"""Build N/D percent and abs-change frames vs Cornerstone v0.2 footing."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bedrock.utils.validation.analysis.diagnostics_plots import (
    _drop_old_only,
    _normalize_schema,
)
from bedrock.utils.validation.analysis.fetch import load_tab

ELECTRICITY_SECTORS = frozenset({'221100', '221110', '221121', '221122'})

_EXEMPTION_REASON_TEXT = {
    'unit_incommensurate_mixed_units': (
        'mixed units are incompatible for plotting (kg/MWh vs kg/USD)'
    ),
    'baseline_monetary_vs_live_mixed': (
        'mixed units are incompatible for plotting (kg/MWh vs kg/USD)'
    ),
}


@dataclass(frozen=True)
class DroppedSector:
    sector: str
    reason: str


@dataclass(frozen=True)
class VsFootingFrames:
    df_n: pd.DataFrame
    df_d: pd.DataFrame
    df_sig: pd.DataFrame
    df_scatter: pd.DataFrame
    drops: list[DroppedSector]


def _sector_str(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()


def _load_ef_tab(sheet_id: str, tab: str) -> pd.DataFrame:
    df = _drop_old_only(_normalize_schema(load_tab(sheet_id, tab, refresh=False)))
    out = df.copy()
    out['sector'] = _sector_str(out['sector'])
    return out


def _load_raw_ef_tab(sheet_id: str, tab: str) -> pd.DataFrame:
    """Load without dropping old-only (needed for exemption / presence checks)."""
    df = _normalize_schema(load_tab(sheet_id, tab, refresh=False)).copy()
    df['sector'] = _sector_str(df['sector'])
    return df


def humanize_exemption_reason(raw: object) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ''
    key = str(raw).strip()
    if not key or key.lower() == 'nan':
        return ''
    return _EXEMPTION_REASON_TEXT.get(key, key.replace('_', ' '))


def _exemption_by_sector(step_n: pd.DataFrame, step_d: pd.DataFrame) -> dict[str, str]:
    out: dict[str, str] = {}
    for df in (step_n, step_d):
        if 'exemption_reason' not in df.columns:
            continue
        for _, row in df.iterrows():
            sector = str(row['sector'])
            reason = humanize_exemption_reason(row.get('exemption_reason'))
            if reason and sector not in out:
                out[sector] = reason
    return out


def collect_electricity_drops(
    step_n_raw: pd.DataFrame,
    step_d_raw: pd.DataFrame,
    step_live_sectors: set[str],
    footing_live_sectors: set[str],
) -> list[DroppedSector]:
    """Ordered drop rules for ``ELECTRICITY_SECTORS`` only."""
    exemptions = _exemption_by_sector(step_n_raw, step_d_raw)
    drops: list[DroppedSector] = []
    seen: set[str] = set()

    candidates = (
        set(exemptions)
        | (step_live_sectors & ELECTRICITY_SECTORS)
        | (footing_live_sectors & ELECTRICITY_SECTORS)
        | (
            (
                set(_sector_str(step_n_raw['sector']))
                | set(_sector_str(step_d_raw['sector']))
            )
            & ELECTRICITY_SECTORS
        )
    )

    for sector in sorted(candidates & ELECTRICITY_SECTORS):
        if sector in seen:
            continue
        if sector in exemptions:
            drops.append(DroppedSector(sector, exemptions[sector]))
            seen.add(sector)
            continue
        if sector in step_live_sectors and sector not in footing_live_sectors:
            drops.append(
                DroppedSector(
                    sector,
                    'not present in v0.2 baseline '
                    '(cannot compare on a shared sector code)',
                )
            )
            seen.add(sector)
            continue
        if sector in footing_live_sectors and sector not in step_live_sectors:
            drops.append(
                DroppedSector(
                    sector,
                    'present only in v0.2 baseline '
                    '(aggregate electricity; not in this step)',
                )
            )
            seen.add(sector)
    return drops


def _perc_diff(step: pd.Series, base: pd.Series) -> pd.Series:
    denom = base.abs()
    out = (step - base) / denom
    out = out.where(denom != 0)
    return out.replace([float('inf'), float('-inf')], float('nan'))


def _merge_kind(
    step: pd.DataFrame,
    footing: pd.DataFrame,
    kind: str,
    drop_sectors: set[str],
) -> pd.DataFrame:
    col = f'{kind}_new'
    s = step[['sector', 'sector_name', col]].copy()
    s[col] = pd.to_numeric(s[col], errors='coerce')
    b = footing[['sector', col]].rename(columns={col: '_base'})
    b['_base'] = pd.to_numeric(b['_base'], errors='coerce')
    m = s.merge(b, on='sector', how='inner')
    m = m[~m['sector'].isin(drop_sectors)]
    m[f'{kind}_perc_diff'] = _perc_diff(m[col], m['_base'])
    return m[['sector', 'sector_name', f'{kind}_perc_diff', col, '_base']].reset_index(
        drop=True
    )


def vs_footing_ef_frames(
    step_sheet_id: str,
    footing_sheet_id: str,
) -> VsFootingFrames:
    """Recompute N/D % and scatter abs Δ vs footing ``*_new`` (not sheet CEDA)."""
    step_n_raw = _load_raw_ef_tab(step_sheet_id, 'N_and_diffs')
    step_d_raw = _load_raw_ef_tab(step_sheet_id, 'D_and_diffs')
    step_n = _drop_old_only(step_n_raw)
    step_d = _drop_old_only(step_d_raw)
    foot_n = _load_ef_tab(footing_sheet_id, 'N_and_diffs')
    foot_d = _load_ef_tab(footing_sheet_id, 'D_and_diffs')

    step_live = set(step_n['sector']) | set(step_d['sector'])
    foot_live = set(foot_n['sector']) | set(foot_d['sector'])
    drops = collect_electricity_drops(step_n_raw, step_d_raw, step_live, foot_live)
    drop_sectors = {d.sector for d in drops}

    n_merged = _merge_kind(step_n, foot_n, 'N', drop_sectors)
    d_merged = _merge_kind(step_d, foot_d, 'D', drop_sectors)

    df_n = n_merged[['sector', 'sector_name', 'N_perc_diff']].copy()
    df_d = d_merged[['sector', 'sector_name', 'D_perc_diff']].copy()

    df_scatter = pd.DataFrame(
        {
            'ef_new': n_merged['N_new'].to_numpy(),
            'ef_old': n_merged['_base'].to_numpy(),
            'ef_change': (n_merged['N_new'] - n_merged['_base']).to_numpy(),
            'ef_pct_change': n_merged['N_perc_diff'].to_numpy(),
            'sector_name': n_merged['sector_name'].astype(str).to_numpy(),
        },
        index=pd.Index(n_merged['sector'].astype(str), name='sector'),
    )

    sig_raw = _normalize_schema(
        load_tab(step_sheet_id, 'D_and_N_significant_sectors', refresh=False)
    ).copy()
    sig_raw['sector'] = _sector_str(sig_raw['sector'])
    sig_sectors = set(sig_raw['sector']) & set(df_n['sector'])
    df_sig = (
        df_n[df_n['sector'].isin(sig_sectors)][['sector', 'sector_name', 'N_perc_diff']]
        .merge(
            df_d[df_d['sector'].isin(sig_sectors)][['sector', 'D_perc_diff']],
            on='sector',
            how='inner',
        )
        .reset_index(drop=True)
    )

    return VsFootingFrames(
        df_n=df_n,
        df_d=df_d,
        df_sig=df_sig,
        df_scatter=df_scatter,
        drops=drops,
    )


def format_drop_footnote(drops: list[DroppedSector]) -> str:
    if not drops:
        return ''
    return '\n'.join(f'{d.sector} dropped: {d.reason}' for d in drops)
