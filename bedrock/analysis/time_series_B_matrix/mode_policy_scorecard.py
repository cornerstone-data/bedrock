"""Hybrid attribution mode-policy scorecard for facility FBS (#1040).

Compares mode assignment policies on metrics M1-M5 (see
``.cursor/plan/09_29_facility_fbs_mode_gate_1040.md`` and GitHub #1040).

Policies
--------
S0  Status quo: per-year ``modes_by_sector(facility_coverage_bands(...))``.
A   Late-band freeze: one map from ``--band-year`` (default 2022), all years.
A2  Median-coverage freeze (Wes #2 / v0.5 rec): one facility-vs-MECS side per
    sector from median coverage over the span vs 0.8; facility-side floor vs
    vector taken from the late ``--band-year`` native mode (else floor).
B   Sticky forward: from 2017; switch only when native mode matches prior
    native year and differs from sticky mode (2-year confirmation).
C   Sticky from late: same rule walking backward from the last year.
E   Gate band (Wes #1): hold prior mode while coverage is in
    [``--gate-lo``, ``--gate-hi``] (default 0.75-0.85); else use native mode.
D   Stable NEI population (demoted): NEI-below mass only from FacilityIDs that
    also appear in the baseline NEI year (default 2019).

Share metrics (M2/M3c/M4) use a **BEA-sector** Hybrid blend (facility CO2e vs
MECS amounts by ``bea_detail_for_naics``), not the full industry_spec NAICS
rollup in production. Enough to rank policies; not bit-identical to FBS shares.

Run share replay on the #1044 twin-fix checkout when possible so mine
Coal/Petroleum weights are not GHGRP-twin inflated.

Usage::

    uv run python bedrock/analysis/time_series_B_matrix/mode_policy_scorecard.py
    uv run python bedrock/analysis/time_series_B_matrix/mode_policy_scorecard.py --modes-only
    uv run python bedrock/analysis/time_series_B_matrix/mode_policy_scorecard.py --years 2019,2020,2021,2022
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from bedrock.extract.stewifbs.facility_combustion import (
    FACILITY_SCOPE_PREFIXES,
    build_facility_combustion,
)
from bedrock.transform.ghg.facility_coverage import (
    ATTRIBUTION_MIN_COVERAGE,
    COMBUSTION_FUEL_CLASSES,
    GHGRP_THRESHOLD_KG,
    bea_detail_for_naics,
    facility_coverage_bands,
    ghgrp_combustion_floor,
    modes_by_sector,
)

logger = logging.getLogger(__name__)

OUT = Path('bedrock/analysis/time_series_B_matrix/output')
CACHE = OUT / 'mode_policy_cache'
MECS_STEM = (
    'bedrock/transform/output_data/'
    'Energy_manufacturing_national_nowcast_{year}_v0.3.0_92b7a8a.parquet'
)

YEARS_DEFAULT = tuple(range(2017, 2025))
FLOWABLES = (
    ('Coal', 'Energy'),
    ('Natural Gas', 'Energy'),
    ('Petroleum', 'Money'),
)
FACILITY_MODES = frozenset({'facility_floor', 'facility_vector'})
MECS_MODE = 'keep_prior'
GATE_LO_DEFAULT = 0.75
GATE_HI_DEFAULT = 0.85


def _nei_year_for(method_year: int) -> int:
    return 2022 if method_year > 2022 else method_year


def _bands_path(year: int, nei_year: int, *, tag: str = 'native') -> Path:
    return CACHE / f'bands_{tag}_{year}_nei{nei_year}.parquet'


def _union_path(year: int, nei_year: int) -> Path:
    return CACHE / f'union_{year}_nei{nei_year}.parquet'


def load_or_build_bands(
    year: int,
    *,
    nei_year: int | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Production coverage bands; cached under ``CACHE``."""
    nei_year = int(nei_year if nei_year is not None else _nei_year_for(year))
    path = _bands_path(year, nei_year, tag='native')
    if path.exists() and not refresh:
        return pd.read_parquet(path)
    CACHE.mkdir(parents=True, exist_ok=True)
    logger.info('Building native bands year=%s nei_year=%s', year, nei_year)
    bands = facility_coverage_bands(year, nei_year=nei_year)
    bands.to_parquet(path, index=False)
    return bands


def load_or_build_union(
    year: int,
    *,
    nei_year: int | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """Prefer-GHGRP ∪ NEI facility combustion with keep_flowables fuels."""
    nei_year = int(nei_year if nei_year is not None else _nei_year_for(year))
    path = _union_path(year, nei_year)
    if path.exists() and not refresh:
        return pd.read_parquet(path)
    CACHE.mkdir(parents=True, exist_ok=True)
    logger.info('Building facility union year=%s nei_year=%s', year, nei_year)
    union = build_facility_combustion(
        year,
        nei_year=nei_year,
        sector_prefixes=FACILITY_SCOPE_PREFIXES,
        exclude_sectors=('221100',),
        keep_flowables=('Petroleum', 'Natural Gas', 'Coal'),
    )
    union.to_parquet(path, index=False)
    return union


def load_or_build_bands_stable_nei(
    year: int,
    *,
    baseline_year: int = 2019,
    refresh: bool = False,
) -> pd.DataFrame:
    """Coverage bands with NEI-below mass limited to baseline-year FacilityIDs.

    GHGRP floor is unchanged. NEI-only combustion below the GHGRP threshold
    counts only if that FacilityID also appears in the baseline NEI union year.
    """
    nei_year = _nei_year_for(year)
    path = _bands_path(year, nei_year, tag=f'stableNEI{baseline_year}')
    if path.exists() and not refresh:
        return pd.read_parquet(path)

    CACHE.mkdir(parents=True, exist_ok=True)
    logger.info(
        'Building stable-NEI bands year=%s baseline=%s', year, baseline_year
    )
    # Full unions (no keep_flowables) matching facility_coverage_bands.
    union = build_facility_combustion(
        year,
        nei_year=nei_year,
        sector_prefixes=FACILITY_SCOPE_PREFIXES,
        exclude_sectors=('221100',),
    )
    baseline_nei_year = _nei_year_for(baseline_year)
    baseline = build_facility_combustion(
        baseline_year,
        nei_year=baseline_nei_year,
        sector_prefixes=FACILITY_SCOPE_PREFIXES,
        exclude_sectors=('221100',),
    )
    baseline_ids = set(
        baseline.loc[baseline['source'].astype(str) == 'NEI', 'FacilityID'].astype(str)
    )

    floor = ghgrp_combustion_floor(year, nei_year=nei_year)
    nei = union[
        (union['source'] == 'NEI') & union['fuel_class'].isin(COMBUSTION_FUEL_CLASSES)
    ].copy()
    nei = nei[nei['FacilityID'].astype(str).isin(baseline_ids)]
    per_facility = nei.groupby(['FacilityID', 'sector'])['CO2e'].sum().reset_index()
    big = per_facility['CO2e'] > GHGRP_THRESHOLD_KG
    out = pd.DataFrame(
        {
            'ghgrp_Mt': floor,
            'nei_below_Mt': per_facility[~big].groupby('sector')['CO2e'].sum() / 1e9,
            'nei_above_Mt': per_facility[big].groupby('sector')['CO2e'].sum() / 1e9,
        }
    ).fillna(0.0)
    out['total_Mt'] = out.sum(axis=1)
    out = out[out['total_Mt'] > 0]
    out['coverage'] = out['ghgrp_Mt'] / (out['ghgrp_Mt'] + out['nei_below_Mt']).replace(
        0.0, np.nan
    )
    out['unresolved'] = out['nei_above_Mt'] / out['total_Mt']
    testable = out['total_Mt'] >= 0.5
    out['verdict'] = np.where(
        testable & (out['coverage'] >= 0.95) & (out['unresolved'] <= 0.05),
        'vector',
        'floor',
    )
    out = out.rename_axis('sector').reset_index()
    out['year'] = year
    out['nei_year'] = nei_year
    out.to_parquet(path, index=False)
    return out


def native_modes_by_year(
    years: tuple[int, ...],
    *,
    refresh: bool = False,
) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    for y in years:
        bands = load_or_build_bands(y, refresh=refresh)
        out[y] = modes_by_sector(bands)
    return out


def coverage_by_year(
    years: tuple[int, ...],
    *,
    refresh: bool = False,
) -> dict[int, dict[str, float]]:
    """BEA sector -> coverage float, per method year (from cached bands)."""
    out: dict[int, dict[str, float]] = {}
    for y in years:
        bands = load_or_build_bands(y, refresh=refresh)
        out[y] = {
            str(row.sector): float(row.coverage)
            for row in bands.itertuples(index=False)
        }
    return out


def modes_policy_s0(native: dict[int, dict[str, str]]) -> dict[int, dict[str, str]]:
    return {y: dict(m) for y, m in native.items()}


def modes_policy_a_late_freeze(
    native: dict[int, dict[str, str]],
    *,
    band_year: int,
) -> dict[int, dict[str, str]]:
    frozen = dict(native[band_year])
    return {y: dict(frozen) for y in native}


def modes_policy_a2_median_freeze(
    native: dict[int, dict[str, str]],
    coverage: dict[int, dict[str, float]],
    *,
    band_year: int,
    min_coverage: float = ATTRIBUTION_MIN_COVERAGE,
) -> dict[int, dict[str, str]]:
    """One facility-vs-MECS side per sector from median coverage (Wes #2).

    Facility-side floor vs vector is taken from the late ``band_year`` native
    mode when that mode is facility_*; otherwise ``facility_floor``.
    """
    years = sorted(coverage)
    sectors: set[str] = set()
    for y in years:
        sectors |= set(coverage[y])
        sectors |= set(native.get(y, {}))

    frozen: dict[str, str] = {}
    late_modes = native.get(band_year, {})
    for sector in sectors:
        covs = [
            coverage[y][sector]
            for y in years
            if sector in coverage[y] and np.isfinite(coverage[y][sector])
        ]
        if not covs:
            frozen[sector] = MECS_MODE
            continue
        med = float(np.median(covs))
        if med < min_coverage:
            frozen[sector] = MECS_MODE
            continue
        late = late_modes.get(sector, 'facility_floor')
        frozen[sector] = late if late in FACILITY_MODES else 'facility_floor'
    return {y: dict(frozen) for y in native}


def modes_policy_b_sticky_forward(
    native: dict[int, dict[str, str]],
) -> dict[int, dict[str, str]]:
    years = sorted(native)
    out: dict[int, dict[str, str]] = {years[0]: dict(native[years[0]])}
    for y_prev, y in zip(years, years[1:]):
        out[y] = {}
        sectors = set(native[y]) | set(out[y_prev]) | set(native[y_prev])
        for sector in sectors:
            prev = out[y_prev].get(sector, MECS_MODE)
            nat = native[y].get(sector, MECS_MODE)
            nat_prev = native[y_prev].get(sector, MECS_MODE)
            if nat == prev:
                out[y][sector] = prev
            elif nat == nat_prev and nat != prev:
                out[y][sector] = nat
            else:
                out[y][sector] = prev
    return out


def modes_policy_c_sticky_from_late(
    native: dict[int, dict[str, str]],
) -> dict[int, dict[str, str]]:
    years = sorted(native, reverse=True)
    out: dict[int, dict[str, str]] = {years[0]: dict(native[years[0]])}
    for y_later, y in zip(years, years[1:]):
        # Walk backward: y is earlier than y_later.
        out[y] = {}
        sectors = set(native[y]) | set(out[y_later]) | set(native[y_later])
        for sector in sectors:
            later = out[y_later].get(sector, MECS_MODE)
            nat = native[y].get(sector, MECS_MODE)
            nat_later = native[y_later].get(sector, MECS_MODE)
            if nat == later:
                out[y][sector] = later
            elif nat == nat_later and nat != later:
                out[y][sector] = nat
            else:
                out[y][sector] = later
    return {y: out[y] for y in sorted(out)}


def modes_policy_e_gate_band(
    native: dict[int, dict[str, str]],
    coverage: dict[int, dict[str, float]],
    *,
    gate_lo: float = GATE_LO_DEFAULT,
    gate_hi: float = GATE_HI_DEFAULT,
) -> dict[int, dict[str, str]]:
    """Hold prior mode while coverage is inside [gate_lo, gate_hi] (Wes #1)."""
    years = sorted(native)
    out: dict[int, dict[str, str]] = {years[0]: dict(native[years[0]])}
    for y_prev, y in zip(years, years[1:]):
        out[y] = {}
        sectors = set(native[y]) | set(out[y_prev]) | set(coverage.get(y, {}))
        for sector in sectors:
            prev = out[y_prev].get(sector, MECS_MODE)
            nat = native[y].get(sector, MECS_MODE)
            cov = coverage.get(y, {}).get(sector, float('nan'))
            if np.isfinite(cov) and gate_lo <= cov <= gate_hi:
                out[y][sector] = prev
            else:
                out[y][sector] = nat
    return out


def modes_policy_d_stable_nei(
    years: tuple[int, ...],
    *,
    baseline_year: int = 2019,
    refresh: bool = False,
) -> dict[int, dict[str, str]]:
    out: dict[int, dict[str, str]] = {}
    for y in years:
        bands = load_or_build_bands_stable_nei(
            y, baseline_year=baseline_year, refresh=refresh
        )
        out[y] = modes_by_sector(bands)
    return out


def _facility_mecs_class(a: str, b: str) -> str:
    """Classify a mode transition for M1c."""
    fa, fb = a in FACILITY_MODES, b in FACILITY_MODES
    if fa != fb:
        return 'facility_mecs'
    if a != b and fa and fb:
        return 'floor_vector'
    if a != b:
        return 'other'
    return 'none'


def metric_m1(modes: dict[int, dict[str, str]]) -> dict[str, Any]:
    years = sorted(modes)
    changes = 0
    fac_mecs = 0
    floor_vec = 0
    other = 0
    switched_sectors: set[str] = set()
    for y0, y1 in zip(years, years[1:]):
        sectors = set(modes[y0]) | set(modes[y1])
        for s in sectors:
            a = modes[y0].get(s, MECS_MODE)
            b = modes[y1].get(s, MECS_MODE)
            if a == b:
                continue
            changes += 1
            switched_sectors.add(s)
            kind = _facility_mecs_class(a, b)
            if kind == 'facility_mecs':
                fac_mecs += 1
            elif kind == 'floor_vector':
                floor_vec += 1
            else:
                other += 1
    return {
        'M1a_mode_changes': changes,
        'M1b_sectors_ever_switch': len(switched_sectors),
        'M1c_facility_mecs_changes': fac_mecs,
        'M1c_floor_vector_changes': floor_vec,
        'M1c_other_changes': other,
        'switcher_sectors': sorted(switched_sectors),
    }


def metric_m5(native: dict[int, dict[str, str]]) -> pd.DataFrame:
    """Pairwise mode disagreement among late native maps."""
    candidates = [y for y in (2021, 2022, 2024) if y in native]
    rows = []
    for i, a in enumerate(candidates):
        for b in candidates[i + 1 :]:
            sectors = set(native[a]) | set(native[b])
            disagree = sum(
                1
                for s in sectors
                if native[a].get(s, MECS_MODE) != native[b].get(s, MECS_MODE)
            )
            rows.append(
                {
                    'year_a': a,
                    'year_b': b,
                    'n_sectors': len(sectors),
                    'n_disagree': disagree,
                    'disagree_rate': disagree / len(sectors) if sectors else 0.0,
                }
            )
    return pd.DataFrame(rows)


def metric_m3_modes(
    modes: dict[int, dict[str, str]],
    native: dict[int, dict[str, str]],
    *,
    late_year: int = 2024,
) -> dict[str, float]:
    if late_year not in native or late_year not in modes:
        late_year = max(set(native) & set(modes))
    nat = native[late_year]
    pol = modes[late_year]
    sectors = set(nat) | set(pol)
    agree = sum(
        1 for s in sectors if nat.get(s, MECS_MODE) == pol.get(s, MECS_MODE)
    )
    native_fac = {s for s, m in nat.items() if m in FACILITY_MODES}
    pol_fac_agree = sum(1 for s in native_fac if pol.get(s, MECS_MODE) in FACILITY_MODES)
    return {
        'M3_late_year': float(late_year),
        'M3a_agree_rate': agree / len(sectors) if sectors else 0.0,
        'M3b_native_facility_kept': (
            pol_fac_agree / len(native_fac) if native_fac else 1.0
        ),
        'M3_n_sectors': float(len(sectors)),
    }


def _mecs_by_bea(year: int, flowable: str, mecs_class: str) -> pd.Series:
    path = Path(MECS_STEM.format(year=year))
    if not path.exists():
        raise FileNotFoundError(f'MECS FBS missing: {path}')
    mecs = pd.read_parquet(path)
    use = mecs[
        (mecs['Flowable'].astype(str) == flowable)
        & (mecs['Class'].astype(str) == mecs_class)
    ].copy()
    use['sector'] = use['SectorConsumedBy'].map(bea_detail_for_naics)
    use = use.dropna(subset=['sector'])
    return use.groupby(use['sector'].astype(str))['FlowAmount'].sum()


def _facility_by_bea(union: pd.DataFrame, flowable: str) -> pd.Series:
    use = union[union['Flowable'].astype(str) == flowable]
    return use.groupby(use['sector'].astype(str))['CO2e'].sum()


def hybrid_bea_shares(
    *,
    facility: pd.Series,
    mecs: pd.Series,
    modes: dict[str, str],
) -> pd.Series:
    """One fuel's Hybrid shares on BEA detail (sums to 1)."""
    fac_s = facility.astype(float)
    mecs_s = mecs.astype(float)
    fac_share = fac_s / fac_s.sum() if float(fac_s.sum()) > 0 else fac_s * 0.0
    mecs_share = mecs_s / mecs_s.sum() if float(mecs_s.sum()) > 0 else mecs_s * 0.0
    sectors = sorted(set(fac_share.index) | set(mecs_share.index) | set(modes))
    weights: dict[str, float] = {}
    for s in sectors:
        f = float(fac_share.get(s, 0.0))
        m = float(mecs_share.get(s, 0.0))
        mode = modes.get(s, MECS_MODE)
        if mode == MECS_MODE:
            w = m
        elif mode == 'facility_vector':
            w = f
        else:
            w = max(f, m)
        if w > 0:
            weights[s] = w
    ser = pd.Series(weights, dtype=float)
    total = float(ser.sum())
    if total <= 0:
        return ser
    return ser / total


def share_churn_pp(shares_by_year: dict[int, pd.Series]) -> float:
    """Sum over sectors of |Δ share| across consecutive years, in percentage points."""
    years = sorted(shares_by_year)
    total = 0.0
    for y0, y1 in zip(years, years[1:]):
        a = shares_by_year[y0]
        b = shares_by_year[y1]
        idx = sorted(set(a.index) | set(b.index))
        total += float(
            sum(abs(float(a.get(s, 0.0)) - float(b.get(s, 0.0))) for s in idx)
        )
    return total * 100.0


def share_l1(a: pd.Series, b: pd.Series) -> float:
    idx = sorted(set(a.index) | set(b.index))
    return float(sum(abs(float(a.get(s, 0.0)) - float(b.get(s, 0.0))) for s in idx))


def build_hybrid_shares_panel(
    years: tuple[int, ...],
    modes: dict[int, dict[str, str]],
    *,
    refresh: bool = False,
) -> dict[str, dict[int, pd.Series]]:
    """flowable -> year -> BEA share series."""
    panel: dict[str, dict[int, pd.Series]] = {f: {} for f, _ in FLOWABLES}
    for y in years:
        union = load_or_build_union(y, refresh=refresh)
        for flowable, mecs_class in FLOWABLES:
            fac = _facility_by_bea(union, flowable)
            mecs = _mecs_by_bea(y, flowable, mecs_class)
            panel[flowable][y] = hybrid_bea_shares(
                facility=fac, mecs=mecs, modes=modes[y]
            )
    return panel


def metric_m2(
    panel: dict[str, dict[int, pd.Series]],
    *,
    switchers: set[str],
    s0_m2a: float | None = None,
) -> dict[str, float]:
    # Pool fuels: average churn across fuels (comparable magnitude).
    churns = []
    switcher_churns = []
    for flowable, by_year in panel.items():
        churns.append(share_churn_pp(by_year))
        # Restrict series to switcher sectors only, renormalize each year.
        restricted: dict[int, pd.Series] = {}
        for y, shares in by_year.items():
            sub = shares.reindex(sorted(switchers)).fillna(0.0)
            total = float(sub.sum())
            restricted[y] = sub / total if total > 0 else sub
        switcher_churns.append(share_churn_pp(restricted))
    m2a = float(np.mean(churns)) if churns else 0.0
    m2b = float(np.mean(switcher_churns)) if switcher_churns else 0.0
    out = {
        'M2a_hybrid_churn_pp_mean_fuel': m2a,
        'M2b_switcher_churn_pp_mean_fuel': m2b,
    }
    if s0_m2a is not None and s0_m2a > 0:
        out['M2c_ratio_to_S0_M2a'] = m2a / s0_m2a
    return out


def metric_m3_shares(
    panel: dict[str, dict[int, pd.Series]],
    s0_panel: dict[str, dict[int, pd.Series]],
    *,
    late_year: int = 2024,
) -> dict[str, float]:
    dists = []
    for flowable in panel:
        if late_year not in panel[flowable] or late_year not in s0_panel[flowable]:
            continue
        dists.append(share_l1(panel[flowable][late_year], s0_panel[flowable][late_year]))
    return {
        'M3c_share_L1_vs_S0_late': float(np.mean(dists)) if dists else float('nan'),
    }


def metric_m4(
    modes: dict[int, dict[str, str]],
    native: dict[int, dict[str, str]],
    panel: dict[str, dict[int, pd.Series]],
    s0_panel: dict[str, dict[int, pd.Series]],
    *,
    early_years: tuple[int, ...] = (2017, 2018, 2019),
) -> dict[str, float]:
    early = [y for y in early_years if y in modes and y in native]
    too_facility = 0
    too_mecs = 0
    # Weight proxy: mean facility share mass on sectors wrongly in facility mode.
    fac_weight = 0.0
    n_weight = 0
    for y in early:
        for s in set(modes[y]) | set(native[y]):
            pol = modes[y].get(s, MECS_MODE)
            nat = native[y].get(s, MECS_MODE)
            if nat == MECS_MODE and pol in FACILITY_MODES:
                too_facility += 1
            if nat in FACILITY_MODES and pol == MECS_MODE:
                too_mecs += 1
        for flowable in panel:
            if y not in panel[flowable] or y not in s0_panel[flowable]:
                continue
            # sectors in too_facility for this year
            bad = [
                s
                for s in panel[flowable][y].index
                if native[y].get(s, MECS_MODE) == MECS_MODE
                and modes[y].get(s, MECS_MODE) in FACILITY_MODES
            ]
            fac_weight += float(panel[flowable][y].reindex(bad).fillna(0.0).sum())
            n_weight += 1
    dists = []
    for y in early:
        for flowable in panel:
            if y in panel[flowable] and y in s0_panel[flowable]:
                dists.append(share_l1(panel[flowable][y], s0_panel[flowable][y]))
    return {
        'M4a_early_keepPrior_to_facility_sector_years': float(too_facility),
        'M4b_early_facility_to_keepPrior_sector_years': float(too_mecs),
        'M4a_mean_share_mass_on_too_facility': (
            fac_weight / n_weight if n_weight else 0.0
        ),
        'M4c_mean_early_share_L1_vs_S0': float(np.mean(dists)) if dists else float('nan'),
    }


def modes_long(modes: dict[int, dict[str, str]], policy: str) -> pd.DataFrame:
    rows = []
    for y, mp in modes.items():
        for sector, mode in mp.items():
            rows.append({'policy': policy, 'year': y, 'sector': sector, 'mode': mode})
    return pd.DataFrame(rows)


def run_scorecard(
    years: tuple[int, ...],
    *,
    band_year: int = 2022,
    baseline_nei_year: int = 2019,
    gate_lo: float = GATE_LO_DEFAULT,
    gate_hi: float = GATE_HI_DEFAULT,
    modes_only: bool = False,
    refresh: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    OUT.mkdir(parents=True, exist_ok=True)
    CACHE.mkdir(parents=True, exist_ok=True)

    logger.info('Building native modes for %s', years)
    native = native_modes_by_year(years, refresh=refresh)
    if band_year not in native:
        raise ValueError(f'band_year {band_year} not in years {years}')
    coverage = coverage_by_year(years, refresh=False)

    policies: dict[str, dict[int, dict[str, str]]] = {
        'S0': modes_policy_s0(native),
        'A_late_freeze': modes_policy_a_late_freeze(native, band_year=band_year),
        'A2_median_freeze': modes_policy_a2_median_freeze(
            native, coverage, band_year=band_year
        ),
        'B_sticky_forward': modes_policy_b_sticky_forward(native),
        'C_sticky_from_late': modes_policy_c_sticky_from_late(native),
        'E_gate_band': modes_policy_e_gate_band(
            native, coverage, gate_lo=gate_lo, gate_hi=gate_hi
        ),
        'D_stable_nei': modes_policy_d_stable_nei(
            years, baseline_year=baseline_nei_year, refresh=refresh
        ),
    }

    m5 = metric_m5(native)
    m5.to_csv(OUT / 'mode_policy_m5_anchor.csv', index=False)

    s0_m1 = metric_m1(policies['S0'])
    switchers = set(s0_m1['switcher_sectors'])

    score_rows: list[dict[str, Any]] = []
    mode_frames = []

    s0_panel: dict[str, dict[int, pd.Series]] | None = None
    if not modes_only:
        logger.info('Building S0 Hybrid BEA share panel')
        s0_panel = build_hybrid_shares_panel(years, policies['S0'], refresh=refresh)

    for name, modes in policies.items():
        logger.info('Scoring policy %s', name)
        mode_frames.append(modes_long(modes, name))
        m1 = metric_m1(modes)
        row: dict[str, Any] = {
            'policy': name,
            'band_year': band_year,
            'gate_lo': gate_lo,
            'gate_hi': gate_hi,
            **{k: v for k, v in m1.items() if k != 'switcher_sectors'},
            **metric_m3_modes(modes, native, late_year=max(years)),
        }
        if not modes_only:
            assert s0_panel is not None
            panel = (
                s0_panel
                if name == 'S0'
                else build_hybrid_shares_panel(years, modes, refresh=False)
            )
            row.update(
                metric_m2(
                    panel,
                    switchers=switchers,
                    s0_m2a=(
                        None
                        if name == 'S0'
                        else score_rows[0].get('M2a_hybrid_churn_pp_mean_fuel')
                    ),
                )
            )
            row.update(metric_m3_shares(panel, s0_panel, late_year=max(years)))
            row.update(metric_m4(modes, native, panel, s0_panel))
        score_rows.append(row)

    if not modes_only and score_rows:
        s0_m2a = float(score_rows[0]['M2a_hybrid_churn_pp_mean_fuel'])
        score_rows[0]['M2c_ratio_to_S0_M2a'] = 1.0
        for row in score_rows[1:]:
            if s0_m2a > 0 and 'M2a_hybrid_churn_pp_mean_fuel' in row:
                row['M2c_ratio_to_S0_M2a'] = (
                    float(row['M2a_hybrid_churn_pp_mean_fuel']) / s0_m2a
                )

    score = pd.DataFrame(score_rows)
    modes_df = pd.concat(mode_frames, ignore_index=True)
    score.to_csv(OUT / 'mode_policy_scorecard.csv', index=False)
    modes_df.to_parquet(OUT / 'mode_policy_by_sector_year.parquet', index=False)
    (OUT / 'mode_policy_scorecard.json').write_text(
        score.drop(
            columns=[c for c in score.columns if c.startswith('switcher')],
            errors='ignore',
        ).to_json(orient='records', indent=2),
        encoding='utf-8',
    )
    meta = {
        'years': list(years),
        'band_year': band_year,
        'baseline_nei_year': baseline_nei_year,
        'gate_lo': gate_lo,
        'gate_hi': gate_hi,
        'modes_only': modes_only,
        'n_s0_switchers': len(switchers),
        's0_switchers_head': sorted(switchers)[:30],
        'policies': list(policies),
    }
    (OUT / 'mode_policy_run_meta.json').write_text(
        json.dumps(meta, indent=2), encoding='utf-8'
    )
    logger.info('Wrote %s', OUT / 'mode_policy_scorecard.csv')
    return score, modes_df, m5


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--years',
        default='2017-2024',
        help='Inclusive range YEAR-YEAR or comma list (default 2017-2024)',
    )
    parser.add_argument('--band-year', type=int, default=2022)
    parser.add_argument('--baseline-nei-year', type=int, default=2019)
    parser.add_argument('--gate-lo', type=float, default=GATE_LO_DEFAULT)
    parser.add_argument('--gate-hi', type=float, default=GATE_HI_DEFAULT)
    parser.add_argument(
        '--modes-only',
        action='store_true',
        help='Skip Hybrid share replay (M2/M3c/M4); only M1/M3ab/M5',
    )
    parser.add_argument(
        '--refresh',
        action='store_true',
        help='Rebuild cached bands/unions even if present',
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    years_arg = str(args.years)
    if '-' in years_arg and ',' not in years_arg:
        a, b = years_arg.split('-', 1)
        years = tuple(range(int(a), int(b) + 1))
    else:
        years = tuple(int(x) for x in years_arg.split(','))

    score, _modes, m5 = run_scorecard(
        years,
        band_year=args.band_year,
        baseline_nei_year=args.baseline_nei_year,
        gate_lo=args.gate_lo,
        gate_hi=args.gate_hi,
        modes_only=args.modes_only,
        refresh=args.refresh,
    )
    print('\n=== scorecard ===')
    print(score.to_string(index=False))
    print('\n=== M5 anchor disagreement ===')
    print(m5.to_string(index=False))


if __name__ == '__main__':
    main()
