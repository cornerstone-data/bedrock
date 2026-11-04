"""Batch triage: IV-ranked commodities × dig tables at one peak year each.

Reads an ``IV_score.csv`` and a dig output dir from
:mod:`~bedrock.analysis.time_series_B_matrix.B_change_diagnostics`, and writes
one row per code with the deep-dive intake columns (E/x/B/N_d moves, L-held vs
L-effect, top stratum, path = B / L / both). Does not rebuild the span.

The ``N`` columns are on ``L_dom`` and ``N_d``, the basis of the IV score, so the
change table is computed here on the domestic span. ``B_change_real.csv`` in the
dig is on the full ``L`` and ``N``. The other dig tables don't depend on ``L``.

Defaults point at the IV pins used for the 2026-11-04 ``2122A0`` pilot
(``dig_fbs_79273ed_mut_5514c3b``). Override with ``--dig-dir`` / ``--iv-csv``.

::

    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage
    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage --top 30
    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage \\
        --above-iv-score 0.2
    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage \\
        --codes 2122A0,221100,325110
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.time_series_B_matrix import B_change_diagnostics as bcd
from bedrock.analysis.time_series_B_matrix.IV_score import (
    domestic_span,
    load_or_build_span,
)

logger = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
OUTPUT = HERE / 'output'
DEFAULT_IV = OUTPUT / 'IV_score' / 'IV_score.csv'
DEFAULT_DIG = OUTPUT / 'dig_fbs_79273ed_mut_5514c3b'
DEFAULT_CACHE = OUTPUT / 'cache_fbs_79273ed_mut_5514c3b'
OUT_DIR = OUTPUT / 'sector_deep_dive_triage'

#: |L_effect| / |L_held| above this → structure path; inverse → direct; else both.
PATH_RATIO = 2.0


def _pct(a: float, b: float) -> float:
    if not np.isfinite(a) or not np.isfinite(b) or a == 0.0:
        return float('nan')
    return 100.0 * (b / a - 1.0)


def _path(l_held: float, l_effect: float) -> str:
    ah, ae = abs(l_held), abs(l_effect)
    if ah == 0.0 and ae == 0.0:
        return 'unclear'
    if ae > PATH_RATIO * ah:
        return 'L'
    if ah > PATH_RATIO * ae:
        return 'B'
    return 'both'


def _sector_E_mt(cache_dir: Path) -> pd.DataFrame:
    """Sector × year emissions in Mt from the dig span cache."""
    path = cache_dir / 'E_stratified.parquet'
    E = pd.read_parquet(path)
    g = E.groupby(['sector', 'year'], as_index=False)['CO2e'].sum()
    g['E_Mt'] = g['CO2e'] / 1e9
    g['sector'] = g['sector'].astype(str)
    pivoted = g.pivot(index='sector', columns='year', values='E_Mt')
    pivoted.columns = pivoted.columns.map(str)
    return pivoted


def _top_stratum(div: pd.DataFrame, sector: str, year_to: int) -> pd.Series:
    sub = div[(div['sector'].astype(str) == sector) & (div['year_to'] == year_to)]
    if sub.empty:
        return pd.Series(
            {
                'top_MetaSources': '',
                'top_attribution': '',
                'top_divergence_Mt': float('nan'),
                'top_oscillation': float('nan'),
            }
        )
    row = (
        sub.assign(_a=sub['divergence'].abs())
        .sort_values('_a', ascending=False)
        .iloc[0]
    )
    return pd.Series(
        {
            'top_MetaSources': row['MetaSources'],
            'top_attribution': row['attribution'],
            'top_divergence_Mt': row['divergence'] / 1e9,
        }
    )


def triage(
    iv: pd.DataFrame,
    dig_dir: Path,
    cache_dir: Path,
    *,
    codes: list[str] | None = None,
    top: int | None = None,
    above_iv_score: float | None = None,
) -> pd.DataFrame:
    """One triage row per code at that code's IV ``peak_year``.

    *codes* takes precedence over *above_iv_score*, which takes precedence over
    *top*.
    """
    iv = iv.copy()
    iv['commodity'] = iv['commodity'].astype(str)
    if codes:
        want = [c.strip() for c in codes]
        missing = [c for c in want if c not in set(iv['commodity'])]
        if missing:
            raise ValueError(f'codes not in IV_score.csv: {missing}')
        iv = iv[iv['commodity'].isin(want)].copy()
        iv = iv.set_index('commodity').loc[want].reset_index()
    elif above_iv_score is not None:
        iv = iv[iv['IV'] > above_iv_score].sort_values('rank')
        if iv.empty:
            raise ValueError(f'no commodity has an IV score above {above_iv_score}')
    else:
        iv = iv.sort_values('rank').head(top or 15)

    full_span = load_or_build_span()
    if bcd.CACHE_DIR.resolve() != cache_dir.resolve():
        raise ValueError(f'--cache-dir {cache_dir} is not the IV span {bcd.CACHE_DIR}')
    bc = bcd.B_change(domestic_span(full_span), real=True)
    bc['commodity'] = bc['commodity'].astype(str)
    # divergence_by_sector_stratum.csv is on nominal x, so it can't be read beside
    # the real %dx here. This is the same grouping on real x.
    div = (
        pd.read_csv(dig_dir / 'divergence_detail_real.csv', dtype={'sector': str})
        .groupby(['sector', 'year_to', 'attribution', 'MetaSources'])['divergence']
        .sum()
        .reset_index()
    )
    span = pd.read_csv(dig_dir / 'sector_stratum_span.csv')
    span['sector'] = span['sector'].astype(str)
    x = pd.read_csv(dig_dir / 'x_real.csv').set_index('sector')
    x.index = x.index.astype(str)
    x.columns = x.columns.map(str)
    B = pd.read_csv(dig_dir / 'B_total_real.csv', index_col=0)
    B.index = B.index.astype(str)
    B.columns = B.columns.map(str)
    E = _sector_E_mt(cache_dir)

    rows: list[dict[str, object]] = []
    for _, ivrow in iv.iterrows():
        code = str(ivrow['commodity'])
        year_to = int(ivrow['peak_year'])
        year_from = year_to - 1
        brow = bc[(bc['commodity'] == code) & (bc['year_to'] == year_to)]
        if brow.empty:
            logger.warning('%s: no B change row for year_to=%s', code, year_to)
            continue
        b = brow.iloc[0]
        yf, yt = str(year_from), str(year_to)

        def _level(frame: pd.DataFrame, key: str) -> tuple[float, float, float]:
            if (
                key not in frame.index
                or yf not in frame.columns
                or yt not in frame.columns
            ):
                return float('nan'), float('nan'), float('nan')
            a = float(np.asarray(frame.loc[key, yf]).item())
            c = float(np.asarray(frame.loc[key, yt]).item())
            return a, c, _pct(a, c)

        E0, E1, dE = _level(E, code)
        x0, x1, dx = _level(x, code)
        B0, B1, dB_level = _level(B, code)

        stratum = _top_stratum(div, code, year_to)
        osc = float('nan')
        if stratum['top_MetaSources']:
            cell = span[
                (span['sector'] == code)
                & (span['MetaSources'] == stratum['top_MetaSources'])
                & (span['attribution'] == stratum['top_attribution'])
            ]
            if not cell.empty:
                osc = float(cell.iloc[0]['oscillation'])

        l_held = float(b['pct_change_N_L_held'])
        l_effect = float(b['pct_change_N_L_effect'])
        rows.append(
            {
                'rank': int(ivrow['rank']),
                'code': code,
                'name': ivrow.get('name', b.get('name', '')),
                'IV': float(ivrow['IV']),
                'peak_year': year_to,
                'year_from': year_from,
                'pct_dE': dE,
                'pct_dx': dx,
                'pct_dB': float(b['pct_change_B']),
                'pct_dN_d': float(b['pct_change_N']),
                'pct_dN_d_L_held': l_held,
                'pct_dN_d_L_effect': l_effect,
                'abs_delta_B_pct_of_N_d': float(b['abs_delta_B_pct_of_N']),
                'own_direct_share_of_N_d': float(b['own_direct_share_of_N']),
                'path': _path(l_held, l_effect),
                'E_from_Mt': E0,
                'E_to_Mt': E1,
                'B_from': B0,
                'B_to': B1,
                'top_MetaSources': stratum['top_MetaSources'],
                'top_attribution': stratum['top_attribution'],
                'top_divergence_Mt': stratum['top_divergence_Mt'],
                'top_oscillation': osc,
                'IV_oscillation': (
                    float(ivrow['oscillation'])
                    if 'oscillation' in ivrow.index and pd.notna(ivrow['oscillation'])
                    else float('nan')
                ),
            }
        )
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--iv-csv', type=Path, default=DEFAULT_IV)
    parser.add_argument('--dig-dir', type=Path, default=DEFAULT_DIG)
    parser.add_argument(
        '--cache-dir',
        type=Path,
        default=DEFAULT_CACHE,
        help='span cache with E_stratified.parquet (for sector E Mt)',
    )
    parser.add_argument(
        '--top',
        type=int,
        default=15,
        help='IV ranks 1..N (ignored if --codes or --above-iv-score)',
    )
    parser.add_argument(
        '--above-iv-score',
        type=float,
        default=None,
        help='only commodities with an IV score above this (ignored if --codes)',
    )
    parser.add_argument(
        '--codes',
        type=str,
        default='',
        help='comma-separated BEA codes (overrides --top)',
    )
    parser.add_argument(
        '--out',
        type=Path,
        default=None,
        help='output CSV (default: output/sector_deep_dive_triage/triage_topN.csv)',
    )
    args = parser.parse_args(argv)

    codes = [c.strip() for c in args.codes.split(',') if c.strip()] or None
    iv = pd.read_csv(args.iv_csv)
    table = triage(
        iv,
        args.dig_dir,
        args.cache_dir,
        codes=codes,
        top=None if codes else args.top,
        above_iv_score=args.above_iv_score,
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if args.out is not None:
        out = args.out
    elif codes:
        out = OUT_DIR / f'triage_{len(codes)}codes.csv'
    elif args.above_iv_score is not None:
        out = OUT_DIR / f'triage_above_iv_{args.above_iv_score}.csv'
    else:
        out = OUT_DIR / f'triage_top{args.top}.csv'
    table.to_csv(out, index=False)
    logger.info('Wrote %s (%d rows)', out, len(table))
    cols = [
        'rank',
        'code',
        'peak_year',
        'path',
        'pct_dE',
        'pct_dx',
        'pct_dB',
        'pct_dN_d',
        'pct_dN_d_L_held',
        'pct_dN_d_L_effect',
        'abs_delta_B_pct_of_N_d',
        'top_attribution',
        'top_MetaSources',
        'top_divergence_Mt',
        'top_oscillation',
    ]
    print(table[cols].round(2).to_string(index=False))
    print(f'\nWrote {out}')
    return out


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    main()
