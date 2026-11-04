"""Sector triage: what moved over 2017-2024 for each commodity the IV score flags.

Answers the four questions in ``About_sector_deep_dive.md`` and writes one row per
commodity, plus a draft entry for the tracker and for
``About_sector_trend_explanations.md``:

1. Which sectors? The IV score, from an ``IV_score.csv``.
2. Did emissions change? ``E`` in 2017 and 2024, and its largest sources.
3. Did output change? ``q`` in 2017 and 2024, in constant 2017 dollars.
4. Did the supply chain change? The driving factor, and the largest ``A`` cells.

The driving factor splits each annual change in ``N_d`` into the part from ``B``,
with ``L_dom`` held at the prior year, and the part from ``L_dom``. Absolute values
are summed over the years so that moves in opposite directions don't cancel.

An ``A`` cell's contribution to the ``L_dom`` part uses
``L_t - L_t-1 = L_t (A_t - A_t-1) L_t-1``, so for target ``k`` the cell ``(i, j)``
contributes ``N_d_i,t * dA_ij * L_jk,t-1``. These add up to the ``L_dom`` part
exactly, and ``--check`` asserts that. All of this is in constant 2017 dollars.

Runs from the span cache that ``IV_score`` builds.

::

    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage
    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage \\
        --above-iv-score 0.2
    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage \\
        --codes 2122A0,221100,325110
    uv run python -m bedrock.analysis.time_series_B_matrix.sector_deep_dive_triage \\
        --check
"""

from __future__ import annotations

import argparse
import logging
import typing as ta
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.time_series_B_matrix import B_change_diagnostics as bcd
from bedrock.analysis.time_series_B_matrix.IV_score import (
    domestic_span,
    load_or_build_span,
)
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRY_DESC

logger = logging.getLogger(__name__)

HERE = Path(__file__).resolve().parent
OUTPUT = HERE / 'output'
DEFAULT_IV = OUTPUT / 'IV_score' / 'IV_score.csv'
OUT_DIR = OUTPUT / 'sector_deep_dive_triage'

FIRST_YEAR = 2017
LAST_YEAR = 2024
N_SOURCES = 3
N_CELLS = 3
NOT_ASSESSED = 'Not yet assessed.'


@dataclass(frozen=True)
class Basis:
    """Everything the triage reads, in constant 2017 dollars on the domestic basis."""

    E: pd.DataFrame
    q: pd.DataFrame
    changes: pd.DataFrame
    N: pd.DataFrame
    L: dict[int, pd.DataFrame]
    A: dict[int, np.ndarray]


def load_basis() -> Basis:
    span = domestic_span(load_or_build_span())
    L = bcd.L_real(span)
    identity = np.eye(len(L[FIRST_YEAR]))
    E = span.E.copy()
    E['sector'] = E['sector'].astype(str)
    changes = bcd.B_change(span, real=True)
    changes['commodity'] = changes['commodity'].astype(str)
    return Basis(
        E=E,
        q=span.q * bcd.commodity_rho(span).reindex(span.q.index)[span.q.columns],
        changes=changes,
        N=bcd.N_total(span, real=True),
        L=L,
        A={year: identity - np.linalg.inv(m.to_numpy()) for year, m in L.items()},
    )


def select(
    iv: pd.DataFrame,
    codes: list[str] | None,
    above_iv_score: float | None,
    top: int,
) -> pd.DataFrame:
    """*codes* takes precedence over *above_iv_score*, and that over *top*."""
    if codes:
        missing = sorted(set(codes) - set(iv['commodity']))
        if missing:
            raise ValueError(f'codes not in the IV table: {missing}')
        return iv.set_index('commodity').loc[codes].reset_index()
    ranked = iv.sort_values('rank')
    if above_iv_score is None:
        return ranked.head(top)
    chosen = ranked[ranked['IV'] > above_iv_score]
    if chosen.empty:
        raise ValueError(f'no commodity has an IV score above {above_iv_score}')
    return chosen


def name_of(code: str) -> str:
    return ta.cast(ta.Mapping[str, str], INDUSTRY_DESC).get(code, code).strip()


def cell(frame: pd.DataFrame, row: str, column: int) -> float:
    return float(np.asarray(frame.loc[row, column]).item())


def pct_change(first: float, last: float) -> float:
    return 100 * (last / first - 1) if first else float('nan')


def e_sources(basis: Basis, code: str) -> str:
    """The largest sources of ``E`` over the span, with their shares."""
    rows = basis.E[basis.E['sector'] == code]
    by_source = rows.groupby(['MetaSources', 'attribution'])['CO2e'].sum()
    shares = (by_source / by_source.sum() * 100).nlargest(N_SOURCES).round()
    return '; '.join(
        f'`{source}` via `{route}` ({share:.0f}%)'
        for (source, route), share in shares.items()
        if share > 0
    )


def a_cell_moves(basis: Basis, code: str) -> tuple[pd.DataFrame, pd.Series]:
    """Each ``A`` cell's gross contribution to ``code``'s ``N_d``, percent of ``N_d``.

    Also returns the contributions summed by year, which equal the ``L_dom`` part.
    """
    index = basis.L[FIRST_YEAR].index
    k = index.get_loc(code)
    gross = np.zeros((len(index), len(index)))
    by_year = {}
    years = sorted(basis.L)
    for prior, current in zip(years, years[1:]):
        N_current = basis.N[current].reindex(index).fillna(0.0).to_numpy(dtype=float)
        L_prior = basis.L[prior].to_numpy()[:, k]
        change = np.outer(N_current, L_prior) * (basis.A[current] - basis.A[prior])
        change = change / cell(basis.N, code, prior) * 100
        gross += np.abs(change)
        by_year[current] = float(change.sum())
    return pd.DataFrame(gross, index=index, columns=index), pd.Series(by_year)


def top_a_cells(basis: Basis, code: str) -> str:
    gross, _ = a_cell_moves(basis, code)
    values = gross.to_numpy()
    largest = np.argsort(values, axis=None)[::-1][:N_CELLS]
    sellers, buyers = np.unravel_index(largest, values.shape)
    return '; '.join(
        f'{name_of(str(gross.index[i]))} bought by '
        f'{name_of(str(gross.columns[j]))} ({values[i, j]:.1f})'
        for i, j in zip(sellers, buyers)
    )


def triage(iv: pd.DataFrame, basis: Basis) -> pd.DataFrame:
    """One row per commodity in *iv*, in its order."""
    sector_E = basis.E.groupby(['sector', 'year'])['CO2e'].sum().unstack() / 1e9
    rows = []
    for _, row in iv.iterrows():
        code = str(row['commodity'])
        E_first = cell(sector_E, code, FIRST_YEAR)
        E_last = cell(sector_E, code, LAST_YEAR)
        q_first = cell(basis.q, code, FIRST_YEAR) / 1e9
        q_last = cell(basis.q, code, LAST_YEAR) / 1e9
        moves = basis.changes[basis.changes['commodity'] == code]
        from_B = float(moves['pct_change_N_L_held'].abs().sum())
        from_L = float(moves['pct_change_N_L_effect'].abs().sum())
        B_share = 100 * from_B / (from_B + from_L)
        rows.append(
            {
                'rank': int(row['rank']),
                'code': code,
                'name': name_of(code),
                'IV': float(row['IV']),
                'E_2017_Mt': E_first,
                'E_2024_Mt': E_last,
                'pct_E': pct_change(E_first, E_last),
                'E_sources': e_sources(basis, code),
                'q_2017_bn': q_first,
                'q_2024_bn': q_last,
                'pct_q': pct_change(q_first, q_last),
                'driving_factor': 'B' if B_share >= 50 else 'L',
                'B_share_pct': B_share,
                'L_share_pct': 100 - B_share,
                'top_A_cells': top_a_cells(basis, code),
                'peak_year': int(row['peak_year']),
            }
        )
    return pd.DataFrame(rows)


def draft(table: pd.DataFrame) -> str:
    """Draft tracker rows and log entries, with the explanations left to write."""
    lines = [
        '## Tracker rows',
        '',
        '| Code | Sector | IV score | Driving factor | Status | Issues and PRs |',
        '|---|---|---|---|---|---|',
    ]
    for _, row in table.iterrows():
        lines.append(
            f"| `{row['code']}` | {row['name']} | {row['IV']:.2f} | "
            f"B {row['B_share_pct']:.0f}%, L {row['L_share_pct']:.0f}% | "
            'unresolved | none yet |'
        )
    lines += ['', '## Log entries', '']
    for _, row in table.iterrows():
        lines += [
            f"### {row['code']} {row['name']}",
            '',
            '| Field | Value |',
            '|---|---|',
            f"| **E** | {row['E_2017_Mt']:,.1f} → {row['E_2024_Mt']:,.1f} Mt CO2e "
            f"({row['pct_E']:+.0f}%) |",
            f"| **E source** | {row['E_sources']} |",
            f'| **E explanation** | {NOT_ASSESSED} |',
            f"| **q** | ${row['q_2017_bn']:,.1f}bn → ${row['q_2024_bn']:,.1f}bn "
            f"({row['pct_q']:+.0f}%) |",
            f'| **q explanation** | {NOT_ASSESSED} |',
            f"| **A explanation** | {NOT_ASSESSED} Largest cells, gross percent of "
            f"`N_d`: {row['top_A_cells']} |",
            f'| **L explanation** | {NOT_ASSESSED} |',
            '',
        ]
    return '\n'.join(lines)


def check(iv: pd.DataFrame, basis: Basis) -> None:
    """Assert that the ``A`` cells add up to the ``L_dom`` part for every year."""
    worst = 0.0
    for code in iv['commodity'].astype(str):
        _, by_year = a_cell_moves(basis, code)
        moves = basis.changes[basis.changes['commodity'] == code]
        expected = moves.set_index('year_to')['pct_change_N_L_effect']
        worst = max(worst, float((by_year - expected).abs().max()))
    assert worst < 1e-6, f'A cells miss the L effect by {worst:.1e}'
    logger.info(
        'OK  A cells add up to the L effect for %d commodities (worst %.1e)',
        len(iv),
        worst,
    )


def main(argv: list[str] | None = None) -> Path | None:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--iv-csv', type=Path, default=DEFAULT_IV)
    parser.add_argument('--top', type=int, default=15, help='IV ranks 1 to N')
    parser.add_argument(
        '--above-iv-score',
        type=float,
        help='only commodities with an IV score above this; overrides --top',
    )
    parser.add_argument(
        '--codes', default='', help='comma-separated codes; overrides the other two'
    )
    parser.add_argument(
        '--out', type=Path, help='output CSV; the draft is written beside it'
    )
    parser.add_argument('--check', action='store_true', help='assert and exit')
    args = parser.parse_args(argv)

    iv = pd.read_csv(args.iv_csv, dtype={'commodity': str})
    codes = [c.strip() for c in args.codes.split(',') if c.strip()] or None
    chosen = select(iv, codes, args.above_iv_score, args.top)
    basis = load_basis()
    if args.check:
        check(chosen, basis)
        return None

    table = triage(chosen, basis)
    if args.out is not None:
        out = args.out
    elif codes:
        out = OUT_DIR / f'triage_{len(codes)}codes.csv'
    elif args.above_iv_score is not None:
        out = OUT_DIR / f'triage_above_iv_{args.above_iv_score}.csv'
    else:
        out = OUT_DIR / f'triage_top{args.top}.csv'
    out.parent.mkdir(parents=True, exist_ok=True)
    table.to_csv(out, index=False)
    draft_path = out.with_name(f'{out.stem}_draft.md')
    draft_path.write_text(draft(table), encoding='utf-8')

    columns = ['rank', 'code', 'IV', 'pct_E', 'pct_q', 'driving_factor', 'B_share_pct']
    print(table[columns].round(2).to_string(index=False))
    print(f'\nWrote {out}\nWrote {draft_path}')
    return out


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    main()
