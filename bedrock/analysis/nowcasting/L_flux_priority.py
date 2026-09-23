"""Which commodities' ``N`` is most moved by ``L``, and which ``A`` cells do it.

#937 established that ``L`` moves ``N`` 1.1x to 3.0x as much as the emission
factors do, and that the B smoothing project has no lever on it. That is a
bound, not a work list. This module turns it into one: **rank commodities by
how much ``L`` destabilises their ``N``, then name the ``A`` cells responsible**
so each can be taken up and settled.

``pct_change_N_L_effect`` in D6b is already the right measure — the part of a
commodity's year-on-year ``N`` move that survives holding ``L`` at the prior
year — but it has only ever been reported as a median over commodities. Nobody
has ranked the commodities themselves.

Two screens, because they answer different questions:

- **gross** ``sum_years |L effect|`` — how much ``L`` moved this commodity's
  ``N`` in total. The size of the problem.
- **oscillation** ``1 - |net| / gross`` — whether it moved and arrived back
  where it started. Near 1 is the signature of churn with nothing underneath
  it; near 0 is a trend and is usually real. Same screen the B driver tracker
  uses, and the same caveat: it is where to start an investigation, not proof.

⚠️ **Ranked on the output-weighted gross.** A commodity whose ``N`` swings 40%
but which nobody buys does not destabilise the model; one that swings 4% and
is bought by everything does. The unweighted column is carried alongside, and
they disagree — read both.

⚠️ **Everything is on the real basis** (#957, #958). On the nominal one
``L``'s movement is partly relative prices, and deflating turns out to make it
*larger* rather than smaller, so this ranking could not have been produced
before that was fixed.

``A`` is recovered from the cached ``L`` as ``A = I - L^-1``, exact to 5e-16,
so the cell attribution needs no model build. The attribution is a **partition**
— contributions sum to the ``L`` effect exactly — using the same symmetric form
as :mod:`bedrock.analysis.nowcasting.results.nd_drivers`::

    contribution of (i, j) to the L effect on c
        = 1/2 [ (B2 L2)_i dA_ij (L1)_jc + (B2 L1)_i dA_ij (L2)_jc ]

with ``B2`` the current year's factors throughout, matching how the ``L``
effect is defined (``B2 @ L2 - B2 @ L1``).

::

    uv run python -m bedrock.analysis.nowcasting.L_flux_priority
    uv run python -m bedrock.analysis.nowcasting.L_flux_priority --check
    uv run python -m bedrock.analysis.nowcasting.L_flux_priority --top 25
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.nowcasting.L_dollar_basis import deflate_L
from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (
    B_total,
    Span,
    commodity_rho,
    load_span,
)
from bedrock.utils.math.formulas import compute_L_matrix
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRY_DESC

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent / 'output'

#: Below this share of a commodity's own ``N``, a percentage move is a ratio to
#: a rounding-scale base. Not a gate — a flag on the output.
TINY_N = 1e-9


#: Widened to plain str keys; INDUSTRY_DESC is typed on a Literal of the 405
#: codes, and these frames carry codes as ordinary strings.
_INDUSTRY_NAME: dict[str, str] = {str(k): str(v) for k, v in INDUSTRY_DESC.items()}


def name_of(code: str) -> str:
    return _INDUSTRY_NAME.get(code, code)


def A_from_L(L: pd.DataFrame) -> pd.DataFrame:
    """``A = I - L^-1``, the direct requirements behind a Leontief inverse.

    The span cache holds ``L`` and not ``A``, and inverting back is exact —
    5e-16 on the round trip, which ``--check`` asserts. That is what keeps the
    cell attribution here free of a model build.
    """
    values = np.eye(len(L)) - np.linalg.inv(L.to_numpy())
    return pd.DataFrame(values, index=L.index, columns=L.columns)


def real_parts(span: Span) -> tuple[pd.DataFrame, dict[int, pd.DataFrame]]:
    """Real ``B`` and real ``L`` for every year — the only basis used here."""
    rho = commodity_rho(span)
    return (
        B_total(span, real=True),
        {year: deflate_L(L, rho[year]) for year, L in span.L.items()},
    )


def l_effect_panel(span: Span) -> pd.DataFrame:
    """The ``L`` effect on every commodity, every year pair.

    ``N_to - N_held`` where ``N_held`` runs the current year's factors through
    the prior year's ``L``: the part of the ``N`` move the factors cannot
    explain. Carries the factor part alongside so the two can be compared per
    commodity rather than only at the median.
    """
    B, L = real_parts(span)
    years = sorted(L)
    frames = []
    for prior, current in zip(years, years[1:]):
        L_prior, L_current = L[prior], L[current]
        N_from = B[prior].reindex(L_prior.index).fillna(0.0) @ L_prior
        N_to = B[current].reindex(L_current.index).fillna(0.0) @ L_current
        N_held = B[current].reindex(L_prior.index).fillna(0.0) @ L_prior
        denom = N_from.where(N_from.abs() > TINY_N)
        frames.append(
            pd.DataFrame(
                {
                    'year_from': prior,
                    'year_to': current,
                    'commodity': N_from.index,
                    'N_from': N_from.to_numpy(),
                    'pct_L_effect': ((N_to - N_held) / denom * 100).to_numpy(),
                    'pct_factor_effect': ((N_held - N_from) / denom * 100).to_numpy(),
                    'pct_change_N': ((N_to - N_from) / denom * 100).to_numpy(),
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def priority(span: Span, panel: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per commodity: how much ``L`` moves its ``N``, across the span.

    ``gross`` sums the absolute yearly ``L`` effects and ``net`` sums them
    signed, so ``oscillation`` separates churn from trend. ``weighted_gross``
    scales gross by the commodity's mean share of total commodity output,
    which is what makes the ranking a priority rather than a curiosity.
    """
    panel = l_effect_panel(span) if panel is None else panel
    share = span.q.div(span.q.sum(axis=0), axis=1).mean(axis=1)

    # the year each commodity's L effect was largest, taken by row rather than
    # by a groupby.apply so the frame keeps a plain index
    ranked_rows = panel.assign(_abs=panel['pct_L_effect'].abs()).dropna(subset=['_abs'])
    worst = ranked_rows.sort_values('_abs').groupby('commodity').last()

    grouped = panel.groupby('commodity')
    out = pd.DataFrame(
        {
            'gross': grouped['pct_L_effect'].apply(lambda s: s.abs().sum()),
            'net': grouped['pct_L_effect'].sum(),
            'factor_gross': grouped['pct_factor_effect'].apply(lambda s: s.abs().sum()),
        }
    )
    out['worst_year'] = worst['year_to']
    out['worst_year_pct'] = worst['pct_L_effect']
    out['oscillation'] = 1.0 - out['net'].abs() / out['gross'].where(out['gross'] > 0)
    out['output_share'] = share.reindex(out.index).fillna(0.0)
    out['weighted_gross'] = out['gross'] * out['output_share']
    # >1 means L moved this commodity's N more than its own factors did
    out['L_over_factors'] = out['gross'] / out['factor_gross'].where(
        out['factor_gross'] > 0
    )
    out['name'] = [name_of(str(c)) for c in out.index]
    return out.sort_values('weighted_gross', ascending=False)


def driving_cells(span: Span, targets: list[str], top: int = 10) -> pd.DataFrame:
    """The ``A`` cells behind each target's ``L`` effect, year by year.

    An exact partition: the contributions sum to the ``L`` effect, which
    ``--check`` asserts. ``input`` is the commodity whose requirement changed
    and ``bought_by`` the industry whose column it changed in — the pair is the
    cell of ``A`` to go and look at, and the two are usually not the target.
    """
    B, L = real_parts(span)
    years = sorted(L)
    rows: list[dict[str, object]] = []
    for prior, current in zip(years, years[1:]):
        L1, L2 = L[prior].to_numpy(), L[current].to_numpy()
        axis = L[prior].index
        dA = A_from_L(L[current]).to_numpy() - A_from_L(L[prior]).to_numpy()
        b2 = B[current].reindex(axis).fillna(0.0).to_numpy()
        left2, left1 = b2 @ L2, b2 @ L1
        position = {str(code): n for n, code in enumerate(axis)}
        n_from = b2 @ L1
        for code in targets:
            if code not in position:
                continue
            c = position[code]
            contrib = 0.5 * (
                np.outer(left2, L1[:, c]) * dA + np.outer(left1, L2[:, c]) * dA
            )
            base = float(n_from[c])
            flat = pd.Series(contrib.ravel())
            for rank, k in enumerate(flat.abs().nlargest(top).index, start=1):
                i, j = divmod(int(k), len(axis))
                rows.append(
                    {
                        'commodity': code,
                        'name': name_of(code),
                        'year_from': prior,
                        'year_to': current,
                        'rank': rank,
                        'input': axis[i],
                        'input_name': name_of(str(axis[i])),
                        'bought_by': axis[j],
                        'bought_by_name': name_of(str(axis[j])),
                        'contribution_pct_of_N': (
                            float(contrib[i, j]) / base * 100 if base else np.nan
                        ),
                        'A_from': float(A_from_L(L[prior]).to_numpy()[i, j]),
                        'A_to': float(A_from_L(L[current]).to_numpy()[i, j]),
                    }
                )
    return pd.DataFrame(rows)


def check(span: Span) -> None:
    """The two identities this rests on."""
    B, L = real_parts(span)
    years = sorted(L)

    # 1. A recovers from L exactly
    worst = 0.0
    for year in years:
        back = compute_L_matrix(A=A_from_L(L[year]))
        worst = max(
            worst,
            float(
                np.abs(back.to_numpy() - L[year].to_numpy()).max()
                / np.abs(L[year].to_numpy()).max()
            ),
        )
    assert worst < 1e-10, f'A = I - L^-1 does not round-trip: {worst:.2e}'
    logger.info('OK  A recovers from the cached L to %.1e, every year', worst)

    # 2. the cell contributions partition the L effect
    worst = 0.0
    for prior, current in zip(years, years[1:]):
        L1, L2 = L[prior].to_numpy(), L[current].to_numpy()
        axis = L[prior].index
        dA = A_from_L(L[current]).to_numpy() - A_from_L(L[prior]).to_numpy()
        b2 = B[current].reindex(axis).fillna(0.0).to_numpy()
        left2, left1 = b2 @ L2, b2 @ L1
        effect = b2 @ L2 - b2 @ L1
        for c in range(0, len(axis), 37):  # every 37th column, ~11 per year
            contrib = 0.5 * (
                np.outer(left2, L1[:, c]) * dA + np.outer(left1, L2[:, c]) * dA
            )
            scale = max(abs(float(effect[c])), 1e-12)
            worst = max(worst, abs(float(contrib.sum()) - float(effect[c])) / scale)
    assert (
        worst < 1e-6
    ), f'cell contributions do not partition the L effect: {worst:.2e}'
    logger.info(
        'OK  A-cell contributions partition the L effect (worst rel %.1e)', worst
    )


def main(top: int = 15, cells: int = 10) -> dict[str, pd.DataFrame]:
    span = load_span()
    logger.info(
        'span %d-%d, FBS %s, MUT %s',
        min(span.L),
        max(span.L),
        span.vintages.fbs,
        span.vintages.mut,
    )
    panel = l_effect_panel(span)
    ranked = priority(span, panel)
    targets = [str(c) for c in ranked.head(top).index]
    tables = {
        'L_flux_priority': ranked,
        'L_flux_panel': panel,
        'L_flux_driving_cells': driving_cells(span, targets, top=cells),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        table.to_csv(OUT_DIR / f'{name}.csv')

    show = ranked.head(top)[
        ['name', 'gross', 'net', 'oscillation', 'output_share', 'L_over_factors']
    ]
    logger.info(
        'Top %d commodities by output-weighted L flux:\n%s',
        top,
        show.round(3).to_string(),
    )
    logger.info(
        'Unweighted top %d, for contrast:\n%s',
        top,
        ranked.nlargest(top, 'gross')[['name', 'gross', 'oscillation', 'output_share']]
        .round(3)
        .to_string(),
    )
    logger.info('Wrote %d tables to %s', len(tables), OUT_DIR)
    return tables


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--top', type=int, default=15)
    parser.add_argument('--cells', type=int, default=10)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    if args.check:
        check(load_span())
    else:
        main(top=args.top, cells=args.cells)
