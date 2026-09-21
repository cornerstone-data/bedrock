"""#937: how much of ``L``'s movement in ``N`` is relative prices.

`N = B @ L`. The B smoothing diagnostics report that ``L`` moves ``N`` two to
four times as much as the emission factors do, and hand the question here
because ``L`` comes from ``A = U_norm @ V_norm``, the nowcast's own IO product.
This module answers the two things #937 asks: how much of that ``L`` movement
is relative prices rather than structure, and whether ``A`` should be deflated.

**The short answer is that deflating ``A`` changes ``N`` by nothing at all, and
the 2-to-4x figure is an artefact of a mixed dollar basis.** Both follow from
one identity, written in the house price term ``rho``.

``rho`` is the useeior/Cornerstone price ratio: ``rho_j = PI_j[base] / PI_j[y]``
for commodity *j* in year *y*, the same quantity
:func:`~bedrock.utils.economic.inflation_helpers_cornerstone.derive_price_index_panel`
publishes as the Excel ``Rho`` panel and
:func:`~bedrock.utils.economic.inflation_helpers_cornerstone.get_rho_inflation_ratio`
returns. ⚠️ It is the **reciprocal** of the forward ratio
``get_cornerstone_industry_price_ratio`` gives, so every formula below divides
where a forward ratio would multiply.

A real coefficient is ``a_real[i, j] = a[i, j] * rho_i / rho_j`` - deflate the
input, re-inflate the output - and because

    I - A_real = diag(rho) (I - A) diag(1/rho)

the same similarity transform carries through the inverse::

    L_real = diag(rho) L diag(1/rho)    L_real[i, j] = L[i, j] * rho_i / rho_j

so ``L`` never has to be re-derived from a deflated ``A``. A real direct factor
is ``B_real = B / rho`` (same kilograms, fewer constant dollars), and then

    N_real[j] = sum_i (B[i] / rho_i) (L[i, j] rho_i / rho_j)
              = (1 / rho_j) * sum_i B[i] L[i, j]

The ``rho_i`` cancels. A consistently deflated ``N`` is just the nominal ``N``
with its own denominator deflated, which is what
:func:`~bedrock.utils.validation.diagnostics_helpers.inflation_adjust_ef_denom_to_new_base_year`
already does on the reporting path. Verified to 5e-16 by ``--check``.

⚠️ **The cancellation needs both sides or neither.** ``B_change_diagnostics``
used to deflate ``B`` and leave ``L`` at each year's own prices, so its ``N``
was a hybrid its own docstring warned about - and the 2-to-4x claim in #937 is
measured on that hybrid. Put both sides on one basis and the ``L`` effect in
2021 falls from 14.8 to 6.0 percentage points. ``L`` still leads the factors,
by 1.1x to 3.0x rather than 2x to 4x, and the worst year is no longer 2021.
**Fixed there on 2026-09-21 (#957)**; this module keeps the hybrid alongside
the two consistent bases so the correction stays measurable.

Where deflating ``A`` *does* matter is reading it as structure rather than as
an input to ``N``: on the nominal ``L`` the median commodity's total
requirements fall 3.5% over 2017-2024, and on the real one 0.3%. Anything that
reads a supply chain as lengthening or shortening has to deflate first.

Runs off the span ``B_change_diagnostics`` already caches, so it needs no model
build::

    uv run python -m bedrock.analysis.nowcasting.L_dollar_basis
    uv run python -m bedrock.analysis.nowcasting.L_dollar_basis --check

Findings in [About_the_L_dollar_basis.md](About_the_L_dollar_basis.md).
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (
    B_total,
    Span,
    commodity_rho,
    load_span,
)
from bedrock.utils.math.formulas import rebase_coefficient_matrix
from bedrock.utils.validation.analysis.plotting import setup_mpl

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent / 'output'

#: The three internally consistent ways to hold ``B`` and ``L``, plus the one
#: the diagnostics use today. ``mixed`` is not a dollar basis - it is a real
#: ``B`` against a nominal ``L`` - and it is here to be measured, not used.
BASES = ('nominal', 'mixed', 'real')


# --- the deflator -----------------------------------------------------------
#
# ``commodity_rho`` lives in ``B_change_diagnostics`` next to ``deflate_x``,
# because it is built from the span's own ``x_real / x`` and so cannot drift
# from the deflator ``B`` already uses. The matrix transform lives in
# ``formulas`` because ``A`` and ``L`` take it identically.


def deflate_L(L: pd.DataFrame, rho: pd.Series) -> pd.DataFrame:
    """``diag(rho) L diag(1/rho)`` - the Leontief inverse at constant prices.

    Thin wrapper on :func:`~bedrock.utils.math.formulas.rebase_coefficient_matrix`
    so this module reads in its own terms. Exact rather than approximate: the
    same similarity transform deflates ``A``, and it carries through the
    inverse, so this is ``(I - A_real)^-1`` without rebuilding ``A``. The
    diagonal is invariant (``rho_j / rho_j``), which ``--check`` asserts.
    """
    return rebase_coefficient_matrix(matrix=L, rho=rho)


# --- the three bases --------------------------------------------------------


def bases(span: Span) -> dict[str, tuple[pd.DataFrame, dict[int, pd.DataFrame]]]:
    """``B`` and ``L`` held together on each basis.

    ``nominal`` and ``real`` are consistent and differ only by units.
    ``mixed`` - real ``B``, nominal ``L`` - is what ``B_change_diagnostics``
    reports and is a hybrid of the two.
    """
    rho = commodity_rho(span)
    L_real = {year: deflate_L(span.L[year], rho[year]) for year in span.L}
    return {
        'nominal': (B_total(span, real=False), span.L),
        'mixed': (B_total(span, real=True), span.L),
        'real': (B_total(span, real=True), L_real),
    }


def n_split(
    B: pd.DataFrame,
    L: dict[int, pd.DataFrame],
    weights: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Year-on-year ``N`` movement split into the factor and ``L`` halves.

    Holding ``L`` at the prior year isolates the part of the move the emission
    factors explain; the remainder is ``L``. One row per year pair, reporting
    the median absolute percentage over commodities and - when *weights* is
    given, commodity output - the output-weighted mean absolute.

    ⚠️ The three columns do not add up. Each is a separate median (or weighted
    mean) of an absolute value, and a commodity whose factor and ``L`` effects
    have opposite signs contributes to both without contributing to the total.
    Read them as three magnitudes, not as a partition.
    """
    years = sorted(L)
    rows = []
    for prior, current in zip(years, years[1:]):
        L_prior = L[prior]
        N_from = B[prior].reindex(L_prior.index).fillna(0.0) @ L_prior
        N_to = B[current].reindex(L[current].index).fillna(0.0) @ L[current]
        # the current year's factors run through the PRIOR year's L
        N_held = B[current].reindex(L_prior.index).fillna(0.0) @ L_prior
        denom = N_from.where(N_from != 0)
        total = ((N_to - N_from) / denom * 100).abs()
        factors = ((N_held - N_from) / denom * 100).abs()
        l_effect = ((N_to - N_held) / denom * 100).abs()

        def agg(series: pd.Series) -> float:
            if weights is None:
                return float(series.median())
            w = weights[current].reindex(series.index).fillna(0.0).clip(lower=0)
            return float(np.average(series.fillna(0.0), weights=w))

        rows.append(
            {
                'year': current,
                'total': agg(total),
                'factors': agg(factors),
                'L': agg(l_effect),
                'L_over_factors': agg(l_effect) / agg(factors),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def basis_comparison(span: Span, weighted: bool = False) -> pd.DataFrame:
    """:func:`n_split` on all three bases, side by side."""
    weights = span.q if weighted else None
    return pd.concat(
        {name: n_split(B, L, weights) for name, (B, L) in bases(span).items()},
        axis=1,
    )


def supply_chain_length(span: Span) -> pd.DataFrame:
    """``L`` column sums - total requirements per dollar of final demand.

    The one place in this module where deflating ``A`` changes an answer, since
    this reads ``L`` as structure rather than feeding it to ``N``. A nominal
    column sum falls whenever the inputs a commodity buys get cheaper relative
    to the commodity itself, with no change in what is bought.
    """
    rho = commodity_rho(span)
    rows = []
    for year in sorted(span.L):
        nominal = span.L[year].to_numpy().sum(axis=0)
        real = deflate_L(span.L[year], rho[year]).to_numpy().sum(axis=0)
        rows.append(
            {
                'year': year,
                'nominal_median': float(np.median(nominal)),
                'real_median': float(np.median(real)),
                'nominal_mean': float(nominal.mean()),
                'real_mean': float(real.mean()),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def deflator_axis_gap(span: Span) -> pd.DataFrame:
    """Size the one inconsistency left in the real basis.

    ``B_total(real=True)`` divides each *industry*'s emissions by that
    industry's own deflated output and only then maps to commodities through
    ``V_norm``; the ``L`` deflation has to work on the commodity axis, because
    that is the axis ``A`` is on. Compared here against ``B / rho`` on the
    commodity axis, the two being equal only where industry prices are uniform
    within a commodity's supplying mix. The two coincide only where industry prices
    are uniform within a commodity's supplying mix.

    Reported as the relative gap between the two ways of writing a real ``B``.
    It is immaterial at the median and is not at the tail, so a single
    commodity's real ``N`` should be read against this before it is quoted.
    """
    base = int(span.x.columns[0])
    rho = commodity_rho(span)
    B_nom, B_real = B_total(span, real=False), B_total(span, real=True)
    index = span.L[base].index
    rows = []
    for year in sorted(span.L):
        by_industry = B_real[year].reindex(index).fillna(0.0)
        by_commodity = B_nom[year].reindex(index).fillna(0.0) / rho[year]
        gap = (
            (by_industry - by_commodity) / by_commodity.where(by_commodity != 0)
        ).abs()
        rows.append(
            {
                'year': year,
                'median': float(gap.median()),
                'p95': float(gap.quantile(0.95)),
                'max': float(gap.max()),
            }
        )
    return pd.DataFrame(rows).set_index('year')


def per_commodity(span: Span) -> pd.DataFrame:
    """The mixed and real ``N`` move for every commodity-year.

    What the medians hide: the hybrid basis does not overstate uniformly, it
    adds noise in both directions, so the commodities the reported series puts
    at the top of a year are not the ones a consistent basis does.
    """
    built = bases(span)
    years = sorted(span.L)
    frames = []
    for name in ('mixed', 'real'):
        B, L = built[name]
        for prior, current in zip(years, years[1:]):
            L_prior = L[prior]
            N_from = B[prior].reindex(L_prior.index).fillna(0.0) @ L_prior
            N_to = B[current].reindex(L[current].index).fillna(0.0) @ L[current]
            N_held = B[current].reindex(L_prior.index).fillna(0.0) @ L_prior
            denom = N_from.where(N_from != 0)
            frames.append(
                pd.DataFrame(
                    {
                        'basis': name,
                        'year_from': prior,
                        'year_to': current,
                        'commodity': N_from.index,
                        'pct_change_N': (N_to - N_from) / denom * 100,
                        'pct_factors': (N_held - N_from) / denom * 100,
                        'pct_L': (N_to - N_held) / denom * 100,
                    }
                ).reset_index(drop=True)
            )
    long = pd.concat(frames, ignore_index=True)
    wide = long.pivot_table(
        index=['year_from', 'year_to', 'commodity'],
        columns='basis',
        values=['pct_change_N', 'pct_factors', 'pct_L'],
    )
    wide.columns = pd.Index(
        [f'{col[0]}_{col[1]}' for col in wide.columns.to_flat_index()]
    )
    wide['basis_gap_N'] = wide['pct_change_N_mixed'] - wide['pct_change_N_real']
    return wide.reset_index().sort_values(
        'basis_gap_N', key=lambda s: s.abs(), ascending=False
    )


def L_gross_movement(span: Span) -> pd.DataFrame:
    """``sum |dL|`` year on year, nominal against real.

    Deflating does not quieten ``L``: the real series is within 8% of the
    nominal one every year but 2024, where it is 64% *larger* - nominal prices
    were masking real structural movement, not creating it.
    """
    rho = commodity_rho(span)
    years = sorted(span.L)
    rows = []
    for prior, current in zip(years, years[1:]):
        nominal = (span.L[current] - span.L[prior]).abs().to_numpy().sum()
        real = (
            (
                deflate_L(span.L[current], rho[current])
                - deflate_L(span.L[prior], rho[prior])
            )
            .abs()
            .to_numpy()
            .sum()
        )
        rows.append(
            {
                'year': current,
                'nominal': nominal,
                'real': real,
                'real_over_nominal': real / nominal,
            }
        )
    return pd.DataFrame(rows).set_index('year')


# --- checks -----------------------------------------------------------------


def check(span: Span) -> None:
    """Assert the identities the module rests on, and size the two caveats.

    Analysis modules here carry their checks as a CLI flag rather than as unit
    tests, so this is the thing to run after touching the transform.
    """
    years = sorted(span.L)
    base = years[0]
    rho = commodity_rho(span)

    # 1. the diagonal survives the similarity transform untouched
    for year in years:
        before = np.diag(span.L[year].to_numpy())
        after = np.diag(deflate_L(span.L[year], rho[year]).to_numpy())
        assert np.allclose(before, after), f'{year}: L diagonal moved under deflation'
    logger.info('OK  L diagonal is invariant under the deflation, every year')

    # 2. the cancellation: with ONE deflator family, deflating A does not move N
    B_nom = B_total(span, real=False)
    worst = 0.0
    for year in years:
        L = span.L[year]
        rho_y = rho[year].reindex(L.index).fillna(1.0)
        both = (B_nom[year].reindex(L.index).fillna(0.0) / rho_y) @ deflate_L(L, rho_y)
        denom_only = (B_nom[year].reindex(L.index).fillna(0.0) @ L) / rho_y
        worst = max(
            worst, float(np.abs(both - denom_only).max() / np.abs(denom_only).max())
        )
    assert worst < 1e-12, f'the cancellation does not hold: {worst:.2e}'
    logger.info(
        'OK  deflating A leaves N unchanged to %.1e - a real N is the nominal '
        'one with its denominator deflated',
        worst,
    )

    # 3. the mixed basis reproduces what B_change_diagnostics publishes
    mixed = n_split(*bases(span)['mixed'])
    logger.info(
        'the hybrid basis #937 was measured on - what B_change_real.csv held '
        'before #957 restated it:\n%s',
        mixed.round(2).to_string(),
    )

    # 4. the deflator choice is not load-bearing
    # rho read straight onto the commodity axis, 1:1 by code - the reciprocal
    # of what get_cornerstone_industry_price_ratio does on the nowcast path
    rho_ind = span.x_real / span.x
    rho_flat = pd.DataFrame(
        {y: rho_ind[y].reindex(span.L[base].index).fillna(1.0) for y in years}
    )
    variants = {
        'base-year V_norm weights': rho,
        'current-year V_norm weights': pd.concat(
            [commodity_rho(span, weight_year=y)[y].rename(y) for y in years],
            axis=1,
        ),
        '1:1 industry->commodity': rho_flat,
    }
    B_real = B_total(span, real=True)
    spread: dict[str, pd.Series] = {}
    for name, variant in variants.items():
        L_real = {y: deflate_L(span.L[y], variant[y]) for y in years}
        spread[name] = n_split(B_real, L_real)['L']

    # chained: rebase each pair onto its own prior year instead of onto the
    # base year, so no year carries seven years of accumulated price movement
    chained = {}
    for prior, current in zip(years, years[1:]):
        step = (rho[current] / rho[prior]).reindex(span.L[current].index).fillna(1.0)
        L_prior = span.L[prior]
        L_current = deflate_L(span.L[current], step)
        N_from = B_real[prior].reindex(L_prior.index).fillna(0.0) @ L_prior
        N_to = B_real[current].reindex(L_current.index).fillna(0.0) @ L_current
        N_held = B_real[current].reindex(L_prior.index).fillna(0.0) @ L_prior
        denom = N_from.where(N_from != 0)
        chained[current] = float(((N_to - N_held) / denom * 100).abs().median())
    spread['chained to the prior year'] = pd.Series(chained)
    table = pd.DataFrame(spread)
    worst_gap = float((table.max(axis=1) - table.min(axis=1)).max())
    logger.info(
        'L effect under four deflator choices (median |%%|):\n%s\n'
        'widest disagreement in any year: %.2f pp',
        table.round(2).to_string(),
        worst_gap,
    )
    assert (
        worst_gap < 0.5
    ), f'the deflator choice moves the answer by {worst_gap:.2f} pp'
    logger.info('OK  the finding does not depend on which deflator is used')

    # 5. the residual inconsistency in the real basis, measured not waved at
    gap = deflator_axis_gap(span)
    logger.info(
        'industry-axis vs commodity-axis real B, relative gap:\n%s',
        gap.map(lambda v: f'{v:.2%}').to_string(),
    )
    assert float(gap['median'].max()) < 0.01, (
        'the two ways of writing a real B disagree at the median - the '
        'commodity deflator is no longer a fair carry of the industry one'
    )
    logger.info(
        'OK  immaterial at the median; read the tail before quoting one commodity'
    )


# --- output -----------------------------------------------------------------


def plot_bases(comparison: pd.DataFrame, name: str = 'L_dollar_basis.png') -> None:
    """The same N movement read on each basis, so the hybrid stands out."""
    setup_mpl()
    fig, ax = plt.subplots(figsize=(9, 5.4))
    styles = {
        'nominal': ('tab:grey', '--', 'nominal B and L'),
        'mixed': ('tab:red', '-', 'real B, nominal L  (the hybrid, pre-#957)'),
        'real': ('tab:blue', '-', 'real B and L'),
    }
    for basis, (colour, dash, label) in styles.items():
        ax.plot(
            comparison.index,
            comparison[(basis, 'total')],
            color=colour,
            linestyle=dash,
            marker='o',
            label=label,
        )
    ax.set_ylabel('median |% change in N|')
    ax.set_xlabel('year')
    ax.set_title('The 2021 and 2023 spikes in N are a dollar-basis artefact', pad=12)
    # headroom above the 2021 peak so the legend never sits on a series
    peak = float(comparison.xs('total', level=1, axis=1).max().max())
    ax.set_ylim(top=peak * 1.45)
    ax.legend(loc='upper left', framealpha=0.95, fontsize='small')
    ax.grid(alpha=0.3)
    fig.tight_layout()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_DIR / name, dpi=150)
    plt.close(fig)


def main() -> dict[str, pd.DataFrame]:
    span = load_span()
    logger.info(
        'span %s, FBS %s, MUT %s',
        f'{min(span.L)}-{max(span.L)}',
        span.vintages.fbs,
        span.vintages.mut,
    )
    tables = {
        'L_basis_median': basis_comparison(span),
        'L_basis_output_weighted': basis_comparison(span, weighted=True),
        'L_supply_chain_length': supply_chain_length(span),
        'L_gross_movement': L_gross_movement(span),
        'L_commodity_rho': commodity_rho(span),
        'L_deflator_axis_gap': deflator_axis_gap(span),
        'L_basis_per_commodity': per_commodity(span),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        table.to_csv(OUT_DIR / f'{name}.csv')
    plot_bases(tables['L_basis_median'])
    for name in ('L_basis_median', 'L_basis_output_weighted'):
        logger.info('%s:\n%s', name, tables[name].round(2).to_string())
    logger.info(
        'supply chain length, span change:\n%s',
        (
            tables['L_supply_chain_length'].iloc[-1]
            / tables['L_supply_chain_length'].iloc[0]
            - 1
        )
        .mul(100)
        .round(2)
        .to_string(),
    )
    logger.info('Wrote %d tables and a plot to %s', len(tables), OUT_DIR)
    return tables


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument(
        '--check',
        action='store_true',
        help='assert the identities and size the deflator sensitivity, then exit',
    )
    args = parser.parse_args()
    if args.check:
        check(load_span())
    else:
        main()
