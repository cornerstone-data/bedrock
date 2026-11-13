"""IV score: one number per ``B`` over 2017-2024, importance times volatility.

The score ranks commodity emission factors for review. It is high when a
factor moves or jumps and when that factor drives ``N``. It does not say why a
factor moved; :mod:`~bedrock.analysis.time_series_B_matrix.B_change_diagnostics`
splits a flagged factor into ``E``, ``x`` and ``Vnorm``.

::

    IV_j = I_j * V_j

    S_jk = B_j L_dom_jk / N_d_k          the share of N_d_k from emissions at j
    N_d  = B L_dom, with L_dom = (I - Adom)^-1
    I_j  = mean over years of sum_k w_k S_jk
    m_j  = median over years of d_jt
    V_j  = sqrt(mean over years of (d_jt - m_j)^2 + trend_weight * m_j^2)
    d_jt = (B_jt - B_jt-1) / mean(B_jt, B_jt-1), with B in constant 2017 dollars

The domestic matrices are used because a US ``B`` only applies to the domestic
part of the chain: the global model in ``ceda`` gives imported inputs their source
countries' factors. ``IV_full_L`` keeps the score on the full ``L`` and ``N`` for
comparison.

``V_Nd`` is the same volatility on each commodity's real ``N_d``, the factor users
get. ``V`` only sees a commodity's own emissions, so it can't register what moves
in its supply chain: a service sector's ``B`` is its own fuel and refrigerants,
and its electricity reaches it through ``L_dom``.

``w_k`` is how much each commodity's ``N`` counts and sums to 1 each year. See
:data:`WEIGHT_SCHEMES`, or pass any weights of your own. With ``equal`` weights
``IV_j`` reads as: in a typical year, the move in this factor shifts the average
commodity's ``N_d`` by this many percentage points.

``S`` is built on nominal ``B`` and ``L_dom``. On a consistent real basis ``rho``
cancels, apart from the gap between deflating on the industry and commodity
axes (``L_deflator_axis_gap.csv``). ``V`` has to be real, because a nominal
factor falls whenever prices rise.

Commodities with no ``N`` in some year are dropped from the targets ``k``.

Findings are in ``About_IV_score.md``.

::

    uv run python -m bedrock.analysis.time_series_B_matrix.IV_score
    uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --check
    uv run python -m bedrock.analysis.time_series_B_matrix.IV_score \\
        --weights final_demand
    uv run python -m bedrock.analysis.time_series_B_matrix.IV_score \\
        --weights-csv my_weights.csv
    uv run python -m bedrock.analysis.time_series_B_matrix.IV_score \\
        --trend-weight 0
"""

from __future__ import annotations

import argparse
import logging
import re
import typing as ta
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.time_series_B_matrix import B_change_diagnostics as bcd
from bedrock.transform.eeio.electricity_delivery import (
    DELIVERY_CODE,
    PUBLISHED_LABELS,
)
from bedrock.utils.config.config_controllers import temp_usa_config
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DELIVERY_NAME
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRY_DESC

logger = logging.getLogger(__name__)

#: The GHG FBS stem the v1.0 year configs name.
FBS_STEM = 'GHG_national_Cornerstone_{year}'

#: Named weightings of the ``N`` values. ``equal`` counts each published factor
#: once. ``final_demand`` and ``output`` count a commodity by its dollars, with
#: negative net final demand set to 0. ``footprint`` counts it by the emissions
#: embodied in its final demand, which reduces ``I_j`` to the emissions share
#: ``B_j q_j / sum(B q)``, so ``L`` drops out.
WEIGHT_SCHEMES: tuple[str, ...] = ('equal', 'final_demand', 'output', 'footprint')

#: The weight on the squared median change in ``V``. At 1, a steady change counts
#: the same as year-to-year noise of the same size. At 0, a steady trend scores 0.
DEFAULT_TREND_WEIGHT = 1.0

OUT_DIR = bcd.OUTPUT_DIR / 'IV_score'

#: Added to the span cache and output folders with the delivery margin on, so
#: those runs never replace the results of the configs as written.
DELIVERY_MARGIN_LABEL = 'delivery_margin'


def delivery_margin_on() -> bool:
    """Whether this run sets ``electricity_delivery_margin`` on the year configs."""
    return bool(bcd.CONFIG_OVERRIDES.get('electricity_delivery_margin'))


def sector_name(code: str) -> str:
    """The name of a sector in this run's schema."""
    if code == DELIVERY_CODE:
        return ELECTRICITY_DELIVERY_NAME
    if delivery_margin_on() and code in PUBLISHED_LABELS:
        return PUBLISHED_LABELS[code][1]
    return ta.cast(ta.Mapping[str, str], INDUSTRY_DESC).get(code, code)


def config_vintages(years: tuple[int, ...] = bcd.YEARS) -> bcd.SpanVintages:
    """The one FBS and one MUT vintage the year configs name across the span."""
    fbs: set[str] = set()
    mut: set[str] = set()
    for year in years:
        with temp_usa_config(bcd.CONFIG_TEMPLATE.format(year=year)):
            config = get_usa_config()
            name = config.cornerstone_ghg_fbs_filename or ''
            match = re.search(r'_(v[^_]+_[0-9a-f]+)\.parquet$', name)
            if match is None:
                raise ValueError(f'{year}: no vintage in FBS filename {name!r}')
            fbs.add(match.group(1))
            mut.add(str(config.nowcast_mut_vintage))
    if len(fbs) != 1 or len(mut) != 1:
        raise ValueError(f'The year configs disagree: FBS {fbs}, MUT {mut}')
    return bcd.SpanVintages(fbs=fbs.pop(), mut=mut.pop())


def load_or_build_span(vintages: bcd.SpanVintages | None = None) -> bcd.Span:
    """The span at *vintages*, cached under ``output/`` per FBS and MUT pair."""
    pinned = vintages or config_vintages()
    bcd.FBS_STEM = FBS_STEM
    bcd.CACHE_DIR = bcd.OUTPUT_DIR / (
        f'cache_fbs_{pinned.fbs.rsplit("_", 1)[-1]}'
        f'_mut_{pinned.mut.rsplit("_", 1)[-1]}'
        f'{"_" + DELIVERY_MARGIN_LABEL if delivery_margin_on() else ""}'
    )
    if (bcd.CACHE_DIR / 'vintages.txt').exists():
        span = bcd.load_span()
        if span.vintages != pinned:
            raise ValueError(f'{bcd.CACHE_DIR} holds {span.vintages}, not {pinned}')
        return span
    span = bcd.build_span(bcd.YEARS, pinned)
    bcd.save_span(span)
    return span


def domestic_span(span: bcd.Span) -> bcd.Span:
    """*span* with ``L_dom`` as its ``L``, cached beside it.

    Everything that reads ``span.L`` then runs on the domestic basis, so
    ``bcd.N_total`` gives ``N_d``. Call after :func:`load_or_build_span`, which
    sets the cache directory.
    """
    L_dom: dict[int, pd.DataFrame] = {}
    for year in sorted(span.L):
        path = bcd.CACHE_DIR / f'L_dom_{year}.parquet'
        if path.exists():
            L_dom[year] = pd.read_parquet(path)
            continue
        with temp_usa_config(
            bcd.CONFIG_TEMPLATE.format(year=year),
            cache_bearing_modules=bcd.CACHE_BEARING_MODULES,
            nowcast_mut_vintage=span.vintages.mut,
            **bcd.CONFIG_OVERRIDES,
        ):
            L_dom[year] = bcd.compute_L_matrix(A=bcd.derive_Aq_scaled().Adom)
        L_dom[year].to_parquet(path)
    return replace(span, L=L_dom)


def n_shares(span: bcd.Span) -> dict[int, pd.DataFrame]:
    """``S_jk = B_j L_jk / N_k`` per year, emitters ``j`` by target ``N`` ``k``.

    Each column sums to 1. Targets with no ``N`` in any year are dropped.
    """
    B = bcd.B_total(span, real=False)
    N = bcd.N_total(span, real=False)
    index = span.L[min(span.L)].index
    positive = N.reindex(index).gt(0).all(axis=1)
    targets = positive[positive].index
    return {
        year: span.L[year]
        .mul(B[year].reindex(index).fillna(0.0), axis=0)
        .loc[:, targets]
        .div(N[year].reindex(targets), axis=1)
        for year in span.L
    }


def net_final_demand(span: bcd.Span) -> pd.DataFrame:
    """``y = L^-1 q``, commodity by year, from the span's own ``L`` and ``q``.

    On the domestic span this is the final demand met by domestic output.
    """
    index = span.L[min(span.L)].index
    return pd.DataFrame(
        {
            year: np.linalg.solve(
                L.to_numpy(),
                span.q[year].reindex(index).fillna(0.0).to_numpy(dtype=float),
            )
            for year, L in span.L.items()
        },
        index=index,
    )


def n_weights(
    span: bcd.Span, targets: pd.Index, weights: str | pd.Series = 'equal'
) -> pd.DataFrame:
    """``w_k`` by year, summing to 1 each year.

    *weights* is a name in :data:`WEIGHT_SCHEMES` or a series keyed on commodity
    that holds for every year. Commodities the series leaves out get 0.
    """
    years = sorted(span.L)
    if isinstance(weights, pd.Series):
        raw = pd.DataFrame({y: weights.reindex(targets).fillna(0.0) for y in years})
    elif weights == 'equal':
        raw = pd.DataFrame(1.0, index=targets, columns=years)
    elif weights == 'final_demand':
        raw = net_final_demand(span).reindex(targets).clip(lower=0.0)
    elif weights == 'output':
        raw = span.q.reindex(targets).fillna(0.0)
    elif weights == 'footprint':
        # Negative net final demand stays in, so the identity with the emissions
        # share holds exactly.
        N = bcd.N_total(span, real=False)
        raw = net_final_demand(span).reindex(targets) * N.reindex(targets)
    else:
        raise ValueError(f'Unknown weights {weights!r}; use one of {WEIGHT_SCHEMES}')
    return raw[years] / raw[years].sum()


def importance(
    span: bcd.Span,
    shares: dict[int, pd.DataFrame],
    weights: str | pd.Series = 'equal',
) -> pd.Series:
    """``I_j``, the weighted share of ``N`` from emitter ``j``. Sums to 1."""
    targets = shares[min(shares)].columns
    w = n_weights(span, targets, weights)
    by_year = pd.DataFrame({year: S @ w[year] for year, S in shares.items()})
    return by_year.mean(axis=1)


def annual_change(B: pd.DataFrame) -> pd.DataFrame:
    """``d_jt``, the change against the two-year mean, so a zero stays finite."""
    prior = B.shift(1, axis=1)
    mean = (B.abs() + prior.abs()) / 2
    return ((B - prior) / mean.where(mean > 0)).iloc[:, 1:].fillna(0.0)


def volatility(
    B: pd.DataFrame, trend_weight: float = DEFAULT_TREND_WEIGHT
) -> pd.DataFrame:
    """``V_j``, plus the columns that say what kind of movement it is.

    Movement around the median annual change and the median change itself are
    added in quadrature, the second scaled by *trend_weight*. The median is the
    trend, so a single jump does not count as one.

    - ``V_no_trend``: ``V`` at a trend weight of 0
    - ``median_change``: the trend, per year
    - ``years_with_trend``: the number of years moving the same way as the median
    - ``peak_year``, ``largest_deviation``: the year furthest from the median
    - ``jump_share``: that year's share of squared deviations; 1 is a single jump
    - ``oscillation``: ``1 - |sum d| / sum |d|``; 0 goes one way, 1 returns
    """
    d = annual_change(B)
    median = d.median(axis=1)
    deviation = d.sub(median, axis=0)
    squared = deviation.pow(2)
    gross = d.abs().sum(axis=1)
    return pd.DataFrame(
        {
            'V': np.sqrt(squared.mean(axis=1) + trend_weight * median.pow(2)),
            'V_no_trend': np.sqrt(squared.mean(axis=1)),
            'median_change': median,
            'years_with_trend': d.mul(median, axis=0).gt(0).sum(axis=1),
            'peak_year': deviation.abs().idxmax(axis=1).astype(int),
            'largest_deviation': deviation.abs().max(axis=1),
            'jump_share': (squared.max(axis=1) / squared.sum(axis=1)).fillna(0.0),
            'oscillation': (1 - d.sum(axis=1).abs() / gross).fillna(0.0),
            'net_change': d.sum(axis=1),
        }
    )


def largest_n_move(span: bcd.Span, targets: pd.Index) -> pd.DataFrame:
    """The largest single move in any ``N`` that one factor's annual change causes.

    Percent of the prior year's ``N``, with ``L`` held at the prior year. The
    average in ``I`` dilutes a factor that moves one ``N`` a long way, and this
    column shows it.
    """
    B = bcd.B_total(span, real=True)
    N = bcd.N_total(span, real=True)
    L_real = bcd.L_real(span)
    years = sorted(L_real)
    index = L_real[years[0]].index
    moves = []
    for prior, current in zip(years, years[1:]):
        dB = (B[current] - B[prior]).reindex(index).fillna(0.0)
        move = L_real[prior].loc[:, targets].mul(dB, axis=0)
        moves.append(move.div(N[prior].reindex(targets), axis=1).abs() * 100)
    stacked = np.stack([m.to_numpy() for m in moves])  # year, emitter, target
    peaks = [
        np.unravel_index(stacked[:, j, :].argmax(), stacked[:, j, :].shape)
        for j in range(len(index))
    ]
    return pd.DataFrame(
        {
            'largest_N_move_pct': stacked.max(axis=(0, 2)),
            'largest_N_move_year': [years[1:][int(t)] for t, _ in peaks],
            'largest_N_move_target': [str(targets[int(k)]) for _, k in peaks],
        },
        index=index,
    )


def iv_table(
    span: bcd.Span,
    full_span: bcd.Span,
    weights: str | pd.Series = 'equal',
    trend_weight: float = DEFAULT_TREND_WEIGHT,
) -> pd.DataFrame:
    """One row per commodity, sorted on ``IV``, the headline under *weights*.

    *span* is the domestic span. ``IV_<scheme>`` carries the score under each
    named weighting alongside, ``IV_full_L`` the score on *full_span*, and
    ``IV_trend`` the part of the score that comes from the trend alone.
    """
    shares = n_shares(span)
    targets = shares[min(shares)].columns
    B_real = bcd.B_total(span, real=True)
    index = shares[min(shares)].index
    V = volatility(B_real.reindex(index).fillna(0.0), trend_weight)

    table = pd.DataFrame(index=index)
    table['name'] = [sector_name(str(c)) for c in index]
    table['I'] = importance(span, shares, weights)
    table = table.join(V)
    table['IV'] = table['I'] * table['V'] * 100
    # The same volatility on the commodity's total domestic factor. A service
    # sector's B is its own fuel and refrigerants, so what it buys, electricity
    # above all, moves only N_d (#1305).
    N_real = bcd.N_total(span, real=True).reindex(index).fillna(0.0)
    table['V_Nd'] = volatility(N_real, trend_weight)['V']
    table['rank'] = table['IV'].rank(ascending=False, method='min').astype(int)
    table['IV_trend'] = table['I'] * table['median_change'].abs() * 100
    table['IV_full_L'] = (
        importance(full_span, n_shares(full_span), weights) * table['V'] * 100
    )
    for scheme in WEIGHT_SCHEMES:
        table[f'IV_{scheme}'] = importance(span, shares, scheme) * table['V'] * 100

    B_nominal = bcd.B_total(span, real=False).reindex(index).fillna(0.0)
    q = span.q.reindex(index).fillna(0.0)
    table['mean_emissions_Mt'] = (B_nominal * q).mean(axis=1) / 1e9
    table['own_share_of_N'] = pd.DataFrame(
        {
            year: pd.Series([S.at[k, k] for k in targets], index=targets, dtype=float)
            for year, S in shares.items()
        }
    ).mean(axis=1)
    table = table.join(largest_n_move(span, targets))
    own_measure = bcd.B_change(span, real=True).groupby('commodity')[
        'abs_delta_B_pct_of_N'
    ]
    table['sum_abs_delta_B_pct_of_N'] = own_measure.sum().reindex(index).fillna(0.0)
    return table.sort_values('IV', ascending=False).rename_axis(index='commodity')


def movement_by_year(span: bcd.Span, table: pd.DataFrame) -> pd.DataFrame:
    """Each year's share of squared ``I * deviation``, with and without the top ``B``."""
    B_real = bcd.B_total(span, real=True).reindex(table.index).fillna(0.0)
    d = annual_change(B_real)
    deviation = d.sub(d.median(axis=1), axis=0)
    squared = deviation.mul(table['I'], axis=0).pow(2)
    without_top = squared.iloc[1:]
    return pd.DataFrame(
        {
            'all_commodities': squared.sum() / squared.sum().sum(),
            f'without_{table.index[0]}': without_top.sum() / without_top.sum().sum(),
        }
    ).rename_axis(index='year')


def check(span: bcd.Span) -> None:
    """Assert the identities the score rests on and its response to test series."""
    shares = n_shares(span)
    worst = max(float((S.sum() - 1).abs().max()) for S in shares.values())
    assert worst < 1e-9, f'S does not sum to 1 over emitters: {worst:.1e}'
    logger.info('OK  S sums to 1 over emitters for every N (worst %.1e)', worst)

    for scheme in WEIGHT_SCHEMES:
        total = float(importance(span, shares, scheme).sum())
        assert abs(total - 1) < 1e-9, f'{scheme}: I sums to {total}'
    logger.info('OK  I sums to 1 under every named weighting')

    B = bcd.B_total(span, real=False)
    index = shares[min(shares)].index
    emissions = (B.reindex(index).fillna(0.0) * span.q.reindex(index).fillna(0.0))[
        sorted(span.L)
    ]
    share = (emissions / emissions.sum()).mean(axis=1)
    gap = float((importance(span, shares, 'footprint') - share).abs().max())
    assert gap < 1e-9, f'footprint importance is not the emissions share: {gap:.1e}'
    logger.info('OK  footprint importance is the emissions share (gap %.1e)', gap)

    years = sorted(span.L)
    test_series = {
        'flat': [1.0] * 8,
        'steady': [0.8**i for i in range(8)],
        'wobble_10pct': [1.0, 1.1] * 4,
        'step_50pct': [1.0] * 4 + [1.5] * 4,
        'spike_50pct': [1.0] * 4 + [1.5] + [1.0] * 3,
    }
    series = pd.DataFrame(test_series, index=years).T
    V = volatility(series)['V']
    V_no_trend = volatility(series, trend_weight=0.0)['V']
    assert V['flat'] == 0, 'a flat series must score 0'
    assert abs(V['steady'] - 0.2 / 0.9) < 1e-12, 'a steady trend must score its rate'
    assert V_no_trend['steady'] < 1e-12, 'at trend weight 0 a trend must score 0'
    assert V['spike_50pct'] == V_no_trend['spike_50pct'], 'a jump is not a trend'
    assert V_no_trend['spike_50pct'] > V_no_trend['step_50pct'], V_no_trend.to_string()
    assert V_no_trend['step_50pct'] > V_no_trend['wobble_10pct'], V_no_trend.to_string()
    logger.info('OK  test series, V:\n%s', V.round(3).to_string())


def main(
    weights: str | pd.Series = 'equal',
    out_dir: Path = OUT_DIR,
    trend_weight: float = DEFAULT_TREND_WEIGHT,
) -> pd.DataFrame:
    full_span = load_or_build_span()
    span = domestic_span(full_span)
    logger.info('FBS %s, MUT %s', span.vintages.fbs, span.vintages.mut)
    table = iv_table(span, full_span, weights, trend_weight)
    by_year = movement_by_year(span, table)
    steady = table.sort_values('IV_trend', ascending=False)[
        ['name', 'IV_trend', 'I', 'median_change', 'years_with_trend', 'net_change']
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(out_dir / 'IV_score.csv')
    by_year.to_csv(out_dir / 'IV_score_by_year.csv')
    steady.to_csv(out_dir / 'IV_score_steady_trends.csv')

    columns = ['name', 'IV', 'I', 'V', 'peak_year', 'jump_share', 'oscillation']
    logger.info('Top 30:\n%s', table[columns].head(30).round(3).to_string())
    logger.info('Largest steady trends:\n%s', steady.head(15).round(3).to_string())
    logger.info('Share by year:\n%s', by_year.round(3).to_string())
    return table


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--check', action='store_true', help='assert and exit')
    parser.add_argument('--weights', choices=WEIGHT_SCHEMES, default='equal')
    parser.add_argument(
        '--weights-csv',
        type=Path,
        help='CSV with columns commodity and weight; overrides --weights',
    )
    parser.add_argument(
        '--trend-weight',
        type=float,
        default=DEFAULT_TREND_WEIGHT,
        help='weight on the squared median annual change in V; 0 ignores a trend',
    )
    parser.add_argument(
        '--electricity-delivery-margin',
        action='store_true',
        help='split electricity into generation and delivery in every year',
    )
    args = parser.parse_args()
    run_dir = OUT_DIR
    if args.electricity_delivery_margin:
        bcd.CONFIG_OVERRIDES['electricity_delivery_margin'] = True
        run_dir = OUT_DIR / DELIVERY_MARGIN_LABEL
    if args.check:
        check(domestic_span(load_or_build_span()))
    elif args.weights_csv:
        custom = pd.read_csv(args.weights_csv, dtype={'commodity': str})
        main(
            custom.set_index('commodity')['weight'],
            run_dir,
            trend_weight=args.trend_weight,
        )
    else:
        main(args.weights, run_dir, trend_weight=args.trend_weight)
