"""A single national generation price cannot represent a below-price purchaser.

The live allocator (``allocate_purchaser_gtd``, methods Discussion #88 D0/D4)
converts MWh to generation dollars with one national price ``p``, then caps the
result at the purchaser's own electricity bill and water-fills the excess onto
others in the same class (D8). Inside manufacturing the MWh come from MECS
Table 7.7 purchased kWh (Discussion #90 M4).

Those two decisions are individually defensible and **jointly inconsistent**.
The cap is an identity, not an edge case:

    gen_i = MWh_i x p,  capped at bill_i
    gen_i <= bill_i   <=>   bill_i / MWh_i >= p

So a purchaser is capped **if and only if** its own all-in electricity price is
below the single national generation price. This module verifies that iff
exactly -- the clipped set and the below-``p`` set are the same set, in every
year -- and then prices the consequence.

The consequence is the part that matters. A capped purchaser has **zero** T&D,
because T&D is the residual of the bill after generation and the cap consumes
the whole bill. It is therefore modelled as buying generation and no delivery,
which no real purchaser does. Since generation carries essentially all of the
direct emissions (the results deck puts the three children at 7.197 / 0.226 /
0.0 kg CO2e per USD), a capped purchaser's blended electricity emission factor
is the pure generation factor -- about twice the factor a comparable unclipped
purchaser gets.

This contradicts the method's own stated logic. Discussion #88's guiding
principle 6 and the results deck both say the price differential between
purchasers is what shows up in T&D. The flat price sets T&D to zero precisely
for the purchasers whose prices sit furthest below average, so the mechanism
that is supposed to carry the price difference is switched off exactly where the
difference is largest.

::

    python -m bedrock.analysis.electricity.current.eia_gtd.flat_price_clipping
    # options: --years 2017-2024  --top 12  --csv  --check
    #          --mut-vintage v0.3.0_4276083   (skip the GCS probe; the year
    #                                          configs omit the pin)
"""

from __future__ import annotations

import argparse
import logging
import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent

#: UGO 2017 generation-dollar share and T/(T+D), from
#: ``build_electricity_disagg_go_weights``. ``p_share`` sets the generation
#: price and therefore who clips, so these are pinned rather than guessed.
P_SHARE_2017 = 0.34171400104769045
TD_SHARE_2017 = 0.05924206666536638

#: Direct GHG emission factors for the three electricity children, kg CO2e per
#: USD, as published in the results deck for the EIA-anchored + MECS build
#: ("3-way split" column). Used only to price the consequence of a zero-T&D
#: allocation; nothing in the identity check depends on them.
EF_GENERATION = 7.197
EF_TRANSMISSION = 0.226
EF_DISTRIBUTION = 0.0

#: A T&D allocation this small is zero at the scale of an electricity bill.
_ZERO_TD_ATOL_USD = 1.0

_NAMES: dict[str, str] = {str(k): str(v) for k, v in COMMODITY_DESC.items()}


class YearResult(ta.NamedTuple):
    """One year's allocation, plus the price ``p`` it was run at."""

    table: pd.DataFrame
    p_cents_per_kwh: float
    vintage: str


def _desc(code: str) -> str:
    return _NAMES.get(code, '')


def _row(frame: pd.DataFrame, label: str) -> 'pd.Series[float]':
    """One row as a float Series; a duplicated label would give a frame."""
    selected = frame.loc[label]
    if isinstance(selected, pd.DataFrame):
        raise ValueError(f'{label!r} is duplicated in the index, got {selected.shape}')
    return selected.astype(float)


def _activate(year: int, mut_vintage: str | None) -> str:
    from bedrock.analysis.nowcasting.results._ef_smoke_lib import (  # noqa: PLC0415
        clear_year_caches,
        config_stem,
        resolved_mut_vintage,
    )
    from bedrock.utils.config.usa_config import (  # noqa: PLC0415
        get_usa_config,
        reset_usa_config,
        set_global_usa_config,
    )

    clear_year_caches()
    reset_usa_config()
    set_global_usa_config(f'{config_stem(year)}.yaml')
    if mut_vintage:
        object.__setattr__(get_usa_config(), 'nowcast_mut_vintage', mut_vintage)
        return mut_vintage
    return resolved_mut_vintage()


def allocate_year(year: int, mut_vintage: str | None) -> YearResult:
    """Run the live allocator for ``year`` and tabulate the Industrial class."""
    from bedrock.extract.iot.nowcast_mut_storage import (  # noqa: PLC0415
        load_nowcast_detail_Utot_usa,
        load_nowcast_detail_Ytot_usa,
    )
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        ELECTRICITY_AGGREGATE,
        IMPORT_FD_CODE,
        allocate_purchaser_gtd,
        industrial_manufacturing_pool,
    )

    vintage = _activate(year, mut_vintage)
    U = load_nowcast_detail_Utot_usa()
    Y = load_nowcast_detail_Ytot_usa()
    purchases = _row(U, ELECTRICITY_AGGREGATE).add(
        _row(Y, ELECTRICITY_AGGREGATE), fill_value=0.0
    )
    purchases.index = [str(i) for i in purchases.index]
    if IMPORT_FD_CODE in purchases.index:
        purchases = purchases.drop(index=IMPORT_FD_CODE)

    allocation = allocate_purchaser_gtd(
        purchases,
        self_use_key=ELECTRICITY_AGGREGATE,
        eia_year=year,
        p_share=P_SHARE_2017,
        td_share=TD_SHARE_2017,
        industrial_weights='mecs',
    )
    # allocate_purchaser_gtd reports p in USD per MWh; 1 c/kWh = 10 USD/MWh.
    p_cents = allocation.p / 10.0
    pool = industrial_manufacturing_pool()
    industrial = [
        c for c in purchases.index if allocation.end_use_class.get(c) == 'Industrial'
    ]

    t_dollars = allocation.t_dollars.reindex(industrial)
    d_dollars = allocation.d_dollars.reindex(industrial)
    gen = allocation.gen_dollars.reindex(industrial)
    bill = purchases.reindex(industrial)
    mwh = allocation.mwh.reindex(industrial)

    table = pd.DataFrame(
        {
            'name': [_desc(str(c))[:38] for c in industrial],
            'is_manufacturing': [c in pool for c in industrial],
            'bill': bill,
            'mwh': mwh,
            'generation': gen,
            'td': t_dollars + d_dollars,
            'clipped': allocation.clipped.reindex(industrial).astype(bool),
        }
    )
    # The purchaser's own all-in price: USD/MWh / 10 = cents/kWh.
    table['price_cents_per_kwh'] = (
        table['bill'] / table['mwh'].replace(0.0, float('nan')) / 10.0
    )
    table['below_p'] = table['price_cents_per_kwh'] < p_cents
    table['td_share'] = table['td'] / table['bill'].replace(0.0, float('nan'))
    table['blended_ef'] = (
        gen * EF_GENERATION + t_dollars * EF_TRANSMISSION + d_dollars * EF_DISTRIBUTION
    ) / table['bill'].replace(0.0, float('nan'))
    return YearResult(table=table, p_cents_per_kwh=p_cents, vintage=vintage)


def summarize(results: dict[int, YearResult]) -> pd.DataFrame:
    """One row per year: the identity, the zero-T&D count, and the EF distortion."""
    rows = {}
    for year, result in results.items():
        live = result.table[result.table['bill'] > 0]
        clipped = live[live['clipped']]
        unclipped = live[~live['clipped']]
        zero_td = clipped[clipped['td'].abs() < _ZERO_TD_ATOL_USD]
        rows[year] = {
            'p_cents_per_kwh': result.p_cents_per_kwh,
            'purchasers': len(live),
            'clipped': len(clipped),
            'below_p': int(live['below_p'].sum()),
            'iff_agreement': int((live['clipped'] == live['below_p']).sum()),
            'clipped_zero_td': len(zero_td),
            'clipped_bill_bn': float(clipped['bill'].sum()) / 1e9,
            'clipped_bill_pct': float(clipped['bill'].sum())
            / float(live['bill'].sum())
            * 100,
            'p_percentile': float(live['below_p'].mean()) * 100,
            'median_td_share_unclipped': float(unclipped['td_share'].median()) * 100,
            'median_ef_unclipped': float(unclipped['blended_ef'].median()),
            'median_ef_clipped': (
                float(clipped['blended_ef'].median()) if len(clipped) else float('nan')
            ),
        }
    t = pd.DataFrame(rows).T
    t['ef_distortion_x'] = t['median_ef_clipped'] / t['median_ef_unclipped']
    return t


def growth_decomposition(results: dict[int, YearResult]) -> pd.DataFrame:
    """Why the capped count grows: the bar rising, or the distribution falling?

    A purchaser is capped when its own price is below ``p``, so the count can
    only grow because ``p`` rose or because own prices fell. Separate them by
    counterfactual, on the purchasers present in both the base year and year
    *t* so the changing sector list contributes nothing.
    """
    years = sorted(results)
    base = years[0]
    prices = {
        y: results[y].table.loc[results[y].table['bill'] > 0, 'price_cents_per_kwh']
        for y in years
    }
    p_of = {y: results[y].p_cents_per_kwh for y in years}

    rows = {}
    for year in years:
        common = prices[base].index.intersection(prices[year].index)
        then, now = prices[base].reindex(common), prices[year].reindex(common)
        baseline = int((then < p_of[base]).sum())
        rows[year] = {
            'p': p_of[year],
            'common_purchasers': len(common),
            'baseline': baseline,
            'actual': int((now < p_of[year]).sum()),
            'if_only_p_moved': int((then < p_of[year]).sum()),
            'if_only_prices_moved': int((now < p_of[base]).sum()),
            'p10_own_price': float(now.quantile(0.10)),
            'median_own_price': float(now.median()),
        }
    t = pd.DataFrame(rows).T
    t['growth'] = t['actual'] - t['baseline']
    t['from_p'] = t['if_only_p_moved'] - t['baseline']
    t['from_prices'] = t['if_only_prices_moved'] - t['baseline']
    t['interaction'] = t['growth'] - t['from_p'] - t['from_prices']
    return t


def run_checks(summary: pd.DataFrame) -> int:
    """The two claims the case rests on. Returns the failure count."""
    failures = 0

    disagreed = summary['purchasers'] - summary['iff_agreement']
    if float(disagreed.abs().max()) > 0:
        years = list(summary.index[disagreed > 0])
        print(
            'FAIL  clipping is not exactly "own price below p": '
            f'purchasers disagree in {years}'
        )
        failures += 1

    if not (summary['clipped'] == summary['clipped_zero_td']).all():
        print('FAIL  some clipped purchaser retains T&D; the zero-T&D claim is wrong')
        failures += 1

    print(f'check: {failures} failure(s) over {len(summary)} years')
    return failures


def _parse_years(spec: str) -> list[int]:
    lo, _, hi = spec.partition('-')
    return list(range(int(lo), int(hi or lo) + 1))


def _fmt(value: float) -> str:
    return f'{value:,.2f}'


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years', default='2017-2024')
    parser.add_argument('--top', type=int, default=12)
    parser.add_argument('--csv', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--mut-vintage', default=None)
    args = parser.parse_args()

    years = _parse_years(args.years)
    results = {y: allocate_year(y, args.mut_vintage) for y in years}
    summary = summarize(results)

    if args.check:
        raise SystemExit(1 if run_checks(summary) else 0)

    print('=== the cap is an identity: clipped == own price below p ===')
    print(
        summary[
            ['p_cents_per_kwh', 'purchasers', 'clipped', 'below_p', 'iff_agreement']
        ].to_string(float_format=_fmt)
    )

    print('\n=== every clipped purchaser is left with zero T&D ===')
    print(
        summary[
            [
                'clipped',
                'clipped_zero_td',
                'clipped_bill_bn',
                'clipped_bill_pct',
                'p_percentile',
                'median_td_share_unclipped',
            ]
        ].to_string(float_format=_fmt)
    )

    print('\n=== so their electricity emission factor is the pure generation one ===')
    print(
        summary[
            ['median_ef_unclipped', 'median_ef_clipped', 'ef_distortion_x']
        ].to_string(float_format=_fmt)
    )

    if len(years) > 1:
        growth = growth_decomposition(results)
        print('\n=== why the capped count grows: the bar, or the distribution? ===')
        print(
            growth[
                [
                    'p',
                    'common_purchasers',
                    'baseline',
                    'actual',
                    'if_only_p_moved',
                    'if_only_prices_moved',
                ]
            ].to_string(float_format=_fmt)
        )
        print(
            '\n  baseline             = base-year prices vs base-year p\n'
            '  if_only_p_moved      = base-year prices vs p(t)\n'
            '  if_only_prices_moved = prices(t) vs base-year p'
        )
        print('\n--- decomposition, and where the distribution went ---')
        print(
            growth[
                [
                    'growth',
                    'from_p',
                    'from_prices',
                    'interaction',
                    'p10_own_price',
                    'median_own_price',
                ]
            ].to_string(float_format=_fmt)
        )

    last = results[years[-1]]
    live = last.table[last.table['bill'] > 0]
    columns = ['name', 'bill', 'price_cents_per_kwh', 'generation', 'td', 'blended_ef']
    print(f'\n=== {years[-1]}: clipped purchasers, largest bills ===')
    clipped = live[live['clipped']].sort_values('bill', ascending=False)
    print(clipped.head(args.top)[columns].to_string(float_format=lambda v: f'{v:,.4g}'))
    print(f'\n=== {years[-1]}: the largest unclipped bills, for contrast ===')
    unclipped = live[~live['clipped']].sort_values('bill', ascending=False)
    print(
        unclipped.head(args.top // 2)[columns].to_string(
            float_format=lambda v: f'{v:,.4g}'
        )
    )

    if args.csv:
        summary_path = OUT_DIR / 'flat_price_clipping.csv'
        summary.to_csv(summary_path)
        print(f'\nwrote {summary_path}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    main()
