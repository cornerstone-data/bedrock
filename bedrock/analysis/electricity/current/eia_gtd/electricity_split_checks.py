"""Checks on the electric power split into ``221100`` generation and ``42ELEC`` delivery (#903).

For each year the split is built from the 2026 v1.0 config with
``electricity_delivery_margin`` on, and the same year with it off. Every check reports per
year, and a failing check names the buyers or sectors responsible.

1. **Class kWh equal EIA's.** Residential, commercial, industrial and transportation kWh
   each equal EIA Table 2.2 sales. The regression test against v1, whose residential came
   out 0.532 times EIA's.
2. **Generation plus delivery equals each buyer's bill.**
3. **No unexplained negative delivery.** A buyer priced below generation has delivery of
   zero and is reported as floored, not as a failure.
4. **Emissions are conserved**, and generation per kWh is the same for every buyer that
   isn't floored. Total direct emissions with the flag on equal those with it off.
5. **Output is conserved.** Generation plus delivery, less the line losses delivery buys
   from generation, equals electric power's output before the split.
6. **A spend-based electricity factor** for the average buyer is within
   :data:`SPEND_TOLERANCE` of today's ``221100`` factor. The 10% is chosen here, not taken
   from #1218, which says "about the same". Factors by class are printed; no upper bound
   by class is set, because none has been defined.
7. **The kWh-weighted industrial price** is within :data:`PRICE_TOLERANCE` of EIA's
   industrial price, and the manufacturing price equals MECS exactly at the 2018 and 2022
   surveys.
8. **Prices don't depend on the bills.** Doubling every bill leaves buyer prices, class kWh
   and the generation price unchanged.

Checks 5 and 6, and the emissions half of 4, build the full model twice per year.
``--skip-model`` leaves them out.

::

    python -m bedrock.analysis.electricity.current.eia_gtd.electricity_split_checks
    # options: --years 2017-2024  --skip-model  --check  --mut-vintage v1.0.0_5514c3b
"""

from __future__ import annotations

import argparse
import logging
import typing as ta
from dataclasses import dataclass, field

import pandas as pd

from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (
    CACHE_BEARING_MODULES as _CACHE_BASE,
)

logger = logging.getLogger(__name__)

#: Modules whose caches hold the model for one config. ``derived`` caches B and Aq, which
#: would otherwise carry the flag-on factors into the flag-off build.
CACHE_MODULES = (*_CACHE_BASE, 'bedrock.transform.eeio.derived')

#: Dollar tolerance for identities on bills.
USD_ATOL = 1.0
#: Relative tolerance for identities that should hold exactly.
REL_TOL = 1e-9
#: How far the average buyer's factor may sit from today's ``221100`` factor.
SPEND_TOLERANCE = 0.10
#: How far the industrial price may sit from EIA's.
PRICE_TOLERANCE = 0.01

CHECKS = (
    'class_kwh',
    'bill_identity',
    'negative_delivery',
    'emissions',
    'output',
    'spend_factor',
    'prices',
    'bills_independent',
)


@dataclass
class Report:
    """Failures by check and year. A check that didn't run has no entry in ``ran``."""

    failures: dict[tuple[str, int], list[str]] = field(default_factory=dict)
    ran: set[tuple[str, int]] = field(default_factory=set)

    def record(self, check: str, year: int, problems: ta.Sequence[str]) -> None:
        self.ran.add((check, year))
        if problems:
            self.failures[(check, year)] = list(problems)

    def table(self, years: ta.Sequence[int]) -> pd.DataFrame:
        def cell(check: str, year: int) -> str:
            if (check, year) not in self.ran:
                return '-'
            return 'FAIL' if (check, year) in self.failures else 'ok'

        return pd.DataFrame(
            {y: [cell(c, y) for c in CHECKS] for y in years}, index=pd.Index(CHECKS)
        )


def _names(index: ta.Iterable[ta.Any], limit: int = 8) -> str:
    codes = [str(c) for c in index]
    extra = f' and {len(codes) - limit} more' if len(codes) > limit else ''
    return ', '.join(codes[:limit]) + extra


# --- checks that need only a split ---------------------------------------------------


def check_class_kwh(frame: pd.DataFrame, sales: ta.Mapping[str, float]) -> list[str]:
    """Check 1: each scaled class's kWh against EIA's sales."""
    from bedrock.transform.eeio.electricity_delivery import (  # noqa: PLC0415
        SCALED_CLASSES,
    )

    problems = []
    for cls in SCALED_CLASSES:
        ours = float(frame.loc[frame['class'] == cls, 'kwh'].sum()) / 1000.0
        ratio = ours / float(sales[cls])
        if abs(ratio - 1.0) > 1e-6:
            members = frame.index[frame['class'] == cls]
            problems.append(f'{cls} kWh are {ratio:.4f} times EIA: {_names(members)}')
    return problems


def check_bill_identity(frame: pd.DataFrame) -> list[str]:
    """Check 2: generation plus delivery equals the bill."""
    gap = (frame['gen'] + frame['delivery'] - frame['bill']).abs()
    off = gap[gap > USD_ATOL]
    if len(off) == 0:
        return []
    return [f'generation plus delivery differs from the bill: {_names(off.index)}']


def check_negative_delivery(frame: pd.DataFrame) -> list[str]:
    """Check 3: delivery below zero, which a floored buyer doesn't have."""
    negative = frame.index[frame['delivery'] < -USD_ATOL]
    if len(negative) == 0:
        return []
    return [f'negative delivery: {_names(negative)}']


def check_generation_uniform(split: ta.Any) -> list[str]:
    """Check 4, second half: generation per kWh is one price for every buyer not floored."""
    from bedrock.transform.eeio.electricity_delivery import OWN_USE  # noqa: PLC0415

    frame = split.buyers
    live = frame[(~frame['floored']) & (frame['class'] != OWN_USE) & (frame['kwh'] > 0)]
    per_kwh = live['gen'] / live['kwh'] * 100.0
    off = per_kwh[(per_kwh / split.generation_price - 1.0).abs() > 1e-6]
    if len(off) == 0:
        return []
    return [
        f'generation per kWh is not {split.generation_price:.3f} c/kWh: {_names(off.index)}'
    ]


def check_bills_independent(
    frame: pd.DataFrame, split_with: ta.Callable[[pd.Series], ta.Any]
) -> list[str]:
    """Check 8: doubling every bill moves neither prices, class kWh nor the generation price."""
    doubled = split_with(frame['bill'] * 2.0)
    base = split_with(frame['bill'])
    problems = []
    if abs(doubled.generation_price - base.generation_price) > 0:
        problems.append('the generation price moves with the bills')
    before, after = base.buyers['price'].fillna(-1.0), doubled.buyers['price'].fillna(
        -1.0
    )
    if not (before == after).all():
        moved = after.index[before != after]
        problems.append(f'buyer prices move with the bills: {_names(moved)}')
    scaled = base.buyers['class'].isin(
        ['Residential', 'Commercial', 'Industrial', 'Transportation']
    )
    class_kwh = base.buyers[scaled].groupby('class')['kwh'].sum()
    class_kwh2 = doubled.buyers[scaled].groupby('class')['kwh'].sum()
    drift = ((class_kwh2 / class_kwh - 1.0).abs() > 1e-9).pipe(lambda s: s.index[s])
    if len(drift) > 0:
        problems.append(f'class kWh move with the bills: {_names(drift)}')
    return problems


def check_prices(year: int, split: ta.Any) -> tuple[list[str], dict[str, float]]:
    """Check 7: the industrial price against EIA's, and manufacturing against MECS at anchors."""
    from bedrock.transform.eeio import electricity_price as ep  # noqa: PLC0415

    problems = []
    frame = split.buyers
    members = frame[frame['class'] == 'Industrial']
    weighted = float((members['price'] * members['kwh']).sum() / members['kwh'].sum())
    eia = ep.eia_class_price(year, 'Industrial')
    gap = weighted / eia - 1.0
    if abs(gap) > PRICE_TOLERANCE:
        problems.append(
            f'kWh-weighted industrial price {weighted:.3f} c/kWh is {gap:+.1%} against '
            f'EIA {eia:.3f}'
        )
    if year in ep.SURVEYS:
        mecs = ep.survey_prices(ep.mecs_table(year), ep.naics_by_industry())['price']
        ours = ep.manufacturing_price(year).reindex(mecs.index)
        gap_by_industry = ours / mecs - 1.0
        off = gap_by_industry[gap_by_industry.abs() > 1e-6]
        if len(off) > 0:
            problems.append(
                f'differs from MECS {year} in {len(off)} industries (median '
                f'{off.median():+.1%}, largest {off.abs().max():.1%}): {_names(off.index)}'
            )
    return problems, {'weighted_c_kwh': weighted, 'eia_c_kwh': eia, 'gap': gap}


# --- checks that need the model ------------------------------------------------------


@dataclass(frozen=True)
class ModelFacts:
    """What the flag changes in one year's model."""

    n_generation: float
    n_delivery: float
    n_today: float
    emissions_on: float
    emissions_off: float
    output_before: float
    output_after: float
    losses_usd: float


def _vector_n() -> 'pd.Series[float]':
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa,
    )
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
    )

    aq = derive_Aq_usa()
    return compute_n(
        M=compute_M_matrix(B=derive_B_usa(), L=compute_L_matrix(A=aq.Adom + aq.Aimp))
    )


def _emissions_total() -> float:
    from bedrock.transform.allocation.derived import derive_E_usa  # noqa: PLC0415

    return float(derive_E_usa().to_numpy().sum())


def model_facts(year: int, mut_vintage: str | None) -> tuple[ta.Any, ModelFacts]:
    """The split and model facts for *year*, with the flag on and then off."""
    from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (  # noqa: PLC0415
        CONFIG_TEMPLATE,
        resolve_vintages,
    )
    from bedrock.transform.eeio import electricity_delivery as ed  # noqa: PLC0415
    from bedrock.transform.eeio import sector_disagg_pipeline as pipe  # noqa: PLC0415
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        reallocate_electricity_coproduction,
    )
    from bedrock.utils.config.config_controllers import temp_usa_config  # noqa: PLC0415

    vintage = mut_vintage or resolve_vintages(years=(year,)).mut

    def build(flag: bool) -> dict[str, ta.Any]:
        with temp_usa_config(
            CONFIG_TEMPLATE.format(year=year),
            cache_bearing_modules=CACHE_MODULES,
            nowcast_mut_vintage=vintage,
            electricity_delivery_margin=flag,
        ):
            n = _vector_n()
            out: dict[str, ta.Any] = {
                'n': n,
                'emissions': _emissions_total(),
            }
            if flag:
                v, udom, uimp, va = reallocate_electricity_coproduction(
                    pipe.derive_V_after_waste(),
                    *pipe.derive_U_after_waste(),
                    pipe.derive_VA_after_waste(),
                )
                gen, slot = ed.GENERATION_SECTOR, ed.DELIVERY_SLOT
                out['before'] = float(pd.Series(v.loc[gen], dtype=float)[gen])
                split = ed.active_delivery_split()
                v_after = ed.restructured_tables()[0]
                out['split'] = split
                out['after'] = float(
                    pd.Series(v_after.loc[gen], dtype=float)[gen]
                    + pd.Series(v_after.loc[slot], dtype=float)[slot]
                )
            return out

    on, off = build(True), build(False)
    split = on['split']
    facts = ModelFacts(
        n_generation=float(on['n'][ed.GENERATION_SECTOR]),
        n_delivery=float(on['n'][ed.DELIVERY_SLOT]),
        n_today=float(off['n'][ed.GENERATION_SECTOR]),
        emissions_on=on['emissions'],
        emissions_off=off['emissions'],
        output_before=on['before'],
        output_after=on['after'],
        losses_usd=split.losses_usd,
    )
    return split, facts


def check_emissions(facts: ModelFacts) -> list[str]:
    """Check 4, first half: total direct emissions with the flag on and off."""
    if abs(facts.emissions_on / facts.emissions_off - 1.0) > REL_TOL:
        return [
            f'total direct emissions {facts.emissions_on:,.0f} with the flag, '
            f'{facts.emissions_off:,.0f} without'
        ]
    return []


def check_output(facts: ModelFacts) -> list[str]:
    """Check 5: generation plus delivery, less losses, equals electric power before the split."""
    after = facts.output_after - facts.losses_usd
    if abs(after - facts.output_before) > max(USD_ATOL, REL_TOL * facts.output_before):
        return [
            f'generation plus delivery less losses is ${after / 1e9:,.3f}bn, '
            f'electric power before the split ${facts.output_before / 1e9:,.3f}bn'
        ]
    return []


def spend_factor(
    split: ta.Any, facts: ModelFacts
) -> tuple[float, pd.Series, list[str]]:
    """Check 6: the average buyer's purchaser-price factor, and the factor by class."""
    from bedrock.transform.eeio.electricity_delivery import OWN_USE  # noqa: PLC0415

    buyers = split.buyers[split.buyers['class'] != OWN_USE]
    by_class = buyers.groupby('class')[['gen', 'delivery', 'bill']].sum()
    factor = (
        facts.n_generation * by_class['gen'] + facts.n_delivery * by_class['delivery']
    ) / by_class['bill']
    total = by_class.sum()
    average = float(
        (facts.n_generation * total['gen'] + facts.n_delivery * total['delivery'])
        / total['bill']
    )
    gap = average / facts.n_today - 1.0
    problems = []
    if abs(gap) > SPEND_TOLERANCE:
        problems.append(
            f'average buyer {average:.2f} kg/$ is {gap:+.1%} against today\'s '
            f'{facts.n_today:.2f}'
        )
    return average, pd.Series(factor, dtype=float), problems


# --- driver --------------------------------------------------------------------------


def _parse_years(text: str) -> list[int]:
    lo, _, hi = text.partition('-')
    return list(range(int(lo), int(hi or lo) + 1))


def run_year(
    year: int, report: Report, skip_model: bool, mut_vintage: str | None
) -> dict[str, ta.Any]:
    """Every check for *year*. Returns figures to print."""
    from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (  # noqa: PLC0415
        CONFIG_TEMPLATE,
        resolve_vintages,
    )
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_table_2_2_end_use_mwh,
    )
    from bedrock.transform.eeio import electricity_delivery as ed  # noqa: PLC0415
    from bedrock.utils.config.config_controllers import temp_usa_config  # noqa: PLC0415

    figures: dict[str, ta.Any] = {}
    facts = None
    if skip_model:
        vintage = mut_vintage or resolve_vintages(years=(year,)).mut
        with temp_usa_config(
            CONFIG_TEMPLATE.format(year=year),
            cache_bearing_modules=CACHE_MODULES,
            nowcast_mut_vintage=vintage,
            electricity_delivery_margin=True,
        ):
            split = ed.active_delivery_split()
    else:
        split, facts = model_facts(year, mut_vintage)

    frame = split.buyers
    sales = eia_table_2_2_end_use_mwh(year)
    report.record('class_kwh', year, check_class_kwh(frame, sales))
    report.record('bill_identity', year, check_bill_identity(frame))
    report.record('negative_delivery', year, check_negative_delivery(frame))
    uniform = check_generation_uniform(split)
    report.record(
        'bills_independent',
        year,
        check_bills_independent(
            frame,
            lambda bills: _resplit(year, bills, frame, split),
        ),
    )
    problems, price_figures = check_prices(year, split)
    report.record('prices', year, problems)
    figures.update(price_figures)
    figures['floored'] = int(frame['floored'].sum())
    figures['floored_bill_bn'] = float(frame.loc[frame['floored'], 'bill'].sum()) / 1e9

    if facts is None:
        report.record('emissions', year, uniform)
        return figures
    report.record('emissions', year, [*check_emissions(facts), *uniform])
    report.record('output', year, check_output(facts))
    average, by_class, spend_problems = spend_factor(split, facts)
    report.record('spend_factor', year, spend_problems)
    figures.update(
        average_factor=average,
        n_today=facts.n_today,
        n_generation=facts.n_generation,
        n_delivery=facts.n_delivery,
    )
    figures['by_class'] = by_class
    return figures


def _resplit(year: int, bills: pd.Series, frame: pd.DataFrame, split: ta.Any) -> ta.Any:
    """The split of *bills* with the same classes, prices and EIA totals as *split*."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_table_2_2_end_use_mwh,
        eia_table_2_14_export_mwh,
        eia_table_2_14_year_for_egrid_year,
    )
    from bedrock.transform.eeio import electricity_delivery as ed  # noqa: PLC0415

    sales = eia_table_2_2_end_use_mwh(year)
    export_mwh = eia_table_2_14_export_mwh(eia_table_2_14_year_for_egrid_year(year))
    return ed.split_bills(
        bills,
        frame['class'],
        pd.Series(frame['price'], dtype=float).dropna(),
        {cls: float(sales[cls]) for cls in ed.SCALED_CLASSES},
        split.generation_price,
        export_mwh,
        split.losses_mwh,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--years', default='2017-2024')
    parser.add_argument('--skip-model', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--mut-vintage', default=None)
    args = parser.parse_args()

    years = _parse_years(args.years)
    report = Report()
    figures = {}
    for year in years:
        logger.warning('--- %d ---', year)
        figures[year] = run_year(year, report, args.skip_model, args.mut_vintage)

    print('=== checks by year ===')
    print(report.table(years).to_string())
    prices = pd.DataFrame(
        {
            y: {
                k: f[k]
                for k in (
                    'weighted_c_kwh',
                    'eia_c_kwh',
                    'gap',
                    'floored',
                    'floored_bill_bn',
                )
            }
            for y, f in figures.items()
        }
    ).T
    print('\n=== industrial price against EIA, and floored buyers ===')
    print(prices.to_string(float_format=lambda v: f'{v:,.3f}'))
    if not args.skip_model:
        factors = pd.DataFrame(
            {
                y: {
                    'average': f['average_factor'],
                    'today': f['n_today'],
                    'generation': f['n_generation'],
                    'delivery': f['n_delivery'],
                    **f['by_class'].to_dict(),
                }
                for y, f in figures.items()
            }
        ).T
        print('\n=== purchaser-price electricity factor, kg CO2e per $ ===')
        print(factors.to_string(float_format=lambda v: f'{v:,.2f}'))

    for (check, year), problems in sorted(report.failures.items()):
        for problem in problems:
            print(f'FAIL  {check} {year}: {problem}')
    if args.check:
        print(
            f'check: {len(report.failures)} failing check-years over {len(years)} years'
        )
        raise SystemExit(1 if report.failures else 0)


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    main()
