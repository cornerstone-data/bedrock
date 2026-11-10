"""Electric power's value added on a consolidated basis, 2017 (#1012).

BEA's ``221100`` is every electric entity consolidated: sales between them cancel, so
output is what reaches outside buyers. FERC Form 1 (EIA Table 8.3) covers one group of
those entities, major investor-owned utilities, and its revenue and expenses include
sales and purchases between entities. This puts the two on one basis.

1. **Output.** Retail plus delivery revenue over all utilities in EIA-861 is the
   consolidated output. It is compared with BEA's gross output.
2. **Weight.** Investor-owned utilities' share of that output, not of FERC revenue, is
   the weight for BEA's totals.
3. **Intermediate purchases.** FERC's, less purchased power, against BEA's at that
   weight. FERC has no labor line, so investor-owned compensation is unobserved and is
   taken at the same weight (and, for comparison, at the revenue weight #1012 used).
4. **Non-labor value added.** FERC's taxes plus operating surplus (operating income
   plus depreciation) against BEA's value added less compensation.

``--fuel`` builds the v1.0 model for each year (about 4 minutes) and compares the fuel
rows of the electric column with EIA-923 delivered cost, and the model's CO2 with
EIA-923 heat times EPA factors.

::

    python -m bedrock.analysis.electricity.current.eia_gtd.consolidated_basis_1012
    # options: --check  --fuel  --year 2017
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd

logger = logging.getLogger(__name__)

#: BEA's published 2017 figures for ``221100``, $bn, as recorded in #1012. ``T1`` pins
#: intermediate plus value added to output, and ``T18`` pins value added.
BEA_OUTPUT = 389.4
BEA_VALUE_ADDED = 258.5
BEA_INTERMEDIATE = 131.0
BEA_COMPENSATION = 60.61
#: FERC revenue over BEA output, the weight #1012 first used.
REVENUE_WEIGHT = 0.676

INVESTOR_OWNED = 'Investor Owned'
#: EIA-861 revenue classes that reach outside buyers.
CONSOLIDATED_CLASSES = ('retail_sales', 'delivery_customers')

#: EIA Table 8.3 lines that make up electric expenses. They reconcile exactly.
FERC_HIERARCHY = (
    'expenses: Operation',
    'expenses: Maintenance',
    'expenses: Depreciation',
    'expenses: Taxes and Other',
)

#: kg CO2 per MMBtu, EPA stationary combustion.
KG_CO2_PER_MMBTU = {'coal': 95.0, 'gas': 53.06, 'oil': 74.0}
FUEL_ROWS = {'coal': '212100', 'gas': '211000', 'oil': '324110'}

#: Results shown as percentages.
PERCENT_KEYS = (
    'output_vs_bea',
    'investor_weight',
    'revenue_gap',
    'investor_share_of_non_labor',
)


def revenue_by_group(year: int) -> pd.DataFrame:
    """EIA-861 revenue by ownership type and class, $bn."""
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        RESALE_REVENUE,
        RESALE_VOLUME,
        _cached,
    )

    revenue = pd.read_parquet(_cached(RESALE_REVENUE))
    owners = pd.read_parquet(
        _cached(RESALE_VOLUME), columns=['report_date', 'utility_id_eia', 'entity_type']
    )
    for frame in (revenue, owners):
        frame['year'] = pd.to_datetime(frame['report_date']).dt.year
    owners = owners[owners['year'] == year].drop_duplicates('utility_id_eia')
    merged = revenue[revenue['year'] == year].merge(
        owners[['utility_id_eia', 'entity_type']], on='utility_id_eia', how='left'
    )
    table = merged.pivot_table(
        index='entity_type',
        columns='revenue_class',
        values='revenue',
        aggfunc='sum',
        observed=True,
    )
    return table / 1e9


def ferc(year: int) -> dict[str, float]:
    """FERC Form 1 lines for major investor-owned utilities, electric only, $bn."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        eia_table_8_3_line,
    )

    def line(name: str) -> float:
        return eia_table_8_3_line(year, name) / 1e9

    revenue = line('revenue: Electric Utility')
    expenses = line('expenses: Electric Utility')
    return {
        'revenue': revenue,
        'expenses': expenses,
        'hierarchy_gap': expenses - sum(line(n) for n in FERC_HIERARCHY),
        'purchased_power': line('expenses: Purchased Power'),
        'taxes': line('expenses: Taxes and Other'),
        'operating_surplus': revenue - expenses + line('expenses: Depreciation'),
    }


def ferc_intermediate(f: dict[str, float], compensation: float) -> float:
    """FERC's purchases, less purchased power: revenue less every value added line."""
    value_added = compensation + f['taxes'] + f['operating_surplus']
    return f['revenue'] - value_added - f['purchased_power']


def compare(year: int = 2017) -> dict[str, float]:
    """The consolidated-basis comparison. See the module docstring."""
    groups = revenue_by_group(year)
    outside = groups[list(CONSOLIDATED_CLASSES)].sum(axis=1)
    total_outside = float(outside.sum())
    weight = float(outside[INVESTOR_OWNED]) / total_outside
    f = ferc(year)
    non_labor_bea = BEA_VALUE_ADDED - BEA_COMPENSATION
    non_labor_ferc = f['taxes'] + f['operating_surplus']
    at_weight = ferc_intermediate(f, BEA_COMPENSATION * weight)
    at_revenue_weight = ferc_intermediate(f, BEA_COMPENSATION * REVENUE_WEIGHT)
    return {
        'consolidated_output_861': total_outside,
        'output_vs_bea': total_outside / BEA_OUTPUT - 1.0,
        'investor_output_861': float(outside[INVESTOR_OWNED]),
        'investor_weight': weight,
        'investor_revenue_861': float(groups.loc[INVESTOR_OWNED].sum()),
        'investor_revenue_ferc': f['revenue'],
        'revenue_gap': float(groups.loc[INVESTOR_OWNED].sum()) / f['revenue'] - 1.0,
        'bea_intermediate_at_weight': BEA_INTERMEDIATE * weight,
        'ferc_intermediate_comp_at_weight': at_weight,
        'gap_comp_at_weight': at_weight - BEA_INTERMEDIATE * weight,
        'ferc_intermediate_comp_at_revenue_weight': at_revenue_weight,
        'gap_comp_at_revenue_weight': at_revenue_weight - BEA_INTERMEDIATE * weight,
        'ferc_non_labor_value_added': non_labor_ferc,
        'bea_non_labor_value_added': non_labor_bea,
        'investor_share_of_non_labor': non_labor_ferc / non_labor_bea,
        'hierarchy_gap': f['hierarchy_gap'],
    }


def run_checks(result: dict[str, float]) -> list[str]:
    """Failures of the three reconciliations the comparison rests on."""
    failures = []
    if abs(result['output_vs_bea']) > 0.01:
        failures.append(
            f"EIA-861 retail plus delivery is {result['output_vs_bea']:+.1%} against "
            "BEA's gross output"
        )
    if abs(result['revenue_gap']) > 0.05:
        failures.append(
            f"investor-owned revenue in EIA-861 is {result['revenue_gap']:+.1%} "
            "against FERC's"
        )
    if abs(result['hierarchy_gap']) > 0.1:
        failures.append(
            f"FERC's electric expenses miss their lines by ${result['hierarchy_gap']:.2f}bn"
        )
    return failures


# --- the fuel rows, on the model -------------------------------------------------------


def eia923() -> tuple[pd.DataFrame, pd.DataFrame]:
    """EIA-923 delivered cost ($bn) and heat (million MMBtu) by year and fuel."""
    from bedrock.analysis.nowcasting.utilities_expense_seed import (  # noqa: PLC0415
        FUEL_RECEIPTS,
        _cached,
    )

    receipts = pd.read_parquet(
        _cached(FUEL_RECEIPTS),
        columns=[
            'report_date',
            'fuel_type_code_pudl',
            'total_fuel_cost',
            'fuel_received_units',
            'fuel_mmbtu_per_unit',
        ],
    )
    receipts['year'] = pd.to_datetime(receipts['report_date']).dt.year
    receipts['mmbtu'] = (
        receipts['fuel_received_units'] * receipts['fuel_mmbtu_per_unit']
    )
    receipts = receipts[receipts['fuel_type_code_pudl'].isin(FUEL_ROWS)]
    cost, heat = (
        receipts.pivot_table(
            index='year',
            columns='fuel_type_code_pudl',
            values=value,
            aggfunc='sum',
            observed=True,
        )
        for value in ('total_fuel_cost', 'mmbtu')
    )
    return cost / 1e9, heat / 1e6


def model_year(year: int, mut_vintage: str) -> dict[str, float]:
    """The electric column's fuel rows ($bn) and CO2 (Mt) in the v1.0 model for *year*."""
    from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (  # noqa: PLC0415
        CACHE_BEARING_MODULES,
        CONFIG_TEMPLATE,
    )
    from bedrock.transform.allocation.derived import derive_E_usa  # noqa: PLC0415
    from bedrock.transform.eeio import electricity_delivery as ed  # noqa: PLC0415
    from bedrock.utils.config.config_controllers import temp_usa_config  # noqa: PLC0415

    with temp_usa_config(
        CONFIG_TEMPLATE.format(year=year),
        cache_bearing_modules=(
            *CACHE_BEARING_MODULES,
            'bedrock.transform.eeio.derived',
        ),
        nowcast_mut_vintage=mut_vintage,
        electricity_delivery_margin=True,
    ):
        _, udom, uimp, *_ = ed.restructured_tables()
        use = pd.Series(udom[ed.GENERATION_SECTOR] + uimp[ed.GENERATION_SECTOR])
        emissions = derive_E_usa()
        out = {f'use_{k}': float(use[row]) / 1e9 for k, row in FUEL_ROWS.items()}
        out['model_co2_mt'] = (
            float(pd.Series(emissions.loc['CO2'], dtype=float)[ed.GENERATION_SECTOR])
            / 1e9
        )
        return out


def fuel_report(years: list[int]) -> None:
    """Print the fuel rows against EIA-923, their movement since 2017, and CO2."""
    from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (  # noqa: PLC0415
        resolve_vintages,
    )

    cost, heat = eia923()
    mut = resolve_vintages(years=tuple(years)).mut
    model = pd.DataFrame({y: model_year(y, mut) for y in years}).T
    levels = pd.DataFrame(index=years)
    for fuel in FUEL_ROWS:
        levels[f'{fuel}_model'] = model[f'use_{fuel}']
        levels[f'{fuel}_eia923'] = cost.reindex(years)[fuel]
        levels[f'{fuel}_ratio'] = levels[f'{fuel}_model'] / levels[f'{fuel}_eia923']
    print(
        '=== fuel rows of the electric column, $bn, against EIA-923 delivered cost ==='
    )
    print(levels.round(2).to_string())
    moves = pd.DataFrame(index=years)
    for fuel in FUEL_ROWS:
        moves[f'{fuel}_model_x{years[0]}'] = (
            levels[f'{fuel}_model'] / levels[f'{fuel}_model'].iloc[0]
        )
        moves[f'{fuel}_eia923_x{years[0]}'] = (
            levels[f'{fuel}_eia923'] / levels[f'{fuel}_eia923'].iloc[0]
        )
    print(f'\n=== movement since {years[0]} ===')
    print(moves.round(2).to_string())
    implied = sum(heat.reindex(years)[k] * KG_CO2_PER_MMBTU[k] for k in FUEL_ROWS) / 1e3
    co2 = pd.DataFrame(
        {'model_Mt': model['model_co2_mt'], 'eia923_implied_Mt': implied}
    )
    co2['ratio'] = co2['model_Mt'] / co2['eia923_implied_Mt']
    print('\n=== CO2 of the electric column against EIA-923 heat times EPA factors ===')
    print(co2.round(3).to_string())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--fuel', action='store_true')
    parser.add_argument('--year', type=int, default=2017)
    args = parser.parse_args()

    result = compare(args.year)
    print(f'=== {args.year}, $bn ===')
    for key, value in result.items():
        percent = key in PERCENT_KEYS
        print(f'{key:44s} {value:.1%}' if percent else f'{key:44s} {value:,.2f}')
    failures = run_checks(result)
    for failure in failures:
        print(f'FAIL  {failure}')
    if args.fuel:
        fuel_report(list(range(2017, 2025)))
    if args.check:
        print(f'check: {len(failures)} failure(s)')
        raise SystemExit(1 if failures else 0)


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING, format='%(message)s')
    main()
