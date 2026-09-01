"""RAS prechecks: is a nowcast year's assembly fit to hand to the engine?

Runs the gates Wes named before the first real-year balance (2026-08-31):
the seed identities, the mask-versus-seed contradictions, the T1 injection
consistency, and the never-imported guard - each measured on the exact
objects :mod:`~bedrock.transform.iot.nowcast_sut_assembly` hands the engine,
so a PASS here means the engine's own refusals will not fire.

**Tolerances.** Exact matches are not expected - BEA publishes in $1M units
and every derivation sums rounded cells - so each gate carries a stated
bound rather than equality:

* ``IDENTITY_TOL_USD_M`` ($5M) for the cross-block identities T12-T14 and the
  margin nets T15/T16: the published 2017 tables themselves reproduce these
  only to publication rounding, and the derived series sum hundreds of
  rounded cells.
* ``CONSISTENCY_TOL_USD_M`` ($1M) for T18 and the fit-vs-T1 wiring, which are
  the same numbers reached by two code paths and must agree to the unit.
* Mask contradictions and never-imported mass use ``DUST_USD_M`` ($1M) - the
  assembly sweeps violations at or under it and this script prints the sweep;
  anything above survives the sweep and fails the gate.

T1, T11 and T17 seed residuals are **reported, not gated**: closing them is
what the balance is *for*.

From the repo root::

    uv run python bedrock/analysis/nowcasting/ras_prechecks.py --years 2018-2023
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

from bedrock.transform.iot.nowcast_interior_fit import (
    FIT_YEARS,
    interior_column_targets,
)
from bedrock.transform.iot.nowcast_mask import VA_ROWS, never_imported_violations
from bedrock.transform.iot.nowcast_sut_assembly import YearBalance, assemble
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY

IDENTITY_TOL_USD_M = 5.0
CONSISTENCY_TOL_USD_M = 1.0

#: Hard targets the seed must already satisfy (gated) versus the tensions the
#: balance exists to close (reported only).
GATED_IDENTITIES = ('T12', 'T13', 'T14', 'T15', 'T16')
REPORTED_ONLY = ('T1', 'T11', 'T17')


def _seed_target_residuals(balance: YearBalance) -> pd.DataFrame:
    rows = []
    for target in balance.targets:
        if not target.hard:
            continue
        err = (target.evaluate(balance.seeds) - target.values).abs()
        rows.append(
            {
                'target': target.name,
                'margins': len(err),
                'max_abs_residual': float(err.max()),
                'total_abs_residual': float(err.sum()),
            }
        )
    return pd.DataFrame(rows).set_index('target')


def _check_lines(balance: YearBalance) -> list[tuple[str, bool, str]]:
    """(name, passed, detail) triples for one year's gates."""
    checks: list[tuple[str, bool, str]] = []
    year = balance.year

    # 1. Mask contradictions: the sweep's survivors are hard stops.
    survivors = balance.sweep[~balance.sweep['swept']]
    dust = balance.sweep[balance.sweep['swept']]
    detail = (
        f'{len(dust)} dust cells swept '
        f'(total {dust["value_usd_m"].abs().sum():,.2f} $M)'
    )
    if len(survivors):
        worst = survivors.reindex(
            survivors['value_usd_m'].abs().sort_values(ascending=False).index
        ).head(5)
        cells = '; '.join(
            f'{r.block} {r.row} x {r.column} = {r.value_usd_m:,.1f} ({r.layer})'
            for r in worst.itertuples()
        )
        detail = f'{len(survivors)} above the sweep bound: {cells} | {detail}'
    checks.append(('mask contradictions', not len(survivors), detail))

    # 2. Seed identities, gated at the stated tolerance.
    residuals = _seed_target_residuals(balance)
    for name in GATED_IDENTITIES:
        if name not in residuals.index:
            checks.append((f'{name} on the seed', False, 'target missing'))
            continue
        worst_m = float(residuals['max_abs_residual'][name])
        checks.append(
            (
                f'{name} on the seed',
                worst_m <= IDENTITY_TOL_USD_M,
                f'max {worst_m:,.2f} $M (tol {IDENTITY_TOL_USD_M:g})',
            )
        )

    # 3. T18: injected from the seed's own VA block, so this is the same
    # number reached twice and must agree to the unit.
    t18_m = float(residuals['max_abs_residual']['T18'])
    checks.append(
        (
            'T18 injection agrees with the seed',
            t18_m <= CONSISTENCY_TOL_USD_M,
            f'max {t18_m:,.2f} $M',
        )
    )

    # 4. T1 wiring: the interior fit's column targets are USD; T1 minus the
    # seed's VA column sums is the same quantity in $M. A drift here means
    # the fit and the target set are on different gross-output arms (#724).
    t1 = next(t for t in balance.targets if t.name == 'T1')
    industries = t1.values.index
    fit_cols_m = (
        interior_column_targets(year).reindex(industries).fillna(0.0)
        / MILLION_CURRENCY_TO_CURRENCY
    )
    va_m = balance.seeds['use'].loc[list(VA_ROWS), industries].sum(axis=0)
    drift = ((t1.values - va_m) - fit_cols_m).abs()
    checks.append(
        (
            'T1 sits on the fit\'s gross-output arm',
            float(drift.max()) <= CONSISTENCY_TOL_USD_M,
            f'max |T1 - VA - fit column target| = {float(drift.max()):,.2f} $M',
        )
    )

    # 5. Never-imported commodities carry no import mass.
    mcif = balance.seeds['supply']['MCIF']
    offending = never_imported_violations(mcif)
    offending = offending[offending.abs() > CONSISTENCY_TOL_USD_M]
    detail = (
        'clean'
        if offending.empty
        else '; '.join(f'{k} = {v:,.1f} $M' for k, v in offending.head(5).items())
    )
    checks.append(('never-imported MCIF', offending.empty, detail))

    return checks


def run(years: range, *, fitted: bool = True) -> int:
    failed = 0
    for year in years:
        print(f'\n=== {year} ===')
        try:
            balance = assemble(year, fitted=fitted)
        except Exception as error:  # noqa: BLE001 - report and continue the span
            print(f'  FAIL  assembly - {type(error).__name__}: {error}')
            failed += 1
            continue
        for name, passed, detail in _check_lines(balance):
            print(f'  {"PASS" if passed else "FAIL"}  {name}  ({detail})')
            failed += not passed
        residuals = _seed_target_residuals(balance)
        print('  reported (the balance\'s job, not a gate):')
        for name in REPORTED_ONLY:
            if name in residuals.index:
                row = residuals.loc[name]
                print(
                    f'    {name}: max {row.max_abs_residual:,.0f} $M, '
                    f'total {row.total_abs_residual:,.0f} $M '
                    f'over {int(row.margins)} margins'
                )
    print(f'\n{"ALL PRECHECKS PASS" if not failed else f"{failed} CHECKS FAILED"}')
    return 1 if failed else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--years', default=f'{FIT_YEARS[0]}-{FIT_YEARS[-1]}', help='e.g. 2018-2023'
    )
    parser.add_argument(
        '--raw-interior',
        action='store_true',
        help='precheck the raw Step-3 interior instead of the fitted one',
    )
    args = parser.parse_args(argv)
    first, _, last = args.years.partition('-')
    return run(range(int(first), int(last or first) + 1), fitted=not args.raw_interior)


if __name__ == '__main__':
    sys.exit(main())
