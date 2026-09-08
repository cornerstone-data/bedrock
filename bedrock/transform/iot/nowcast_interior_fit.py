"""Fit the Use interior to its two hard margins — Step 5's identity core as a
seed operation.

The precondition campaign (#776–#788, ``About_step5_preconditions.md``) left
one standing blocker: the 402 x 402 intermediate interior is a 2017-carried
seed whose row and column sums satisfy neither hard identity — 20.0% of
intermediate use on the supply-equals-use rows at 2023, 6.4% of intermediate
inputs on the worst industry-column year. Every *margin* is now anchored on
published 2017 detail, observed annually, or conditioned on the published
annual summary tables, so the identity targets themselves carry no known
concept errors. This module reconciles the interior to them:

- **row target**, per commodity: total supply at purchaser prices minus final
  demand — ``T016[c] − Σ Y[c, :]`` (the T11 identity solved for intermediate
  use);
- **column target**, per industry: gross output minus value added —
  ``GO[i] − VAPRO[i]`` (the T1/T18 identity solved for intermediate inputs).

⚠️ **``GO`` here is the census-adjusted panel** —
:func:`~bedrock.transform.iot.derived_intermediate_and_value_added.detail_gross_output_panel`
with ``ec_adjusted=True`` — which pays the T1 injection debt #779 recorded:
the balance's industry-output targets must carry the same 2022+ Economic
Census conditioning as every other consumer of industry output, or the fit
would pull the interior toward a series the rest of the build no longer uses.

What the fit is, and is not
---------------------------

One biproportional (IPF) pass over the whole interior: alternately scale rows
to their targets and columns to theirs, to an absolute tolerance, in the
style :func:`~bedrock.transform.iot.nowcast_supply_go_control.fit_group`
proved out. Multiplicative scaling **conserves each row's and column's
internal structure** — it moves levels, never relocates mass within a row —
which is exactly why upstream structure work (bl-young's S00300 use
distribution, #767) must land *before or with* this fit: the fit inherits
whatever within-row story the seed tells.

It is **not** the Step 5 balance. GRAS adds the mask tiers, sign locks and
economy-wide soft targets on top; this fit's product is a seed whose margins
already satisfy the two hard identities, so the balance starts inside its
feasible region instead of being handed a 20% correction to absorb into
whatever cells it likes.

The wedge, and who wins
-----------------------

The two target sets cannot both close exactly: economy-wide,
``Σ_c (T016 − ΣY)`` and ``Σ_i (GO − VAPRO)`` disagree by the net residual the
campaign left (small against ~17tn — the gross gaps were two-sided and mostly
cancel). An IPF needs one total, so **the column side wins**: ``GO`` is the
observed ``UGO305-A`` series and ``VAPRO`` is BEA's own published value added,
which the seed block is reconciled onto (#850). Row targets are scaled uniformly to the column
total and the wedge is reported per year, never hidden.

Guards — hold and report, never silently drop
---------------------------------------------

- a row (column) whose seed support is empty but whose target is nonzero
  cannot be scaled into existence: held at seed, target recorded unmet;
- a row (column) whose target and seed sum disagree in sign would flip every
  cell: held, recorded;
- zero cells stay zero (multiplication), so the 2017 sparsity — the implicit
  mask — survives untouched.

:func:`fit_report` carries the honesty metrics: the wedge, the held axes and
their unmet mass, the residual left on each margin, and how much interior
mass the fit moved (gross, and the top movers) — because a fit that closes
the identities by moving a third of the interior is a different claim than
one that closes them by moving 5%.

Run::

    uv run python -m bedrock.transform.iot.nowcast_interior_fit --years 2023
    uv run python -m bedrock.transform.iot.nowcast_interior_fit           # span
"""

from __future__ import annotations

import argparse
import dataclasses
import sys

import numpy as np
import pandas as pd

from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: Stop when the largest absolute miss on either margin is below this, in USD.
#: Absolute, not relative, for :func:`~bedrock.transform.iot.nowcast_supply_go_control.fit_group`'s
#: reason: targets span six orders of magnitude and a relative rule chases
#: float noise on the small ones.
TOLERANCE_USD = 1e6

#: IPF sweeps before giving up. The 402 x 402 problem converges in hundreds;
#: the cap is generous because held axes slow the exchange between the rest.
MAX_ITERATIONS = 20_000

#: Below this absolute seed mass ($) an axis is treated as unscalable: a zero
#: cannot be scaled up, and a ratio against dust is numerically meaningless.
EMPTY_AXIS_USD = 1e6

#: Years the fit runs for — bounded by the margins, all of which now cover
#: the span. ✅ 2024 joined once the AIES release of 2026-09-03 sourced the
#: trade and transport margins and F03000.
FIT_YEARS: tuple[int, ...] = tuple(range(2017, 2025))

#: How many support-infeasible axes the fit may relax (hold and report) before
#: giving up. Each relaxation is one axis whose target is unreachable on its
#: seed support — 2023's ``336111`` automobiles row (the #670 classification
#: residual) is the motivating case. Small on purpose: if many axes need
#: relaxing, the targets are wrong, not the support.
RELAXATION_BUDGET = 8


@dataclasses.dataclass(frozen=True)
class FitResult:
    """The fitted interior plus the honesty metrics, USD."""

    interior: pd.DataFrame
    row_targets: pd.Series
    column_targets: pd.Series
    #: Σ row targets − Σ column targets before the uniform row rescale.
    wedge_usd: float
    #: commodities held (empty or sign-conflicted), with their unmet target.
    held_rows: pd.Series
    #: industries held, with their unmet target.
    held_columns: pd.Series
    iterations: int
    #: largest absolute margin miss left on an *active* axis.
    residual_usd: float
    #: half the sum of |fitted − seed| — the mass the fit moved.
    moved_usd: float
    #: axes relaxed as support-infeasible (target unreachable on the seed's
    #: nonzero cells), with the unmet miss at relaxation. Distinct from the
    #: guard-held axes: these had support and a well-signed target, and the
    #: exchange still could not reach them.
    relaxed_rows: pd.Series
    relaxed_columns: pd.Series


def interior_row_targets(year: int) -> pd.Series:
    """Per-commodity intermediate-use target: ``T016 − Σ Y``, USD."""
    from bedrock.transform.iot.nowcast import (  # noqa: PLC0415
        derive_initial_supply_bridge,
        derive_initial_Y_pur,
    )

    commodities = pd.Index(USA_2017_COMMODITY_CODES, name='commodity')
    supply = derive_initial_supply_bridge(int(year), download_sources_ok=True)
    t016 = pd.to_numeric(supply['T016'], errors='coerce').reindex(commodities)
    if t016.isna().any():
        missing = sorted(t016.index[t016.isna()])
        raise ValueError(
            f'{year} T016 is unsourced on {len(missing)} commodities '
            f'(first: {missing[:5]}); the row targets need every supply column '
            f'component sourced for the year.'
        )
    y = derive_initial_Y_pur(int(year), download_sources_ok=True)
    final = y.sum(axis=1).reindex(commodities).fillna(0.0)
    return (t016 - final).rename('row_target')


def interior_column_targets(year: int) -> pd.Series:
    """Per-industry intermediate-input target: ``GO − VAPRO``, USD.

    ``GO`` is the census-adjusted output panel (the T1 injection, see the
    module docstring); ``VAPRO`` is the six-row value-added block summed with
    its stored signs (the subsidy rows are negative, so a plain sum is the
    identity).

    ⚠️ **This is only a well-posed target because the block is reconciled to
    BEA's published value added** by
    :func:`~bedrock.transform.iot.nowcast._reconcile_to_published_vapro`.
    Without it the block sums to a national control on a 2017 distribution,
    and the difference from gross output is not intermediate inputs but the
    accumulated allocation error: six industries below zero at 2024, and
    semiconductors 30% of its own output away (#850).
    """
    from bedrock.transform.iot.derived_intermediate_and_value_added import (  # noqa: PLC0415
        detail_gross_output_panel,
    )
    from bedrock.transform.iot.nowcast import (  # noqa: PLC0415
        derive_initial_value_added,
    )
    from bedrock.utils.economic.units import (  # noqa: PLC0415
        MILLION_CURRENCY_TO_CURRENCY,
    )

    industries = pd.Index(USA_2017_INDUSTRY_CODES, name='industry')
    # the panel is million USD; everything else in this module is USD
    panel = detail_gross_output_panel(ec_adjusted=True) * MILLION_CURRENCY_TO_CURRENCY
    go = (
        pd.to_numeric(panel[int(year)], errors='coerce').reindex(industries).fillna(0.0)
    )
    vapro = (
        derive_initial_value_added(int(year), download_sources_ok=True)
        .sum(axis=0)
        .reindex(industries)
        .fillna(0.0)
    )
    return (go - vapro).rename('column_target')


def refuse_negative_column_targets(targets: pd.Series, year: int) -> None:
    """Raise if any industry's intermediate-input target is below zero.

    A negative target is not a borderline case, it is an impossible one: value
    added would exceed gross output. Before #850 it passed silently into the
    fit, which drove the whole column to it and emptied the industry's input
    recipe - ``334413`` semiconductors reached an intermediate share of 0.002
    in the published A matrix against 0.242 in the 2017 benchmark, and the
    defect was read as a method win in the model comparison before it was
    traced. The cause was the value-added block failing to reconcile to
    published ``VAPRO``; this refuses the symptom so that any future
    recurrence fails here rather than several steps downstream.
    """
    negative = targets[targets < 0.0].sort_values()
    if negative.empty:
        return
    worst = ', '.join(
        f'{i} ({v / 1e9:,.1f}bn USD)' for i, v in negative.head(5).items()
    )
    raise ValueError(
        f'{year}: {len(negative)} industries have a negative intermediate-input '
        f'target (value added above gross output), which cannot happen: {worst}. '
        f'Both sides are controls - gross output from detail_gross_output_panel '
        f'and value added from derive_initial_value_added - so check that the '
        f'value-added block still reconciles to published VAPRO (#850).'
    )


def _held_axes(sums: pd.Series, targets: pd.Series) -> tuple[pd.Series, pd.Series]:
    """(active mask, held targets) for one axis under the guards."""
    empty = sums.abs() < EMPTY_AXIS_USD
    conflicted = ~empty & (np.sign(sums) != np.sign(targets)) & (targets != 0.0)
    held = empty & (targets.abs() >= EMPTY_AXIS_USD) | conflicted
    active = ~empty & ~conflicted
    return active, targets[held]


def fit_interior(
    year: int,
    seed: pd.DataFrame | None = None,
    tolerance_usd: float = TOLERANCE_USD,
    max_iterations: int = MAX_ITERATIONS,
) -> FitResult:
    """Fit the interior for *year* to both hard-identity margins.

    ``seed`` defaults to
    :func:`~bedrock.transform.iot.nowcast.derive_initial_U_intermediate`;
    passing one is for tests and for re-running on an upstream row rework
    (#767) without rebuilding.
    """
    if int(year) not in FIT_YEARS:
        raise ValueError(
            f'the interior fit runs for {FIT_YEARS[0]}-{FIT_YEARS[-1]}; got '
            f'{year}. The bound is the margins.'
        )
    if seed is None:
        from bedrock.transform.iot.nowcast import (  # noqa: PLC0415
            derive_initial_U_intermediate,
        )

        seed = derive_initial_U_intermediate(int(year))
    commodities = pd.Index(USA_2017_COMMODITY_CODES, name='commodity')
    industries = pd.Index(USA_2017_INDUSTRY_CODES, name='industry')
    matrix = (
        seed.reindex(index=commodities, columns=industries).fillna(0.0).astype(float)
    )

    row_targets = interior_row_targets(int(year))
    column_targets = interior_column_targets(int(year))
    refuse_negative_column_targets(column_targets, int(year))

    base_row_targets = row_targets.copy()
    row_active, held_rows = _held_axes(matrix.sum(axis=1), row_targets)
    col_active, held_columns = _held_axes(matrix.sum(axis=0), column_targets)
    relaxed_rows: dict[str, float] = {}
    relaxed_columns: dict[str, float] = {}

    fitted = matrix.to_numpy(copy=True)
    iterations = 0
    residual = np.inf
    wedge = 0.0
    for _relaxation in range(RELAXATION_BUDGET + 1):
        # The wedge: close the active row total onto the active column total —
        # the observed-GO side wins (module docstring). Held and relaxed axes
        # keep their seed mass, so the closure compares what the fit can reach.
        row_targets = base_row_targets.copy()
        active_row_total = float(row_targets[row_active].sum())
        held_row_seed = float(matrix.loc[~row_active].to_numpy().sum())
        held_col_seed = float(matrix.loc[:, ~col_active].to_numpy().sum())
        reachable_col_total = (
            float(column_targets[col_active].sum()) + held_col_seed - held_row_seed
        )
        wedge = active_row_total - reachable_col_total
        if active_row_total == 0.0:
            raise ValueError(f'{year}: no active rows to fit; the seed is empty.')
        row_targets[row_active] *= reachable_col_total / active_row_total

        fitted = matrix.to_numpy(copy=True)
        r_t = row_targets.to_numpy()
        c_t = column_targets.to_numpy()
        r_a = row_active.to_numpy()
        c_a = col_active.to_numpy()

        for iterations in range(1, max_iterations + 1):
            row_sums = fitted.sum(axis=1)
            with np.errstate(divide='ignore', invalid='ignore'):
                row_factor = np.where(r_a & (row_sums != 0.0), r_t / row_sums, 1.0)
            fitted *= row_factor[:, None]

            col_sums = fitted.sum(axis=0)
            with np.errstate(divide='ignore', invalid='ignore'):
                col_factor = np.where(c_a & (col_sums != 0.0), c_t / col_sums, 1.0)
            fitted *= col_factor[None, :]

            row_miss = np.abs(fitted.sum(axis=1) - r_t)[r_a].max(initial=0.0)
            col_miss = np.abs(fitted.sum(axis=0) - c_t)[c_a].max(initial=0.0)
            residual = float(max(row_miss, col_miss))
            if residual < tolerance_usd:
                break
        if residual < tolerance_usd:
            break

        # Non-convergence: the worst stuck axis is support-infeasible — its
        # target cannot be reached on its nonzero cells without breaking the
        # axes it shares them with. Relax it (hold at seed, record the miss)
        # and refit; if many need this, the targets are wrong, not the support.
        row_misses = pd.Series(np.abs(fitted.sum(axis=1) - r_t), index=commodities)
        row_misses[~row_active] = 0.0
        col_misses = pd.Series(np.abs(fitted.sum(axis=0) - c_t), index=industries)
        col_misses[~col_active] = 0.0
        if float(row_misses.max()) >= float(col_misses.max()):
            axis = str(row_misses.idxmax())
            relaxed_rows[axis] = float(row_misses.max())
            row_active = row_active & (row_active.index != axis)
        else:
            axis = str(col_misses.idxmax())
            relaxed_columns[axis] = float(col_misses.max())
            col_active = col_active & (col_active.index != axis)
    else:
        raise ValueError(
            f'{year}: the interior fit still had a residual of '
            f'{residual:,.0f} USD after relaxing {RELAXATION_BUDGET} '
            f'support-infeasible axes '
            f'(rows: {sorted(relaxed_rows)}; columns: '
            f'{sorted(relaxed_columns)}). That many infeasibilities means the '
            f'targets are wrong, not the support - stop and diagnose.'
        )

    result = pd.DataFrame(fitted, index=commodities, columns=industries)
    moved = float(np.abs(fitted - matrix.to_numpy()).sum()) / 2.0
    return FitResult(
        interior=result,
        row_targets=row_targets,
        column_targets=column_targets,
        wedge_usd=float(wedge),
        held_rows=held_rows,
        held_columns=held_columns,
        iterations=iterations,
        residual_usd=residual,
        moved_usd=moved,
        relaxed_rows=pd.Series(relaxed_rows, dtype=float).rename('relaxed_row'),
        relaxed_columns=pd.Series(relaxed_columns, dtype=float).rename(
            'relaxed_column'
        ),
    )


def fit_report(result: FitResult, year: int) -> str:
    """The honesty metrics as a printable block, million USD."""
    m = 1e6
    seed_total = float(result.interior.to_numpy().sum())
    movers = (
        (result.interior.sum(axis=1) - result.row_targets)
        .abs()
        .sort_values(ascending=False)
    )
    lines = [
        f'{year}: converged in {result.iterations} sweeps, residual '
        f'{result.residual_usd / m:,.2f} $M on {result.interior.shape[0]}x'
        f'{result.interior.shape[1]}',
        f'  interior total {seed_total / m:,.0f} $M | mass moved by the fit '
        f'{result.moved_usd / m:,.0f} $M '
        f'({100 * result.moved_usd / seed_total:.1f}% of the interior)',
        f'  row-vs-column wedge {result.wedge_usd / m:,.0f} $M '
        f'({100 * result.wedge_usd / seed_total:.2f}% of the interior) - '
        f'rows rescaled onto the observed-GO side',
        f'  held: {len(result.held_rows)} rows '
        f'({result.held_rows.abs().sum() / m:,.0f} $M unmet), '
        f'{len(result.held_columns)} columns '
        f'({result.held_columns.abs().sum() / m:,.0f} $M unmet)',
    ]
    if len(result.relaxed_rows) or len(result.relaxed_columns):
        relaxed = ', '.join(
            f'{axis} {miss / m:,.0f}M'
            for axis, miss in [
                *result.relaxed_rows.items(),
                *result.relaxed_columns.items(),
            ]
        )
        lines.append(f'  ⚠️ relaxed as support-infeasible: {relaxed}')
    if len(result.held_rows):
        worst = result.held_rows.abs().sort_values(ascending=False).head(5)
        lines.append(
            '  worst held rows: '
            + ', '.join(f'{c} {v / m:,.0f}' for c, v in worst.items())
        )
    _ = movers
    return '\n'.join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--years', type=int, nargs='*', default=list(FIT_YEARS))
    args = parser.parse_args(argv)
    for year in args.years:
        print(fit_report(fit_interior(int(year)), int(year)))
    return 0


if __name__ == '__main__':
    sys.exit(main())
