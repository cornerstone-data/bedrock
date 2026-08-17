"""The precheck: what the mask costs, before the balance runs.

Production form of ``mask_layer_feasibility.py``. A balance that cannot
succeed should say so up front and name the margin, because *"the balance did
not converge"* is not actionable and *"commodity 336112 has $1 of free mass
against a $439,551 row"* is.

**Leverage is the metric.** ::

    leverage = |margin total| / |free mass in the margin|

Leverage 1 means the free cells move 1% to deliver a 1% change in the target;
leverage 10 means they move 10%; ``inf`` means the margin cannot move at all.
An empty margin scores 1.0 rather than ``inf`` - a margin with nothing in it is
not constrained by the mask. Leverage is invisible in a cell count, which is
why a mask has to be measured in dollars: freezing the 2017 final-demand block
freezes 2.7% of the Use panel's nonzero cells and 39.9% of its dollars.

**Two outcomes, and only one of them raises.**

- A nonzero residual target facing **zero free mass** is infeasible. There is
  no assignment of the free cells that satisfies it, so this raises rather than
  letting a solver converge to something meaningless.
- High leverage **warns**. It is feasible but fragile, and it usually means the
  mask has quietly relocated the estimate somewhere else - on the 2017 Use
  panel, freezing final demand pushes a fifth of commodities onto the Supply
  table to close ``T016 = T019``. That may be the right modelling choice; it
  must not be an accidental one.

A margin that is entirely frozen but whose target the frozen mass *already
satisfies* is a no-op, not an error: the constraint is simply redundant.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.offset import margin, offset_target, split_fixed
from bedrock.utils.economic.balance.targets import Target, TargetSet

# ``mask_layer_feasibility.py``'s threshold: the free cells must move 10% to
# deliver a 1% change in the target before this is worth flagging.
DEFAULT_LEVERAGE_WARN = 10.0

Severity = Literal['fatal', 'warning']


class InfeasibleBalance(ValueError):
    """A target the mask has made unsatisfiable."""


@dataclass(frozen=True, eq=False)
class Infeasibility:
    """One margin that cannot, or can only barely, meet its target."""

    block: str
    axis: str
    label: str
    severity: Severity
    kind: str
    source: str
    residual_target: float
    total_mass: float
    frozen_mass: float
    free_mass: float
    leverage: float

    def describe(self) -> str:
        return (
            f'[{self.severity}] {self.block}.{self.axis} {self.label!r} '
            f'({self.kind}, source {self.source}): residual target '
            f'{self.residual_target:,.0f} against free mass '
            f'{self.free_mass:,.0f} of {self.total_mass:,.0f} total '
            f'(leverage {self.leverage:,.1f})'
        )


def leverage(total_mass: np.ndarray, free_mass: np.ndarray) -> np.ndarray:
    """``|total| / |free|``, with an empty margin scored 1.0 rather than ``inf``.

    Matches ``mask_layer_feasibility._leverage`` so the two agree on the 2017
    numbers.
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        lev = np.where(free_mass > 0, total_mass / free_mass, np.inf)
    return np.where(total_mass == 0, 1.0, lev)


def _target_report(
    target: Target,
    seed: pd.DataFrame,
    frozen: pd.DataFrame,
    free: pd.DataFrame,
) -> pd.DataFrame:
    """Per-margin masses, leverage and residual target for one target."""
    masses = {
        'total_mass': margin(seed.abs(), target.axis, target.restrict_to),
        'frozen_mass': margin(frozen.abs(), target.axis, target.restrict_to),
        'free_mass': margin(free.abs(), target.axis, target.restrict_to),
    }
    if target.aggregator is not None:
        masses = {k: target.aggregator.apply(v) for k, v in masses.items()}
    report = pd.DataFrame(masses).reindex(target.values.index)
    if report.isna().any().any():
        missing = list(report.index[report.isna().any(axis=1)])
        raise KeyError(
            f'{target.label} names margin labels the block does not have: ' f'{missing}'
        )
    report['residual_target'] = offset_target(target, frozen).values
    report['leverage'] = leverage(
        report['total_mass'].to_numpy(dtype=float),
        report['free_mass'].to_numpy(dtype=float),
    )
    report.insert(0, 'block', target.block)
    report.insert(1, 'axis', target.axis)
    report.insert(2, 'source', target.source)
    report.insert(3, 'hard', target.hard)
    report.index.name = 'margin'
    return report


def margin_report(
    seed: pd.DataFrame,
    mask: SutMask,
    targets: TargetSet,
    *,
    block: str | None = None,
) -> pd.DataFrame:
    """Per-margin diagnostics for every target on ``block``.

    One row per constrained margin, carrying frozen mass, free mass, leverage
    and the residual target. This is the report a failed balance is read
    against, so it is a public product rather than a by-product of
    :func:`precheck`.
    """
    selected = targets if block is None else targets.for_block(block)
    frozen, free = split_fixed(seed, mask)
    frames = [_target_report(t, seed, frozen, free) for t in selected]
    if not frames:
        return pd.DataFrame(
            columns=[
                'block',
                'axis',
                'source',
                'hard',
                'total_mass',
                'frozen_mass',
                'free_mass',
                'residual_target',
                'leverage',
            ]
        )
    return pd.concat(frames)


def precheck(
    seed: pd.DataFrame,
    mask: SutMask,
    targets: TargetSet,
    *,
    block: str | None = None,
    leverage_warn: float = DEFAULT_LEVERAGE_WARN,
    tol: float = 1e-6,
    raise_on_fatal: bool = True,
) -> list[Infeasibility]:
    """Report every margin the mask has made infeasible or fragile.

    ``seed`` is the **full** matrix, not the free part: the precheck does its
    own split so it can report frozen and free mass side by side. Findings come
    back fatal-first, and by default a fatal one raises
    :class:`InfeasibleBalance` after the whole set has been collected - so a
    caller sees every problem at once rather than fixing them one run at a
    time.

    Set ``raise_on_fatal=False`` to collect findings without raising, which is
    what a diagnostics report wants.
    """
    report = margin_report(seed, mask, targets, block=block)
    findings: list[Infeasibility] = []
    for label, row in report.iterrows():
        residual = float(row['residual_target'])
        free_mass = float(row['free_mass'])
        lev = float(row['leverage'])
        stuck = free_mass <= tol and abs(residual) > tol
        fragile = np.isfinite(lev) and lev > leverage_warn
        if not stuck and not fragile:
            continue
        findings.append(
            Infeasibility(
                block=str(row['block']),
                axis=str(row['axis']),
                label=str(label),
                severity='fatal' if stuck else 'warning',
                kind='no_free_mass' if stuck else 'high_leverage',
                source=str(row['source']),
                residual_target=residual,
                total_mass=float(row['total_mass']),
                frozen_mass=float(row['frozen_mass']),
                free_mass=free_mass,
                leverage=lev,
            )
        )
    findings.sort(key=lambda f: (f.severity != 'fatal', -f.leverage))
    fatal = [f for f in findings if f.severity == 'fatal']
    if fatal and raise_on_fatal:
        listed = '\n  '.join(f.describe() for f in fatal)
        raise InfeasibleBalance(
            f'{len(fatal)} margin(s) have a nonzero residual target and no free '
            f'mass to meet it:\n  {listed}'
        )
    return findings
