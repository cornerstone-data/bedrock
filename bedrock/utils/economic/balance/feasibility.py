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
not constrained by the mask.

**Mass is summed across a target's terms**, weighted by ``|coefficient|``. That
falls out of the linear-combination form and it is the right answer: for
``T016 = T019`` the free mass is the free mass on the Supply row *plus* the
free mass on the Use row, which is exactly ``mask_layer_plan.md`` §3's finding
that leverage has to be read across both tables. A commodity frozen on the Use
side is only stuck if its Supply side is frozen too, and the arithmetic now
says so without a special case.

**Two outcomes, and only one of them raises.**

- A **hard** target with a nonzero residual facing **zero free mass** is
  infeasible. There is no assignment of the free cells that satisfies it, so
  this raises rather than letting a solver converge to something meaningless.
  The same situation on a *soft* target reports as a warning instead: giving
  way is what soft means.
- High leverage **warns**. It is feasible but fragile, and it usually means the
  mask has quietly relocated the estimate somewhere else.

A margin that is entirely frozen but whose target the frozen mass *already
satisfies* is a no-op, not an error: the constraint is simply redundant.

**Placeholders never certify.** A target set carrying an unsourced value
(``PLACEHOLDER:``) will run, but :func:`precheck` refuses it unless
``allow_placeholders=True`` is passed explicitly. A shape-correct placeholder is
useful for building an engine against; it must not be mistaken for an estimate.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.offset import split_fixed_blocks
from bedrock.utils.economic.balance.targets import Target, TargetSet

# ``mask_layer_feasibility.py``'s threshold: the free cells must move 10% to
# deliver a 1% change in the target before this is worth flagging.
DEFAULT_LEVERAGE_WARN = 10.0

Severity = Literal['fatal', 'warning']
Blocks = Mapping[str, pd.DataFrame]
Masks = Mapping[str, SutMask]


class InfeasibleBalance(ValueError):
    """A target the mask has made unsatisfiable."""


class UnsourcedTargets(ValueError):
    """A target set still carrying placeholder values."""


@dataclass(frozen=True, eq=False)
class Infeasibility:
    """One margin that cannot, or can only barely, meet its target."""

    target: str
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
            f'[{self.severity}] {self.target} {self.label!r} ({self.kind}, '
            f'source {self.source}): residual target '
            f'{self.residual_target:,.0f} against free mass '
            f'{self.free_mass:,.0f} of {self.total_mass:,.0f} total '
            f'(leverage {self.leverage:,.1f})'
        )


def leverage(total_mass: np.ndarray, free_mass: np.ndarray) -> np.ndarray:
    """``|total| / |free|``, with an empty margin scored 1.0 rather than ``inf``.

    Matches ``mask_layer_feasibility._leverage`` in both formula **and
    argument order** so the two agree on the 2017 numbers. They previously took
    the same two arrays in opposite orders, which is a silent wrong answer when
    porting a check between them.
    """
    with np.errstate(divide='ignore', invalid='ignore'):
        lev = np.where(free_mass > 0, total_mass / free_mass, np.inf)
    return np.where(total_mass == 0, 1.0, lev)


def _mass(target: Target, blocks: Blocks) -> pd.Series:
    """Absolute mass a target's terms reach, summed with ``|coefficient|``."""
    total = pd.Series(0.0, index=target.values.index)
    for term in target.terms:
        # ``margin_of`` already applies the coefficient, and the margin of an
        # absolute frame is non-negative, so taking |·| here weights the term
        # by |coefficient| without a second multiply.
        contribution = term.margin_of(blocks[term.block].abs()).reindex(
            target.values.index
        )
        total = total + contribution.abs()
    return total


def _target_report(
    target: Target, seeds: Blocks, frozen: Blocks, free: Blocks
) -> pd.DataFrame:
    """Per-margin masses, leverage and residual target for one target."""
    report = pd.DataFrame(
        {
            'total_mass': _mass(target, seeds),
            'frozen_mass': _mass(target, frozen),
            'free_mass': _mass(target, free),
            'residual_target': target.residual_against(frozen),
        }
    )
    report['leverage'] = leverage(
        report['total_mass'].to_numpy(dtype=float),
        report['free_mass'].to_numpy(dtype=float),
    )
    report.insert(0, 'target', target.name or target.label)
    report.insert(1, 'blocks', '+'.join(target.blocks))
    report.insert(2, 'source', target.source)
    report.insert(3, 'hard', target.hard)
    report.insert(4, 'placeholder', target.is_placeholder)
    report.index.name = 'margin'
    return report


REPORT_COLUMNS = [
    'target',
    'blocks',
    'source',
    'hard',
    'placeholder',
    'total_mass',
    'frozen_mass',
    'free_mass',
    'residual_target',
    'leverage',
]


def margin_report(seeds: Blocks, masks: Masks, targets: TargetSet) -> pd.DataFrame:
    """Per-margin diagnostics for every target.

    One row per constrained margin, carrying frozen mass, free mass, leverage
    and the residual target. This is the report a failed balance is read
    against, so it is a public product rather than a by-product of
    :func:`precheck`.
    """
    frozen, free = split_fixed_blocks(seeds, masks)
    frames = [_target_report(t, seeds, frozen, free) for t in targets]
    if not frames:
        return pd.DataFrame(columns=REPORT_COLUMNS)
    return pd.concat(frames)


def precheck(
    seeds: Blocks,
    masks: Masks,
    targets: TargetSet,
    *,
    leverage_warn: float = DEFAULT_LEVERAGE_WARN,
    tol: float = 1e-6,
    raise_on_fatal: bool = True,
    allow_placeholders: bool = False,
) -> list[Infeasibility]:
    """Report every margin the mask has made infeasible or fragile.

    ``seeds`` are the **full** matrices, not the free parts: the precheck does
    its own split so it can report frozen and free mass side by side. Findings
    come back fatal-first, and by default a fatal one raises
    :class:`InfeasibleBalance` after the whole set has been collected - so a
    caller sees every problem at once rather than fixing them one run at a
    time.

    Set ``raise_on_fatal=False`` to collect findings without raising, which is
    what a diagnostics report wants.
    """
    unsourced = targets.placeholders
    if len(unsourced) and not allow_placeholders:
        named = ', '.join(t.label for t in unsourced)
        raise UnsourcedTargets(
            f'{len(unsourced)} target(s) still carry placeholder values and '
            f'cannot be certified: {named}. Pass allow_placeholders=True to '
            f'run anyway - a placeholder is shape-correct, not an estimate'
        )

    report = margin_report(seeds, masks, targets)
    findings: list[Infeasibility] = []
    for label, row in report.iterrows():
        residual = float(row['residual_target'])
        free_mass = float(row['free_mass'])
        lev = float(row['leverage'])
        stuck = free_mass <= tol and abs(residual) > tol
        fragile = np.isfinite(lev) and lev > leverage_warn
        if not stuck and not fragile:
            continue
        # Only a *hard* constraint with nowhere to move is fatal. A soft target
        # on a fully frozen margin is unsatisfiable too, but giving way is what
        # soft means - it should report, not block. Reachable today, because
        # every placeholder target is soft.
        is_fatal = stuck and bool(row['hard'])
        findings.append(
            Infeasibility(
                target=str(row['target']),
                label=str(label),
                severity='fatal' if is_fatal else 'warning',
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
