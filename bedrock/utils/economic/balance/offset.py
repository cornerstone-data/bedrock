"""The offset method: a fixed-value mask, engine-agnostically.

Neither candidate engine can hold a cell at a nonzero value. Both can exclude a
cell from participating. The offset method turns the first into the second, in
about twenty lines, so the mask stops being a reason to prefer one engine over
the other (``mask_layer_plan.md`` §2)::

    X  = F + Z            F zero off the mask, Z zero on it
    r' = r - F @ 1        row targets, less the frozen row mass
    c' = c - 1ᵀ @ F       column targets, less the frozen column mass
    A' = A - R @ F @ Cᵀ   and the same for aggregate-level targets

Balance ``Z`` against the residual targets, then add ``F`` back. The engine only
ever sees a participation mask.

**Three properties, all learned the hard way.**

- **A fixed cell is held at its value, not zeroed.** ceda's ``free_mask`` does
  ``np.where(mask, matrix, 0.0)`` and loses the value entirely.
- **Targets keep their sign.** Subtracting frozen mass can carry a positive
  target across zero, so a residual target is a different object from the
  published one: :meth:`~.targets.Target.with_values` permits negatives on the
  residual for exactly this reason. ``F03000`` is negative outright in 2020
  before any offsetting happens.
- **``F`` is excluded from the seed, not merely flagged.** Passing the full
  matrix *and* the full targets double-counts the frozen mass, which is a
  silent wrong answer rather than an error - :func:`assert_free_seed` is the
  guard, and it is cheap enough to call before every balance.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.targets import Axis, Target, TargetSet


def margin(
    frame: pd.DataFrame,
    axis: Axis,
    restrict_to: tuple[str, ...] | None = None,
) -> pd.Series:
    """The row or column margin of ``frame``.

    ``axis='row'`` sums across columns and yields one value per row;
    ``axis='column'`` sums down rows and yields one per column. ``restrict_to``
    narrows the *summed* axis - with ``axis='column'`` it selects which rows
    participate - which is how a single row's cells can be constrained by
    column group.
    """
    if axis == 'row':
        selected = frame if restrict_to is None else frame.loc[:, list(restrict_to)]
        return selected.astype(float).sum(axis=1)
    if axis == 'column':
        selected = frame if restrict_to is None else frame.loc[list(restrict_to)]
        return selected.astype(float).sum(axis=0)
    raise ValueError(f'axis must be row or column, got {axis!r}')


def split_fixed(seed: pd.DataFrame, mask: SutMask) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split ``seed`` into its fixed part ``F`` and its free part ``Z``.

    ``F`` carries the seed's values on the fixed cells and zero elsewhere; ``Z``
    is the complement. ``F + Z == seed`` exactly. The seed is validated against
    the mask first, so a seed that contradicts its own structural zeros fails
    here rather than producing a quietly wrong balance.
    """
    mask.validate_against(seed)
    fixed = mask.fixed_value
    values = seed.astype(float)
    frozen = values.where(fixed, 0.0)
    free = values.where(~fixed, 0.0)
    return frozen, free


def assert_free_seed(seed: pd.DataFrame, mask: SutMask) -> None:
    """Guard against double-counting the frozen mass.

    A balance run with residual targets must be given ``Z``, not ``X``. Handing
    it the full matrix *and* the offset targets counts the fixed cells twice,
    and nothing downstream notices - the solver converges happily onto a wrong
    answer. This is the check that turns that into an error.
    """
    values = seed.to_numpy(dtype=float)
    bad = mask.fixed_value.to_numpy() & (values != 0)
    if bad.any():
        rows, cols = np.nonzero(bad)
        first = (seed.index[rows[0]], seed.columns[cols[0]])
        raise ValueError(
            f'{int(bad.sum())} fixed cells are nonzero in the seed, first at '
            f'{first} = {values[rows[0], cols[0]]}. Residual targets already '
            f'have the frozen mass subtracted, so the seed must be the free '
            f'part Z from split_fixed - passing the full matrix double-counts '
            f'F'
        )


def offset_target(target: Target, frozen: pd.DataFrame) -> Target:
    """One target, less the frozen mass its margin already contains.

    The aggregate case is ``A - R @ F @ Cᵀ``: the frozen margin is aggregated
    the same way the target is, so a mask sitting *inside* an aggregate is
    accounted for rather than ignored.
    """
    frozen_margin = margin(frozen, target.axis, target.restrict_to)
    if target.aggregator is not None:
        frozen_margin = target.aggregator.apply(frozen_margin)
    aligned = frozen_margin.reindex(target.values.index)
    if aligned.isna().any():
        missing = list(aligned.index[aligned.isna()])
        raise KeyError(
            f'{target.label} names margin labels the block does not have: ' f'{missing}'
        )
    residual = pd.to_numeric(target.values, errors='raise') - aligned
    return target.with_values(residual, source_suffix=' (residual)')


def offset_targets(targets: TargetSet, frozen: pd.DataFrame, block: str) -> TargetSet:
    """Offset every target on ``block``; pass the others through untouched.

    Targets on other blocks are returned as they were, so a set spanning the
    Use and Supply panels can be offset one block at a time without the caller
    having to partition it first.
    """
    return TargetSet(
        tuple(offset_target(t, frozen) if t.block == block else t for t in targets)
    )


def restore_fixed(balanced: pd.DataFrame, frozen: pd.DataFrame) -> pd.DataFrame:
    """Add ``F`` back after the engine has balanced ``Z``.

    The fixed cells come out bit-identical to the seed, which is the property
    the whole offset exists to deliver.
    """
    if not balanced.index.equals(frozen.index):
        raise ValueError('restore_fixed: row labels differ')
    if not balanced.columns.equals(frozen.columns):
        raise ValueError('restore_fixed: column labels differ')
    return balanced.astype(float) + frozen.astype(float)
