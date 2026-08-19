"""The offset method: a fixed-value mask, engine-agnostically.

Neither candidate engine can hold a cell at a nonzero value. Both can exclude a
cell from participating. The offset method turns the first into the second, in
about twenty lines, so the mask stops being a reason to prefer one engine over
the other::

    X  = F + Z            F zero off the mask, Z zero on it
    r' = r - F @ 1        row targets, less the frozen row mass
    c' = c - 1ᵀ @ F       column targets, less the frozen column mass
    A' = A - R @ F @ Cᵀ   and the same for aggregate-level targets

Balance ``Z`` against the residual targets, then add ``F`` back. The engine only
ever sees a participation mask.

Because a target may span blocks, the offset works over a **mapping** of block
name to frame rather than one frame at a time: the residual of
``supply.row − use.row`` is not defined until both frozen parts are known.

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

from collections.abc import Mapping

import numpy as np
import pandas as pd

from bedrock.utils.economic.balance.mask import SutMask
from bedrock.utils.economic.balance.targets import Axis, Target, TargetSet

Blocks = Mapping[str, pd.DataFrame]
Masks = Mapping[str, SutMask]


def margin(
    frame: pd.DataFrame,
    axis: Axis,
    restrict_to: tuple[str, ...] | None = None,
) -> pd.Series:
    """The row or column margin of ``frame``.

    ``axis='row'`` sums across columns and yields one value per row;
    ``axis='column'`` sums down rows and yields one per column. ``restrict_to``
    narrows the *summed* axis.
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
    return values.where(fixed, 0.0), values.where(~fixed, 0.0)


def split_fixed_blocks(
    seeds: Blocks, masks: Masks
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """:func:`split_fixed` over every block, keyed the same way."""
    missing = set(seeds) ^ set(masks)
    if missing:
        raise KeyError(f'seeds and masks disagree on blocks: {sorted(missing)}')
    frozen: dict[str, pd.DataFrame] = {}
    free: dict[str, pd.DataFrame] = {}
    for block, seed in seeds.items():
        frozen[block], free[block] = split_fixed(seed, masks[block])
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


def offset_target(target: Target, frozen: Blocks) -> Target:
    """One target, less the frozen mass its margins already contain.

    Handles the aggregate case as ``A - R @ F @ Cᵀ`` and the cross-block case
    by evaluating every term against its own frozen block, so a mask sitting
    inside an aggregate - or on the other side of an identity - is accounted
    for rather than ignored.
    """
    return target.with_values(
        target.residual_against(frozen), source_suffix=' (residual)'
    )


def offset_targets(targets: TargetSet, frozen: Blocks) -> TargetSet:
    """Offset every target against the frozen parts of the blocks it reads."""
    return TargetSet(tuple(offset_target(t, frozen) for t in targets))


def restore_fixed(balanced: pd.DataFrame, frozen: pd.DataFrame) -> pd.DataFrame:
    """Add ``F`` back after the engine has balanced ``Z``.

    The fixed cells come out bit-identical to the seed, which is the property
    the whole offset exists to deliver - but only if the engine left ``Z``'s
    fixed cells at exactly zero. An engine that leaks mass onto them would have
    that leak silently *added* to ``F``, so this asserts instead of trusting.
    Catching a leaky engine is the point of the check; overwriting with ``F``
    would hide it.
    """
    if not balanced.index.equals(frozen.index):
        raise ValueError('restore_fixed: row labels differ')
    if not balanced.columns.equals(frozen.columns):
        raise ValueError('restore_fixed: column labels differ')
    leaked = (frozen.to_numpy() != 0) & (balanced.to_numpy(dtype=float) != 0)
    if leaked.any():
        rows, cols = np.nonzero(leaked)
        first = (balanced.index[rows[0]], balanced.columns[cols[0]])
        raise ValueError(
            f'{int(leaked.sum())} fixed cells are nonzero in the balanced free '
            f'part, first at {first} = '
            f'{balanced.to_numpy(dtype=float)[rows[0], cols[0]]}. The engine '
            f'moved mass onto a masked cell; adding F back would bury that '
            f'rather than hold the cell at its value'
        )
    return balanced.astype(float) + frozen.astype(float)


def restore_fixed_blocks(balanced: Blocks, frozen: Blocks) -> dict[str, pd.DataFrame]:
    """:func:`restore_fixed` over every block."""
    return {
        block: restore_fixed(frame, frozen[block]) for block, frame in balanced.items()
    }
