"""Scaffolding and ndarray GRAS kernel.

``Target``, ``SutMask``, the offset method and the feasibility precheck do
not depend on the scaler. ``gras_balance`` is the scaler: one signed matrix,
row/col vectors, ``free_mask``, ``sign_flex``. Nothing in this package
imports scipy.

Typical call order::

    frozen, free = split_fixed_blocks(seeds, masks)   # X = F + Z, per block
    residual     = offset_targets(targets, frozen)
    precheck(seeds, masks, targets)                   # raises if a margin is stuck
    # scale Z (gras_balance per block, or a SUT adapter)
    result       = restore_fixed_blocks(balanced, frozen)

Callers map ``free_mask = mask.free.to_numpy()`` and
``sign_flex = (mask.sign_lock.to_numpy() == 0)``. Kernel
``sign_flex is None`` is all-False (stricter than a default ``SutMask``).

``seeds`` and ``masks`` are mappings of block name to frame, because a target
may relate the Use panel to the Supply panel.

**The kernel only ever sees a participation mask and residual target
vectors**, which is what makes a fixed-value mask expressible without
touching the scaler.
"""

from bedrock.utils.economic.balance.feasibility import (
    DEFAULT_LEVERAGE_WARN,
    REPORT_COLUMNS,
    Infeasibility,
    InfeasibleBalance,
    UnsourcedTargets,
    leverage,
    margin_report,
    precheck,
)
from bedrock.utils.economic.balance.gras import GrasBalanceResult, gras_balance
from bedrock.utils.economic.balance.mask import SutMask, assert_subsidies_negative
from bedrock.utils.economic.balance.offset import (
    assert_free_seed,
    margin,
    offset_target,
    offset_targets,
    restore_fixed,
    restore_fixed_blocks,
    split_fixed,
    split_fixed_blocks,
)
from bedrock.utils.economic.balance.targets import (
    PLACEHOLDER_PREFIX,
    Aggregator,
    Axis,
    Target,
    TargetSet,
    TargetTerm,
)

__all__ = [
    'DEFAULT_LEVERAGE_WARN',
    'PLACEHOLDER_PREFIX',
    'REPORT_COLUMNS',
    'Aggregator',
    'Axis',
    'GrasBalanceResult',
    'InfeasibleBalance',
    'Infeasibility',
    'SutMask',
    'Target',
    'TargetSet',
    'TargetTerm',
    'UnsourcedTargets',
    'assert_free_seed',
    'assert_subsidies_negative',
    'gras_balance',
    'leverage',
    'margin',
    'margin_report',
    'offset_target',
    'offset_targets',
    'precheck',
    'restore_fixed',
    'restore_fixed_blocks',
    'split_fixed',
    'split_fixed_blocks',
]
