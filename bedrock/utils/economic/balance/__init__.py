"""Scaffolding, ndarray GRAS kernel, and SUT orchestration.

Step 5's shared layer
(`#653 <https://github.com/cornerstone-data/bedrock/issues/653>`_) plus the
ndarray GRAS kernel and Use-then-Supply wrapper
(`#588 <https://github.com/cornerstone-data/bedrock/issues/588>`_).
``Target``, ``SutMask``, the offset method and the feasibility precheck do
not depend on the scaler. ``gras_balance`` is the scaler: one signed matrix,
row/col vectors, ``free_mask``, ``sign_flex``. ``engine`` is the SUT adapter:
hard T1 and T11–T17 stay exact; soft T2/T4/T7 are imposed (blend-once from
entry ``Z``; T4 is a column-neutral closer). T6/T8/T9 whole-name defer when
T12–T14 occupy a slot. ``WEIGHTS`` are uncalibrated. Nothing in this package
imports scipy.

The pieces fit together in one order::

    frozen, free = split_fixed_blocks(seeds, masks)   # X = F + Z, per block
    residual     = offset_targets(targets, frozen)
    precheck(seeds, masks, targets)                   # raises if a margin is stuck
    out          = engine(free, residual, masks)      # Use then Supply; impose_soft=True
    result       = restore_fixed_blocks(out.blocks, frozen)

Wrapper mapping: ``free_mask = mask.free.to_numpy()``,
``sign_flex = (mask.sign_lock.to_numpy() == 0)``. Kernel
``sign_flex is None`` is all-False (stricter than a default ``SutMask``).

``seeds`` and ``masks`` are mappings of block name to frame, because a target
may relate the Use panel to the Supply panel - ``T016 = T019`` and the
product-tax identities all do.

**The kernel only ever sees a participation mask and residual target
vectors**, which is what makes a fixed-value mask expressible without
touching the scaler.

Analysis behind the design: ``bedrock/analysis/nowcasting/mask_layer_plan.md``
and ``target_set_plan.md``, with the measurements in
``mask_layer_feasibility.py``.
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
from bedrock.utils.economic.balance.orchestrate import SutBalanceResult, engine
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
    'SutBalanceResult',
    'SutMask',
    'Target',
    'TargetSet',
    'TargetTerm',
    'UnsourcedTargets',
    'assert_free_seed',
    'assert_subsidies_negative',
    'engine',
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
