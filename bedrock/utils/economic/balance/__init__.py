"""Engine-agnostic scaffolding for a constrained matrix balance.

Step 5's shared layer
(`#653 <https://github.com/cornerstone-data/bedrock/issues/653>`_). ``Target``,
``SutMask``, the offset method and the feasibility precheck are the same code
whether the balancing engine ends up vendored, hardened from an existing
implementation, or written fresh - so this package deliberately does not depend
on that decision, and nothing in it imports a solver.

The pieces fit together in one order::

    frozen, free = split_fixed(seed, mask)        # X = F + Z
    residual     = offset_targets(targets, frozen, block)
    precheck(seed, mask, targets)                 # raises if a margin is stuck
    balanced     = engine(free, residual, mask.frozen)     # not ours
    result       = restore_fixed(balanced, frozen)

The engine only ever sees a participation mask and residual targets, which is
what makes a fixed-value mask expressible without touching the engine at all.

Analysis behind the design: ``bedrock/analysis/nowcasting/mask_layer_plan.md``
and ``target_set_plan.md``, with the measurements in
``mask_layer_feasibility.py``.
"""

from bedrock.utils.economic.balance.feasibility import (
    DEFAULT_LEVERAGE_WARN,
    Infeasibility,
    InfeasibleBalance,
    leverage,
    margin_report,
    precheck,
)
from bedrock.utils.economic.balance.mask import SutMask, assert_subsidies_negative
from bedrock.utils.economic.balance.offset import (
    assert_free_seed,
    margin,
    offset_target,
    offset_targets,
    restore_fixed,
    split_fixed,
)
from bedrock.utils.economic.balance.targets import (
    Aggregator,
    Axis,
    Target,
    TargetSet,
)

__all__ = [
    'DEFAULT_LEVERAGE_WARN',
    'Aggregator',
    'Axis',
    'InfeasibleBalance',
    'Infeasibility',
    'SutMask',
    'Target',
    'TargetSet',
    'assert_free_seed',
    'assert_subsidies_negative',
    'leverage',
    'margin',
    'margin_report',
    'offset_target',
    'offset_targets',
    'precheck',
    'restore_fixed',
    'split_fixed',
]
