Economic utilities for inflation adjustments, currency conversion, target
year scaling, and the Step 5 GRAS kernel plus SUT wrapper.

``bedrock.utils.economic.balance.gras_balance`` is an ndarray-only GRAS
scaler (Lenzen, Wood and Gallego 2007; Temurshoev, Miller and Bouwmeester
2013 all-negative margins). It takes one signed matrix, row and column
target vectors, a participation ``free_mask``, and ``sign_flex``. It does
not import scipy. Inputs are copied; the caller arrays are not mutated.

``engine(free, residual, masks)`` is the SUT wrapper: Use then Supply,
hard T1 and T11–T17 only. Soft / placeholder targets (T2, T4, T6–T9) are
skipped — their ``.values`` are not read; unconstrained slots hold at
the current Z row/col sum. T11 is written only onto live rows of the
panel being scaled; empty-free T11 slots hold so Supply can absorb a
frozen or structurally empty Use commodity. KRAS is not this package yet.

Nonzero holds are the offset layer, not the kernel: split ``X = F + Z``,
balance ``Z``, restore ``F``. Participation is ``mask.free``
(``~(structural_zero | fixed_value)``), never ``mask.frozen`` and never
"nonzero cells of ``Z``".

The wrapper passes::

    free_mask = mask.free.to_numpy()
    sign_flex = (mask.sign_lock.to_numpy() == 0)

Kernel ``sign_flex is None`` is all-False (no cell may change sign), which
is stricter than a default ``SutMask`` (``sign_lock`` 0 means flex is
allowed). Omitting ``sign_flex`` would sign-lock the whole SUT.
