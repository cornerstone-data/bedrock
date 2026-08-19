Economic utilities for inflation adjustments, currency conversion, target
year scaling, and a GRAS kernel.

``bedrock.utils.economic.balance.gras_balance`` is an ndarray-only GRAS
scaler (Lenzen, Wood and Gallego 2007; Temurshoev, Miller and Bouwmeester
2013 all-negative margins). It takes one signed matrix, row and column
target vectors, a participation ``free_mask``, and ``sign_flex``. It does
not import scipy. Inputs are copied; the caller arrays are not mutated.

Nonzero holds are the offset layer, not the kernel: split ``X = F + Z``,
balance ``Z``, restore ``F``. Participation is ``mask.free``
(``~(structural_zero | fixed_value)``), never ``mask.frozen`` and never
"nonzero cells of ``Z``".

Callers pass::

    free_mask = mask.free.to_numpy()
    sign_flex = (mask.sign_lock.to_numpy() == 0)

Kernel ``sign_flex is None`` is all-False (no cell may change sign), which
is stricter than a default ``SutMask`` (``sign_lock`` 0 means flex is
allowed). Omitting ``sign_flex`` would sign-lock the whole SUT.
