"""GHG characterization matrix builders.

KNOWN DIVERGENCE FROM USEEIOR
-----------------------------
In `useeior`, the `C` matrix is a real GWP characterization matrix
(`indicator x flow`) that converts physical-mass elementary flows in
`B [kg gas / USD]` into impact units via `D = C @ B [kgCO2e / USD]`.

In bedrock (Cornerstone), `B` is already in `kgCO2e / USD` -- AR6 GWPs are
applied upstream during FBS aggregation in
`bedrock.transform.allocation.derived` (`fbs['CO2e'] = ... * GWP100_AR6_CEDA`).
See also `bedrock.utils.math.formulas` (top docstring): `B (g,c) [kgco2e/USD]`.

As a consequence, the bedrock-side `C` returned by
`build_ghg_characterization_matrix` is intentionally *trivial*: a single
`Greenhouse Gases` indicator row of ones across the GHG axis. The product
`C @ B` then equals `B.sum(axis=0)` -- the per-commodity total `kgCO2e/USD`.

This matches the *shape* of `useeior`'s `C` but not its *semantics*. It exists
so that the bedrock published workbook carries the same `C`/`D` sheet names
as `useeior`'s `writeModeltoXLSX` output without inventing fake GWPs.

TODO(divergence-from-useeior): resolve before bedrock can be treated as a
drop-in `useeior` replacement. Options are documented in
`bedrock/publish/README.md`:

  1. Switch bedrock `B` to physical-mass units and ship a real GWP `C` from
     `bedrock.utils.emissions.gwp.GWP100_AR6_CEDA`.
  2. Or, emit a `B_phys` companion sheet alongside the CO2e `B` and a real
     GWP `C` keyed to `B_phys`.
"""

from __future__ import annotations

import pandas as pd

GREENHOUSE_GASES_INDICATOR = 'Greenhouse Gases'


def build_ghg_characterization_matrix(ghg_index: list[str]) -> pd.DataFrame:
    """Trivial `(1, |ghg|)` row-summer.

    See module docstring for the divergence-from-useeior caveat. The
    returned matrix has a single `Greenhouse Gases` indicator row of all
    ones, so `C @ B == B.sum(axis=0)` for any GHG-indexed `B` already in
    CO2e per dollar.
    """
    if not ghg_index:
        raise ValueError(
            'build_ghg_characterization_matrix requires non-empty ghg_index'
        )
    if len(set(ghg_index)) != len(ghg_index):
        raise ValueError(f'ghg_index must be unique; got duplicates in {ghg_index!r}')
    C = pd.DataFrame(
        [[1.0] * len(ghg_index)],
        index=pd.Index([GREENHOUSE_GASES_INDICATOR], name='indicator'),
        columns=pd.Index(list(ghg_index), name='ghg'),
        dtype=float,
    )
    return C
