"""Cornerstone matrix bundle for electricity disaggregation (PR1.1).

Single-sourced with ``bedrock.publish.excel.writer`` for ``V`` and domestic/import
Use totals. See ``Electricity_disagg_implementation.md`` § PR 1.1.
"""

from __future__ import annotations

import pandas as pd

from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_U_set,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Ytot_full_cs_matrix,
)


def intermediate_use_totals() -> pd.DataFrame:
    """Commodity × industry total Use (``Udom + Uimp``) from one ``U_set`` call."""
    uset = derive_cornerstone_U_set()
    return uset.Udom + uset.Uimp


def materialize_electricity_disagg_cornerstone_frames() -> dict[str, pd.DataFrame]:
    """Return the six cornerstone-native frames required for electricity CSV export.

    Keys: ``V``, ``Udom``, ``Uimp``, ``VA``, ``Y``, ``E`` — same objects as the
    underlying ``derive_*`` functions (cached where those functions are cached).
    """
    V = derive_cornerstone_V()
    uset = derive_cornerstone_U_set()
    VA = derive_cornerstone_VA()
    Y = derive_cornerstone_Ytot_full_cs_matrix()
    E = derive_E_usa()
    return {
        'V': V,
        'Udom': uset.Udom,
        'Uimp': uset.Uimp,
        'VA': VA,
        'Y': Y,
        'E': E,
    }
