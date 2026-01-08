from __future__ import annotations

import pandas as pd

from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_international_bunker_fuels() -> pd.Series[float]:
    """
    intentionally left blank
    """
    allocated = pd.Series(dtype=float)
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
