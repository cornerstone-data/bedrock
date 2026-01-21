from __future__ import annotations

import pandas as pd

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS as CEDA_SECTORS


def get_ceda_sector_index() -> pd.Index[str]:
    return pd.Index(CEDA_SECTORS, name="sector", dtype=str)
