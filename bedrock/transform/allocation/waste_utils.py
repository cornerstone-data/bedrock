from __future__ import annotations

import pandas as pd

from bedrock.utils.taxonomy.cornerstone.industries import WASTE_DISAGG_INDUSTRIES


def waste_562000_allocation_series_ceda_allocator_to_cornerstone_schema(
    total_emission: float,
) -> pd.Series[float]:
    """Return a sector-indexed Series for waste (562000) split across Cornerstone waste industries."""
    waste_inds = WASTE_DISAGG_INDUSTRIES["562000"]
    if not waste_inds:
        return pd.Series({"562000": float(total_emission)})
    per_industry = float(total_emission) / float(len(waste_inds))
    return pd.Series({code: per_industry for code in waste_inds}, dtype=float)
