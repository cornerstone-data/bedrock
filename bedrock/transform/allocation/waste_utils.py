from __future__ import annotations

import pandas as pd

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.cornerstone.industries import WASTE_DISAGG_INDUSTRIES


def waste_562000_allocation_series_ceda_allocator_to_cornerstone_schema(
    total_emission: float,
) -> pd.Series[float]:
    """Return a sector-indexed Series for waste (562000) that is schema-aware.

    When the Cornerstone 2026 schema is active, the total emission for the
    CEDA aggregate sector 562000 is split equally across the Cornerstone
    waste industries corresponding to that aggregate. When the Cornerstone
    schema is not active, the emission is kept on the CEDA sector 562000.
    """
    cfg = get_usa_config()

    if cfg.use_cornerstone_2026_model_schema:
        waste_inds = WASTE_DISAGG_INDUSTRIES["562000"]
        if not waste_inds:
            return pd.Series({"562000": float(total_emission)})
        per_industry = float(total_emission) / float(len(waste_inds))
        return pd.Series({code: per_industry for code in waste_inds}, dtype=float)

    return pd.Series({"562000": float(total_emission)})
