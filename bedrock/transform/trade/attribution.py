"""Clean functions for Trade FBS attribution weights (#729)."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bedrock.transform.flowbysector import FlowBySector


def collapse_detail_supply_to_t007(fbs: FlowBySector, **_kwargs: Any) -> FlowBySector:
    """Collapse ``Detail_Supply_Mix`` industry×commodity rows to commodity ``T007``.

    Proportional Trade attribution merges on ``PrimarySector``. Multiple
    industry rows per commodity would under-count in the denominator (only one
    row per sector enters the uniqueness filter). Summing onto
    ``SectorConsumedBy`` yields the same commodity vector as
    ``_supply_fbs_commodity_vector`` / bridge ``T007``.
    """
    if fbs.empty:
        raise ValueError(f'{fbs.full_name} is empty; cannot build T007 weights')
    if 'SectorConsumedBy' not in fbs.columns:
        raise ValueError(
            f'{fbs.full_name} missing SectorConsumedBy; cannot build T007 weights'
        )

    raw = pd.DataFrame(fbs)
    first_cols = [
        c
        for c in raw.columns
        if c not in ('FlowAmount', 'SectorConsumedBy', 'SectorProducedBy')
    ]
    agg = {c: 'first' for c in first_cols}
    agg['FlowAmount'] = 'sum'
    # SectorProducedBy is dropped in the groupby (commodity margin only).
    out = raw.groupby('SectorConsumedBy', as_index=False).agg(agg)
    return FlowBySector(
        out,
        full_name=fbs.full_name,
        config=fbs.config,
        convert_df_to_flowby=True,
    )
