"""FlowSA parse helpers for NowcastMUT-backed attribution FBAs."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bedrock.extract.iot.nowcast_mut_storage import (
    _load_stored_table,
    latest_nowcast_mut_vintage,
)
from bedrock.transform.flowbyfunctions import assign_fips_location_system
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.mapping.location import US_FIPS

# NowcastMUT stores USD; BEA Detail Use AfterRedef FBAs use million USD.
_USD_TO_MILLION_USD = 1e-6


def nowcast_detail_use_after_redef_parse(
    *, source: str, year: int, **_: Any
) -> pd.DataFrame:
    """Melt NowcastMUT after-redef Use into a BEA-shaped Money FBA.

    Loads ``Nowcast_Detail_Use_after_redef_{year}`` at the active config's
    ``nowcast_mut_vintage``, and at the newest GCS vintage for that year only
    when the config names none. Reading the newest upload unconditionally made
    a regenerated GHG FBS depend on whichever MUT was uploaded last. Shape
    matches ``bea_parse`` for ``BEA_Detail_Use_AfterRedef`` so GHG method
    exclusion lists and proportional attribution keep working.
    """
    year_i = int(year)
    configured = (get_usa_config().nowcast_mut_vintage or '').strip()
    vintage = configured or latest_nowcast_mut_vintage(year=year_i, stage='after')
    use = _load_stored_table('Use', vintage=vintage, year=year_i, stage='after')

    df = use.reset_index()
    index_col = df.columns[0]
    df = df.rename(columns={index_col: 'ActivityProducedBy'})
    df = df.melt(
        id_vars=['ActivityProducedBy'],
        var_name='ActivityConsumedBy',
        value_name='FlowAmount',
    )
    df['FlowAmount'] = df['FlowAmount'].astype(float) * _USD_TO_MILLION_USD

    df = df.reset_index(drop=True)
    df['SourceName'] = source
    df['Year'] = str(year_i)
    df['FlowName'] = f'USD{year_i}'
    df['Class'] = 'Money'
    df['FlowType'] = 'TECHNOSPHERE_FLOW'
    df['Location'] = US_FIPS
    df = assign_fips_location_system(df, year_i)
    df['Unit'] = 'Million USD'
    df['DataReliability'] = 5
    df['DataCollection'] = 5
    df['Description'] = f'{source}_{year_i}_nowcast_after_redef'

    obj = df.select_dtypes(include='object')
    df[obj.columns] = obj.apply(
        lambda s: s.map(lambda x: x.strip() if isinstance(x, str) else x)
    )
    return df
