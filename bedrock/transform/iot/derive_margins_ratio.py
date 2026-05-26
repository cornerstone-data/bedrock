from __future__ import annotations

import numpy as np
import pandas as pd

from bedrock.extract.iot.margins import load_2017_margins_usa
from bedrock.transform.eeio.derived_2017_helpers import EXPANDED_SECTORS_2012_TO_2017
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (
    USA_2017_COMMODITY_INDEX,
    load_usa_2017_commodity__ceda_v7_correspondence,
)


def derive_2017_producer_to_purchaser_price_ratio_usa() -> pd.Series[float]:
    """
    Derive the ratio to convert EF from producer to purchaser price for each sector.
    Formula: purchaser price = producer price + margin
    Since original EF is in kgCO2e/USD_producer, ratio here is calculated as
    (output_producer / (output_producer + margin)).
    """
    corresp = load_usa_2017_commodity__ceda_v7_correspondence()
    corresp.columns.names = ["commodity"]

    margin = corresp @ (
        load_2017_margins_usa()
        .groupby("commodity")
        .sum()
        .reindex(USA_2017_COMMODITY_INDEX)
        .fillna(0.0)
    )
    # assume expanded_sectors will receive equal portion of value from aggregated sector
    margin.loc[EXPANDED_SECTORS_2012_TO_2017, :] *= 1 / len(
        EXPANDED_SECTORS_2012_TO_2017
    )

    return (margin["Producer's Price"] / margin["Purchaser's Price"]).replace(
        [np.inf, -np.inf, np.nan], 1.0
    )
