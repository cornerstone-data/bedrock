from __future__ import annotations

import pandas as pd

from ceda_usa.extract.allocation.bea import load_bea_use_table
from ceda_usa.extract.allocation.epa import (
    load_recent_trends_in_ghg_emissions_and_sinks,
)
from ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from ceda_usa.utils.units import MEGATONNE_TO_KG


def allocate_product_uses() -> pd.Series[float]:
    beau = load_bea_use_table()
    pct: pd.Series[float] = beau.loc[  # type: ignore
        pd.Index(
            [
                "621100",
                "621200",
                "621300",
                "621400",
                "621500",
                "621600",
                "621900",
                "622000",
                "623A00",
            ]
        ),
        "325120",
    ].squeeze()
    pct = pct / pct.sum()
    emissions = load_recent_trends_in_ghg_emissions_and_sinks().loc[
        ("N2Oc", "N2O from Product Uses")
    ]

    return (pct * emissions).reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
