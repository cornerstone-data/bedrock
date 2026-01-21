from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_co2_emissions_from_fossil_fuels_for_non_energy_uses,
)
from bedrock.transform.allocation.transportation_fuel_use.derived import (
    get_personal_consumption_expenditure_petref_cons_purchased,
    get_res_pet_ref_cons_for_transport,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_non_energy_fuels_transport() -> pd.Series[float]:
    emissions = load_co2_emissions_from_fossil_fuels_for_non_energy_uses().loc[
        ("Transportation", "TOTAL")
    ]
    bea_use = load_bea_use_table()
    transportation_gov_pce_sectors = [
        "481000",
        "482000",
        "483000",
        "484000",
        "485000",
        "486000",
        "48A000",
        "492000",
        "S00500",
        "S00600",
        "491000",
        "GSLGO",
        "S00203",
        "F01000",
    ]

    use = bea_use.loc[
        pd.Index(transportation_gov_pce_sectors),
        "324110",
    ].astype(float)
    use["F01000"] = use["F01000"] * (
        get_res_pet_ref_cons_for_transport()
        / get_personal_consumption_expenditure_petref_cons_purchased()
    )

    assert isinstance(use, pd.Series), "use is not a series"
    allocated = emissions * (use / use.sum())
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
