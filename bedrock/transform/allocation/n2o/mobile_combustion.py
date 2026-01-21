from __future__ import annotations

import pandas as pd

from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_n2o_emissions_from_mobile_combustion,
)
from bedrock.utils.economic.units import MEGATONNE_TO_KG
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


def allocate_mobile_combustion() -> pd.Series[float]:
    beaU = load_bea_use_table()

    emissions = load_n2o_emissions_from_mobile_combustion()

    ag_equip_emissions = emissions.loc[("Non-Road c", "Agricultural Equipment e")]
    ag_equip_sectors = [
        "1111A0",
        "1111B0",
        "111200",
        "111300",
        "111400",
        "111900",
        "112120",
        "1121A0",
        "112300",
        "112A00",
        "113000",
        "114000",
    ]
    ag_pct = beaU.loc[
        pd.Index(ag_equip_sectors),
        "333111",  # Farm machinery and equipment manufacturing
    ]
    ag_pct = ag_pct / ag_pct.sum()
    ag_alloc = ag_pct * ag_equip_emissions

    mining_equip_emissions = emissions.loc[
        ("Non-Road c", "Construction/Mining Equipment f")
    ]
    mining_equip_sectors = [
        "211000",
        "212100",
        "212230",
        "2122A0",
        "212310",
        "2123A0",
        "213111",
        "21311A",
    ]

    mining_pct = beaU.loc[
        pd.Index(mining_equip_sectors), "333120"  # Construction machinery manufacturing
    ]
    mining_pct = mining_pct / mining_pct.sum()
    mining_alloc = mining_pct * mining_equip_emissions

    transport_alloc = pd.Series(
        {
            "481000": emissions.loc[("Non-Road c", "Aircraft ")],
            "482000": emissions.loc[("Non-Road c", "Raild")],
            "483000": emissions.loc[("Non-Road c", "Ships and Boats")],
            "484000": (
                emissions.loc[
                    pd.MultiIndex.from_product(
                        [
                            ["Diesel On-Road b"],
                            [
                                "Medium- and Heavy-Duty Trucks",
                                "Medium- and Heavy-Duty Buses",
                            ],
                        ]
                    )
                ].sum()
                + emissions.loc[
                    ("Gasoline On-Road b", "Medium- and Heavy-Duty Trucks and Buses")
                ]
            ),
        }
    )

    other_emissions = emissions.loc[("Non-Road c", "Other g")]
    other_sectors = [
        "485000",
        "48A000",
        "492000",
        "532100",
        "532400",
        "621900",
        "713900",
        "811100",
        "811300",
    ]
    other_pct = beaU.loc[pd.Index(other_sectors), "324110"]  # Petroleum refineries
    other_pct = other_pct / other_pct.sum()
    other_alloc = other_pct * other_emissions

    allocated = pd.concat(
        (ag_alloc, mining_alloc, transport_alloc, other_alloc)
    ).astype(float)

    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
