from __future__ import annotations

import typing as ta

import pandas as pd

from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTOR, CEDA_V7_SECTORS
from bedrock.ceda_usa.utils.units import MEGATONNE_TO_KG
from bedrock.extract.allocation.bea import load_bea_use_table
from bedrock.extract.allocation.epa import (
    load_ch4_emissions_from_mobile_combustion,
)

MOBILE_SOURCE_TO_BEA_INDUSTRY_MAPPING: ta.Dict[
    ta.Tuple[str, str],
    ta.Union[CEDA_V7_SECTOR, ta.List[CEDA_V7_SECTOR]],
] = {
    ("Gasoline On-Road b", "Medium- and Heavy-Duty Trucks and Buses"): "484000",
    ("Diesel On-Road b", "Medium- and Heavy-Duty  Trucks and Buses"): "484000",
    ("Non-Road c", "Ships and Boats"): "483000",
    ("Non-Road c", "Rail d"): "482000",
    ("Non-Road c", "Aircraft"): "481000",
    ("Non-Road c", "Agricultural Equipmente"): [
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
    ],
    ("Non-Road c", "Construction/Mining Equipmentf"): [
        "211000",
        "212100",
        "212230",
        "2122A0",
        "212310",
        "2123A0",
        "213111",
    ],
    ("Alternative Fuel On-Road", "TOTAL"): [
        "485000",
        "48A000",
        "492000",
    ],
    ("Non-Road c", "Other g"): [
        "485000",
        "48A000",
        "492000",
        "532100",
        "532400",
        "621900",
        "713900",
        "811100",
        "811300",
    ],
}

PERSONAL_USE_TO_BEA_INDUSTRY_MAPPING: ta.Dict[ta.Tuple[str, str], str] = {
    ("Gasoline On-Road b", "Passenger Cars"): "F01000",
    ("Gasoline On-Road b", "Light-Duty Trucks"): "F01000",
    ("Diesel On-Road b", "Passenger Cars"): "F01000",
    ("Diesel On-Road b", "Light-Duty Trucks"): "F01000",
}

USE_OF_FUEL_TO_BEA_INDUSTRY_MAPPING: ta.Dict[str, CEDA_V7_SECTOR] = {
    "Ag_Equipment": "333111",
    "Construction_Mining_Equipment": "333120",
    "Petroleum": "324110",
    "Natural_Gas": "221200",
}


def allocate_mobile_combustion() -> pd.Series[float]:
    emissions = load_ch4_emissions_from_mobile_combustion()
    bea_use = load_bea_use_table()

    def _allocate_emissions(
        mobile_source: ta.Tuple[str, str], use_of_fuel: str
    ) -> pd.Series[float]:
        use = bea_use.loc[
            pd.Index(MOBILE_SOURCE_TO_BEA_INDUSTRY_MAPPING[mobile_source]),
            USE_OF_FUEL_TO_BEA_INDUSTRY_MAPPING[use_of_fuel],
        ].astype(float)
        return (use / use.sum()).mul(emissions.loc[mobile_source])

    def _map_emissions(mobile_source: ta.Tuple[str, str]) -> pd.Series[float]:
        ser = emissions.loc[pd.Index([mobile_source])]
        ser.index = pd.Index(
            [MOBILE_SOURCE_TO_BEA_INDUSTRY_MAPPING[i] for i in ser.index]
        )
        return ser

    allocated_personal_use_emissions = (
        emissions.loc[
            pd.Index(
                PERSONAL_USE_TO_BEA_INDUSTRY_MAPPING.keys(),
            ),
        ]
        .groupby(PERSONAL_USE_TO_BEA_INDUSTRY_MAPPING)  # type: ignore
        .sum()
    )

    allocated = pd.concat(
        [
            _allocate_emissions(
                ("Non-Road c", "Agricultural Equipmente"), "Ag_Equipment"
            ),
            _allocate_emissions(
                ("Non-Road c", "Construction/Mining Equipmentf"),
                "Construction_Mining_Equipment",
            ),
            _allocate_emissions(("Non-Road c", "Other g"), "Petroleum").add(
                _allocate_emissions(
                    ("Alternative Fuel On-Road", "TOTAL"), "Natural_Gas"
                ),
                fill_value=0.0,
            ),
            _map_emissions(("Non-Road c", "Rail d")),
            _map_emissions(("Non-Road c", "Ships and Boats")),
            _map_emissions(
                ("Gasoline On-Road b", "Medium- and Heavy-Duty Trucks and Buses")
            ),
            allocated_personal_use_emissions,
        ]
    )
    return allocated.reindex(CEDA_V7_SECTORS, fill_value=0.0) * MEGATONNE_TO_KG
