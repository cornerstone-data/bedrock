from __future__ import annotations

import typing as ta

"""
NOTE: table numbers change year-over-year in the GHG inventory report.
For updates, refer to the documentation in each function for the contents of the table,
and find the updated table number in the latest GHG inventory.
"""
TBL_NUMBERS_2022 = ta.Literal[
    "3-24",
    "A-5",
    "A-68",
]

TBL_NUMBERS_2023 = ta.Literal[
    "3-25",
    "A-5",
    "A-69",
]

TBL_NUMBERS = ta.Union[TBL_NUMBERS_2022, TBL_NUMBERS_2023]

EPA_TABLE_NAMES = ta.Literal[
    "co2_fossil_fuel_non_energy_uses",
    "energy_consumption_co2_by_fuel_type",
    "fuel_consumption_by_vehicle_type",
]

EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2022: ta.Dict[EPA_TABLE_NAMES, TBL_NUMBERS_2022] = {
    "co2_fossil_fuel_non_energy_uses": "3-24",
    "energy_consumption_co2_by_fuel_type": "A-5",
    "fuel_consumption_by_vehicle_type": "A-68",
}

EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2023: ta.Dict[EPA_TABLE_NAMES, TBL_NUMBERS_2023] = {
    "co2_fossil_fuel_non_energy_uses": "3-25",
    "energy_consumption_co2_by_fuel_type": "A-5",
    "fuel_consumption_by_vehicle_type": "A-69",
}
