"""
The goal of this file is to recreate the spreadsheet found here: https://docs.google.com/spreadsheets/d/1qBsIhrw1es_VF_WbmViCfXcz7iNC4ArQ/edit#gid=680949284

This is the fuel used by various transportation methods. The spreadsheet does a lot of complex caluclations for a few data sources and then is ultimately used in
the allocation model.
"""

from __future__ import annotations

import functools

import numpy as np
import pandas as pd

from bedrock.extract.allocation.bea import (
    load_bea_personal_consumption_expenditure,
    load_bea_use_table,
)
from bedrock.extract.allocation.eia import (
    load_heating_oil_annual_avg_residential_price,
    load_propane_annual_avg_residential_price,
)
from bedrock.extract.allocation.epa import (
    load_fuel_consumption_by_fuel_and_vehicle_type as _load_table_a94,
)
from bedrock.extract.allocation.epa import load_tbtu_across_fuel_types
from bedrock.transform.allocation.transportation_fuel_use.constants import (
    TRANSPORTATION_FUEL_TYPES,
)
from bedrock.utils.economic.units import (
    HEATING_OIL_MMBTU_PER_GALLON,
    PROPANE_MMBTU_PER_GALLON,
)


def get_personal_consumption_expenditure_petref_cons_purchased() -> float:
    return load_bea_personal_consumption_expenditure()[
        "Gasoline and other energy goods"
    ]


def get_total_residential_heat_oil_exp() -> float:
    return (
        # propane
        load_tbtu_across_fuel_types().loc[["Kerosene", "LPG"], "Res"].sum()
        * (load_propane_annual_avg_residential_price() / PROPANE_MMBTU_PER_GALLON)
    ) + (
        # heating oil
        load_tbtu_across_fuel_types().loc["Distillate Fuel Oil", "Res"]  # type: ignore
        * (load_heating_oil_annual_avg_residential_price() / HEATING_OIL_MMBTU_PER_GALLON)  # type: ignore
    )


def get_res_pet_ref_cons_for_transport() -> float:
    return (
        get_personal_consumption_expenditure_petref_cons_purchased()
        - get_total_residential_heat_oil_exp()
    )


PETROLEUM_PRODUCTS_SECTOR = "324110"

load_table_a94 = functools.cache(_load_table_a94)


def derive_fuel_percent_breakout() -> pd.Series[float]:
    absolute_fuel_allocation = derive_fuel_allocation()
    total_per_fuel = absolute_fuel_allocation.groupby("fuel_type").sum()
    return (
        absolute_fuel_allocation.div(total_per_fuel, level="fuel_type")
        .groupby(["fuel_type", "sector"])
        .sum()
    )


def derive_fuel_allocation() -> pd.Series[float]:
    gasoline = allocate_gasoline()
    diesel = allocate_diesel()
    lpg = allocate_lpg()
    jet_fuel = allocate_jet_fuel()
    etc = allocate_etc()
    return pd.concat([gasoline, diesel, lpg, jet_fuel, etc])


def allocate_gasoline() -> pd.Series[float]:
    def allocate_gasoline_usage_from_passenger_cars() -> pd.Series[float]:
        allocation_industries = ["F01000", "S00600", "491000", "GSLGO"]
        numerators: list[float] = [
            bea_use_table.loc[ind, PETROLEUM_PRODUCTS_SECTOR]  # type: ignore
            * (RES_PR_MOTOR_GASOLINE_PERC if ind == "F01000" else 1.0)  # type: ignore
            for ind in allocation_industries
        ]

        TOTAL_GASOLINE_FOR_PASSENGER_CARS = table_a94.loc[
            ("Motor Gasolineb,c", "Passenger Cars")
        ]

        return pd.Series(
            np.array(numerators) / sum(numerators) * TOTAL_GASOLINE_FOR_PASSENGER_CARS,
            index=allocation_industries,
        )

    table_a94 = load_table_a94()
    bea_use_table = load_bea_use_table()

    TOTAL_MOTOR_GASOLINE = derive_total_motor_gasoline()
    TOTAL_DIESEL = derive_total_diesel()

    MOTOR_GASOLINE_PERC = TOTAL_MOTOR_GASOLINE / (TOTAL_MOTOR_GASOLINE + TOTAL_DIESEL)

    RES_PR_MOTOR_GASOLINE = get_res_pet_ref_cons_for_transport() * MOTOR_GASOLINE_PERC

    RES_PR_MOTOR_GASOLINE_PERC = (
        RES_PR_MOTOR_GASOLINE
        / get_personal_consumption_expenditure_petref_cons_purchased()
    )

    TOTAL_GASOLINE_FOR_LDT = table_a94.loc[("Motor Gasolineb,c", "Light-Duty Trucks")]

    MAGIC_NUMBER_PETROLEUM_INTO_LDT_NUMERATOR = 9656  # TODO: where is this number from?
    RETAIL_PRICE_MOTOR_GASOLINE = 4.192  # Annual (2022) average retail price from https://www.eia.gov/totalenergy/data/monthly/pdf/sec9_6.pdf

    allocated_industries = [
        "F01000",
        "492000",
    ]

    ldt_gasoline = pd.Series(0.0, index=allocated_industries)
    ldt_gasoline.loc["492000"] = (
        MAGIC_NUMBER_PETROLEUM_INTO_LDT_NUMERATOR / RETAIL_PRICE_MOTOR_GASOLINE
    )
    ldt_gasoline.loc["F01000"] = TOTAL_GASOLINE_FOR_LDT - ldt_gasoline.loc["492000"]

    motorcycle_gasoline = table_a94.loc[("Motor Gasolineb,c", "Motorcycles")]
    buses_gasoline = table_a94.loc[("Motor Gasolineb,c", "Buses")]
    med_and_hd_trucks_gasoline = table_a94.loc[
        ("Motor Gasolineb,c", "Medium- and Heavy-Duty Trucks")
    ]
    recreational_boats_gasoline = table_a94.loc[
        ("Motor Gasolineb,c", "Recreational Boatsd")
    ]

    return _add_fuel_level_to_index(
        pd.concat(
            [
                allocate_gasoline_usage_from_passenger_cars(),
                ldt_gasoline,
                pd.Series(
                    [
                        motorcycle_gasoline,
                        buses_gasoline,
                        med_and_hd_trucks_gasoline,
                        recreational_boats_gasoline,
                    ],
                    index=["F01000", "485000", "484000", "F01000"],
                ),
            ]
        )
        .groupby(level=0)
        .sum(),
        TRANSPORTATION_FUEL_TYPES.GASOLINE,
    )


def allocate_diesel() -> pd.Series[float]:
    table_a94 = load_table_a94()
    bea_use_table = load_bea_use_table()
    allocated_bus_diesel = allocate_total_across_industries(
        total=table_a94.loc[("Distillate Fuel Oil (Diesel Fuel)b,c", "Buses")],
        column_industry=PETROLEUM_PRODUCTS_SECTOR,
        # NOTE: 485000 is inclusive of S00201 - State and local government transit and ground passenger transportation
        allocation_industries=[
            "485000",  # Transit and ground passenger transportation
            "48A000",  # Scenic and sightseeing transportation
        ],
        bea_use_table=bea_use_table,
    )

    diesel_allocation_industries = [
        # assume diesel fuel for mht use is consumed by truck transportation as well as government sectors
        # because they are more likely to operate their only truck fleets than ordinary industries
        "484000",  # Truck Transportation
        "491000",  # Postal Service
        "S00102",  # Other federal government enterprises
        "GSLGE",  # State and local government educational services
        "GSLGH",  # State and local government (hospitals and health services)
        "GSLGO",  # State and local government (other services)
        "S00203",  # Other state and local government enterprises
    ]

    allocated_mht_diesel = allocate_total_across_industries(
        total=table_a94.loc[
            ("Distillate Fuel Oil (Diesel Fuel)b,c", "Medium- and Heavy-Duty Trucks")
        ],
        column_industry=PETROLEUM_PRODUCTS_SECTOR,
        allocation_industries=diesel_allocation_industries,
        bea_use_table=bea_use_table,
    )
    additional_diesel = pd.Series(
        [
            table_a94.loc[("Distillate Fuel Oil (Diesel Fuel)b,c", "Passenger Cars")],
            table_a94.loc[
                ("Distillate Fuel Oil (Diesel Fuel)b,c", "Light-Duty Trucks")
            ],
            table_a94.loc[
                ("Distillate Fuel Oil (Diesel Fuel)b,c", "Recreational Boats")
            ],
            table_a94.loc[
                (
                    "Distillate Fuel Oil (Diesel Fuel)b,c",
                    "Ships and Non-Recreational Boats",
                )
            ],
            table_a94.loc[("Distillate Fuel Oil (Diesel Fuel)b,c", "Raile")],
        ],
        index=["F01000", "F01000", "F01000", "483000", "482000"],
    )

    diesel = pd.concat([allocated_bus_diesel, allocated_mht_diesel, additional_diesel])
    return _add_fuel_level_to_index(
        diesel.groupby(level=0).sum(), TRANSPORTATION_FUEL_TYPES.DIESEL
    )


def allocate_lpg() -> pd.Series[float]:
    table_a94 = load_table_a94()
    return _add_fuel_level_to_index(
        pd.Series(
            {
                "F01000": table_a94.loc[("LPGf", "Passenger Cars")]
                + table_a94.loc[("LPGf", "Light-Duty Trucks")],
                "485000": table_a94.loc[("LPGf", "Buses")],
                "484000": table_a94.loc[("LPGf", "Medium- and Heavy-Duty Trucks")],
            }
        ),
        TRANSPORTATION_FUEL_TYPES.LPG,
    )


def allocate_jet_fuel() -> pd.Series[float]:
    table_a94 = load_table_a94()
    return _add_fuel_level_to_index(
        pd.Series(
            [
                table_a94.loc[("Jet Fuelf", "Commercial Aircraft")],
                table_a94.loc[("Jet Fuelf", "General Aviation Aircraft")],
                table_a94.loc[("Jet Fuelf", "Military Aircraft")],
            ],
            index=["481000", "481000", "S00500"],
        ),
        TRANSPORTATION_FUEL_TYPES.JET_FUEL,
    )


def allocate_etc() -> pd.Series[float]:
    table_a94 = load_table_a94()
    aviation_gasoline = _add_fuel_level_to_index(
        pd.Series(
            {
                "481000": table_a94.loc[
                    ("Aviation Gasolinef", "General Aviation Aircraft")
                ]
            }
        ),
        TRANSPORTATION_FUEL_TYPES.AVIATION_GASOLINE,
    )
    residential_fuel_oil = _add_fuel_level_to_index(
        pd.Series(
            {
                "483000": table_a94.loc[
                    ("Residual Fuel Oilf, g", "Ships and Non-Recreational Boats")
                ]
            }
        ),
        TRANSPORTATION_FUEL_TYPES.RESIDUAL_FUEL_OIL,
    )
    natural_gas = _add_fuel_level_to_index(
        pd.Series(
            {
                "486000": table_a94.loc[
                    ("Natural Gasf (trillion cubic feet)", "Pipelines")
                ]
            }
        ),
        TRANSPORTATION_FUEL_TYPES.NATURAL_GAS,
    )
    return pd.concat([aviation_gasoline, residential_fuel_oil, natural_gas])


def derive_total_motor_gasoline() -> float:
    table_a94 = load_table_a94()
    return table_a94.loc[
        pd.IndexSlice["Motor Gasolineb,c", ["Passenger Cars", "Light-Duty Trucks"]]
    ].sum()


def derive_total_diesel() -> float:
    table_a94 = load_table_a94()
    return table_a94.loc[
        pd.IndexSlice[
            "Distillate Fuel Oil (Diesel Fuel)b,c",
            ["Passenger Cars", "Light-Duty Trucks"],
        ]
    ].sum()


def allocate_total_across_industries(
    *,
    total: float,
    column_industry: str,
    allocation_industries: list[str],
    bea_use_table: pd.DataFrame,
) -> pd.Series[float]:
    """
    Allocate the total across the industries in the BEA use table
    """
    numerators: list[float] = [
        bea_use_table.loc[ind, column_industry] for ind in allocation_industries  # type: ignore
    ]
    return pd.Series(
        [total * (numerator / sum(numerators)) for numerator in numerators],
        index=allocation_industries,
    )


def _add_fuel_level_to_index(
    series: pd.Series[float], fuel_type: TRANSPORTATION_FUEL_TYPES
) -> pd.Series[float]:
    new_level_values = [fuel_type] * len(series)
    multi_index_tuples = list(zip(new_level_values, series.index))
    multi_index = pd.MultiIndex.from_tuples(
        multi_index_tuples, names=["fuel_type", "sector"]
    )
    return pd.Series(series.values, index=multi_index)
