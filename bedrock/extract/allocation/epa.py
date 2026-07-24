from __future__ import annotations

import functools
import os
import posixpath
import typing as ta

import pandas as pd

from bedrock.extract.allocation.epa_constants import (
    EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2022,
    EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2023,
    EPA_TABLE_NAMES,
    TBL_NUMBERS,
)
from bedrock.transform.allocation.utils import parse_index_with_aggregates
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_dir_for_gcs_sub_bucket


def _get_epa_data_year() -> int:
    """Get EPA main and annex table years from config"""
    year = get_usa_config().usa_ghg_data_year
    if year not in [2022, 2023]:
        raise ValueError(f"Unsupported EPA GHG data year: {year}")
    return year


def _get_epa_table_name_to_table_number_map() -> (
    ta.Mapping[EPA_TABLE_NAMES, TBL_NUMBERS]
):
    year = _get_epa_data_year()
    if year == 2022:
        return EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2022
    elif year == 2023:
        return EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2023
    else:
        raise ValueError(f"Unsupported EPA GHG data year: {year}")


def _get_gcs_epa_dir_for_table(tbl_name: TBL_NUMBERS) -> str:
    """Get GCS EPA directories based on config year"""
    year = _get_epa_data_year()
    section = tbl_name.split("-")[0]
    base = gcs_extract_input_path("EPA_GHGI", year)

    main_or_annex_dir = (
        {
            "main": "EPA_GHGI_2022_Main_Tables",
            "annex": "EPA_GHGI_2022_Annex_Tables",
        }
        if year == 2022
        else {
            "main": "EPA_GHGI_2023_Selected_Tables",
            "annex": posixpath.join("EPA_GHGI_2023_Selected_Tables", "Annex"),
        }
    )
    # # TODO: eventually go to the full set of csv tables for 2023
    # # replacing the above if else chunk
    # main_or_annex_dir = {
    #     "main": f"EPA_GHGI_{year}_Main_Tables",
    #     "annex": f"EPA_GHGI_{year}_Annex_Tables",
    # }

    if section == "A":
        return posixpath.join(base, main_or_annex_dir["annex"], f"Table {tbl_name}.csv")

    chapter_dir = (
        {
            1: "Introduction",
            2: "Trends in Greenhouse Gas Emissions and Removals",
            3: "Energy",
            4: "Industrial Processes",
            5: "Agriculture",
            6: "LULUCF",  # Land Use, Land-Use Change, and Forestry
            7: "Waste",
            9: "Recalculations and Improvements",
        }
        if year == 2022
        else {
            1: "Chapter 1 - Introduction",
            2: "Chapter 2 - Trends in Greenhouse Gas Emissions and Removals",
            3: "Chapter 3 - Energy",
            4: "Chapter 4 - Industrial Processes and Product Use",
            5: "Chapter 5 - Agriculture",
            6: "Chatper 6 - LULUCF",
            7: "Chapter 7 - Waste",
            9: "Chatper 9 - Recalculations and Improvements",
        }
    )
    return posixpath.join(
        base,
        main_or_annex_dir["main"],
        chapter_dir[int(section)],
        f"Table {tbl_name}.csv",
    )


def _map_special_string_to_zero_in_tbl(df: pd.DataFrame) -> pd.DataFrame:
    return df.map(
        lambda x: (
            0.0 if type(x) is str and x.strip() in ("NO", "+", "(+)", "-", "NE") else x
        )
    )


def _load_epa_tbl_from_gcs(
    tbl_name: TBL_NUMBERS, loader: ta.Optional[ta.Callable[[str], pd.DataFrame]] = None
) -> pd.DataFrame:
    table_dir = _get_gcs_epa_dir_for_table(tbl_name)
    gcs_sub_bucket = os.path.split(table_dir)[0]
    return load_from_gcs(
        name=os.path.split(table_dir)[-1],
        sub_bucket=gcs_sub_bucket,
        local_dir=local_dir_for_gcs_sub_bucket(gcs_sub_bucket),
        loader=loader or pd.read_csv,
    )


@functools.cache
def load_co2_emissions_from_fossil_fuels_for_non_energy_uses() -> pd.Series[float]:
    """Adjusted Non-Energy Use Fossil Fuel Consumption, Storage, and Emissions (MMT CO2)"""
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()[
                "co2_fossil_fuel_non_energy_uses"
            ],
            loader=lambda pth: pd.read_csv(
                pth,
                encoding="latin1",
                skiprows=2,
                index_col=0,
            ).iloc[:22, :],
        )
        .fillna(0.0)
        .replace(",", "", regex=True)
    ).astype(float)

    tbl.index = parse_index_with_aggregates(
        tbl.index,
        [
            "Industry",
            "Transportation",
            "U.S. Territories",
            "Total",
        ],
    )

    if _get_epa_data_year() == 2023:
        # column renames due to typos in 2022 table
        tbl = tbl.rename(
            index={
                "Natural Gas to Chemical\nPlants": "Natural Gas to Chemical Plants",
                "HGL": "HGL b",
                "Natural Gasoline": "Natural Gasoline c",
            },
        )

    assert tbl.index.is_unique
    return tbl.loc[:, "(MMT CO2 Eq.)"]


@functools.cache
def load_fuel_consumption_by_fuel_and_vehicle_type() -> pd.Series[float]:
    """
    Fuel Consumption by Fuel and Vehicle Type (million gallons unless otherwise specified)
    """
    total_fuel_usage_tbl = (
        _map_special_string_to_zero_in_tbl(
            _load_epa_tbl_from_gcs(
                _get_epa_table_name_to_table_number_map()[
                    "fuel_consumption_by_vehicle_type"
                ],
                lambda x: pd.read_csv(
                    x, encoding="latin1", skiprows=1, nrows=39, index_col=[0]
                ),
            )
            .fillna(0.0)
            .replace(",", "", regex=True)
        )
    ).astype(float)

    data_for_year = total_fuel_usage_tbl[str(_get_epa_data_year())]

    TOP_LEVEL_CATEGORIES = (
        [
            "Motor Gasolineb,c",
            "Distillate Fuel Oil (Diesel Fuel)b,c",
            "Jet Fuelf",
            "Aviation Gasolinef",
            "Residual Fuel Oilf, g",
            "Natural Gasf (trillion cubic feet)",
            "LPGf",
            "Electricityh,i",
        ]
        if _get_epa_data_year() == 2022
        else [
            "Motor Gasoline",
            "Distillate Fuel Oil\n(Diesel Fuel)^{b,c}",
            "Jet Fuel",
            "Aviation Gasoline",
            "Residual Fuel Oil",
            "Natural Gas (trillion cubic feet)",
            "LPG",
            "Electricity",
        ]
    )

    def _transform_to_multiindex(
        series: pd.Series[float], top_levels: list[str]
    ) -> pd.Series[float]:
        """
        Helper function for table a94. The table just has "subcategories" tabbed in once in the same column as the category,
        but not *all* of them are tabbed in. So, just predefine the top-level categories and use that to definitively set them.
        """
        multiindex_list = []
        current_top_level = None

        for item in series.index:
            if item in top_levels:
                current_top_level = item
                sub_category = ""
            else:
                sub_category = item
            multiindex_list.append((current_top_level, sub_category.strip()))

        multiindex = pd.MultiIndex.from_tuples(
            multiindex_list, names=["Top-Level Category", "Sub-Category"]
        )

        new_series = pd.Series(series.values, index=multiindex)
        return new_series

    total_fuel_usage = _transform_to_multiindex(
        data_for_year, TOP_LEVEL_CATEGORIES
    ).dropna()

    if _get_epa_data_year() == 2023:
        total_fuel_usage = total_fuel_usage.rename(
            index={
                "Motor Gasoline": "Motor Gasolineb,c",
                "Distillate Fuel Oil\n(Diesel Fuel)^{b,c}": "Distillate Fuel Oil (Diesel Fuel)b,c",
                "Jet Fuel": "Jet Fuelf",
                "Aviation Gasoline": "Aviation Gasolinef",
                "Residual Fuel Oil": "Residual Fuel Oilf, g",
                "Natural Gas (trillion cubic feet)": "Natural Gasf (trillion cubic feet)",
                "LPG": "LPGf",
                "Electricity": "Electricityh,i",
                "Aircraft": "General Aviation Aircraft",
            },
        )

        # other column renames due to typos in 2022 table
        specific_index_renames = {
            ("Motor Gasolineb,c", "Recreational Boats"): (
                "Motor Gasolineb,c",
                "Recreational Boatsd",
            ),
            ("Distillate Fuel Oil (Diesel Fuel)b,c", "Rail"): (
                "Distillate Fuel Oil (Diesel Fuel)b,c",
                "Raile",
            ),
        }
        total_fuel_usage.index = total_fuel_usage.index.map(
            lambda x: specific_index_renames.get(x, x)
        )

    assert total_fuel_usage.index.is_unique
    return total_fuel_usage


@functools.cache
def load_mmt_co2e_across_fuel_types() -> pd.DataFrame:
    """
    Energy Consumption Data and CO2 Emissions from Fossil Fuel Combustion by Fuel Type
    """
    return (
        _map_special_string_to_zero_in_tbl(
            _load_epa_tbl_from_gcs(
                _get_epa_table_name_to_table_number_map()[
                    "energy_consumption_co2_by_fuel_type"
                ],
                loader=lambda x: pd.read_csv(
                    x, encoding="latin1", skiprows=3, index_col=[0], nrows=33
                )
                .iloc[:, 7:-1]
                .fillna(0.0)
                .replace(",", "", regex=True)
                .rename(columns=lambda x: x.split(".")[0].strip())
                .rename(index=lambda x: x.strip()),
            )
        )
        .rename({"LPG (Propane)": "LPG"})
        .astype(float)
    )


@functools.cache
def load_tbtu_across_fuel_types() -> pd.DataFrame:
    """
    Energy Consumption Data by Fuel Type (TBtu) and Adjusted Energy Consumption Data
    """
    raw_table = _load_epa_tbl_from_gcs(
        _get_epa_table_name_to_table_number_map()[
            "energy_consumption_co2_by_fuel_type"
        ],
        loader=lambda x: pd.read_csv(
            # There are some bad bytes in the table
            x,
            skiprows=3,
            index_col=[0],
            nrows=34,
            encoding="latin1",
        ),
    )
    return _map_special_string_to_zero_in_tbl(
        raw_table.iloc[:, :7]
        .fillna(0.0)
        .rename(columns=lambda x: x.split(".")[0].strip())
        .rename(index=lambda x: x.strip())
        .rename({"LPG (Propane)": "LPG"})  # Make the name match the old table
        .replace(",", "", regex=True)
    ).astype(float)
