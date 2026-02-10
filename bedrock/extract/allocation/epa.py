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
from bedrock.utils.emissions.gwp import derive_ar5_to_ar6_multiplier
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.io.gcp_paths import GCS_CEDA_INPUT_DIR

IN_DIR = os.path.join(os.path.dirname(__file__), "..", "input_data")


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
        return posixpath.join(
            GCS_CEDA_INPUT_DIR, main_or_annex_dir["annex"], f"Table {tbl_name}.csv"
        )

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
        GCS_CEDA_INPUT_DIR,
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
    return load_from_gcs(
        name=os.path.split(table_dir)[-1],
        sub_bucket=os.path.split(table_dir)[0],
        local_dir=IN_DIR,
        loader=loader or pd.read_csv,
    )


@functools.cache
def load_n2o_emissions_from_stationary_combustion() -> pd.Series[float]:
    """N2O Emissions from Stationary Combustion (MMT CO2 Eq.)"""
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["n2o_stationary_combustion"],
            loader=lambda pth: pd.read_csv(
                pth,
                skiprows=1,
                index_col=0,
            ).iloc[:26, :],
        )
    ).astype(float)
    tbl.columns = tbl.columns.astype(int)
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        [
            "Electric Power",
            "Industrial",
            "Commercial",
            "Residential",
            "U.S. Territories",
            "Total",
        ],
    )
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["N2O"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_recent_trends_in_ghg_emissions_and_sinks() -> pd.Series[float]:
    """
    Recent Trends in U.S. Greenhouse Gas Emissions and Sinks (MMT CO2 Eq.)
    """
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()[
                "recent_trends_in_ghg_emissions_and_sinks"
            ],
            loader=lambda pth: pd.read_csv(
                pth,
                skiprows=1,
                index_col=0,
            ).iloc[:99, 1:],
        ).replace(",", "", regex=True)
    ).astype(float)

    tbl.columns = tbl.columns.astype(int)
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        (
            [
                "CO2",
                "CH4c",
                "N2Oc",
                "HFCs",
                "PFCs",
                "SF6",
                "NF3",
            ]
            if _get_epa_data_year() == 2022
            else [
                "CO2",
                "CH4",
                "N2O",
                "HFCs",
                "PFCs",
                "SF6",
                "NF3",
            ]
        ),
    )

    if _get_epa_data_year() == 2023:
        tbl = tbl.rename(
            index={
                "CH4": "CH4c",
                "N2O": "N2Oc",
                # other column renames due to typos in 2022 table
                "Carbide Production and\nConsumption": "Carbide Production and Consumption",
                "Abandoned Underground Coal\nMines": "Abandoned Underground Coal Mines",
                "Field Burning of Agricultural\nResidues": "Field Burning of Agricultural Residues",
                "Magnesium Production and\nProcessing": "Magnesium Production and Processing",
                "Anaerobic Digestion at Biogas\nFacilities": "Anaerobic Digestion at Biogas Facilities",
            },
        )
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        ar5_to_ar6_multiplier = derive_ar5_to_ar6_multiplier()
        # Differentiate between fossil and non-fossil CH4 sources
        CH4_NON_FOSSIL = [
            "Enteric Fermentation",
            "Landfills",
            "Manure Management",
            "Wastewater Treatment",
            "Rice Cultivation",
            "Composting",
            "Field Burning of Agricultural Residues",
            "Anaerobic Digestion at Biogas Facilities",
            "Incineration of Waste",
        ]
        CH4_SOURCE = final_tbl["CH4c"].index.to_list()
        CH4_FOSSIL = [source for source in CH4_SOURCE if source not in CH4_NON_FOSSIL]

        # Define a list of multipliers for where the multi-index is needed for clarity and maintainability
        # This list was determined by examining the usage of the load_recent_trends_in_ghg_emissions_and_sinks function
        multipliers_where_multi_index_is_needed = [
            (("CH4c", CH4_NON_FOSSIL), "CH4_non_fossil"),
            (("CH4c", CH4_FOSSIL), "CH4_fossil"),
            (("HFCs", ["Fluorochemical Production", "Electronics Industry"]), "HFC-23"),
            (("HFCs", ["Magnesium Production and Processing"]), "HFC-134a"),
        ]
        for idx, multiplier_key in multipliers_where_multi_index_is_needed:
            final_tbl.loc[idx] = final_tbl.loc[idx].mul(
                ar5_to_ar6_multiplier[multiplier_key]
            )
        # Apply multipliers for N2O, SF6, and NF3 where the multi-index is not needed
        final_tbl["N2Oc"] = final_tbl["N2Oc"].mul(ar5_to_ar6_multiplier["N2O"])
        final_tbl["SF6"] = final_tbl["SF6"].mul(ar5_to_ar6_multiplier["SF6"])
        final_tbl["NF3"] = final_tbl["NF3"].mul(ar5_to_ar6_multiplier["NF3"])

        return final_tbl
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_ch4_emissions_from_enteric_fermentation() -> pd.Series[float]:
    """CH4 Emissions from Enteric Fermentation (MMT CO2 Eq.)"""
    tbl = _load_epa_tbl_from_gcs(
        _get_epa_table_name_to_table_number_map()["ch4_enteric_fermentation"],
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=1,
            index_col=0,
        ).iloc[:9],
    )
    tbl.columns = tbl.columns.astype(int)
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["CH4_non_fossil"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_ch4_emissions_from_natural_gas_systems() -> pd.Series[float]:
    """CH4 Emissions from Natural Gas Systems (MMT CO2 Eq.)"""
    tbl = _load_epa_tbl_from_gcs(
        _get_epa_table_name_to_table_number_map()["ch4_natural_gas_systems"],
        loader=lambda pth: pd.read_csv(pth, skiprows=1, index_col=0),
    ).dropna()
    tbl.columns = tbl.columns.astype(int)
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["CH4_fossil"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
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
def load_co2_emissions_from_natural_gas_systems() -> pd.Series[float]:
    """CO2 Emissions from Natural Gas Systems (MMT)"""
    tbl = (
        _map_special_string_to_zero_in_tbl(
            _load_epa_tbl_from_gcs(
                _get_epa_table_name_to_table_number_map()["co2_natural_gas_systems"],
                loader=lambda pth: pd.read_csv(
                    pth,
                    skiprows=1,
                    index_col=0,
                ),
            )
        )
        .astype(float)
        .dropna()
    )
    tbl.columns = tbl.columns.astype(int)
    assert tbl.index.is_unique
    return tbl.loc[:, _get_epa_data_year()]


@functools.cache
def load_direct_n2o_from_agricultural_soils() -> pd.Series[float]:
    """Direct N2O from Agricultural Soils"""
    tbl = (
        _map_special_string_to_zero_in_tbl(
            _load_epa_tbl_from_gcs(
                _get_epa_table_name_to_table_number_map()[
                    "direct_n2o_agricultural_soils"
                ],
                loader=lambda pth: pd.read_csv(
                    pth,
                    skiprows=1,
                    index_col=0,
                ),
            )
        )
        .astype(float)
        .dropna()
    )
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        [
            "Cropland",
            "Grassland",
            "Total",
        ],
    )
    tbl.columns = tbl.columns.astype(int)

    if _get_epa_data_year() == 2023:
        # column renames due to typos in 2022 table
        tbl = tbl.rename(
            index={
                "Organic Amendment": "Organic Amendment a",
                "Residue N": "Residue N b",
            },
        )
    assert tbl.index.is_unique
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["N2O"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_indirect_n2o_from_agricultural_soils() -> pd.Series[float]:
    """Indirect N2O from Agricultural Soils (MMT CO2 Eq.)"""
    tbl = (
        _map_special_string_to_zero_in_tbl(
            _load_epa_tbl_from_gcs(
                _get_epa_table_name_to_table_number_map()[
                    "indirect_n2o_agricultural_soils"
                ],
                loader=lambda pth: pd.read_csv(
                    pth,
                    skiprows=1,
                    index_col=0,
                ),
            )
        )
        .astype(float)
        .dropna()
    )

    tbl.index = parse_index_with_aggregates(
        tbl.index,
        [
            "Cropland",
            "Grassland",
            "Total",
        ],
    )
    tbl.columns = tbl.columns.astype(int)
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    assert tbl.index.is_unique
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["N2O"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_ch4_and_n2o_emissions_from_manure_management() -> pd.Series[float]:
    """CH4 and N2O Emissions from Manure Management (MMT CO2 Eq.)"""

    def loader(pth: str) -> pd.DataFrame:
        if _get_epa_data_year() == 2022:
            return pd.read_csv(pth, skiprows=1, index_col=0).iloc[:21, 1:]
        else:
            return pd.read_csv(pth, skiprows=1, index_col=0).iloc[:21, 4:]

    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["ch4_n2o_manure_management"],
            loader=loader,
        )
    ).astype(float)
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        (
            ["CH4a", "N2Ob", "Total"]
            if _get_epa_data_year() == 2022
            else ["CH4", "N2O", "Total"]
        ),
    )

    # TODO: When we delete the 2022 codepath, we can go to the callsites of this function
    # and update the index to use the new index names instead of renaming
    if _get_epa_data_year() == 2023:
        tbl = tbl.rename(index={"CH4": "CH4a", "N2O": "N2Ob"}, level=0)

    tbl.columns = tbl.columns.astype(int)
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        ar5_to_ar6 = derive_ar5_to_ar6_multiplier()
        final_tbl["CH4a"] = final_tbl["CH4a"] * ar5_to_ar6["CH4_non_fossil"]
        final_tbl["N2Ob"] = final_tbl["N2Ob"] * ar5_to_ar6["N2O"]
        return final_tbl
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_ch4_emissions_from_petroleum_systems() -> pd.Series[float]:
    """CH4 Emissions from Petroleum Systems (MMT CO2 Eq.)"""
    tbl = _load_epa_tbl_from_gcs(
        _get_epa_table_name_to_table_number_map()["ch4_petroleum_systems"],
        loader=lambda pth: pd.read_csv(pth, skiprows=1, index_col=0),
    ).dropna()
    tbl.columns = tbl.columns.astype(int)

    if _get_epa_data_year() == 2023:
        # column renames due to typos in 2022 table
        tbl = tbl.rename(
            index={
                "Total": "Total ",
            },
        )
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["CH4_fossil"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_co2_emissions_from_petroleum_systems() -> pd.Series[float]:
    """CO2 Emissions from Petroleum Systems (MMT CO2)"""
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["co2_petroleum_systems"],
            loader=lambda pth: pd.read_csv(pth, skiprows=1, index_col=0).iloc[:5],
        )
    ).astype(float)
    tbl.columns = tbl.columns.astype(int)

    if _get_epa_data_year() == 2023:
        tbl = tbl.rename(
            index={
                # column renames due to typos in 2022 table
                "Production": "Production ",
            },
        )

    return tbl.loc[:, _get_epa_data_year()]


@functools.cache
def load_ch4_and_n2o_from_field_burning() -> pd.Series[float]:
    """
    if config year 2022:
        CH4, N2O, CO, and NOx Emissions from Field Burning of Agricultural Residues (kt)
    if config year 2023:
        CH4 and N2O Emissions from Field Burning of Agricultural Residues (MMT CO2 Eq.)
    """

    def loader(pth: str) -> pd.DataFrame:
        if _get_epa_data_year() == 2022:
            return pd.read_csv(pth, skiprows=1, index_col=0).iloc[:-2, :]
        else:
            return pd.read_csv(pth, skiprows=1, index_col=0)

    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["ch4_n2o_field_burning"],
            loader=loader,
        )
    ).astype(float)
    tbl.columns = tbl.columns.astype(int)
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        [
            "CH4",
            "N2O",
        ],
    )
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        ar5_to_ar6 = derive_ar5_to_ar6_multiplier()
        final_tbl["CH4"] = final_tbl["CH4"] * ar5_to_ar6["CH4_non_fossil"]
        final_tbl["N2O"] = final_tbl["N2O"] * ar5_to_ar6["N2O"]
        return final_tbl
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_ch4_emissions_from_stationary_combustion() -> pd.Series[float]:
    """CH4 Emissions from Stationary Combustion (MMT CO2 Eq.)"""

    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["ch4_stationary_combustion"],
            lambda pth: pd.read_csv(pth, skiprows=1, index_col=0).iloc[:26, :],
        )
    ).astype(float)
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        [
            "Electric Power",
            "Industrial",
            "Commercial",
            "Residential",
            "U.S. Territories",
            "Total",
        ],
    )
    tbl.columns = tbl.columns.astype(int)
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["CH4_fossil"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_n2o_emissions_from_mobile_combustion() -> pd.Series[float]:
    """N2O Emissions from Mobile Combustion (MMT CO2 Eq.)"""
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["n2o_mobile_combustion"],
            loader=lambda pth: pd.read_csv(
                pth,
                encoding="latin1",
                skiprows=1,
                index_col=0,
            ),
        )
    ).astype(float)
    tbl.columns = tbl.columns.astype(int)
    # NOTE: 2022 table doesn't have value for "Medium- and Heavy-Duty Trucks and Buses",
    # so we fill it with 2021 value here
    tbl.loc["Medium- and Heavy-Duty Trucks and Buses", _get_epa_data_year()] = tbl.loc[
        "Medium- and Heavy-Duty Trucks and Buses", 2021
    ]
    tbl = tbl.dropna()
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        (
            [
                "Gasoline On-Road b",
                "Diesel On-Road b",
                "Alternative Fuel On-Road",
                "Non-Road c",
                "Total",
            ]
            if _get_epa_data_year() == 2022
            else [
                "Gasoline On-Road",
                "Diesel On-Road",
                "Alternative Fuel On-Road",
                "Non-Road",
                "Total",
            ]
        ),
    )

    # TODO: When we delete the 2022 codepath, we can go to the callsites of this function
    # and update the index to use the new index names instead of renaming
    if _get_epa_data_year() == 2023:
        tbl = tbl.rename(
            index={
                "Gasoline On-Road": "Gasoline On-Road b",
                "Diesel On-Road": "Diesel On-Road b",
                "Non-Road": "Non-Road c",
                "Agricultural Equipment": "Agricultural Equipment e",
                # other column renames due to typos in 2022 table
                "Construction/Mining Equipment": "Construction/Mining Equipment f",
                "Aircraft": "Aircraft ",
                "Rail": "Raild",
                "Other": "Other g",
            },
        )

    assert tbl.index.is_unique
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["N2O"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_co2_emissions_from_petrochemical_production() -> pd.Series[float]:
    tbl = (
        _map_special_string_to_zero_in_tbl(
            _load_epa_tbl_from_gcs(
                _get_epa_table_name_to_table_number_map()[
                    "co2_ch4_petrochemical_production"
                ],
                loader=lambda pth: pd.read_csv(
                    pth,
                    skiprows=1,
                    index_col=0,
                ),
            )
        )
        .replace(",", "", regex=True)
        .astype(float)
    )
    tbl.index = tbl.index.str.strip()
    tbl.columns = tbl.columns.astype(int)
    return tbl.loc["CO2":"CH4", _get_epa_data_year()][1:-1]  # type: ignore


@functools.cache
def load_ch4_emissions_from_petrochemical_production() -> pd.Series[float]:
    tbl = (
        _map_special_string_to_zero_in_tbl(
            _load_epa_tbl_from_gcs(
                _get_epa_table_name_to_table_number_map()[
                    "co2_ch4_petrochemical_production"
                ],
                loader=lambda pth: pd.read_csv(
                    pth,
                    skiprows=1,
                    index_col=0,
                ),
            )
        )
        .replace(",", "", regex=True)
        .astype(float)
    )
    tbl.index = tbl.index.str.strip()
    tbl.columns = tbl.columns.astype(int)
    final_tbl = tbl.loc["CH4":"Total", _get_epa_data_year()][1:-1]  # type: ignore
    assert isinstance(final_tbl, pd.Series)
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["CH4_fossil"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_co2_emissions_from_soda_ash_prodution() -> pd.Series[float]:
    """CO2 Emissions from Soda Ash Production (MMT CO2 Eq.)"""
    tbl = _load_epa_tbl_from_gcs(
        _get_epa_table_name_to_table_number_map()["co2_soda_ash_production"],
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=1,
            index_col=0,
        ),
    )
    tbl.columns = tbl.columns.astype(int)
    return tbl.loc[:, _get_epa_data_year()]


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
def load_ch4_emissions_from_mobile_combustion() -> pd.Series[float]:
    """CH4 Emissions from Mobile Combustion (MMT CO2 Eq.)"""
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["ch4_mobile_combustion"],
            loader=lambda pth: pd.read_csv(
                pth, encoding="latin1", skiprows=1, index_col=0
            ).iloc[:19, :],
        )
    ).astype(float)
    tbl.columns = tbl.columns.astype(int)
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        (
            [
                "Gasoline On-Road b",
                "Diesel On-Road b",
                "Alternative Fuel On-Road",
                "Non-Road c",
                "Total",
            ]
            if _get_epa_data_year() == 2022
            else [
                "Gasoline On-Road",
                "Diesel On-Road",
                "Alternative Fuel On-Road",
                "Non-Road",
                "Total",
            ]
        ),
    )

    if _get_epa_data_year() == 2023:
        tbl = tbl.rename(
            index={
                "Gasoline On-Road": "Gasoline On-Road b",
                "Diesel On-Road": "Diesel On-Road b",
                "Non-Road": "Non-Road c",
                # other column renames due to typos in 2022 table
                "Agricultural Equipment": "Agricultural Equipmente",
                "Construction/Mining Equipment": "Construction/Mining Equipmentf",
                "Other": "Other g",
                "Rail": "Rail d",
            },
        )
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        return final_tbl * derive_ar5_to_ar6_multiplier()["CH4_fossil"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_hfc_emissions_from_transportation_sources() -> pd.Series[float]:
    """
    HFC Emissions from Transportation Sources (MMT CO2 Eq.)
    """
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["hfc_transportation_sources"],
            loader=lambda pth: pd.read_csv(
                pth,
                skiprows=1,
                index_col=0,
            ).iloc[:13, :],
        )
    )
    tbl.columns = pd.Index([int(float(x)) for x in tbl.columns])
    tbl.index = parse_index_with_aggregates(
        tbl.index,
        [
            "Mobile AC",
            "Comfort Cooling for Trains and Buses",
            "Refrigerated Transport",
            "Total",
        ],
    )
    final_tbl = tbl.astype(float).loc[:, _get_epa_data_year()]

    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        # Assume all HFCs used for cooling in transportation are HFC-134a
        return final_tbl * derive_ar5_to_ar6_multiplier()["HFC-134a"]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_hfc_emissions_from_ods_substitutes() -> pd.Series[float]:
    """
    Emissions of HFCs from ODS substitutes
    Output from this function is only used for determining the ratio of HFCs among all ODS substitutes,
    therefore no AR5 or AR6 multiplier is applied.
    """
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["hfc_ods_substitutes"],
            loader=lambda pth: pd.read_csv(
                pth,
                skiprows=1,
                index_col=0,
            ).dropna(),
        )
    ).astype(float)
    tbl.columns = tbl.columns.astype(int)
    assert tbl.index.is_unique
    return tbl.loc[:, _get_epa_data_year()]


@functools.cache
def load_hfc_pfc_emissions_from_ods_substitutes() -> pd.Series[float]:
    """
    Emissions of HFCs, PFCs, and CO2 from ODS Substitutes (MMT CO2 Eq.) by Sector
    """
    tbl = _load_epa_tbl_from_gcs(
        _get_epa_table_name_to_table_number_map()[
            "hfc_pfc_emissions_ods_substitutes_by_sector"
        ],
        loader=lambda pth: pd.read_csv(
            pth,
            skiprows=1,
            index_col=0,
        ).iloc[:13, :],
    )
    tbl.columns = tbl.columns.astype(int)
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        ar5_to_ar6_multiplier = pd.Series(derive_ar5_to_ar6_multiplier())
        # According to Table 4-124 in the EPA GHG Inventory report:
        # https://library.edf.org/AssetLink/145ky510ew61fk1tq5c2klp5kq5yp33j.pdf#page=465,
        # different ODS substitutes cause different HFC emissions, so we need to distinguish between them.
        # Refrigerants primarily causes HFC-134a emissions
        REFRIGERANT_USE_CASES = [
            "Refrigeration/Air Conditioning",
            "Commercial Refrigeration",
            "Domestic Refrigeration",
            "Industrial Process Refrigeration",
            "Transport Refrigeration",
            "Mobile Air Conditioning",
            "Residential Stationary Air Conditioning",
            "Commercial Stationary Air Conditioning",
        ]
        final_tbl[REFRIGERANT_USE_CASES] = (
            final_tbl[REFRIGERANT_USE_CASES] * ar5_to_ar6_multiplier["HFC-134a"]
        )
        # Aerosols causes HFC-143a and HFC-227ea emissions, so we need to average them
        final_tbl["Aerosols"] = (
            final_tbl["Aerosols"]
            * ar5_to_ar6_multiplier[["HFC-143a", "HFC-227ea"]].mean()
        )
        # Foams causes HFC-134a and HFC-245fa emissions, so we need to average them
        final_tbl["Foams"] = (
            final_tbl["Foams"] * ar5_to_ar6_multiplier[["HFC-134a", "HFC-245fa"]].mean()
        )
        # Solvents causes HFC-43-10mee, HFC-365mfc, HFC-245fa emissions, so we need to average them
        final_tbl["Solvents"] = (
            final_tbl["Solvents"]
            * ar5_to_ar6_multiplier[["HFC-43-10mee", "HFC-365mfc", "HFC-245fa"]].mean()
        )
        # Fire protection materials causes HFC-125 emissions
        final_tbl["Fire Protection"] = (
            final_tbl["Fire Protection"] * ar5_to_ar6_multiplier["HFC-125"]
        )

        return (
            tbl.loc[:, _get_epa_data_year()]
            * derive_ar5_to_ar6_multiplier()["HFC-134a"]
        )
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_pfc_emissions_from_aluminum_production() -> pd.Series[float]:
    """
    PFC Emissions from Aluminum Production
    """
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()["pfc_aluminum_production"],
            loader=lambda pth: pd.read_csv(
                pth,
                skiprows=1,
                index_col=0,
            ).dropna(),
        )
    ).astype(float)
    tbl.columns = tbl.columns.astype(int)
    assert tbl.index.is_unique
    final_tbl = tbl.loc[:, _get_epa_data_year()]

    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        ar5_to_ar6_multiplier = pd.Series(derive_ar5_to_ar6_multiplier())
        SHARED_HFC_GASES = [
            gas
            for gas in ar5_to_ar6_multiplier.index
            if gas in ar5_to_ar6_multiplier.index and gas in final_tbl.index
        ]
        return final_tbl.loc[SHARED_HFC_GASES] * ar5_to_ar6_multiplier[SHARED_HFC_GASES]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


@functools.cache
def load_pfc_hfc_sf6_nf3_n2o_emissions_from_semiconductor_manufacture() -> (
    pd.Series[float]
):
    """
    PFC, HFC, SF6, NF3, and N2O Emissions from Electronics Industry (MMT CO2e)
    """
    tbl = _map_special_string_to_zero_in_tbl(
        _load_epa_tbl_from_gcs(
            _get_epa_table_name_to_table_number_map()[
                "pfc_hfc_sf6_nf3_n2o_semiconductor_manufacture"
            ],
            loader=lambda pth: pd.read_csv(
                pth,
                skiprows=2,
                index_col=0,
            ).iloc[:13, :],
        )
    )
    tbl = tbl.astype(float)
    tbl.columns = tbl.columns.astype(int)

    if _get_epa_data_year() == 2023:
        tbl = tbl.rename(
            index={
                "C3F8": "C3F8 ",
            },
        )

    assert tbl.index.is_unique
    final_tbl = tbl.loc[:, _get_epa_data_year()]
    if get_usa_config().ipcc_ar_version == "AR5":
        return final_tbl
    elif get_usa_config().ipcc_ar_version == "AR6":
        ar5_to_ar6_multiplier = pd.Series(derive_ar5_to_ar6_multiplier())
        ar5_to_ar6_multiplier = ar5_to_ar6_multiplier.rename(
            index={"c-C4F8": "C4F8", "C3F8": "C3F8 "},
        )
        SHARED_HFC_GASES = [
            gas
            for gas in ar5_to_ar6_multiplier.index
            if gas in ar5_to_ar6_multiplier.index and gas in final_tbl.index
        ]
        return final_tbl.loc[SHARED_HFC_GASES] * ar5_to_ar6_multiplier[SHARED_HFC_GASES]
    else:
        raise ValueError(
            f"Unsupported IPCC AR version: {get_usa_config().ipcc_ar_version}"
        )


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
