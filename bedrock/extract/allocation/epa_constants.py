from __future__ import annotations

import ast
import typing as ta
from typing import Dict, List

import pandas as pd

from bedrock.utils.config.settings import extractpath, transformpath

"""
NOTE: table numbers change year-over-year in the GHG inventory report.
For updates, refer to the documentation in each function for the contents of the table,
and find the updated table number in the latest GHG inventory.
"""
TBL_NUMBERS_2022 = ta.Literal[
    "2-1",
    "3-8",
    "3-9",
    "3-14",
    "3-15",
    "3-23",
    "3-24",
    "3-44",
    "3-46",
    "3-73",
    "3-75",
    "4-51",
    "4-55",
    "4-103",
    "4-121",
    "4-125",
    "4-127",
    "5-3",
    "5-6",
    "5-17",
    "5-18",
    "5-29",
    "A-4",
    "A-5",
    "A-68",
    "A-89",
]

TBL_NUMBERS_2023 = ta.Literal[
    "2-1",
    "3-8",
    "3-9",
    "3-14",
    "3-15",
    "3-25",
    "3-45",
    "3-47",
    "3-64",
    "3-66",
    "4-51",
    "4-55",
    "4-100",
    "4-118",
    "4-122",
    "4-124",
    "5-3",
    "5-7",
    "5-18",
    "5-19",
    "5-29",
    "A-5",
    "A-69",
    "A-90",
]

TBL_NUMBERS = ta.Union[TBL_NUMBERS_2022, TBL_NUMBERS_2023]

EPA_TABLE_NAMES = ta.Literal[
    "recent_trends_in_ghg_emissions_and_sinks",
    "ch4_stationary_combustion",
    "n2o_stationary_combustion",
    "ch4_mobile_combustion",
    "n2o_mobile_combustion",
    "co2_fossil_fuel_non_energy_uses",
    "ch4_petroleum_systems",
    "co2_petroleum_systems",
    "ch4_natural_gas_systems",
    "co2_natural_gas_systems",
    "co2_soda_ash_production",
    "co2_ch4_petrochemical_production",
    "pfc_aluminum_production",
    "pfc_hfc_sf6_nf3_n2o_semiconductor_manufacture",
    "hfc_ods_substitutes",
    "hfc_pfc_emissions_ods_substitutes_by_sector",
    "ch4_enteric_fermentation",
    "ch4_n2o_manure_management",
    "direct_n2o_agricultural_soils",
    "indirect_n2o_agricultural_soils",
    "ch4_n2o_field_burning",
    "energy_consumption_co2_by_fuel_type",
    "fuel_consumption_by_vehicle_type",
    "hfc_transportation_sources",
]

EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2022: ta.Dict[EPA_TABLE_NAMES, TBL_NUMBERS_2022] = {
    "recent_trends_in_ghg_emissions_and_sinks": "2-1",
    "ch4_stationary_combustion": "3-8",
    "n2o_stationary_combustion": "3-9",
    "ch4_mobile_combustion": "3-14",
    "n2o_mobile_combustion": "3-15",
    "co2_fossil_fuel_non_energy_uses": "3-24",
    "ch4_petroleum_systems": "3-44",
    "co2_petroleum_systems": "3-46",
    "ch4_natural_gas_systems": "3-73",
    "co2_natural_gas_systems": "3-75",
    "co2_soda_ash_production": "4-51",
    "co2_ch4_petrochemical_production": "4-55",
    "pfc_aluminum_production": "4-103",
    "pfc_hfc_sf6_nf3_n2o_semiconductor_manufacture": "4-121",
    "hfc_ods_substitutes": "4-125",
    "hfc_pfc_emissions_ods_substitutes_by_sector": "4-127",
    "ch4_enteric_fermentation": "5-3",
    "ch4_n2o_manure_management": "5-6",
    "direct_n2o_agricultural_soils": "5-17",
    "indirect_n2o_agricultural_soils": "5-18",
    "ch4_n2o_field_burning": "5-29",
    "energy_consumption_co2_by_fuel_type": "A-5",
    "fuel_consumption_by_vehicle_type": "A-68",
    "hfc_transportation_sources": "A-89",
}

EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2023: ta.Dict[EPA_TABLE_NAMES, TBL_NUMBERS_2023] = {
    "recent_trends_in_ghg_emissions_and_sinks": "2-1",
    "ch4_stationary_combustion": "3-8",
    "n2o_stationary_combustion": "3-9",
    "ch4_mobile_combustion": "3-14",
    "n2o_mobile_combustion": "3-15",
    "co2_fossil_fuel_non_energy_uses": "3-25",
    "ch4_petroleum_systems": "3-45",
    "co2_petroleum_systems": "3-47",
    "ch4_natural_gas_systems": "3-64",
    "co2_natural_gas_systems": "3-66",
    "co2_soda_ash_production": "4-51",
    "co2_ch4_petrochemical_production": "4-55",
    "pfc_aluminum_production": "4-100",
    "pfc_hfc_sf6_nf3_n2o_semiconductor_manufacture": "4-118",
    "hfc_ods_substitutes": "4-122",
    "hfc_pfc_emissions_ods_substitutes_by_sector": "4-124",
    "ch4_enteric_fermentation": "5-3",
    "ch4_n2o_manure_management": "5-7",
    "direct_n2o_agricultural_soils": "5-18",
    "indirect_n2o_agricultural_soils": "5-19",
    "ch4_n2o_field_burning": "5-29",
    "energy_consumption_co2_by_fuel_type": "A-5",
    "fuel_consumption_by_vehicle_type": "A-69",
    "hfc_transportation_sources": "A-90",
}


# ---------------------------------------------------------
# The following functions are used to run through all of the .py files within ceda allocation to
# determine the EPA GHGI Table numbers that are used to attribute data to sectors. The output of this
# information is used to compare the original CEDA method results to the CEDA method recreated within
# FLOWSA methodology
# ---------------------------------------------------------

# parse and cache the EPA script that includes function definitions for where data is loaded

with open(extractpath / "allocation/epa.py", "r") as f:
    EPA_TREE = ast.parse(f.read(), filename=str(extractpath / "allocation/epa.py"))

EPA_FUNCS = {
    node.name: node for node in ast.walk(EPA_TREE) if isinstance(node, ast.FunctionDef)
}

_table_cache: Dict[str, List[str]] = {}


# helper functions to run through related EPA functions to connect GHGI table numbers
# to the gas/source combos


def extract_table_names_from_loader(func_def: ast.FunctionDef) -> list[str]:

    if func_def.name in _table_cache:
        return _table_cache[func_def.name]

    table_names = set()

    for node in ast.walk(func_def):

        # Pattern 1: map()["A-17"]
        if isinstance(node, ast.Subscript):
            if (
                isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and node.value.func.id == "_get_epa_table_name_to_table_number_map"
            ):
                if isinstance(node.slice, ast.Constant) and isinstance(
                    node.slice.value, str
                ):
                    table_names.add(node.slice.value)

        # Pattern 2: load_xxx("A-17")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id.startswith("load_"):
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        table_names.add(arg.value)

        # Pattern 3: TABLE = "A-17"
        if isinstance(node, ast.Assign):
            if isinstance(node.value, ast.Constant) and isinstance(
                node.value.value, str
            ):
                table_names.add(node.value.value)

    result = sorted(table_names)
    _table_cache[func_def.name] = result
    return result


def build_alias_graph(tree: ast.AST) -> dict[str, str]:
    alias_map = {}

    for node in ast.walk(tree):

        # import X as Y
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "bedrock.extract.allocation.epa"
        ):
            for alias in node.names:
                local = alias.asname or alias.name
                alias_map[local] = alias.name

        # Y = X
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    alias_map[target.id] = node.value.id

        # Y = wrapper(X)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            if node.value.args:
                arg = node.value.args[0]
                if isinstance(arg, ast.Name):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            alias_map[target.id] = arg.id

    # resolve chains
    resolved = {}
    for local in alias_map:
        seen = set()
        cur = local
        while cur in alias_map and cur not in seen:
            seen.add(cur)
            cur = alias_map[cur]
        resolved[local] = cur

    return resolved


def extract_loader_calls(tree: ast.AST) -> set[str]:
    loaders = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func

            if isinstance(func, ast.Name) and func.id.startswith("load_"):
                loaders.add(func.id)

            elif isinstance(func, ast.Attribute) and func.attr.startswith("load_"):
                loaders.add(func.attr)

    return loaders


def return_emissions_source_table_numbers() -> pd.DataFrame:
    rows = []

    directories = [
        transformpath / "allocation/co2",
        transformpath / "allocation/ch4",
        transformpath / "allocation/other_gases",
    ]

    for directory in directories:
        for file in directory.glob("*.py"):
            if file.name.startswith("_"):
                continue

            emission_source = file.stem

            with open(file, "r") as f:
                tree = ast.parse(f.read(), filename=str(file))

            alias_graph = build_alias_graph(tree)
            loader_calls = extract_loader_calls(tree)

            # resolve aliases
            resolved_loaders = {alias_graph.get(name, name) for name in loader_calls}

            # extract table names
            table_names = []
            for loader in resolved_loaders:
                if loader in EPA_FUNCS:
                    table_names.extend(
                        extract_table_names_from_loader(EPA_FUNCS[loader])
                    )

            table_names = sorted(set(table_names))
            table_numbers = [
                "EPA_GHGI_T_"
                + EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2023[
                    ta.cast(EPA_TABLE_NAMES, name)
                ].replace("-", "_")
                for name in table_names
                if name in EPA_TABLE_NAME_TO_TABLE_NUMBER_MAP_2023
            ]

            rows.append(
                {
                    "emissions_source": emission_source,
                    "table_numbers": table_numbers,
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    max_len = df["table_numbers"].apply(len).max()
    for i in range(max_len):
        df[f"table_number_{i+1}"] = df["table_numbers"].apply(
            lambda lst, idx=i: lst[idx] if idx < len(lst) else None
        )

    return df.drop(columns=["table_numbers"])
