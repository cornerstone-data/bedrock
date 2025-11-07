"""
Emissions sources

 * Emissions source category created from: https://docs.google.com/spreadsheets/d/1qBsIhrw1es_VF_WbmViCfXcz7iNC4ArQ/edit?usp=drive_link&ouid=109874530742529012238&rtpof=true&sd=true
 * Category mapping based on: https://docs.google.com/spreadsheets/d/1iBRJesKBiyQYC1i1mQTbAkuAuwrbxNKMYVQi6Ii1_R4/edit#gid=1583025027
 * Based on this mapping between source -> category: https://docs.google.com/spreadsheets/d/1iBRJesKBiyQYC1i1mQTbAkuAuwrbxNKMYVQi6Ii1_R4/edit#gid=1583025027
"""

from __future__ import annotations

import enum
import typing as ta

from ceda_usa.utils.taxonomy.bea.v2012_industry import BEA_2012_INDUSTRY_CODE

COMMERCIAL_FUEL_OIL_AND_NATURAL_GAS_SECTORS: ta.List[BEA_2012_INDUSTRY_CODE] = [
    "221300",
    "423100",
    "423400",
    "423600",
    "423800",
    "423A00",
    "424200",
    "424400",
    "424700",
    "424A00",
    "425000",
    "4200ID",
    "441000",
    "445000",
    "452000",
    "444000",
    "446000",
    "447000",
    "448000",
    "454000",
    "4B0000",
    "493000",
    "511110",
    "511120",
    "511130",
    "5111A0",
    "511200",
    "512100",
    "512200",
    "515100",
    "515200",
    "517110",
    "517210",
    "517A00",
    "518200",
    "519130",
    "5191A0",
    "522A00",
    "52A000",
    "523900",
    "523A00",
    "524113",
    "5241XX",
    "524200",
    "525000",
    "531HSO",
    "531HST",
    "531ORE",
    "532100",
    "532400",
    "532A00",
    "533000",
    "541100",
    "541511",
    "541512",
    "54151A",
    "541200",
    "541300",
    "541610",
    "5416A0",
    "541700",
    "541800",
    "541400",
    "541920",
    "541940",
    "5419A0",
    "550000",
    "561300",
    "561700",
    "561100",
    "561200",
    "561400",
    "561500",
    "561600",
    "561900",
    "562000",
    "611100",
    "611A00",
    "611B00",
    "621100",
    "621200",
    "621300",
    "621400",
    "621500",
    "621600",
    "621900",
    "622000",
    "623A00",
    "623B00",
    "624100",
    "624400",
    "624A00",
    "711100",
    "711200",
    "711500",
    "711A00",
    "712000",
    "713100",
    "713200",
    "713900",
    "721000",
    "722110",
    "722211",
    "722A00",
    "811100",
    "811200",
    "811300",
    "811400",
    "812100",
    "812200",
    "812300",
    "812900",
    "813100",
    "813A00",
    "813B00",
    "814000",
    "S00102",
    "GSLGE",
    "GSLGH",
]


ALLOCATED_GAS = ta.Literal[
    "CO2",
    "CH4",
    "N2O",
    "HFC-23",
    "HFC-32",
    "HFC-125",
    "HFC-134a",
    "HFC-143a",
    "HFC-236fa",
    "CF4",
    "C2F6",
    "C3F8",
    "C4F8",
    "SF6",
    "NF3",
]
ALLOCATED_GASES: ta.List[ALLOCATED_GAS] = list(ta.get_args(ALLOCATED_GAS))


@enum.unique
class EmissionsSourceCategory(str, enum.Enum):
    def __new__(
        cls, *args: ta.Any, **kwargs: ta.Dict[str, ta.Any]
    ) -> EmissionsSourceCategory:
        obj = str.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(self, code: str, description: str) -> None:
        self._code = code
        self._description = description

    def __str__(self) -> str:
        return self.value

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EmissionsSourceCategory):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return NotImplemented

    @property
    def description(self) -> str:
        return self._description

    coal_mining_oil_and_gas_extraction = (
        "coal_mining_oil_and_gas_extraction",
        "Coal mining, Oil & Gas extraction",
    )
    waste_management = "waste_management", "Waste management"
    enteric_fermentation = "enteric_fermentation", "Enteric fermentation"
    other_industrial_processes = (
        "other_industrial_processes",
        "Other Industrial processes",
    )
    land_management = "land_management", "Land management"
    other_transport = "other_transport", "Other transport"
    iron_and_steel = "iron_and_steel", "Iron and steel"
    landfills = "landfills", "Landfills"
    natural_gas_system = "natural_gas_system", "Natural gas system"
    petroleum_system = "petroleum_system", "Petroleum system"
    commercial_fuel_combustion = (
        "commercial_fuel_combustion",
        "Commercial fuel combustion",
    )
    electricity_generation = "electricity_generation", "Electricity generation"
    industrial_fuel_combustion = (
        "industrial_fuel_combustion",
        "Industrial fuel combustion",
    )
    residential_fuel_combustion = (
        "residential_fuel_combustion",
        "Residential fuel combustion",
    )
    aviation = "aviation", "Aviation"
    disel_combustion_for_transportation = (
        "disel_combustion_for_transportation",
        "Disel combustion for transportation",
    )
    gasoline_combustion_for_transportation = (
        "gasoline_combustion_for_transportation",
        "Gasoline combustion for transportation",
    )
    other_fossil_fuel_combustion = (
        "other_fossil_fuel_combustion",
        "Other fossil fuel combustion",
    )
    cement_production = "cement_production", "Cement production"
    marine_bunker_fuel_combustion = (
        "marine_bunker_fuel_combustion",
        "Marine bunker fuel combustion",
    )
    biomass_and_biomass_derived_fuel_combustion = (
        "biomass_and_biomass_derived_fuel_combustion",
        "Biomass and biomass-derived fuel combustion",
    )
    refrigerants = "refrigerants", "Refrigerants"


@enum.unique
class EmissionsSource(str, enum.Enum):
    def __new__(cls, *args: ta.Any, **kwargs: ta.Dict[str, ta.Any]) -> EmissionsSource:
        obj = str.__new__(cls)
        obj._value_ = args[0]
        return obj

    def __init__(
        self,
        code: str,
        description: str,
        category: EmissionsSourceCategory,
        gas: ALLOCATED_GAS,
    ) -> None:
        assert gas in ALLOCATED_GASES, f'unregonized gas "{gas}"'
        self._code = code
        self._description = description
        self._category = category
        self._gas = gas

    def __str__(self) -> str:
        return self.value

    def __eq__(self, other: object) -> bool:
        if isinstance(other, EmissionsSource):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return NotImplemented

    @property
    def description(self) -> str:
        return self._description

    @property
    def category(self) -> EmissionsSourceCategory:
        return self._category

    @property
    def gas(self) -> ALLOCATED_GAS:
        return self._gas

    def __hash__(self) -> int:
        return hash(self.value)

    # CO2
    co2_electricity_generation = (
        "co2_electricity_generation",
        "CO2 Electricity Generation",
        EmissionsSourceCategory.electricity_generation,
        "CO2",
    )
    co2_transportation_natural_gas = (
        "co2_transportation_natural_gas",
        "CO2 Transportation Natural Gas",
        EmissionsSourceCategory.other_transport,
        "CO2",
    )
    co2_transportation_aviation_gasoline = (
        "co2_transportation_aviation_gasoline",
        "CO2 Transportation Aviation Gasoline",
        EmissionsSourceCategory.aviation,
        "CO2",
    )
    co2_transportation_distillate_fuel_oil = (
        "co2_transportation_distillate_fuel_oil",
        "CO2 Transportation Distillate Fuel Oil",
        EmissionsSourceCategory.disel_combustion_for_transportation,
        "CO2",
    )
    co2_transportation_jet_fuel = (
        "co2_transportation_jet_fuel",
        "CO2 Transportation Jet Fuel",
        EmissionsSourceCategory.aviation,
        "CO2",
    )
    co2_transportation_lpg = (
        "co2_transportation_lpg",
        "CO2 Transportation LPG",
        EmissionsSourceCategory.other_transport,
        "CO2",
    )
    co2_transportation_motor_gasoline = (
        "co2_transportation_motor_gasoline",
        "CO2 Transportation Motor Gasoline",
        EmissionsSourceCategory.gasoline_combustion_for_transportation,
        "CO2",
    )
    co2_transportation_residual_fuel = (
        "co2_transportation_residual_fuel",
        "CO2 Transportation Residual Fuel",
        EmissionsSourceCategory.other_fossil_fuel_combustion,
        "CO2",
    )
    co2_industrial_coal = (
        "co2_industrial_coal",
        "CO2 Industrial Coal",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "CO2",
    )
    co2_industrial_natural_gas = (
        "co2_industrial_natural_gas",
        "CO2 Industrial Natural Gas",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "CO2",
    )
    co2_industrial_petrol = (
        "co2_industrial_petrol",
        "CO2 Industrial Petrol",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "CO2",
    )
    co2_residential = (
        "co2_residential",
        "CO2 Residential",
        EmissionsSourceCategory.residential_fuel_combustion,
        "CO2",
    )
    co2_commercial_coal = (
        "co2_commercial_coal",
        "CO2 Commercial Coal",
        EmissionsSourceCategory.commercial_fuel_combustion,
        "CO2",
    )
    co2_commercial_natural_gas = (
        "co2_commercial_natural_gas",
        "CO2 Commercial Natural Gas",
        EmissionsSourceCategory.commercial_fuel_combustion,
        "CO2",
    )
    co2_commercial_petrol = (
        "co2_commercial_petrol",
        "CO2 Commercial Petrol",
        EmissionsSourceCategory.commercial_fuel_combustion,
        "CO2",
    )
    co2_us_territories = (
        "co2_us_territories",
        "CO2 U.S. Territories",
        EmissionsSourceCategory.other_fossil_fuel_combustion,
        "CO2",
    )
    co2_non_energy_fuels_coal_coke = (
        "co2_non_energy_fuels_coal_coke",
        "CO2 Non-Energy Use of Fuels Coal&coke",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_non_energy_fuels_natural_gas = (
        "co2_non_energy_fuels_natural_gas",
        "CO2 Non-Energy Use of Fuels Natural gas",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_non_energy_fuels_petrol = (
        "co2_non_energy_fuels_petrol",
        "CO2 Non-Energy Use of Fuels Petrol",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "CO2",
    )
    co2_non_energy_fuels_transport = (
        "co2_non_energy_fuels_transport",
        "CO2 Non-Energy Use of Fuels Transport",
        EmissionsSourceCategory.other_transport,
        "CO2",
    )
    co2_iron_and_steel_production_and_metallurgical_coke_production = (
        "co2_iron_and_steel_production_and_metallurgical_coke_production",
        "CO2 Iron and Steel Production & Metallurgical Coke Production",
        EmissionsSourceCategory.iron_and_steel,
        "CO2",
    )
    co2_natural_gas_systems = (
        "co2_natural_gas_systems",
        "CO2 Natural Gas Systems",
        EmissionsSourceCategory.natural_gas_system,
        "CO2",
    )
    co2_cement_production = (
        "co2_cement_production",
        "CO2 Cement Production",
        EmissionsSourceCategory.cement_production,
        "CO2",
    )
    co2_petrochemical_production = (
        "co2_petrochemical_production",
        "CO2 Petrochemical Production",
        EmissionsSourceCategory.other_fossil_fuel_combustion,
        "CO2",
    )
    co2_lime_production = (
        "co2_lime_production",
        "CO2 Lime Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_ammonia_production = (
        "co2_ammonia_production",
        "CO2 Ammonia Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_incineration_of_waste = (
        "co2_incineration_of_waste",
        "CO2 Incineration of Waste",
        EmissionsSourceCategory.waste_management,
        "CO2",
    )
    co2_petroleum_systems = (
        "co2_petroleum_systems",
        "CO2 Petroleum Systems",
        EmissionsSourceCategory.petroleum_system,
        "CO2",
    )
    co2_liming_of_agricultural_soils = (
        "co2_liming_of_agricultural_soils",
        "CO2 Liming of Agricultural Soils",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_urea_consumption_for_nonagricultural_purposes = (
        "co2_urea_consumption_for_nonagricultural_purposes",
        "CO2 Urea Consumption for Non-Agricultural Purposes",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_other_process_uses_of_carbonates = (
        "co2_other_process_uses_of_carbonates",
        "CO2 Other Process Uses of Carbonates",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_urea_fertilization = (
        "co2_urea_fertilization",
        "CO2 Urea Fertilization",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_aluminum_production = (
        "co2_aluminum_production",
        "CO2 Aluminum Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_soda_ash_production_and_consumption = (
        "co2_soda_ash_production_and_consumption",
        "CO2 Soda Ash Production and Consumption",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_ferroalloy_production = (
        "co2_ferroalloy_production",
        "CO2 Ferroalloy Production",
        EmissionsSourceCategory.iron_and_steel,
        "CO2",
    )
    co2_titanium_dioxide_production = (
        "co2_titanium_dioxide_production",
        "CO2 Titanium Dioxide Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_zinc_production = (
        "co2_zinc_production",
        "CO2 Zinc Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_phosphoric_acid_production = (
        "co2_phosphoric_acid_production",
        "CO2 Phosphoric Acid Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_glass_production = (
        "co2_glass_production",
        "CO2 Glass Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_carbon_dioxide_consumption = (
        "co2_carbon_dioxide_consumption",
        "CO2 Carbon Dioxide Consumption",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_peatlands_remaining_peatlands = (
        "co2_peatlands_remaining_peatlands",
        "CO2 Peatlands Remaining Peatlands",
        EmissionsSourceCategory.land_management,
        "CO2",
    )
    co2_lead_production = (
        "co2_lead_production",
        "CO2 Lead Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_silicon_carbide_production_and_consumption = (
        "co2_silicon_carbide_production_and_consumption",
        "CO2 Silicon Carbide Production and Consumption",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_magnesium_production_and_processing = (
        "co2_magnesium_production_and_processing",
        "CO2 Magnesium Production and Processing",
        EmissionsSourceCategory.other_industrial_processes,
        "CO2",
    )
    co2_land_use_land_use_change_and_forestry_sinks = (
        "co2_land_use_land_use_change_and_forestry_sinks",
        "CO2 Land Use, Land-Use Change, and Forestry (Sink)a",
        EmissionsSourceCategory.land_management,
        "CO2",
    )
    co2_wood_biomass_and_ethanol_consumption = (
        "co2_wood_biomass_and_ethanol_consumption",
        "CO2 Wood Biomass and Ethanol Consumptionb",
        EmissionsSourceCategory.biomass_and_biomass_derived_fuel_combustion,
        "CO2",
    )
    co2_international_bunker_fuels = (
        "co2_international_bunker_fuels",
        "CO2 International Bunker Fuelsc",
        EmissionsSourceCategory.marine_bunker_fuel_combustion,
        "CO2",
    )
    ch4_enteric_fermentation = (
        "ch4_enteric_fermentation",
        "CH4 Enteric Fermentation",
        EmissionsSourceCategory.enteric_fermentation,
        "CH4",
    )
    ch4_natural_gas_systems = (
        "ch4_natural_gas_systems",
        "CH4 Natural Gas Systems",
        EmissionsSourceCategory.natural_gas_system,
        "CH4",
    )
    ch4_landfills = (
        "ch4_landfills",
        "CH4 Landfills",
        EmissionsSourceCategory.landfills,
        "CH4",
    )
    ch4_manure_management = (
        "ch4_manure_management",
        "CH4 Manure Management",
        EmissionsSourceCategory.land_management,
        "CH4",
    )
    ch4_coal_mining = (
        "ch4_coal_mining",
        "CH4 Coal Mining",
        EmissionsSourceCategory.coal_mining_oil_and_gas_extraction,
        "CH4",
    )
    ch4_petroleum_systems = (
        "ch4_petroleum_systems",
        "CH4 Petroleum Systems",
        EmissionsSourceCategory.petroleum_system,
        "CH4",
    )
    ch4_wastewater_treatment = (
        "ch4_wastewater_treatment",
        "CH4 Wastewater Treatment",
        EmissionsSourceCategory.waste_management,
        "CH4",
    )
    ch4_rice_cultivation = (
        "ch4_rice_cultivation",
        "CH4 Rice Cultivation",
        EmissionsSourceCategory.land_management,
        "CH4",
    )
    ch4_stationary_combustion_electric = (
        "ch4_stationary_combustion_electric",
        "CH4 Stationary Combustion Electric",
        EmissionsSourceCategory.electricity_generation,
        "CH4",
    )
    ch4_stationary_combustion_industrial_coal = (
        "ch4_stationary_combustion_industrial_coal",
        "CH4 Stationary Combustion Industrial Coal",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "CH4",
    )
    ch4_stationary_combustion_industrial_fuel_oil = (
        "ch4_stationary_combustion_industrial_fuel_oil",
        "CH4 Stationary Combustion Industrial Fuel oil",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "CH4",
    )
    ch4_stationary_combustion_industrial_natural_gas = (
        "ch4_stationary_combustion_industrial_natural_gas",
        "CH4 Stationary Combustion Industrial Natural gas",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "CH4",
    )
    ch4_stationary_combustion_commercial_fuel_oil = (
        "ch4_stationary_combustion_commercial_fuel_oil",
        "CH4 Stationary Combustion Commercial Fuel oil",
        EmissionsSourceCategory.commercial_fuel_combustion,
        "CH4",
    )
    ch4_stationary_combustion_commercial_natural_gas = (
        "ch4_stationary_combustion_commercial_natural_gas",
        "CH4 Stationary Combustion Commercial Natural gas",
        EmissionsSourceCategory.commercial_fuel_combustion,
        "CH4",
    )
    ch4_stationary_combustion_residential = (
        "ch4_stationary_combustion_residential",
        "CH4 Stationary Combustion Residential",
        EmissionsSourceCategory.residential_fuel_combustion,
        "CH4",
    )
    ch4_abandoned_oil_and_gas_wells = (
        "ch4_abandoned_oil_and_gas_wells",
        "CH4 Abandoned Oil and Gas Wells",
        EmissionsSourceCategory.coal_mining_oil_and_gas_extraction,
        "CH4",
    )
    ch4_abandoned_underground_coal_mines = (
        "ch4_abandoned_underground_coal_mines",
        "CH4 Abandoned Underground Coal Mines",
        EmissionsSourceCategory.coal_mining_oil_and_gas_extraction,
        "CH4",
    )
    ch4_mobile_combustion = (
        "ch4_mobile_combustion",
        "CH4 Mobile Combustion",
        EmissionsSourceCategory.other_transport,
        "CH4",
    )
    ch4_composting = (
        "ch4_composting",
        "CH4 Composting",
        EmissionsSourceCategory.waste_management,
        "CH4",
    )
    ch4_field_burning_of_agricultural_residues = (
        "ch4_field_burning_of_agricultural_residues",
        "CH4 Field Burning of Agricultural Residues",
        EmissionsSourceCategory.land_management,
        "CH4",
    )
    ch4_petrochemical_production = (
        "ch4_petrochemical_production",
        "CH4 Petrochemical Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CH4",
    )
    ch4_ferroalloy_production = (
        "ch4_ferroalloy_production",
        "CH4 Ferroalloy Production",
        EmissionsSourceCategory.other_industrial_processes,
        "CH4",
    )
    ch4_silicon_carbide_production_and_consumption = (
        "ch4_silicon_carbide_production_and_consumption",
        "CH4 Silicon Carbide Production and Consumption",
        EmissionsSourceCategory.other_industrial_processes,
        "CH4",
    )
    ch4_iron_and_steel_production_and_metallurgical_coke_production = (
        "ch4_iron_and_steel_production_and_metallurgical_coke_production",
        "CH4 Iron and Steel Production & Metallurgical Coke Production",
        EmissionsSourceCategory.iron_and_steel,
        "CH4",
    )
    ch4_incineration_of_waste = (
        "ch4_incineration_of_waste",
        "CH4 Incineration of Waste",
        EmissionsSourceCategory.waste_management,
        "CH4",
    )
    ch4_international_bunker_fuels = (
        "ch4_international_bunker_fuels",
        "CH4 International Bunker Fuels",
        EmissionsSourceCategory.other_transport,
        "CH4",
    )
    n2o_agricultural_soil_management_cropland_fertilizer = (
        "n2o_agricultural_soil_management_cropland_fertilizer",
        "N2O Agricultural Soil Management Cropland Fertilizer",
        EmissionsSourceCategory.land_management,
        "N2O",
    )
    n2o_agricultural_soil_management_cropland_mineralization_and_other = (
        "n2o_agricultural_soil_management_cropland_mineralization_and_other",
        "N2O Agricultural Soil Management Mineralization and other",
        EmissionsSourceCategory.land_management,
        "N2O",
    )
    n2o_agricultural_soil_management_grassland = (
        "n2o_agricultural_soil_management_grassland",
        "N2O Agricultural Soil Management Grassland",
        EmissionsSourceCategory.land_management,
        "N2O",
    )
    n2o_stationary_combustion_electric = (
        "n2o_stationary_combustion_electric",
        "N2O Stationary Combustion Electric",
        EmissionsSourceCategory.electricity_generation,
        "N2O",
    )
    n2o_stationary_combustion_industrial_coal = (
        "n2o_stationary_combustion_industrial_coal",
        "N2O Stationary Combustion Industrial Coal",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "N2O",
    )
    n2o_stationary_combustion_industrial_fuel_oil = (
        "n2o_stationary_combustion_industrial_fuel_oil",
        "N2O Stationary Combustion Industrial Fuel oil",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "N2O",
    )
    n2o_stationary_combustion_industrial_natural_gas = (
        "n2o_stationary_combustion_industrial_natural_gas",
        "N2O Stationary Combustion Industrial Natural gas",
        EmissionsSourceCategory.industrial_fuel_combustion,
        "N2O",
    )
    n2o_stationary_combustion_commercial_fuel_oil = (
        "n2o_stationary_combustion_commercial_fuel_oil",
        "N2O Stationary Combustion Commercial Fuel oil",
        EmissionsSourceCategory.commercial_fuel_combustion,
        "N2O",
    )
    n2o_stationary_combustion_commercial_natural_gas = (
        "n2o_stationary_combustion_commercial_natural_gas",
        "N2O Stationary Combustion Commercial Natural gas",
        EmissionsSourceCategory.commercial_fuel_combustion,
        "N2O",
    )
    n2o_stationary_combustion_residential = (
        "n2o_stationary_combustion_residential",
        "N2O Stationary Combustion Residential",
        EmissionsSourceCategory.residential_fuel_combustion,
        "N2O",
    )
    n2o_mobile_combustion = (
        "n2o_mobile_combustion",
        "N2O Mobile Combustion",
        EmissionsSourceCategory.other_transport,
        "N2O",
    )
    n2o_manure_management = (
        "n2o_manure_management",
        "N2O Manure Management",
        EmissionsSourceCategory.land_management,
        "N2O",
    )
    n2o_nitric_acid_production = (
        "n2o_nitric_acid_production",
        "N2O Nitric Acid Production",
        EmissionsSourceCategory.other_industrial_processes,
        "N2O",
    )
    n2o_adipic_acid_production = (
        "n2o_adipic_acid_production",
        "N2O Adipic Acid Production",
        EmissionsSourceCategory.other_industrial_processes,
        "N2O",
    )
    n2o_wastewater_treatment = (
        "n2o_wastewater_treatment",
        "N2O Wastewater Treatment",
        EmissionsSourceCategory.waste_management,
        "N2O",
    )
    n2o_from_product_uses = (
        "n2o_from_product_uses",
        "N2O N2O from Product Uses",
        EmissionsSourceCategory.land_management,
        "N2O",
    )
    n2o_caprolactam_glyoxal_and_glyoxylic_acid_production = (
        "n2o_caprolactam_glyoxal_and_glyoxylic_acid_production",
        "N2O Caprolactam, Glyoxal, and Glyoxylic Acid Production",
        EmissionsSourceCategory.other_industrial_processes,
        "N2O",
    )
    n2o_composting = (
        "n2o_composting",
        "N2O Composting",
        EmissionsSourceCategory.waste_management,
        "N2O",
    )
    n2o_incineration_of_waste = (
        "n2o_incineration_of_waste",
        "N2O Incineration of Waste",
        EmissionsSourceCategory.waste_management,
        "N2O",
    )
    n2o_semiconductor_manufacture = (
        "n2o_semiconductor_manufacture",
        "N2O Semiconductor Manufacture",
        EmissionsSourceCategory.other_industrial_processes,
        "N2O",
    )
    n2o_field_burning_of_agricultural_residues = (
        "n2o_field_burning_of_agricultural_residues",
        "N2O Field Burning of Agricultural Residues",
        EmissionsSourceCategory.land_management,
        "N2O",
    )
    n2o_international_bunker_fuels = (
        "n2o_international_bunker_fuels",
        "N2O International Bunker Fuelsb",
        EmissionsSourceCategory.other_transport,
        "N2O",
    )
    hfc_32_substitution_of_ozone_depleting_substances_transport = (
        "hfc_32_substitution_of_ozone_depleting_substances_transport",
        "HFCs Substitution of Ozone Depleting Substancesd Transport HFC-32",
        EmissionsSourceCategory.refrigerants,
        "HFC-32",
    )
    hfc_125_substitution_of_ozone_depleting_substances_transport = (
        "hfc_125_substitution_of_ozone_depleting_substances_transport",
        "HFCs Substitution of Ozone Depleting Substancesd Transport HFC-125",
        EmissionsSourceCategory.refrigerants,
        "HFC-125",
    )
    hfc_134a_substitution_of_ozone_depleting_substances_transport = (
        "hfc_134a_substitution_of_ozone_depleting_substances_transport",
        "HFCs Substitution of Ozone Depleting Substancesd Transport HFC-134a",
        EmissionsSourceCategory.refrigerants,
        "HFC-134a",
    )
    hfc_143a_substitution_of_ozone_depleting_substances_transport = (
        "hfc_143a_substitution_of_ozone_depleting_substances_transport",
        "HFCs Substitution of Ozone Depleting Substancesd Transport HFC-143a",
        EmissionsSourceCategory.refrigerants,
        "HFC-143a",
    )
    hfc_236fa_substitution_of_ozone_depleting_substances_transport = (
        "hfc_236fa_substitution_of_ozone_depleting_substances_transport",
        "HFCs Substitution of Ozone Depleting Substancesd Transport HFC-236fa",
        EmissionsSourceCategory.refrigerants,
        "HFC-236fa",
    )
    hfc_32_substitution_of_ozone_depleting_substances_others = (
        "hfc_32_substitution_of_ozone_depleting_substances_others",
        "HFCs Substitution of Ozone Depleting Substancesd Others HFC-32",
        EmissionsSourceCategory.refrigerants,
        "HFC-32",
    )
    hfc_125_substitution_of_ozone_depleting_substances_others = (
        "hfc_125_substitution_of_ozone_depleting_substances_others",
        "HFCs Substitution of Ozone Depleting Substancesd Others HFC-125",
        EmissionsSourceCategory.refrigerants,
        "HFC-125",
    )
    hfc_134a_substitution_of_ozone_depleting_substances_others = (
        "hfc_134a_substitution_of_ozone_depleting_substances_others",
        "HFCs Substitution of Ozone Depleting Substancesd Others HFC-134a",
        EmissionsSourceCategory.refrigerants,
        "HFC-134a",
    )
    hfc_143a_substitution_of_ozone_depleting_substances_others = (
        "hfc_143a_substitution_of_ozone_depleting_substances_others",
        "HFCs Substitution of Ozone Depleting Substancesd Others HFC-143a",
        EmissionsSourceCategory.refrigerants,
        "HFC-143a",
    )
    hfc_236fa_substitution_of_ozone_depleting_substances_others = (
        "hfc_236fa_substitution_of_ozone_depleting_substances_others",
        "HFCs Substitution of Ozone Depleting Substancesd Others HFC-236fa",
        EmissionsSourceCategory.refrigerants,
        "HFC-236fa",
    )
    hfc_32_foams = (
        "hfc_32_foams",
        "HFCs Foams HFC-32",
        EmissionsSourceCategory.other_industrial_processes,
        "HFC-32",
    )
    hfc_125_foams = (
        "hfc_125_foams",
        "HFCs Foams HFC-125",
        EmissionsSourceCategory.other_industrial_processes,
        "HFC-125",
    )
    hfc_134a_foams = (
        "hfc_134a_foams",
        "HFCs Foams HFC-134a",
        EmissionsSourceCategory.other_industrial_processes,
        "HFC-134a",
    )
    hfc_143a_foams = (
        "hfc_143a_foams",
        "HFCs Foams HFC-143a",
        EmissionsSourceCategory.other_industrial_processes,
        "HFC-143a",
    )
    hfc_236fa_foams = (
        "hfc_236fa_foams",
        "HFCs Foams HFC-236fa",
        EmissionsSourceCategory.other_industrial_processes,
        "HFC-236fa",
    )
    hfc_23_hcfc_22_production = (
        "hfc_23_hcfc_22_production",
        "HFCs HCFC-22 Production HFC-23",
        EmissionsSourceCategory.refrigerants,
        "HFC-23",
    )
    hfc_23_semiconductor_manufacture = (
        "hfc_23_semiconductor_manufacture",
        "HFCs Semiconductor Manufacture HFC-23",
        EmissionsSourceCategory.other_industrial_processes,
        "HFC-23",
    )
    hfc_134a_magnesium_production_and_processing = (
        "hfc_134a_magnesium_production_and_processing",
        "HFCs Magnesium Production and Processing HFC-134a",
        EmissionsSourceCategory.other_industrial_processes,
        "HFC-134a",
    )
    pfc_cf4_aluminum_production = (
        "pfc_cf4_aluminum_production",
        "PFCs Aluminum Production CF4",
        EmissionsSourceCategory.other_industrial_processes,
        "CF4",
    )
    pfc_c2f6_aluminum_production = (
        "pfc_c2f6_aluminum_production",
        "PFCs Aluminum Production C2F6",
        EmissionsSourceCategory.other_industrial_processes,
        "C2F6",
    )
    pfc_cf4_semiconductor_manufacture = (
        "pfc_cf4_semiconductor_manufacture",
        "PFCs Semiconductor Manufacture CF4",
        EmissionsSourceCategory.other_industrial_processes,
        "CF4",
    )
    pfc_c2f6_semiconductor_manufacture = (
        "pfc_c2f6_semiconductor_manufacture",
        "PFCs Semiconductor Manufacture C2F6",
        EmissionsSourceCategory.other_industrial_processes,
        "C2F6",
    )
    pfc_c3f8_semiconductor_manufacture = (
        "pfc_c3f8_semiconductor_manufacture",
        "PFCs Semiconductor Manufacture C3F8",
        EmissionsSourceCategory.other_industrial_processes,
        "C3F8",
    )
    pfc_c4f8_semiconductor_manufacture = (
        "pfc_c4f8_semiconductor_manufacture",
        "PFCs Semiconductor Manufacture C4F8",
        EmissionsSourceCategory.other_industrial_processes,
        "C4F8",
    )
    sf6_electrical_transmission_and_distribution = (
        "sf6_electrical_transmission_and_distribution",
        "SF6 Electrical Transmission and Distribution",
        EmissionsSourceCategory.electricity_generation,
        "SF6",
    )
    sf6_magnesium_production_and_processing = (
        "sf6_magnesium_production_and_processing",
        "SF6 Magnesium Production and Processing",
        EmissionsSourceCategory.other_industrial_processes,
        "SF6",
    )
    sf6_semiconductor_manufacture = (
        "sf6_semiconductor_manufacture",
        "SF6 Semiconductor Manufacture",
        EmissionsSourceCategory.other_industrial_processes,
        "SF6",
    )
    nf3_semiconductor_manufacture = (
        "nf3_semiconductor_manufacture",
        "NF3 Semiconductor Manufacture",
        EmissionsSourceCategory.other_industrial_processes,
        "NF3",
    )
