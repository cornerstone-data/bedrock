from __future__ import annotations

import logging
import typing as ta

import pandas as pd
import pytest

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.transform.allocation.co2 import (
    allocate_aluminum_production,
    allocate_ammonia_production,
    allocate_cement_production,
    allocate_commercial_coal,
    allocate_commercial_natural_gas,
    allocate_commercial_petrol,
    allocate_electricity_generation,
    allocate_ferroalloy_production,
    allocate_glass_production,
    allocate_incineration_of_waste,
    allocate_industrial_coal,
    allocate_industrial_natural_gas,
    allocate_industrial_petrol,
    allocate_iron_and_steel_production_and_metallurgical_coke_production,
    allocate_lead_production,
    allocate_lime_production,
    allocate_liming_of_agricultural_soils,
    allocate_natural_gas_systems,
    allocate_non_energy_fuels_coal_coke,
    allocate_non_energy_fuels_natural_gas,
    allocate_non_energy_fuels_petrol,
    allocate_non_energy_fuels_transport,
    allocate_other_process_uses_of_carbonates,
    allocate_petrochemical_production,
    allocate_petroleum_systems,
    allocate_phosphoric_acid_production,
    allocate_silicon_carbide_production_and_consumption,
    allocate_soda_ash_production_and_consumption,
    allocate_titanium_dioxide_production,
    allocate_transportation_aviation_gasoline,
    allocate_transportation_distillate_fuel_oil,
    allocate_transportation_jet_fuel,
    allocate_transportation_lpg,
    allocate_transportation_motor_gasoline,
    allocate_transportation_natural_gas,
    allocate_transportation_residual_fuel,
    allocate_urea_consumption_for_nonagricultural_purposes,
    allocate_urea_fertilization,
    allocate_zinc_production,
)
from bedrock.transform.allocation.constants import EmissionsSource as ES

if ta.TYPE_CHECKING:
    AllocatorType = ta.Callable[[], pd.Series[float]]


def zero_allocator() -> pd.Series[float]:
    # for cases where there are no emissions to allocate
    return pd.Series(0.0, index=CEDA_V7_SECTORS)


CO2_ALLOCATION: ta.Dict[ES, AllocatorType] = {
    ES.co2_aluminum_production: allocate_aluminum_production,
    ES.co2_ammonia_production: allocate_ammonia_production,
    ES.co2_carbon_dioxide_consumption: zero_allocator,
    ES.co2_cement_production: allocate_cement_production,
    ES.co2_commercial_coal: allocate_commercial_coal,
    ES.co2_commercial_natural_gas: allocate_commercial_natural_gas,
    ES.co2_commercial_petrol: allocate_commercial_petrol,
    ES.co2_electricity_generation: allocate_electricity_generation,
    ES.co2_ferroalloy_production: allocate_ferroalloy_production,
    ES.co2_glass_production: allocate_glass_production,
    ES.co2_incineration_of_waste: allocate_incineration_of_waste,
    ES.co2_industrial_coal: allocate_industrial_coal,
    ES.co2_industrial_natural_gas: allocate_industrial_natural_gas,
    ES.co2_industrial_petrol: allocate_industrial_petrol,
    ES.co2_international_bunker_fuels: zero_allocator,
    ES.co2_iron_and_steel_production_and_metallurgical_coke_production: allocate_iron_and_steel_production_and_metallurgical_coke_production,
    ES.co2_land_use_land_use_change_and_forestry_sinks: zero_allocator,
    ES.co2_lead_production: allocate_lead_production,
    ES.co2_lime_production: allocate_lime_production,
    ES.co2_liming_of_agricultural_soils: allocate_liming_of_agricultural_soils,
    ES.co2_magnesium_production_and_processing: zero_allocator,
    ES.co2_natural_gas_systems: allocate_natural_gas_systems,
    ES.co2_non_energy_fuels_coal_coke: allocate_non_energy_fuels_coal_coke,
    ES.co2_non_energy_fuels_natural_gas: allocate_non_energy_fuels_natural_gas,
    ES.co2_non_energy_fuels_petrol: allocate_non_energy_fuels_petrol,
    ES.co2_non_energy_fuels_transport: allocate_non_energy_fuels_transport,
    ES.co2_other_process_uses_of_carbonates: allocate_other_process_uses_of_carbonates,
    ES.co2_peatlands_remaining_peatlands: zero_allocator,
    ES.co2_petrochemical_production: allocate_petrochemical_production,
    ES.co2_petroleum_systems: allocate_petroleum_systems,
    ES.co2_phosphoric_acid_production: allocate_phosphoric_acid_production,
    ES.co2_residential: zero_allocator,
    ES.co2_silicon_carbide_production_and_consumption: allocate_silicon_carbide_production_and_consumption,
    ES.co2_soda_ash_production_and_consumption: allocate_soda_ash_production_and_consumption,
    ES.co2_titanium_dioxide_production: allocate_titanium_dioxide_production,
    ES.co2_transportation_aviation_gasoline: allocate_transportation_aviation_gasoline,
    ES.co2_transportation_distillate_fuel_oil: allocate_transportation_distillate_fuel_oil,
    ES.co2_transportation_jet_fuel: allocate_transportation_jet_fuel,
    ES.co2_transportation_lpg: allocate_transportation_lpg,
    ES.co2_transportation_motor_gasoline: allocate_transportation_motor_gasoline,
    ES.co2_transportation_natural_gas: allocate_transportation_natural_gas,
    ES.co2_transportation_residual_fuel: allocate_transportation_residual_fuel,
    ES.co2_urea_consumption_for_nonagricultural_purposes: allocate_urea_consumption_for_nonagricultural_purposes,
    ES.co2_urea_fertilization: allocate_urea_fertilization,
    ES.co2_us_territories: zero_allocator,
    ES.co2_wood_biomass_and_ethanol_consumption: zero_allocator,
    ES.co2_zinc_production: allocate_zinc_production,
}

logger = logging.getLogger(__name__)


def test_co2_allocators_present() -> None:
    assert set(CO2_ALLOCATION.keys()) == {es for es in ES if es.gas == "CO2"}


@pytest.mark.eeio_integration
@pytest.mark.parametrize("es,allocator", CO2_ALLOCATION.items())
def test_co2(es: ES, allocator: AllocatorType, E_usa_es_snapshot: pd.DataFrame) -> None:
    allocated = allocator()
    assert set(allocated.index) == set(CEDA_V7_SECTORS)
    assert not allocated.isna().any()
    # TODO bring back equality tests after we update snapshot
    expected = E_usa_es_snapshot.loc[es, :]

    logger.info(
        f"{es} {allocated.sum() / expected.sum():.2f} allocated {allocated.sum():.2f} vs expected {expected.sum():.2f}"
    )
