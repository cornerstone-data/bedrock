from __future__ import annotations

import logging
import typing as ta

import pandas as pd
import pytest

from bedrock.transform.allocation.ch4 import (
    allocate_abandoned_oil_and_gas_wells,
    allocate_abandoned_underground_coal_mines,
    allocate_coal_mining,
    allocate_composting,
    allocate_enteric_fermentation,
    allocate_field_burning_of_agricultural_residues,
    allocate_landfills,
    allocate_manure_management,
    allocate_mobile_combustion,
    allocate_natural_gas_systems,
    allocate_petrochemical_production,
    allocate_petroleum_systems,
    allocate_rice_cultivation,
    allocate_stationary_combustion_commercial_fuel_oil,
    allocate_stationary_combustion_commercial_natural_gas,
    allocate_stationary_combustion_electric,
    allocate_stationary_combustion_industrial_coal,
    allocate_stationary_combustion_industrial_fuel_oil,
    allocate_stationary_combustion_industrial_natural_gas,
    allocate_wastewater_treatment,
)
from bedrock.transform.allocation.constants import EmissionsSource as ES
from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS

if ta.TYPE_CHECKING:
    AllocatorType = ta.Callable[[], pd.Series[float]]


def zero_allocator() -> pd.Series[float]:
    # for cases where there are no emissions to allocate
    return pd.Series(0.0, index=CEDA_V7_SECTORS)


CH4_ALLOCATION: ta.Dict[ES, AllocatorType] = {
    ES.ch4_abandoned_oil_and_gas_wells: allocate_abandoned_oil_and_gas_wells,
    ES.ch4_abandoned_underground_coal_mines: allocate_abandoned_underground_coal_mines,
    ES.ch4_coal_mining: allocate_coal_mining,
    ES.ch4_composting: allocate_composting,
    ES.ch4_enteric_fermentation: allocate_enteric_fermentation,
    ES.ch4_ferroalloy_production: zero_allocator,
    ES.ch4_field_burning_of_agricultural_residues: allocate_field_burning_of_agricultural_residues,
    ES.ch4_incineration_of_waste: zero_allocator,
    ES.ch4_international_bunker_fuels: zero_allocator,
    ES.ch4_iron_and_steel_production_and_metallurgical_coke_production: zero_allocator,
    ES.ch4_landfills: allocate_landfills,
    ES.ch4_manure_management: allocate_manure_management,
    ES.ch4_mobile_combustion: allocate_mobile_combustion,
    ES.ch4_natural_gas_systems: allocate_natural_gas_systems,
    ES.ch4_petrochemical_production: allocate_petrochemical_production,
    ES.ch4_petroleum_systems: allocate_petroleum_systems,
    ES.ch4_rice_cultivation: allocate_rice_cultivation,
    ES.ch4_silicon_carbide_production_and_consumption: zero_allocator,
    ES.ch4_stationary_combustion_commercial_fuel_oil: allocate_stationary_combustion_commercial_fuel_oil,
    ES.ch4_stationary_combustion_commercial_natural_gas: allocate_stationary_combustion_commercial_natural_gas,
    ES.ch4_stationary_combustion_electric: allocate_stationary_combustion_electric,
    ES.ch4_stationary_combustion_industrial_coal: allocate_stationary_combustion_industrial_coal,
    ES.ch4_stationary_combustion_industrial_fuel_oil: allocate_stationary_combustion_industrial_fuel_oil,
    ES.ch4_stationary_combustion_industrial_natural_gas: allocate_stationary_combustion_industrial_natural_gas,
    ES.ch4_stationary_combustion_residential: zero_allocator,
    ES.ch4_wastewater_treatment: allocate_wastewater_treatment,
}

logger = logging.getLogger(__name__)


def test_ch4_allocators_present() -> None:
    assert set(CH4_ALLOCATION.keys()) == {es for es in ES if es.gas == "CH4"}


@pytest.mark.eeio_integration
@pytest.mark.parametrize("es,allocator", CH4_ALLOCATION.items())
def test_ch4(es: ES, allocator: AllocatorType, E_usa_es_snapshot: pd.DataFrame) -> None:
    allocated = allocator()
    assert set(allocated.index) == set(CEDA_V7_SECTORS)
    assert not allocated.isna().any()
    # TODO bring back equality tests after we update snapshot
    expected = E_usa_es_snapshot.loc[es, :]

    logger.info(
        f"{es} {allocated.sum() / expected.sum():.2f} allocated {allocated.sum():.2f} vs expected {expected.sum():.2f}"
    )
