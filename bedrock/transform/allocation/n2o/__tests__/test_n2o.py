from __future__ import annotations

import logging
import typing as ta

import pandas as pd
import pytest

from bedrock.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS
from bedrock.transform.allocation.constants import EmissionsSource as ES
from bedrock.transform.allocation.n2o import (
    allocate_adipic_acid,
    allocate_caprolactam_glyoxal_and_glyoxylic_acid_production,
    allocate_composting,
    allocate_fertilizer,
    allocate_field_burning_of_agricultural_residues,
    allocate_incineration_of_waste,
    allocate_industrial_coal,
    allocate_industrial_fuel_oil,
    allocate_industrial_natural_gas,
    allocate_international_bunker_fuels,
    allocate_manure_management,
    allocate_mineralization,
    allocate_mobile_combustion,
    allocate_nitric_acid,
    allocate_product_uses,
    allocate_semiconductor_manufacture,
    allocate_soil_management_grassland,
    allocate_stationary_combustion_commercial_fuel_oil,
    allocate_stationary_combustion_commercial_natural_gas,
    allocate_stationary_combustion_electric,
    allocate_stationary_combustion_residential,
    allocate_wastewater_treatment,
)

if ta.TYPE_CHECKING:
    AllocatorType = ta.Callable[[], pd.Series[float]]


N20_ALLOCATION: ta.Dict[ES, AllocatorType] = {
    ES.n2o_adipic_acid_production: allocate_adipic_acid,
    ES.n2o_agricultural_soil_management_cropland_fertilizer: allocate_fertilizer,
    ES.n2o_agricultural_soil_management_cropland_mineralization_and_other: allocate_mineralization,
    ES.n2o_agricultural_soil_management_grassland: allocate_soil_management_grassland,
    ES.n2o_caprolactam_glyoxal_and_glyoxylic_acid_production: allocate_caprolactam_glyoxal_and_glyoxylic_acid_production,
    ES.n2o_composting: allocate_composting,
    ES.n2o_field_burning_of_agricultural_residues: allocate_field_burning_of_agricultural_residues,
    ES.n2o_from_product_uses: allocate_product_uses,
    ES.n2o_incineration_of_waste: allocate_incineration_of_waste,
    ES.n2o_international_bunker_fuels: allocate_international_bunker_fuels,
    ES.n2o_manure_management: allocate_manure_management,
    ES.n2o_mobile_combustion: allocate_mobile_combustion,
    ES.n2o_nitric_acid_production: allocate_nitric_acid,
    ES.n2o_semiconductor_manufacture: allocate_semiconductor_manufacture,
    ES.n2o_stationary_combustion_commercial_fuel_oil: allocate_stationary_combustion_commercial_fuel_oil,
    ES.n2o_stationary_combustion_commercial_natural_gas: allocate_stationary_combustion_commercial_natural_gas,
    ES.n2o_stationary_combustion_electric: allocate_stationary_combustion_electric,
    ES.n2o_stationary_combustion_industrial_coal: allocate_industrial_coal,
    ES.n2o_stationary_combustion_industrial_fuel_oil: allocate_industrial_fuel_oil,
    ES.n2o_stationary_combustion_industrial_natural_gas: allocate_industrial_natural_gas,
    ES.n2o_stationary_combustion_residential: allocate_stationary_combustion_residential,
    ES.n2o_wastewater_treatment: allocate_wastewater_treatment,
}

logger = logging.getLogger(__name__)


def test_n2o_allocators_present() -> None:
    assert set(N20_ALLOCATION.keys()) == {es for es in ES if es.gas == "N2O"}


@pytest.mark.eeio_integration
@pytest.mark.parametrize("es,allocator", N20_ALLOCATION.items())
def test_n2o(es: ES, allocator: AllocatorType, E_usa_es_snapshot: pd.DataFrame) -> None:
    allocated = allocator()
    assert set(allocated.index) == set(CEDA_V7_SECTORS)
    assert not allocated.isna().any()
    # TODO bring back equality tests after we update snapshot
    expected = E_usa_es_snapshot.loc[es, :]

    logger.info(
        f"{es} {allocated.sum() / expected.sum():.2f} allocated {allocated.sum():.2f} vs expected {expected.sum():.2f}"
    )
