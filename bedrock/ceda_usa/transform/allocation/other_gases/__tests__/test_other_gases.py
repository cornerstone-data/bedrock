# isort: skip_file
from __future__ import annotations

import typing as ta

import pandas as pd
import pytest
import logging
from bedrock.ceda_usa.transform.allocation.constants import EmissionsSource as ES

from bedrock.ceda_usa.transform.allocation.other_gases import (
    allocate_hfc_23_hcfc_22_production,
    allocate_hfc_23_semiconductor_manufacture,
    allocate_hfc_32_foams,
    allocate_hfc_32_others,
    allocate_hfc_32_transport,
    allocate_hfc_125_foams,
    allocate_hfc_125_others,
    allocate_hfc_125_transport,
    allocate_hfc_134a_foams,
    allocate_hfc_134a_magnesium_production,
    allocate_hfc_134a_others,
    allocate_hfc_134a_transport,
    allocate_hfc_143a_foams,
    allocate_hfc_143a_others,
    allocate_hfc_143a_transport,
    allocate_hfc_236fa_foams,
    allocate_hfc_236fa_others,
    allocate_hfc_236fa_transport,
    allocate_nf3_semiconductor_manufacture,
    allocate_pfc_c2f6_aluminum_production,
    allocate_pfc_c2f6_semiconductor_manufacture,
    allocate_pfc_c3f8_semiconductor_manufacture,
    allocate_pfc_c4f8_semiconductor_manufacture,
    allocate_pfc_cf4_aluminum_production,
    allocate_pfc_cf4_semiconductor_manufacture,
    allocate_sf6_electricity,
    allocate_sf6_magnesium_production,
    allocate_sf6_semiconductor_manufacture,
)
from bedrock.ceda_usa.utils.taxonomy.bea.ceda_v7 import CEDA_V7_SECTORS


if ta.TYPE_CHECKING:
    AllocatorType = ta.Callable[[], pd.Series[float]]

OTHER_GASES_ALLOCATION: ta.Dict[ES, AllocatorType] = {
    ES.hfc_32_substitution_of_ozone_depleting_substances_transport: allocate_hfc_32_transport,
    ES.hfc_125_substitution_of_ozone_depleting_substances_transport: allocate_hfc_125_transport,
    ES.hfc_134a_substitution_of_ozone_depleting_substances_transport: allocate_hfc_134a_transport,
    ES.hfc_143a_substitution_of_ozone_depleting_substances_transport: allocate_hfc_143a_transport,
    ES.hfc_236fa_substitution_of_ozone_depleting_substances_transport: allocate_hfc_236fa_transport,
    ES.hfc_32_substitution_of_ozone_depleting_substances_others: allocate_hfc_32_others,
    ES.hfc_125_substitution_of_ozone_depleting_substances_others: allocate_hfc_125_others,
    ES.hfc_134a_substitution_of_ozone_depleting_substances_others: allocate_hfc_134a_others,
    ES.hfc_143a_substitution_of_ozone_depleting_substances_others: allocate_hfc_143a_others,
    ES.hfc_236fa_substitution_of_ozone_depleting_substances_others: allocate_hfc_236fa_others,
    ES.hfc_32_foams: allocate_hfc_32_foams,
    ES.hfc_125_foams: allocate_hfc_125_foams,
    ES.hfc_134a_foams: allocate_hfc_134a_foams,
    ES.hfc_143a_foams: allocate_hfc_143a_foams,
    ES.hfc_236fa_foams: allocate_hfc_236fa_foams,
    ES.hfc_23_hcfc_22_production: allocate_hfc_23_hcfc_22_production,
    ES.hfc_23_semiconductor_manufacture: allocate_hfc_23_semiconductor_manufacture,
    ES.hfc_134a_magnesium_production_and_processing: allocate_hfc_134a_magnesium_production,
    ES.pfc_cf4_aluminum_production: allocate_pfc_cf4_aluminum_production,
    ES.pfc_c2f6_aluminum_production: allocate_pfc_c2f6_aluminum_production,
    ES.pfc_cf4_semiconductor_manufacture: allocate_pfc_cf4_semiconductor_manufacture,
    ES.pfc_c2f6_semiconductor_manufacture: allocate_pfc_c2f6_semiconductor_manufacture,
    ES.pfc_c3f8_semiconductor_manufacture: allocate_pfc_c3f8_semiconductor_manufacture,
    ES.pfc_c4f8_semiconductor_manufacture: allocate_pfc_c4f8_semiconductor_manufacture,
    ES.sf6_electrical_transmission_and_distribution: allocate_sf6_electricity,
    ES.sf6_semiconductor_manufacture: allocate_sf6_semiconductor_manufacture,
    ES.sf6_magnesium_production_and_processing: allocate_sf6_magnesium_production,
    ES.nf3_semiconductor_manufacture: allocate_nf3_semiconductor_manufacture,
}

logger = logging.getLogger(__name__)


def test_other_allocators_present() -> None:
    assert set(OTHER_GASES_ALLOCATION.keys()) == {
        es for es in ES if es.gas not in ("N2O", "CO2", "CH4")
    }


@pytest.mark.eeio_integration
@pytest.mark.parametrize("es,allocator", OTHER_GASES_ALLOCATION.items())
def test_other_gases(
    es: ES, allocator: AllocatorType, E_usa_es_snapshot: pd.DataFrame
) -> None:
    allocated = allocator()
    assert set(allocated.index) == set(CEDA_V7_SECTORS)
    assert not allocated.isna().any()
    # TODO bring back equality tests after we update snapshot
    expected = E_usa_es_snapshot.loc[es, :]

    logger.info(
        f"{es} {allocated.sum() / expected.sum():.2f} allocated {allocated.sum():.2f} vs expected {expected.sum():.2f}"
    )
