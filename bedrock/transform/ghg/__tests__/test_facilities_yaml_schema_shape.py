"""Schema-shape checks for year Cornerstone GHG methods."""

from __future__ import annotations

import pytest

from bedrock.extract.stewifbs.stewiFBS import facility_combustion_to_sector
from bedrock.extract.umd.UMD_GHGIA import prepare_facilities_industrial_combustion
from bedrock.utils.config.common import load_yaml_dict

_REQUIRED_SETS = (
    'petroleum_still_gas',
    'petroleum_facility',
    'petroleum_use',
    'natural_gas_lease_plant',
    'natural_gas_nonmanufacturing',
    'ng_manufacturing',
    'coal',
)


@pytest.mark.parametrize('year', range(2017, 2025))
def test_cornerstone_year_wires_facility_activity_sets(year: int) -> None:
    method = load_yaml_dict(f'GHG_national_Cornerstone_{year}', 'FBS')
    facilities = method['sources_to_cache']['GHGRP_NEI_Facilities']
    assert facilities['FBS_datapull_fxn'] is facility_combustion_to_sector
    assert '221100' in facilities['exclude_sectors']
    assert {'Petroleum', 'Natural Gas', 'Coal'} <= set(facilities['keep_flowables'])
    industrial = method['source_names']['UMD_GHGIA_T_3_11']
    assert (
        industrial['clean_fba_before_activity_sets']
        is prepare_facilities_industrial_combustion
    )
    assert '221100' in industrial['clean_parameter']['exclude_sectors']
    assert industrial['clean_parameter']['annex_fba']
    sets = industrial['activity_sets']
    for name in _REQUIRED_SETS:
        assert name in sets, f'{year} missing activity set {name}'
    lease = sets['natural_gas_lease_plant']['attribution_source'][
        'GHGRP_NEI_Facilities'
    ]['selection_fields']
    assert lease['Flowable'] == 'Natural Gas - lease and plant'
    petroleum_use = sets['petroleum_use']
    assert 'Nowcast_Detail_Use_AfterRedef' in petroleum_use['attribution_source']
    assert (
        'Petroleum Industrial - Motor Gasoline'
        in petroleum_use['selection_fields']['PrimaryActivity']
    )
    assert (
        'Petroleum Industrial - Distillate Fuel Oil'
        in sets['petroleum_distillate']['selection_fields']['PrimaryActivity']
    )


@pytest.mark.parametrize('year', range(2017, 2025))
def test_still_gas_attributes_to_refineries_only(year: int) -> None:
    """Still gas is refinery fuel: weight on refineries' petroleum only.

    The cached facility source carries refineries as 32411 (industry spec); a
    selection on '324110' alone matched nothing and dropped all still gas.
    """
    method = load_yaml_dict(f'GHG_national_Cornerstone_{year}', 'FBS')
    still_gas = method['source_names']['UMD_GHGIA_T_3_11']['activity_sets'][
        'petroleum_still_gas'
    ]
    fields = still_gas['attribution_source']['GHGRP_NEI_Facilities']['selection_fields']
    assert fields['SectorProducedBy'] == ['32411', '324110']
