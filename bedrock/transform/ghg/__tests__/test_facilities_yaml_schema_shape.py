"""Schema-shape checks for facilities nowcast GHG method YAMLs."""

from __future__ import annotations

from pathlib import Path

_GHG_DIR = Path(__file__).resolve().parents[1]
_REQUIRED_SETS = (
    'petroleum_still_gas',
    'petroleum_facility',
    'petroleum_use',
    'natural_gas_lease_plant',
    'natural_gas_nonmanufacturing',
    'ng_manufacturing',
    'coal',
)
_FACILITY_FLOWABLES = (
    'Flowable: Petroleum',
    'Flowable: Natural Gas',
    'Flowable: Coal',
    'Flowable: Natural Gas - lease and plant',
)


def test_facilities_yamls_wire_prep_and_activity_sets() -> None:
    paths = sorted(_GHG_DIR.glob('GHG_national_Cornerstone_nowcast_facilities_*.yaml'))
    assert [p.name for p in paths] == [
        f'GHG_national_Cornerstone_nowcast_facilities_{y}.yaml'
        for y in range(2017, 2025)
    ]
    for path in paths:
        text = path.read_text(encoding='utf-8')
        year = path.stem.rsplit('_', 1)[-1]
        assert f'!include:GHG_national_Cornerstone_nowcast_{year}.yaml' in text
        assert 'GHGRP_NEI_Facilities:' in text
        assert (
            'FBS_datapull_fxn: !script_function:stewiFBS facility_combustion_to_sector'
            in text
        )
        assert (
            'clean_fba_before_activity_sets: !script_function:UMD_GHGIA '
            'prepare_facilities_industrial_combustion'
        ) in text
        assert "exclude_sectors:" in text
        assert "'221100'" in text
        assert 'annex_fba:' in text
        for name in _REQUIRED_SETS:
            assert f'{name}:' in text, f'{path.name} missing activity set {name}'
        for flowable in _FACILITY_FLOWABLES:
            assert flowable in text, f'{path.name} missing {flowable}'
        assert 'Nowcast_Detail_Use_AfterRedef:' in text
        assert 'Petroleum Industrial - Distillate Fuel Oil' in text
        assert 'Petroleum Industrial - Motor Gasoline' in text
