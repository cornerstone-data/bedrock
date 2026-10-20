"""Schema-shape checks for facility overrides in the canonical GHG methods."""

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


def _facility_year_yamls() -> list[Path]:
    """Canonical methods. Facility overrides are written in these files."""
    return [
        _GHG_DIR / f'GHG_national_Cornerstone_{year}.yaml' for year in range(2017, 2025)
    ]


def test_facilities_yamls_wire_prep_and_activity_sets() -> None:
    paths = _facility_year_yamls()
    assert [p.name for p in paths] == [
        f'GHG_national_Cornerstone_{year}.yaml' for year in range(2017, 2025)
    ]
    for path in paths:
        text = path.read_text(encoding='utf-8')
        includes = [line.strip() for line in text.splitlines() if '!include:' in line]
        assert includes == ['!include:Cornerstone_2025_target.yaml']
        assert 'GHGRP_NEI_Facilities:' in text
        assert '!script_function:stewiFBS' in text
        assert 'facility_combustion_to_sector' in text
        assert 'prepare_facilities_industrial_combustion' in text
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


def test_still_gas_attributes_to_refineries_only() -> None:
    """Still gas is refinery fuel: weight on refineries' petroleum only.

    The cached facility source carries refineries as 32411 (industry spec); a
    selection on '324110' alone matched nothing and dropped all still gas.
    """
    for path in _facility_year_yamls():
        text = path.read_text(encoding='utf-8')
        block = text.split('petroleum_still_gas:', 1)[1].split(
            'petroleum_facility:', 1
        )[0]
        assert 'GHGRP_NEI_Facilities:' in block, path.name
        assert (
            "'32411'" in block and "'324110'" in block
        ), f'{path.name}: still gas must attribute to refineries only'
