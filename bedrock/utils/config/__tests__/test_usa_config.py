# ruff: noqa: PLC0415

from typing import Generator

import pytest

from bedrock.utils.config.usa_config import (
    CANONICAL_USA_CONFIG,
    USAConfig,
    _load_usa_config_from_file_name,
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)


@pytest.fixture(autouse=True, scope='function')
def reset_global_usa_config_before_test() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield


def test_get_usa_config_loads_waste_disagg() -> None:
    """Test that the usa_config correctly loads the waste
    disaggregation yaml file"""
    set_global_usa_config('test_usa_config_waste_disagg.yaml')
    config = get_usa_config()
    assert config.implement_waste_disaggregation is True


def test_global_usa_config() -> None:
    set_global_usa_config('test_usa_config.yaml')
    usa_config = get_usa_config()
    assert usa_config.usa_ghg_data_year == 2023
    assert usa_config.snapshot_version_or_git_sha == 'v0'


def test_global_usa_config_with_snapshot_git_sha() -> None:
    set_global_usa_config('test_usa_config_git_sha.yaml')
    usa_config = get_usa_config()
    assert (
        usa_config.snapshot_version_or_git_sha
        == '2ebb51f7190c3a62b5d8b2420bff9b20f57282fc'
    )


def test_retired_usa_config_cannot_be_loaded() -> None:
    with pytest.raises(ValueError, match='retired and cannot be loaded'):
        _load_usa_config_from_file_name('v8_ceda_2025_usa.yaml')
    with pytest.raises(ValueError, match='retired and cannot be loaded'):
        set_global_usa_config('2025_usa_ceda_ghg_from_flowsa')


def test_unknown_diagnostics_cli_override_key_raises() -> None:
    with pytest.raises(ValueError, match='Unknown diagnostics_cli_overrides'):
        set_global_usa_config(
            'test_usa_config.yaml',
            diagnostics_cli_overrides={'not_a_real_field': 'x'},
        )


def test_cannot_call_global_usa_config_twice() -> None:
    set_global_usa_config('test_usa_config.yaml')
    with pytest.raises(ValueError):
        set_global_usa_config('test_usa_config.yaml')


def test_set_global_usa_config_diagnostics_cli_overrides() -> None:
    set_global_usa_config(
        'test_usa_config.yaml',
        diagnostics_cli_overrides={
            'diagnostics_baseline_source': 'gcs_useeio_xlsx',
            'useeio_baseline_xlsx_gs_uri': (
                'gs://cornerstone-default/snapshots/x/y.xlsx'
            ),
            'useeio_baseline_xlsx_sha256': 'a' * 64,
            'useeio_model_version_label': 'test-label',
        },
    )
    cfg = get_usa_config()
    assert cfg.diagnostics_baseline_source == 'gcs_useeio_xlsx'
    gs_uri = cfg.useeio_baseline_xlsx_gs_uri
    assert gs_uri is not None
    assert gs_uri.endswith('y.xlsx')
    assert cfg.useeio_baseline_xlsx_sha256 == 'a' * 64
    assert cfg.useeio_model_version_label == 'test-label'


def test_useeio_baseline_requires_sha_in_github_actions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('GITHUB_ACTIONS', 'true')
    with pytest.raises(ValueError, match='sha256'):
        set_global_usa_config(
            'test_usa_config.yaml',
            diagnostics_cli_overrides={
                'diagnostics_baseline_source': 'gcs_useeio_xlsx',
                'useeio_baseline_xlsx_gs_uri': (
                    'gs://cornerstone-default/snapshots/x/y.xlsx'
                ),
            },
        )


def test_config_via_environment_variable() -> None:
    """Test that config can be loaded from environment variable in worker processes."""
    import os

    from bedrock.utils.config.usa_config import USA_CONFIG_ENV_VAR

    set_global_usa_config('test_usa_config.yaml')
    assert USA_CONFIG_ENV_VAR in os.environ
    assert os.environ[USA_CONFIG_ENV_VAR] == 'test_usa_config.yaml'

    # reset config (mimick a worker process with a new
    # memory space) and reload the config from env variable
    reset_usa_config(should_reset_env_var=False)
    usa_config = get_usa_config()
    assert usa_config.usa_ghg_data_year == 2023


def test_disallow_deflate_x_without_use_e_for_x_in_b() -> None:
    with pytest.raises(
        ValueError,
        match='deflate_x_to_detail_io_year_for_B requires',
    ):
        USAConfig.model_validate(
            {
                'deflate_x_to_detail_io_year_for_B': True,
                'use_E_data_year_for_x_in_B': False,
            },
            strict=True,
        )


def test_disallow_multiple_margins_flags() -> None:
    with pytest.raises(ValueError, match='At most one margins flag may be true'):
        USAConfig.model_validate(
            {'useeio_margins': True, 'cornerstone_industry_avg_margins': True},
            strict=True,
        )


def test_electricity_disagg_config_parsing() -> None:
    config = _load_usa_config_from_file_name(
        'test_usa_config_waste_disagg_electricity.yaml'
    )
    assert config.implement_waste_disaggregation is True
    assert config.implement_electricity_reallocation is True


def test_electricity_disaggregation_requires_reallocation_and_waste() -> None:
    with pytest.raises(
        ValueError,
        match='implement_electricity_disaggregation requires',
    ):
        USAConfig.model_validate(
            {
                'implement_electricity_disaggregation': True,
                'implement_electricity_reallocation': False,
                'implement_waste_disaggregation': True,
            },
            strict=True,
        )


def test_electricity_disaggregation_rejects_unsupported_io_year() -> None:
    with pytest.raises(
        ValueError,
        match=r'implement_electricity_disaggregation requires '
        r'\(usa_detail_io_source, usa_ghg_data_year\)',
    ):
        USAConfig.model_validate(
            {
                'usa_detail_io_source': 'nowcast',
                'usa_base_io_data_year': 2023,
                'model_base_year': 2023,
                'usa_ghg_data_year': 2023,
                'apply_io_year_adjustments': False,
                'implement_waste_disaggregation': True,
                'implement_electricity_reallocation': True,
                'implement_electricity_disaggregation': True,
            },
            strict=True,
        )


@pytest.mark.parametrize(
    ('usa_detail_io_source', 'year', 'extra'),
    [
        ('bea_published', 2023, {}),
        ('bea_published', 2024, {}),
        (
            'nowcast',
            2024,
            {
                'usa_base_io_data_year': 2024,
                'model_base_year': 2024,
                'apply_io_year_adjustments': False,
            },
        ),
    ],
)
def test_electricity_disaggregation_allows_supported_io_year(
    usa_detail_io_source: str,
    year: int,
    extra: dict[str, object],
) -> None:
    cfg = USAConfig.model_validate(
        {
            'usa_detail_io_source': usa_detail_io_source,
            'usa_ghg_data_year': year,
            'implement_waste_disaggregation': True,
            'implement_electricity_reallocation': True,
            'implement_electricity_disaggregation': True,
            **extra,
        },
        strict=True,
    )
    assert cfg.implement_electricity_disaggregation is True
    assert (cfg.usa_detail_io_source, cfg.usa_ghg_data_year) == (
        usa_detail_io_source,
        year,
    )


def test_electricity_disagg_requires_waste() -> None:
    with pytest.raises(
        ValueError,
        match='implement_electricity_reallocation requires implement_waste_disaggregation',
    ):
        USAConfig.model_validate(
            {
                'implement_electricity_reallocation': True,
                'implement_waste_disaggregation': False,
            },
            strict=True,
        )


def test_electricity_mixed_units_config_parsing() -> None:
    config = _load_usa_config_from_file_name(
        'test_usa_config_waste_disagg_electricity_mixed_units.yaml'
    )
    assert config.implement_electricity_mixed_units is True
    assert config.implement_electricity_disaggregation is True


def test_electricity_mixed_units_requires_disaggregation() -> None:
    with pytest.raises(
        ValueError,
        match='implement_electricity_mixed_units requires',
    ):
        USAConfig.model_validate(
            {
                'implement_electricity_mixed_units': True,
                'implement_electricity_disaggregation': False,
                'implement_electricity_reallocation': True,
                'implement_waste_disaggregation': True,
            },
            strict=True,
        )


def test_nowcast_requires_ghg_year_to_match_io_year() -> None:
    """x is the nowcast Make row sum at the IO year; E must be that same year."""
    with pytest.raises(
        ValueError, match='usa_ghg_data_year must equal usa_base_io_data_year'
    ):
        USAConfig.model_validate(
            {
                'usa_detail_io_source': 'nowcast',
                'usa_base_io_data_year': 2023,
                'model_base_year': 2023,
                'usa_ghg_data_year': 2022,
            },
            strict=True,
        )


def test_nowcast_forbids_deflated_x_in_b() -> None:
    with pytest.raises(
        ValueError, match='deflate_x_to_detail_io_year_for_B is incompatible'
    ):
        USAConfig.model_validate(
            {
                'usa_detail_io_source': 'nowcast',
                'usa_base_io_data_year': 2023,
                'model_base_year': 2023,
                'usa_ghg_data_year': 2023,
                'use_E_data_year_for_x_in_B': True,
                'deflate_x_to_detail_io_year_for_B': True,
            },
            strict=True,
        )


@pytest.mark.parametrize('year', range(2017, 2025))
def test_v0_4_nowcast_yamls_load(year: int) -> None:
    cfg = _load_usa_config_from_file_name(
        f'2025_usa_cornerstone_v0_4_nowcast_{year}.yaml'
    )
    assert cfg.usa_detail_io_source == 'nowcast'
    assert cfg.usa_base_io_data_year == cfg.usa_ghg_data_year == year
    assert cfg.model_base_year == year
    assert cfg.usa_detail_original_year == year
    assert not cfg.deflate_x_to_detail_io_year_for_B


@pytest.mark.parametrize('year', range(2017, 2025))
def test_v0_5_yearly_yamls_load(year: int) -> None:
    """Same family as the v0.4 per-year nowcast configs, on the v0.5 build.

    Named without ``_nowcast_``: v0.5 has no ``bea_published`` per-year
    family to disambiguate against, so the qualifier would be noise.
    """
    cfg = _load_usa_config_from_file_name(f'2025_usa_cornerstone_v0_5_{year}.yaml')
    assert cfg.usa_detail_io_source == 'nowcast'
    assert cfg.usa_base_io_data_year == cfg.usa_ghg_data_year == year
    assert cfg.model_base_year == year
    assert cfg.usa_detail_original_year == year
    assert not cfg.deflate_x_to_detail_io_year_for_B
    assert cfg.nowcast_mut_vintage == 'v0.3.0_3096818'
    assert cfg.use_facility_ghg_attribution

    v4 = _load_usa_config_from_file_name(
        f'2025_usa_cornerstone_v0_4_nowcast_{year}.yaml'
    )
    _v5_only = {
        'nowcast_mut_vintage',
        'use_facility_ghg_attribution',
        'cornerstone_ghg_fbs_filename',
    }
    assert cfg.model_dump(exclude=_v5_only) == v4.model_dump(exclude=_v5_only)


def test_v0_5_release_yaml_pins_the_v0_5_build() -> None:
    """v0.4's configuration on the v0.5 nowcast build; MUT pin + facility GHG."""
    v05 = _load_usa_config_from_file_name('2025_usa_cornerstone_v0_5.yaml')
    v04 = _load_usa_config_from_file_name('2025_usa_cornerstone_v0_4.yaml')
    assert v05.nowcast_mut_vintage == 'v0.3.0_3096818'
    assert v05.use_facility_ghg_attribution
    assert not v04.use_facility_ghg_attribution
    _v05_only = {
        'nowcast_mut_vintage',
        'use_facility_ghg_attribution',
        'cornerstone_ghg_fbs_filename',
    }
    assert v05.model_dump(exclude=_v05_only) == v04.model_dump(exclude=_v05_only)


def test_v0_5_9_pins_the_2026q2_rebuild() -> None:
    """v0.5.9 loads the v0.5.0_a166975 MUT and the 2026Q2 price index."""
    v059 = _load_usa_config_from_file_name('2025_usa_cornerstone_v0_5_9.yaml')
    v05 = _load_usa_config_from_file_name('2025_usa_cornerstone_v0_5.yaml')
    assert v059.nowcast_mut_vintage == 'v0.5.0_a166975'
    assert v059.bea_price_index_vintage == '2026Q2'
    assert v059.use_facility_ghg_attribution
    assert v059.cornerstone_ghg_fbs_filename == (
        'GHG_national_Cornerstone_nowcast_facilities_2024_v0.5.0_a166975.parquet'
    )
    _v059_only = {
        'nowcast_mut_vintage',
        'bea_price_index_vintage',
        'cornerstone_ghg_fbs_filename',
    }
    assert v059.model_dump(exclude=_v059_only) == v05.model_dump(exclude=_v059_only)


def test_v0_5_9_transport_selects_the_year_cornerstone_fbs() -> None:
    """Transport config is nowcast 2024 and pins the older transportation FBS."""
    from bedrock.transform.allocation.derived import (
        _select_cornerstone_ghg_fbs_base_name,
    )

    transport = _load_usa_config_from_file_name(
        '2025_usa_cornerstone_v0_5_9_transport.yaml'
    )
    assert transport.usa_detail_io_source == 'nowcast'
    assert transport.usa_base_io_data_year == 2024
    assert transport.usa_ghg_data_year == 2024
    assert transport.nowcast_mut_vintage == 'v0.5.0_a166975'
    assert transport.bea_price_index_vintage == '2026Q2'
    assert transport.cornerstone_ghg_fbs_filename == (
        'GHG_national_Cornerstone_nowcast_facilities_transportation_2024_'
        'v0.5.0_011f41f.parquet'
    )
    set_global_usa_config('2025_usa_cornerstone_v0_5_9_transport.yaml')
    assert _select_cornerstone_ghg_fbs_base_name() == 'GHG_national_Cornerstone_2024'
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config('2025_usa_cornerstone_v0_5_9.yaml')
    assert _select_cornerstone_ghg_fbs_base_name() == 'GHG_national_Cornerstone_2024'


def test_v0_6_matches_v0_5_9_transport() -> None:
    """v0.6 matches v0.5.9 transport except the GHG FBS pin and
    use_facility_ghg_attribution. The loader returns
    GHG_national_Cornerstone_2024.
    """
    from bedrock.transform.allocation.derived import (
        _select_cornerstone_ghg_fbs_base_name,
    )

    v06 = _load_usa_config_from_file_name('2025_usa_cornerstone_v0_6.yaml')
    transport = _load_usa_config_from_file_name(
        '2025_usa_cornerstone_v0_5_9_transport.yaml'
    )
    assert v06.usa_detail_io_source == 'nowcast'
    assert v06.usa_base_io_data_year == 2024
    assert v06.nowcast_mut_vintage == 'v0.5.0_a166975'
    assert not v06.use_facility_ghg_attribution
    assert v06.cornerstone_ghg_fbs_filename == (
        'GHG_national_Cornerstone_nowcast_facilities_transportation_2024_'
        'v0.5.0_82e24cd.parquet'
    )
    excluded = {'cornerstone_ghg_fbs_filename', 'use_facility_ghg_attribution'}
    assert v06.model_dump(exclude=excluded) == transport.model_dump(exclude=excluded)
    set_global_usa_config('2025_usa_cornerstone_v0_6.yaml')
    assert _select_cornerstone_ghg_fbs_base_name() == 'GHG_national_Cornerstone_2024'
    reset_usa_config(should_reset_env_var=True)


@pytest.mark.parametrize('year', range(2017, 2025))
def test_v1_0_yearly_yamls_load(year: int) -> None:
    """v1.0 year series: v0.6 methodology on retained-activity year FBS.

    Named ``2026_usa_cornerstone_v1_0_{year}``. Each member pins
    ``GHG_national_Cornerstone_{year}_v0.6.0_635777c``. The unsuffixed
    ``2026_usa_cornerstone_v1_0`` (live ``CANONICAL_USA_CONFIG``) matches
    the 2024 series member. ``2025_usa_cornerstone_v0_6`` keeps the
    facilities-transportation 2024 pin for the v0.6 release / waterfall FINAL.
    """
    from bedrock.transform.allocation.derived import (
        _select_cornerstone_ghg_fbs_base_name,
    )

    cfg = _load_usa_config_from_file_name(f'2026_usa_cornerstone_v1_0_{year}.yaml')
    release = _load_usa_config_from_file_name('2025_usa_cornerstone_v0_6.yaml')
    assert cfg.usa_detail_io_source == 'nowcast'
    assert cfg.usa_base_io_data_year == cfg.usa_ghg_data_year == year
    assert cfg.model_base_year == year
    assert cfg.usa_detail_original_year == year
    assert not cfg.deflate_x_to_detail_io_year_for_B
    assert cfg.bea_price_index_vintage == '2026Q2'
    assert cfg.cornerstone_ghg_fbs_filename == (
        f'GHG_national_Cornerstone_{year}_v0.6.0_635777c.parquet'
    )
    assert cfg.nowcast_mut_vintage == 'v1.0.0_5514c3b'

    exclude = {
        'model_base_year',
        'usa_base_io_data_year',
        'usa_ghg_data_year',
        'cornerstone_ghg_fbs_filename',
        'nowcast_mut_vintage',
    }
    assert cfg.model_dump(exclude=exclude) == release.model_dump(exclude=exclude)

    assert release.cornerstone_ghg_fbs_filename == (
        'GHG_national_Cornerstone_nowcast_facilities_transportation_2024_'
        'v0.5.0_82e24cd.parquet'
    )

    set_global_usa_config(f'2026_usa_cornerstone_v1_0_{year}.yaml')
    assert _select_cornerstone_ghg_fbs_base_name() == (
        f'GHG_national_Cornerstone_{year}'
    )
    reset_usa_config(should_reset_env_var=True)


def test_v1_0_release_yaml_matches_2024_series_member() -> None:
    """Unsuffixed v1.0 is the 2024 model; same dump as ``…_v1_0_2024``."""
    from bedrock.transform.allocation.derived import (
        _select_cornerstone_ghg_fbs_base_name,
    )

    base = _load_usa_config_from_file_name('2026_usa_cornerstone_v1_0.yaml')
    y2024 = _load_usa_config_from_file_name('2026_usa_cornerstone_v1_0_2024.yaml')
    assert base.model_dump() == y2024.model_dump()
    assert base.cornerstone_ghg_fbs_filename == (
        'GHG_national_Cornerstone_2024_v0.6.0_635777c.parquet'
    )
    set_global_usa_config('2026_usa_cornerstone_v1_0.yaml')
    assert _select_cornerstone_ghg_fbs_base_name() == 'GHG_national_Cornerstone_2024'
    reset_usa_config(should_reset_env_var=True)
    assert CANONICAL_USA_CONFIG == '2026_usa_cornerstone_v1_0'


def test_v0_4_release_yaml_loads() -> None:
    cfg = _load_usa_config_from_file_name('2025_usa_cornerstone_v0_4.yaml')
    assert cfg.usa_detail_io_source == 'nowcast'
    assert cfg.nowcast_mut_vintage == 'v0.3.0_4276083'
    assert cfg.usa_base_io_data_year == cfg.usa_ghg_data_year == 2024
    assert cfg.model_base_year == 2024
    assert cfg.iot_before_or_after_redefinition == 'after'
    assert cfg.implement_waste_disaggregation
    assert cfg.cornerstone_industry_avg_margins
    assert not cfg.apply_io_year_adjustments
    assert not cfg.deflate_x_to_detail_io_year_for_B


def _resolved_field_diff(a: str, b: str) -> dict[str, tuple[object, object]]:
    da = _load_usa_config_from_file_name(f'{a}.yaml').model_dump()
    db = _load_usa_config_from_file_name(f'{b}.yaml').model_dump()
    return {k: (da[k], db[k]) for k in da if da[k] != db[k]}


def test_v0_6_waterfall_rungs_bracket_the_release_config() -> None:
    """G5 matches v0.5.9. G5 to the v0.6 release is the GHG FBS pin
    and use_facility_ghg_attribution.
    """
    assert (
        _resolved_field_diff(
            'v06_waterfall_g5_facilities', '2025_usa_cornerstone_v0_5_9'
        )
        == {}
    )
    g4_to_g5 = _resolved_field_diff(
        'v06_waterfall_g4_nowcast', 'v06_waterfall_g5_facilities'
    )
    assert set(g4_to_g5) == {
        'nowcast_mut_vintage',
        'bea_price_index_vintage',
        'use_facility_ghg_attribution',
        'cornerstone_ghg_fbs_filename',
    }
    release = _resolved_field_diff(
        'v06_waterfall_g5_facilities', '2025_usa_cornerstone_v0_6'
    )
    assert set(release) == {
        'use_facility_ghg_attribution',
        'cornerstone_ghg_fbs_filename',
    }
    assert release['use_facility_ghg_attribution'] == (True, False)


def test_electricity_reaggregation_config_parsing() -> None:
    config = _load_usa_config_from_file_name(
        'test_usa_config_waste_disagg_electricity_reaggregation.yaml'
    )
    assert config.implement_electricity_reaggregation is True
    assert config.implement_electricity_disaggregation is True
    assert config.implement_electricity_mixed_units is False


def test_electricity_reaggregation_requires_disaggregation() -> None:
    with pytest.raises(
        ValueError,
        match='implement_electricity_reaggregation requires',
    ):
        USAConfig.model_validate(
            {
                'implement_electricity_reaggregation': True,
                'implement_electricity_disaggregation': False,
                'implement_electricity_reallocation': True,
                'implement_waste_disaggregation': True,
            },
            strict=True,
        )


def test_electricity_reaggregation_xor_mixed_units() -> None:
    with pytest.raises(
        ValueError,
        match='implement_electricity_reaggregation is mutually exclusive',
    ):
        USAConfig.model_validate(
            {
                'implement_electricity_reaggregation': True,
                'implement_electricity_mixed_units': True,
                'implement_electricity_disaggregation': True,
                'implement_electricity_reallocation': True,
                'implement_waste_disaggregation': True,
            },
            strict=True,
        )
