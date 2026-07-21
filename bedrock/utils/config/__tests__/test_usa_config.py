# ruff: noqa: PLC0415

from typing import Generator

import pytest

from bedrock.utils.config.usa_config import (
    EEIOWasteDisaggConfig,
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


def test_eeio_waste_disagg_config_parsing_happy_path() -> None:
    """Test to ensure that the disaggregation config file is properly loaded"""
    config = _load_usa_config_from_file_name('test_usa_config_waste_disagg.yaml')
    assert config.eeio_waste_disaggregation is not None
    wd = config.eeio_waste_disaggregation
    assert isinstance(wd, EEIOWasteDisaggConfig)
    assert (
        wd.use_weights_file
        == 'extract/disaggregation/waste_disagg_inputs/WasteDisaggregationDetail2017_Use.csv'
    )
    assert (
        wd.make_weights_file
        == 'extract/disaggregation/waste_disagg_inputs/WasteDisaggregationDetail2017_Make.csv'
    )
    assert wd.year == 2017
    assert wd.source_name == 'WasteDisaggregationDetail2017'


def test_eeio_waste_disagg_config_optional_missing() -> None:
    """Test to ensure that the disaggregation config is not
    true when not specified in the yaml file"""
    config = _load_usa_config_from_file_name('test_usa_config.yaml')
    assert config.eeio_waste_disaggregation is None


def test_get_usa_config_loads_waste_disagg() -> None:
    """Test that the usa_config correctly loads the waste
    disaggregation yaml file"""
    set_global_usa_config('test_usa_config_waste_disagg.yaml')
    config = get_usa_config()
    assert config.implement_waste_disaggregation is True
    assert config.eeio_waste_disaggregation is not None
    assert (
        config.eeio_waste_disaggregation.source_name == 'WasteDisaggregationDetail2017'
    )


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


def test_phoebe_year_vector_accepts_2017_io_fields() -> None:
    cfg = USAConfig.model_validate(
        {'model_base_year': 2017, 'usa_io_data_year': 2017},
        strict=True,
    )
    assert cfg.model_base_year == 2017
    assert cfg.usa_io_data_year == 2017


def test_disallow_new_ghg_method_with_m2_flag() -> None:
    with pytest.raises(
        ValueError,
        match='new_ghg_method and use_ghg_national_2023_m2 cannot both be true',
    ):
        USAConfig.model_validate(
            {'new_ghg_method': True, 'use_ghg_national_2023_m2': True},
            strict=True,
        )


def test_allow_m2_flag_when_new_ghg_method_is_false() -> None:
    cfg = USAConfig.model_validate(
        {
            'new_ghg_method': False,
            'use_ghg_national_2023_m2': True,
            'use_useeio_schema': True,
        },
        strict=True,
    )
    assert cfg.use_ghg_national_2023_m2 is True


def test_disallow_m2_without_useeio_schema() -> None:
    with pytest.raises(
        ValueError,
        match='use_ghg_national_2023_m2 requires use_useeio_schema to be true',
    ):
        USAConfig.model_validate(
            {'use_ghg_national_2023_m2': True, 'use_useeio_schema': False},
            strict=True,
        )


def test_disallow_deflate_x_without_use_e_for_x_in_b() -> None:
    with pytest.raises(
        ValueError,
        match='deflate_x_to_detail_io_year_for_B requires use_E_data_year_for_x_in_B',
    ):
        USAConfig.model_validate(
            {
                'deflate_x_to_detail_io_year_for_B': True,
                'use_E_data_year_for_x_in_B': False,
            },
            strict=True,
        )


@pytest.mark.parametrize(
    'flags',
    [
        {'useeio_margins': True, 'ceda_margins': True},
        {'useeio_margins': True, 'cornerstone_industry_avg_margins': True},
        {'ceda_margins': True, 'cornerstone_industry_avg_margins': True},
    ],
)
def test_disallow_multiple_margins_flags(flags: dict[str, bool]) -> None:
    with pytest.raises(ValueError, match='At most one margins flag may be true'):
        USAConfig.model_validate(flags, strict=True)


def test_electricity_disagg_config_parsing() -> None:
    config = _load_usa_config_from_file_name(
        'test_usa_config_waste_disagg_electricity.yaml'
    )
    assert config.implement_waste_disaggregation is True
    assert config.implement_electricity_reallocation is True


def test_electricity_disaggregation_config_parsing() -> None:
    config = _load_usa_config_from_file_name(
        'test_usa_config_waste_disagg_electricity_disaggregation.yaml'
    )
    assert config.implement_waste_disaggregation is True
    assert config.implement_electricity_reallocation is True
    assert config.implement_electricity_disaggregation is True


def test_v0_2_electricity_disaggregation_config_parsing() -> None:
    config = _load_usa_config_from_file_name(
        '2025_usa_cornerstone_v0_2_electricity_disaggregation.yaml'
    )
    assert config.implement_electricity_disaggregation is True
    assert config.implement_electricity_mixed_units is False
    assert config.snapshot_version_or_git_sha == (
        '7372464249c434c9bebb172c065a4d0e3702176e'
    )


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
