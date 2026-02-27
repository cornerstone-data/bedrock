# ruff: noqa: PLC0415

from typing import Generator

import pytest
from pydantic import ValidationError

from bedrock.utils.config.usa_config import (
    EEIOWasteDisaggConfig,
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
    _load_usa_config_from_file_name,
)


@pytest.fixture(autouse=True, scope="function")
def reset_global_usa_config_before_test() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield


def test_eeio_waste_disagg_config_parsing_happy_path() -> None:
    config = _load_usa_config_from_file_name("test_usa_config_waste_disagg.yaml")
    assert config.eeio_waste_disaggregation is not None
    wd = config.eeio_waste_disaggregation
    assert isinstance(wd, EEIOWasteDisaggConfig)
    assert wd.use_weights_file == "extract/disaggregation/WasteDisaggregationDetail2017_Use.csv"
    assert wd.make_weights_file == "extract/disaggregation/WasteDisaggregationDetail2017_Make.csv"
    assert wd.year == 2017
    assert wd.source_name == "WasteDisaggregationDetail2017"


def test_eeio_waste_disagg_config_optional_missing() -> None:
    config = _load_usa_config_from_file_name("test_usa_config.yaml")
    assert config.eeio_waste_disaggregation is None


def test_eeio_waste_disagg_config_invalid_fails() -> None:
    with pytest.raises(ValidationError):
        _load_usa_config_from_file_name("test_usa_config_waste_disagg_invalid.yaml")


def test_get_usa_config_loads_waste_disagg() -> None:
    set_global_usa_config("test_usa_config_waste_disagg.yaml")
    config = get_usa_config()
    assert config.implement_waste_disaggregation is True
    assert config.eeio_waste_disaggregation is not None
    assert config.eeio_waste_disaggregation.source_name == "WasteDisaggregationDetail2017"


def test_global_usa_config() -> None:
    set_global_usa_config("test_usa_config.yaml")
    usa_config = get_usa_config()
    assert usa_config.usa_ghg_data_year == 2023


def test_cannot_call_global_usa_config_twice() -> None:
    set_global_usa_config("test_usa_config.yaml")
    with pytest.raises(ValueError):
        set_global_usa_config("test_usa_config.yaml")


def test_config_via_environment_variable() -> None:
    """Test that config can be loaded from environment variable in worker processes."""
    import os

    from bedrock.utils.config.usa_config import USA_CONFIG_ENV_VAR

    set_global_usa_config("test_usa_config.yaml")
    assert USA_CONFIG_ENV_VAR in os.environ
    assert os.environ[USA_CONFIG_ENV_VAR] == "test_usa_config.yaml"

    # reset config (mimick a worker process with a new
    # memory space) and reload the config from env variable
    reset_usa_config(should_reset_env_var=False)
    usa_config = get_usa_config()
    assert usa_config.usa_ghg_data_year == 2023
