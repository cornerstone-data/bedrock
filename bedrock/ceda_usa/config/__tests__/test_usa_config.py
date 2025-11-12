# ruff: noqa: PLC0415

from typing import Generator

import pytest

from bedrock.ceda_usa.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)


@pytest.fixture(autouse=True, scope="function")
def reset_global_usa_config_before_test() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield


def test_global_usa_config() -> None:
    set_global_usa_config("test_usa_config.yaml")
    usa_config = get_usa_config()
    assert usa_config.usa_ghg_data_year == 2022


def test_cannot_call_global_usa_config_twice() -> None:
    set_global_usa_config("test_usa_config.yaml")
    with pytest.raises(ValueError):
        set_global_usa_config("test_usa_config.yaml")


def test_config_via_environment_variable() -> None:
    """Test that config can be loaded from environment variable in worker processes."""
    import os

    from bedrock.ceda_usa.config.usa_config import CEDA_USA_CONFIG_ENV_VAR

    set_global_usa_config("test_usa_config.yaml")
    assert CEDA_USA_CONFIG_ENV_VAR in os.environ
    assert os.environ[CEDA_USA_CONFIG_ENV_VAR] == "test_usa_config.yaml"

    # reset config (mimick a worker process with a new
    # memory space) and reload the config from env variable
    reset_usa_config(should_reset_env_var=False)
    usa_config = get_usa_config()
    assert usa_config.usa_ghg_data_year == 2022
