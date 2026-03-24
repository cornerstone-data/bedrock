from __future__ import annotations

from typing import Generator

import pytest

from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.snapshots.loader import resolve_snapshot_key


@pytest.fixture(autouse=True, scope="function")
def reset_global_usa_config_before_test() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield


def test_resolve_snapshot_key_uses_v0_when_config_defaults() -> None:
    set_global_usa_config("test_usa_config.yaml")
    assert resolve_snapshot_key() == "v0"


def test_resolve_snapshot_key_uses_configured_git_sha() -> None:
    set_global_usa_config("test_usa_config_git_sha.yaml")
    assert resolve_snapshot_key() == "5f32e53941e58023a331ef9f3df46e8834891aa2"
