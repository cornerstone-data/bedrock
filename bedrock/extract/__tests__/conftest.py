from __future__ import annotations

import typing as ta

import pytest

import bedrock.extract.allocation.epa as epa
import bedrock.utils.config.common as common
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config

# EPA_GHGI FBA generation loads staged tables via allocation/epa.py, which
# reads usa_ghg_data_year from the global config. Pin 2023 so regen tests do
# not inherit v0.3 (2024 / pre-built FBS from GCS).
_USA_CONFIG_FOR_EPA_FBA_INTEGRATION = 'test_usa_config'


def _clear_epa_extract_caches() -> None:
    for obj in epa.__dict__.values():
        cache_clear = getattr(obj, 'cache_clear', None)
        if cache_clear is not None:
            cache_clear()


@pytest.fixture(autouse=True)
def usa_config_2023_for_epa_fba_integration(
    request: pytest.FixtureRequest,
) -> ta.Iterator[None]:
    if request.node.get_closest_marker('eeio_integration') is None:
        yield
        return

    _clear_epa_extract_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(_USA_CONFIG_FOR_EPA_FBA_INTEGRATION)
    common.download_fba_on_api_error = True
    try:
        yield
    finally:
        _clear_epa_extract_caches()
        reset_usa_config(should_reset_env_var=True)
        common.download_fba_on_api_error = False
