from __future__ import annotations

import typing as ta

import pandas as pd
import pytest

import bedrock.extract.allocation.epa as epa
import bedrock.utils.config.common as common
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.snapshots.loader import load_current_snapshot

# CH4/N2O/CO2/other_gases integration tests exercise per-source EPA allocation at
# usa_ghg_data_year 2023. EPA table extract supports 2022/2023 only; there is
# no EPA 2024 inventory path and these allocators are slated for retirement.
# Runtime v0.3 loads pre-built 2024 FBS from GCS and does not use this path.
_ALLOCATION_2023_SUBPACKAGES = frozenset({'ch4', 'n2o', 'co2', 'other_gases'})
_USA_CONFIG_FOR_ALLOCATION_INTEGRATION = 'test_usa_config'


def _clear_epa_extract_caches() -> None:
    for obj in epa.__dict__.values():
        cache_clear = getattr(obj, 'cache_clear', None)
        if cache_clear is not None:
            cache_clear()


def _is_epa_allocation_integration_test(request: pytest.FixtureRequest) -> bool:
    if request.node.get_closest_marker('eeio_integration') is None:
        return False
    parts = request.node.path.parts
    try:
        allocation_idx = parts.index('allocation')
    except ValueError:
        return False
    if allocation_idx + 1 >= len(parts):
        return False
    return parts[allocation_idx + 1] in _ALLOCATION_2023_SUBPACKAGES


@pytest.fixture(autouse=True)
def usa_config_2023_for_epa_allocation_integration(
    request: pytest.FixtureRequest,
) -> ta.Iterator[None]:
    if not _is_epa_allocation_integration_test(request):
        yield
        return

    _clear_epa_extract_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(_USA_CONFIG_FOR_ALLOCATION_INTEGRATION)
    common.download_fba_on_api_error = True
    try:
        yield
    finally:
        _clear_epa_extract_caches()
        reset_usa_config(should_reset_env_var=True)
        common.download_fba_on_api_error = False


@pytest.fixture(scope='session')
def E_usa_es_snapshot() -> pd.DataFrame:
    return load_current_snapshot('E_USA_ES')
