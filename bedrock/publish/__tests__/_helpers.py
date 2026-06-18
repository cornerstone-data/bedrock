"""Shared helpers for `bedrock/publish/__tests__/`.

The publish pipeline calls into a stack of `@functools.cache`-d
`derive_*_usa()` functions and a process-wide `USAConfig`. Integration
tests must reset both between configs or stale state leaks between
tests. Mirrors the pattern in
`bedrock/transform/eeio/__tests__/test_waste_disagg_pipeline_integration.py`.

`setup_config` also flips `bedrock.utils.config.common.download_fba_on_api_error`
so eeio_integration tests work in environments without USDA FBA API
credentials (FBA is fetched from the GCS cache instead).
"""

from __future__ import annotations

import bedrock.utils.config.common as common
from bedrock.publish.cache_reset import (
    clear_all_publish_caches,
)
from bedrock.utils.config.usa_config import (
    reset_usa_config,
    set_global_usa_config,
)


def clear_all_caches() -> None:
    clear_all_publish_caches()


def setup_config(config_name: str) -> None:
    """Reset config + caches, then set a fresh global config.

    Also enables FBA-on-API-error fallback so derivations can pull
    FBA from the GCS cache when USDA API credentials are absent.
    """
    clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)
    common.download_fba_on_api_error = True


def teardown() -> None:
    clear_all_caches()
    reset_usa_config(should_reset_env_var=True)
    common.download_fba_on_api_error = False
