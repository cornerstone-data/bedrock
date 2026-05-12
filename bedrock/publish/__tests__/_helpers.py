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

from typing import Callable

import bedrock.utils.config.common as common
from bedrock.publish.excel.writer import clear_publish_caches
from bedrock.transform.eeio.derived import (
    derive_Aq_usa,
    derive_B_usa_non_finetuned,
    derive_C_usa,
    derive_D_usa,
    derive_y_for_national_accounting_balance_usa,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_q,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_y_nab,
    derive_cornerstone_Ytot_matrix_set,
)
from bedrock.utils.config.usa_config import (
    reset_usa_config,
    set_global_usa_config,
)

# Upstream derive_* caches that the writer's getters compose. The writer's
# own publish-side getter caches are cleared via `clear_publish_caches()`
# below.
CACHED_FUNCTIONS: list[Callable[..., object]] = [
    derive_B_usa_non_finetuned,
    derive_C_usa,
    derive_D_usa,
    derive_Aq_usa,
    derive_y_for_national_accounting_balance_usa,
    derive_cornerstone_V,
    derive_cornerstone_x,
    derive_cornerstone_q,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    derive_cornerstone_Ytot_matrix_set,
    derive_cornerstone_VA,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_y_nab,
]


def clear_all_caches() -> None:
    for fn in CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_publish_caches()


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
