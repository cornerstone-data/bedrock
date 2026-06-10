"""Clear publish getters and upstream derive_* caches between configs."""

from __future__ import annotations

from collections.abc import Callable

from bedrock.publish.model_objects import clear_publish_caches
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    get_waste_disagg_weights,
)
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

UPSTREAM_CACHED_DERIVES: list[Callable[..., object]] = [
    derive_B_usa_non_finetuned,
    derive_C_usa,
    derive_D_usa,
    derive_Aq_usa,
    derive_y_for_national_accounting_balance_usa,
    cornerstone_sector_disagg_active,
    get_waste_disagg_weights,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
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


def clear_all_publish_caches() -> None:
    for fn in UPSTREAM_CACHED_DERIVES:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_publish_caches()
