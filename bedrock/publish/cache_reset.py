"""Clear publish getters and upstream derive_* caches between configs."""

from __future__ import annotations

from collections.abc import Callable

from bedrock.extract.iot.io_2017 import (
    load_2017_margins_after_redef_usa,
    load_2017_margins_before_redef_usa,
)
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
from bedrock.transform.eeio.electricity_disaggregation import (
    _derive_post_reallocation_checkpoint_for_disagg,
    build_electricity_disagg_use_intersection_weights,
    build_electricity_ugo305_scaling_ratios,
    get_electricity_commodity_row_weights,
)
from bedrock.transform.iot.derive_PRO_to_PUR_ratio import (
    derive_margins_cornerstone_usa_at_year,
    derive_phi_cornerstone_usa_at_year,
    derive_phi_cornerstone_usa_panel,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    clear_cornerstone_inflation_caches,
    derive_price_index_panel,
    get_price_index_ratio,
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
    get_electricity_commodity_row_weights,
    _derive_post_reallocation_checkpoint_for_disagg,
    build_electricity_disagg_use_intersection_weights,
    build_electricity_ugo305_scaling_ratios,
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
    load_2017_margins_before_redef_usa,
    load_2017_margins_after_redef_usa,
    derive_margins_cornerstone_usa_at_year,
    derive_phi_cornerstone_usa_at_year,
    derive_phi_cornerstone_usa_panel,
    derive_price_index_panel,
    get_price_index_ratio,
]


def clear_all_publish_caches() -> None:
    clear_cornerstone_inflation_caches()
    for fn in UPSTREAM_CACHED_DERIVES:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_publish_caches()
