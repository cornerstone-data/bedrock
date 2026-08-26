"""Clear publish getters and upstream derive_* caches between configs."""

from __future__ import annotations

import sys
from collections.abc import Callable
from types import ModuleType

from bedrock.extract.iot.io_2017 import (
    load_2017_margins_after_redef_usa,
    load_2017_margins_before_redef_usa,
)
from bedrock.publish.model_objects import clear_publish_caches
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_disaggregation_enabled,
    electricity_mixed_units_enabled,
    electricity_reallocation_enabled,
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
    derive_cornerstone_A_margin,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_mixed_units,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_q,
    derive_cornerstone_U_set,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_VA,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_mixed_units,
    derive_cornerstone_Ytot_matrix_set,
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

# Cached electricity helpers cleared only if their modules are already loaded.
# Never import electricity_disaggregation / electricity_end_use_mapping here —
# that would re-couple v0.3 / waste-only publish clears.
_ELECTRICITY_DISAGG_CACHED_ATTRS: tuple[str, ...] = (
    'get_2017_eia_purchaser_allocation',
    '_derive_post_reallocation_checkpoint_for_disagg',
    'build_electricity_disagg_use_intersection_weights',
    'build_electricity_detail_GO_growth_ratios',
    'build_electricity_disagg_go_weights',
    'applied_utilities_summary_q_growth_ratio',
)

UPSTREAM_CACHED_DERIVES: list[Callable[..., object]] = [
    derive_B_usa_non_finetuned,
    derive_C_usa,
    derive_D_usa,
    derive_Aq_usa,
    derive_y_for_national_accounting_balance_usa,
    cornerstone_sector_disagg_active,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    electricity_mixed_units_enabled,
    get_waste_disagg_weights,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    derive_cornerstone_V,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_q,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    derive_cornerstone_Ytot_matrix_set,
    derive_cornerstone_VA,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_A_margin,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_B_mixed_units,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_mixed_units,
    load_2017_margins_before_redef_usa,
    load_2017_margins_after_redef_usa,
    derive_margins_cornerstone_usa_at_year,
    derive_phi_cornerstone_usa_at_year,
    derive_phi_cornerstone_usa_panel,
    derive_price_index_panel,
    get_price_index_ratio,
]


def _clear_cached_attrs(mod: ModuleType, names: tuple[str, ...]) -> None:
    for name in names:
        fn = getattr(mod, name, None)
        cache_clear = getattr(fn, 'cache_clear', None)
        if callable(cache_clear):
            cache_clear()


def _clear_electricity_caches_if_loaded() -> None:
    """Flush elec ``@cache``s only when those modules are already in ``sys.modules``.

    After a flag-on run, leftover caches must still clear when switching to v0.3;
    under a never-loaded v0.3 process, this is a no-op and never imports elec.
    """
    ed = sys.modules.get('bedrock.transform.eeio.electricity_disaggregation')
    if ed is not None:
        _clear_cached_attrs(ed, _ELECTRICITY_DISAGG_CACHED_ATTRS)
    gtd = sys.modules.get('bedrock.transform.eeio.electricity_gtd_allocation')
    if gtd is not None:
        _clear_cached_attrs(gtd, ('get_2017_eia_purchaser_allocation',))
        clear_reanchored = getattr(gtd, 'clear_reanchored_electricity_q', None)
        if callable(clear_reanchored):
            clear_reanchored()
    eum = sys.modules.get('bedrock.transform.eeio.electricity_end_use_mapping')
    if eum is not None:
        # No @cache today; keep for future-proofing if helpers become cached.
        _clear_cached_attrs(
            eum,
            (
                'build_end_use_map',
                'electricity_end_use_retail_prices_cents_kwh',
            ),
        )
    cys = sys.modules.get('bedrock.transform.eeio.cornerstone_year_scaling')
    if cys is not None and hasattr(cys, 'clear_summary_year_scaled_aq'):
        cys.clear_summary_year_scaled_aq()
    egrid = sys.modules.get('bedrock.extract.disaggregation.egrid_generation')
    if egrid is not None:
        _clear_cached_attrs(
            egrid,
            (
                'eia_table_2_2_end_use_mwh',
                'eia_table_2_14_export_mwh',
                'eia_table_2_14_year_for_egrid_year',
                'eia_table_3_1_total_mwh',
                'egrid_mwh_for_io_year',
            ),
        )


def clear_all_publish_caches() -> None:
    clear_cornerstone_inflation_caches()
    for fn in UPSTREAM_CACHED_DERIVES:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    _clear_electricity_caches_if_loaded()
    clear_publish_caches()
