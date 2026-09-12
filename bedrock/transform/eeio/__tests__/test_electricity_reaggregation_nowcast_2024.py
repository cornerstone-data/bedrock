"""Nowcast-2024 electricity reaggregation integration + config matrix tests."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Callable, Iterator
from unittest.mock import patch

import pytest

from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.publish.model_objects import get_N, get_q
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    cornerstone_sector_disagg_active,
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
    electricity_disaggregation_enabled,
    electricity_reaggregation_enabled,
    electricity_reallocation_enabled,
    get_waste_disagg_weights,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_reaggregated,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_B_reaggregated,
    derive_cornerstone_U_set,
    derive_cornerstone_U_set_reaggregated,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_V,
    derive_cornerstone_V_reaggregated,
    derive_cornerstone_VA,
    derive_cornerstone_VA_reaggregated,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_x_reaggregated,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_reaggregated,
    derive_disagg_Ytot_reaggregated,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    ELECTRICITY_DISAGG_SECTORS,
    GENERATION_SECTOR,
    _derive_post_reallocation_checkpoint_for_disagg,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_eia_purchaser_allocation,
)
from bedrock.transform.eeio.electricity_gtd_allocation import (
    get_reanchored_eia_purchaser_allocation,
    mecs_purchased_kwh,
)
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    clear_cornerstone_inflation_caches,
)

_NOWCAST_REAGG_CONFIG = (
    '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_reaggregation.yaml'
)

_LADDER_CONFIGS: dict[str, dict[str, bool]] = {
    '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_footing': {
        'implement_electricity_reallocation': False,
        'implement_electricity_disaggregation': False,
        'implement_electricity_reaggregation': False,
        'cornerstone_industry_avg_margins': False,
    },
    '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_reallocation': {
        'implement_electricity_reallocation': True,
        'implement_electricity_disaggregation': False,
        'implement_electricity_reaggregation': False,
        'cornerstone_industry_avg_margins': False,
    },
    '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_disaggregation': {
        'implement_electricity_reallocation': True,
        'implement_electricity_disaggregation': True,
        'implement_electricity_reaggregation': False,
        'cornerstone_industry_avg_margins': False,
    },
    '2025_usa_cornerstone_v0_4_nowcast_2024_electricity_reaggregation': {
        'implement_electricity_reallocation': True,
        'implement_electricity_disaggregation': True,
        'implement_electricity_reaggregation': True,
        'cornerstone_industry_avg_margins': True,
    },
}

_CACHED_FUNCTIONS: list[Callable[..., object]] = [
    get_waste_disagg_weights,
    electricity_reallocation_enabled,
    electricity_disaggregation_enabled,
    electricity_reaggregation_enabled,
    derive_disagg_io_bundle,
    cornerstone_sector_disagg_active,
    derive_disagg_Ytot_with_trade,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_eia_purchaser_allocation,
    mecs_purchased_kwh,
    _derive_post_reallocation_checkpoint_for_disagg,
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
    derive_cornerstone_U_with_negatives,
    derive_cornerstone_U_set,
    derive_cornerstone_VA,
    derive_cornerstone_x,
    derive_cornerstone_x_after_redefinition,
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_Aq_reaggregated,
    derive_cornerstone_B_non_finetuned,
    derive_cornerstone_B_reaggregated,
    derive_cornerstone_V_reaggregated,
    derive_cornerstone_U_set_reaggregated,
    derive_cornerstone_VA_reaggregated,
    derive_cornerstone_x_reaggregated,
    derive_disagg_Ytot_reaggregated,
    derive_cornerstone_y_nab,
    derive_cornerstone_y_nab_reaggregated,
]


def _clear_caches() -> None:
    for fn in _CACHED_FUNCTIONS:
        if hasattr(fn, 'cache_clear'):
            fn.cache_clear()
    clear_cornerstone_inflation_caches()
    clear_all_publish_caches()
    from bedrock.transform.eeio.cornerstone_year_scaling import (  # noqa: PLC0415
        clear_summary_year_scaled_aq,
    )
    from bedrock.transform.eeio.electricity_gtd_allocation import (  # noqa: PLC0415
        clear_reanchored_electricity_q,
    )

    clear_summary_year_scaled_aq()
    clear_reanchored_electricity_q()
    from bedrock.transform.iot.derive_PRO_to_PUR_ratio import (  # noqa: PLC0415
        derive_phi_cornerstone_usa_panel,
        derive_phi_cornerstone_usa_panel_published,
    )

    derive_phi_cornerstone_usa_panel.cache_clear()
    derive_phi_cornerstone_usa_panel_published.cache_clear()


@contextmanager
def _dollar_industrial_weights() -> Iterator[None]:
    import bedrock.transform.eeio.electricity_gtd_allocation as gtd  # noqa: PLC0415

    orig = gtd.allocate_purchaser_gtd

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        kwargs = dict(kwargs)
        kwargs['industrial_weights'] = 'dollars'
        return orig(*args, **kwargs)

    with patch.object(gtd, 'allocate_purchaser_gtd', _wrapped):
        yield


def _setup(config_name: str) -> None:
    _clear_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)


def _teardown() -> None:
    _clear_caches()
    reset_usa_config(should_reset_env_var=True)


@pytest.mark.parametrize('stem', list(_LADDER_CONFIGS))
def test_nowcast_2024_ladder_configs_flag_matrix(stem: str) -> None:
    reset_usa_config(should_reset_env_var=True)
    try:
        set_global_usa_config(stem)
        cfg = get_usa_config()
        expected = _LADDER_CONFIGS[stem]
        assert cfg.usa_detail_io_source == 'nowcast'
        assert cfg.usa_base_io_data_year == 2024
        assert cfg.model_base_year == 2024
        assert cfg.usa_ghg_data_year == 2024
        assert cfg.nowcast_mut_vintage == 'v0.3.0_4276083'
        assert cfg.apply_io_year_adjustments is False
        assert cfg.implement_waste_disaggregation is True
        assert cfg.use_cornerstone_ghg_model is True
        assert cfg.implement_electricity_mixed_units is False
        for field, value in expected.items():
            assert getattr(cfg, field) is value, field
    finally:
        reset_usa_config(should_reset_env_var=True)


@pytest.mark.eeio_integration
def test_nowcast_reagg_getters_are_405() -> None:
    _setup(_NOWCAST_REAGG_CONFIG)
    try:
        with _dollar_industrial_weights():
            n = get_N()
            q = get_q()
        assert ELECTRICITY_AGGREGATE in n.columns
        assert ELECTRICITY_AGGREGATE in q.index
        for code in ELECTRICITY_DISAGG_SECTORS:
            assert code not in n.columns
            assert code not in q.index
    finally:
        _teardown()


@pytest.mark.eeio_integration
def test_nowcast_aq_scaled_sets_reanchored_allocation() -> None:
    _setup(_NOWCAST_REAGG_CONFIG)
    try:
        with _dollar_industrial_weights():
            derive_cornerstone_Aq_scaled()
            alloc = get_reanchored_eia_purchaser_allocation()
        assert alloc is not None
    finally:
        _teardown()


@pytest.mark.eeio_integration
def test_nowcast_aq_scaled_skips_published_reanchor_helpers() -> None:
    _setup(_NOWCAST_REAGG_CONFIG)
    try:
        with (
            _dollar_industrial_weights(),
            patch(
                'bedrock.transform.eeio.electricity_gtd_allocation'
                '.reanchor_electricity_aq_after_year_scaling',
            ) as p_reanchor_scaled,
            patch(
                'bedrock.transform.eeio.cornerstone_year_scaling'
                '.get_summary_year_scaled_aq',
            ) as p_summary,
            patch(
                'bedrock.transform.eeio.electricity_gtd_allocation'
                '._purchaser_electricity_purchases_from_aq',
            ) as p_from_aq,
            patch(
                'bedrock.transform.eeio.electricity_gtd_allocation'
                '._scaled_export_fd_electricity_purchases',
            ) as p_export,
        ):
            derive_cornerstone_Aq_scaled()
        p_reanchor_scaled.assert_not_called()
        p_summary.assert_not_called()
        p_from_aq.assert_not_called()
        p_export.assert_not_called()
    finally:
        _teardown()


@pytest.mark.eeio_integration
def test_nowcast_va_g_non_negative_after_reanchor() -> None:
    _setup(_NOWCAST_REAGG_CONFIG)
    try:
        with _dollar_industrial_weights():
            aq = derive_cornerstone_Aq_scaled()
        g = GENERATION_SECTOR
        q = aq.scaled_q.astype(float)
        udom = aq.Adom.multiply(q, axis=1)
        uimp = aq.Aimp.multiply(q, axis=1)
        inputs = float(udom[g].sum()) + float(uimp[g].sum())
        x_g = float(q.loc[g])
        va_g = x_g - inputs
        assert va_g >= -1e-6
    finally:
        _teardown()
