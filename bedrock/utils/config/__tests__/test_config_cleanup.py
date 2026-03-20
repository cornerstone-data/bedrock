"""Tests verifying the config cleanup removed the correct flags and behavior."""

from __future__ import annotations

import inspect
from typing import Generator

import pytest

from bedrock.utils.config.usa_config import (
    USAConfig,
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)


@pytest.fixture(autouse=True)
def _reset_config() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield
    reset_usa_config(should_reset_env_var=True)


REMOVED_FLAGS = [
    'use_cornerstone_2026_model_schema',
    'load_E_from_flowsa',
    'new_ghg_method',
    'use_E_data_year_for_x_in_B',
    'implement_waste_disaggregation',
    'update_transportation_ghg_method',
    'update_ghg_coa_allocation',
    'update_electricity_ghg_method',
    'update_ghg_attribution_method_for_ng_and_petrol_systems',
    'update_flowsa_refrigerant_method',
    'add_new_ghg_activities',
    'update_enteric_fermentation_and_manure_management_ghg_method',
    'update_liming_and_fertilizer_ghg_method',
    'update_other_gases_ghg_method',
    'usa_ghg_methodology',
]

KEPT_FLAGS = [
    'scale_a_matrix_with_useeio_method',
    'scale_a_matrix_with_summary_tables',
    'scale_a_matrix_with_price_index',
]


def test_removed_flags_not_in_usa_config() -> None:
    for flag in REMOVED_FLAGS:
        assert flag not in USAConfig.model_fields, f"{flag} should be removed"


def test_kept_flags_still_in_usa_config() -> None:
    for flag in KEPT_FLAGS:
        assert flag in USAConfig.model_fields, f"{flag} should be retained"


def test_default_config_loads_successfully() -> None:
    set_global_usa_config("v8_ceda_2025_usa.yaml")
    cfg = get_usa_config()
    assert cfg.model_base_year == 2023
    assert cfg.usa_ghg_data_year == 2023


def test_allocation_sectors_returns_cornerstone() -> None:
    from bedrock.transform.allocation.utils import get_allocation_sectors
    from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES

    assert get_allocation_sectors() == list(INDUSTRIES)


def test_waste_allocation_always_splits() -> None:
    from bedrock.transform.allocation.waste_utils import (
        waste_562000_allocation_series_ceda_allocator_to_cornerstone_schema,
    )

    result = waste_562000_allocation_series_ceda_allocator_to_cornerstone_schema(100.0)
    assert '562000' not in result.index
    assert len(result) > 1


def test_derived_wrappers_delegate_to_cornerstone() -> None:
    from bedrock.transform.eeio import derived

    for fn_name in [
        'derive_B_usa_non_finetuned',
        'derive_y_for_national_accounting_balance_usa',
        'derive_Aq_usa',
    ]:
        fn = getattr(derived, fn_name)
        unwrapped = getattr(fn, '__wrapped__', fn)
        source = inspect.getsource(unwrapped)
        assert 'get_usa_config' not in source, (
            f"{fn_name} should not reference get_usa_config"
        )


def test_B_non_finetuned_no_inflate_branch() -> None:
    from bedrock.transform.eeio.derived_cornerstone import (
        derive_cornerstone_B_non_finetuned,
    )

    unwrapped = getattr(
        derive_cornerstone_B_non_finetuned, '__wrapped__',
        derive_cornerstone_B_non_finetuned,
    )
    source = inspect.getsource(unwrapped)
    assert 'use_E_data_year_for_x_in_B' not in source
    assert 'inflate_cornerstone_B_matrix' not in source


def test_waste_disagg_weights_never_returns_none() -> None:
    from bedrock.transform.eeio.derived_cornerstone import get_waste_disagg_weights

    unwrapped = getattr(
        get_waste_disagg_weights, '__wrapped__', get_waste_disagg_weights,
    )
    source = inspect.getsource(unwrapped)
    assert 'return None' not in source
    assert 'implement_waste_disaggregation' not in source


def test_cornerstone_Aq_always_uses_disaggregated_path() -> None:
    from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_Aq

    unwrapped = getattr(
        derive_cornerstone_Aq, '__wrapped__', derive_cornerstone_Aq,
    )
    source = inspect.getsource(unwrapped)
    assert 'expand_square_matrix' not in source
    assert '_derive_cornerstone_Aq_from_disaggregated' in source


def test_epa_data_year_no_2022() -> None:
    from bedrock.extract.allocation import epa

    source = inspect.getsource(epa._get_epa_data_year)
    assert '2022' not in source
