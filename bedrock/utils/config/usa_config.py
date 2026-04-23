from __future__ import annotations

import os
import typing as ta

import pandas as pd
import yaml
from pydantic import BaseModel, Field, model_validator

CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'configs')
USA_CONFIG_ENV_VAR = 'USA_CONFIG_FILE'

DIAGNOSTICS_CLI_OVERRIDE_KEYS: frozenset[str] = frozenset(
    {
        'diagnostics_baseline_source',
        'useeio_baseline_xlsx_gs_uri',
        'useeio_baseline_xlsx_sha256',
        'useeio_model_version_label',
    }
)


class EEIOWasteDisaggConfig(BaseModel):
    use_weights_file: str
    make_weights_file: str
    year: int
    source_name: str


class USAConfig(BaseModel):
    #####
    # Model base settings
    #####
    model_base_year: ta.Literal[2022, 2023, 2024] = 2023
    bea_io_level: ta.Literal['detail', 'summary'] = 'detail'
    bea_io_scheme: ta.Literal[2017, 2022] = 2017  # documentation purposes
    price_type: ta.Literal['producer', 'purchaser'] = 'producer'
    iot_before_or_after_redefinition: ta.Literal['before', 'after'] = 'after'

    #####
    # Data selection
    #####
    usa_base_io_data_year: ta.Literal[2012, 2017] = (
        2017  # BEA's benchmark year for Detail Input-Output data
    )
    usa_io_data_year: ta.Literal[2022, 2023, 2024] = (
        2022  # CEDA's legacy USA IO data year
    )
    usa_ghg_data_year: ta.Literal[2023, 2024] = 2023

    ipcc_ar_version: ta.Literal['AR5', 'AR6'] = 'AR6'

    #####
    # Methodology selection
    #####
    ### Schema/Taxonomy selection
    use_cornerstone_2026_model_schema: bool = False  # DRI: mo.li
    ### IO Methodology selection
    use_E_data_year_for_x_in_B: bool = False  # DRI: mo.li
    implement_waste_disaggregation: bool = False  # DRI: jorge.vendries
    eeio_waste_disaggregation: ta.Optional[EEIOWasteDisaggConfig] = None
    scale_a_matrix_with_useeio_method: bool = False  # DRI: mo.li
    scale_a_matrix_with_summary_tables: bool = False  # DRI: mo.li
    scale_a_matrix_with_price_index: bool = False  # DRI: mo.li
    ### GHG Methodology selection
    load_E_from_flowsa: bool = False  # if True, use load_E_from_flowsa()
    usa_ghg_methodology: ta.Literal['national', 'state'] = 'national'
    update_transportation_ghg_method: bool = False  # DRI: ben.young
    update_ghg_coa_allocation: bool = False  # DRI: catherine.birney
    update_electricity_ghg_method: bool = False  # DRI: catherine.birney
    update_ghg_attribution_method_for_ng_and_petrol_systems: bool = (
        False  # DRI: catherine.birney
    )
    new_ghg_method: bool = False  # if True, it is the new Cornerstone GHG FBS
    update_flowsa_refrigerant_method: bool = False  # DRI: catherine.birney
    add_new_ghg_activities: bool = False  # DRI: catherine.birney
    update_enteric_fermentation_and_manure_management_ghg_method: bool = (
        False  # DRI: mo.li
    )
    update_liming_and_fertilizer_ghg_method: bool = False  # DRI: mo.li
    update_other_gases_ghg_method: bool = False  # DRI: catherine.birney

    #####
    # Diagnostics baseline (parquet snapshots vs USEEIO Excel on GCS)
    #####
    diagnostics_baseline_source: ta.Literal['gcs_snapshot', 'gcs_useeio_xlsx'] = (
        'gcs_snapshot'
    )
    useeio_baseline_xlsx_gs_uri: ta.Optional[str] = Field(
        default=None,
        description=(
            'gs://cornerstone-default/... URI for the USEEIO baseline workbook. '
            'Typically supplied via useeio_baseline_pin.json with '
            'generate_diagnostics --useeio_baseline_pin_json, or set in YAML.'
        ),
    )
    useeio_baseline_xlsx_sha256: ta.Optional[str] = Field(
        default=None,
        description=(
            'SHA-256 (64 hex chars) of the exact xlsx bytes at useeio_baseline_xlsx_gs_uri. '
            'In CI, use bedrock/utils/snapshots/useeio_baseline_pin.json with '
            'generate_diagnostics --useeio_baseline_pin_json. Required in GitHub Actions '
            "when diagnostics_baseline_source is 'gcs_useeio_xlsx'."
        ),
    )
    useeio_model_version_label: ta.Optional[str] = Field(
        default=None,
        description=(
            'Short label for config_summary / auditing. Typically set in useeio_baseline_pin.json.'
        ),
    )

    @model_validator(mode='after')
    def _validate_diagnostics_baseline(self) -> USAConfig:
        """USEEIO baseline needs a GCS URI; CI must pin the xlsx with SHA256."""
        if self.diagnostics_baseline_source == 'gcs_useeio_xlsx':
            if not self.useeio_baseline_xlsx_gs_uri:
                raise ValueError(
                    'useeio_baseline_xlsx_gs_uri is required when '
                    "diagnostics_baseline_source is 'gcs_useeio_xlsx'"
                )
            if os.environ.get('GITHUB_ACTIONS') == 'true':
                if not self.useeio_baseline_xlsx_sha256:
                    raise ValueError(
                        'useeio_baseline_xlsx_sha256 is required in GitHub Actions '
                        "when diagnostics_baseline_source is 'gcs_useeio_xlsx'"
                    )
        return self

    #####
    # Baseline snapshot
    #####
    # The git SHA below is the baseline snapshots used for diagnostic comparison
    # generated on main with configuration: 2025_usa_cornerstone_full_model.
    snapshot_version_or_git_sha: ta.Literal[
        'v0',
        '1bda811e0169436ae90fd356fbef512ce7518ccb',  # v0.1
        '2ebb51f7190c3a62b5d8b2420bff9b20f57282fc',  # v0.2
        '9fe22d9afdfdb6806397b2356eb3cf4c4c346744',  # v0.2 2025_usa_cornerstone_fbs_schema
    ] = 'v0'

    @property
    def usa_detail_original_year(self) -> ta.Literal[2012, 2017]:
        return 2017

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable dictionary representation of the config.

        Nested BaseModel values (such as EEIOWasteDisaggConfig) are converted
        to plain dictionaries via model_dump(), so callers can safely pass
        this mapping to pandas or json libraries.
        """
        result: dict[str, object] = {}
        for field_name in self.model_fields:
            value = getattr(self, field_name)
            if isinstance(value, BaseModel):
                result[field_name] = value.model_dump()
            else:
                result[field_name] = value
        return result

    def to_dataframe(self, config_name: str) -> pd.DataFrame:
        config_dict = self.to_dict()
        config_dict_df = pd.DataFrame(
            [
                {'config_field': key, 'value': value}
                for key, value in config_dict.items()
            ]
        )
        summaries = pd.concat(
            [
                pd.DataFrame(
                    {'config_field': 'config_name', 'value': config_name}, index=[0]
                ),
                config_dict_df,
            ],
        )
        return summaries


_usa_config: ta.Optional[USAConfig] = None


def _load_usa_config_from_file_name(config_file_name: str) -> USAConfig:
    assert config_file_name.endswith('.yaml'), 'config file name must end with .yaml'
    with open(os.path.join(CONFIG_DIR, config_file_name)) as f:
        data = yaml.safe_load(f)
    config = USAConfig.model_validate(data, strict=True)
    return config


def set_global_usa_config(
    config_file: str,
    *,
    diagnostics_cli_overrides: dict[str, object] | None = None,
) -> None:
    """Set the process-wide USA config from YAML.

    Args:
        config_file: Config stem or filename under ``configs/`` (``.yaml`` is
            appended if missing).
        diagnostics_cli_overrides: If set, merged onto the YAML-loaded dict
            before ``USAConfig`` validation. Keys must be a subset of
            ``DIAGNOSTICS_CLI_OVERRIDE_KEYS`` (diagnostics baseline + USEEIO pin
            fields). Used by ``generate_diagnostics`` so one run can change those
            fields without a forked config file.
    """
    global _usa_config
    config_file_env = os.environ.get(USA_CONFIG_ENV_VAR)

    if (_usa_config is not None) or (config_file_env is not None):
        raise ValueError('Global USA config already set')

    if not config_file.endswith('.yaml'):
        config_file += '.yaml'

    base = _load_usa_config_from_file_name(config_file)
    if diagnostics_cli_overrides:
        unknown = set(diagnostics_cli_overrides) - DIAGNOSTICS_CLI_OVERRIDE_KEYS
        if unknown:
            raise ValueError(
                f'Unknown diagnostics_cli_overrides keys: {sorted(unknown)}'
            )
        filtered = {
            k: v
            for k, v in diagnostics_cli_overrides.items()
            if k in DIAGNOSTICS_CLI_OVERRIDE_KEYS and v is not None
        }
        merged = base.model_dump(mode='python')
        merged.update(filtered)
        _usa_config = USAConfig.model_validate(merged, strict=True)
    else:
        _usa_config = base
    os.environ[USA_CONFIG_ENV_VAR] = config_file


def get_usa_config() -> USAConfig:
    global _usa_config
    if _usa_config is None:
        env_usa_config_file = os.environ.get(USA_CONFIG_ENV_VAR)
        if env_usa_config_file:
            _usa_config = _load_usa_config_from_file_name(env_usa_config_file)
        else:
            set_global_usa_config('2025_usa_cornerstone_full_model.yaml')
    assert _usa_config is not None
    return _usa_config


def reset_usa_config(should_reset_env_var: bool = False) -> None:
    """For testing purposes"""
    global _usa_config
    _usa_config = None
    if should_reset_env_var and USA_CONFIG_ENV_VAR in os.environ:
        del os.environ[USA_CONFIG_ENV_VAR]
