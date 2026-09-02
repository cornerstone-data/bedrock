from __future__ import annotations

import os
import typing as ta

import pandas as pd
import yaml
from pydantic import BaseModel, Field, model_validator

CONFIG_DIR = os.path.join(os.path.dirname(__file__), 'configs')
USA_CONFIG_ENV_VAR = 'USA_CONFIG_FILE'
CANONICAL_USA_CONFIG = '2025_usa_cornerstone_v0_3'

# Stems with no yaml under configs/. Historical EF sheets / combine keys may
# still use these strings (e.g. CEDA_V0_BASELINE); load/run is not supported.
# Compare against frozen GCS snapshot key ``v0`` via snapshot_version_or_git_sha.
RETIRED_USA_CONFIG_STEMS: frozenset[str] = frozenset(
    {
        'v8_ceda_2025_usa',
        '2025_usa_ceda_ghg_from_flowsa',
    }
)

BEA_PUBLISHED_DETAIL_IO_YEARS: frozenset[int] = frozenset({2012, 2017})
NOWCAST_IO_YEARS: frozenset[int] = frozenset(
    {2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024}
)
NowcastDetailIoYear = ta.Literal[2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

DIAGNOSTICS_CLI_OVERRIDE_KEYS: frozenset[str] = frozenset(
    {
        'diagnostics_baseline_source',
        'snapshot_version_or_git_sha',
        'useeio_baseline_xlsx_gs_uri',
        'useeio_baseline_xlsx_sha256',
        'useeio_model_version_label',
        'model_base_year',
        'usa_ghg_data_year',
    }
)


class USAConfig(BaseModel):
    #####
    # Model base settings
    #####
    model_base_year: ta.Literal[2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024] = 2023
    bea_io_level: ta.Literal['detail', 'summary'] = 'detail'
    bea_io_scheme: ta.Literal[2017, 2022] = 2017  # documentation purposes
    price_type: ta.Literal['producer', 'purchaser'] = 'producer'
    iot_before_or_after_redefinition: ta.Literal['before', 'after'] = 'after'

    #####
    # Data selection
    #####
    usa_detail_io_source: ta.Literal['bea_published', 'nowcast'] = 'bea_published'
    nowcast_mut_vintage: ta.Optional[str] = Field(
        default=None,
        description=(
            'Artifact build label for nowcast BEA-detail MUT tables on GCS '
            '(e.g. v0.3.0_16f96b1). When omitted and usa_detail_io_source is '
            'nowcast, loaders pick the most recently uploaded Make parquet for '
            'the configured year and redefinition stage.'
        ),
    )
    usa_base_io_data_year: ta.Literal[
        2012, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024
    ] = 2017  # BEA benchmark year (bea_published) or IO calendar year (nowcast)
    usa_io_data_year: ta.Literal[2017, 2022, 2023, 2024] = (
        2022  # CEDA's legacy USA IO data year
    )
    usa_ghg_data_year: ta.Literal[2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024] = 2023

    ipcc_ar_version: ta.Literal['AR5', 'AR6'] = 'AR6'

    #####
    # Methodology selection
    #####
    ### IO Methodology selection
    # "IO year adjustments" bucket: CEDA A/q scaling to usa_io_data_year with
    # summary dollar-year rebase, bedrock-derived industry inflation factors,
    # and gross output at usa_ghg_data_year as B's denominator x.
    apply_io_year_adjustments: bool = False
    use_E_data_year_for_x_in_B: bool = Field(
        default=False,
        description=(
            'Deprecated USEEIO-parity compatibility flag: GHG-year x in B '
            'without the rest of the IO-year adjustments. Superseded by '
            'apply_io_year_adjustments; kept for configs on the pre-v0.3 A '
            'footing until the USEEIO-recreation removal.'
        ),
    )
    deflate_x_to_detail_io_year_for_B: bool = Field(
        default=False,
        description=(
            'Deflate BEA gross-output industry x at usa_ghg_data_year to '
            'usa_detail_original_year chain dollars before E/x in B '
            '(derive_cornerstone_B_via_vnorm). Requires '
            'use_E_data_year_for_x_in_B to be true.'
        ),
    )
    implement_waste_disaggregation: bool = False  # DRI: jorge.vendries
    implement_electricity_reallocation: bool = False  # DRI: jorge.vendries
    implement_electricity_disaggregation: bool = False  # DRI: jorge.vendries
    implement_electricity_mixed_units: bool = False  # DRI: jorge.vendries
    scale_a_matrix_with_useeio_method: bool = False  # DRI: mo.li
    # USEEIO-parity margins (useeior Rho/CPI path); anchors the USEEIO-baseline
    # release-waterfall chain (v03_waterfall_useeio_g1_schema_ghg).
    useeio_margins: bool = False  # DRI: WesIngwersen
    cornerstone_industry_avg_margins: bool = False  # DRI: WesIngwersen
    ### GHG Methodology selection
    # "GHG model allocation" bucket: Cornerstone GHG FBS (pre-built parquet at
    # usa_ghg_data_year) vs the legacy CEDA-methodology FBS (2023 only).
    use_cornerstone_ghg_model: bool = False

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

    @model_validator(mode='after')
    def _validate_deflate_x_requires_use_e_for_x_in_b(self) -> USAConfig:
        if self.deflate_x_to_detail_io_year_for_B and not self.use_ghg_year_x_in_B:
            raise ValueError(
                'deflate_x_to_detail_io_year_for_B requires use_E_data_year_for_x_in_B '
                'or apply_io_year_adjustments to be true'
            )
        return self

    @model_validator(mode='after')
    def _validate_margins_mutual_exclusivity(self) -> USAConfig:
        if self.useeio_margins and self.cornerstone_industry_avg_margins:
            raise ValueError(
                'At most one margins flag may be true; got: '
                'useeio_margins, cornerstone_industry_avg_margins'
            )
        return self

    @model_validator(mode='after')
    def _validate_ghg_flag_compatibility(self) -> USAConfig:
        if (
            self.implement_electricity_reallocation
            and not self.implement_waste_disaggregation
        ):
            raise ValueError(
                'implement_electricity_reallocation requires '
                'implement_waste_disaggregation'
            )
        if self.implement_electricity_disaggregation and not (
            self.implement_waste_disaggregation
            and self.implement_electricity_reallocation
        ):
            raise ValueError(
                'implement_electricity_disaggregation requires '
                'implement_waste_disaggregation and implement_electricity_reallocation'
            )
        if self.implement_electricity_mixed_units and not (
            self.implement_electricity_disaggregation
        ):
            raise ValueError(
                'implement_electricity_mixed_units requires '
                'implement_electricity_disaggregation'
            )
        return self

    @model_validator(mode='after')
    def _validate_detail_io_source(self) -> USAConfig:
        if self.usa_detail_io_source == 'bea_published':
            if self.usa_base_io_data_year not in BEA_PUBLISHED_DETAIL_IO_YEARS:
                raise ValueError(
                    'usa_base_io_data_year must be 2012 or 2017 when '
                    "usa_detail_io_source is 'bea_published'; "
                    f'got {self.usa_base_io_data_year}'
                )
        elif self.usa_detail_io_source == 'nowcast':
            if self.usa_base_io_data_year not in NOWCAST_IO_YEARS:
                raise ValueError(
                    'usa_base_io_data_year must be 2017–2024 when '
                    "usa_detail_io_source is 'nowcast'; "
                    f'got {self.usa_base_io_data_year}'
                )
            if self.usa_base_io_data_year != self.model_base_year:
                raise ValueError(
                    'usa_base_io_data_year must equal model_base_year when '
                    "usa_detail_io_source is 'nowcast'; "
                    f'got usa_base_io_data_year={self.usa_base_io_data_year}, '
                    f'model_base_year={self.model_base_year}'
                )
            if self.apply_io_year_adjustments:
                raise ValueError(
                    'apply_io_year_adjustments is incompatible with '
                    "usa_detail_io_source 'nowcast'"
                )
        return self

    #####
    # Baseline snapshot
    #####
    # The git SHA below is the baseline snapshots used for diagnostic comparison
    # generated on main with configuration: 2025_usa_cornerstone_v0_3.
    snapshot_version_or_git_sha: ta.Literal[
        'v0',
        '1bda811e0169436ae90fd356fbef512ce7518ccb',  # v0.1
        '2ebb51f7190c3a62b5d8b2420bff9b20f57282fc',  # test
        '9fe22d9afdfdb6806397b2356eb3cf4c4c346744',  # test: snapshot from 2025_usa_cornerstone_fbs_schema
        '7372464249c434c9bebb172c065a4d0e3702176e',  # v0.2
        '4d67c8f0f5721a30ce03f4d3eef85a82e7199032',  # v0.3.0-alpha (config: 2025_usa_cornerstone_v0_2)
        '5a90baf0272fe8841e40db8cd513885b34051e86',  # v0.3-beta (config: 2025_usa_cornerstone_v0_3)
        '9a47eaa1060e6900154c7b819934a8a1669461c3',  # v0.3.0 before #513 (industry-x expand fix)
        'c60bdf4308cb660eee80a246214901cff9122820',  # v0.3.0
        '00524c3c8ba122a7a5b7f2139ff7ea6de08947bb',  # v0.3.1 (current .SNAPSHOT_KEY)
    ] = 'v0'

    @property
    def usa_detail_original_year(self) -> NowcastDetailIoYear:
        if self.usa_detail_io_source == 'nowcast':
            return ta.cast(NowcastDetailIoYear, self.usa_base_io_data_year)
        return 2017

    @property
    def use_ghg_year_x_in_B(self) -> bool:
        """B's denominator x is gross output at ``usa_ghg_data_year``."""
        return self.apply_io_year_adjustments or self.use_E_data_year_for_x_in_B

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable dictionary representation of the config.

        Nested BaseModel values are converted to plain dictionaries via
        model_dump(), so callers can safely pass this mapping to pandas or
        json libraries.
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


def _normalize_usa_config_file_name(config_file_name: str) -> str:
    if not config_file_name.endswith('.yaml'):
        return f'{config_file_name}.yaml'
    return config_file_name


def _raise_if_retired_usa_config(config_file_name: str) -> None:
    stem = _normalize_usa_config_file_name(config_file_name).removesuffix('.yaml')
    if stem in RETIRED_USA_CONFIG_STEMS:
        raise ValueError(
            f'USA config {stem!r} is retired and cannot be loaded. '
            "For CEDA v0 EF comparisons, pin snapshot_version_or_git_sha to 'v0' "
            'on a live Cornerstone config; do not regenerate the legacy model.'
        )


def _load_usa_config_from_file_name(config_file_name: str) -> USAConfig:
    assert config_file_name.endswith('.yaml'), 'config file name must end with .yaml'
    _raise_if_retired_usa_config(config_file_name)
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
            ``DIAGNOSTICS_CLI_OVERRIDE_KEYS`` (diagnostics baseline source,
            snapshot key, USEEIO pin fields, model years). Used by
            ``generate_diagnostics`` so one run can change the comparison
            target and years without a forked config file.
    """
    global _usa_config
    config_file_env = os.environ.get(USA_CONFIG_ENV_VAR)

    if (_usa_config is not None) or (config_file_env is not None):
        raise ValueError('Global USA config already set')

    config_file = _normalize_usa_config_file_name(config_file)
    _raise_if_retired_usa_config(config_file)

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
            set_global_usa_config(f'{CANONICAL_USA_CONFIG}.yaml')
    assert _usa_config is not None
    return _usa_config


def reset_usa_config(should_reset_env_var: bool = True) -> None:
    """Clear the process-wide USA config."""
    global _usa_config
    _usa_config = None
    if should_reset_env_var:
        os.environ.pop(USA_CONFIG_ENV_VAR, None)
