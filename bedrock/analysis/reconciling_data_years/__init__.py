"""Pin analysis-specific config flags for every script in this package.

Scripts that swap the global config mid-run must re-set these flags inside their swap
helper — the toggle here only covers the initial config.
"""

from bedrock.utils.config.usa_config import get_usa_config

_cfg = get_usa_config()
_cfg.usa_io_data_year = 2017
_cfg.iot_before_or_after_redefinition = (
    "after"  # Must use after for use with cornerstone schema?
)
_cfg.use_cornerstone_2026_model_schema = True  # if not it will default to CEDAv7 schema which could cause issues with ghg_method?
_cfg.implement_waste_disaggregation = False
_cfg.ipcc_ar_version = "AR6"
_cfg.skip_scrap_adjustment_in_vnorm = True
