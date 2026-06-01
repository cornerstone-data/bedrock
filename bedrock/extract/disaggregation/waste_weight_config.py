"""Resolve waste disaggregation weight file paths from USA config."""

from __future__ import annotations

from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig, USAConfig

# Repo-relative to the bedrock package root (for EEIOWasteDisaggConfig and YAML).
WASTE_INPUTS_REL = "extract/disaggregation/waste_disagg_inputs"

WASTE_DISAGG_USE_FILENAME = "WasteDisaggregationDetail2017_Use.csv"
WASTE_DISAGG_MAKE_FILENAME = "WasteDisaggregationDetail2017_Make.csv"

CORNERSTONE_WASTE_SOURCE_NAME = "WasteDisaggregationDetail2017"
CORNERSTONE_WASTE_YEAR = 2017


def cornerstone_bundled_waste_disagg_config() -> EEIOWasteDisaggConfig:
    """Bedrock-bundled waste disagg weights (after-redefinition default)."""
    return EEIOWasteDisaggConfig(
        use_weights_file=f"{WASTE_INPUTS_REL}/{WASTE_DISAGG_USE_FILENAME}",
        make_weights_file=f"{WASTE_INPUTS_REL}/{WASTE_DISAGG_MAKE_FILENAME}",
        year=CORNERSTONE_WASTE_YEAR,
        source_name=CORNERSTONE_WASTE_SOURCE_NAME,
    )


def effective_waste_disagg_config(cfg: USAConfig) -> EEIOWasteDisaggConfig:
    """Resolve waste weight files for *cfg*.

    Precedence:
    1. before-redefinition IO → USEEIOR v1.8.0 (USEEIO parity)
    2. explicit ``cfg.eeio_waste_disaggregation`` → YAML
    3. else → bundled Cornerstone CSVs (after-redefinition default)
    """
    if cfg.iot_before_or_after_redefinition == "before":
        from bedrock.extract.disaggregation.useeior_waste_weights import (  # noqa: PLC0415
            useeior_v1_8_waste_disagg_config,
        )

        return useeior_v1_8_waste_disagg_config()
    if cfg.eeio_waste_disaggregation is not None:
        return cfg.eeio_waste_disaggregation
    return cornerstone_bundled_waste_disagg_config()
