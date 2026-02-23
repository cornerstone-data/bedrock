"""EEIO waste disaggregation configuration.

Waste disaggregation extends the cornerstone schema (405 sectors) by splitting
sector 562000 (Waste management and remediation services) into 7 sub-sectors
using weight data from Make and Use table CSV files.

Config contract:
- When implement_waste_disaggregation=True, use_cornerstone_2026_model_schema
  must be True. Disaggregation targets the cornerstone schema only.
- Weight files: WasteDisaggregationDetail2017_Make.csv and
  WasteDisaggregationDetail2017_Use.csv
- Disaggregation logic MUST consult get_usa_config().implement_waste_disaggregation
  and CANNOT proceed without this flag being True. The user must load a config
  file (e.g. 2025_usa_cornerstone_waste_disaggregation.yaml) that sets this
  tag. The disaggregation code path must gate execution on this check.

Reference: .cursor/Disagg_project/implementation-steps_v2.md
"""

from pathlib import Path

from bedrock.utils.config.settings import disaggregationpath

WEIGHT_SOURCE = "WasteDisaggregationDetail2017"
MAKE_WEIGHT_FILENAME = "WasteDisaggregationDetail2017_Make.csv"
USE_WEIGHT_FILENAME = "WasteDisaggregationDetail2017_Use.csv"


def get_waste_disaggregation_data_path() -> Path:
    """Return path to the disaggregation data folder.

    Contains Make and Use weight CSV files for waste sector disaggregation.

    Returns:
        Path to bedrock/extract/disaggregation/
    """
    return disaggregationpath


def get_waste_disaggregation_weight_source() -> str:
    """Return identifier for the waste disaggregation weight data source.

    Returns:
        "WasteDisaggregationDetail2017" — used for provenance/metadata.
    """
    return WEIGHT_SOURCE


def get_waste_disaggregation_make_weight_path() -> Path:
    """Return path to the Make table weight CSV file.

    Returns:
        Path to WasteDisaggregationDetail2017_Make.csv
    """
    return get_waste_disaggregation_data_path() / MAKE_WEIGHT_FILENAME


def get_waste_disaggregation_use_weight_path() -> Path:
    """Return path to the Use table weight CSV file.

    Returns:
        Path to WasteDisaggregationDetail2017_Use.csv
    """
    return get_waste_disaggregation_data_path() / USE_WEIGHT_FILENAME
