"""
CEDA USA - Emissions allocation and input-output analysis for the United States.

This package provides functionality for processing USA economic and emissions data
as part of the CEDA (Carbon Emissions Database for Applications) EEIO database.
"""

__version__ = "0.1.0"

# Core modules that can be imported directly
import bedrock.ceda_usa.utils as utils
from bedrock.ceda_usa.transform.eeio import (
    derived,
    derived_2017,
    derived_2017_helpers,
    scale_abq_via_summary,
)
from bedrock.ceda_usa.utils import constants
from bedrock.extract.iot import io_2012, io_2017

__all__ = [
    "constants",
    "derived",
    "io_2012",
    "io_2017",
    "derived_2017",
    "derived_2017_helpers",
    "scale_abq_via_summary",
    "bea_v2017_to_ceda_v7_helpers",
    "utils",
]
