"""
CEDA USA - Emissions allocation and input-output analysis for the United States.

This package provides functionality for processing USA economic and emissions data
as part of the CEDA (Carbon Emissions Database for Applications) EEIO database.
"""

__version__ = "0.1.0"

# Core modules that can be imported directly
import bedrock.utils as utils
from bedrock.extract.iot import io_2012, io_2017
from bedrock.transform.eeio import (
    derived,
    derived_2017,
    derived_2017_helpers,
    scale_abq_via_summary,
)

__all__ = [
    "derived",
    "io_2012",
    "io_2017",
    "derived_2017",
    "derived_2017_helpers",
    "scale_abq_via_summary",
    "utils",
]
