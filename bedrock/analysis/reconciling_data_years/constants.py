"""Shared constants for the reconciling data years analysis package.

Centralizes output paths, years for analysis, sectors of interest
"""

from __future__ import annotations

from pathlib import Path

from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC
from bedrock.utils.validation.significant_sectors import SIGNIFICANT_SECTORS

OUTPUT_DIR = Path(__file__).parent / "output"
RESULTS_DIR = OUTPUT_DIR / "results"
PLOTS_DIR = OUTPUT_DIR / "plots"


## Years for analysis

# BEA detail IO base year — the inflation `original_year` for every script.
ORIGINAL_YEAR: int = get_usa_config().usa_base_io_data_year

# Latest year
LATEST_TARGET_YEAR: int = 2024


## Select commodities to highlight in analysis. Start with SIGNIFICANT_SECTORS
## Need to select ~8 and get short names for them
sectors = [sector['sector'] for sector in SIGNIFICANT_SECTORS]
sectors.remove('562000')
sector_names = {sector: COMMODITY_DESC[sector] for sector in sectors}
