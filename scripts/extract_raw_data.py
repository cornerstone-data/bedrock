"""
For the listed datasets, acquire the raw data and store locally
"""

import os

from bedrock.extract.generateflowbyactivity import load_fba_config, process_fba_config
from bedrock.utils.config.settings import extractpath

FBA_RAW_DATA = (
    ('EIA_MECS_Energy', '2018'),
    ('USDA_CoA_Cropland', '2022'),
    ('USDA_CoA_Cropland_NAICS', '2022'),
)

IN_DIR = os.path.join(extractpath, "input_data")
os.makedirs(IN_DIR, exist_ok=True)


def extract_raw_data() -> None:
    for source, year in FBA_RAW_DATA:
        source, year, config = load_fba_config(source, year)
        config['extract_data_from_raw_sources'] = True
        process_fba_config(source, year, config, call_only=True)


if __name__ == "__main__":
    extract_raw_data()
