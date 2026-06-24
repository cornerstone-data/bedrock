"""USEEIOR industry CPI levels — separate from bedrock BEA / CEDA parquet.

Loaded only when ``USAConfig.useeio_margins`` is true.

Upstream (cornerstone-data/useeior)
------------------------------------
- Object: ``data/Detail_CPI_IO_17sch.rda`` → ``MultiYearIndustryCPI`` in Detail /
  2017-schema ``loadIOData``.
- Bedrock pin: tag ``v1.8.0`` (released 2025-11-11).
- Last change to ``Detail_CPI_IO_17sch.rda`` on ``v1.8.0``: **2025-09-26**, commit
  ``4007dad`` (*update GO, CPI, and Value added from annual update #7*).

Bedrock packaging
-----------------
- GCS: ``extract/input-data/USEEIOR_v180_IndustryCPI/useeior_v1.8.0_Detail_CPI_IO_17sch.csv``
- Local cache: ``bedrock/extract/input_data/USEEIOR_v180_IndustryCPI/`` (gitignored CSV)

Cornerstone configs (``useeio_margins: false``) keep ``BEA_PriceIndex`` parquet.
"""

from __future__ import annotations

import functools
import os

import pandas as pd

from bedrock.utils.io.gcp import download_gcs_file_if_not_exists
from bedrock.utils.io.gcp_paths import gcs_extract_input_path
from bedrock.utils.io.local_extract_input_data import local_extract_input_dir

# Distinct from ``BEA_PriceIndex`` (bedrock parquet) and ``update_inflation_factors``.
USEEIOR_INDUSTRY_CPI_GCS_SOURCE = 'USEEIOR_v180_IndustryCPI'
USEEIOR_DETAIL_CPI_IO_17SCH_FILENAME = 'useeior_v1.8.0_Detail_CPI_IO_17sch.csv'
USEEIOR_DETAIL_CPI_IO_17SCH_GCS_PATH = gcs_extract_input_path(
    USEEIOR_INDUSTRY_CPI_GCS_SOURCE
)
USEEIOR_DETAIL_CPI_TAG = 'v1.8.0'
USEEIOR_DETAIL_CPI_UPSTREAM_PATH = 'data/Detail_CPI_IO_17sch.rda'
USEEIOR_DETAIL_CPI_LAST_UPSTREAM_CHANGE = '2025-09-26'  # useeior 4007dad @ v1.8.0


def _normalize_useeior_sector_index(index: pd.Index) -> pd.Index:
    return pd.Index(
        index.astype(str).str.replace('/US', '', regex=False).str.strip(),
        name=index.name,
    )


@functools.cache
def obtain_useeior_detail_industry_cpi_levels() -> pd.DataFrame:
    """Sector × year industry CPI levels from pinned USEEIOR v1.8.0 export.

    Returns a wide ``DataFrame``: index = BEA 2017 detail sector codes,
    columns = int years, values = chain-type price index (2017 = 100).
    """
    local_path = os.path.join(
        local_extract_input_dir(USEEIOR_INDUSTRY_CPI_GCS_SOURCE),
        USEEIOR_DETAIL_CPI_IO_17SCH_FILENAME,
    )
    download_gcs_file_if_not_exists(
        USEEIOR_DETAIL_CPI_IO_17SCH_FILENAME,
        USEEIOR_DETAIL_CPI_IO_17SCH_GCS_PATH,
        local_path,
    )
    if not os.path.isfile(local_path):
        raise FileNotFoundError(
            f'USEEIOR industry CPI not found at {local_path!r}. '
            f'Expected GCS object gs://cornerstone-default/'
            f'{USEEIOR_DETAIL_CPI_IO_17SCH_GCS_PATH}/'
            f'{USEEIOR_DETAIL_CPI_IO_17SCH_FILENAME} or a local copy under '
            f'bedrock/extract/input_data/{USEEIOR_INDUSTRY_CPI_GCS_SOURCE}/. '
            f'Upstream: useeior {USEEIOR_DETAIL_CPI_UPSTREAM_PATH} @ '
            f'tag {USEEIOR_DETAIL_CPI_TAG} (last changed '
            f'{USEEIOR_DETAIL_CPI_LAST_UPSTREAM_CHANGE}).'
        )

    raw = pd.read_csv(local_path, index_col=0)
    raw.index = _normalize_useeior_sector_index(raw.index)
    year_cols: dict[int, pd.Series] = {}
    for col in raw.columns:
        year_cols[int(str(col).strip())] = pd.to_numeric(raw[col], errors='coerce')
    out = pd.DataFrame(year_cols).sort_index(axis=1)
    return out.astype(float)
