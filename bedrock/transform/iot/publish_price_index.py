"""Publish the derived BEA industry price index as a versioned extract artifact.

:func:`~bedrock.transform.iot.derived_price_index.derive_industry_price_index`
is BEA data with light processing, not model output, so it is written beside
the other processed sources under ``extract/output-data`` rather than into a
model snapshot. CEDA loads the same object, so both repos read one copy.

The file name carries the BEA release it was built from::

    BEA_PriceIndex_<vintage>_v<pkg_version>_<githash>.parquet
    BEA_PriceIndex_<vintage>_v<pkg_version>_<githash>_metadata.json

Long format, one row per sector x year:

===============  ======================================================
``sector_code``  BEA 2017 detail industry code, plus the waste splits
``year``         int
``price_index``  chain-type price index for gross output, 2017 = 100
===============  ======================================================

⚠️ A published vintage is never overwritten. Consumers pin the full stem
(vintage, version and hash), so replacing the object behind a stem would move
their numbers silently; publish a new stem instead.

Usage::

    uv run python -m bedrock.transform.iot.publish_price_index --vintage 2026Q2
    uv run python -m bedrock.transform.iot.publish_price_index --vintage 2026Q2 --upload
"""

from __future__ import annotations

import argparse
import json
import posixpath
import typing as ta
from datetime import datetime
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.gdp import BeaDataVersion
from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.utils.config.settings import (
    FBA_DIR,
    GIT_BRANCH,
    GIT_HASH,
    GIT_HASH_LONG,
    PKG_VERSION_NUMBER,
)

ARTIFACT_NAME = 'BEA_PriceIndex'
#: GCS prefix under ``GCS_CORNERSTONE``, beside the FBA parquets.
GCS_EXTRACT_OUTPUT_DIR = 'extract/output-data'


def price_index_long(vintage: BeaDataVersion) -> pd.DataFrame:
    """The derived index for ``vintage`` as sector x year rows."""
    wide = derive_industry_price_index(vintage).astype(float)
    wide.index.name = 'sector_code'
    long = wide.reset_index().melt(
        id_vars='sector_code', var_name='year', value_name='price_index'
    )
    return long.astype({'sector_code': str, 'year': int, 'price_index': float})


def artifact_stem(vintage: BeaDataVersion) -> str:
    stem = f'{ARTIFACT_NAME}_{vintage}_v{PKG_VERSION_NUMBER}'
    return f'{stem}_{GIT_HASH}' if GIT_HASH is not None else stem


def save_price_index(
    vintage: BeaDataVersion,
    out_dir: Path | None = None,
    *,
    upload: bool = False,
) -> list[Path]:
    """Write the parquet and its metadata sidecar; optionally upload both."""
    directory = Path(out_dir) if out_dir is not None else FBA_DIR
    directory.mkdir(parents=True, exist_ok=True)
    stem = artifact_stem(vintage)

    df = price_index_long(vintage)
    parquet_path = directory / f'{stem}.parquet'
    df.to_parquet(parquet_path, index=False)
    meta = {
        'tool': 'bedrock',
        'category': 'FlowByActivity',
        'name_data': f'{ARTIFACT_NAME}_{vintage}',
        'tool_version': PKG_VERSION_NUMBER,
        'git_hash': GIT_HASH,
        'ext': 'parquet',
        'date_created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'tool_meta': {
            'bea_data_vintage': vintage,
            'source': (
                'BEA GDP-by-industry GrossOutput.xlsx: UGO304-A detail, '
                'UGO305-A weights for duplicated codes, TGO104-Q quarterly '
                'summary averaged for the final year'
            ),
            'units': 'chain-type price index, 2017 = 100',
            'years': [int(df['year'].min()), int(df['year'].max())],
            'n_sectors': int(df['sector_code'].nunique()),
            'branch': GIT_BRANCH,
            'commit': GIT_HASH_LONG,
            'builder': 'bedrock.transform.iot.publish_price_index',
        },
    }
    meta_path = directory / f'{stem}_metadata.json'
    meta_path.write_text(json.dumps(meta, indent=4))
    written = [parquet_path, meta_path]

    if upload:
        # Deferred import: building locally must not need GCS credentials.
        from bedrock.utils.io.gcp import (  # noqa: PLC0415
            GCS_CORNERSTONE,
            gcs_path_exists,
            upload_file_to_gcs,
        )

        urls = {
            path: posixpath.join(GCS_CORNERSTONE, GCS_EXTRACT_OUTPUT_DIR, path.name)
            for path in written
        }
        existing = [url for url in urls.values() if gcs_path_exists(url)]
        if existing:
            raise FileExistsError(
                f'{existing} already published; a stem is never overwritten. '
                'Commit the change and publish under the new git hash.'
            )
        for path, url in urls.items():
            upload_file_to_gcs(str(path), url)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Publish the derived BEA industry price index.'
    )
    parser.add_argument(
        '--vintage', required=True, choices=list(ta.get_args(BeaDataVersion))
    )
    parser.add_argument('--out-dir', type=Path, default=None)
    parser.add_argument('--upload', action='store_true')
    args = parser.parse_args(argv)
    for path in save_price_index(args.vintage, args.out_dir, upload=args.upload):
        print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
