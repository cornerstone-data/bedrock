"""Publish the industry price index as a versioned extract artifact.

:func:`~bedrock.transform.iot.derived_price_index.derive_industry_price_index`
is BEA data with light processing, not model output, so it is written beside
the other processed sources under ``extract/output-data`` rather than into a
model snapshot.

Two flavors share the same BEA release vintage:

* ``bea`` -- pure derived BEA index (``BEA_PriceIndex_*``).
* ``cornerstone`` (default) -- the same derive plus model overlays; the first
  overlay puts ``221100``, ``S00101``, and ``S00202`` on EIA average retail
  price. CEDA and retail-output US configs pin this family.

The file name carries the flavor and the BEA release it was built from::

    BEA_PriceIndex_<vintage>_v<pkg_version>_<githash>.parquet
    Cornerstone_PriceIndex_<vintage>_v<pkg_version>_<githash>.parquet

Long format, one row per sector x year:

===============  ======================================================
``sector_code``  BEA 2017 detail industry code, plus the waste splits
``year``         int
``price_index``  chain-type price index for gross output, 2017 = 100
===============  ======================================================

⚠️ A published stem is never overwritten. Consumers pin the full stem
(vintage, version and hash), so replacing the object behind a stem would move
their numbers silently; publish a new stem instead.

Usage::

    uv run python -m bedrock.transform.iot.publish_price_index --vintage 2026Q2
    uv run python -m bedrock.transform.iot.publish_price_index --vintage 2026Q2 --upload
    uv run python -m bedrock.transform.iot.publish_price_index --vintage 2026Q2 --flavor bea
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

PriceIndexFlavor = ta.Literal['bea', 'cornerstone']

BEA_ARTIFACT_NAME = 'BEA_PriceIndex'
CORNERSTONE_ARTIFACT_NAME = 'Cornerstone_PriceIndex'
#: Back-compat alias for pure-BEA stem validation in loaders.
ARTIFACT_NAME = BEA_ARTIFACT_NAME
#: GCS prefix under ``GCS_CORNERSTONE``, beside the FBA parquets.
GCS_EXTRACT_OUTPUT_DIR = 'extract/output-data'

_ARTIFACT_BY_FLAVOR: dict[PriceIndexFlavor, str] = {
    'bea': BEA_ARTIFACT_NAME,
    'cornerstone': CORNERSTONE_ARTIFACT_NAME,
}


def artifact_name(flavor: PriceIndexFlavor = 'cornerstone') -> str:
    return _ARTIFACT_BY_FLAVOR[flavor]


def _wide_price_index(
    vintage: BeaDataVersion, flavor: PriceIndexFlavor
) -> pd.DataFrame:
    """Derived BEA wide index, with Cornerstone overlays when ``flavor`` asks."""
    wide = derive_industry_price_index(vintage).astype(float)
    if flavor == 'cornerstone':
        wide = _apply_cornerstone_overlays(wide)
    wide.index.name = 'sector_code'
    return wide


def _apply_cornerstone_overlays(levels: pd.DataFrame) -> pd.DataFrame:
    """Model overlays on the derived BEA index for the Cornerstone artifact.

    Ordered list so future Cornerstone-only adjustments append here. The first
    overlay matches ``_electric_lines_on_eia_retail_price`` (EIA retail on the
    three electric industries).
    """
    from bedrock.utils.economic.inflation_helpers_usa import (  # noqa: PLC0415
        _electric_lines_on_eia_retail_price,
    )

    return _electric_lines_on_eia_retail_price(levels)


def _cornerstone_overlay_meta() -> dict[str, object]:
    from bedrock.transform.iot.eia_utility_go_adjustment import (  # noqa: PLC0415
        CONTROLLED_YEARS,
        ELECTRIC_INDUSTRIES,
    )

    return {
        'electric_price_override': 'EIA average retail (retail_price_index)',
        'electric_industries': list(ELECTRIC_INDUSTRIES),
        'electric_controlled_years': [int(y) for y in CONTROLLED_YEARS],
        'electric_after_span': (
            "BEA year-to-year change on the EIA-based level at the span's last year"
        ),
    }


def price_index_long(
    vintage: BeaDataVersion, flavor: PriceIndexFlavor = 'cornerstone'
) -> pd.DataFrame:
    """The index for ``vintage`` / ``flavor`` as sector x year rows."""
    wide = _wide_price_index(vintage, flavor)
    long = wide.reset_index().melt(
        id_vars='sector_code', var_name='year', value_name='price_index'
    )
    return long.astype({'sector_code': str, 'year': int, 'price_index': float})


def artifact_stem(
    vintage: BeaDataVersion, flavor: PriceIndexFlavor = 'cornerstone'
) -> str:
    stem = f'{artifact_name(flavor)}_{vintage}_v{PKG_VERSION_NUMBER}'
    return f'{stem}_{GIT_HASH}' if GIT_HASH is not None else stem


def save_price_index(
    vintage: BeaDataVersion,
    out_dir: Path | None = None,
    *,
    upload: bool = False,
    flavor: PriceIndexFlavor = 'cornerstone',
) -> list[Path]:
    """Write the parquet and its metadata sidecar; optionally upload both."""
    directory = Path(out_dir) if out_dir is not None else FBA_DIR
    directory.mkdir(parents=True, exist_ok=True)
    name = artifact_name(flavor)
    stem = artifact_stem(vintage, flavor)

    df = price_index_long(vintage, flavor)
    parquet_path = directory / f'{stem}.parquet'
    df.to_parquet(parquet_path, index=False)
    tool_meta: dict[str, object] = {
        'bea_data_vintage': vintage,
        'flavor': flavor,
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
    }
    if flavor == 'cornerstone':
        tool_meta.update(_cornerstone_overlay_meta())
    meta = {
        'tool': 'bedrock',
        # Not a FlowBy: the FlowBy loaders route any non-FBA category to FBS.
        'category': 'PriceIndex',
        'name_data': f'{name}_{vintage}',
        'tool_version': PKG_VERSION_NUMBER,
        'git_hash': GIT_HASH,
        'ext': 'parquet',
        'date_created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'tool_meta': tool_meta,
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
        description=(
            'Publish the industry price index (Cornerstone with overlays by '
            'default; pure BEA via --flavor bea).'
        )
    )
    parser.add_argument(
        '--vintage', required=True, choices=list(ta.get_args(BeaDataVersion))
    )
    parser.add_argument(
        '--flavor',
        choices=list(ta.get_args(PriceIndexFlavor)),
        default='cornerstone',
    )
    parser.add_argument('--out-dir', type=Path, default=None)
    parser.add_argument('--upload', action='store_true')
    args = parser.parse_args(argv)
    for path in save_price_index(
        args.vintage, args.out_dir, upload=args.upload, flavor=args.flavor
    ):
        print(path)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
