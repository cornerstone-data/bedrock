# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import os
import posixpath
import time
import typing as ta

import click
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import bedrock.utils.config.common as common
from bedrock.utils.config.settings import GIT_HASH_LONG, PATHS
from bedrock.utils.config.usa_config import set_global_usa_config
from bedrock.utils.io.gcp import upload_file_to_gcs
from bedrock.utils.snapshots.names import SnapshotName

if ta.TYPE_CHECKING:
    T = ta.TypeVar("T", bound=ta.Union[pd.DataFrame, pd.Series[float]])

logger = logging.getLogger(__name__)

SNAPSHOT_BASE = os.path.dirname(__file__)
GCS_SNAPSHOT_DIR = "snapshots"


def snapshot_dir() -> str:
    """Return local directory for snapshots using git hash as subdirectory."""
    sdir = os.path.join(SNAPSHOT_BASE, "data", snapshot_prefix())
    if not os.path.exists(sdir):
        os.makedirs(sdir)
    return sdir


def snapshot_prefix() -> str:
    """Return git hash for snapshot versioning."""
    if not GIT_HASH_LONG:
        raise RuntimeError(
            "GIT_HASH_LONG is not set - cannot determine snapshot prefix"
        )
    return GIT_HASH_LONG


def assert_cache_empty() -> None:
    """Assert that cache directory is empty before generating snapshots."""
    cache_dir = PATHS.local_path
    if not os.path.exists(cache_dir):
        return

    cache_contents = os.listdir(cache_dir)
    ignored_files = {
        ".gitignore",
        ".gitkeep",
        "Bibliography",
        "Log",
        "FBSComparisons",
        "Plots",
        "DisplayTables",
    }
    extra_files = set(cache_contents) - ignored_files

    if extra_files:
        newline = "\n"
        raise RuntimeError(
            f"Cache directory contains derived results. Please clear cache first.\n"
            f"Cache location: {cache_dir}\n"
            f"Found:\n{newline.join(sorted(extra_files))}"
        )


def assert_snapshots_empty() -> None:
    """Assert that snapshot directory is empty before generating new snapshots."""
    sdir = snapshot_dir()
    if os.listdir(sdir):
        raise RuntimeError(
            f"Snapshot directory is not empty. Please clean: rm -rf {sdir}"
        )


def write_snapshot(
    df_or_ser: ta.Union[pd.DataFrame, pd.Series[float]], name: SnapshotName
) -> None:
    """Write DataFrame or Series to parquet file in snapshot directory."""
    snapshot_pth = os.path.join(snapshot_dir(), f"{name}.parquet")

    if isinstance(df_or_ser, pd.Series):
        pq.write_table(
            pa.Table.from_pandas(df_or_ser.to_frame()),
            snapshot_pth,
        )
    elif isinstance(df_or_ser, pd.DataFrame):
        pq.write_table(
            pa.Table.from_pandas(df_or_ser),
            snapshot_pth,
        )
    else:
        raise RuntimeError(
            f"Unexpected type {type(df_or_ser)}, expected DataFrame or Series"
        )


def upload_snapshots(
    adhoc: bool = False, snapshot_prefix_override: str | None = None
) -> None:
    """Upload all snapshots in local directory to GCS."""
    prefix = snapshot_prefix_override if snapshot_prefix_override else snapshot_prefix()
    sdir = snapshot_dir()

    for file in os.listdir(sdir):
        if not file.endswith('.parquet'):
            continue

        logger.info(f"Uploading snapshot {prefix}/{file}")
        local_path = os.path.join(sdir, file)
        gcs_path = posixpath.join(
            GCS_SNAPSHOT_DIR,
            prefix,
            "adhoc" if adhoc else "",
            file,
        )
        gs_url = f"gs://cornerstone-default/{gcs_path}"
        upload_file_to_gcs(local_path, gs_url)


@click.command(help='Generate snapshots for bedrock EEIO model')
@click.option('--config_name', required=True, type=str, default='v8_ceda_2025_usa')
@click.option('--adhoc', is_flag=True, help='Upload to adhoc directory')
@click.option(
    '--snapshot_prefix_override',
    default=None,
    type=str,
    help='Override git hash prefix',
)
@click.option('--skip_upload', is_flag=True, help='Skip GCS upload')
def generate_snapshots(
    config_name: str,
    adhoc: bool,
    snapshot_prefix_override: str | None,
    skip_upload: bool,
) -> None:
    """Generate snapshots for all USA matrices and upload to GCS."""
    total_start = time.time()
    set_global_usa_config(config_name)

    # If an FBA requires an API key to generate, download the FBA from GCS
    common.download_fba_on_api_error = True

    # Late-binding imports after config is set
    from bedrock.transform.allocation.derived import derive_E_usa
    from bedrock.transform.eeio.derived import (
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
        derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection,
        derive_y_for_national_accounting_balance_usa,
        derive_ydom_and_yimp_usa,
    )

    # Assert clean state
    assert_cache_empty()
    assert_snapshots_empty()

    logger.info(f'Generating snapshots for config: {config_name}')
    logger.info(f'Snapshot prefix: {snapshot_prefix_override or snapshot_prefix()}')

    # Generate E_USA_ES
    t0 = time.time()
    logger.info('Generating E_USA_ES snapshot')
    write_snapshot(derive_E_usa(), 'E_USA_ES')
    logger.info(f'[TIMING] E_USA_ES completed in {time.time() - t0:.1f}s')

    # Generate B_USA_non_finetuned
    t0 = time.time()
    logger.info('Generating B_USA_non_finetuned snapshot')
    write_snapshot(derive_B_usa_non_finetuned(), 'B_USA_non_finetuned')
    logger.info(f'[TIMING] B_USA_non_finetuned completed in {time.time() - t0:.1f}s')

    # Generate Aq matrices (Adom_USA, Aimp_USA, scaled_q_USA)
    t0 = time.time()
    logger.info('Generating Aq_USA snapshots (Adom, Aimp, scaled_q)')
    aq_set = derive_Aq_usa()
    write_snapshot(aq_set.Adom, 'Adom_USA')
    write_snapshot(aq_set.Aimp, 'Aimp_USA')
    write_snapshot(aq_set.scaled_q, 'scaled_q_USA')
    logger.info(f'[TIMING] Aq_USA snapshots completed in {time.time() - t0:.1f}s')

    # Generate y_nab_USA
    t0 = time.time()
    logger.info('Generating y_nab_USA snapshot')
    write_snapshot(derive_y_for_national_accounting_balance_usa(), 'y_nab_USA')
    logger.info(f'[TIMING] y_nab_USA completed in {time.time() - t0:.1f}s')

    # Generate ytot_USA and exports_USA
    t0 = time.time()
    logger.info('Generating ytot_USA and exports_USA snapshots')
    y_and_trade_set = (
        derive_Y_and_trade_matrix_usa_from_summary_target_year_ytot_and_structural_reflection()
    )
    write_snapshot(y_and_trade_set.ytot, 'ytot_USA')
    write_snapshot(y_and_trade_set.exports, 'exports_USA')
    logger.info(
        f'[TIMING] ytot_USA and exports_USA completed in {time.time() - t0:.1f}s'
    )

    # Generate ydom_USA and yimp_USA
    t0 = time.time()
    logger.info('Generating ydom_USA and yimp_USA snapshots')
    y_vector_set = derive_ydom_and_yimp_usa()
    write_snapshot(y_vector_set.ydom, 'ydom_USA')
    write_snapshot(y_vector_set.yimp, 'yimp_USA')
    logger.info(f'[TIMING] ydom_USA and yimp_USA completed in {time.time() - t0:.1f}s')

    # Upload to GCS
    if not skip_upload:
        t0 = time.time()
        logger.info('Uploading snapshots to GCS')
        upload_snapshots(adhoc=adhoc, snapshot_prefix_override=snapshot_prefix_override)
        logger.info(f'[TIMING] Upload completed in {time.time() - t0:.1f}s')

    logger.info(
        f'[TIMING] Total snapshot generation completed in {time.time() - total_start:.1f}s'
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    generate_snapshots()
