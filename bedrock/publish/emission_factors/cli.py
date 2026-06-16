"""CLI for cornerstone supply-chain emission factor CSV publish."""

# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import os
import time

import click

import bedrock.utils.config.common as common
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.utils.config.settings import GIT_HASH_LONG
from bedrock.utils.config.usa_config import set_global_usa_config

logger = logging.getLogger(__name__)

_PUBLISH_PKG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_OUTPUT_DIR = os.path.join(_PUBLISH_PKG_DIR, 'output')


def _publish_prefix() -> str:
    if not GIT_HASH_LONG:
        raise RuntimeError('GIT_HASH_LONG is not set')
    return GIT_HASH_LONG


def _default_output_dir(config_name: str) -> str:
    return os.path.join(_DEFAULT_OUTPUT_DIR, _publish_prefix(), config_name)


@click.command(help='Publish cornerstone supply-chain CO2e emission factors as CSV.')
@click.option('--config_name', required=True, type=str)
@click.option(
    '--dollar_year',
    required=True,
    type=int,
    help='Target USD year for purchaser-price emission factors.',
)
@click.option(
    '--output_dir',
    default=None,
    type=str,
    help='Output directory. Defaults to bedrock/publish/output/<git_sha>/<config_name>/.',
)
@click.option(
    '--write_matrices',
    is_flag=True,
    default=False,
    help='Also write M_pur and N_pur CSVs under matrices/.',
)
def publish(
    config_name: str,
    dollar_year: int,
    output_dir: str | None,
    write_matrices: bool,
) -> None:
    from bedrock.publish.emission_factors.writer import write_emission_factors

    t0 = time.time()
    clear_all_publish_caches()
    set_global_usa_config(config_name)
    common.download_fba_on_api_error = True

    local_dir = output_dir or _default_output_dir(config_name)
    paths = write_emission_factors(
        local_dir,
        config_name=config_name,
        dollar_year=dollar_year,
        write_matrices=write_matrices,
    )

    logger.info(
        '[TIMING] emission-factor publish completed in %.1fs (paths=%s)',
        time.time() - t0,
        paths,
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    publish()
