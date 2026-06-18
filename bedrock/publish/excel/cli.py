"""XLSX publish CLI: build the bedrock model workbook locally.

Invoke as:

    uv run python -m bedrock.publish.excel.cli --config_name <cfg>

GCS upload is TODO. Other formats (e.g. supply-chain factors at
https://github.com/cornerstone-data/supply-chain-factors) will have
their own `bedrock.publish.<format>.cli` entry points.
"""

# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import os
import time

import click

import bedrock.utils.config.common as common
from bedrock.utils.config.settings import GIT_HASH_LONG
from bedrock.utils.config.usa_config import set_global_usa_config

logger = logging.getLogger(__name__)

# bedrock/publish/excel/cli.py -> bedrock/publish/
_PUBLISH_PKG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_OUTPUT_DIR = os.path.join(_PUBLISH_PKG_DIR, 'output')


def _publish_prefix() -> str:
    if not GIT_HASH_LONG:
        raise RuntimeError('GIT_HASH_LONG is not set')
    return GIT_HASH_LONG


def _default_output_dir() -> str:
    return os.path.join(_DEFAULT_OUTPUT_DIR, _publish_prefix())


def _publish_xlsx(
    config_name: str,
    output_dir: str | None,
) -> str:
    from bedrock.publish.excel import write_model_to_xlsx

    local_dir = output_dir or _default_output_dir()
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, f'{config_name}.xlsx')

    logger.info('publish: building XLSX for config=%s -> %s', config_name, local_path)
    write_model_to_xlsx(local_path, config_name=config_name)
    return local_path


@click.command(help='Publish the bedrock EEIO model as a local XLSX workbook.')
@click.option('--config_name', required=True, type=str)
@click.option(
    '--output_dir',
    default=None,
    type=str,
    help='Local directory for the workbook. The file is always named '
    '<config_name>.xlsx. Defaults to bedrock/publish/output/<git_sha>/.',
)
def publish(config_name: str, output_dir: str | None) -> None:
    """Run the XLSX publish pipeline."""
    t0 = time.time()
    set_global_usa_config(config_name)
    common.download_fba_on_api_error = True

    local_path = _publish_xlsx(config_name=config_name, output_dir=output_dir)

    logger.info(
        '[TIMING] publish completed in %.1fs (local=%s)',
        time.time() - t0,
        local_path,
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    publish()
