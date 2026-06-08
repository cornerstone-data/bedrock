"""CLI: export before/after 221100 electricity reallocation Make / Use matrices.

Invoke as::

    uv run python -m bedrock.analysis.electricity.write_221100_reallocation_matrices \\
        --config_name test_usa_config_waste_disagg_electricity.yaml
"""

from __future__ import annotations

import logging
import time

import click

import bedrock.utils.config.common as common
from bedrock.analysis.electricity.reallocation_matrices import (
    write_electricity_reallocation_intermediate_outputs,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    derive_disagg_io_bundle,
    electricity_reallocation_enabled,
)
from bedrock.utils.config.usa_config import set_global_usa_config

logger = logging.getLogger(__name__)


@click.command(
    help='Export before/after 221100 electricity reallocation V and extended U to Excel.'
)
@click.option(
    '--config_name',
    required=True,
    type=str,
    help='USA config YAML with implement_electricity_reallocation enabled.',
)
def main(config_name: str) -> None:
    t0 = time.time()
    set_global_usa_config(config_name)
    common.download_fba_on_api_error = True

    if not electricity_reallocation_enabled():
        raise click.ClickException(
            f'{config_name!r} must set implement_electricity_reallocation: true'
        )

    bundle = derive_disagg_io_bundle()
    output_path = write_electricity_reallocation_intermediate_outputs(
        v=bundle.V,
        udom=bundle.Udom,
        uimp=bundle.Uimp,
        va=bundle.VA,
    )

    logger.info(
        '[TIMING] 221100 reallocation matrix export completed in %.1fs (output=%s)',
        time.time() - t0,
        output_path,
    )
    print(f'Wrote electricity reallocation matrices to {output_path.resolve()}')


if __name__ == '__main__':
    main()
