"""CLI: export three-stage electricity pipeline V / U / Y matrices (PR3 analysis).

Invoke as::

    uv run python -m bedrock.analysis.electricity.write_221100_disaggregation_matrices \\
        --config_name 2025_usa_cornerstone_full_model_electricity_disaggregation.yaml
"""

from __future__ import annotations

import logging
import time

import click

import bedrock.utils.config.common as common
from bedrock.analysis.electricity.disaggregation_matrices import (
    assert_disaggregation_export_config,
    write_electricity_disaggregation_intermediate_outputs,
)
from bedrock.utils.config.usa_config import set_global_usa_config

logger = logging.getLogger(__name__)


@click.command(
    help=(
        "Export three-stage electricity pipeline V, extended U, and Y "
        "(waste → reallocation → disaggregation) to Excel."
    )
)
@click.option(
    "--config_name",
    required=True,
    type=str,
    help=(
        "USA config YAML with implement_waste_disaggregation, "
        "implement_electricity_reallocation, and "
        "implement_electricity_disaggregation enabled."
    ),
)
def main(config_name: str) -> None:
    t0 = time.time()
    set_global_usa_config(config_name)
    common.download_fba_on_api_error = True

    try:
        assert_disaggregation_export_config()
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    output_path = write_electricity_disaggregation_intermediate_outputs()

    logger.info(
        "[TIMING] 221100 disaggregation matrix export completed in %.1fs (output=%s)",
        time.time() - t0,
        output_path,
    )
    print(f"Wrote electricity disaggregation matrices to {output_path.resolve()}")


if __name__ == "__main__":
    main()
