# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import time

import click

from bedrock.utils.config.usa_config import get_usa_config, set_global_usa_config
from bedrock.utils.io.gcp import update_sheet_tab

logger = logging.getLogger(__name__)


@click.command(
    help="Run diagnostics comparing current EEIO model against previous release"
)
@click.option("--sheet_id", required=True, type=str)
@click.option(
    "--config_name",
    required=True,
    type=str,
    default="v8_ceda_2025_usa",
)
def generate_diagnostics(
    sheet_id: str,
    config_name: str,
) -> None:
    total_start = time.time()
    set_global_usa_config(config_name)

    # Late-binding imports - depend on global config
    from bedrock.utils.validation.calculate_ef_diagnostics import (
        calculate_ef_diagnostics,
    )
    from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
        calculate_national_accounting_balance_diagnostics,
    )

    logger.info("----- Calculating and writing diagnostics -----")

    t0 = time.time()
    calculate_ef_diagnostics(sheet_id=sheet_id)
    logger.info(f"[TIMING] EF diagnostics completed in {time.time() - t0:.1f}s")

    t0 = time.time()
    calculate_national_accounting_balance_diagnostics(sheet_id=sheet_id)
    logger.info(
        f"[TIMING] National accounting balance diagnostics completed in {time.time() - t0:.1f}s"
    )

    logger.info("----- Updating config summary -----")
    t0 = time.time()
    update_sheet_tab(
        sheet_id,
        "config_summary",
        get_usa_config().to_dataframe(config_name),
    )
    logger.info(f"[TIMING] Config summary update completed in {time.time() - t0:.1f}s")
    logger.info(
        f"[TIMING] Total diagnostics completed in {time.time() - total_start:.1f}s"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    generate_diagnostics()
