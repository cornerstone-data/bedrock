# ruff: noqa: PLC0415
from __future__ import annotations

import logging

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
    set_global_usa_config(config_name)

    # Late-binding imports - depend on global config
    from bedrock.utils.validation.calculate_ef_diagnostics import (
        calculate_ef_diagnostics,
    )
    from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
        calculate_national_accounting_balance_diagnostics,
    )

    logger.info("----- Calculating and writing diagnostics -----")

    calculate_ef_diagnostics(sheet_id=sheet_id)
    calculate_national_accounting_balance_diagnostics(sheet_id=sheet_id)

    logger.info("----- Updating config summary -----")
    update_sheet_tab(
        sheet_id,
        "config_summary",
        get_usa_config().to_dataframe(config_name),
    )


if __name__ == "__main__":
    generate_diagnostics()
