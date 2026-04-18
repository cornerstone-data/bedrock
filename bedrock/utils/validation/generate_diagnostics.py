# ruff: noqa: PLC0415
from __future__ import annotations

import logging
import time

import click
import pandas as pd

import bedrock.utils.config.common as common
from bedrock.utils.config.settings import (
    GIT_BRANCH,
    GIT_HASH_LONG,
    GIT_PR_URL,
)
from bedrock.utils.config.usa_config import get_usa_config, set_global_usa_config
from bedrock.utils.io.gcp import update_sheet_tab
from bedrock.utils.snapshots.loader import resolve_snapshot_key

logger = logging.getLogger(__name__)


@click.command(
    help='Run diagnostics comparing current EEIO model against previous release'
)
@click.option('--sheet_id', required=True, type=str)
@click.option(
    '--config_name',
    required=True,
    type=str,
    default='v8_ceda_2025_usa',
)
@click.option('--git_branch', default=None, type=str, help='Override git branch name')
@click.option('--pr_url', default=None, type=str, help='Override PR URL')
@click.option(
    '--diagnostics_baseline_source',
    default=None,
    type=click.Choice(['gcs_snapshot', 'gcs_useeio_xlsx']),
    help='Override diagnostics baseline source (merged onto config YAML)',
)
@click.option(
    '--useeio_baseline_pin_json',
    default=None,
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    help=(
        'JSON with gs_uri, sha256, model_version_label (see '
        'bedrock/utils/snapshots/useeio_baseline_pin.json). Selects '
        "diagnostics_baseline_source='gcs_useeio_xlsx' unless overridden by "
        '--diagnostics_baseline_source.'
    ),
)
def generate_diagnostics(
    sheet_id: str,
    config_name: str,
    git_branch: str | None,
    pr_url: str | None,
    diagnostics_baseline_source: str | None,
    useeio_baseline_pin_json: str | None,
) -> None:
    total_start = time.time()
    overrides: dict[str, object] = {}
    if useeio_baseline_pin_json is not None:
        from bedrock.utils.validation.useeio_excel_baseline import (
            load_useeio_baseline_pin_overrides,
        )

        pin_fragment = load_useeio_baseline_pin_overrides(useeio_baseline_pin_json)
        overrides.update(pin_fragment)
        overrides['diagnostics_baseline_source'] = 'gcs_useeio_xlsx'
    if diagnostics_baseline_source is not None:
        overrides['diagnostics_baseline_source'] = diagnostics_baseline_source
    set_global_usa_config(
        config_name,
        diagnostics_cli_overrides=overrides if overrides else None,
    )

    # If an FBA requires an API key to generate, download the FBA during diagnostics
    common.download_fba_on_api_error = True

    # Late-binding imports - depend on global config
    from bedrock.utils.validation.calculate_ef_diagnostics import (
        calculate_ef_diagnostics,
    )
    from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
        calculate_national_accounting_balance_diagnostics,
    )

    logger.info('----- Calculating and writing diagnostics -----')

    t0 = time.time()
    calculate_ef_diagnostics(sheet_id=sheet_id)
    logger.info(f'[TIMING] EF diagnostics completed in {time.time() - t0:.1f}s')

    t0 = time.time()
    calculate_national_accounting_balance_diagnostics(sheet_id=sheet_id)
    logger.info(
        f'[TIMING] National accounting balance diagnostics completed in {time.time() - t0:.1f}s'
    )

    logger.info('----- Updating config summary -----')
    t0 = time.time()
    cfg = get_usa_config()
    config_df = cfg.to_dataframe(config_name)
    baseline_snap = (
        resolve_snapshot_key()
        if cfg.diagnostics_baseline_source == 'gcs_snapshot'
        else 'N/A (USEEIO Excel baseline)'
    )
    # Run metadata only — diagnostics_baseline_source and useeio_* already appear
    # once in config_df from USAConfig.to_dataframe().
    git_rows: list[dict[str, str]] = [
        {'config_field': 'git_commit', 'value': GIT_HASH_LONG or 'unknown'},
        {
            'config_field': 'git_branch',
            'value': git_branch or GIT_BRANCH or 'unknown',
        },
        {'config_field': 'git_pr_url', 'value': pr_url or GIT_PR_URL or 'N/A'},
        {'config_field': 'baseline_snapshot_key_used', 'value': baseline_snap},
    ]
    git_metadata = pd.DataFrame(git_rows)
    config_df = pd.concat([git_metadata, config_df], ignore_index=True)
    update_sheet_tab(
        sheet_id,
        'config_summary',
        config_df,
    )
    logger.info(f'[TIMING] Config summary update completed in {time.time() - t0:.1f}s')
    logger.info(
        f'[TIMING] Total diagnostics completed in {time.time() - total_start:.1f}s'
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s')
    generate_diagnostics()
