from __future__ import annotations

import json
from pathlib import Path
from typing import Generator
from unittest.mock import patch

import pandas as pd
import pytest
from click.testing import CliRunner

from bedrock.utils.config.usa_config import get_usa_config, reset_usa_config
from bedrock.utils.validation.generate_diagnostics import generate_diagnostics


@pytest.fixture(autouse=True, scope="function")
def reset_global_usa_config_before_test() -> Generator[None, None, None]:
    reset_usa_config(should_reset_env_var=True)
    yield


def test_generate_diagnostics_writes_baseline_snapshot_key_to_config_summary() -> None:
    runner = CliRunner()

    with (
        patch(
            'bedrock.utils.validation.calculate_ef_diagnostics.calculate_ef_diagnostics'
        ),
        patch(
            'bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.calculate_national_accounting_balance_diagnostics'
        ),
        patch(
            'bedrock.utils.validation.generate_diagnostics.update_sheet_tab'
        ) as mock_update,
    ):
        result = runner.invoke(
            generate_diagnostics,
            [
                "--sheet_id",
                "test_sheet",
                "--config_name",
                "test_usa_config_git_sha",
                "--git_branch",
                "test_branch",
            ],
        )

    assert result.exit_code == 0

    written_df = mock_update.call_args.args[2]
    assert isinstance(written_df, pd.DataFrame)

    baseline_row = written_df.loc[
        written_df['config_field'] == 'baseline_snapshot_key_used', 'value'
    ]
    assert len(baseline_row) == 1
    assert baseline_row.iloc[0] == '2ebb51f7190c3a62b5d8b2420bff9b20f57282fc'


def test_generate_diagnostics_pin_json_selects_useeio_baseline(tmp_path: Path) -> None:
    pin = tmp_path / 'pin.json'
    pin.write_text(
        json.dumps(
            {
                'gs_uri': 'gs://cornerstone-default/snapshots/x/y.xlsx',
                'sha256': 'a' * 64,
                'model_version_label': 'cli-test-pin',
            }
        ),
        encoding='utf-8',
    )
    runner = CliRunner()

    with (
        patch(
            'bedrock.utils.validation.calculate_ef_diagnostics.calculate_ef_diagnostics'
        ),
        patch(
            'bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.calculate_national_accounting_balance_diagnostics'
        ),
        patch('bedrock.utils.validation.generate_diagnostics.update_sheet_tab'),
    ):
        result = runner.invoke(
            generate_diagnostics,
            [
                '--sheet_id',
                'test_sheet',
                '--config_name',
                'test_usa_config_git_sha',
                '--git_branch',
                'test_branch',
                '--useeio_baseline_pin_json',
                str(pin),
            ],
        )

    assert result.exit_code == 0
    assert get_usa_config().diagnostics_baseline_source == 'gcs_useeio_xlsx'
    assert get_usa_config().useeio_model_version_label == 'cli-test-pin'


def test_generate_diagnostics_diagnostics_baseline_source_overrides_pin_mode(
    tmp_path: Path,
) -> None:
    pin = tmp_path / 'pin.json'
    pin.write_text(
        json.dumps(
            {
                'gs_uri': 'gs://cornerstone-default/snapshots/x/y.xlsx',
                'sha256': 'b' * 64,
                'model_version_label': 'from-pin',
            }
        ),
        encoding='utf-8',
    )
    runner = CliRunner()

    with (
        patch(
            'bedrock.utils.validation.calculate_ef_diagnostics.calculate_ef_diagnostics'
        ),
        patch(
            'bedrock.utils.validation.calculate_national_accounting_balance_diagnostics.calculate_national_accounting_balance_diagnostics'
        ),
        patch('bedrock.utils.validation.generate_diagnostics.update_sheet_tab'),
    ):
        result = runner.invoke(
            generate_diagnostics,
            [
                '--sheet_id',
                'test_sheet',
                '--config_name',
                'test_usa_config_git_sha',
                '--git_branch',
                'test_branch',
                '--useeio_baseline_pin_json',
                str(pin),
                '--diagnostics_baseline_source',
                'gcs_snapshot',
            ],
        )

    assert result.exit_code == 0
    assert get_usa_config().diagnostics_baseline_source == 'gcs_snapshot'
    assert get_usa_config().useeio_model_version_label == 'from-pin'
