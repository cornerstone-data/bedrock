from __future__ import annotations

from typing import Generator
from unittest.mock import patch

import pandas as pd
import pytest
from click.testing import CliRunner

from bedrock.utils.config.usa_config import reset_usa_config
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
    assert baseline_row.iloc[0] == 'ff3c5a0ea73b26cecd09fd0613b8b34e1f30bcdc'
