"""Tests for local Excel import into diagnostics cache."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bedrock.analysis.electricity_disagg_diagnostics.local_data import (
    import_workbook_to_cache,
    local_workbook_path,
    seed_cache_from_local_dir,
)
from bedrock.analysis.electricity_disagg_diagnostics.manifest import load_manifest
from bedrock.utils.validation.analysis.bly_plots import TAB_BLY
from bedrock.utils.validation.analysis.fetch import load_tab


@pytest.fixture
def sample_workbook(tmp_path: Path) -> Path:
    path = tmp_path / '2025_usa_cornerstone_v0_2.xlsx'
    bly = pd.DataFrame(
        {
            'index': ['1111A0', '221100'],
            'BLy_new (MtCO2e)': [1.0, 2.0],
            'BLy_old (MtCO2e)': [0.9, 1.8],
        }
    )
    config = pd.DataFrame(
        {
            'config_field': ['config_name', 'implement_electricity_reallocation'],
            'value': ['2025_usa_cornerstone_v0_2', 'False'],
        }
    )
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        bly.to_excel(writer, sheet_name=TAB_BLY, index=False)
        config.to_excel(writer, sheet_name='config_summary', index=False)
    return path


def test_local_workbook_path_resolves_xlsx(
    tmp_path: Path, sample_workbook: Path
) -> None:
    found = local_workbook_path(tmp_path, '2025_usa_cornerstone_v0_2')
    assert found == sample_workbook


def test_import_workbook_mixed_config_summary_types(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'mixed_config.xlsx'
    config = pd.DataFrame(
        {
            'config_field': ['config_name', 'model_base_year'],
            'value': ['2025_usa_cornerstone_v0_2', 2024],
        }
    )
    bly = pd.DataFrame({'index': ['1111A0'], 'BLy_new (MtCO2e)': [1.0]})
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        bly.to_excel(writer, sheet_name=TAB_BLY, index=False)
        config.to_excel(writer, sheet_name='config_summary', index=False)
    import_workbook_to_cache(path, 'mixed_types_sheet')
    cfg = load_tab('mixed_types_sheet', 'config_summary', refresh=False)
    assert cfg['value'].dtype == object
    assert cfg.loc[1, 'value'] == '2024'


def test_import_workbook_to_cache(tmp_path: Path, sample_workbook: Path) -> None:
    import_workbook_to_cache(sample_workbook, 'test_sheet_id')
    df = load_tab('test_sheet_id', TAB_BLY, refresh=False)
    assert list(df['index'].astype(str)) == ['1111A0', '221100']


def test_seed_cache_from_local_dir_uses_manifest_configs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = load_manifest()
    for config_name in {
        manifest.footing.config,
        *(s.config for s in manifest.steps),
    }:
        path = tmp_path / f'{config_name}.xlsx'
        bly = pd.DataFrame(
            {
                'index': ['1111A0'],
                'BLy_new (MtCO2e)': [1.0],
            }
        )
        config_rows = {
            'config_field': ['config_name'],
            'value': [config_name],
        }
        if 'electricity_reallocation' in config_name:
            config_rows['config_field'] += [
                'implement_electricity_reallocation',
                'implement_electricity_disaggregation',
                'implement_electricity_mixed_units',
                'snapshot_version_or_git_sha',
                'diagnostics_baseline_source',
            ]
            config_rows['value'] += [
                'True',
                'False',
                'False',
                '7372464249c434c9bebb172c065a4d0e3702176e',
                'gcs_snapshot',
            ]
        elif 'electricity_disaggregation' in config_name:
            config_rows['config_field'] += [
                'implement_electricity_reallocation',
                'implement_electricity_disaggregation',
                'implement_electricity_mixed_units',
                'snapshot_version_or_git_sha',
                'diagnostics_baseline_source',
            ]
            config_rows['value'] += [
                'True',
                'True',
                'False',
                '7372464249c434c9bebb172c065a4d0e3702176e',
                'gcs_snapshot',
            ]
        elif 'electricity_mixed_units' in config_name:
            config_rows['config_field'] += [
                'implement_electricity_reallocation',
                'implement_electricity_disaggregation',
                'implement_electricity_mixed_units',
                'snapshot_version_or_git_sha',
                'diagnostics_baseline_source',
            ]
            config_rows['value'] += [
                'True',
                'True',
                'True',
                '7372464249c434c9bebb172c065a4d0e3702176e',
                'gcs_snapshot',
            ]
        config = pd.DataFrame(config_rows)
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            bly.to_excel(writer, sheet_name=TAB_BLY, index=False)
            config.to_excel(writer, sheet_name='config_summary', index=False)

    seed_cache_from_local_dir(manifest, tmp_path)
    df = load_tab(manifest.footing.sheet_id, TAB_BLY, refresh=False)
    assert not df.empty
