"""Tests for local Excel import into diagnostics cache."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from bedrock.analysis.electricity_disagg_diagnostics.local_workbooks import (
    import_workbook_to_cache,
    local_workbook_path,
    seed_cache_from_local_dir,
)
from bedrock.analysis.electricity_disagg_diagnostics.manifest import load_manifest
from bedrock.analysis.electricity_disagg_diagnostics.paths import V03_SNAPSHOT_SHA
from bedrock.utils.validation.analysis.bly_plots import TAB_BLY
from bedrock.utils.validation.analysis.fetch import load_tab

_FOOTING = '2025_usa_cornerstone_v0_3_electricity_footing'
_REALLOC = '2025_usa_cornerstone_v0_3_electricity_reallocation'
_DISAGG = '2025_usa_cornerstone_v0_3_electricity_disaggregation'
_MIXED = '2025_usa_cornerstone_v0_3_electricity_mixed_units'


@pytest.fixture
def sample_workbook(tmp_path: Path) -> Path:
    path = tmp_path / f'{_FOOTING}.xlsx'
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
            'value': [_FOOTING, 'False'],
        }
    )
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        bly.to_excel(writer, sheet_name=TAB_BLY, index=False)
        config.to_excel(writer, sheet_name='config_summary', index=False)
    return path


def test_local_workbook_path_resolves_xlsx(
    tmp_path: Path, sample_workbook: Path
) -> None:
    found = local_workbook_path(tmp_path, _FOOTING)
    assert found == sample_workbook


def test_import_workbook_mixed_config_summary_types(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'mixed_config.xlsx'
    config = pd.DataFrame(
        {
            'config_field': ['config_name', 'model_base_year'],
            'value': [_FOOTING, 2024],
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


def _methodology_rows(config_name: str) -> dict[str, list[object]]:
    flags: dict[str, str] = {
        'implement_electricity_reallocation': 'False',
        'implement_electricity_disaggregation': 'False',
        'implement_electricity_mixed_units': 'False',
    }
    if 'electricity_reallocation' in config_name:
        flags['implement_electricity_reallocation'] = 'True'
    if 'electricity_disaggregation' in config_name:
        flags['implement_electricity_reallocation'] = 'True'
        flags['implement_electricity_disaggregation'] = 'True'
    if 'electricity_mixed_units' in config_name:
        flags['implement_electricity_reallocation'] = 'True'
        flags['implement_electricity_disaggregation'] = 'True'
        flags['implement_electricity_mixed_units'] = 'True'
    fields: list[object] = [
        'config_name',
        'cornerstone_industry_avg_margins',
        'apply_io_year_adjustments',
        'model_base_year',
        'usa_ghg_data_year',
        'snapshot_version_or_git_sha',
        'diagnostics_baseline_source',
        *flags.keys(),
    ]
    values: list[object] = [
        config_name,
        'False',
        'True',
        2024,
        2024,
        V03_SNAPSHOT_SHA,
        'gcs_snapshot',
        *flags.values(),
    ]
    return {'config_field': fields, 'value': values}


def test_seed_cache_from_local_dir_uses_manifest_configs(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / 'manifest.yaml'
    manifest_path.write_text(
        yaml.safe_dump(
            {
                'meta': {'title': 'test'},
                'footing': {
                    'label': 'Cornerstone v0.3.1 electricity footing',
                    'sheet_id': 'test_footing_sheet',
                    'config': _FOOTING,
                },
                'steps': [
                    {
                        'label': 'realloc',
                        'sheet_id': 'test_realloc_sheet',
                        'config': _REALLOC,
                    },
                    {
                        'label': 'disagg',
                        'sheet_id': 'test_disagg_sheet',
                        'config': _DISAGG,
                    },
                    {
                        'label': 'mixed',
                        'sheet_id': 'test_mixed_sheet',
                        'config': _MIXED,
                    },
                ],
                'final': {
                    'label': 'final',
                    'sheet_id': 'test_mixed_sheet',
                    'config': _MIXED,
                },
            }
        ),
        encoding='utf-8',
    )
    manifest = load_manifest(manifest_path)
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
        config = pd.DataFrame(_methodology_rows(config_name))
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            bly.to_excel(writer, sheet_name=TAB_BLY, index=False)
            config.to_excel(writer, sheet_name='config_summary', index=False)

    seed_cache_from_local_dir(manifest, tmp_path)
    df = load_tab(manifest.footing.sheet_id, TAB_BLY, refresh=False)
    assert not df.empty
