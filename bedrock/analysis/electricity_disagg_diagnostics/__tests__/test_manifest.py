"""Tests for manifest loading and validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from bedrock.analysis.electricity_disagg_diagnostics.manifest import (
    RunExpectation,
    expectations_for_manifest,
    load_manifest,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import V03_SNAPSHOT_SHA


def test_load_manifest_rejects_placeholder_sheet_ids(tmp_path: Path) -> None:
    manifest_path = tmp_path / 'manifest.yaml'
    manifest_path.write_text(
        yaml.safe_dump(
            {
                'meta': {'title': 'test'},
                'footing': {
                    'label': 'foot',
                    'sheet_id': '<SHEET_ID_FOOTING>',
                    'config': '2025_usa_cornerstone_v0_3_electricity_footing',
                },
                'steps': [],
                'final': {
                    'label': 'final',
                    'sheet_id': '<SHEET_ID_MIXED>',
                    'config': '2025_usa_cornerstone_v0_3_electricity_mixed_units',
                },
            }
        ),
        encoding='utf-8',
    )
    with pytest.raises(ValueError, match='sheet_id'):
        load_manifest(manifest_path)


def test_load_manifest_can_skip_sheet_id_check() -> None:
    manifest = load_manifest(require_sheet_ids=False)
    assert manifest.footing.config == '2025_usa_cornerstone_v0_3_electricity_footing'
    assert manifest.final.config.endswith('electricity_mixed_units')


def test_expectations_include_methodology_and_sha(tmp_path: Path) -> None:
    manifest_path = tmp_path / 'manifest.yaml'
    manifest_path.write_text(
        yaml.safe_dump(
            {
                'meta': {'title': 'test'},
                'footing': {
                    'label': 'foot',
                    'sheet_id': 'footing_sheet',
                    'config': '2025_usa_cornerstone_v0_3_electricity_footing',
                },
                'steps': [
                    {
                        'label': 'realloc',
                        'sheet_id': 'realloc_sheet',
                        'config': '2025_usa_cornerstone_v0_3_electricity_reallocation',
                    },
                    {
                        'label': 'disagg',
                        'sheet_id': 'disagg_sheet',
                        'config': '2025_usa_cornerstone_v0_3_electricity_disaggregation',
                    },
                    {
                        'label': 'mixed',
                        'sheet_id': 'mixed_sheet',
                        'config': '2025_usa_cornerstone_v0_3_electricity_mixed_units',
                    },
                ],
                'final': {
                    'label': 'final',
                    'sheet_id': 'mixed_sheet',
                    'config': '2025_usa_cornerstone_v0_3_electricity_mixed_units',
                },
            }
        ),
        encoding='utf-8',
    )
    manifest = load_manifest(manifest_path)
    expectations = expectations_for_manifest(manifest)
    assert len(expectations) == 4  # footing + 3 steps; final shares mixed sheet_id
    footing_exp = expectations[0][1]
    assert isinstance(footing_exp, RunExpectation)
    assert footing_exp.snapshot_sha == V03_SNAPSHOT_SHA
    assert footing_exp.cornerstone_industry_avg_margins is False
    assert footing_exp.apply_io_year_adjustments is True
    assert footing_exp.model_base_year == 2024
    assert footing_exp.usa_ghg_data_year == 2024
    assert footing_exp.implement_electricity_reallocation is None
    mixed_exp = expectations[-1][1]
    assert mixed_exp.implement_electricity_mixed_units is True
    assert mixed_exp.snapshot_sha == V03_SNAPSHOT_SHA
