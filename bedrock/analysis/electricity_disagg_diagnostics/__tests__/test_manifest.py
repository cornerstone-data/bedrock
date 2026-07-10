"""Tests for manifest loading and validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from bedrock.analysis.electricity_disagg_diagnostics.manifest import load_manifest


def test_load_manifest_rejects_placeholder_sheet_ids(tmp_path: Path) -> None:
    manifest_path = tmp_path / 'manifest.yaml'
    manifest_path.write_text(
        yaml.safe_dump(
            {
                'meta': {'title': 'test'},
                'footing': {
                    'label': 'foot',
                    'sheet_id': '<SHEET_ID_FOOTING>',
                    'config': '2025_usa_cornerstone_v0_2',
                },
                'steps': [],
                'final': {
                    'label': 'final',
                    'sheet_id': '<SHEET_ID_MIXED>',
                    'config': '2025_usa_cornerstone_v0_2_electricity_mixed_units',
                },
            }
        ),
        encoding='utf-8',
    )
    with pytest.raises(ValueError, match='sheet_id'):
        load_manifest(manifest_path)
