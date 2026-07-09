"""Tests for manifest loading and validation."""

from __future__ import annotations

import pytest

from bedrock.analysis.electricity_disagg_diagnostics.manifest import load_manifest


def test_load_manifest_rejects_placeholder_sheet_ids() -> None:
    with pytest.raises(ValueError, match='sheet_id'):
        load_manifest()
