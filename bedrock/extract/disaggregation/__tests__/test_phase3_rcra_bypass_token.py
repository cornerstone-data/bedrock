"""Smoke: derive_waste_weights for Phase 3 years records rcra_path=br_bypass."""

from __future__ import annotations

import pytest

from bedrock.analysis.nowcasting.waste_disaggregation.phase3_pins import PHASE3_YEARS
from bedrock.extract.disaggregation.derive_waste_weights import derive_waste_weights


@pytest.mark.realdata
@pytest.mark.parametrize("year", [2018, 2024])
def test_derive_records_rcra_br_bypass(year: int) -> None:
    _weights, prov = derive_waste_weights(year)
    notes = " | ".join(prov.fallback_notes)
    assert "rcra_path=br_bypass" in notes
    assert year in PHASE3_YEARS
