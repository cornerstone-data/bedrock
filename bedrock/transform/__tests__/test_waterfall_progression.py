"""Live reproducibility of dual-ladder waterfall configs (except pinned baselines).

Each live case rebuilds ``derive_Aq_usa`` + ``derive_B_usa_non_finetuned`` for
one waterfall config, q-weights ``1ᵀ B L`` with canonical v0.5
``scaled_q_USA``, and compares to the diagnostics sheet ``N_new`` pin.

Configs covered:

* ``v03_waterfall_useeio_g1_schema_ghg``
* ``v03_waterfall_ceda_g1a_schema_ghg``
* ``v03_waterfall_ceda_g1b_waste_disagg``
* ``v05_waterfall_g2_methods``
* ``v05_waterfall_g3_data``
* ``v05_waterfall_g4_nowcast``
* ``2025_usa_cornerstone_v0_5``

Not rebuilt: pinned USEEIO baseline; historical v03 G2 / G3 / FINAL.

Pins are sheet ``N_new`` x canonical q (``waterfall_progression --sheet-n-new``).
Early G1* sheets are the 2026-10-02 remint on ``main`` in the v0.5 Diagnostics
Drive folder. US ladder sheets are the 2026-10-01 USEEIO cut on ``3a1dddc``.

Assessment plot bars are checked separately from sheets only -- see
``test_assessment_useeio_*``.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys

import pytest

from bedrock.utils.validation.waterfall_progression import (
    ASSESSMENT_USEEIO_BEDROCK_LEVELS,
    assert_useeio_track_sheet_ids_match_registry,
    assessment_useeio_bedrock_levels,
    live_waterfall_configs,
    sheet_n_new_levels,
)

# q-weighted sheet N_new (kgCO2e/USD). Source: --sheet-n-new with
# releases.v0_5_0 scaled_q_USA. Live 1^T B L matches these.
EXPECTED_LIVE_N_NEW = {
    'v03_waterfall_useeio_g1_schema_ghg': 0.3114421,
    'v03_waterfall_ceda_g1a_schema_ghg': 0.2532428,
    'v03_waterfall_ceda_g1b_waste_disagg': 0.2556327,
    'v05_waterfall_g2_methods': 0.2386031,
    'v05_waterfall_g3_data': 0.2395920,
    'v05_waterfall_g4_nowcast': 0.2208994,
    '2025_usa_cornerstone_v0_5': 0.2218014,
}

# Assessment USEEIO-track bars (diagnostics columns x canonical v0.5 q).
# Pin uses N_old_inflated on the G2 sheet; G2–FINAL use N_new (= live pins).
EXPECTED_ASSESSMENT_USEEIO_BEDROCK_N = {
    'pinned_useeio_baseline': 0.2486566,
    'G2': 0.2386031,
    'G3': 0.2395920,
    'G4': 0.2208994,
    'FINAL': 0.2218014,
}

ATOL_KG_PER_USD = 1e-4
_STEP_TIMEOUT_S = 3600


def _run_progression_cli(arg: str) -> float:
    proc = subprocess.run(
        [sys.executable, '-m', 'bedrock.utils.validation.waterfall_progression', arg],
        capture_output=True,
        text=True,
        timeout=_STEP_TIMEOUT_S,
        check=False,
    )
    assert proc.returncode == 0, (
        f'waterfall step {arg!r} failed (rc={proc.returncode}):\n'
        f'stdout tail: {proc.stdout[-2000:]}\nstderr tail: {proc.stderr[-2000:]}'
    )
    match = re.search(r'\{"weighted_avg_n_kg_per_usd":\s*([-0-9.eE]+)\}', proc.stdout)
    assert match, f'no JSON result on stdout for {arg!r}: {proc.stdout[-2000:]}'
    return float(json.loads(match.group(0))['weighted_avg_n_kg_per_usd'])


@pytest.mark.eeio_integration
def test_live_waterfall_config_set_matches_registries() -> None:
    configs = live_waterfall_configs()
    assert set(configs) == set(EXPECTED_LIVE_N_NEW)
    assert len(configs) == len(EXPECTED_LIVE_N_NEW)


@pytest.mark.eeio_integration
def test_sheet_n_new_pins_match_expected() -> None:
    """Diagnostics N_new × canonical q matches the live expected pins."""
    levels = sheet_n_new_levels()
    assert set(levels) == set(EXPECTED_LIVE_N_NEW)
    for config_name, expected in EXPECTED_LIVE_N_NEW.items():
        assert levels[config_name] == pytest.approx(
            expected, abs=ATOL_KG_PER_USD
        ), config_name


@pytest.mark.eeio_integration
@pytest.mark.parametrize('config_name', list(EXPECTED_LIVE_N_NEW))
def test_live_config_matches_sheet_n_new(config_name: str) -> None:
    """Full model rebuild: live 1ᵀBL @ canonical q == sheet N_new pin."""
    level = _run_progression_cli(config_name)
    assert level == pytest.approx(EXPECTED_LIVE_N_NEW[config_name], abs=ATOL_KG_PER_USD)


@pytest.mark.eeio_integration
def test_assessment_useeio_sheet_ids_match_registry() -> None:
    assert_useeio_track_sheet_ids_match_registry()
    pin, g2 = ASSESSMENT_USEEIO_BEDROCK_LEVELS[0], ASSESSMENT_USEEIO_BEDROCK_LEVELS[1]
    assert pin.sheet_id == g2.sheet_id
    assert pin.n_column == 'N_old_inflated'
    assert g2.n_column == 'N_new'
    assert g2.key == 'G2'


@pytest.mark.eeio_integration
def test_assessment_useeio_bedrock_weighted_n_from_sheets() -> None:
    """Sheet × canonical q reproduces assessment bedrock bars (no rebuild)."""
    levels = assessment_useeio_bedrock_levels()
    assert set(levels) == set(EXPECTED_ASSESSMENT_USEEIO_BEDROCK_N)
    for key, expected in EXPECTED_ASSESSMENT_USEEIO_BEDROCK_N.items():
        assert levels[key] == pytest.approx(expected, abs=ATOL_KG_PER_USD), key
