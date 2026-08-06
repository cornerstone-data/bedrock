"""Live reproducibility of v0.3 waterfall configs (except pinned baselines).

Each live case rebuilds ``derive_Aq_usa`` + ``derive_B_usa_non_finetuned`` for
one ``v03_waterfall_*`` config, q-weights ``1ᵀ B L`` with canonical
``scaled_q_USA``, and compares to the diagnostics sheet ``N_new`` pin.

Configs covered (union of USEEIO + CEDA group registries):

* ``v03_waterfall_useeio_g1_schema_ghg``
* ``v03_waterfall_ceda_g1a_schema_ghg``
* ``v03_waterfall_ceda_g1b_waste_disagg``
* ``v03_waterfall_g2_methods``
* ``v03_waterfall_g3_data``
* ``v03_waterfall_final``

Not rebuilt: pinned USEEIO baseline / pinned CEDA v0 baseline.

Pins recomputed 2026-08-05 via::

    uv run python -m bedrock.utils.validation.waterfall_progression --sheet-n-new

Assessment plot bars (``N_old_inflated`` / ``N_new_inflated``) are checked
separately from sheets only — see ``test_assessment_useeio_*``.
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

# q-weighted sheet N_new (kgCO2e/USD). Source: --sheet-n-new.
# Live 1ᵀBL matches these. USEEIO G1 (~0.314) is *not* the assessment G1 bar:
# that config builds B with deflate_x_to_detail_io_year_for_B, so N_new is in
# usa_detail_original_year dollars; the figure uses N_new_inflated (PI rebase
# to model_base_year 2024$). G2/G3/FINAL assessment bars use N_new and match.
EXPECTED_LIVE_N_NEW = {
    'v03_waterfall_useeio_g1_schema_ghg': 0.3135957,
    'v03_waterfall_ceda_g1a_schema_ghg': 0.2543301,
    'v03_waterfall_ceda_g1b_waste_disagg': 0.2563729,
    'v03_waterfall_g2_methods': 0.2398356,
    'v03_waterfall_g3_data': 0.2416736,
    'v03_waterfall_final': 0.2416736,
}

# Assessment USEEIO-track bars (ceda combine_ef columns × canonical q).
# Pin/G1 use inflated columns; G2/G3 use N_new (= live pins above).
EXPECTED_ASSESSMENT_USEEIO_BEDROCK_N = {
    'pinned_useeio_baseline': 0.2520542,
    'G1': 0.2462974,
    'G2': 0.2398356,
    'G3': 0.2416736,
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
    """Diagnostics N_new × canonical q still matches the live expected pins."""
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
    pin, g1 = ASSESSMENT_USEEIO_BEDROCK_LEVELS[0], ASSESSMENT_USEEIO_BEDROCK_LEVELS[1]
    assert pin.sheet_id == g1.sheet_id
    assert pin.n_column == 'N_old_inflated'
    assert g1.n_column == 'N_new_inflated'


@pytest.mark.eeio_integration
def test_assessment_useeio_bedrock_weighted_n_from_sheets() -> None:
    """Sheet × canonical q reproduces assessment bedrock bars (no rebuild)."""
    levels = assessment_useeio_bedrock_levels()
    assert set(levels) == set(EXPECTED_ASSESSMENT_USEEIO_BEDROCK_N)
    for key, expected in EXPECTED_ASSESSMENT_USEEIO_BEDROCK_N.items():
        assert levels[key] == pytest.approx(expected, abs=ATOL_KG_PER_USD), key
