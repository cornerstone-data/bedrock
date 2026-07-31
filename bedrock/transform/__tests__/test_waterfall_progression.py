"""Waterfall-progression regression: incremental bucket flags reproduce the release steps.

The v0→v0.3 release waterfall's bedrock-side steps are, in bucket-flag
vocabulary (the ``v03_waterfall_*`` configs):

    v0 snapshot baseline
      → + use_cornerstone_ghg_model            (G1a: "GHG model allocation")
      → + implement_waste_disaggregation       (G1b: "Waste disagg.")
      → + apply_io_year_adjustments + margins  (G2:  "IO year adjustments")
      → usa_ghg_data_year: 2023 → 2024         (G3:  "US data update")

Each test derives one step's q-weighted average total emission factor

    wavg N = Σᵢ Nᵢ qᵢ / Σᵢ qᵢ,   N = 1ᵀ B L

in a fresh subprocess (so ``functools.cache`` state cannot leak between
configs), each step weighted by its own run's gross-output vector q — the
release waterfall convention, where every bar is a true quantity of that
state.

The per-sector N vectors are grounded in the ``N_and_diffs`` tabs of the
pinned diagnostics sheets in
``bedrock.utils.validation.analysis.release_v0_v03_ceda_groups`` — the
sheets the v0.3 assessment was built from. The expected levels below were
pinned from a run whose per-sector N matched those sheets, with the same
weighted average recomputed from the sheet N columns agreeing to well
within the tolerance.

If the pre-built ``GHG_national_Cornerstone_{year}`` FBS parquets are
re-uploaded with new vintages, these levels move by design — re-pin the
expected values consciously, exactly like a snapshot bump.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys

import pytest

# q-weighted average N (kgCO2e per USD gross output, model_base_year dollars).
# Pinned 2026-07-31 from live derivations cross-checked per sector against the
# pinned sheets' ``N_new`` columns; see release_v0_v03_ceda_groups for sheet IDs.
EXPECTED_WEIGHTED_AVG_N = {
    'v0_baseline': 0.2563185,
    'v03_waterfall_ceda_g1a_schema_ghg': 0.2655134,
    'v03_waterfall_ceda_g1b_waste_disagg': 0.2543695,
    'v03_waterfall_g2_methods': 0.2398356,
    'v03_waterfall_g3_data': 0.2416736,
    # FINAL is the full v0.3 methodology; it must telescope to the last step.
    'v03_waterfall_final': 0.2416736,
}

# Absolute tolerance in kgCO2e/USD. Steps reproduce the pinned levels to
# <1e-6; 1e-4 headroom absorbs float/order noise without masking any change
# big enough to move a waterfall bar (~1e-2 between steps).
ATOL_KG_PER_USD = 1e-4

_STEP_TIMEOUT_S = 3600


def _weighted_avg_n(arg: str) -> float:
    """Run the weighted-average-N computation for one step in a fresh interpreter."""
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
def test_v0_baseline_weighted_avg_n() -> None:
    level = _weighted_avg_n('--v0-baseline')
    assert level == pytest.approx(
        EXPECTED_WEIGHTED_AVG_N['v0_baseline'], abs=ATOL_KG_PER_USD
    )


@pytest.mark.eeio_integration
@pytest.mark.parametrize(
    'config_name',
    [
        'v03_waterfall_ceda_g1a_schema_ghg',
        'v03_waterfall_ceda_g1b_waste_disagg',
        'v03_waterfall_g2_methods',
        'v03_waterfall_g3_data',
        'v03_waterfall_final',
    ],
)
def test_waterfall_step_weighted_avg_n(config_name: str) -> None:
    level = _weighted_avg_n(config_name)
    assert level == pytest.approx(
        EXPECTED_WEIGHTED_AVG_N[config_name], abs=ATOL_KG_PER_USD
    )
