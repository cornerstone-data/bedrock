"""Waterfall-progression regression: incremental bucket flags reproduce the release steps.

The v0→v0.3 release waterfall's bedrock-side steps are, in bucket-flag
vocabulary (the ``v03_waterfall_*`` configs):

    v0 snapshot baseline
      → + use_cornerstone_ghg_model            (G1a: "GHG model allocation")
      → + implement_waste_disaggregation       (G1b: "Waste disagg.")
      → + apply_io_year_adjustments + margins  (G2:  "IO year adjustments")
      → usa_ghg_data_year: 2023 → 2024         (G3:  "US data update")

Each test derives one step's total attributed emissions ΣBLy = Σ diag(d)·L·y
in a fresh subprocess (so ``functools.cache`` state cannot leak between
configs) and compares against the totals of the ``BLy_new_vs_BLy_old`` tabs
of the pinned diagnostics sheets in
``bedrock.utils.validation.analysis.release_v0_v03_ceda_groups`` — the same
sheets the v0.3 assessment waterfall figure was built from.

Note on the published figure: its bars (baseline 5,069; −42/+16/−254/+22)
are the USA *portion of the global MRIO* after ingesting these bedrock
snapshots, so they differ from the bedrock-standalone totals asserted here.
The bedrock-side ground truth is the pinned sheets; the MRIO ingestion is
validated in the ceda repository.

If the pre-built ``GHG_national_Cornerstone_{year}`` FBS parquets are
re-uploaded with new vintages, these totals move by design — re-pin the
expected values consciously, exactly like a snapshot bump.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys

import pytest

# ΣBLy (MtCO2e) — totals of the pinned sheets' ``BLy_new (MtCO2e)`` column
# (``BLy_old`` for the baseline). Sheets dispatched 2026-07-13 from the
# v03_waterfall configs; see release_v0_v03_ceda_groups for sheet IDs.
EXPECTED_BLY_TOTAL_MT = {
    'v0_baseline': 4918.4928,
    'v03_waterfall_ceda_g1a_schema_ghg': 5708.6328,
    'v03_waterfall_ceda_g1b_waste_disagg': 4843.6614,
    'v03_waterfall_g2_methods': 4643.2885,
    'v03_waterfall_g3_data': 4655.0986,
    # FINAL is the full v0.3 methodology; it must telescope to the last step.
    'v03_waterfall_final': 4655.0986,
}

# Absolute tolerance in MtCO2e. The baseline reproduces the sheet total to
# <0.001 Mt; 0.5 Mt headroom absorbs float/order noise without masking any
# change big enough to move a waterfall bar.
ATOL_MT = 0.5

_STEP_TIMEOUT_S = 3600


def _bly_total_mt(arg: str) -> float:
    """Run the ΣBLy computation for one step in a fresh interpreter."""
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
    match = re.search(r'\{"bly_total_mt":\s*([-0-9.eE]+)\}', proc.stdout)
    assert match, f'no JSON result on stdout for {arg!r}: {proc.stdout[-2000:]}'
    return float(json.loads(match.group(0))['bly_total_mt'])


@pytest.mark.eeio_integration
def test_v0_baseline_bly_matches_pinned_sheets() -> None:
    total = _bly_total_mt('--v0-baseline')
    assert total == pytest.approx(EXPECTED_BLY_TOTAL_MT['v0_baseline'], abs=ATOL_MT)


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
def test_waterfall_step_bly_matches_pinned_sheets(config_name: str) -> None:
    total = _bly_total_mt(config_name)
    assert total == pytest.approx(EXPECTED_BLY_TOTAL_MT[config_name], abs=ATOL_MT)
