# Electricity disaggregation BLy dispersion waterfall

Chained incremental BLy dispersion charts for PR2 (reallocation) → PR3 (3-way split) → PR4 (mixed units), vs Cornerstone v0.2 footing.

## Prerequisites

1. **Configs** (in repo):
   - `2025_usa_cornerstone_v0_2` — footing
   - `2025_usa_cornerstone_v0_2_electricity_reallocation`
   - `2025_usa_cornerstone_v0_2_electricity_disaggregation`
   - `2025_usa_cornerstone_v0_2_electricity_mixed_units` — FINAL

2. **Diagnostics runs** — trigger [`.github/workflows/generate_diagnostics.yml`](../../.github/workflows/generate_diagnostics.yml) four times (`use_useeio_baseline` unchecked), one per config above.

3. **Manifest** — replace placeholder `sheet_id` values in [`manifest.yaml`](manifest.yaml).

## Usage

```bash
# from repo root, using .venv
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.refresh_cache
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.run_all
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.run_all --refresh
```

Outputs (gitignored):

- `output/electricity_bly_dispersion_waterfall_mmt.png`
- `output/electricity_bly_dispersion_waterfall_pct.png`

## Metrics

- Each step bar: `Σ_sector |ΔBLy|` for one chained transition (after sum-preserving `221100`↔children alignment).
- Combined (FINAL): footing → mixed-units config.
- Offsetting bar: shown when `sum(steps) - combined > 1e-4` MMT.
- % chart: normalized to footing `BLy_new` total (Cornerstone v0.2), not CEDA v0 `BLy_old`.
