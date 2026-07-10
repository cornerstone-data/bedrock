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

### Option A — local Excel exports (no Google API)

If Application Default Credentials cannot read Cornerstone Sheets, download each diagnostics
workbook from Drive (**File → Download → Microsoft Excel (.xlsx)**) and save under
[`local_data/`](local_data/) using the config stem as the filename:

| File name | Config |
|---|---|
| `2025_usa_cornerstone_v0_2.xlsx` | footing |
| `2025_usa_cornerstone_v0_2_electricity_reallocation.xlsx` | step 1 |
| `2025_usa_cornerstone_v0_2_electricity_disaggregation.xlsx` | step 2 |
| `2025_usa_cornerstone_v0_2_electricity_mixed_units.xlsx` | step 3 + FINAL |

Then run (imports workbooks into the parquet cache, then plots):

```bash
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.run_all --local-dir bedrock/analysis/electricity_disagg_diagnostics/local_data
```

Or import and plot in two steps:

```bash
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.import_local
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.run_all
```

### Option B — live Google Sheets

```bash
# from repo root, using .venv
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.refresh_cache
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.run_all
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.run_all --refresh
```

Outputs (gitignored):

- `output/electricity_bly_dispersion_waterfall_mmt.png`
- `output/electricity_bly_dispersion_waterfall_pct.png`
- `output/electricity_bly_net_change_waterfall_mmt.png`
- `output/electricity_bly_net_change_waterfall_pct.png`

## Metrics

- **Dispersion** charts: each step bar = `Σ_sector |ΔBLy|` (gross cross-sector reallocation).
- **Net change** charts: level bars = `Σ BLy_new` at each step; a **"BLy change due to …"** bar appears only when total U.S. BLy changes between consecutive steps (signed net, after alignment).
