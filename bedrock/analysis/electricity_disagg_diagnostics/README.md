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

## EF diagnostics plots vs v0.2 footing

Compares **each electricity step** (reallocation, 3-way split, mixed units) to the
**Cornerstone v0.2** workbook’s absolute `N_new` / `D_new` — not to the previous
step, and not via each sheet’s in-tab CEDA `%` columns.

```bash
uv run python -m bedrock.analysis.electricity_disagg_diagnostics.plot_ef \
  --local-dir bedrock/analysis/electricity_disagg_diagnostics/local_data
```

Outputs under `output/ef/`:

| Path | Contents |
|---|---|
| `output/ef/electricity_reallocation/` | Suite PNGs for that step vs v0.2 |
| `output/ef/electricity_disaggregation/` | Suite PNGs for 3-way vs v0.2 |
| `output/ef/electricity_mixed_units/` | Suite PNGs for mixed units vs v0.2 |
| `output/ef/panel/ef_panels_vs_v0_2_N.png` | 3-panel N % hist (realloc / 3-way / mixed) |
| `output/ef/panel/ef_panels_vs_v0_2_D.png` | 3-panel D % hist |

Per-step suite includes `ef_perc_diff_histogram.png`, `ef_pct_change_vs_abs_change.png`,
`ef_abs_change_histogram.png`, `ef_n_perc_diff_histogram.png`, `ef_pct_change_vs_ef_size.png`.

When any of `221100` / `221110` / `221121` / `221122` are dropped from a figure
(e.g. mixed-units `221110` kg/MWh vs kg/USD, or children missing from v0.2), the
figure footnote lists the sector code and reason.

`plot_ef` seeds the cache with `REQUIRED_TABS + EF_TABS` (`N_and_diffs`,
`D_and_diffs`, `D_and_N_significant_sectors`). BLy-only import via `run_all` /
`import_local` still uses `REQUIRED_TABS` only.

## Metrics

- **Dispersion** charts: each step bar = `Σ_sector |ΔBLy|` (gross cross-sector reallocation).
- **Net change** charts: level bars = `Σ BLy_new` at each step; a **"BLy change due to …"** bar appears only when total U.S. BLy changes between consecutive steps (signed net, after alignment).
