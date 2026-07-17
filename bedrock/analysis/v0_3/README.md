# v0.3 release diagnostics

Plot and dispatch scripts for the v0.3 release EF diagnostics
progression. They read or dispatch Google Sheets produced by
`bedrock.utils.validation.generate_diagnostics` and reuse shared loaders in
`bedrock.utils.validation.analysis` (`fetch`, `plotting`, `bly_plots`, etc.).

The sheet registries (`release_v0_3_progression.py`,
`release_v0_v03_useeio_groups.py`, `release_v0_v03_ceda_groups.py`) live in
`bedrock.utils.validation.analysis` so combine combos stay self-contained.

Plot outputs land in `output/release_v0_3/` or `output/release_v0_v03_groups/`
(gitignored).

## Scripts

| Script | Purpose |
|--------|---------|
| `export_q_v0_3_cornerstone.py` | Dispatch USA `scaled_q_USA` (commodity gross output) to a Google Sheet for ceda v0.3 weighted-N waterfalls. |
| `plot_ef_release_v0_3.py` | Release progression histograms, FINAL v0.2 vs v0.3 overlays, and BLy charts from registered sheets. |
| `plot_ef_v0_v03_useeio_groups.py` | Stacked G1→G2→G3 wholesale USEEIO progression (sequential + cumulative-vs-pin panels, FINAL overlays, producer). FINAL N overlay flags pinned key sectors. |
| `plot_ef_v0_v03_ceda_groups.py` | Stacked G1a→G1b→G2→G3 wholesale CEDA progression (sequential + cumulative-vs-v0 panels, FINAL overlays, producer). FINAL N overlay flags pinned key sectors. |
| `dispatch_ef_release_v0_3.py` | Dispatch v0.3 release steps (MECS through FINAL) to the EF time-series Drive folder; appends to `output/release_v0_3/ef_run_index_release_v0_3.csv`. |
| `dispatch_ef_v03_waterfall.py` | Dispatch `v03_waterfall_*` group endpoints (USEEIO or CEDA baseline) to the v03 waterfall Drive folder; appends to a baseline-specific run index CSV. |

## Export USA q (for ceda weighted-N)

```powershell
# Default: create/update Google Sheet ``q_v0_3_cornerstone`` in the v0.3
# waterfall Drive folder (tabs: q, README, optional extra_codes).
uv run python -m bedrock.analysis.v0_3.export_q_v0_3_cornerstone

# Override Drive folder or sheet title
uv run python -m bedrock.analysis.v0_3.export_q_v0_3_cornerstone `
    --folder-id <DRIVE_FOLDER_ID> --sheet-title q_v0_3_cornerstone
```

Uses the current snapshot key (`.SNAPSHOT_KEY`) unless `--snapshot-key` is set.
Default Drive folder: `V03_WATERFALL_DRIVE_FOLDER_ID` in
`bedrock.analysis.v0_3.constants`.

## Plot

```powershell
uv run python -m bedrock.analysis.v0_3.plot_ef_release_v0_3
uv run python -m bedrock.analysis.v0_3.plot_ef_release_v0_3 --compare-to v0.2
```

Pass `--skip-progression`, `--skip-overlay`, or `--skip-bly` to omit figure groups.
`--compare-to v0` (default) uses each sheet's in-tab diff vs CEDA v0 / USEEIO.
`--compare-to v0.2` recomputes each v0.3 step vs FINAL v0.2 (2024 steps get
`[+1yr infl]` on the panel title where applicable).

Wholesale v0→v0.3 USEEIO groups (stacked G1→G2→G3 marginals, IO@2024 producer):

```powershell
uv run python -m bedrock.analysis.v0_3.plot_ef_v0_v03_useeio_groups
```

Wholesale v0→v0.3 CEDA groups (stacked G1a→G1b→G2→G3 marginals, IO@2024 producer):

```powershell
uv run python -m bedrock.analysis.v0_3.plot_ef_v0_v03_ceda_groups
```

Dispatch group sheets with `dispatch_ef_v03_waterfall`, then paste sheet IDs from
the baseline-specific run index into `release_v0_v03_useeio_groups.py` or
`release_v0_v03_ceda_groups.py` before combine/plot.

## Dispatch

```powershell
uv run python -m bedrock.analysis.v0_3.dispatch_ef_release_v0_3 --dry-run
uv run python -m bedrock.analysis.v0_3.dispatch_ef_release_v0_3 `
    --only-configs 2025_usa_cornerstone_full_model_v0_3_ghgi_mecs

uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --dry-run
uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall `
    --only-configs v03_waterfall_useeio_g1_schema_ghg

uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --baseline ceda --dry-run
uv run python -m bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --baseline ceda `
    --only-configs v03_waterfall_ceda_g1a_schema_ghg
```

Run indexes: `ef_run_index_v03_waterfall.csv` (USEEIO), `ef_run_index_v03_waterfall_ceda.csv` (CEDA).

## Combine

Merged workbooks use combos in `bedrock.utils.validation.analysis.combinations`
(`v0.2_to_v0.3_ceda`, `v0.2_to_v0.3_useeio`, `v0_to_v03_useeio_groups`,
`v0_to_v03_ceda_groups`). Example filenames end with the baseline tag (`_ceda`,
`_useeio`) so both v0.2→v0.3 workbooks can sit in one folder:

```powershell
uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics `
    --combo v0.2_to_v0.3_ceda `
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_3/ef_diagnostics_merged_v0_2_to_v0_3_ceda.xlsx

uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics `
    --combo v0.2_to_v0.3_useeio `
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_3/ef_diagnostics_merged_v0_2_to_v0_3_useeio.xlsx

uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics `
    --combo v0_to_v03_useeio_groups `
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_v03_groups/ef_diagnostics_merged_v0_to_v03_useeio_groups_useeio.xlsx

uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics `
    --combo v0_to_v03_ceda_groups `
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_v03_groups/ef_diagnostics_merged_v0_to_v03_ceda_groups_ceda.xlsx
```

Re-run with `--refresh` after sheet tab content changes.

Flag comparison for waterfall configs and baselines:
`bedrock/utils/validation/analysis/v03_waterfall_config_comparison.md`.
