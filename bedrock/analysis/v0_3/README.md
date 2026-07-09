# v0.3 release diagnostics

Plot and dispatch scripts for the v0.3 release-deck EF diagnostics
progression. They read or dispatch Google Sheets produced by
`bedrock.utils.validation.generate_diagnostics` and reuse shared loaders in
`bedrock.utils.validation.analysis` (`fetch`, `plotting`, `bly_plots`, etc.).

The sheet registry (`release_v0_3_progression.py`) lives in
`bedrock.utils.validation.analysis` so combine combos stay self-contained.

Plot, dispatch, and merged workbooks all land in `output/release_v0_3/`
(gitignored).

## Scripts

| Script | Purpose |
|--------|---------|
| `plot_ef_release_v0_3.py` | Deck-style progression histograms, FINAL v0.2 vs v0.3 overlays, and BLy charts from registered sheets. |
| `dispatch_ef_release_v0_3.py` | Dispatch v0.3 release steps (MECS through FINAL) to the EF time-series Drive folder; appends to `output/release_v0_3/ef_run_index_release_v0_3.csv`. |

## Plot

```powershell
uv run python -m bedrock.analysis.v0_3.plot_ef_release_v0_3
```

Pass `--skip-progression`, `--skip-overlay`, or `--skip-bly` to omit figure groups.

## Dispatch

```powershell
uv run python -m bedrock.analysis.v0_3.dispatch_ef_release_v0_3 --dry-run
uv run python -m bedrock.analysis.v0_3.dispatch_ef_release_v0_3 `
    --only-configs 2025_usa_cornerstone_full_model_v0_3_ghgi_mecs
```

## Combine

Merged workbooks use combos in `bedrock.utils.validation.analysis.combinations`
(`v0.2_to_v0.3_ceda`, `v0.2_to_v0.3_useeio`), which read sheet IDs from
`release_v0_3_progression.py` in that package:

```powershell
uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics `
    --combo v0.2_to_v0.3_ceda `
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_3/ef_diagnostics_merged_v0_2_to_v0_3_ceda.xlsx

uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics `
    --combo v0.2_to_v0.3_useeio `
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_3/ef_diagnostics_merged_v0_2_to_v0_3_useeio.xlsx
```

Re-run with `--refresh` after sheet tab content changes.
