# USEEIO Nowcast — EF Diagnostics Runbook

Step N4 of [`docs/implement_useeio_nowcast_plan.md`](docs/implement_useeio_nowcast_plan.md). Steps N1–N3 are code changes that landed in this branch; this step is operator-triggered.

## Pre-flight checklist

- [ ] All 7 useeio_nowcast parquets exist locally (sanity for the analyst path; the workflow re-derives via the new branch in CI):
  ```
  ls bedrock/analysis/a_matrix_time_series/output/results/A_useeio_nowcast_*.parquet
  ```
  Expected: one per year 2017–2023.

- [ ] Both YAMLs exist:
  - `bedrock/utils/config/configs/2025_usa_cornerstone_A_useeio_nowcast.yaml` — minimal config for the A-matrix-only analysis (Steps N1–N3).
  - `bedrock/utils/config/configs/2025_usa_cornerstone_full_model_A_useeio_nowcast.yaml` — **full v0.2 model** (`load_E_from_flowsa`, `new_ghg_method`, `use_E_data_year_for_x_in_B`, `implement_waste_disaggregation`, `load_useeio_nowcast_A_matrix`) — required for EF diagnostics so apples-to-apples vs the other 4 `full_model_A_*` YAMLs.

- [ ] The new flag is wired: `load_useeio_nowcast_A_matrix` in `bedrock/utils/config/usa_config.py`.

- [ ] The branch is wired: `if cfg.load_useeio_nowcast_A_matrix:` near the top of `derive_cornerstone_Aq_scaled()` in `bedrock/transform/eeio/derived_cornerstone.py`.

- [ ] PR is open and CI is green (especially `pytest bedrock/transform/__tests__/test_usa.py -k cornerstone`).

## Trigger the workflow

The `generate_diagnostics` workflow consumes the **full-model** YAML (NOT the minimal one — the minimal one is for A-matrix-only analysis) and produces EFs in a Google Sheet, automatically benchmarked against CEDA-US (v0) and (with one checkbox) the USEEIO Excel baseline.

For each `model_base_year ∈ {2018, 2019, 2020, 2021, 2022, 2023}`:

1. Open GitHub Actions → `generate_diagnostics` → **Run workflow**.
2. Set parameters:
   | Parameter | Value |
   |---|---|
   | **Branch** | the branch with this change |
   | **USA config name** | `2025_usa_cornerstone_full_model_A_useeio_nowcast` |
   | **Model base year override** | `2023` (or whichever target year) |
   | **Google Sheets ID** | a fresh sheet ID for this approach × year |
   | **Benchmark to USEEIO GCS Excel baseline** | ☑ **tick this** (gives the USEEIO-do-nothing comparison automatically) |
3. Click **Run workflow**.
4. The workflow:
   - Loads `2025_usa_cornerstone_full_model_A_useeio_nowcast.yaml` → sets `load_useeio_nowcast_A_matrix=True` (plus all v0.2 full-model flags).
   - Calls `derive_cornerstone_Aq_scaled()` → short-circuits to our new branch → calls `derive_useeio_nowcast_Aq_cornerstone(year=2023)`.
   - That function loads V/U/U_imports from `gs://cornerstone-default/extract/input-data/USEEIO_nowcasted_MUTs/`, derives Cornerstone A, applies 0.98 cap, returns `SingleRegionAqMatrixSet`.
   - Workflow runs full model end-to-end and writes EFs + sector outputs to the Sheet.

If the workflow runs but the USEEIO checkbox was forgotten, re-trigger — it's cheap. Track run URLs in `step6_run_index.csv` (matches the parent plan's audit convention).

### Waste disaggregation (now applied — apples-to-apples across approaches)

All 5 full-model A YAMLs set `implement_waste_disaggregation: True`, including `useeio_nowcast`. `derive_useeio_nowcast_Aq_cornerstone()` mirrors the other approaches' Cornerstone disagg path:

1. Map USEEIO's year-specific BEA-detail V/U to Cornerstone schema via correspondence multiplication (`industry_corresp() @ V @ commodity_corresp().T`, etc.).
2. Apply `apply_waste_disagg_to_V` and `apply_waste_disagg_to_U` from `bedrock.transform.eeio.waste_disaggregation` — the **same** helpers and **same** 2017 benchmark weights (`WasteDisaggregationDetail2017`) used by `_derive_cornerstone_Aq_from_disaggregated`.

Net: the BEA `562000` row/col is split into 7 Cornerstone children (`562111, 562HAZ, 562212, 562213, 562910, 562920, 562OTH`) using 2017 make/use weights identically to the other approaches. **Waste-sector EFs are comparable across all 6 approaches** with no asterisk.

The only methodological asymmetry that remains: the *2017 weights are applied to year-specific V/U*, which assumes the waste-make/use mix is stable across years (the same stability assumption the other approaches make implicitly).

## Compile cross-approach EF comparison

After all 6 approaches have completed runs (5 existing + `useeio_nowcast`):

1. Open each sheet, copy the "summary" tab.
2. Compile into `bedrock/analysis/a_matrix_time_series/output/results/step6_ef_comparison.xlsx`:
   - One tab per approach.
   - `summary_vs_useeio` tab — diff of every approach against USEEIO baseline EFs.
   - `summary_vs_ceda` tab — same vs CEDA-US.
3. Produce diagnostic figures:
   - `output/plots/step6_ef_divergence_scatter_vs_useeio.png` — EF values per approach against USEEIO, log-log scatter, color = approach.
   - `output/plots/step6_ef_divergence_scatter_vs_ceda.png` — same vs CEDA-US.
   - `output/plots/step6_ef_useeio_nowcast_vs_alternatives.png` — focused 1×3 hexbin: useeio_nowcast EFs vs (summary_tables, industry_PI, commodity_PI). This is the figure that answers "does any internal alternative match the externally-balanced nowcast more closely than the others?"

## Year-coverage caveat for plots

`useeio_nowcast` has no 2024 upstream data. Anywhere the plot iterates years and may try to load `A_useeio_nowcast_2024.parquet`:
- `derive_A_time_series.py` already filters via `_years_for(approach, TARGET_YEARS)` → only 2017–2023 for `useeio_nowcast`.
- `compare_approaches.py` `USEEIO_NOWCAST_PAIRS` is evaluated at `USEEIO_NOWCAST_TARGET_YEAR = 2023`.
- Step 6 EF workflow: only trigger for model_base_year ∈ {2018..2023}. Running 2024 will fail with `ValueError: USEEIO nowcast not available for 2024.` — that's intended.

## What to write up after the workflow runs

Answer in 1 paragraph each:
1. **Does any internal alternative track useeio_nowcast more closely than the others?** Compare `summary_tables`, `industry_price_index`, `commodity_price_index` against `useeio_nowcast` cell-by-cell (Step 3 `pairwise_hexbins_useeio_nowcast_*.png`) AND in EF space (Step 6 scatter).
2. **Are the persistent USEEIO reconciliation outliers (`5412OP`, `GSLG`, `81`, `722`, `23`) visible in the EF differences?** If so, flag in the README so downstream users know these sectors carry larger uncertainty in the nowcast.
3. **Methodological circularity caveat**: USEEIO's GRAS and bedrock's `summary_tables` both reconcile 2017 detail to BEA summary aggregates. Close agreement is partly a methodological tautology, not validation. Highlight this in the recommendation section.

## Caveats baked into the upstream nowcast (carry these forward to the EF write-up)

From [`USEEIO_nowcasting.md`](../../../USEEIO_nowcasting.md):
- Within-summary detail-level technology is frozen at 2017 (the dominant limitation).
- Real input substitution is not captured (constant-volume recipe assumption).
- Margin rates are held fixed at 2017 PCE Bridge values.
- Most FD columns are 2017-shape + RAS rebalance — only 12 curated columns have fresh NIPA-driven estimates.
- VA structural change uses a single commodity-weighted average Rho across all VA rows.

These limits mean `useeio_nowcast` and `summary_tables` give similar A-matrix updates by design — both freeze the same things. The interesting divergences are between `useeio_nowcast` and the price-index methods.
