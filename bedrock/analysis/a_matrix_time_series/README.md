# `a_matrix_time_series` — A-matrix methodology comparison

This module compares **six ways of deriving the Cornerstone A matrix** for years 2017–2024 and feeds the cell-level + EF-level diagnostics that back the v0.3 recommendation for which method ships in the 2026 model. Plain-English glossary of the six approaches lives in [`docs/analysis_plan.md`](docs/analysis_plan.md); the canonical list (with stable string keys used everywhere in this folder) is in [`constants.py`](constants.py).

| Category | Approach key | Role |
|---|---|---|
| Baseline (do-nothing) | `useeio` | 2017 A used as-is |
| Baseline (production) | `ceda_default` | Current CEDA-US two-step scale + inflate |
| Internal alternative | `summary_tables` | Scale 2017 → target via summary-A ratios |
| Internal alternative | `industry_price_index` | Industry PI applied to 2017 A |
| Internal alternative | `commodity_price_index` | V-norm-derived commodity PI applied to 2017 A |
| External reference | `useeio_nowcast` | EPA USEEIO team's GRAS-balanced detail SUTs (2017–2023) |

`FOCUS_APPROACHES` in [`constants.py`](constants.py) is the 3-approach subset the v0.3 plots zoom in on: `commodity_price_index`, `summary_tables`, `useeio_nowcast`. `industry_price_index` is kept in the data layer but omitted from the focus plots (superseded by `commodity_price_index`).

---

## Where things live

```
a_matrix_time_series/
├── README.md                         # this file — live navigation
├── useeio_nowcast_ef_runbook.md      # live operator runbook (USEEIO-nowcast EF run)
├── docs/
│   ├── analysis_plan.md              # historical: epic spec (5 approaches, DoD, key questions)
│   └── implement_useeio_nowcast_plan.md  # historical: integration of the 6th approach
│
├── constants.py                      # paths, approach order, colors, year coverage
├── _loaders.py                       # load (Adom, Aimp) parquet pairs by (approach, year)
├── _run_report.py                    # publish result tabs to a Google Sheets run-report
├── __init__.py                       # pins update_inflation_factors=True + apply_inflation_to_V=True
│
├── derive_A_time_series.py           # Step 1: cache A_{approach}_{year}.parquet
├── derive_useeio_nowcast_A.py        # Step 1 (external): cache A_useeio_nowcast_{year}.parquet
├── derive_A_cells_long.py            # Step 2: A_cells_long.parquet + scatter/divergence
├── derive_A_cells_stability.py       # Step 2.5: Jaccard + persistence (cell-set stability)
│
├── compare_approaches.py             # Step 3: pairwise hexbins at the latest year
├── compare_key_sectors.py            # Step 4: impact-weighted top cells / heatmap
├── compare_summary_a_errors.py       # Step 5: weighted RMSE vs published BEA summary A
├── compare_price_ratios.py           # Pre-flight: industry-PI vs commodity-PI sanity
├── compare_method_stability.py       # Step 7d: YoY N stability from compiled EF sheets
│
├── dispatch_ef_time_series.py        # Step 7a: trigger generate_diagnostics workflow runs
├── compile_ef_diagnostics.py         # Step 7b: aggregate per-run Sheets → workbook + parquet
├── plot_ef_diagnostics.py            # Step 7c: EF scatter + histogram from compiled parquet
├── plot_v0_3_n_pct_hist.py           # Ad-hoc: single-sheet N/D histogram
│
├── recover_ef_run_index.py           # Utility: reconstruct ef_run_index.csv from Drive
├── view_vnorm_and_price_ratios.py    # Utility: print V-norm + industry-vs-commodity PI summaries
│
└── output/
    ├── results/   # parquets, CSVs, last_run_sheet_id.txt, ef_run_index.csv
    └── plots/     # PNGs — published artifacts
```

The file order above mirrors the execution DAG (data → cross-approach views → EF orchestration → utilities). Within each block, scripts share a prefix (`derive_*` for data producers, `compare_*` for cross-approach views, `dispatch_*` / `compile_*` / `plot_*` for the async EF pipeline) so `ls | sort` clusters them by role.

All driver scripts share the `constants.py` paths and color palette, and the `__init__.py` toggles two config flags that **every** script in this folder assumes: `update_inflation_factors=True` (BEA-derived industry PI path) and `apply_inflation_to_V=True` (V inflated to `model_base_year` for V-norm computations). Scripts that swap the global config mid-run (currently only `derive_A_time_series.py`) re-set those flags after each swap.

---

## How to run things — execution DAG

```
                          (optional pre-flight)
                          compare_price_ratios.py
                                    │
        ┌───────────────────────────┴───────────────────────────┐
        ▼                                                       ▼
derive_A_time_series.py                              derive_useeio_nowcast_A.py
        │                                                       │
        └──────────────────────────┬────────────────────────────┘
                                   ▼
                     A_{approach}_{year}.parquet
                                   │
        ┌────────────────┬──────────────────┬──────────────┬─────────────┐
        ▼                ▼                  ▼              ▼             ▼
 derive_A_cells_  derive_A_cells_     compare_      compare_       compare_
    long.py        stability.py      approaches.py  key_sectors.py  summary_a_errors.py
        │
        └──► A_cells_long.parquet (used by stability + several plots)

                                   │  (Step 6 / 7 — async, via GH Actions)
                                   ▼
                      dispatch_ef_time_series.py
                                   │
                          generate_diagnostics
                                   │
                                   ▼
                       compile_ef_diagnostics.py
                                   │
                                   ▼
                       ef_scatter_coords.parquet
                                   │
                  ┌────────────────┴────────────────┐
                  ▼                                 ▼
        plot_ef_diagnostics.py            compare_method_stability.py
```

### 1. (Optional) Pre-flight sanity

```bash
python -m bedrock.analysis.a_matrix_time_series.compare_price_ratios
```

Confirms industry-PI and commodity-PI distributions differ enough that they're worth studying separately. Skip if you've run it before — outputs are committed.

### 2. Cache A matrices for every (approach × year)

```bash
# Internal approaches — loops 5 YAMLs × {2017…2024} and writes parquets.
python -m bedrock.analysis.a_matrix_time_series.derive_A_time_series

# External reference — depends on the USEEIO nowcast extract module.
python -m bedrock.analysis.a_matrix_time_series.derive_useeio_nowcast_A
```

Outputs land in `output/results/A_{approach}_{year}.parquet`. The first script also creates a run-report Sheet in the [Drive folder](https://drive.google.com/drive/folders/1UcPmwLnL6MwTq9pMYJw5d43FJQOFQVO_) and writes `last_run_sheet_id.txt` so downstream scripts can append tabs to the same Sheet.

`useeio_nowcast` covers 2017–2023 only — `APPROACH_YEAR_COVERAGE` in `constants.py` is the source of truth and every script that mixes it with other approaches filters via that table.

### 3. Cell-level diagnostics

```bash
python -m bedrock.analysis.a_matrix_time_series.derive_A_cells_long
python -m bedrock.analysis.a_matrix_time_series.derive_A_cells_stability
```

`derive_A_cells_long.py` produces the tall `A_cells_long.parquet` used by several downstream scripts plus baseline-reference and divergence-share plots. `derive_A_cells_stability.py` adds Jaccard + persistence diagnostics on top of that parquet.

### 4. Cross-approach + key-sector + summary-A diagnostics

```bash
python -m bedrock.analysis.a_matrix_time_series.compare_approaches
python -m bedrock.analysis.a_matrix_time_series.compare_key_sectors
python -m bedrock.analysis.a_matrix_time_series.compare_summary_a_errors
```

These three are independent (all read `output/results/*.parquet`); run in any order.

### 5. EF diagnostics — Step 6 / 7 (async via GH Actions)

This phase fans out to the `generate_diagnostics` GitHub Actions workflow; one Sheet per `(scenario, approach, year)` cell. The runs for the v0.3 evaluation have already been dispatched and live in Drive folder [`1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s`](https://drive.google.com/drive/folders/1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s). See [`useeio_nowcast_ef_runbook.md`](useeio_nowcast_ef_runbook.md) for the operator checklist.

For a methodology flag outside this A-matrix scenario matrix, use
[`bedrock/utils/validation/evaluate_feature_impact.md`](../../utils/validation/evaluate_feature_impact.md)
and `python -m bedrock.utils.validation.dispatch_diagnostics` instead of the
scenario dispatcher below. Shared create-sheet / trigger helpers live in
`bedrock.utils.validation.dispatch_diagnostics`; this module owns the
time-series scenario matrix and `EF_TIME_SERIES_DRIVE_FOLDER_ID`.

**Reviewer path — skip dispatch, use the existing runs:**

```bash
# 5a-review. Pull the run index from Drive — required if you don't already
#            have output/results/ef_run_index.csv locally. Needs Google
#            application-default-credentials.
python -m bedrock.analysis.a_matrix_time_series.recover_ef_run_index \
    --folder-id 1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s

# 5b-review. Compile — reads ef_run_index.csv, pulls each Sheet's EF diff
#            tab, writes ef_scatter_coords.parquet.
python -m bedrock.analysis.a_matrix_time_series.compile_ef_diagnostics

# 5c-review. Plot.
python -m bedrock.analysis.a_matrix_time_series.plot_ef_diagnostics
python -m bedrock.analysis.a_matrix_time_series.compare_method_stability
```

**Engineer path — triggering new runs** (requires GitHub Actions `workflow:write` and the `gh` CLI authenticated):

```bash
# Dispatch is idempotent — skips cells already in ef_run_index.csv.
python -m bedrock.analysis.a_matrix_time_series.dispatch_ef_time_series \
    --git-ref main \
    --scenarios isolate_a_matrix,bundle_v0_3 \
    --years 2019,2020,2021,2022,2023

# Then wait for GH Actions to finish (each run takes ~30–60 min), then
# proceed to compile + plot as in the reviewer path above.
```

### Ad-hoc

`plot_v0_3_n_pct_hist.py` renders a single-sheet N/D histogram for any diagnostics Sheet ID — useful when staring at one approach's results without running the full compile pipeline.

---

## Conventions

- **Approach keys are strings**, not enums. `APPROACH_ORDER` in `constants.py` is the canonical ordering for any plot grid or legend.
- **Two-baseline reporting is non-negotiable**: every comparison in this folder reports against both `useeio` (do-nothing invariant) and `ceda_default` (production status quo). See [`docs/analysis_plan.md` § Comparison baselines](docs/analysis_plan.md#comparison-baselines-apply-to-every-comparison-in-this-plan) for the rationale.
- **Year coverage gaps live in `APPROACH_YEAR_COVERAGE`** — scripts that mix `useeio_nowcast` with other approaches filter on it (`useeio_nowcast` has no 2024 data upstream).
- **`output/results/` holds data + bookkeeping**, `output/plots/` holds PNGs. The Sheet-tab side-channel is opt-in: scripts publish via `_run_report.py`, which silently no-ops when the auth/Sheet isn't configured.
- **All scripts use snake_case filenames** and one of the prefixes above. Utilities that don't fit a pipeline prefix (`recover_*`, `view_*`) are grouped at the bottom of the listing.

---

## Related reading

- [`../../utils/config/feature_flag.md`](../../utils/config/feature_flag.md) /
  [`../../utils/validation/evaluate_feature_impact.md`](../../utils/validation/evaluate_feature_impact.md)
  — `USAConfig` flag and diagnostics playbooks.
- [`useeio_nowcast_ef_runbook.md`](useeio_nowcast_ef_runbook.md) — **live** operator runbook for running the USEEIO-nowcast EF diagnostics via GH Actions.
- [`docs/analysis_plan.md`](docs/analysis_plan.md) — **historical** epic spec: 5-approach taxonomy, Definition of Done, Checkpoints A–D, six Key Questions.
- [`docs/implement_useeio_nowcast_plan.md`](docs/implement_useeio_nowcast_plan.md) — **historical** integration plan for the 6th (external) approach (Steps N1–N4).
- [`bedrock/transform/eeio/derived_cornerstone.py`](../../transform/eeio/derived_cornerstone.py) — `derive_cornerstone_Aq_scaled()` is the gated entry point that the five internal YAMLs select between.
- [`bedrock/analysis/time_series_B_matrix/`](../time_series_B_matrix) — sibling module the time-series caching pattern was modeled on.
