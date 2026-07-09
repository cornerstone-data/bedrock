# validation/analysis

Post-run analysis and plotting for diagnostics produced by
`bedrock/utils/validation/generate_diagnostics.py`.

The producer side writes results (N, D, significant sectors, etc.) to a
Google Sheet. This package is the consumer side: it loads those tabs
back, caches them locally as parquet, and renders analysis figures.

## Layout

- `plotting.py` — shared matplotlib primitives (percent histogram, absolute-change
  histogram, reference lines, styled text boxes).
- `fetch.py` — `load_tab` / `load_tabs` read Sheet tabs via
  `bedrock.utils.io.gcp.read_sheet_tab`, coerce numerics, and cache as parquet
  under `.cache/<sheet_id>/<tab>.parquet`.
- `_cli.py` — `common_options` (sheet id, refresh, tag, out dir) and
  output-dir resolution helpers.
- `bly_plots.py` — BLy sector stacked-bar data prep + `bly_plot_options` CLI decorator.
- `diagnostics_plots.py` — umbrella entry point. Produces five EF PNGs every run,
  and a sixth (`bly_sector_stacked_net_change.png`) when `BLy_new_vs_BLy_old` is
  present. If that tab is missing or cannot be read, the command still succeeds
  and only the BLy figure is omitted (five PNGs total).
  - `ef_perc_diff_histogram.png` — 2×2 N/D percent-diff distributions
    (all sectors + significant sectors)
  - `ef_n_perc_diff_histogram.png` — standalone N percent-diff distribution
    (all sectors)
  - `ef_pct_change_vs_abs_change.png` — |% change| vs |absolute change|
  - `ef_pct_change_vs_ef_size.png` — |% change| vs old EF size
  - `ef_abs_change_histogram.png` — distribution of absolute EF changes
  - `bly_sector_stacked_net_change.png` — per-sector BLy new vs old (stacked net change)
- `combine_ef_diagnostics.py` — aggregator over multiple diagnostics runs.
  Reads diagnostics Sheets directly from a Drive folder (via
  `bedrock.utils.io.gcp.list_drive_folder` + `fetch.load_tab`), merges them
  into the local workbook `analysis/output/<combo>/ef_diagnostics_merged.xlsx`
  (always written) plus an output Google Sheet (optional). Output tabs:
  `D_and_diffs_merged`, `N_and_diffs_merged`, `D_net_diff` / `N_net_diff`
  (vs a configurable target column per run), `totals`, `totals_net_diff`,
  and `config_summary_merged`. Two pieces of behavior follow the
  diagnostics _mode_ recorded in each run's `config_summary`
  (`diagnostics_baseline_source`):
  - **Release-vs-snapshot runs** (`diagnostics_baseline_source ==
    'gcs_snapshot'`) carry a `BLy_and_E_orig_diffs` tab, so the merger
    produces `totals` / `totals_net_diff` with columns `BLy`, `E_orig`,
    `BLy - E_orig`, `(BLy - E_orig) / E_orig (%)` — unchanged from the
    legacy xlsx flow. Their `useeio_baseline_pin_*` fields are empty.
  - **USEEIO Excel-baseline comparisons** (`diagnostics_baseline_source ==
    'gcs_useeio_xlsx'`) omit `BLy_and_E_orig_diffs` by design (no
    `E_old` for the Excel baseline path) but always carry
    `BLy_new_vs_BLy_old` (per-sector). The merger sums that tab across
    sectors to produce the same one-row-per-config `totals` schema, with
    columns `BLy_new`, `BLy_old`, `BLy_new - BLy_old`,
    `(BLy_new - BLy_old) / BLy_old (%)`. These runs DO carry
    `useeio_baseline_pin_*` fields naming the pinned Excel artifact,
    which is what makes the synthetic `pinned_useeio_baseline` column
    below meaningful.
  When a combo's `target_mapping` references the special target
  `pinned_useeio_baseline`, the merger injects a synthetic
  `pinned_useeio_baseline` column into `D_and_diffs_merged` /
  `N_and_diffs_merged` /
  `config_summary_merged`, sourced from the first input run's
  `D_old_inflated` and `N_old_purchaser` (falling back to
  `N_old_inflated`) and pin metadata. Per-config N columns prefer
  `N_new_purchaser`, then `N_new_inflated`, then `N_new`. This lets a
  USEEIO-rebuild combo's net-diff show `run.D_new − pinned_baseline`
  instead of the default self vs self. Combos that don't opt in (e.g.
  the v0.2 release-vs-release setup) keep their original output schema
  unchanged.
- `combinations.py` — registered diagnostics combinations (one `ComboSpec`
  per named multi-run comparison). Holds the Drive folder ID, ordered input
  Sheet titles, and per-`config_name` target mapping. The destination Sheet
  for merged output is always passed on the command line via
  `--output-sheet-id`.
- `release_v0_3_progression.py` — v0.3 release-deck sheet registry (IDs,
  titles, `config_name` per step). Consumed by `combinations.py` and
  `bedrock.analysis.v0_3.plot_ef_release_v0_3`. v0.2 FINAL sheets record
  `config_name` `2025_usa_cornerstone_full_model` (renamed to
  `2025_usa_cornerstone_v0_2` in configs after dispatch). Atomic v0.3 steps
  (inflation, A/price, MECS) net-diff against that column, not stepwise.

## Running

```bash
uv run python -m bedrock.utils.validation.analysis.diagnostics_plots \
    --sheet-id <google_sheet_id> [--refresh] [--tag <label>] [--out-dir <path>] \
    [--bly-group-small-threshold <Mt CO2e>]
```

`--bly-group-small-threshold` defaults to `3.0` (see `bly_plots.DEFAULT_GROUP_SMALL_THRESHOLD`). Pass `0` to disable grouping small sectors into “Other”.

`--sheet-id` falls back to the `BEDROCK_DIAGNOSTICS_SHEET_ID` environment
variable, so you can set it once and omit the flag on subsequent runs:

```bash
export BEDROCK_DIAGNOSTICS_SHEET_ID=1Qa...
uv run python -m bedrock.utils.validation.analysis.diagnostics_plots --tag my-run
```

Outputs default to `analysis/output/<tag>/`; the parquet cache lives at
`analysis/.cache/<sheet_id>/`. Pass `--refresh` to bypass the cache.

### combine_ef_diagnostics

Merge a registered combination's diagnostics Sheets into one workbook:

```bash
uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics \
    --combo v0.2 [--refresh] [--output-xlsx PATH] [--output-sheet-id ID]
```

`--combo` picks a `ComboSpec` from `combinations.COMBINATIONS`. Merged
`D_and_diffs` / `N_and_diffs` tabs use each run's `D_new_inflated` when
present, otherwise `D_new`. N columns prefer `N_new_purchaser` (when the
producer emitted purchaser-price columns), then `N_new_inflated`, then
`N_new`. The local
workbook is always written, defaulting to
`analysis/output/<combo>/ef_diagnostics_merged.xlsx`; pass `--output-xlsx
<path>` to override or `--output-xlsx ""` to skip. The Google Sheets push
only happens when `--output-sheet-id <id>` is supplied — without it, the
command writes the local xlsx and nothing else. Tab fetches share the
parquet cache in `analysis/.cache/<sheet_id>/`, so re-runs are fast —
pass `--refresh` to bypass it.

Example reproducing the v0.2 run (writes the same destination Sheet as
the original script):

```bash
uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics \
    --combo v0.2 \
    --output-sheet-id 1TOLpjg80GBeb3C8sVKGvYRL9U5HfUgKSz_IHoWHainY
```

### v0.3 release

Plot, dispatch, and sheet registry live in `bedrock/analysis/v0_3/` — see
[`bedrock/analysis/v0_3/README.md`](../../../analysis/v0_3/README.md).

Combine v0 baseline through v0.3 (CEDA) or FINAL v0.2 through v0.3
(USEEIO) from this package:

```bash
uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics \
    --combo v0.2_to_v0.3_ceda \
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_3/ef_diagnostics_merged_v0_2_to_v0_3_ceda.xlsx

uv run python -m bedrock.utils.validation.analysis.combine_ef_diagnostics \
    --combo v0.2_to_v0.3_useeio \
    --output-xlsx bedrock/analysis/v0_3/output/release_v0_3/ef_diagnostics_merged_v0_2_to_v0_3_useeio.xlsx
```

Re-run with `--refresh` after sheet tabs change.
