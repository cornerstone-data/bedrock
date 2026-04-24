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
