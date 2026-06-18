---
name: plot-ef-diagnostics
description: Use this skill to generate plots from one or more EF (emission-factor) diagnostics Google Sheets — the per-sector N (total EF) / D (direct EF) percent-diff distributions vs the CEDA-US (v0) baseline, cross-scenario comparisons (e.g. current scenario vs a v0.2 snapshot sheet), and deck-style histogram panels. Trigger when the user says "plot N against CEDA v0", "compare N to the v0.2 sheet", "make the EF diagnostics plots", "match the deck histogram style", "plot the diagnostics for this sheet", or hands over a diagnostics sheet URL/ID and asks for N/EF comparison plots.
disable-model-invocation: false
argument-hint: <diagnostics-sheet-id-or-url> [vs <baseline-sheet-id> | --deck-style] [color]
---

# Plot EF diagnostics

Produce N/EF comparison plots from diagnostics sheets. **Reuse the repo's existing plotting machinery — every panel style the user asks for already exists.** Import the fetch + plot primitives below before writing anything new.

## Step 0 — orient (always)

- A diagnostics sheet has tabs `N_and_diffs`, `D_and_diffs`, `D_and_N_significant_sectors`, `config_summary`. Columns: `sector, sector_name, N_new, N_old_inflated, N_old, N_perc_diff, comparison_type` (D mirrors N).
- **`N_perc_diff` is already "N vs CEDA v0"** — `(N_new − N_old_inflated) / |N_old_inflated|`, where `N_old_inflated` is the CEDA baseline inflated to the run's dollar year. So most "vs CEDA" asks need no recomputation.
- Reading a sheet via the Drive MCP returns markdown that **escapes underscores** (`N\_new`). Don't grep the raw export for `N_new` — use `load_tab` (below).
- Check `config_summary` for run identity + dollar year (`config_name`, `model_base_year`, `usa_io_data_year`, `usa_ghg_data_year`).

## Clarify first — ask before plotting

Once you know the sheet(s), confirm via `AskUserQuestion` (defaults as recommended). Skip a question only when the user already answered it.

| Detail | Default |
|---|---|
| Diagnostics sheet ID(s) | from the user — accept URL or bare ID; extract the ID |
| Comparison mode | vs CEDA v0 (Recipe A). B = vs another sheet; C = deck histogram style |
| Baseline / comparison sheet | required for Recipe B (e.g. the v0.2 sheet whose `N_new` is the baseline) |
| EF kind | N (total), D (direct), or both |
| Panel color / label | by approach (`APPROACH_COLORS`); orange `#ff7f0e` if asked |
| Output tag / folder | sheet- or config-derived; lands under `bedrock/utils/validation/analysis/output/<tag>/` |

For Recipe B, surface the **dollar-year guard** (below) and ask whether to proceed on raw `N_new` or switch to inflation-adjusted columns.

## Building blocks (import, don't re-implement)

- **Fetch (parquet-cached):** `from bedrock.utils.validation.analysis.fetch import load_tab` → `load_tab(sheet_id, "N_and_diffs")` (pass `refresh=True` to re-pull).
- **Plot primitives:** `from bedrock.utils.validation.analysis.plotting import setup_mpl, percent_histogram, apply_axis_fonts, save_and_close, DEFAULT_XLIM, TITLE_FONTSIZE`.
- **Deck-panel extractor + constants:** `from bedrock.analysis.a_matrix_time_series.plot_v0_3_n_pct_hist import _pct_values` and `from bedrock.analysis.a_matrix_time_series.plot_ef_diagnostics import HIST_BINS, HIST_PCT_CLIP, HIST_FONT_SCALE, HIST_STATS_EXTRA_SCALE, STATS_FONTSIZE, TITLE_FONTSIZE, AXIS_LABEL_FONTSIZE, TICK_LABEL_FONTSIZE`.
- **Approach palette** (`from bedrock.analysis.a_matrix_time_series.constants import APPROACH_COLORS`): `useeio #7f7f7f`, `ceda_default #bcbd22`, `summary_tables #1f77b4`, `industry_price_index #9467bd`, `commodity_price_index #2ca02c`, `useeio_nowcast #ff7f0e`.

## Recipe A — single sheet, N/D vs CEDA v0 (full suite)

The maintained CLI; produces the N % diff histogram (the core ask) + N/D 2×2 + EF scatters from the sheet's own diffs:
```bash
uv run python -m bedrock.utils.validation.analysis.diagnostics_plots \
    --sheet-id <SHEET_ID> --tag <label>
```
Outputs: `ef_n_perc_diff_histogram.png`, `ef_perc_diff_histogram.png`, `ef_pct_change_vs_abs_change.png`, `ef_pct_change_vs_ef_size.png`, `ef_abs_change_histogram.png` (+ `bly_*` if a `BLy` tab exists).

## Recipe B — cross-scenario: this sheet's N vs another sheet's N (e.g. v0.2)

For "current scenario vs the v0.2 sheet" (v0.2's N lives in *its* `N_new`), **no rerun needed** — inner-join the two sheets' `N_new` on `sector` (row order differs; drop zero/old-only rows).

**Guard first:** compare `N_new` directly **only if dollar years match** (`model_base_year`, `usa_io_data_year`, `usa_ghg_data_year` equal in both `config_summary` tabs). If they differ, the comparison conflates method change with inflation — use the inflation-adjusted columns instead.

Reuse `load_tab` + `percent_histogram`: produce a log-log scatter (`x = baseline N`, `y = current N`, y=x line, R², median/p95 of % diff) and a `percent_histogram` of `(current − baseline)/|baseline|`. (Prior inline script saved under `output/current_vs_v0_2/`.)

## Recipe C — deck histogram style (match the slides)

Deck panels (e.g. "[CEDA as baseline] Bundled effect in N", titled by approach) come from `plot_ef_diagnostics._hist_panel` / `plot_v0_3_n_pct_hist._render`: each panel is one sheet's `N_perc_diff`, clipped to ±`HIST_PCT_CLIP` (100%), `HIST_BINS` (60) bins, zero line, `PercentFormatter` x-axis "Percentage Diff (%)", y "sector count", an `n / median / p95(|·|)` white box top-left, title + bar color from the approach.

- Single sheet, exact deck style: `python -m bedrock.analysis.a_matrix_time_series.plot_v0_3_n_pct_hist <SHEET_ID>`.
- **Color override** (the script colors by approach, grey for unrecognized configs): replicate the `_render` body in a small figure, pulling pct via `_pct_values(load_tab(sid, "N_and_diffs"), "N")`, and pass an explicit color. **Orange = `#ff7f0e`** (the `useeio_nowcast` entry).
- **Side-by-side** (e.g. "v0.2 vs new"): lay panels out 1×N reusing the same constants so output matches the deck. Keep v0.2 blue (`#1f77b4`); color the new scenario as requested.

## Standing rules

- **Search before creating.** Prefer Recipe A/C's entry points; drop to an inline script only for cross-sheet joins (B) or a color/layout the scripts don't expose.
- **Never aggregate A-matrix Δ across columns** — different denominators per column, not commensurable. (N/EF per-sector diffs are fine to pool.)
- **Name artifacts by content, not step number** — no `step*` prefixes in PNG/CSV/tab names.
- Verify each PNG by reading it back before reporting done; report key stats (n, median, p95, top movers).

## Reference

- `bedrock/utils/validation/analysis/`: `diagnostics_plots.py` (CLI), `plotting.py` (primitives), `fetch.py` (cached loader).
- `bedrock/analysis/a_matrix_time_series/`: `plot_ef_diagnostics.py` (`_hist_panel`, `_scatter_panel`, constants), `plot_v0_3_n_pct_hist.py` (`_render`, `_pct_values`), `constants.py` (`APPROACH_COLORS`).
- Sheets come from the `dispatch-ef-diagnostics` skill / `generate_diagnostics` workflow.
