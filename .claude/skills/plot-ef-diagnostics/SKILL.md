---
name: plot-ef-diagnostics
description: Use this skill to generate plots from one or more EF (emission-factor) diagnostics Google Sheets — per-sector N (total EF) / D (direct EF) percent-diff distributions vs the CEDA-US (v0) or USEEIO baseline, cross-scenario comparisons (e.g. current scenario vs a v0.2 snapshot sheet), deck-style histogram panels, release-progression panels (one panel per scenario step, vs CEDA v0 / USEEIO / prior release), version overlays (v0.2 vs v0.3 on one axis), and BLy sector net-change charts. Trigger when the user says "plot N against CEDA v0", "compare N/D to the v0.2 sheet", "make the EF diagnostics plots", "match the deck histogram style", "overlay v0.2 vs v0.3", "panel plots per release step", "BLy chart", or hands over a diagnostics sheet URL/ID and asks for N/EF comparison plots.
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
- **Baselines & price basis:** the **CEDA v0** comparison is the in-sheet `N_perc_diff` (producer prices). The **USEEIO** comparison lives in `_purchaser` columns (USEEIO-baseline sheets only): `N_new_purchaser` (model, purchaser price) vs `N_old_purchaser` (USEEIO baseline) ⇒ **USEEIO N % diff = `(N_new_purchaser − N_old_purchaser)/|N_old_purchaser|`**. **`D` has NO `_purchaser` columns** — so D comparisons are producer/CEDA-only (there is no USEEIO-D).
- **Trust the config, not the dispatch year.** Resolve a config's intrinsic dollar year with `from bedrock.utils.config.usa_config import _load_usa_config_from_file_name` → `_load_usa_config_from_file_name("<name>.yaml").model_base_year` (and `.usa_ghg_data_year`). Among the release configs, only `…_2024_io_ghg` and FINAL `…_v0_3` are `model_base_year=2024`; `…_umd_2024_ghgia` is **IO@2023 / GHG@2024** (`model_base_year=2023`).

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
- **Deck-panel extractor + constants:** `from bedrock.utils.validation.analysis.ef_hist_panels import pct_values, draw_per_sector_pct_hist_panel, HIST_BINS, HIST_PCT_CLIP, HIST_FONT_SCALE, HIST_STATS_EXTRA_SCALE, PANEL_STATS_FONTSIZE, PANEL_TITLE_FONTSIZE, PANEL_AXIS_LABEL_FONTSIZE, PANEL_TICK_LABEL_FONTSIZE`.
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

Deck panels (e.g. "[CEDA as baseline] Bundled effect in N", titled by approach) use `draw_per_sector_pct_hist_panel` from `ef_hist_panels` (also used by `plot_v0_3_n_pct_hist` and A-matrix `plot_ef_diagnostics` histogram grids): each panel is one sheet's `N_perc_diff`, clipped to ±`HIST_PCT_CLIP` (100%), `HIST_BINS` (60) bins, zero line, `PercentFormatter` x-axis "Percentage Diff (%)", y "sector count", an `n / median / p95(|·|)` white box top-left, title + bar color from the approach.

- Single sheet, exact deck style: `python -m bedrock.analysis.a_matrix_time_series.plot_v0_3_n_pct_hist <SHEET_ID>`.
- **Color override** (the script colors by approach, grey for unrecognized configs): replicate the `_render` body in a small figure, pulling pct via `pct_values(load_tab(sid, "N_and_diffs"), "N")`, and pass an explicit color. **Orange = `#ff7f0e`** (the `useeio_nowcast` entry).
- **Side-by-side** (e.g. "v0.2 vs new"): lay panels out 1×N reusing the same constants so output matches the deck. Keep v0.2 blue (`#1f77b4`); color the new scenario as requested.

## Recipe D — release-progression panels (one panel per scenario step)

For a multi-step release (e.g. the v0.2 / v0.3 config-sheet scenarios), build a deck-style grid (`ncols=3`) where each panel is one step's per-sector % diff, reusing the Recipe-C panel body. Three framings, each its own figure × baseline:

- **vs CEDA v0** (producer): pct = in-sheet `N_perc_diff` / `D_perc_diff` — each step compared at its *own* dollar year, so **no inflation**.
- **vs USEEIO** (purchaser, **N only**): pct = `(N_new_purchaser − N_old_purchaser)/|N_old_purchaser|`.
- **vs the prior release's FINAL** (e.g. v0.3 step vs v0.2 FINAL): cross-sheet `N_new` (or `D_new`) joined on `sector` — needs the inflation step below.

The **FINAL step is just the last panel** — don't also emit a redundant standalone for it. v0.2 and v0.3 progressions have **different scenario lists per baseline** (CEDA-FBS path vs USEEIO-Phoebe path — the two tables in the config sheet); panel each from its own list.

**Cross-release dollar-year inflation:** to compare a 2024-model step (`_2024_io_ghg`, FINAL `v0_3`) against a 2023 baseline (v0.2 FINAL), inflate the baseline N per sector 2023→2024 using the model's own factor: `r[s] = N_old_inflated(2024 sheet)[s] / N_old_inflated(2023 sheet)[s]` (≈0.985; `N_old` is identical across sheets, so the ratio is a clean deflator), then `base_2024 = base_2023 × r`. Flag those panels (e.g. `[+1yr infl]`). `umd_2024_ghgia` is IO@2023 → no inflation.

## Recipe E — overlay two versions on one axis (v0.2 vs v0.3)

`overlay_ef_hist` overlays multiple sheets' N/D % diff **vs CEDA v0** on shared axes (semi-transparent bars, median lines, legend = `n / median / % up / % beyond ±20%`):
```bash
uv run python -m bedrock.utils.validation.analysis.overlay_ef_hist \
  --series "v0.2=<v0.2 FINAL sheet>" --series "v0.3=<v0.3 FINAL sheet>" --out-dir <dir>
```
→ `ef_overlay_hist_{N,D}.png`. The CLI is **CEDA-only**; for an **overlay vs USEEIO**, call the same primitive directly on the purchaser columns: `from bedrock.utils.validation.analysis.plotting import overlay_pct_diff_histogram`, pass `{label: (N_new_purchaser − N_old_purchaser)/|N_old_purchaser| × 100}` per version (**N only** — no purchaser D).

## BLy (sector net-change) chart

`diagnostics_plots` emits `bly_sector_stacked_net_change.png` when the sheet has a `BLy_new_vs_BLy_old` tab. For a custom title (e.g. per release), build it directly: `from bedrock.utils.validation.analysis.bly_plots import build_sector_stack_frame, TAB_BLY` + `from …plotting import plot_stacked_net_change`, then `plot_stacked_net_change(ax, build_sector_stack_frame(load_tab(sid, TAB_BLY)), title=…, ylabel="Gross change (MMT CO2e)")`.

## Standing rules

- **Search before creating.** Prefer Recipe A/C's entry points; drop to an inline script only for cross-sheet joins (B) or a color/layout the scripts don't expose.
- **Generated PNGs are gitignored** — `bedrock/utils/validation/analysis/output/` is ignored (do not re-include or `git add -f`); never commit plot PNGs.
- **Consolidating for a deck:** flatten into one folder, **group-prefix** filenames, add `_panel` to multi-panel figures, and **stamp the group into each plot's title/banner** so it's identifiable without the filename. Drop scatters / abs-change plots unless asked.
- **No Google Slides editing** — the connected Google tools are Drive-only (no Slides API), and decks shared from other accounts may 404. To get plots into a deck, upload the PNGs to Drive via `create_file` (base64) for manual drag-in; you can't insert them programmatically.
- **Never aggregate A-matrix Δ across columns** — different denominators per column, not commensurable. (N/EF per-sector diffs are fine to pool.)
- **Name artifacts by content, not step number** — no `step*` prefixes in PNG/CSV/tab names.
- Verify each PNG by reading it back before reporting done; report key stats (n, median, p95, top movers).

## Reference

- `bedrock/utils/validation/analysis/`: `diagnostics_plots.py` (single-sheet CLI), `overlay_ef_hist.py` (version-overlay CLI), `ef_hist_panels.py` (`pct_values`, `draw_per_sector_pct_hist_panel`), `bly_plots.py` (`build_sector_stack_frame`), `plotting.py` (primitives: `percent_histogram`, `overlay_pct_diff_histogram`, `plot_stacked_net_change`), `fetch.py` (cached loader).
- `bedrock/analysis/a_matrix_time_series/`: `plot_ef_diagnostics.py` (A-matrix scatter + histogram grids), `plot_v0_3_n_pct_hist.py` (single-sheet CLI), `constants.py` (`APPROACH_COLORS`).
- `bedrock/utils/config/usa_config.py`: `_load_usa_config_from_file_name("<name>.yaml")` to resolve a config's intrinsic dollar years.
- Sheets come from the `dispatch-ef-diagnostics` skill / `generate_diagnostics` workflow.
