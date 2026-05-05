# A Matrix Transformation Analysis — Draft Plan

## Goal

Analyze five approaches for deriving the A matrix (in commodity-by-commodity format) across a time-series (2017–2024), and recommend one approach as Cornerstone's method for the 2026 model.

The five approaches partition into:

- **Two baselines** (always plotted, never the recommendation candidates):
  1. **USEEIO** — derive A in the 2017 benchmark year and use as-is.
  2. **CEDA** — current production default: scale 2017 → IO year via summary-table ratios, then inflate IO year → model year via price index.
- **Three alternative approaches** (the candidates being evaluated):
  1. **Summary tables** — scale 2017 A directly to 2018–2024 via summary-table ratios (price *and* quantity changes captured together; no separate inflation step).
  2. **Industry price index** — inflate 2017 A directly to 2018–2024 using industry-specific price indices applied as if commodity-specific.
  3. **Commodity price index (V-norm)** — inflate 2017 A directly to 2018–2024 using a *commodity-specific* price index derived from the industry price index and Vnorm (commodity mix).

Element-wise comparison of every (approach × year) pair informs the recommendation.

### Comparison baselines (apply to every comparison in this plan)

**Every divergence / difference / ranking computation in this analysis must be reported against both baselines, side by side:**

1. **USEEIO baseline** — the "do nothing" invariant; isolates the *combined* effect of any year-scaling method.
2. **CEDA-US baseline** — the current production default (two-step scale + inflate); isolates how each *alternative* method differs from what the model ships today. Inside the `generate_diagnostics` GitHub Actions pipeline this baseline is named **v0** and is applied automatically (see Step 6 for details).

Reporting against both is non-negotiable: USEEIO alone tells you "did anything change vs not adjusting", CEDA-US alone tells you "did the production path change", and only the pair tells you whether an alternative method is closer to or further from the production status quo while still capturing real year-over-year structural change. Where a single figure can't fit both baselines, produce two figures with a shared color/legend convention.

---

## How to Use This Doc

### Definition of Done for the whole project

The project is complete when **all** of the following are true:

1. A parquet cache of A matrices exists on disk for every (approach × year) pair in the agreed-upon span.
2. All figures in the Deliverables section exist as PNG files in `bedrock/analysis/a_matrix_time_series/output/figures/`.
3. A README in that directory summarizes findings with a single-sentence recommendation per method and a one-paragraph methodology recommendation.
4. The six "Key Questions" below each have a written answer backed by a figure reference.
5. A code change exists on a branch that contains the new `bedrock/analysis/a_matrix_time_series/` module and the README, and at least one peer has reviewed it.

### Before writing any code — read these (in order)

1. [.claude/plans/issue_182_a_matrix_methods.md](issue_182_a_matrix_methods.md) — background on the three alternative methods. *(Existing doc from Feb 2026 — scoped when Issue #182 was opened. Read this first for the "why".)*
2. [.claude/plans/issue_182_implementation_plan.md](issue_182_implementation_plan.md) — how the existing flags are wired. *(Existing doc from Feb 2026 — the implementation blueprint that was executed to land the three flags now on `main`.)*
3. [bedrock/transform/eeio/derived_cornerstone.py](../bedrock/transform/eeio/derived_cornerstone.py) — read `derive_cornerstone_Aq_scaled()` (L498) top to bottom. You should be able to explain what each existing if-branch does (USEEIO, summary tables, industry price index) plus the default CEDA path before writing any code.
4. [bedrock/analysis/time_series_B_matrix/derive_B_time_series.py](../bedrock/analysis/time_series_B_matrix/derive_B_time_series.py) — this is your template. Your code should feel structurally similar.
5. Run the smoke test: `pytest bedrock/transform/__tests__/test_usa.py -k cornerstone` and confirm it passes on `main` before branching.

### Check-in checkpoints (don't skip)

Show work to a reviewer before proceeding past these gates:

- **Checkpoint A (after Step 0)**: the V-norm flag implementation code change is open and smoke-tested. Do not start Step 1 until this is merged.
- **Checkpoint B (after Step 1)**: the parquet cache is populated for all (approach × year) combinations and you have printed summary statistics (shape, non-null count, column-sum max) for each matrix. This catches silent caching bugs.
- **Checkpoint C (after Step 3)**: review the cross-approach divergence plots before investing in Steps 4–6.
- **Checkpoint D (before Step 7)**: only proceed if Step 3/4 surfaced a concrete motivation — otherwise, skip.

### Out of Scope (do NOT do these)

- Do not modify `derive_cornerstone_Aq_scaled()` or any production pipeline code, other than adding the V-norm branch in Step 0.
- Do not introduce new config flags beyond the V-norm one.
- Do not build a dashboard, UI, or anything interactive. Static PNGs + markdown README only.
- Do not fix unrelated bugs you find along the way. File them as issues.

### If you get stuck

- If a config flag produces NaNs or all-zeros: first check `derive_cornerstone_Aq()` base output is sane, then re-read the flag branch in `derive_cornerstone_Aq_scaled()`.
- If you cannot produce a year (e.g. 2018 summary A tables missing): skip that year, document the gap in the README, and continue. Don't invent interpolation.
- If any figure looks implausible (EF shifts > 10×, negative A cells): stop and surface to a reviewer before building on it.

## Background

### Current state
- Config flags are wired in `derive_cornerstone_Aq_scaled()` at [bedrock/transform/eeio/derived_cornerstone.py:498](../bedrock/transform/eeio/derived_cornerstone.py#L498):
  - `scale_a_matrix_with_useeio_method` → returns 2017 base A unchanged
  - `scale_a_matrix_with_summary_tables` → single-step `scale_cornerstone_A(2017 → model_year)`, skips price inflation
  - `scale_a_matrix_with_price_index` → single-step `inflate_cornerstone_A_matrix(2017 → model_year)`, skips summary scaling
  - Default (CEDA): two-step `scale_cornerstone_A(2017 → io_year)` then `inflate_cornerstone_A_matrix(io_year → model_year)`
- YAMLs already exist: `2025_usa_cornerstone_a_{useeio,summary_tables,price_index}.yaml`
- **Pending** (Step 0 of this plan): a fifth branch + flag `scale_a_matrix_with_commodity_price_index` for approach #5 (V-norm-derived commodity price index) — identical to the industry-price-index path but uses commodity-level prices obtained by V-norm transforming the industry-level price index, rather than applying industry prices directly.

### The five approaches recap (from [notes](https://docs.google.com/document/d/1RlK2ivSnHrku3Q2k5GxRVI_eO25xZ5VE4iIuAlm8TwU/edit?tab=t.0#heading=h.s7guag7kk8nq))

| # | Role | Approach | Mechanism | Implicit assumption |
|---|---|---|---|---|
| 1 | Baseline | **USEEIO (do nothing)** | 2017 A used as-is | 2017 technology mix = target year technology mix |
| 2 | Baseline | **CEDA (scale + inflate)** | Two-step: `scale_cornerstone_A(2017 → io_year)` then `inflate_cornerstone_A_matrix(io_year → model_year)` | Separating quantity changes (summary-table ratios) and price changes (price index) is more accurate than either alone |
| 3 | Alternative | **Summary tables** | Element-wise multiply 2017 A by `A_summary(target) / A_summary(2017)`, with 0.98 column-cap | Summary-level structural change is the best signal for detail-level change; captures price + quantity in one step |
| 4 | Alternative | **Industry price index** | `diag(p) @ A @ diag(1/p)` with `p = price(target) / price(2017)`, industry-level (treated as if commodity-level) | Perfect price inelasticity — physical flows constant, A shifts only from relative price changes |
| 5 | Alternative | **Commodity price index (V-norm)** | Same formula as #4, but `p` is first V-norm transformed from industry → commodity space | Same as #4, plus: commodity prices are more appropriate than industry prices for A matrix inflation, and co-production mixing is non-negligible |

**Note on #5**: this is the "commodity-transformed" price variant discussed in the notes under *V-Norm Transformation Discussion*. Requires a new config flag and a new branch in `derive_cornerstone_Aq_scaled()` — see Step 0 below. We will use the static Vnorm (2017 benchmark version, same as that used in the B matrix transformation). In the next phase, we will test adjusting Vnorm to years that are consistent with A matrix years.

### Known caveats from the notes

- **Summary table scaling** needs a 0.98 cap on column sums (implemented in `scale_cornerstone_A` at [cornerstone_year_scaling.py:111](../bedrock/transform/eeio/cornerstone_year_scaling.py#L111)) and is highly aggregated — especially in manufacturing/services.
- **Price index** currently uses **industry-level** price indices, not V-norm-transformed commodity prices. This trades mathematical rigor for transparency and verifiability.
- **USEEIO PR is old** — will need a smoke test and possibly a rebase/fix before it produces comparable outputs.
- **No ground-truth A matrix** exists for the target year at BEA detail resolution, so we cannot judge "better" element-wise. Industry gross output (which we do have annually, after redefinition) is the closest external anchor.

---

## Analysis Steps

### Step −1 — Rename the existing price-index approach for clarity (PREREQUISITE)

Before the V-norm branch lands, rename the existing price-index pieces so that `industry_price_index` and `commodity_price_index` read as parallel siblings. Ship this as its own small PR — mechanical renames only, no logic changes. This keeps Step 0 a pure additive change instead of a mixed rename+add diff.

**Changes required:**

1. In [bedrock/utils/config/usa_config.py](../bedrock/utils/config/usa_config.py): rename the flag
   - `scale_a_matrix_with_price_index` → `scale_a_matrix_with_industry_price_index`
2. In [bedrock/transform/eeio/derived_cornerstone.py](../bedrock/transform/eeio/derived_cornerstone.py): rename the if-branch reference in `derive_cornerstone_Aq_scaled()`.
3. Rename all YAML files in [bedrock/utils/config/configs/](../bedrock/utils/config/configs/) from lowercase-`a` to uppercase-`A` for the A-matrix methods (the "A matrix" noun is capitalized everywhere else in the codebase and docs):
   - `2025_usa_cornerstone_a_useeio.yaml` → `2025_usa_cornerstone_A_useeio.yaml`
   - `2025_usa_cornerstone_a_summary_tables.yaml` → `2025_usa_cornerstone_A_summary_tables.yaml`
   - `2025_usa_cornerstone_a_price_index.yaml` → `2025_usa_cornerstone_A_industry_price_index.yaml` (combines rename + clarifier)
4. `grep -rn "scale_a_matrix_with_price_index\|2025_usa_cornerstone_a_" bedrock/` and update every hit (tests, CI configs, docs).
5. Smoke test: `pytest bedrock/transform/__tests__/test_usa.py -k cornerstone`.

**Definition of Done for Step −1:**
- Code change lands with only renames — no logic diffs — reviewed and merged.
- Existing diagnostics CI still passes on the three old branches (the rename does not break downstream consumers).
- No references to the old flag name or old YAML filename remain in the codebase.

### Step 0 — Implement Vnorm-derived price index approach (PREREQUISITE)

Before any analysis can run, the 5th approach (commodity price index, alternative #3) must be wired into the pipeline.

**Changes required:**

1. Add flag `scale_a_matrix_with_vnorm_price_index: bool = False  # DRI: TBD` in [bedrock/utils/config/usa_config.py](../bedrock/utils/config/usa_config.py) alongside the other three.
2. Add a new helper `get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)` in [bedrock/utils/economic/inflate_cornerstone_to_target_year.py](../bedrock/utils/economic/inflate_cornerstone_to_target_year.py) that:
   - Takes the existing industry-level price ratio from `get_cornerstone_price_ratio()` (currently indexed by commodity because of how CEDA v7 is structured — confirm by reading the function).
   - Transforms to commodity space via `V @ diag(p_industry) @ V^{-1}` (or the equivalent Vnorm transform — verify the exact mechanics with a reviewer before coding).
   - Returns a commodity-indexed price ratio Series.
3. Add `inflate_cornerstone_A_matrix_with_commodity_pi()` that uses the Vnorm ratio with the same `diag(p) @ A @ diag(1/p)` formula.
4. Add a fourth `if cfg.scale_a_matrix_with_commodity_price_index:` branch in `derive_cornerstone_Aq_scaled()`, modeled on the existing `scale_a_matrix_with_price_index` branch.
5. Add YAML `bedrock/utils/config/configs/2025_usa_cornerstone_a_commodity_price_index.yaml`.
6. Smoke test: `pytest bedrock/transform/__tests__/test_usa.py -k cornerstone`.

**Definition of Done for Step 0:**
- Code change is open with the above changes, tests pass, and a reviewer has approved.
- Running the pipeline with the new YAML produces A matrices with no NaNs. Check column sums: if any column sum is ≥ 1, **surface for review** — do not silently clip or apply a 0.98-cap fix. Unlike summary-table scaling (which has a well-understood cap rationale), a column-sum violation on the V-norm path likely indicates a math error in the transform and needs human review.
- Print the commodity-price-ratio distribution vs the industry-price-ratio distribution (mean, median, 5th/95th percentile) — if they are nearly identical, surface for review since the analysis may be moot.

**Out of scope for this step:** do not refactor any of the other four branches (USEEIO, CEDA default, summary tables, industry price index). Do not add unit tests beyond what's needed to satisfy existing type checks — the analysis itself is the acceptance test.

### Step 1 — Produce A matrices for all (approach × year) combinations

For each `model_base_year ∈ {2017, 2018, 2019, 2020, 2021, 2022, 2024}` and each approach ∈ `{useeio, ceda_default, summary_tables, industry_price_index, commodity_price_index}` (2 baselines + 3 alternatives = 5 total):

1. **Do NOT create new YAML files per year.** Instead, load one of the five approach YAMLs via `get_usa_config()` and programmatically override `model_base_year` in memory (or use a pytest-style config fixture). Rationale: YAMLs are a user-facing deployment surface — committing 35 of them for a one-off analysis clutters the repo and invites copy-paste drift. The five approach YAMLs from Step −1 / Step 0 are sufficient; year is the only thing that varies per run.
2. Invoke `derive_cornerstone_Aq_scaled()` and cache the resulting `(Adom, Aimp, q)` to disk (parquet, keyed by approach + year).

**Notes:**
- For `Y = 2017`, all five variants should return the same A (ratios are 1.0, price factors are 1.0) — this is a useful sanity check.
- USEEIO is year-invariant by construction — produce it once and reuse; it serves as one of the two fixed baseline lines on every plot (CEDA default is the other; CEDA *does* change with year).
- Output path: `bedrock/analysis/a_matrix_time_series/output/A_{approach}_{year}.parquet`
- Mirror the structure of `bedrock/analysis/time_series_B_matrix/derive_B_time_series.py` — it already solves the "loop over years and cache parquets" problem.

**Definition of Done for Step 1:**
- Parquet files exist for all (approach, year) combinations in the agreed span.
- A `cache_summary.csv` exists with columns `(approach, year, n_rows, n_cols, nan_count, neg_count, max_col_sum, file_size_bytes)` — one row per matrix. This is the single artifact reviewed at Checkpoint B.
- The 2017 sanity check passes: all approaches produce identical A for year=2017 within `1e-10` relative tolerance.

### Step 2 — Cell-by-cell time-series diagnostics

For each approach, compute per-cell summaries across years.

**Metrics:**
- **Magnitude distribution**: `|A[i,j]|` histogram per year; look for regime shifts.
- **Year-over-year delta**: `A_Y[i,j] - A_{Y-1}[i,j]`, aggregated as L1/L2 norms per column (industry) and per row (commodity).
- **Trajectories**: for sectors in the key-sector list (see Step 4), plot `A[i,j]` vs year per approach on the same axes.

**Recommended plot formats** (pick based on which question the plot must answer):

| Plot type | Best for | Notes |
|---|---|---|
| **Small-multiple line plots** (`sector × approach` grid) | Tracking individual A cells over time across approaches | Canonical Tufte-style faceted grid; keep axes consistent per row |
| **ECDF overlay** of `|A[i,j]|` per approach at a fixed year | Tail behavior across approaches | Better than histograms when distributions are long-tailed |
| **Scatter x-y plot** for a single-year vs. baseline comparison (`A_approach[i,j]` on x, `A_baseline[i,j]` on y) | Spotting *systematic* over/under-shoot of an approach against a baseline; locating outlier cells where the two methods disagree most | Plot the identity `y = x` reference line — cells off the diagonal are the divergence story. Use log–log axes because A cells span ~6 orders of magnitude. Color by BEA summary sector group. With ~160k cells, switch to a hexbin (already listed below) once the markers overplot. Produce one scatter per (approach × baseline) pair to honor the two-baseline convention |

**Reuse existing plot functions from [bedrock/analysis/time_series_B_matrix/derive_B_time_series.py](../bedrock/analysis/time_series_B_matrix/derive_B_time_series.py) wherever possible — Step 2 is structurally the same "metric over years, faceted by sector" problem the B-matrix analysis already solved.** Map to the existing helpers:

| Step 2 need | Existing function to copy/adapt | How |
|---|---|---|
| Aggregate trend of `sum(abs(A))` or `col_sum_mean` over years per approach | `plot_aggregate_trends()` (L599) | Feed `(year, metric)` tuples per approach; reuse styling |
| Top-N sectors with largest A-cell drift | `plot_top_sector_time_series()` (L675) | Replace gas-axis with approach-axis; sectors stay on y |
| Per-sector time series in a faceted grid | `plot_sector_change_time_series()` (L704) | Swap "gas" grouping for "approach" grouping |
| Cumulative sector contribution | `plot_stacked_bar()` (L774) | As-is for column-sum-contribution view |
| Spaghetti of all sectors | `plot_all_sectors_line()` (L796) | Direct reuse — indexes every sector to 2017 = 100 |

If any of these functions would require meaningful surgery to reuse, prefer copying + adapting inside the new `a_matrix_time_series/` module rather than generalizing the B-matrix one — keep the two modules independent.

**Deliverable artifact:** a tall-format table with columns `(row_sector, col_sector, year, approach, A_value, delta_from_2017, delta_yoy, dom_or_imp)` — written once as parquet, used by all downstream plots. Call it `A_cells_long.parquet`.

**Definition of Done for Step 2:**
- `A_cells_long.parquet` exists and is documented in the module README (schema + row count).
- PNGs `step2_{heatmap,ridgeline,yoy_norms}_{dom,imp}.png` exist in the figures directory.
- Written answer (3–5 sentences) to: "do any approaches show regime shifts — sudden jumps between adjacent years — and if so, where?"

### Step 2.5 — Cell-level set stability and persistence

Step 2's `divergence_share_*.png` aggregates over cell identity: a flat year-over-year share of cells above a threshold could be (a) the same cells differing every year (structural offset), (b) a rotating cast of cells averaging to the same count, or (c) the same cells with magnitudes drifting around the bar. (a) and (b) imply very different stories. Step 2.5 distinguishes them.

**Two diagnostics, both keyed on the existing `A_cells_long.parquet`:**

1. **Year×year Jaccard heatmap** of "above-threshold" cell sets, per (approach × baseline). High off-diagonal Jaccard ⇒ structural offset (case a). Decay away from the diagonal ⇒ rotating membership (case b). Read the (approach, baseline) panel and the threshold together — at sufficiently tight thresholds even structural offsets drop out of the set.
2. **Persistence histogram** — for each cell, count years above threshold. Stack the share of cells in each `n_years_above` bucket per (approach × baseline). A large "always-above" segment confirms the structural-offset reading; mass spread across middle buckets indicates rotation.

**Implementation:** `bedrock/analysis/a_matrix_time_series/derive_A_cells_stability.py`. Reads the parquet from Step 2 only — no upstream pipeline changes. Computes both diagnostics over `ALL_THRESHOLDS = (1e-6, 1e-5, 1e-4, 1e-3, 1e-2)` (matching Step 2) and renders plots at `PLOT_THRESHOLDS = (1e-4, 1e-3)` where the cell sets are non-degenerate.

**Definition of Done for Step 2.5:**
- PNGs `set_stability_jaccard_thr{thr}_{kind}.png` for `kind ∈ {dom, imp}` and `thr ∈ PLOT_THRESHOLDS`, plus the multi-threshold composite `persistence_by_threshold_{kind}.png`.
- CSVs `set_stability_jaccard.csv` and `persistence_categories.csv` cover all of `ALL_THRESHOLDS`.
- Sheet tabs `set_stability_jaccard` and `persistence_categories` appended to the run-report Sheet.
- Written answer (3–5 sentences) to: "is the divergence pattern in Step 2's share-of-cells line a stable cell population (structural) or rotating membership, and how does that finding interact with the fixed `usa_io_data_year=2022` leg of `ceda_default`?"

### Step 3 — Cross-approach comparison at fixed target year (2024)

Pick `model_base_year = 2024` and compute pairwise differences between approaches on the **same** A matrix grid:

- Hexbin scatter: `A_summary_tables[i,j]` vs `A_industry_price_index[i,j]` (and vs `A_commodity_price_index`), colored by column-sector BEA summary code.
- **ECDF (paired)** of relative divergence per approach, computed twice — once vs USEEIO and once vs CEDA. Both ECDFs go in the same figure (or a 1×2 panel) so the reader can read off each approach's distance from each baseline at the same threshold.
  - Panel A: `(A_approach - A_useeio) / A_useeio` per approach
  - Panel B: `(A_approach - A_ceda) / A_ceda` per approach
- Column-sum diagnostics: verify all columns ≤ 1 (summary tables relies on the 0.98 cap — count how many columns actually hit the cap).
- Industry-price vs commodity-price ratio comparison (industry vs vnorm approach diff) — answers whether the V-norm transform produces a materially different A.

**Why ECDF specifically (not histogram):**
- **Answers the actual question directly.** The question is "what fraction of A cells diverge by more than X?" — an ECDF lets you read that off any threshold on the x-axis in one glance. A histogram forces the reader to eyeball-integrate.
- **No binning artifacts.** A-matrix cells span many orders of magnitude (`~10^-6` to `~10^-1`), and divergence ratios have a heavy tail. Histograms require either log-bins or wide bins, both of which obscure the shape — ECDFs sidestep the choice entirely.
- **Overlaying five approaches is cleaner than overlaying five histograms.** 5 lines stack readably; 5 colored histograms fight each other visually.
- **Tail focus.** The interesting divergences live in the tails — e.g. "5% of cells diverge by more than 50%". ECDFs show the tail mass directly as the distance from 1.0 on the y-axis.

**Expected finding from the notes:** summary-table scaling should show the largest divergence and is most prone to capping in manufacturing/service columns.

**Definition of Done for Step 3:**
- PNG `step3_pairwise_hexbins.png` (grid of hexbins, one per approach-pair).
- PNG `step3_divergence_ecdf_vs_useeio.png` and `step3_divergence_ecdf_vs_ceda.png` — one ECDF panel per baseline, same approach color convention across both.
- CSV `step3_column_cap_audit.csv` listing every column where summary-tables scaling triggered the 0.98 cap.
- Written answer to: "how much do approaches disagree against each baseline (USEEIO and CEDA), and is vnorm meaningfully different from industry-price?"

### Step 4 — Zoom into key sectors

Pick a curated sector list where priors differ across approaches:

| Sector | Why it matters |
|---|---|
| Energy inputs (e.g. electricity, natural gas) into manufacturing | Near-fixed stoichiometry → price-index approach expected to be most valid |
| Travel / hospitality / discretionary services | Price elasticity likely high → price-index assumption most likely to break |
| Waste (562111–562xxx disaggregated children) | Intragroup treatment + inherited price ratios; want to confirm nothing exploded |
| High-volatility commodities (petroleum refining, primary metals) | Year-scaling noise most visible |
| Sectors with large disaggregation ratios (1 BEA → many Cornerstone children) | Summary-table scaling uses parent ratio — all children move together, may be unrealistic |

For each: small-multiple line plot per column-industry showing `A[commodity, industry]` across years for each approach.

**Definition of Done for Step 4:**
- PNG `step4_keysector_{energy,travel,waste,volatility,disagg}.png` — one figure per category above.
- CSV `step4_sector_shortlist.csv` listing the actual BEA codes chosen with one-line justification per row. The shortlist is reviewed before the figures are finalized.
- Written answer to: "where are the largest cross-approach disagreements, and do they line up with the theoretical priors from the notes?"

### Step 5 — Industry output as ground-truth anchor

Notes say detail-level industry output time-series is "relatively stable reference/ground truth". This means: we already have annual BEA after-redefinition gross output — see `derive_gross_output_after_redefinition()`. Compare:

- **Model-derived industry output** from each scaled A matrix (via full-model Leontief computation, Step 6).
- **Observed BEA after-redefinition gross output** for the same year.

For each approach, per year, compute industry-level relative error vs observed. Aggregate as a single scalar (weighted RMSE over industries) to rank approaches.

**Caveat to name in the write-up:** industry output is a *column-sum-adjacent* diagnostic; it can agree well even when individual A cells are wrong. It gives directional/magnitude signal, not element-wise truth.

**Definition of Done for Step 5:**
- CSV `step5_industry_output_errors.csv` with columns `(approach, year, industry_rmse_vs_bea, top_5_worst_industries)`.
- PNG `step5_output_rmse_ranking.png` — bar chart of RMSE per approach per year (5 approaches × 7 years = 35 bars, grouped).
- Written answer to: "does industry output error meaningfully discriminate between approaches, or are they all within noise?"

### Step 6 — Full-model diagnostics (EF / EI impacts)

For each approach × 2024, run the full model via the existing `generate_diagnostics` GitHub Actions workflow (same diagnostics Google Sheet format the B-matrix work uses; backed by `calculate_ef_diagnostics.py`).

#### How the diagnostics pipeline supplies the two baselines

The two-baseline convention is enforced by the `generate_diagnostics.yml` workflow itself — the engineer does not implement comparison logic, just configures the workflow correctly:

| Baseline | How it enters the diagnostics output | Engineer action required |
|---|---|---|
| **CEDA-US** | Automatically used as **v0** inside the diagnostics pipeline. Every diagnostics run produces a comparison against CEDA-US whether you ask for it or not. | None — comes for free with every workflow run. |
| **USEEIO** | Opt-in. Activated by ticking the **"Benchmark to USEEIO GCS Excel baseline (URI, SHA, label from `useeio_baseline_pin.json`)"** checkbox in the `Run workflow` dialog. | **Always tick this box** when triggering Step 6 runs, so each approach gets compared against both baselines in a single pass. |

If the USEEIO checkbox is forgotten on any run, that run only produces the CEDA-US comparison and must be re-triggered to recover the USEEIO comparison — re-running is cheap, but track it.

#### Triggering the runs

For each of the five approaches:
1. Open `generate_diagnostics` → `Run workflow`.
2. Set **USA config name** to the approach's YAML stem (e.g. `2025_usa_cornerstone_A_industry_price_index`).
3. Set **Google Sheets ID** to the destination sheet for that approach.
4. **Tick** the "Benchmark to USEEIO GCS Excel baseline" checkbox.
5. (Optional) attach the PR URL for that approach.
6. Run.

The CEDA-default approach is already on `main` and already exercised by the production diagnostics — its sheet may already exist; reuse it rather than re-running unless config has drifted.

#### Output

- Emission factors per sector for each approach.
- Percent difference in final-demand-driven emissions and sector-level gross output, against **both** baselines (USEEIO via the checkbox, CEDA-US via v0). Both come from the same diagnostics sheet — no extra computation needed on the engineer's side.

**Action**: compile the resulting diagnostics sheets (one per approach) into one comparison workbook covering all five approaches rather than re-running from scratch.

**Definition of Done for Step 6:**
- All five approaches have a completed diagnostics run with the USEEIO checkbox ticked. Save the run URLs in `step6_run_index.csv` (columns: `approach`, `run_id`, `sheet_id`, `useeio_box_ticked`, `triggered_at`) so the workbook compilation step can locate every input.
- One Excel workbook `step6_ef_comparison.xlsx` in the output directory with one tab per approach plus a `summary_vs_useeio` tab and a `summary_vs_ceda` tab.
- PNGs `step6_ef_divergence_scatter_vs_useeio.png` and `step6_ef_divergence_scatter_vs_ceda.png` — EF values per approach against each baseline, shared color convention.
- Written answer to: "which approach's EFs look most plausible against the USEEIO published EFs and the CEDA-US production default?"

---

## Deliverables (concrete artifact list)

This is the exhaustive, named list of files the engineer must produce. If a file below does not exist by project end, the project is not done.

### Code
- [ ] `bedrock/utils/config/usa_config.py` — new flag `scale_a_matrix_with_vnorm_price_index` (Step 0)
- [ ] `bedrock/utils/economic/inflate_cornerstone_to_target_year.py` — new `get_vnorm_commodity_price_ratio()` + `inflate_cornerstone_A_matrix_vnorm()` (Step 0)
- [ ] `bedrock/transform/eeio/derived_cornerstone.py` — new branch in `derive_cornerstone_Aq_scaled()` (Step 0)
- [ ] `bedrock/utils/config/configs/2025_usa_cornerstone_a_vnorm_price_index.yaml` (Step 0)
- [ ] `bedrock/analysis/a_matrix_time_series/__init__.py`
- [ ] `bedrock/analysis/a_matrix_time_series/derive_A_time_series.py` — parquet caching driver (Step 1); mirror `derive_B_time_series.py`
- [ ] `bedrock/analysis/a_matrix_time_series/derive_A_cells_stability.py` — Step 2.5 set-stability + persistence diagnostics
- [ ] `bedrock/analysis/a_matrix_time_series/compare_approaches.py` — generates Step 2–4 plots
- [ ] `bedrock/analysis/a_matrix_time_series/compare_industry_output.py` — Step 5
- [ ] `bedrock/analysis/a_matrix_time_series/compile_ef_diagnostics.py` — Step 6

### Data artifacts (`bedrock/analysis/a_matrix_time_series/output/`)
- [ ] `A_{approach}_{year}.parquet` for each (approach, year) — Step 1
- [ ] `cache_summary.csv` — Step 1 Checkpoint B artifact
- [ ] `A_cells_long.parquet` — Step 2 tall-format table
- [ ] `set_stability_jaccard.csv`, `persistence_categories.csv` — Step 2.5
- [ ] `step3_column_cap_audit.csv` — Step 3
- [ ] `step4_sector_shortlist.csv` — Step 4
- [ ] `step5_industry_output_errors.csv` — Step 5
- [ ] `step6_run_index.csv` — Step 6 (workflow-run audit trail; one row per approach)
- [ ] `step6_ef_comparison.xlsx` — Step 6

### Figures (`bedrock/analysis/a_matrix_time_series/output/figures/`)
- [ ] `step2_heatmap_{dom,imp}.png`, `step2_ridgeline_{dom,imp}.png`, `step2_yoy_norms_{dom,imp}.png`
- [ ] `set_stability_jaccard_thr{1e-1,1e-2,1e-3}_{dom,imp}.png`, `persistence_by_threshold_{dom,imp}.png` — Step 2.5
- [ ] `step3_pairwise_hexbins.png`, `step3_divergence_ecdf_vs_useeio.png`, `step3_divergence_ecdf_vs_ceda.png`
- [ ] `step4_keysector_{energy,travel,waste,volatility,disagg}.png`
- [ ] `step5_output_rmse_ranking.png`
- [ ] `step6_ef_divergence_scatter_vs_useeio.png`, `step6_ef_divergence_scatter_vs_ceda.png`
- [ ] (optional) `step7_coproduction_sectors.png`

### Write-up
- [ ] `bedrock/analysis/a_matrix_time_series/README.md` — ≤2 pages. Must contain:
   - Method-by-method pros/cons table (5 rows: 2 baselines + 3 alternatives)
   - Answers to all six Key Questions (one paragraph each, figure reference inline)
   - **Explicit recommendation of one approach** as Cornerstone's method, with reasoning trace tying back to the figures
   - Explicit list of limitations + years/methods skipped for data-availability reasons

---

## Key Questions to Answer (from the notes)

- [ ] How much do the three alternative approaches disagree at the cell level, against each baseline (USEEIO and CEDA)? (Step 3)
- [ ] Where are the biggest disagreements — manufacturing, services, energy, or waste? (Steps 3–4)
- [ ] Does industry-output error meaningfully discriminate between approaches? (Step 5)
- [ ] Which approach's EF results land closest to each baseline (USEEIO published EFs and CEDA-US production default)? (Step 6)
- [ ] Is the price-index assumption defensible for discretionary-spending sectors, or does it introduce noise? (Step 4)
- [ ] Is the summary-table 0.98 cap firing often enough to be a concern? (Step 3)

---

## Open Decisions Before Starting

1. **Time-series span**: brief calls for 2018–2024 plus the 2017 benchmark = 7 years; the current Step 1 list `{2017, 2018, 2019, 2020, 2021, 2022, 2024}` skips 2023. Confirm — do we have summary A tables and price indices for all 7 listed years? `USA_SUMMARY_MUT_YEARS` in [bedrock/utils/taxonomy/bea/matrix_mappings.py] caps `model_base_year` at `{2022, 2023, 2024}` — so the scale-method approaches likely cannot produce 2018–2021 directly without summary-table availability gaps. **Action: read `derive_summary_Adom_usa()` and enumerate the supported years before committing to the final span; document any year that has to be skipped per approach.**
2. ~~**Reference baseline for "divergence from"**~~ — **resolved**: use both USEEIO and CEDA-US as paired baselines on every comparison (see "Comparison baselines" section at the top). In Step 6, both baselines are produced by the diagnostics workflow itself — CEDA-US automatically (as v0), USEEIO via the workflow's checkbox.
3. **Output destination**: add as a new `bedrock/analysis/a_matrix_time_series/` module vs extend `time_series_B_matrix`? New module keeps scope clean.
4. **Significance threshold for outliers**: the `diagnostics_visual_plan.md` uses `|perc_diff| > 1.0` (100%). Reuse that unless evidence says otherwise.

---

## Dependencies / Risks

- **USEEIO PR freshness**: notes say "may need minor code updates since PR is from a while ago". Rebase + smoke-test the USEEIO YAML end-to-end before investing in comparisons. (USEEIO is a baseline, so the analysis is dead in the water until this is reliable.)
- **Memory / runtime**: caching × 5 approaches × 7 years = 35 A matrices (USEEIO contributes 1 since it's year-invariant, so practically ~31). Leverage `@functools.cache` + parquet caching; only do full-model propagation (Step 6) for the single target year, not all 7.
- **No element-wise ground truth**: be explicit about this limitation in the write-up. The recommendation must be grounded in a combination of (industry output error + EF plausibility + methodological transparency), not in a single "best" metric.
- **V-norm math must be verified**: Step 0's transform logic must be reviewed before merge. Getting this wrong silently produces a plausible-but-meaningless 5th approach.

---

## Suggested Milestones (rough sizing)

These are sizing estimates, not commitments. Adjust after Checkpoint A.

| Milestone | Steps | Rough effort | Gate |
|---|---|---|---|
| M1 — V-norm branch merged | Step 0 | 2–3 days | Checkpoint A |
| M2 — Cache populated | Step 1 | 1 day | Checkpoint B |
| M3 — Cross-approach plots | Steps 2–3 | 2–3 days | Checkpoint C |
| M4 — Sector deep-dives + output ground truth | Steps 4–5 | 2 days | — |
| M5 — EF compile + write-up | Step 6 + README | 1–2 days | Final review |
| M6 (optional) — V-norm deep dive | Step 7 | 1 day | Checkpoint D |

Total: roughly 1.5–2 working weeks for the core path (M1–M5).

---

## Reference Files

| File | Role |
|---|---|
| [bedrock/transform/eeio/derived_cornerstone.py](../bedrock/transform/eeio/derived_cornerstone.py) | `derive_cornerstone_Aq_scaled()` — the gated entry point (L498) |
| [bedrock/transform/eeio/cornerstone_year_scaling.py](../bedrock/transform/eeio/cornerstone_year_scaling.py) | `scale_cornerstone_A` (0.98 cap logic) |
| [bedrock/utils/economic/inflate_cornerstone_to_target_year.py](../bedrock/utils/economic/inflate_cornerstone_to_target_year.py) | `inflate_cornerstone_A_matrix` (diagonalization) |
| [bedrock/transform/iot/derived_gross_industry_output.py](../bedrock/transform/iot/derived_gross_industry_output.py) | `derive_gross_output_after_redefinition()` — ground-truth anchor |
| [bedrock/analysis/time_series_B_matrix/derive_B_time_series.py](../bedrock/analysis/time_series_B_matrix/derive_B_time_series.py) | Template for time-series caching + plotting module layout |
| [bedrock/utils/validation/calculate_ef_diagnostics.py](../bedrock/utils/validation/calculate_ef_diagnostics.py) | Existing EF diagnostics pipeline (Step 6) |
| [.claude/plans/issue_182_a_matrix_methods.md](issue_182_a_matrix_methods.md) | Background analysis of the three methods |
| [.claude/plans/issue_182_implementation_plan.md](issue_182_implementation_plan.md) | How the three flags were wired in |
