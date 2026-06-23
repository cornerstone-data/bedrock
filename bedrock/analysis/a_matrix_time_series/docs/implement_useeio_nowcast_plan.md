# USEEIO Nowcast Integration — Plan

## Goal

Integrate the upstream USEEIO team's **nowcasted detailed Supply-Use Tables** (2018–2023) into [bedrock/analysis/a_matrix_time_series/](.) as an additional A-matrix approach and as an independent external anchor for Step 5 of [analysis_plan.md](analysis_plan.md). End state: every figure and CSV in the existing analysis carries one extra series (`useeio_nowcast`) so reviewers can read off "how do bedrock's five approaches line up against an externally-balanced 2023 detail SUT?"

Background on what the upstream pipeline does and what it captures: [/USEEIO_nowcasting.md](../../../USEEIO_nowcasting.md). Source artifacts live at `~/Desktop/nowcasting/final nowcasted tables/` (V_out, U_out, U_imports_out CSVs for 2017–2023).

## TL;DR

1. Stage the USEEIO nowcasted CSVs to GCS (read-only, versioned by upload-date).
2. Add a 6th approach `useeio_nowcast` whose A-matrix is derived from the upstream V/U/U_imports tables instead of from the bedrock 2017 base + a scaling/inflation rule. The YAML is configurable but the derivation runs *outside* `derive_cornerstone_Aq_scaled()` — this is exogenous data, not a method.
3. Build a one-shot loader that maps BEA Detail Code_Loc → Cornerstone schema, derives `Adom`/`Aimp` per year, and writes them into the same parquet layout as the existing approaches: `output/results/A_useeio_nowcast_{year}.parquet`.
4. Re-run Steps 1–5 of the existing plan with the new approach added to `APPROACH_ORDER`. Skip Step 0 (no new flag). Skip Step 6 unless the nowcast is selected as a candidate.
5. Step 5 (industry-output anchor) gets a parallel **A-matrix anchor**: compare each of the five internal approaches' `A_2023` against `A_useeio_nowcast_2023` cell-by-cell. This is the closest thing to external ground truth that exists at detail resolution.

---

## Why this is worth doing

The existing analysis lacks an external A-matrix reference. Step 5 uses observed BEA annual gross output as anchor — but as the plan itself notes, gross output is "column-sum-adjacent" and can agree even when individual A cells are wrong. The USEEIO nowcasted SUTs give us, for the first time, a detail-resolution A matrix for 2018–2023 that was balanced against BEA's annual summary SUTs by an independent EPA-funded team using GRAS (see [USEEIO_nowcasting.md](../../../USEEIO_nowcasting.md)). It's not perfect (it freezes 2017 within-summary technology, the same fundamental limitation as bedrock's `summary_tables` method — documented in that doc's "what is NOT captured" section), but it's the only externally-balanced detail-level series available, and the methodological overlap with `summary_tables` is exactly what makes it informative: it isolates where the bedrock alternatives disagree from a published implementation of the same conceptual approach.

---

## Inputs

| Input | Source | Format | Status |
|---|---|---|---|
| `V_out_{yr}.csv` | `gs://cornerstone-default/extract/input-data/USEEIO_nowcasted_MUTs/` | commodity × industry, BEA Detail bare codes (no `/US`), mUSD | ✅ staged |
| `U_out_{yr}.csv` | same | commodity × (industry + FD + VA), BEA Detail bare codes, mUSD | ✅ staged |
| `U_imports_out_{yr}.csv` | same | commodity × (industry + FD), mUSD | ✅ staged |
| BEA Detail → Cornerstone schema map | bedrock's existing `cs_commodity_to_bea_map()` + `expand_square_matrix(zero_intragroup_cross_terms=True)` | reused | ✅ exists |
| 2017 Vnorm (scrap-corrected) | bedrock's existing `bea_Vnorm_scrap_corrected()` | reused | ✅ exists |
| Unorm / Vnorm helpers | bedrock's existing `compute_Unorm_matrix`, `compute_Vnorm_matrix` | reused | ✅ exists |

Years available: **2017, 2018, 2019, 2020, 2021, 2022, 2023** (7 files per table × 3 tables = 21 CSVs).

Gap vs. existing plan: **no 2024** (USEEIO pipeline hasn't been run for 2024 upstream). Strategy: drop 2024 from cross-approach plots that include `useeio_nowcast`, OR carry-forward 2023 with an explicit "extrapolated" label. **Recommend drop.**

Format note: USEEIO column/row headers are **bare BEA Detail codes** (e.g. `1111A0`, `211000`) — no `/US` suffix. Bedrock's internal Cornerstone codes carry no suffix either, so loaders can pass codes through unchanged, but `bea_v2017_to_ceda_v7_helpers.py` and adjacent code that expects `code/US` will need stripping at the boundary.

---

## Pre-requisites

### P0 — Stage source data to GCS — ✅ DONE

21 CSVs at `gs://cornerstone-default/extract/input-data/USEEIO_nowcasted_MUTs/` (verified via `gsutil ls`). Upstream branch state: `cornerstone-data/USEEIO@nowcasting` HEAD `2025-09-30` (per [USEEIO_nowcasting.md](../../../USEEIO_nowcasting.md)). Authoritative method writeup: `USEEIO_nowcasting_2025_10_15_rev1.docx` (Wood/Vendries/Young).

**Follow-up nice-to-have (non-blocking):** add a `MANIFEST.txt` next to the CSVs recording the upstream branch SHA and rev1 doc version for reproducibility. Can land alongside Step N1.

### P1 — BEA Detail → Cornerstone schema map — ✅ ALREADY EXISTS

Bedrock's `derive_cornerstone_Aq()` ([derived_cornerstone.py:448](../../../bedrock/transform/eeio/derived_cornerstone.py#L448)) already does BEA Detail (~400) → Cornerstone (~405) expansion via:

- `cs_commodity_to_bea_map()` — the mapping itself
- `expand_square_matrix(matrix, CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True)` — applies it with intragroup-zeroing to prevent Leontief-inverse inflation
- `expand_vector(vec, CS_COMMODITY_LIST, com_map)` — vector variant

These are the same helpers used when waste disaggregation is **off**. `useeio_nowcast` will use them as-is — no new crosswalk needed.

**Caveat:** when production YAML has waste disagg **on** (`get_waste_disagg_weights()` returns non-None), `derive_cornerstone_Aq()` takes a different path with disaggregated V/U in Cornerstone space. USEEIO upstream does **not** disaggregate waste — its V/U are at BEA Detail. So `useeio_nowcast` always goes through the `expand_square_matrix` path regardless of comparison-baseline YAML settings. For an apples-to-apples comparison, **run all 6 approaches with waste disagg off** in this analysis, OR explicitly call out that waste rows are not comparable.

---

## Steps

### Step N1 — Load USEEIO nowcasted tables and derive A matrices

**Pre-flight smoke-test findings (2017 + 2023, see [`investigation/useeio_nowcast_smoke_2023.py`](investigation/useeio_nowcast_smoke_2023.py)):**

- Industry sets match exactly (bedrock and USEEIO both use bare BEA Detail codes, no `/US` suffix).
- USEEIO `U_out` includes 3 VA rows (`V00100, V00200, V00300`) appended below 402 commodity rows — harmless, dropped by reindex to bedrock's commodity space.
- Bedrock's `bea_Vnorm_scrap_corrected` has an index-alignment quirk that pads to 406 commodity columns (`331314, S00101, S00201, S00202`) — these are non-real commodities and get dropped by `expand_square_matrix`. **Pre-existing bedrock behavior; not a USEEIO issue.**
- 2023 Cornerstone-space output is clean: `(405, 405)`, no NaN, no negatives, all column sums ≤ `COLUMN_CAP` after cap is applied.
- Decisions 8/9/10 above were settled by the smoke test outputs.

**Code lives in three layers, matching bedrock's `extract/` → `transform/` → `analysis/` convention:**

| Layer | File | Role |
|---|---|---|
| Extract | [`bedrock/extract/iot/useeio_nowcast.py`](../../extract/iot/useeio_nowcast.py) | GCS loaders for `V_out`, `U_out`, `U_imports_out` (transposes V to industry × commodity, slices intermediate-Use block, `@functools.cache` per year). Owns `USEEIO_NOWCAST_YEARS` and `USEEIO_NOWCAST_INDUSTRY_COUNT`. |
| Transform | [`bedrock/transform/eeio/derived_useeio_nowcast.py`](../../transform/eeio/derived_useeio_nowcast.py) | A-matrix derivation: `compute_Unorm_matrix` → Vnorm (inline scrap-corrected) → `compute_A_matrix` → `expand_square_matrix`. Applies negative-clip + 0.98 cap. Exposes `derive_useeio_nowcast_Aq_cornerstone(year)` returning `SingleRegionAqMatrixSet`. Called from `derive_cornerstone_Aq_scaled()`'s new branch. |
| Analysis | [`derive_useeio_nowcast_A.py`](derive_useeio_nowcast_A.py) | Thin driver: loops `USEEIO_NOWCAST_YEARS`, calls the transform function, writes parquet caches in the same layout `_loaders.load_a_pair` expects. |

**Per year `yr ∈ {2018..2023}` (and 2017 as identity sanity check):**

1. Read the three CSVs from `gs://cornerstone-default/extract/input-data/USEEIO_nowcasted_MUTs/` into pandas DataFrames. Row/column headers are bare BEA Detail codes.
2. Slice the intermediate Use blocks: `U_out_{yr}` columns `[0:numIndustries]` → intermediate use (commodity × industry); drop FD and VA columns. Same slicing for `U_imports_out_{yr}` to get the imports intermediate block (commodity × industry). Confirm column counts match `len(detail_industry_codes)` from `derive_cornerstone_Aq()`.
3. Subtract: `U_dom_intermediate = U_intermediate − U_imports_intermediate`.
4. Compute industry gross output: `x = V_out_{yr}.sum(axis=0)`. Sanity-check for `yr=2017` that this matches bedrock's `Detail_GrossOutput_IO` to numerical tolerance.
5. Apply bedrock's existing helpers (no new math) to produce BEA-space A matrices:
   ```python
   Unorm_dom = compute_Unorm_matrix(U=U_dom_intermediate, x=x)
   Unorm_imp = compute_Unorm_matrix(U=U_imports_intermediate, x=x)
   Vnorm = bea_Vnorm_scrap_corrected()  # bedrock's 2017 V-norm — see decision #2 below
   Adom_bea = Unorm_dom @ Vnorm
   Aimp_bea = Unorm_imp @ Vnorm
   ```
6. Expand to Cornerstone schema using bedrock's existing path:
   ```python
   com_map = cs_commodity_to_bea_map()
   Adom = expand_square_matrix(Adom_bea, CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True)
   Aimp = expand_square_matrix(Aimp_bea, CS_COMMODITY_LIST, com_map, zero_intragroup_cross_terms=True)
   ```
7. **Clip negative U cells to 0** with magnitude logging (decision #9 above). Worst negatives in 2023 are ~$2k mUSD on intra-electronics intermediate blocks — reconciliation noise, not real demand.
8. **Apply the 0.98 column cap** to Adom and Aimp separately (decision #10 above), mirroring `scale_cornerstone_A`'s post-processing. Only `S00102` (noncomparable imports) is observed to need it in 2023.
9. Sanity asserts (mirror `derive_cornerstone_Aq()`): `(Adom >= 0).all()`, `(Aimp >= 0).all()`, no NaNs, all column sums ≤ 1.
10. Write to `output/results/A_useeio_nowcast_{yr}.parquet` in the same long-form (kind={dom,imp}) layout that `_loaders.py::load_a_pair` expects.

**Decision recap embedded in step 5:**
- **Vnorm year**: use bedrock's 2017 `bea_Vnorm_scrap_corrected()` rather than computing a year-specific V-norm from `V_out_{yr}`. Rationale: keeps the variable-of-interest (input shares from `U_out_{yr}`) clean — only one channel of structural change. Mirrors the `commodity_price_index` approach's convention.
- **Imports split**: USEEIO already published `U_imports_out_{yr}` as a separate matrix balanced to summary imports. Use it directly — do not re-derive imports shares from a 2017 import share assumption (this is one of the things the nowcast actually updates).

**Module entrypoint:**
```python
python -m bedrock.analysis.a_matrix_time_series.derive_useeio_nowcast_A
```
which loops over years 2017–2023.

**Definition of Done for Step N1:**
- 7 parquet files written.
- `load_a_pair("useeio_nowcast", 2023)` returns `dom` and `imp` DataFrames with the same shape as `load_a_pair("summary_tables", 2023)`.
- Cache-summary row appended for each year/kind.
- Column sums of `Adom` (and `Aimp`) are ≤ `COLUMN_CAP` for every column after the cap is applied. Log which columns hit the cap.
- ~~2017 identity check vs bedrock `bea_Aq()`~~ — **NOT required.** The smoke test confirms USEEIO 2017 differs from bedrock 2017 by ~9% L1 (Adom) / 22% L1 (Aimp) due to different BEA file vintages (see decision #8). Record the gap in the cache-summary as informational; do not block on it.

### Step N2 — Wire `useeio_nowcast` into shared constants

Edit `constants.py`:

1. Append `"useeio_nowcast"` to `APPROACH_ORDER` after `commodity_price_index`. **Do not** add it to `ALTERNATIVE_APPROACHES` — it is not a candidate for the production method; it is an external reference.
2. Add a color (`#9467bd` — distinct from existing 5).
3. Define a new tuple `EXTERNAL_REFERENCES: tuple[str, ...] = ("useeio_nowcast",)` to formalize the "reference, not candidate" role. Update plot helpers (Step N3) to honor it.

`APPROACH_YAMLS` in `derive_A_time_series.py` does **not** get a new entry — the nowcast approach has no YAML because it isn't derived through `derive_cornerstone_Aq_scaled()`. Step N1's standalone module produces its parquet files directly.

### Step N3 — Re-run Steps 1–5 with the new approach

For each downstream step, the diff is bounded:

| Existing step | Change |
|---|---|
| **Step 1** (`derive_A_time_series.py`) | No change. `useeio_nowcast` parquets come from Step N1's separate module. Cache-summary tab gets 7 extra rows from Step N1. |
| **Step 2** (cell-level long-form table `A_cells_long.parquet`) | Loader iterates `APPROACH_ORDER` so it picks up `useeio_nowcast` automatically. Verify with row count = old × 6/5. |
| **Step 2.5** (Jaccard + persistence) | Auto-included via `APPROACH_ORDER`. |
| **Step 3** (`compare_approaches.py`) | Pairwise hexbins: add `(useeio_nowcast, summary_tables)`, `(useeio_nowcast, industry_price_index)`, `(useeio_nowcast, commodity_price_index)` to `PAIRS`. ECDF panels: add `useeio_nowcast` as a third reference series ("alternatives vs USEEIO-do-nothing", "vs CEDA-US", "vs USEEIO-nowcast"). Column-cap audit unchanged. |
| **Step 4** (key sectors) | Auto-included; small-multiple lines pick up the 6th series. |
| **Step 5** (industry output anchor) | Add a parallel **A-matrix anchor** sub-step: per approach and year, compute `||A_approach - A_useeio_nowcast||` (L1 per column, RMSE overall) for 2018–2023. Report as `step5_a_matrix_rmse_vs_useeio_nowcast.csv`. The headline message becomes "which approach tracks the externally-balanced nowcast most closely on both industry output and A-cell values?" |

**Step 6 is deferred.** Running EFs through the nowcast is expensive (full diagnostics workflow per approach × year) and only worth it if Step N3 surfaces the nowcast as a contender for the production recommendation. Decision gate: if `useeio_nowcast` lines up systematically with one of the three alternative methods in Step N3, add it as a 6th tile in Step 6 outputs. Otherwise skip.

### Step N4 — Update the write-up

Edit [analysis_plan.md](analysis_plan.md) (or write a sibling `README.md` for this sub-plan):

1. Add a row to the "five approaches" table for `useeio_nowcast` — role = "external reference", mechanism = "upstream EPA team's GRAS-balanced detail SUT, 2018–2023".
2. Add to **Key Questions**: "do any of the three alternative bedrock methods track the externally-balanced nowcast more closely than the others, and at which sectors?"
3. In the final recommendation section, treat `useeio_nowcast` alignment as one of three pieces of evidence (alongside industry-output error and EF plausibility). Caveat: the nowcast freezes 2017 within-summary technology — the same fundamental limitation as `summary_tables` — so close agreement with `summary_tables` is partly a methodological tautology, not validation. Call this out.

---

## Deliverables

### Code
- [x] `bedrock/extract/iot/useeio_nowcast.py` — GCS loaders (extract layer)
- [x] `bedrock/transform/eeio/derived_useeio_nowcast.py` — A-matrix derivation (transform layer)
- [x] `bedrock/analysis/a_matrix_time_series/derive_useeio_nowcast_A.py` — Step N1 driver (analysis layer)
- [x] `bedrock/utils/config/usa_config.py` — `load_useeio_nowcast_A_matrix` flag
- [x] `bedrock/transform/eeio/derived_cornerstone.py` — new branch in `derive_cornerstone_Aq_scaled()`
- [x] `bedrock/analysis/a_matrix_time_series/constants.py` — Step N2 updates (`APPROACH_ORDER`, color, `EXTERNAL_REFERENCES`, `APPROACH_YEAR_COVERAGE`)
- [x] `bedrock/analysis/a_matrix_time_series/derive_A_time_series.py` — `APPROACH_YAMLS` + `_years_for()` filter + cache-clearing of the new module
- [x] `bedrock/analysis/a_matrix_time_series/compare_approaches.py` — `USEEIO_NOWCAST_PAIRS` + plot block
- [x] `bedrock/utils/config/configs/2025_usa_cornerstone_A_useeio_nowcast.yaml` — minimal config for A-matrix-only analysis (Steps N1–N3)
- [x] `bedrock/utils/config/configs/2025_usa_cornerstone_v0_2_A_useeio_nowcast.yaml` — full v0.3 model for EF diagnostics (Step N4 — runs `generate_diagnostics` workflow end-to-end)

### Data artifacts (under `bedrock/analysis/a_matrix_time_series/output/results/`)
- [ ] `A_useeio_nowcast_{yr}.parquet` for `yr ∈ 2017..2023`
- [ ] Updated `cache_summary.csv` (7 extra rows × `dom`/`imp` = 14)
- [ ] Updated `A_cells_long.parquet` (+ ~1M rows)
- [ ] `step5_a_matrix_rmse_vs_useeio_nowcast.csv` — new ground-truth-anchor diagnostic
- [ ] `useeio_nowcast_MANIFEST.txt` — provenance: upstream branch SHA, report version, GCS upload date

### Figures (under `output/plots/`)
- [ ] All existing Step 2–5 figures rebuilt with the 6th series visible (`useeio_nowcast` color = `#9467bd`)
- [ ] New `step3_divergence_ecdf_vs_useeio_nowcast.png` — alternatives' divergence against the nowcast (parallel to the existing two ECDF panels against USEEIO-do-nothing and CEDA-US)
- [ ] New `step5_a_matrix_rmse_ranking.png` — bar chart of L1/RMSE per approach × year vs. `useeio_nowcast` (parallel to the existing industry-output RMSE bar chart)

### Write-up
- [ ] `bedrock/analysis/a_matrix_time_series/README.md` — append a "USEEIO nowcast integration" section summarizing what changed and how to interpret the new series.
- [ ] One-paragraph addition to `analysis_plan.md` listing `useeio_nowcast` in the approach table with its role.

---

## Open decisions before starting

1. **Year coverage**: drop 2024 from the `useeio_nowcast` line, OR carry-forward 2023 with an "extrapolated" label. **Recommend drop**; the gap is informative.
2. ~~**V-norm year**: 2017 vs year-specific~~ — **resolved in Step N1: use bedrock's 2017 `bea_Vnorm_scrap_corrected()`** for cleanest comparison and to keep one channel of structural change at a time.
3. ~~**Imports treatment**~~ — **resolved: use USEEIO's `U_imports_out_{yr}` directly** rather than re-deriving from a 2017 imports-share assumption. The imports update is one of the things the nowcast meaningfully adds.
4. ~~**GCS bucket**~~ — **resolved: `gs://cornerstone-default/extract/input-data/USEEIO_nowcasted_MUTs/` (already uploaded).**
5. ~~**Crosswalk for 1→N**~~ — **resolved: bedrock's `expand_square_matrix(zero_intragroup_cross_terms=True)` + `cs_commodity_to_bea_map()` already handle this.** Same as `derive_cornerstone_Aq()`'s non-disagg branch.
6. **Should we run Step 6 (EF diagnostics) with the nowcast at all?** Default: no, unless Step N3 surfaces it as a contender. Re-evaluate after Step N3.
7. **Waste disaggregation flag during the comparison**: turn the comparison runs' `get_waste_disagg_weights()` **off** (so all 6 approaches go through the BEA-detail → Cornerstone expand path) — OR keep it on for the other 5 and mark waste rows non-comparable for `useeio_nowcast`. **Recommend off for the comparison runs**, since the diagnostic value is comparing input-share methodology, not waste disaggregation.
8. ~~**2017 identity tolerance**~~ — **resolved: PARKED.** The 2017 USEEIO_nowcast A does **not** equal bedrock's 2017 BEA A. Empirically (from the smoke test): Adom relative L1 ≈ 9.1%, Aimp relative L1 ≈ 22.1%, with diagonal manufacturing the hot spot (`336500/336500`, `336991/336991`, `333993`, `333994`, `334220/334210`). Probable root cause: USEEIO's R-data `Detail_Use_2017_PRO_AfterRedef` files differ from the BEA XLSX bedrock loads via `load_2017_Utot_usa` (different vintages of the same nominal benchmark). Definitive diagnosis is **parked** — we proceed treating `useeio_nowcast` as an independent reference, not as a strict identity-at-2017 baseline. Document the gap in the README; do not gate Step N1 on it.
9. ~~**Negative cells in intermediate U from GRAS reconciliation**~~ — **resolved: clip to 0 and log magnitude.** ~1854 cells in U_dom for 2023 with summed magnitude ≈ 0.27% of \|U_dom\|; worst offenders are intra-electronics blocks (`334220→336411`, `334220→518200`, `333318→48A000`). These are reconciliation noise, not real negative input demand. The clip + log is implemented in the smoke test; carry it into Step N1 unchanged.
10. ~~**0.98 column-sum cap**~~ — **resolved: apply per matrix** (Adom and Aimp separately) inside Step N1, mirroring `scale_cornerstone_A`'s post-processing. Without the cap, USEEIO's `S00102` column (noncomparable imports) reaches 1.55 — a known edge case the cap already handles for the other approaches. Applying the cap keeps the apples-to-apples comparison across all 6 approaches clean.

---

## Risks / caveats

- **Methodological overlap with `summary_tables`.** USEEIO's GRAS pipeline conceptually overlaps with bedrock's `summary_tables` method — both use BEA summary SUTs as the constraint and freeze 2017 within-summary technology. Don't claim "USEEIO nowcast validates `summary_tables`" if they align — that's circular. The diagnostic value is the *opposite*: where bedrock's `summary_tables` *disagrees* with `useeio_nowcast`, the difference comes from bedrock-specific schema (Cornerstone disaggregation), the 0.98 column cap, or different aggregation timing — all worth investigating.
- **2017 identity check is approximate, not exact.** USEEIO's 2017 GRAS output ≠ the BEA 2017 benchmark exactly; the rev1 report documents persistent ~$0.1–0.3M deviations at 5412OP, GSLG, 81, 722, 23. Set the tolerance accordingly (1% L1 norm rather than `1e-10` like the other approaches' identity check).
- **Disaggregation may dominate the signal.** If a single BEA Detail code maps to N Cornerstone children, the nowcast carries no information about within-Cornerstone-child structure — all children inherit the parent's coefficients. For sectors with heavy disaggregation (waste, software), the nowcast tells us nothing the existing approaches don't.
- **Crosswalk fragility.** If the existing bedrock BEA-to-Cornerstone mapping isn't a strict refinement (i.e. some Cornerstone codes draw from multiple BEA Detail rows), the row-derivation is more complex than the simple distribute-by-share recipe. Verify direction of mapping early in P1.
- **Sign conventions.** USEEIO tables may include negative cells (inventory changes in FD, taxes/subsidies in VA). The intermediate-Use block is non-negative by construction, so deriving Adom/Aimp from intermediate Use only should produce non-negative A — but double-check on first 2023 run.
- **Imports row-sum identity.** USEEIO enforces `U_imports.sum(axis=1) ≈ 0` (US-tables identity); bedrock's Adom + Aimp split may use a different convention. Reconcile before claiming any disagreement is "real".

---

## Reference files

| File | Role |
|---|---|
| [/USEEIO_nowcasting.md](../../../USEEIO_nowcasting.md) | Full description of the upstream nowcasting pipeline, what it captures, and limitations |
| [analysis_plan.md](analysis_plan.md) | Parent plan; this plan extends Steps 1–5 |
| [_loaders.py](_loaders.py) | Parquet loader convention to mirror |
| [derive_A_time_series.py](derive_A_time_series.py) | Existing approach driver — useeio_nowcast bypasses this |
| [compare_approaches.py](compare_approaches.py) | Step 3 cross-approach hexbins — gets new PAIRS entries |
| [constants.py](constants.py) | `APPROACH_ORDER`, `APPROACH_COLORS` — extension point |
| `~/Desktop/nowcasting/final nowcasted tables/` | Source CSVs (21 files, 7 years × 3 tables) |
| `~/Desktop/USEEIO_nowcasting_2025_10_15_rev1.docx` | Authoritative method write-up by Wood/Vendries/Young |

---

## Suggested sizing

| Phase | Effort | Gate |
|---|---|---|
| P0 + P1 (staging + crosswalk) | 0.5–1 day | crosswalk reviewed |
| Step N1 (derive A from nowcast) | 1–2 days | 2017 identity check passes (1% L1 tol) |
| Step N2 + N3 (rewire + re-run downstream) | 1 day | Updated figures reviewed |
| Step N4 (write-up) | 0.5 day | README amended |

**Total: ~3–4 working days** for the core path, assuming the BEA-Detail→Cornerstone crosswalk already exists in usable form. If P1 needs new disaggregation weights, add 1–2 days.
