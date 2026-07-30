# Methods Discussion #85 — Analysis Report

Analysis-only exploration of Decisions 3, 5, and 7 under
`[bedrock/analysis/electricity/d_85/](../)`. Generated from the disaggregation
config
`[2025_usa_cornerstone_full_model_electricity_disaggregation.yaml](../../../utils/config/configs/2025_usa_cornerstone_full_model_electricity_disaggregation.yaml)`.

## Run metadata


| Parameter           | Value                                                                                |
| ------------------- | ------------------------------------------------------------------------------------ |
| `model_base_year`   | 2023                                                                                 |
| `usa_io_data_year`  | 2022                                                                                 |
| `usa_ghg_data_year` | 2023                                                                                 |
| E source            | Cached national GCS FBS (`flowsa` not installed locally)                             |
| GO precondition     | 0.39% residual on aggregate `221100` absorbed into VA before PR3 (post-reallocation) |


**Companion artifacts**


| Decision | Excel report                                                                   |
| -------- | ------------------------------------------------------------------------------ |
| 3        | `[decision3_table83_report.xlsx](decision3_table83_report.xlsx)`               |
| 5        | `[decision5_table24_report.xlsx](decision5_table24_report.xlsx)`               |
| 7        | `[decision7_ugo305_scaling_report.xlsx](decision7_ugo305_scaling_report.xlsx)` |
| All      | `[analysis_summary.json](analysis_summary.json)`                               |


**Figures (Decisions 3 & 5)** — embedded in the sections below; regenerate with
`python -m bedrock.analysis.electricity.d_85.figures`.


| Figure | File                                                                                   | Section                                 |
| ------ | -------------------------------------------------------------------------------------- | --------------------------------------- |
| A      | `[figure_a_pr3_scenario_map.png](figure_a_pr3_scenario_map.png)`                       | Background — PR3 steps                  |
| C      | `[figure_c_step2_intersection_matrices.png](figure_c_step2_intersection_matrices.png)` | Decision 3 — intersection weights       |
| D      | `[figure_d_market_clearing_gaps.png](figure_d_market_clearing_gaps.png)`               | Decision 5 (covers D3 and D5 scenarios) |


> **Note:** `analysis_summary.json` may list legacy scenario IDs (`d8_mixed`,
> `d8_offdiag`) until `_run_summary.py` is re-run with current code. Balance
> tables in this report use the renamed IDs and include purchased-power scenarios.

Regenerate figures only:

```powershell
.\.venv\Scripts\python.exe -m bedrock.analysis.electricity.d_85.figures
```

**Re-run**

```powershell
.\.venv\Scripts\python.exe bedrock/analysis/electricity/d_85/output/_run_summary.py
```

---

## Background — PR3 electricity disaggregation (steps 1–4)

All Decision 3 and 5 scenarios start from the **stage-2 checkpoint**: waste
disaggregation and co-production reallocation are complete, but aggregate sector
`221100` is still intact in V, U, and Y. PR3 then splits that aggregate into
three child sectors — `221110` (generation), `221121` (transmission), and
`221122` (distribution) — using normalized weight shares. Production baseline
uses UGO305-A 2017 gross-output weights for every step; non-baseline scenarios
override one step at a time while holding the others on UGO305.


| Step                         | Matrix               | What it does                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1 — Make intersection**    | `V`                  | Splits the `221100` Make diagonal into a 3×3 diagonal block (each child commodity maps to its matching industry column). Preserves total Make industry and commodity totals.                                                                                                                                                                                                                                                        |
| **2 — Use intersection**     | `Udom`, `Uimp`       | Replaces the single aggregate cell `U[221100, 221100]` with a 3×3 block on the Use diagonal (commodity × industry). Total dollars in the intersection cell are preserved (`T`).                                                                                                                                                                                                                                                     |
| **3 — Use industry columns** | `Udom`, `Uimp`, `VA` | Splits the aggregate `221100` **industry column** (all commodity rows that purchase electricity as an input). Rebalances VA so each child column sums to its gross output `x_k = w_k × x_agg`. Preserves purchaser column totals and industry GO (`q = x`). **Fuel commodities** (`212100`, `211000`, `324110`, `424700`, `221200`) purchasing the aggregate industry column are routed **100% to `221110`**, not split by weights. |
| **4 — Commodity rows**       | `Udom`, `Uimp`, `Y`  | Splits the aggregate `221100` **commodity row** (electricity sales) across purchaser industry columns and final-demand columns in `Y` using weight shares. Decision 5 replaces uniform weights with Table 2.4 price-tilted per-column weights. No separate fuel routing at this step — fuel was already assigned in step 3.                                                                                                         |


After step 4, aggregate `221100` is removed from V, U, and Y and the IO is
reindexed to the 407-sector electricity schema. **Decision 7** does not alter
these disaggregation steps; it simulates a different **year-scaling** path on the
resulting A/q matrices after baseline disaggregation is complete.

### Figure A — PR3 scenario map

![Figure A: PR3 pipeline with Decision 3 and Decision 5 overrides](figure_a_pr3_scenario_map.png)

**Figure A** is a schematic of the four PR3 steps and which step each analysis
scenario overrides. Production baseline applies UGO305-A 2017 weights at every
step. **Decision 3** scenarios change **step 2 only** (the Use-intersection
3×3 block) while steps 1, 3, and 4 stay on UGO305. **Decision 5** scenarios
change **step 4 only** (commodity-row and final-demand splits in `U` and `Y`)
using Table 2.4 price tilts; steps 1–3 stay on UGO305. **Decision 7** is not
shown here because it does not alter these steps — it runs a post-disaggregation
year-scaling simulation on the resulting matrices.

---

## Decision 3 — Table 8.3 intersection weights

**Question:** How do EPA Table 8.3 IOU expense shares differ from UGO305-A GO
weights, and what happens if step-2 Use intersection uses 8.3 (diagonal or
hybrid off-diagonal) while steps 1, 3, 4 stay on UGO305?

### Weight comparison (2017)


| Source                            | `w_221110` (Generation) | `w_221121` (Transmission) | `w_221122` (Distribution) |
| --------------------------------- | ----------------------- | ------------------------- | ------------------------- |
| UGO305-A                          | 34.2%                   | 3.9%                      | 61.9%                     |
| Table 8.3 — Production + T/D      | 86.7%                   | 9.5%                      | 3.8%                      |
| Table 8.3 — Purchased Power + T/D | 76.4%                   | 16.8%                     | 6.8%                      |


Two Table 8.3 weight sets map IOU operating expenses to G/T/D shares. **Production**
uses `expenses: Production`; **Purchased Power** substitutes `expenses: Purchased Power` for generation while keeping the same transmission and distribution line
items. Both are IOU-only and heavily generation-weighted relative to UGO305; Purchased
Power shifts some share from generation toward transmission vs Production.

### Scenarios

Each scenario below changes **step 2 only** (Use intersection). Steps 1, 3,
and 4 use UGO305 weights; step 4 row/Y splits are uniform (equal-price).


| ID                                 | Meaning                                                                                                                                                                                            |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `**baseline`**                     | Production PR3 path: UGO305-A 2017 weights at all four steps. Reference for balance and EF comparisons.                                                                                            |
| `**t8.3_production_diag**`         | **Table 8.3 Production diagonal at step 2.** The 3×3 Use-intersection block uses Production + T/D expense shares (formerly `d8_mixed`). Purely diagonal.                                           |
| `**t8.3_production_offdiag`**      | **Hybrid off-diagonal with Production weights.** UGO column totals (Rule D1); generation column 100% diagonal; T/D columns split rows by Production + T/D shares (Rule D2). Formerly `d8_offdiag`. |
| `**t8.3_purchased_power_diag`**    | Same as `t8.3_production_diag`, but step-2 weights use **Purchased Power** + T/D instead of Production + T/D.                                                                                      |
| `**t8.3_purchased_power_offdiag`** | Same as `t8.3_production_offdiag`, but off-diagonal row splits use **Purchased Power** + T/D shares.                                                                                               |



| ID                             | Step 2 change                                 | Result                                 |
| ------------------------------ | --------------------------------------------- | -------------------------------------- |
| `baseline`                     | UGO305 diagonal                               | Reference PR3                          |
| `t8.3_production_diag`         | Production + T/D diagonal                     | VA positive; commodity clearing broken |
| `t8.3_production_offdiag`      | Hybrid off-diagonal (Production weights)      | **metrics_only** — VA balancing failed |
| `t8.3_purchased_power_diag`    | Purchased Power + T/D diagonal                | VA positive; commodity clearing broken |
| `t8.3_purchased_power_offdiag` | Hybrid off-diagonal (Purchased Power weights) | **metrics_only** — VA balancing failed |


### Figure C — Step-2 Use intersection matrices

![Figure C: Step-2 Use intersection 3x3 heatmaps for four Decision 3 scenarios](figure_c_step2_intersection_matrices.png)

**Figure C** shows the 3×3 Use-intersection block that replaces aggregate cell
`U[221100, 221100]` at PR3 step 2. Rows are electricity commodity codes
(generation, transmission, distribution); columns are the matching industry
columns. Cell values are in billions of dollars; the block total is preserved
across all panels.

- **Baseline (left):** UGO305 diagonal — distribution carries ~62% of
intersection dollars, generation ~34%.
- **Production + T/D diagonal:** Table 8.3 Production expense shares on the
diagonal — generation dominates (~87%), compressing T/D shares vs baseline.
- **Production hybrid off-diagonal:** UGO column totals (Rule D1) with
generation column 100% on-diagonal and T/D columns split across rows by
Production weights (Rule D2). Off-diagonal cells appear in the T/D rows.
- **Purchased Power + T/D diagonal:** Same structure as Production diagonal but
with Purchased Power replacing Production for the generation share; more weight
shifts to transmission (17% vs 10%) and distribution (7% vs 4%).

The purchased-power off-diagonal variant (`t8.3_purchased_power_offdiag`) is
omitted from this figure because its pattern mirrors the production off-diagonal
panel with different row-split weights; both fail VA balancing at step 3.

### Balance and VA


| Scenario                     | metrics_only | VA 221110 ($B)    | Max market-clearing gap |
| ---------------------------- | ------------ | ----------------- | ----------------------- |
| baseline                     | No           | 70.2              | ~$5.6M (baseline noise) |
| t8.3_production_diag         | No           | 64.3              | ~$6.5B (distribution)   |
| t8.3_production_offdiag      | **Yes**      | 0 (step 3 failed) | ~$16.5B (generation)    |
| t8.3_purchased_power_diag    | No           | 65.5              | ~$6.2B (distribution)   |
| t8.3_purchased_power_offdiag | **Yes**      | 0 (step 3 failed) | ~$14.9B (generation)    |


Per-child **q = x** (industry GO) is preserved exactly in all scenarios that
complete step 3. **Commodity market clearing** (`Use row + Y − q`) degrades when
step-2 intersection diverges from Make-side weights:


| Scenario                     | Gap 221110 | Gap 221121 | Gap 221122 |
| ---------------------------- | ---------- | ---------- | ---------- |
| baseline                     | −$3.1M     | −$0.4M     | −$5.6M     |
| t8.3_production_diag         | +$5.9B     | +$0.6B     | −$6.5B     |
| t8.3_production_offdiag      | +$16.5B    | +$1.0B     | −$6.4B     |
| t8.3_purchased_power_diag    | +$4.7B     | +$1.4B     | −$6.2B     |
| t8.3_purchased_power_offdiag | +$14.9B    | +$2.0B     | −$5.9B     |


Purchased Power diagonal reduces the generation surplus vs Production diagonal
(approx $4.7B vs $5.9B) and increases the transmission gap (approx $1.4B vs $0.6B), as
expected from the higher T-share in Purchased Power weights. Off-diagonal hybrids
fail step 3 for both weight sets. **Figure D** (in the Decision 5 section) plots
these gaps alongside Decision 5 scenarios for side-by-side comparison.

### EF diagnostics (tab C)

Direct **D**-vector changes vs baseline are **0%** on tracked significant
sectors (`221110`, `221121`, `221122`, `212100`, `331110`, `F01000`). The
analysis EF path computes `D = f(B)` where `B = (E/x) @ Vnorm` uses **Make**
matrices only. Step-2 **Use** intersection changes affect **A** but not **B** in
this pipeline. E attribution is fixed via `split_electricity_e_for_disaggregated_b()`.

Off-diagonal scenarios (`t8.3_production_offdiag`, `t8.3_purchased_power_offdiag`)
EF tabs are skipped (`metrics_only`).

### Decision 3 conclusion

- `**t8.3_production_diag`** and `**t8.3_purchased_power_diag**` are IO-feasible
for VA/GO but break commodity clearing by billions — not production-ready without
rebalancing.
- `**t8.3_production_offdiag**` and `**t8.3_purchased_power_offdiag**` fail VA
balancing — reject for production unless a rebalancing method is added.
- Purchased Power weights are a modestly less extreme alternative to Production
weights but do not resolve clearing or VA failures.
- Table 8.3 expense shares should not replace UGO305 wholesale; the weight gap
is too large and structurally mismatched (IOU expenses vs national GO).

---

## Decision 5 — Table 2.4 price-differentiated row/Y splits

**Question:** If row/Y splits use Table 2.4 retail price tilts (steps 1–3 on
UGO305), what happens to q/x balance and EFs?

Steps 1–3 are unchanged (UGO305 throughout). Only **step 4** differs: each
purchaser column gets its own row/Y weight vector based on the EPA end-use class
of that column and the Table 2.4 retail price for that end-use relative to the
national average.

### Scenarios


| ID               | Meaning                                                                                                                                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `**baseline`**   | Uniform UGO305 weights at step 4 (equal-price split of the `221100` commodity row on U and Y). Same as production PR3.                                                                                                          |
| `**p24_2017**`   | Step 4 uses Table 2.4 **2017** retail prices to tilt row/Y weights. Higher residential prices shift share toward transmission/distribution columns; lower industrial prices shift share toward generation. Steps 1–3 unchanged. |
| `**p24_target`** | Same price-tilt method as `p24_2017`, but Table 2.4 prices at `**usa_ghg_data_year` (2023)** instead of 2017. Tests sensitivity to price year choice.                                                                           |


### End-use mapping coverage (aggregate 221100 purchases)


| EPA end-use    | Share of electricity purchases |
| -------------- | ------------------------------ |
| Commercial     | 40.7%                          |
| Residential    | 40.0%                          |
| Industrial     | 17.5%                          |
| Transportation | 1.8%                           |


Mapping follows rule-based NAICS/FD classification plus
`[data/cornerstone_to_epa_end_use.csv](../data/cornerstone_to_epa_end_use.csv)`
overrides.

### q vs x balance

Steps 1–3 unchanged → **Make q and industry x identical** across scenarios.
Row/Y price tilt breaks **commodity market clearing** only:


| Scenario   | `q_221110` | `x_221110` | Market-clearing gap 221110 |
| ---------- | ---------- | ---------- | -------------------------- |
| baseline   | $155.7B    | $155.7B    | −$3.1M                     |
| p24_2017   | $155.7B    | $155.7B    | **−$5.70B**                |
| p24_target | $155.7B    | $155.7B    | **−$5.79B**                |


Offsetting gaps on transmission (+$337M / +$342M) and distribution (+$5.35B /
$5.44B). `p24_2017` and `p24_target` are nearly identical (~1.5% larger gaps
for target-year prices).

### Figure D — Commodity market-clearing gaps (Decisions 3 & 5)

![Figure D: Market-clearing gaps by scenario and electricity child sector](figure_d_market_clearing_gaps.png)

**Figure D** plots the commodity market-clearing gap
$\text{gap}_k = \sum_j U_{k,j} + \sum_f Y_{k,f} - q_k$ for each electricity
child sector and scenario. Bars are grouped by scenario (baseline, four
Decision 3 variants, two Decision 5 variants); colors distinguish generation
(green), transmission (orange), and distribution (blue).

The **top inset** zooms to baseline only, where gaps are sub-million-dollar
rounding noise. The **main panel** uses billions of dollars for the
non-baseline scenarios. **Hatched bars** mark `metrics_only` scenarios where
step 3 VA balancing failed (`t8.3_production_offdiag`,
`t8.3_purchased_power_offdiag`); gaps are shown for diagnostics only.

Key patterns visible in the chart:

- **Decision 3 diagonal** scenarios create a large positive generation gap and
offsetting negative distribution gap (~$4.7–6.5B magnitude), consistent with
step-2 intersection weights diverging from Make-side UGO305 shares while
step 4 row/Y splits remain on UGO305.
- **Decision 3 off-diagonal** scenarios amplify generation surplus (~$15–17B)
before VA failure; hatched bars flag that these IOs did not complete step 3.
- **Decision 5** price-tilt scenarios show the mirror pattern: a large negative
generation gap (~$5.7B) with offsetting positive transmission and distribution
gaps, because row/Y weights shift sales toward T/D purchaser columns without
updating Make-side `q`.

Gaps sum to approximately zero across the three child sectors in each scenario
that completes step 3, confirming dollars are conserved but misallocated across
commodity rows.

### EF diagnostics

Direct **D** changes are **0%** vs baseline. Row/Y reallocation does not alter
**V** (Make); therefore **B** and direct **D** are unchanged in the current EF
pipeline.

### Decision 5 conclusion

- Price-differentiated row/Y splits are structurally viable for **purchaser
preservation** and **industry GO** but create ~$5.7B commodity-market
imbalance on generation.
- Any production implementation needs a documented rebalancing strategy (RAS,
intersection adjustment, margins) — analysis-only here.
- End-use mapping is dominated by Commercial + Residential (~81% combined);
Industrial is ~18%.

---

## Decision 7 — UGO305 differentiated year scaling

**Question:** What if Step-1 summary-ratio scaling uses per-child UGO305 detail
GO ratios instead of a shared Utilities `"22"` ratio?

Decision 7 is a **post-disaggregation scaling simulation**, not a change to
steps 1–4. After baseline PR3 IO is built, the analysis substitutes
per-child UGO305 gross-output growth ratios (2017 → `usa_io_data_year`) into the
summary-ratio step that normally applies one identical Utilities sector `"22"`
ratio to all three electricity children.

### Variants


| ID                                                           | Meaning                                                                                                                                                                                                                                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `**baseline` / `scenario_baseline` / `production_baseline`** | Current production scaling: identical Utilities `"22"` summary ratio applied to 221110, 221121, and 221122 in the 2017→2022 step, then per-industry PI inflation to model base year. `scenario_baseline` and `production_baseline` scaled q values match in this run.     |
| `**d7_pure**`                                                | Replace the shared `"22"` ratio with each child's own UGO305 detail GO ratio `ratio_k = GO_k(2022) / GO_k(2017)`. Generation, transmission, and distribution scale independently in Step 1.                                                                               |
| `**d7_anchored**`                                            | Same per-child UGO305 ratios as `d7_pure`, then rescaled so the **weighted mean** of electricity-block scale factors (weights = UGO305 2017 shares) equals the shared Utilities `"22"` ratio. Preserves aggregate Utilities scaling while allowing G/T/D differentiation. |


### Detail GO ratios (2017 → 2022)


| Sector                | UGO305 ratio_k | Shared Utilities `"22"` ratio |
| --------------------- | -------------- | ----------------------------- |
| 221110 (Generation)   | **1.623**      | 1.432                         |
| 221121 (Transmission) | **1.292**      | 1.432                         |
| 221122 (Distribution) | **1.331**      | 1.432                         |


Generation detail GO grew faster than transmission/distribution over this
period.

### Scaled q trajectories (after full A/q scaling chain, IO-year dollars)


| Variant                  | q_221110       | q_221121      | q_221122      |
| ------------------------ | -------------- | ------------- | ------------- |
| Baseline (shared `"22"`) | $203.4B        | $23.2B        | $368.5B       |
| d7_pure                  | $230.4B (+13%) | $20.9B (−10%) | $342.5B (−7%) |
| d7_anchored              | $230.9B        | $21.0B        | $343.2B       |


**Pure vs anchored** differ negligibly — anchored normalization preserves the
electricity-block weighted mean vs the shared Utilities summary step.

Scenario baseline scaled q matches production baseline scaled q in this run.

### EF diagnostics

Direct **D** changes are **0%** for both `d7_pure` and `d7_anchored`. Scaling
overrides enter via **A/q**; direct **D** derives from **B(V, x)** which is
unchanged. Upstream/indirect effects (via **L/M/N**) are not captured in the
direct-D tab.

### Decision 7 conclusion

- UGO305 detail ratios justify **differentiated G/T/D scaling in Step 1** — the
shared `"22"` ratio materially over-scales transmission and under-scales
generation relative to detail GO trends.
- **d7_anchored** is preferred if production must preserve block-level scale
equivalence with current Utilities summary behavior.
- This is the most straightforward production candidate among the three
decisions (post-hoc simulation here; production wiring would be in
`cornerstone_year_scaling.py`).

---

## Cross-cutting conclusions


| Decision | Primary finding                                                                                                                                 | Production readiness          |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| **3**    | 8.3 Production and Purchased Power weights both diverge from UGO305; diagonal step 2 breaks clearing (~$4.7–6.5B); off-diagonal hybrids fail VA | Not ready                     |
| **5**    | Price tilt preserves GO, breaks commodity clearing (~$5.7B); EF unchanged on direct D                                                           | Not ready without rebalancing |
| **7**    | Detail GO ratios separate G/T/D q trajectories meaningfully; pure ≈ anchored                                                                    | Strongest candidate           |


### EF pipeline caveat

All tab-C EF comparisons show ~0% direct-**D** change because:

1. **E attribution is fixed** — `split_electricity_e_for_disaggregated_b()` does
  not vary with IO weight experiments.
2. **Direct D uses B from V/x** — U-intersection and row/Y changes do not alter
  **V**; scaling overrides affect **A/q** but not **B** in `derive_B_from_scenario`.
3. **x basis mismatch** — scenario B uses 2017 Make-derived x; production
  baseline uses `derive_cornerstone_x_after_redefinition` at `usa_ghg_data_year`.

Indirect/blended EF comparisons would require extending the analysis pipeline
(e.g. full L/M/N diff, or B sensitivity with authoritative model-year x).

### RAS / commodity-market rebalancing

Decisions 3 and 5 both break the same identity while preserving Make-side
**q = x** and (for D5) purchaser column totals:

$$
\text{gap}_k = \sum_j U_{k,j} + \sum_f Y_{k,f} - q_k
$$

for electricity child commodities $k \in \{221110, 221121, 221122\}$. Step 2
(D3) or step 4 (D5) reallocates Use/Y without updating **q** from **V**, so
commodity rows no longer clear (~$5–7B in the scenarios above).

**Existing implementations**


| Location                  | Method                           | Notes                                                                                                                                                                                                                                                                                                                                      |
| ------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **bedrock**               | None                             | No RAS/IPFP/biproportional helper. PR3 uses deterministic splits + VA column balancing in `electricity_disaggregation.py`. Closest analogue is summary-table block scaling in `cornerstone_year_scaling.py` (not iterative balancing).                                                                                                     |
| **cornerstone-data/ceda** | Row-scale balance (production)   | `derive_AqY_set_oecd_with_balance()` scales off-diagonal **U** and **Y** rows by one factor per commodity so $q \approx U\mathbf{1} + Y\mathbf{1}$. Documented internally as **“half a RAS iteration”** — row adjustment only, no column preservation.                                                                                     |
| **cornerstone-data/ceda** | RAS/IPFP (design, not on `main`) | ABSR investigation (`projects/absr/national_accounting_balance_investigation.md`) describes full **iterative proportional fitting**: alternate row and column multipliers until margins match. Key lesson: row and column targets must have **consistent totals** (rescale column targets by $\sum r / \sum c$) or IPFP does not converge. |


**How full RAS could close D3/D5 gaps**

For scenarios that complete step 3 (`t8.3_production_diag`, `t8.3_purchased_power_diag`, `p24_*`):

1. **Seed** $X_0$: electricity rows `{221110, 221121, 221122}` × all purchaser
  industry columns and final-demand columns in **Y** (domestic + import Use, or
   run separately and sum).
2. **Row targets** (fixed from Make): $r_k = q_k$ from `compute_q(V)`.
3. **Column targets** (preserve economic story): $c_j = \sum_k X_{0,kj}$ — keep
  each purchaser’s total electricity bill unchanged (already preserved by D5;
   D3 preserves purchaser column totals from step 3).
4. **IPFP/RAS**: alternate row scaling $X \leftarrow \mathrm{diag}(r / X\mathbf{1})X$ and column scaling $X \leftarrow X\mathrm{diag}(c / X^\top\mathbf{1})$ until convergence; reconcile $\sum r$ and $\sum c$ before iterating if needed.
5. **Write back** into `Udom`/`Uimp`/`Y`; do **not** change **V** or **q**.


| Scenario                           | RAS feasibility                                                    | Caveat                                                                   |
| ---------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| **D3 `t8.3_production_diag`**      | Good — gaps sum to ~$0 across G/T/D; **q = x** holds               | Redistributes dollars; does not enforce Table 8.3 intersection *pattern* |
| **D3 `t8.3_purchased_power_diag`** | Good — same structure; slightly smaller gen gap (~$4.7B)           | Same as Production diagonal                                              |
| **D5 `p24_*`**                     | Good — same row-target setup; purchaser totals preserved by design | Price-tilt *shares* become soft (seed-shaped) after balancing            |
| **D3 `t8.3_*_offdiag`** | **Insufficient alone** — step 3 VA balancing failed | Needs GRAS or step 2/3 repair before row-level RAS |


**Recommendations for production**

1. **Prefer full IPFP over CEDA-style row-scale** for PR3 experiments: row-scale
  closes commodity clearing but **sacrifices purchaser column totals**, which
   D5 explicitly preserves.
2. **Add a small bedrock utility** (e.g. `bedrock/utils/math/ras.py`) with
  tested IPFP on nonnegative seeds; document tolerance and max-iteration
   defaults. No need to port CEDA’s unmerged MRIO balance module — the
   electricity case is a **3 × N** block, not full MRIO.
3. **Wire as an optional post-step-4 hook** in analysis first: run RAS on
  unbalanced scenarios, report pre/post market-clearing gaps and max relative
   cell change vs seed. Gate production on acceptance criteria (e.g. gap <
   `$1M`, max cell drift < 5% except within the 3×3 intersection block).
4. **Do not expect RAS to fix `t8.3_*_offdiag`** — treat VA/column failure separately
  (GRAS with VA rows, or reject hybrid off-diagonal intersection).
5. **Extend EF analysis if RAS ships**: balanced **U/Y** changes **A** and
  indirect emissions even when **V** and direct **B** are unchanged; tab C’s
   0% direct-D result would not carry through to full L/M/N impacts.

### Recommended next steps (out of scope for this analysis)

1. **Decision 7:** Flag-gated per-child scaling in production; update planning
  doc after methods review.
2. **Decisions 3 & 5:** Prototype **3-row IPFP** rebalancing on `t8.3_production_diag`,
  `t8.3_purchased_power_diag`, and `p24_2017`; re-run commodity clearing and
  document cell drift before any production PR.
3. **EF analysis:** Add indirect pathway comparison tab if methods #85 needs EF
  impacts beyond direct D; include post-RAS **A** if rebalancing is adopted.

---

*Generated by `bedrock/analysis/electricity/d_85/output/_run_summary.py`.*