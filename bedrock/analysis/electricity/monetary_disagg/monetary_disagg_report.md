# Monetary Electricity Disaggregation — Implementation Report

Post-implementation documentation of PR 3.1 monetary disaggregation for aggregate sector
`221100` → `{221110, 221121, 221122}`. Analysis code lives under
[`bedrock/analysis/electricity/monetary_disagg/`](.) and does not modify production paths.

**Config:** [`test_usa_config_waste_disagg_electricity_disaggregation.yaml`](../../../utils/config/configs/test_usa_config_waste_disagg_electricity_disaggregation.yaml)

| Parameter | Value |
| --- | --- |
| `model_base_year` | 2023 |
| `usa_io_data_year` | 2022 |
| `usa_ghg_data_year` | 2023 |
| E source | eGRID FBS (national GCS) |
| GO precondition | ~0.39% residual on aggregate `221100` absorbed into VA before PR3 (post-reallocation) |

**Companion artifacts**

| Artifact | Path |
| --- | --- |
| Pipeline stages (V/U/Y) | [`electricity_pipeline_stages_V_U_Y.xlsx`](electricity_pipeline_stages_V_U_Y.xlsx) |
| Balance tables | [`output/monetary_disagg_balance_tables.xlsx`](output/monetary_disagg_balance_tables.xlsx) |
| Decision figures | [`output/figure_d2_*.png`](output/) … [`figure_d5_*.png`](output/) |
| Prior scenario analysis | [`../d_85/output/discussion_85_analysis_report.md`](../d_85/output/discussion_85_analysis_report.md) |

---

## A. Introduction — four PR3 disaggregation steps

After waste disaggregation and electricity co-production reallocation, aggregate sector
`221100` remains intact in Make (`V`), Use (`Udom`/`Uimp`), Value Added (`VA`), and
final demand (`Y`). PR3 splits that aggregate into three child sectors — `221110`
(generation), `221121` (transmission), and `221122` (distribution) — using normalized
weight shares. The production implementation follows [Methods Discussion #85](https://github.com/cornerstone-data/methods/discussions/85).

| Step | Matrix | What it does |
| --- | --- | --- |
| **1 — Make intersection** | `V` | Splits the `221100` Make diagonal into a 3×3 diagonal block (each child commodity maps to its matching industry column). Preserves total Make industry and commodity totals. **Weights:** BEA UGO305-A 2017 gross output (Decision 2). |
| **2 — Use intersection** | `Udom`, `Uimp` | Replaces the single aggregate cell `U[221100, 221100]` with a 3×3 block on the Use diagonal. Total dollars in the intersection cell are preserved (`T`). **Weights:** EIA Table 8.3 Purchased Power + T/D expense shares (Decision 3). |
| **3 — Use industry columns** | `Udom`, `Uimp`, `VA` | Splits the aggregate `221100` **industry column** (all commodity rows that purchase electricity as an input). Rebalances VA so each child column sums to its gross output `x_k = w_k × x_agg`. **Fuel commodities** (`212100`, `211000`, `324110`, `424700`, `221200`) route **100% to `221110`**. Other non-VA rows split by `w_go`; VA is the residual (Decision 4). |
| **4 — Commodity rows** | `Udom`, `Uimp`, `Y` | Splits the aggregate `221100` **commodity row** (electricity sales) across purchaser industry columns and final-demand columns in `Y`. Production uses **compensating row weights** `w_row` so aggregate gen/T/D totals match UGO305 despite step 2 using Table 8.3 intersection weights (Decision 5). |

After step 4, aggregate `221100` is removed from V, U, and Y and the IO is reindexed to
the 407-sector electricity schema. **Decision 6** (E attribution) and **Decision 7**
(cross-year scaling) operate downstream of these four steps.

---

## B. Methods #85 decisions — summary

Resolved decisions as of PR 3.1 implementation (mirrors the summary table in
[Discussion #85](https://github.com/cornerstone-data/methods/discussions/85)):

| # | Decision | One-line takeaway | Resolution |
| --- | --- | --- | --- |
| **1** | Schema | 405 taxonomy unchanged; 407 outputs only when flag is on | Finish disagg with 407 as flag only; fold into base schema in a follow-up PR. |
| **2** | Make + weights | Split anchored to BEA UGO305 gross output (10 electricity sectors) | Use UGO305 for consistency with other code paths and future gen-tech disaggregation. |
| **3** | Use intersection | Diagonal-only 3×3 block; no cross electricity-for-electricity flows | Keep diagonal structure; derive step-2 weights from **Purchased Power** (not Production) in Table 8.3. |
| **4** | Use column | Fuels → generation; VA balances each column to gross output | Keep as is; revisit if VA skew or A coefficients become problematic. |
| **5** | Use row + Y | Equal-price; same gen/T/D shares for all consumers | Keep UGO305 as constraint on row **totals**; use compensating `w_row` on non-intersection cells so totals match UGO305 despite step 2. Defer Table 2.4 price tilts to the physical-units PR. |
| **6** | E | eGRID primary; distribution ≈ zero direct GHG | Remove gas-type fallback; error if eGRID FBS unavailable. |
| **7** | Scaling | Three children inherit parent Utilities summary ratio | **Implemented:** use UGO305 target-year detail GO ratios to differentiate G/T/D scaling (post-summary correction in `cornerstone_year_scaling.py`). |

---

## C. Final matrix state by decision (production path)

Figures below show the **implemented** production outcome after the full four-step
disaggregation. Heatmap style follows Figure C in the prior analysis report
([`discussion_85_analysis_report.md`](../d_85/output/discussion_85_analysis_report.md)).

### Decision 2 — Make intersection (step 1)

UGO305-A 2017 gross-output shares split the Make diagonal while preserving aggregate
Make row and column totals (`$455.8B` each).

![Decision 2 — Make diagonal after step 1](output/figure_d2_make_intersection.png)

| Weight source | w_221110 (Gen) | w_221121 (Trans) | w_221122 (Dist) |
| --- | --- | --- | --- |
| UGO305-A (production) | 34.2% | 3.9% | 61.9% |

### Decision 3 — Use intersection (step 2)

Diagonal 3×3 block; total intersection dollars preserved. Weights from EIA Table 8.3
**Purchased Power + T/D** (76.4% / 16.8% / 6.8% in 2017), not UGO305.

![Decision 3 — Use intersection after step 2](output/figure_d3_use_intersection.png)

| Weight source | w_221110 | w_221121 | w_221122 |
| --- | --- | --- | --- |
| Table 8.3 Purchased Power + T/D | 76.4% | 16.8% | 6.8% |
| UGO305-A (steps 1 & 3) | 34.2% | 3.9% | 61.9% |

The generation-heavy intersection reallocates dollars within the electricity block without
changing the intersection total (`~$34.5B` domestic + imported).

### Decision 4 — Use industry columns + VA (step 3)

Selected purchaser commodity rows × three electricity industry columns. Fuel rows assign
100% to generation; other shown rows split by `w_go`. VA (annotated below each column)
is the residual balancing each column to target gross output.

![Decision 4 — Industry column split](output/figure_d4_use_columns.png)

Aggregate VA across the three child columns increases by **`+$1.77B`** relative to
aggregate `221100` VA before disaggregation. This matches the pre-PR3 gross-output
identity residual (`x − (U + VA)` on the aggregate column) that was absorbed into VA at
the post-reallocation checkpoint.

### Decision 5 — Commodity row + Y (step 4)

Left panel: electricity commodity rows × sample purchaser columns (Use + Y). Right panel:
`w_row` vs `w_go` — compensating row weights restore UGO305 aggregate allocation on
non-intersection cells while step 2 uses Table 8.3 intersection weights.

![Decision 5 — Commodity row / Y split and compensating weights](output/figure_d5_row_y.png)

| Sector | w_go | w_int (step 2) | w_row (step 4 & Y) |
| --- | --- | --- | --- |
| 221110 | 0.342 | 0.764 | 0.331 |
| 221121 | 0.039 | 0.168 | 0.036 |
| 221122 | 0.619 | 0.068 | 0.633 |

Without compensation (`t8.3_purchased_power_diag` scenario), per-child market-clearing
gaps reach **~$4.7B / +$1.4B / −$6.2B**. With production `w_row`
(`t8.3_purchased_power_diag_compensated` ≡ baseline), per-child gaps fall to **~$3M /
$0.4M / $5.6M** — within baseline numerical noise.

---

## D. Make–Use balance — framework

Monetary IO disaggregation must preserve several linked identities. At the **Make** side,
steps 1 and 3–4 preserve aggregate electricity **gross output** `x` and **commodity
output** `q` from `V`. At the **Use** side, purchaser **column** totals (intermediate +
VA) and the aggregate **commodity row** total (Use + Y) should match pre-disaggregation
levels when weights are consistent across steps.

When step 2 uses Table 8.3 weights that differ materially from UGO305, the intersection
block no longer aligns with Make-derived `q_k = w_go,k × q_agg`. Step 4 must either
accept commodity-market clearing gaps or **reallocate** non-intersection row cells.
Production chooses compensating `w_row`: intersection cells stay at `w_int`; remaining row
mass is split so Σ_k (U_row,k + Y_row,k) matches UGO305 totals on the aggregate row.

**Column balance (step 3)** enforces `x_k = Σ_i U_{i,k} + VA_k` per child industry.
Fuel routing to generation and VA-as-residual can skew VA shares across G/T/D even when
Make totals are exact.

**Row balance (steps 2 + 4)** requires `(U + Y)_{k,·} ≈ q_k` per child commodity for a
fully cleared IO. Mixed weights make this approximate; production holds aggregate row
totals to UGO305 while tolerating ~$10M aggregate net clearing error across children.

---

## E. E attribution and cross-year scaling

### Decision 6 — Emissions (E)

**Numerator:** eGRID-based national GHG inventory is the sole path. Plant combustion maps
to `221110`; SF6 maps to `221121`; `221122` receives negligible direct emissions. The
previous gas-type fallback (`split_electricity_e_for_disaggregated_b`) was removed — missing
eGRID FBS data raises an error.

**Denominator (x in B = E/x @ Vnorm):** When `use_E_data_year_for_x_in_B` is true
(Cornerstone full-model configs), aggregate `221100` gross output from the BEA time series
is expanded to 407 sectors using Make row shares from the disaggregated `V` (same UGO305
split as step 1). When B uses Make-derived `x` directly, no separate split is needed
because `V` is already 407-wide.

Indirect fuel-chain emissions remain upstream (fuels assigned to generation in step 3).

### Decision 7 — Scaling

After disaggregation, BEA summary sector `"22"` (Utilities) would apply one price index
to all three children. Production applies the standard summary scaling, then a **D7
correction** using UGO305 detail gross-output ratios between `original_year` and
`target_year` per child (`build_electricity_ugo305_scaling_ratios` /
`apply_electricity_d7_scaling_correction_to_A` and `_q` in
`cornerstone_year_scaling.py`).

For 2017 → 2022, detail GO ratios differentiate G/T/D (generation ~1.62×, transmission
~1.29×, distribution ~1.33× vs a shared Utilities ratio ~1.43×). See the Decision 7
section in [`discussion_85_analysis_report.md`](../d_85/output/discussion_85_analysis_report.md)
for scenario-level q trajectories prior to production wiring.

---

## F. Make–Use balance — production results

Tables generated from `build_assets.py` (see run instructions below). Stage 2 =
post-reallocation checkpoint (aggregate `221100`); stage 3 = production
`derive_disagg_io_bundle()` output.

### Preservation at the disaggregation boundary

| Metric | Stage 2 (221100) | Stage 3 (children Σ) | Δ | Passes (≤ $1) |
| --- | --- | --- | --- | --- |
| Make row x | $455.762B | $455.762B | $0 | ✓ |
| Make col q | $455.762B | $455.762B | $0 | ✓ |
| Use commodity row (U + Y) | $455.753B | $455.753B | ~$0 | ✓ |
| Y row sum | $177.974B | $177.974B | $0 | ✓ |
| VA column sum | $284.998B | $286.765B | **+$1.766B** | — |
| GO identity residual (stage 2) | $1.766B | — | — | — |

**Make side:** `x` and `q` are preserved exactly — the disaggregation is a pure split of
aggregate Make totals.

**Use side — commodity row:** Aggregate electricity purchases (Use + Y) are preserved to
numerical precision.

**Use side — VA:** The **+$1.77B** VA increase equals the pre-PR3 GO-identity residual
on aggregate `221100`. That residual was absorbed into aggregate VA at the
post-reallocation checkpoint; step 3 distributes it across child columns when enforcing
`x_k = U_{·,k} + VA_k`.

**Per-child market clearing** `(U_row + Y_row) − q` with production compensating weights:

| Scenario | 221110 gap | 221121 gap | 221122 gap |
| --- | --- | --- | --- |
| Production baseline | −$3.1M | −$0.4M | −$5.6M |
| PP diag, no compensation | +$4.71B | +$1.44B | −$6.16B |
| PP diag + compensated (`w_row`) | −$3.1M | −$0.4M | −$5.6M |

Per-child gaps partially cancel; aggregate net clearing error is **~$9M** (~0.002% of
electricity `q`), comparable to baseline without Table 8.3 intersection weights.

### Before vs after summary

| Quantity | Before (221100) | After (Σ children) | Interpretation |
| --- | --- | --- | --- |
| Total electricity output (Make q) | $455.762B | $455.762B | Exact preservation |
| Total electricity sales (Use+Y row) | $455.753B | $455.753B | Exact preservation |
| Industry inputs + VA (221100 col) | $453.996B | $455.762B (via VA) | VA absorbs GO residual |
| Child q vs child Use+Y | — | ~$9M net gap | Acceptable; compensation prevents $B-scale gaps |

Full pipeline stage workbooks and per-sector slices are in
[`electricity_pipeline_stages_V_U_Y.xlsx`](electricity_pipeline_stages_V_U_Y.xlsx)
(sheets `V/U/Y_after_elec_reallocation` vs `*_after_elec_disaggregation`, plus
`electricity_balance`).

---

## How to reproduce

From the repository root, with the project virtualenv activated:

```powershell
# Figures + balance Excel (recommended single entry point)
.\.venv\Scripts\python.exe -m bedrock.analysis.electricity.monetary_disagg.build_assets
```

This writes:

- `bedrock/analysis/electricity/monetary_disagg/output/figure_d2_make_intersection.png`
- `bedrock/analysis/electricity/monetary_disagg/output/figure_d3_use_intersection.png`
- `bedrock/analysis/electricity/monetary_disagg/output/figure_d4_use_columns.png`
- `bedrock/analysis/electricity/monetary_disagg/output/figure_d5_row_y.png`
- `bedrock/analysis/electricity/monetary_disagg/output/monetary_disagg_balance_tables.xlsx`
- `bedrock/analysis/electricity/monetary_disagg/electricity_pipeline_stages_V_U_Y.xlsx`

Figures only:

```powershell
.\.venv\Scripts\python.exe -m bedrock.analysis.electricity.monetary_disagg.figures
```

Prior Decision 3/5 scenario analysis (non-production experiments):

```powershell
.\.venv\Scripts\python.exe -m bedrock.analysis.electricity.d_85.figures
.\.venv\Scripts\python.exe bedrock/analysis/electricity/d_85/output/_run_summary.py
```

**Requirements:** Local extract parquet for EIA Electric Power Annual 2017 (Table 8.3),
BEA UGO305 detail GO, and cached national eGRID FBS — same as the integration test config.
Runtime ~1–2 minutes on a typical developer machine.

---

## Implementation references (production code)

| Decision | Primary module |
| --- | --- |
| 2 | `build_electricity_disagg_go_weights`, `disaggregate_make_intersection` |
| 3 | `build_electricity_disagg_use_intersection_weights`, `disaggregate_use_intersection` |
| 4 | `disaggregate_use_industry_columns`, `_enforce_go_identity_precondition` |
| 5 | `get_electricity_commodity_row_weights`, `disaggregate_use_commodity_rows`, Y split in `cornerstone_disagg_pipeline.py` |
| 6 | `_apply_electricity_disagg_cornerstone_mapping` in `derived.py` |
| 7 | `build_electricity_ugo305_scaling_ratios`, `apply_electricity_d7_scaling_correction_*` in `cornerstone_year_scaling.py` |

All in `bedrock/transform/eeio/electricity_disaggregation.py` unless noted.
