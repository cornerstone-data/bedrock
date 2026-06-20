# Methods Discussion #85 — Analysis Report

Analysis-only exploration of Decisions 3, 5, and 7 under
[`bedrock/analysis/electricity/d_85/`](../). Generated from the disaggregation
config
[`2025_usa_cornerstone_full_model_electricity_disaggregation.yaml`](../../../utils/config/configs/2025_usa_cornerstone_full_model_electricity_disaggregation.yaml).

## Run metadata

| Parameter | Value |
|-----------|-------|
| `model_base_year` | 2023 |
| `usa_io_data_year` | 2022 |
| `usa_ghg_data_year` | 2023 |
| E source | Cached national GCS FBS (`flowsa` not installed locally) |
| GO precondition | 0.39% residual on aggregate `221100` absorbed into VA before PR3 (post-reallocation) |

**Companion artifacts**

| Decision | Excel report |
|----------|--------------|
| 3 | [`decision3_table83_report.xlsx`](decision3_table83_report.xlsx) |
| 5 | [`decision5_table24_report.xlsx`](decision5_table24_report.xlsx) |
| 7 | [`decision7_ugo305_scaling_report.xlsx`](decision7_ugo305_scaling_report.xlsx) |
| All | [`analysis_summary.json`](analysis_summary.json) |

**Re-run**

```powershell
.\.venv\Scripts\python.exe bedrock/analysis/electricity/d_85/output/_run_summary.py
```

---

## Decision 3 — Table 8.3 intersection weights

**Question:** How do EPA Table 8.3 IOU expense shares differ from UGO305-A GO
weights, and what happens if step-2 Use intersection uses 8.3 (diagonal or
hybrid off-diagonal) while steps 1, 3, 4 stay on UGO305?

### Weight comparison (2017)

| Source | w_221110 (Generation) | w_221121 (Transmission) | w_221122 (Distribution) |
|--------|-------------------------|-------------------------|-------------------------|
| UGO305-A | 34.2% | 3.9% | 61.9% |
| EPA Table 8.3 | 86.7% | 9.5% | 3.8% |

Table 8.3 reflects IOU-only operating expenses (production + transmission +
distribution). It is heavily generation-weighted and nearly inverts the UGO305
distribution share. IOU expense shares are not a national IOU+coop+public mix.

### Scenarios

| ID | Step 2 change | Result |
|----|---------------|--------|
| `baseline` | UGO305 diagonal | Reference PR3 |
| `d8_mixed` | Table 8.3 diagonal at step 2 only | VA positive; commodity clearing broken |
| `d8_offdiag` | Hybrid off-diagonal (UGO column totals + 8.3 row splits on T/D cols) | **metrics_only** — VA balancing failed |

### Balance and VA

| Scenario | metrics_only | VA 221110 ($B) | Max market-clearing gap |
|----------|--------------|----------------|-------------------------|
| baseline | No | 70.2 | ~$5.6M (baseline noise) |
| d8_mixed | No | 64.3 | ~$6.5B (generation) |
| d8_offdiag | **Yes** | 0 (step 3 failed) | ~$16.5B (generation) |

Per-child **q = x** (industry GO) is preserved exactly in all scenarios that
complete step 3. **Commodity market clearing** (`Use row + Y − q`) degrades when
step-2 intersection diverges from Make-side weights:

| Scenario | Gap 221110 | Gap 221121 | Gap 221122 |
|----------|------------|------------|------------|
| baseline | −$3.1M | −$0.4M | −$5.6M |
| d8_mixed | +$5.9B | +$0.6B | −$6.5B |
| d8_offdiag | +$16.5B | +$1.0B | −$6.4B |

### EF diagnostics (tab C)

Direct **D**-vector changes vs baseline are **0%** on tracked significant
sectors (`221110`, `221121`, `221122`, `212100`, `331110`, `F01000`). The
analysis EF path computes `D = f(B)` where `B = (E/x) @ Vnorm` uses **Make**
matrices only. Step-2 **Use** intersection changes affect **A** but not **B** in
this pipeline. E attribution is fixed via `split_electricity_e_for_disaggregated_b()`.

`d8_offdiag` EF tab is skipped (`metrics_only`).

### Decision 3 conclusion

- **`d8_mixed`** is IO-feasible for VA/GO but breaks commodity clearing by
  billions — not production-ready without rebalancing.
- **`d8_offdiag`** fails VA balancing — document and reject for production unless
  a rebalancing method is added.
- Table 8.3 weights should not replace UGO305 wholesale; the weight gap is too
  large and structurally mismatched (IOU expenses vs national GO).

---

## Decision 5 — Table 2.4 price-differentiated row/Y splits

**Question:** If row/Y splits use Table 2.4 retail price tilts (steps 1–3 on
UGO305), what happens to q/x balance and EFs?

### End-use mapping coverage (aggregate 221100 purchases)

| EPA end-use | Share of electricity purchases |
|-------------|--------------------------------|
| Commercial | 40.7% |
| Residential | 40.0% |
| Industrial | 17.5% |
| Transportation | 1.8% |

Mapping follows rule-based NAICS/FD classification plus
[`data/cornerstone_to_epa_end_use.csv`](../data/cornerstone_to_epa_end_use.csv)
overrides.

### q vs x balance

Steps 1–3 unchanged → **Make q and industry x identical** across scenarios.
Row/Y price tilt breaks **commodity market clearing** only:

| Scenario | q_221110 | x_221110 | Market-clearing gap 221110 |
|----------|----------|----------|----------------------------|
| baseline | $155.7B | $155.7B | −$3.1M |
| p24_2017 | $155.7B | $155.7B | **−$5.70B** |
| p24_target | $155.7B | $155.7B | **−$5.79B** |

Offsetting gaps on transmission (+$337M / +$342M) and distribution (+$5.35B /
$5.44B). `p24_2017` and `p24_target` are nearly identical (~1.5% larger gaps
for target-year prices).

### EF diagnostics

Direct **D** changes are **0%** vs baseline. Row/Y reallocation does not alter
**V** (Make); therefore **B** and direct **D** are unchanged in the current EF
pipeline. Fuel commodity rows are not price-tilted (production fuel rule
preserved).

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

### Detail GO ratios (2017 → 2022)

| Sector | UGO305 ratio_k | Shared Utilities `"22"` ratio |
|--------|----------------|---------------------------------|
| 221110 (Generation) | **1.623** | 1.432 |
| 221121 (Transmission) | **1.292** | 1.432 |
| 221122 (Distribution) | **1.331** | 1.432 |

Generation detail GO grew faster than transmission/distribution over this
period.

### Scaled q trajectories (after full A/q scaling chain, IO-year dollars)

| Variant | q_221110 | q_221121 | q_221122 |
|---------|----------|----------|----------|
| Baseline (shared `"22"`) | $203.4B | $23.2B | $368.5B |
| d7_pure | $230.4B (+13%) | $20.9B (−10%) | $342.5B (−7%) |
| d7_anchored | $230.9B | $21.0B | $343.2B |

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

| Decision | Primary finding | Production readiness |
|----------|-----------------|-------------------|
| **3** | 8.3 weights diverge sharply from UGO305; diagonal 8.3 at step 2 breaks commodity clearing (~$6.5B); off-diagonal hybrid fails VA | Not ready |
| **5** | Price tilt preserves GO, breaks commodity clearing (~$5.7B); EF unchanged on direct D | Not ready without rebalancing |
| **7** | Detail GO ratios separate G/T/D q trajectories meaningfully; pure ≈ anchored | Strongest candidate |

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

### Recommended next steps (out of scope for this analysis)

1. **Decision 7:** Flag-gated per-child scaling in production; update planning
   doc after methods review.
2. **Decisions 3 & 5:** If pursued, prototype rebalancing and re-run commodity
   clearing before any production PR.
3. **EF analysis:** Add indirect pathway comparison tab if methods #85 needs EF
   impacts beyond direct D.

---

*Generated by `bedrock/analysis/electricity/d_85/output/_run_summary.py`.*
