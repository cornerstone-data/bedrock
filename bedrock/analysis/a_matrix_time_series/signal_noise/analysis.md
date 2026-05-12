# A-matrix transformation analysis — findings

Empirical results and root-cause investigation for epic #337 / Step 6 EF diagnostics.
Plan and per-step structure: `analysis_plan.md`.

## Setup

7 diagnostics runs at `model_base_year = 2023`, `update_inflation_factors = False`
(default; preserves pre-#369 production behavior on the legacy parquet PI source).
`apply_inflation_to_V` defaults to `False` for every approach except
`commodity_price_index` (which has it set in its YAML). All five approach configs
ran without YAML edits — see `analysis_plan.md` Phase 1.

| approach | YAML | A-matrix construction | runs |
|---|---|---|---|
| `useeio` | `2025_usa_cornerstone_A_useeio.yaml` | `A_2017_BEA_detail` directly (no temporal adjustment) | vs CEDA |
| `summary_tables` | `2025_usa_cornerstone_A_summary_tables.yaml` | `scale_cornerstone_A` via summary-table ratios (price + physical shifts) | vs CEDA, vs USEEIO |
| `industry_price_index` | `2025_usa_cornerstone_A_industry_price_index.yaml` | `diag(p) · A_2017 · diag(1/p)`, `p` = BEA detail industry PI | vs CEDA, vs USEEIO |
| `commodity_price_index` | `2025_usa_cornerstone_A_commodity_price_index.yaml` | Same form, `p` = V-norm-weighted commodity ratio | vs CEDA, vs USEEIO |

`useeio`-vs-USEEIO is degenerate (same methodology) and was skipped. `B/x` are the same across the four candidates at a given baseline, so all variation in `D` (direct EF) is invariant across approach panels — variation in `N` (total EF) reflects only the `(I − A)⁻¹` part.

Outputs:
- Per-(approach, baseline) Sheets in Drive folder `1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s` (v0.3 Diagnostics).
- Compiled workbook + scatter coords: `output/results/{ef_comparison.xlsx, ef_run_index.csv, ef_scatter_coords.parquet}`.
- 2×2 scatter + histogram PNGs per `(baseline, ef_kind)` in `output/plots/ef_*`.

## Headline summary numbers

`|N_perc_diff|` quantiles per approach (sectors per row).

vs **CEDA-US (v0)**:

| approach | N_p50 | N_p95 | N_max | n_significant (>10%) |
|---|---:|---:|---:|---:|
| `summary_tables` | **0.053** | **0.207** | 0.564 | **93** |
| `useeio` | 0.084 | 0.288 | 0.756 | 178 |
| `industry_price_index` | 0.121 | 0.438 | 0.911 | 247 |
| `commodity_price_index` | 0.121 | 0.438 | 0.881 | 245 |

vs **USEEIO**:

| approach | N_p50 | N_p95 | N_max | n_significant |
|---|---:|---:|---:|---:|
| `summary_tables` | 0.189 | 0.629 | 3.072 | 436 |
| `industry_price_index` | **0.096** | **0.531** | 3.130 | 527 |
| `commodity_price_index` | **0.095** | **0.531** | 3.130 | 530 |

**Headline reads:**
1. `summary_tables` reproduces v0 most closely (n_p50 = 5.3%) — same methodology, drift of ~5 pp reflects schema-mapping noise + the post-#369 inflation gate, not a methodology difference.
2. `industry_price_index` and `commodity_price_index` are functionally indistinguishable — every quantile, every panel, both vs CEDA and vs USEEIO.
3. Histogram bias direction (medians, vs CEDA): `summary_tables` ≈ −3.4%, `useeio` ≈ −5.0%, `industry_price_index` ≈ +9.6%, `commodity_price_index` ≈ +9.5%. Vs USEEIO: `summary_tables` ≈ −15.2%, `industry_price_index` ≈ −2.9%, `commodity_price_index` ≈ −2.8%.

## Mechanism (Phase 1 trace)

`derive_cornerstone_Aq_scaled()` → A matrix (varies by approach) → `derive_cornerstone_B_non_finetuned()` → B matrix → `calculate_ef_diagnostics.py` computes `N = (I − A)⁻¹ · B/x` and `D = B/x`. The four candidates diverge **only** in A-matrix construction; B/x identical across them.

Mapping methodologies onto the data:
- `summary_tables` *is* v0's methodology (modulo schema mapping) → predicts tight match to CEDA.
- `useeio` shares the BEA-2017 detail base with v0 but skips temporal adjustment → predicts modest, mostly-symmetric drift.
- `industry_price_index` rescales A by relative price changes only → drops the *physical* inter-industry shift component v0 captures via summary tables → predicts directional bias.
- `commodity_price_index` is `industry_price_index` mapped through V-norm supplier weights, but for ~95% of codes the primary supplier industry equals the commodity code → predicts ≈ industry_price_index.

Empirically (`output/results/ratio_summary.csv` at year 2024):
- `ind_median = 1.289`, `com_median = 1.290` — V-norm-weighted commodity ratio differs from industry ratio by `rel_delta_p95 = 0.019`.
- 52 of 405 codes (12.8%) differ by >1%; 0 differ by >5%.

That is mechanically why the entire commodity-PI path collapses onto the industry-PI numbers in this dataset.

## Phase 2 — D2.2 (sectors driving the bias)

Gap statistic per sector: `gap = N_industry_pdiff − N_summary_pdiff`, both vs CEDA. Distribution:

- mean = +0.156, median = +0.131, p95 = +0.474, p5 = −0.077, range = (−0.71, +1.04)

The +0.131 median exactly reproduces the 13 pp histogram-bias gap (industry_pi median ≈ +9.6%, summary_tables median ≈ −3.4%). So whatever drives `gap` drives the population-level bias.

### Where the gap matters materially (impact = |gap| × |N|)

NAICS-3 aggregation, by total impact:

| NAICS | description | n_sectors | sum_impact | median_gap |
|---|---|---:|---:|---:|
| **325** | Chemicals | 19 | **2.11** | +0.16 |
| **311** | Food manufacturing | 24 | **2.05** | +0.12 |
| **336** | Transportation equipment | 25 | 1.54 | +0.17 |
| 322 | Paper | 8 | 0.70 | +0.17 |
| 333 | Machinery | 28 | 0.68 | +0.13 |
| 326 | Plastics & rubber | 10 | 0.67 | +0.16 |
| 335 | Electrical equipment | 18 | 0.66 | +0.09 |
| 332 | Fabricated metal | 20 | 0.60 | −0.02 |
| **334** | Computer/electronics | 20 | 0.56 | **+0.47** |
| 339 | Misc manufacturing | 11 | 0.51 | +0.19 |

Two competing magnitudes. NAICS 334 (electronics) has the **largest per-sector gap** (median +0.47) — the textbook case where price-only scaling and physical scaling predict opposite directions, because electronics had falling prices but rising real output 2017→2024. But electronics have low per-sector EFs, so material impact is mid-pack.

The systemic bias is dominated by **chemicals (325) and food manufacturing (311)** — moderate per-sector gaps (+0.12 to +0.16) but high baseline EFs.

### One outlier worth flagging

`33641A` (Propulsion units for space vehicles): gap = −0.71. The only large-magnitude reversal. `summary_tables` shows +56.4% vs CEDA, `industry_price_index` shows −14.6%. Likely a schema-mapping artifact in the BEA summary table for a small specialty sector, not a general-mechanism finding.

## Root cause (one sentence)

`industry_price_index` overstates 2024 EFs vs `summary_tables` because it credits sectors only for nominal price changes; on energy-intensive manufacturing sectors (chemicals, food, transportation equipment) where 2017→2024 saw both significant price inflation *and* material physical-efficiency gains, the omitted physical-shift component is what `summary_tables` captures and where the population bias lives.

## Year-over-year stability

Per-sector N time series 2019→2023 (deflated to 2023$), 4 transitions per sector. `useeio` excluded (no temporal scaling — N changes only via B/x drift, not a comparable method). Bottom 5% of `mean_N` dropped to avoid divide-by-near-zero blowups.

| method | per-sector mean \|YoY\|, median | per-sector mean \|YoY\|, emissions-weighted |
|---|---:|---:|
| `commodity_price_index` | **3.2%** | **4.8%** |
| `industry_price_index` | **3.2%** | **4.8%** |
| `summary_tables` | 7.3% | 8.0% |

Pooled-distribution reads (from `n_yoy_distribution.png`):
- PI methods: pooled p50 ≈ 3%, p75 ≈ 5%, upper whisker ≈ 10%.
- `summary_tables`: pooled p50 ≈ 7%, p75 ≈ 11%, upper whisker ≈ 25%.
- ECDF cross-reads: share of sector-years with `|YoY| ≤ 5%` — PI ≈ 73%, `summary_tables` ≈ 40%. At `|YoY| ≤ 10%` — PI ≈ 92%, `summary_tables` ≈ 73%.

Per-transition pattern: 2022→2023 is the noisiest transition for all three methods (medians rise + IQRs widen). `summary_tables` is the widest box in *every* transition — the gap isn't a one-year artifact. PI methods are visually indistinguishable across all transitions (same V-norm collapse as the vs-v0 panel).

**Reads:** PI methods are ~2× more YoY-stable than `summary_tables` by every quantile. The gap is consistent across transitions, not driven by a single bad year.

Outputs:
- `output/plots/n_yoy_distribution.png` — 3-panel: pooled box, per-transition box, ECDF.
- `output/plots/n_indexed_lines.png` — head-sector trajectories rebased to 2019=100, faceted by method.
- `output/results/n_yoy_ranking.csv`, `n_yoy_per_sector.csv` — per-approach metrics + per-sector detail.

## Signal-vs-noise decomposition of `summary_tables` (Phase A + B)

Sub-analysis answering the core unknown from §Decision: how much of `summary_tables`' YoY excess motion over PI is real economic signal vs. BEA-summary-table noise. Full plan: `signal_noise_plan.md`. Implementation: `derive_A_snapshots.py` → `compute_lmdi_phys.py` → `compute_consistency_tests.py`.

### Approach

Decomposition lives at the **A-cell level**, not at N — see `signal_noise_plan.md` §"Why decompose at the A-matrix level". Across all 405×405 cells × 8 years × {dom, imp}:

- `A_pi_ij(y) = (p_i(y) / p_j(y)) · A_2017_ij` — commodity-PI price-only counterfactual. Both numerator and denominator of `A_ij = z_ij/x_j` are inflated from 2017$ to year-y$ at constant quantity.
- `A_summary_ij(y)` — same 2017 base rescaled via BEA summary-table ratios; carries both price and physical change.
- `Q_phys_ij(y) = A_summary / A_pi` — implied physical-shift multiplier. Dividing cancels both inflation factors (because they're both encoded in `A_pi`), giving the multiplicative change in the **real (price-deflated) input coefficient** since 2017. Anchored: `Q_phys(2017) ≡ 1` (verified clean after excluding `S00102`, `GSLG*` and other BEA-special j-codes).
- `phys_ij(y) = ln Q_phys_ij(y) − ln Q_phys_ij(y-1)` — YoY physical effect. Aggregated to NAICS-3 of the output sector j using LMDI logarithmic-mean weights (exact additive decomposition, no residual term).

Output of Phase A: 7 transitions × ~85 NAICS-3 × {dom, imp} of `phys_effect_avg_annual`.

### Phase B — three internal-consistency tests

What Phase A gives us is the **algebraic residual** of `summary_tables` after stripping commodity-PI. It mixes (a) real physical/structural change, (b) BEA summary-table revisions, (c) summary→detail aggregation jitter, (d) imperfect commodity-PI deflation, (e) survey sampling error. Real signal has statistical signatures that noise doesn't:

| test | signal expectation | dom result | imp result |
|---|---|---:|---:|
| 1. Lag-1 autocorr (pooled, demeaned) | ρ₁ > 0 (drift persists) | **−0.05** | **−0.20** |
| 2. Coherence — LMDI-weighted ICC, mean across transitions | ICC → 1 (cells in same NAICS-3 move together) | **0.18** | **0.19** |
| 3a. Median \|phys\|/yr | < 5%/yr (real change is slow) | 3.8% | **7.8%** |
| 3b. p90 \|phys\|/yr | < 10%/yr | **15.3%** | **24.1%** |
| 3c. Excess kurtosis | small | **+12.9** | **+13.7** |
| 3d. Skew | strongly positive (directional) | +1.6 | +1.7 (mild) |

**Reads:**
1. Both `dom` and `imp` are noise-dominated at the pooled level. No positive persistence, low coherence, fat-tailed magnitude distributions roughly symmetric around 0.
2. `imp` is materially noisier than `dom` on every measure — consistent with commodity-PI being a worse deflator for imported flows (landed-price paths diverge from domestic commodity PI).
3. Anti-persistent `imp` autocorr (−0.20) suggests an oscillation pattern: BEA over-corrects each year, flipping sign.

### Per-NAICS-3 view

A NAICS-3 × kind passes the "signal-clean" bar when its 7-transition series has lag-1 r > 0.20, median \|phys\| ≤ 7 %/yr, and max \|phys\| ≤ 25 %/yr. Result:

| kind | NAICS-3 passing | share of total \|impact\| represented |
|---|---:|---:|
| dom | 5 / 85 | 4.2% |
| imp | 4 / 85 | 8.1% |

The **high-impact** NAICS-3 — the ones that actually move N — all **fail persistence**, often with strongly negative lag-1 r:

| naics3 | description | abs_impact_sum | r_lag1 | median \|phys\|/yr |
|---|---|---:|---:|---:|
| 336 | Transportation equipment | 4.36 | **−0.70** | 4.1% |
| 325 | Chemicals | 2.95 | **−0.74** | 4.4% |
| 334 | Computer/electronics | 2.38 | −0.31 | 4.4% |
| 324 | Petroleum & coal | 2.36 | −0.12 | **15.2%** |
| 332 | Fabricated metal | 1.90 | **−0.59** | 1.9% |
| 311 | Food manufacturing | 1.83 | −0.25 | 0.9% |
| 333 | Machinery | 1.77 | **−0.48** | 2.6% |
| 112 | Livestock | 1.21 | **−0.69** | 7.5% |

The sectors most responsible for `summary_tables`' overall YoY motion sign-flip year-to-year — the textbook signature of revision/aggregation noise.

**One genuinely signal-clean and impactful exception:** `327` (cement & nonmetallic minerals, dom) — impact 1.04, r=+0.66, median 1.5%/yr. Plausible as a real industry story (cement-process efficiency / output-mix shift). Not enough on its own to anchor a methodology decision.

Full ranking: `output/results/lmdi_signal_clean_naics3.csv`.

### What this means for the decision

The implicit assumption behind keeping `summary_tables` (or v0's hybrid) was that its YoY excess over PI carries real physical-shift signal worth the noise cost. Phase B says **that excess is noise-dominated, especially for the sectors that drive N**. The directional bias in N at year 2024 between PI and `summary_tables` (§Phase 2: ~13 pp on chemicals, food, transport equip) is real — `summary_tables` does see *something* PI doesn't between 2017 and 2024 in aggregate — but the *path* from 2017 to 2024 in those sectors is dominated by sign-flipping noise, not smooth drift. Customers reading year-over-year reports would see oscillation, not real efficiency change.

Outputs:
- `output/results/lmdi_phys_cells.parquet`, `lmdi_phys_by_naics3.csv`, `lmdi_consistency_tests.csv`, `lmdi_signal_clean_naics3.csv`.
- `output/plots/lmdi_phys_naics3_bars.png`, `lmdi_consistency_autocorr.png`, `lmdi_consistency_magnitude.png`.

## External validation against BEA-BLS KLEMS (Phase C)

Phase A produced an algebraic residual; Phase B's *internal* tests said the residual lacks the statistical signatures of smooth structural drift. Phase C asks the *external* question: does the residual track an **independent** measure of real physical change?

### Reference dataset

BEA-BLS Integrated Industry-Level Production Account (KLEMS), 1997–2024, 2017=100 base — same anchor as our A-matrix base. Two series serve as independent physical-change references:

- **Materials/Output quantity ratio**: real materials per unit real output. The most direct analogue to the column-sum of A (intermediate-input intensity).
- **Integrated TFP index**: total factor productivity. Captures real output relative to combined real inputs.

KLEMS uses BEA "Production Account Codes" (PAC), most of which map 1:1 to our NAICS-3; aggregates (`3361MV`+`3364OT` ≡ NAICS-3 336; `311FT` covers 311+312; `111CA` covers 111+112) are combined via geometric mean of per-PAC YoY growth.

### Sign-pattern reasoning

If Phase A is measuring the same thing as KLEMS Mat/Out, both should rise and fall together: positive Pearson r. TFP moves the opposite way (more materials per output = lower productivity), so r vs TFP should be *negative*. Observing **r_MatOut > 0 AND r_TFP < 0 together** is the joint signature of "Phase A is tracking the same real economic quantity KLEMS sees."

### Per-NAICS-3 results

11 top-impact NAICS-3 (dom side; Phase A `kind` collapsed to dom+imp total since KLEMS measures total industry behavior):

| naics3 | description | r vs Mat/Out | r vs TFP | Phase A mean %/yr | Mat/Out mean %/yr | verdict |
|---|---|---:|---:|---:|---:|---|
| **334** | Computer/electronics | **+0.89** | −0.73 | −2.9 | −3.7 | strong corroboration |
| **311** | Food manufacturing | **+0.83** | −0.67 | −0.3 | −0.5 | strong corroboration |
| **325** | Chemicals | +0.58 | **−0.96** | −3.7 | −3.1 | corroborated |
| **327** | Cement / nonmetallic min. | +0.57 | −0.60 | +1.6 | +1.3 | corroborated (matches Phase B signal-clean flag) |
| **212** | Mining ex. oil & gas | +0.57 | −0.83 | +1.2 | −2.3 | direction match, magnitude diverges |
| **324** | Petroleum & coal | +0.54 | −0.79 | −1.9 | −1.3 | corroborated |
| 339 | Misc manufacturing | +0.29 | −0.41 | −3.2 | −5.0 | weak |
| 336 | Transport equipment | +0.12 | +0.02 | −1.2 | −1.3 | uncorrelated |
| 333 | Machinery | +0.03 | −0.23 | −1.2 | −1.6 | uncorrelated |
| 332 | Fabricated metal | −0.08 | +0.36 | +2.0 | +1.1 | uncorrelated |
| 112 | Farms (contaminated) | −0.01 | −0.17 | +0.4 | −0.2 | uncorrelated (contaminated mapping) |

**Six of eleven** top-impact sectors clear `r vs Mat/Out ≥ +0.5` while simultaneously showing strongly negative r vs TFP. That's the expected joint sign-pattern, and it covers the highest-emission sectors in the system (`311`, `325`, `334`).

### What this revises in Phase B's reading

Phase B's headline was "noise-dominated based on lag-1 autocorrelation ≈ 0." Phase C says: **non-persistent ≠ non-real**. For the 6 corroborated sectors, Phase A's per-transition motion correlates with an independent KLEMS measure of real materials-intensity change at r ≥ +0.5 — that's signal, even though it doesn't drift smoothly year-to-year. The real economy itself oscillates in materials-intensity in those sectors; Phase A is faithfully picking that up, and Phase B's persistence test was the wrong filter.

What Phase B got right:
- The high-impact NAICS-3 with strongly negative *internal* lag-1 r (336, 332, 333) really are uncorrelated with external KLEMS too. Their motion in `summary_tables` looks like genuine noise (revision-driven), not just non-monotonic real signal.
- Magnitude implausibility (p90 |x| at 15%/yr for dom) remains real — Phase A still amplifies KLEMS Mat/Out a bit. Comparison of mean %/yr columns: roughly matching for 311, 325, 324, 327; modestly amplified for 339, 334. Order of magnitude is right, but Phase A is not a quiet noise-free signal either.

The verdict isn't binary — it's **per-sector**. About half the top-impact sectors carry real signal (corroborated externally); the other half carry noise that PI would correctly suppress.

### Aggregation caveat: NAICS-3 `336`

`336` is the largest |impact| NAICS-3 and the most starkly uncorrelated with KLEMS (r=+0.12, r_TFP=+0.02). Its KLEMS series is the geometric mean of `3361MV` (motor vehicles, NAICS-4 = 3361/3362/3363) and `3364OT` (other transport equip, NAICS-4 = 3364/3365/3366/3369). If the physical signal lives in one half and the noise in the other, the equal-weighted average dilutes signal toward zero. A weighted aggregation (by gross-output share) would partially recover this; out of scope for the first pass.

Outputs:
- `output/results/klems_validation_per_transition.csv`, `klems_validation_summary.csv`.
- `output/plots/klems_validation_scatter.png` — per-NAICS-3 scatter of Phase A vs KLEMS Mat/Out with y=x reference and per-sector verdict color.

## Decision: which method?

Pareto-undominated **by the headline numbers**; Phase B + C together reweight the tradeoff sector-by-sector rather than producing a single global ranking.

| method | N_p50 vs v0 | YoY p50 | wins on | loses on |
|---|---:|---:|---|---|
| `summary_tables` | **5.3%** | 7.3% | matches v0; captures real signal in 6/11 top-impact sectors (Phase C corroborated) | high YoY noise; carries revision noise in the other 5/11 |
| `industry_price_index` | 12.1% | **3.2%** | YoY stability; transparent | omits real materials-intensity signal in 6 high-impact sectors |
| `commodity_price_index` | 12.1% | **3.2%** | same as industry_pi numerically; slightly better theory (commodity-space A) | (same omission) |

### Three reading errors to avoid

1. **v0-distance is partly circular.** `summary_tables` matching v0 at 5.3% reflects "same methodology by construction" (§Mechanism), not "more accurate." Read v0-distance as a *continuity* metric, not a *correctness* metric.
2. **`summary_tables`' "completeness" is partial.** Phase C confirmed real signal in ~half the top-impact sectors (334, 311, 325, 327, 324, 212 — r_MatOut ≥ +0.5 with the expected joint sign on r_TFP). For the other half (336, 332, 333, 339, 112), the extra motion is uncorrelated with KLEMS and is most likely revision noise.
3. **PI's "stability" is partly stability-by-omission.** For the 6 corroborated-signal sectors, PI's lower YoY isn't a virtue — it's a directional loss of signal that KLEMS independently confirms. The §Phase 2 13 pp cumulative bias on chemicals / food is *real signal* PI cannot see.

### What changed after Phase B + C

- **Phase B alone** would have argued for PI: lag-1 r ≈ 0 looks like noise.
- **Phase C revises that**: for 6/11 top-impact sectors, Phase A correlates strongly (+0.5 to +0.9 on Mat/Out, simultaneously −0.5 to −1.0 on TFP — the expected joint sign pattern). The motion is real even though non-monotonic.
- **Net read**: about half the top-impact dom |impact| sits in sectors where `summary_tables` carries externally-corroborated signal PI misses; the other half is noise PI would correctly suppress.

### Three live options

**Option A — `commodity_price_index` (PI-default, accept the signal loss)**

- Pick `commodity_price_index` over `industry_price_index` on theory (A is commodity-space; PI deflator should match — see `signal_noise_plan.md`). Numerically indistinguishable (`rel_delta_p95 = 0.019`), so it's a free correctness improvement over industry_pi.
- Cleanest, lowest-noise, easiest to audit.
- Pays the cost: ~13 pp cumulative directional bias on chemicals (325), food (311), electronics (334) — sectors where Phase C confirms PI is missing real signal that customers and the literature would expect to see.

**Option B — `summary_tables` (accept the YoY noise)**

- Captures real signal in 6/11 top-impact sectors per Phase C.
- Pays the cost: ~2× YoY p50 noise vs PI, and in 5/11 top-impact sectors that noise has no real-signal backing.
- Continuity-with-v0 (5.3% N_p50) is real but largely circular.

**Option C — per-sector hybrid (`summary_tables` for corroborated sectors, PI for the rest)**

- Empirically defensible: Phase C gives a defensible per-sector pass/fail list.
- Cleanly answers the methodology question, but operationally complex: requires per-NAICS-3 method selection logic, methodology audit at each year revision, and clear customer-facing communication about why some sectors update differently year-over-year.
- Most accurate on theory; highest implementation cost.

### Recommendation (revised after Phase C)

**Option A (`commodity_price_index`) remains the recommended default**, but the tradeoff is now better-quantified:

- It's the cleanest defaults answer (stable YoY, transparent, low audit surface).
- The signal loss is real, ~13 pp at p50 on energy-intensive sectors, and Phase C confirms it's not just noise difference.
- If product strategy ever requires faithfully reflecting real materials-intensity changes in chemicals / food / electronics / cement, the right answer is **not to default to summary_tables** (which gets it right for those sectors but wrong for others); the right answer is **Option C** with a per-sector schedule grounded in Phase C's pass/fail.

If product strategy can tolerate the directional bias on those sectors in exchange for stability and methodological simplicity, ship `commodity_price_index`. If not, the principled next step is the per-sector hybrid — but that's worth its own design discussion.

### What's been ruled out

- **v0's pure hybrid** (`summary_tables` 2017→2022 + PI 2022→2023): Phase B + C show the signal/noise mix is per-sector, not per-year. A time-based split doesn't fix the wrong sectors. Discontinuity at 2022 also complicates audit.
- **`industry_price_index`**: dominated by `commodity_price_index` on theory at zero numerical cost.

### Caveats on Phase C corroboration

1. **`336` aggregation artifact**: largest |impact| NAICS-3 in the dataset, and Phase C marks it uncorrelated (r=+0.12). The KLEMS series combines `3361MV` + `3364OT` with equal weight; if the physical signal lives in one half and the noise in the other, the average dilutes signal. Output-share-weighted aggregation could recover this — pending follow-up.
2. **`311` contamination**: KLEMS `311FT` covers NAICS-3 311+312 (food + beverages/tobacco). Strong corroboration (r=+0.83) is mildly weakened by this — beverages drift differently from food.
3. **Imports (`kind = imp`)**: not validated by KLEMS (which is for domestic production). Phase B's stronger noise verdict on `imp` (lag-1 r=−0.20, p90=24%/yr) holds independently.

### Remaining decision-support work (optional)

1. ~~**Signal-vs-noise decomposition of `summary_tables` YoY**~~ ✓ done.
2. ~~**Phase C — KLEMS external validation**~~ ✓ done.
3. **Customer-shaped acceptance test** — simulate year-over-year footprint reports for the 8–10 sector mixes representing Cornerstone's largest customers under each method. Concrete question: how many would see a >10% YoY swing on stable operations? Most data already exists in `n_yoy_per_sector.csv`. Highest-leverage remaining item.
4. **USEEIO triangulation** — at year 2023, `commodity_price_index` p50 = 9.5% vs USEEIO; `summary_tables` 15.2%. Already favors PI; could be sharpened with a side-by-side at the sectors Phase C corroborates.
5. **Refine `336` KLEMS aggregation** — output-share-weighted combination of `3361MV`+`3364OT`. If 336 actually does correlate, the Phase C signal-sector list expands and the recommendation tilts further toward Option C.

## Open items

- **D2.1** — V-norm concentration: ✓ confirmed from `ratio_summary.csv` (`p95 rel_delta = 0.019`).
- **D2.3** — multiplicative decomposition of `N_summary ≈ N_industry · (price_corr) · (phys_corr)`. Partially answered by Phase A/B at the **A-cell** level (Q_phys_ij). N-level multiplicative decomposition still pending; less load-bearing now that the A-level question is answered.
- **Phase 3** — generalize: confirm the price + physical-shift decomposition for each top-impact sector (325, 311, 336) individually. Partially answered by Phase B per-NAICS-3 view; these three all show negative lag-1 r and fail persistence.
- **Phase 4** — origin: ✓ answered. CEDA's summary_tables + 2022→2023-PI hybrid; open to reconsideration. See §Decision.
- **Phase 6** — visualize: per-sector scatter of (BEA price-index Δ 2017→2024) vs (summary-table multiplier 2017→2024). Less load-bearing after Phase B.
- **Phase C — KLEMS external validation** ✓ done. Corroborates Phase A residual for 6/11 top-impact NAICS-3 (334, 311, 325, 327, 324, 212). See § above + §Decision.
- **Customer-shaped acceptance test** (§Decision remaining work item #3).
- **Refine `336` KLEMS aggregation** to output-share-weighted (§Decision remaining item #5).

## Pointers

- Compile + plot scripts: `compile_ef_diagnostics.py`, `plot_ef_diagnostics.py`.
- Stability scripts: `compare_method_stability.py` (producer of `n_yoy_*` artifacts).
- Signal-vs-noise (Phase A/B): `derive_A_snapshots.py`, `compute_lmdi_phys.py`, `plot_lmdi_phys.py`, `compute_consistency_tests.py`, `plot_consistency_tests.py`, `extract_signal_clean_naics3.py`. Plan: `signal_noise_plan.md`.
- KLEMS external validation (Phase C): `validate_klems.py`. Source data: BEA-BLS Integrated Industry-Level Production Account workbook (1997-2024), pulled from `gs://cornerstone-default/extract/input-data/BEA_KLEMS/` into `bedrock/extract/input_data/BEA_KLEMS/`. Path overridable via `$KLEMS_XLSX` for offline use.
- Per-pair tabs in `output/results/ef_comparison.xlsx`: 7 named `{approach}__vs_{baseline}`.
- Long-format scatter coords: `output/results/ef_scatter_coords.parquet`.
- Plan: `analysis_plan.md`. Redundancy cleanup queue: `.claude/plans/a_matrix_redundancy_cleanup_plan.md`.
