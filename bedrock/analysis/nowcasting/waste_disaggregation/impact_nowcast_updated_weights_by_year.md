# Impact of year-aligned waste disaggregation weights (2018–2024)

**Audience:** reviewers deciding whether year-varying waste weights belong on Cornerstone’s nowcast production path.  
**Scope:** control (frozen **2017** production weights) vs treatment (weights rebuilt to match each nowcast year), both arms on the **same GCS** MUT pin `v0.3.0_92b7a8a`. Local MUT comparison is **out of scope** for this report.  
**Companion 2024-only note:** [`impact_nowcast_updated_weights.md`](impact_nowcast_updated_weights.md).  
**Production status:** **FLIP** — see [`production_gate.md`](production_gate.md) and [`flip_release_note.md`](flip_release_note.md).

---

## 1. Introduction

Cornerstone’s nowcast path already builds an annual detail MUT for each calendar year 2018–2024, then runs the EEIO model to produce emission factors:

- **D** — direct emissions intensity (emissions per dollar of industry output)
- **N** — total (life-cycle) emissions intensity (direct + supply chain)

Waste disaggregation sits in the EEIO stage: after correspondence, the single BEA waste sector `562000` is split into seven Cornerstone waste activities using **percent-share weight tables** (not dollar IO). Today, production still applies the **2017** weight shares even though parent dollars move with each nowcast year. That year mismatch is the methodological tension this work addresses.

This report answers: *If we rebuild waste weight shares for each nowcast year 2018–2024 and hold everything else fixed (including the GCS MUT), how do economy-wide and waste-sector emission factors change relative to the frozen-2017 baseline?*

Results below come from a paired A/B for every year: same GCS MUT on both arms, same configs except the waste-weight year on the treatment arm. Electricity steps were left **off** so the comparison isolates the weight update.

---

## 2. Methods

### 2.1 Study design

| Arm | Waste weights | MUT | Electricity |
|-----|---------------|-----|-------------|
| **Control** | Bundled **2017** CSVs (production default) | Same GCS pin as treatment | Off |
| **Treatment** | Derived for year **Y** (`waste_weights_year: match_io`) | **Same** GCS pin | Off |

**Configs** (under [`configs/`](configs/)):

- `2025_usa_cornerstone_v0_4_nowcast_{Y}_waste_weights_control.yaml`
- `2025_usa_cornerstone_v0_4_nowcast_{Y}_waste_weights_match_io.yaml`

For each year we report percent differences (treatment − control) / control for N and D across sectors, plus focused waste-child charts. Artifacts: `cache/impact_{Y}_v0.3.0_92b7a8a/`. Figures: [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) (local PNGs untracked).

### 2.2 Main plan decisions carried into this run

These choices come from the feasibility lock and the analysis plan:

1. **EEIO-only change.** Weights update after correspondence; nowcast Steps 1–7 and GCS MUT contents are not rebuilt for this A/B.
2. **Industry mix (Use column sums).** SAS Table 3 total expenses by waste child NAICS for **≤2022** with **prior-weighted** suppression recovery. For **2023–2024** under Flip/`match_io`, hold post-fill SAS 2022 levels and move 2024 by AIES-only child-share ratios (share-seam grade triggered). Do not use purchaser refuse / `EXPS_REFUSE` for child mix. Table 2 Use-row revenue uses the same prior-weighted recovery (same-table revenue priors only). Impact caches regenerated 2026-09-30.
3. **Who buys waste (Use rows / FD).** Economic Census `ecnclcust` customer-class receipts. **EC 2022** for model years 2022+; **bare EC 2017 freeze** for 2018–2021 (`resolve_ec_year`) because there is no intercensal 6-digit who-buys substitute.
4. **Waste×waste Use intersection.** Rebuild from **RCRA ≥2017**, not the workbook’s 2012 RCRA embedded in the 2017 CSVs. Analysis uses a temporary Biennial Report shipper→receiver path recorded as `rcra_path=br_bypass` (CRHW FBS remains generation-only).
5. **Reject refuse for industry mix.** Purchaser refuse series stay on the aggregate nowcast seed only.
6. **Electricity off** for control vs treatment so deltas are attributable to weights.
7. **Same GCS MUT on both arms.** One verified GCS `nowcast_mut_vintage` for the full 2018–2024 panel on control and treatment.

### 2.3 MUT and other input vintages used here

**GCS MUT (both arms, all years):** `v0.3.0_92b7a8a`  
Verified by GCS probe for a complete 2018–2024 panel (`cache/gcs_mut_vintage.json`). Control and treatment load this same pin via `nowcast_mut_vintage` in the year-pinned YAMLs.

**Treatment weight-input vintages by model year** (resolver / carry rules as implemented on the derive path):

| Model year | Industry mix (Use columns) | Who-buys / FD (EC) | RCRA intersection (BR odd-year carry) | Commodity row-sum proxy |
|------------|----------------------------|--------------------|----------------------------------------|-------------------------|
| 2018 | SAS Table 3 (2018) | EC **2017** (freeze) | →2017 BR | SAS revenue |
| 2019 | SAS Table 3 (2019) | EC **2017** (freeze) | 2019 BR | SAS revenue |
| 2020 | SAS Table 3 (2020) | EC **2017** (freeze) | →2019 BR | SAS revenue |
| 2021 | SAS Table 3 (2021) | EC **2017** (freeze) | 2021 BR | SAS revenue |
| 2022 | SAS Table 3 (2022), **prior-weighted** `S`/`D` recovery | EC **2022** | →2021 BR | SAS revenue (prior-weighted) |
| 2023 | **Chain:** hold post-fill SAS 2022 (AIES unused for levels) | EC **2022** | →2021 BR | SAS **2022** carry |
| 2024 | **Chain:** SAS 2022 × (AIES 2024/2023), renormalised | EC **2022** | →2021 BR | SAS **2022** carry |

**Control weight provenance (every year):** bundled 2017 Use/Make CSVs, including the workbook’s **2012** RCRA intersection shares — unchanged from production.

Make static expert rules and the seven-child code set stay time-invariant on both arms.

### 2.4 How to read the metrics

- **N median %** — median percent change in total EF across sectors; a small negative value means most sectors move slightly down when weights update.
- **N p95 \|%\|** — 95th percentile of absolute percent change; a tail-width measure for economy-wide spillover.
- **Waste N max \|%\| mover** — among the seven waste children, the sector with the largest absolute N percent change (sign retained).

Paired deltas combine industry mix, who-buys, and RCRA intersection updates together; they are **not** an industry-mix-only experiment. An earlier Sep-22 run with an unpinned control MUT is **not** treated as flip evidence.

---

## 3. Results

### 3.0 Weight shares: 2017 CSVs vs 2024 derive

Before emission-factor results, the tables below show how the **weight inputs themselves** move when replacing bundled 2017 CSVs with 2024-derived shares (the treatment arm of the 2024 A/B).

<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->

Comparison of **bundled 2017 production weight CSVs** (control) vs **2024 derived** weights (treatment `match_io`). Values are percent shares among the seven Cornerstone waste children (each vector sums to ~100%). Δ is 2024 − 2017 in percentage points.

_2024 derive provenance:_ RCRA=2021, EC=2022, SAS=2022, AIES=2024/BASIC; notes: SAS Table 3 prior_by_naics={'562211': 8022000000.0, '562112': 1733000000.0, '562213': 1132000000.0}; SAS Table 3 expenses waste suppression recovery (prior_weighted_residual under NAICS 562): parent=95778000000, published_detail=84242000000, residual=11536000000, n_suppressed=3, priors={'562112': 1733000000.0, '562211': 8022000000.0, '562213': 1132000000.0}, require_complete_priors=True, recovered_naics=['562112', '562211', '562213']; waste industry-mix chain: 2024 = post-fill SAS 2022 × (AIES 2024 / AIES 2023) child-share ratios, renormalised; SAS Table 2 prior_by_naics={'562112': 2507000000.0, '562213': 1148000000.0}; SAS Table 2 revenue waste suppression recovery (prior_weighted_residual under NAICS 562): parent=136674000000, published_detail=132211000000, residual=4463000000, n_suppressed=2, priors={'562112': 2507000000.0, '562213': 1148000000.0}, require_complete_priors=True, recovered_naics=['562112', '562213']; rcra_path=br_bypass.

**Industry mix — Use column sum (industry output)**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 47.60% | 48.62% | +1.02 pp |
| 562HAZ | 12.10% | 10.93% | -1.17 pp |
| 562212 | 8.27% | 8.15% | -0.12 pp |
| 562213 | 1.51% | 1.04% | -0.47 pp |
| 562910 | 16.60% | 15.21% | -1.39 pp |
| 562920 | 4.48% | 4.64% | +0.16 pp |
| 562OTH | 9.43% | 11.39% | +1.96 pp |

**Commodity mix — Use row sum (commodity output)**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 46.30% | 48.98% | +2.68 pp |
| 562HAZ | 10.30% | 8.73% | -1.57 pp |
| 562212 | 9.80% | 8.22% | -1.58 pp |
| 562213 | 1.50% | 1.03% | -0.47 pp |
| 562910 | 16.60% | 16.17% | -0.43 pp |
| 562920 | 7.53% | 5.89% | -1.64 pp |
| 562OTH | 7.97% | 10.97% | +3.00 pp |

**Make column sum**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 47.91% | 48.62% | +0.71 pp |
| 562HAZ | 12.80% | 10.93% | -1.87 pp |
| 562212 | 8.15% | 8.15% | +0.00 pp |
| 562213 | 1.35% | 1.04% | -0.31 pp |
| 562910 | 14.60% | 15.21% | +0.61 pp |
| 562920 | 5.42% | 4.64% | -0.78 pp |
| 562OTH | 9.76% | 11.39% | +1.63 pp |

**Use waste×waste intersection (shipper→receiver shares)**

Max abs cell Δ (context only; not dominating-slice input): `562HAZ`←`562HAZ` = +21.73 pp (2017=57.98%, 2024=79.72%).

Diagonal cells (receiver = shipper):

| Child | 2017 diag | 2024 diag | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 0.00% | 0.00% | +0.00 pp |
| 562HAZ | 57.98% | 79.72% | +21.73 pp |
| 562212 | 0.06% | 0.13% | +0.07 pp |
| 562213 | 0.00% | 0.00% | +0.00 pp |
| 562910 | 0.13% | 0.05% | -0.08 pp |
| 562920 | 0.00% | 0.05% | +0.05 pp |
| 562OTH | 1.33% | 0.13% | -1.20 pp |

<!-- /AUTO:WEIGHT_DELTA_2017_VS_2024 -->

### 3.1 Per-year summary (GCS, year-aligned vs 2017)

<!-- AUTO:PER_YEAR_SUMMARY -->
| Year | N median % | N p95 \|%\| | Waste N max \|%\| mover | Notes |
|------|------------|-------------|-------------------------|-------|
| 2018 | -0.39% | 1.59% | 562OTH (+25.2%) | EC freeze |
| 2019 | -0.36% | 1.54% | 562OTH (+17.7%) | EC freeze |
| 2020 | -0.39% | 1.96% | 562920 (+15.3%) | EC freeze |
| 2021 | -0.43% | 2.13% | 562HAZ (-28.5%) | EC freeze |
| 2022 | -0.30% | 1.36% | 562HAZ (-16.3%) | EC 2022 |
| 2023 | -0.35% | 1.59% | 562HAZ (-25.4%) | AIES |
| 2024 | -0.29% | 1.40% | 562213 (+23.4%) | AIES |
<!-- /AUTO:PER_YEAR_SUMMARY -->

Economy-wide, the weight update is a **small, consistently slightly negative** shift in median N (roughly −0.01% to −0.43% across years after post-recovery regen). Tail width (p95 of \|N %\|) stays on the order of **~0.1–2%**. Direct EF (D) is essentially unchanged for most non-waste sectors (median D % ≈ 0 in the 2024 summary); large D moves concentrate in waste children where industry mix and intersection shares change most.

Within waste, the **largest mover changes by year**. After prior-weighted fill + chain, mid-panel extremes are moderate (e.g. 2022 `562HAZ` **−16%**, 2021 `562HAZ` −28.5%) — the equal-fill Flip interim’s +150% 2022 HAZ spike and +319% 562213 YoY rebound are retired. 2024’s largest waste mover is `562213` (+23.4%).

### 3.2 Figures by year

Each year has three charts: economy-wide **N** percent-diff histogram, economy-wide **D** percent-diff histogram, and waste-sector **N/D** percent bars (treatment vs control).

<!-- AUTO:FIGURES_BY_YEAR -->
### 2018

Charts (N / D histograms + waste-sector bars): [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) — `impact_2018_v0.3.0_92b7a8a_*.png` (local copies under `figures/` stay untracked).

### 2019

Charts (N / D histograms + waste-sector bars): [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) — `impact_2019_v0.3.0_92b7a8a_*.png` (local copies under `figures/` stay untracked).

### 2020

Charts (N / D histograms + waste-sector bars): [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) — `impact_2020_v0.3.0_92b7a8a_*.png` (local copies under `figures/` stay untracked).

### 2021

Charts (N / D histograms + waste-sector bars): [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) — `impact_2021_v0.3.0_92b7a8a_*.png` (local copies under `figures/` stay untracked).

### 2022

Charts (N / D histograms + waste-sector bars): [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) — `impact_2022_v0.3.0_92b7a8a_*.png` (local copies under `figures/` stay untracked).

### 2023

Charts (N / D histograms + waste-sector bars): [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) — `impact_2023_v0.3.0_92b7a8a_*.png` (local copies under `figures/` stay untracked).

### 2024

Charts (N / D histograms + waste-sector bars): [issue #1031](https://github.com/cornerstone-data/bedrock/issues/1031) — `impact_2024_v0.3.0_92b7a8a_*.png` (local copies under `figures/` stay untracked).
<!-- /AUTO:FIGURES_BY_YEAR -->

### 3.3 Caveats

1. Control still embeds workbook **2012** RCRA intersection; treatment uses **BR ≥2017** shipper→receiver (`rcra_path=br_bypass`). Part of every year’s delta is that RCRA refresh, not industry mix alone. **Settled for Flip**: BR bypass is OK until follow-on BR→FBS work; do not treat bypass retirement as a remaining Flip gate.
2. Who-buys for 2018–2021 stays on **bare EC 2017** while industry mix moves with SAS — **who-buys search `freeze_confirmed`** ([`who_buys_search.md`](who_buys_search.md)); intentional, not a Flip blocker. SAS-scale / §11 remain optional hardening only.
3. SAS Table 3 industry mix **and** Table 2 Use-row revenue apply **prior-weighted suppression recovery** under NAICS `562` ([`waste_n_variance.md`](waste_n_variance.md)). Impact caches `impact_{Y}_v0.3.0_92b7a8a/` regenerated 2026-09-30 under prior-weighted fill + AIES-only industry-mix chain.
4. Figures are on GCS MUT `v0.3.0_92b7a8a`. Durable copies live on [#1031](https://github.com/cornerstone-data/bedrock/issues/1031) (local `impact_*.png` untracked).

---

## 4. Conclusion

Updating waste disaggregation weights to align with each nowcast year **does change emission factors**, but the economy-wide footprint of that change is **modest and stable across 2018–2024**:

- Median total EF (N) shifts slightly **downward** every year (about **−0.01% to −0.43%** after post-recovery regen).
- Large absolute moves are **concentrated in waste children**, not in a broad rewriting of non-waste N; direct EF (D) for most of the economy stays near zero change.
- Tail risk (p95 of \|N %\|) remains on the order of **~0.1–2%**, with wider tails in the mid-panel (around 2021) and the narrowest in the AIES years (2023–2024).
- Within waste, **year-aligned data matter**: the identity of the largest mover and the sign/magnitude of child N/D shifts change as SAS → EC 2022 → AIES sources come online — evidence that freezing 2017 shares on a moving MUT is not a neutral choice for waste-sector EFs.

**Interpretation for the nowcasting approach:** year-matched waste weights on a pinned GCS MUT remove a known structure/dollar mismatch without destabilizing economy-wide N/D. That supports treating year-varying weights as a **credible alignment fix**, not a wholesale EF rewrite.

**Production status (Flip):** canonical nowcast YAMLs ship **`waste_weights_year: match_io`**. Settled path notes: BR RCRA bypass until follow-on BR→FBS work; RCRA ≥2017 refresh; who-buys 2018–2021 = bare EC 2017 (`freeze_confirmed`). Evidence: [`waste_n_variance.md`](waste_n_variance.md) (waste-N variance) + [`waste_y2y_comparison.md`](waste_y2y_comparison.md) (Y2Y comparison). Release note + snapshot ops: [`flip_release_note.md`](flip_release_note.md). Gate: [`production_gate.md`](production_gate.md).
