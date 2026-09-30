# Impact of year-aligned waste disaggregation weights (2018–2024)

**Audience:** reviewers deciding whether year-varying waste weights belong on Cornerstone’s nowcast production path.  
**Scope:** control (frozen **2017** production weights) vs treatment (weights rebuilt to match each nowcast year), both arms on the **same GCS** MUT pin `v0.3.0_92b7a8a`. Local / Track C MUT comparison is **out of scope** for this report.  
**Companion 2024-only note:** [`impact_nowcast_updated_weights.md`](impact_nowcast_updated_weights.md).  
**Production status:** **FLIP** — see [`phase3_production_gate.md`](phase3_production_gate.md) and [`phase32_flip_release_note.md`](phase32_flip_release_note.md).

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

These choices come from the Phase 1 feasibility lock and the Phase 3 analysis plan:

1. **EEIO-only change.** Weights update after correspondence; nowcast Steps 1–7 and GCS MUT contents are not rebuilt for this A/B.
2. **Industry mix (Use column sums).** SAS Table 3 total expenses by waste child NAICS for **≤2022**; AIES total firm expenses (`EXPS_TOT_DVAL`) for **2023–2024**. Do not use purchaser refuse / `EXPS_REFUSE` for child mix.
3. **Who buys waste (Use rows / FD).** Economic Census `ecnclcust` customer-class receipts. **EC 2022** for model years 2022+; **bare EC 2017 freeze** for 2018–2021 (`resolve_ec_year`) because there is no intercensal 6-digit who-buys substitute.
4. **Waste×waste Use intersection.** Rebuild from **RCRA ≥2017**, not the workbook’s 2012 RCRA embedded in the 2017 CSVs. Phase 3 uses a temporary Biennial Report shipper→receiver path recorded as `rcra_path=br_bypass` (CRHW FBS remains generation-only).
5. **Reject refuse for industry mix.** Purchaser refuse series stay on the aggregate nowcast seed only.
6. **Electricity off** for control vs treatment so deltas are attributable to weights.
7. **Same GCS MUT on both arms.** One verified GCS `nowcast_mut_vintage` for the full 2018–2024 panel on control and treatment.

### 2.3 MUT and other input vintages used here

**GCS MUT (both arms, all years):** `v0.3.0_92b7a8a`  
Verified by GCS probe for a complete 2018–2024 panel (`cache/phase3_gcs_mut_vintage.json`). Control and treatment load this same pin via `nowcast_mut_vintage` in the year-pinned YAMLs.

**Treatment weight-input vintages by model year** (resolver / carry rules as implemented on the derive path):

| Model year | Industry mix (Use columns) | Who-buys / FD (EC) | RCRA intersection (BR odd-year carry) | Commodity row-sum proxy |
|------------|----------------------------|--------------------|----------------------------------------|-------------------------|
| 2018 | SAS Table 3 (2018) | EC **2017** (freeze) | →2017 BR | SAS revenue |
| 2019 | SAS Table 3 (2019) | EC **2017** (freeze) | 2019 BR | SAS revenue |
| 2020 | SAS Table 3 (2020) | EC **2017** (freeze) | →2019 BR | SAS revenue |
| 2021 | SAS Table 3 (2021) | EC **2017** (freeze) | 2021 BR | SAS revenue |
| 2022 | SAS Table 3 (2022) | EC **2022** | →2021 BR | SAS revenue |
| 2023 | AIES **2023** EXP01 | EC **2022** | →2021 BR | SAS **2022** carry |
| 2024 | AIES **2024** BASIC | EC **2022** | →2021 BR | SAS **2022** carry |

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

_2024 derive provenance:_ RCRA=2021, EC=2022, SAS=2022, AIES=2024/BASIC; notes: SAS Table 2 revenue waste suppression recovery (equal_residual under NAICS 562): parent=136674000000, published_detail=132211000000, residual=4463000000, n_suppressed=2, fill_each=2231500000, recovered_naics=['562112', '562213']; rcra_path=br_bypass; RCRA intersection from BR shipper->receiver rows (year=2021); bypasses CRHW FBS - temporary diagnostics path; Phase 4 owns FBS replacement; BR intersection stats: rows_seen=1829582, rows_used_received=1248765, rows_used_shipped=311228, rows_skipped_missing_ids=21699, rows_with_tons_or_ids=1559993, rows_skipped_non_waste_endpoint=1401094, rows_used_waste_intersection=158899.

**Industry mix — Use column sum (industry output)**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 47.60% | 45.59% | -2.01 pp |
| 562HAZ | 12.10% | 10.01% | -2.10 pp |
| 562212 | 8.27% | 7.03% | -1.24 pp |
| 562213 | 1.51% | 0.77% | -0.74 pp |
| 562910 | 16.60% | 19.86% | +3.25 pp |
| 562920 | 4.48% | 4.77% | +0.29 pp |
| 562OTH | 9.43% | 11.97% | +2.54 pp |

**Commodity mix — Use row sum (commodity output)**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 46.30% | 48.98% | +2.68 pp |
| 562HAZ | 10.30% | 8.13% | -2.17 pp |
| 562212 | 9.80% | 8.22% | -1.58 pp |
| 562213 | 1.50% | 1.63% | +0.13 pp |
| 562910 | 16.60% | 16.17% | -0.43 pp |
| 562920 | 7.53% | 5.89% | -1.64 pp |
| 562OTH | 7.97% | 10.97% | +3.00 pp |

**Make column sum**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 47.91% | 45.59% | -2.32 pp |
| 562HAZ | 12.80% | 10.01% | -2.80 pp |
| 562212 | 8.15% | 7.03% | -1.12 pp |
| 562213 | 1.35% | 0.77% | -0.58 pp |
| 562910 | 14.60% | 19.86% | +5.25 pp |
| 562920 | 5.42% | 4.77% | -0.66 pp |
| 562OTH | 9.76% | 11.97% | +2.21 pp |

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
| 2022 | -0.10% | 0.44% | 562HAZ (+150.2%) | EC 2022 |
| 2023 | -0.05% | 0.26% | 562213 (+42.8%) | AIES |
| 2024 | -0.01% | 0.08% | 562213 (+63.9%) | AIES |
<!-- /AUTO:PER_YEAR_SUMMARY -->

Economy-wide, the weight update is a **small, consistently slightly negative** shift in median N (roughly −0.01% to −0.43% across years after post-recovery regen). Tail width (p95 of \|N %\|) stays on the order of **~0.1–2%**. Direct EF (D) is essentially unchanged for most non-waste sectors (median D % ≈ 0 in the 2024 summary); large D moves concentrate in waste children where industry mix and intersection shares change most.

Within waste, the **largest mover changes by year**. Later years (especially 2023–2024 under AIES) highlight **562213** (solid waste combustors / incinerators) with large positive N/D shifts. Mid-panel extremes (2021–2022) are large but **no longer wipe to zero** after SAS Table 2/3 equal-residual suppression recovery — e.g. 2022 `562HAZ` +150% and 2021 `562HAZ` −28.5% reflect recovered shares vs 2017 control structure, not Census-`S`-as-zero.

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

1. Control still embeds workbook **2012** RCRA intersection; treatment uses **BR ≥2017** shipper→receiver (`rcra_path=br_bypass`). Part of every year’s delta is that RCRA refresh, not industry mix alone. **Settled for Flip** (Phase 3.2): BR bypass is OK until Phase 4; do not treat bypass retirement as a remaining Flip gate.
2. Who-buys for 2018–2021 stays on **bare EC 2017** while industry mix moves with SAS — **Phase 3.2.A `freeze_confirmed`** ([`phase32_who_buys_search.md`](phase32_who_buys_search.md)); intentional, not a Flip blocker. SAS-scale / §11 remain optional hardening only.
3. SAS Table 3 industry mix **and** Table 2 Use-row revenue apply **equal-residual suppression recovery** under NAICS `562` ([`phase32_waste_n_variance.md`](phase32_waste_n_variance.md)). Impact caches `impact_{Y}_v0.3.0_92b7a8a/` were **regenerated** after that recovery (full 2018–2024 panel).
4. Figures are on GCS MUT `v0.3.0_92b7a8a`. Durable copies live on [#1031](https://github.com/cornerstone-data/bedrock/issues/1031) (local `impact_*.png` untracked).

---

## 4. Conclusion

Updating waste disaggregation weights to align with each nowcast year **does change emission factors**, but the economy-wide footprint of that change is **modest and stable across 2018–2024**:

- Median total EF (N) shifts slightly **downward** every year (about **−0.01% to −0.43%** after post-recovery regen).
- Large absolute moves are **concentrated in waste children**, not in a broad rewriting of non-waste N; direct EF (D) for most of the economy stays near zero change.
- Tail risk (p95 of \|N %\|) remains on the order of **~0.1–2%**, with wider tails in the mid-panel (around 2021) and the narrowest in the AIES years (2023–2024).
- Within waste, **year-aligned data matter**: the identity of the largest mover and the sign/magnitude of child N/D shifts change as SAS → EC 2022 → AIES sources come online — evidence that freezing 2017 shares on a moving MUT is not a neutral choice for waste-sector EFs.

**Interpretation for the nowcasting approach:** year-matched waste weights on a pinned GCS MUT remove a known structure/dollar mismatch without destabilizing economy-wide N/D. That supports treating year-varying weights as a **credible alignment fix**, not a wholesale EF rewrite.

**Production status (Phase 3.2.D Flip):** canonical nowcast YAMLs ship **`waste_weights_year: match_io`**. Settled path notes: BR RCRA bypass until Phase 4; RCRA ≥2017 refresh; who-buys 2018–2021 = bare EC 2017 (`freeze_confirmed`). Evidence: [`phase32_waste_n_variance.md`](phase32_waste_n_variance.md) (3.2.B) + [`phase32_y2y_comparison.md`](phase32_y2y_comparison.md) (3.2.C). Release note + snapshot ops: [`phase32_flip_release_note.md`](phase32_flip_release_note.md). Gate: [`phase3_production_gate.md`](phase3_production_gate.md).
