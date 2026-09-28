# Impact of year-aligned waste disaggregation weights (2018–2024)

**Audience:** reviewers deciding whether year-varying waste weights belong on Cornerstone’s nowcast production path.  
**Scope:** Phase 3 dual-MUT — control (frozen 2017 weights) vs treatment (weights rebuilt to match each nowcast year), both arms on the same MUT within each track. **Track A** = GCS pin `v0.3.0_f709829`; **Track C** = local rebuild `v0.1.0_9b75212` (not published to GCS).  
**Companion 2024-only note:** [`impact_nowcast_updated_weights.md`](impact_nowcast_updated_weights.md).  
**Production status:** default **HOLD** — see [`phase3_production_gate.md`](phase3_production_gate.md).

---

## 1. Introduction

Cornerstone’s nowcast path already builds an annual detail MUT for each calendar year 2018–2024, then runs the EEIO model to produce emission factors:

- **D** — direct emissions intensity (emissions per dollar of industry output)
- **N** — total (life-cycle) emissions intensity (direct + supply chain)

Waste disaggregation sits in the EEIO stage: after correspondence, the single BEA waste sector `562000` is split into seven Cornerstone waste activities using **percent-share weight tables** (not dollar IO). Today, production still applies the **2017** weight shares even though parent dollars move with each nowcast year. That year mismatch is the methodological tension this work addresses.

This report answers: *If we rebuild waste weight shares for each nowcast year 2018–2024 and hold everything else fixed, how do economy-wide and waste-sector emission factors change relative to the frozen-2017 baseline?*

Results below come from a paired A/B for every year: same MUT within a track, same configs except the waste-weight year on the treatment arm. Electricity steps were left **off** so the comparison isolates the weight update. Track C repeats that A/B on a **locally rebuilt** MUT panel so we can see whether the weight-update story depends on the GCS MUT cut.

---

## 2. Methods

### 2.1 Study design

| Arm | Waste weights | MUT | Electricity |
|-----|---------------|-----|-------------|
| **Control** | Bundled **2017** CSVs (production default) | Same pin as treatment within the track | Off |
| **Treatment** | Derived for year **Y** (`waste_weights_year: match_io`) | **Same** pin | Off |

**Track A (GCS)** configs:

- `2025_usa_cornerstone_v0_4_nowcast_{Y}_waste_weights_control.yaml`
- `2025_usa_cornerstone_v0_4_nowcast_{Y}_waste_weights_match_io.yaml`

**Track C (local MUT)** configs (analysis-only; do not overwrite Track A):

- `…_nowcast_{Y}_waste_weights_control_local.yaml`
- `…_nowcast_{Y}_waste_weights_match_io_local.yaml`

For each year we report percent differences (treatment − control) / control for N and D across sectors, plus focused waste-child charts. Track A artifacts: `cache/impact_{Y}_v0.3.0_f709829/`; figures `figures/impact_{Y}_v0.3.0_f709829_*.png`. Track C artifacts: `cache/impact_{Y}_v0.1.0_9b75212/` (separate dirs so GCS results are never overwritten).

### 2.2 Main plan decisions carried into this run

These choices come from the Phase 1 feasibility lock and the Phase 3 dual-MUT plan:

1. **EEIO-only change.** Weights update after correspondence; nowcast Steps 1–7 and GCS MUT contents are not rebuilt for this A/B.
2. **Industry mix (Use column sums).** SAS Table 3 total expenses by waste child NAICS for **≤2022**; AIES total firm expenses (`EXPS_TOT_DVAL`) for **2023–2024**. Do not use purchaser refuse / `EXPS_REFUSE` for child mix.
3. **Who buys waste (Use rows / FD).** Economic Census `ecnclcust` customer-class receipts. **EC 2022** for model years 2022+; **bare EC 2017 freeze** for 2018–2021 (`resolve_ec_year`) because there is no intercensal 6-digit who-buys substitute.
4. **Waste×waste Use intersection.** Rebuild from **RCRA ≥2017**, not the workbook’s 2012 RCRA embedded in the 2017 CSVs. Phase 3 uses a temporary Biennial Report shipper→receiver path recorded as `rcra_path=br_bypass` (CRHW FBS remains generation-only).
5. **Reject refuse for industry mix.** Purchaser refuse series stay on the aggregate nowcast seed only.
6. **Electricity off** for control vs treatment so deltas are attributable to weights.
7. **Same MUT on both arms within a track.** Track A pins one verified GCS MUT vintage for the full 2018–2024 panel. Track C rebuilds Steps 5→6→7 locally (**no `--gcs`**) and re-runs the same weight A/B on that local vintage so GCS vs local MUT drift can be checked.

### 2.3 MUT and other input vintages used here

**Track A MUT (both arms, all years):** `v0.3.0_f709829`  
Verified by GCS probe for a complete 2018–2024 panel (`cache/phase3_gcs_mut_vintage.json`). Control and treatment load this same pin via `nowcast_mut_vintage` in the year-pinned YAMLs.

**Track C MUT (both arms, all years):** `v0.1.0_9b75212`  
Recorded after a successful local Step 5→6→7 rebuild (`cache/phase3_local_mut_rebuild.json`). These MUTs are **local only** — they were **not** uploaded to GCS. If a newer full-panel MUT vintage is published to GCS later, re-probe / re-pin Track A (and optionally re-run Track C against that published cut) rather than treating `v0.1.0_9b75212` as a permanent production pin.

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

### 3.1 Per-year summary (Track A, GCS)

<!-- AUTO:PER_YEAR_SUMMARY -->
| Year | N median % | N p95 \|%\| | Waste N max \|%\| mover | Notes |
|------|------------|-------------|-------------------------|-------|
| 2018 | -0.39% | 1.59% | 562OTH (+25.2%) | EC freeze |
| 2019 | -0.36% | 1.53% | 562OTH (+17.8%) | EC freeze |
| 2020 | -0.40% | 2.01% | 562920 (+15.3%) | EC freeze |
| 2021 | -0.51% | 2.45% | 562213 (-95.0%) | EC freeze |
| 2022 | -0.62% | 2.63% | 562HAZ (-100.0%) | EC 2022 |
| 2023 | -0.27% | 1.24% | 562213 (+42.8%) | AIES |
| 2024 | -0.23% | 1.13% | 562213 (+63.8%) | AIES |
<!-- /AUTO:PER_YEAR_SUMMARY -->

Economy-wide, the weight update is a **small, consistently slightly negative** shift in median N (roughly −0.2% to −0.6% across years). Tail width (p95 of \|N %\|) stays on the order of **1–3%**. Direct EF (D) is essentially unchanged for most non-waste sectors (median D % ≈ 0 in the 2024 summary); large D moves concentrate in waste children where industry mix and intersection shares change most.

Within waste, the **largest mover changes by year**. Later years (especially 2023–2024 under AIES) highlight **562213** (solid waste combustors / incinerators) with large positive N/D shifts. Some earlier years show large absolute moves in **562OTH**, **562920**, or near-zeroing of **562HAZ** / **562213** under SAS-era shares — expected when child expense or intersection mass is suppressed or reallocated relative to the 2017 workbook baseline.

### 3.2 Figures by year

Each year has three charts: economy-wide **N** percent-diff histogram, economy-wide **D** percent-diff histogram, and waste-sector **N/D** percent bars (treatment vs control).

<!-- AUTO:FIGURES_BY_YEAR -->
### 2018

![2018 N](figures/impact_2018_v0.3.0_f709829_N_perc_diff_hist.png)

![2018 D](figures/impact_2018_v0.3.0_f709829_D_perc_diff_hist.png)

![2018 waste](figures/impact_2018_v0.3.0_f709829_waste_sectors_N_D_pct.png)

### 2019

![2019 N](figures/impact_2019_v0.3.0_f709829_N_perc_diff_hist.png)

![2019 D](figures/impact_2019_v0.3.0_f709829_D_perc_diff_hist.png)

![2019 waste](figures/impact_2019_v0.3.0_f709829_waste_sectors_N_D_pct.png)

### 2020

![2020 N](figures/impact_2020_v0.3.0_f709829_N_perc_diff_hist.png)

![2020 D](figures/impact_2020_v0.3.0_f709829_D_perc_diff_hist.png)

![2020 waste](figures/impact_2020_v0.3.0_f709829_waste_sectors_N_D_pct.png)

### 2021

![2021 N](figures/impact_2021_v0.3.0_f709829_N_perc_diff_hist.png)

![2021 D](figures/impact_2021_v0.3.0_f709829_D_perc_diff_hist.png)

![2021 waste](figures/impact_2021_v0.3.0_f709829_waste_sectors_N_D_pct.png)

### 2022

![2022 N](figures/impact_2022_v0.3.0_f709829_N_perc_diff_hist.png)

![2022 D](figures/impact_2022_v0.3.0_f709829_D_perc_diff_hist.png)

![2022 waste](figures/impact_2022_v0.3.0_f709829_waste_sectors_N_D_pct.png)

### 2023

![2023 N](figures/impact_2023_v0.3.0_f709829_N_perc_diff_hist.png)

![2023 D](figures/impact_2023_v0.3.0_f709829_D_perc_diff_hist.png)

![2023 waste](figures/impact_2023_v0.3.0_f709829_waste_sectors_N_D_pct.png)

### 2024

![2024 N](figures/impact_2024_v0.3.0_f709829_N_perc_diff_hist.png)

![2024 D](figures/impact_2024_v0.3.0_f709829_D_perc_diff_hist.png)

![2024 waste](figures/impact_2024_v0.3.0_f709829_waste_sectors_N_D_pct.png)
<!-- /AUTO:FIGURES_BY_YEAR -->

### 3.3 Track C — GCS vs local MUT (weight-update N under each cut)

Same control/treatment weight contrast as Track A, both arms pinned to the **local** MUT vintage `v0.1.0_9b75212` from Track B. Caches: `cache/impact_{Y}_v0.1.0_9b75212/` (see `cache/phase3_local_impact_index.json`).

**Important:** these MUTs exist **only on this machine** (Track B ran without `--gcs`). They are **not** a published GCS NowcastMUT vintage. When newer MUTs are uploaded to GCS, re-run Track A on the new pin (and/or rebuild Track C against that published vintage) — do not treat `v0.1.0_9b75212` as durable cloud evidence.

| Year | GCS N median % | Local N median % | Δ median (pp) | GCS N p95 \|%\| | Local N p95 \|%\| | Δ p95 (pp) | GCS waste max mover | Local waste max mover |
|------|----------------|------------------|---------------|-----------------|-------------------|------------|---------------------|-----------------------|
| 2018 | -0.39% | -0.39% | -0.00 | 1.59% | 1.59% | +0.00 | 562OTH (+25.2%) | 562OTH (+25.2%) |
| 2019 | -0.36% | -0.36% | +0.00 | 1.53% | 1.54% | +0.01 | 562OTH (+17.8%) | 562OTH (+17.7%) |
| 2020 | -0.40% | -0.40% | +0.00 | 2.01% | 2.01% | -0.01 | 562920 (+15.3%) | 562920 (+15.3%) |
| 2021 | -0.51% | -0.51% | +0.00 | 2.45% | 2.48% | +0.03 | 562213 (-95.0%) | 562213 (-95.0%) |
| 2022 | -0.62% | -0.63% | -0.01 | 2.63% | 2.69% | +0.06 | 562HAZ (-100.0%) | 562HAZ (-100.0%) |
| 2023 | -0.27% | -0.28% | -0.01 | 1.24% | 1.21% | -0.03 | 562213 (+42.8%) | 562213 (+43.0%) |
| 2024 | -0.23% | -0.23% | +0.00 | 1.13% | 1.11% | -0.02 | 562213 (+63.8%) | 562213 (+63.8%) |

**Reading:** under the local MUT cut, the weight-update N median and waste max-mover **match Track A to ~0.01 pp** on median and keep the same largest waste mover every year. Tail width (p95 \|N %\|) differs by at most ~0.06 pp (2022). For this panel, the waste-weight A/B story is **not** an artifact of the GCS `f709829` MUT alone.

### 3.4 Caveats

1. Control still embeds workbook **2012** RCRA intersection; treatment uses **BR ≥2017** shipper→receiver (`rcra_path=br_bypass`). Part of every year’s delta is that RCRA refresh, not industry mix alone.
2. Who-buys for 2018–2021 stays on **EC 2017** while industry mix moves with SAS — intentional under Decision 4, but those years are only partially year-aligned on the who-buys slice.
3. Track C’s MUT `v0.1.0_9b75212` is **local-only**. Prefer re-running against a **published GCS** vintage before treating Track C as release-blocking evidence; if GCS and local ever diverge materially after a new upload, prefer the published cut for flip decisions.

---

## 4. Conclusion

Updating waste disaggregation weights to align with each nowcast year **does change emission factors**, but the economy-wide footprint of that change is **modest and stable across 2018–2024**:

- Median total EF (N) shifts slightly **downward** every year (about **−0.2% to −0.6%**).
- Large absolute moves are **concentrated in waste children**, not in a broad rewriting of non-waste N; direct EF (D) for most of the economy stays near zero change.
- Tail risk (p95 of \|N %\|) remains on the order of **1–3%**, with the widest tails in the mid-panel (around 2021–2022) and the narrowest in the AIES years (2023–2024).
- Within waste, **year-aligned data matter**: the identity of the largest mover and the sign/magnitude of child N/D shifts change as SAS → EC 2022 → AIES sources come online — evidence that freezing 2017 shares on a moving MUT is not a neutral choice for waste-sector EFs.
- **Track C (local MUT)** reproduces the same weight-update N medians and waste movers as Track A (GCS). The alignment story is robust to this local vs GCS MUT difference — with the caveat that the local MUTs are unpublished and should be re-checked when newer MUT vintages land on GCS.

**Interpretation for the nowcasting approach:** year-matched waste weights remove a known structure/dollar mismatch without destabilizing economy-wide N/D in this dual-MUT design. That supports treating year-varying weights as a **credible alignment fix**, not a wholesale EF rewrite.

**What this report does not decide:** production still defaults to **HOLD**. Remaining gates include retiring or validating the BR RCRA bypass, re-running on any **new GCS MUT upload**, and an explicit stakeholder flip decision. Until then, canonical nowcast configs should continue to ship **2017** weight shares.
