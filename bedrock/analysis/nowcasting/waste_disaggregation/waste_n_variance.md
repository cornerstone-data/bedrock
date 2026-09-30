# Waste-sector N variance (absolute EFs + weight slices)

**Pin:** Shared Google Cloud Make/Use tables (MUT) vintage `v0.3.0_92b7a8a` — impact caches **regenerated** after the SAS Table 2/3 suppression fix below.  
**Who-buys path:** who-buys search outcome `freeze_confirmed` — for model years 2018–2021, customer-class “who buys waste” shares stay on Economic Census **2017** (no intercensal 6-digit substitute found).  
**−100% semantics:** the by-year table’s `N_perc_diff` is **unclipped**. −100% means treatment total EF (N) ≈ 0 while control N > 0 — **not** the histogram display that clips at ±100%. Pre-fix caches showed −100% / −95% wipes; **post-fix** extremes below no longer wipe to zero.

Artifacts: `cache/impact_{Y}_v0.3.0_92b7a8a/waste_sectors_control_vs_treatment.csv`, `cache/weight_delta_2017_vs_{Y}.csv` (dominating slice = argmax of `{|use_col_delta|, |use_row_delta|, |intersection_diag_delta|}`).

---

## Plain-language takeaway (for Flip)

**Flip** = put year-matched waste weight shares into production. **HOLD** = keep today’s frozen 2017 shares.  
**N** = total (life-cycle) emission factor; **D** = direct emission factor. **Waste children** = seven detailed waste activities split from BEA’s single waste sector `562000`.

**What this note answers:** When we rebuild waste weight shares to match each nowcast year (instead of freezing 2017 shares), do some waste children’s emission factors look broken — and if so, is that a data bug or a real structure change?

**Short answer:** The scary −100% / −95% cases were a **data bug** (Census-hidden cells treated as true zero). After fixing that, the economy-wide picture stays calm, and the remaining large moves sit in a **few waste children**, not across the whole model. Flip is therefore a judgment about whether year-matched waste structure (including those concentrated child moves) is preferable to keeping outdated 2017 shares on a moving MUT — not about whether the update rewrites national EFs.

### What we saw at first (and why it looked like a blocker)

In the paired runs (same MUT dollars; only waste **percent shares** differ), two children jumped out:

- **2022 hazardous waste (`562HAZ`)** — treatment total EF went to **exactly zero** (−100% vs frozen-2017 control).
- **2021 combustors / incinerators (`562213`)** — treatment total EF fell to roughly **5% of control** (−95%).

Those look like “the new weights destroy the sector.” Digging into the weight tables showed something simpler: Census **SAS** (Service Annual Survey) marks some 6-digit waste lines as **suppressed** (flag `S`) with a published zero. Our loader treated that as “this industry has no expense / revenue,” so the child’s share of the waste split became 0%. The parent NAICS `562` (all waste management) totals were still published — so the money was there; the detail cells were just hidden for disclosure.

That is **not** the same as the MUT having $0 for waste, and it is **not** evidence that year-aligned weights are conceptually wrong.

### What we fixed (**suppression recovery**)

**Suppression recovery** = when Census hides detail but publishes the parent total, estimate the hidden lines from the shortfall instead of treating them as $0.

Take `parent(562) − sum(published detail)` and allocate that residual across suppressed waste lines using **prior-weighted** fills when same-table (or Table 3 AIES detail-dollar) priors exist; **equal** only when no prior is available and Flip/`match_io` is not requiring complete priors. Only Census flags **`S`/`D`** count as suppressed — not `(s)` or `Z`.

- **SAS Table 3** (firm expenses) → restores **industry mix** (how large each waste child is among waste firms) — also drives Make-column shares by construction.
- **SAS Table 2** (firm revenue) → restores **commodity / Use-row mix** (how waste output is split across children).

Every fill is recorded in derive provenance notes.

After regenerating the impact caches with that recovery:

| Case | Before fix | After equal-fill (interim Flip evidence) | After prior-weighted + chain (current) |
|------|------------|------------------------------------------|----------------------------------------|
| 2022 `562HAZ` | N = 0 (−100%) | N ≈ 3.0 vs control ≈ 1.2 (**+150%**) | N ≈ 1.02 vs control ≈ 1.22 (**−16%**) |
| 2021 `562213` | N ≈ 0.4 (−95%) | N ≈ 5.6 vs control ≈ 7.1 (**−20%**) | unchanged ≈ **−20%** (n=1 fill = residual) |

Equal-fill removed the wipe but overstated 2022 HAZ/562213 industry mix. **Prior-weighted** 2022 fill brings HAZ industry mix to ~10.8% and cuts the paired HAZ spike; the AIES-only **chain** removes the equal-fill 2022→2023 `562213` +319% YoY rebound from the production path.

### Where the leftover movement comes from (not a mystery box)

Weight shares have a few independent slices. For the two extremes after prior-weighted regen:

- **2022 `562HAZ`** — the biggest change vs 2017 is still the **waste×waste intersection** (Biennial Report / RCRA ≥2017), not a vanished industry mix. Industry mix for HAZ is **~10.8%** after prior-weighted recovery (was ~12% in 2017 CSVs; equal-fill had pushed it toward ~8% with an inflated 562213 sibling).
- **2021 `562213`** — industry mix is fine; the largest slice gap vs 2017 among industry mix / Use-row / intersection is the **commodity / Use-row** share (SAS Table 2 after recovery). **Who-buys** for 2018–2021 still uses Economic Census 2017 by design (`freeze_confirmed`).

### What Flip would and would not change (from this note alone)

- **Would not** broadly rewrite economy-wide total EFs. Median sector N moves about **−0.01% to −0.4%**; the fat tail of absolute moves is roughly **0.1–2%**. Direct EFs (D) for most non-waste sectors barely move.
- **Would** change **waste-child** EFs in some years by tens of percent (occasionally more), because those children are where the weight shares actually update.
- **Would** replace a known mismatch (2017 shares on 2018–2024 dollars) with year-matched shares that follow published SAS / AIES (Annual Integrated Economic Survey, post-SAS expenses) / Economic Census / RCRA inputs — including an explicit, documented guess for Census-suppressed cells.
- **Would still** use the temporary Biennial Report shipper→receiver path for waste×waste intersection until follow-on BR→FBS work, and Economic Census 2017 who-buys for 2018–2021 — both already accepted as OK for Flip.

**Decision framing for this note:** If the worry was “year-aligned weights zero out hazardous waste / incinerators,” that worry is resolved. Remaining concentrated waste-child EF shifts were accepted for production Flip (2026-09-29) — see [`flip_release_note.md`](flip_release_note.md). Pair with the year-to-year note ([`waste_y2y_comparison.md`](waste_y2y_comparison.md)) for how much movement happens even under frozen weights.

---

## SAS Table 2/3 suppression recovery (implemented)

**Problem found:** Treatment shares went to **0%** where Census marks cells **`Suppressed=S`** with `FlowAmount=0` — not measured $0.

| Slice | Source | Pre-recovery wipe example |
|-------|--------|---------------------------|
| Use/Make **column** (industry mix — size of each waste child among waste firms) | SAS **Table 3** expenses | 2022 `562HAZ` (−100% N) via 562112/562211 |
| Use **row** (commodity mix — split of waste output across children) | SAS **Table 2** revenue | 2021 `562213` Use-row share = 0 (also `S` on Table 2) |

**Pattern locked:**

| Metric | Result |
|--------|--------|
| Published parent control? | **Yes** — NAICS **`562`** total is unsuppressed on both Table 2 and Table 3 |
| Mask | Only Census **`S`/`D`** (not `(s)` sampling markup or `Z`) |
| Fill | **Prior-weighted** residual among suppressed detail NAICS; equal only if no prior and `require_complete_priors=False` |
| Table 3 priors | Nearest published same-table SAS expense year per NAICS in `{Y-1…2017}`; fallback **AIES 2023 detail-NAICS dollars** (not child-aggregated shares) |
| Table 2 priors | Nearest published same-table SAS **revenue** only — **no** AIES / Table 3 cross-table prior |
| Flip / `match_io` | `require_complete_priors=True` — raise if multi-suppress missing prior after table-specific rules |

**Implementation:** shared `recover_suppressed_sas_waste_detail` in [`sas_waste_metrics.py`](../../../extract/disaggregation/sas_waste_metrics.py); loaders own prior search:

- `load_sas_table3_expense_shares(year, *, require_complete_priors=False)`
- `load_sas_table2_revenue_shares(year, *, require_complete_priors=False)`

1. `residual = FlowAmount(562) − sum(published waste-detail amounts)`
2. Allocate `residual` with complete / partial / equal rules (tag `prior_weighted_residual` or `equal_residual`)
3. Append provenance notes on `WeightDerivationProvenance.fallback_notes`

**Table 3 expenses (live, prior-weighted):**

| Year | Parent | Published detail | Residual | `n_suppressed` | Fill (prior-weighted) | Prior year(s) |
|------|-------:|-----------------:|---------:|---------------:|-----------------------|---------------|
| 2022 | $95.778B | $84.242B | $11.536B | 3 | 562112≈$1.84B; 562211≈$8.50B; 562213≈$1.20B | 2021 SAS expenses for all three |
| 2021 | (562 control) | — | $1.440B | 1 | $1.440B (`equal_residual`, n=1) | — |

Equal-fill would have given each 2022 suppressed cell ≈$3.85B (overstating 562213). Prior-weighted restores a plausible 562213 share (~1.3 pp of industry mix).

**Table 2 revenue (live, prior-weighted):**

| Year | Parent | Published detail | Residual | `n_suppressed` | Fill (prior-weighted) | Prior year(s) |
|------|-------:|-----------------:|---------:|---------------:|-----------------------|---------------|
| 2021 | $124.319B | $123.113B | $1.206B | 1 | $1.206B | — |
| 2022 | $136.674B | $132.211B | $4.463B | 2 | 562112≈$3.06B; 562213≈$1.40B | 2021 SAS revenue |

Use column and Make column still share one industry-mix vector by construction (`derive_waste_weights`); Table 3 recovery lifts **both** together. Table 2 recovery restores Use-row commodity shares independently.

### SAS→AIES industry-mix share-seam grade (post prior-weighted fill)

Metric: \(\max_c \lvert 100\cdot s_{c,2023}^{\text{AIES}} - 100\cdot s_{c,2022}^{\text{SAS, post-fill}}\rvert\) among Cornerstone waste children (industry-mix shares only).

| Child | SAS 2022 post-fill (pp) | AIES 2023 (pp) | Δ pp |
|-------|------------------------:|---------------:|-----:|
| 562111 | 50.06 | 47.15 | −2.91 |
| 562HAZ | 10.79 | 9.92 | −0.87 |
| 562212 | 8.25 | 7.15 | −1.10 |
| 562213 | 1.25 | 0.93 | −0.32 |
| 562910 | 14.35 | 18.82 | **+4.46** |
| 562920 | 4.54 | 4.68 | +0.14 |
| 562OTH | 10.76 | 11.36 | +0.60 |

**max \|Δ\| = 4.46 pp ≥ 3 pp → chain triggers.** Production Flip/`match_io` holds post-fill SAS 2022 industry-mix shares for **2023** and moves **2024** by AIES-only 2024/2023 child-share ratios (`chain_waste_industry_mix_across_aies`; Decision 2 level override — AIES supplies YoY movement only). Independent of `chain_services_seed_across_aies`.

---

## (i) Absolute EF — required extremes (**post prior-weighted + chain** caches)

Caches under `cache/impact_{Y}_v0.3.0_92b7a8a/` regenerated 2026-09-30 after prior-weighted fill + AIES-only industry-mix chain.

### 2022 `562HAZ` — post prior-weighted recovery

| Metric | Control | Treatment | Notes |
|--------|--------:|----------:|-------|
| **N** (total EF) | 1.216 | **1.018** | No wipe; treatment slightly below control |
| **D** (direct EF) | 0.067 | 0.094 | D up while N down (structure mix) |
| `N_perc_diff` | — | **−0.163** (−16.3%) | Equal-fill Flip evidence had been **+150%** |

**Pre-recovery (historical):** treatment N/D = 0 → unclipped −100%. **Equal-fill interim:** ~+150% paired Δ from overstated residual mass into HAZ siblings. **Prior-weighted:** fills 562112/562211/562213 from 2021 SAS expense priors → HAZ industry mix ~10.8%; remaining paired Δ is structure (intersection / who-buys / commodity), not a suppression zero.

### 2021 `562213` — post recovery (n=1 → exact residual)

| Metric | Control | Treatment | Notes |
|--------|--------:|----------:|-------|
| **N** (total EF) | 7.057 | **5.622** | ~80% of control (was ~5% pre-recovery) |
| **D** (direct EF) | 6.885 | 5.475 | Same pattern |
| `N_perc_diff` | — | **−0.203** (−20.3%) | Unchanged vs equal-fill (single suppressed cell) |

**Pre-recovery (historical):** treatment N ≈ 0.355 (−95%). Both Table 3 and Table 2 had sole `S` on `562213`; n=1 fill equals the residual. Remaining −20% is a real share/structure shift, not a suppression zero.

Economy-wide paired median N % / p95 \|N %\| stay modest (by-year §3.1: ≈ −0.29% to −0.43% / ≈1.4–2.1% after regen). Variance concern remains **waste-child concentration**, not economy-wide rewrite.

---

## (ii) Weight-slice attribution — **post prior-weighted** shares

These are **percent shares among the seven waste children** (each vector sums to ~1), **not** BEA MUT dollar cells.

### 2022 `562HAZ` (after prior-weighted recovery)

From `cache/weight_delta_2017_vs_2022.csv`:

| Slice | 2017 CSV share | 2022 treatment share | Δ |
|-------|---------------:|---------------------:|--:|
| **Use column** (industry mix) | 0.121012 (~12.1%) | **0.107922 (~10.8%)** | −0.013090 |
| **Make column** (same industry-mix vector on Make) | 0.128026 (~12.8%) | **0.107922 (~10.8%)** | −0.020104 |
| **Use row** (commodity mix from SAS Table 2) | 0.103000 (~10.3%) | **0.087341 (~8.7%)** | −0.015659 |
| **Intersection diagonal** (same child ships to itself in waste×waste) | 0.579842 (~58.0%) | 0.797185 (~79.7%) | **+0.217343** |

**Dominating slice** (largest absolute Δ among industry mix / commodity Use-row / intersection diagonal): **`intersection`**

**Raw before recovery:** Table 3: 562112 / 562211 / 562213 = `S`; Table 2: 562112 / 562213 = `S`.  
**After prior-weighted recovery:** Table 3 fills → `562HAZ` industry-mix **~10.8%**; Table 2 fills → commodity Use-row **~8.7%**.

Who-buys / final-demand customer-class rows still from **Economic Census 2022** (`ecnclcust`). Intersection from the temporary **Biennial Report shipper→receiver path** (`rcra_path=br_bypass`; diagonal share up).

### 2021 `562213` (after recovery)

From `cache/weight_delta_2017_vs_2021.csv`:

| Slice | 2017 share | 2021 share | Δ |
|-------|-----------:|-----------:|--:|
| Industry mix (`use_col`) | 0.015102 | **0.016660** | +0.001558 |
| Commodity / Table 2 (`use_row`) | 0.015000 | **0.009701** | **−0.005299** |
| Intersection diagonal | ~0 | ~0 | 0.000 |

**Dominating slice:** **`who_buys`** in the locked CSV vocabulary — this means the **commodity / Use-row** gap (`|use_row_delta|` largest among industry mix / Use-row / intersection diagonal). It does **not** mean Economic Census customer-class who-buys changed for 2021 (those stay on EC 2017 under `freeze_confirmed`).

Industry mix and Use-row are both restored from their respective 562 residuals. Remaining Use-row Δ vs 2017 is a **share shift**, not a suppression wipe. Who-buys for 2018–2021 remains Economic Census 2017 (`freeze_confirmed`).

---

## Acceptance bar (variance portion)

1. Extremes documented with absolute N/D from **post prior-weighted** caches; −100%/−95% explained as pre-recovery suppression wipe; equal-fill +150% HAZ spike retired by prior-weighted fill — **met**.  
2. Economy-wide band still modest after regen — **met**.  
3–4. See Y2Y note ([`waste_y2y_comparison.md`](waste_y2y_comparison.md)) for control panel + stakeholder call.
