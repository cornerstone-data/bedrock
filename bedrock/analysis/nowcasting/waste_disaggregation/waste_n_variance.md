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

We do that the same way elsewhere in bedrock when a parent total is published and several children are hidden: take `parent(562) − sum(published detail)` and split that residual **equally** across the suppressed waste lines.

- **SAS Table 3** (firm expenses) → restores **industry mix** (how large each waste child is among waste firms) — also drives Make-column shares by construction.
- **SAS Table 2** (firm revenue) → restores **commodity / Use-row mix** (how waste output is split across children).

Every fill is recorded in derive provenance notes.

After regenerating the impact caches with that recovery:

| Case | Before fix | After fix |
|------|------------|-----------|
| 2022 `562HAZ` | N = 0 (−100%) | N ≈ 3.0 vs control ≈ 1.2 (**+150%**) |
| 2021 `562213` | N ≈ 0.4 (−95%) | N ≈ 5.6 vs control ≈ 7.1 (**−20%**) |

So the “sector disappears” story is gone. What remains for `562HAZ` in 2022 is a **large increase** vs 2017 shares (hazardous waste is still in the mix, but year-2022 structure plus refreshed waste-firm-to-waste-firm shipments differ from the workbook). For `562213` in 2021, treatment is lower than control by about a fifth — a real share shift, not a wipe.

### Where the leftover movement comes from (not a mystery box)

Weight shares have a few independent slices. For the two extremes after the fix:

- **2022 `562HAZ`** — the biggest change vs 2017 is the **waste×waste intersection** (which waste firms ship to which other waste firms, from Biennial Report / RCRA data for ≥2017), not a vanished industry mix. Industry mix for HAZ is ~8% after recovery (was ~12% in 2017 CSVs) — smaller, but not zero.
- **2021 `562213`** — industry mix is fine; the largest slice gap vs 2017 is the **commodity / Use-row** share (SAS Table 2 after recovery). **Who-buys** (which customer classes purchase waste services) for 2018–2021 still uses Economic Census 2017 by design (`freeze_confirmed`); that is settled and not a Flip blocker.

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

**Pattern locked (same for both tables; best fit vs other bedrock recoveries):**

| Metric | Result |
|--------|--------|
| Published parent control? | **Yes** — NAICS **`562`** total is unsuppressed on both Table 2 and Table 3 |
| # suppressed detail cells under that control | Often **>1** (e.g. 2022 Table 3: three cells; 2022 Table 2: two) → **rejects** single-cell subtraction when `n>1` |
| Intermediate parents 5621/5622/5629? | **Absent** → **rejects** NAICS hierarchy walk (`estimate_suppressed_sectors_equal_attribution`) |
| Cross-table allocation prior? | Only **partial** overlap of which NAICS are `S` → prefer **equal** split on each table’s own residual |

**Implementation:** shared `recover_suppressed_sas_waste_detail` in [`sas_waste_metrics.py`](../../../extract/disaggregation/sas_waste_metrics.py), with wrappers:

- `recover_suppressed_sas_table3_waste_expenses` → `load_sas_table3_expense_shares`
- `recover_suppressed_sas_table2_waste_revenue` → `load_sas_table2_revenue_shares`

Same family as `estimate_suppressed_ec_pxi` (Economic Census product×industry equal-residual fill):

1. `residual = FlowAmount(562) − sum(unsuppressed waste-detail amounts)`
2. Split `residual` **equally** across suppressed detail NAICS that map to Cornerstone waste children
3. Tag filled rows `SuppressionRecovery='equal_residual'`; append provenance notes on `WeightDerivationProvenance.fallback_notes`

**Table 3 expenses (live):**

| Year | Parent | Published detail | Residual | `n_suppressed` | fill_each | Recovered NAICS |
|------|-------:|-----------------:|---------:|---------------:|----------:|-----------------|
| 2022 | $95.778B | $84.242B | $11.536B | 3 | ≈$3.845B | 562112, 562211, 562213 |
| 2021 | (562 control) | — | $1.440B | 1 | $1.440B | 562213 |

**Table 2 revenue (live):**

| Year | Parent | Published detail | Residual | `n_suppressed` | fill_each | Recovered NAICS |
|------|-------:|-----------------:|---------:|---------------:|----------:|-----------------|
| 2021 | $124.319B | $123.113B | $1.206B | 1 | $1.206B | 562213 |
| 2022 | $136.674B | $132.211B | $4.463B | 2 | ≈$2.232B | 562112, 562213 |

Use column and Make column still share one industry-mix vector by construction (`derive_waste_weights`); Table 3 recovery lifts **both** together. Table 2 recovery restores Use-row commodity shares independently.

---

## (i) Absolute EF — required extremes (**post-recovery** caches)

### 2022 `562HAZ` — post-recovery

| Metric | Control | Treatment | Notes |
|--------|--------:|----------:|-------|
| **N** (total EF) | 1.216 | **3.042** | No longer wiped; treatment > control |
| **D** (direct EF) | 0.067 | 0.126 | Same direction |
| `N_perc_diff` | — | **+1.502** (+150.2%) | Largest paired waste mover in 2022 |

**Pre-recovery (historical):** treatment N/D = 0 → unclipped −100%. Root cause was Table 3 `S` treated as true zero → 0% industry-mix share. **Not** MUT $0 for parent `562000`. Equal-residual fill (~8% industry mix) removes the wipe; remaining large **positive** paired Δ reflects restored HAZ mass plus Biennial Report waste×waste shipments / Economic Census 2022 who-buys vs frozen 2017 control shares.

### 2021 `562213` — post-recovery

| Metric | Control | Treatment | Notes |
|--------|--------:|----------:|-------|
| **N** (total EF) | 7.057 | **5.622** | ~80% of control (was ~5%) |
| **D** (direct EF) | 6.885 | 5.475 | Same pattern |
| `N_perc_diff` | — | **−0.203** (−20.3%) | No longer −95% wipe |

**Pre-recovery (historical):** treatment N ≈ 0.355 (−95%). Both Table 3 and Table 2 had sole `S` on `562213`; recovery restores industry-mix (~1.7%) and commodity / Use-row (~0.97%). Remaining −20% is a real share/structure shift (largest weight-slice gap still commodity / Use-row), not a suppression zero.

Economy-wide paired median N % / p95 \|N %\| stay modest (by-year §3.1: ≈ −0.01% to −0.43% / ≈0.1–2.1% after regen). Variance concern remains **waste-child concentration**, not economy-wide rewrite.

---

## (ii) Weight-slice attribution — **post Table 2+3 recovery** shares

These are **percent shares among the seven waste children** (each vector sums to ~1), **not** BEA MUT dollar cells.

### 2022 `562HAZ` (after equal-residual recovery)

From `cache/weight_delta_2017_vs_2022.csv`:

| Slice | 2017 CSV share | 2022 treatment share | Δ |
|-------|---------------:|---------------------:|--:|
| **Use column** (industry mix) | 0.121012 (~12.1%) | **0.080297 (~8.0%)** | −0.040715 |
| **Make column** (same industry-mix vector on Make) | 0.128026 (~12.8%) | **0.080297 (~8.0%)** | −0.047729 |
| **Use row** (commodity mix from SAS Table 2) | 0.103000 (~10.3%) | **0.081270 (~8.1%)** | −0.021730 |
| **Intersection diagonal** (same child ships to itself in waste×waste) | 0.579842 (~58.0%) | 0.797185 (~79.7%) | **+0.217343** |

**Dominating slice** (largest absolute Δ among industry mix / commodity Use-row / intersection diagonal): **`intersection`**

**Raw before recovery:** Table 3: 562112 / 562211 / 562213 = `S`; Table 2: 562112 / 562213 = `S`.  
**After recovery:** Table 3 fills → `562HAZ` industry-mix **~8.0%**; Table 2 fills → commodity Use-row **~8.1%**.

Who-buys / final-demand customer-class rows still from **Economic Census 2022** (`ecnclcust`). Intersection from the temporary **Biennial Report shipper→receiver path** (`rcra_path=br_bypass`; diagonal share up).

### 2021 `562213` (after equal-residual recovery)

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

1. Extremes documented with absolute N/D from **post-recovery** caches; −100%/−95% explained as pre-recovery suppression wipe; post-recovery shares + Table 2/3 method documented — **met**.  
2. Economy-wide band still modest after regen — **met**.  
3–4. See Y2Y note ([`waste_y2y_comparison.md`](waste_y2y_comparison.md)) for control panel + stakeholder call.
