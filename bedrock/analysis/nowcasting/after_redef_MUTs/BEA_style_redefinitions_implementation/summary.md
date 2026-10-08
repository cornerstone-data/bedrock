# Nowcasting redefinitions — review summary

**Date:** 2026-08-29
**Repo state reviewed:** `nowcast` / `jv_nowcast_after_redefinitions_MUTs` at `65d1709d` (*Provenance map for the other four SUT blocks (#756)*)
**Constraint:** review-only at writing; these planning docs now live under `bedrock/analysis/nowcasting/after_redef_MUTs/BEA_style_redefinitions_implementation/`. Implementation of the BEA-style path has been attempted; see `BEA-style_redefinitions_reconstruction_report.md`.

---

## 4.1 Current status of the nowcast project

### What bedrock is, and what nowcast is trying to add

`bedrock` is the pipeline that builds the Cornerstone U.S. environmentally-extended IO model. On `main` it starts from the **2017 BEA detail Make/Use/Import tables after redefinitions**, maps them into Cornerstone schema, applies config-gated methodology (waste disaggregation, electricity, GHG, etc.), and publishes EEIO artifacts (`B`, `Adom`, `Aimp`, `ytot`, …). Annual movement of *gross output* is already handled: `derive_gross_output_after_redefinition` reads BEA's underlying GO series and applies 2017 co-production *movement* ratios so industry totals sit on an after-redefinitions basis. The **tables themselves** stay the frozen 2017 after-redef MUT.

The nowcast project ([project 26](https://github.com/orgs/cornerstone-data/projects/26/views/1), milestone `v0.5`) replaces that frozen 2017 MUT with a constructed **2018–2025** MUT quartet: Make, Use (producer price), Import matrix, and Margins, **after redefinitions**, in Cornerstone schema, stored on GCS and consumed by the model-build pipeline. Phase 1 is 2018–2024 (all source data already published). Phase 2, after the BEA annual update expected by 30 Sep 2026, adds 2025 and refreshes 2018–2024.

Code for this work lives on the long-lived `nowcast` branch and is **not** on `main`. `plan.md` is absent from `main`. Locally, `nowcast` is a **single-commit orphan** (`65d1709d`) with **no git merge-base with** `main`. That is a repo-wide fact, not a prerequisite for #572 — do not rebase or recreate `nowcast` as part of this work.

Feature work for redefinitions is on `jv_nowcast_after_redefinitions_MUTs`. As of 2026-08-29 it is the same commit as `nowcast` (`65d1709d`), so it is already based on `nowcast`. Carry the implementation there and open the PR against `nowcast`, not `main`. If `nowcast` moves meanwhile, merge `nowcast` into the feature branch; do not rebase onto `main`.

### Pipeline, and why Step 7 sits where it does

```
NIPA / Census / trade sources
      →  SUT sections (Use FD, VA, intermediate; Supply output, imports, margins, taxes)
      →  RAS / GRAS  →  balanced SUT, BEA 2017 Detail, *before* redefinitions
      →  Step 6      →  MUT before redefinitions (Make, Use PRO, Import, Margins)
      →  Step 7      →  MUT after redefinitions          ← issue #572
      →  Step 8      →  Cornerstone schema
      →  Step 9      →  GCS → model build
```

That ordering is forced: the published SUT *is* a before-redefinitions construct, so everything upstream stays before-redefinitions and is redefined **once**, here. A leftover VA-only redefinition path would produce a mixed-state SUT (VA after, everything else before), which is why the old “transform VA FBS into after redefinitions” card was retired and folded into #572.

### How diagnostics work

Two layers, not one.

**EEIO /** `main` **diagnostics** (what the production model is doing): snapshot tests of the derived matrices against GCS parquet; `generate_diagnostics` writes emission-factor comparison sheets (CEDA-US v0, USEEIO, prior bedrock snapshots) via `dispatch_diagnostics.py`. Config flags isolate methodology choices.

**Nowcast diagnostics** (what this project uses): `bedrock.analysis.nowcasting.table_match` scores a candidate block against the published 2017 detail SUT cell by cell (`MATCH` / `PARTIAL` / `MISS` / `EXTRA` / `ABSENT`), plus row/column margins. `sections.py` registers each SUT block; `plots.py` draws it; `progress_report.md` is the human summary. Per-step `--check` scripts (margins, compensation, tax-axis conversion, intermediate drift, …) sit beside the transform modules. The design rule, stated in the plan and in #572: **totals cannot validate redefinitions or margins**. Both net to ~nothing economy-wide. Validation is cell-by-cell.

Step 7 does not yet have a `table_match` section. The same engine is the intended assertion: run the transform on the published 2017 *before* tables and score against the published 2017 *after* tables.

### Status by step (plan + code + issues, as of 2026-08-29)

Project 26 status columns could not be read (`gh` token lacks `read:project`). The picture below is from the plan, `progress_report.md` (re-run 2026-08-28 at `f02be4f8`), the `nowcast` tree, and the 79 `nowcasting`-labelled issues (39 open / 40 closed).


| Step                         | What it is                          | Status                                                                                                                                                                                                                                                                                                                     |
| ---------------------------- | ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **0 Hygiene**                | Summary SUT vintage, BEA code space | Effectively done. #573 closed. BEA code-space gate #566–#569 closed.                                                                                                                                                                                                                                                       |
| **1 Final demand (PUR)**     | 19 SUT FD columns                   | **In progress.** Live candidate. Coverage 88.5%, accuracy **59.1%** against 2017. All 19 columns sourced; mix/attribution still moving. Open: #762 (re-exports), #576 (NIPA reconcile), #606 (`S00300`), #670 / #703 / #747 (trade gaps), #660 / #650 (inventories splits).                                                |
| **2 Value added**            | 5 SUT VA rows                       | **Built 2017–2024.** Coverage 99.9%, accuracy 79.4% — the entire shortfall is `T00TOP`. `T00SUB` matches 2017 exactly. `V00100`/`V00300`/`T00OTOP` are seeds; Step 5 holds `VAPRO` via GO. Open follow-up: #689 (federal subsidies).                                                                                       |
| **3 Intermediate**           | 402 × 402 Use interior              | **Built.** 100% vs 2017 is **circular** (seeded from that table). Real content is the annual mix movement (survey seeds + inflation carry). Open: #705 (columns that drift), #699 (theta), #578 (government), #707 (AIES 2024).                                                                                            |
| **4 Supply**                 | Domestic output + bridge            | **Mostly built.** 4a (interior) 100% coverage / 99.6% accuracy, near-circular on the 2017 mix. 4b imports, 4c `TRADE`/`TRANS`, 4d taxes/subsidies, 4e identities: closed. Open: #615 (NAPCS product-line margins), #672 (transport weights), #723 / #722 / #724 (moving the 4a mix off 2017), #763 (import split holdout). |
| **5 Balance**                | GRAS on the SUT                     | **Engine exists, not yet a nowcast.** `gras_balance`, mask, target set, Use-then-Supply `engine`, and a first soft layer have landed. `WEIGHTS` uncalibrated. **Has never been run on a nowcast seed** (`progress_report.md`). Open: #588 (parent), #749 (wiring / structural zeros / TRADE label).                        |
| **6 SUT → MUT before redef** | Make, Use PRO, Import, Margins      | **Not started.** #582, #697 (6b), #583, #584, #745, #585 (2017 replay) all open. 4c already produces the transaction-level margins 6d mostly reshapes.                                                                                                                                                                     |
| **7 Redefinitions**          | Before → after MUT                  | **Not started.** [#572](https://github.com/cornerstone-data/bedrock/issues/572) open, 0 comments, assigned WesIngwersen. No transform module. The only related code is `compute_coproduction_ratios` / `adjust_gross_output`, which move the **GO vector**, not the four tables.                                           |
| **8 Schema**                 | BEA detail → Cornerstone            | **Not started.** #586. Existing `industry_corresp` / `commodity_corresp` on `main` are the template.                                                                                                                                                                                                                       |
| **9 Storage / pipeline**     | GCS + model build                   | **Not started.** #592, #593.                                                                                                                                                                                                                                                                                               |


**Critical path as of the 2026-08-19 re-rank (still accurate):** finish Step 5 on a real seed → Step 6 (especially the 2017 replay, #585) → **#572** → Step 8 → Step 9. The plan still lists #572 as P2, after the 2017 SUT→MUT replay.

**However:** Step 7's *algorithm* can be developed and accepted against the published 2017 before/after MUT pair **without waiting for Step 6**. All eight loaders already exist in `bedrock/extract/iot/io_2017.py`. What Step 6 blocks is applying the transform to a *nowcast* year, not proving it on 2017.

### What already exists that Step 7 will use

- Before/after 2017 MUT loaders for Make, Use (intermediate 402 × 402), Import (402 × 402), Margins.
- `compute_coproduction_ratios(V_before, V_after)` → movement as a share of source-industry GO; `adjust_gross_output`; `test_2017_redefinition_roundtrip` (integration). This is the issue's named template. It is **not** the Chapter 9 algorithm, and it operates on the GO *vector*, not on Use/Import/Margins.
- `table_match` / `sections` for cell-by-cell scoring.
- Measured 2017 Use-intermediate magnitudes (from `About_BEA_IOT_table_valuation_differences.md`): **5,740 of 161,604** intermediate cells differ; **553,635 million** gross movement; largest cell **42,893**; net **−7**.



### Gaps that will bite Step 7 even on 2017

- `load_2017_Utot_*` is commodity × industry only. Value added is a separate after-redef-only loader (`load_2017_value_added_usa` → `V00100` / `V00200` / `V00300`). There is **no before-redef VA loader**. Chapter 9 reallocates those three rows with the rest of the Use column, so a before-redef slice is **required**. Add `load_2017_value_added_before_redef_usa()` next to the existing after loader — same Excel `load_2017_Utot_before_redef_usa` already reads, just the VA rows that loader currently drops. This is not a fifth deliverable: #572's quartet stays Make / Use / Import / Margins; VA rides inside Use in BEA's workbook and is only a separate object because bedrock sliced it that way.
- `load_2017_Ytot_usa` (final demand) is after-redef only. That is fine: Chapter 9 does not move final uses.
- 2012 extract (`io_2012.py`) has after-redef Make/Use only (`VR` / `UR`). **Do not run a second validation on 2012.** There is no before-redef 2012 pair in the repo, and this plan does not add one. The 2017 before/after pair is the reconstruction gate. Application to 2018–2025 is a later nowcast-year call, not a second benchmark test.

---



## 4.2 BEA's redefinitions methods (concise)

Source: [BEA, *Concepts and Methods of the U.S. Input-Output Accounts*, September 2006](https://www.bea.gov/sites/default/files/methodologies/IOmanual_092906.pdf), primarily **Chapter 4** (what gets redefined) and **Chapter 9** (how inputs move). Chapters 5, 11, and the glossary support.

### Why two sets of tables exist

BEA publishes a **standard** (featured, NAICS-consistent) make/use pair **before redefinitions**, and a **supplementary** pair **after redefinitions**. Since 1997 the featured tables are the before-redef ones, so they stay comparable to other industry statistics. The after-redef tables exist to make each industry's input structure more homogeneous, which is what the total-requirements tables are built from. Commodity output is the same in both; industry output is not.

Redefinition is **not** the same as reclassification:


|                              | Affects                               | Which tables                                                           |
| ---------------------------- | ------------------------------------- | ---------------------------------------------------------------------- |
| **Reclassification**         | Commodity output, not industry output | Standard *and* supplementary (done while building the standard tables) |
| **Redefinition**             | Industry output, not commodity output | Supplementary only                                                     |
| **Other secondary products** | Neither                               | Left on the producing industry in both sets                            |




### What gets redefined (Chapter 4)

A secondary product is redefined when its **input pattern differs substantially** from the producing industry's primary product — a move toward the commodity-technology assumption. Standing rules:

- Construction (including force-account / own-account) → construction. Some manufacturing installation receipts too.
- Manufacturing in non-manufacturing → manufacturing (e.g. meat slaughtering in wholesale → meatpacking).
- Trade in non-trade → trade. **No wholesale ↔ retail redefinitions.** Reselling by household-facing services → retail; reselling by business-facing establishments (e.g. manufacturing) → wholesale. The amount redefined is the **margin** (sales less cost of goods sold), not gross sales.
- Rental of equipment and vehicles → rental / real estate.
- Services in non-services, and some service ↔ service (e.g. repairs by lessors → repair).

Explicit **exceptions**:

- **Captive** activities where the buyer cannot go elsewhere (airline/rail meals, theater and sports concessions) stay with the host industry.
- **Within manufacturing**, Census-identified secondary products are generally *not* redefined (simplification, not a strict reading of the rule).
- Structure/land rents of non-real-estate firms are an **implicit** redefinition: they already sit in real estate in *both* tables.
- Tiny service-to-service secondaries (the manual's example: < $5 million) may be left as “other secondary.” If two service industries swap secondaries, treat both the same way.

On the **Make** table this is a row move: secondary output of commodity *c* by industry *i* is subtracted from row *i* and added to the row of the industry for which *c* is primary. At BEA detail, that destination industry code equals the commodity code. Column sums (commodity output) are unchanged; row sums (industry output) change.

### How inputs move (Chapter 9 — reallocations)

Redefining output is not enough. The intermediate inputs **and** value-added components used to produce that output have to move with it. Those transfers are **reallocations**. They are the transit from the use table before redefinitions to the use table after.

**Default estimator — destination-industry direct requirements:**

1. From the Make movement, the redefined output amount is R (source industry *i* → destination industry *d*, the primary producer of that commodity).
2. From the **before-redef Use** table, destination *d*'s input coefficients are b_{c,d} = U_{c,d} / x_d.
3. The reallocation vector is b_{\cdot,d} \times R. Subtract it from column *i*, add it to column *d*.

Worked example in the manual (two industries): $10 of commodity B produced by A is redefined to B. Industry B's recipe is 20% A, 30% B, 50% value added, so the $10 moves as $2 of A, $3 of B, $5 of VA. Final uses and commodity output do not change. Industry output of A falls $10; of B rises $10. The sum of reallocations **equals** the redefined output (purchaser value, including taxes).

**That default is not what BEA always does.** The manual is explicit:

- Reallocations use “an input structure appropriate for the redefined output and **not necessarily all inputs of the primary industry**” (Ch. 12 n. 14).
- **Own-account software** uses a *standard* distribution for every industry: compensation, rent, electricity, office supplies, depreciation.
- **Small manufacturing resale** (the wholesale-margin redefinition) reallocates **only compensation and gross operating surplus**, not a full wholesale recipe.
- **Large** redefinitions get **hand-built** reallocations: own-account construction by homeowners / electric utilities / telephone companies; auto repair by new-car dealers; gaming at casino hotels; meals at lodging; auto leasing by finance companies.
- After the mechanical pass, analysts **review and patch**: confirm the source column actually contains the inputs being removed (else the after table goes negative — usually meaning those inputs were never in the source estimate); confirm inappropriate leftovers are gone (beef should not remain in hotels); confirm the identity (reallocations = redefined output).

Negatives are a signal, not a rounding error. They mean the originally estimated inputs omitted the secondary product's recipe.

### What Chapter 9 does *not* specify

The 2006 manual is written for make and use. It does not describe an import matrix or a margins dataset. For those, the implication of the method — not a published recipe — is:

- **Final demand columns do not move.**
- **Imports** that were used to produce the redefined output should move with the Use column (or be re-derived from the after-redef Use by the same proportionality Step 6c uses).
- **Margins** are a (buyer, commodity) transaction table. When a Use cell's buyer industry changes, the matching margins row should change with it.



### Why a 2017 cell-ratio carry is a different method

Issue #572 and `plan.md` §Step 7 propose deriving per-cell ratios from the 2017 before/after pair, on the pattern of `compute_coproduction_ratios`. That **describes** the 2017 movement and, applied back to 2017, round-trips by construction. It does not implement Chapter 9: it does not use destination-industry recipes, does not distinguish “other secondary” from redefined secondary except insofar as 2017 happened to move the cell, and it freezes 2017 *shares of source-industry GO* rather than applying a production-function rule to the nowcast year's Make/Use. For a nowcast, that is the wrong object. The 2017 pair should teach (a) **which** (industry, commodity) pairs are redefinitions and (b) **where** the destination-recipe rule is not enough.

The two methods share loaders, `table_match`, and the 2017 exact-match gate. The code in the middle is different:

| | #572 ratio carry | Chapter 9 path (these docs) |
|---|---|---|
| **What you store from 2017** | One number per cell (or per Make off-diagonal): movement ÷ source GO, or `after/before` | A list of `(source, commodity)` pairs + share + which rule (default dest-`B`, wholesale-VA-only, software strip, named custom, …) |
| **Make, year `t`** | `move = ratio × x[t][source]` — scales with industry size | `move = share × V[t][source, commodity]` — scales with that secondary product |
| **Use / VA, year `t`** | Scale the 2017 cell delta, or multiply the cell by `after_2017/before_2017` | `dest_recipe[t] × move`. New cells appear in the dest column; zeros stay zeros unless the recipe puts mass there |
| **2017 test** | One multiply; equality is automatic | Run the recipe, then add C1–C6 until equality. The overlay is extra code and a residual table |
| **Nowcast year** | Same ratios, new `x` (or new `before` cells) | Same pair list and rule IDs; new `R` and new dest `B` from year `t` |
| **Volume** | Short: ratio builder + apply, ~the existing GO helper × 4 tables | Longer: census → classify → default operator → named rules → residual overlay |

It is not a thicker comment around `compute_coproduction_ratios`. The 2017 object is a **classification + rule table**, not a dense ratio matrix — except the leftover overlay, which is dense and 2017-only.

---



## 4.3 Next steps for implementing issue #572

Implement #572 as the three-part goal, not as a 2017 ratio dump. Detailed sequencing is in [bea-style-plan.md](bea-style-plan.md). Short version:

### 0. Align the issue with the method

#572's body still says “derive per-cell redefinition ratios … same idea as `compute_coproduction_ratios`.” That will exactly recreate 2017 and teach nothing about 2018–2025. Update the issue (and `plan.md` §Step 7) so acceptance is:

1. A Chapter 9 implementation (Make move + destination-recipe Use reallocation, with the documented special cases as named rules).
2. A 2017 reconstruction report that **quantifies the residual** against the published after tables (cells, dollars, by redefinition type) — not a silent exact match.
3. Residual rules, added only for structure that the algorithm misses, until the 2017 after tables match within publication rounding.
4. The four magnitude checks (5,740 cells / 553,635 gross / 42,893 largest / net −7) still required, on the *observed* 2017 before→after delta, as a sanity check that we are looking at the right pair.

Do **not** wait for Step 6 to start. The 2017 loaders are the acceptance fixture.

### 1. Measure the 2017 movement (no algorithm yet)

Write an analysis module that loads the eight published tables and reports, per deliverable:

- Which Make off-diagonals move (redefinitions) vs stay (other secondary). This *is* the classification Chapter 4 would have produced for 2017.
- Use-intermediate delta vs the four published magnitudes.
- VA (`V00100`/`V00200`/`V00300`) before vs after — requires a new before-redef VA slice from the same Excel the Utot-before loader already reads.
- Import and Margins deltas; confirm FD is unchanged.
- Identity checks: commodity output unchanged; industry output changes by the Make row movement; sum of Use-column reallocations equals redefined output.

This is the diagnostic #572 is missing. `table_match` is the scorer; a new `sections.py` entry per MUT deliverable is the CI assertion later.

### 2. Implement the Chapter 9 core on Make and Use, test on 2017

- **Make:** for each 2017-classified redefinition pair `(source, commodity)`, move `V[source, commodity]` (or the 2017 *fraction of that cell* that moved, if the move was partial) onto the destination industry. Not “all off-diagonals,” and not a share of total GO.
- **Use intermediate + VA:** for each such `R`, apply destination-industry `B` from the *before* Use table; subtract from source column, add to destination. Guard negatives (clip and renormalize, or skip and record). Enforce reallocations-sum-to-`R`.
- Score against published after-redef Make, Utot, and VA. **Expect a large, structured residual.** That residual *is* the finding for part (2) of the goal.



### 3. Add residual rules until 2017 matches

Layer rules in the order the manual names them, re-scoring after each:

1. Wholesale-resale redefinitions: VA-only (compensation + GOS), not the full wholesale recipe.
2. Own-account software: the standard five-input distribution, applied to every source industry.

   Own-account software is software a firm writes for **itself** (capitalized), not software it sells. BEA counts that as output of the writing industry and then redefines it to the software industry. Chapter 9 does **not** use the software industry's full input recipe for that move. It uses one fixed mix of five inputs — employee compensation, rent, electricity, office supplies, and depreciation — the same *shape* for every source industry; only the scale (`R`) changes. In code: one 5-element recipe, recovered once from the 2017 Use delta on those pairs, then `recipe × R` for each source. That is why this is a named rule (C2) and not the default destination-`B` operator.
3. Named large redefinitions (casino hotels, lodging meals, dealer auto repair, finance auto leasing, own-account construction of utilities/telecom/homeowners): replace dest-average `B` with a recipe recovered from the 2017 delta for that pair.
4. Negative-input repair: if the source column lacks an input the recipe wants, take what is there and make up the rest from VA (or the documented alternative), never go negative.
5. Leftover-input sweep: if source retains inputs that dest uses and the source primary should not, move them.
6. Only then, a **pair-specific overlay** for whatever 2017 cells still miss — recorded as data (a residual reallocation table), not as silent ratio magic — so it is obvious what is algorithm and what is 2017 calibration.

Stop when Make / Use / VA match the published after tables to BEA million-dollar rounding, and the four magnitude checks pass.

### 4. Import matrix and Margins

Chapter 9 does not define these. Two candidates, to be decided by which one reconstructs 2017:

- **A.** Move import (resp. margins) cells in lockstep with the Use (resp. buyer) movement already computed.
- **B.** Re-apply Step 6c proportionality (imports) / 6b margin identity (margins) to the after-redef Use.

Pick the one that matches 2017; if neither does, the residual overlay from step 3 covers them too.

### 5. Nowcast-year API, then wait on Step 6

The function signature should already be year-agnostic: `(V_before, U_before, Uimp_before, margins_before, va_before) → after quartet`, with the 2017-learned **classification** (which pairs redefine, which special rule applies) held fixed, and **amounts and recipes** taken from the year being transformed. Wiring it to Step 6's nowcast-year before-redef MUT is a later PR. Do not apply 2017 *dollar* deltas to 2018–2025.

### What not to do

- Do not implement `after = before * (after_2017 / before_2017)` per cell. It cannot create the cells reallocation must create, dies on zeros, and round-trips 2017 without testing the method.
- Do not validate on column or grand totals.
- Do not put a VA-specific redefinition anywhere in Steps 1–6.
- Do not collapse to Cornerstone schema first (Step 8 is after this; ratios/rules are BEA-detail objects).
- Do not treat `compute_coproduction_ratios` as the Use/Import/Margins implementation. Keep it for what it is: the GO-vector adjustment the rest of bedrock already uses on `main`.
