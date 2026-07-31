# Nowcast US IOT — plan, Phases 1 and 2

Nowcasted national Supply/Use tables → Make/Use/Import/Margins deliverables, 2018-2025.

GitHub project: [cornerstone-data/projects/26 — "Nowcast US IOT Phase 1"](https://github.com/orgs/cornerstone-data/projects/26)
(33 items, milestone `v0.5`). Its description: *"Draw on and further improve code from flowsa, USEEIO
and useeior repositories that implement a nowcasting approach to estimate US **2018-2025** Make, Use
and Import Matrices."* Code lands on the long-lived `nowcast` integration branch, not `main`.

**Year scope is 2018-2025, split across two project phases:**

| | Scope | Timing | Gate |
|---|---|---|---|
| **Phase 1** (this plan's main body) | build the pipeline; 2018-2024 | now → Sept 2026 | all source data already published |
| **Phase 2** ([final section](#phase-2--2025-after-the-bea-annual-update)) | add 2025 **and** refresh 2018-2024 on the revised data | early Oct 2026 | BEA annual update, expected by **Sept 30, 2026** |

2025 is deliberately *not* in Phase 1: it sits beyond every published BEA annual table today — no
summary MUT, no summary SUT, no detailed gross output — so it cannot be sourced or controlled the way
2018-2024 can. The BEA annual update expected by Sept 30 closes that gap, and **also revises
2018-2024 retrospectively**, so Phase 2 re-runs the whole series rather than only appending a year.
Phase 1's outputs are therefore a working series, not a final one.

## What we are building, and in what order

**Intermediate product: a detailed, balanced SUT pair** — a Supply table and a Use table (Supply-Use
framework) at BEA 2017 Detail granularity, for each nowcast year. This is what the current work
assembles section by section.

**End product: the MUT quartet** — a **Make** table, a **Use** table (producer price), an **Import
matrix**, and a **Margins** dataset, all **after redefinitions**, in Cornerstone schema, stored on GCS
and consumed by the model-build pipeline.

Those are not the same thing, and the gap between them is three distinct pieces of work that the
earlier version of this plan did not lay out:

1. **Finish sourcing the SUT.** Several Supply-table columns and two Use-table columns still have no
   data source (§"Still unsourced" below). The Supply table in particular has barely been scoped —
   most of the plan to date has been Use-table work.
2. **Convert SUT → MUT.** Framework change *and* valuation change. Purchaser → producer requires the
   margins; Supply → Make requires transposition plus a basic → producer revaluation; the import
   matrix has to be split out of the Use table entirely.
3. **Apply redefinitions.** The SUT is a before-redefinitions construct; everything downstream in
   bedrock expects after-redefinitions MUT.

The pipeline, end to end:

```
NIPA/Census/trade sources
      ↓  (FBA → FBS, per section)
SUT Use (PUR) sections: final demand · value added · intermediate
SUT Supply sections:    domestic output · imports · margins · taxes-less-subsidies
      ↓  (RAS)
BALANCED SUT, BEA_2017_Detail, before redefinitions
      ↓  (framework + valuation conversion)
MUT before redefinitions: Make (PRO) · Use (PRO) · Import matrix · Margins
      ↓  (redefinition ratios from 2017 before/after benchmark)
MUT after redefinitions
      ↓  (industry_corresp / commodity_corresp)
Cornerstone schema → GCS → model build
```

**Why SUT first rather than building MUT directly.** The source data is natively SUT-shaped: NIPA final
demand is in purchaser prices (which is the SUT Use cell basis), imports/margins/product taxes arrive
as commodity-level aggregates (which is exactly the Supply table's trailing-column structure), and the
SUT balance identity (total supply at purchaser = total use at purchaser, per commodity) is the
cleanest thing to RAS against. Converting once, at the end, from a balanced SUT is less error-prone
than trying to hold producer-price MUT consistency through every section build.

## Framework facts this plan depends on

These are established in [`About_BEA_IOT_table_valuation_differences.md`](bedrock/analysis/compare_NIPA_to_IOT/About_BEA_IOT_table_valuation_differences.md)
(2017 detail, verified cell-for-cell there) and drive most of the design decisions below.

**Supply table structure** (commodity × industry cells = domestic output at *basic* value; trailing
columns bridge to purchaser):

```
T013 = T007 + MCIF + MADJ       total supply, BASIC        36,398,867
T014 = TRADE + TRANS            margins                             1
T015 = MDTY  + TOP   + SUB      taxes less subsidies          695,565
T016 = T013  + T014  + T015     total supply, PURCHASER    37,094,434
       T013  + T015             derived PRODUCER           37,094,432   (not published)
```

Two traps carried forward from that doc: `T014` nets to ~**1** economy-wide (a trade margin is added to
the good and subtracted from the trade commodity that earned it), so **any margin validation must be
per-commodity — an aggregate check will pass while every cell is wrong**. And `SUB` is stored
**negative** in the Supply table but positive in the Use table.

**Use table (SUT) structure**: cells at *purchaser* value, intermediate and final demand alike.
`T019` (total use, purchaser) = Supply's `T016` per commodity. Value added splits three ways:

```
VABAS = V00100 + T00OTOP + V00300
VAPRO = VABAS  + T00TOP  - T00SUB
T018  = T005   + VABAS            # intermediate at purchaser + VA at basic
```

**SUT vs MUT differences that bite:**
- **`F05000` (imports) is a MUT-only column.** It does not exist in the SUT Use table — imports enter
  on the *Supply* side as `MCIF` + `MADJ`. The current `derive_initial_Y_pur` reindexes to all 20
  `BEA_2017_FINAL_DEMAND_CODES`, which is the **MUT** column list; the SUT Use FD block is that list
  minus `F05000`. Needs reconciling (§Step 1d).
- Tax rows don't correspond: SUT splits `T00OTOP`/`T00TOP`/`T00SUB` where MUT carries one net `V00200`.
- `T018`, `VABAS`, `VAPRO` are SUT-only; `T006`, `T008` are MUT-only.
- `V00100`, `V00300`, `T005` exist in **both** with **different values** (compensation differs by 3
  million, gross operating surplus by 16). Nothing errors — you silently get the other number.
- **Redefinition moves money between cells while preserving every total**, so a totals check cannot
  tell you that you picked the wrong redefinition state. 5,740 of 161,604 intermediate cells differ;
  553,635 million moves gross; largest single cell shifts 42,893; net −7.

**Summary SUT needs its 2023-2024 vintage loaded (small, do it early).** BEA publishes summary SUT for
**2017-2024**, same span as summary MUT — but bedrock currently only has the **2017-2022** workbooks
wired up: `USA_SUMMARY_SUT_MAPPING_2017_2022`
([matrix_mappings.py:53-60](bedrock/utils/taxonomy/bea/matrix_mappings.py#L53-L60)) is the only SUT
mapping, and [`_load_usa_summary_sut`](bedrock/extract/iot/io_2017.py#L766-L795) hardcodes it, so
asking for 2023 or 2024 will fail on a missing sheet rather than fall back.

The fix mirrors what summary MUT already does: add a `USA_SUMMARY_SUT_MAPPING_2017_2024` constant for
the newer workbooks, upload them to `GCS_USA_SUP_DIR`, and give `_load_usa_summary_sut` the same
vintage-pinning branch `_load_usa_summary_mut` uses at
[io_2017.py:730-739](bedrock/extract/iot/io_2017.py#L730-L739) — older years stay pinned to the oldest
file containing them, so BEA's historical revisions don't silently move values under existing
consumers. Worth doing in Step 0: **summary SUT totals are the natural RAS control for every nowcast
year**, and two of the seven years can't be loaded today.

Two loose ends to tidy while in there: `_load_usa_summary_sut`'s `year` parameter is typed
`USA_SUMMARY_MUT_YEARS` (1997 onward), which admits years no SUT workbook contains — it wants its own
`USA_SUMMARY_SUT_YEARS` literal. And [`BEA.py:106,117`](bedrock/extract/bea/BEA.py#L106-L117) casts to
`USA_SUMMARY_MUT_YEARS` to satisfy that signature; the cast can go once the type is right.

**The published SUT is before redefinitions** (confirmed). This is why the detail SUT files
(`Supply_2017_DET.xlsx`, `Use_SUT_Framework_2017_DET.xlsx`) carry no before/after qualifier where MUT
ships both variants ([matrix_mappings.py:9-30](bedrock/utils/taxonomy/bea/matrix_mappings.py#L9-L30)) —
there is only one SUT, and redefinition is a MUT-framework step applied after conversion.

Two consequences run through the whole plan:
- **The build order is forced, not chosen.** Sourcing an SUT means working before redefinitions; the
  redefinition therefore has to happen after the SUT→MUT conversion, which is exactly Step 6 → Step 7.
- **Everything upstream should stay before-redefinitions for consistency**, including the Step 3
  intermediate seed. Seeding from `Use_SUT_Framework_2017_DET` gives a before-redef, purchaser-valued
  seed in one shot — the right basis on both axes. This also makes #497's instruction to work on the
  after-redefinitions Use table the outlier to reconcile rather than a constraint to design around
  (open question 3).

## Grounding: what already exists

**Module.** [`bedrock/transform/eeio/nowcast.py`](bedrock/transform/eeio/nowcast.py) exists and
currently implements exactly one thing: `derive_initial_Y_pur(year)` — the final-demand block of the
SUT Use table, purchaser price, BEA_2017_Detail schema, from the `NIPA_FD_<year>` FBS methods. It
handles the Cornerstone-schema collapse problem via `map_fbs_sectors_to_model_schema` applied to both
sector columns. A baseline validator exists at
[`bedrock/utils/validation/nowcast_initial_Y_pur_baseline.py`](bedrock/utils/validation/nowcast_initial_Y_pur_baseline.py).

**2017 benchmark loaders — all four end-product table types already load**, before *and* after
redefinitions, from [`io_2017.py`](bedrock/extract/iot/io_2017.py):

| Deliverable | After-redef loader | Before-redef loader |
|---|---|---|
| Make | `load_2017_V_after_redef_usa` | `load_2017_V_before_redef_usa` |
| Use (PRO) | `load_2017_Utot_after_redef_usa` | `load_2017_Utot_before_redef_usa` |
| Import matrix | `load_2017_Uimp_after_redef_usa` | `load_2017_Uimp_before_redef_usa` |
| Margins | `load_2017_margins_after_redef_usa` | `load_2017_margins_before_redef_usa` |

Plus SUT loaders (`_load_2017_detail_sut_usa`, `_load_usa_summary_sut`) and summary MUT loaders
(`load_summary_V_usa`, `load_summary_Utot_usa`, `load_summary_Uimp_usa`). **Every benchmark input the
conversion and redefinition steps need is already loadable** — no new extract work for the 2017 anchor.

**Existing machinery to reuse, not rebuild:**
- [`derive_PRO_to_PUR_ratio.py`](bedrock/transform/iot/derive_PRO_to_PUR_ratio.py) — margins-based
  PRO↔PUR ratios (`phi`), with margin filters, negative-margin treatment, and
  `derive_margins_cornerstone_usa_at_year(year)` / `_inflate_margins_to_year` already inflating margin
  components to a target year. Directly relevant to both the Supply-table margin columns and the
  PUR→PRO conversion.
- [`derived_gross_industry_output.py`](bedrock/transform/iot/derived_gross_industry_output.py) —
  `compute_coproduction_ratios` / `adjust_gross_output` (lines ~150-212) derive a co-production
  movement ratio from the benchmark year's before/after Make tables and carry it forward. **This is
  the template for Step 7 redefinitions**, and the board already names it as the pattern for the VA
  before→after transform.
- [`inflation_helpers_cornerstone.py`](bedrock/utils/economic/inflation_helpers_cornerstone.py) —
  bedrock's own commodity-inflation approach, referenced by #497.
- [`Detail_Supply.yaml`](bedrock/transform/detail/Detail_Supply.yaml) /
  [`Detail_Use_SUT.yaml`](bedrock/transform/detail/Detail_Use_SUT.yaml) — existing FBS methods that
  disaggregate a **Summary SUT** to detail using 2017 detail proportions. Their own headers warn *"The
  resulting tables do not reflect valid balanced Supply/Use tables"*, so they are not the deliverable —
  but they are a ready-made **fallback seed / cross-check** for any SUT block we can't source directly,
  and they already encode which codes to exclude from each framework.

**Not in bedrock, to be ported:** RAS ([`sut_ras.py`](https://github.com/cornerstone-data/USEEIO/blob/nowcasting/nowcasting/sut_ras.py)
→ `bedrock/utils/economic/`), balance checks ([`check_balances.py`](https://github.com/cornerstone-data/USEEIO/blob/nowcasting/nowcasting/check_balances.py)
→ `bedrock/utils/validation/`), and commodity mix / intermediate nowcasting
(`CalculateIntermediateUseAndCommodityMix.R`, per #497). Confirmed: no RAS/GRAS code anywhere in
bedrock today.

## Section status

### SUT Use — final demand block (purchaser price)

| Code | Description | Status |
|---|---|---|
| F01000 | Personal consumption expenditures | ✅ `FD_PCE` (+ 5 name-mismatch activity_sets) via `BEA_PCEBridge` |
| F02E00 | Nonres. private fixed investment in equipment | ✅ **done** — `BEA_PEQBridge` built and wired into `NIPA_FD_2017`, reconciles to within 0.22%. Merged 2026-07-24 in [#524](https://github.com/cornerstone-data/bedrock/pull/524), closing [#496](https://github.com/cornerstone-data/bedrock/issues/496) ([#525](https://github.com/cornerstone-data/bedrock/issues/525) too) |
| F02N00 | Nonres. private fixed investment in IP products | ✅ `FD_IP_direct`/`FD_IP_proportional` |
| F02R00 | Residential private fixed investment | ✅ `FD_Structures1` |
| F02S00 | Nonres. private fixed investment in structures | ✅ `FD_Structures2` |
| F06/F07/F10 (12 codes) | Federal/State/Local CE, Equip, IP, Structures | ✅ `FD_Gov_*` — ⚠️ SLG Equipment/Structures/IP attribution bug still open |
| F03000 | Change in private inventories | ❌ [#529](https://github.com/cornerstone-data/bedrock/issues/529)/[#530](https://github.com/cornerstone-data/bedrock/issues/530)/[#531](https://github.com/cornerstone-data/bedrock/issues/531) |
| F04000 | Exports | ❌ [#526](https://github.com/cornerstone-data/bedrock/issues/526)/[#527](https://github.com/cornerstone-data/bedrock/issues/527)/[#528](https://github.com/cornerstone-data/bedrock/issues/528) |
| ~~F05000~~ | ~~Imports~~ | **Not an SUT column** — belongs to Supply (`MCIF`/`MADJ`); appears only on MUT conversion |

### SUT Use — other blocks

| Block | Status |
|---|---|
| Value added (`V00100`, `T00OTOP`, `V00300`, `T00TOP`, `T00SUB`) | ❌ NIPA tables identified, not built — Step 2 |
| Intermediate (commodity × industry) | ❌ Method identified (#497), not built — Step 3 |

### SUT Supply — every column

**This is the least-developed part of the project.** Nothing here is built.

| Column | What it is | Candidate source | Status |
|---|---|---|---|
| cells / `T007` | Domestic output, commodity × industry, basic value | Nowcast gross industry output (`derived_gross_industry_output.py`) × commodity mix (port from `CalculateIntermediateUseAndCommodityMix.R`, #497) | ❌ **unsourced method** |
| `MCIF` | Imports, c.i.f. | #527 trade-data pick (same decision as F04000) | ❌ blocked on #527 |
| `MADJ` | Import adjustment (c.i.f./f.o.b.) | Likely 2017 ratios applied to `MCIF` — BEA-internal construct, no direct annual source | ❌ **unsourced, needs a decision** |
| `MDTY` | Import duties | NIPA T30500 (customs duties line) or trade-source duties | ❌ **unsourced** |
| `TRADE` | Wholesale + retail margins, by commodity | Aggregate of the nowcast Margins dataset (§Step 4c) | ❌ **unsourced** |
| `TRANS` | Transportation margins, by commodity | Aggregate of the nowcast Margins dataset (§Step 4c) | ❌ **unsourced** |
| `TOP` | Taxes on products | NIPA T30500 total + commodity split (2017 shares?) | ❌ total available, **split unsourced** |
| `SUB` | Subsidies (stored negative) | NIPA T31300 total + commodity split | ❌ total available, **split unsourced** |

**`TRADE`/`TRANS` and the Margins deliverable sit at two different levels of detail — build the finer
one and aggregate.** The Margins table is per **commodity-buyer transaction**: its index is
`(Industry Code, Commodity Code)` where the industry side spans both `USA_2017_INDUSTRY_CODES` and
`USA_2017_FINAL_DEMAND_CODES` (i.e. the buyer, intermediate or final), and its columns are
`Producers' Value, Transportation, Wholesale, Retail, Purchasers' Value`
([io_2017.py:333-359](bedrock/extract/iot/io_2017.py#L333-L359)). The Supply table's `TRADE`/`TRANS`
are single values per commodity. They reconcile by aggregation:

```
TRADE[c] = Σ_buyers ( Wholesale[b,c] + Retail[b,c] )
TRANS[c] = Σ_buyers   Transportation[b,c]
```

So the Supply columns are a **commodity-level control on the transaction-level Margins dataset**, not
the same object. Two consequences for sequencing:

- Build the **transaction-level** dataset in Step 4c and derive the two Supply columns from it by the
  identities above. The reverse is impossible — commodity totals can't be disaggregated back to buyers
  without reintroducing the very assumption the Margins table exists to carry.
- Step 6b's PUR→PRO conversion **requires the transaction-level detail**: margin rates differ by
  buyer for the same commodity (a good bought by a household carries retail margin that the same good
  bought as an intermediate input may not), so a commodity-average rate would misallocate across
  columns. Conveniently, the published table carries both `Producers' Value` and `Purchasers' Value`
  per transaction, so 6b's conversion is largely "apply the nowcast table's per-cell PRO/PUR ratio."

## Still unsourced — the open data questions, collected

Ranked by how much they block:

1. **Commodity mix for the Supply table's domestic-output block** — biggest single gap. Without it
   there is no Supply table at all.
2. **Trade data source (#527)** — one decision unblocks four things: `F04000` exports, Supply `MCIF`,
   `MDTY`, and the Step 6 import matrix. Candidates: (a) Census Trade in Goods + BEA services, with
   existing code to port from USEEIO's `download_imports_data.py`; (b) BEA ITA Table 2.1 (goods) +
   Table 3.1 (services) joined via BEA's ITA→NIPA linkage table; (c) BACI, framed in the issue more as
   validation than primary. Requirement: match the 2017 detail Use table closely, annual, comparable to
   BACI.
3. **Commodity split of product taxes and subsidies** (`TOP`, `SUB`, and the Use table's
   `T00TOP`/`T00SUB` rows) — NIPA gives totals (T30500, T31300); allocating to 402 commodities does
   not come for free. Default proposal: 2017 detail Supply shares, held constant, inflated with the
   commodity.
4. **`MADJ`** — no annual published analogue; almost certainly 2017-ratio-based. Small in magnitude,
   but it sits inside the `T013` identity, so it can't just be dropped.
5. **Change in inventories commodity attribution (#530)** — explicitly scoped in the issue to ship
   NIPA-total-only first: *"We need to at least use the Change in Private Inventories NIPA totals as a
   starting place, but further work in attributing those to commodity based on the level of
   fabrication may have to wait."* NIPA T1.1.5 line 14 = Use `F03000` total exactly; the commodity
   split needs ASM stage-of-fabrication + Economic Census materials-consumed data. Deferred.
6. ~~Import matrix allocation method~~ — **settled**: proportional to the commodity's use shares along
   its Use-matrix row (Step 6c). Not a data gap; it needs the Step 6b Use matrix as input, which makes
   6c strictly downstream of 6b.

## Value added — fully specified on the board

| Component | Code | NIPA table |
|---|---|---|
| Compensation of employees | V00100 | T60200D |
| Other taxes on production | T00OTOP | T30500 (excl. taxes on products) |
| Gross operating surplus | V00300 | **Constructed**: T61200D + T61400D + T61500D + T61700D + T61300D + T62200D |
| Taxes on products and imports | T00TOP | T30500 (taxes-on-products portion) |
| Less: Subsidies | T00SUB | T31300 |

Reconciliation targets, also specified: NIPA T1.14 (Gross Value Added by Sector) at top level; each
Section-6 table's total = the corresponding Use-table row/group total; `VABAS` → T10305; `T018` → GDP
via T10105. Allocation to BEA industries "likely needs to use the 2017 table ratios."

---

# Phase 1 build steps

**Naming:** *Phase* is the project-level split — **Phase 1** (this document's main body, 2018-2024)
and **Phase 2** (2025, after the BEA annual update — see the section at the end). *Step* is a stage
of the build pipeline, 0 through 9 below. Phase 2 re-runs the same steps on one more year; it does
not introduce a step 10.

### Step 0 — hygiene ✅ mostly done
- `nowcast.py` scaffolded; PEQ Bridge landed in [#524](https://github.com/cornerstone-data/bedrock/pull/524)
  (merged 2026-07-24), closing #523's last checklist item and #496.
- Remaining: final activity-mapping review; fix the open SLG Equipment/Structures/IP attribution bug.
- **Wire up the 2023-2024 summary SUT workbooks** (§Framework facts) — new mapping constant, GCS
  upload, vintage-pinning branch in `_load_usa_summary_sut`, plus the `USA_SUMMARY_SUT_YEARS` type.
  Small, and Step 5's control totals depend on it.

### Step 1 — SUT Use: final demand block (PUR) — *in progress*
- 1a ✅ PEQ Bridge / F02E00.
- 1b ✅ F0-code assignment (via `assign_sector_consumed_by_from_clean_parameter`, #539).
- 1c ✅ `derive_initial_Y_pur(year)`.
- 1d ❌ **Split the column list by framework.** Add an SUT FD code list (the 20 MUT codes minus
  `F05000`) and have `derive_initial_Y_pur` target *that*; `F05000` gets created in Step 6b from the
  Supply-side imports, not sourced as a Use column. Then land **F04000 exports** per #526/#527/#528.
- 1e ❌ **F03000 inventories** — NIPA total only, per #530's own scoping.
- 1f ❌ Validate per-column against published NIPA aggregates (the PCE reconciliation to ~1.3% is the
  template).

### Step 2 — SUT Use: value added block
- New `NIPA_VA_<year>.yaml` FBS method(s) over the 7 Section-6/3.5/3.13 tables above, reusing the
  `NIPA_FD_<year>.yaml` machinery (`extract_table_info`, `drop_unassigned`, activity_sets).
- Allocate to BEA industries via 2017 table ratios.
- Reconcile against T1.14 / VABAS→T10305 / T018→T10105.
- **Note:** the board's "transform VA into after redefinitions" item is *deferred to Step 7* here —
  VA should stay before-redefinitions through the SUT, and get redefined once, with everything else,
  rather than in its own one-off step. (Deviation from the board item; flagged as open question 3.)

### Step 3 — SUT Use: intermediate block
- **Seed from the actual dollar Use matrix**, not the `A` coefficient matrix. Going `A → U` via
  `U ≈ A @ diag(x)` discards the rounding/negative-clipping baked in when `A` was built.
- Nowcast forward per #497: port `CalculateIntermediateUseAndCommodityMix.R`'s logic but (a) use
  bedrock's commodity inflation, (b) apply to intermediate uses only (not VA), (c) apply to the
  after-redefinitions Use table.
- ⚠️ **Conflict to resolve:** #497 says after-redefinitions, but this plan builds the SUT
  before-redefinitions and redefines in Step 7. Either seed from
  `load_2017_Utot_before_redef_usa()` to stay internally consistent, or accept a mixed-state SUT.
  Recommend the former — see open question 3.
- ⚠️ Valuation: the seed is a **producer-price** MUT Use table; the SUT intermediate block is
  **purchaser**. Either seed from `Use_SUT_Framework_2017_DET` instead (native purchaser, native SUT),
  or convert. **Recommend seeding from the detail SUT file** — it's already loadable via
  `_load_2017_detail_sut_usa` and avoids a conversion round-trip.

### Step 4 — SUT Supply table *(new — the largest unscoped block)*
- 4a. **Domestic output block** — nowcast gross industry output, then split each industry's output
  across commodities using a nowcast commodity mix (port from the #497 R script). Basic value.
- 4b. **Import columns** — `MCIF`, `MADJ`, `MDTY` from the #527 source decision, with `MADJ` most
  likely a 2017-ratio construct.
- 4c. **Margins dataset, then the margin columns.** Build the **transaction-level** Margins dataset
  here — `(buyer, commodity) × {Producers' Value, Transportation, Wholesale, Retail, Purchasers'
  Value}`, BEA detail granularity, buyers spanning industries *and* final-demand codes — since it is
  both the Step 6d deliverable and Step 6b's conversion input. Start from 2017 detail margin **rates**
  per (buyer, commodity, margin type), carried forward on trade/transport output and commodity
  inflation; `derive_PRO_to_PUR_ratio.py`'s `_inflate_margins_to_year` is a working precedent but is
  Cornerstone-schema and phi-oriented (it aggregates to per-commodity totals via `_margins_by_commodity`
  before computing phi), so expect adaptation rather than reuse — the nowcast needs the pre-aggregation
  detail preserved.
  Then derive the Supply columns by aggregation: `TRADE[c] = Σ_b (Wholesale + Retail)`,
  `TRANS[c] = Σ_b Transportation`.
  **Validate per commodity, never in aggregate** (`T014` nets to ~1 economy-wide) — and separately
  check that the transaction-level table reproduces the two Supply columns commodity by commodity,
  since that identity is the only thing tying the fine and coarse objects together.
- 4d. **Tax/subsidy columns** (`TOP`, `SUB`) — NIPA T30500/T31300 totals split by 2017 commodity
  shares. Remember `SUB` is negative here and positive in the Use table.
- 4e. **Verify the four Supply identities per commodity** (`T013`/`T014`/`T015`/`T016` above), 402/402,
  before declaring the Supply table done.

### Step 5 — Balance the SUT (RAS)
- Port `sut_ras.py` into `bedrock/utils/economic/`, adapting off the rpy/R dependency.
- Balance on the SUT identity: **total supply at purchaser (`T016`) = total use (`T019`) per
  commodity**, plus industry-output consistency between Supply and Use.
- Control totals: summary SUT totals by default (available for all of 2017-2024 once the 2023-2024
  workbooks are wired up in Step 0), with industry/commodity gross output as the named alternative.
- 2025's controls come from the BEA annual update, in Phase 2 — see that section.
- Port `check_balances.py` into `bedrock/utils/validation/` alongside.

### Step 6 — SUT → MUT conversion *(new — produces the actual deliverables)*
Still in BEA_2017_Detail schema, still before redefinitions. Four outputs:

- 6a. **Make table** (industry × commodity, producer price) — transpose the Supply table's
  domestic-output block (drop `MCIF`/`MADJ`: Make is domestic only), then revalue basic → producer by
  allocating `T015` (`MDTY`+`TOP`+`SUB`) across each commodity's producing industries. Allocation basis
  needs a decision — proportional to each industry's domestic production of that commodity is the
  obvious default. Validate against `load_2017_V_before_redef_usa()` by running the conversion on the
  **2017 SUT** and checking it reproduces the published 2017 before-redef Make.
- 6b. **Use table, producer price** — for each purchaser-valued cell, replace it with the 4c Margins
  table's `Producers' Value` for that same `(buyer, commodity)` pair, and add the stripped
  Transportation/Wholesale/Retail amounts to the margin-supplying commodity rows *within the same
  buyer's column*. The per-cell join is the whole reason 4c is built at transaction level — a
  commodity-average margin rate would misallocate across buyers. Also **create the `F05000` imports
  column here** from the Supply-side `MCIF`/`MADJ`, and **collapse the VA rows** from the SUT's
  `T00OTOP`/`T00TOP`/`T00SUB` split into MUT's single net `V00200`.
  ⚠️ USEEIO issue #4 flags that the external `nipa_final_demand_estimates.py` does this conversion with
  PCEBridge's 5 value-chain columns — which only covers PCE, not all of final demand — and leaves
  "splitting a margin category across individual commodity sectors" unresolved. A transaction-level
  Margins dataset (4c) is exactly the fix, since the buyer dimension is what PCEBridge was standing in
  for. Check `derive_PRO_to_PUR_ratio.py` before implementing — its margin filters and
  negative-margin treatment (`_apply_margins_filter`, `_margin_negatives_treatment`) encode decisions
  already made about this table that should carry over.
- 6c. **Import matrix** (commodity × industry + FD columns) — **allocate each commodity's imports
  across its using columns in proportion to that commodity's own use shares**, taken from the Step 6b
  Use matrix row:

  ```
  Uimp[c, j] = imports[c] × Use[c, j] / Σ_j Use[c, j]
  ```

  i.e. work along the row, converting each column's share of total use of commodity `c` into its share
  of the imports of `c`. This is the standard proportionality assumption — every buyer of a commodity
  is assumed to draw imported and domestic supply in the same mix.

  Three implementation details that decide whether it's right:
  - **The row must be the producer-price Use table from 6b**, not the purchaser-price SUT Use, or the
    shares carry margin distortion that varies by column.
  - **Exclude `F05000` from the denominator.** That column is the imports total itself (and carries a
    negative sign in the MUT convention), so including it double-counts and skews every share.
  - **The column scope is not "all columns" — BEA leaves seven FD columns identically zero.**
    Verified against the published import matrices (2017 detail and 2022 summary, raw sheets): the
    populated FD columns are `F01000`, `F02E00`, `F02N00`, `F02R00`, `F02S00`, `F03000`, `F06E00`,
    `F06N00`, `F07E00`, `F07N00`, `F10E00`, `F10N00`. Exactly zero, in both vintages: **`F04000`
    exports**, all three government *consumption* columns (`F06C00`/`F07C00`/`F10C00`) and all three
    government *structures* columns (`F06S00`/`F07S00`/`F10S00`). So a naive proportional spread
    across every use column would put imports where BEA puts none. Restrict the allocation to the
    intermediate columns plus the twelve FD columns above. (The government zeros are most likely
    because government is carried as an *industry* in these accounts, so its purchased imports land in
    the industry columns rather than the FD ones — worth confirming, but the zeros themselves are
    measured, not inferred.)

  Validation, not allocation basis: BEA's published **summary** import matrix is available through
  2024 (`load_summary_Uimp_usa`), so the nowcast detail matrix can be aggregated to summary and
  compared. Divergence there is a signal the proportionality assumption is straining for particular
  commodities — the known weakness of this method is commodities where import and domestic use
  genuinely differ by buyer.
- 6d. **Margins dataset** — mostly a reshape/publish of 4c's output into BEA's shape: index
  `(Industry Code, Commodity Code)`, columns `Producers' Value, Transportation, Wholesale, Retail,
  Purchasers' Value`, matching `load_2017_margins_*_usa` so downstream consumers see a drop-in. The
  substantive work happened in 4c; what's left here is conforming the index/column labels and units
  (BEA ships million USD; the loaders scale by `MILLION_CURRENCY_TO_CURRENCY`).
- **Whole-phase validation:** run 6a–6d against the **2017 SUT** and diff against the four published
  2017 before-redef MUT tables. If the conversion can't reproduce the benchmark year it will not be
  right for 2018–2025.

### Step 7 — Redefinitions: before → after *(new)*
- Derive per-cell redefinition ratios from the 2017 benchmark's before/after MUT pairs — the same idea
  as `compute_coproduction_ratios`/`adjust_gross_output` in `derived_gross_industry_output.py`, applied
  to Make, Use, Import matrix, and Margins.
- Apply in **BEA detail space, before the Cornerstone schema collapse** — the ratios are defined on
  BEA detail codes and don't survive aggregation cleanly. (This reorders the previous plan, which had
  schema conversion first.)
- ⚠️ **Totals cannot validate this step.** Redefinition preserves every total by construction. Validate
  cell-by-cell against the 2017 before/after pair: ~5,740 of 161,604 intermediate cells should move,
  ~553,635 million gross, largest single cell ~42,893, net ~−7.
- This subsumes the board's separate "transform VA FBS into after redefinitions" item.

### Step 8 — Cornerstone schema conversion
- `industry_corresp()` / `commodity_corresp()` from `cornerstone_expansion.py`, honoring
  `cfg.iot_before_or_after_redefinition` (which by this point should be `after`).

### Step 9 — Storage and pipeline integration
- Store Make/Use/Import/Margins per year via bedrock's normal GCS snapshot path.
- Wire the nowcasted products into the model-build pipeline; regenerate snapshots and diagnostics.

## Testing strategy

- **Benchmark-year replay is the backbone.** Phases 6 and 7 both have a published 2017 answer. Run the
  conversion and the redefinition on 2017 and diff against the published tables before trusting any
  nowcast year. This is the single highest-value test in the project.
- **Per-commodity, never aggregate**, for anything touching margins or redefinitions — both net out to
  ~nothing economy-wide, so aggregate checks pass on broken data.
- **Identity checks as unit tests**: the four Supply identities, `VABAS`/`VAPRO`/`T018`, and
  `T016 == T019` per commodity.
- **Reconciliation against published NIPA aggregates** per section (T1.14, T10305, T10105, T1.1.5
  line 14, Section-6 totals) — the board already specifies most targets, so these are numeric, not
  eyeball, tests.
- **Unit tests** for the ported RAS (small hand-checkable matrices — zero control totals are the
  classic silent failure), the SUT/MUT FD code lists, and the margin reassignment in 6b.
- **Golden-file per year** once Step 1 stabilizes, so later phases don't silently drift the FD block.

## Open questions

1. **#527 trade-data pick** — Census goods + BEA services (port `download_imports_data.py`), BEA ITA
   with NIPA linkage, or BACI? Blocks F04000, `MCIF`, `MDTY`, and informs 6c.
2. **Commodity mix method for Step 4a** — port the R script's approach wholesale, or derive from
   2017 detail Supply shares moved by industry output? Nothing is built here yet.
3. **Reconcile #497's after-redefinitions instruction.** Now that the SUT is confirmed
   before-redefinitions, "build the SUT before-redef, redefine once in Step 7" is the only internally
   consistent ordering, and this plan follows it. #497 says to nowcast the intermediate block on the
   *after*-redefinitions Use table, which would mix states. Worth understanding **why** #497 says that
   before overriding it — it may be carrying a constraint from the USEEIO R implementation that
   doesn't apply here, or it may encode something about the inflation approach that does.
4. **RAS control totals** — summary SUT totals for all years (the default, once 2023-2024 are loaded),
   or industry/commodity gross output? The board lists the latter as an explicit option. No longer
   forced either way by Phase 2: summary SUT will exist for 2025 too, so this is a methodological
   preference rather than a data constraint.
5. **`nowcast.py` vs. `bedrock/transform/iot/` boundary** — is `nowcast.py` the per-year orchestrator
   calling into `transform/iot/`'s existing functions, or should the new SUT/MUT code live in
   `transform/iot/` (where #495 pointed `nipa_final_demand_estimates.py`)? Steps 4-7 are a lot of new
   code; worth settling before writing it.
6. **Interim caching layout** while developing (final destination is GCS, per the board).
7. **`MADJ` treatment** — 2017 ratio, or drop it and absorb into `MCIF`? Affects whether `T013`
   reconciles exactly.

---

# Phase 2 — 2025, after the BEA annual update

**Trigger:** the BEA annual update, expected by **Sept 30, 2026**, from which we expect **detailed
gross output by sector** and enough to **derive commodity output**. **Target: complete in early
October 2026.**

Phase 2 is two jobs, not one:

1. **Add 2025**, using the newly-published detailed gross output.
2. **Refresh 2018-2024** with the revised NIPA and industry-account data. BEA revises history in every
   annual update, so the Phase 1 years get rebuilt on the new vintage rather than left on the vintage
   they were first built with.

**The goal is that both are a data-and-config change, not a code change.** Everything in Steps 0-9
should already work for an arbitrary year and an arbitrary vintage by the time Phase 1 closes. That is
a design constraint on Phase 1, not just an aspiration for Phase 2 — see "What Phase 1 must not do".

## What the annual update unblocks for 2025

Detailed gross output by sector is the missing input to the least-developed part of the project:

- **Step 4a (Supply domestic-output block)** — gross industry output × commodity mix is exactly the
  construction Step 4a specifies. For 2018-2024 this is nowcast; for 2025 it comes straight from the
  update, which makes 2025's Supply table *better* founded than the interpolated years, not worse.
- **Step 5 (RAS controls)** — the update carries a **2025 summary SUT/MUT** (confirmed), so 2025 is
  controlled the same way every other year is; no special-casing, and no forced switch to the
  gross-output path. The newly-available commodity and industry output make that alternative *usable*
  for 2025, but it stays optional.
- **Steps 2 and 3** — a real 2025 industry-output anchor constrains the VA and intermediate blocks
  instead of leaving them purely inflation-carried.

## Refreshing 2018-2024 on the revised vintage

This is the part that needs preparation *during* Phase 1, because it cuts against how bedrock's
loaders are deliberately built today.

`_load_usa_summary_mut` pins older years to the oldest file containing them
([io_2017.py:730-739](bedrock/extract/iot/io_2017.py#L730-L739)) — *"BEA revises historical data in
each new release. We pin older years to the oldest file containing them so values stay stable across
releases."* That convention exists to protect consumers like `scale_cornerstone_B`, and it is the
right default for them. **A deliberate historical refresh is the opposite behaviour**, so Phase 2 needs
a way to say "use the newest vintage for every year" without silently moving the ground under the
model-build consumers that depend on stability.

Options to settle in Phase 1: a vintage selector threaded through the SUT/MUT loaders (nowcast asks
for latest, existing consumers keep pinned), or a separate set of nowcast-specific loaders. Either
way, **decide before October** — discovering the pinning behaviour mid-refresh is how a week
disappears.

Also worth scoping ahead of time:
- **Which sources actually revise.** NIPA tables behind the FD and VA blocks, GDP-by-industry gross
  output, and the summary SUT/MUT workbooks all revise; the 2017 detail benchmark does not. So the
  benchmark-derived pieces (redefinition ratios, 2017 margin rates, commodity shares) are stable
  across the refresh, and only the nowcast-year inputs move.
- **A before/after diff** of the refreshed 2018-2024 against the Phase 1 outputs, as the deliverable
  that shows what the revision did — this is a useful analysis product in its own right, not just a
  regression check.
- **Snapshot/version handling** — refreshed years replace or sit alongside the Phase 1 GCS artifacts,
  and any published v0.5 numbers derived from them need a stated position.

## Phase 2 work items

1. **Check what actually shipped** (day 1). The 2025 summary SUT/MUT is expected; what still needs
   confirming on the day is the **granularity of the detailed gross output** and the **span of the
   revisions**, both of which size the rest of the phase.
2. **Ingest the new vintage** — new workbooks → new mapping constants → GCS upload → extend the
   vintage branch, the same mechanic as the 2023-2024 summary SUT work in Step 0, plus the
   latest-vintage selector described above.
3. **Extend the year literals.** At minimum `USA_SUMMARY_MUT_YEARS` / `USA_GROSS_INDUSTRY_OUTPUT_YEARS`
   ([matrix_mappings.py:63-83](bedrock/utils/taxonomy/bea/matrix_mappings.py#L63-L83)), plus
   `usa_io_data_year` and `model_base_year` in
   [usa_config.py:49-64](bedrock/utils/config/usa_config.py#L49-L64) — all currently stop at 2024.
   Grep for `2024` rather than assuming that list is complete.
4. **Add `NIPA_FD_2025.yaml`** (the series currently runs 2017-2024), plus the 2025 counterparts of
   whatever VA and trade/inventory methods Steps 1d/1e/2 produce.
5. **Re-run Steps 1-9 for 2018-2025 as one series** on the refreshed vintage — not 2025 bolted onto
   frozen predecessors. Same per-commodity identity checks throughout.
6. **Produce the revision diff** for 2018-2024, old vintage vs. new.
7. **Publish**: 2018-2025 Make/Use/Import/Margins to GCS, refresh diagnostics, extend the model build.

## What Phase 1 must not do

For Phase 2 to be a short exercise rather than a rebuild. Worth checking against these at Phase 1
close:

- **No hardcoded terminal year.** Any `2024` meaning "the last year we have" rather than "the year
  2024" is a Phase 2 bug waiting to happen.
- **No hardcoded vintage.** Every loader the nowcast path touches should take the vintage as an input,
  since Phase 2 re-runs every year on a newer one.
- **Extend the summary SUT vintage handling to 2025** when the update lands — same mechanic as the
  2023-2024 work in Step 0, now a routine annual chore rather than a one-off.
- **Keep the 2017 benchmark-replay tests year-parameterised**, so the Step 6/7 conversion validation
  reruns unchanged after the refresh.
- **Per-year yamls are fine; per-year branches in Python are not.** The yaml-per-year pattern is
  established and cheap to extend; year logic embedded in code is what makes a refresh expensive.
