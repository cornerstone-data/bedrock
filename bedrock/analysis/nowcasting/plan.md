# Nowcast US IOT — plan, Phases 1 and 2

Nowcasted national Supply/Use tables → Make/Use/Import/Margins deliverables, 2018-2025.

GitHub project: [cornerstone-data/projects/26 — "Nowcast US IOT Phase 1"](https://github.com/orgs/cornerstone-data/projects/26)
(33 items, milestone `v0.5`). Its description: *"Draw on and further improve code from flowsa, USEEIO
and useeior repositories that implement a nowcasting approach to estimate US **2018-2025** Make, Use
and Import Matrices."* Code lands on the long-lived `nowcast` integration branch, not `main`, until a phase is complete.

**Branching policy.** Work for this project **branches off `nowcast`** and **merges back into
`nowcast`** when ready. `nowcast` is **kept up to date with `main` by merge, not rebase** — a rebase
would rewrite history that feature branches are based on — and it is not merged back into `main` until
the project is complete. So `nowcast` is effectively `main` for everything in this plan: base branches
on it, target PRs at it, and refresh it from `main` rather than rebasing individual feature branches
onto `main`.

This applies to the two stale branches as well. `nipa_fd_allocation_fix` (14 behind main) gets rebased
onto `nowcast` after #569 passes, and the four commits worth salvaging from it land on `nowcast` — not
on `main`, which would recreate the duplicate-SHA situation `fix_non_naics_sector_levels` is already
in.

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

These are established in [`About_BEA_IOT_table_valuation_differences.md`](compare_NIPA_to_IOT/About_BEA_IOT_table_valuation_differences.md)
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
([matrix_mappings.py:53-60](../../utils/taxonomy/bea/matrix_mappings.py#L53-L60)) is the only SUT
mapping, and [`_load_usa_summary_sut`](../../extract/iot/io_2017.py#L766-L795) hardcodes it, so
asking for 2023 or 2024 will fail on a missing sheet rather than fall back.

The fix mirrors what summary MUT already does: add a `USA_SUMMARY_SUT_MAPPING_2017_2024` constant for
the newer workbooks, upload them to `GCS_USA_SUP_DIR`, and give `_load_usa_summary_sut` the same
vintage-pinning branch `_load_usa_summary_mut` uses at
[io_2017.py:730-739](../../extract/iot/io_2017.py#L730-L739) — older years stay pinned to the oldest
file containing them, so BEA's historical revisions don't silently move values under existing
consumers. Worth doing in Step 0: **summary SUT totals are the natural RAS control for every nowcast
year**, and two of the seven years can't be loaded today.

Two loose ends to tidy while in there: `_load_usa_summary_sut`'s `year` parameter is typed
`USA_SUMMARY_MUT_YEARS` (1997 onward), which admits years no SUT workbook contains — it wants its own
`USA_SUMMARY_SUT_YEARS` literal. And [`BEA.py:106,117`](../../extract/bea/BEA.py#L106-L117) casts to
`USA_SUMMARY_MUT_YEARS` to satisfy that signature; the cast can go once the type is right.

**The published SUT is before redefinitions** (confirmed). This is why the detail SUT files
(`Supply_2017_DET.xlsx`, `Use_SUT_Framework_2017_DET.xlsx`) carry no before/after qualifier where MUT
ships both variants ([matrix_mappings.py:9-30](../../utils/taxonomy/bea/matrix_mappings.py#L9-L30)) —
there is only one SUT, and redefinition is a MUT-framework step applied after conversion.

Two consequences run through the whole plan:
- **The build order is forced, not chosen**, and is now settled as the project's ordering:
  **SUT → MUT before redefinitions → MUT after redefinitions → MUT in Cornerstone schema** = Steps
  6 → 7 → 8.
- **Everything upstream of Step 7 stays before-redefinitions**, including the Step 3 intermediate seed.
  Seeding from `Use_SUT_Framework_2017_DET` gives a before-redef, purchaser-valued seed in one shot —
  the right basis on both axes. #497's instruction to work on the after-redefinitions Use table is
  therefore **overridden**, not designed around (was open question 3).

## Grounding: what already exists

**Module.** [`bedrock/transform/eeio/nowcast.py`](../../transform/eeio/nowcast.py) exists and
currently implements exactly one thing: `derive_initial_Y_pur(year)` — the final-demand block of the
SUT Use table, purchaser price, BEA_2017_Detail schema, from the `NIPA_FD_<year>` FBS methods. It
handles the Cornerstone-schema collapse problem via `map_fbs_sectors_to_model_schema` applied to both
sector columns. A baseline validator exists at
[`bedrock/analysis/nowcasting/initial_Y_pur_baseline.py`](../../analysis/nowcasting/initial_Y_pur_baseline.py).

**2017 benchmark loaders — all four end-product table types already load**, before *and* after
redefinitions, from [`io_2017.py`](../../extract/iot/io_2017.py):

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
- [`derive_PRO_to_PUR_ratio.py`](../../transform/iot/derive_PRO_to_PUR_ratio.py) — margins-based
  PRO↔PUR ratios (`phi`), with margin filters, negative-margin treatment, and
  `derive_margins_cornerstone_usa_at_year(year)` / `_inflate_margins_to_year` already inflating margin
  components to a target year. Directly relevant to both the Supply-table margin columns and the
  PUR→PRO conversion.
- [`derived_gross_industry_output.py`](../../transform/iot/derived_gross_industry_output.py) —
  `compute_coproduction_ratios` / `adjust_gross_output` (lines ~150-212) derive a co-production
  movement ratio from the benchmark year's before/after Make tables and carry it forward. **This is
  the template for Step 7 redefinitions**, and the board already names it as the pattern for the VA
  before→after transform.
- [`inflation_helpers_cornerstone.py`](../../utils/economic/inflation_helpers_cornerstone.py) —
  bedrock's own commodity-inflation approach, referenced by #497.
- [`Detail_Supply.yaml`](../../transform/detail/Detail_Supply.yaml) /
  [`Detail_Use_SUT.yaml`](../../transform/detail/Detail_Use_SUT.yaml) — existing FBS methods that
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
| F04000 | Exports | ⏳ **source settled** (§Trade data below, [#557](https://github.com/cornerstone-data/bedrock/pull/557)) — Census goods + BEA services, ITA-controlled; not built. Implementation is [#528](https://github.com/cornerstone-data/bedrock/issues/528) |
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
| `MCIF` | Imports, c.i.f. | Census goods **CIF** (`GEN_CIF_YR`) + BEA `IntlServTrade`, ITA-controlled — §Trade data below | ⏳ **source settled** ([#557](https://github.com/cornerstone-data/bedrock/pull/557)), not built |
| `MADJ` | Import adjustment (c.i.f./f.o.b.) | Likely 2017 ratios applied to `MCIF` — BEA-internal construct, no direct annual source | ❌ **unsourced, needs a decision** |
| `MDTY` | Import duties | Effective duty rate from Census `CAL_DUT_YR ÷` customs value (NAICS-6, same endpoint as `MCIF`) × NIPA T30500 customs-duties level — §`MDTY` below | ⏳ **sourced**, not built. ⚠️ calculated ≠ collected duty, and the gap widens across 2018-2025 |
| `TRADE` | Wholesale + retail margins, by commodity | Aggregate of the nowcast Margins dataset (§Step 4c) | ❌ **unsourced** |
| `TRANS` | Transportation margins, by commodity | Aggregate of the nowcast Margins dataset (§Step 4c) | ❌ **unsourced** |
| `TOP` | Taxes on products | NIPA T30500 total + commodity split (2017 shares?) | ❌ total available, **split unsourced** |
| `SUB` | Subsidies (stored negative) | NIPA T31300 total + commodity split | ❌ total available, **split unsourced** |

**`TRADE`/`TRANS` and the Margins deliverable sit at two different levels of detail — build the finer
one and aggregate.** The Margins table is per **commodity-buyer transaction**: its index is
`(Industry Code, Commodity Code)` where the industry side spans both `USA_2017_INDUSTRY_CODES` and
`USA_2017_FINAL_DEMAND_CODES` (i.e. the buyer, intermediate or final), and its columns are
`Producers' Value, Transportation, Wholesale, Retail, Purchasers' Value`
([io_2017.py:333-359](../../extract/iot/io_2017.py#L333-L359)). The Supply table's `TRADE`/`TRANS`
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
2. ~~**Trade data source (#527)**~~ — **settled** in
   [#557](https://github.com/cornerstone-data/bedrock/pull/557): Census goods + BEA services as the
   primary extract, BEA ITA as the national totals control, BACI out of scope. See §Trade data below.
   What remains is not a source question but three reconciliation questions the analysis leaves open —
   import valuation overlap, the thin service→Detail concordance, and specials/margins policy. `MDTY`
   turns out to ride on the same Census request (`CAL_DUT_YR`), so it is no longer a separate gap
   either — see §`MDTY`.
3. **Commodity split of product taxes and subsidies** (`TOP`, `SUB`, and the Use table's
   `T00TOP`/`T00SUB` rows) — NIPA gives totals (T30500, T31300); allocating to 402 commodities does
   not come for free. Default proposal: 2017 detail Supply shares, held constant, inflated with the
   commodity.
4. **`MADJ`** — no annual published analogue; likely 2017-ratio-based, though Census `GEN_CHA_YR`
   (import charges) measures the same c.i.f./f.o.b. wedge and comes free with the `MCIF` request, so
   try it before defaulting to a fixed ratio. Small in magnitude, but it sits inside the `T013`
   identity, so it can't just be dropped.
5. **Change in inventories commodity attribution (#530)** — explicitly scoped in the issue to ship
   NIPA-total-only first: *"We need to at least use the Change in Private Inventories NIPA totals as a
   starting place, but further work in attributing those to commodity based on the level of
   fabrication may have to wait."* NIPA T1.1.5 line 14 = Use `F03000` total exactly; the commodity
   split needs ASM stage-of-fabrication + Economic Census materials-consumed data. Deferred.
6. ~~Import matrix allocation method~~ — **settled**: proportional to the commodity's use shares along
   its Use-matrix row (Step 6c). Not a data gap; it needs the Step 6b Use matrix as input, which makes
   6c strictly downstream of 6b.

## Trade data — source settled, structure still to earn

Settled in [#557](https://github.com/cornerstone-data/bedrock/pull/557) (analysis lands in
`bedrock/analysis/nowcasting/trade_data/`, closing [#527](https://github.com/cornerstone-data/bedrock/issues/527);
implementation is [#528](https://github.com/cornerstone-data/bedrock/issues/528)). This one decision
feeds `F04000`, Supply `MCIF`, and Step 6c's import matrix.

| Role | Source |
|---|---|
| Goods extract | Census International Trade, NAICS-6 — imports **CIF** (`GEN_CIF_YR`, to match Supply `MCIF`), exports FAS-family (`ALL_VAL_YR`) |
| Services extract | BEA `IntlServTrade`, by type of service, imports and exports |
| National totals control | BEA ITA Tables 2.1 (goods) / 3.1 (services) — scale or residual-constrain; a raw Census+BEA sum is **not** the final total |
| Sector bridge | USEEIO Census→BEA Detail and service-type→Detail concordances; bedrock `NAICS_to_BEA_Crosswalk_2017` as goods backup |
| 2017 structure/specials benchmark | Use `F04000`/`F05000` and Supply `MCIF` |

### `MDTY` — rate from Census, level from NIPA

The duties column was listed above as unsourced, but it does not need a separate source decision: the
**same Census imports endpoint already chosen for `MCIF` carries duty**. From
[`/data/timeseries/intltrade/imports/naics`](https://api.census.gov/data/timeseries/intltrade/imports/naics/variables.html),
alongside `GEN_CIF_YR`:

| Variable | Meaning |
|---|---|
| `CAL_DUT_YR` | Year-to-date imports for consumption, **calculated duty** |
| `DUT_VAL_YR` | Year-to-date imports for consumption, **dutiable value** |
| `CON_VAL_YR` / `GEN_VAL_YR` | **Customs value** — the denominator for an effective rate |
| `GEN_CHA_YR` / `CON_CHA_YR` | Import **charges** (freight/insurance) — see below |

The underlying Imports-for-Consumption / General Imports files carry all of this at **HTS line ×
country of origin**, so the NAICS endpoint is an aggregation of something finer if we ever need it.
[USITC DataWeb](https://dataweb.usitc.gov/) republishes the same fields and is the easier front end for
pulling duty and customs value together by HTS line and year programmatically — worth preferring if the
Census API proves awkward.

**Recipe: use Census for the *rate*, not the level.** Compute an effective duty rate per sector —
`CAL_DUT_YR ÷ customs value` — map it to BEA Detail with the concordance already built for `MCIF`, and
apply it to the ITA-controlled import vector. Take the **national level** from the customs-duties line
of NIPA T30500, the table Step 2 and §4d already hit for `T00OTOP`/`TOP`. Effective-rate-by-sector is
the standard construction for exactly this purpose, and it keeps `MDTY` on the same national-accounts
basis as the rest of the Supply table's `T015` block.

**Why not use Census duty levels directly — `CAL_DUT_YR` is an estimate, and it errs in both
directions.** It is the statutory rate applied to dutiable value at entry, so it:
- is **overstated** where U.S. goods returned after processing/assembly abroad have a duty-free portion
  of value;
- is **understated** where articles dutiable at various or special rates show a dutiable value but no
  calculated duty;
- captures **no drawbacks, refunds, or exclusions**, and is not what CBP actually assessed or
  collected.

⚠️ **This is a live problem for our year range specifically, not a footnote.** The gap between
calculated and collected duties has **widened notably** under the recent tariff actions — exclusions
and in-transit exemptions — and our nowcast span is 2018-2025, i.e. precisely the tariff era. A
calculated-duty *level* would drift from the national accounts over exactly the years we are
estimating, which is the argument for anchoring to NIPA every year rather than trusting the Census
total. If the two need reconciling, **actual duty revenue comes from CBP/Treasury collections, not
Census** — that is the series to reach for, and a calculated-vs-collected spread by year is a cheap
diagnostic worth producing while building this.

**Services carry no duty**, so unlike `MCIF` this column is goods-only — the thin service→Detail
concordance that wrecks export structure is irrelevant here. Net of the tariff-era caveat, `MDTY`
should be the most tractable of the three import columns.

Bonus: `GEN_CHA_YR` (import charges) is a direct measurement of the freight/insurance wedge that makes
CIF overshoot when added to full BEA services — so it feeds the valuation-overlap question in problem 1
above, and plausibly `MADJ` (open question 7) as well, since c.i.f./f.o.b. adjustment is the same wedge.
Worth pulling in the same request even if only used diagnostically.

**Code home:** [flowsa `imports`](https://github.com/cornerstone-data/flowsa/tree/imports) —
`Census_USATrade` and `BEA_IEA` FBAs, which cover imports **and** exports. USEEIO's
`download_imports_data.py` is imports-only legacy and is *not* the production extract; USEEIO stays
useful for concordances. Neither FBA is in bedrock or on flowsa `master` today, so Step 1d/4b start
with a vendor/merge.

**What the 2017 tests say** (million USD, national): combined Census+BEA imports 2,940,917 vs Use
`|F05000|` 2,626,305 and `MCIF` 2,649,430 (**+12%**); combined exports 2,383,547 vs Use `F04000`
2,082,970 (**+14%**). Right ballpark, systematically high — hence the ITA control step. On commodity
structure after Detail mapping, imports come out usable (Pearson 0.60, 0.81 excluding `S00xxx`) and
**exports do not** (0.34, only 0.53 excluding `S00xxx`).

**Three carried-forward problems, all on the reconciliation side, none of them a source problem:**

1. **Import valuation overlap.** Census CIF includes freight/insurance that BOP records inside
   services, so CIF goods + full services double-counts. Decide customs (`GEN_VAL_YR`) vs CIF *before*
   the ITA control, and keep CIF when the target is Supply `MCIF` unless the control step sets levels.
2. **The service→Detail concordance is thin** — 10 API types, ~70% of `AllTypesOfService`, missing
   travel and charges for IP use. This is why exports are weak, not the goods NAICS map (which covers
   97% of import and 92% of export value): **27.5% of Use `F04000` value has zero extract**, vs 10% on
   imports. Remaining holes after `S00900` (below): `533000` IP-royalties-like (~73B), wholesale
   (`423*`, `424A00`) and truck transport (`484000`).

   **`S00900` is off that list — it needs no extract at all.** At ~204B it was the biggest single hole,
   9.8% of the `F04000` column, so removing it takes the zero-extract share from 27.5% to **~17.7%**.
   It has *zero intermediate use* across all 402 industries, and only two final-demand entries, which
   are one offsetting reclassification of nonresident expenditure:

   ```
   SUPPLY  produced by S00600 (federal nondefense)     3,468
           imports MCIF                                  -26
           total supply T013 / T016                    3,441
   USE     intermediate, all 402 industries                0
           PCE      F01000                           -200,997
           exports  F04000                            204,439
           total use T019                              3,441
   ```

   The PCE side is **already built**: −200,997 is exactly `DEXFRC` *Less: Expenditures in the United
   States by nonresidents* (−199,435) plus U20405 line 149 personal remittances in kind (−1,562) — the
   two lines `FD_PCE_less_nonresident` already handles with `negate_flows` (`7a04a71`). The export side
   is then forced by the identity: `F04000 = −F01000 + total supply = 200,997 + 3,441 = 204,438`
   against a published 204,439, one apart on rounding. So `S00900` also does not depend on the thin
   service concordance — no travel or charges-for-IP mapping has to land before this cell is right.
   Filed on #528.
3. **Specials and margins policy.** `S00300` (noncomparable imports, ~260B) and the margin-heavy export
   cells need an explicit rule — held from Use, taken as a residual after mapped commodities, or out of
   the extract's scope — rather than silent zeros. **`S00900`'s rule is now settled: derive it from the
   supply identity** (item 2). `S00300` is a different case and is unaffected — it is 0 in `F04000`, so
   its ~260B is import and intermediate presence, not an exports hole. The `compare()` run makes the
   sequencing point: matched cells overshoot (+12% imports, +30% exports) while specials soak the
   difference, so **totals can look fine while structure is wrong — fix specials and the service map
   before applying the ITA scale**, not after.

Also still open: **exports are FAS/BOP, the Use column is PUR** — a margins bridge that doesn't exist
yet. Provisionally accept FAS and flag it.

**Acceptance bar before this is trusted for nowcast years** (from #557, not yet met): national totals
within ~2-3% of Use/`MCIF`; import commodity vector Pearson ≳ 0.85 and export ≳ 0.75-0.85 on
non-special codes; top-20 Jaccard ≳ 0.7 / ≳ 0.6; every known hole covered by a written rule. If the raw
extract can't hit those bars, a documented pipeline that does (extract structure × scale to Use totals,
or extract + Use residual on specials only) is an acceptable substitute — and *that* pipeline is what
the nowcast years carry forward.

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
  Supply-side imports, not sourced as a Use column. Then land **F04000 exports** per the settled
  §Trade data recipe (#528): vendor `Census_USATrade` + `BEA_IEA` from flowsa `imports`, map to BEA
  Detail, apply the specials/margins rule, control to ITA — and prove 2017 against the acceptance bars
  before running any nowcast year. Same vendor step serves Step 4b, so do it once.
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
  rather than in its own one-off step. (Deviation from the board item, now settled — the whole SUT is
  before-redefinitions and Step 7 is the single redefinition point.)

### Step 3 — SUT Use: intermediate block
- **Seed from the actual dollar Use matrix**, not the `A` coefficient matrix. Going `A → U` via
  `U ≈ A @ diag(x)` discards the rounding/negative-clipping baked in when `A` was built.
- Nowcast forward per #497: port `CalculateIntermediateUseAndCommodityMix.R`'s logic but (a) use
  bedrock's commodity inflation, (b) apply to intermediate uses only (not VA), and (c) — departing from
  #497 — apply to the **before**-redefinitions table, per the settled ordering below.
- ✅ **Redefinition state — settled** (was open question 3): seed **before redefinitions**, consistent
  with the whole SUT; the single redefinition happens in Step 7. #497's after-redefinitions instruction
  is overridden.
- ✅ **Valuation — settled**: the MUT Use seed is **producer**-price where the SUT intermediate block
  is **purchaser**. **Seed from `Use_SUT_Framework_2017_DET`** — native SUT, native purchaser, native
  before-redef, all three right in one object, and already loadable via `_load_2017_detail_sut_usa`.
  No conversion round-trip, and no `load_2017_Utot_before_redef_usa()` in this step.
- ⚠️ **Annual survey expense data — probed; works for agriculture and government, not for the rest.**
  The method above freezes every industry's *input structure* at its 2017 shape. Annual surveys collect
  inputs to production at **purchaser prices** (the SUT Use basis exactly), so they looked like the way
  to put real annual movement into it. Probed in [#564](https://github.com/cornerstone-data/bedrock/issues/564);
  full results in
  [`annual_survey_expense_sources.md`](annual_survey_expense_sources.md).

  | Sector | Verdict | What to use |
  |---|---|---|
  | Agriculture `11` | ✅ **use** | ERS FIWS "intermediate product expenses" — **89-91% commodity-mappable**, already in bedrock (`USDA_ERS_FIWS`, no API key), and carries **2024-2025** |
  | Government `G*` | ✅ **use** | `govslocalfin` Current Operations − Salaries and Wages as the column total, 2017-2024 |
  | Manufacturing | ⚠️ energy only | `CSTELEC`/`CSTFU` at 6-digit, 2018-2021 |
  | Services | ⚠️ control totals only | SAS Table 3, 227 six-digit NAICS, 2013-2022 |
  | Wholesale / retail | ❌ nothing | absent from AIES `exp02` |

  **Why the business surveys fail: depth and coverage never coincide.** Manufacturing publishes at full
  6-digit NAICS with zero suppression but only **8.3%** of its column is commodity-mappable (82.5% is one
  materials bucket); the service sectors are 16-45% mappable but publish **one row per sector**. The AIES
  "1992-2023" span is nominal — detailed expenses return **2023 only**, and 2017/2022 have no ASM at all
  (Economic Census years). Manufacturing's mappable share moved a median **0.65pp** across 2018-2021,
  which does not beat inflation-carried 2017 proportions.

  **Why agriculture and government do work.** Both are absent from the business surveys and have their
  own sources. ERS FIWS publishes *intermediate product expenses* as an explicit concept, split into feed
  / seed / fertilizer / pesticide / fuel / electricity / livestock / repairs / transport — 89-91%
  mappable with no giant residual, and the **levels** move hard (total 226.6 → 317.4 → 298.5 billion
  across 2017-2025) in a way a commodity price index will not reproduce. Its limits: one farm sector
  rather than the ~10 BEA agriculture industries, so splitting to detail still needs 2017 proportions;
  and the current year is an ERS **forecast**, not a realized estimate. For government, the `G*`
  industries are mostly not commodity-specific anyway, so a column total is the right object — and the
  same pull yields `Salaries and Wages` for **Step 2** and `Capital Outlay` by function, which bears
  directly on the **open SLG Equipment/Structures/IP attribution bug** (§Step 0). State and local only;
  federal still needs a source.

  **NIPA Section 3 carries the government intermediate purchases, and they match the Use table
  exactly.** Found while mapping the government sectors for Step 2. This bears directly on **#578** and
  changes what that issue should build.

  *Column totals, all general government.* `T31005` (Table 3.10.5, *Government Consumption Expenditures
  and General Government Gross Output*) publishes `Intermediate goods and services purchased` at every
  government level, against the SUT's `T005` column totals:

  | NIPA line | code | $M | BEA detail | $M | diff |
  |---|---|---:|---|---:|---:|
  | Federal, national defense | `W087RC` | 218,671 | `S00500` | 218,671 | **0** |
  | Federal, nondefense | `W131RC` | 108,827 | `S00600` | 108,827 | **0** |
  | State and local | `W140RC` | 724,013 | `GSLGE`+`GSLGH`+`GSLGO` | 724,011 | 2 |

  Its value-added lines tie the same way. This covers **federal**, which the Census state-and-local
  finances source does not — the note above says "federal still needs a source", and this is one. It
  should also track the IOT more closely than an external survey, because BEA builds both sides from the
  same accounts; the zero differences are that, not luck.

  *Cell level, defense only.* `T31105` (Table 3.11.5, *National Defense Consumption Expenditures and
  Gross Investment by Type*) goes further and breaks that 218,671 into 14 named leaves that sum to it
  exactly — so for `S00500` this is a **cell-level** source, not just a column control:

  | | $M | share |
  |---|---:|---:|
  | Aircraft, missiles, ships, vehicles, electronics, other durables; petroleum, ammunition, other nondurables | 61,526 | 28.1% |
  | Transportation of material; travel of persons | 14,556 | 6.7% |
  | Installation support; weapons support; personnel support | 142,589 | 65.2% |

  **Spot-checked against the actual `S00500` column, and the names map but the values do not.** Every
  goods and transport leaf was compared to the BEA commodities it should land on:

  | NIPA leaf | NIPA $M | concept-matched IO cells | IO $M | gap |
  |---|---:|---|---:|---:|
  | **Ammunition** | 3,654 | `33299A` | 3,646 | **−0.2%** |
  | Aircraft | 16,979 | `336411`+`336412`+`336413` | 35,186 | +107% |
  | Ships | 1,871 | `336611` | 5,302 | +183% |
  | Electronics | 5,314 | `334511`+`334220`+`33441A` | 20,545 | +287% |
  | Missiles | 3,517 | `336414` | 6,854 | +95% |
  | Petroleum products | 7,929 | `324110` | 11,609 | +46% |
  | Vehicles | 1,377 | `336120`/`336211`/`336992` | **0** | −100% |
  | Transportation of material | 6,216 | `484000`+`482000`+`483000`+`492000` | 5,212 | −16% |
  | Travel of persons | 8,340 | `481000`+`721000`+`722110` | 7,513 | −10% |

  **Ammunition is the only cell-level match.** The goods lines are all far below their IO counterparts,
  and `Vehicles` has no IO intermediate counterpart at all — every whole-vehicle commodity is zero in
  this column, including `336992` military armored vehicle and tank; only *parts* codes carry value. The
  two frameworks draw the intermediate/investment boundary differently for durables.

  Adding NIPA's matching gross-investment leaf closes aircraft to −1.3% but leaves ships at −67%,
  vehicles at −100% and electronics at +70%, so that is not the explanation either — one hit in five is
  coincidence.

  A warning worth carrying: matching these by *value* alone finds `Ships` → switchgear (1.4%) and
  `Electronics` → ship building (0.2%). Both are nonsense. Value proximity in a 172-cell column is not
  evidence.

  **So the by-type table constrains the defense column without being able to populate it.** The total
  matches exactly and the composition does not; NIPA is a control and a hint at shape, not a source of
  cells. Only ammunition can be placed directly.

  **Defense is the only column with a by-type table.** Nondefense and state-and-local have no `3.11.5`
  equivalent, so `T31005`'s column total is all NIPA offers them; `T31505` (by function) is the next
  thing to check for the state-and-local split.

  **So #578 should use NIPA as the control and the finances data for distribution**, rather than choosing
  between them. The commodity mapping has to be done locally throughout — the spot check above shows
  even the defense by-type table cannot supply cells, ammunition aside.

  **Step 3's default stays #497's inflation-carried 2017 proportions**, with agriculture and government
  as the two justified departures.

### Step 4 — SUT Supply table *(new — the largest unscoped block)*
- 4a. **Domestic output block** — nowcast gross industry output, then split each industry's output
  across commodities using a nowcast commodity mix (port from the #497 R script). Basic value.
- 4b. **Import columns** — `MCIF` from the settled §Trade data recipe: Census **CIF** goods + BEA
  services, mapped to Detail, controlled to ITA. Keep CIF here specifically because `MCIF` is the CIF
  target (2017: Use `|F05000|` / `MCIF` = 0.991, so the two are near-interchangeable as a check).
  Shares the extract with Step 1d — build once, use for both columns.
  `MDTY` comes from the **same request**: pull `CAL_DUT_YR` and customs value alongside `GEN_CIF_YR`,
  form an effective duty rate per sector, apply it to the controlled import vector, and set the level
  from NIPA T30500's customs-duties line (§`MDTY`). Do **not** carry Census duty levels through — the
  calculated-vs-collected gap widens across exactly our 2018-2025 span.
  `MADJ` most likely a 2017-ratio construct — but pull `GEN_CHA_YR` (import charges) in the same
  request first, since it measures the c.i.f./f.o.b. wedge directly and may beat a fixed ratio
  (open question 7).
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
- **Supply/Use match visualization (#587)** — a full-table picture, cell by cell *and* on the row and
  column totals: green where we have input data that matches the reference, a shade of yellow where the
  match is imperfect, white where there is nothing to compare. Built as a comparison engine plus a
  renderer, so the same logic is an assertion in CI and a picture in review. **Every step gets a
  reference to compare against** (Step 1 the Use SUT FD columns, Step 4 the published detail Supply,
  Step 6 the four before-redef MUT tables, and so on), which makes this the one diagnostic that spans
  the whole build. It is also the natural answer to the totals trap: `T014` nets to ~1 and redefinition
  preserves every total, so a green interior with yellow margins localises an error that no scalar
  check can see.

## Issue coverage and priority

Reviewed 2026-08-05 against [project 26](https://github.com/orgs/cornerstone-data/projects/26) (40 items
after this pass) and every open issue in the repo.

### The gating layer sits above these steps

[`bea_code_space_cleanup.md`](https://github.com/cornerstone-data/bedrock/blob/plan_bea_code_space/.claude/plan/bea_code_space_cleanup.md)
is **P0 for this project**, not a
parallel cleanup. Of the 406 BEA detail codes in `NAICS_Crosswalk_BEA_2017_Detail.csv`, **210 cannot be
reached through any NAICS level**, so any FBS routed through a NAICS target schema drops or renormalises
them. Every final-demand defect chased on 2026-07-29 reduced to that one cause.

It had almost no issue coverage: only #546 (the round-trip symptom, and it was not even on the nowcast
board) plus #547 for one downstream instance. Now tracked as **#566** (diagnostic — answers the open
A-vs-B question and confirms or kills the GHG hypothesis) → **#567** (treat `non_naics` codes as their
own roots) and **#568** (`SectorSourceName` expresses a non-NAICS schema) → **#569** (the Phase 1 gate:
GHG A/B zero delta *with the BEA codes present*, plus removing the `'531'`/`'23'`/`'92'` hacks).

**Branch state, verified 2026-08-05.** The Phase 1 branch is **`bea_code_space_phase1`**, off
`nowcast`, pushed and tracking `origin/bea_code_space_phase1` — level with `nowcast`, 13 ahead of
`main` (all plan docs).

**Nothing needed cherry-picking.** An earlier note here said to cherry-pick the `Sector_Levels` swallow
fix as a precondition; that was wrong, because the check behind it tested SHA ancestry rather than
content. The fix is **already in `main` and `nowcast`** via `e1261f0` (PR #549), content-identical to
`d55e8e0`/`468908d`. **The Phase 1 precondition is already met.**

Main also already carries `dcf7077` (S00401/S00402 via `non_naics`), `091bcc3` (U50505 granularity),
`9668ce7` (PCE/PEQ on own bridge rows — **#547's fix**, so #547 may be closeable) and `42f7e59` (the
revert).

The two older branches are both stale: `fix_non_naics_sector_levels` is 13 behind main and 1 ahead,
and that one commit duplicates content already merged — retire it. `nipa_fd_allocation_fix` is 14
behind, 7 ahead, tip `dde7da4` breaks GHG. Phase 1 does **not** branch from it: #569's gate is *GHG A/B
returns to zero delta*, which cannot be demonstrated from a base that itself breaks GHG, and it is
Phase 2 work sitting on unmet Phase 1 preconditions — the mistake the cleanup plan itself records. It
stays as the **Phase 2 consumer branch**, rebased onto `nowcast` after #569 passes. Four of its commits
are not Phase-2-gated and are worth landing on `nowcast` separately to shrink it: `4434f7e` (three-way
bridge comparison, which #576 depends on), `220682e`, `ea852b8`, `e4e24a7`.

### What is gated, and what is not

The steps split cleanly by whether they run through the FBS attribution machinery. **This is the reason
the project does not have to stall behind P0.**

| Gated by the code-space fix | Independent of it |
|---|---|
| Step 1 — FD block (already bitten) | Step 4a domestic output / commodity mix |
| Step 2 — VA; `NIPA_VA_*.yaml` is a new FBS and will hit the same wall | Step 4c transaction-level margins |
| Step 3 — intermediate, incl. the agriculture/government departures | Step 4e Supply identities |
| Step 4b/4d — #528 maps NAICS→BEA Detail, the exact lossy path | Steps 5, 6, 7 — transform over benchmark tables |

Step 4 is both the **longest pole** and mostly **ungated**, so it starts immediately. Sequencing
everything behind Phase 1 would idle the critical path; sequencing nothing behind it means Step 2
rediscovers the same 210-code problem from scratch.

### Coverage by step

| Step | Issues | Remaining gap |
|---|---|---|
| 0 Hygiene | #523, #539/#540, **#573** (summary SUT vintage), **#574** (SLG attribution) | — |
| 1 FD block | #504, #523, #526/#527/#528, #529/#530/#531, #547, **#575** (1d code list), **#576** (1f reconciliation) | — |
| 2 Value added | #535, #536, #537, #538 | — |
| 3 Intermediate | #497, #564, **#577** (agriculture), **#578** (government) | — |
| **4 Supply table** | **#570** (4a), **#571** (4c), **#579** (4b), **#580** (4d), **#581** (4e) | — |
| 5 RAS | **#588** (sut_ras), **#589** (load_suts_from_r), **#590** (check_balances), **#591** (optional controls) | — |
| 6 SUT→MUT | USEEIO #4 (6b), **#582** (6a), **#583** (6c), **#584** (6d), **#585** (2017 replay) | 6b is tracked in USEEIO, not bedrock |
| 7 Redefinitions | **#572** | — |
| 8 Cornerstone schema | **#586** | — |
| 9 Storage/pipeline | **#592** (GCS storage), **#593** (pipeline integration) | — |

**No drafts remain.** The six Step 5 and Step 9 cards were converted in place to issues #588-#593,
keeping their board position and `Todo` status, then given step-prefixed titles, bodies, the
`nowcasting` label and milestone `v0.5`. The superseded VA-redefinition draft was removed earlier and
replaced by #572. **Every item on the board is now a trackable issue.**

### Priority

- **P0** — #566 → #567/#568 → #569. Land `fix_non_naics_sector_levels` first.
- **P0-parallel** — #570 (4a) and #571 (4c). 4c has the highest fan-out of anything unbuilt: it feeds
  the Supply margin columns, Step 6b's PUR→PRO conversion, and the Step 6d Margins deliverable.
- **P1** — code-space Phase 2 (retarget `FD_Gov`/`FD_Structures`/`FD_IP`, drop
  `map_fbs_sectors_to_model_schema`, roll out 2018-2024), which unblocks #576, Step 2 and Step 3.
  #574 may fall out of this rather than needing its own attribution work — diagnose first. Then #579,
  #580, #581 once the trade FBS path is safe. #573 any time; it is small and Step 5 needs it.
- **P2** — Step 5 (promote the four drafts), then #582/#583/#584 and **#585**, the 2017 benchmark
  replay — the single highest-value test in the project — then #572.
- **P3** — #586, then Step 9 (promote drafts).

**Filed 2026-08-05:** #573-#587 new, #588-#593 converted from drafts. One issue per sub-step, all on
the board with milestone `v0.5`. **Every plan step now has full issue coverage.** Board: 33 → 56 items,
zero drafts.

**Titles carry their step.** All 45 board issues were renamed to a `Step <n><letter>: ` prefix so the
board reads against this plan — `Step 1a`, `Step 4c`, `Step 6a` and so on. Two non-step prefixes:
`BEA code space: ` for the gating layer (#546, #566-#569), and `Diagnostics: ` for #587, which spans
every step. USEEIO #4 is `Step 6b`. Pull requests were left alone; they inherit from their issue.

**The board is sorted to match.** Item positions run gate → Step 0 → 1 → 1a-1f → 2 → 3 → 4a-4e → 5 →
6a-6d → 6 (the whole-phase replay, after the sub-steps it validates) → 7 → 8 → 9 → Diagnostics. Pull
requests sit with the step of the issue they close. Note this sets **manual position**, so it holds
only while a view has no explicit sort of its own; a saved sort in the view UI overrides it.

## Open questions

1. ~~**#527 trade-data pick**~~ — **resolved** in
   [#557](https://github.com/cornerstone-data/bedrock/pull/557): Census goods + BEA services as the
   primary extract (from flowsa `imports`, not USEEIO's imports-only downloader), BEA ITA 2.1/3.1 as
   the national totals control, BACI out of scope. Details and the carried-forward reconciliation work
   in §Trade data. `MDTY` was *not* covered by the pick and stays open (§Still unsourced).
2. **Commodity mix method for Step 4a** — port the R script's approach wholesale, or derive from
   2017 detail Supply shares moved by industry output? Nothing is built here yet.
3. ~~**Reconcile #497's after-redefinitions instruction.**~~ — **resolved**: the pipeline is
   **SUT → MUT before redefinitions → MUT after redefinitions → MUT in Cornerstone schema**, i.e.
   Steps 6 → 7 → 8 exactly as written. Everything upstream of Step 7 stays before-redefinitions,
   including the Step 3 intermediate seed (seed from `Use_SUT_Framework_2017_DET` — native SUT, native
   purchaser, native before-redef). #497's instruction to nowcast the intermediate block on the
   *after*-redefinitions Use table is **overridden**; it would mix states inside the SUT.
4. **RAS control totals** — summary SUT totals for all years (the default, once 2023-2024 are loaded),
   or industry/commodity gross output? The board lists the latter as an explicit option. No longer
   forced either way by Phase 2: summary SUT will exist for 2025 too, so this is a methodological
   preference rather than a data constraint.
5. **`nowcast.py` vs. `bedrock/transform/iot/` boundary** — is `nowcast.py` the per-year orchestrator
   calling into `transform/iot/`'s existing functions, or should the new SUT/MUT code live in
   `transform/iot/` (where #495 pointed `nipa_final_demand_estimates.py`)? Steps 4-7 are a lot of new
   code; worth settling before writing it.
6. **Interim caching layout** while developing (final destination is GCS, per the board).
7. **`MADJ` treatment** — Census `GEN_CHA_YR` import charges (measures the wedge directly, free with
   the `MCIF` pull), a 2017 ratio, or drop it and absorb into `MCIF`? Affects whether `T013` reconciles
   exactly.

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
([io_2017.py:730-739](../../extract/iot/io_2017.py#L730-L739)) — *"BEA revises historical data in
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
   ([matrix_mappings.py:63-83](../../utils/taxonomy/bea/matrix_mappings.py#L63-L83)), plus
   `usa_io_data_year` and `model_base_year` in
   [usa_config.py:49-64](../../utils/config/usa_config.py#L49-L64) — all currently stop at 2024.
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
