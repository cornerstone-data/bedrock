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
  on the *Supply* side as `MCIF` + `MADJ`. `derive_initial_Y_pur` reindexes to
  `SUT_FINAL_DEMAND_CODES` (the 20 MUT FD codes minus `F05000`). `F05000` is created in Step 6b from
  Supply-side imports, not sourced as a Use column.
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
consumers. Still worth doing in Step 0 — two of the seven years can't be loaded today — but **note the
changed justification**: this was written when summary SUT totals were assumed to be the RAS control.
Under §Step 5 Decision 3 they are deliberately **not** the default target, and their most likely role
is the opposite one, as the independent object the balanced detail SUT is *validated* against. That is
a reason to load them, not a reason to skip it.

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

**Not in bedrock, to be ported:** a balancing algorithm (**no longer a straight `sut_ras.py` port** —
see Step 5, which now carries three open decisions) and commodity mix / intermediate nowcasting
(`CalculateIntermediateUseAndCommodityMix.R`, per #497). Confirmed: no RAS/GRAS code anywhere in
bedrock today.

**The USEEIO port list shrank to one file.** `check_balances.py` and `load_suts_from_r.py` were both
on it and are now **dropped, #590 and #589 closed**. The SUTs come from Steps 1-4 as bedrock objects,
so nothing R-produced needs loading; and the balance checks are a dozen lines against our own tables,
not a port — `check_balances.py`'s own `check_row_col_balance` is domestic-only and never checks
`T016` = `T019` at all. The checks live in Step 5 (post-balance identities), Step 4e/#581 (Supply
identities) and #587 (per-cell match). One idea was worth keeping: its `compare_disagg_to_agg` is the
detail-vs-published-aggregate comparison that Decision 3 gives summary SUT its new role in, and it
shares the aggregator machinery with Decision 3's aggregate constraints.

## Section status

### Final uses — three methods, not one (purchaser price)

**1A `NIPA_final_consumption`** carries `F01000` and the `F02*`/`F06*`/`F07*`/`F10*` columns.
`F03000` is **1C** and `F04000` is **1B**, each its own FBS — see §Step 1.

| Code | Description | Status |
|---|---|---|
| F01000 | Personal consumption expenditures | ✅ `FD_PCE` (+ 5 name-mismatch activity_sets) via `BEA_PCEBridge` — **reproduces the bridge cell for cell**, 259/259 commodities, zero cells off by >$1M ([#631](https://github.com/cornerstone-data/bedrock/pull/631)) |
| F02E00 | Nonres. private fixed investment in equipment | ✅ `BEA_PEQBridge` — **reproduces the bridge cell for cell**, 107/107 commodities, zero cells off by >$1M ([#631](https://github.com/cornerstone-data/bedrock/pull/631)). Built in [#524](https://github.com/cornerstone-data/bedrock/pull/524), closing [#496](https://github.com/cornerstone-data/bedrock/issues/496)/[#525](https://github.com/cornerstone-data/bedrock/issues/525). ⚠️ But it disagrees with the **Use table** by ~$15B at the column total — [#547](https://github.com/cornerstone-data/bedrock/issues/547) |
| F02N00 | Nonres. private fixed investment in IP products | ✅ `FD_IP_direct`/`FD_IP_proportional` |
| F02R00 | Residential private fixed investment | ✅ `FD_Structures1` — ⚠️ short `S00402` by 1,883, blocked behind [#635](https://github.com/cornerstone-data/bedrock/issues/635) |
| F02S00 | Nonres. private fixed investment in structures | ✅ `FD_Structures2` |
| F06/F07/F10 (12 codes) | Federal/State/Local CE, Equip, IP, Structures | ✅ `FD_Gov_*` — **all twelve reproduce the Use table cell for cell**, zero cells off by >$1M ([#633](https://github.com/cornerstone-data/bedrock/issues/633)/[#634](https://github.com/cornerstone-data/bedrock/pull/634)). The former SLG Equipment/Structures/IP bug was `S00402` unreachable via two crosswalks, and was never SLG-only |
| F03000 | Change in private inventories | ⏳ **own FBS — 1C.** Source settled, mostly already extracted (§`F03000` below, [`inventories_estimation_plan.md`](inventories_estimation_plan.md)) — `U50705BU1` is in the extract list and unused; not built. [#529](https://github.com/cornerstone-data/bedrock/issues/529)/[#530](https://github.com/cornerstone-data/bedrock/issues/530)/[#531](https://github.com/cornerstone-data/bedrock/issues/531) |
| F04000 | Exports | ⏳ **own FBS — 1B.** 2017 overlay ([#528](https://github.com/cornerstone-data/bedrock/issues/528): [#617](https://github.com/cornerstone-data/bedrock/pull/617) [#618](https://github.com/cornerstone-data/bedrock/pull/618) [#622](https://github.com/cornerstone-data/bedrock/pull/622) [#623](https://github.com/cornerstone-data/bedrock/pull/623)) — Census FAS goods + IEA TypeOfService leaves; `S00900` from −F010 + Supply T016. ITA scale and 2018–2024 methods open. 2017 scorecard FAIL + inventory vs #557 bars. ⏳ Schema retarget onto `BEA_detail_commodity_target.yaml` pending in [#638](https://github.com/cornerstone-data/bedrock/pull/638), which also supplies the `target_schema_year` key the methods have been missing since [#630](https://github.com/cornerstone-data/bedrock/pull/630) |
| ~~F05000~~ | ~~Imports~~ | **Not an SUT column** — belongs to Supply (`MCIF`/`MADJ`); appears only on MUT conversion |

### SUT Use — other blocks

| Block | Status |
|---|---|
| Value added (`V00100`, `T00OTOP`, `V00300`, `T00TOP`, `T00SUB`) | ❌ NIPA tables identified, not built — Step 2 |
| Intermediate (commodity × industry) | ❌ Method identified (#497), not built — Step 3 |

### SUT Supply — every column

**This is the least-developed part of the project.** `MCIF` has a 2017 candidate; every other column is unsourced.

| Column | What it is | Candidate source | Status |
|---|---|---|---|
| cells / `T007` | Domestic output, commodity × industry, basic value | Nowcast gross industry output (`derived_gross_industry_output.py`) × commodity mix (port from `CalculateIntermediateUseAndCommodityMix.R`, #497) | ❌ **unsourced method** |
| `MCIF` | Imports, c.i.f. | Census goods **CIF** (`GEN_CIF_YR`) + BEA `IntlServTrade` — §Trade data below | ⏳ **2017 candidate** ([#528](https://github.com/cornerstone-data/bedrock/issues/528) / [#622](https://github.com/cornerstone-data/bedrock/pull/622) [#623](https://github.com/cornerstone-data/bedrock/pull/623)) — Trade_Imports FBS on `MCIF` only. ITA scale, `MDTY`, and `MADJ` open |
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
are single values per commodity. They reconcile by aggregation — **but the trade identity carries a
tax term**, because the two tables sit in different frameworks:

```
Σ_buyers ( Wholesale[b,c] + Retail[b,c] )  =  TRADE[c] + TOP[c]
Σ_buyers   Transportation[b,c]             =  TRANS[c]
```

Confirmed by BEA (B. Jolliff, National Economic Accounts, 2025-05-30): the Margins table is on the
make-use framework and the Supply table on supply-use, and *"the difference is accounted for in
wholesale and retail trade commodity tax — in the supply-use framework these taxes show up in the
Taxes on products column; in the margins table these values are built into each of the margins
fields."*

Measured on 2017 detail: `TRANS` holds directly (320/402 commodities within 1%), and the trade
identity **with** the `TOP` term holds for 226 of the 255 commodities that carry positive `TRADE`.
Dropping the term overstates `TRADE` by **385,283 million, 11.8%**. See
[#571](https://github.com/cornerstone-data/bedrock/issues/571) for the full check.

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
   What remains is not a source question but reconciliation on #528: ITA G+S scale, FAS vs PUR,
   import valuation overlap (CIF vs customs / `MADJ`), and `S00300` specials policy. IEA TypeOfService
   leaves are mapped; Crosswalk revisions sit in `bedrock/transform/trade/README.md`. `MDTY` rides on
   the same Census request (`CAL_DUT_YR`) — see §`MDTY`.
3. **Commodity split of product taxes and subsidies** (`TOP`, `SUB`, and the Use table's
   `T00TOP`/`T00SUB` rows) — NIPA gives totals (T30500, T31300); allocating to 402 commodities does
   not come for free. Default proposal: 2017 detail Supply shares, held constant, inflated with the
   commodity.
4. **`MADJ`** — no annual published analogue; likely 2017-ratio-based, though Census `GEN_CHA_YR`
   (import charges) measures the same c.i.f./f.o.b. wedge and comes free with the `MCIF` request, so
   try it before defaulting to a fixed ratio. Small in magnitude, but it sits inside the `T013`
   identity, so it can't just be dropped.
5. ~~**Change in inventories commodity attribution (#530)**~~ — **rescoped, and no longer a deferral**
   (§`F03000` below, [`inventories_estimation_plan.md`](inventories_estimation_plan.md)). The old text
   here deferred it on needing "ASM stage-of-fabrication + Economic Census materials-consumed data".
   Both statements were wrong in the same way: those sources govern **manufacturing, 2% of the
   column**, while **wholesale + retail is 126%** of it. The stage split is already published in
   `U50705BU1` — which bedrock already extracts and never reads — and three of BEA's four allocation
   rules are functions of tables this project already builds. What remains is one crosswalk of ~25
   trade industries to BEA commodities.
6. ~~Import matrix allocation method~~ — **settled**: proportional to the commodity's use shares along
   its Use-matrix row (Step 6c). Not a data gap; it needs the Step 6b Use matrix as input, which makes
   6c strictly downstream of 6b.

## Trade data — source settled, 2017 path in #528

Settled in [#557](https://github.com/cornerstone-data/bedrock/pull/557) (analysis in
`bedrock/analysis/nowcasting/trade_data/`, closing [#527](https://github.com/cornerstone-data/bedrock/issues/527)).
Implementation is [#528](https://github.com/cornerstone-data/bedrock/issues/528): extract
[#617](https://github.com/cornerstone-data/bedrock/pull/617), FBS
[#618](https://github.com/cornerstone-data/bedrock/pull/618), Y F040 + Supply MCIF overlay
[#622](https://github.com/cornerstone-data/bedrock/pull/622), 2017 scorecard + IEA leaf Crosswalk
[#623](https://github.com/cornerstone-data/bedrock/pull/623). This path feeds `F04000`, Supply `MCIF`,
and Step 6c's import matrix. ITA G+S scale, shared BEA Detail target (#567 / #568), `MDTY` / `MADJ`,
`S00300` hold-from-Supply, and 2018–2024 Trade methods remain open on #528.

| Role | Source |
|---|---|
| Goods extract | Census International Trade, NAICS-6 — imports **CIF** (`GEN_CIF_YR`, to match Supply `MCIF`), exports FAS-family (`ALL_VAL_YR`) |
| Services extract | BEA `IntlServTrade`, by type of service, imports and exports |
| National totals control | BEA ITA Tables 2.1 (goods) / 3.1 (services) — scale or residual-constrain; a raw Census+BEA sum is **not** the final total. Loader exists; FBS methods do not apply the scale. |
| Sector bridge | `NAICS_Crosswalk_Census_USATrade.csv` (Census NAICS-6 → Detail); `NAICS_Crosswalk_BEA_IEA.csv` (IntlServTrade TypeOfService leaves → Detail). Parents omitted when children are mapped. |
| 2017 structure/specials benchmark | Use `F04000` and Supply `MCIF` (`F05000` is MUT-only) |

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

**Services carry no duty**, so unlike `MCIF` this column is goods-only — the IEA TypeOfService map
does not apply. Net of the tariff-era caveat, `MDTY` should be the most tractable of the three import
columns.

Bonus: `GEN_CHA_YR` (import charges) is a direct measurement of the freight/insurance wedge that makes
CIF overshoot when added to full BEA services — so it feeds the valuation-overlap question in problem 1
above, and plausibly `MADJ` (open question 7) as well, since c.i.f./f.o.b. adjustment is the same wedge.
Worth pulling in the same request even if only used diagnostically.

**Code home:** `bedrock/extract` `Census_USATrade`, `BEA_IEA`, and `BEA_ITA`; FBS methods
`Trade_Exports_<year>` / `Trade_Imports_<year>` under `bedrock/transform/trade/`. USEEIO's
`download_imports_data.py` is imports-only legacy and is *not* the production extract.

**What the 2017 scorecard says** (`score_2017_trade_detail`, USD, after F040 overlay + `S00900`
identity and MCIF-only bridge; inventory in `bedrock/transform/trade/README.md`):

| | National % | Pearson all / non-`S00*` | Top-20 Jaccard all / non-`S00*` |
|---|---|---|---|
| F040 exports | +6.15% | 0.93 / 0.89 | 0.60 / 0.60 |
| MCIF imports | +1.29% | 0.62 / 0.84 | 0.67 / 0.74 |

F040 national miss is Census FAS goods vs SUT goods F040 (~+14%); mapped IEA leaves sit ~2% under SUT
service F040. Census + IEA `AllTypesOfService` tracks ITA G+S; SUT F040 is the lower frame. MCIF
national sits inside ~2–3% because `S00300` miss (260 B) offsets CIF goods and mapped-service
overshoot. Export Pearson on non-specials clears the #557 bar; import Pearson is just short of 0.85.
Uniform ITA scale does not fill `MISS` holes or move Pearson.

**Three carried-forward problems, all on the reconciliation side, none of them a source problem:**

1. **Import valuation overlap.** Census CIF includes freight/insurance that BOP records inside
   services, so CIF goods + full services double-counts. Decide customs (`GEN_VAL_YR`) vs CIF *before*
   the ITA control, and keep CIF when the target is Supply `MCIF` unless the control step sets levels.
2. **IEA TypeOfService → Detail** maps leaves whose labels name the commodity (or a small family the
   type spans). Parent totals are omitted when children are mapped. Unmapped by rule: `Travel` other
   than `TravelHealth`, transport port services (no 2017 `488000`), `GovtGoodsAndServicesNie`,
   `OthBusinessNie`, digitally deliverable cross-cuts (`PotIctEnServ*`). IP license leaves land on
   `533000` / `511200` / `512*`. Crosswalk revisions (EBOPS–CPC–NAICS, TTSA for Travel, 1:m weights)
   are listed in `bedrock/transform/trade/README.md`. Goods NAICS coverage is high; remaining ≥1 B
   `MISS` holes include couriers `492000`, `550000`, publishing `51112*`/`51113*`, bakery `311810`,
   cattle `1121A0` (no Census `1121*` activity), restaurants, and electric `221100`.

   **`S00900` needs no extract.** `derive_initial_Y_pur` sets
   `Y[S00900, F04000] = −Y[S00900, F01000] + Supply_T016[S00900] × 1e6` (2017). It has *zero
   intermediate use* across all 402 industries, and only two final-demand entries, which are one
   offsetting reclassification of nonresident expenditure:

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
   two lines `FD_PCE_less_nonresident` already handles with `negate_flows` (`7a04a71`). The export
   identity matches published F040 within 1 M USD.
3. **Specials and margins policy.** `S00300` (noncomparable imports, ~260B) needs an explicit rule —
   held from Supply, taken as a residual after mapped commodities, or out of the extract's scope —
   rather than a silent zero. **`S00900`'s rule is the supply identity** (item 2). `S00300` is 0 in
   `F04000`; its ~260B is import and intermediate presence. Totals can look fine while structure is
   wrong — ITA scale after specials and the service map, not before.

**Exports are FAS/BOP; the Use column is PUR** — a margins bridge that doesn't exist yet. Provisionally
accept FAS and flag it.

**Acceptance bar before this is trusted for nowcast years** (from #557, not met): national totals
within ~2-3% of Use/`MCIF`; import commodity vector Pearson ≳ 0.85 and export ≳ 0.75-0.85 on
non-special codes; top-20 Jaccard ≳ 0.7 / ≳ 0.6; every known hole covered by a written rule. If the raw
extract can't hit those bars, a documented pipeline that does (extract structure × scale to Use totals,
or extract + Use residual on specials only) is an acceptable substitute — and *that* pipeline is what
the nowcast years carry forward. The 2017 gate is `uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail`.

## `F03000` — change in inventories, rescoped

Full treatment in [`inventories_estimation_plan.md`](inventories_estimation_plan.md), from three
emails from David Hill (BEA, National Economic Accounts, 2025-03/05), reproduced in #530's body.
**This section replaces the previous "deferred, needs ASM and Economic Census" position.**

BEA's method is four rules over three inventory types, applied to CIPI **by holding industry** — NIPA
records *where* inventory is held, the Use column records *what* is held:

| Inventory type | "What held" rule | Source |
|---|---|---|
| Finished goods | primary products of the reporting industry | industry commodity mix — **Step 4a** |
| Work-in-process | same, primary to the industry | industry commodity mix — **Step 4a** |
| M&S — merchandise trade | what the trade industry sells | trade product line — **the one gap** |
| M&S — production materials | the industry's intermediate input mix | intermediate block — **Step 3** |

**Three of the four rules are functions of tables this project already builds**, and both are
published for 2017 — so the method is testable on the benchmark today without waiting on Step 3 or 4a.

**`U50705BU1` (Table 5.7.5BU1, CIPI by Industry) is already extracted and never read.** It sits in
[`BEA_NIPA.yaml:30`](../../extract/bea/BEA_NIPA.yaml#L30) for 2012-2024 with no consuming activity set
in `NIPA_FD_2017.yaml`. **It also already publishes the stage-of-fabrication split** that Hill
attributes to ASM (`C30M`/`C30W`/`C30F`, lines 29-37, each durable/nondurable) — so no ASM pull is
needed for a first pass.

**The magnitudes invert the emphasis, and this is the reason the old scoping was wrong.** 2017 nonfarm
CIPI by holding industry: wholesale 30,329 + retail 17,930 = **48,259, i.e. 126% of the 38,353
column**; manufacturing is **818, about 2%**. The ASM and `EC1731MATFUEL` machinery the deferral was
waiting on governs the 2% branch. The 126% branch runs on the simplest of the four rules.

**The gap is one crosswalk: ~25 `U50705BU1` trade industries → BEA detail commodities.** It is the same
question as the NAPCS → I-O concordance in [#615](https://github.com/cornerstone-data/bedrock/issues/615)
at coarser resolution, so ⚠️ **decide the two together** — #615 subsumes it, and if #615 stays deferred
this is the cheap version that margins may be able to borrow. Concept-matched spot checks on the three
largest trade lines land right: drugs wholesalers 11,287 → `325412` 7,547; petroleum wholesalers
−5,885 → `324110` −7,387; motor vehicle dealers 14,151 → `336111`+`336112` 9,500.

**Farm is a separate build, and neither half needs a new extractor.** `U50705BU1` is **nonfarm-only**
— 2017 total 32,674 against nonfarm 38,353 implies farm ≈ −5,679, **17% of the column**, and a build
that omits it is silently wrong. The **level** comes from NIPA (add 5.7.5B to `BEA_NIPA.yaml`; farm
CIPI is not extracted today), never from USDA — sourcing it elsewhere breaks the exact-total identity
that is the one free thing here. The **commodity split** comes from `USDA_ERS_FIWS`, already in
bedrock: its `Inventory` variable splits Crops / Livestock / Purchased inputs, 1939-2025, no API key,
and `Purchased inputs` gives farm the same M&S stage that `U50705BU1` gives manufacturing.
⚠️ **FIWS publishes stock levels — do not difference it and call the result CIPI.** The 2016→2017
difference is −887 against a farm CIPI of ≈ −5,679: right sign, ≈6× off, because differencing
book-value stocks carries holding gains that CIPI excludes via the valuation adjustment. Structure
from FIWS, level from NIPA — the same construction as §`MDTY`.

⚠️ **And do not apply the M&S rule to the raw Use column**: it includes services and non-storables that
are never held in inventory, which is why BEA cites materials-and-fuels rather than the Use column.

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
  Still needed, but as a **validation baseline** rather than the RAS control — see §Framework facts and
  §Step 5 Decision 3.
  Small, and Step 5's control totals depend on it.

### Step 1 — Final Uses with Trade (PUR) — *in progress*

⚠️ **Rescoped 2026-08-14 ([#523](https://github.com/cornerstone-data/bedrock/issues/523)): there is
no single Final Demand FBS.** Final uses are **three separate FBS methods**, built and validated
independently, with trade in scope because exports are a final use and the same extraction serves the
Supply-side import columns. The old shape put trade and inventories inside a method that never
contained them — `F03000` was hardcoded zero and `F04000` was bolted on inside
`derive_initial_Y_pur` — so the split mostly ratifies what was already true.

**1A — `NIPA_final_consumption`** (renamed from `NIPA_FD`; the old name promised a block it never
delivered). `F01000`, the four `F02*` investment columns, and the twelve `F06*`/`F07*`/`F10*`
government columns.
- ✅ PEQ Bridge / `F02E00`; PCE Bridge / `F01000`.
- ✅ F0-code assignment (`assign_sector_consumed_by_from_clean_parameter`, #539).
- ✅ **PCE and equipment reproduce their bridges cell for cell** — 259/259 and 107/107 commodities,
  zero cells off by >$1M (#630, #631).
- ✅ **All twelve government columns reproduce the Use table cell for cell** (#633, #634). `S00402`
  was unreachable in them because it was missing from *two* crosswalks — one supplying the
  attribution weight, one the receiving set — and fixing either alone changed nothing. F10E00 had
  been misallocating 32.8% of its column.
- ❌ Rename the method and yamls off `NIPA_FD`; review all activity mappings; 2018–2024 have no PCE
  activity sets (#621); port the common yaml (#495).

**1B — Trade** (#526, #528). ⏳ `F04000` exports plus the Supply-side import columns from one
extraction — Census FAS goods + `BEA_IEA` `TypeOfService` leaves, mapped to BEA Detail, with
`S00900` from the identity −F010 + Supply `T016`. Built and wired for 2017 (#617, #618, #622, #623);
the same extract serves Step 4b. Open: ITA G+S scale, FAS→PUR, and the 2018–2024 methods. The 2017
scorecard is **FAIL + inventory** against the #557 bars (`score_2017_trade_detail`; §Trade data).
⏳ **Schema fix pending in [#638](https://github.com/cornerstone-data/bedrock/pull/638).** The trade
yamls declared `target_naics_year` where the code has read `target_schema_year` since #630, so they
raised `KeyError` — which took `derive_initial_Y_pur` down with them and blocked validating the NIPA
columns that have nothing to do with trade. #638 retargets both methods onto the shared
`BEA_detail_commodity_target.yaml` (the same include `NIPA_final_consumption` uses), which supplies
the key, and retires `Trade_detail_passthrough.yaml` along with the
`activity_schema: NAICS_2017_Code` weight-source override. Once it lands, the end-to-end validation
path opens up — and 1A's columns can be checked without a working trade leg for the first time.

**1C — Change in inventories** (#529, #530, #531). `F03000`, the full 402-row column, per
[`inventories_estimation_plan.md`](inventories_estimation_plan.md). **Not NIPA-total-only**: that
ships one scalar where the SUT needs a column, and leaves the allocation to Step 5's RAS, which has
neither a seed nor a sign-safe structure for a column that runs 3× gross to net across 61 negative
commodities. Start by consuming `U50705BU1`, already extracted and unused, plus the farm line it
omits (≈ −5,679 in 2017, 17% of the total).

**Above the three — composition and validation.**
- ⏳ **Split the column list by framework** (#575) — ✅ done in substance: `derive_initial_Y_pur`
  targets `SUT_FINAL_DEMAND_CODES` (MUT FD minus `F05000`), and `F05000` is created in Step 6b from
  Supply-side imports rather than sourced as a Use column. What remains is a *composition* concern:
  the assembly of three methods' output still lives inside `derive_initial_Y_pur`, so a trade-side
  failure blocks validating NIPA columns that have nothing to do with trade. Pulling assembly out
  decouples them.
- ❌ **Validate per-column against published NIPA aggregates** (#576). Already satisfied for the
  government columns and for PCE/equipment against their bridges; outstanding for `F03000`
  (unbuilt), `F04000` (blocked), and the two gaps below.

**Gaps carried.** `F02R00` is short `S00402` by 1,883 (#633), blocked behind #635 — a `ValueError`
in one activity_set discards every activity_set and returns an empty FBS as a valid result, so the
line cannot safely be given its own set. And `F02E00` matches the PEQ bridge exactly while
disagreeing with the Use table by ~$15B at the column total (#547) — a source-data question, not an
attribution one.

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
- 4b. **Import columns** — 2017 `MCIF` is the Trade Imports FBS (Census **CIF** goods + IEA services,
  Detail Crosswalk). Keep CIF because `MCIF` is the CIF target (2017: Use `|F05000|` / `MCIF` = 0.991).
  ITA scale, `S00300` residual, and later years stay on #528. Shares the extract with Step 1d.
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
  per (buyer, commodity, margin type); `derive_PRO_to_PUR_ratio.py`'s `_inflate_margins_to_year` is a
  working precedent but is Cornerstone-schema and phi-oriented (it aggregates to per-commodity totals
  via `_margins_by_commodity` before computing phi), so expect adaptation rather than reuse — the
  nowcast needs the pre-aggregation detail preserved.

  **Five method points, all from BEA correspondence (B. Jolliff, 2025-05/06) and verified against 2017
  detail in [#571](https://github.com/cornerstone-data/bedrock/issues/571):**

  1. **Apply rates to `T013`, not domestic output.** *"Total product supply (column OR) includes total
     commodity output plus imports; margins are distributed based on the value in column OR."* Imported
     goods carry domestic margin, so a rate carried on `T007` alone is biased — and the 2009 manual
     says the same twice independently, using "interim supply (output + imports)" as the weight in both
     the wholesale step (4) and the transport step (2). **This is a real sequencing constraint — see
     below.**
     ⚠️ Note `TRADE`/`T013` is **not** bounded by 1 and a ratio above 1 is not evidence of an error:
     margin is *added to* basic value (`T016 = T013 + T014 + T015`), not carved out of it. 21
     commodities exceed 1 legitimately — apparel is 1.84 — while `TRADE`/`T016` exceeds 1 for none.
  2. **Aggregate with the tax term**: `Σ_b (Wholesale + Retail) = TRADE[c] + TOP[c]`, not without it.
  3. **Excise and sales tax sit in different fields.** Sales tax is inside the Wholesale/Retail
     columns; excise is inside `Producers' Value`. Both land in `TOP`, which is why the identity in (2)
     over-corrects by the excise share — a −1.29% residual overall, concentrated on exactly the excise
     goods (tobacco, distilleries and breweries all at ≈ −0.37 of their `TOP`). **Model this split if
     margins are ever needed in basic prices**; ≈0.37 for alcohol and tobacco is a usable start.
  4. **Negative margins are the change in inventories — do not clip them.** All 31 negative rows in the
     2017 table are buyer `F03000`, −8,076 million, and nothing else in the table is negative. They are
     a timing artefact: margin is booked when inventories build, so a drawdown shows negative.
     ⚠️ `_margin_negatives_treatment`'s `abs_negative_margin_columns` flag would destroy exactly this
     signal — check before reusing.
  5. **Import `TRANS` is domestic-port-to-user only.** The foreign-port-to-domestic-port leg is already
     inside `MCIF`, so the transport margin is not the whole freight bill and must not be built as one.

  Then derive the Supply columns by aggregation, per (2) above.
  **Validate per commodity, never in aggregate.** `T014` nets to **1** economy-wide against **7,361,003**
  of gross mass — so a totals check here does not merely risk passing on broken data, it passes on
  *anything*. Separately check that the transaction-level table reproduces the two Supply columns
  commodity by commodity, since that identity is the only thing tying the fine and coarse objects
  together.

  **The negative side is nearly free; budget the effort on the positive side.** The −3.68 trillion is
  supplied by just 24 commodities giving up almost all their own output — 19 trade commodities at
  **96.8%** of their combined `T007` (eight retail sectors at exactly −100.0%) and 5 transport
  commodities at **56.8%**. That side is close to a function of trade/transport output, which 4a
  produces anyway. The work is the **positive-side allocation across the 255 receiving commodities**,
  which is what the transaction-level rates are for.
- 4d. **Tax/subsidy columns** (`TOP`, `SUB`) — NIPA T30500/T31300 totals split by 2017 commodity
  shares. Remember `SUB` is negative here and positive in the Use table.
- 4e. **Verify the four Supply identities per commodity** (`T013`/`T014`/`T015`/`T016` above), 402/402,
  before declaring the Supply table done.

### Step 5 — Balance the SUT (RAS)

**⚠️ Three decisions gate this step. Do not start writing code until all three are made** — the
**starting point** (Decision 1), the **objective function** (Decision 2), and the **target set**
(Decision 3), below. What follows replaces the earlier one-line instruction to port `sut_ras.py`;
that is no longer the recommended route.

**What stays true regardless of how the decisions land:**
- Balance on the SUT identity: **total supply at purchaser (`T016`) = total use (`T019`) per
  commodity**, plus industry-output consistency between Supply and Use. This is an accounting
  identity we impose, not a target we source.
- **Verify the balance in place**, in `bedrock/utils/validation/`, as tests rather than scripts:
  `T016` = `T019` per commodity 402/402, and industry output consistent between the Supply and Use
  sides. Written against bedrock's own tables — **this is no longer a `check_balances.py` port**
  (#590 closed; §Grounding). The four Supply identities stay with Step 4e/#581 and the per-cell match
  with #587; share those, don't rewrite them. ⚠️ Per commodity, never in aggregate — `T014` nets to
  ~1 economy-wide, so an aggregate check passes while every cell is wrong. ⚠️ And label which checks
  are **by-construction**: `T016` = `T019` is imposed by the balance and proves only that the solver
  ran, as does anything Decision 3 promotes to a target.
- The **seed** is not in question: it is the Steps 1-4 output, block by block. What is in question is
  what the balancer is allowed to do to it, and what it is aimed at.

**What is no longer assumed:** earlier drafts of this step said "summary SUT totals by default, with
industry/commodity gross output as the named alternative." **Strike the default.** The targets are now
Decision 3, to be enumerated margin by margin and sourced deliberately.

#### Why the straight `sut_ras.py` port is off

**Bedrock's SUT is full of negatives, and the negatives are structural, not noise.** `F03000`
inventory change, negative `TRANS` on the trade and transport commodities, `SUB` subsidies, and the
give-up side of every margin reassignment. A balancer that cannot carry a negative cell through a
scaling step is not a candidate — it will either clamp real mass to zero or flip signs on accounting
lines that have a fixed sign by construction. That single fact reorders the candidates.

**The comparison (2026-08-08) found neither candidate is what Step 5 wants today:**

| | `ceda/utils/ras_balancing.py` | `nowcasting/sut_ras.py` |
|---|---|---|
| Repo | `cornerstone-data/ceda` (**private**), usually at `~/ceda` | `cornerstone-data/USEEIO`, branch `nowcasting` |
| Size / state | 879 lines, typed, 20 tests | 432 lines, untyped, no tests, magic epsilons |
| Algorithm | RAS / IPFP, **non-negative only** | **GRAS** — positive and negative parts scaled separately |
| Negatives | ⚠️ **clamps negative seed mass to zero** (`ras_balancing.py:574` dense / `:695` sparse — logged, and reported as an upstream regression — built for CEDA's non-negative U/Y) | survives them; `sign_flex` controls where a flip is permitted |
| Scope | **one matrix**, row + column targets | **the whole SUT** — `V`, `Ui`, `Ufd`, `Uva`, plus import layers `Umi`/`Umfd` |
| Targets | row/col vectors | vectors **and aggregate-level** targets via aggregator matrices (`Vagg_rows`, `G_va`, `G_fd`) |
| Engineering | dense + sparse, mask-aware, elementwise convergence, stall detection with projection of locally infeasible cuts, `close_rows_exactly`, per-margin diagnostics | fixed 1000 iterations, `1e-5` tolerance, no mask, no diagnostics |

Read that table as two different things rather than two versions of one thing. **ceda has the engine
engineering; `sut_ras` has the algorithm and the SUT orchestration.** The port question is which half
is cheaper to rebuild.

#### The architecture this actually needs — two layers

Keeping these separate is what makes the decision tractable:

1. **Engine** — take one matrix, a seed, and margin targets; scale to the targets. This is where
   RAS-vs-GRAS lives, and where ceda's mask/convergence/stall machinery lives. Small surface, fully
   unit-testable on hand-checkable matrices.
2. **SUT orchestration** — sequence the blocks (`V`, `Ui`, `Ufd`, `Uva`, imports), decide which
   identity each pass enforces, carry aggregate-level targets, and iterate to joint convergence. Only
   `sut_ras.py` has any of this, and it is the part with no tests.

The two decisions below can land differently at each layer — e.g. ceda's engine under an
orchestration layer written fresh against `sut_ras.py` as the reference.

#### Decision 1 — the starting point

| Option | What it costs | What it buys |
|---|---|---|
| **A. Start from ceda, add GRAS + a SUT layer** (current lean) | Add sign-split scaling to a codebase whose invariant is non-negativity — the clamp is load-bearing in places, not a one-line delete. Write the SUT layer from scratch. **Plus aggregate-level constraints, which Decision 3 shows are required and ceda's row/column-vector API cannot express.** | Typed, tested, diagnosable from day one. Mask-awareness and stall projection are exactly what a 402×402 detail balance with structural zeros needs, and rebuilding them is weeks. |
| **B. Start from `sut_ras.py`, harden it** | Type it, test it, replace magic epsilons, add masking and real convergence reporting — i.e. rebuild ceda's engineering around it. | GRAS, the SUT sequencing, **and aggregate-level targets** are already there, and they are the parts that are conceptually hard to get right rather than merely tedious. Decision 3 raises the value of this column. |
| **C. Fresh engine in bedrock, both as references** | Most upfront work; no inherited tests. | No inherited invariant fighting us, no private-repo or dependency-pin entanglement, and the objective function is a deliberate choice rather than an artifact. |

**Practical constraints that bear on this, all verified:**
- **bedrock has no `scipy` dependency** ([`pyproject.toml`](../../../pyproject.toml) — `pandas`,
  `numpy`, no `scipy`), and ceda's `ras_balancing` imports `scipy.sparse`. Option A means either
  adding scipy to bedrock or dropping the sparse path. At 402×402 dense, the sparse path may not be
  worth carrying.
- **ceda pins `numpy==1.26.4` / `pandas==2.2.2`; bedrock requires `numpy>=2.2.6` / `pandas>=2.3.0`.**
  Any editable install must be `--no-deps`, or use `PYTHONPATH`. This rules out depending on ceda as a
  package — a **vendored port** is the only sane form of Option A.
- **ceda is a private repo.** Fine for us; worth stating, because it constrains anything published.
- ceda's module is near free-standing (`ceda/__init__.py` and `ceda/utils/__init__.py` are 0 bytes,
  `ceda.utils.logging` is stdlib-only) and **ceda does not import bedrock**, so there is no cycle.

#### Decision 2 — the objective function

RAS is not one algorithm; it is a family distinguished by what is minimized and what is treated as
hard. Picking this **before** writing code is the point, because the engine's inner loop is a direct
expression of the choice, and the negatives question is decided here rather than patched later.

| Objective | Minimizes | Negatives | Notes |
|---|---|---|---|
| **RAS / IPFP** | cross-entropy (KL) to the seed | not admitted | What ceda implements. Cheapest, best-understood, wrong for our seed as-is. |
| **GRAS** (Junius–Oosterhaven 2003, corrected Lenzen et al. 2007) | generalized cross-entropy with positive and negative parts scaled by `r` and `1/r` | preserved, signs held unless flexed | What `sut_ras` implements. The natural fit for a seed with structural negatives; note the two published variants differ in the negative-part treatment and we should be explicit about which one we implement. |
| **KRAS** (Lenzen et al. 2009) | as GRAS, plus reliability-weighted constraint violation | preserved | Admits **inconsistent and general (e.g. aggregate, inequality) constraints** by making them soft with weights. Directly relevant once Decision 3 lands: a target set drawn from NIPA, GDP-by-industry and the trade accounts **will not be mutually consistent to the dollar**, and the seed comes from seven independent nowcast paths on top of that. |
| **Constrained least squares / QP** | weighted squared deviation from the seed | native | Handles bounds, sign constraints, and inequalities cleanly; needs an optimizer (scipy, or a hand-rolled projected solver) and scales worse. |

**The sub-questions that make this a real decision, not a label:**
- **Which constraints are hard and which are soft?** Commodity balance (`T016` = `T019`) is an
  accounting identity. Every *sourced* target is an estimate, from an account with its own vintage and
  its own revision schedule. Treating both kinds as hard is what makes a balance fail to converge;
  treating the second kind as soft, weighted by how much we trust each source, is most of what KRAS is
  for. Decision 3 supplies the list this applies to.
- **Where may a sign flip, and where may it not?** `sut_ras`'s `sign_flex` is the mechanism; the
  policy is ours to set, per block. `SUB` and the margin give-up side should almost certainly be
  sign-locked; `F03000` cells arguably should not be.
- **What is held fixed entirely?** A mask of cells the balancer may not touch (structural zeros; any
  block we consider directly measured rather than nowcast). ceda has this, `sut_ras` does not.
- **What does "converged" mean?** Elementwise per-margin tolerance (ceda's, and the right answer —
  a global `max` bound is meaningless across margins spanning six orders of magnitude), plus an
  `atol` floor, since several commodity targets are legitimately near zero.

#### Decision 3 — the target set

**Do not inherit "summary SUT totals" as the control.** It reads like a default because it is a
single, internally consistent, already-balanced object, but it is the wrong instrument for three
reasons: it is a *derived* product (BEA's own aggregation of the detail we are trying to estimate, so
controlling to it makes the nowcast reproduce BEA's aggregation rather than exploit the sources we
already extract); it **aggregates away the exact dimension we are estimating**; and it is **late**,
lagging the NIPA and GDP-by-industry releases that are the whole reason a nowcast is possible. Using
it wholesale would throw away the timeliness that justifies the project.

**Enumerate the targets margin by margin instead, and source each one deliberately.** The candidate
set, by where it binds:

| Margin | Candidate target | Source | Notes |
|---|---|---|---|
| Use — industry columns | total industry input = **gross industry output** | BEA **GDP-by-industry** gross output | Timely and annual. Published above detail for the nowcast years — see "at what level" below. |
| Supply — industry columns | same gross output vector | as above | Supply/Use column agreement is itself a constraint, and it is free once the vector is chosen. |
| Use — **FD columns** | **NIPA column totals, one per FD code** | PCE (`F01000`); fixed investment by equipment/IP/residential/nonres structures (`F02*`); `F03000` inventories per [`inventories_estimation_plan.md`](inventories_estimation_plan.md); exports `F04000` and imports `F05000` from the trade step / ITA; the twelve federal and S&L columns from the Section-3 government tables | **This is the strongest part of the target set** — the FD block is where NIPA is most current and most authoritative, and the columns map one-to-one onto SUT codes. |
| Use — VA rows | compensation, taxes-less-subsidies, GOS row totals | NIPA T1.14, `VABAS`→T10305, `T018`→T10105, Section-6 tables | Already specified for Step 2 (§Value added — fully specified on the board) — the same aggregates, reused as constraints rather than only as checks. |
| Supply — trailing columns | imports (`MCIF`/`MADJ`), duties (`MDTY`), `TOP`/`SUB`, margin totals | trade step, §`MDTY`, Step 4 | Each already has a sourcing decision in Step 4; Step 5 just has to say which of them binds. |
| Both — commodity rows | total supply = total use per commodity | *not sourced* | The identity being solved, not an exogenous target. Commodity gross output is a separate, optional target if we want one. |

**Three properties decide whether a candidate belongs in the set at all:**

1. **Is it observed, or is it ours?** A "target" we produced ourselves is not a constraint, it is a
   preference with extra steps. **Detail gross output for 2018-2024 is nowcast by Step 4a** — imposing
   it on the balance is circular. Only aggregates that enter from outside the model qualify. This is
   the sharpest edge in the whole decision, and it is why the honest answer differs between Phase 1 and
   Phase 2 (see below).
2. **At what level is it observed?** If gross output is published at summary and we balance at detail,
   the truthful constraint is *"these N detail industries sum to the published summary industry"* —
   **not** a detail vector we manufactured by applying 2017 shares. That means **aggregate-level
   constraints via aggregator matrices**, which is precisely `sut_ras`'s `Vagg`/`G_va`/`G_fd`
   machinery and precisely what ceda's row/column-vector engine **cannot express today**. ⚠️ **This
   feeds straight back into Decision 1**: it is a concrete, non-trivial capability that Option A has to
   build, and it moves the balance of that table.
3. **Hard or soft, and how trusted?** Decision 2's question, applied per row of the table above.
   NIPA FD totals, GDP-by-industry output and the trade accounts are three different accounts on three
   different vintages; they will not reconcile to the dollar, so a set held entirely hard is
   infeasible by construction.

⚠️ **Targets and tests are the same aggregates — spend each one only once.** The testing strategy
below lists T1.14, T10305, T10105, T1.1.5 line 14 and the Section-6 totals as *reconciliation tests*.
Anything promoted to a balance target passes those tests **by construction** and stops being evidence.
So Decision 3 must be made jointly with the testing strategy, and should **deliberately hold some
aggregates back, unimposed, as out-of-sample checks**. Name them when the decision is recorded.

**Phase 1 vs Phase 2 changes the answer, and that is fine.** For 2018-2024 the detail gross output is
nowcast, so the industry-column target can only be honestly imposed at the level GDP-by-industry
publishes. After the annual update, 2025 has **observed** detailed gross output (§What the annual
update unblocks) — so 2025 can carry a genuine detail-level industry constraint that the earlier years
cannot. The design consequence: **the target set must be per-year configuration, not a constant**, and
the engine must accept aggregate and detail constraints in the same run.

#### Recommendation to decide against

**Vendor ceda's engine + GRAS + a KRAS-style soft-constraint layer, aimed at a NIPA/GDP-by-industry
target set.** Concretely: vendor ceda's engine into `bedrock/utils/economic/`, replace its scaling
core with GRAS, drop the sparse path (and with it the scipy dependency) unless profiling says
otherwise, keep the mask/convergence/stall/diagnostics machinery intact, **add aggregate-level
constraint support** (Decision 3, property 2 — this is new work Option A does not inherit), and write
the SUT orchestration layer fresh with `sut_ras.py` as the specification. Target NIPA FD column totals
and GDP-by-industry gross output at their published level, hold them soft with per-source weights,
hold the commodity identity hard, and keep summary SUT **out of the target set and in the test set**.

This is a recommendation, not a decision. **Record the decision here, with its reasoning, before any
code is written**, and update the linked issues to match.

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
  eyeball, tests. ⚠️ **These overlap the Step 5 target set (Decision 3).** Any aggregate imposed as a
  balance constraint passes here by construction and is no longer evidence of anything. When Decision 3
  is recorded, name which aggregates stay **unimposed** so this section keeps some real out-of-sample
  content — and mark the rest as identities-by-construction rather than leaving them looking like
  passing tests.
- **Unit tests** for the balancer — small hand-checkable matrices, with **a negative-cell case and a
  sign-lock case in the first batch**, not added later: they are the whole reason Step 5 was rescoped.
  Whichever starting point wins, these tests are written against the objective function chosen in
  Decision 2, so they encode the decision rather than the implementation. Keep the classic silent
  failure in the batch too: a zero control total.
- **Unit tests** for the SUT/MUT FD code lists and the margin reassignment in 6b.
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
| 1 FD block | #504, #523, #526/#527/#528, #529/#530/#531 (rescoped — see §`F03000`), #547, **#575** (1d code list), **#576** (1f reconciliation) | — |
| 2 Value added | #535, #536, #537, #538 | — |
| 3 Intermediate | #497, #564, **#577** (agriculture), **#578** (government) | — |
| **4 Supply table** | **#570** (4a), **#571** (4c), **#579** (4b), **#580** (4d), **#581** (4e) | — |
| 5 RAS | **#588** (balancer, parent — *rescoped: no longer a `sut_ras` port; blocked on Step 5's three decisions*), **#591** (sub-issue — *rescoped: the target set is Decision 3, no longer an "optional" alternative control*) | ~~#589~~ (load_suts_from_r) and ~~#590~~ (check_balances) **closed not planned, 2026-08-09** — neither port is needed |
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

  **4c splits into two halves with different dependencies, and only the first is truly parallel.**
  BEA distributes margins on `T013` = `T007` + `MCIF` + `MADJ` (§Step 4c point 1), so:

  | half | needs | when |
  |---|---|---|
  | **Derive 2017 rates** per (buyer, commodity, margin type) from the published Margins table, and prove the two identities reproduce the 2017 Supply columns | nothing — the 2017 tables are already loaded | **start now, genuinely parallel** |
  | **Apply those rates** to a nowcast year | `T007` from **4a** (#570) *and* `MCIF`/`MADJ` from **4b** (#579) | after both |

  Doing the first half now is what de-risks the other three consumers, because it settles the rate
  structure and the identities without waiting on any other step. Applying rates to `T007` alone to
  avoid the 4b dependency is the tempting shortcut and is **wrong** — it drops the margin on imports.
- **P1** — code-space Phase 2 (retarget `FD_Gov`/`FD_Structures`/`FD_IP`, drop
  `map_fbs_sectors_to_model_schema`, roll out 2018-2024), which unblocks #576, Step 2 and Step 3.
  #574 may fall out of this rather than needing its own attribution work — diagnose first. Then #579,
  #580, #581 once the trade FBS path is safe. #573 any time; it is small and Step 5 needs it.

  **#530/#531 (`F03000`) moves up into P1 on the rescope.** Its phase 1 — consume `U50705BU1`, which
  is already extracted, plus the farm line — needs no new extract and no crosswalk, and it closes one
  of the eight whole columns currently reported as `miss` in
  [`progress_report.md`](progress_report.md). It is FBS-routed, so it sits behind the code-space fix
  like the rest of Step 1; the ~25-line trade crosswalk (phase 2) is not, and can be built in
  parallel alongside #610.
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
4. **The Step 5 balance — three coupled decisions, all open, all blocking.** Detail in §Step 5.
   The earlier form of this question ("summary SUT totals or gross output?") **presumed a default that
   is now struck**; see 4c.
   - 4a. **Starting point** — vendor and adapt ceda's `ras_balancing.py`, harden `sut_ras.py`, or
     write fresh with both as references? *Blocks all of Step 5.* Options, costs and the verified
     dependency constraints are in Decision 1; the lean is vendor-ceda + fresh SUT layer, though 4c
     pushes back on it.
   - 4b. **Objective function** — RAS/IPFP, GRAS (and which published variant), KRAS, or constrained
     least squares? With it: which constraints are hard vs soft, where signs may flip, what the mask
     holds fixed, and what counts as converged. Decision 2. **The negatives in bedrock's SUT are
     structural, so plain RAS is out; the rest is open.**
   - 4c. **Target set** — **do not assume summary SUT totals.** Build the set margin by margin from
     detailed gross industry output and NIPA column totals for the FD block, and decide at what level
     each is honestly observed (aggregate constraints, not manufactured detail vectors). Decision 3.
     Two consequences that reach outside Step 5: aggregate-level targets are a capability 4a's
     leading option lacks, and **every aggregate promoted to a target stops being available as a
     test** — so this must be settled jointly with the testing strategy.
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
- **Step 5 (RAS targets)** — ⚠️ **rewritten under Decision 3.** The earlier note here said the update
  carries a 2025 summary SUT/MUT (it does — confirmed), so 2025 could be "controlled the same way every
  other year is." That reasoning is retired along with the summary-SUT default. The live point is the
  opposite one: **2025 is the first year with *observed* detailed gross output**, so it is the first
  year that can carry a genuine detail-level industry-column constraint instead of an aggregate one
  imposed over nowcast detail. 2025 is therefore *better* constrained than 2018-2024, and the target
  set must be per-year configuration to express that.
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
