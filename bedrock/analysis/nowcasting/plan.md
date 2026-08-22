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
SUT Use table, purchaser price, BEA_2017_Detail schema, from the `NIPA_final_dom_uses_<year>` FBS methods. It
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

**Not in bedrock, to be ported:** commodity mix / intermediate nowcasting
(`CalculateIntermediateUseAndCommodityMix.R`, per #497). The Step 5 **engine**
is in [`bedrock/utils/economic/balance/gras.py`](../../utils/economic/balance/gras.py)
(`gras_balance`, GRAS: Lenzen, Wood and Gallego 2007 + Temurshoev, Miller and
Bouwmeester 2013). The SUT wrapper is
[`engine`](../../transform/iot/nowcast_sut_gras.py)
(`engine(free, residual, masks)` → `out.blocks`): Use then Supply, hard T1 and
T11–T17 exact. Soft T2/T7 blend once from the entry ``Z``; T4 is a
column-neutral closer. T6/T8/T9 whole-name defer when T12–T14 occupy a
slot. ``WEIGHTS`` are uncalibrated. Unit tests:
[`balance/__tests__/test_gras.py`](../../utils/economic/balance/__tests__/test_gras.py),
[`test_nowcast_sut_gras.py`](../../transform/iot/__tests__/test_nowcast_sut_gras.py),
[`test_full_nowcasting_sut_balance.py`](../../transform/iot/__tests__/test_full_nowcasting_sut_balance.py).
A **zero target is legal**; a **nonzero target on an empty free margin** raises.

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

**1A `NIPA_final_dom_uses`** carries `F01000` and the `F02*`/`F06*`/`F07*`/`F10*` columns.
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
| F04000 | Exports | ⏳ **own FBS — 1B.** 2017 overlay ([#528](https://github.com/cornerstone-data/bedrock/issues/528): [#617](https://github.com/cornerstone-data/bedrock/pull/617) [#618](https://github.com/cornerstone-data/bedrock/pull/618) [#622](https://github.com/cornerstone-data/bedrock/pull/622) [#623](https://github.com/cornerstone-data/bedrock/pull/623) [#638](https://github.com/cornerstone-data/bedrock/pull/638) [#642](https://github.com/cornerstone-data/bedrock/pull/642)) — Census FAS goods + IEA TypeOfService leaves on `BEA_detail_commodity_target.yaml`; `S00900` from −F010 + Supply T016. Overlay mass is mapped Trade Detail (`_trade_fbs_commodity_vector`). ITA G+S scale helper exists and is not applied (#647). 2018–2024 methods open. 2017 scorecard FAIL + inventory vs #557 bars. |
| ~~F05000~~ | ~~Imports~~ | **Not an SUT column** — belongs to Supply (`MCIF`/`MADJ`); appears only on MUT conversion |

### SUT Use — other blocks

| Block | Status |
|---|---|
| Value added (`V00100`, `T00OTOP`, `V00300`) | ⏳ 24 NIPA tables extracted and reconciled (#536); three FBS methods to build — Step 2 |
| Value added (`T00TOP`, `T00SUB`) by industry | ⏳ Built by commodity (Step 4d); the industry row is a *conversion*, and the market-share operator fails it (r=0.20) — **Step 5 solves the split**, seeded at r=0.95 by the Step 4c level split. Construction converts exactly; the residual is 20 named industries. See Step 2 |
| Intermediate (commodity × industry) | ❌ Method identified (#497), not built — Step 3 |

### SUT Supply — every column

**Every column now has a candidate.** `MCIF`, `MADJ` and `MDTY` are 2017-only; `T007`, `TRADE`, `TRANS`, `TOP` and `SUB` are multi-year. As of 2026-08-22 the 2017 block scores 99.0% coverage and 0.092% on the grand total, and all four subtotals are evaluable.

| Column | What it is | Candidate source | Status |
|---|---|---|---|
| cells / `T007` | Domestic output, commodity × industry, basic value | Nowcast gross industry output (`derived_gross_industry_output.py`) × commodity mix (port from `CalculateIntermediateUseAndCommodityMix.R`, #497) | ❌ **unsourced method** |
| `MCIF` | Imports, c.i.f. | Census goods **CIF** (`GEN_CIF_YR`) + BEA `IntlServTrade` — §Trade data below | ⏳ **2017 candidate** ([#528](https://github.com/cornerstone-data/bedrock/issues/528) / [#622](https://github.com/cornerstone-data/bedrock/pull/622) [#623](https://github.com/cornerstone-data/bedrock/pull/623) [#642](https://github.com/cornerstone-data/bedrock/pull/642)) — Trade_Imports FBS on `MCIF`; overlay is mapped Detail mass (ITA scale helper unused, #647) |
| `MADJ` | Import adjustment (c.i.f./f.o.b.) | Census `GEN_CHA_YR` mapped to Detail, reassigned onto 2017 Supply `MADJ` destination codes (signed shares), leveled to published Supply `MADJ` (`madj_detail_usd`) | ⏳ **2017 candidate** — charges + destination reassignment in `derive_initial_supply_bridge` |
| `MDTY` | Import duties | Effective duty rate from Census `CAL_DUT_YR ÷` customs value (NAICS-6, same endpoint as `MCIF`) × NIPA T30500 customs-duties level — §`MDTY` below | ⏳ **2017 candidate** — `mdty_detail_usd` in `derive_initial_supply_bridge` |
| `TRADE` | Wholesale + retail margins, by commodity | 2017 published column moved by the Census wholesale/retail gross margin (`nowcast_trade_margins.py`, #612/#613) — **not** an aggregate of the nowcast Margins dataset | ⏳ **2017-2023 candidate** — receiving split is the **frozen 2017 mix** |
| `TRANS` | Transportation margins, by commodity | Per-mode allocation of observed annual freight revenue (`Margins_Transport_<year>` FBS, #611) | ⏳ **2017-2022 candidate** — within-group weight frozen at 2017 (#672) |
| `TOP` | Taxes on products | NIPA T30500 less customs duties, annually; ten **named NIPA product lines** placed on their own commodities (29.8% of the column) and the sales-tax residual on frozen 2017 shares (`nowcast_product_taxes.py`, #580) | ⏳ **2017-2024 candidate** — total observed to $1M; residual split frozen |
| `SUB` | Subsidies (stored negative) | NIPA T31300 annually; each commodity anchored on its published 2017 value and moved by its own NIPA **type line**, with 2020-21 `other` on BEA's published PPP-by-industry allocation (`nowcast_subsidies.py`, #580) | ⏳ **2017-2024 candidate** — 2022-24 `other` still on the anchor ([#689](https://github.com/cornerstone-data/bedrock/issues/689)) |

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
   [#647](https://github.com/cornerstone-data/bedrock/issues/647) reopens whether nowcast should apply
   that ITA control. What remains on #528 is FAS vs PUR, import valuation overlap (CIF vs customs /
   `MADJ`), and `S00300` specials policy. IEA TypeOfService leaves are mapped; Crosswalk revisions
   sit in `bedrock/transform/trade/README.md`. `MDTY` rides on the same Census request (`CAL_DUT_YR`)
   — see §`MDTY`.
3. **Product taxes and subsidies — the split, on *both* axes** (`TOP`, `SUB`, and the Use table's
   `T00TOP`/`T00SUB` rows). NIPA gives totals (T30500, T31300); allocating them does not come for
   free.
   - **By commodity** (Supply `TOP`/`SUB`) — ✅ **both built (#580, step 4d above).** The default
     proposal was 2017 detail Supply shares held constant. `TOP` keeps it for 70.2% of the column
     and replaces it with NIPA's own named product lines for the other 29.8%. `SUB` **abandons it
     entirely** — it is the case the default breaks on, putting ~420bn of 2020 pandemic support onto
     housing — in favour of anchor-and-move per NIPA type, plus BEA's PPP-by-industry allocation for
     2020-21.
   - **By industry** (Use `T00TOP`/`T00SUB` rows) — ✅ **no longer a data gap, as of 2026-08-17.**
     Published detail gross output is at *producer* prices and the SUT column identity is at *basic*;
     the wedge is exactly `T00TOP − T00SUB` per industry (verified to $4M on $34T). Rather than assume
     a 2017 conversion ratio, **Step 5 targets producer prices and solves the industry split** — so
     this allocation is now an *output* of the balance, anchored only by the economy-wide T30500 /
     T31300 totals. Step 2 still supplies a seed. See Step 5 Decision 3 and
     [`target_set_plan.md`](target_set_plan.md) §4.

     ⚠️ **And the conversion is now measured, not just declined** (2026-08-22,
     [`tax_axis_conversion.py`](tax_axis_conversion.py)). The money is the same on both axes, so this
     was always a *transformation* rather than a second estimate — the question was only whether an
     operator reproduces the published row. The benchmark market-share matrix does not:
     **correlation 0.202** on `T00TOP`, absolute error 114.6% of the row, because **55.7% of the row
     sits in trade industries** and market shares place a product tax with the producer instead of the
     seller. Measured *in* the benchmark year, so this is not the drift objection — it is stronger.
     The seed's usable content is `4200ID` = `MDTY` exactly, plus a **margin**-based operator, still
     to be tested.
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
   trade industries to BEA commodities. ✅ **Built 2026-08-19 (#666)** — the crosswalk went to 54 NIPA
   lines over 260 commodities, and the column is live at −2.28% on its total. It comes off this list
   as a *sourcing* question; what is left is allocation accuracy (#660, #664, #665).
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
and Step 6c's import matrix. ITA G+S scale is [#647](https://github.com/cornerstone-data/bedrock/issues/647).
`S00300` hold-from-Supply and 2018–2024 Trade methods remain open on #528.

| Role | Source |
|---|---|
| Goods extract | Census International Trade, NAICS-6 — imports **CIF** (`GEN_CIF_YR`, to match Supply `MCIF`), exports FAS-family (`ALL_VAL_YR`) |
| Services extract | BEA `IntlServTrade`, by type of service, imports and exports |
| National totals control | BEA ITA Tables 2.1 (goods) / 3.1 (services) — loader `ita_gs_totals_usd` and `scale_amounts_to_ita` exist. Nowcast F040/MCIF overlay uses mapped Trade FBS Detail mass. Whether to apply a national ITA (or other) control is [#647](https://github.com/cornerstone-data/bedrock/issues/647). |
| Sector bridge | `Sector_Crosswalk_Census_USATrade.csv` (Census NAICS-6 → Detail); `Sector_Crosswalk_BEA_IEA.csv` (IntlServTrade TypeOfService leaves → Detail). Parents omitted when children are mapped. Methods include `BEA_detail_commodity_target.yaml`. |
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
identity and Supply `MCIF`; inventory in `bedrock/transform/trade/README.md`):

| | National % | Pearson all / non-`S00*` | Top-20 Jaccard all / non-`S00*` |
|---|---|---|---|
| F040 exports | +6.15% | 0.93 / 0.89 | 0.60 / 0.60 |
| MCIF imports | +1.29% | 0.62 / 0.84 | 0.67 / 0.74 |

F040 national miss is Census FAS goods vs SUT goods F040 (~+14%); mapped IEA leaves sit ~2% under SUT
service F040. Census + IEA `AllTypesOfService` tracks ITA G+S; SUT F040 is the lower frame. MCIF
national sits inside ~2–3% because `S00300` miss (260 B) offsets CIF goods and mapped-service
overshoot. Export Pearson on non-specials clears the #557 bar; import Pearson is just short of 0.85.

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
3. **Goods allocation surface mismatch (`#670`).** Large non-`MISS` errors remain in directly mapped
   goods families (notably `336*`), with very large absolute gaps despite zero `MISS` holes. This points
   to a likely mismatch between a NAICS-based Census goods Crosswalk and BEA's product-level foreign-trade
   allocation/reconciliation workflow. Tracked in [#670](https://github.com/cornerstone-data/bedrock/issues/670).

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
4. **Specials and margins policy.** `S00300` (noncomparable imports, ~260B) needs an explicit rule —
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
Use `score_2017_trade_detail_baseline.csv` as the pinned regression baseline for this gate and update it
only when scorecard movement is intentional.

## `F03000` — change in inventories, rescoped

Full treatment in [`inventories_estimation_plan.md`](inventories_estimation_plan.md), from three
emails from David Hill (BEA, National Economic Accounts, 2025-03/05), reproduced in #530's body.
**This section replaces the previous "deferred, needs ASM and Economic Census" position.**

✅ **Status 2026-08-19: built and wired (#666).** The scoping below held up — the trade branch is the
126% branch and it ran on the simplest rule, the crosswalk was the one gap and it was closable, and
farm did need its own level from NIPA. What the build added to the scoping is §Step 1C's three
follow-ons (#660, #664, #665) and the measurement that makes them rankable. The rest of this section
is the scoping as written, kept because it is what the follow-ons are still working from.

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
in `NIPA_final_dom_uses_2017.yaml`. **It also already publishes the stage-of-fabrication split** that Hill
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

⚠️ **Superseded — kept for provenance.** The `V00300` construction below is the one sketched on
the issue; `value_added_control_totals.py` replaced it with an assembly that closes to +13
million on 7.9 trillion. `T00TOP`/`T00SUB` are built by commodity in Step 4d instead, and their
industry row is a conversion Step 5 solves rather than a Step 2 source. Read §Step 2.

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

**1A — `NIPA_final_dom_uses`** (renamed from `NIPA_FD`; the old name promised a whole final-demand
block it never delivered). `F01000`, the four `F02*` investment columns, and the twelve
`F06*`/`F07*`/`F10*` government columns — the final *domestic* uses NIPA sources directly. Exports
are 1B because they are not domestic; inventories are 1C.
- ✅ PEQ Bridge / `F02E00`; PCE Bridge / `F01000`.
- ✅ F0-code assignment (`assign_sector_consumed_by_from_clean_parameter`, #539).
- ✅ **PCE and equipment reproduce their bridges cell for cell** — 259/259 and 107/107 commodities,
  zero cells off by >$1M (#630, #631).
- ✅ **All twelve government columns reproduce the Use table cell for cell** (#633, #634). `S00402`
  was unreachable in them because it was missing from *two* crosswalks — one supplying the
  attribution weight, one the receiving set — and fixing either alone changed nothing. F10E00 had
  been misallocating 32.8% of its column.
- ✅ **Renamed off `NIPA_FD`** — all eight yamls are `NIPA_final_dom_uses_<year>.yaml`. The
  `BEA_NIPA_FD_*` activity-to-sector crosswalks keep their names: they describe the mapping from
  `BEA_NIPA` activities to sectors, which is a different namespace from the method and is shared
  across all eight years.
- ✅ **Structures lines map to their own commodity** (#576) — the crosswalk had expanded ten lines
  into an 11- or 4-sector catch-all, putting $142B on the wrong commodity inside `F02R00` while the
  column total stayed right.
- ❌ Review all activity mappings; 2018–2024 have no PCE activity sets (#621); port the common yaml
  (#495).

**1B — Trade** (#526, #528). ⏳ `F04000` exports plus the Supply-side import columns from one
extraction — Census FAS goods + `BEA_IEA` `TypeOfService` leaves, mapped to BEA Detail, with
`S00900` from the identity −F010 + Supply `T016`. Built and wired for 2017 (#617, #618, #622, #623);
the same extract serves Step 4b. Open: ITA G+S scale ([#647](https://github.com/cornerstone-data/bedrock/issues/647)), FAS→PUR, and the 2018–2024 methods. The 2017
scorecard is **FAIL + inventory** against the #557 bars (`score_2017_trade_detail`; §Trade data).
✅ **Schema fix landed ([#638](https://github.com/cornerstone-data/bedrock/pull/638)).** The trade
yamls had declared `target_naics_year` where the code has read `target_schema_year` since #630, so they
raised `KeyError` — which took `derive_initial_Y_pur` down with them and blocked validating the NIPA
columns that have nothing to do with trade. #638 retargeted both methods onto the shared
`BEA_detail_commodity_target.yaml` (the same include `NIPA_final_dom_uses` uses), which supplies
the key, and retired `Trade_detail_passthrough.yaml` along with the
`activity_schema: NAICS_2017_Code` weight-source override. The end-to-end validation path is open,
and 1A's columns can be checked without a working trade leg.

**1C — Change in inventories** (#529, #530, #531). `F03000`, the full 402-row column, per
[`inventories_estimation_plan.md`](inventories_estimation_plan.md). **Not NIPA-total-only**: that
ships one scalar where the SUT needs a column, and leaves the allocation to Step 5's RAS, which has
neither a seed nor a sign-safe structure for a column that runs 3× gross to net across 61 negative
commodities. Built from `U50705BU1`, already extracted and unused, plus the farm line it omits
(≈ −5,679 in 2017, 17% of the total).

✅ **First pass landed 2026-08-19 (#666).** `Inventories_2017` generates, attributes across all five
branches, and is wired into `derive_initial_Y_pur` — `F03000` was a hardcoded all-zero column and is
now 256 commodities totalling 31,936 against a published 32,682 (−2.28%). Trade weights come from
`Census_EC_PxI` product lines, the same data BEA reaches for in its margin method; manufacturing
follows the finished-goods rule on the 22 industry leaves; mining and farm are equal-split
placeholders.

⚠️ **Read the column, not its total.** The total is the one free thing here — it equals NIPA CIPI by
construction — while gross mass is 3× net. Per commodity the first pass is **69.7% sign agreement and
101% absolute error against published gross**, so the column total says almost nothing. Three follow-ons
carry the remaining error, all measured rather than suspected: **#660** (mining and farm commodity
splits — EIA MER / USGS for mining, the FIWS crops-livestock split for farm), **#664** (manufacturing
needs per-industry stage shares; `336411` aircraft is −288 against a published −6,314), **#665**
(`S00402` at 380 against 3,969, because used-goods value sits in wholesale lines that route to
`S00401`).

⚠️ **The manufacturing commodity mix must move to the nowcast year's Supply table once Step 4a (#570)
builds one.** It is on the 2017 benchmark today. Freezing each industry's product composition at the
benchmark attributes later years with a mix that no longer holds, and it stops being visible once
buried in an allocation. Source change, not a method change.

**Five NIPA pseudo-codes were the recurring defect, and are now guarded.** `44X`, `336MV`, `336OT`,
`4521` and `4529` look like NAICS and are not, so `startswith` matching against holding industries
silently found nothing and the branch fell back to an equal split. `_assert_naics_are_real` fails the
build instead. Same silent-empty family as #635.

**Above the three — composition and validation.**
- ⏳ **Split the column list by framework** (#575) — ✅ done in substance: `derive_initial_Y_pur`
  targets `SUT_FINAL_DEMAND_CODES` (MUT FD minus `F05000`), and `F05000` is created in Step 6b from
  Supply-side imports rather than sourced as a Use column. What remains is a *composition* concern:
  the assembly of three methods' output still lives inside `derive_initial_Y_pur`, so a trade-side
  failure blocks validating NIPA columns that have nothing to do with trade. Pulling assembly out
  decouples them.
- ❌ **Validate per-column against published NIPA aggregates** (#576). Already satisfied for the
  government columns and for PCE/equipment against their bridges; `F03000` now reconciles at the
  column total but not per commodity; outstanding for `F04000` (blocked) and the two gaps below.

⚠️ **The board-level state of Step 1, as of 2026-08-19:** all 19 final-demand columns are sourced,
**no column is a whole-column `miss` any more**, and two are outside tolerance at the column total —
`F04000` (+6.15%) and `F03000` (−2.28%). Coverage against the published detail SUT is 95.5% and
accuracy 55.1%; the two moved in opposite directions because `F03000` added 256 cells that are right
in aggregate and mostly wrong per commodity. [`progress_report.md`](progress_report.md).

⚠️ **A catalog regression rode in with #666 and was caught by the diagnostic, not by a test.** The
`Census_EC_PxI` entry was inserted *inside* `BEA_PEQBridge` in `source_catalog.yaml`, taking
`BEA_PEQBridge`'s `activity_schema` with it; the PEQ bridge stopped being sector-like and the whole
`F02E00` column collapsed onto `S00402` — $986B of a $978B column on used and secondhand goods, with
every equipment commodity at zero. Fixed 2026-08-19. **Two lessons worth carrying**: a YAML entry
inserted at the wrong indentation silently steals its neighbour's keys, and duplicate keys in the
catalog resolve last-wins with no error — the `Census_EC_PxI` block carried `activity_schema` twice
and neither reader complained. Nothing in the test suite covers the catalog's block structure.

**Gaps carried.** `F02R00` is short `S00402` by 1,883 (#633), blocked behind #635 — a `ValueError`
in one activity_set discards every activity_set and returns an empty FBS as a valid result, so the
line cannot safely be given its own set. And `F02E00` matches the PEQ bridge exactly while
disagreeing with the Use table by ~$15B at the column total (#547) — a source-data question, not an
attribution one. ✅ **Which of the two is authoritative is now decided (2026-08-17): the Use table.**
The bridge is an *allocation device* — it says how a column distributes across commodities, not how
large the column is — and the object we are building is a SUT, so the column total has to be on the
Use table's basis. ⚠️ That settles *which*, not *how*: for 2018-2024 there is no detail Use table, so
#547 still has to establish which NIPA aggregate reproduces the Use-table basis rather than the
bridge basis, and explain the $15B. Until then `F02E00`'s nowcast target is off by ~1% of the column.

### Step 2 — SUT Use: value added block

⚠️ **Scope is three rows, not five.** `V00100`, `T00OTOP`, `V00300` — the `VABAS` components, which
is what `SUT_VALUE_ADDED_CODES` and the `use_va_detail_sut` diagnostic already target.

**`T00TOP`/`T00SUB` are a conversion question, not a sourcing one, and the conversion was measured.**
They are already built by commodity in Step 4d (#690), so the industry row is a *transformation* of
money that exists — nothing gets estimated twice. The only question is whether an available operator
reproduces the published industry row. ✅ **Tested 2026-08-22 for 2017, and the obvious operator
fails**: the benchmark market-share matrix from the Supply table gives correlation **0.202** on
`T00TOP` with an absolute error of **114.6% of the row**, and 0.676 / 79.8% on `T00SUB`
([`tax_axis_conversion.py`](tax_axis_conversion.py)).

⚠️ **The reason is structural, not noise: 55.7% of published `T00TOP` sits in wholesale and retail
industries**, because a tax on a product is remitted by whoever *sells* it and market shares place it
with whoever *makes* it. The pairs are stark — petroleum wholesalers `424700` are 88,362 published
against 13 estimated while refineries `324110` are 397 against 92,893; motor vehicle dealers `441000`
are 45,947 against 2,301 while assemblers `336111`+`336112` are 26 against 24,141. The whole tax moves
one stage up the chain.

⚠️ **And this is measured *in* the benchmark year, against the very table the mix comes from.** So it
is not the "2017 ratios drift" objection the 2026-08-17 decision was argued from — it is stronger.
A conversion that fails in 2017 cannot be rescued by being applied nearer to 2017.

So the decision above stands — the industry distribution stays free for Step 5 under economy-wide soft
targets — but *free* is not *unseeded*, and two pieces of structure are in hand:
- ✅ **`4200ID` takes `MDTY` exactly**: published `T00TOP` there is 38,513 against a Supply `MDTY` of
  38,507. Customs duties are a lookup, 5.1% of the row, in every year.
- ✅ **A usable operator exists, and Step 4c already built most of it.** `top_by_level` splits `TOP`
  per commodity into producer-level (325,829) and trade-level (391,096) from an identity with nothing
  modelled in it — excise sits in Producers' Value, sales tax inside the margin columns. That is
  exactly the producer-versus-seller distinction market shares get wrong, and its trade-level total
  lands within **+2.2%** of published wholesale-plus-retail `T00TOP`. Progression:

  | operator | corr | \|error\| |
  |---|---:|---:|
  | market share on all `TOP + MDTY` | 0.204 | 114.6% |
  | + level split, trade-level by trade output | 0.743 | 41.9% |
  | + motor fuel routed to `424700` by name | **0.946** | 29.9% |

  ⚠️ **Do we need to differentiate trade industries *within* wholesale and *within* retail? Only
  within wholesale, and only for one code.** Non-trade industries are 44.3% of the row and, once the
  producer-level portion is separated, plain market shares give **corr 0.987** on them. Within retail,
  output shares (which for a trade industry are very nearly margin shares) give 0.744 — retail product
  tax is general sales tax, broad-based, HHI 0.137. Within wholesale they give **−0.192**, because
  `424700` petroleum wholesalers takes **51.3% of wholesale product tax on 3.4% of wholesale output**:
  wholesale tax is motor fuel excise, not a broad-based tax.

  ✅ **And wholesale does not need a commodity × trade-industry matrix either — it needs one named
  routing.** `NAMED_TAX_LINES` already carries motor fuel as `324110` and `trade_level_share` already
  says that tax is 99.8% trade-level; sending it to `424700` takes the row to 0.946 and wholesale
  itself to 0.973. So **the general commodity × trade-industry margin matrix the PRO:PUR
  producer-price work will need is not required here.** Still a seed, not a target — 29.9% error.
- `T00SUB`'s residual is two named structures rather than a smear — government enterprises `S00203`
  (19,471 published vs 1,964) and `S00102` (6,339 vs 102) are subsidies paid to an *operator*, so no
  product-side operator can place them. `T30800` already carries them.
- ✅ **Construction needs none of this — it converts on plain market shares, corr 1.000, error 1.7%.**
  Commodity `TOP` is 1,907 against a published `T00TOP` of 1,857, and `MDTY` and `SUB` are zero on both
  axes. Probed because construction is block-shaped like the trade industries and could have been a
  second petroleum; it is the opposite, because BEA defines the construction industries *by type of
  structure*, so the Make block is 94.5% in-block and **100.0% diagonal** — no producer-versus-seller
  distinction exists when whoever builds the structure sells it. `top_by_level` agrees: 100% of
  construction `TOP` is producer-level, so the trade routings are inert here. Tax sits on 3 of the 12
  codes, none of them a `NAMED_TAX_LINES` entry. The residual 1.7% is a leak to own-account and
  secondary construction (5.5% of the block's output, largely `531HST` and state and local government);
  market shares leak 30.4 where the published row leaks 50 — right direction, about half the size.

⚠️ **Do the remaining sectors each need the same probe? No — the residual error is 20 industries, not
402.** Under the best operator the top 20 industries by absolute error carry **80.3%** of it and the top
5 carry 35.1%, and 17 of those 20 are wholesale or retail:

| block | published | share of row | share of the error |
|---|---:|---:|---:|
| wholesale | 172,194 | 22.8% | 41.9% |
| retail | 210,297 | 27.8% | 33.7% |
| non-trade | 334,447 | 44.3% | 24.4% |
| `4200ID` customs | 38,513 | 5.1% | 0.0% |

What is left is the within-trade allocation already characterised, plus five named non-trade
structures — `721000` accommodation (lodging tax, −6,592), `517210` wireless (−4,310), `221100`
electric power (−3,934), and government enterprises handed tax they do not carry (`S00202` +3,439
against a published zero, `GSLGE` +1,698, both of which belong to the Step 7 reallocation). Construction
was the last block-shaped unknown worth a sweep of its own and it came back exact, so **build against
this seed and repair the named twenty later**: Step 5's balance moves these cells under soft targets
anyway, so seed accuracy below the block level is not what the build is waiting on.

✅ **Decided 2026-08-22 — three FBS methods, one per Use row**, rather than one method or plain Python
modules. Reasoning, and the evidence behind it:

- **FBS, not Python.** Step 4's modules are arithmetic on published matrices; Step 2 is genuinely
  NIPA-leaf → BEA-detail attribution with weights, which is the engine's job. The plumbing already
  exists: `BEA_Detail_Use_SUT` melts the Use SUT through `VAPRO`, so the **2017 benchmark VA block by
  industry is loadable as an FBA attribution source today** (existing methods deliberately exclude
  those rows). No new extractor, and "frozen 2017 detail shares under an annual NIPA control" — which
  is all `T00OTOP` can honestly be — becomes a single proportional attribution.
- **Three, not one.** The rows have different quality bars: `V00100` carries a **hard** group-level
  constraint in Step 5's target set, while `V00300` and `T00OTOP` are **seeds only**, deliberately
  unimposed so the income side stays out of sample. `V00300` needs ~20 activity sets across 8 NIPA
  tables plus the housing/farm/government lookups; coupling that churn to the one row Step 5 actually
  constrains is the mistake Step 1 paid for before the 1A/1B/1C rescope.
- **The one thing FBS cannot say** is compensation's anchor-and-move construction (2017 detail share ×
  QCEW growth, renormalised in-parent, then the control). `multiplication` does not preserve the group
  total, so the exact-control step has no primitive. Build the moved-share vector as a cached
  `FBS_outside_flowsa` source via `FBS_datapull_fxn` and let the yaml do **one** proportional
  attribution against it — proportional normalises within group, so the control holds by construction,
  and the arithmetic stays readable.

✅ **#536 done — 24 NIPA tables added to `BEA_NIPA.yaml`**, all annual and complete for 2012-2024, at
~950 extra FBA rows per year. Reconciliation through the FBA is pinned by
`extract/bea/__tests__/test_bea_nipa_value_added_tables.py`. Three findings changed the plan:

- ⚠️ **Take the *paid* line, not the table's root.** 6.2D and 6.3D each state their total twice —
  line 1 received by residents, line 2 paid by domestic industries and government. Value added wants
  line 2. On it, wages + both supplements close to **0** against compensation (8,485,016 + 604,656 +
  1,345,306 = 10,434,978) and land on the SUT's `V00100` within 3. Reading line 1 is what left the
  ~10,600 that `compensation_disaggregation_plan.md` carried as an open item; it is the rest-of-world
  adjustment `A4187C`, stated in 6.2D's own lines 97-99.
- ⚠️ **6.11D is three panels under one code**, and only the first is by industry: lines 1-20 industry,
  22-36 type of fund, 37-45 *benefits paid* (2,370,770, a different concept). Its industry grain is
  **17, not 36**, and selecting the whole table double-counts — the U20405 memorandum-block hazard
  again. `T61600D`, `T71100` and `T11400` also restate a code; select by line in all four.
- ✅ **3.8 gives the government-enterprise surplus an industry axis** it was thought not to have,
  federal and state-and-local summing to 1.10's `A108RC` exactly.

Remaining:
- Add identity crosswalk rows for the three codes (`BEA_2017_Code`, as `F01000` has). The #567/#568
  gate is **closed**, so this is three CSV lines rather than a decision.
- A `assign_sector_produced_by_from_clean_parameter` mirror: VA codes are Use *rows*, so the code goes
  on `SectorProducedBy` and the industry on `SectorConsumedBy` — the transpose of the FD methods.
- Reconcile against T1.14 / `VABAS`→T10305 / `VAPRO`→T10105, per §Testing strategy.
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

  **Full treatment in [`output_estimation_plan.md`](output_estimation_plan.md)**, with BEA's own
  per-industry source table at [`output_estimation_sources.csv`](output_estimation_sources.csv).

  ✅ **The manufacturing half now runs as a method.**
  `transform/commodity_output/Commodity_output_manufacturing_<year>.yaml` builds 236 BEA detail
  commodities per year for 2018-2021 from Annual Survey of Manufactures product data, at basic
  prices, scoring 0.94-0.97 of level and 4.5-6.8% weighted error against published summary `T007`.
  Still outstanding for 4a: mining `q` (no annual product survey exists), the services half
  (industry output × mix), 2022-2024, and the rebalance to the summary control.

  ⚠️ **The estimand is *commodity* output, not industry output.** `T007`'s column sums are commodity
  output; industry output is an input to the construction, not the deliverable. That admits two
  routes, and the manual says BEA uses different ones by industry: industry output × commodity mix
  where only industry receipts are collected (most services), but for mining and manufacturing
  *"the calculation of commodity output starts with [product] data rather than the industry data"*
  (ch. 5 p. 79). Applying one route to both regimes imposes the services method on manufacturing,
  where observed product data exists and is better. Score both against published 2017 per cell.

  ⚠️ **The target is output *before* redefinitions, and this has to be settled before anything is
  built.** The BEA IO manual's chapter 5 walks each industry from source data down to two different
  lines, and names them: the *featured* measure is **NAICS industry output before redefinitions**,
  "more useful for comparisons with the economic statistics from other sources", while output after
  redefinitions "is generally referred to as **I-O industry output**" and is "generally not as
  comparable with other economic statistics" (p. 78). The SUT framework is aligned with the former.
  Our own file list already says so: the tables we target are `Supply_2017_DET.xlsx` and
  `Margins_Before_Redefinitions_2017_DET.xlsx`, and the only `After_Redefinitions` files in
  [`matrix_mappings.py`](../../utils/taxonomy/bea/matrix_mappings.py) are the legacy summary
  Make/Use series — a different family, not our target. **So we do not perform the redefinitions BEA
  performs.**

  **No totals check can catch this**, which is why it is stated here rather than left to validation.
  Redefinition moves money between cells while preserving every total:
  [`About_BEA_IOT_table_valuation_differences.md`](compare_NIPA_to_IOT/About_BEA_IOT_table_valuation_differences.md)
  measured **553,635 million moving gross across 5,740 cells for a net of −7**. A benchmark replay
  that scores on totals would pass with the wrong version throughout.

  **What that means for the manual's recipe.** Chapter 5's worked tables (5.1 cheese, 5.2 telecom)
  run *through* the line we want to the line we do not. Take the industry column down to
  `NAICS OUTPUT`, not `I-O OUTPUT`. On the commodity side, do not apply `Redefinitions out` — but
  **do** apply `Reclassifications` and the make-table adjustment, since neither is a redefinition:
  reclassification is BEA disagreeing with Census about what is primary, and the make-table
  adjustment absorbs product-vs-industry source inconsistency. Both survive the choice of version.

  Chapter 5 is PDF pages 69–92 of the 2009 IO manual (narrative to p. 83, then table 5.A's worked
  per-industry source and calculation summaries) — the closest thing to a per-industry recipe for
  what external series each industry's output is built from. ⚠️ It documents the **1997** benchmark
  and says so in its preface, so it is authoritative on concepts and structure only; every named
  source needs checking against current Census products, since NAPCS landed in the 2007 Economic
  Census and changed what product data exists at all. For the between-benchmark years the current
  list is in [BEA's 2023 comprehensive-update preview](https://apps.bea.gov/scb/issues/2023/06-june/0623-nea-preview.htm):
  ASM, the Annual Surveys of Wholesale and of Retail Trade, SAS, Value of Construction Put in Place,
  QCEW, IRS corporate and partnership tabulations, and USDA farm statistics.

  ⏳ **Test `Census_EC_PxI` as the commodity-mix source before settling on a ported 2017 mix.**
  Economic Census *Products by Industry* is an **observed** industry × product matrix — which is what
  this block is — where a ported mix holds the benchmark year's proportions fixed. It is extracted as
  of #529 (`census_ec_pxi_port`), 2017 today and 2022 live on the API as `ecnnapcsprd`, so a mix that
  actually moves between benchmarks is in reach rather than hypothetical.

  ⚠️ **Expect it to behave here even though it failed for the inventories trade weights**, and the
  reason is the point of the test. That failure was specific: NAPCS *trade* lines describe resale, so
  composing NAPCS → NAICS → BEA returns the industries that **sell** a good rather than the good
  itself — "Wholesale sales of refined petroleum products" resolves to construction commodities. For
  4a we want what industries **produce**, which is the manufacturing case, where the same composition
  was 35% one-to-one. The asymmetry that disqualifies it for trade is what recommends it here.

  **The test is decisive and cheap**, because the answer is published: reproduce the 2017 detail
  Make/Supply commodity mix per industry from `Census_EC_PxI` and score it per cell, the same way
  Step 1's columns were scored. Do this *before* porting the #497 mix, not after.

  Three known limits to measure rather than assume:
  - **Economic Census coverage.** No agriculture, no government, and parts of services are out of
    scope, so this can at best supply the mix for the sectors it covers and the ported mix has to
    carry the rest. Quantify the covered share of `T007` first.
  - **Suppression.** Recoverable — `estimate_suppressed_ec_pxi` takes the published detail from 90.5%
    to 100.0% of control on 2017 — but 12,929 cells are equal-split placeholders, and a commodity mix
    is exactly the sort of per-cell use where that matters. Read `SuppressionRecovery` and check
    whether the mix is stable when split cells are excluded.
  - **NAPCS → I-O commodity.** Still the missing link (#615). The concordance being built for
    inventories serves this too, which is an argument for building it once, properly, rather than per
    use.

  The same source and the same concordance feed **Step 3**'s intermediate input mix and two of BEA's
  four inventories rules, so the porting cost is shared three ways.
- 4b. **Import columns** — 2017 `MCIF` is the Trade Imports FBS (Census **CIF** goods + IEA services,
  Detail Crosswalk). Keep CIF because `MCIF` is the CIF target (2017: Use `|F05000|` / `MCIF` = 0.991).
  ITA scale is [#647](https://github.com/cornerstone-data/bedrock/issues/647); `S00300` residual and later years stay on #528. Shares the extract with Step 1d.
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

  1. **Apply rates to supply including imports, not to domestic output.** *"Total product supply
     (column OR) includes total commodity output plus imports; margins are distributed based on the
     value in column OR."* Imported goods carry domestic margin, so a rate carried on `T007` alone is
     biased — and the 2009 manual says the same twice independently, using "interim supply (output +
     imports)" as the weight in both the wholesale step (4) and the transport step (2). **This is a
     real sequencing constraint — see below.**
     ⚠️ **`T013` is the wrong base by a hair, and 4d is a prerequisite.** Measured in
     [`margins_estimation_plan.md`](margins_estimation_plan.md) §Phase 1: the Margins table's
     `Producers' Value` is a **producer**-value object (`Σ_buyers` = `T013` + `T015` to −0.0013%,
     against `T013` alone at +1.9%), and per commodity it is producer value *less* the trade-level tax
     that rides inside the margin columns. The base to rebuild is
     `T013 + MDTY + SUB + producer-level TOP` — 377 of 378 receiving commodities within 1% against
     293 for `T013` and 201 for full producer value. So **4c's application phase needs `TOP`/`SUB`
     from 4d**, and only the producer-level slice of `TOP`: adding all of it double-counts the
     trade-level share already inside the rates, by 19% on petroleum refineries and 36% on tobacco.
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
- 4d. **Tax/subsidy columns** (`TOP`, `SUB`) — NIPA T30500/T31300 totals by commodity.
  Remember `SUB` is negative here and positive in the Use table.
  ⚠️ **Not last, and not optional to 4c**: 4c's application phase needs `TOP` and `SUB` for its base
  (point 1 above), so 4d runs before 4c's second half rather than after the margin columns. It also
  needs to carry `TOP`'s **producer-level vs trade-level split**, not just the total — #610 measured
  the 2017 split per commodity (9.6% producer-level over the 203 margin-bearing commodities with
  `TOP` > 100 million), and only the producer-level part belongs in the margin base.
  - ✅ **`TOP` is built, 2017-2024** (`bedrock/transform/iot/nowcast_product_taxes.py`, #580). The
    plan said "split by 2017 commodity shares"; the build **beats that on 29.8% of the column and
    keeps it on the other 70.2%**, which is the split that matters:
    - The **total is observed, not estimated** — `(LA000236 + LA000238) − B235RC`, 716,925 against a
      published 716,926. ⚠️ Customs duties must be netted or `MDTY`'s 38,513 is counted twice.
    - **Ten named NIPA product lines** (motor fuel, alcohol, tobacco, air transport, ACA health
      insurance fee, medical devices, pharmaceutical, insurance receipts, public utilities,
      severance) are placed on their own commodities and move at their *own* annual NIPA value.
      They diverge sharply from the column: tobacco 0.33x federal / 0.78x state by 2024 against a
      column at 1.42x, air transport 0.33x in 2020, severance 2.99x in 2022. The two constructions
      differ by **3.7% of the 2020 column and 5.8% of the 2024 one** — 26,210 on tobacco in 2024,
      17,602 on `5241XX` in 2020.
    - **The remaining 70.2% is the frozen 2017 share vector**, deliberately. Two movers were built
      and both rejected on measurement: a service-side-only `T007` mover drifts to 1.86x against a
      1.53x control because the goods side absorbs the renormalisation, and an all-commodity `T007`
      mover cannot move `S00402` (15,699 of 2017 `TOP`, eighth largest) because its `T007` is zero
      by definition. The right base is purchaser value by commodity, which is Step 5's output.
    - The **producer/trade split** is carried by `top_by_level`, freezing each commodity's 2017
      trade-level share (`Wholesale + Retail − TRADE`, 391,162). ⚠️ Frozen per *commodity*, not per
      tax line — carrying it per line would move ~0.9% of the column and needs clipping.
  - ✅ **`SUB` is built, 2017-2024** (`bedrock/transform/iot/nowcast_subsidies.py`, #580), and it
    is **not `TOP` with a sign flipped**:
    - NIPA's type lines **partition** the total rather than covering a third of it, so the
      construction is anchor-and-move per type — each commodity keeps its published 2017 value and
      moves at its own type's NIPA growth. 2017 replays to under $1M per commodity.
    - ⚠️ **The type lines carry most of the pandemic signal on their own**: air carriers 238 → 19,966
      in 2020 (**84x**, payroll support), agricultural **4.0x**, housing only 1.23x.
    - ⚠️ **2020-21 `other` is 84% of the column and the anchor is worthless there** — the eight
      anchored `other` commodities are 64% insurance carriers, so moving them would put ~377bn of PPP
      on `5241XX`, as against ~420bn on housing from freezing the whole column. Those two years use
      **BEA's own published PPP-by-industry allocation** (2023 Comprehensive Update, 19 sectors,
      cached to GCS), split within sector by `T007`.
    - ⚠️ **PPP is 76% of the 2020 line and only 45% of the 2021 one.** The remainder — ERC, Provider
      Relief, Restaurant Revitalization, Shuttered Venue — is assumed to distribute like PPP.
      [#689](https://github.com/cornerstone-data/bedrock/issues/689) sources it from USAspending,
      where the NAICS axis returns empty for assistance awards but the CFDA axis names each
      programme. **2022-24 `other` is still on the anchor** and is #689's too.
- 4e. **Verify the four Supply identities per commodity** (`T013`/`T014`/`T015`/`T016` above), 402/402,
  before declaring the Supply table done.

### Step 5 — Balance the SUT (RAS)

**⚠️ Three decisions structured this step.** The **starting point** (Decision 1,
Option A for the engine: vendored ceda dense + GRAS), the **objective function**
(Decision 2), and the **target set** (Decision 3) are recorded below. The
Use-then-Supply wrapper `engine` is in
[`nowcast_sut_gras.py`](../../transform/iot/nowcast_sut_gras.py) (hard T1,
T11–T17 exact; soft T2/T4/T7 imposed; T6/T8/T9 deferred to T12–T14). The
soft **mechanism** has landed; **WEIGHTS are uncalibrated** — do not treat
the full Step 5 balancer as complete. What follows replaces the earlier
one-line instruction to port `sut_ras.py`; that is no longer the recommended
route.

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
2. **SUT orchestration** — sequence the two panels (Use then Supply), decide which
   identity each pass enforces, and iterate to joint T11 convergence. Soft
   T2/T4/T7 are imposed around the same kernel (blend-once; T4 closer). T6/T8/T9
   whole-name defer when T12–T14 occupy a slot. Historical
   `sut_ras.py` sequenced `V`/`Ui`/`Ufd`/`Uva`; that script is the comparison
   table below, not `engine()`.

The two decisions below can land differently at each layer — e.g. ceda's engine under an
orchestration layer written fresh against `sut_ras.py` as the reference.

#### The scaffolding is built — how to run it (#653, #654, #591)

**For anyone calling the SUT wrapper.** Everything below the ndarray kernel
is built — mask, targets, offset, precheck in [#659](https://github.com/cornerstone-data/bedrock/pull/659),
`gras_balance` in [`gras.py`](../../utils/economic/balance/gras.py), and
`engine` in [`nowcast_sut_gras.py`](../../transform/iot/nowcast_sut_gras.py).
The soft mechanism (T2/T4/T7; T6/T8/T9 deferred) has landed.
**What remains of Step 5 is calibrating `WEIGHTS` and wiring real NIPA/ITA
values** — not a second scaler.

| Module | What it is |
|---|---|
| [`bedrock/utils/economic/balance/`](../../utils/economic/balance/) | generic scaffolding (`Target`, `SutMask`, offset, precheck) and the ndarray GRAS kernel (`gras_balance`) |
| [`transform/iot/nowcast_sut_gras.py`](../../transform/iot/nowcast_sut_gras.py) | nowcast SUT adapter — Use then Supply, hard T1 and T11–T17; soft T2/T4/T7 |
| [`transform/iot/nowcast_mask.py`](../../transform/iot/nowcast_mask.py) | the mask sourcing — Tiers 0/1/3/4, and the panel labels |
| [`transform/iot/nowcast_targets.py`](../../transform/iot/nowcast_targets.py) | the target set — T1 through T17 |

**The wrapper's job is SUT orchestration.** Offset has already peeled frozen
mass (`X = F + Z`) and residualised the targets. `engine` turns
`free` / `residual` / `masks` into per-block `gras_balance` calls: extract
ndarrays, map `free_mask = mask.free` and `sign_flex = (sign_lock == 0)`, and
handle cross-block identities (T11–T17) by choosing which kernel vectors to
pass. T11 is imposed only on **live** rows of the panel being scaled;
empty-free T11 slots hold (a frozen Use commodity closes on Supply).
It does not re-implement GRAS, hold fixed values, or renormalise signs.
Soft T2/T7 write unconstrained kernel slots from an entry-`Z` blend; T4
runs after every Use pass as a column-neutral closer (T1 stays exact when
`close_rows_on_last=False`). T6/T8/T9 whole-name defer when hard T12–T14
occupy any of their slots. Two panels, Use then
Supply — not `sut_ras` `V`/`Ui`/`Ufd`/`Uva`.

```python
from bedrock.transform.iot.nowcast_mask import BLOCKS, build_sut_masks, published_2017_panel
from bedrock.transform.iot.nowcast_sut_gras import engine
from bedrock.transform.iot.nowcast_targets import build_target_set
from bedrock.utils.economic.balance import (
    offset_targets, precheck, restore_fixed_blocks, split_fixed_blocks,
)

seeds   = {block: published_2017_panel(block) for block in BLOCKS}   # the 2017 replay seed
masks   = build_sut_masks(2017)
targets = build_target_set(2017)

frozen, free = split_fixed_blocks(seeds, masks)      # X = F + Z
residual     = offset_targets(targets, frozen)       # r' = r - F @ 1
precheck(seeds, masks, targets, allow_placeholders=True)
out    = engine(free, residual, masks)
result = restore_fixed_blocks(out.blocks, frozen)    # fixed cells come back bit-identical
```

The **kernel** this package already has, once a wrapper (or a test) has extracted one block's vectors:

```python
result = gras_balance(
    matrix=Z.to_numpy(),
    row_targets=row_targets,
    col_targets=col_targets,
    free_mask=mask.free.to_numpy(),                 # not (Z != 0)
    sign_flex=(mask.sign_lock.to_numpy() == 0),     # must pass; kernel default is not SutMask
)
```

Kernel `sign_flex is None` → all-False (no cell may change sign). A default `SutMask`
(`sign_lock` 0) means flex **is** allowed. Omitting `sign_flex` in a later adapter silently
sign-locks the whole SUT. Do not write `gras_balance(free, residual, masks)`.

`seeds` and `masks` are **mappings of block name to frame** (`'use'`, `'supply'`), because a target
may relate the two panels — `T016 = T019` and the product-tax identities all do.

**Two things to inspect first:**

```bash
# what the target set contains, and which values are real
uv run python -c "from bedrock.transform.iot.nowcast_targets import target_set_summary; \
print(target_set_summary(2017).to_string())"

# whether the published 2017 tables satisfy every hard constraint
uv run python -c "from bedrock.transform.iot.nowcast_targets import hard_target_residuals; \
print(hard_target_residuals(2017).to_string())"

# what the mask freezes, in cells and in dollars
uv run python -c "from bedrock.transform.iot.nowcast_mask import mask_summary; \
print(mask_summary().to_string())"

uv run pytest bedrock/utils/economic/balance/ bedrock/transform/iot/__tests__/ -q
```

The residual check is the useful one: on the published 2017 tables every hard constraint holds to
BEA's $1M publication rounding, worst **21 on a $34 trillion table**. If an engine change breaks a
constraint definition, that number moves and nothing else in the pipeline would say so.

⚠️ **Values are part real, part placeholder — shapes are not.** T1 (gross output) and T11-T17 (the
identities) carry real values. T2, T4 and T6-T9 carry `PLACEHOLDER:`-prefixed sources while the NIPA
and ITA reads are wired, and `precheck` **refuses to certify a set containing one** unless
`allow_placeholders=True` is passed. Their shapes, labels and aggregators are correct, so an engine
built against this set does not change when Steps 1-4 land — **only values do.**

⚠️ **Three properties the engine must not break**, all of which fail silently
([`mask_layer_plan.md`](mask_layer_plan.md) §2):

- **a fixed cell is held at its value, not zeroed** — the offset guarantees it, but an engine that
  re-clamps will undo it;
- **targets keep their sign** — residual targets go negative even where the published target was
  positive, and `F03000` is −37,568 outright in 2020;
- **`F` is excluded from the seed** — pass `free`, never `seeds`; `assert_free_seed` is the guard.

#### Decision 1 — the starting point

| Option | What it costs | What it buys |
|---|---|---|
| **A. Start from ceda, add GRAS + a SUT layer** (**chosen for the engine**) | Sign-split scaling; SUT layer from scratch (later PR). Aggregate-level constraints stay in the wrapper. | Typed, tested, diagnosable. `gras_balance` is this slice. |
| **B. Start from `sut_ras.py`, harden it** | Type it, test it, replace magic epsilons, add masking and real convergence reporting — i.e. rebuild ceda's engineering around it. | GRAS, the SUT sequencing, **and aggregate-level targets** are already there, and they are the parts that are conceptually hard to get right rather than merely tedious. Decision 3 raises the value of this column. |
| **C. Fresh engine in bedrock, both as references** | Most upfront work; no inherited tests. | No inherited invariant fighting us, no private-repo or dependency-pin entanglement, and the objective function is a deliberate choice rather than an artifact. |

⚠️ **Both capabilities Option A was buying have now failed to hold up
(2026-08-17).** Measured in [`mask_layer_plan.md`](mask_layer_plan.md):

- **ceda's mask is the wrong kind of mask.** `free_mask` is a *participation*
  mask — `masked = np.where(mask, matrix, 0.0)` ([`ras_balancing.py:573`](../../../../ceda/ceda/utils/ras_balancing.py#L573)) zeroes non-free
  cells, so a **fixed nonzero value cannot be expressed at all**. Two further
  blockers in the same ten lines: `np.maximum(row/col_targets, 0.0)` at `:570-571`
  destroys a negative column target (`F03000` is −37,568 in 2020), and
  `np.maximum(masked, 0.0)` at `:579` clamps negative seed mass. These are
  load-bearing invariants of a non-negative codebase, not lines to delete.
- **Aggregate-level constraints are still required** — for the value-added rows
  rather than for gross output (Decision 3, revised). ceda's row/column-vector
  API still cannot express them.

The good news is that the mask itself is **engine-agnostic and cheap**: the
offset method (split `X = F + Z`, balance `Z` against residual targets `r − F·1`,
`c − 1ᵀF`, `A − R·F·Cᵀ`, add `F` back) reduces a fixed-value mask to a
participation mask under any engine. ~20 lines. But residual targets can change
sign, which RAS cannot carry and GRAS can — so this lands on Decision 2 as well.

**What ceda still genuinely offers** is convergence, stall projection and
diagnostics. That is a smaller prize than the table above assumed.

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
| **GRAS** (Junius–Oosterhaven 2003, corrected Lenzen et al. 2007; Temurshoev et al. 2013 all-negative margins) | generalized cross-entropy with positive and negative parts scaled by `r` and `1/r` | preserved, signs held unless flexed | **Implemented** in `gras_balance`. Lenzen 2007 + Temurshoev 2013; 2003 is the name only. |
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
- **What is held fixed entirely?** ✅ **Settled 2026-08-17** —
  [`mask_layer_plan.md`](mask_layer_plan.md), measured by
  [`mask_layer_feasibility.py --check`](mask_layer_feasibility.py). Three layers,
  kept separate because conflating them is what made the mask look free:
  **structural zeros** (a pattern; both engines get it for free), **fixed values**
  (the mask proper; neither engine has it, but the offset method supplies it), and
  **sign locks** (`sign_flex`, not a mask).
  **The rule: mask a cell only if the source reports *that cell*; if it reports
  the margin, it is a target. Never both.** So PCE and equipment are *not* masked
  — they reproduce their BEA bridges cell for cell, but the bridge is a 2017
  commodity split applied to a current-year NIPA line, and that split is exactly
  what the balance exists to correct. Only the six 1:1 NIPA-line-to-commodity
  columns are masked: `F06C00 F07C00 F10C00 F06N00 F07N00 F10N00` — **17 cells,
  5.1% of the Use panel's mass.**
  ⚠️ Freezing whole blocks is not affordable, and the cell count hides it: the FD
  block is **2.7% of the Use panel's nonzero cells and 39.9% of its dollars**;
  FD + VA is 74.2%. Freeze FD and 27 commodity rows lose all Use-side freedom and
  51 more exceed 10× leverage — but 26 of the 27 are absorbed 1:1 by their Supply
  row, so the real cost is that **the balance silently relocates onto the Supply
  table** for housing, government, health, education and construction.
  `S00900` (0.9% joint freedom) and `4200ID` are held out of the balance rather
  than masked — but ⚠️ **on the commodity axis only.** `4200ID` is customs
  duties and stays an *industry*: its column carries `T00TOP` = `VAPRO` =
  38,513, which is the Supply `MDTY` total and its published gross output.
  Corrected 2026-08-17; see [`mask_layer_plan.md`](mask_layer_plan.md) §3.
- **What does "converged" mean?** ✅ Elementwise per-margin
  `|sum - target| <= atol + rtol |target|`, reported as
  `GrasBalanceResult.converged`. A global `max` bound is meaningless across
  margins spanning six orders of magnitude; `atol` floors near-zero commodity
  targets.

#### Decision 3 — the target set

✅ **Settled 2026-08-17.** Full specification, sourcing and code design in
[`target_set_plan.md`](target_set_plan.md); the numbers are reproduced by
[`mask_layer_feasibility.py --check`](mask_layer_feasibility.py). Summary here.

**Summary SUT stays struck** as the control — derived, late, and it aggregates away the dimension
being estimated. It belongs to the test set (#573). That part of the earlier text stands.

⚠️ **But the premise this issue was built on does not.** #591 property 1 said *"detail gross output
for 2018-2024 is nowcast by Step 4a — imposing it is circular"*, and concluded the industry
constraint could only be imposed at summary level. **Refuted on two grounds:**

- `BEA_Detail_GrossOutput_IO_<year>` is already extracted for **2017-2024, all 402 detail
  industries**, from BEA's *Underlying* GDP-by-Industry table **UGO305-A**, and
  [`derive_gross_output_before_redefinition`](../../transform/iot/derived_gross_industry_output.py)
  is a **straight read** of it. No 2017 shares. Only the *after*-redefinition variant applies
  co-production ratios, and the SUT is before redefinitions throughout.
- It behaves like an estimate: `GO_i(t)/GO_i(2017)` takes **402 distinct values across 402
  industries**, dispersion 0.67× to 5.25×, and the spread lives *inside* summary industries
  (`335911` batteries 4.70× against `335110` electric lamps 0.67×). A shares-based series would show
  one ratio per summary industry.

**So the industry-column target is imposed at detail, for every Phase 1 year**, and the Phase 1 /
Phase 2 asymmetry on this point dissolves — 2025 is the same, not better. The target set stays
per-year *configuration*, but it is near-constant across 2018-2024.

**The set** (H = hard, S = soft, mask = imposed cell-wise instead, — = deliberately unimposed):

| Margin | Target | Source | Level | Mode |
|---|---|---|---|---|
| **Use** industry columns | gross output, **producer prices** — the margin is `T005 + VAPRO` | UGO305-A, unconverted | detail, 402 | **H** |
| Use FD columns ×13 | NIPA column total per code | PCE, `F02*`, `F03000`, `F04000`, Section-3 equipment/structures | column total | **S** |
| Use FD columns ×6 | `F06C00 F07C00 F10C00 F06N00 F07N00 F10N00` | — | — | **mask** |
| Use VA `V00100` | compensation | T60200D | **industry group, aggregated** | **S** |
| Use VA `T00OTOP`, `V00300` | — | — | — | **—** |
| Use VA `T00TOP`, `T00SUB` | economy-wide totals | T30500, T31300 | scalar | **S** |
| Supply `MCIF`, `MDTY`, `TOP`, `SUB` | column totals | ITA, T30500, T31300 | column total | **S** |
| Supply `MADJ`, `TRADE`, `TRANS` | — | ours (Step 4b/4c) | — | **—** |
| Commodity rows | `T016 = T019` | identity | detail, 402 | **H** |
| Use `T00SUB` ↔ Supply `SUB` | `Σ T00SUB = Σ SUB` ⚠️ sign convention | identity | scalar | **H** |
| Use `T00TOP` ↔ Supply `TOP` + `MDTY` | `Σ T00TOP = Σ TOP + Σ MDTY` | identity | scalar | **H** |
| Use `T00TOP[4200ID]` ↔ Supply `MDTY` | equal | identity | scalar | **H** |
| Supply `TRADE` column | `Σ TRADE = 0` | identity | scalar | **H** |
| Supply `TRANS` column | `Σ TRANS = 0` | identity | scalar | **H** |
| Supply industry columns | `BAS + TAX − SUB = PRO`, per industry | identity | detail, 402 | **H** |

Only the identities are hard. Every sourced target is an estimate from an account with its own
vintage, and a set held entirely hard is infeasible by construction — the argument for the KRAS-style
soft layer in Decision 2.

⚠️ **Three cross-block identities were added 2026-08-17** ([`target_set_plan.md`](target_set_plan.md)
§2a). They tie the Use table's product-tax rows to the Supply table's product-tax columns, and since
both sides sit *inside* the balance they cost no source — so the NIPA totals that T6/T8/T9 spend can
anchor the **level** instead of doing double duty on the split.

**`T00SUB = SUB` holds exactly (59,876). `T00TOP = TOP` does not — it is `T00TOP = TOP + MDTY`**,
because customs duties are a product tax the Supply table books in its own column while the Use table
folds it into `T00TOP`. `4200ID` is the hinge: `T00TOP[4200ID]` = 38,513 = the `MDTY` total, and
`T00TOP` less `4200ID` = the `TOP` total. Residuals of 6 to 18 on 755,451 are BEA's $1M publication
rounding.

Because `MDTY` is nowcast annually from Census duty rates levelled to NIPA `B235RC`, the third
identity doubles as a **free consistency check on that estimate**.

⚠️ **And margins are a redistribution, so their columns sum to zero** ([`target_set_plan.md`](target_set_plan.md)
§2b): `Σ TRADE = 1` and `Σ TRANS = 10` on 3,264,932 and 415,580 of margin added. A margin is value
moved, not created — the 19 wholesale/retail and 5 transport commodities give up exactly what is
added onto the goods they carry. This is the line between the two families of Supply column:
`TRADE`/`TRANS` redistribute and are zero-sum; `MCIF`, `MDTY`, `TOP`, `MADJ`, `SUB` add to supply and
are not.

⚠️ **Two corrections, 2026-08-17.** **T1 binds the Use panel only** — this table said "Supply + Use
industry columns", but the Use column reproduces gross output to 13 per industry while the Supply
column, being basic-priced, misses by up to 88,363. **The Supply industry columns are constrained by
T17 instead**, `BAS + TAX − SUB = PRO`; before it that axis carried no constraint at all. Its wedge
is reachable only on the Use table, since the Supply panel gives product taxes by commodity and never
by industry. And **T12's form depends on the sign convention**: `Σ T00SUB + Σ SUB = 0` on BEA's raw
tables, `Σ T00SUB − Σ SUB = 0` inside the balance where both are stored negative. Writing the sum
form in the balance is wrong by exactly 2 × 59,876 and looks entirely plausible — only the residual
check caught it. [`target_set_plan.md`](target_set_plan.md) §2a and §2c.

**These are the only constraints on Step 4c's own output.** `TRADE`/`TRANS` stay deliberately
unimposed as *targets* — a target we produced is a preference with extra steps — but unimposed left
them with no constraint at all, and the failure mode is silent: add 3.3T of margin onto goods while
the trade commodities give up 3.1T and nothing reports it. T15/T16 close that while leaving the
distribution free.

⚠️ **Published gross output is at *producer* prices; the SUT column identity is at *basic*.** The
wedge is exact per industry — `GO(producer) = T007(basic) + T00TOP − T00SUB`, max residual **$4M on
$34T** — but converting the target would need `T00TOP`/`T00SUB` **by industry**, which Step 2
allocates with 2017 ratios, putting an allocation assumption underneath the hardest constraint in the
set.

✅ **Decided 2026-08-17: do not convert. The target stays at producer prices and the balance solves
the allocation** — a fixed 2017 conversion ratio is exactly what we cannot assume. So the industry
column margin is `T005 + VAPRO` (intermediate plus **all five** VA rows, verified to **$1 per
industry**), not `T005 + VABAS`, and **the product-tax industry split becomes an output of Step 5
rather than an input to it.** `T00TOP`/`T00SUB` keep their economy-wide soft targets, which anchor the
level while leaving the industry distribution free.

⚠️ **Sign trap, and it is BEA's, not ours.** The Use table stores `T00SUB` **positive** (0 of 402
cells negative) while the Supply table stores `SUB` **negative** (15 of 15). So the producer-price
column margin is *not* a plain sum of the column — `T00SUB` carries coefficient −1, and a plain
five-row sum misses `VAPRO` by up to **38,943 on one industry**. Either the target machinery takes
signed coefficients or `T00SUB` is normalised negative at load; **prefer the latter**, checked once at
the boundary rather than at every call site (#653).

⚠️ **Aggregate-level constraints are still required — for `V00100`, not gross output.** T60200D
publishes compensation by industry group, so the truthful constraint is *"these N detail industries
sum to the published group"*. Still `sut_ras`'s `G_va` machinery, still beyond ceda's API.

**Deliberately held back, so the testing strategy keeps real content: the income side.** `V00300`
gross operating surplus, `T00OTOP`, `VAPRO`→T10105 GDP, `VABAS`→T10305, NIPA T1.14 by sector, and the
summary SUT. GDP is the strongest of these because it has a **known, interpretable tolerance**: the
statistical discrepancy, ~$67.9B in 2017 and 0.35% of GDP. The price is that Step 2's value added
enters Step 5 as a *seed only* for two rows — worth paying, since with every income-side aggregate
imposed a green reconciliation run would prove nothing beyond "the solver ran".

#### Recommendation to decide against

**GRAS + a KRAS-style soft-constraint layer, aimed at the Decision 3 target set, with the mask
supplied by the offset method.** **Decision 1 Option A is chosen for the engine:** vendored ceda
dense path, GRAS in place of RAS (Lenzen, Wood and Gallego 2007 + Temurshoev, Miller and
Bouwmeester 2013), clamps deleted, mask via offset outside the engine, no scipy.
[`gras_balance`](../../utils/economic/balance/gras.py) is that engine. Convergence is elementwise
`|sum - target| <= atol + rtol |target|`, reported on `GrasBalanceResult.converged`. The SUT
orchestration layer is [`engine`](../../transform/iot/nowcast_sut_gras.py): Use then Supply,
hard T1 and T11–T17, joint T11 stop. Soft T2/T4/T7 are imposed (blend-once;
T4 closer). T6/T8/T9 defer to T12–T14. **WEIGHTS are uncalibrated.** Hold the
commodity identity and gross output hard, everything sourced soft with
per-source weights, and keep summary SUT out of the target set and in the test set.

Decisions 2 (mask) and 3 (target set) are **recorded**. Decision 1 is **resolved for the engine**;
the Use-then-Supply wrapper and the soft mechanism have landed. What remains of Step 5
code is WEIGHTS calibration after NIPA/ITA replace placeholders.

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
- **Reconciliation against published NIPA aggregates** per section — numeric, not eyeball, tests.
  ✅ **The split is now recorded** (Step 5 Decision 3 / [`target_set_plan.md`](target_set_plan.md) §6),
  and the two kinds must be labelled differently in any report:
  - **By construction, not evidence** — imposed as balance targets, so they pass whatever happens:
    detail gross output; the thirteen FD column totals; the six masked FD columns; `T00TOP`/`T00SUB`
    economy-wide; imports and duties; `T016 = T019`; compensation by industry group.
  - **Genuinely out of sample** — the **income side**, deliberately unimposed: `V00300` gross
    operating surplus and its six-table NIPA construction, `T00OTOP`, **`VAPRO` → T10105 GDP**,
    `VABAS` → T10305, T1.14 gross value added **by sector**, and the summary SUT (#573) in every year.
    GDP is the best of these because its expected gap is known and interpretable — the statistical
    discrepancy, ~$67.9B in 2017, 0.35% of GDP — so the test has a real tolerance rather than an
    arbitrary one.
- **Mask and target feasibility, before the balance runs.** `precheck(seed, mask, targets)` reports
  frozen mass, free mass and leverage per margin; a nonzero residual target facing zero free mass is
  infeasible and must **raise**, not converge to something meaningless. Leverage above ~10× warns.
  See [`mask_layer_plan.md`](mask_layer_plan.md) §3.
- **Unit tests** for the balancer — [`balance/__tests__/test_gras.py`](../../utils/economic/balance/__tests__/test_gras.py):
  hand-checkable matrices, including a negative-cell case and a sign-lock case. A **zero target is
  legal**; a **nonzero target on an empty free margin** raises. Do not treat "zero control total" as
  the silent RAS failure — that failure is the empty-free-margin case.
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

Reviewed 2026-08-05 against [project 26](https://github.com/orgs/cornerstone-data/projects/26) and
every open issue in the repo, and **re-swept 2026-08-19**: the fourteen issues filed since the first
pass (#606, #610-#615, #635, #650, #660, #664, #665) were added to the board, and every item's `Status`
was set from what has actually merged rather than from when the card was made.

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
| 1 FD block | #504, #523, #526/#527/#528, #529/#530/#531 (built — see §`F03000`), **#660**/**#664**/**#665** (the three `F03000` follow-ons), #547, #606 (`S00300`), **#575** (1d code list), **#576** (1f reconciliation), #621 (2018-2024 PCE), #635 (the swallowed `ValueError`) | — |
| 2 Value added | #535, #536, #537, #538 | — |
| 3 Intermediate | #497, #564, **#577** (agriculture), **#578** (government) | — |
| **4 Supply table** | **#570** (4a), **#579** (4b — *done*), **#571** (4c, parent) split into **#610** (2017 rates — *done*), **#611** (FAF transport chain — *in progress*), **#612** (annual trade levels — *done*), **#613** (apply to nowcast years, derive `TRADE`/`TRANS`), **#614** (validate per commodity), **#615** (deferred NAPCS concordance), **#620** (transport trend validation), **#580** (4d), **#581** (4e) | 4c's five sub-issues were filed after the first pass and are now on the board |
| 5 RAS | **#588** (balancer, parent — `gras_balance`, `engine`, and the soft mechanism landed; WEIGHTS uncalibrated), **#653** / **#654** / **#591** (mask/target scaffolding — **landed** in [#659](https://github.com/cornerstone-data/bedrock/pull/659)), **#655** (gross output at basic prices) | ~~#589~~ (load_suts_from_r) and ~~#590~~ (check_balances) **closed not planned, 2026-08-09** — neither port is needed |
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
  BEA distributes margins on supply including imports (§Step 4c point 1), so:

  | half | needs | when |
  |---|---|---|
  | ✅ **Derive 2017 rates** per (buyer, commodity, margin type) from the published Margins table, and prove the two identities reproduce the 2017 Supply columns | nothing — the 2017 tables are already loaded | **done, #610** |
  | ✅ **The two Supply columns** `TRADE`/`TRANS` | nothing downstream — see below | **done, #611/#612/#613** |
  | **Apply those rates** to build the transaction-level **Margins table** | `T007` from **4a** (#570), `MCIF`/`MADJ`/`MDTY` from **4b** (#579) *and* `TOP`/`SUB` from **4d** (#580) | after all three |

  Doing the first half first is what de-risked the other three consumers, and it also **settled what
  the second half depends on**: the rate base is producer value less trade-level tax, not `T013`, so
  4d joins 4a and 4b as a prerequisite (§Step 4c point 1). Applying rates to `T007` alone to avoid the
  4b dependency is the tempting shortcut and is **wrong** — it drops the margin on imports.

  ⚠️ **Corrected 2026-08-21: the two Supply columns never applied rates to a nowcast base, and so
  never needed 4a/4b/4d.** This section used to read as if `TRADE`/`TRANS` fell out of the
  rate-application half. They do not, and the dependency above survives **only** for the
  transaction-level Margins table (§Phase 4b of
  [`margins_estimation_plan.md`](margins_estimation_plan.md)):

  - `TRANS` (#611) allocates each mode's **observed annual freight revenue** over its receiving
    commodities. Nothing in it touches the nowcast base.
  - `TRADE` (#612/#613) is **anchor-and-move**: the published 2017 column rescaled by the Census
    wholesale and retail gross margin. The *level* moves annually and so does the kind-of-business
    split; the **receiving split — which commodities get the margin — is the frozen 2017 product
    mix**, for every year 2018-2023. It is a rescaled 2017 column, not a rate applied to nowcast
    output.

  ⚠️ **That frozen mix is the open assumption in #613, and it is why #613 does not close as "the
  column is sourced" on its own.** Nothing annual observes which goods a wholesaler's margin sits on
  — BEA's answer is the product-line method, deferred at #615 — so the mix is 100% of the trade
  commodity detail carried on a 2017 observation. Either **#614** (validate per commodity) closes
  alongside it, or the shares stay pinned by test so the assumption cannot drift silently;
  `test_receiving_shares_are_frozen_at_2017` in
  [`test_nowcast_trade_margins.py`](../../transform/iot/__tests__/test_nowcast_trade_margins.py) is
  that pin.
- **P1** — code-space Phase 2 (retarget `FD_Gov`/`FD_Structures`/`FD_IP`, drop
  `map_fbs_sectors_to_model_schema`, roll out 2018-2024), which unblocks #576, Step 2 and Step 3.
  #574 may fall out of this rather than needing its own attribution work — diagnose first. Then #579,
  #580, #581 once the trade FBS path is safe. #573 any time; it is small and Step 5 needs it.

  ✅ **#529/#530/#531 (`F03000`) is done as a first pass (2026-08-19, #666)** — it was P1 on the
  rescope and it closed the last whole-column `miss` in
  [`progress_report.md`](progress_report.md). What is left of it is three ranked follow-ons rather
  than a build: **#660** (mining and farm splits — the largest single error, `211000` at −4,754
  against −7,577), then **#664** (manufacturing stage shares — `336411` at −288 against −6,314),
  then **#665** (`S00402`, at 380 against 3,969). All three are *allocation* work on a column whose
  total is already right, so they are P1 only against the per-commodity bar, not against anything
  downstream — nothing else in the build is blocked on them.
- **P2** — Step 5 (#588 parent, with **#653** scaffolding *landed*, **#654** mask and **#591** target
  set built to first pass in #659; **#655** gross output at producer prices next), then #582/#583/#584
  and **#585**, the 2017 benchmark replay — the single highest-value test in the project — then #572.
- **P3** — #586, then Step 9 (#592, #593).

**Re-ranked 2026-08-19.** The two things that moved: 4c is no longer one card — #610 and #612 are done
and #611 is in progress, so the critical path through Step 4 now runs #570 (4a) → #613, with #580 (4d)
alongside. And `F03000` came off the critical path entirely: the column exists, so the remaining
inventories work competes on accuracy rather than blocking coverage. **The single highest-leverage
unbuilt thing is still #570 (4a)** — `T007` is a whole missing Supply column, it gates #613, and Step 5
cannot target gross output without it.

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

**Re-sorted 2026-08-19, 66 → 78 items.** The twelve added issues were appended at the bottom on
creation, so the whole board was re-positioned into the order above and they were slotted into their
steps: #660/#664/#665 behind #529-#531 as the `F03000` follow-ons, #650 with them (it extends the same
`Census_EC_PxI` source to 2022), #610-#615 and #620 behind #571 as the 4c chain, #606 and #635 into
Step 1 validation, #621/#646/#647/#668 with 1A and 1B.

**`Status` now means what has merged, not when the card was made,** and **`Done` still means closed** —
every `Done` item on the board is a closed issue, and breaking that would make the column unreadable.
Nine items moved off `Todo` on that basis, and six of them were then closed outright:

| issue | what closed it |
|---|---|
| #530 (1C source and method) | #666 — the four rules, the sources behind each, and the farm level |
| #531 (1C FBAs and crosswalks) | #651, #652, #666 — `U50705BU1`/5.7.5B/`U70205`, the `Census_EC_PxI` extractor, and a 1,218-row crosswalk over 54 NIPA lines |
| #610 (4c-1 2017 rates) | #626 — and it settled that #580 (4d) is a prerequisite for #613 |
| #612 (4c-3 annual trade levels) | #628 |
| #653 (Step 5 scaffolding) | #659 — deliberately Decision-1-independent, which is why it could land first |
| #654 (Step 5 fixed-value mask) | #659 |

The three that stay open are open for a reason: **#529** has the three `F03000` follow-ons under it,
**#611** is the transport chain in progress, and **#655** is the gross output target. #591 and #588
were already `In Progress`.

✅ **Every issue on the board now carries its step in its title.** Eleven were renamed 2026-08-19 —
#615 and #620 into `Step 4c`, #635 and #658 into `Step 1`, #646/#647/#668 into `Step 1B`, and
#650/#660/#664/#665 into `Step 1C`. ⚠️ An earlier count of ten here was wrong in both directions: it
included #606, which already carried `Step 1/3` and is correct as it stands, and it missed #615 and
#620. Only the five pull requests are unprefixed, which is the standing exception — they inherit from
the issue they close.

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
4. **The Step 5 balance — three coupled decisions.** Detail in §Step 5.
   The earlier form of this question ("summary SUT totals or gross output?") **presumed a default that
   is now struck**; see 4c.
   - 4a. ✅ **Starting point — Option A for the engine, resolved.** Vendored ceda dense
     `ras_balancing.py` + GRAS, no scipy/sparse, clamps deleted, mask via offset.
     [`gras_balance`](../../utils/economic/balance/gras.py). The SUT-orchestration half of Option A
     (wrapper `engine(free, residual, masks)`) is ✅ Use then Supply, hard T1 and T11–T17.
     Soft T2/T4/T7 imposed; T6/T8/T9 deferred. **WEIGHTS uncalibrated.**
   - 4b. **Objective function** — ✅ inner loop is GRAS (Lenzen 2007 + Temurshoev 2013);
     `GrasBalanceResult.converged` is elementwise `atol`/`rtol`. Mask policy recorded
     ([`mask_layer_plan.md`](mask_layer_plan.md)); plain RAS is out. **Soft mechanism
     landed; WEIGHTS remain uncalibrated.** Sign-flex *mechanism* is `sign_flex` on the kernel; *policy* (which cells) is the mask.
   - 4c. ✅ **Target set — settled 2026-08-17.** [`target_set_plan.md`](target_set_plan.md), #591.
     Detail gross output (hard, and **observed at detail for every Phase 1 year** — the circularity
     premise was wrong), thirteen NIPA FD column totals plus six masked columns, compensation by
     industry group, the Supply trailing totals. **The income side is deliberately held back**, so
     GDP stays a real test with the statistical discrepancy as its tolerance.
5. **`nowcast.py` vs. `bedrock/transform/iot/` boundary** — is `nowcast.py` the per-year orchestrator
   calling into `transform/iot/`'s existing functions, or should the new SUT/MUT code live in
   `transform/iot/` (where #495 pointed `nipa_final_demand_estimates.py`)? Steps 4-7 are a lot of new
   code; worth settling before writing it.
6. **Interim caching layout** while developing (final destination is GCS, per the board).
7. **`MADJ` treatment** — 2017 candidate maps Census `GEN_CHA_YR`, reassigns onto nonzero Supply `MADJ` Detail codes by signed published `MADJ` shares, and levels to published Supply `MADJ` (`madj_detail_usd`). Destination mix is a 2017 hold-structure for later years; an annual level target beyond Supply `MADJ` remains open. Affects whether `T013` reconciles exactly once `T007` is sourced.

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
  construction Step 4a specifies. ⚠️ **Corrected 2026-08-17:** this section previously said the
  2018-2024 gross output was nowcast and only 2025's came "straight from the update". It does not.
  BEA's *Underlying* GDP-by-Industry table **UGO305-A publishes gross output for all 402 detail
  industries annually**, it is already extracted as `BEA_Detail_GrossOutput_IO_<year>` for 2017-2024,
  and `derive_gross_output_before_redefinition` reads it directly. What 2025 adds is one more year of
  the same series, not a new kind of input. **The commodity mix, not the output vector, is what
  Step 4a nowcasts.**
- **Step 5 (RAS targets)** — ⚠️ **rewritten twice.** The original note said 2025 could be "controlled
  the same way every other year is" off the summary SUT; that went with the summary-SUT default. The
  replacement said 2025 was the first year with observed detail gross output and would therefore be
  *better* constrained. **That is wrong too, for the reason above** — every Phase 1 year already
  carries a detail-level industry constraint. 2025 is the same, not better. The target set stays
  per-year configuration because weights and hold-backs will move, not because the sources differ by
  year. See Decision 3 and [`target_set_plan.md`](target_set_plan.md) §1.
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
4. **Add `NIPA_final_dom_uses_2025.yaml`** (the series currently runs 2017-2024), plus the 2025 counterparts of
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
