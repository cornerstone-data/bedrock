# Plan: `bedrock/transform/eeio/nowcast.py` — nowcasted national Supply/Use/Import tables

GitHub project: [cornerstone-data/projects/26 — "Nowcast 2018-2024 IOT"](https://github.com/orgs/cornerstone-data/projects/26)
(22 items, milestone `v0.5`). Fetched successfully this round — the plan below is now built directly off
the board's issues/draft-issues rather than assumptions.

## Where this fits on the board

**The current branch (`523_update_NIPA_FD_FBS`) *is* issue [#523](https://github.com/cornerstone-data/bedrock/issues/523)
"Update Final Demand FBS method"** (status: In Progress, linked PR #524). Its checklist:
- [x] with PCEBridge for allocation — done (this session's `FD_PCE` work)
- [x] clean up the NIPA FBAs — done (footnote/suffix stripping, crosswalk fixes, this session)
- [ ] review all activity mappings — partially covered by this session's crosswalk conflict review;
  worth a final pass before closing #523
- [ ] test data against the 2017 data — partially covered (PCE reconciliation to ~1.3%); the two bugs
  fixed this session (stale crosswalk rows, dead `FD_Structures1_used`) were found *by* this testing
- [ ] add the PEQBridge as well for use in allocation of investment in equipment — **not started; this
  is the direct answer to "how does F02E00 get built"** (see Phase 1a below)

So finishing #523 (the PEQ Bridge + a final mapping/data review) is the natural on-ramp into the new
`nowcast.py` module, not a separate track.

## Grounding: what already exists (revised with board context)

**Module path & naming decision.** The board has an explicit open item, ["Refactor load_suts_from_r.py
into transform/eeio"](https://github.com/cornerstone-data/USEEIO/blob/nowcasting/nowcasting/load_suts_from_r.py)
(draft issue, no number yet), whose body literally says *"Determine to integrate into
derived_cornerstone.py, new module or elsewhere."* — `bedrock/transform/eeio/nowcast.py` is a reasonable
answer to that open decision. Confirmed `bedrock/transform/eeio/` is the right sibling location
(`derived_cornerstone.py`, `derived_useeio_nowcast.py`, `cornerstone_year_scaling.py`). Note there's
*also* an established `bedrock/transform/iot/` module (`derived_gross_industry_output.py`,
`derive_PRO_to_PUR_ratio.py`, `derived_price_index.py`) that already does before/after-redefinitions
and PRO/PUR adjustment for gross output — several board items point at *that* directory too (issue #495
routes `nipa_final_demand_estimates.py` → `bedrock/transform/iot`). Worth deciding up front whether
`nowcast.py` is the top-level orchestrator that *calls into* `bedrock/transform/iot/`'s existing
functions, or whether some of this project's new code belongs there instead — flagged in open
questions.

**Target final-demand schema** (unchanged from before): `bedrock/utils/taxonomy/bea/v2017_final_demand.py`'s
20 `BEA_2017_FINAL_DEMAND_CODES`. Status per column:

| Code | Description | Status |
|---|---|---|
| F01000 | Personal consumption expenditures | ✅ `FD_PCE` (this session) |
| F02E00 | Nonresidential private fixed investment in **equipment** | ❌ → **Phase 1a: PEQ Bridge** ([#525](https://github.com/cornerstone-data/bedrock/issues/525), [#496](https://github.com/cornerstone-data/bedrock/issues/496)) |
| F02N00 | Nonresidential private fixed investment in IP products | ✅ `FD_IP_direct`/`FD_IP_proportional` |
| F02R00 | Residential private fixed investment | ✅ `FD_Structures1` |
| F02S00 | Nonresidential private fixed investment in structures | ✅ `FD_Structures2` |
| F03000 | Change in private inventories | ❌ → **Phase 1e: [#529](https://github.com/cornerstone-data/bedrock/issues/529)** (source/method in [#530](https://github.com/cornerstone-data/bedrock/issues/530), implementation in [#531](https://github.com/cornerstone-data/bedrock/issues/531)) |
| F04000 | Exports of goods and services | ❌ → **Phase 1d: [#526](https://github.com/cornerstone-data/bedrock/issues/526)** (source decision in [#527](https://github.com/cornerstone-data/bedrock/issues/527), implementation in [#528](https://github.com/cornerstone-data/bedrock/issues/528)) |
| F05000 | Imports of goods and services | ❌ → same, **#526/#527/#528** |
| F06/F07/F10 (12 codes) | Federal/State/Local CE, Equip, IP, Structures | ✅ `FD_Gov_*` (⚠️ SLG Equipment/Structures/IP attribution bug still open, see memory `bedrock_nipa_fd_pce.md`) |

**F02E00 is issue #496 + #525, not a mystery.** Issue #496 ("NIPA Private investment in equipment
table not used in FBS method", assigned to Wes) identifies the source: **NIPA Table 5.5.5U** has an FBA
already generated (per the flowsa `BEA_NIPA.yaml` reference in the issue body) but it was never wired
into an FBS method. Issue #525 ("Add in PEQ Bridge", assigned to Wes, no body yet) is the equipment
analog of the PCE Bridge work just finished — build a `BEA_PEQBridge` source (mirroring
`BEA_PCEBridge.yaml`/`bea_parse`'s `"PCEBridge" in source` branch from this session) and a
`FD_IP_equipment`-style activity_set in `NIPA_FD_<year>.yaml`, following the exact same pattern as
`FD_PCE` (Table 5.5.5U in place of U20405, PEQBridge in place of PCEBridge).

**F04000/F05000 (exports/imports) — issue #526 "Integrate international trade data into initial tables",
with two sub-issues:**
- **[#527](https://github.com/cornerstone-data/bedrock/issues/527) "Determine best data source and
  existing code for international trade data for initial estimates"** — a source decision, not yet made.
  Requirement stated in the issue: data needs to match the 2017 detailed Use table as closely as
  possible, be available annually, and be comparable to BACI trade data. Three candidates given:
  1. Census Trade in Goods + BEA trade in services data — already used in
     [`cornerstone-data/USEEIO`'s `import_emission_factors/download_imports_data.py`](https://github.com/cornerstone-data/USEEIO/blob/master/import_emission_factors/download_imports_data.py)
     (existing code to potentially port, same pattern as `sut_ras.py`/`load_suts_from_r.py`).
  2. BEA ITA accounts with NIPA link — ITA Table 2.1 (goods) + ITA Table 3.1 (services), joined via
     BEA's "Linkage table from ITA to NIPA for foreign transactions."
  3. BACI data — framed in the issue more as a comparison/validation source than a primary pick.
- **[#528](https://github.com/cornerstone-data/bedrock/issues/528) "Implement the trade data into
  FBA/FBS"** — no body yet; follows once #527 picks a source, same FBA→FBS pattern as everything else.

**F03000 (change in private inventories) — issue #529 "Integrate Change in Inventories data", with two
sub-issues:**
- **[#530](https://github.com/cornerstone-data/bedrock/issues/530) "Change in inventories: Reevaluate
  and determine data src and method"** — has a detailed writeup already. Key facts: NIPA Table 5.7.5B
  ("Change in Private Inventories by Industry") gives the "where held" (holding industry) view, while
  the Use table's F03000 column is "what held" (commodity) — **the totals match exactly** (NIPA Table
  1.1.5 line 14 = Use table F03000 total), but the by-commodity split doesn't come for free. Three
  inventory sub-types are distinguished (finished goods, work-in-process, materials & supplies), each
  needing different source data (Annual Survey of Manufacturers for stage-of-fabrication; Economic
  Census "Materials Consumed by Kind of Industry" / "Products by Industry" for commodity composition).
  **Explicit, load-bearing scoping decision already made in the issue body:** *"We need to at least use
  the Change in Private Inventories NIPA totals as a starting place, but further work in attributing
  those to commodity based on the level of fabrication may have to wait."* — i.e. Phase 1e can ship
  with F03000 as an aggregate/NIPA-total-only column first, deferring correct commodity-level
  attribution to a later pass, mirroring how Phase 1 itself accepts an all-zero placeholder for columns
  not yet built.
- **[#531](https://github.com/cornerstone-data/bedrock/issues/531) "Integrate change in inventory
  related datasets"** — outcome of #530; "likely integrate the NIPA data into the NIPA BEA and then into
  NIPA FD FBS," i.e. the same FBA→FBS pattern as PCE/PEQ, once #530 settles the method.

So **all 4 previously-unsourced final-demand columns now have a tracked issue** — none are pure open
questions anymore, though #527's source pick and #530's commodity-attribution method are real decisions
still to be made (not yet blocking, since both explicitly allow starting from a coarser/total-only
estimate).

**PUR→PRO conversion: don't rebuild this, it already exists.** The board's USEEIO issue #4 flags a
known problem — *"Use complete Margins and not PCE Bridge for PUR to PRO conversion"* — because the
external `nipa_final_demand_estimates.py` script uses PCEBridge's 5 value-chain columns for margin
conversion, which only covers PCE, not all of final demand, and splitting a margin category (e.g.
Transportation) across individual commodity sectors is flagged there as unresolved. **But bedrock
already has a margins-based, `USA_2017_FINAL_DEMAND_CODES`-aware PRO↔PUR mechanism**:
`bedrock/transform/iot/derive_PRO_to_PUR_ratio.py` — `derive_phi_cornerstone_usa_at_year(year)` /
`derive_margins_cornerstone_usa_at_year(year)` compute per-sector, per-year PRO:PUR ratios (`phi`) from
the full Margins data already, keyed against the same final-demand code taxonomy this project targets.
The Phase 1 Y-PUR matrix is explicitly *staying* in PUR price per your instruction, so this doesn't
block Phase 1 — but when a PRO conversion is eventually wanted, check whether this existing machinery
already solves the sector-splitting problem USEEIO issue #4 called out as unresolved, before
re-implementing anything.

**No RAS implementation in bedrock — but one exists to port, not invent.** Confirmed no RAS/GRAS code
anywhere in bedrock (comments only). The board has it as a named integration task: [`sut_ras.py`
(USEEIO nowcasting branch)](https://github.com/cornerstone-data/USEEIO/blob/nowcasting/nowcasting/sut_ras.py)
→ **`bedrock/utils/economic/`** (per the draft issue "Integrate sut_ras.py into util/economic" — a
specific target directory, not `bedrock/utils/math/` as I'd guessed last round). Phase 5 is a port +
adapt, not new design.

**Value Added section — fully specified on the board, not an open question anymore.** Two draft issues
give exact NIPA table numbers and formulas:

Use-table VA component → BEA code → NIPA table:
| Component | Code | NIPA table |
|---|---|---|
| Compensation of employees | V00100 | T60200D (Table 6.2D) |
| Other taxes on production | T00OTOP | T30500 (Table 3.5, excl. taxes on products) |
| Gross operating surplus | V00300 | **Constructed**: T61200D + T61400D + T61500D + T61700D + T61300D + T62200D |
| Taxes on products and imports | T00TOP | T30500 (taxes-on-products portion) |
| Less: Subsidies | T00SUB | T31300 (Table 3.13) |

("Extract additional NIPA tables for value added" draft issue gives the same 7 underlying Section-6/3.5
tables individually: T60200D, T61200D, T61400D, T61500D, T61700D, T62200D, T61300D — `V00300` is their
sum.) Model will carry `SectorProducedBy`/`SectorConsumedBy` like the FD model (orientation TBD per the
issue itself); allocation to specific BEA industries "likely needs to use the 2017 table ratios."

**VA reconciliation targets — also specified**, not something to invent: NIPA Table 1.14 (Gross Value
Added by Sector) as the top-level check (total VA should equal total VA in the Use table); each
Section-6 table's total should equal the corresponding Use-table row/group total; `VABAS` (=
V00100+T00OTOP+V00300) should reconcile to T10305; `T018` (after adding T00TOP, subtracting T00SUB)
should reconcile to GDP via T10105.

**VA needs a before→after-redefinitions transform, and there's a template for it.** Draft issue
"Transform Value Added FBS into after redefinitions": *"The VA FBS will be in before redefinitions. The
estimates should be transformed based on the same approach used to adjust the gross industry output
estimates"* — pointing at `bedrock/transform/iot/derived_gross_industry_output.py` lines 150–212
(confirmed present: `compute_coproduction_ratios`/`adjust_gross_output`, which derive a co-production
movement ratio from the benchmark year's before/after Make tables and apply it going forward). Reuse
this pattern rather than deriving a new one for VA.

**Intermediate transactions start from the actual dollar Use matrix, not the A coefficient matrix
(confirmed by you this round).** The industry-transactions (intermediate) section originates from the
**Use table** itself — e.g. `load_2017_Utot_after_redef_usa()` / `_load_2017_detail_supply_use_usa()`
in `io_2017.py` for the 2017 benchmark — not backed out of `A` (`A` is a derived coefficient matrix;
going `A → U` via `U ≈ A @ diag(x)` would discard whatever rounding/negative-clipping happened when `A`
was built, so it's the wrong direction). Issue #497 ("Prepare initial intermediate estimates using
commodity inflation", assigned to Wes) then describes how that seed Use table gets **nowcast forward**
year over year: port the logic from USEEIO's `CalculateIntermediateUseAndCommodityMix.R`, but (a) use
bedrock's own commodity-inflation approach instead of the R script's, (b) apply it *only* to
intermediate uses by industry (not value added), and (c) apply it to the **after-redefinitions** Use
table specifically. So the sequencing is: **start from the dollar Use table (benchmark year) → inflate
forward by commodity per #497 → get each subsequent year's industry-transactions section** — the A
matrix stays out of this section's derivation entirely (it's downstream of the Use table, not an input
to reconstructing it). Check `bedrock/utils/economic/inflation_helpers_cornerstone.py` (already imported
by `derive_PRO_to_PUR_ratio.py`) for the "internal commodity inflation approach" referenced.

**Other integration items on the board** (all Todo, not yet scoped into phases below in detail):
- [`check_balances.py`](https://github.com/cornerstone-data/USEEIO/blob/nowcasting/nowcasting/check_balances.py)
  (USEEIO nowcasting branch) → integrate into bedrock's validation framework (`bedrock/utils/validation/`).
- "Optional: Balance to industry and commodity output rather than summary tables" — implies the
  *default* RAS control totals are summary-table totals, with industry/commodity gross output as a
  named optional alternative.
- "Store created Make, Use, and Import matrices on GCS" — final artifacts go through bedrock's normal
  GCS snapshot/caching path (see memory `bedrock_data_access.md`), not just local parquet.
- "Update bedrock pipeline to build models with nowcasted products, generate snapshots and update
  diagnostics" — the final wire-in to the main model-build pipeline; last phase, depends on everything
  else.

## Revised phased plan

### Phase 0 — finish #523, scaffold `nowcast.py`
- Close out #523's remaining checklist items (mapping review, 2017 test) as a matter of hygiene before
  building on top of `NIPA_FD_*.yaml`.
- Create `bedrock/transform/eeio/nowcast.py` with a docstring linking this plan + project #26, and
  decide the `nowcast.py` vs. `bedrock/transform/iot/` boundary (open question below) before writing
  much code, since it affects where several pieces land.

### Phase 1 — Final demand section, PUR price, BEA_2017_Detail schema
1a. **PEQ Bridge (#525/#496)** — build `BEA_PEQBridge` mirroring this session's `BEA_PCEBridge` work
   exactly: new `bedrock/extract/bea/BEA_PEQBridge.yaml`, a `"PEQBridge" in source` branch in
   `bea_parse` (BEA.py), source-catalog registration, and an `FD_IP_equipment`-style activity_set in
   `NIPA_FD_<year>.yaml` sourcing NIPA Table 5.5.5U in place of U20405. This closes F02E00 and #523's
   last checklist item in one piece of work.
1b. **F0-code lookup** — small mapping from each activity_set's `assign_fields.ActivityConsumedBy`
    label (or the yaml's `ActivityConsumedBy` selector value) to `BEA_2017_FINAL_DEMAND_CODE`, for
    pivoting FBS output into Y-matrix columns.
1c. **Per-year Y-PUR assembly** (`derive_nowcast_Y_pur(year)`): `generateFlowBySector`, pivot
    `SectorProducedBy` × F0-code, reindex to all 20 `BEA_2017_FINAL_DEMAND_CODES` (F03000/F04000/F05000
    stay all-zero until Phases 1d/1e land).
1d. **F04000/F05000 (exports/imports, #526/#527/#528)** — resolve #527's source decision (Census
    goods+BEA services vs. BEA ITA-with-NIPA-linkage vs. BACI), then build the FBA/FBS in #528 following
    the established NIPA_FD pattern (or a new source module if the pick isn't NIPA-table-shaped).
1e. **F03000 (inventories, #529/#530/#531)** — per #530's explicit scoping, ship the NIPA Table 1.1.5 /
    5.7.5B **total** first (matches the Use table's F03000 total exactly even without a commodity
    split), deferring the finished-goods/work-in-process/materials-and-supplies commodity attribution
    (Annual Survey of Manufacturers + Economic Census data) to a follow-up. This keeps Phase 1e
    unblocked by the harder commodity-attribution question.
1f. **Validate** — same reconciliation technique as this session's PCE work; also check against the
    VA reconciliation targets once Phase 2 exists, since Use-table row/column totals tie the sections
    together.

### Phase 2 — Value Added section
- Build FBS method(s) sourcing the 7 Section-6/3.5/3.13 NIPA tables per the mapping above (likely a new
  `NIPA_VA_<year>.yaml` following the `NIPA_FD_<year>.yaml` pattern — reuse `extract_table_info`/
  `drop_unassigned`/activity_sets machinery).
- Allocate to specific BEA industries using 2017 table ratios (per the draft issue).
- Reconcile against NIPA Table 1.14 + the VABAS/T018 formulas above.
- Transform before→after redefinitions using the `compute_coproduction_ratios`/`adjust_gross_output`
  pattern from `bedrock/transform/iot/derived_gross_industry_output.py`.

### Phase 3 — Intermediate transactions (industry × commodity)
- **Seed from the actual dollar Use matrix** (`load_2017_Utot_after_redef_usa()` or the equivalent
  detail-level loader for the benchmark year) — *not* backed out of the `A` coefficient matrix.
- Nowcast that seed forward year over year by porting/adapting
  `CalculateIntermediateUseAndCommodityMix.R`'s logic using bedrock's own commodity-inflation approach
  (`bedrock/utils/economic/inflation_helpers_cornerstone.py`), applied to industry intermediate uses
  only, on the after-redefinitions Use table (per issue #497).

### Phase 4 — Schema conversion (Cornerstone schema, after redefinitions)
- Unchanged from last round: reuse `industry_corresp()`/`commodity_corresp()` (`cornerstone_expansion.py`)
  and `cfg.iot_before_or_after_redefinition`.

### Phase 5 — RAS rebalancing
- Port `sut_ras.py` (USEEIO nowcasting branch) into `bedrock/utils/economic/`, adapting off rpy/R
  dependencies as needed (same theme as the `load_suts_from_r.py` refactor item).
- Default control totals: summary-table totals (with industry/commodity gross output as an optional
  alternative per the board).

### Phase 6 — Validation, storage, pipeline integration
- Port `check_balances.py` into `bedrock/utils/validation/`.
- Store final Make/Use/Import matrices via bedrock's normal GCS snapshot path.
- Wire nowcasted products into the main model-build pipeline; regenerate snapshots/diagnostics.

## Testing strategy (unchanged, reinforced by board)
- Per-phase reconciliation against independent published totals — the board itself specifies most of
  these targets now (NIPA 1.14, VABAS/T018 formulas, Section-6 row/group totals), so testing has
  concrete numeric targets, not just "does it look plausible."
- Unit tests for the F0-code lookup, the PEQ Bridge parse branch (mirror this session's PCEBridge
  tests if any exist), and the ported RAS function (small hand-checkable matrices; a RAS port is exactly
  the kind of code that can silently misbehave on edge cases like a zero control total).
- Golden-file test per year once Phase 1 stabilizes, so later phases don't silently drift the Y-PUR
  matrix underneath them.

## Open questions for you (revised — several from last round are now answered by the board or by you directly)
0. ~~Intermediate transactions starting point~~ — confirmed: the dollar **Use matrix** (benchmark year),
   nowcast forward via commodity inflation (#497); *not* backed out of the `A` coefficient matrix.
1. ~~Years in scope~~ — board title says "2018-2024"; confirm this still matches (2017 is the benchmark
   year already built).
2. ~~F03000/F04000/F05000 sources~~ — now tracked: exports/imports in #526/#527/#528 (source pick
   still open between 3 candidates), inventories in #529/#530/#531 (method open, but scoped to start
   from the NIPA total only).
3. ~~Value Added NIPA tables~~ — answered above (T60200D, T61200D, T61400D, T61500D, T61700D, T62200D,
   T61300D, T30500, T31300).
4. **`nowcast.py` vs. `bedrock/transform/iot/` boundary** — is `nowcast.py` the top-level per-year
   orchestrator calling into existing `bedrock/transform/iot/` functions (PRO/PUR, gross output
   adjustment) and new VA/intermediate logic, or should some of *this* project's new code live in
   `bedrock/transform/iot/` instead, matching where issue #495 pointed `nipa_final_demand_estimates.py`?
5. ~~RAS control totals~~ — default is summary-table totals per the board; confirm before Phase 5.
6. **Caching convention** — still open; "Store created Make, Use, and Import matrices on GCS" confirms
   the *eventual* destination but not the *interim* per-year local-cache layout while developing.
7. **Sequencing vs. #523** — do you want #523 fully closed (mapping review + 2017 test pass) before
   starting Phase 1a (PEQ Bridge), or is it fine to start PEQ Bridge in parallel since it's really the
   same underlying pattern?
8. **#527's trade-data source pick** — Census Trade in Goods + BEA services, BEA ITA-with-NIPA-linkage,
   or BACI? Affects whether Phase 1d follows the existing NIPA_FD FBS pattern or needs a new source
   module (e.g. porting `download_imports_data.py` from the USEEIO repo).
9. **#530's inventory-commodity-attribution timing** — confirmed OK to ship F03000 as a NIPA-total-only
   column in Phase 1e (per the issue's own scoping), with the Annual-Survey-of-Manufacturers/Economic-
   Census commodity split deferred — or do you want that fuller treatment before Phase 1 is considered
   done?
