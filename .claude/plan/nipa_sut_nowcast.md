# Plan: `bedrock/transform/eeio/nowcast.py` — nowcasted national Supply/Use/Import tables

GitHub project: [cornerstone-data/projects/26 — "Nowcast 2018-2024 IOT"](https://github.com/orgs/cornerstone-data/projects/26)
(milestone `v0.5`).

Status reviewed 2026-07-28 against `origin/main`, issue states, and the code.

**The project board itself was not re-read this round** — `gh project item-list 26` needs the
`read:project` scope, which the current token lacks (`gh auth refresh -s read:project`). Board
draft issues carry no issue number, so the items below that came from drafts — the `sut_ras.py`
and `load_suts_from_r.py` refactors, the VA table extraction, the GCS storage item, the
industry/commodity balancing option — are as of the previous revision and may have moved.

## Status since the last revision

Merged to `main`:

| PR | what landed |
|---|---|
| [#524](https://github.com/cornerstone-data/bedrock/pull/524) | `NIPA_FD_*` FBS methods, `FD_PCE`, crosswalk fixes |
| [#540](https://github.com/cornerstone-data/bedrock/pull/540) | `assign_sector_consumed_by_from_clean_parameter`, so an activity_set sets its own `SectorConsumedBy` |
| [#541](https://github.com/cornerstone-data/bedrock/pull/541) | `bedrock/analysis/compare_NIPA_to_IOT` — compare a NIPA table against a BEA IOT row or column |
| [#542](https://github.com/cornerstone-data/bedrock/pull/542) | `BEA_NIPA` caches `FlatFiles.ZIP` under `extract/input_data/`, GCS fallback |

Closed: [#525](https://github.com/cornerstone-data/bedrock/issues/525) (PEQ Bridge),
[#496](https://github.com/cornerstone-data/bedrock/issues/496) (equipment table unused).

Open: [#523](https://github.com/cornerstone-data/bedrock/issues/523) (2 of 5 boxes unchecked),
[#526](https://github.com/cornerstone-data/bedrock/issues/526)/[#527](https://github.com/cornerstone-data/bedrock/issues/527)/[#528](https://github.com/cornerstone-data/bedrock/issues/528) (trade),
[#529](https://github.com/cornerstone-data/bedrock/issues/529)/[#530](https://github.com/cornerstone-data/bedrock/issues/530)/[#531](https://github.com/cornerstone-data/bedrock/issues/531) (inventories),
[#495](https://github.com/cornerstone-data/bedrock/issues/495), [#497](https://github.com/cornerstone-data/bedrock/issues/497).

`bedrock/transform/eeio/nowcast.py` exists and implements `derive_initial_Y_pur(year)`:
`generateFlowBySector('NIPA_FD_<year>')`, resolve both sector columns through
`map_fbs_sectors_to_model_schema`, pivot to commodity × `BEA_2017_FINAL_DEMAND_CODE`,
reindex to all 20 codes.

## Final-demand column status

| Code | Description | Status |
|---|---|---|
| F01000 | Personal consumption expenditures | done — `FD_PCE`, all years |
| F02E00 | Nonresidential fixed investment, equipment | done — `FD_Equipment` via `BEA_PEQBridge`, **2017 only** |
| F02N00 | Nonresidential fixed investment, IP products | done — `FD_IP_direct` / `FD_IP_proportional` |
| F02R00 | Residential fixed investment | done — `FD_Structures1` |
| F02S00 | Nonresidential fixed investment, structures | done — `FD_Structures2` |
| F06/F07/F10 (12 codes) | Federal/State/Local CE, Equip, IP, Structures | done — `FD_Gov_*`; SLG Equipment/Structures/IP attribution bug still open |
| F03000 | Change in private inventories | not started — #529/#530/#531 |
| F04000 | Exports | not started — #526/#527/#528 |
| F05000 | Imports | not started — #526/#527/#528 |

`derive_initial_Y_pur` returns the three unsourced columns as all-zero.

## Blocking defect: proportional attribution ignores the PCE/PEQ line

`FD_PCE_*` and `FD_IP_equipment` split each NIPA line across commodities using the wrong
weights. Line totals are preserved, cells are not.

Measured for 2017, `derive_initial_Y_pur` against the Use SUT column it should reproduce:

| column | FBS | Use SUT | total diff | cells within 1% |
|---|---|---|---|---|
| F01000 | 13,130,149 | 13,290,627 | −1.21% | 18 of 297 |
| F02E00 | 1,177,593 | 1,159,949 | +1.52% | 9 of 297 |

Per-line test (`Table`/`Code`/`Line` survive into the FBS, so this is measurable directly):

- **A. Every line is fully allocated** — 209 of 209 attributed lines land their NIPA total to
  0.00%. Ratios per line already sum to 1; that is not the defect.
- **B. Shares are wrong** — 278 of 1,387 (line, commodity) pairs within one percentage point.
- **C. Destination is wrong** — of 87 lines the bridge assigns to a single commodity, only 10
  land on it.

Two compounding faults, the first dominant:

**1. Everything is routed through NAICS, and the BEA detail schema does not survive it.**
Both sides are crosswalked to NAICS before the merge — the NIPA line through
`NAICS_Crosswalk_BEA_NIPA_FD_PCE.csv`, the bridge through `BEA_2017_Detail` on the `*bea`
anchor — so in NAICS space 207 of 212 lines do overlap. The 5 that do not are housing:
`531HSO`/`531HST` against crosswalk `531110`.

The round trip itself is lossy. Of the 402 BEA 2017 detail commodities:

| | |
|---|---|
| no entry in `NAICS_Crosswalk_BEA_2017_Detail.csv` at all | 5 — `4200ID`, `S00300`, `S00401`, `S00402`, `S00900` |
| mapping to more than one NAICS | 199 of 402 |

So a NAICS-based `industry_spec` cannot express the BEA detail schema: half of it is aggregates
of several NAICS, and five codes have no NAICS existence. `S00402` is one of the five and is
the worst cell in both columns above. `BEA_detail_target.yaml`'s header claims 1:1
correspondence; that holds as BEA→NAICS-set, not as an invertible code mapping.

Direction (Wes, 2026-07-28): do not map to NAICS at all during FBS creation. That needs the
NIPA crosswalks to target BEA detail codes directly, the `*bea` anchor to stop crosswalking the
bridge, and a target schema that enumerates BEA codes. No FBS method does this today —
`Detail_Use_SUT.yaml` also includes `BEA_detail_target.yaml`, and gets away with it only
because both its sides are BEA tables crosswalked identically, so the round trip is symmetric.

**2. Cross-category weighting.** `proportionally_attribute`
(`bedrock/transform/flowby.py:1214-1226`) merges the source on `PrimarySector` + `Location`
only, and the `FD_PCE_*` sets select the bridge with nothing but `FlowName: "Purchasers'
Value"`. So each commodity's weight is its economy-wide bridge total, not its value in the line
being attributed. For "Musical instruments" (5,337 million) the bridge's split is 339990 91.9%,
316000 7.1%, S00402 1.0%; the applied split is S00402 57.0%, 316000 29.6%, 339990 13.5% —
S00402 is 222,775 million across all categories against 53 million here.

Fix, on branch `nipa_fd_allocation_fix`:

1. Keep the whole FBS in BEA detail commodity space. Retarget the NIPA FD crosswalks to
   `BEA_2017_Detail_Code`, stop crosswalking the bridge, and give the method a target schema
   that enumerates BEA codes rather than NAICS levels. This is new capability, not a config
   change, and it is what makes `S00402`, `4200ID` and the housing codes representable.
2. Key the attribution on the PCE line so each line's ratios come from its own bridge rows:
   either `ActivityProducedBy: <category>` in each set's `attribution_source.selection_fields`,
   or `attribute_on: ['PrimarySector', 'ActivityProducedBy']` per `flowby.py:1300-1327`.
   The `attribute_on` form is preferred — it also lets the ~15 `FD_PCE_*` sets collapse into one
   set selecting all of U20405, which is what #503's common yaml needs anyway.
3. Review the activity mappings while retargeting them (#523's open box), including the 5
   housing lines that have no NAICS counterpart.

Related discussion: cornerstone-data/stateior#5.

Test harness for this work, all measurable without a full pipeline run: per-line full
allocation (currently 209/209 exact — this must not regress), per-line shares against the
bridge's own split, and single-commodity lines landing 100% on their commodity. Compare the
assembled column against the Use SUT with `compare_NIPA_to_IOT`, remembering the FBS is in
dollars and the Use table in millions.

## Refactor to a common yaml (#503, reopened)

`NIPA_FD_2017.yaml`–`NIPA_FD_2024.yaml` repeat every activity_set per year. Refactor onto a
shared `NIPA_FD_common.yaml`, following
https://github.com/cornerstone-data/flowsa/blob/nipa/flowsa/methods/flowbysectormethods/NIPA_FD_common.yaml.
This also closes the per-year rollout gap below — `FD_Equipment` would apply to every year by
construction rather than by copy — and lands naturally with the attribution fix above.

## Two gaps to close first

**Per-year rollout.** `FD_Equipment` is in `NIPA_FD_2017.yaml` only. 2018–2024 have `FD_PCE`
and the `FD_Gov_*`/`FD_Structures*`/`FD_IP_*` blocks but no equipment set, so
`derive_initial_Y_pur(2018..2024)` returns F02E00 as zero. The 2017 set uses NIPA Table
U50505 with `activity_to_sector_mapping: BEA_NIPA_FD_Equipment` and `BEA_PEQBridge`
attribution, reconciling to 0.22%; copying it per year is mechanical.

**Schema granularity.** `Cornerstone_2025_target.yaml`'s `industry_spec` (`default: NAICS_3`)
collapses 77% of rows / 86% of dollar value to 3-digit parents that are not BEA_2017_Detail
codes. `nowcast.py` corrects this after the fact with `map_fbs_sectors_to_model_schema`.
The alternative is to point `NIPA_FD_<year>.yaml` at a raw `BEA_detail_2017` target schema so
no collapse happens. Still undecided; the workaround is in place and tested.

## Validation tooling now available

`compare_NIPA_to_IOT` (#541) compares a NIPA table against a BEA IOT row or column, matching
on codes then names, and reports matched cells, unmatched rows on each side, and per-cell
composition. Its two worked examples are Phase 2's reconciliation targets already built:

- `nipa_compensation_vs_sut_v00100` — NIPA 6.2D against Use SUT `V00100`, 69/69 cells,
  −1 million on $10.4 trillion.
- `nipa_taxes_vs_sut_t00otop` — NIPA 3.5 against `T00OTOP` and `T00TOP`, −9 and −13.

That covers `V00100`, `T00OTOP`, `T00TOP` from the VA table below. The same pattern applies to
`V00300` and `T00SUB`.

It also fixes a premise. The SUT Use table's **cells are at purchaser value**, not basic: total
use `T019` equals the Supply table's purchaser total `T016` (37,094,434) for all 402
commodities, against basic `T013` 36,398,867. Its industry columns are then totalled at both
basic (`T018` 33,772,568) and producer (`T005` + `VAPRO` 34,468,127). So Phase 1's decision to
keep Y in PUR makes it directly comparable to the benchmark Use table's final-demand columns
with no conversion. See `bedrock/analysis/compare_NIPA_to_IOT/About_BEA_IOT_table_valuation_differences.md`.

## Reference: Value Added mapping (Phase 2)

| Component | Code | NIPA table |
|---|---|---|
| Compensation of employees | V00100 | T60200D |
| Other taxes on production | T00OTOP | T30500, excl. taxes on products |
| Gross operating surplus | V00300 | constructed: T61200D + T61400D + T61500D + T61700D + T61300D + T62200D |
| Taxes on products and imports | T00TOP | T30500, taxes-on-products portion |
| Less: subsidies | T00SUB | T31300 |

Reconciliation targets: NIPA Table 1.14 for total VA; each Section-6 table total against the
corresponding Use-table row or group; `VABAS` = V00100 + T00OTOP + V00300 against T10305;
`T018` against GDP via T10105. All five identities are verifiable in the 2017 data with
`compare_NIPA_to_IOT`.

VA arrives before redefinitions and needs the same transform used for gross industry output:
`compute_coproduction_ratios` / `adjust_gross_output` in
`bedrock/transform/iot/derived_gross_industry_output.py` (lines 150–212).

## Reference: unsourced columns

**Trade (#526/#527/#528).** Source not yet picked. Candidates in #527: Census Trade in Goods +
BEA services (existing code in USEEIO's `import_emission_factors/download_imports_data.py`);
BEA ITA Table 2.1 + 3.1 joined via BEA's ITA-to-NIPA linkage table; BACI, framed as
comparison rather than primary. Requirement: match the 2017 detail Use table, annual, and
comparable to BACI.

**Inventories (#529/#530/#531).** NIPA Table 5.7.5B gives holding industry; the Use table's
F03000 is by commodity. Totals match exactly (NIPA 1.1.5 line 14 = Use F03000 total), the
commodity split does not come for free. #530 scopes this explicitly: start from the NIPA totals,
defer commodity attribution by stage of fabrication.

## Phases

### Phase 0 — done
`nowcast.py` scaffolded. The `nowcast.py` vs `bedrock/transform/iot/` boundary is still
undecided in principle, but in practice `nowcast.py` is the orchestrator and
`bedrock/transform/iot/` holds the reusable transforms.

### Phase 1 — final demand, PUR price, BEA_2017_Detail schema
- 1a. PEQ Bridge — done for 2017. **Remaining: roll `FD_Equipment` out to 2018–2024.**
- 1b. F0-code lookup — done, superseded by #539/#540; each activity_set assigns its own
  `SectorConsumedBy`.
- 1c. Per-year Y-PUR assembly — done, `derive_initial_Y_pur(year)`.
- 1d. F04000/F05000 — resolve #527's source pick, then build per #528.
- 1e. F03000 — ship NIPA totals first per #530, defer the commodity split.
- 1f. Validate — use `compare_NIPA_to_IOT` against the benchmark Use table's final-demand
  columns. Y is in PUR and so are those columns, so no conversion is needed.
- 1g. Close #523: review all activity mappings, test against 2017 data.

### Phase 2 — value added
- New `NIPA_VA_<year>.yaml` following the `NIPA_FD_<year>.yaml` pattern, sourcing the 9 tables
  above.
- Allocate to BEA industries using 2017 table ratios.
- Reconcile with `compare_NIPA_to_IOT` against the targets listed above.
- Transform before → after redefinitions with the `adjust_gross_output` pattern.

### Phase 3 — intermediate transactions
- Seed from the dollar Use matrix for the benchmark year
  (`load_2017_Utot_after_redef_usa()` / `_load_2017_detail_supply_use_usa()`), not from `A`.
- Nowcast forward by commodity inflation per #497, porting
  `CalculateIntermediateUseAndCommodityMix.R`'s logic and using
  `bedrock/utils/economic/inflation_helpers_cornerstone.py`. Intermediate uses only,
  after-redefinitions table.

### Phase 4 — schema conversion
Reuse `industry_corresp()` / `commodity_corresp()` in `cornerstone_expansion.py` and
`cfg.iot_before_or_after_redefinition`.

### Phase 5 — RAS
Port [`sut_ras.py`](https://github.com/cornerstone-data/USEEIO/blob/nowcasting/nowcasting/sut_ras.py)
into `bedrock/utils/economic/`. Default control totals are summary-table totals; industry and
commodity gross output are the named alternative. No RAS code exists in bedrock today.

### Phase 6 — validation, storage, pipeline
Port [`check_balances.py`](https://github.com/cornerstone-data/USEEIO/blob/nowcasting/nowcasting/check_balances.py)
into `bedrock/utils/validation/`. Store Make/Use/Import via the normal GCS snapshot path. Wire
into the model build; regenerate snapshots and diagnostics.

## Testing

- Per-phase reconciliation against published totals, using `compare_NIPA_to_IOT` where the
  target is a NIPA table against an IOT row or column.
- Unit tests for the PEQ Bridge parse branch and the ported RAS function (small hand-checkable
  matrices; zero control totals are the edge case).
- Golden-file test per year once Phase 1 stabilises.

## Open questions

1. **Per-year equipment rollout** — copy `FD_Equipment` into 2018–2024 now, or wait until
   F03000/F04000/F05000 land so the years are filled in one pass?
2. **Schema granularity** — now answered by the defect above: point `NIPA_FD_<year>.yaml` at a
   `BEA_Detail_2017` target schema. It takes bridge commodities reachable as candidates from
   31% to 92% and makes 75 single-commodity lines exact, and it removes the need for
   `map_fbs_sectors_to_model_schema` to undo a NAICS_3 collapse afterwards. Remaining question
   is only where the Cornerstone conversion then happens — presumably Phase 4, on the assembled
   table rather than inside the FBS method.
3. **#527 trade source** — Census goods + BEA services, BEA ITA with NIPA linkage, or BACI?
   Determines whether Phase 1d follows the NIPA_FD pattern or needs a new source module.
4. **#530 inventory attribution** — confirm F03000 ships as NIPA-total-only in Phase 1e.
5. **SLG attribution bug** — `FD_Gov_SLG_Equipment` / `_Structures` / `_IP` log "could not
   attribute ... due to lack of flows" against `BEA_Detail_Use_SUT` while the Federal
   equivalents work. Still unrooted; blocks trusting those three columns.
6. **Interim caching** — per-year local cache layout for nowcast products while developing, ahead
   of the GCS destination.
7. **Years in scope** — board says 2018–2024, 2017 is the benchmark. Confirm.
