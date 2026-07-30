# Plan: Revise FBS pipeline to work seamlessly for direct BEA assignment without passing though NAICS guards

Written 2026-07-29 from the working state on `nipa_fd_allocation_fix`.

## The goal
For FBS creation that targets BEA schema sectors that are not NAICS, be able to assign/attribute directly to them and avoid translating the code to NAICS. Clean up existing code that forces NAICS on non-NAICS based codes.

## Prime example
`NIPA_FD_*` builds the final-demand section of the Use table, so its output has to be
indexed by BEA 2017 detail commodity. Today the FBS is built in NAICS space and
converted afterwards by `map_fbs_sectors_to_model_schema`. That conversion is lossy in
both directions and is the common cause behind a long series of individually-diagnosed
bugs. The goal is for the FBS to be natively BEA, end to end, after which that call can
come out of `nowcast.py` entirely.

Three parts, all required
together:

1. the activity to sector mappings target BEA detail codes directly
2. the `*bea` anchor stops crosswalking the BEA side into NAICS
3. the target schema can express BEA codes

## Why this is the root cause, not a series of unrelated bugs

Every defect chased on 2026-07-29 in nipa_fd_allocation_fix reduced to the same thing: a BEA detail code that is
not a NAICS code cannot be a target, so its value is either dropped or renormalised onto
whichever sibling happened to be reachable.

| symptom | code(s) | value |
|---|---|---|
| housing collapsed onto a bogus `531110` | `531HSO` `531HST` `531ORE` | 2,026,714 |
| SLG enterprises split across police/prisons/fire | `S00203` | 72,509 |
| waste unattributed | `562000` | 28,334 |
| 27 equipment commodities at zero, 52 overshooting | 29 codes incl. `33329A` `336500` `541300` | 220,644 (18.8% of the PEQ bridge) |
| F01000 under-allocation, nonprofit/NPISH cluster | `813100` `611A00` `624100` `813A00` … | most of −424,747 |

The scale of it: of the 406 BEA detail codes in
`NAICS_Crosswalk_BEA_2017_Detail.csv`, **210 cannot be reached through any NAICS level**.
BEA detail is mostly aggregates (`611A00`, `813100`, `5241XX`, `52A000`, `1111A0`) that are
not NAICS codes at all.


`d55e8e0` is the enabling fix for everything below. `assign_technological_correlation`
left-merges the naics key against `Sector_Levels` and then does
`targetLength.astype(int) - sourceLength.astype(int)`; a code absent from that table
leaves NaN, `astype(int)` raises `ValueError`, and `prepare_fbs`'s blanket
`except ValueError` discards **every** activity_set. One unregistered code in `non_naics`
silently zeroed the entire method. That is why `non_naics` "didn't work" for `562000`.

## In the working tree, uncommitted, and currently breaking GHG

- `NAICS_Crosswalk_BEA_2017_Detail_identity.csv` (new) — 406 identity rows, domain taken
  from the existing crosswalk's `Activity` column
- `NAICS_Crosswalk_BEA_NIPA_FD_PCE.csv` retargeted to BEA codes: 2115 → 703 rows, all 259
  sectors BEA. Fully mechanical: 234 already BEA, 1881 resolved to exactly one BEA code,
  0 ambiguous, 0 unmappable, and the `Note` column independently agreed on all 2020
  comparable rows
- `BEA_detail_target.yaml` — 233 `non_naics` codes; `Cornerstone_2025_target.yaml` and
  `CEDA_2025_target.yaml` — 22 each
- **22 rows removed from `NAICS_2017_Crosswalk.csv`** (531 family, `S00401`/`S00402`, 12
  BEA construction codes, `GSLG*`, `S00500`, `S00600`)
- `NIPA_FD_2017.yaml` — two anchors: 9 bridge blocks on `*bea_identity`, 17 Use-SUT
  blocks still on `*bea`
- `S00203` identity row removed from the shared crosswalk (redundant under this
  architecture; the `9221*`→`GSLGO` fix in `29b62f0` stands on its own)

### Measured, NIPA (`pce_alloc.py`, cells within 1% against the 2017 Use SUT)

| | before today | now |
|---|---|---|
| F02E00 | 14 / 300 | **95 / 306** |
| F01000 | 237 / 300 | 238 / 306 |
| F01000 total | −3.20% | −2.98% |

`562000` finally lands, exactly (+28,334). With the two-anchor split every other
final-demand column is at **zero delta** — verified column by column, so the equipment
win costs nothing elsewhere.

### The blocker: this breaks GHG

`GHG_national_Cornerstone_2024` before vs after: totals identical to the cent, but 8695 →
8355 rows, 3920 of 4034 groups moved, 466 sectors affected, and **15 sectors vanish**:

```
230301 230302 233210 233230 233240 233262 2332A0 2332C0 2332D0 233411 233412 2334A0
531HSO 531HST 531ORE
```

`531ORE` loses 60.4bn, `GSLGE` gains 18.2bn. The identical grand total is precisely the
trap `About_BEA_IOT_table_valuation_differences.md` warns about — a totals check cannot
see a distribution error.

This is **not** a target-schema failure. All three schemas were verified to resolve those
22 codes to themselves.

## Hypothesis for the GHG breakage — confirm before acting

`NAICS_2017_Crosswalk.csv` is not only a target-selection table. Its rows carry the NAICS
**hierarchy** (`53,531,531HSO,531HSO,531HSO` places `531HSO` under `53`), and the
aggregation machinery walks that hierarchy. Removing the rows orphans the codes.

Supporting evidence: the two hierarchy tables are now inconsistent. `Sector_Levels` still
registers every code removed from the NAICS crosswalk —

```
531HSO [4,5,6]   230301 [4,5,6]   GSLGE [2,3,4,5,6]   S00401 [2,3,4,5,6]
```

**Next diagnostic, before any further change:** `bedrock/utils/mapping/naics.py` lines
320, 370 and 411 are three unexamined functions that call `return_naics_crosswalk`.
Establish exactly how each treats a code present in `Sector_Levels` but absent from
`NAICS_2017_Crosswalk`. That pins the mechanism and decides the question below.

## The open design question

Either:

- **A.** remove the non-NAICS BEA codes from `Sector_Levels` as well, and make the
  hierarchy functions handle `non_naics` codes as their own roots; or
- **B.** keep both hierarchy tables populated and treat them as the registry of "codes
  that exist", with `non_naics` governing only target selection.

A is the coherent end state and matches the intent ("these are not NAICS, stop pretending
they are"). B is less invasive. The answer depends on what those three functions do.

Note the constraint that forced this open: while a code remains in
`NAICS_2017_Crosswalk`, `non_naics` cannot be its sole route — the melt also produces a
target for it (`531HSO` → `531` under the `NAICS_3` default), giving two targets and
splitting every flow 50/50. That was observed directly on housing: each BEA code received
exactly half its correct value alongside a full-value bogus `531110`.

## The NAICS guards

The pipeline treats "sector" and "NAICS code" as the same thing in a number of places. Each
is a guard that a non-NAICS target has to get past. This inventory is the actual scope of
the plan; `non_naics` is currently the only escape hatch and it is not honoured
consistently downstream of `industry_spec_key`.

| guard | file | what it assumes | status |
|---|---|---|---|
| target selection | `naics.py:67` `industry_spec_key` | targets come from melting the NAICS crosswalk; `non_naics` appends identity rows | works, but is the *only* place that knows about `non_naics` |
| tech correlation | `sectormapping.py:98` | every source and target code is registered in `Sector_Levels` | **fixed** `d55e8e0` — was a `ValueError` swallowed into an empty method |
| hierarchy walkers | `naics.py:320`, `:370`, `:411` | codes have NAICS parents/children in `NAICS_2017_Crosswalk` | **unexamined — the prime suspect for the GHG breakage** |
| sector levels | `flowbysector.py:287` | `Sector_Levels` covers the code | unexamined |
| crosswalk source year | `flowbyactivity.py` ~520 | `SectorSourceName` parses as `NAICS_<year>_Code`, then rows are filtered to that literal | forces every activity-to-sector crosswalk to *declare* NAICS even when its sectors are BEA codes |
| activity schema branch | `flowbyactivity.py` ~486 | `"NAICS" in activity_schema` selects the whole mapping path | non-NAICS sources take the crosswalk path by accident, not by design |
| model schema mapping | `allocation/derived.py:200` | NAICS_3/4/5 expand to NAICS_6 before mapping | to be removed for `nowcast.py`, see ordering |
| out-of-tree | `analysis/time_series_B_matrix/derive_B_time_series.py:257` | reads the NAICS crosswalk directly | check, do not assume |

The `SectorSourceName` guard is the clearest illustration of the problem. The new
`NAICS_Crosswalk_BEA_2017_Detail_identity.csv` maps BEA codes to themselves, yet every row
must still say `NAICS_2017_Code` or the loader drops it. The pipeline has no way to express
"these sectors are BEA detail codes", so a correct crosswalk is forced to misdeclare
itself. A real fix probably means allowing a non-NAICS `SectorSourceName` and carrying the
schema through, rather than adding more codes to the NAICS tables.

## Ordering

### Phase 1 — make the pipeline honour non-NAICS sectors

This is the general fix and everything else depends on it. Done properly, it should need no
new entries in `NAICS_2017_Crosswalk.csv` at all.

1. Diagnose `naics.py:320/370/411` and `flowbysector.py:287`: how does each treat a code
   present in `Sector_Levels` but absent from `NAICS_2017_Crosswalk`? This answers
   **A-vs-B** above and confirms or kills the GHG hypothesis.
2. Make the hierarchy and level machinery treat a `non_naics` code as its own root —
   no parent, no children, length equal to its own — rather than requiring a crosswalk row.
   Prefer this over registering BEA codes in the NAICS tables: the registration approach is
   what produced the dual-target 50/50 split and the `'531'`/`'23'`/`'92'` hacks in the
   `NAICS_6` lists.
3. Decide whether `SectorSourceName` can express a non-NAICS schema, so an identity
   crosswalk stops having to declare `NAICS_2017_Code`.
4. **Gate:** GHG A/B returns to zero delta *with the BEA codes present*, and the
   `'531'`/`'23'`/`'92'` entries can come out of the `NAICS_6` lists without anything
   moving. Nothing in Phase 2 lands before this passes.

### Phase 2 — NIPA_FD as the first consumer

5. Retarget `FD_Gov` (55 rows), `FD_Structures` (67), `FD_IP` (52). Not mechanical, unlike
   `FD_PCE`: they name truncated NAICS prefixes (`31`, `32`, `33`, `5415`, `5417`,
   `32192`, `5121`, `5151`) which need expansion, construction is genuinely ambiguous
   (`238990` → 11 BEA codes, because BEA splits structures by *type* not by NAICS
   industry — the NIPA line name is the only disambiguator), and `FD_Gov` contains a junk
   row mapping a sector to the final-demand code `F10C00`.
6. Collapse the two anchors back to one, once every NIPA crosswalk targets BEA codes.
7. Remove `map_fbs_sectors_to_model_schema` from `nowcast.py`. Do not do this earlier: it
   currently takes the FBS from 26% to 79% BEA-native (5,044,579 → 15,610,313 M), so
   removing it before step 5 is a severe regression. Wes noted 2026-07-29 that it also
   shifts sectors *into* the Cornerstone schema — 12 of its 409 activities are not BEA
   detail codes (`562HAZ`, `562OTH`, `562111`, `562212`, `562213`, `562910`, `562920`,
   `331314`, `S00101`, `S00201`, `S00202`, `F01000`), Cornerstone's waste disaggregation
   being why the whole 562 family has been pathological.
8. Roll out to `NIPA_FD_2018`–`2024`, ideally via the `NIPA_FD_common.yaml` refactor
   (#503) rather than eight copies.

### Phase 3 — retire the workarounds elsewhere

9. `Detail_Use_SUT`, `Detail_Supply`, `Detail_Make`, `Detail_Use` and the 13 `CAP_HAP_*`
   methods all include `BEA_detail_target.yaml` and use the `BEA_2017_Detail` crosswalk.
   They get away with NAICS round-tripping today only because both their sides are BEA
   tables crosswalked identically, so the loss is symmetric. Once Phase 1 lands they can
   move to the identity crosswalk and stop relying on that symmetry.
10. Remove the `'531'`, `'23'`, `'92'` entries from the `NAICS_6` lists in all three target
    schemas — they exist only to reach BEA codes through a NAICS level.

## Data files in scope

| file | role |
|---|---|
| `naics/NAICS_2017_Crosswalk.csv` | the NAICS hierarchy. **Goal: stop adding non-NAICS codes to it** |
| the `Sector_Levels` crosswalk | code → level. Same goal |
| `common/{BEA_detail_target,Cornerstone_2025_target,CEDA_2025_target}.yaml` | target schemas / `non_naics`. CEDA is slated for deletion — edit for consistency, do not spend verification on it |
| `NAICS_Crosswalk_BEA_2017_Detail.csv` | BEA → NAICS. Leave it doing its job; do not patch identity rows into it one code at a time |
| `NAICS_Crosswalk_BEA_2017_Detail_identity.csv` | new, BEA → itself |
| `NAICS_Crosswalk_BEA_NIPA_{FD_Gov,FD_Structures,FD_IP}.csv` | still name NAICS; Phase 2 |

## Acceptance criteria for Phase 1

A method can name a non-NAICS sector as an attribution target and:

1. it is not dropped, renormalised onto a sibling, or split across duplicate targets;
2. no row is added to `NAICS_2017_Crosswalk.csv` or `Sector_Levels` to make it work;
3. nothing raises — and if something does, it surfaces rather than being swallowed into an
   empty method (`d55e8e0` covers the known instance; there may be other blanket handlers);
4. every existing method is unchanged, verified per column and per sector, not on totals.

## Verification

- `pce_alloc.py` — F01000/F02E00 cells within 1% vs the Use SUT. The standing metric.
- `nowcast_initial_Y_pur_baseline.py` — per-column totals, and the three-way bridge
  comparison from `4434f7e`. The three-way view is what separates "our attribution is
  wrong" (`ours_minus_bridge`) from "the source data disagrees"
  (`bridge_minus_use_sut`). For 2017: F01000's gap is **entirely ours** (the PCE bridge
  reproduces the Use table to −6 M on 13.3 trillion, 258 of 259 commodities); F02E00 is
  mostly ours plus a real +15,031 bridge-vs-Use residual.
- **A per-column delta table against the previous state, every time.** A grand total is
  not sufficient — the GHG regression preserved the total exactly.
- GHG A/B via `git stash`, comparing rows, total, *and* the set of sectors present. Row
  count and sector-set changes are the signal; the total is not.

## Notes for whoever picks this up

Mistakes made on 2026-07-29, worth not repeating:

- The anchor was flipped before its preconditions were met, leaving the tree down 2.27
  trillion (`F10C00` to a hard zero) for several runs. Build the safe intermediate first.
- A CRLF-insensitive regex silently no-op'd on `Cornerstone_2025_target.yaml` and
  `CEDA_2025_target.yaml`, leaving codes unreachable after their crosswalk rows had
  already been removed. These files are CRLF; assert that edits changed something.
- A `\r\n`-split read of a `git show` output (which is LF) recovered zero rows and deleted
  a crosswalk entry without replacing it. Rebuild from source rather than patching over
  damage.
- Verify at the level the pipeline actually operates on. An early equipment analysis
  compared the raw bridge against the crosswalk, found perfect agreement, and cleared a
  hypothesis that was in fact correct — the divergence happens *after* the bridge is
  crosswalked.
