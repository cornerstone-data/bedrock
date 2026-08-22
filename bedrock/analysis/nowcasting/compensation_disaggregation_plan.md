# Plan — disaggregating compensation (`V00100`) to BEA 2017 detail

Step 2 of [`plan.md`](plan.md). Covers the largest value-added row: `V00100` is
**10,434,981** million in 2017, **55.2%** of `VABAS`.

Filed here rather than `.claude/plan/` because nowcast planning documents live
beside the code they describe — see the note at the end of `plan.md`.

---

## What is already settled

From [#537](https://github.com/cornerstone-data/bedrock/issues/537), reproducible
via `uv run python -m bedrock.analysis.nowcasting.value_added_control_totals`:

- The control totals reconcile. `V00100` meets NIPA T1.10 `A4002C` within
  **3 million** on 10.4 trillion.
- `T60200D` (6.2D, compensation by industry) has **74 leaf rows** — about BEA
  summary granularity. Nothing in NIPA reaches the 402 detail industries, so
  detail is an allocation problem, not a lookup.
- **No value-added code is in the sector crosswalk yet.** Once a gate; now
  three CSV lines, since #567/#568 closed — see
  [Phase 0](#phase-0--the-crosswalk-gate--cleared).

## The shape of the build — settled 2026-08-22

**Three FBS methods, one per Use row**, not one method and not plain Python
modules: `NIPA_VA_compensation_<year>`, `NIPA_VA_surplus_<year>`,
`NIPA_VA_othertax_<year>`. Reasoning is in [`plan.md`](plan.md) §Step 2; the two
consequences for *this* document are what follow.

**Most of this plan survives, but as configuration rather than code.** The
attribution engine already does NIPA-leaf → BEA-detail with weights, and
`BEA_Detail_Use_SUT` melts the Use SUT through `VAPRO`, so the 2017 benchmark
`V00100` by industry is loadable as an FBA attribution source today — Phase 1
below is a `selection_fields` clause, not an extraction.

⚠️ **One step has no FBS primitive: the anchor-and-move itself.** "2017 detail
share × QCEW growth, renormalised in-parent, then the NIPA control" cannot be
said in yaml, because `multiplication` does not preserve the group total and
there is no renormalise step — which is exactly what Phase 3.4 needs. So:

> **Phases 1–2 build a cached `FBS_outside_flowsa` source** (the `FBS_datapull_fxn`
> hatch, as `stewiFBS_common.yaml` uses) whose FlowAmount per BEA detail industry
> is `V00100_2017,d × QCEW_growth,d`. **Phase 3 is then a single `proportional`
> attribution of the NIPA control against it.** Proportional normalises within
> the group, so 3.4's exact rescale holds by construction rather than by a
> follow-up step, and the arithmetic stays readable in Python instead of becoming
> forty lines of nested `multiplication`/`division`.

That seam is the whole design. Everything else is ordinary activity sets.

**Orientation is the transpose of the final-demand methods.** VA codes are Use
*rows*, so the code goes on `SectorProducedBy` and the industry on
`SectorConsumedBy` — where `NIPA_final_dom_uses` puts the commodity on
`SectorProducedBy` and the `F` code on `SectorConsumedBy`. That needs a mirror of
`assign_sector_consumed_by_from_clean_parameter`, for the same reason the
original exists: `BEA_NIPA` is a `TECHNOSPHERE_FLOW` source, so populating the
sector column before attribution would capture `PrimarySector` and corrupt the
weights. Assign after aggregation, as #539 established.

## The decision that shapes everything else

**Disaggregate wages and supplements separately, then sum. Do not allocate total
compensation by wage shares.**

Supplements-to-wages ratios vary systematically across industries: legally
required contributions are capped and therefore regressive relative to wage
level, while pension and health contributions vary with unionization and firm
size. Allocating *total* compensation on wage shares systematically overstates
high-wage children and understates low-wage ones.

This is cheaper than it sounds, because **NIPA publishes both halves by
industry**, and the identity is exact:

✅ **Restated on the *paid* concept (#536).** The version below originally read
each table's line 1 and left an unexplained ~10,600 against the SUT. 6.2D and
6.3D each state their total **twice** — line 1 is compensation (or wages)
*received by residents*, line 2 the amount *paid* by domestic industries and
government — and value added wants the paid line. The supplements tables have
no such split. On line 2 the identity is exact **and** lands on the SUT:

| | table | line | code | 2017, $M | industry lines |
|---|---|---:|---|---:|---:|
| Wages and salaries, paid | `T60300D` (6.3D) | 2 | `A4102C` | 8,485,016 | 74 |
| Employer contributions, government social insurance | `T61000D` (6.10D) | 1 | `B039RC` | 604,656 | 16 |
| Employer contributions, pension and insurance funds | `T61100D` (6.11D) | 1 | `B040RC` | 1,345,306 | **17** |
| **sum** | | | | **10,434,978** | |
| Compensation of employees, paid | `T60200D` (6.2D) | 2 | `A4002C` | 10,434,978 | 74 |
| **difference** | | | | **0** | |

Verified to the dollar, and 10,434,978 against the Use SUT's `V00100` of
10,434,981 is BEA's own rounding. So supplements need a parent-ratio carry-down
only *below* 16 and 17 industries respectively — not from the top.

> ✅ **The ~10,600 gap is closed, and it was a wrong-line error.** 6.2D line 97
> `A4187C` is the rest-of-world adjustment at −10,607 (receipts 6,347 less
> payments 16,954, lines 98–99), and `A4002C` − `A033RC` equals it exactly.
> Same class of mistake the `V00300` assembly warns about three times over:
> *take the domestic line, not the table's root.* Pinned by
> `test_bea_nipa_value_added_tables.py`.

⚠️ **6.11D is 17 industries, not 36, and reading the whole table double-counts.**
The table is three panels, each restating the 1,345,306 total under the same
code `B040RC`: lines 1–20 by industry, 22–36 by *type of fund*, 37–45 **benefits
paid** (2,370,770 — a different concept entirely). The "36 leaves" above counted
the type panel. So pension and insurance supplements are the *coarsest* piece of
compensation, not the second-finest, and any method reading 6.11D must select
lines 3–20 explicitly — the same hazard as U20405's memorandum block.

⚠️ **Four of the value-added tables restate a code on more than one line** —
`T61100D`, `T61600D`, `T71100`, `T11400`. Select by line there, not by code.

## Data sources, all already in bedrock

| source | what it gives | status |
|---|---|---|
| `BLS_QCEW` | `total_annual_wages` → `Annual payroll`, **Class `Money`, USD**, NAICS | exists; `estimate_suppressed_qcew` already handles suppression |
| `Employment_national_<year>` | QCEW run to **NAICS_6** national | exists, 2002–2023 |
| `Census_VIP` | Value of construction put in place, **by structure type** | exists, with `NAICS_Crosswalk_Census_VIP.csv` and `VIPNametoNAICStoFF.csv` |
| `USDA_ERS_FIWS` | farm income and expenses incl. hired and contract labor | exists, with crosswalk |
| NIPA `T71800` | Table 7.18, *Relation of Wages and Salaries in the NIPA to Wages and Salaries as Published by the BLS* | in `FlatFiles.ZIP` |

**QCEW is an allocator, never a control.** It covers UI-covered employment and
wages: no supplements, and known gaps in agriculture and private households.
It distributes a NIPA total; it does not replace one. `T71800` documents exactly
what BEA adds on top (uncovered workers, misreporting, tips, timing), and is the
reference for how far raw QCEW shares can be trusted.

---

## Phase 0 — the crosswalk gate ✅ **cleared**

This phase was written as a gate and is no longer one. **#567 and #568 are both
closed**, so `SectorSourceName` can express a non-NAICS schema and a `non_naics`
code is its own root in the hierarchy machinery. The crosswalk — now
`Sector_Crosswalk_BEA_2017_Detail.csv` — already carries `F01000`, `S00300` and
`S00900` as identity rows declaring `SectorSourceName: BEA_2017_Code`, and
`test_mixed_bea_naics_assignment.py` covers the mixed BEA/NAICS case.

What remains is not a decision, it is three CSV lines:

- **0.1** Add identity rows for `V00100`, `T00OTOP` and `V00300` under
  `BEA_2017_Code`, following `F01000`'s row exactly.
- **0.2** `VABAS`/`VAPRO`/`T018` are subtotals and must **not** get rows — they
  are computed from the three, not targeted.

Nothing here blocks the later phases any more.

## Phase 1 — establish the benchmark detail structure

The anchor is BEA's own detail compensation, not a QCEW level. Using raw QCEW
levels would import a definitional discontinuity; using QCEW *shares* under a
NIPA control does not.

- **1.1** Extract 2017 detail `V00100` (402 industries) from the Use SUT — already
  available via `nowcasting.value_added_control_totals.sut_value_added_totals`,
  needs a per-industry variant.
- **1.2** Build the detail→summary map and compute, for each summary industry,
  each detail child's share of compensation.
- **1.3** Same for wages alone. **Requires a detail wages series**; the SUT has
  only combined `V00100`. If none exists, derive detail wage shares by applying
  the summary wages/compensation ratio, and record it as an assumption.
- **1.4** Assert the shares sum to 1 within each summary parent.

## Phase 2 — the movement series

✅ **QCEW is cached locally as per-year FBA parquets** in
`extract/input_data/BLS_QCEW/`, 2017–2023, because generating the FBA is slow —
each year is ~9M rows and 23MB, since QCEW comes down at county grain. Dropping
one into `extract/output_data/` makes `getFlowByActivity('BLS_QCEW', year)` find
it through the ordinary "import local" path with no code change; esupy matches on
name and ignores the `v2.0.4` version tag in the filename. Verified for 2017.

⚠️ **The cache is 2017–2023, so the nowcast's 2024 year has no QCEW.** Whatever
carries 2024 is a separate decision, not an oversight to discover later.

**What the national slice actually holds**, measured rather than assumed:

| | 2017 |
|---|---:|
| National `Class: Money` rows (`Location == '00000'`) | 4,545 |
| …at NAICS-6 | 1,937 |
| distinct NAICS-6 codes | 1,075 |
| NAICS-6 payroll, all ownerships | 7,955,155 $M |
| **as a share of NIPA wages paid** (`A4102C`, 8,485,016) | **93.8%** |

✅ **93.8% is the number that settles "allocator, never control".** The missing
6.2% is UI-uncovered employment, and it is not spread evenly — it concentrates in
exactly the sectors Phase 4 carves out. `T71800` itemises what BEA adds on top.

✅ **Ownership is on the flow, not a separate axis.** `FlowName` is
`Annual payroll, {Private, Federal Government, State Government, Local Government}` —
2017 NAICS-6: private 6,772,575, local 691,481, state 265,682, federal 225,417.
Useful, but it still does **not** separate government *enterprises* from general
government, which is why Phase 4 routes government through NIPA instead.

✅ **Crosswalk coverage is 1,027 of the 1,048** NAICS-6 codes the BEA detail
crosswalk names — 98%. The 21 missing are a bounded list to inspect, not a
structural gap.

⚠️ **`Employment_common.yaml` cannot be reused as-is.** Every one of its
`_bls_selection_fields_*` blocks hardcodes `Class: Employment`, so a wages method
needs its own selection block; `estimate_suppressed_qcew` and
`clean_qcew_for_fbs` are reusable, and whether the first behaves on `Class: Money`
is still open question 3.

- **2.1** Add a `Class: Money` national selection block beside the Employment
  ones, and confirm `estimate_suppressed_qcew` behaves on it. **National 6-digit
  has few suppressions, but the fallback to 5-digit with residual allocation must
  be verified, not assumed.**
- **2.2** Map NAICS_6 → BEA detail via the existing concordance, checking the 21.
- **2.3** Compute per-detail-sector wage growth 2017→target year.
- **2.4** Update the Phase 1 benchmark shares by that growth; renormalise within
  each summary parent. This and 2.3 are the body of the `FBS_datapull_fxn`
  described in §The shape of the build.


### The 6.2% gap has a shape, and NIPA states it

✅ **`T71800` closes the gap exactly**, and says what kind of money it is:

| | code | 2017, $M |
|---|---|---:|
| BLS published wages | `BA06RC` | 7,968,336 |
| + adjustment for misreporting on employment tax returns | `BA07RC` | 106,273 |
| + wages not, or not fully, covered by unemployment insurance | `W873RC` | 399,801 |
| — of which government | `W787RC` | 152,442 |
| — of which other | `W786RC` | 247,359 |
| + timing adjustment for accrual basis | `Y663RC` | 0 |
| **= NIPA wages and salaries, received** | `A034RC` | **8,474,410** |

Exact. Add the rest-of-world adjustment and it is `A4102C`, the paid concept the
build actually wants.

⚠️ **And the gap is already known to be non-uniform before any industry table is
opened.** Government's uncovered rate is **11.3%** of government wages against
private's **3.5%** — a factor of three. Spreading 6.2% pro rata is therefore
measurably wrong, not merely inelegant.

### 6.4D makes coverage measurable per industry

**`T60400D` is the table that turns the assumption into a measurement.** QCEW
publishes employment on the same axis, so `QCEW / NIPA` is a coverage ratio *per
industry* rather than one economy-wide number. Measured for 2017 on private
employment, thousands:

| NIPA 6.4D private line | 6.4D | QCEW | QCEW/NIPA |
|---|---:|---:|---:|
| Farms | 819 | 818 | 99.9% |
| Construction | 7,127 | 6,919 | 97.1% |
| Manufacturing | 12,440 | 12,407 | 99.7% |
| Wholesale trade | 5,934 | 5,899 | 99.4% |
| Retail trade | 15,989 | 15,854 | 99.2% |
| Health care and social assistance | 19,576 | 19,322 | 98.7% |
| Accommodation and food services | 13,711 | 13,607 | 99.2% |
| **Educational services** | 3,662 | 2,824 | **77.1%** |
| **Other services, except government** | 7,042 | 4,435 | **63.0%** |

✅ **This is the useful result: coverage is 97–100% across most of the economy and
collapses in exactly two places.** Religious and grantmaking organisations are
largely UI-exempt, which carries "other services"; private households `814000` is
288.5 thousand employees and 7,295 $M of payroll in QCEW against a sector NIPA
states outright. So QCEW growth can be trusted broadly, and the exceptions are a
**named short list** — which is a different and much better method than a flat
6.2% haircut.

⚠️ **Construction at 97.1% is a warning, not a reassurance.** The *count* matches
because QCEW and NIPA agree on how many construction workers there are. They
disagree on how to classify them — trade versus structure type — and that error
is invisible to a coverage ratio. Phase 4's treatment stands.

### Three more lookups NIPA publishes outright

- **`RfHhInstComp` `W151RC` = 18,684**, compensation of employees of private
  households — the SUT's `814000` to the dollar. The plan called this "an
  explicit domestic-worker compensation line" without locating it; this is it.
  The sector QCEW covers worst is the one NIPA hands over directly.
  `W152RC` is the nonprofit institutions counterpart at 871,882.
- **`U32500` (3.25U)** splits general government compensation into wages
  (1,233,594) and supplements (535,640), which 3.10.5 only totals. The
  `GSLGE`/`GSLGH`/`GSLGO` work needs the split.
- **`T60600D` (6.6D)**, wages per full-time-equivalent employee by industry. Not
  a detail source — it stops at the same 74 industries as 6.3D — but it is the
  plausibility check Phase 5 lacks. An implied wage per worker at BEA detail that
  falls outside its parent's range is an error a shares-sum-to-one assertion
  cannot see.

⚠️ **These tables are not all money, and the extractor now knows it.** 6.4D/6.5D
are thousands of *persons* and 6.6D is a ratio, where `bea_nipa_parse` used to
apply a flat `× 1,000,000` and label everything `Money`/`USD`. Scale and unit now
come from each series' own `MetricName`/`DefaultScale`. All 1,812 dollar rows are
bit-identical across the change; 1.14's three chained-dollar lines moved to
`Class: Other` so a `Class: Money` selection cannot add real dollars to nominal.

## Phase 3 — apply controls and assemble

This is `NIPA_VA_compensation_<year>.yaml`, and it is three activity sets plus the
special cases of Phase 4. Each is a `proportional` attribution of a NIPA control
against a weight source, which is why the exact rescale that used to be 3.4 is
now a property of the method rather than a step in it.

- **3.1** Wages. Control is 6.3D **line 2** (`A4102C`, the paid concept — see the
  table above); weight source is the Phase 2 moved-share `FBS_outside_flowsa`.
- **3.2** Supplements. Controls are 6.10D (16 industries) and 6.11D
  (**17**, lines 3–20 only — the type and benefits-paid panels must be excluded
  or the row double-counts). Weight source is the **wage** distribution from 3.1,
  not total compensation, per the decision above. Carrying down by wages is what
  makes this cheaper than it looks, since both halves are published by industry.
- **3.3** `V00100` detail = 3.1 + 3.2, which is the method's output rather than a
  separate step: the two activity sets aggregate onto the same
  `SectorProducedBy = V00100` rows.
- **3.4** ~~Rescale so detail sums exactly to the summary control.~~ **No longer a
  step.** `proportional` normalises within the attribution group, so detail sums
  to its control by construction. Keep it as the Phase 5.1 assertion — a
  regression guard, not a computation.

## Phase 4 — the sectors where QCEW does not work

These are not edge cases; they are ~15% of compensation and they will produce
visibly wrong answers if run through the general path.

**Construction — the big one.** BEA's 12 detail construction sectors are defined
by *type of structure*; QCEW classifies establishments by *trade*. A specialty
trade contractor's wages spread across every structure type.

⚠️ **Correction: a mapping does exist, and that is the trap, not the relief.**
`Sector_Crosswalk_BEA_2017_Detail.csv` carries **236 rows** for NAICS `23*`, and
construction employment is available at those NAICS. But it is dense
many-to-many — NAICS `237210` alone reaches nine BEA construction codes — so the
crosswalk will happily route QCEW through it and the FBS `equal` default will
split evenly. The mapping's existence means the pipeline **will not fail**; it
will produce an even split and call it an answer. Construction therefore needs a
weighted attribution with a stated weight source, never a bare crosswalk hop.

```
233210 Health care structures          2332A0 Office and commercial
233262 Educational and vocational      2332C0 Transportation, highways, streets
233230 Manufacturing structures        2332D0 Other nonresidential
233240 Power and communication         233411 Single-family residential
230301 Nonres. maintenance and repair  233412 Multifamily residential
230302 Residential maintenance/repair  2334A0 Other residential
```

Use **`Census_VIP`** as the movement variable — value put in place *is* by
structure type, which is the axis BEA uses. Fallback: hold benchmark shares
fixed. This assumes compensation tracks output composition within construction.

**Zero by construction.** `531HSO` owner-occupied housing has `V00100` = **0**,
verified. No share formula may assign it anything.

**Imputed / unmappable real estate.** `531HST` tenant-occupied housing (18,920)
and `531ORE` other real estate (93,508) cannot be separated from QCEW 531 wages.
Hold benchmark shares.

**Farm.** The 10 detail farm sectors under `111`/`112` have weak QCEW coverage
(UI exemptions for small farms). **`T70305`** gives the sector control —
compensation 30,857, and its wages/supplements split — and **`USDA_ERS_FIWS`**
hired and contract labor expense is the movement series within it.

**Private households — `814000`** (18,684). ✅ **The line is located:**
`RfHhInstComp` `W151RC`, 18,684 — the SUT figure to the dollar, so this is a
lookup, not an allocation. "Outside QCEW scope entirely" is slightly too
strong — QCEW has 288.5 thousand household employees and 7,295 $M of payroll —
but coverage is poor enough that the NIPA line wins outright.

**Government — and this one is nearly solved.** QCEW *does* carry ownership, on
the flow rather than as a separate axis: `FlowName` is `Annual payroll,
{Private, Federal Government, State Government, Local Government}`, giving
225,417 federal / 265,682 state / 691,481 local at NAICS-6 in 2017. What it
still does not do is distinguish government *enterprises* from general
government — and that, not the ownership split, is the distinction the SUT
needs. NIPA has it, and the numbers tie exactly, so route government through
NIPA and leave the QCEW ownership codes out of it.

**`T31005` (Table 3.10.5, *Government Consumption Expenditures and General
Government Gross Output*)** carries compensation of general government
employees, and it matches the SUT to the dollar:

| NIPA line | code | $M | BEA detail | $M | diff |
|---|---|---:|---|---:|---:|
| Federal, national defense | `B237RC` | 246,097 | `S00500` | 246,097 | **0** |
| Federal, nondefense | `W130RC` | 184,220 | `S00600` | 184,220 | **0** |
| Federal, both | `B568RC` | 430,318 | `S00500`+`S00600` | 430,317 | 1 |
| State and local | `B251RC` | 1,338,917 | `GSLGE`+`GSLGH`+`GSLGO` | 1,338,916 | 1 |

**`S00500` and `S00600` are 1:1 lookups — no allocation at all.** Only the
state-and-local trio needs splitting, inside an exact control.

Government *enterprises* tie exactly too, against `T60200D`'s own lines:

| NIPA line | code | $M | BEA detail | $M | diff |
|---|---|---:|---|---:|---:|
| Federal enterprises | `A4081C` | 59,219 | `491000`+`S00101`+`S00102` | 59,219 | **0** |
| State and local enterprises | `B4086C` | 107,032 | `S00201`+`S00202`+`S00203` | 107,032 | **0** |

Tables 3.2 and 3.3 are receipts-and-expenditures statements and do **not** carry
compensation — 3.10.5 is the one that does.

### `T30800` decomposes the government enterprise surplus by enterprise

**Table 3.8, *Current Surplus of Government Enterprises*** — this corrects the
claim above that the −4,253 surplus line has no industry axis. It does, and the
enterprise names map onto the BEA codes almost directly:

| NIPA enterprise | $M | BEA detail |
|---|---:|---|
| Postal Service | −1,994 | `491000` |
| Tennessee Valley Authority | 2,774 | `S00101` federal electric utilities |
| FHA + other federal | 2,300 | `S00102` |
| Public transit | −48,988 | `S00201` |
| Gas and electricity | 10,970 | `S00202` |
| water/sewerage, toll, liquor, terminals, housing, other | 30,685 | `S00203` |

Federal sums to 3,080 and state and local to −7,333, together the −4,253 in the
`V00300` assembly — exact at every level. This is *current surplus*, a `V00300`
component, not compensation; but it means the six government enterprise codes
have a published source for that line rather than needing a spread.

The remaining government codes, which still need a split within an exact
control:

| enterprises | general government |
|---|---|
| `491000` postal (54,249) | `S00500` federal defense (246,097) |
| `S00101` federal electric utilities (1,997) | `S00600` federal nondefense (184,220) |
| `S00102` other federal enterprises (2,973) | `GSLGE` S&L education (731,648) |
| `S00201` S&L passenger transit (26,850) | `GSLGH` S&L hospitals/health (139,316) |
| `S00202` S&L electric utilities (10,203) | `GSLGO` S&L other (467,952) |
| `S00203` other S&L enterprises (69,979) | |

So the open government question is narrow: **split `GSLGE`/`GSLGH`/`GSLGO`
within 1,338,917, and each enterprise trio within its exact control.** Nothing
here needs QCEW ownership codes. Whether NIPA's by-function tables (`T31505`,
which carries education and health functions) can do the state-and-local
three-way split is the next thing to check.

## Phase 5 — validation

- **5.1** Detail sums to the summary control exactly. True by construction in
  3.4, so assert it to catch regressions rather than to prove correctness.
- **5.2** **Negative implied gross operating surplus is the real test.** Once
  `V00300` and taxes are added, every detail sector's components must fit inside
  its value added. A negative implied GOS is the usual symptom of a bad
  compensation share, and it shows up most often in exactly the construction and
  real-estate splits Phase 4 covers. **This catches errors 5.1 cannot.**
- **5.2b** **Implied wage per worker, against 6.6D.** Divide detail `V00100` by
  a detail employment estimate and check it sits inside its 6.6D parent's
  range. Catches a bad share that 5.1 cannot see and 5.2 only sometimes can:
  a share can be positive, sum to one, and still imply an implausible wage.
- **5.3** Benchmark replay: run the whole pipeline for 2017 and diff against the
  published detail `V00100`. With 2017 as both anchor and target the shares are
  the identity, so this tests the plumbing, not the movement series — a
  necessary but weak check, and it should be labelled as such.
- **5.4** Replay for a year with a published answer other than 2017 if one
  exists; otherwise the movement series is untested against ground truth and
  that limitation should be stated.
- **5.5** Point `use_va_detail_sut.candidate` at the output. The
  [#587](https://github.com/cornerstone-data/bedrock/issues/587) diagnostic then
  gives the cell-by-cell picture for free — its reference, frame and tolerance
  are already settled.

---

## Sequencing

```
Phase 0 (gate) ──> Phase 3 ──> Phase 4 ──> Phase 5
Phase 1 ──┬─> Phase 3
Phase 2 ──┘
```

Phases 1 and 2 are independent of each other and of the Phase 0 gate; start
there. Phase 4 is where most of the risk sits and needs the most review.

---

# The rest of the value-added block

`V00100` is the first target because it is the largest row and the best served.
This section sizes the rest and sketches allocators, so the sequencing after
Phase 5 is a decision rather than a default.

## Priority by contribution to industry output

Against total industry output `T018` = 33,772,568:

| row | $M | share of output | priority |
|---|---:|---:|---|
| `T005` intermediate | 14,856,018 | 44.0% | Step 3, not here |
| `V00100` compensation | 10,434,981 | **30.9%** | **first** — this plan |
| `V00300` gross operating surplus | 7,873,013 | **23.3%** | **second** |
| `T00OTOP` other taxes on production | 608,542 | 1.8% | last |

`T00OTOP` is the row with *no* industry axis anywhere, and it is 1.8% of output.
Those two facts together argue for doing it last and accepting a cruder method,
not for doing it first because it looks hard.

## `V00300` — where the work actually is

Some of these lines touch a handful of sectors and some are genuinely
economy-wide, and the **NIPA leaf names say which**. Read that way, `V00300`
stops being one 7.9-trillion problem:

| component | $M | of `V00300` | industry footprint, from the leaf names |
|---|---:|---:|---|
| Consumption of fixed capital | 3,148,953 | 40.0% | broad. `T62200D` has **63 leaves** at good industry grain — the best-served piece |
| Corporate profits | 1,726,343 | 21.9% | broad, but `T61600D`'s 23 leaves mix in rest-of-world and aggregates; needs careful subtree selection |
| Proprietors' income | 1,428,634 | 18.1% | genuinely broad across 21 sectors — construction 185,791, misc. professional 133,064, health care 121,797, other services 117,682 |
| Net interest and misc. | 720,494 | 9.2% | **concentrated**: real estate 460,161 is 64% of it, and finance and insurance is **negative** at −156,707 |
| Rental income of persons | 642,028 | 8.2% | **nearly single-sector**: leaves are by *type*, not industry — "Permanent site" 453,907 is 71%, plus mobile units and farm owner-occupied housing |
| Business current transfers | 142,925 | 1.8% | small |
| Statistical discrepancy | 67,902 | 0.9% | not allocable to any industry; spread pro rata and say so |
| Current surplus of govt enterprises | −4,253 | −0.1% | trivial in size, but the sign is real |

### The housing concentration

Two sectors carry **19.6% of all `V00300`**:

- `531HSO` owner-occupied housing — 1,164,524, which is **74.9% of that
  sector's own output**
- `531HST` tenant-occupied housing — 376,515, **75.8% of its output**

And the two most concentrated NIPA sources land there: rental income of persons
(642,028) plus the real-estate share of net interest (460,161) is 1,102,189.

So **housing is the highest-value target in `V00300`**, and it is the opposite
of a broad allocation problem — `531HSO` has zero compensation and is almost
entirely operating surplus. Getting the housing pair right is worth more than a
sophisticated method applied to the long tail.

### NIPA already publishes the housing sector's value added, decomposed

**`T70405` (Table 7.4.5, *Housing Sector Output, Gross Value Added, and Net
Value Added*)** is the table this needs. It gives the housing sector's entire
value-added decomposition, and it splits gross value added owner vs tenant:

| | code | $M |
|---|---|---:|
| Gross housing value added | `A2009C` | 1,734,026 |
| — owner-occupied, nonfarm | `B1300C` | 1,322,323 |
| — tenant-occupied, nonfarm | `B1301C` | 393,481 |
| — farm housing | `B1302C` | 18,222 |
| Compensation of employees | `B1033C` | 18,921 |
| Taxes on production and imports | `B1031C` | 254,103 |
| Less: subsidies | `W154RC` | 38,212 |
| **Net interest** | `B1037C` | **332,634** |
| **Rental income of persons** | `B1035C` | **612,969** |
| Proprietors' income | `B1034C` | 72,560 |
| Corporate profits | `B1036C` | 9,557 |
| Current surplus of govt enterprises | `W153RC` | −21,353 |

**It ties to the SUT.** Housing compensation is 18,921 against the SUT's
`531HST` `V00100` of **18,920** — and `531HSO` is zero, so the housing sector's
entire wage bill sits in one detail code and the two agree to a million. That
is strong evidence T7.4.5's housing sector is the `531HSO`+`531HST` pair.

Implied housing gross operating surplus — gross value added less compensation
less taxes-net-of-subsidies — is 1,499,214 against the SUT pair's 1,541,039,
**2.71% apart**. The residual is mostly farm dwellings, which T7.4.5 includes
and the SUT books to the farm sectors. Worth resolving before use, but the
right order of magnitude to trust.

**It answers the two concentrated lines directly:**

| | housing, T7.4.5 | economy-wide | housing share |
|---|---:|---:|---:|
| Net interest | 332,634 | 720,494 | **46.2%** |
| Rental income of persons | 612,969 | 642,028 | **95.5%** |

Rental income is a housing line with a rounding error attached. Net interest is
nearly half housing — and of the 460,161 that `T61500D` puts in real estate,
housing is 332,634, so **72% of the real-estate net interest is housing**.

### And the owner/tenant split of interest is published too

**`T71100` (Table 7.11, *Interest Paid and Received by Sector and Legal Form of
Organization*)** carries owner-occupied housing as its own line:

- `W318RC` owner-occupied housing — **272,372**
- `W498RC` monetary interest paid, owner-occupied housing — 334,815
- `W307RC` imputed, owner-occupied housing — −61,070

Against T7.4.5's housing net interest of 332,634, that leaves roughly **60,000
for tenant-occupied** — consistent with owner-occupied carrying most of the
mortgage debt while being 77% of nonfarm housing gross value added.

So the housing pair does not need an allocator at all. Both concentrated `V00300`
lines are published for the sector, and the owner/tenant split is available
either directly (`T71100` for interest) or via the gross value added shares
(`B1300C`/`B1301C`) for the rest. **This turns the largest single piece of the
`V00300` problem into a lookup.**

`HzRfMortInt` also exists — mortgage interest paid on owner- and tenant-occupied
residential housing, 413,554 — but it is a combined figure despite the title,
so `T71100` is the better source for the split.

### The farm sector has the same treatment

**`T70305` (Table 7.3.5, *Farm Sector Output, Gross Value Added, and Net Value
Added*)** is the exact structural parallel. (Table 7.3.**6** is the chained-dollar
version; 7.3.**5** is current dollars and the one to use.)

| | code | $M |
|---|---|---:|
| Gross farm value added | `B359RC` | 138,731 |
| Compensation of employees | `A2006C` | 30,857 |
| — wages and salaries | `B1019C` | 25,220 |
| — supplements | `B1020C` | 5,637 |
| Taxes on production and imports | `B1017C` | 9,408 |
| Less: subsidies to operators | `B1018C` | 10,115 |
| Net interest | `B1021C` | 12,739 |
| **Farm proprietors' income** | `B042RC` | **41,005** |
| Corporate profits | `B1023C` | 7,060 |
| Consumption of fixed capital | `B366RC` | 46,598 |

**It ties to the SUT the same way housing does.** Farm compensation is 30,857
against the SUT's ten farm detail codes summing to **30,861** — 4 million apart.
Two independent sector checks now agree to within a rounding error, which is
good evidence the NIPA sector tables and the SUT detail codes describe the same
populations.

Gross farm value added is 138,731 against the SUT farm codes' `VABAS` of
148,849, **10,118 apart**. Same class of open item as the housing 2.71%.

**It also supplies `B042RC`, farm proprietors' income, which is the better
source than `T71500`** — that table gives 48,065 for proprietors' income *and*
corporate profits combined, mixing two components.

### Proprietors' income needs three pieces, not two

Worth stating because the obvious reading is wrong. `T61200D`'s root `B046RC` is
nonfarm proprietors' income at 1,088,100, **without** the inventory valuation
and capital consumption adjustments — `T71400` uses the same code as the
endpoint of its IRS reconciliation, which is the tell. So:

```
nonfarm, no adjustments   T61200D B046RC   1,088,100
farm                      T70305  B042RC      41,005
                                          ----------
                                           1,129,105
wanted, with IVA/CCAdj    T1.10   A041RC   1,428,634
remainder = adjustments                      299,529   no industry table
```

Adding farm closes only an eighth of the gap. The 299,529 of adjustments has no
industry axis and joins the statistical discrepancy in the must-be-spread pile.

### Allocator sketches, per component

- **Consumption of fixed capital** — the natural allocator is **capital stock by
  industry**, not a wage or output share. BEA's Fixed Assets tables publish net
  stock and depreciation by industry; not yet in bedrock, so this needs an
  extractor. `T62200D`'s 63 leaves may be enough on their own for the corporate
  part.
- **Corporate profits** — `T61600D` at 23 leaves, then a share carried down.
  Check whether SOI corporate data adds anything at detail before assuming it
  does.
- **Proprietors' income** — the broad one, and the one where a real allocator
  would pay. Census **Nonemployer Statistics** gives receipts by NAICS and is
  the obvious candidate; not in bedrock. Farm comes from `T70305` `B042RC`
  directly. The 299,529 of adjustments has no industry axis and must be spread.
- **Net interest** — take the housing portion from `T70405` `B1037C` (332,634)
  and split it owner/tenant with `T71100` `W318RC`, leaving 388 thousand to ride
  a coarse share across everything else. The negative finance line must survive;
  a share method that assumes positivity will break.
- **Rental income** — take the housing portion from `T70405` `B1035C` (612,969,
  95.5% of the line) and route it to `531HSO`/`531HST`. A lookup, not an
  allocation.
- **Statistical discrepancy** — pro rata, flagged as an artefact rather than a
  measurement.

## Product taxes exist on both axes — pick one and derive the other

`T00TOP` and `T00SUB` are **not** industry-only. The same money appears on the
Supply table by commodity, and the two sides reconcile:

| | Use, by industry | Supply, by commodity | |
|---|---:|---:|---|
| Subsidies | `T00SUB` 59,876 | `SUB` −59,876 | exact; Supply stores it negative |
| Taxes on products | `T00TOP` 755,451 | `TOP` 716,926 + `MDTY` 38,507 = 755,433 | 18 apart, rounding |
| Other taxes on production | `T00OTOP` 608,542 | **absent** | industry-only by construction |

Two things follow.

**The Supply side is more decomposed, not less.** The Use table folds import
duties into `T00TOP`; the Supply table separates them as `MDTY`. So the
commodity axis carries a distinction the industry axis has already lost.

**The commodity axis is the more natural home for a tax on a product.** Excise
taxes concentrate on specific products — tobacco, fuel, alcohol — and that
structure is visible on commodities and smeared across industries. 339 of 402
commodities carry `TOP` against 348 of 402 industries carrying `T00TOP`, so
neither is sparser; the difference is interpretability, not coverage.

**`T00OTOP` has no such choice.** Taxes on *production* attach to producing
units rather than products, which is why the Supply table has no counterpart.
It is industry-only and stays that way.

> ### Coordination risk with Step 4
>
> `supply_bridge_detail_sut` already covers `MDTY`, `TOP` and `SUB` as Step 4
> work. If Step 2 builds `T00TOP`/`T00SUB` by industry *and* Step 4 builds
> `TOP`/`SUB`/`MDTY` by commodity, the same money is estimated twice on two
> axes and the two results will not reconcile except by luck.
>
> **Build once, derive the other through the Make/Supply structure.** Deciding
> which axis is primary is a cross-step decision and should be made before
> either step starts, not discovered when the two disagree. The reconciliation
> above is the test that whichever direction is chosen still holds.

## `T00OTOP` — accept a cruder method

No NIPA table has an industry axis for this. `T30500` is by level of government
and kind of tax. Its composition — largely property taxes and motor vehicle
licences — suggests **capital stock or property value** as the allocation basis,
which is the same missing BEA Fixed Assets input that CFC wants. At 1.8% of
output, that shared dependency is a better reason to build the extractor than
`T00OTOP` is on its own.

## Open questions

1. **Is there a published detail *wages* series**, or must detail wage shares be
   derived from the summary wages/compensation ratio (1.3)?
2. ✅ **Closed (#536).** The ~10,600 gap was a wrong-line error: 6.2D line 1
   `A033RC` is compensation *received*, line 2 `A4002C` compensation *paid*,
   and the difference is the rest-of-world adjustment `A4187C` at −10,607.
   Value added wants the paid line.
3. **Does `estimate_suppressed_qcew` work on `Class: Money`?** It was written
   for Employment. Now cheap to answer, since the cache removes the
   generation cost — and `Employment_common.yaml` needs a `Class: Money`
   selection block either way, because every block there hardcodes
   `Class: Employment`.
4. **Which year is the target**, and does the QCEW lag (~5–6 months) meet the
   nowcast schedule? Sharper now: the local cache runs **2017–2023**, so
   Phase 1's 2024 has no QCEW at all and needs a stated fallback.
5. **BEA Fixed Assets is not in bedrock**, and both CFC (40% of `V00300`) and
   `T00OTOP` want capital stock by industry. One extractor serves both — is it
   worth building before either?
6. **Census Nonemployer Statistics is not in bedrock**, and proprietors' income
   (18% of `V00300`) is the component that would most benefit.
7. **Does `T61600D`'s subtree selection cleanly exclude rest-of-world?** Its
   leaves mix domestic industries with rest-of-world receipts and payments,
   and it is one of the four tables that restates a code (`B394RC`) on two
   lines. Select `A445RC` by code — that one is unique — and take the
   subtree beneath it rather than the table's root.
8. ✅ **Settled, and Step 2 does not build them at all.** The commodity axis
   won: `TOP`, `SUB` and `MDTY` are built in Step 4d
   ([`nowcast_product_taxes`](../../transform/iot/nowcast_product_taxes.py),
   [`nowcast_subsidies`](../../transform/iot/nowcast_subsidies.py), #690), and
   per plan.md's decision of 2026-08-17 the *industry* split of
   `T00TOP`/`T00SUB` is an **output of Step 5's balance rather than an input
   to it** — the producer-price column target is `T005 + VAPRO`, so the
   allocation solves rather than being assumed. Step 2's scope is the three
   `VABAS` rows: `V00100`, `T00OTOP`, `V00300`.
9. **The sector-table gaps against the SUT.** `T70405`'s implied housing
   operating surplus is 2.71% below the `531HSO`+`531HST` pair, and `T70305`'s
   gross farm value added is 10,118 below the ten farm codes. Compensation ties
   to within 1 and 4 million respectively in the two sectors, so the populations
   match and the gap is in the surplus components. Resolve before relying on
   either table for levels rather than shares.
10. **Are there sector tables like 7.3.5 and 7.4.5 for anything else?** Farm and
    housing are the two NIPA breaks out this way. Nothing equivalent was found
    for the other concentrated lines, but the sweep was of table *titles* only.
