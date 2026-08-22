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

- **2.1** Load QCEW `Class: Money` (annual payroll) at NAICS_6 for the target
  year and 2017. Confirm `estimate_suppressed_qcew` behaves on the Money class;
  it was written for Employment. **National 6-digit has few suppressions, but
  the fallback to 5-digit with residual allocation must be verified, not
  assumed.**
- **2.2** Map NAICS_6 → BEA detail via the existing concordance.
- **2.3** Compute per-detail-sector wage growth 2017→target year.
- **2.4** Update the Phase 1 benchmark shares by that growth; renormalise within
  each summary parent.

## Phase 3 — apply controls and assemble

- **3.1** Wages: allocate the 6.3D summary wage control across detail using the
  Phase 2 updated shares.
- **3.2** Supplements: allocate 6.10D (16 industries) and 6.11D (17, lines 3-20) down to
  summary, then to detail by the **wage** distribution from 3.1 — not by total
  compensation, per the decision above.
- **3.3** `V00100` detail = 3.1 + 3.2.
- **3.4** Rescale so detail sums exactly to the summary control.

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

**Private households — `814000`** (18,684). Outside QCEW scope entirely. NIPA
carries an explicit domestic-worker compensation line; carry it down.

**Government — and this one is nearly solved.** QCEW ownership codes do not
distinguish government *enterprises* from general government. NIPA does, and the
numbers tie exactly.

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
   for Employment.
4. **Which year is the target**, and does the QCEW lag (~5–6 months) meet the
   nowcast schedule?
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
