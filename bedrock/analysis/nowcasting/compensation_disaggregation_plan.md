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
- **No value-added code is in `NAICS_Crosswalk_BEA_2017_Detail.csv`.** This is a
  gate, not a detail — see [Phase 0](#phase-0--the-crosswalk-gate).

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

| | table | 2017, $M | leaves |
|---|---|---:|---:|
| Wages and salaries | `T60300D` (6.3D) | 8,474,410 | 74 |
| Employer contributions, government social insurance | `T61000D` (6.10D) | 604,656 | 16 |
| Employer contributions, pension and insurance funds | `T61100D` (6.11D) | 1,345,306 | 36 |
| **sum** | | **10,424,372** | |
| Compensation of employees | `T60200D` (6.2D) | 10,424,372 | 74 |
| **difference** | | **0** | |

Verified to the dollar. So supplements need a parent-ratio carry-down only
*below* 16 and 36 industries respectively — not from the top.

> **Open reconciliation item.** 6.2D's root is 10,424,372 while the Use SUT's
> `V00100` is 10,434,981 and T1.10 `A4002C` is 10,434,978 — a gap of ~10,600,
> most likely the rest-of-world compensation line. Resolve before using 6.2D as
> a control; it is small but it is not rounding.

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

## Phase 0 — the crosswalk gate

`V00100`, `T00OTOP` and `V00300` are absent from
`NAICS_Crosswalk_BEA_2017_Detail.csv` on both sides. The precedent is an
identity row, as `F01000` has and as `7a04a71` added for `S00300`/`S00900`.

The crosswalk declares `SectorSourceName: NAICS_2017_Code` and nothing else, and
these codes are not NAICS. Adding them as identity rows under a NAICS source
name is the move that put 210 BEA detail codes out of reach of the hierarchy
machinery — **this is [#568](https://github.com/cornerstone-data/bedrock/issues/568), and Step 2 hits it when it writes its first crosswalk row, not later.**

- **0.1** Decide: wait for #567/#568, or add identity rows now and accept the
  known breakage. Recommend deciding explicitly rather than discovering it.
- **0.2** Add rows for `V00100` at minimum. `VABAS`/`VAPRO` are aggregates and
  should not be targets.

**Blocks every later phase that routes through FBS attribution.** Phases 1–2
are pure analysis and can proceed regardless.

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
- **3.2** Supplements: allocate 6.10D (16 industries) and 6.11D (36) down to
  summary, then to detail by the **wage** distribution from 3.1 — not by total
  compensation, per the decision above.
- **3.3** `V00100` detail = 3.1 + 3.2.
- **3.4** Rescale so detail sums exactly to the summary control.

## Phase 4 — the sectors where QCEW does not work

These are not edge cases; they are ~15% of compensation and they will produce
visibly wrong answers if run through the general path.

**Construction — the big one.** BEA's 12 detail construction sectors are defined
by *type of structure*; QCEW classifies establishments by *trade*. There is no
valid mapping — a specialty trade contractor's wages spread across every
structure type.

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
(UI exemptions for small farms). Use **`USDA_ERS_FIWS`** hired and contract
labor expense as the movement series.

**Private households — `814000`** (18,684). Outside QCEW scope entirely. NIPA
carries an explicit domestic-worker compensation line; carry it down.

**Government.** QCEW ownership codes do not distinguish government *enterprises*
from general government, which BEA splits:

| enterprises | general government |
|---|---|
| `491000` postal (54,249) | `S00500` federal defense (246,097) |
| `S00101` federal electric utilities (1,997) | `S00600` federal nondefense (184,220) |
| `S00102` other federal enterprises (2,973) | `GSLGE` S&L education (731,648) |
| `S00201` S&L passenger transit (26,850) | `GSLGH` S&L hospitals/health (139,316) |
| `S00202` S&L electric utilities (10,203) | `GSLGO` S&L other (467,952) |
| `S00203` other S&L enterprises (69,979) | |

Use 6.2D's own government lines plus BEA government detail, not QCEW ownership.

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
  the obvious candidate; not in bedrock. `USDA_ERS_FIWS` covers the farm part
  already.
- **Net interest** — concentrated enough that the real-estate line deserves
  explicit handling and the remainder can ride a coarse share. The negative
  finance line must survive; a share method that assumes positivity will break.
- **Rental income** — route the housing leaves straight to `531HSO`/`531HST`
  rather than allocating. This is close to a lookup.
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
2. **The ~10,600 gap** between 6.2D's root and the SUT's `V00100`.
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
   leaves mix domestic industries with rest-of-world receipts and payments.
8. **Which axis is primary for product taxes and subsidies — commodity or
   industry?** A cross-step decision between Step 2 and Step 4, and the one
   question here that cannot be answered inside this plan alone.
