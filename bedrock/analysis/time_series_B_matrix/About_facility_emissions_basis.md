# A facility-reported basis for industrial emissions

What happens if the sector split for industrial emissions comes from facilities
that reported them, instead of from a purchase row or a survey share.

Measured with **D15** in `B_change_diagnostics.py` (`--facility-data`), 2017-2022,
against the production basis on FBS `v0.3.0_796a6ca` and MUT `v0.3.0_4276083`.
Driver context is in [`B_driver_investigation.md`](B_driver_investigation.md);
the diagnostic register is in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md).

---

## 1. The two bases

**Today.** EPA table 3-11, industrial stationary fuel combustion, is a national
total spread across sectors by a vector. `ng_manufacturing` and
`coal_manufacturing` ride MECS survey shares; `petroleum_industrial` and
`natural_gas_nonmanufacturing` ride rows of the nowcast Use table. No sector
assignment in it points at a measured source, and ~71% of the family moves with
the Use table.

**Proposed.** GHGRP first — facilities report under a mandatory GHG programme —
then NEI for the facilities below GHGRP's 25,000 tCO2e threshold, deduplicated on
`FRS_ID`. Sector comes from the **facility's** NAICS.

⚠️ **Not subpart C only, and not combustion SCCs only.** A cement kiln burns fuel
and calcines limestone in one vessel, and both inventories report the vessel:
GHGRP subpart H is a single CO2 number with no combustion field, and NEI puts
69.1 of cement's 69.2 Mt on process SCCs. Restricting to "the combustion parts"
scores cement at 0.01 coverage — a coverage failure that is entirely an artefact
of the question. The basis therefore takes every GHGRP subpart except `D`
(electricity, which runs on eGRID here) and SCC branches 1, 2 and 3, and is
compared against everything the inventory assigns the sector.

**2022: 1,486 Mt over 20,638 facilities.**

---

## 2. How much of the split could it carry?

**Scope: mining, utilities and manufacturing** — BEA detail codes beginning 21,
22 and 31-33, 232 sectors. These are the industries whose emissions happen at a
plant somebody reports. Everything else a vector currently places — government
buildings, livestock, trucking, real estate — is mobile, biological or diffuse,
and no facility reports it because none emits it.

⚠️ Two sectors are excluded from *both* sides of the comparison: `221100`
electric power, which runs on eGRID in this model and is filtered out of the
facility union by construction, and `F01000` personal consumption, which has no
gross output and is outside this module entirely.

⚠️ **Scoring the basis across every sector measures the boundary, not the basis.**
In scope it reaches 94% of the allocated mass; across all 391 sectors it reaches
34%, and the difference is entirely sectors that were never candidates. An
earlier version of this note quoted the unscoped figure without saying so.

### What is in scope, and which part of it is even improvable

| | Mt | |
|---|---:|---|
| **vector-allocated** | **720** | placed by MECS or a Use row — the only part a facility basis could restate |
| inventory-assigned (`Direct`) | 524 | the inventory names the sector itself; counted, never improvable — §3 |
| **total inventory, in scope** | **1,244** | |

### How much of the allocated 720 Mt has facility data behind it

| facility coverage of the sector | sectors | vector-allocated Mt | share of allocated |
|---|---:|---:|---:|
| **covered** — facility total is 50-150% of the inventory's | 55 | 395.5 | **54.9%** |
| **partly covered** — 5-50% | 88 | 109.9 | 15.3% |
| **over 150%** — double counting, see below | 25 | 171.3 | 23.8% |
| **none** — under 5% | 64 | 43.4 | **6.0%** |

**94% of the vector-allocated mass in mining, utilities and manufacturing has
facility-reported data behind it.** The 6% that does not is 64 small specialty
manufacturers, none above 8 Mt — paint and coating 7.4, soft drinks 5.2, light
trucks 4.4.

By major group:

| | sectors | vector-allocated Mt | facility-reported Mt |
|---|---:|---:|---:|
| 21 mining | 8 | 144.5 | 318.1 |
| 22 utilities (excluding electric power) | 2 | 0.6 | 36.7 |
| 31-33 manufacturing | 222 | 575.0 | 868.5 |

### Where it reaches, does it disagree — and which side is wrong?

⚠️ **There is no authoritative denominator here, which is the whole point.** For
these sectors the inventory does **not** attribute fossil combustion directly. The
"current method" column is the GHGI national total pushed through MECS shares and
BEA Use rows — the very derivation under suspicion — plus whatever the inventory
did assign directly. So a facility total above it is a **disagreement**, not an
overcount, and the ratio alone cannot say which side moved.

One column can be read without that caveat. **GHGRP alone is a single source, so
no cross-source double count is possible in it** — whatever #925 does to the
union, it cannot inflate this:

| sector | current: allocated | current: `Direct` | GHGRP alone | facility union | GHGRP ÷ current |
|---|---:|---:|---:|---:|---:|
| `324110` petroleum refineries | 83.4 | 3.6 | **176.4** | 228.0 | **2.03** |
| `311221` wet corn milling | 7.4 | 0.0 | **14.8** | 20.5 | **2.00** |
| `327310` cement | 15.9 | 41.9 | 67.9 | 89.2 | 1.18 |
| `327400` lime and gypsum | 10.8 | 12.2 | 25.5 | 34.3 | 1.11 |
| `325310` fertilizer | 26.6 | 21.6 | 40.3 | 46.2 | 0.84 |
| `331110` iron and steel | 21.7 | 47.0 | 55.9 | 65.9 | 0.81 |
| `221200` natural gas distribution | 0.2 | 30.5 | 23.4 | 26.9 | 0.76 |
| `211000` oil and gas extraction | 110.9 | 211.9 | 221.2 | 256.0 | 0.69 |
| `325110` petrochemicals | 48.2 | 20.7 | 26.6 | 54.2 | 0.39 |

**In 14 in-scope sectors GHGRP alone exceeds the current method — 368 Mt against
231 Mt.** Refineries at 2.03x is the clearest: a mandatory, verified programme
reports more than twice what our derivation assigns, from one source that cannot
double count itself. The remaining explanations are that the current method
under-allocates, or that GHGRP counts process units the inventory books to another
table. Facility-side overcounting is not one of them.

⚠️ **Only one comparison here settles direction**, and it is the like-for-like one
in §4: GHGRP subpart C is combustion and table 3-11 is combustion, and subpart C
is a threshold-limited **lower bound**. When the current method falls below it —
`211000` at 0.18 in 2020 — the current method is too low and there is nothing to
argue about. Everywhere else the ratios above are a flag for investigation, not a
verdict.

## 3. ⚠️ Where a facility basis cannot help

**433 Mt of the process emissions this widening pulls in are already
`Direct`-attributed** — the inventory names the sector itself, with no Use row,
no MECS and no vector in between. A facility basis cannot improve an assignment
that was never derived.

So the columns are kept apart, and `allocated_Mt` is the one to rank on: within
the 3,459 Mt in scope, 2,099 Mt is vector-allocated and improvable; 1,359 Mt is
already `Direct` and is not.
Widening the *comparison* was necessary to stop scoring kilns against the wrong
denominator. It does not widen what is on offer.

---

## 4. The case on accuracy

Three independent checks, all pointing the same way, all on `211000` — the
largest oscillating cell in the B panel:

1. **It breaches a measured floor.** GHGRP subpart C is a lower bound, since it
   only covers facilities over the reporting threshold. In 2020 the current basis
   allocates oil and gas extraction **8.6 Mt where its own facilities reported
   46.4 Mt** — a factor of 5.4 below a floor. 2017 breaches it too, at 0.66.
2. **Its driver is a coefficient nothing corroborates.** Purchases of refined
   petroleum per $100 of the sector's own gross output run 0.85 → 2.64 → 1.69,
   a 3.11x rise at the 98th percentile of that Use row, against a median buyer
   of 0.91x. Both sides are nominal and every buyer purchases the same commodity,
   so no price move can produce it. [#922](https://github.com/cornerstone-data/bedrock/issues/922).
3. **The physical series does not move.** GHGRP subpart C for the same facilities
   holds 44.9-48.4 Mt across 2017-2023, a **1.08x span**, while the current basis
   spans **12.8x**.

And the facility basis marks the sector **down 4.6 percentage points** of share —
reached from facility records rather than from the coefficient, and pointing the
same way as all three.

---

## 5. The case on smoothing, which is the one that matters

Accuracy in a single year is not what this project is for. `B` is rocky because
the vector that splits emissions across sectors **moves between years**, so the
test is which basis reshuffles less. Same measure used for the MECS cutoff: the
sum of the absolute year-on-year change in every sector's share, in percentage
points, over the 235 sectors both bases carry.

| year | current basis | facility basis | ratio |
|---|---:|---:|---:|
| 2018 | 14.3 pp | **3.9 pp** | 0.27 |
| 2019 | 5.7 pp | **4.3 pp** | 0.75 |
| 2020 | 25.9 pp | **5.6 pp** | 0.22 |
| 2021 | 32.6 pp | **3.8 pp** | 0.12 |
| 2022 | 16.9 pp | **4.1 pp** | 0.24 |
| **median** | **15.6 pp** | **4.0 pp** | **0.24x** |

**The facility basis reshuffles a quarter as much**, and the gap is widest
exactly where the B panel hurts most.

⚠️ **Look at 2020 and 2021.** The current basis reshuffles 25.9 and 32.6
percentage points; the facility basis reshuffles 5.6 and 3.8, which is what it
does in every other year. Tracker row 9 attributes the 2021 spike to the COVID
rebound and reads it as a real event not to be smoothed. Against a facility
series that barely moves through the same two years, **most of that spike is the
allocation vector, not the pandemic.** Emissions at those facilities did not
reshuffle; the purchase rows and survey shares used to place them did.

That is the substantive claim this basis makes: it would remove the single
largest source of year-to-year movement in the emissions side of `B`, and it
would do so without smoothing anything — by replacing a derived split with a
reported one.

⚠️ Two honest qualifications. Facility counts step from 14.3k to 22.1k between
2019 and 2020 when NEI reclassified, so the facility series has a vintage seam of
its own — though its churn is low on both sides of it, which is the opposite of
what a seam-driven artefact looks like. And churn is measured on shares, so a
stable double count would not show up here; section 2's deduplication defect has
to be fixed before the levels are trusted.

---

## 6. What it also buys

- **Provenance.** Two thirds of the allocated mass would point at named
  facilities with reported emissions rather than at a purchase row.
- **Location.** Every facility carries a state and most carry coordinates. If
  this code base later produces state-level EEIO models, the location breakout
  would rest on measured data. That is nearly free now and expensive to retrofit.
- **A category that does not exist today.** `fuel_class` separates fuel a
  facility **bought** from fuel that never changed hands — `self_supplied`,
  **144.9 Mt in 2022**. Fuel nobody sold is never a purchase, so no row of the
  Use table can represent it, and attributing it with one is a category error
  rather than an inaccuracy. Tracker row 17, and see §8 for where the other
  two thirds of that mass came from.

---

## 7. Prerequisites for implementation

Tracked on
[**Facility-based fuel combustion GHGs**](https://github.com/orgs/cornerstone-data/projects/34),
which feeds
[B Smoothing Phase 1](https://github.com/orgs/cornerstone-data/projects/31)
through a single issue:
[#929](https://github.com/cornerstone-data/bedrock/issues/929) is the
integration step and is cross-listed on both boards, so the B smoothing work
depends on that one while the rest of project 34 feeds it.

### Blocking — the levels cannot be quoted until these are settled

| | | why it blocks |
|---|---|---|
| [#925](https://github.com/cornerstone-data/bedrock/issues/925) | FRS deduplication between GHGRP and NEI | 668 Mt of GHGRP mass has no NEI match, so an unmatched site is counted twice. Every ratio above 1 in §2 is suspect until this is fixed. ⚠️ Does **not** touch §5 — churn is measured on shares, and a stable double count does not move a share |
| [#926](https://github.com/cornerstone-data/bedrock/issues/926) | NEI's 2020/2021 SCC reclassification | Combustion SCCs go from 3.2% to 75.1% of NEI CO2 with the total flat. `fuel_class` is derived from the SCC, so it means something different either side of the break |

### Coverage — needed before the basis spans the model

| | | |
|---|---|---|
| [#931](https://github.com/cornerstone-data/bedrock/issues/931) | GHGRP 2024 into StEWI from the FOIA'd static files | GHGRP stops at 2023 |
| [#932](https://github.com/cornerstone-data/bedrock/issues/932) | NEI (EIS) 2023 and 2024 | NEI stops at 2022, which is what bounds D15 today |

Together these take the basis from 2017-2022 to the full 2017-2024 nowcast span,
so §5's churn comparison could be stated for every year the model produces.

### Method decisions

| | | |
|---|---|---|
| [#927](https://github.com/cornerstone-data/bedrock/issues/927) | Lease and plant fuel is self-supplied but classified as purchased | An oil and gas producer burning its own field gas is burning natural gas, and the SCC says natural gas. Same "no purchase exists" defect as byproduct gas, and the structural reason `211000` sits below the floor in 2017 |
| [#928](https://github.com/cornerstone-data/bedrock/issues/928) | The residual rule for sectors the basis cannot reach | 64 in-scope sectors carrying 43.4 Mt have no facility data, and 88 more are partly covered. Mixing a facility level with a Use-derived remainder needs a stated method |

### Integration

| | | |
|---|---|---|
| [#929](https://github.com/cornerstone-data/bedrock/issues/929) | Build a **new** facility-based GHG FBS alongside the existing one | ⚠️ The current method must keep running. Every before-and-after number here compares against it, and §5's claim is only checkable while both series can be built from the same code on the same span |
| [#930](https://github.com/cornerstone-data/bedrock/issues/930) | Preserve facility location for a future state-level model | Not needed nationally; nearly free now and expensive to retrofit |

### ⚠️ Not a prerequisite, and not a backlog

The 203 sectors with no facility data — 1,386 Mt across all sectors, two thirds
of what a vector places — are government, agriculture, trucking and buildings.
They stay on the Use row, which is also where MECS never reached. No facility
reports them because none emits them. In the mining, utilities and manufacturing
scope this basis is for, the equivalent figure is **6% of allocated mass**.


---

## 8. Lease and plant fuel: what subpart W says that subpart C cannot (#927)

`fuel_class` began as byproduct gas alone — refinery still gas, coke oven gas,
blast furnace gas — read off NEI's process-gas SCCs. That left the larger half of
the same defect invisible. **An oil and gas producer burning its own field gas is
burning natural gas, and the SCC says natural gas.**

The GHGRP answers it directly, in a place `stewi` does not import.

### The lease side is reported, not inferred

Envirofacts view `ef_w_combust_large_units` carries one row per combustion unit
type and fuel — facility, industry segment, **quantity of fuel burned**, unit of
measure, CO2, CH4 and N2O — and its fuel list separates `Field gas and/or process
gas` and `… natural gas that is not of pipeline quality` from `Natural gas
(pipeline quality)`. The reporter writes that label. The unit rows reconcile to
the facility totals in `ef_w_combust_equip_summ` exactly, so every tonne of
subpart W combustion carries a fuel type.

Gas burned at onshore production facilities, Bcf, volumes screened on an implied
0.02–0.15 t CO2 per Mscf:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| self-supplied | **233** | 223 | 265 | 257 | 267 | 297 | 313 | **330** |
| pipeline | 110 | 123 | 129 | 136 | 142 | 137 | 148 | 156 |

Gathering and boosting is roughly twice that, and about half the **carbon** at
both is self-supplied — lower than the share of gas, because the purchased side
also carries diesel.

### The plant side is reported, but labelled as if bought

Gas processing plants report their combustion under subpart C, where 234.7 of the
236.1 Mt of CO2 they reported over 2019-2024 is labelled `Natural Gas (Weighted
U.S. Average)` against 1.15 Mt of `Fuel Gas`. The label is an emission-factor
choice, not a procurement statement. So the classification comes from **what the
facility is**: a plant flagged as an `Onshore natural gas processing` reporter in
subpart W burns the stream it is processing, which is what EIA counts as plant
fuel. ⚠️ That one is an inference, and `fuel_class_basis` says so — the lease
side reads `GHGRP subpart W fuel`, the plant side `GHGRP segment`.

### Which segments report where, so the two halves do not double count

| segment | combustion reported under | facility-years also in the subpart C fuel tables |
|---|---|---|
| onshore production | **subpart W** | 4 of 2,801 |
| gathering and boosting | **subpart W** | 0 of 2,157 |
| natural gas distribution | **subpart W** | 0 of 969 |
| onshore natural gas processing | **subpart C** | 2,701 of 2,704 |
| transmission compression | **subpart C** | 3,915 of 3,915 |

### What it does to D14

The floor was built by filtering `Process == 'C'`, so it held no lease fuel at
all. In 2017 the facilities carrying a NAICS of 211 reported 44.93 Mt CO2e under
subpart C and 177.18 Mt under subpart W, and that 44.93 is 78% gas processing
plants, 11% offshore production and 10% facilities filing no subpart W report —
**none of it onshore production**. `211000` was being scored against another
segment's fuel.

| `211000` | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| floor, subpart C only | 44.9 | 46.8 | 47.6 | 46.5 | 46.5 | 46.7 | 48.4 | 44.9 |
| floor, **C + W combustion** | 113.3 | 120.7 | 130.2 | 122.8 | 127.5 | 134.0 | 140.2 | 139.7 |
| what we allocate | 29.6 | 54.4 | 53.1 | 37.7 | 60.1 | 90.1 | 58.6 | 55.6 |
| ratio, subpart C only | 0.66 | 1.16 | 1.12 | 0.81 | 1.29 | 1.93 | 1.21 | 1.24 |
| **ratio, whole floor** | **0.26** | 0.45 | 0.41 | 0.31 | 0.47 | 0.67 | 0.42 | 0.40 |

⚠️ **The verdict moves from `intermittent` to `boundary_offset`, and that is not
a reprieve.** `boundary_offset` names a *pattern* — below the floor in every year
— and the docstring's reading of it as a definition difference holds for
petroleum refineries, whose still gas the inventory books outside table 3-11.
`211000` is the other kind: lease and plant fuel **are** inside table 3-11, and
the allocation misses them because a purchase row cannot see fuel nobody sold.
Its cumulative shortfall, 589 Mt over the span, is now the largest in the test —
ahead of refineries at 321 Mt.

`21311A`, other support activities for mining, enters the test for the first time
once its floor clears 1 Mt, and lands `intermittent`.
