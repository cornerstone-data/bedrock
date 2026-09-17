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

## 2. Does it agree with the inventory?

Coverage is facility total over everything the inventory assigns that sector:

| basis | sectors | inventory Mt | of which allocated | of which `Direct` | facility Mt |
|---|---:|---:|---:|---:|---:|
| facility | 58 | 876.2 | 396.7 | 479.5 | 730.2 |
| facility + residual | 101 | 251.2 | 132.9 | 118.4 | 72.0 |
| facility exceeds the inventory | 29 | 238.4 | 183.7 | 54.7 | 567.4 |
| no facility data | 205 | 5,007.4 | 2,303.2 | 2,704.2 | 2.1 |

The sectors this work is about land well:

| sector | inventory Mt | facility Mt | coverage |
|---|---:|---:|---:|
| `331110` iron and steel | 68.8 | 65.9 | **0.96** |
| `325310` fertilizer | 48.2 | 46.2 | **0.96** |
| `211000` oil and gas extraction | 322.8 | 256.0 | 0.79 |
| `327400` lime and gypsum | 23.1 | 34.3 | 1.49 |
| `327310` cement | 57.8 | 89.2 | 1.54 |
| `324110` petroleum refineries | 86.9 | 228.0 | 2.62 |

⚠️ **The overshoots are not yet a finding.** Deduplication depends on `FRS_ID`
matching, and 668 Mt of GHGRP mass has no NEI match at all, so a site the match
misses is counted twice. Refineries at 2.62 is the clearest suspect. That is an
implementation defect to fix before any of these levels are quoted, not evidence
about the inventory.

---

## 3. ⚠️ Where a facility basis cannot help

**433 Mt of the process emissions this widening pulls in are already
`Direct`-attributed** — the inventory names the sector itself, with no Use row,
no MECS and no vector in between. A facility basis cannot improve an assignment
that was never derived.

So the columns are kept apart, and `allocated_Mt` is the one to rank on: 3,016 Mt
is vector-allocated and improvable; 3,357 Mt is already `Direct` and is not.
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
  facility **bought** from fuel it **made itself** — refinery still gas, coke
  oven gas, blast furnace gas, 46.9 Mt in 2022. Byproduct gas is never a
  purchase, so no row of the Use table can represent it, and attributing it with
  one is a category error rather than an inaccuracy. Tracker row 17.

---

## 7. Known gaps before this can ship

1. **Deduplication.** 668 Mt of GHGRP mass has no `FRS_ID` match into NEI, and
   unmatched sites are double counted. Section 2's overshoots are the symptom.
2. **The NEI 2020/2021 reclassification.** Combustion SCCs go from 3.2% to 75.1%
   of NEI CO2 with the total flat. GHGRP has no such break and covers 2017-2023.
3. **Lease fuel.** An oil and gas producer burning its own field gas is burning
   natural gas, and the SCC says natural gas, so `facility_derived` does not
   catch it. Same "no purchase exists" defect as byproduct gas.
4. **The 205 sectors with no facility data**, 2,303 Mt of allocated mass —
   agriculture, construction and the rest of the dispersed sources. They stay on
   the Use row, which is also where MECS never reached.
