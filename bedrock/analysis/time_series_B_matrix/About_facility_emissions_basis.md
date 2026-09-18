# A facility-reported basis for industrial emissions

What happens if the sector split for industrial emissions comes from facilities
that reported them, instead of from a purchase row or a survey share.

Measured with **D15** in `B_change_diagnostics.py` (`--facility-data`), 2017-2022,
against the production basis on FBS `v0.3.0_99655e9` and MUT `v0.3.0_4276083`.

⚠️ **Restated on the published build.** This note first quoted
`v0.3.0_796a6ca`, a local-only build off a commit no ref reaches, which the
span vintage resolver picked because it ranked candidates on file mtime. That
is the defect #912 turned out to be, fixed in `6e6a5947`. The facility side is
GHGRP and NEI and a build vintage cannot touch it — every facility-only figure
below was re-measured and came back identical. What moved is our own
allocation, and with it every comparison against it. Two findings reversed and
say so in place.
Driver context is in [`B_driver_investigation.md`](B_driver_investigation.md);
the diagnostic register is in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md).

---

## 1. The two bases

**Today.** EPA table 3-11, industrial stationary fuel combustion, is a national
total spread across sectors by a vector. `ng_manufacturing` and
`coal_manufacturing` ride MECS survey shares; `petroleum_industrial` and
`natural_gas_nonmanufacturing` ride rows of the nowcast Use table. No sector
assignment in it points at a measured source, and 59% of the family moves with
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
22 and 31-33, 233 sectors. These are the industries whose emissions happen at a
plant somebody reports. Everything else a vector currently places — government
buildings, livestock, trucking, real estate — is mobile, biological or diffuse,
and no facility reports it because none emits it.

⚠️ Two sectors are excluded from *both* sides of the comparison: `221100`
electric power, which runs on eGRID in this model and is filtered out of the
facility union by construction, and `F01000` personal consumption, which has no
gross output and is outside this module entirely.

⚠️ **Scoring the basis across every sector measures the boundary, not the basis.**
In scope it reaches 93% of the allocated mass; across all 393 sectors it reaches
33%, and the difference is entirely sectors that were never candidates. An
earlier version of this note quoted the unscoped figure without saying so.

### What is in scope, and which part of it is even improvable

| | Mt | |
|---|---:|---|
| **vector-allocated** | **699** | placed by MECS or a Use row — the only part a facility basis could restate |
| inventory-assigned (`Direct`) | 524 | the inventory names the sector itself; counted, never improvable — §3 |
| **total inventory, in scope** | **1,223** | |

### How much of the allocated 720 Mt has facility data behind it

| facility coverage of the sector | sectors | vector-allocated Mt | share of allocated |
|---|---:|---:|---:|
| **covered** — facility total is 50-150% of the inventory's | 51 | 372.7 | **53.3%** |
| **partly covered** — 5-50% | 90 | 109.2 | 15.6% |
| **over 150%** — double counting, see below | 24 | 167.2 | 23.9% |
| **none** — under 5% | 68 | 50.0 | **7.2%** |

**93% of the vector-allocated mass in mining, utilities and manufacturing has
facility-reported data behind it.** The 7% that does not is 68 small specialty
manufacturers, none above 8 Mt — paint and coating 7.9, soft drinks 4.8,
paperboard containers 4.7, light trucks 4.6.

By major group:

| | sectors | vector-allocated Mt | facility-reported Mt |
|---|---:|---:|---:|
| 21 mining | 8 | 128.2 | 318.1 |
| 22 utilities (excluding electric power) | 2 | 0.9 | 36.7 |
| 31-33 manufacturing | 223 | 570.0 | 868.7 |

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
| `324110` petroleum refineries | 76.2 | 3.6 | **176.4** | 228.0 | **2.21** |
| `311221` wet corn milling | 7.4 | 0.0 | **14.8** | 20.5 | **2.00** |
| `327310` cement | 15.9 | 41.9 | 67.9 | 89.2 | 1.17 |
| `327400` lime and gypsum | 10.9 | 12.2 | 25.5 | 34.3 | 1.11 |
| `325310` fertilizer | 25.7 | 21.6 | 40.3 | 46.2 | 0.85 |
| `331110` iron and steel | 21.7 | 47.0 | 55.9 | 65.9 | 0.81 |
| `221200` natural gas distribution | 0.1 | 30.5 | 23.4 | 26.9 | 0.76 |
| `211000` oil and gas extraction | 91.3 | 211.9 | 221.2 | 256.0 | 0.73 |
| `325110` petrochemicals | 47.9 | 20.7 | 26.6 | 54.2 | 0.39 |

⚠️ The **GHGRP alone** and **facility union** columns are identical to what this
note first reported — they are external data. Every other column is ours, and
every one of them fell.

**In 23 in-scope sectors GHGRP alone exceeds the current method — 373 Mt against
228 Mt.** That is nine more sectors than the retired build showed, because the
current method assigns less: the GHGRP side did not move. Refineries at 2.21x is
the clearest case: a mandatory, verified programme reports more than twice what
our derivation assigns, from one source that cannot double count itself. The remaining explanations are that the current method
under-allocates, or that GHGRP counts process units the inventory books to another
table. Facility-side overcounting is not one of them.

⚠️ **Only one comparison here settles direction**, and it is the like-for-like one
in §4: GHGRP subpart C is combustion and table 3-11 is combustion, and subpart C
is a threshold-limited **lower bound**. When the current method falls below it —
`211000` at 0.81 in 2020 — the current method is too low and there is nothing to
argue about. Everywhere else the ratios above are a flag for investigation, not a
verdict.

## 3. ⚠️ Where a facility basis cannot help

**524 Mt of the process emissions this widening pulls in are already
`Direct`-attributed** — the inventory names the sector itself, with no Use row,
no MECS and no vector in between. A facility basis cannot improve an assignment
that was never derived.

So the columns are kept apart, and `allocated_Mt` is the one to rank on: across
the 3,461 Mt the widened comparison covers, 2,102 Mt is vector-allocated and
improvable; 1,359 Mt is already `Direct` and is not. ⚠️ These three totals are
the *only* figures in this note the vintage barely moved, and the reason is
instructive: the retired build differs from the published one purely by
redistributing emissions across sectors, so every national total is the same to
within a rounding step while every per-sector split changed.
Widening the *comparison* was necessary to stop scoring kilns against the wrong
denominator. It does not widen what is on offer.

---

## 4. The case on accuracy

Three independent checks, all pointing the same way, all on `211000` — the
largest oscillating cell in the B panel:

1. **It breaches a measured floor.** GHGRP subpart C is a lower bound, since it
   only covers facilities over the reporting threshold. In 2020 the current basis
   allocates oil and gas extraction **37.7 Mt where its own facilities reported
   46.5 Mt** — 1.23x below a floor. 2017 breaches it too, and harder, at 0.66.
   ⚠️ The retired build put 2020 at 8.6 Mt, a factor of 5.4 below. The breach is
   real in both, but it is a modest one, not a fivefold one.
2. **Its driver is a coefficient nothing corroborates.** Purchases of refined
   petroleum per $100 of the sector's own gross output run 0.85 → 2.64 → 1.69,
   a 3.11x rise at the 98th percentile of that Use row, against a median buyer
   of 0.91x. Both sides are nominal and every buyer purchases the same commodity,
   so no price move can produce it. [#922](https://github.com/cornerstone-data/bedrock/issues/922).
3. **The physical series does not move.** GHGRP subpart C for the same facilities
   holds 44.9-48.4 Mt across 2017-2024, a **1.08x span**, while the current basis
   spans **3.05x**.

⚠️ **The fourth check reversed and is withdrawn.** This note previously added
that the facility basis marks the sector **down 4.6 percentage points** of
share, reading it as a fourth line of evidence pointing the same way as the
other three. On the published build `share_shift_pp` for `211000` is
**+1.81 pp** — the facility basis would give the sector *more* share, not less.

That is not a contradiction of the three checks above, and it is worth being
precise about why: they compare our allocation against subpart C, which is
combustion only and threshold-limited, while the share shift compares it against
the full facility union of 256 Mt, which includes the process units GHGRP books
under other subparts. The three checks say the sector's fuel combustion is
allocated erratically and sometimes below a measured floor. They never said the
sector was over-allocated overall, and the share shift never supported that
reading on this build. The case in §5 does not depend on it.

---

## 5. The case on smoothing, which is the one that matters

Accuracy in a single year is not what this project is for. `B` is rocky because
the vector that splits emissions across sectors **moves between years**, so the
test is which basis reshuffles less. Same measure used for the MECS cutoff: the
sum of the absolute year-on-year change in every sector's share, in percentage
points, over the 227 in-scope sectors both bases carry — those with a positive
allocated mass and facility data in at least one year of the span.

| year | current basis | facility basis | ratio |
|---|---:|---:|---:|
| 2018 | 8.7 pp | **3.9 pp** | 0.45 |
| 2019 | 6.3 pp | **4.3 pp** | 0.68 |
| 2020 | 11.9 pp | **5.6 pp** | 0.47 |
| 2021 | 24.0 pp | **3.7 pp** | 0.16 |
| 2022 | 10.0 pp | **4.1 pp** | 0.41 |
| **median** | **10.0 pp** | **4.1 pp** | **0.41x** |

⚠️ **The facility column is unchanged from the retired build** — 3.9, 4.3, 5.6,
3.7, 4.1 against 3.9, 4.3, 5.6, 3.8, 4.1 — which is the check that this measure
behaves: the facility basis is external data and should not move when our build
does. The current basis column fell throughout, so the headline weakens.

**The facility basis reshuffles about two fifths as much**, not the quarter this
note first claimed, and the gap is still widest exactly where the B panel hurts
most.

⚠️ **Look at 2020 and 2021.** The current basis reshuffles 11.9 and 24.0
percentage points; the facility basis reshuffles 5.6 and 3.7, which is what it
does in every other year. Tracker row 9 attributes the 2021 spike to the COVID
rebound and reads it as a real event not to be smoothed. Against a facility
series that barely moves through the same two years, **most of that spike is the
allocation vector, not the pandemic** — 2021 remains the year with the widest
gap between the two bases, by a clear margin, on the published build as on the
retired one. Emissions at those facilities did not
reshuffle; the purchase rows and survey shares used to place them did.

That is the substantive claim this basis makes: it would remove the single
largest source of year-to-year movement in the emissions side of `B`, and it
would do so without smoothing anything — by replacing a derived split with a
reported one.

⚠️ Two honest qualifications. Facility counts step from 14,292 to 22,139 between
2019 and 2020 when NEI reclassified, so the facility series has a vintage seam of
its own — though its churn is low on both sides of it, which is the opposite of
what a seam-driven artefact looks like. And churn is measured on shares, so a
stable double count would not show up here; section 2's deduplication defect has
to be fixed before the levels are trusted.

---

## 6. What it also buys

- **Provenance.** In mining, utilities and manufacturing, 93% of the allocated
  mass would point at named facilities with reported emissions rather than at a
  purchase row. Across every sector the model carries it is 33%, and the
  difference is sectors no facility reports — see §7.
- **Location.** Every facility carries a state and most carry coordinates. If
  this code base later produces state-level EEIO models, the location breakout
  would rest on measured data. That is nearly free now and expensive to retrofit.
- **A category that does not exist today.** `fuel_class` separates fuel a
  facility **bought** from fuel it **made itself** — refinery still gas, coke
  oven gas, blast furnace gas, 46.9 Mt in 2022. Byproduct gas is never a
  purchase, so no row of the Use table can represent it, and attributing it with
  one is a category error rather than an inaccuracy. Tracker row 17.

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
| [#928](https://github.com/cornerstone-data/bedrock/issues/928) | The residual rule for sectors the basis cannot reach | 68 in-scope sectors carrying 50.0 Mt have no facility data, and 90 more are partly covered. Mixing a facility level with a Use-derived remainder needs a stated method |

### Integration

| | | |
|---|---|---|
| [#929](https://github.com/cornerstone-data/bedrock/issues/929) | Build a **new** facility-based GHG FBS alongside the existing one | ⚠️ The current method must keep running. Every before-and-after number here compares against it, and §5's claim is only checkable while both series can be built from the same code on the same span |
| [#930](https://github.com/cornerstone-data/bedrock/issues/930) | Preserve facility location for a future state-level model | Not needed nationally; nearly free now and expensive to retrofit |

### ⚠️ Not a prerequisite, and not a backlog

The 208 sectors with no facility data — 1,410 Mt across all sectors, 67%
of what a vector places — are government, agriculture, trucking and buildings.
They stay on the Use row, which is also where MECS never reached. No facility
reports them because none emits them. In the mining, utilities and manufacturing
scope this basis is for, the equivalent figure is **7% of allocated mass**.
