# A facility-reported basis for industrial emissions

What happens if the sector split for industrial emissions comes from facilities
that reported them, instead of from a purchase row or a survey share.

⚠️ **This is not only about fuel combustion.** It began as a question about the
table 3-11 combustion split, and the combustion case is still §5's. But a
facility reports its combustion, its process units and its fugitives as one
number, and scoring that against everything the inventory gives the sector
turned up a **second, larger finding about the process and fugitive mass the
inventory assigns directly** — §2. The two are separable and are kept separate
below, because they have different remedies.

Measured with **D15** and **D15b** in `B_change_diagnostics.py`
(`--facility-data`), against the production basis on FBS `v0.3.0_99655e9` and
MUT `v0.3.0_4276083`. The facility union is bounded at 2022 by NEI; the
GHGRP-only comparisons in §2 run the full 2017-2024 span.

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

**Today, in two parts.** The *combustion* part is EPA table 3-11, industrial
stationary fuel combustion — a national total spread across sectors by a vector.
`ng_manufacturing` and `coal_manufacturing` ride MECS survey shares;
`petroleum_industrial` and `natural_gas_nonmanufacturing` ride rows of the
nowcast Use table. No sector assignment in it points at a measured source, and
59% of the family moves with the Use table. The *process and fugitive* part is
`Direct`-attributed: the inventory names the sector itself, from its own
petroleum systems, natural gas systems and industrial process tables, with no
vector in between. 699 Mt and 524 Mt respectively, in scope, in 2022.

**Both parts are in question, for different reasons.** The combustion split is
derived from something that moves for non-emissions reasons. The `Direct`
assignment is not derived at all — but it can still name the wrong sector, and
§2 shows it does, by 96.7 Mt in one case.

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

## 2. What the facilities say, against what the inventory assigns

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

### Does the inventory give these sectors what their own facilities report?

**GHGRP is a floor.** It covers only facilities over 25,000 tCO2e, so a sector's
GHGRP total is a **lower bound** on what its facilities emit. Where that floor
clears the *whole* inventory assignment — `allocated` and `Direct` together —
the sector is **under-attributed**, and the statement survives every boundary
objection the half-ratios attract, because it assumes nothing about which
subpart answers which inventory table. **This is the column to read first.**

⚠️ Checked against double counting: across all **7,096 GHGRP facilities in 2022,
not one** reports a subpart sum above its own facility total. #925's
deduplication defect is a property of the GHGRP–NEI *union* and cannot touch a
GHGRP-only column.

**In 23 in-scope sectors the floor clears the entire assignment — 145 Mt.**
Sorted by the floor against the inventory, 2022, Mt:

| sector | `allocated` | `Direct` | **inventory** | subpart C | other subparts | **GHGRP floor** | **floor ÷ inventory** | **Mt** | C ÷ 3-11 | other ÷ `Direct` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `324110` petroleum refineries | 76.2 | 3.6 | 79.7 | 114.8 | 61.6 | 176.4 | **2.21** | **+96.7** | 1.51 | 17.35 |
| `311221` wet corn milling | 7.4 | 0.0 | 7.4 | 14.8 | 0.0 | 14.8 | **2.00** | **+7.4** | 2.00 | — |
| `325120` industrial gas manufacturing | 9.2 | 8.5 | 17.7 | 4.3 | 27.3 | 31.6 | **1.78** | **+13.8** | 0.59 | 3.20 |
| `21311A` other support activities for mining | 4.4 | 0.7 | 5.0 | 0.5 | 8.1 | 8.7 | **1.72** | **+3.6** | 0.13 | 12.03 |
| `322120` paper mills | 13.1 | 0.0 | 13.1 | 12.5 | 3.6 | 16.1 | **1.23** | **+3.0** | 0.98 | — |
| `327310` cement manufacturing | 15.9 | 41.9 | 57.8 | 0.6 | 67.3 | 67.9 | **1.17** | **+10.1** | 0.04 | 1.61 |
| `327400` lime and gypsum product manufacturing | 10.9 | 12.2 | 23.1 | 11.0 | 14.5 | 25.5 | **1.11** | **+2.4** | 1.02 | 1.19 |
| `322130` paperboard mills | 18.4 | 0.0 | 18.4 | 11.2 | 4.7 | 15.9 | 0.87 | -2.5 | 0.63 | — |
| `325310` fertilizer manufacturing | 25.7 | 21.6 | 47.3 | 11.8 | 28.5 | 40.3 | 0.85 | -7.0 | 0.93 | 1.32 |
| `331110` iron and steel mills and ferroalloy manufacturing | 21.7 | 47.0 | 68.8 | 35.0 | 20.9 | 55.9 | 0.81 | -12.9 | 1.63 | 0.44 |
| `325190` other basic organic chemical manufacturing | 55.9 | 11.8 | 67.7 | 45.6 | 6.7 | 52.3 | 0.77 | -15.4 | 1.00 | 0.57 |
| `221200` natural gas distribution | 0.1 | 30.5 | 30.6 | 8.5 | 14.8 | 23.4 | 0.76 | -7.3 | 58.50 | 0.49 |
| `211000` oil and gas extraction | 91.3 | 211.9 | 303.2 | 46.7 | 174.4 | 221.2 | 0.73 | -82.1 | 0.52 | 0.82 |
| `324190` other petroleum and coal products manufacturing | 10.8 | 0.0 | 10.8 | 4.0 | 3.8 | 7.8 | 0.72 | -3.0 | 1.02 | — |
| `325211` plastics material and resin manufacturing | 35.1 | 0.0 | 35.1 | 16.4 | 3.7 | 20.1 | 0.57 | -15.0 | 0.56 | — |
| `212100` coal mining | 8.5 | 55.4 | 63.8 | 0.2 | 28.0 | 28.2 | 0.44 | -35.7 | 0.02 | 0.51 |
| `325180` other basic inorganic chemical manufacturing | 18.8 | 3.3 | 22.1 | 5.6 | 3.5 | 9.0 | 0.41 | -13.0 | 0.38 | 1.05 |
| `325110` petrochemical manufacturing | 47.9 | 20.7 | 68.6 | 22.3 | 4.3 | 26.6 | 0.39 | -42.0 | 1.00 | 0.21 |
| `2122A0` iron, gold, silver, and other metal ore mining | 11.3 | 0.0 | 11.3 | 1.0 | 3.0 | 3.9 | 0.35 | -7.3 | 0.32 | — |
| `221300` water, sewage and other systems | 0.7 | 44.1 | 44.8 | 4.8 | 0.0 | 4.8 | 0.11 | -40.0 | — | 0.00 |

⚠️ **A half-ratio above 1 does not imply under-attribution.** Fertilizer runs
1.32 on the process half and lands at **0.85 on the total**; other basic
inorganic chemicals run 1.05 and land at **0.41**. Both are over-attributed on
the process side and under-covered overall. The correction runs the other way
for cement, whose process half of 1.61 is inflated because subpart H reports a
kiln's fuel together with its calcination — on the total it is **1.17**, and
that is the figure to quote. The total is the test; the halves diagnose *why*.

**Two different findings sit in the positive rows, and they need different
fixes:**

- **Process misattribution**, where `Direct` > 0 — refineries, industrial gas,
  cement, lime. The inventory books this sector's process mass somewhere else.
- **Combustion under-allocation**, where `Direct` = 0 — wet corn milling, paper
  mills, sugar, soybean, ferrous foundries. No process mass is in dispute; the
  table 3-11 vector simply gives these sectors too little. Same finding as the
  subpart C floor breach below, arriving from the other side.

### ⚠️ The largest single finding is a reallocation, not a level error

| | GHGI total | GHGRP floor | gap |
|---|---:|---:|---:|
| `324110` petroleum refineries | 79.7 | 176.4 | **+96.7** |
| `211000` oil and gas extraction | 303.2 | 221.2 | **−82.1** |
| **the pair together** | **383.0** | **397.6** | **+14.6 — ratio 1.04** |

Separately the two read 2.21 and 0.73. **Together they read 1.04.** The
mechanism is visible in the source tables: `211000`'s `Direct` carries 56.4 Mt
of `UMD_GHGIA_T_3_25` and `T_3_26` — the GHGI's petroleum systems tables — while
`324110` receives 3.55 Mt from those same two. **The refining segment of
petroleum systems is being attributed to extraction.** Refineries hold at
+95 to +111 Mt in every year 2017-2024, so this is neither a vintage artefact
nor a single-year excursion.

Always check the obvious counterpart sector before reading a ratio as a level.

### Where GHGRP must not be used

Substituting it where it sits below the inventory would delete real emissions —
33 sectors, 363 Mt in 2022. The largest are petrochemicals −42.0, water and
sewage −40.0, coal mining −35.7, other basic organic −15.4, plastics −15.0,
other basic inorganic −13.0 and iron and steel −12.9. These stay on the GHGI.
`211000` is the exception that proves the rule: its −82.1 is not under-coverage,
it is the refinery mass it should not be holding.

⚠️ **Water and sewage deserves its own line, because the obvious objection was
checked rather than assumed.** Water utilities are largely state and local
**government enterprises** — the reallocation plan puts $68,351m of `S00203`
water output against $15,138m of private `221300`, making the merged sector 82%
government — so a municipally owned plant could report under a government NAICS
and never reach `221300`. It does not: facilities at NAICS 2213 report only
subparts C and D in every year 2017-2022; GHGRP facilities *named* as water or
wastewater operators total 12 facilities and 1.04 Mt across all NAICS; all
GHGRP mass at NAICS 92xx is 3.21 Mt and is mostly military bases. Publicly
owned treatment works are not a GHGRP source category whoever owns them.

### The like-for-like half, where the combustion case lives

**D15b**, `facility_scope_split.csv`. GHGRP subpart C is stationary fuel
combustion and every other subpart is process or fugitive, so each half scores
against the half of the inventory it corresponds to — subpart C against the
table 3-11 allocation the vector splits, and the rest against `Direct`. In
scope, 2022:

| | inventory | GHGRP | ratio |
|---|---:|---:|---:|
| combustion — table 3-11 vs subpart C | 592.9 | 431.7 | 0.73 |
| process and fugitive — `Direct` vs every other subpart | 524.0 | 494.0 | 0.94 |

⚠️ **That 0.94 is offsetting errors, not coverage.** Per sector the process half
runs from 0.00 to 17.35, median 0.73. Five in-scope sectors carrying 147.6 Mt of
`Direct` get 40.4 Mt from GHGRP — water and sewage 44.1 Mt against nothing at
all, iron and steel 0.44, natural gas distribution 0.49, petrochemicals 0.21,
semiconductors 0.06. **The facility union is not a candidate replacement for
`Direct`** — [#953](https://github.com/cornerstone-data/bedrock/issues/953).

Subpart C is threshold-limited, so it too is a **floor**, and against it
combustion meets combustion with no process mass on either side. In **21
in-scope sectors the current method allocates less table 3-11 combustion than
those sectors' own facilities reported: 184 Mt against 257 Mt**, a 72 Mt
shortfall led by refineries (+38.7), iron and steel (+13.5), natural gas
distribution (+8.4) and wet corn milling (+7.4).

## 3. ⚠️ What a facility basis can and cannot do to `Direct`

**524 Mt of the process emissions this widening pulls in are already
`Direct`-attributed** — the inventory names the sector itself, with no Use row,
no MECS and no vector in between.

⚠️ **An earlier version of this note said a facility basis "cannot improve an
assignment that was never derived". That is half right and the half it misses
is the larger finding.** A facility basis cannot *re-derive* a `Direct` row —
there is no vector to replace, and GHGRP does not report most of what the
inventory books directly, so it cannot supply the level either (§2, and
[#953](https://github.com/cornerstone-data/bedrock/issues/953)). What it can do
is show that a `Direct` row is **on the wrong sector**, because it observes
which facility emitted it. That is exactly the refineries and `211000` result:
no level is in dispute across the pair — 383.0 Mt against a 397.6 Mt floor,
ratio 1.04 — only which of the two industries holds it.

So relocation is on offer where re-derivation is not, and the two must not be
confused: `allocated_Mt` is what a facility basis can *restate*, `direct_Mt` is
what it can *relocate*.

The columns are therefore kept apart, and `allocated_Mt` is the one to rank on
for the combustion case: across
the 3,461 Mt the widened comparison covers, 2,102 Mt is vector-allocated and
improvable; 1,359 Mt is already `Direct` and is not. ⚠️ These three totals are
the *only* figures in this note the vintage barely moved, and the reason is
instructive: the retired build differs from the published one purely by
redistributing emissions across sectors, so every national total is the same to
within a rounding step while every per-sector split changed.
Widening the *comparison* was necessary to stop scoring kilns against the wrong
denominator. It does not widen what is on offer.

---

## 4. The combustion case on `211000`, in depth

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

### Per sector, on levels rather than shares — and the two tests agree

⚠️ **A different measure from the one above, and a weaker result.** The table
above is *share* churn: how much the split reshuffles mass between sectors,
which is what makes `B` rocky. This one is *level* volatility: the median
absolute year-on-year change in each sector's own number, 2017-2024, current
basis against GHGRP. A sector's level can move for real reasons, so this is the
harder test and the honest one to show next to it.

| sector | mean inventory Mt | current: median year-on-year | GHGRP: median year-on-year | ratio |
|---|---:|---:|---:|---:|
| `211000` oil and gas extraction | 287.9 | 6.2% | 6.2% | 1.01 |
| `324110` petroleum refineries | 81.7 | 5.9% | **0.8%** | **0.14** |
| `325190` other basic organic chemical manufacturing | 79.4 | 4.9% | **2.0%** | **0.40** |
| `212100` coal mining | 74.0 | 3.1% | 10.5% | 3.38 |
| `331110` iron and steel mills and ferroalloy manufacturing | 69.5 | 5.1% | 5.1% | 1.01 |
| `325110` petrochemical manufacturing | 68.3 | 2.9% | **2.8%** | **0.97** |
| `327310` cement manufacturing | 53.6 | 3.5% | **1.9%** | **0.54** |
| `221300` water, sewage and other systems | 44.7 | 0.7% | 4.0% | 5.71 |
| `325310` fertilizer manufacturing | 42.6 | 3.6% | **1.7%** | **0.48** |
| `325211` plastics material and resin manufacturing | 36.3 | 2.9% | 6.9% | 2.34 |
| `221200` natural gas distribution | 30.4 | 1.0% | 4.3% | 4.43 |
| `325180` other basic inorganic chemical manufacturing | 24.9 | 6.6% | **5.1%** | **0.77** |
| `327400` lime and gypsum product manufacturing | 21.7 | 6.3% | **4.5%** | **0.71** |
| `322130` paperboard mills | 17.1 | 2.7% | **0.8%** | **0.31** |
| `325120` industrial gas manufacturing | 16.9 | 8.9% | **3.0%** | **0.34** |
| `2122A0` iron, gold, silver, and other metal ore mining | 15.5 | 25.7% | **7.7%** | **0.30** |
| `322120` paper mills | 15.4 | 1.9% | 2.6% | 1.34 |
| `324190` other petroleum and coal products manufacturing | 9.5 | 6.5% | **2.7%** | **0.41** |
| `21311A` other support activities for mining | 9.4 | 14.4% | **7.5%** | **0.52** |
| `327200` glass and glass product manufacturing | 8.6 | 2.4% | 4.3% | 1.74 |
| `2123A0` other nonmetallic mineral mining and quarrying | 8.0 | 10.1% | **7.7%** | **0.77** |
| `31161A` animal (except poultry) slaughtering, rendering, and processing | 6.5 | 4.0% | **3.8%** | **0.94** |
| `334413` semiconductor and related device manufacturing | 6.1 | 4.6% | **3.7%** | **0.81** |
| `311221` wet corn milling | 6.1 | 2.4% | **1.1%** | **0.48** |
| `3259A0` all other chemical product and preparation manufacturing | 5.7 | 4.5% | **3.9%** | **0.86** |
| **median over 25 sectors** | | **4.5%** | **3.9%** | **0.86x** |

**Applied everywhere, GHGRP buys little — 4.5% against 3.9%, a 0.86x ratio, and
it is steadier in only 17 of 25 sectors.** That average hides the finding:

| group (by the accuracy test above) | Mt | current | GHGRP | ratio |
|---|---:|---:|---:|---:|
| **GHGRP above the inventory** — the "use it" list, 8 sectors | 213 | 5.7% | **2.3%** | **0.41x** |
| middle, 0.6-1.0 — 8 sectors | — | — | — | 0.74x |
| **GHGRP well below, under 0.6** — 9 sectors | 282 | 4.3% | 6.1% | **1.42x — worse** |

Mass-weighted. **The accuracy test predicts the stability gain.** Where GHGRP
reports more than the inventory assigns, it is also markedly steadier — 0.41x,
and refineries alone go from 5.9% to **0.84%**. Where GHGRP falls well short it
is *less* stable, because a partial, threshold-limited sample of a sector jumps
around as facilities cross the threshold. Selecting sectors on accuracy is
therefore not a separate step from selecting them on stability: it is the same
step, and applying GHGRP indiscriminately would hurt 282 Mt to help 213 Mt.

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
| [#952](https://github.com/cornerstone-data/bedrock/issues/952) | GHGRP for the directly attributed process and fugitive mass, 2017-2024 | Where §2's floor clears the whole assignment the inventory is under-attributing, and the GHGI could not use GHGRP for 2024 at all. ⚠️ The 2024 GHGRP build lacks subparts E, BB, CC, L and O — 5.74 Mt in scope in 2023, none of it in the sectors §2 puts on the use-it list |

Together these take the basis from 2017-2022 to the full 2017-2024 nowcast span,
so §5's churn comparison could be stated for every year the model produces.

### Method decisions

| | | |
|---|---|---|
| [#927](https://github.com/cornerstone-data/bedrock/issues/927) | Lease and plant fuel is self-supplied but classified as purchased | An oil and gas producer burning its own field gas is burning natural gas, and the SCC says natural gas. Same "no purchase exists" defect as byproduct gas, and the structural reason `211000` sits below the floor in 2017 |
| [#928](https://github.com/cornerstone-data/bedrock/issues/928) | The **coverage** residual: sectors the basis cannot reach | 68 in-scope sectors carrying 50.0 Mt have no facility data, and 90 more are partly covered. Mixing a facility level with a Use-derived remainder needs a stated method |
| [#953](https://github.com/cornerstone-data/bedrock/issues/953) | The **scope** residual: a facility total spans `allocated` and `Direct` | §2's denominator has to carry both because the facility number does. That is fine for a comparison, but it means the facility total cannot be dropped in as a *level* for the only part §3 says is improvable — cement's weight is 89.2 against an allocated mass of 15.9 |

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
