# A facility-reported basis for industrial emissions

What happens if the sector split for industrial emissions comes from facilities
that reported them, instead of from a purchase row or a survey share.

Industrial emissions reach a sector two ways in this model, and **this note
examines both**. Fuel combustion is spread across sectors by a vector — MECS
survey shares, or a row of the nowcast Use table. Process and fugitive emissions
are attributed directly: the inventory names the sector itself. 699 Mt and
524 Mt respectively, in scope, in 2022. A facility reports all of it as one
number, which makes both testable against the same evidence.

There are two findings, they are separable, and they have different remedies.
**§2 is the larger one: 23 sectors are under-attributed by 145 Mt** against a
measured floor, and the single biggest case is a misallocation of refinery mass
to oil and gas extraction. **§5 is the original one: the combustion vector
reshuffles two and a half times as much between years as a facility basis
would**, which is what makes `B` rocky.

Measured with **D15** and **D15b** in `B_change_diagnostics.py`
(`--facility-data`), against the production basis on FBS `v0.3.0_99655e9` and
MUT `v0.3.0_4276083`. The facility union is bounded at 2022 by NEI; the
GHGRP-only comparisons in §2 run the full 2017-2024 span.

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

**2022: 1,277 Mt.** It was 1,486 Mt until #925 was settled — see below.

> ⚠️ **The tables in sections 2 and 3 were computed on the 1,486 Mt union and
> have not been recomputed.** #925 took 209 Mt out of it, almost all of it NEI
> mass that was a second copy of a site the GHGRP already reported, and the
> facility column of every ratio here is therefore high. The live numbers are
> the ones `B_change_diagnostics` prints; what changed is set out in
> **section 2.0** immediately below.

---

## 2. What the facilities say, against what the inventory assigns

### 2.0 What #925 changed

`FRS_ID` was the only thing saying that a GHGRP report and an NEI report
describe one plant, and it says so only where FRS has filed both programs under
one registry record. Where FRS filed them under two, the site entered the union
twice — and it had done so for enough of them that recognising the duplicates
finds an NEI counterpart for another **17.4 percentage points** of GHGRP
facilities. Three changes, measured on 2022:

| | union Mt | NEI-only Mt | `324110` coverage | sectors over 1.5x | overshoot Mt |
|---|---:|---:|---:|---:|---:|
| as section 2 was computed | 1,486.3 | 361.4 | 2.86 | 14 | 321.9 |
| + rebuilt facility match list | 1,328.8 | 203.9 | 2.41 | 9 | 203.4 |
| + same-site fallback on the inventories' own addresses | 1,310.6 | 185.7 | 2.40 | 9 | 201.3 |
| + mobile source codes dropped | **1,277.3** | **152.4** | **2.40** | **9** | **168.1** |

- **The rebuilt match list** is StEWI
  [#4](https://github.com/cornerstone-data/standardizedinventories/pull/4),
  which folds FRS registry records that describe one site. The share of GHGRP
  facilities with an NEI counterpart goes from 42.3% to 59.7% in 2022.
- **The same-site fallback** applies the same rule a second time in
  `_same_site_after_FRS`, to the addresses GHGRP and NEI report for themselves,
  reaching the sites where FRS's own attributes disagree.
- **Mobile source codes** were never stationary combustion. NEI files aircraft
  at airports as point sources under SCC `2275`, whose first digit is the same
  as stationary internal combustion, and 33.6 Mt of it was entering the union.
  `48A000` read **7.63x** the inventory on that alone.

⚠️ **The 668 Mt in #925 was the right symptom and the wrong measure.** It counted
GHGRP mass with no NEI match, which includes every landfill, pipeline and
supplier that NEI has no reason to hold. The measure that matters is NEI mass at
a site the GHGRP already reported: **374.9 Mt in 2022 before the rebuilt list and 31.1 Mt after**, a 92%
reduction, with the fallback and the mobile-source fix taking most of what is
left.

**What is still over.** `facility_overshoot_guard` (D15c) is the standing check,
and `--check-facility-overshoot` fails on anything it cannot account for. After
all three fixes, 2022:

| verdict | sectors | overshoot Mt |
|---|---:|---:|
| `reallocation` — a counterpart sector is named and the pair clears | 2 | 118.2 |
| `named` — a boundary difference is on record, not nettable | 1 | 8.0 |
| `unexplained` | **6** | **41.9** |

Refineries are the whole of the first row: `324110` reads 2.40 alone and the
pair with `211000` reads **1.15**, because the inventory books the refining
segment of its petroleum systems tables to extraction. The six unexplained
sectors — industrial gas 16.7 Mt, wet corn milling 11.7, and four smaller — are
what a level-based claim still cannot rest on.

**Scope: mining, utilities and manufacturing** — BEA detail codes beginning 21,
22 and 31-33, 233 sectors. These are the industries whose *fuel combustion*
happens at a plant somebody reports, and the scope was drawn for the combustion
case.

⚠️ **It is too narrow for the attribution case, and two large sectors are
outside it.** `562212` solid waste landfill carries 127.0 Mt of directly
attributed inventory against **96.3 Mt of facility-reported landfill CH4**, and
`486000` pipeline transportation carries 115.5 Mt — all of it `Direct` —
against **67.0 Mt** of GHGRP. **197.9 Mt of GHGRP mass sits outside this
scope**, none of it electric power, which is excluded separately for eGRID.
Waste is additionally invisible rather than merely out of scope: the
NAICS-to-BEA lookup collapses `562212` onto a `562000` the emissions panel does
not carry, because the model disaggregates waste past BEA's single code and
gives the MUTs that finer resolution. GHGRP's own facility NAICS already
matches the model's codes, so the fix is the lookup.
[#955](https://github.com/cornerstone-data/bedrock/issues/955).

Within the scope as drawn, everything a vector places that is *not* here —
government buildings, livestock, trucking, real estate — is mobile, biological
or diffuse, and no facility reports it.

⚠️ Two sectors are excluded from *both* sides of the comparison: `221100`
electric power, which runs on eGRID in this model and is filtered out of the
facility union by construction, and `F01000` personal consumption, which has no
gross output and is outside this module entirely.

⚠️ **Scoring the basis across every sector would measure the boundary, not the
basis.** In scope it reaches 93% of the allocated mass; across all 393 sectors
it reaches 33%, and the difference is entirely sectors that were never
candidates. Every coverage figure below is the in-scope one.

### What is in scope, and which part of it is even improvable

| | Mt | |
|---|---:|---|
| **vector-allocated** | **699** | placed by MECS or a Use row — the only part a facility basis could restate |
| inventory-assigned (`Direct`) | 524 | the inventory names the sector itself; counted, never improvable — §3 |
| **total inventory, in scope** | **1,223** | |

### How much of the allocated 699 Mt has facility data behind it

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
+95 to +111 Mt in every year 2017-2024, so this is not a single-year
excursion.

Always check the obvious counterpart sector before reading a ratio as a level.

### Split the comparison in two: combustion and process

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

Both halves being floors is what makes the next section possible: each one can
be ruled on separately.

### ⚠️ The verdict is per half, not per sector

**Both halves are threshold-limited, so both are floors**, and each gets its own
test: subpart C against the table 3-11 allocation, and every other subpart
against `Direct`. A sector can clear one floor and fall well short of the other,
and most do — so there is no such thing as a sector where GHGRP "should not be
used". There are sectors where it should not be used *for one part*.

**Iron and steel is the case that makes the point.** On the total it reads 0.81,
which looks like a sector to leave alone. Split:

| | inventory | GHGRP floor | verdict |
|---|---:|---:|---|
| combustion — table 3-11 vs subpart C | 21.5 | 35.0 | **use it: +13.5 Mt under-allocated** |
| process — `Direct` vs other subparts | 47.0 | 20.9 | leave it: GHGRP has 44% of it |

The combustion half is one of the largest floor breaches in the model. A
total-only verdict would have thrown it away.

**Of the 210 in-scope sectors that sit below the inventory on the total, 7
breach the combustion floor anyway, by 27 Mt** — iron and steel +13.5, natural
gas distribution +8.4, water and sewage +4.8, and four smaller.

Sectors with at least 4 Mt on either side, sorted by the combustion gap:

| sector | table 3-11 | subpart C | **combustion** | `Direct` | other subparts | **process** |
|---|---:|---:|---|---:|---:|---|
| `324110` petroleum refineries | 76.1 | 114.8 | **use, +38.7** | 3.6 | 61.6 | **use, +58.1** |
| `331110` iron and steel mills and ferroalloy manufacturing | 21.5 | 35.0 | **use, +13.5** | 47.0 | 20.9 | no, -26.2 |
| `221200` natural gas distribution | 0.1 | 8.5 | **use, +8.4** | 30.5 | 14.8 | no, -15.6 |
| `311221` wet corn milling | 7.4 | 14.8 | **use, +7.4** | 0.0 | 0.0 | — none |
| `221300` water, sewage and other systems | 0.0 | 4.8 | **use, +4.8** | 44.1 | 0.0 | no, -44.1 |
| `311300` sugar and confectionery product manufacturing | 4.1 | 4.6 | **use, +0.5** | 0.0 | 1.1 | — none |
| `2123A0` other nonmetallic mineral mining and quarrying | 6.9 | 7.3 | **use, +0.4** | 0.0 | 1.4 | — none |
| `327400` lime and gypsum product manufacturing | 10.8 | 11.0 | **use, +0.2** | 12.2 | 14.5 | **use, +2.3** |
| `325190` other basic organic chemical manufacturing | 45.6 | 45.6 | no, 0.0 | 11.8 | 6.7 | no, -5.1 |
| `325110` petrochemical manufacturing | 22.3 | 22.3 | no, -0.1 | 20.7 | 4.3 | no, -16.4 |
| `334413` semiconductor and related device manufacturing | 1.0 | 0.8 | no, -0.2 | 5.2 | 0.3 | no, -5.0 |
| `322120` paper mills | 12.8 | 12.5 | no, -0.3 | 0.0 | 3.6 | — none |
| `325310` fertilizer manufacturing | 12.6 | 11.8 | no, -0.8 | 21.6 | 28.5 | **use, +6.9** |
| `327200` glass and glass product manufacturing | 5.9 | 5.0 | no, -0.9 | 2.0 | 1.9 | no, -0.1 |
| `33131B` aluminum product manufacturing from purchased aluminum | 4.3 | 2.0 | no, -2.3 | 0.0 | 0.0 | — none |
| `325120` industrial gas manufacturing | 7.2 | 4.3 | no, -2.9 | 8.5 | 27.3 | **use, +18.8** |
| `3259A0` all other chemical product and preparation manufacturing | 4.4 | 1.4 | no, -3.0 | 0.0 | 0.0 | — none |
| `21311A` other support activities for mining | 4.3 | 0.5 | no, -3.8 | 0.7 | 8.1 | **use, +7.4** |
| `332800` coating, engraving, heat treating and allied activities | 4.1 | 0.3 | no, -3.9 | 0.0 | 0.0 | — none |
| `324121` asphalt paving mixture and block manufacturing | 4.2 | 0.0 | no, -4.2 | 0.0 | 0.0 | — none |
| `312110` soft drink and ice manufacturing | 4.3 | 0.0 | no, -4.3 | 0.0 | 0.0 | — none |
| `31161A` animal (except poultry) slaughtering, rendering, and processing | 7.0 | 2.7 | no, -4.4 | 0.0 | 0.8 | — none |
| `322210` paperboard container manufacturing | 4.7 | 0.0 | no, -4.7 | 0.0 | 0.0 | — none |
| `325510` paint and coating manufacturing | 5.9 | 0.0 | no, -5.9 | 0.0 | 0.0 | — none |
| `325610` soap and cleaning compound manufacturing | 6.5 | 0.3 | no, -6.2 | 0.0 | 0.0 | — none |
| `322130` paperboard mills | 17.8 | 11.2 | no, -6.6 | 0.0 | 4.7 | — none |
| `212100` coal mining | 8.3 | 0.2 | no, -8.1 | 55.4 | 28.0 | no, -27.4 |
| `325180` other basic inorganic chemical manufacturing | 14.9 | 5.6 | no, -9.3 | 3.3 | 3.5 | **use, +0.2** |
| `325211` plastics material and resin manufacturing | 29.5 | 16.4 | no, -13.0 | 0.0 | 3.7 | — none |
| `327310` cement manufacturing | 15.9 | 0.6 | no, -15.3 | 41.9 | 67.3 | **use, +25.4** |
| `211000` oil and gas extraction | 90.1 | 46.7 | no, -43.3 | 211.9 | 174.4 | no, -37.5 |

⚠️ **A "no" on the combustion half has two possible causes and they are not the
same finding.** Either GHGRP genuinely under-covers the sector's combustion, or
a process subpart is reporting the fuel and subpart C never sees it. Cement is
the proven second case — subpart H carries the kiln's fuel with its calcination,
which is why it reads −15.3 on combustion and +25.4 on process. Read a
combustion "no" as *do not substitute here*, never as *the inventory is right*.

⚠️ **Water and sewage deserves its own line, because the obvious objection was
checked rather than assumed.** Water utilities are largely state and local
**government enterprises** — the reallocation plan puts $68,351m of `S00203`
water output against $15,138m of private `221300`, making the merged sector 82%
government — so a municipally owned plant could report under a government NAICS
and never reach `221300`. It does not: facilities at NAICS 2213 report only
subparts C and D in every year 2017-2022; GHGRP facilities *named* as water or
wastewater operators total 12 facilities and 1.04 Mt across all NAICS; all
GHGRP mass at NAICS 92xx is 3.21 Mt and is mostly military bases. Publicly
owned treatment works are not a GHGRP source category whoever owns them — which
is why its process half is 0.00 while its combustion half still clears by
4.8 Mt.

## 3. ⚠️ What a facility basis can and cannot do to `Direct`

**524 Mt of the process emissions this widening pulls in are already
`Direct`-attributed** — the inventory names the sector itself, with no Use row,
no MECS and no vector in between.

⚠️ **"A facility basis cannot improve an assignment that was never derived" is
half right, and the half it misses is the larger finding.** A facility basis
cannot *re-derive* a `Direct` row —
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
1,359 Mt is already `Direct`. Widening the *comparison* was necessary to stop
scoring kilns against the wrong denominator; it does not by itself widen what is
on offer.

---

## 4. The combustion case on `211000`, in depth

Three independent checks, all pointing the same way, all on `211000` — the
largest oscillating cell in the B panel:

1. **It breaches a measured floor.** GHGRP subpart C is a lower bound, since it
   only covers facilities over the reporting threshold. In 2020 the current basis
   allocates oil and gas extraction **37.7 Mt where its own facilities reported
   46.5 Mt** — 1.23x below a floor. 2017 breaches it too, and harder, at 0.66.
   The breach is real but modest: a factor of 1.2, not of 5.
2. **Its driver is a coefficient nothing corroborates.** Purchases of refined
   petroleum per $100 of the sector's own gross output run 0.85 → 2.64 → 1.69,
   a 3.11x rise at the 98th percentile of that Use row, against a median buyer
   of 0.91x. Both sides are nominal and every buyer purchases the same commodity,
   so no price move can produce it. [#922](https://github.com/cornerstone-data/bedrock/issues/922).
3. **The physical series does not move.** GHGRP subpart C for the same facilities
   holds 44.9-48.4 Mt across 2017-2024, a **1.08x span**, while the current basis
   spans **3.05x**.

⚠️ **There is no fourth line of evidence here, and it is worth saying why.**
`share_shift_pp` for `211000` is **+1.81 pp** — against the full facility union
the basis would give the sector *more* share, not less, which reads at first as
a contradiction of the three checks above. It is not. They compare our allocation against subpart C, which is
combustion only and threshold-limited, while the share shift compares it against
the full facility union of 256 Mt, which includes the process units GHGRP books
under other subparts. The three checks say the sector's fuel combustion is
allocated erratically and sometimes below a measured floor. They do not say the
sector is over-allocated overall, and §5 does not depend on it.

---

## 5. The case on smoothing

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

**The facility basis reshuffles about two fifths as much**, and the gap is
widest exactly where the B panel hurts most. ⚠️ The facility column is external
data, which makes it the control on this measure: it should not move when our
own build does, and it does not.

⚠️ **Look at 2020 and 2021.** The current basis reshuffles 11.9 and 24.0
percentage points; the facility basis reshuffles 5.6 and 3.7, which is what it
does in every other year. Tracker row 9 attributes the 2021 spike to the COVID
rebound and reads it as a real event not to be smoothed. Against a facility
series that barely moves through the same two years, **most of that spike is the
allocation vector, not the pandemic** — 2021 is the year with the widest gap
between the two bases by a clear margin. Emissions at those facilities did not
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

Mass-weighted, and ⚠️ **grouped on the total, which is the coarser cut** — the
per-half verdict above splits several of these sectors. The statement here is
about substituting a sector's whole number, which is the case the grouping
describes. **The accuracy test predicts the stability gain.** Where GHGRP
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

⚠️ Two honest qualifications. Two NEI vintage seams sit next to each other and
are easy to confuse. **Facility counts** step between 2019 and 2020 (NEI CO2
reporters 12,030 → 19,989; D15 union 12,827 → 15,743). The **SCC CO2
reclassification** lands a year later, 2020 → 2021: combustion SCC share of
on-site CO2 goes 2.9% → 75.5% with the national total flat (2,333 → 2,427 Mt).
Share churn stays low on both sides of both seams (3.9–5.6 pp), which is the
opposite of a seam-driven artefact — so §5's claim is not an inventory-coding
story. `fuel_class` read from SCC digits is a different matter and is gated at
2021 (tracker row 19 / [#926](https://github.com/cornerstone-data/bedrock/issues/926)).
And churn is measured on shares, so a stable double count would not show up
here; the FRS deduplication in §2.0 is what makes the *levels* trustworthy.

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
  facility **bought** from fuel that never changed hands — `self_supplied`,
  **144.9 Mt in 2022**. Fuel nobody sold is never a purchase, so no row of the
  Use table can represent it, and attributing it with one is a category error
  rather than an inaccuracy. Tracker row 17, and see §8 for where the other
  two thirds of that mass came from. ⚠️ **NEI SCC and NEI-share routes start in
  2021** (`NEI_FUEL_CLASS_FIRST_YEAR`); before that, NEI rows in the union stay
  `unclassified` and only GHGRP subpart W / segment evidence fills the class
  (tracker row 19).

---

## 7. Prerequisites for implementation

Tracked on
[**Integrate facility-reported GHGs for attribution**](https://github.com/orgs/cornerstone-data/projects/34),
which feeds
[B Smoothing Phase 1](https://github.com/orgs/cornerstone-data/projects/31)
through a single issue:
[#929](https://github.com/cornerstone-data/bedrock/issues/929) is the
integration step and is cross-listed on both boards, so the B smoothing work
depends on that one while the rest of project 34 feeds it.

### Blocking — the levels cannot be quoted until these are settled

| | | why it blocks |
|---|---|---|
| ~~[#925](https://github.com/cornerstone-data/bedrock/issues/925)~~ **settled** | FRS deduplication between GHGRP and NEI | 209 Mt came out of the union — see §2.0. What remains is 41.9 Mt over six sectors with no cause on record, which `--check-facility-overshoot` fails on. ⚠️ Never touched §5 — churn is measured on shares, and a stable double count does not move a share |
| ~~[#926](https://github.com/cornerstone-data/bedrock/issues/926)~~ **settled** | NEI's 2020/2021 SCC reclassification | Combustion SCC share of on-site CO2 goes 2.9% → 75.5% with the total flat. `fuel_class` from SCC digits is defined from 2021 only (`NEI_FUEL_CLASS_FIRST_YEAR`); union levels on SCC branches 1-3 stay continuous. Tracker row 19. For a continuous combustion vs process *coding* series across that seam, `nei_combustion_process_backcast` is written on every `B_change_diagnostics.main` run — levels unchanged, `fuel_class` gating unchanged |

### NEI SCC reclassification evidence (#926)

On-site NEI CO2 (SCC branches 1-3), from `nei_scc_reclassification_summary`.
Ungated `fuel_class_*` columns show what SCC digits *would* assign in every
year; production `facility_combustion` does not use that assignment before 2021.

| year | total Mt | SCC 1 | SCC 2 | SCC 3 | combustion % | NEI CO2 facilities | 6-digit NAICS % | D15 union facilities |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 2682.3 | 39.5 | 58.8 | 2584.0 | 3.7 | 11,714 | 75.3 | 12,556 |
| 2018 | 2677.6 | 30.9 | 50.1 | 2596.5 | 3.0 | 11,872 | 75.6 | 12,842 |
| 2019 | 2580.6 | 30.6 | 51.7 | 2498.3 | 3.2 | 12,030 | 76.3 | 12,827 |
| 2020 | 2332.9 | 25.0 | 42.0 | 2265.9 | 2.9 | **19,989** | 60.9 | **15,743** |
| 2021 | 2427.4 | **1178.0** | **655.6** | 593.8 | **75.5** | 17,786 | **99.5** | 13,963 |
| 2022 | 2446.9 | 1158.3 | 711.9 | 576.7 | 76.4 | 18,230 | 99.5 | 14,290 |

Facility-count step: **2019→2020**. SCC / `fuel_class` step: **2020→2021**.
Six-digit NAICS completeness jumps with the SCC step, after a dip in 2020.

`nei_combustion_process_backcast` (always written from
`B_change_diagnostics.main` as `nei_combustion_process_backcast.csv`) restates
only the combustion vs process coding for years before 2021 from 2021/2022
FacilityID twins (then NAICS-6 / sector means). Contemporaneous SCC 1-3 totals
are fixed; national backcast combustion share sits near the post-2021 raw share
(~75%). It does not invent `purchased` / `self_supplied` and does not change
`facility_combustion`. Twin coverage and 2021/2022 share drift are on
[#926](https://github.com/cornerstone-data/bedrock/issues/926#issuecomment-5815291850).

### Coverage — needed before the basis spans the model

| | | |
|---|---|---|
| [#931](https://github.com/cornerstone-data/bedrock/issues/931) | GHGRP 2024 into StEWI from the FOIA'd static files | GHGRP stops at 2023 |
| [#932](https://github.com/cornerstone-data/bedrock/issues/932) | NEI (EIS) 2023 | In StEWI ([standardizedinventories#5](https://github.com/cornerstone-data/standardizedinventories/pull/5)); Option A inventories and region sources on Cornerstone GCS. EPA omitted Carbon Dioxide, so D15 mass and `fuel_class` weights still stop at 2022 (`NEI_LAST_YEAR`). Roster / NAICS only |
| [#970](https://github.com/cornerstone-data/bedrock/issues/970) | Carry NEI CO2 mix (and grade mass) past 2022 | With no CO2 in NEI 2023+, D15 needs a prior-year mix for matched GHGRP facilities and a graded level for below-threshold NEI-only mass |
| [#952](https://github.com/cornerstone-data/bedrock/issues/952) | GHGRP for the directly attributed process and fugitive mass, 2017-2024 | Where §2's floor clears the whole assignment the inventory is under-attributing, and the GHGI could not use GHGRP for 2024 at all. ⚠️ The 2024 GHGRP build lacks subparts E, BB, CC, L and O — 5.74 Mt in scope in 2023, none of it in the sectors §2 puts on the use-it list |

Together these take the basis from 2017-2022 CO2 coverage toward the full
2017-2024 nowcast span. NEI 2023 does not extend the CO2 series; #970 does.

### NEI 2022 → 2023 continuity (measured on StEWI Option A)

EPA's OAR 2023 point dump has pollutant types CAP / HAP / Other / PFAS only —
no Carbon Dioxide rows in any region parquet, and none after Option A. Against
2022 (where combustion SCCs are 76% of NEI CO2 after the #926 reclass):

| | 2022 | 2023 |
|---|---:|---:|
| facilities | 87,683 | 99,186 |
| six-digit NAICS share | 98.7% | 99.0% |
| CO2 total (Mt) | 2,460 | **0** |
| facilities with CO2 | 18,833 | **0** |

Verdict: safe to use 2023 for roster / NAICS; **do not** raise `NEI_LAST_YEAR`
or recompute D15 `fuel_class` weights from 2023. Continuity of the CO2 series
stops at 2022; further years are #970.

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
| [#955](https://github.com/cornerstone-data/bedrock/issues/955) | Admit waste and pipelines to the scope, and fix the waste NAICS mapping | 197.9 Mt of GHGRP sits outside 21/22/31-33. `562212` maps onto a `562000` the model does not carry, so every waste facility drops on the floor despite GHGRP matching the model's own codes |

### ⚠️ Not a prerequisite, and not a backlog

The 208 sectors with no facility data — 1,410 Mt across all sectors, 67%
of what a vector places — are largely government, agriculture, trucking and
buildings. They stay on the Use row, which is also where MECS never reached. In
the mining, utilities and manufacturing scope this basis is for, the equivalent
figure is **7% of allocated mass**.

⚠️ **"No facility reports them" is not true of all of them.** Waste and
pipelines are both facility-reported and both outside the scope as drawn — see
§2 and [#955](https://github.com/cornerstone-data/bedrock/issues/955). This
paragraph covers the genuinely diffuse remainder, not everything excluded.

---

## 8. Lease and plant fuel: what subpart W says that subpart C cannot (#927)

⚠️ **Where the code lives.** This is pipeline data, not a diagnostic, so it does
not sit in `B_change_diagnostics.py`. The two views are acquired and cached by
[`bedrock/extract/epa/EPA_GHGRP_SubpartW.py`](../../extract/epa/EPA_GHGRP_SubpartW.py)
and classified by
[`bedrock/transform/ghg/ghgrp_subpart_w.py`](../../transform/ghg/ghgrp_subpart_w.py);
D14 and D15 are consumers of both, and so is the facility-based FBS when #929
builds it.

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


---

## 9. The 145 Mt under-attribution: what a vector can restate, and what it cannot (#962)

§2 finds the sectors whose GHGRP floor clears their **whole** inventory
assignment, `allocated` and `Direct` together. **D16** follows that through: the
gap does not have one fix, and the split decides which piece of work owns it.

⚠️ **This section replaces an earlier one that asked the question family by
family** — "could a facility vector carry non-energy use, carbonate use, urea?"
— and ruled each out on which subparts its facility mass sat in. That is the
half-ratio reasoning §2 warns against, and the verdict it produced ("the basis
stops at combustion") was withdrawn. The sector total is the test.

### It is a standing condition, not a vintage artefact

| year | sectors | gap, Mt | **restate** | **relocate** |
|---|---:|---:|---:|---:|
| 2018 | 20 | 160.5 | 23.0 | 137.5 |
| 2019 | 20 | 145.3 | 21.9 | 123.4 |
| 2020 | 22 | 153.3 | 22.8 | 130.5 |
| 2021 | 21 | 155.5 | 23.9 | 131.6 |
| 2022 | 23 | 144.7 | 17.8 | 126.8 |
| 2023 | 25 | 153.9 | 20.8 | 133.1 |
| 2024 | 21 | 152.2 | 15.8 | 136.4 |

**12 sectors are under-attributed in every year of the span.** Following §3,
`restate` is where `Direct` is immaterial — no process mass is in dispute, the
table 3-11 vector simply gives the sector too little — and `relocate` is where
`Direct` is material, which a vector cannot touch at all.

### Seven eighths of it is not a vector's to close

| 2022 | Mt | sectors |
|---|---:|---:|
| **restate** — combustion under-allocation | 17.9 | 17 |
| **relocate** — process misattribution | **126.7** | 6 |

`324110` petroleum refineries alone is 96.7 Mt of the second: allocated 76.2,
`Direct` 3.6, against a floor of 176.4. That is §3's pair result — no level is in
dispute across refineries and `211000` together, only which industry holds it —
and it belongs to
[#953](https://github.com/cornerstone-data/bedrock/issues/953). The others are
`325120` industrial gas 13.8, `327310` cement 10.1, `21311A` 3.6, `327400` lime
2.4, `331490` 0.1.

⚠️ **So do not read the residual gap after #929 lands as the integration having
failed.** The facility vector closes the 17.9 Mt, because that is what a better
combustion split does. It was never going to close the rest.

### And the restate half needs no sections beyond the five

In those 23 sectors, **98% of the allocated mass is table 3-11** — 156.7 Mt of
160.5 Mt. Every other vector-attributed family together is **3.8 Mt across all 23
sectors**: non-energy use 2.18, carbonate use 0.83, `T_4_52` 0.44, construction
and mining 0.24, refrigerants 0.06. The tail families are not where the shortfall
lives, and that conclusion needs no claim about which subpart answers which
inventory table.

Coverage facts worth keeping for whoever revisits them: GHGRP subpart `U` holds
0.1 Mt over six facilities against 10.4 Mt of carbonate use allocated across some
twenty sectors; 98% of refrigerants and foams land in sectors reporting under 0.5
Mt to the GHGRP; urea is a consumption line and the GHGRP reports at production;
and non-energy use is attributed on **MECS feedstock quantities**, not on a
purchase row, so #929's premise does not apply to it.
