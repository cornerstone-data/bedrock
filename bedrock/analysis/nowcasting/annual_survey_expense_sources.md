# Annual survey data as a source for intermediate requirements

Whether annual statistical sources can supply information on **inputs to production**, for use in
estimating the SUT Use table's intermediate block for nowcast years 2018-2025. Covers the Census annual
business surveys (Manufactures, Retail, Wholesale, Services, Transportation), the Census survey of
**state and local government finances**, and USDA ERS **farm income** statistics.

**Status: probed against the live Census, ERS and SAS sources.** See [Probe results](#probe-results)
below. **Mixed answer: negative for the business surveys, positive for agriculture and government.**
The desk-research sections that follow record what the sources are; the probe sections record what they
actually contain.

**Probe task:** [#564](https://github.com/cornerstone-data/bedrock/issues/564) — optional, not on the
critical path for Step 3.

---

# Probe results

Run against live sources (2026-08-04).

**Verdict, in short:** the *business* surveys — manufacturing, services, wholesale, retail — do not
deliver a general annual input-structure source. But **agriculture and government, which the business
surveys don't cover at all, both do**, and were probed separately
([below](#government-and-agriculture--probed-separately-and-these-two-work)). The sector-by-sector
recommendation is [here](#revised-recommendation).

## The finding, in one line

**Depth and coverage never coincide.** Manufacturing is published at full 6-digit NAICS with no
suppression — but only **8.3%** of its intermediate consumption is commodity-mappable. The service
sectors are 16-45% mappable — but are published at **one row for the entire sector**.

## Coverage vs. depth, measured

Share of intermediate-ish spend (total operating expenses less payroll, fringe, depreciation, interest,
taxes) falling in expense categories that map to a BEA commodity. AIES `exp02`, 2023, sector level:

| Sector | Intermediate-ish $M | **Mappable** | Materials bucket | "All other" residual | API depth |
|---|---:|---:|---:|---:|---|
| 22 Utilities | 389,788 | **44.9%** | 3.4% | 24.9% | 1 row |
| 61 Education | 36,713 | **36.1%** | 4.3% | 45.4% | 1 row |
| 56 Admin/waste | 417,911 | **33.3%** | 9.5% | 40.7% | 2 rows (L3) |
| 54 Professional | 992,040 | **33.1%** | 4.8% | 48.0% | 9 rows (L4) |
| 72 Accommodation/food | 525,114 | **29.7%** | 10.0% | 45.8% | 2 rows (L3) |
| 71 Arts/rec | 168,447 | **28.5%** | 4.7% | 52.0% | 3 rows (L3) |
| 62 Health | 1,521,174 | **27.5%** | 4.5% | 0.0% | 11 rows (L4) |
| 52 Finance | 1,184,102 | **25.5%** | 1.9% | 58.8% | 4 rows (L3) |
| 51 Information | 902,896 | **24.8%** | 1.7% | 0.0% | 9 rows (L4) |
| 53 Real estate | 416,249 | **23.9%** | 4.4% | 38.8% | 3 rows (L3) |
| 81 Other services | 353,931 | **23.1%** | 8.2% | 0.0% | 1 row |
| 48-49 Transportation | 754,704 | **16.3%** | 3.6% | 0.0% | 9 rows (L3) |
| 31-33 Manufacturing | 4,536,107 | **8.3%** | **82.5%** | 4.4% | **360 rows (L6)** |
| **All sectors** | **12,199,176** | **20.4%** | | | |

Three things to read off this:

1. **The inverse relationship is near-perfect.** The only sector with BEA-Detail-grade depth is the one
   where the data says least, because `EXPS_MAT_DVAL` — a single undifferentiated cell — is 82.5% of
   manufacturing's column.
2. **"All other operating expenses" is 40-58% of intermediate in most service sectors** — larger than
   the mappable share. Even at sector level, half the column is a residual.
3. **Wholesale (42) and retail (44-45) are absent from `exp02` entirely**, as are construction (23),
   mining (21) and agriculture (11). Sectors present: 22, 31-33, 48-49, 51, 52, 53, 54, 56, 61, 62, 71,
   72, 81. So there is **nothing here for the Step 4c margins work**, which was one of the hoped-for
   cross-benefits.
   ⚠️ **Absent from `exp02` is not absent from Census.** Trade expense detail is published in the
   quinquennial **Business Expenses Supplement** to ARTS and AWTS — 13 items, 2017 and 2022, and it is
   BEA's own benchmark source for these columns. #705 tested it and it fails on suppression rather than
   on existence; see [`intermediate_estimation_plan.md`](intermediate_estimation_plan.md) §Sourcing the
   columns that actually drift.

## Year coverage is not what the catalog says

The catalog titles all read "1992 - 2023". **Actual** data availability, by query:

| Endpoint | Expense detail | Years returning rows |
|---|---|---|
| `timeseries/aies/exp02` | ~35 items, all sectors listed above | **2023 only** |
| `timeseries/aies/exp01` | totals | none of 2015-2023 |
| `timeseries/asm/area2017` | **21 items, manufacturing** | **2018-2021** |
| `timeseries/asm/industry` | 21 items, manufacturing (2012 NAICS) | 2013-2016 |
| `timeseries/asm/benchmark2017/2022` | `CSTMTOT` only | — |

**There is no harmonized backfill.** The "1992-2023" span is nominal. 2017 and 2022 return nothing from
ASM because they are **Economic Census years** — ASM does not run in census years — so the annual series
has structural holes at exactly the two years that anchor the benchmark comparison.

Assembling 2018-2025 for manufacturing therefore means: `asm/area2017` for 2018-2021, Economic Census
for 2022, `aies/exp02` for 2023, nothing yet for 2024-2025 — four sources, three seams, in one series.

## The manufacturing series is genuinely clean, and it barely moves

`asm/area2017`, 2018-2021: 648 rows/year, **360 at 6-digit NAICS, every expense item populated,
zero suppression**. That part exceeded expectations.

But the mappable share of manufacturing intermediate is 8.3% (2018) → 8.5% (2021), and per industry:

| Movement in mappable share, 2018→2021 | |
|---|---|
| Median absolute change | **0.65 pp** |
| Mean absolute change | 1.42 pp |
| Industries moving >2pp | 44 / 360 |
| Industries moving >5pp | **4 / 360** |

And the four large movers (`334614` 75.3%→12.7%, `334613` 61.1%→5.1%, `322121` 49.4%→4.2%,
`322122` 28.2%→2.3%) are implausible as economics — magnetic/optical media and paper mills swinging
50-60 points looks like industry restructuring or reclassification, not a change in input mix. They
need to be treated as suspect, not as signal.

**Against the bar set in #564 — beat inflation-carried 2017 proportions — this fails.** A category
covering 8% of the column and moving 0.65pp at the median is not worth a new source dependency and
three survey seams.

## The services workbook, checked separately

`sas-22.xlsx`, already pulled by [`Census_SAS.yaml`](../../extract/census/Census_SAS.yaml):

- **Table 3 "Estimated Expense"** — 405 distinct NAICS codes including **227 at 6-digit**, **2013-2022**,
  with per-year CVs. But the `Item` column has exactly one value: `Expenses`. It is a **total**, not a
  breakdown.
- **Table 5 "Estimated Selected Expenses"** — the real breakdown, 18 common items plus sector-specific
  ones. But **63 industries, and 2020-2022 only**.

So the workbook confirms the API picture from the other direction: depth without structure (Table 3),
or structure without depth or history (Table 5).

⚠️ **The "2020-2022 only" is wrong, and it is this workbook's display window rather than the series.**
`sas-22.xlsx` prints a rolling three-year panel; the 2017, 2018 and 2019 vintages
(`sas-17.xlsx`, `sas-18.xlsx`, `sas-19.xlsx`, same time-series directory) carry the earlier years, and
Table 5's item detail actually runs **2013-2017 and 2020-2022** at 63 industries and 2- to 4-digit
NAICS. The hole is 2018-2019, where the detailed items are not published at all. #705 spliced the
vintages and scored the result: the 2013-2017 era is benchmarked to the **2012** Economic Census and
the 2020-2022 era to the **2017** one, three mappable items were discontinued after 2017, and a seed
built across that seam loses to a frozen 2017 column. See
[`intermediate_estimation_plan.md`](intermediate_estimation_plan.md) §Sourcing the columns that
actually drift — the depth conclusion here needs correcting, the negative verdict does not.

## What is actually worth taking

1. **SAS Table 3 as a column control total.** Total operating expenses at 227 six-digit service NAICS,
   2013-2022, with CVs, already wired into bedrock. It says nothing about *structure*, but it is a
   detail-level control on the **size** of each service industry's column — which the summary SUT
   (Step 5) only constrains at summary level. This is the most useful thing the probe found, and it is
   not what the probe was looking for.
2. **Manufacturing energy inputs, 2018-2021.** `CSTELEC` and `CSTFU` are 2.2-2.3% of the column but map
   to *specific* commodities (221100, 221200/324110) at 6-digit with no suppression — and energy inputs
   matter disproportionately for the EEIO use case downstream. Narrow, but clean and directly relevant.
3. **The 2017 vs 2022 `MATFUEL` comparison** — unaffected by any of the above, since it is Economic
   Census rather than annual survey. Still the highest-value item in #564, and the only one that
   addresses the 82% of manufacturing's column that the annual data cannot see.

## Watch for while in here: MSBO margin, for the trade control totals (#612)

Not part of this issue's question, but this is the probe that will be looking at the annual
manufacturing data closely, and it is the one place the answer might turn up.

**The problem.** Step 4c phase 3 needs an annual wholesale margin level. Census publishes gross margin
for **merchant wholesalers only**. The other two types of operation are missing or near-empty:
manufacturers' sales branches and offices (`TYPOP` `21` with stock, `22` without) had **2,331,241 $M of
sales in 2017 and no published margin at all**, and agents/brokers close only 3.6% of the gap. That is
most of why the Census series is 0.551 of BEA's published Wholesale column. Details and the full
decomposition are in [`margins_estimation_plan.md`](margins_estimation_plan.md) §The 850,540 wholesale
gap.

**Why the manufacturing data is the plausible home.** An MSBO is the manufacturer's own outlet, so the
establishment sits in wholesale but the parent is a manufacturer — which means the markup may be visible
from the manufacturing side even though the wholesale tables do not carry it. Concretely, worth checking:

- **`ecnclcust`, class of customer** — already in the 2017 Economic Census dataset list. If manufacturers'
  shipments are split by customer type, shipments routed through their own branches and offices are the
  quantity being looked for.
- **Any shipments-vs-sales gap in ASM/AIES manufacturing.** A manufacturer's value of shipments is
  measured at the plant; if the same goods are also counted at the branch, the difference is the MSBO
  markup.
- **`ecntypop` beyond sales.** It breaks MSBOs out for sales, payroll, inventories and operating
  expenses. Sales less cost of goods is not available, but `OPEX` against sales bounds what the markup
  can be.

**What would be enough.** Not a full series — a single credible 2017 MSBO markup rate would do, since
Phase 4 anchors the level on 2017 BEA and only needs the annual movement from elsewhere. If nothing
turns up, the decision on record stands: keep the index merchant-wholesaler-only, which is a consistent
basis across all years.

⚠️ Related and separate: the Economic Census and AWTS disagree by **42%** about merchant wholesalers'
*own* margin in 2017 (1,563,667 vs 1,100,925, on sales bases agreeing to 0.06%, entirely in cost of
goods). If this probe touches `ecnmargin` or `ecnprofit` for any reason, that is worth resolving at the
same time — it is larger than the MSBO question.

## Government and agriculture — probed separately, and these two work

Both sectors are **absent from the business surveys entirely** (agriculture `11` is not in `exp02`;
government is not a business sector at all), so they were probed against their own sources. Unlike the
business-survey result above, **both are usable** — for different reasons.

### Government: right object, right depth, annual

Census **State and Local Government Finances**, `timeseries/govslocalfin`, **2017-2024** (2025 not yet).
Structure is **function × object**, not commodity:

- Object categories: `Current Operations`, `Capital Outlay`, `Assistance and Subsidies`,
  `Interest on Debt`, plus an exhibit line for `Salaries and Wages`.
- Function categories: education (elementary/secondary, higher, other), highways, health, hospitals,
  correction, fire, police, judicial, financial administration, public buildings, parks, air/sea
  transport, utilities (water, gas, electric, transit), and more — 234 aggregate codes in all.
- Government types: state, county, municipal, township, special district, school district, and totals.

2022, state + local combined ($ thousands):

| | Amount |
|---|---:|
| Direct expenditure — Current Operations | 3,737,573,883 |
| less Exhibit: Salaries and Wages | 1,133,452,594 |
| **= non-labor intermediate consumption** | **2,604,121,289** (70% of current ops) |
| Direct expenditure — Capital Outlay | 434,947,070 |
| Direct expenditure — Assistance and Subsidies | 68,172,649 |

**Why this one works where the business surveys didn't.** The BEA government industries (`G*`) are
mostly **not commodity-specific** to begin with, so the absence of a commodity split is not the
disqualifier it was for manufacturing. What Step 3 needs for those industries is a **column total**,
and `Current Operations − Salaries and Wages` is exactly that — annual, by function, by government
type, for every year in the Phase 1 span.

Two bonuses beyond Step 3:

- **`Salaries and Wages` feeds Step 2** (`V00100` for the government industries) on the same pull.
- **`Capital Outlay` by function** is the natural check on the FD government investment columns
  (`F06S00`/`F07S00`/`F10S00` structures, `F06E00`/`F10E00` equipment) — which is where the plan's
  **open SLG Equipment/Structures/IP attribution bug** lives (§Step 0). Worth pointing at that bug
  directly.

Caveats: this is **state and local only** — federal needs a separate source. Row counts jump from 137
(2017-2021) to 232 (2022-2024), so something changed in coverage or published detail at 2022; check
before treating the series as continuous. Federal and state tax/pension/employment siblings exist
(`govsstatefin`, `govsstatetax`, `govsemp`, `govspension`, `govsschfin`) if needed.

### Agriculture: the best coverage of anything probed, and it already runs to 2025

USDA **ERS Farm Income and Wealth Statistics** — **already wired into bedrock** as
[`USDA_ERS_FIWS`](../../extract/usda/USDA_ERS_FIWS.yaml), **no API key required**, plain CSV. The yaml
currently declares 2010-2023; the file itself carries **1910-2025**.

It publishes **"Intermediate product expenses"** as an explicit named concept — the IO concept itself,
not an accounting proxy — split into categories that map to BEA commodities almost one-for-one:

| Category | 2017 | 2023 | 2025 | share of intermediate, 2023 |
|---|---:|---:|---:|---:|
| Feed | 54,538 | 80,043 | 62,432 | 25.2% |
| Livestock purchases | 27,414 | 42,980 | 50,522 | 13.5% |
| Fertilizer, lime & soil conditioner | 22,033 | 35,834 | 29,236 | 11.3% |
| Seed | 22,516 | 27,327 | 27,718 | 8.6% |
| Pesticide | 15,716 | 21,617 | 18,142 | 6.8% |
| Repair & maintenance | 15,809 | 20,124 | 20,587 | 6.3% |
| Petroleum fuel & oil | 12,761 | 17,606 | 15,204 | 5.5% |
| Insurance premiums | 10,307 | 15,407 | 14,633 | 4.9% |
| Marketing, storage & transportation | 9,823 | 12,659 | 13,995 | 4.0% |
| Electricity | 5,806 | 7,073 | 7,602 | 2.2% |
| Machine hire & custom work | 4,570 | 6,318 | 6,408 | 2.0% |
| **Total intermediate product expenses** | **226,611** | **317,407** | **298,474** | |
| **→ mappable share** | **89.5%** | **91.0%** | **89.3%** | |

**89-91% mappable, against manufacturing's 8.3%.** There is no giant undifferentiated materials bucket
— the "miscellaneous" residual is ~15% and is itself partly broken out (irrigation, insurance).

**And it has 2024 and 2025 already**, which nothing else probed does. That is directly relevant to
Phase 2, whose entire gate is the absence of 2025 source data. Note ERS publishes the current year as a
**forecast**, so 2025 values are not a realized estimate — flag rather than treat as measured.

Two limits to be honest about:

1. **It is one farm sector, not the ~10 BEA agriculture industries.** Geography is national + 51 states;
   there is no NAICS-6 crop/livestock split. So it constrains the **aggregate** agriculture column, and
   splitting to BEA detail industries still needs 2017 proportions. Coverage is excellent; *depth* is
   the same problem as everywhere else, just less severe because agriculture is a small part of the
   BEA industry list.
2. **The share structure is stable even though levels move hard.** Median share change 2017→2023 is
   **0.35pp** — comparable to manufacturing's 0.65pp. But total intermediate expenses went
   226.6 → 317.4 → 298.5 billion, a 40% rise and partial fall, with feed swinging 54.5 → 83.6 → 62.4.
   **That is the argument for using it**: Step 3's inflation-carried approach moves cells by a commodity
   price index, and a shock of that size and shape is not what a price index reproduces. The value here
   is in the **levels**, not in a shifting mix.

Also worth noting: `Intermediate product expenses, miscellaneous, irrigation` goes to zero for 2024-2025
— a discontinued series, not an economic collapse. Don't propagate the zero.

### Revised recommendation

The negative verdict above stands for **manufacturing, services, wholesale and retail**. It does not
extend to these two:

| Sector | Verdict | What to use |
|---|---|---|
| Agriculture (`11`) | ✅ **Use it** | ERS FIWS intermediate product expenses; 89-91% mappable; already in bedrock; extend the yaml past 2023 to pick up 2024-2025 |
| Government (`G*`) | ✅ **Use it** | `govslocalfin` Current Operations − Salaries and Wages as the column total; commodity split not expected for these industries anyway. Federal still needs a source |
| Manufacturing (`31-33`) | ⚠️ Energy only | `CSTELEC`/`CSTFU` at 6-digit, 2018-2021 |
| Services | ⚠️ Control totals only | SAS Table 3 at 227 six-digit NAICS. ⚠️ **SAS Table 5 is the structural one** and reaches 63 industries for 2013-2017 and 2020-2022 — tested in #705, rejected on the benchmark seam, not on depth |
| Wholesale / retail | ❌ Nothing | absent from `exp02`. ⚠️ The **Business Expenses Supplement** does cover them, quinquennially — tested in #705, rejected on suppression |

## What to drop (business surveys)

- The premise that AIES gives a harmonized annual series across the ASM/SAS/ARTS/AWTS seam. It does not.
- Any expectation of annual input structure for **services** at BEA Detail — one row per sector.
- Any expectation of margins-relevant data for **wholesale/retail** from `exp02` — not in the dataset.
- Reconstructing intermediate *structure* for manufacturing from annual expenses — 82% is one cell.

## Reproducing

Needs `CENSUS_API_KEY` in `.env` (gitignored) for the Census endpoints; ERS FIWS needs no key. Findings
come from `timeseries/aies/exp02`, `timeseries/asm/area2017`, `timeseries/asm/industry`,
`timeseries/govslocalfin`, `sas-22.xlsx`, and the ERS Farm Income and Wealth Statistics CSV;
queries are of the form
`?get=<vars>&for=us:*&time=<year>&key=$CENSUS_API_KEY`. Note `time=`, not `YEAR=` — the latter
silently returns HTTP 204 rather than an error.

---

# Desk research (pre-probe)

Plan context: [`plan.md`](plan.md),
Step 3 (intermediate block). Current Step 3 method is #497's — seed from the 2017 detail Use table and
carry forward on commodity inflation, which means **the input structure of every industry is frozen at
its 2017 shape** and only the price level moves. The question this doc opens is whether the annual
surveys can put real annual movement into that structure.

---

## Why these surveys are the right shape of source

Business expense data is reported at **what the firm actually paid** — margins, freight and
non-deductible taxes included. That is the purchaser-price basis, which is exactly the SUT Use cell
basis (`About_BEA_IOT_table_valuation_differences.md`; the plan's whole reason for building the SUT in
purchaser prices before converting to producer prices in Step 6b). So survey expenses drop into the
Use table's valuation without a conversion step — unlike most alternatives.

They are also **establishment-based and NAICS-coded**, which is the axis bedrock already bridges to BEA
Detail (`NAICS_to_BEA_Crosswalk_2017.csv`).

---

## What actually exists

### The surveys were consolidated — this matters for our year span

The Census Bureau replaced **seven** annual business surveys with the **Annual Integrated Economic
Survey (AIES)** beginning with **data year 2023**: ACES, ARTS, **ASM**, AWTS, M3UFO, Report of
Organization, and **SAS**. First main release of 2023 AIES data was published in 2026.

Our nowcast span is 2018-2025, so it **straddles the changeover**: 2018-2022 are predecessor-survey
years, 2023+ are AIES years. Any method built on these sources has to survive that seam. This is a
concrete instance of the plan's "no hardcoded vintage" Phase 2 constraint.

The good news is that Census appears to have harmonized the history rather than starting fresh — the
AIES API publishes time series labelled **1992-2023**, which would put pre- and post-consolidation
years on one endpoint with one variable list. **Verifying that the backfill is real and consistent is
probe task item 1**, because if it holds, the seam problem largely goes away.

### The AIES API surface

From the Census API catalog (`https://api.census.gov/data.json`):

| Endpoint | Contents |
|---|---|
| `timeseries/aies/basic` | Summary statistics, employer firms, 1992-2023 |
| `timeseries/aies/exp01` | **Total** operating expenses, selected sectors, 1992-2023 |
| `timeseries/aies/exp02` | **Detailed** operating expenses, selected sectors, 1992-2023 |
| `timeseries/aies/inv` | Total inventories, 1992-2023 |
| `timeseries/aies/miscsector` | Sector-specific statistics, 1992-2023 |
| `timeseries/aies/ecom` | E-commerce, 1992-2023 |
| `aiesnonemp` | Employer + nonemployer sales/shipments/revenue, 2023 |

`exp02` is the one that matters. Requires an API key (bedrock's
[`extract/README.md`](../../extract/README.md) already documents Census key signup).

### `exp02` publishes ~35 named expense categories

Pulled from the live variable list
([`exp02/variables.json`](https://api.census.gov/data/timeseries/aies/exp02/variables.json)). Every
item is `$1,000`, each with a published coefficient of variation (`_CV`) — so **sampling error is
available per cell**, which is unusual and useful for deciding how much weight to put on a movement.

Dimensions: `NAICS` (2017 basis), `INDLEVEL`, `SECTOR`, `SUBSECTOR`, `YEAR`, `TAXSTAT`, `TYPOP`,
plus geography.

Categories that look mappable to BEA commodities:

| `exp02` variable | Expense | Plausible BEA Detail target |
|---|---|---|
| `EXPS_ELEC_VAL` | Purchased electricity | 221100 |
| `EXPS_FUEL_VAL` | Purchased fuels (except motor fuels) | 221200 / 324110 |
| `EXPS_FUEL_TRANSP_VAL` | Fuels for transportation equipment | 324110 |
| `EXPS_TRANSP_VAL` | **Purchased freight transportation** | 481000/482000/484000/492000 |
| `EXPS_REFUSE_VAL` | Water, sewer, refuse removal, other utilities | 221300 / 562000 |
| `EXPS_ADVERT_VAL` | Advertising and promotional services | 541800 |
| `EXPS_PROFTECH_VAL` | Professional and technical services | 5412-5416 |
| `EXPS_DATAPROC_VAL` | Data processing, purchased computer services | 518200 / 541500 |
| `EXPS_COMMSVC_VAL` | Communication services | 517xxx |
| `EXPS_EXSOFT_VAL` | Expensed purchases of software | 511200 |
| `EXPS_PRINT_VAL` | Purchased printing services | 323110 |
| `EXPS_MACH_REP_VAL` | Repairs/maintenance, machinery and equipment | 811000 |
| `EXPS_BUILD_REP_VAL` | Repairs/maintenance, buildings and structures | 230301 |
| `EXPS_TRANSP_REP_VAL` | Repairs/maintenance, transportation equipment | 811000 |
| `EXPS_RENT_BUILD_VAL` | Lease/rental, land, buildings, offices | 531000 |
| `EXPS_RENT_MACH_VAL` | Lease/rental, machinery and equipment | 532400 |
| `EXPS_INS_PREM_VAL` | Cost of insurance | 524200 |
| `EXPS_TEMPSTAF_VAL` | Contract labor, incl. temporary help | 561300 |
| `EXPS_CONTRACT_VAL` | Cost of contract work | industry-dependent |
| `EXPS_COMPTR_OTHEQ_VAL` | Expensed computer hardware and other equipment | 334111 etc. |
| `EXPS_MAT_DVAL` | **Purchases of materials, parts, supplies (not for resale)** | ⚠️ see below |
| `EXPS_RESALE_VAL` | **Cost of resale without further manufacturing** | ⚠️ see below |

Sector-specific items also exist (`EXPS_PROGPROD_VAL` program/production costs, `EXPS_BROADCAST_VAL`
broadcast rights, `EXPS_ACCESS_VAL`/`EXPS_NETFEE_VAL`/`EXPS_USC_VAL` telecom, `EXPS_LOSS_*` insurance
claims, `EXPS_SUPPLY_MED_VAL` medical supplies), which cover industries that a single generic expense
list would miss.

Several items are **not** intermediate consumption but land elsewhere in the plan:

| Variable | Where it belongs |
|---|---|
| `PAY_ANN_VAL` + `EXPS_FRNG_BENEFIT_VAL`, `EXPS_BENPAY_HLTH_VAL` | `V00100` compensation — Step 2 |
| `EXPS_TAX_VAL` (taxes and license fees) | `T00OTOP` other taxes on production — Step 2 |
| `EXPS_DEPR_VAL`, `EXPS_INTEREST_VAL` | components of `V00300` gross operating surplus — Step 2 |

So a single `exp02` pull would inform **Steps 2, 3 and 4c**, not Step 3 alone.

### The hard limit: materials are one undifferentiated bucket

**Annual data gives the services side of the column in commodity-like detail and the goods side as a
single number.** `EXPS_MAT_DVAL` — "purchases of other materials, parts, and supplies (not for
resale)" — is one cell. For most of manufacturing that is the *largest* part of intermediate
consumption, and its commodity mix is precisely what a Use column is.

The commodity breakout of materials is a **quinquennial Economic Census** product, not an annual one:
`EC1731MATFUEL` (2017) and `EC2231MATFUEL` (2022), "Manufacturing: Materials Consumed by Kind."

This bounds what the probe can deliver, and it also points at the consolation prize:

> **The 2022 Economic Census gives a second observation of materials structure between 2017 and 2025.**
> Step 3 currently uses one (2017) and interpolates nothing. Even if annual materials detail is
> unavailable, moving from a one-point-frozen structure to a two-point interpolated one is a real
> improvement over the span, and 2022 sits close to the middle of 2018-2025.

`EXPS_RESALE_VAL` (cost of goods for resale) is the trade-sector analogue — it is not an intermediate
input in the IO sense but the purchases side of the **margin** identity, so it belongs to Step 4c
rather than Step 3.

### What bedrock already has

| File | Currently pulls | Gap |
|---|---|---|
| [`Census_SAS.yaml`](../../extract/census/Census_SAS.yaml) | SAS **"Table 3: Estimated Expense for Employer Firms"**, xlsx, **2013-2022** | Already the right table. Stops at 2022 — exactly where AIES takes over |
| [`Census_ASM.yaml`](../../extract/census/Census_ASM.yaml) | `timeseries/asm/area2017`, **`RCPTOT` only**, year **2018** only | Revenue only; no expense items, no year range |
| [`Census_EC.yaml`](../../extract/census/Census_EC.yaml) | Economic Census via API, dataset registry, 2012/2017 | Pattern for adding `EC2231MATFUEL`; no 2022 |

So the extractor plumbing exists in all three shapes needed (xlsx time series, timeseries API, EC
dataset registry) and **the SAS expense table is already wired**. The work is extension, not
greenfield — which is a large part of why this is worth probing before committing to it.

---

## The questions the probe set out to answer

*(All four are answered in [Probe results](#probe-results) above.)*

Not "does annual expense data exist" — it does, some of it is already in bedrock. The questions are:

1. **How deep does the NAICS go, per sector, per year?** Detail-level BEA industries need roughly
   NAICS-6. Expense detail is often published shallower than revenue, and suppression bites hardest at
   depth. This determines whether the data informs the **402-industry detail** or only constrains
   summary-level columns that the summary SUT already controls (Step 5). If only the latter, the value
   is much smaller.
2. **Is the 1992-2023 backfill genuinely consistent** across the ASM/SAS/ARTS/AWTS → AIES seam, or is
   it concatenation with a definitional break in 2023?
3. **Do the ~20 mappable categories cover enough of the column to matter?** They are services-heavy;
   for a services industry that could be most of intermediate consumption, for manufacturing it may be
   a minority with `EXPS_MAT_DVAL` swamping it. The answer will differ by sector, and that's fine —
   partial coverage is still usable as a *constraint* on the sub-column that it covers.
4. **Does it beat the null?** The bar is #497's inflation-carried 2017 structure. A movement that is
   inside the published CV is not information.

---

## Probe plan as originally drafted

Benchmark-year replay is the plan's standard test and applies here directly.

1. **Get `exp02` for 2017** and compare, industry by industry, against the corresponding cells of the
   2017 detail Use table. 2017 is a benchmark year with a published answer, so this measures the
   mapping — not the nowcast — in isolation. Report per category: share of the Use column captured,
   and correlation of the survey-derived split against the published one.
2. **Measure NAICS depth and suppression** per sector per year, 2017-2023. Cheap, and it decides
   whether the rest is worth doing.
3. **Test the seam** — 2022 vs 2023 for the same industries and categories. Look for level shifts that
   are definitional rather than economic.
4. **Compare the 2017 and 2022 `MATFUEL` materials structures.** Independent of everything above, this
   quantifies how much industry input structure actually moved over five years — which is the direct
   measure of how much error #497's frozen-structure assumption carries, and is worth knowing whether
   or not the annual path pans out.
5. **Write up coverage per BEA Detail industry**, so Step 3 can use survey data where it is good and
   fall back to inflation-carried 2017 proportions where it is not. A hybrid is the realistic outcome;
   the deliverable is knowing which industries fall on which side.

## Cautions

- **Establishment vs company basis, and fiscal vs calendar year** — survey respondents report on their
  own accounting year; IO tables are calendar-year.
- **Suppression and sampling error.** These are surveys, not censuses; `_CV` is published per cell,
  so use it rather than treating every published number as solid.
- **Expense categories are accounting concepts, not commodities.** "Purchased professional and
  technical services" spans several BEA commodities and the split is not given. Expect a
  many-to-many bridge, and expect to need 2017 Use shares to resolve it — which reintroduces a benchmark
  dependency, just a much lighter one than freezing the whole column.
- **Double-counting against other Steps.** `EXPS_TAX_VAL` and the compensation items are Step 2's
  territory; `EXPS_RESALE_VAL` and `EXPS_TRANSP_VAL` touch Step 4c margins. Decide ownership before two
  steps source the same dollars.

## References

- [AIES program page](https://www.census.gov/programs-surveys/aies.html) ·
  [About AIES](https://www.census.gov/programs-surveys/aies/about.html) ·
  [AIES API datasets](https://www.census.gov/data/developers/data-sets/aies.html)
- [`exp02` variable list](https://api.census.gov/data/timeseries/aies/exp02/variables.json)
- [2022 Economic Census, NAICS 31-33 tables](https://www.census.gov/data/tables/2022/econ/economic-census/naics-sector-31-33.html)
  (`EC2231MATFUEL`) · [2017 equivalent](https://www.census.gov/data/tables/2017/econ/economic-census/naics-sector-31-33.html)
- [Census API key signup](https://api.census.gov/data/key_signup.html)
