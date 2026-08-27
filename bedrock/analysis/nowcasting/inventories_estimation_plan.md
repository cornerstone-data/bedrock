# Plan — estimating the change in private inventories column (`F03000`)

Step 1e of [`plan.md`](plan.md), issues
[#529](https://github.com/cornerstone-data/bedrock/issues/529) /
[#530](https://github.com/cornerstone-data/bedrock/issues/530) /
[#531](https://github.com/cornerstone-data/bedrock/issues/531).

Method statements below are from **three emails from David Hill (BEA, National
Economic Accounts, 2025-03-13, 2025-03-14 and 2025-05-09)**, reproduced in full
in #530's body, each checked against the published 2017 detail tables and
against what bedrock already extracts.

**This document rescopes #530.** Its previous scoping — *"use the NIPA totals as
a starting place, further work in attributing those to commodity may have to
wait"* — was carried into the plan as a deferral justified on needing ASM
stage-of-fabrication and Economic Census materials-consumed data. Neither is
needed for the dominant share of the column, and one of the two sources the
deferral was waiting on has been sitting in bedrock's extract list all along.

---

## The object

One column of the SUT Use table: 402 commodities, purchaser value, *what* is
held rather than *where*. Measured on the published 2017 detail SUT:

| | |
|---|---:|
| commodities populated | **258** of 402 |
| column total | **32,682** |
| positive mass | 65,723 |
| negative mass | −33,041 |
| gross mass | **98,764** |
| negative commodities | **61** |

**The column total is the one thing that is free.** It equals NIPA CIPI exactly
— Hill gives 2023 as 41,695 on both sides, and 2017 as 32,674 against the
32,682 measured here (8 apart, a vintage difference). Everything else is
allocation.

⚠️ **Gross mass is 3× the net.** Not the `T014` pathology — margins net to 1
against 7.4 million of gross — but far enough that a column-total check is
close to uninformative. 61 commodities carry negative values against a positive
total. **Validate per commodity.**

## What BEA does

Hill's method is four rules over three inventory types, applied to CIPI by
holding industry:

| Inventory type | "What held" rule | Natural source |
|---|---|---|
| **Finished goods** | *"we assume that finished goods inventories are primary products of the reporting industry"* | **primary product of the industry** — see below |
| **Work-in-process** | *"products in assembly are primary to the industry"* | **primary product of the industry** — see below |
| **M&S — merchandise trade** | *"Wholesale and retail industries hold products that they sell. So the 'what held' is based on the type of industry they operate in"* | trade product line |
| **M&S — production materials** | *"reflective of intermediate inputs needed for that industry's production"* | **Econ Census Materials Consumed** — see below |

For the stage split and the materials composition:

> *"We use Annual Survey of Manufacturers data for the breakdown of the total
> inventory into the three stages of fabrication. For the commodity
> composition, we use Econ Census data on materials/fuels consumed by
> industry."* — D. Hill, BEA, 2025-05-09

BEA's primary documentation for all of this is the 2009 IO manual, which Hill
points at directly: **p.32** (ch.2 §8) for the M&S discussion and worked example,
**p.97** (ch.6 §5) and **p.190** (ch.10 §4). Not yet consulted here — the rules
above are read off the correspondence, and the manual is the place to check them
against before Phase 3 is called done.

⚠️ **The rules are stated at the 6-digit industry.** Hill's finished-goods
wording is *"broadly, at the 6-digit industry, we assume that finished goods
inventories are primary products of the reporting industry"*. That qualifier
fixes the level the whole method operates at, and it is the level the
primary-product correspondence is nearly unique at (below).

⚠️ **Finished goods and WIP take the industry's *primary product*, not its
commodity mix.** Hill's wording is "primary products of the reporting industry"
and "primary to the industry" — singular in intent. Spreading an industry's
inventory across every commodity it produces is a commodity-mix construction and
is not what BEA describes. The correspondence is a lookup, and it is nearly
unique: **a 6-digit NAICS maps to exactly one BEA detail commodity 95% of the
time** (5-digit 88%, 4-digit 72%, measured on
`NAICS_to_BEA_Crosswalk_2017.csv`). So this branch needs no product-line data at
all — only the industry → primary commodity correspondence.

This matters for correctness rather than magnitude: finished goods plus WIP is
the manufacturing branch, 818 in 2017, 2% of nonfarm.

**Three of the four rules are functions of tables this project already builds.**
Commodity mix is Step 4a ([#570](https://github.com/cornerstone-data/bedrock/issues/570));
the intermediate input mix is Step 3
([#497](https://github.com/cornerstone-data/bedrock/issues/497)). For the
benchmark year both are published, so the whole method is testable on 2017
today without waiting on either step.

## What is already in bedrock, and unused

**`U50705BU1` — Table 5.7.5BU1, *Change in Private Inventories by Industry* — is
already extracted**, listed at
[`BEA_NIPA.yaml:30`](../../extract/bea/BEA_NIPA.yaml#L30) for 2012–2024. It is
pulled on every run and **never consumed**: there is no inventories activity set
in [`NIPA_final_dom_uses_2017.yaml`](../../transform/nipa/NIPA_final_dom_uses_2017.yaml), whose 22 sets
are all PCE, IP and government.

**The stage-of-fabrication split is already in that table.** Hill attributes it
to ASM, but the published underlying-detail table carries it directly, at lines
29–37:

| line | code | | 2017 $M |
|---|---|---|---:|
| 29 | `C30M` | Materials and supplies | 1,894 |
| 32 | `C30W` | Work-in-process | −3,187 |
| 35 | `C30F` | Finished goods | 2,110 |

each split durable (`C30DM`/`C30DW`/`C30DF`) and nondurable
(`C30NM`/`C30NW`/`C30NF`). No ASM pull is required for a **2017 manufacturing**
first pass.

⚠️ **An earlier version of this plan said ASM "buys finer per-industry stage
shares than durable/nondurable, nothing more". That understates it and was
wrong.** `U50705BU1` carries the stage split for manufacturing *as a whole*,
durable against nondurable. ASM carries it **per 6-digit industry, per year** —
which is the level Hill states the rules operate at. See §Annual inventory
sources.

**What is not in bedrock:** no extractor for Econ Census `EC1731MATFUEL`
(Materials Consumed by Kind of Industry) or `ECNNAPCSPRD` (Products by
Industry). `Census_ASM` exists but pulls only `RCPTOT` from
`timeseries/asm/area2017`, **2018 only** — no inventory variables at all.

## The magnitudes invert the emphasis

2017 nonfarm CIPI by holding industry, from the cached extract:

| line | | $M | share of nonfarm |
|---|---|---:|---:|
| 38 | Wholesale trade | 30,329 | 79% |
| 67 | Retail trade | 17,930 | 47% |
| 2 | Mining, utilities, construction | −14,261 | −37% |
| 77 | Other industries | 3,537 | 9% |
| 3 | **Manufacturing** | **818** | **2%** |
| 1 | **Nonfarm total** | **38,353** | |

**Wholesale + retail is 48,259 — 126% of the column. Manufacturing is 2%.**

So the two sources Hill's May 9 email names, and the two the deferral was
waiting on, govern the branch worth 818 million. The branch worth 48,259 is
merchandise trade, whose rule is the simplest of the four: *what the industry
sells*.

⚠️ **This is the single most important fact in this document.** It is why Step
1e is tractable now and why it was mis-scoped: BEA's answer describes the
manufacturing machinery in the most detail because that is where BEA's method
is most elaborate, not because that is where the money is.

## The merchandise-trade rule visibly works on the largest cells

`U50705BU1` publishes the trade branch at trade-industry detail — roughly 25
lines. Matching them to BEA commodities **by concept first**, then checking the
value:

| NIPA holding industry | $M | concept-matched commodity | 2017 `F03000` $M |
|---|---:|---|---:|
| `C4222` Drugs and druggists' sundries wholesalers | 11,287 | `325412` pharmaceutical preparations | **7,547** |
| `C4227` Petroleum and petroleum products wholesalers | −5,885 | `324110` petroleum refineries | **−7,387** |
| `N631RC` Motor vehicle and parts dealers | 14,151 | `336111` autos + `336112` light trucks | **9,500** |

Right sign, right order of magnitude, on the three largest trade lines, against
a column whose largest entries are `211000` oil and gas −7,577, `325412`
+7,547, `324110` −7,387, `336411` aircraft −6,314.

⚠️ **The direction of that inference matters.** The margins work records the
counter-example: matching by *value* alone pairs `Ships` with switchgear and
`Electronics` with ship building, both nonsense
([`margins_estimation_plan.md`](margins_estimation_plan.md)). Here the concept
match came first and the values then agreed, which is the admissible order.
These are spot checks on three lines, not a fitted result.

## Sources — what exists

| Piece | Where | State |
|---|---|---|
| CIPI column total | NIPA T1.1.5 line 14 | Free, exact, already extracted |
| CIPI by holding industry, nonfarm | `U50705BU1` | **Already extracted 2012–2024, unused** |
| Stage-of-fabrication split, durable/nondurable | `U50705BU1` lines 29–37 | **Already extracted, unused** |
| Farm CIPI level | NIPA 5.7.5B, table id `T50705B` | **Not extracted, but confirmed available.** Farm is `B018RC` line 2 at **−5,679** in 2017 — exactly the figure §Farm infers — and the CIPI control total `A014RC` is line 1 at 32,674. Farm plus nonfarm ties to it. Adding `T50705B` to `BEA_NIPA.yaml`'s table list is the whole job |
| Farm commodity split | `USDA_ERS_FIWS` `Inventory` variable | **Already in bedrock**, unused for this — see §Farm |
| Finished goods / WIP → commodity | Step 4a commodity mix; 2017 Supply table | Published for 2017 — ⚠️ **use the nowcast year's mix once Step 4a builds one**, see below |
| M&S manufacturing → commodity | Step 3 intermediate column; 2017 Use table | Published for 2017 |
| Trade industry → commodity | 29 `U50705BU1` trade lines | ✅ Built — `Sector_Crosswalk_BEA_NIPA_Inventories.csv` |
| Weights within each trade commodity set | `Census_EC_PxI` product mix | ✅ Built — `Sector_Crosswalk_Census_EC_PxI.csv` (#652) |
| **Annual inventories, wholesale** | **AWTS Table 3** | **Annual 1992–2022 by NAICS kind of business** — see §Annual inventory sources |
| **Annual inventories, retail** | **ARTS `invent.xlsx`** | **Annual 1992–2022 by NAICS kind of business** |
| **Annual inventories, manufacturing** | ASM `INV*B`/`INV*E` | **Annual, all three stages, per industry** |
| **Annual inventories, all sectors** | AIES `basic` `INV_E_*` | **All three stages, all sectors — 2023 only** |
| Materials consumed by industry | Econ Census `ecnmatfuel` `EC1731MATFUEL` | **BEA's own source for rule 4.** Not extracted; quinquennial (2012/2017/2022); currently proxied by the Use column |
| Mining inventories | Econ Census `ecnlifomine` | Not extracted; quinquennial. The only source found pointing at open question 2 |

### ⚠️ Take the commodity mix from the nowcast year, not the 2017 benchmark

The manufacturing branch splits each industry's inventory across the commodities
that industry produces, and today that mix is read off the **2017 benchmark
Supply table** because it is the only one published at detail.

**Once Step 4a ([#570](https://github.com/cornerstone-data/bedrock/issues/570))
builds the Supply table body — industry supply of commodities — for each nowcast
year, this branch must switch to that year's mix.** Leaving it on 2017 would
freeze each industry's product composition at the benchmark and quietly
attribute every later year's inventory movement using a mix that no longer
holds. That is the same failure the plan already rejects elsewhere: a benchmark
proportion carried forward is an assumption, not a measurement, and it stops
being visible once it is buried in an allocation.

It matters most where composition actually moves — petroleum and coal products,
computer and electronic products, and transportation equipment, whose auto/truck
and aerospace balance shifts year to year.

The switch is a source change, not a method change: the rule stays "an industry
holds its own products", only the table supplying the mix moves from the 2017
benchmark to the nowcast year's Step 4a output.

### ✅ Re-probed 2026-08-27 — two source rows below were wrong

⚠️ **The table in the next section was written from the API *catalog*, and two of
its rows do not return data.** Re-probed live against the Census API and the
published workbooks on 2026-08-27. What is verified, per year, for the 2018-2023
span the initial SUTs need:

| branch | source | years that return data | notes |
|---|---|---|---|
| **Wholesale** (79% of nonfarm) | AWTS **Table 3** `2022_awts_inv_table3.xlsx` | ✅ **2015-2022** | One workbook, **years as columns** — a single fetch gets the series, not a per-year pull |
| **Retail** (47%) | ARTS **`invent.xlsx`** | ✅ **2013-2022** | Same shape, years as columns |
| **Manufacturing** | ASM **`timeseries/asm/area2017`** | ✅ **2018-2021** | 648 rows/year. Full stage set **beginning *and* end of year** |
| **All three** | AIES **`timeseries/aies/inv`** | ✅ **2023 only** | 977 rows, sectors 31 / 42 / 44 / 51 |
| Manufacturing | ASM `timeseries/asm/industry` | ❌ **none** | **204 No Content for every year 2018-2023.** Variables are published; no data is served |
| All | AIES `timeseries/aies/inv` 1992-2022 | ❌ **none** | **204 for every year before 2023.** The "1992-2023" label is catalog metadata |

❌ **`timeseries/asm/industry` is the endpoint the manufacturing row below names,
and it is empty.** The working ASM surface is `area2017` — the same one
`Census_ASM_Expenses` already uses. Its inventory variables are `INVFINB/E`,
`INVWIPB/E`, `INVMATB/E`, `INVTOTB/E`, plus `INVRSVB/E` **LIFO reserve** and
`INVCB/E`, each with a relative standard error and a flag.

❌ **AIES's harmonised 1992-2023 history is not real for `inv`.** This closes
probe task item 1 of
[`annual_survey_expense_sources.md`](annual_survey_expense_sources.md), which
records the backfill as an open question and hoped it would remove the survey
seam. It does not: `aies/inv` behaves exactly like `aies/basic`, 2023 and nothing
earlier.

⚠️ **`aies/inv` publishes totals only** — `INV_E_TOT_DVAL`, with no
finished/work-in-process/materials split. Stage structure for 2023 has to come
from `aies/basic`'s `INV_E_*_VAL`, and for earlier years from ASM or
`Census_EC_Inventories`.

#### ⚠️ The gap this exposes: manufacturing 2022

ASM `area2017` stops at **2021** and `aies/inv` starts at **2023**, so
manufacturing has a one-year hole at 2022 that no annual survey covers. Wholesale
and retail do not — AWTS and ARTS both reach 2022.

✅ **2022 is a census year**, so `Census_EC_Inventories` (#664) fills it, at
NAICS-6 by stage, already extracted for 2017 and 2022. The hole lands on an
observation rather than on an interpolation, which is the best case available.

**The resulting chain, per branch:**

| branch | 2018-2021 | 2022 | 2023 |
|---|---|---|---|
| wholesale | AWTS Table 3 | AWTS Table 3 | `aies/inv` |
| retail | ARTS `invent.xlsx` | ARTS `invent.xlsx` | `aies/inv` |
| manufacturing | ASM `area2017` | `Census_EC_Inventories` | `aies/inv` |

⚠️ **AWTS Table 3 is on a 2012 NAICS basis**, not 2017 — the header column reads
`2012 NAICS Code`. That needs a vintage bridge before its codes meet BEA's, the
same class of problem `Census_EC_MatFuel` has across its 2017/2022 vintages.

⚠️ **The stock-versus-change rule is unchanged by any of this.** Every source
above is a stock level. Structure from the source, **level from NIPA** — see the
FIWS example below, where differencing gives -887 against a true -5,679.

### Annual inventory sources — every major branch is covered

Probed 2026-08-16 against the Census API and the published survey workbooks. The
finding that matters: **the two branches carrying the column have annual,
industry-level inventory data going back to 1992**, which the plan previously
treated as a manufacturing-only concern solved by `U50705BU1`'s
durable/nondurable rows.

| Branch | Source | Detail | Years |
|---|---|---|---|
| **Wholesale** (79% of nonfarm) | AWTS **Table 3**, `2022_awts_inv_table3.xlsx` | year-end inventories by NAICS kind of business — 42, 423, 4231, 4232, 4234, 42343 … | **1992–2022** |
| **Retail** (47%) | ARTS **`invent.xlsx`** | end-of-year inventories by NAICS kind of business — 441, 4411, 4413, 442 … | **1992–2022** |
| ~~**Manufacturing** (2%)~~ | ❌ ASM `timeseries/asm/industry` — **empty, see above; use `area2017`** | `INVFINB/E`, `INVWIPB/E`, `INVMATB/E`, `INVTOTB/E` — **all three stages, per industry** | annual |
| **All sectors** | AIES `timeseries/aies/basic` (and `aies/inv`, ❌ 2023 only — see above) | `INV_E_FIN_VAL`, `INV_E_WIP_VAL`, `INV_E_MAT_VAL`, `INV_E_TOT_DVAL` | **2023 only** |
| **Farm** | `USDA_ERS_FIWS` `Inventory` | Crops / Livestock / Purchased inputs | 1939–2025 |

Verified end-2023 AIES totals: wholesale $10.32T, manufacturing $4.53T, retail
$3.49T, all with three stages present.

**bedrock already has all four extractors.** `Census_AWTS` and `Census_ARTS`
(from [#612](https://github.com/cornerstone-data/bedrock/issues/612)) pull only
the *margin* workbooks, so wholesale and retail inventories are an added table on
an existing source rather than new work. `Census_ASM` pulls only `RCPTOT`.

⚠️ **These are stock levels, not changes — the same trap §Farm records for
FIWS.** CIPI is a change concept that excludes holding gains through the
inventory valuation adjustment, so differencing published stocks imports a price
effect. FIWS makes the size of that error concrete: differencing gives −887
against a farm CIPI of −5,679, out by roughly 6×. Use these for **structure**,
with the level from NIPA.

⚠️ **2024 has no annual inventory source.** AIES carries no back-years and 2024
is unpublished; AWTS and ARTS stop at 2022. The same seam #612 hit on the trade
margin control totals.

**Construction has no annual source at all.** Only the quinquennial Economic
Census (`ecnvalcon`, `ecnloccons`); `timeseries/eits/vip` is construction
*spending*, not stock. Mining has `ecnlifomine` (Inventories with LIFO
Valuation, 2012/2017/2022) — quinquennial, but the first source found that points
at open question 2, where the NIPA branch combines mining, utilities and
construction at −14,261.

### Rule 4's source is Materials Consumed, not the Use column

Hill names it directly: *"For the commodity composition, we use Econ Census data
on materials/fuels consumed by industry — Manufacturing: Materials Consumed by
Kind of Industry for the U.S.: 2017."*

That is `ecnmatfuel` group `EC1731MATFUEL`, live on the API and **not extracted**.
Probed: 4,624 rows, **388 industries × 291 materials**, with `MATFUELCOST`
(delivered cost) and `MATFUELQTY`. `EC1721MATFUEL` covers mining on the same
endpoint.

**The `MATFUEL` code is 8-digit and NAICS-derived** — `33110090` iron and steel
ingot, `33272203` bolts and nuts, `21111015` natural gas — so it resolves to BEA
on the code prefix through `NAICS_to_BEA_Crosswalk_2017.csv`. The
seller-not-maker problem that made the trade concordance hard does not arise
here, because a material is defined by what it is.

⚠️ **The plan's rule-4 row previously named the industry intermediate input mix
as the source. That is a substitute, not BEA's method**, and it is the weaker one
for validation: the Use table's intermediate column is BEA's own construction, so
predicting `F03000` from it and checking against the published `F03000` leans on
the same build. Materials Consumed is independent survey data. Quinquennial, so
composition is fixed between census years either way.

Hill also suggests *"Product by Industry may also be worth looking at"* — which is
`Census_EC_PxI`, ported in [#652](https://github.com/cornerstone-data/bedrock/pull/652).
The trade weights are therefore on BEA's own suggested path, not an invention.

### The gap, stated precisely

✅ **Closed.** The gap was one crosswalk — the named trade industries → BEA 2017
detail commodities, covering 126% of the column gross. It is built
(`Sector_Crosswalk_BEA_NIPA_Inventories.csv`, 858 rows, 254 of 258 populated
commodities, 91% of gross mass), and the weights within each commodity set are
built alongside it from `Census_EC_PxI`.

What remains is not a missing source but Phase 3: applying the four rules and
validating per commodity.

This is a smaller object than the NAPCS → I-O commodity concordance that
[#615](https://github.com/cornerstone-data/bedrock/issues/615) identifies as the
missing link for BEA's wholesale margin method — but it is **the same problem at
coarser resolution**, since both ask what commodities a kind-of-business deals
in. ⚠️ **Decide the two together.** If #615 is ever built, it subsumes this
crosswalk; if it is not, this crosswalk is the cheap version and margins may be
able to borrow it.

## Farm — level from NIPA, split from ERS

`U50705BU1` is nonfarm-only, so farm is a separate build. It is ≈ **−5,679** in 2017 against a
32,674 total — **17% of the column**, and a build that omits it is silently wrong.

**The level comes from NIPA, not USDA.** The one free thing about this column is that its total ties
to NIPA CIPI exactly; sourcing farm elsewhere would break that identity for a piece that is published.
Farm CIPI is simply **not extracted today** — `BEA_NIPA.yaml` carries `U50705BU1` (nonfarm underlying
detail) but not the farm/nonfarm table. Adding **5.7.5B** to the table list replaces the inferred
−5,679 with a published figure and closes this.

**The commodity split comes from ERS FIWS**, which already sits in bedrock as
[`USDA_ERS_FIWS`](../../extract/usda/USDA_ERS_FIWS.yaml) — no API key, plain CSV. Its `Inventory`
variable splits **`Crops` / `Livestock` / `Purchased inputs`**, national and state, **1939–2025**, and
the existing parser already retains it (it builds `ActivityProducedBy` from the full variable list
rather than filtering to expenses, so no parser change is needed).

⚠️ **FIWS publishes stock levels, not changes — do not difference it and call the result CIPI.**

| $M | 2016 | 2017 | Δ |
|---|---:|---:|---:|
| Crops | 55,682 | 56,796 | +1,115 |
| Livestock | 217,993 | 214,171 | −3,822 |
| Purchased inputs | 29,763 | 31,583 | +1,820 |
| **sum of changes** | | | **−887** |
| *farm CIPI (inferred)* | | | *−5,679* |

Right sign, off by ≈6×. That gap is expected: a first difference of book-value stocks carries holding
gains, which NIPA CIPI excludes via the inventory valuation adjustment. Differencing FIWS would import
a price effect into a quantity concept.

**So use FIWS the way §`MDTY` in [`plan.md`](plan.md) uses Census — structure from the source, level
from NIPA.** FIWS supplies an annual, moving crops-vs-livestock split; the NIPA farm line sets the
magnitude.

Two things fall out for free:

- **`Purchased inputs` is the farm sector's materials and supplies**, so FIWS gives farm the same
  three-stage structure `U50705BU1` gives manufacturing — crops and livestock behave as finished
  goods/WIP (primary products of the industry), purchased inputs as M&S. **BEA's four rules apply to
  farm unchanged.**
- **It covers 2024–2025**, two years past NIPA, so farm does not become the terminal-year problem.
  ⚠️ The current year is an ERS **forecast**, not a realized estimate — the same caveat #577 records.

**The limit is #577's limit.** Crops/livestock is two groups against ~10 BEA agriculture commodities,
so splitting within them still falls back to 2017 detail shares. FIWS cash receipts *are* published by
commodity and could refine that; worth a look, not worth blocking on.

Worth doing rather than freezing 2017 shares, because farm inventories swing hard with crop years —
livestock moved −3,822 in 2017 and +27,310 across 2023–24 — and `1111B0` −1,547 and `1121A0` +1,214
are both in the 2017 column's twelve largest cells.

### The ASM/AIES seam, if finer stage shares are ever wanted

Checked live against the Census API: `timeseries/aies/inv` publishes **20
variables and exactly one inventory measure**, `INV_E_TOT_DVAL` — total
inventories, end of year. **There is no stage-of-fabrication split on the AIES
endpoint**, and AIES is the 2023+ successor covering the back half of the
nowcast span. So the ASM refinement does not extend cleanly across 2018–2025
even if wanted. Another reason to take the durable/nondurable split that
`U50705BU1` already publishes and stop there. (Census sometimes publishes more
in the downloadable table releases than on the timeseries API — worth a look
before concluding the split is unavailable, but not before Phase 1.)

## Approach

Anchor on the published industry detail, allocate by rule, validate per
commodity against 2017 — the same shape as
[`margins_estimation_plan.md`](margins_estimation_plan.md) and
[`compensation_disaggregation_plan.md`](compensation_disaggregation_plan.md).

| phase | what | depends on |
|---|---|---|
| 1 | Consume `U50705BU1`: industry × stage, all years, plus the farm line | **nothing — start here** |
| 2 | Trade industry → commodity crosswalk, ~25 lines | #615 decision |
| 3 | Apply the four rules; validate against the 2017 detail column | 2017 Make/Use published; nowcast years need #570 and #497 |

**Phase 1 — consume what is already extracted.**

⚠️ **This is its own FBS method, not an activity set inside
`NIPA_final_dom_uses_<year>`.** Step 1 was rescoped on 2026-08-14
([#523](https://github.com/cornerstone-data/bedrock/issues/523)) into three
independent methods — `NIPA_final_dom_uses` (1A), Trade (1B) and inventories
(1C) — so `F03000` is built by `Inventories_<year>` and composed alongside the
others. An earlier draft of this plan said otherwise.

`U50705BU1` is nonfarm-only, so add NIPA **5.7.5B** (`T50705B`) to
`BEA_NIPA.yaml`'s table list for the farm line, and take farm's commodity split
from `USDA_ERS_FIWS` — both covered in §Farm. Neither needs a new extractor. The
method's line selection must follow the verified leaf sets in §Traps.

**Phase 2 — the crosswalk.** 29 trade lines to BEA detail commodities. Build it
by concept, never by value proximity.

**Built** as `Sector_Crosswalk_BEA_NIPA_Inventories.csv`: 858 rows, 256
commodities, generated from a documented concept map rather than hand-written.
The approach that worked — NIPA's trade lines *are* NAICS wholesale (423x/424x)
and retail (44x/45x) categories, and NAICS itself defines what each distributes,
so each line maps to the NAICS goods ranges its own definition names and those
expand mechanically through `NAICS_to_BEA_Crosswalk_2017.csv`. Nothing fitted to
the 2017 column.

Measured coverage of the published 2017 `F03000`: **254 of 258 populated
commodities, 91% of gross mass**. The four misses — `211000` oil and gas,
`212100` coal, `21311A`, `213111` — are all the mining/utilities/construction
branch, which is open question 2 and correctly outside a trade crosswalk.

Three gaps that a first pass missed, each with a named NAICS trade category
behind it and each worth re-checking in any rebuild: forestry and fishery
products (42459 sits inside farm product raw materials), published media
(42492 is book, periodical and newspaper wholesalers), and used goods (45331
used merchandise stores, which is where `S00402` belongs). Adding them moved
gross coverage from 86% to 91%.

⚠️ **The crosswalk must key on the NIPA line name, not its series code** — that
is what the FBA carries. See §Traps for the two lines where that fails.

**Still open:** the weights *within* each commodity set. The crosswalk says what
a trade line can reach, not how its value splits across those commodities. That
is Phase 3, and it must not be weighted on the 2017 `F03000` column itself —
that fits to the answer.

**Phase 3 — apply and validate.** Finished goods and WIP on the industry
commodity mix; M&S manufacturing on the industry's intermediate input column,
**restricted to storable goods commodities** (see traps); M&S merchandise trade
on the Phase 2 crosswalk. Validate against the 258 populated cells of the 2017
detail `F03000` column via the
[#587](https://github.com/cornerstone-data/bedrock/issues/587) per-cell picture;
`use_fd_detail_sut` already carries the column.

### The weights within each trade commodity set

Phase 2 says what a trade line can *reach*. Phase 3 needs how its value splits
across those commodities, and **that must not be weighted on the 2017 `F03000`
column itself** — that fits to the answer.

**Economic Census product-line-by-kind-of-business data is the right source.**
Hill's merchandise-trade rule is literally *"what the industry sells"*, which is
what product-line data measures, and it is what BEA uses for the same question in
its margin method. ⚠️ The Supply table's commodity mix is **not** a substitute:
wholesale and retail industries produce *trade margin*, not the goods they
distribute, so a Supply-side mix answers a different question. That is precisely
why BEA reaches for product-line data.

`Census_EC_PxI` is **not in bedrock** — it is on flowsa's `margins` branch, built
for 2017, alongside `write_Crosswalk_NAPCS.py` (whose output is not committed)
and `NAICS_to_NAPCS_Crosswalk_2017.csv` (29,098 rows). The 2022 vintage is live
on the Census API as `ecnnapcsprd`. The port is bounded work and is **shared with
[#615](https://github.com/cornerstone-data/bedrock/issues/615)** and the margins
wholesale allocation.

**The concordance blocker has a cheap route.** #615 names NAPCS product line →
I-O commodity as the one genuinely missing link, built internally by BEA and
unpublished. But `NAICS_to_NAPCS_Crosswalk_2017.csv` maps each NAICS to the NAPCS
products it *produces*, and `NAICS_to_BEA_Crosswalk_2017.csv` maps NAICS → BEA
detail, so composing them in reverse gives NAPCS → NAICS → BEA commodity. Every
NAICS in the NAPCS crosswalk reaches a BEA commodity — 0% unmapped.

⚠️ **Compose it through the *primary* NAICS, not through every producing NAICS.**
Spreading a product line across all its producers is a commodity-mix
construction, and it is not what BEA does: `write_Crosswalk_NAPCS.py` already
implements the **MAKB rule** — the max-value NAICS per NAPCS line — which is
BEA's own. Measured unrestricted, the many-to-many composition looks poor (35% of
NAPCS lines reach exactly one commodity, median 2), but that is the wrong test.
Under one primary NAICS the ambiguity mostly disappears, because **a 6-digit
NAICS maps to exactly one BEA detail commodity 95% of the time** (5-digit 88%,
4-digit 72%).

Selecting the max-value NAICS needs `NAPCSDOL` / `NAICSALL_PCT`, so it depends on
the `Census_EC_PxI` port rather than on the crosswalks alone.

⚠️ **What this does not yet give is weights.** The composition supplies
*structure* only. Weights need `NAPCSDOL` — product-line dollars by kind of
business — which requires the `Census_EC_PxI` port. Until then Phase 3's trade
branch has a receiving set and no split.

⚠️ **EC PxI is quinquennial.** Weights would be fixed at 2017 through 2021 and
2022 thereafter. The trade branch's *composition* is therefore static between
census years while its *level* moves with CIPI — the same cadence as BEA's water
and air difficulty multipliers, and defensible, but it should be stated rather
than discovered.

## Traps

⚠️ **Both NIPA tables are hierarchical, and summing their lines double-counts
badly.** Measured against the 2017 extract:

| table | sum of all lines | true total | factor |
|---|---:|---:|---:|
| `U50705BU1` | 201,270 | 38,353 (nonfarm) | **5.2×** |
| `T50705B` | — | 32,674 (CIPI) | 3 competing decompositions |

This is the same trap `FD_IP_equipment` hit on U50505, where selecting the whole
table took parents and children together
([#547](https://github.com/cornerstone-data/bedrock/issues/547)). **The line list
must be explicit and chosen per branch.** These leaf sets are verified — each
sums to its published parent:

| branch | lines | 2017 $M |
|---|---|---:|
| top level | 2, 3, 38, 67, 77 | 38,353 = line 1 |
| manufacturing **by stage** | 29, 32, 35 | 817 ≈ line 3 |
| manufacturing by durable/nondurable | 4, 17 | 817 ≈ line 3 |
| wholesale, merchant leaves | 43–45, 47–53, 55–63 | 19,192 = line 41 |
| wholesale, nonmerchant | 65, 66 | 11,137 = line 64 |
| retail leaves | 68–76 **excluding 73** | 17,930 = line 67 |
| general merchandise | 74, 75 | −3,281 = line 73 |

All leaves plus farm sum to 32,673 against the published 32,674.

⚠️ **Manufacturing carries two complete decompositions and both are in the
table** — by industry (lines 4–28) and by stage of fabrication (29–37), each
summing to 818. BEA's four rules need the *stage* split, so take 29/32/35 and
exclude 4–28, never both. Wholesale likewise decomposes two ways (merchant /
nonmerchant, or durable / nondurable); pick one. `T50705B` has three, plus the
total repeated at lines 1 and 16 — take lines 2 and 1 only.

⚠️ **Line 46 is a parent of lines 47 and 48**, which sum to exactly 2,150. It is
the only place in the wholesale branch where a leaf sits adjacent to its parent,
and including all three overstates wholesale by that amount. It was found by
checking leaf sums against published branch totals, not by reading the table —
which is the method that works on these tables.

⚠️ **`C42ND` and `C42NN` cannot be matched on name.** They are published as bare
"Durable goods industries" and "Nondurable goods industries", and those names
recur at four levels of `U50705BU1` (lines 4/39/42/65 and 17/40/54/66). Select
them by `Line` and rename via `assign_fields` before attribution — the same
treatment `FD_IP_equipment_residential` gives U50505 line 46. Every other trade
line's name is unique within the table, verified.

**Do not ship the NIPA total alone.** The SUT needs a 402-row column; a total is
one scalar, and the RAS in Step 5 would then invent the allocation with no
economic basis. Worse, the column is 3× gross to net with 61 negative
commodities, so RAS has neither a seed nor a sign-safe structure to work from.

**Do not apply the M&S rule to the raw Use column.** The intermediate input mix
includes services and non-storables — legal services, electricity, advertising —
which are not held in inventory. `EC1731MATFUEL` is materials *and fuels*, goods
only, which is why BEA cites it rather than the Use column. Restrict the proxy
to storable goods commodities or it will place inventory on services.

**Composition is a level; CIPI is a change.** The stage shares and commodity
compositions come from stocks and from materials consumed, both levels; the
thing being allocated is signed. That is BEA's own construction and it is why
negative commodity cells are correct — consistent with the 31 negative rows in
the Margins table, all buyer `F03000`
([`margins_estimation_plan.md`](margins_estimation_plan.md)). **Do not clip.**

**Expect signs to flip by year.** A commodity negative in 2017 is positive in an
inventory-build year. Nothing may hard-code the 2017 sign.

⚠️ **`C4521` department stores +21,237 against `C4529` other general merchandise
−24,518** in 2017. Two adjacent retail lines nearly cancelling at this magnitude
looks like a classification movement rather than economics. Check before
carrying either line into a crosswalk — and check whether the pair is stable
across 2018–2024 before trusting the trade branch year to year.

**Mining, utilities and construction is −14,261 with no sub-detail and no stage
split** in `U50705BU1`. It is 37% of nonfarm in magnitude and needs its own
rule; the 2017 column suggests where it lands (`211000` oil and gas −7,577), but
that is one commodity for a three-sector block.

## The mining branch — sources found, deferred to #660

⚠️ **Second priority.** The sources below are identified and probed; the build is
deferred to [#660](https://github.com/cornerstone-data/bedrock/issues/660) —
later in Phase 1 if there is time, otherwise Phase 2. Recorded here so the probe
does not have to be repeated.

**The branch is mining-only in commodity space, which was not obvious from its
name.** NIPA calls it "mining, utilities, and construction" and it is −14,261.
Measured against the published 2017 `F03000`:

| commodity family | cells | net $M |
|---|---:|---:|
| mining (`211`/`212`/`213`) | 8 | −9,398 |
| utilities (`221`) | **0** | 0 |
| construction (`23*`) | **0** | 0 |

There are **no utilities or construction commodities anywhere in the column**.
That is physically sensible — a utility holds coal and gas, which are mining
commodities, and a construction firm holds cement and steel, which the trade
crosswalk already reaches. So a mining-shaped source is the right shape for the
whole branch, not just a third of it.

All four published cells outside the trade crosswalk's reach are mining:
`211000` −7,577, `212100` −1,172, `21311A` −77, `213111` −10.

### ✅ Levels checked against the benchmark, 2026-08-27 — and they are comparable

Pulled live from the EIA v2 API and differenced 2016 → 2017, against the
published detail `F03000` read from the 2017 Use SUT:

| commodity | physical change | 2017 price | implied $M |
|---|---:|---:|---:|
| crude oil (`petroleum/stoc/cu`, `NUS`) | −65,931 MBBL | $50.80/bbl | **−3,349** |
| natural gas, working (`natural-gas/stor/sum`, `SAO`, December) | −264,342 MMcf | $3.07/Mcf | **−812** |
| coal (`total-energy`, *Coal Stocks, Total, End of Period*) | −26,034 kst | $39.00/ton | **−1,015** |
| **sum** | | | **−5,176** |

| published cell | published | EIA-implied | coverage |
|---|---:|---:|---:|
| `211000` oil and gas extraction | −7,577 | −4,161 | **55%** |
| `212100` coal | −1,172 | −1,015 | **87%** |
| all eight mining commodities | −9,398 | −5,176 | **55%** |

✅ **Same sign on every cell and the right order of magnitude**, with coal at 87%
close enough that the correspondence reads as real rather than coincidental. That
is a stronger result than this plan's earlier "net to the right sign and order of
magnitude", which was inferred rather than computed.

⚠️ **These are rough single prices, so ±20% a row.** It is a magnitude check, not
a calibration — and the build rule is unchanged: **structure from the source,
level from NIPA**, so the price sensitivity never reaches `F03000`.

⚠️ **`211000` at 55% is the honest gap, and it should not be closed with a price
adjustment.** Crude plus working gas does not reach the whole oil-and-gas cell;
the likely remainder is NGLs, lease stocks, and refinery-held crude outside the
storage series. The coal cell at 87% shows what good coverage looks like on the
same method, so the shortfall is scope, not price.

#### ❌ The gas direction below is wrong

⚠️ **The table further down states natural gas *rose* +531,118 MMcf over
2016 → 2017**, and concludes from crude falling against gas rising that "the
branch cannot be split on a single commodity proxy". **Both EIA storage measures
show gas falling:**

| measure | 2016 | 2017 | change |
|---|---:|---:|---:|
| `SAO` working gas | 3,296,944 | 3,032,602 | **−264,342** |
| `SAT` total underground | 7,676,680 | 7,392,391 | **−284,289** |

✅ **Crude and gas moved the same way**, so `211000` at −7,577 is a *sum* of
same-signed movements rather than a net of opposing ones. **That makes the branch
more tractable than this plan concluded, not less** — the stated obstacle is not
there.

#### Endpoints, as they actually answer

| want | route | note |
|---|---|---|
| crude stocks | `petroleum/stoc/cu`, `frequency=annual`, `duoarea=NUS` | ✅ annual directly |
| gas stocks | `natural-gas/stor/sum`, `process=SAO` (working) or `SAT` (total) | ⚠️ **monthly only** — annual returns 0 rows; take December |
| coal stocks | `total-energy`, series *Coal Stocks, Total, End of Period* | ⚠️ needs `sort[0][column]=period`; without it the window returns only the last year |
| coal, v2 `coal/` routes | — | ❌ no stocks route; shipments, production, prices and reserves only |

### Sources, probed 2026-08-18

| Source | Detail | Cadence | State |
|---|---|---|---|
| **EIA MER `T03.04`** petroleum stocks | by product | monthly from 1973 | keyless CSV; also on the v2 API |
| **EIA MER `T06.03`** coal stocks | by holding sector | monthly from 1973 | keyless CSV |
| **EIA `petroleum/stoc/cu`** | crude oil | **annual** from 1936 | v2 API, `EIA_API_KEY` now in `.env` |
| **EIA `natural-gas/stor/sum`** | natural gas | **annual** | v2 API |
| **USGS Minerals Yearbook** | metal ores and industrial minerals | annual | 52 `USGS_MYB_*` extractors already in bedrock |
| Econ Census `ecnlifomine` | 6-digit NAICS, **beginning *and* end of year** | 2012/2017/2022 | not extracted; best 2017 cross-check |
| AIES `basic` | — | — | ⛔ **ruled out.** Rows exist for NAICS 21/22/23 but **zero inventory values** |

2016 → 2017, the movements behind `211000` and `212100`:

| | 2016 | 2017 | change |
|---|---:|---:|---:|
| Crude oil | 387,832 MBBL | 321,901 | **−65,931** |
| Natural gas | 6,649,840 MMCF | 7,180,958 | **+531,118** |
| Coal | 192,990 kst | 166,956 | **−26,033** |

⚠️ **Crude fell while gas rose**, so `211000` at −7,577 is the *net* of two large
opposing physical movements. The branch cannot be split on a single commodity
proxy. At rough 2017 prices the two net to the right sign and order of magnitude.

⚠️ **These are physical units, and converting them needs a price — which
reintroduces the holding gains the IVA exists to strip.** Same trap as §Farm,
where differencing FIWS stocks gives −887 against a true −5,679, out by ~6×. So
this follows the same rule as farm and `MDTY`: **structure from the source, level
from NIPA.** EIA and USGS give the relative split across crude oil, natural gas,
coal and metal ores; NIPA's −14,261 sets the magnitude.

`ecnlifomine` is the exception worth noting — it publishes beginning *and* end of
year directly, so it needs no differencing, but only every five years. Its 2017
mining total is +824 against the branch's −14,261, the gap being scope (mining
only), LIFO book valuation, and Economic Census establishment exclusions. Use it
to check the EIA-derived 2017 shape, not to set a level.

## Open questions

1. **Trade crosswalk, or wait for #615?** The ~25-line version is cheap and
   covers the column; the NAPCS concordance is the general solution and serves
   margins too. Sequencing decision, not a data question.
2. ~~**Rule for mining/utilities/construction**~~ — **sources identified,
   deferred to [#660](https://github.com/cornerstone-data/bedrock/issues/660).**
   The branch turns out to be mining-only in commodity space, and EIA MER plus
   USGS Minerals Yearbook cover it annually. Second priority: return to it later
   in Phase 1 if there is time, otherwise Phase 2. See §The mining branch.
3. **Which storable-goods filter** for the M&S manufacturing branch — a
   commodity-level goods/services flag, or port `EC1731MATFUEL` properly.
4. ~~**Is the department-store/other-general-merchandise pair a
   reclassification?**~~ — **yes, answered 2026-08-18.** Measured across every
   published year: 2018-2024 always share a sign with gross equal to net, while
   2017 alone has them opposed at 13.9x their own net. 2017 therefore takes
   their parent, line 73. See `Inventories_2017.yaml`.
5. **Splitting within farm crops and livestock** — 2017 detail shares, or FIWS
   cash receipts by commodity? Same shape as #577's question, and probably wants
   the same answer. **Also second priority, deferred to
   [#660](https://github.com/cornerstone-data/bedrock/issues/660)**: the farm
   level is published and settled, only the within-crops/livestock split is
   open, and it governs -5,679.
