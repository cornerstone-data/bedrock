# Annual business-survey expense data as a source for intermediate requirements

Desk-research notes on whether the Census annual business surveys (Manufactures, Retail, Wholesale,
Services, Transportation) can supply annual information on **inputs to production**, for use in
estimating the SUT Use table's intermediate block for nowcast years 2018-2025.

**Status: desk research only.** Nothing here has been pulled through an API or reconciled against a
BEA table. This is the input to a probe task, not the result of one.

**Probe task:** [#564](https://github.com/cornerstone-data/bedrock/issues/564) — optional, not on the
critical path for Step 3.

Plan context: [`.claude/plan/nowcast_phase1_plan.md`](../../../.claude/plan/nowcast_phase1_plan.md),
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

## The real question

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

## Suggested probe, in order

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
