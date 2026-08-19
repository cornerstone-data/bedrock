# Plan — estimating commodity output (Step 4a)

Step 4a of [`plan.md`](plan.md), issue
[#570](https://github.com/cornerstone-data/bedrock/issues/570) — the domestic
output block `T007` of the SUT Supply table, commodity × industry at basic
value.

Method statements below are from the **BEA IO manual (2009), chapter 5
"Output"** — PDF pages 69–92, narrative to p. 83 and table 5.A's per-industry
source summaries at pp. 84–92 — and from BEA's *Survey of Current Business*
comprehensive-update articles, which carry the changes forward from the manual's
1997 vintage.

---

## The estimand is **commodity** output

`T007` is a commodity × industry block. Its **column sums are commodity output**
and its row sums are industry output, and it is commodity output that Step 4a
exists to produce — the Supply table is indexed on commodities, and every
downstream consumer (4e's identities, Step 5's RAS, Step 6a's Make transpose)
reads it that way. Industry output is an input to the construction, not the
deliverable.

That matters because it admits **two construction routes**, and the manual says
BEA uses different ones for different industries:

1. **Industry output × commodity mix** — what #570 currently specifies. Needed
   wherever only industry receipts are collected, which is most of services.
   The manual (p. 79): *"In industries where only data on industry receipts are
   collected and tabulated, more adjustments are necessary when calculating
   commodity output."* You begin with all industry production, remove secondary
   products, then add the primary commodity produced by other industries.

2. **Product data directly** — for mining and manufacturing, the manual is
   explicit that commodity output does **not** come from industry output at all:
   *"In industries where product shipments or product receipts are collected and
   tabulated, the calculation of commodity output starts with that data rather
   than the industry data"* (p. 79). Census tabulates shipments *by product*,
   covering the product's total "no matter where it is made".

⚠️ **The worked examples are built to make this contrast**, and the manual flags
it: in the cheese case *"the calculations of industry output and of commodity
output start from different data sources"*, while in telecom *"the calculations
start from the same data source"* (p. 79). A single construction route applied
to both regimes silently imposes the services method on manufacturing, where
observed product data exists and is better.

**So the industries have to be partitioned by regime** — which ones derive `q`
from industry output, and which derive it independently — rather than run through
one construction. Sized on published 2017 detail `T007`:

| regime | commodities | `T007` | share |
|---|---:|---:|---:|
| B services, receipts only → industry output × mix | 108 | 19.8 tn | **61.8%** |
| A manufacturing, product data → independent | 231 | 5.4 tn | 16.9% |
| D trade (margin output, its own problem) | 19 | 3.2 tn | 9.9% |
| C construction (VIP survey) | 12 | 1.7 tn | 5.2% |
| E special and government | 8 | 1.1 tn | 3.4% |
| C farm (USDA) | 13 | 0.5 tn | 1.4% |
| A mining, product data → independent | 8 | 0.4 tn | 1.3% |

⚠️ **The value and the detail point in opposite directions.** Regime A is only
**18.2% of commodity output but 239 of 399 commodities** — manufacturing holds
60% of the rows and a sixth of the dollars. So a value-weighted score will say
the mix barely matters while a per-commodity score says it dominates, and both
are right about different things. Services carry the dollars in 108 rows;
manufacturing carries the commodity detail, and is also where product
substitution and secondary production actually move a mix.

**Open:** whether route 2 is reachable for us. It depends on the same
NAPCS → I-O commodity concordance that #615 is building, and on `Census_EC_PxI`
coverage — which is exactly the test §Step 4a already schedules before porting a
mix. Score both routes against published 2017 per cell.

## How BEA itself does the nonbenchmark year

This is the question Step 4a asks, and BEA answers it directly. From
[Integrating the 2002 Benchmark I-O and Annual Industry
Accounts](https://apps.bea.gov/scb/pdf/2007/12%20December/1207_indyaccount.pdf)
(Dec 2007, n. 9):

> Gross output in the annual industry accounts is calculated using annual survey
> data to **extrapolate gross output from the make table** in the most recent
> benchmark I-O accounts.

⚠️ **Note what is extrapolated: the make table, not industry output.** The block
itself is carried forward, which means the benchmark's commodity mix rides along
unless something moves it. That is a **third route** alongside the two above, and
it is the one #570's "port the 2017 mix" already describes — so the R-script
approach is not a shortcut, it is BEA's own practice. The `Census_EC_PxI` test
§Step 4a schedules is therefore a test of whether we can do *better* than BEA
here, not of whether we can match them.

**The framing to build against is best-level / best-change.** From the
[2002 benchmark gross output
preview](https://apps.bea.gov/scb/pdf/2005/09September/0905_I-O_Accounts.pdf)
(Sept 2005):

> The benchmark I-O accounts set "best levels" for industry and commodity output
> levels for the annual I-O accounts, which are prepared using less detailed and
> less comprehensive source data. […] Annual I-O and annual NIPA estimates are
> "best-change," since they are estimated using extrapolators that are considered
> the most reliable estimates of year-to-year growth. When the next Economic
> Census data are completely incorporated into the accounts, the "best-change"
> estimates are revised so as to **minimize the change from the original
> estimates, yet pass through the best-level estimate**.

That is exactly the nowcast's situation, and it names the reconciliation rule:
minimum change subject to passing through the benchmark. It is the same objective
Step 5's RAS optimises, which is worth knowing before choosing a balancing
method — the two steps are solving one problem, not two.

## Before redefinitions

Settled in [`plan.md`](plan.md) §Step 4a: the target is **NAICS output before
redefinitions**, not the manual's "I-O industry output". We do not perform the
redefinitions BEA performs.

**Corroborated independently**, with BEA's reason, in the [2002 benchmark I-O
article](https://apps.bea.gov/scb/pdf/2007/10%20October/1007_benchmark_io.pdf)
(Oct 2007):

> The **standard** make and use tables are constructed **before** the
> redefinition of selected secondary products; all of the products — primary and
> secondary — that are produced by an industry are assigned to that industry. As
> a result, the data in these tables are consistent with GDP-by-industry
> accounts, the gross-domestic-product-by-state accounts, and with other industry
> data reported by other statistical agencies.

The after-redefinition versions are **supplementary** tables, published for
traditional I-O analysis, which "requires" homogeneous industries. So the
before/after split is not a preference: before-redefinitions is the *standard*
presentation and the one consistent with outside source data — which is the whole
basis on which a nowcast extrapolates. Note its consequence for the recipe below,
since chapter 5's worked tables run *through* the line we want to the line we do
not.

## Where to start

⚠️ **The benchmark replay the issue specifies is circular under a ported mix.**
#570 says to "run the construction for 2017 and compare against the published
detail Supply table" — but if the mix is carried from 2017, 2017 reproduces
itself exactly and the test proves nothing. It only bites for a route that
*estimates* the mix.

**There is a non-circular target, and it covers every nowcast year.** BEA
publishes annual Supply tables at **summary** level for **2017-2024**
(`Supply_Tables_*_Summary.xlsx`, loaded by `_load_usa_summary_sut`), whose `T007`
column is *Total Commodity Output*. So both margins of the detail block are
observed:

| margin | observed at | years |
|---|---|---|
| industry output (columns) | **detail**, 402 industries | 1997-2024 |
| commodity output `T007` (rows) | **summary**, 75 commodities | 2017-2024 |
| interior cells | detail | 2017 only |

Detail industry output is already loaded and already carries the right
convention — `derive_gross_output(year, 'before')` in
[`derived_gross_industry_output.py`](../../transform/iot/derived_gross_industry_output.py),
402 industries, 1997-2024. It is BEA's published series, not something we model.

**First task, then:** carry the 2017 detail mix forward onto observed detail
industry output, aggregate to summary, and score the resulting commodity output
against the published summary `T007` for 2018-2024. That is cheap, uses only
what is already loaded, and is not circular. It answers the question that decides
everything else — *how wrong is a frozen mix, and where* — and the prediction to
test is that the error concentrates in regime A, where product substitution moves
a mix and where independent product data exists to fix it.

It also gives Step 4a a validation harness that works in every year rather than
only at the benchmark.

## The adjustment ladder

Chapter 5's tables 5.1 (cheese, NAICS 311513) and 5.2 (telecom, NAICS 5133) give
the line items. Marked for what we keep:

| line | industry | commodity | keep? |
|---|---|---|---|
| shipments / receipts | start | start (product, where it exists) | yes |
| beginning / ending inventory | yes | yes | yes |
| imputations (own-account software and construction) | yes | yes | yes |
| miscellaneous receipts | yes | yes | yes |
| tax misreporting | yes | yes | yes |
| nonemployers | yes | yes | yes |
| cost of resales | yes | yes | yes |
| = **NAICS OUTPUT** | **our industry line** | | **stop here** |
| redefinitions out | to I-O output | yes | **no** — after-redefinitions concept |
| reclassifications | | yes | yes — BEA vs Census on what is primary |
| secondary out / secondary in | | yes (receipts-only industries) | yes |
| make-table adjustment | | yes | yes — product-vs-industry source inconsistency |

The last three are not redefinitions and survive the version choice.
Reclassification is BEA disagreeing with Census about what is primary; the
make-table adjustment exists *"because of small inconsistencies between the
product and industry data"* and without it *"total commodity output and total
industry output would not be equal"* (p. 80 n. 15).

⚠️ **That last identity is a free target.** Total commodity output equals total
industry output for the economy, and total industry output before redefinitions
equals total industry output after (p. 78). Cheap to assert, and it fails loudly
if the ladder is misapplied economy-wide — though not per commodity, which is
where the real errors live.

## Sources

[`output_estimation_sources.csv`](output_estimation_sources.csv) is BEA's own
table C1, *"Principal Data Sources for Industry and Commodity Output and
Prices"*, from the [November 2023 comprehensive-update
results](https://apps.bea.gov/scb/issues/2023/11-november/1123-nea-comprehensive-update.htm)
— 44 industries across 21 sectors, with the **benchmark-year** and
**nonbenchmark-year** sources given separately.

**The nonbenchmark column is the one that matters for us**, because it is BEA's
own answer to the question Step 4a asks: what moves output between benchmarks.
It is IEA summary detail, not BEA detail, so it says which series drives a
sector rather than which drives a commodity — a starting map, not a lookup.

Cross-cutting, from the [June 2023
preview](https://apps.bea.gov/scb/issues/2023/06-june/0623-nea-preview.htm):
ASM, the Annual Surveys of Wholesale and of Retail Trade, SAS, Value of
Construction Put in Place, QCEW, IRS corporate and partnership tabulations, and
USDA farm statistics.

⚠️ **The manual documents the 1997 benchmark and says so in its preface.** It is
authoritative on concepts and structure only. Every source it names needs
checking against current Census products — NAPCS landed in the 2007 Economic
Census and changed what product data exists at all, which is precisely what
route 2 above depends on.

## Carried-forward changes from update articles

Method changes accumulate across comprehensive updates, so an article is only
current until the next one supersedes it. Recorded here as they are found.

- **Secondary production of wholesale and retail trade broken out by type**
  ([2018 IEA preview](https://apps.bea.gov/scb/issues/2018/08-august/0818-industry-economic-accounts-preview.htm)):
  *"secondary production of retail and wholesale commodities will be broken-out
  to reflect the specific type of retail or wholesale commodity, as opposed to
  being captured in a single aggregate commodity."* Secondary production is
  exactly what the commodity mix distributes, so this changes the shape of the
  trade rows in `T007` from the 2012 benchmark forward. Also bears on Step
  4c/4d, where the trade margin has to land on a specific trade commodity.
- **Equity REITs reclassified** from finance to real estate (2023 update) —
  moves industry output between two sectors we build.
- **RIC gross output = sum of intermediate expenses**, value added zero by
  construction (2023 update).
- **Air transportation's annual indicator became SAS**, replacing BTS, from 2010
  (2018 update) — and SAS *"provide[s] break-outs of domestic and international
  freight and passenger transportation"*, which is the same domestic/international
  split the transport margin coverage ratios turn on
  ([`margins_estimation_plan.md`](margins_estimation_plan.md)).
- **Used goods**: the scrap/used/secondhand line is consolidated into the
  corresponding new commodities in the **summary and sector** use tables (2023
  update). Detail is unaffected — `S00401` and `S00402` are both still in
  [`v2017_commodity.py`](../../utils/taxonomy/bea/v2017_commodity.py) and in our
  built outputs, so [#665](https://github.com/cornerstone-data/bedrock/issues/665)
  stands as written.

Earlier, from the swept articles:

- **The SUT framework became BEA's featured I-O presentation with the 2018
  comprehensive update** ([2019 annual
  update](https://apps.bea.gov/scb/issues/2019/11-november/1119-industry-update.htm)).
  Before that the featured presentation was Make/Use. Relevant when reading any
  pre-2018 method statement about "the make table" — it is describing the same
  block we call `T007`, under the older presentation.
- **R&D, and entertainment/literary/artistic originals, capitalised** (2013
  comprehensive, [preview](https://apps.bea.gov/scb/pdf/2013/06%20June/0613_preview_comprehensive_iea_revision.pdf)
  / [results](https://apps.bea.gov/scb/pdf/2014/02%20February/0214_industry%20economic%20accounts.pdf)).
  Own-account production of these became output, so gross output levels for the
  producing industries step up at that revision. Already in the 2017 benchmark,
  but it breaks comparability with anything estimated on a pre-2013 basis.
- **Retail trade margin PPIs from BLS, and new Census business expense data**
  (2010 comprehensive,
  [preview](https://apps.bea.gov/scb/pdf/2010/03%20March/0310_indy_accts.pdf)) —
  the first improved real gross output and value added for retail trade; the
  second "improved the commodity mix of most industries' intermediate inputs".
  The commodity-mix half is Step 3's concern, but it is the same Census expense
  data.
- **NAICS re-basing between benchmarks** (2010 preview): a concordance
  reallocates the prior benchmark's make table onto the new NAICS structure using
  weights from a **back-extrapolation of the new benchmark make table**. Worth
  knowing if we ever cross a NAICS vintage; we do not today, since everything is
  2017.
- **Banking implicit services and P&C insurance normal losses** entered via the
  2003 NIPA comprehensive revision and were incorporated into the 2002 benchmark
  ([2007 benchmark article](https://apps.bea.gov/scb/pdf/2007/10%20October/1007_benchmark_io.pdf)).
  Both define gross output for their industries and both are still in force.

⚠️ **ASM does not exist in Economic Census years.** The Census Bureau does not
conduct it when the quinquennial census is conducted, so there is no 2017 ASM and
the Economic Census takes its place ([2019 annual
update](https://apps.bea.gov/scb/issues/2019/11-november/1119-industry-update.htm)).
Any indicator chain built on ASM has a hole every five years that the benchmark
fills — which is fine for us in 2017 but matters for 2022.

**Swept:** 2004 (integrated annual I-O), 2005 (2002 benchmark gross output
preview), 2007 (2002 benchmark, and its integration with the annual accounts),
2010, 2013 and 2014 (comprehensive revision preview and results), 2018 preview,
2019 annual update, 2023 preview and results. Local text extracts are not
committed; regenerate from the URLs above.

**Not swept:** the pre-2004 benchmark articles (1992 and 1997), and the annual
updates between 2005 and 2018 other than those listed. The 1997 pair would be
worth reading only if we need to reconcile the 2009 manual's own vintage against
what changed after it.
