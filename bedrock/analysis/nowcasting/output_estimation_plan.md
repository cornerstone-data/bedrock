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

**Open:** whether route 2 is reachable for us. It depends on the same
NAPCS → I-O commodity concordance that #615 is building, and on `Census_EC_PxI`
coverage — which is exactly the test §Step 4a already schedules before porting a
mix. Score both routes against published 2017 per cell.

## Before redefinitions

Settled in [`plan.md`](plan.md) §Step 4a: the target is **NAICS output before
redefinitions**, not the manual's "I-O industry output". We do not perform the
redefinitions BEA performs. Not repeated here — but note its consequence for the
recipe below, since chapter 5's worked tables run *through* the line we want to
the line we do not.

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

**To do: walk back through the earlier update articles.** 2018 and 2023 are
read; the 2013 and 2004 comprehensive updates and the intervening annual updates
are not. Each one's changes carry forward into the 2017 benchmark we are
reproducing.
