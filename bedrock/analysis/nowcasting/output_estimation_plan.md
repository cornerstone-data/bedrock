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

**There is a *less* circular reference, and it covers every nowcast year.** BEA
publishes annual Supply tables at **summary** level for **2017-2024**
(`Supply_Tables_*_Summary.xlsx`, loaded by `_load_usa_summary_sut`), whose `T007`
column is *Total Commodity Output*. Both margins of the detail block are
available from it:

| margin | published at | years |
|---|---|---|
| industry output (columns) | **detail**, 402 industries | 1997-2024 |
| commodity output `T007` (rows) | **summary**, 75 commodities | 2017-2024 |
| interior cells | detail | 2017 only |

Detail industry output is already loaded and already carries the right
convention — `derive_gross_output(year, 'before')` in
[`derived_gross_industry_output.py`](../../transform/iot/derived_gross_industry_output.py),
402 industries, 1997-2024. It is BEA's published series, not something we model.

⚠️ **It is a reference, not ground truth, and the distinction is load-bearing.**
Outside 2017 nothing here is observed. Summary `T007` for 2018-2024 is BEA's own
**best-change estimate**, produced by the machinery described above — the
benchmark make table extrapolated with annual survey indicators. Only the 2017
benchmark is a *best-level* estimate resting on the Economic Census.

So scoring against it measures **divergence from BEA's annual estimate, not from
truth**, and it carries a weaker form of the circularity that disqualifies the
benchmark replay: wherever BEA also carried the mix forward, we agree with them
by construction rather than by being right. What the comparison *does* buy is
BEA's annual indicator work, which is more than a frozen mix — so a divergence
marks a place where an annual indicator moved something and our frozen mix did
not. That is genuinely informative, and matching perfectly is not the goal.

**First task, then:** carry the 2017 detail mix forward onto published detail
industry output, aggregate to summary, and score the resulting commodity output
against the published summary `T007` for 2018-2024. That is cheap, uses only
what is already loaded, and is not circular. It answers the question that decides
everything else — *how wrong is a frozen mix, and where* — and the prediction to
test is that the error concentrates in regime A, where product substitution moves
a mix and where independent product data exists to fix it.

It also gives Step 4a a validation harness that works in every year rather than
only at the benchmark.

### First result: the row margin, reconciled — and the mix is better than feared

[`frozen_mix_diagnostic.py`](frozen_mix_diagnostic.py) runs it.

**The row margin had to be reconciled first, and it was a valuation gap.** The
Supply block is basic value; `derive_gross_output` returns producer prices. The
wedge is taxes on products less subsidies, so
`GO(basic) = GO(producer) - T00TOP + T00SUB` — with `T00SUB` stored **positive**
in the Use table and negative in the Supply table, which is BEA's convention and
the sign trap [#655](https://github.com/cornerstone-data/bedrock/issues/655)
documents. Applied per industry on 2017 this closes to **0.0002% in total, with
no industry off by more than 0.1%** and a maximum single-industry difference of
$4M. The row margin is solved.

With that in place the identity control closes exactly and the drift is
measurable:

| year | wtd mean abs err | max | worst groups |
|---|---:|---:|---|
| **2017 (control)** | **0.00%** | 0.0% | — identity closes |
| 2018 | 0.37% | 8.1% | `Other`, `Used`, `113FF` |
| 2022 | 3.15% | 34.8% | `525` +35, `483` +22, `213` +18 |
| 2024 | 1.06% | 12.9% | `Other`, `315AL`, `Used` |

**A frozen 2017 mix is good to about 0.4% one year out and 3% at worst.** That
vindicates BEA's own practice of carrying the make table forward, and it means
the summary correction a nowcast needs is small — so the risk of flattening
detail by closing it is correspondingly small. 2022 being worse than 2024 looks
like COVID-era distortion washing out rather than accumulating drift.

⚠️ **Two things this does not measure, and both matter.**

1. **It is scored at summary, 73 groups.** A summary group can be right while its
   detail children are wrong in offsetting directions. The within-group detail
   drift — which is exactly what a uniform ratio would flatten — is still
   unmeasured, and there is no published detail target to measure it against.
   This is the open question, not the aggregate.
2. **Later years conflate mix drift with product-tax-rate drift**, because the
   basic÷producer ratio is held at 2017. The two cannot be separated until Step
   5's balance solves the tax split (#655) — a real coupling, and the reason 4a
   cannot be finished in isolation from 5.

The named groups are worth chasing on their own terms: `525` funds and trusts,
`483` water transport and `213` mining support are small in value but move
20-35%, which is a mix story rather than a level one.

⚠️ **A methodological note worth keeping. Never select the block by code shape.**
The first run picked industry columns by *length*, which silently swallowed
`'TRADE '` — the label carries a trailing space, making it six characters like a
BEA detail code — injecting the whole trade margin column and inflating apparel
16x. Fixing that exposed the same mistake on the rows, where the length filter
dropped `GSLGE`/`GSLGH`/`GSLGO`, five characters, losing 1.7tn of state and local
government output. **Both mistakes left an economy-wide total within half a
percent of unity and a plausible-looking per-group error table.** Neither was
found by inspection; both were found by asking whether the identity closed.
Score the identity first.

### The three divergent groups, diagnosed

`525`, `483` and `213` were the worst of 2022. They turn out to share one
mechanism: **a frozen mix converts an industry's output growth — often price
growth — into commodity output that did not happen.**

| group | published `T007` 2017→22 | same-code industry GO | gap |
|---|---:|---:|---:|
| `525` funds and trusts | ×0.891 | ×1.176 | −24.2% |
| `483` water transport | ×0.997 | ×1.216 | −18.0% |
| `213` mining support | ×0.951 | ×0.923 | +3.0% |

⚠️ **`213` is the instructive one, because its own industry barely moved.** Its
error is almost entirely **`211000` oil and gas extraction**, whose output went
**×2.57** on the 2021-22 energy price surge and which holds a 5.3% secondary
share of mining-support commodities in the 2017 block. That single industry
contributes **+20,347** of a +14,593 net rise, in a commodity whose published
output *fell* 4.9%. A price move in one industry propagated through a frozen
secondary share into a phantom quantity move in another commodity.

`483` and `525` are the simpler shape — own-industry driven, 95% and 100% shares
— where industry output rose 22% and 18% while commodity output stayed flat and
fell 11%. `525` is very likely the **RIC measurement change** from the 2023
comprehensive update (RIC gross output redefined as the sum of intermediate
expenses), which a 2017 mix cannot know about; `GSLGO` also contributes, holding
3.1% of funds and trusts, which is government employee retirement.

**Design implication — hypothesised, then tested and rejected.** The obvious
reading of `213` is that a frozen mix propagates a *price* move into a phantom
quantity move, and that deflating before applying the mix would fix it. **It does
not.**

Deflating column-wise by the summary **industry** price index — the non-circular
choice, since the commodity index is itself derived from the Make table mix —
moves the 2022 mix-only error from **1.06% to 1.01%** weighted, and `213` from
5.6% to 4.4%. The industry deflators are large (`324` at 1.913, `211` at 1.809,
median 1.218), so this is not a case of the correction being too small to see:
price largely **cancels**, because the same deflator applies to our build and to
BEA's published block alike.

⚠️ So the residual mix error is **real reallocation, not a price artifact**. The
`213` story is that oil and gas genuinely produced relatively less
mining-support commodity, or that BEA reallocated it — not that a price surge
leaked through a frozen share. Deflation is not the fix, and a design built
around it would be solving the wrong problem.

⚠️ **A note on deflator choice, which is why this test is trustworthy.** The
commodity price index is derived from the Supply/Make table mix, so using it to
test whether the mix moved would be circular. The industry index applied
column-wise avoids that. Note also that a uniform column-wise deflator leaves
within-column *shares* algebraically unchanged — dividing a column by `d[i]`
cancels in `B[c,i] / sum_c B[c,i]` — so it cannot move the mix-drift measure
itself. What it changes, and what this test exercises, is the **level** pushed
through the mix and the level in the comparison.

### Where the summary control is free, and where it costs

The concern about flattening detail has a size:

- **21 of 73 summary groups have a single detail child**, covering **19.8% of
  commodity output**. For these, imposing published `T007` is **exact** — there
  is no within-group distribution to flatten. `525` and `483` are both in this
  set, so both can simply be aligned to BEA's published estimate.
- The value-weighted mean is **6.7 detail children per group**, so the typical
  dollar sits in a group where a uniform ratio *would* flatten. The largest are
  `5412OP` (10 children), `42` wholesale (11), `23` construction (12), `621` (7).
- `213` has two children, so its correction is nearly free as well.

⚠️ **This reframes the block.** Summary `T007` is *published for 2017-2024*, so
for every year we care about, commodity output at summary is already estimated by
BEA at a level we are unlikely to beat — it embodies their annual indicator work
across every sector. Step 4a's real job is therefore **the detail split within
summary groups**, not the level.

That is a choice to adopt BEA's summary level as our reference rather than a
claim that it is observed. It is defensible while our own indicator coverage is
thinner than theirs, and it should be revisited per sector as that changes — a
sector where we have a better annual indicator than BEA is a sector where
deferring to their summary level *loses* information.

That also says where the remaining work is: the ~80% of output sitting in
multi-child groups, and specifically the big ones above.

### Within-group detail movement, measured without the 2012 benchmark

The question summary scoring cannot reach is whether detail children move
*inside* a summary group. It does **not** need the 2012 benchmark: the published
**detail industry gross output series runs 1997-2024**, so within-group detail
movement is observable annually and directly.

Detail industry shares within their summary group, drift from 2017:

| year | wtd mean \|Δshare\| | max | children moved >1pt |
|---|---:|---:|---:|
| 2018 | 0.00357 | 0.083 | 33 of 402 |
| 2020 | 0.01185 | 0.190 | 114 of 402 |
| 2022 | 0.01470 | 0.139 | 129 of 402 |
| 2024 | 0.01889 | 0.150 | 140 of 402 |

**By 2024, 4.88% of a summary group's output has been reallocated among its
detail children** (value-weighted), and detail within-group drift is **about
three times** BEA's summary-level mix drift over the same window (0.0189 against
0.0063). Biggest reshuffles: `4A0` other retail (11.7% reallocated across 6
children), `511` publishing (11.1% across 5), `3361MV` motor vehicles (11.1%
across 14), `561` (9.3%), `521CI` (8.7%).

So within-group detail movement is real and substantial — working at summary
alone would discard it.

⚠️ **This is a proxy, and the distinction matters.** It measures movement in
detail *industry output*, not in the commodity *mix*. It is strong evidence by
analogy — if detail industries reshuffle three times as much as summary
aggregates, detail mixes plausibly do too — but the mix itself remains
unmeasured at detail after 2017, and only a second benchmark could settle that.

### What this means for the construction

**The design is sound, and it is not flattening.** In
`built[c] = Σ_i mix17[c,i] × GO[i]`, the within-group distribution is driven by
**detail industry gross output**, which demonstrably moves — 4.88% reallocated by
2024. Anchoring a group's *level* to BEA's summary `T007` is then a single
factor per group applied on top of a distribution that detail data already
shaped.

That is the answer to the concern about losing detail: the objection is to
deriving a *change ratio* at summary and applying it uniformly to all children,
which would indeed erase within-group variation. Here the change comes from
detail GO and only the residual level correction is uniform. The two are not the
same operation.

**Remaining exposure** is the frozen `mix17` itself. Detail industry movement is
captured; detail *mix* movement is not, and is unmeasurable until a second
detail benchmark exists. Given summary mix drift is 0.006 and the mix accounts
for roughly a third of our total error, this is a bounded risk rather than an
open-ended one.

## The construction: primary data for `q` where possible, rebalanced to summary

**Decided.** Rather than build `q` everywhere from a frozen mix, collect primary
data and estimate `q` **directly at detail** where the data allows, then rebalance
the detail to BEA's published summary total. Frozen-mix `mix17 x x` is the
fallback for commodities with no primary source, not the default.

(`q` = commodity output, `x` = industry output, throughout.)

### Which commodities to estimate — the priority is concentrated

A commodity needs primary data only to the extent its `q` does **not** come from
its own industry. Where `q` is essentially all diagonal, `q ≈ x`, and `x` is
already published at detail for 1997-2024 — the mix does no work and there is
nothing to estimate.

Measured on the 2017 detail block:

| | |
|---|---:|
| total `q` | 33.77 tn |
| secondary-sourced, i.e. dependent on the mix | **9.5%** = 3.22 tn |
| commodities with secondary share <5% | **231 of 399**, 60.2% of `q` |
| top 20 commodities' share of the at-risk dollars | **61.6%** |
| top 50 | 80.8% |
| top 100 | 92.1% |

⚠️ **90.5% of `q` needs no mix work at all**, and the exposed remainder is
concentrated in about 50 commodities. That is the target list.

**Top 15 by dollars at risk** (`q` bn, secondary share, at-risk bn):

| commodity | `q` | sec | at risk |
|---|---:|---:|---:|
| `541700` Scientific R&D services | 617 | 0.59 | **366** |
| `541800` Advertising and PR | 403 | 0.68 | **275** |
| `622000` Hospitals | 1,052 | 0.21 | **225** |
| `541511` Custom computer programming | 270 | 0.57 | 154 |
| `518200` Data processing and hosting | 247 | 0.43 | 106 |
| `611A00` Colleges and universities | 249 | 0.37 | 91 |
| `221100` Electric power generation | 432 | 0.19 | 81 |
| `722A00` All other food and drinking places | 179 | 0.45 | 80 |
| `713200` Gambling | 109 | 0.68 | 74 |
| `532100` Automotive equipment rental | 133 | 0.53 | 71 |
| `221300` Water, sewage and other systems | 80 | 0.88 | 70 |
| `523900` Other financial investment | 388 | 0.16 | 60 |
| `811100` Automotive repair | 186 | 0.30 | 57 |
| `515100` Radio and TV broadcasting | 81 | 0.57 | 46 |
| `517110` Wired telecommunications | 352 | 0.13 | 45 |

⚠️ **This inverts the earlier read on which primary source matters most.** The
regime table above shows manufacturing holding 231 of 399 commodity *rows*, which
made ASM product lines look like the priority gap. By dollars at risk the list is
overwhelmingly **services** — R&D, advertising, programming, hospitals, data
processing, education, gambling. Manufacturing rows are numerous but their `q` is
nearly all diagonal, so a frozen mix costs almost nothing there.

**So the primary-data programme should target `Census_SAS` product lines first,
not `Census_ASM` product tables.** SAS is already extracted and already read at
product-line granularity for the transport margins (Table 8), so the machinery
exists. ASM product lines drop well down the list.

### Rebalancing

Detail `q` estimated from primary data is then reconciled to BEA's published
summary `T007`. That is a level correction on a distribution primary data has
already shaped — not a change ratio pushed uniformly onto children — which is the
distinction that makes it non-flattening. Where no primary source exists, the
frozen mix supplies the shape and the same rebalance supplies the level.

**Open:** whether the rebalance should be proportional within group or minimise
change subject to the margin — BEA's own best-change rule is the latter, and it
is the same objective Step 5 solves, so the two should be chosen together.

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
