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

**By dollars at risk the top of the list is services** — R&D, advertising,
programming, hospitals, data processing, gambling.

⚠️ **But "manufacturing is nearly all diagonal" is wrong, and was a bad read of
the aggregate.** Multi-source production is the *norm* there, not the exception:

| manufacturing | |
|---|---:|
| commodities | 231 |
| `q` | 5.43 tn |
| at risk | **0.38 tn** (6.9% of mfg `q`, **11.7% of all at-risk dollars**) |
| **median industries producing each commodity** | **10** (p90 22, max 60) |
| commodities with secondary share >10% | 49, covering 20.5% of mfg `q` |

Petrochemicals is 37.6% secondary, alumina and primary aluminium 69%,
semiconductor machinery 42%, bread and bakery 23%; other basic organic chemicals
draws on **30** producing industries. These are not diagonal commodities.

⚠️ **And feasibility runs the other way from value, which is what makes
manufacturing worth doing.** Services hold ~88% of the at-risk dollars but have
almost no *annual* product data — `Census_SAS` Table 8 reaches 12 NAICS and none
of the priority list — so they are stuck with quinquennial `Census_EC_PxI` and
interpolation between 2017 and 2022. Manufacturing holds ~12% of the at-risk
dollars but **has annual product data**: ASM product lines and the M3 shipments
series. It is the part of the block where the mix can be moved *year by year*
from reported data rather than interpolated between benchmarks.

**So both tracks are wanted, for different reasons.** Services for the dollars,
on a five-year benchmark cadence; manufacturing for the annual signal, on the
~49 multi-source commodities above. `Census_ASM` as extracted is `RCPTOT` only
(industry receipts, 2018) — the product tables are the gap.

### Which primary source actually reaches the priority list

Checked, and the answer is not the one the priority list suggested.

⚠️ **`Census_SAS` Table 8 is far narrower than assumed.** *Estimated Revenue by
Product and Class of Customer* covers **12 NAICS** in 2022 — `484` truck, the
`5111x` publishing group, `5112` software, `517311` telecom, `51913` search
portals, and `5613x`/`5615x`/`561599` admin services. **None of the top priority
commodities appear.** Table 2 carries 378 rows of revenue by detailed NAICS, but
that is `x`, not `q`. So SAS gives industry revenue for the priority list and no
product split — it cannot reach the 3.22 tn.

**`Census_EC_PxI` is the vehicle.** Economic Census *Products by Industry*, 2017,
carries 32,641 rows over 947 industries with the product code in `FlowName` (28
distinct products for `5417` alone). It covers **15 of 17** priority NAICS:
`5417`, `5418`, `5415`, `622`, `5182`, `7132`, `5321`, `2211`, `2213`, `8111`,
`5151`, `7225` and more.

**Missing: `6113` colleges and `5171` wired telecom.** Colleges are largely
non-profit and government, which the Economic Census does not cover — consistent
with the manual's note that BEA estimates nonprofit output from *operating
expenses* rather than receipts. Those two need a different source.

✅ **`Census_EC_PxI` 2022 is now wired** (#650): 32,024 rows, 903 industries,
3,503 products, pulled as `EC2200NAPCSPRDIND` on the same endpoint. A second
product-by-industry benchmark is what lets the mix *move* on observed product
data instead of being frozen at 2017.

✅ **And the vintages join, for our industries.** The yaml warns that the
2017-built concordance reaches only 70.5% of 2022 value against 94.4% of 2017 —
but that is **trade-goods specific**. Across the twelve priority service
industries, carrying 3.84tn of 2022 value, the join is **98.6% by code and 98.1%
by description**:

| NAICS | 2022 $bn | by code | by desc |
|---|---:|---:|---:|
| `5417` R&D | 298 | 100.0% | 100.0% |
| `5418` Advertising | 126 | 99.8% | 99.0% |
| `5415` Computer systems | 548 | 98.7% | 96.0% |
| `622` Hospitals | 1,343 | 98.6% | 97.4% |
| `5182` Data processing | 273 | 95.0% | 100.0% |
| `7225` Food service | 800 | 100.0% | 99.8% |
| **total** | **3,838** | **98.6%** | **98.1%** |

⚠️ **`5151` broadcasting has no 2022 rows at all** — 2022 is titled "Selected
Sectors" where 2017 is "All Sectors", so industries drop out. Check presence
before differencing, rather than reading an absence as a collapse to zero.

⚠️ **BEA's 2022 summary table does not incorporate the 2022 Economic Census** —
the 2022 benchmark I-O is not published, so their annual estimate is a
best-change extrapolation off the *2017* benchmark. That cuts two ways: our
EC_PxI-informed mix carries information their published mix does not, which is
the opportunity; and rebalancing detail to their summary level could discard it,
which is the risk. The coherent division is to take the **level** from BEA's
annual indicators, which are current, and the **mix** from EC_PxI, which they
have not used.

Still gated on the **NAPCS-collection → BEA commodity concordance**
([#615](https://github.com/cornerstone-data/bedrock/issues/615)). Note the
wrinkle `Census_EC_PxI.yaml` already documents: the identifier is Census's *2017
NAPCS collection* code, not the published NAPCS structure, so the concordance
target is the collection codes.

**Revised sequence:** wire EC_PxI 2022, build the concordance, move the mix
between the two benchmarks for the ~50 exposed commodities, fall back to frozen
2017 elsewhere, then rebalance. SAS drops to a supporting role for the dozen
NAICS its Table 8 does cover.

### Does PxI actually reproduce the supply mix? — precondition tested, and it fails

The decisive test is whether `Census_EC_PxI` reproduces the published 2017 mix
per cell. That needs the product → BEA commodity concordance, which does not
exist for services. But a **necessary precondition** needs no concordance at all:
does PxI account for the right *total* per industry?

Mapping PxI's NAICS to BEA detail industries and comparing against the 2017
Supply block's column totals:

| | |
|---|---:|
| industries matched | 367 |
| PxI total vs supply block | 32.89 tn vs 27.01 tn |
| median ratio | **0.853** |
| within ±10% of the supply column | **104 of 367** |
| within ±25% | 212 of 367 |

Priority industries scatter widely: `541700` R&D **0.580**, `713200` gambling
0.717, `518200` 0.862 — against `622000` hospitals **1.267**, `541511` 1.266,
`722A00` 1.251.

⚠️ **So PxI is not the supply mix, and was never going to be.** It is the raw
product data BEA *starts* from, before the whole adjustment ladder above —
imputations for own-account software and construction, miscellaneous receipts,
tax-misreporting and nonemployer coverage, removal of the cost of resales, then
reclassifications and secondary in/out. PxI is also a **weighted sample**, not an
enumeration (`Census_EC_PxI.yaml`). A level match was never the right
expectation.

**That does not disqualify a *share* use, which is what Step 4a needs.** Most of
those adjustments are industry-level and scale a whole column, leaving the
within-column shares intact. The mix could therefore be sound where the level is
not. But that is an argument, not a measurement — **the mix itself remains
untested until the concordance exists**, and this precondition result means the
concordance work has to be followed by the per-cell mix test before anything is
built on it.

⚠️ **`Census_EC_PxI` carries an all-sectors total row that must be dropped.**
`ActivityProducedBy == '00'` is a single 2-digit code holding **34.36 tn** beside
935 six-digit industries holding 32.89 tn — **51.1% of the file**. Any groupby
that does not filter to 6-digit codes aggregates the total alongside the detail.
This is the same class of defect as the Supply table's `T017` row, and it is the
fourth instance in this work.

**Corrected:** the earlier "only 48.9% of value mapped" was not a mapping
failure — 48.9% is exactly the 6-digit share, and once `'00'` is excluded the
NAICS→BEA mapping covers **100.0% of value**. The level scatter is therefore
**real, not an artifact**: redone on 6-digit rows only it is unchanged at median
ratio 0.853, 104 of 365 industries within ±10%. The conclusion above stands; the
reasoning that qualified it did not.

### Do service industries produce trade output? — checked in the published block

Before deciding how to treat the retail and wholesale product lines that appear
inside service industries, the first-principles question is whether the published
**before-redefinitions** Supply table shows those industries producing trade
commodities at all. If it does, the margin is part of their industry output until
redefinition moves it, and cannot simply be excluded.

**It does.** 49 of 93 service-sector industries carry non-zero output of a trade
commodity in the 2017 detail block. But the amounts are small:

| industry | output bn | trade bn | trade share |
|---|---:|---:|---:|
| `811100` Automotive repair | 131.4 | 1.59 | **1.21%** |
| `532100` Automotive equipment rental | 64.1 | 0.45 | 0.70% |
| `221100` Electric power | 365.8 | 2.24 | 0.61% |
| `518200` Data processing | 175.2 | 0.51 | 0.29% |
| `622000` Hospitals | 843.9 | 1.36 | 0.16% |
| `541700` R&D, `541800` advertising, `713200`, `561300` | — | 0.00 | 0.00% |
| **all service-sector industries** | **16.17 tn** | **0.022 tn** | **0.136%** |

⚠️ **So it is real but negligible, and the two sources agree.** PxI's trade
product lines are 0.4% of PxI value for these industries; the Supply block shows
0.136% of output. Those are consistent once the valuation difference is applied —
PxI reports trade **sales** while the Supply block reports trade **margin**, and a
typical margin rate on 0.4% of sales lands squarely on 0.14% of output.

**That corroborates the `trade-margin` flag rather than an exclusion.** These
lines map to a real thing the Supply table also records; they simply need a
margin rate applied before they can be compared with commodity output. Keeping
them flagged is right, and the stakes are bounded at a few tenths of a percent.

It also confirms the before-redefinitions choice is doing visible work: after
redefinition these cells would have been moved out to the trade industries, and
the question could not have been asked of the table at all.

### The mix test: run, and the dominant-seller rule fails systematically

[`pxi_mix_test.py`](pxi_mix_test.py) builds each industry's commodity mix from
PxI product lines through the services concordance and scores it against the
published 2017 block. `L1` is half the sum of absolute share differences — the
fraction of the mix that would have to move.

**Across 38 industries with coverage above 30%: median L1 0.285, nine under 0.05,
eighteen over 0.30.**

It works well where the industry's products are distinctive — `621600` home
health 0.003, `811100` auto repair 0.010, `811300` 0.014, `623A00` 0.016,
`622000` hospitals 0.017, `561300` 0.019, `541800` advertising 0.030, `541700`
R&D 0.035.

⚠️ **But it fails completely, and systematically, on shared products.** Three
health industries score **L1 = 1.000** — entirely disjoint from the published
mix — because `621100` physicians, `621300` outpatient and `621400` home health
all sell *"Patient care, related to ICD-10 major category"*, and the seed's
dominant-seller rule assigns that product to `622000` hospitals, which is simply
the largest seller. Every one of those industries then appears to produce
hospital output and nothing of its own. `711100`, `713200`, `813A00` and
`52A000` fail the same way.

**This is a flaw in the rule, not in the data.** "Patient care" is the *primary*
product of several distinct BEA commodities at once. A single global
product → commodity map cannot express that, and collapsing it onto the biggest
seller destroys precisely the distinction the Supply table exists to record.

**The fix is to make the mapping industry-conditional**, and it works. A third
row class beside single-target and split-target: `own-commodity`, carrying no
target and resolving **against the seller**. A product earns it automatically
when it is at least 30% of *two or more* selling industries that map to
different BEA commodities — 14 products, 1,562bn, 41.8% of seed value. That
matches the manual's framing, where the question is which products are secondary
to an industry rather than what a product is in the abstract.

| | before | after |
|---|---:|---:|
| median L1 | 0.285 | **0.075** |
| industries under 0.05 | 9 | **17** |
| industries over 0.30 | 18 | **7** |

The three total failures are gone, and nothing that worked was broken:

| industry | before | after |
|---|---:|---:|
| `621100` physicians | 1.000 | **0.005** |
| `621400` home health | 1.000 | **0.006** |
| `621300` outpatient | 1.000 | **0.055** |
| `722110` full-service restaurants | 0.451 | **0.007** |
| `722211` limited-service | 0.538 | **0.014** |
| `711100` performing arts | 0.984 | **0.039** |
| `52A000` other financial | 0.725 | **0.046** |
| `813A00` | 0.985 | **0.066** |
| `713200` gambling | 0.918 | **0.181** |
| `622000` hospitals *(control)* | 0.017 | 0.018 |

⚠️ **`own-commodity` also supersedes the reviewed split for prepared meals**, and
should. The reviewed target `722` was right; a fixed 45.9/44.9/9.1 split applies
one global ratio to every seller, whereas resolving against the seller sends
full-service restaurants' meals to `722110` and limited-service's to `722211`.
That is why those two go from 0.451 and 0.538 to 0.007 and 0.014.

### Second experiment: advertising belongs to `541800`

The seven failures clustered in media, and the published block said why — those
industries produce large shares of **`541800` advertising** where our build gave
them zero. `515100` broadcasting's single largest published output *is* `541800`
at **59.3%**; `515200` carries 35.2% and `5111A0` 21.4%.

The advertising products were scattered across four different treatments:
"Internet advertising" 112bn → `519130`, "Television air time" 84bn →
`own-commodity`, "Radio air time" 13bn → `515100`, "Advertising space in printed
publications" 22bn → unmapped. Routing all 26 advertising products to `541800`
(and adding the 11 that were unmapped, 43bn):

| industry | before | after |
|---|---:|---:|
| `519130` internet publishing | 0.691 | **0.234** |
| `515100` broadcasting | 0.623 | **0.310** |
| `515200` cable | 0.797 | **0.445** |
| `5111A0` directory publishers | 0.901 | **0.687** |
| `541800` advertising *(receiving)* | — | **0.015** |

⚠️ **Coverage below about 0.5 makes the mix an artifact, not a measurement.**
The remap appeared to *worsen* the headline because it gave `511110` newspapers
and `511120` periodicals enough coverage to enter the run — at 44%, carrying
only their advertising lines, so `541800` looks like their main output. That is
the threshold's fault, not the mapping's. Scored on industries we actually see:

| min coverage | industries | median L1 | under 0.05 | over 0.30 |
|---|---:|---:|---:|---:|
| 0.5 | 33 | **0.066** | 15 | **4** |
| 0.6 | 31 | 0.066 | 14 | 4 |
| 0.7 | 24 | 0.089 | 10 | 2 |

The default is now 0.5. **Median L1 0.066 with four industries over 0.30**,
against 0.285 and eighteen before `own-commodity` existed.

⚠️ **My earlier ten-industry read was unrepresentative and too favourable.** The
priority list is drawn from industries with distinctive products, which is
exactly the population this rule handles well. Scoring the full set is what
exposed the failure, and the same trap is available to anyone who samples the
easy cases first.

### Third experiment: the two stubborn industries

`5111A0` at 0.687 and `515200` at 0.445 needed two further mechanisms, each
justified by what the published block says rather than by tuning.

**1. The `own-commodity` trigger was too strict.** It required a product to be
≥30% of **two** sellers mapping to different commodities. "Specialty content for
consumers" is **99% of `5111A0`'s output** but only 4% of `519130`'s, so it
missed — and `5111A0` got none of its own commodity. Weakening the trigger to
≥30% of **any** seller (still requiring ≥2 distinct commodities) takes
`own-commodity` from 8 products to 59 and `5111A0` from **0.687 to 0.077**.

⚠️ **But the automatic rule must not override an explicit decision.** Weakening
it first swept "Internet advertising" and "Television air time" back into
`own-commodity`, undoing the advertising fix and sending `519130` from 0.234
back to 0.691. Reviewed products are now marked `locked` and are never
auto-flagged. **An automatic rule and a reviewed decision are not the same kind
of statement, and the rule must lose.**

**2. Some products are a different commodity depending on who makes them.**
`515200` cable programming's *"Licensing of rights to exhibit, broadcast, or rent
audiovisual works"* is 52% of its output, but the published block says `515200`
produces 42.7% **`515100` broadcasting**. For `512100` motion picture the same
product is 91% of output and *is* its own commodity. Neither a global target nor
`own-commodity` can say that, so the corrections file now takes an optional
`industry` column — a per-(industry, product) override. `515200` goes **0.445 to
0.193** while `512100` stays at 0.016, unchanged.

⚠️ **Read the corrections with `dtype=str`.** Without it pandas types the code
columns as floats and `515200` becomes `515200.0`, which matches nothing — the
override silently did not fire, and the only symptom was an unchanged score.

**Where the mix test now stands:**

| | after `own-commodity` | after advertising | now |
|---|---:|---:|---:|
| median L1 | 0.075 | 0.066 | **0.064** |
| under 0.05 | 17 | 15 | **16** |
| over 0.30 | 7 | 4 | **3** |

Remaining: `511120` periodicals 0.416 (51.6% coverage — near the floor where the
mix is an artifact), `541512` 0.370, `221300` water and sewage 0.322.

### What the mix test does and does not establish

⚠️ **The concordance was tuned against the answer.** Advertising to `541800`,
the cable override and the `own-commodity` threshold were all chosen by
inspecting where the published mix disagreed, then scored against that same
published mix. The headline 0.064 is therefore partly fitted, and quoting it as
an unbiased accuracy would be wrong.

Splitting the 35 industries by whether they were inspected while choosing rules:

| | n | median L1 | under 0.05 | over 0.30 |
|---|---:|---:|---:|---:|
| tuned on | 15 | 0.056 | 7 | 1 |
| **held out — never inspected** | 20 | **0.150** | **9** | 2 |
| all | 35 | 0.064 | 16 | 3 |

**The held-out median is 2.7x the tuned median.** That is real overfitting and
0.150 is the honest number.

**What is nonetheless established:** nine of twenty never-inspected industries
land under 0.05 — `621600` 0.003, `623A00` 0.007, `811100` 0.011, `811300`
0.014, `532100` 0.017, `524113` 0.017, `561300` 0.022, `541700` 0.032, `221100`
0.040. That cannot be an artifact of tuning, and it is far better than anything
a frozen or mechanical mapping produced. **`Census_EC_PxI` can reproduce the
published supply mix for services**, which is the necessary condition for using
it to move the mix.

**What is not established.** That PxI *is* BEA's source, rather than a series
consistent with it; the levels still diverge (median ratio 0.853) and the
adjustment ladder that would reconcile them is unverified. Coverage is partial —
industries below 50% are excluded outright, and manufacturing, trade and
agriculture are untested. And the concordance is demonstrably incomplete on
industries nobody has looked at yet.

**The honest work queue is the held-out failures**, not the tuned ones:
`541512` 0.370, `221300` 0.322, `721000` 0.294, `339950` 0.285, `713100` 0.276.
Each is a place the concordance has a gap that inspection would likely close —
but every one closed that way moves it from held-out to tuned, so the unbiased
estimate has to be re-established on a fresh holdout rather than recomputed on
the same set.

### The cadence split: services quinquennial, manufacturing annual

Checked against the Census API rather than assumed.

**Services carry no annual product detail.** `Census_SAS` Table 8, *Estimated
Revenue by Product and Class of Customer*, covers 12 NAICS — truck, publishing,
software, telecom, search portals, admin services — and none of the priority
list. So for services the only product-level source is `Census_EC_PxI`, which is
**quinquennial**: 2017 and 2022, interpolated between.

**Manufacturing does, and in the same code space.** The Census API carries
`timeseries/asm/value2017`, which returns `NAICS2017` x **`NAPCS2017`** (the 2017
NAPCS *collection* code) with `NAPCSDOL` — 17,269 rows for 2018 and 43,131 for
2021; 2023 not yet published. There is also `timeseries/asm/product`, the older
`PSCODE`/`PRODVAL` product-class series.

⚠️ **`asm/value2017` is the annual analogue of `Census_EC_PxI`, keyed on the same
NAPCS collection codes** — so the concordance built for EC_PxI applies to it
directly rather than needing its own. That is what makes an *annual* moving
manufacturing mix reachable instead of a five-year interpolation.

**So the two tracks differ in cadence, not just in coverage:**

| | at-risk dollars | product source | cadence |
|---|---:|---|---|
| services | ~88% | `Census_EC_PxI` | **quinquennial** — 2017, 2022 |
| manufacturing | ~12% | `asm/value2017` | **annual** — 2018 onward |

That sharpens the earlier point that feasibility runs opposite to value. Services
hold the dollars but can only be interpolated between census years; manufacturing
holds a eighth of them but can be tracked year by year from reported data.

⚠️ The ASM product series carries aggregate rows the same way everything else
here does — the first row returned is `NAICS 31-33` with NAPCS `0000000000`.
Filter before aggregating; this class of defect has now appeared five times in
this work.

### Annual ASM: the data is there, the manufacturing concordance is not

Pulled `timeseries/asm/value2017` for 2018-2022.

| | |
|---|---|
| available | **2018, 2019, 2020, 2021** (17,269 rows in 2018; ~43,000 after) |
| **2022** | **absent** — ASM is not conducted in an Economic Census year, so EC 2022 fills exactly that gap |
| NAPCS codes | 2,791, of which **2,783 shared with `Census_EC_PxI`** |
| ASM value on shared codes | **100.0%** |

⚠️ **So the concordance transfers directly** — anything built on EC_PxI product
codes applies to the annual ASM series without a second mapping. That is what
makes annual manufacturing commodity output reachable.

⚠️ **Aggregates are 90.6% of ASM value** (NAICS shorter than six digits, or the
all-zero NAPCS code). Filtering is not optional; the detail is 13,372 rows and
about 4.95tn in 2021.

**But the dominant-industry seed does not work for manufacturing.** Summing
EC_PxI 2017 manufacturing products by commodity and scoring against published
`T007`:

| | |
|---|---:|
| built | 4.01 tn |
| published | 5.20 tn |
| weighted mean abs error | **27.6%** |
| within ±10% | **77 of 228** |

with pathological cases — `325211` plastics resin at ratio **0.034**, `324121`
asphalt at 0.030, `336111` automobiles at **2.89**.

**The failure is structural, not a tuning problem.** Route 2's whole premise is
that a manufactured commodity's output is *independent of who makes it* — the
manual's "the total for the product no matter where it is made". A rule that
assigns a product to whichever industry predominantly makes it therefore uses
exactly the wrong instrument, and it collapses cross-industry production into
the largest producer. It worked for services because a service is usually
produced by its own industry; manufacturing is the case it cannot handle.

**What manufacturing needs is a product → commodity map keyed on what the product
is**, over the 1,968 manufacturing products in EC_PxI 2017 — a larger job than
services' 202, validated the same way against published 2017 `T007`.

❌ **The mechanical route does not exist. Checked the docs.**
`timeseries/asm/product` works with the documented parameters (`for=us:1`, and
`PSCODE` really is NAICS-based — `311111` "Dog and cat food manufacturing"), but
it **stops at 2016**: 2,030 rows for 2016, empty for 2017 onward. It is the
legacy pre-2017-NAICS series and cannot reach the 2018+ window.
`asm/benchmark2017` and `asm/benchmark2022` are **industry-level only** — no
product dimension at all.

**So `asm/value2017` on NAPCS collection codes is the only annual manufacturing
product source, and the manufacturing concordance is unavoidable.** 1,968
products, validated against published 2017 `T007` the same way services were.

✅ **Side finding for the inventories work.** `asm/benchmark2017` and
`asm/benchmark2022` carry inventories **by stage** at industry level —
`INVFINB`/`INVFINE` finished goods, `INVWIPB`/`INVWIPE` work in process,
`INVMATB`/`INVMATE` materials, plus `CSTMTOT` and `VALADD`. That is precisely
what [#664](https://github.com/cornerstone-data/bedrock/issues/664) asks for —
per-industry stage shares from ASM rather than the durable/nondurable split —
and neither dataset is currently extracted.

### Can the trade concordance seed the manufacturing one? — tested, no

The idea is sound in principle: the NAPCS goods being *wholesaled or retailed*
should be the same goods being *produced*, so the trade concordance's 273
good→BEA judgements ought to transfer to manufacturing products.

**They do not.** Matching on token overlap after removing the wholesale/retail
lines that sit inside manufacturing industries:

| threshold | products | share of mfg value |
|---|---:|---:|
| Jaccard ≥ 0.5 | 22 | 0.9% |
| Jaccard ≥ 0.4 | 46 | **2.2%** |
| Jaccard ≥ 0.3 | 111 | 5.9% |

and the matches are unreliable at the useful end — *"Plastics and rubber
products contract manufacturing"* pairs with *"recyclable plastics and rubber"*,
mapping a manufactured good to `S00401` **scrap**.

**Why it fails.** The trade list is 273 coarse categories describing *what a
store sells*; manufacturing is 1,796 specific descriptions of *a manufacturing
operation*. They are different levels and different concepts, so the vocabularies
barely intersect even where the underlying good is the same.

⚠️ **A naive exact match looks far better than it is.** Matching stripped
descriptions returns 164 "hits" — but they are wholesale lines *inside*
manufacturing industries matching the trade list, not manufactured goods, and
they are 1.4% of value. Drop `sales of` lines before measuring anything here.

**A better mechanical seed exists, and it is BEA's own commodity names.**
Manufacturing product descriptions are structured "Manufacturing of X", which
matches the Supply table's `Commodity Description` far better than it matches
trade categories:

| threshold | products | share of mfg value |
|---|---:|---:|
| Jaccard ≥ 0.5 | 65 | 7.5% |
| Jaccard ≥ 0.4 | 107 | 11.7% |
| Jaccard ≥ 0.3 | 201 | **16.1%** |
| Jaccard ≥ 0.2 | 573 | 34.9% |

and the matches read correctly — *"Manufacturing of civilian aircraft"* → `336411`
Aircraft manufacturing, *"Manufacturing of ready-mix concrete"* → `327320` at an
exact token match.

**So the manufacturing concordance can be part-seeded from BEA commodity
descriptions** — roughly a sixth of value at a defensible threshold, a third if
0.2 is accepted with review — but the majority still needs judgement. The trade
concordance is not the shortcut.

### The NAICS index file as a term pool — tested, and it makes accuracy worse

The [2022 NAICS Index File](https://www.census.gov/naics/2022NAICS/2022_NAICS_Index_File.xlsx)
carries **20,398 index items** mapping specific product and activity terms to
6-digit NAICS, 10,164 of them in manufacturing across 346 industries — a far
richer vocabulary than the 231 BEA commodity names. Composed with NAICS→BEA it
is an obvious candidate for seeding the manufacturing concordance.

**On coverage it is roughly 3x better than BEA commodity names alone:**

| threshold | BEA names only | + NAICS index |
|---|---:|---:|
| Jaccard ≥ 0.5 | 7.5% | **20.5%** |
| ≥ 0.4 | 11.7% | 31.1% |
| ≥ 0.3 | 16.1% | 47.1% |
| ≥ 0.2 | 34.9% | **75.6%** |

⚠️ **But coverage is not accuracy, and on accuracy it is worse.** Scored the
same way as everything else — commodity output summed from products against
published 2017 `T007` — with the dominant-industry rule as the fallback so
coverage is 100% either way:

| seeding | wtd abs err | within ±25% |
|---|---:|---:|
| **dominant-industry only** | **28.2%** | **131 of 227** |
| hybrid, index ≥ 0.5 | 29.2% | 123 |
| hybrid, index ≥ 0.4 | 30.6% | 119 |
| hybrid, index ≥ 0.3 | 34.2% | 109 |
| hybrid, index ≥ 0.2 | 41.0% | 73 |

**The degradation is monotonic in how much the index is trusted.** Even at the
strictest threshold it loses to the simple rule. Token overlap against 10,164
index items reliably finds a lexically similar item, and lexical similarity is
not conceptual identity — whereas the dominant-industry rule, structurally wrong
as it is for manufacturing, at least reads actual production data.

⚠️ **Measuring the index seed alone would have flattered it.** On its own it
scored 50.2% error at ≥0.2, against the baseline's 28.2% — but that comparison is
confounded, because unmatched products are dropped and the level falls short
(2.98tn against 4.91tn published). The fallback is what makes the comparison
fair, and it is what reverses the apparent direction of the earlier coverage
result.

**Conclusion: lexical matching is not the route for manufacturing.** Three
vocabularies have now been tried — trade good names (2.2% of value), BEA
commodity descriptions (16.1%), and the full NAICS index (75.6% coverage but
worse accuracy) — and none improves on a rule that is itself only 28.2% accurate.
The manufacturing concordance needs either real judgement over the 1,796
products or a signal other than description text.

### The concordance is not the binding constraint

Census does publish a route from NAPCS collection codes to NAICS, via the 2012
Economic Census product codes — which *were* NAICS-keyed (`21111131` is NAICS
`211111` plus a product suffix):
[2017 NAPCS-Based Collection Code to 2012 Product Code](https://www2.census.gov/programs-surveys/economic-census/technical-documentation/napcs/2017_NAPCS-Based_Collection_Code_to_2012_Product_Code_20200312_no_highlight.xlsx),
8,237 rows. Composed with NAICS 2012 → BEA it resolves **3,775 NAPCS codes** and
covers **86.4% of manufacturing product value**.

**It does not beat the simple rule:**

| seeding | wtd abs err | ±25% | ±10% |
|---|---:|---:|---:|
| dominant-industry only | **28.2%** | 131/227 | 77 |
| Census concordance only | 29.9% | 114/193 | 71 |
| Census concordance, dominant fallback | 29.1% | 129/227 | 77 |

⚠️ **Four independent mappings now land in the same place: 28-30%.**
Dominant-industry, trade-good names, the 20,398-term NAICS index, and Census's
own official concordance. That consistency is the finding — **the error is not in
the mapping**. If it were, four unrelated methods would not converge on the same
number.

**What it points to instead is the adjustment ladder.** EC_PxI product value is
not BEA commodity output: it is a weighted sample of product shipments *before*
imputations, nonemployer and tax-misreporting coverage, removal of cost of
resales, and secondary in/out. That is a 20-30% wedge by construction, which is
exactly what all four methods measure. **Improving the concordance further will
not close it**, and effort should go to the ladder rather than to more mapping.

⚠️ **There is no NAPCS → NAICS concordance for 2017 or later**, and this is by
design — NAPCS is a demand-based classification explicitly *not* industry-of-origin
based. The 2012 product codes were the last NAICS-keyed product codes Census
published, so any NAPCS → NAICS route must go back through 2012 and inherit that
vintage's structure.

**Two files worth having anyway**, from
[`.../technical-documentation/napcs/`](https://www2.census.gov/programs-surveys/economic-census/technical-documentation/napcs/):

- [`2017_to_2022_NAPCS_Concordance_Final_08242022.xlsx`](https://www2.census.gov/programs-surveys/economic-census/technical-documentation/napcs/2017_to_2022_NAPCS_Concordance_Final_08242022.xlsx)
  — the official vintage bridge. This is what
  [#650](https://github.com/cornerstone-data/bedrock/issues/650) needs for the
  2017↔2022 description drift that `Census_EC_PxI.yaml` documents, in place of
  matching on description.
- `2017`/`2022_NAPCS-Based_Collection_Code_to_NAPCS_Trilateral_Product_Code.xlsx`
  — resolves the complaint recorded in `Census_EC_PxI.yaml` that "0 of the 620
  trade product codes appear in either official file". The collection codes are
  not the trilateral codes, and this is the published bridge between them.

### Rebalancing

Detail `q` estimated from primary data is then reconciled to BEA's published
summary `T007`. That is a level correction on a distribution primary data has
already shaped — not a change ratio pushed uniformly onto children — which is the
distinction that makes it non-flattening. Where no primary source exists, the
frozen mix supplies the shape and the same rebalance supplies the level.

**Decided: minimise change subject to the margin**, which is BEA's own
best-change rule and the same objective Step 5's balance solves.

⚠️ **With one margin and an entropic metric the two candidates coincide.**
Minimising Kullback–Leibler divergence from the seed subject to a single group
sum gives exactly `q'[i] = q[i] × T / Σq` — proportional scaling *is* the
minimum-change solution. The choice only acquires content in three cases:
simultaneous row and column margins (biproportional, which is Step 5 and not
4a), a quadratic rather than entropic metric, or **cells carrying different
confidence**.

**The third is the one that matters here, and it is the whole point of the
construction.** A commodity whose `q` comes from primary data should barely
move; one resting on the frozen mix should absorb the adjustment. Plain
proportional scaling treats them identically, which would spend the primary data
and then discard it. So "minimise change" must be **confidence-weighted**, and
that is precisely what distinguishes it from proportional here.

⚠️ **This lands on the mask layer, and on a gap.**
[`mask_layer_plan.md`](mask_layer_plan.md) already defines the needed concept —
a *fixed value*, "cell is nonzero, directly measured, must come out unchanged" —
and records that **neither balancing engine has it today**
([#588](https://github.com/cornerstone-data/bedrock/issues/588) Decision 2). So
this construction has a real dependency on that work rather than being
independent of it.

It also inherits the mask layer's governing constraint: **a source can be spent
on a cell or on a margin, never on both.** A SAS-derived `q` for `541700` is
either a fixed cell or a margin target, and Step 4a has to say which before Step
5's target set is settled.

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
