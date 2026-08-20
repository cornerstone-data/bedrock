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

### Manufacturing: the concordance is not the constraint

The official Census concordance exists and works mechanically. The
[2017 NAPCS-Based Collection Code to 2012 Product Code](https://www2.census.gov/programs-surveys/economic-census/technical-documentation/napcs/2017_NAPCS-Based_Collection_Code_to_2012_Product_Code_20200312_no_highlight.xlsx)
file gives 8,237 NAPCS codes against 2012 Economic Census product codes, and
**2012 product codes are NAICS-based** (`21111131` is NAICS `211111` plus a
product suffix). So NAPCS → NAICS 2012 → BEA is a published crosswalk, not a
judgement: 3,775 NAPCS codes acquire a BEA target and **86.4% of manufacturing
product value maps**.

⚠️ **It scores no better than the crude rule.** Four independent approaches now
converge:

| mapping | wtd abs err |
|---|---:|
| dominant-industry (crude) | 28.2% |
| **official Census NAPCS → 2012 → NAICS → BEA** | **29.9%** |
| + NAICS index file, best threshold | 29.2% |
| trade-concordance transfer | rejected, 2.2% coverage |

**When four unrelated mappings land within two points of each other, the mapping
is not what is binding.**

**What is binding — evidence, in the order it was ruled out:**

- **Not a uniform level gap.** Built is 3.41tn against 4.55tn published, but
  rescaling by that single factor moves the error only 29.9% → **29.3%**. The
  dispersion is commodity-specific, not a ladder constant.
- **Not unmapped products.** Correlation between a commodity's shortfall and
  unmapped value in its own industry is **−0.370** — the wrong sign — and the
  worst commodities carry *less* unmapped value than the rest (0.7% against 1.0%).
- **Partly suppression, and this is actionable.** **53.2% of manufacturing PxI
  rows are suppressed and publish as zero.** The gradient is real: commodities
  whose industries are under 30% suppressed have median ratio **0.96**, against
  **0.76** at 50-70% suppression.

⚠️ **Every accuracy number above was measured on data with half its cells
zeroed.** `getFlowByActivity('Census_EC_PxI', 2017)` returns the *published*
FBA; suppression recovery lives in
[`estimate_suppressed_ec_pxi`](../../extract/census/Census_EC.py) and runs in the
FBS clean-function path, so none of this work has used it.

**So the next step is to apply suppression recovery and re-score, before any
further concordance effort.** The function exists and the plan already records it
taking published detail from 90.5% to 100.0% of control.

Suppression does not explain everything — `325412` pharmaceutical preparation at
ratio **0.08** (24 of 29 rows zero), `325211` plastics resin at 0.08 (34 of 49),
`334510` electromedical at 0.09 (26 of 34) are far more extreme than the bin
medians. Those three are the diagnostic targets once recovery is in.

### Suppression recovery: the manufacturing route works

Applying [`estimate_suppressed_ec_pxi`](../../extract/census/Census_EC.py) before
building `q`, with the official Census NAPCS → 2012 → NAICS → BEA concordance
unchanged:

| | published (suppressed) | **recovered** |
|---|---:|---:|
| commodities scored | 193 | 197 |
| built vs published | 3.41 vs 4.55 tn — **0.749** | 4.56 vs 4.78 tn — **0.954** |
| wtd abs error | 29.9% | **14.1%** |
| within ±25% | 114 | **165** |
| within ±10% | 71 | **134** |

**The error more than halves and the level closes to within 5%.** Manufacturing
product value goes from 4.019tn to 5.482tn and the zero-cell rate from **53.2%
to 4.7%**.

⚠️ **This was the binding constraint all along.** Four mappings converging at
28-30% looked like a concordance ceiling; it was the same suppression floor
underneath all of them. The lesson is general: **when unrelated methods converge
on the same error, suspect the input, not the method.**

**What is left is genuinely a concordance problem**, and it is now small and
legible — a handful of commodities where value lands on a neighbour:

| commodity | ratio | published |
|---|---:|---:|
| `331200` Steel product from purchased steel | **4.70** | 9bn |
| `336111` Automobile manufacturing | 2.89 | 33bn |
| `326290` Other rubber product | 2.03 | 17bn |
| `316000` Leather and allied product | **0.10** | 5bn |
| `331520` Nonferrous metal foundries | 0.15 | 12bn |
| `324190` Other petroleum and coal products | 0.18 | 28bn |

These pair up — steel-from-purchased-steel against iron and steel mills,
automobile against light truck — which is the signature of the **modal** NAPCS →
BEA rule assigning a whole code to one side of a pair. Splitting those codes
across their 2012 product codes rather than taking the mode is the next
refinement, and it is now worth doing because it is the residual rather than a
rounding error on a much larger one.

⚠️ **Next: the same treatment for annual ASM.** `asm/value2017` carries its own
suppression and its own aggregate rows, and it has no `'00'` all-industries
product total to recover against — the recovery here works by subtracting
published industries from that total. Whether an equivalent control exists in
ASM is the open question before the annual series can be built.

### The split, tested — marginal, and it exposed the real gap

Splitting each NAPCS code's value across every BEA commodity its 2012 product
codes imply, weighted by how many map to each, instead of taking the mode:

| rule | level | wtd abs err | ±25% | ±10% |
|---|---:|---:|---:|---:|
| modal | 0.954 | 14.1% | 165 | 134 |
| **split by 2012-code count** | 0.939 | **13.8%** | 169 | 136 |

**Marginal, and for a structural reason: only 2.1% of NAPCS codes are
multi-target** — 79 map to two BEA commodities, 2 to three, and 3,694 to exactly
one. There is almost nothing for a split to act on.

⚠️ **My "modal rule picking one side of a pair" diagnosis was wrong.**
`336111` automobile, `316000` leather and `331520` nonferrous foundries have
**zero** multi-target codes, so no split could have moved them.

**What is actually wrong: 34 manufacturing commodities have no built value at
all — 0.654tn, 12.0% of manufacturing `q`** — and 33 of them are never the
target of any NAPCS code. `331110` iron and steel mills gets **0.0** while its
pair `331200` gets **4.70x**; `325110` petrochemical 63.4bn, `336390` other motor
vehicle parts 61.2bn, `312200` tobacco 47.8bn and `336310` gasoline engines
38.2bn are all unbuilt.

**Two causes, and the larger one is ours:**

1. ⚠️ **Our NAICS 2012 → BEA crosswalk is incomplete.** Of 473 distinct
   manufacturing NAICS 2012 codes appearing in the Census concordance, **172 are
   absent from the `NAICS_2012_Code` column** of
   `NAICS_to_BEA_Crosswalk_2017.csv` — `311222`, `311223`, `311311`, `311312`,
   `311320`, `311330`, `311711`, `311712` and so on. Their product value cannot
   reach a BEA commodity at all. Only 200 distinct BEA commodities are reachable
   through the crosswalk as it stands. **This is the next thing to fix, and it is
   our data rather than Census's.**
2. Some commodities genuinely have no 2012 product codes in the Census
   concordance — `331110` and `336390` return zero rows. Those need a different
   source rather than a crosswalk repair.

### Closing the crosswalk gap — the codes are 2007 NAICS, not 2012

The 172 unreachable codes are not 2012 NAICS at all. **They are 2007 codes**:
`311222` Soybean Processing became `311224` in *2012*; `331316` → `331318`,
`332116` → `332119`, `333295` → `333242`. Census's "2012 product codes" embed
2007-vintage NAICS wherever a product line was carried forward unchanged.

That is why two obvious fixes both returned nothing:

- `NAICS_Year_Concordance.csv`'s **2012 column** resolves **0 of 172** — it has
  no rows for codes that were already gone by 2012.
- Census's official **[2012→2017 concordance](https://www.census.gov/naics/concordances/2012_to_2017_NAICS.xlsx)**
  is complete (1,069 six-digit rows) and still contains none of them, for the
  same reason.

✅ **The repo's own concordance has the answer in its `NAICS_2007_Code` column.**
Resolving 2007 → 2017 → BEA recovers **169 of 172**, and the results are
unambiguous and correct on inspection:

| 2007 | 2017 | BEA | |
|---|---|---|---|
| `311222`, `311223` | `311224` | `311224` | soybean and other oilseed processing |
| `311311`, `311312`, `311320`, `311330` | `311314`, `311351`, `311352` | `311300` | sugar and confectionery |
| `311711`, `311712` | `311710` | `311700` | seafood |
| `312210`, `312221` | `312230` | `312200` | tobacco |

| | prefix heuristic | **2007-vintage concordance** |
|---|---:|---:|
| level | 0.928 | **0.952** |
| wtd abs error | 13.8% | **11.5%** |
| within ±25% | 189/231 | **199/231** |
| within ±10% | 143 | **153** |
| **unbuilt commodities** | 10 | **0** |

✅ **The prefix heuristic is no longer needed.** Layering it on top of the
concordance changes nothing — identical to three decimal places — so it should be
dropped rather than kept as a fallback. A real vintage concordance strictly
dominates a structural inference, and keeping both would leave a rule in the code
that never fires and cannot be tested.

⚠️ **The lesson is about vintage, not about crosswalks.** Every failure in this
sequence came from assuming a code's vintage from the column it sits in. The
concordance file was right all along; it was being asked the wrong question.

### Motor vehicles need `U70205`, not the product data

`336111` automobile is built at 94.6bn against 32.8bn published while `336112`
light truck is 202.1bn against 215.4bn. The cause is visible in the codes: the
three NAPCS codes reaching `336111` are *"Manufacturing of complete passenger
vehicles"*, which **does not distinguish a car from an SUV or pickup**. No
concordance refinement can split what the source does not separate.

✅ **BEA publishes the split annually, and it is already extracted.**
`U70205` — Table 7.2.5U Motor Vehicle Output — is in `BEA_NIPA.yaml` today:
`A953RC` motor vehicle output 567.6bn, `A716RC` **truck output 466.8bn**,
`A133RC` **auto output 100.8bn**, plus `B148RC` domestic output of new autos
88.6bn. Autos are **17.8%** of motor vehicle output in 2017, which is why
assigning "complete passenger vehicles" to `336111` overstates it nearly
threefold.

That is also what BEA's own Table C1 says it uses for this industry — Wards
Intelligence unit production and J.D. Power average net cost — so an external
split here is the documented method, not a workaround.

### Annual ASM: suppression is fully recoverable — the control was misread

`asm/value2017` carries the same suppression as the Economic Census — **57.4% of
its 13,372 six-digit detail rows are zero** in 2021, with no flag distinguishing
a withheld cell from a true zero (`NAPCSDOL_IMP` is 0 everywhere, `NAPCSDOL_S` is
a standard error).

⚠️ **An earlier version of this section got the control wrong, and it is worth
recording how.** It compared published value by NAICS level using totals that
still contained the NAPCS `0000000000` all-products row — the very aggregate the
detail beside it sums to. Every level therefore read as roughly double its true
product value, and the sector rollup `31-33` read as *smaller* than its own
children, which prompted the conclusion that ASM had no usable top control. That
was the trap this document had already flagged for `31-33` on the NAICS axis,
walked into on the NAPCS axis one paragraph later. Corrected — product detail
only, `0000000000` excluded:

| year | `31-33` | 3-digit | 4-digit | 5-digit | 6-digit | control |
|---|---:|---:|---:|---:|---:|---:|
| 2018 | 3,652 | — | — | — | **5,694** | 5,891 |
| 2019 | 3,372 | 3,120 | 4,273 | **5,374** | 4,532 | 5,734 |
| 2020 | — | 2,945 | 3,869 | **4,868** | 4,231 | 5,204 |
| 2021 | 3,809 | 3,534 | 4,581 | **5,695** | 4,953 | 6,080 |

✅ **The control is the industry's all-products row, and it is complete.** The
`0000000000` line is published for **every industry at every NAICS level** — 360
of 360 six-digit industries in all four years — because a total across all
products discloses nothing about any one company. It sums to the *same* national
figure at each level (5,734bn in 2019), which is the proof that NAICS suppression
does not touch it. So ASM does have the same kind of control `Census_EC_PxI` has
in its `'00'` product total — **transposed**: EC controls a product across
industries, ASM controls an industry across products.

✅ **And the least-suppressed NAICS level is five-digit, not six.** Suppression
bites hardest where the cell is smallest, so six-digit detail is more withheld
(4,953bn in 2021) than the five-digit rollup containing it (5,695bn); going
coarser than five loses value again because ASM tabulates a product against fewer
industries there. **The industry axis is summed away** on the road to commodity
output, so its granularity is free to choose — taking the least-suppressed level
is a pure gain, not a trade against detail. 2018 publishes only six-digit rows
and is picked accordingly, which is why it needs no rollup at all.

⚠️ **`'31-33'` is five characters**, so it survives a `len == 5` filter alongside
real five-digit NAICS. Match `^\d+$` before selecting a level. This is the sixth
aggregate-row trap in this work, after `'TRADE '`, `GSLGE`, `T017`, PxI `'00'`
and the ASM all-zero NAPCS code — and the misread above is its seventh instance.

⚠️ **Recovery fixes the level and only approximates the mix.** Because the
control runs across products within an industry, the residual's split across
*commodities* is an equal-share guess. That is strictly weaker than the EC case,
where the control runs across industries within a product and the recovered mass
therefore lands in exactly the right commodity. Worth stating plainly: ASM
recovery is near-lossless for the manufacturing total and only indicative for any
single commodity's share of the residual.

### Annual manufacturing `q` from ASM — built, scored, and running as an FBS

Detail commodity output built from `asm/value2017` through
`napcs_to_bea_2017.csv`, scored against BEA's **published summary** Supply
`T007` for the same year, over the 19 manufacturing summary groups:

| year | six-digit, raw | | least-suppressed level | | **+ industry-total recovery** | |
|---|---:|---:|---:|---:|---:|---:|
| | level | wtd err | level | wtd err | level | wtd err |
| 2018 | 0.949 | 6.1% | 0.949 | 6.1% | **0.971** | **4.5%** |
| 2019 | 0.708 | 29.7% | 0.912 | 9.4% | **0.948** | **6.1%** |
| 2020 | 0.746 | 25.8% | 0.912 | 9.3% | **0.950** | **6.8%** |
| 2021 | 0.728 | 27.5% | 0.908 | 9.6% | **0.943** | **6.7%** |

Both moves matter and they are independent: choosing the level recovers most of
what six-digit suppression hides, and the industry-total residual closes the
rest. Together they take 2019–2021 from ~28% to under 7%.

⚠️ **This supersedes the five-digit-parent recovery previously recorded here**
(0.914/9.3% in 2019), which distributed each five-digit parent's residual over
its zero six-digit children. That route reaches only the five-digit total by
construction — 93.7% of control — where the industry product total reaches 100%.
It is also strictly more work for a worse answer, since building at five-digit
directly needs no distribution at all. Its one advantage is that it preserves the
commodity axis exactly; that is a real property, and it is the reason the
combined method's residual split is called a guess above rather than a
measurement.

⚠️ **These are scored at summary, over 19 groups — not comparable to the
11.3% the 2017 Economic Census build scores at detail over 239 commodities.**
Aggregation hides offsetting errors between detail children, so the summary
figure is the more forgiving of the two by construction. The honest statement is
that annual ASM reaches roughly 5–7% at summary granularity, and its detail
accuracy is **unmeasured** because no published detail exists for those years.

⚠️ **3.4% of ASM product value maps to no BEA commodity**, and the largest single
gap is `3361MV` motor vehicles at 0.74 — the NAPCS code is "complete passenger
vehicles" and carries no car/truck split, so no concordance can close it. It
needs NIPA table 7.2.5U (`U70205`), already extracted (#676).

**What this establishes:** manufacturing commodity output can be built annually
from reported product data, at a level within about 5–7% of BEA's own estimate,
for 2018–2021. That is the route the manual describes for manufacturing —
product data directly, independent of industry output — and it **now runs as a
method**: `transform/commodity_output/Commodity_output_manufacturing_<year>.yaml`,
producing 236 BEA detail commodities per year.


### Valuation: the built `q` is at basic prices — verified, not assumed

`T007` is basic value, so a product-based `q` has to be too. Census
`NAPCSDOL` is *value of shipments* — f.o.b. plant, net of discounts, **excluding
freight and excise taxes** — which should make it basic rather than producer. That
was worth testing rather than taking on trust, because the wedge is 7.0% of
manufacturing basic value (383bn of `TOP` in 2017) and would sit squarely inside
the residual we are trying to explain.

**The excise-heavy commodities settle it**, since `TOP` is most of their value:

| commodity | built | basic | basic + `TOP` | vs basic | vs producer |
|---|---:|---:|---:|---:|---:|
| `312200` Tobacco | 47.2 | 47.8 | 83.8 | **0.99** | 0.56 |
| `312140` Distilleries | 14.9 | 15.0 | 26.1 | **0.99** | 0.57 |
| `324110` Petroleum refineries | 496.9 | 478.0 | 577.0 | **1.04** | 0.86 |
| `312120` Breweries | 25.8 | 29.4 | 37.2 | 0.88 | 0.70 |
| **manufacturing total** | **5.182** | **5.432** | 5.815 | **0.954** | 0.891 |

Tobacco and distilleries carry excise near 75% of basic, and the build lands on
**0.99** against basic in both. It is basic value.

✅ **So the remaining gap is coverage and method, not valuation** — which also
means no basic↔producer conversion belongs anywhere in this construction. That
matters for the row margin too: Step 4a's industry side needs
`GO(basic) = GO(producer) - T00TOP + T00SUB`, but the commodity side needs no
conversion at all. **The two margins of `T007` arrive in different valuations
from their sources, and only one of them needs correcting.**

⚠️ `325412` pharmaceutical preparation remains at **0.52** even after suppression
recovery, against 0.99 for tobacco. That is not valuation; it is the largest
single unexplained commodity left in the manufacturing build.

### Mining has no annual product survey

⚠️ **The conclusion this section reaches is superseded** by the triage below,
which measured where mining's `q` actually moves. The source review here stands;
the inference that USGS and EIA are therefore the answer does not, because it
aims at `212`, the most stable piece. See "Mining — yes, but not where the plan
said".

Checked rather than assumed:

- **ASM is manufacturing only** — NAICS 31, 32, 33 and nothing else.
- **AIES** (`timeseries/aies/basic`, all sectors, 2023+) covers mining but has
  **no product dimension** — receipts, expenses, inventories, value added and
  payroll by NAICS only.
- The Economic Census carries mining products, but quinquennially.

So annual mining commodity output has to come from **USGS Mineral Yearbook
quantities and EIA**, which is what BEA's own Table C1 says it uses. That is a
different construction from manufacturing's — quantity times price rather than
reported product value — and the Mineral Yearbook is already an extract here.

✅ **Side finding: `aies/basic` carries inventories by stage for all sectors
annually** — `INV_E_FIN_VAL`, `INV_E_WIP_VAL`, `INV_E_MAT_VAL` — which is a
broader and more current source for
[#664](https://github.com/cornerstone-data/bedrock/issues/664) than the ASM
benchmark noted earlier.

### The remaining sectors, triaged before building — and two of them are skips

⚠️ **Superseded by the correction that follows this section.** The `q/x` metric
used here is observed in the published summary Supply table, so it does not
decide whether a source is needed. The structural findings below (diagonality,
who produces what) stand; every recommendation drawn from them does not.

Manufacturing is done; the queue was mining, agriculture, construction,
government and utilities. Rather than build each in turn, all five were measured
first against the only two questions that decide whether a source is needed:

1. **Is the commodity diagonal?** If a commodity comes almost entirely from its
   own industry *and* that industry makes almost nothing else, then `q ≈ x` and
   `x` is already published at detail for 1997-2024. No source is needed.
2. **Does `q/x` move?** If it does not, freezing the 2017 ratio is not an
   approximation to apologise for — it is the answer.

`q/x` by summary group from the published Supply tables, 2017-2024, with the
largest deviation from 2017 in either direction:

| group | `q/x` 2017 | max dev | reading |
|---|---:|---:|---|
| `111CA` farms | 0.989 | **0.004** | flat |
| `23` construction | 1.058 | **0.006** | flat |
| `GFGD` federal defense | 0.960 | 0.007 | flat |
| `GSLG` state/local general | 0.766 | 0.017 | flat |
| `211` oil and gas extraction | 0.832 | 0.023 | flat |
| `GSLE` state/local enterprises | 0.287 | 0.027 | moves ~9% relative |
| `212` mining, ex oil and gas | 0.829 | 0.043 | moves |
| `113FF` forestry, fishing | 1.130 | 0.045 | moves |
| `22` utilities | 1.320 | 0.047 | moves |
| `GFGN` federal nondefense | 0.935 | 0.059 | moves |
| `713` amusements, gambling | 1.439 | 0.099 | moves |
| **`213` support for mining** | 1.108 | **0.226** | **largest mover in the economy** |

#### Agriculture — skip

The 13 agriculture industries put **98.7%** of their output on their own
diagonal and **99.5%** inside the sector; 0.5% leaks out. Every crop and
livestock commodity has `row_diag = 1.000`. Combined with a `q/x` that moves
0.4% across eight years, `q ≈ 0.989 x` is not a stopgap — it is as good as the
published industry output it rests on. **No agricultural product source is
needed for Step 4a.** USDA data remains wanted elsewhere; it does nothing here.

The one caveat is `113FF` (forestry, fishing, support), which runs at 1.130 and
moves 0.045 — but it is a 60bn group and the movement is worth ~2bn.

#### Construction — skip

The construction sub-block is **100.0% diagonal**: all 12 industries produce
only their own commodity, and `q/x` moves 0.006 over eight years.

⚠️ `q > x` for every construction commodity, which looks like missing structure
and is not. The excess is **own-account construction by other sectors** —
mining industries producing `233240`, government producing `2332C0`/`2332D0` —
so it is a property of *their* columns, not of construction's. `233240` at
`q/x = 1.211` is the extreme case and is mining's own-account drilling
structures.

**`Census_VIP` does not help commodity output.** It is a value-put-in-place
series and the crosswalk already exists, but the construction block has no mix
to estimate. VIP earns its keep on levels and deflators, not here.

#### Mining — yes, but not where the plan said

Mining is 84.0% own-diagonal with **10.7% leaking outside the sector**, and it
splits three ways:

| piece | `q` | behaviour |
|---|---:|---|
| `211000` crude oil and gas | 204bn | `row_diag` 0.997, `q/x` dev 0.023 — nearly free |
| `212xxx` minerals | 82bn | `row_diag` 0.92-1.00, `q/x` dev 0.043 — nearly free |
| `213xxx` support activities | 118bn | `q/x` dev **0.226** — the real problem |

⚠️ **This overturns "annual mining commodity output has to come from USGS
Mineral Yearbook quantities and EIA", recorded above.** That aims at `212`,
which is the *smallest* and *second-most-stable* piece — the whole eight-year
movement there is worth about 4bn. The volatile piece is `213`, and the Mineral
Yearbook does not cover it at all.

`213` moves because it is a **make/buy split, not a quantity times price
problem**: oil and gas extractors and coal miners do drilling and support work
in-house, and that in-house work is recorded as secondary production of the
support commodity. `211000` alone contributes 12.9bn of it and `212100` a
further 5.6bn. The ratio peaks at **1.334 in 2022**, the post-COVID drilling
rebound. Freezing 2017 would carry a 20% error through the drilling cycle.

⚠️ Whether that split is *observable* annually is a separate question and should
not be assumed. Rig counts and well completions measure total drilling activity,
not who performed it. This needs checking before it is promised.

Also unresolved: `211000` ships 25.4bn of `324110` refined petroleum — lease
condensate and plant liquids leaving the extraction industry as a refinery
product.

#### Government — do the enterprises, skip general government

Government splits cleanly in two, and only one half is work.

**General government** (`GSLGO`, `GSLGE`, `GSLGH`, `S00500`, `S00600`) has
`row_diag = 1.000` **exactly** — nobody but government produces these
commodities — and a flat `q/x`. There is nothing to estimate.

**But `col_diag` is not 1**, and that is where the work is: government
industries produce large amounts of *other* sectors' commodities, and those land
squarely on the services priority list:

| industry | secondary output | lands on |
|---|---:|---|
| `GSLGH` state/local hospitals | 221.9bn | `622000` hospitals |
| `GSLGE` state/local education | 90.7bn / 47.1bn | `611A00` colleges / `541700` R&D |
| `S00500`+`S00600` federal | 36.7bn | `541700` R&D |
| `S00203` state/local enterprises | 68.0bn / 42.3bn / 50.6bn | `221300` water / `713200` gambling / `531HST`+`531ORE` housing |

`GSLGH` is the sharpest illustration: 312.9bn of industry output carrying only
68.7bn of its own commodity, `q/x = 0.220`. **Skipping government would strand
`221300`, `713200` and most of `622000`'s at-risk dollars** — three of the
commodities the priority list ranks highest.

#### Utilities — the same problem as government enterprises

| commodity | `q` | own industry | government enterprises |
|---|---:|---:|---:|
| `221100` electric power | 432bn | 351bn | **79bn** (`S00202` 63.4, `S00101` 15.7) |
| `221300` water, sewage | 80bn | **10bn** | **68bn** (`S00203`), 84.7% |
| `221200` natural gas distribution | 75bn | 60bn | 6.1bn (`S00203`) |

✅ **`221300` has `row_diag = 0.125`.** The water and sewage commodity is not
mainly produced by a water utility industry at all — it is produced by state and
local government. Solving `S00101`/`S00202`/`S00203` solves `221100`, `221300`,
`221200` and `713200` in one move, which is why utilities and government are one
piece of work rather than two.

**The sources follow the split:**

- **Electricity, and probably natural gas — EIA.**
  `EIA_ElectricPowerAnnual` is already extracted for 2014-2024 and carries
  `Investor-owned electric utilities` alongside `Total Electric Industry`, so
  the residual is the public-power share that `S00202`/`S00101` stand for.
  ⚠️ The residual is public power **plus cooperatives**, which are private, so
  it is an upper bound rather than the figure itself.
- **Water, sewage, transit, housing, lotteries — Census state and local
  government finances.** The Annual Survey of State and Local Government
  Finances reports utility revenue by function, which is the natural
  decomposition of `S00203`. **Not yet extracted here** — there is no government
  finance source in `extract/`, so this is a new FBA.


### ⚠️ Correction: the triage above measured the wrong quantity

The `q/x` triage was asked to decide which sectors need a data source. It cannot,
because **`q` at summary level is published**. Phase 1 is 2018-2024 and its gate
is "all source data already published"; 2025 is excluded from Phase 1 precisely
*because* it has no summary SUT. So for every Phase 1 year, BEA's summary Supply
table supplies `q` per summary group, and the user's own framing — "rebalancing
the detailed to the observed summary total" — makes that an observed control.

**A moving `q/x` is therefore not a problem to be sourced. It is data we are
handed.** `213`'s 0.226 swing, called "the largest mover in the economy" above,
is published in the annual summary Supply table for every year. Nothing needs to
estimate it.

What Step 4a actually owes is the **within-summary-group split of `q` across its
detail children**, and nothing else. That reframes every question:

- **21 summary groups have exactly one detail child**, carrying **6,676bn** of
  `q` at *zero* residual. `211` crude oil and gas, `GSLE` state/local
  enterprises, `GFGD` federal defense and `GFGN` federal nondefense are all in
  this set — their `q` is published, full stop.
- The second input is also published: **BEA's detail gross-output workbook
  `UGO305-A`** gives `x` per detail industry for every year, independently of
  the summary SUT, so using it is not circular.

#### The right screen: leverage x drift

Two things must both be true before an external mix source can earn anything:

`leverage`
    how far the group's **commodity** composition sits from its **industry**
    composition — the L1 gap between detail `x` shares and detail `q` shares in
    2017. Zero means industry output already answers the question and no mix
    data will ever be needed.
`drift`
    how far the child `x` shares move across 2017-2024, the proxy for how much
    the composition travels.

⚠️ **`exposure = q x leverage x drift` is wrong** - see the `213` check below.
Multiplying two small fractions understates the real spread by roughly two
orders of magnitude. The single-child findings and the observed-summary argument
in this section stand; the exposure column does not.

`exposure = q x leverage x drift`. The queued sectors:

| group | children | `q` bn | leverage | drift | **exposure** |
|---|---:|---:|---:|---:|---:|
| `22` utilities | 3 | 587 | 0.105 | 0.040 | **2.43bn** |
| `23` construction | 12 | 1,668 | 0.015 | 0.064 | **1.57bn** |
| `GSLG` state/local general | 3 | 1,737 | 0.098 | 0.007 | **1.27bn** |
| `111CA` farms | 10 | 401 | 0.020 | 0.066 | **0.53bn** |
| `212` minerals | 5 | 82 | 0.043 | 0.076 | **0.27bn** |
| `213` mining support | 2 | 118 | 0.011 | 0.081 | **0.11bn** |
| `113FF` forestry, fishing | 3 | 60 | 0.020 | 0.050 | **0.06bn** |
| `211`, `GSLE`, `GFGD`, `GFGN` | 1 each | 1,290 | — | — | **0** |

**Every sector in the queue is a skip.** All five together are about 4.7bn of
exposure — less than a quarter of the single largest item in the economy.

Economy-wide, total exposure is **91.2bn against 33,758bn of `q`, or 0.27%**, and
it concentrates where it always did:

| group | `q` bn | leverage | drift | exposure |
|---|---:|---:|---:|---:|
| `5412OP` legal, accounting, other professional | 1,974 | 0.236 | 0.045 | **21.0bn** |
| `532RL` rental and leasing | 425 | 0.117 | 0.130 | 6.4bn |
| `5415` computer systems design | 525 | 0.200 | 0.060 | 6.3bn |
| `81` other services | 830 | 0.102 | 0.059 | 5.0bn |
| `513` publishing, broadcasting | 705 | 0.109 | 0.063 | 4.8bn |
| `42` wholesale trade | 1,820 | 0.052 | 0.048 | 4.5bn |

#### What this does and does not overturn

✅ The original priority list was **right about the shape** — services carry the
mix work, `5412OP` above all — and the manufacturing build was worth doing
because it is the one regime with genuinely annual product data.

⚠️ It was **wrong about magnitude by roughly an order of magnitude**, because it
scored "secondary share x commodity size" without crediting the summary control.
`541700` R&D was listed at 366bn "at risk"; its whole summary group's exposure is
21bn.

⚠️ **Both mining recommendations in this document are void** — the original
"USGS Mineral Yearbook quantities and EIA", and the triage's correction of it to
`213` support activities. Mining's total exposure is 0.38bn. No mining source is
needed for Step 4a.

⚠️ **The government-enterprise and utilities recommendation is also void as
stated.** `S00203` is alone in `GSLE`, so its `q` is published and needs nothing;
`S00101`/`S00202` sit inside `GFE`, exposure 0.14bn. Utilities retains 2.43bn
because `221300` water really is produced by government rather than by a water
utility industry — that structure is real, it is just worth 2.4bn rather than
being a headline. **Neither `EIA_ElectricPowerAnnual` nor a new Census government
finance extract is justified by Step 4a.** They may be justified elsewhere.

⚠️ **Caveats on the screen itself.** `leverage` is in-sample on 2017, the only
year with a published detail block, so it measures the structural commodity/
industry gap rather than a held-out error. `drift` is industry-share movement
standing in for commodity-mix movement. `leverage x drift` is an order-of-
magnitude screen, not an error bound — which is enough when the readings are
0.1bn against 21bn, and would not be if they were close.


### `213` checked properly — and the exposure formula was wrong

Asked to verify `213` before acting on it. The structural checks pass:

- Summary group `213` has exactly two children, `213111` and `21311A`, both
  present as rows *and* columns of the 2017 detail block.
- The identity closes: detail `q` sums to 118,269 against a published summary
  `q(213)` of 118,268 — one unit apart on rounding.
- Both codes carry real values in `UGO305-A` for every year (2017: 27,011 and
  79,918). ⚠️ This needed checking because the screen used
  `.reindex(kids).fillna(0.0)`; a code absent from the workbook would have been
  silently scored against a fabricated zero, and `21311A` is exactly the kind of
  synthetic aggregate code that goes missing.

⚠️ **`UGO305-A` is industry output, not commodity output.** It supplies `x`, and
using it to split `q` is a *proxy* whose quality is the whole question. That is
what `leverage` was meant to measure, and the arithmetic built on it was wrong.

#### The formula was wrong

`exposure = q x leverage x drift` reported **0.11bn** for `213`. Multiplying two
small fractions produced a small number, but they do not compose that way. The
actual disagreement between two defensible allocations of the *same* published
`q(213)`:

| year | `x`-share of `213111` | A: by `x`-share | B: frozen 2017 `q`-share | gap |
|---|---:|---:|---:|---:|
| 2017 | 0.253 | 29,875 | 28,579 | 1,297 |
| 2021 | 0.174 | 15,951 | 22,111 | 6,159 |
| 2024 | 0.171 | 22,134 | 31,233 | **9,099** |

Drilling's industry share falls from 25.3% to 17.1% across the window; freezing
2017 `q`-shares denies that move entirely. The gap **widens every year** — the
opposite of two small independent factors multiplying to nothing.

#### The right method needs no source, and `213` still does not

**Method C — carry the 2017 commodity x industry mix onto published industry
output** — is the correct construction, and it is neither A nor B:

`q_c(y) = Σ_i (V17[c,i] / x17[i]) · x_i(y)`, rescaled to the published `q(g,y)`.

It is exact in 2017 by construction (10m off on rounding) and it credits the
industries **outside** the group that produce these commodities, which A and B
both miss. That matters here: `211000` produces 4,841 of `213111` and 8,085 of
`21311A`, and `212100` a further 5,639 — **18.6bn of in-house drilling and
support, 15.7% of the group**, and every one of those industries has published
annual `x`.

So the answer stands but for a better reason: **no external source is needed for
`213`.** Rig counts and EIA drilling data are not required, and the `q/x` swing
is published. What was wrong was the claimed precision.

**Residual uncertainty is single-digit billions, not 0.11bn**: method C sits
2.7-4.1bn from A and 3.3-6.2bn from B across 2018-2024, on a group of 118bn.

#### ⚠️ Every ranking in this document is a proxy, including the last one

Three metrics have now been tried on the same question and all three are proxies
with different failure modes:

| metric | failure mode |
|---|---|
| secondary share x commodity size | ignores the summary control — **overstates** ~10x |
| `leverage x drift` | multiplies two fractions — **understates**, badly |
| `\|C - A\|` method spread | measures how much worse a *worse* method is — **overstates** C |

None of them is the error, because **within-group commodity-mix error is not
measurable after 2017**: no published detail `q` exists to score against. The
rankings agree on *shape* — `5412OP` is first under all three, and mining,
agriculture and forestry are last under all three — and disagree on magnitude by
an order of magnitude or more. Shape is what they can be trusted for.

✅ **There is a real held-out test available and it has not been run.**
`USA_DETAIL_MUT_YEARS` is `[2007, 2012, 2017]`, so detail Make tables exist for
2007 and 2012. Carrying the **2012** detail mix onto 2017 published industry
output and scoring against the published 2017 detail block is a genuine
out-of-sample measurement of exactly this error, over a five-year horizon that
matches the nowcast's. It needs the 2012→2017 code concordance
(`Sector_Crosswalk_BEA_2012_Detail.csv` exists) and nothing else.

✅ **It has now been run** - see "The held-out test, run" below. Result: 0.94%.
The claims below held, and the ranking did not.

**That test should settle the priority list before any more sources are
scoped.** Until it runs, the defensible claims are: mining, agriculture,
forestry and construction rank last under every metric tried, and `5412OP` ranks
first under every metric tried.


### The held-out test, run — carrying a stale mix costs 0.94%

Every ranking above was a proxy scored in-sample on 2017. This is the
measurement: the **2012** benchmark Make table's commodity mix carried onto
**2017** published industry output, scored against the **2017** benchmark. The
horizon matches — Step 4a carries 2017 forward to 2018-2024, and 2012→2017 is
the same kind of five-year extrapolation.

Run with `uv run python -m bedrock.analysis.nowcasting.mix_holdout_test`.

| | L1 error | of `q` |
|---|---:|---:|
| **C: 2012 mix × 2017 industry output**, summary control applied | **325.3bn** | **0.94%** |
| A: industry output alone, same control | 1,152.7bn | 3.35% |
| C, no summary control | 348.0bn | 1.01% |
| A, no summary control | 1,331.5bn | 3.86% |

✅ **Two findings at once.** The mix does real work — 3.35% → 0.94%, a 3.6×
improvement over allocating group totals by industry output. And the residual
after five years of staleness is **under 1%**, which is the honest figure for
what Step 4a carries into 2024.

⚠️ The summary control is worth only 0.07pp here (1.01% → 0.94%). It pins group
totals, and group totals were mostly right already; what it cannot fix is the
split *inside* a group, which is where nearly all the error lives.

#### Where the error actually is — and it is not where the proxies said

| group | `q` bn | **stale-mix err** | no-mix err |
|---|---:|---:|---:|
| `521CI` banking, credit | 980 | **39.5** | 28.4 |
| `5415` computer systems design | 529 | **35.9** | 64.0 |
| `514` data processing, information | 316 | **30.3** | 75.1 |
| `561` administrative services | 900 | **29.1** | 43.6 |
| `325` chemicals | 742 | 16.9 | 15.7 |
| `513` publishing, broadcasting | 733 | 16.1 | 79.5 |
| `511` | 310 | 15.5 | 18.4 |
| `5412OP` legal, accounting, other professional | 1,981 | **13.0** | 244.1 |
| `GSLG` state/local general government | 1,737 | 9.8 | 186.9 |

⚠️ **`5412OP` ranked first under all three proxies and is eighth here, at 0.65%
of its own `q`.** Its no-mix error is 244bn, so the mix does enormous work — but
the *2012* mix still predicts 2017 well. **The proxies were measuring leverage
and calling it error.** High leverage means the mix matters; it says nothing
about whether the mix *moves*, and only the second one costs anything. The true
leaders — `521CI`, `5415`, `514`, `561` — appeared nowhere in any earlier
ranking.

#### Nine groups where a stale mix is worse than no mix at all

| group | stale-mix err | no-mix err |
|---|---:|---:|
| `521CI` | 39.5 | **28.4** |
| `325` | 16.9 | **15.7** |
| `333` | 9.50 | **9.41** |
| `487OS` | 8.2 | **4.2** |
| `322` | 4.5 | **1.9** |
| `23` construction | 1.99 | **0.00** |
| `512`, `337`, `4A0` | — | smaller |

✅ **`23` construction is the clean case and confirms the structural finding
independently**: its no-mix error is *exactly zero*, because the construction
sub-block is 100% diagonal, so industry output **is** commodity output. Carrying
a 2012 mix forward there does not help — it actively injects 1.99bn of error.
For these nine groups the rule is "use industry output, do not carry a mix".

#### The queued sectors, measured

| group | `q` bn | stale-mix err | of group |
|---|---:|---:|---:|
| `GSLG` | 1,737 | 9.80 | 0.6% |
| `22` utilities | 617 | 5.46 | 0.9% |
| `23` construction | 1,670 | 1.99 | 0.1% |
| `212` minerals | 84 | 0.84 | 1.0% |
| `111CA` farms | 391 | 0.51 | 0.1% |
| `113FF` | 60 | 0.32 | 0.5% |
| **`213` mining support** | 118 | **0.0046** | **0.004%** |

**`213` lands at $4.6m on a $118bn group.** The skip conclusion holds, now on a
measurement rather than a proxy. Mining, agriculture, forestry and construction
are confirmed skips; utilities and `GSLG` are sub-1% and rank tenth and
eleventh.

#### What the test cannot see

⚠️ **After redefinitions, producer prices.** BEA moved the 2012 benchmark off
static download into an interactive application, so the only 2012 detail Make
available is the redefined one in `CEDA6IO.xlsx`, paired with the 2017 redefined
table to keep both sides in one space. Economy-wide, redefinitions cut the
off-diagonal share from **9.54% to 5.53%**, 1.73×.

✅ That bias is far smaller than it looks *for this test*, because what is scored
is the within-group split and cross-group secondary production is absorbed by
the summary control either way. Within-group off-diagonal barely moves — the
largest gap is `5415` at 0.053, then `213` at 0.023, and most groups sit at
0.000.

⚠️ **`213` is the one queued sector the test cannot speak to on its own terms**:
its interesting secondary production, oil and gas extraction doing its own
drilling, is exactly what redefinitions reassign, so it reads 96% diagonal here
against 80% before redefinitions. The conclusion survives for a reason the test
*can* support — that production is **cross-group** (`211` into `213`), so
published summary `q(213)` absorbs it and only the two-way split remains.

⚠️ Other limits: the span is five years against Step 4a's seven to 2024, so this
understates the far end; 398 of 402 commodities are covered (99.85% of `q`); and
2012→2017 is a single draw, not a distribution — one benchmark pair cannot say
how much of the 0.94% is period-specific.


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
