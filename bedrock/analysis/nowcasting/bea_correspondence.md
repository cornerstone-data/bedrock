# BEA correspondence — method questions and answers

Durable record of method questions put to BEA and the answers received. These
answers are not in the published manuals, and several of them **contradict** the
manual reading, so the exchange is kept here rather than surviving only as
quoted fragments in the plan.

Cited from
[`margins_estimation_plan.md`](margins_estimation_plan.md) §Transportation and
§Negative margins are inventory timing.

---

## 2026-08-11 — Transportation margin allocation across commodities

**Question put:** 2026-08-05, recorded in commit `7488437` (branch
`faf_transport_margins`, closed with
[#627](https://github.com/cornerstone-data/bedrock/pull/627)). Three questions:
whether transport cost is distributed on tons as the manual reads, or on
ton-miles or a commodity-varying revenue rate; whether the distribution is
per-mode or modes are combined; and how local delivery and small-shipment cost
is handled, since our reconstruction under-allocated light high-value
commodities by a factor of five to eight.

**Answered by:** William Nicolls, Section Chief for distributive services,
Bureau of Economic Analysis. Cc: Harvey Davis, Edward Morgan, William (Billy)
Jolliff.

### What BEA said

On the **target of the exercise**:

> We really aren't looking for a specific value since that is already measured
> in margin output, we are simply trying to estimate the percentage of that
> margin output that is distributed to each commodity. That said, we prefer to
> have revenue data rather than relying on other measures.

On **rail**:

> For rail, we purchase the Freight Commodity Statistics from the American
> Association of Railroads which gives us very detailed revenue by product
> shipped by rail. The Surface Transportation Board also publishes this
> information along with the Commodity Revenue Stratification Report. We receive
> these data annually.

On **truck**:

> For truck, we use Service Annual Survey Table 8 which gives us revenue by
> product shipped but at a very aggregated level (basically at a sector level).
> We receive these data annually.

On **water and air**:

> For water and air, we do not have revenue by product so we estimate it by
> other means. We use a similar methodology for both air and water where we use
> ton-miles from the BTS/Census Commodity Flow Statistics. We do make
> adjustments to the data, though. We have a multiplier of 1, 2 or 3 based on
> the difficulty of transporting the commodity. For air, we only made one
> adjustment for animals and fish. For water, we made many adjustments based on
> the difficulty and expense of rearranging cargo after a delivery at port. For
> example, products like grain or oil that sit free in the cargo hold stay
> unadjusted. Products that don't sit free and are palletized, but are
> rearranged fairly easily get a multiplier of 2 and heavy machinery receive a
> multiplier of 3. These weights are only updated every 5 years.

On **combining modes**:

> We do not combine modes and we ignore multi-modal reported data since we
> cannot differentiate those in margin output.

On **local delivery and small shipments**:

> We do not handle local or small shipments separately. We are looking to
> distribute our margin data to commodities in order to estimate the total
> margin for each commodity and distribute it proportionally to each purchase of
> that commodity. High value commodities receive their portion of freight margin
> based on the weights estimated from the source data. The specific cost of a
> commodity doesn't figure in to the calculation, just the cost to ship it
> reported in the data.

### What it changed

1. **Retired the ton-mile allocator and closed
   [#627](https://github.com/cornerstone-data/bedrock/pull/627).** Rail and
   truck — 84% of `TRANS` — are allocated on revenue by product, not on any
   volume measure.
2. **Explained the 5–8× under-allocation.** Not a missing local-delivery term.
   Revenue per ton-mile is far higher for light high-value freight than for
   bulk, so a pure ton-mile allocator starves exactly those commodities.
3. **Confirmed the anchor-plus-growth construction** — BEA is estimating a
   percentage split, not a level.
4. **Confirmed the per-mode treatment** and that multi-modal data is discarded.
5. **Demoted the unpublished water multiplier table** from apparent blocker to
   2.3% of `TRANS`, and promoted truck's "Other goods" bucket to the largest
   unknown in the chain.

Note the multiplier weights ton-miles rather than replacing them, so water and
air use a weighted ton-mile share, `m_c · tonmiles_c ÷ Σ m_i · tonmiles_i` with
`m ∈ {1,2,3}`.

---

## 2026-08-17 — Follow-up answered: truck's "other goods", pipeline, the multipliers

**Question put:** 2026-08-14, five questions, numbered as sent. Questions 1 and 2
share a subject so they were grouped; strict order of value would put pipeline
second.

**Answered by:** William Nicolls, BEA, 2026-08-17. Cc: Harvey Davis, Edward
Morgan, William (Billy) Jolliff.

⚠️ **Four of the five are answered; question 4 is not.** Whether 2023 is
continuous with the earlier series or rebased, and how BEA handles a year like
2024 before the next AIES release, went unaddressed and remains the open item.

1. **How is SAS Table 8's "Other goods" distributed?** It is 32–34% of Total
   Motor Carrier Revenue in every year 2015–2023, and truck is ~68% of `TRANS`,
   so it covers ~22% of the entire transport margin with no commodity identity.
   Also asked: whether Table 8 is the right table (BEA described it as "basically
   at a sector level", but its groups are commodity groups), and whether BEA uses
   the eleven published groups or a more detailed tabulation.
2. **Is the Table 8 group → I-O commodity concordance published, or internal**
   like the NAPCS product-line concordance? The eleven groups are a bespoke
   Census taxonomy, neither SCTG nor NAICS.
3. **What is the pipeline (`486000`) allocation basis?** 11.9% of the 2017
   transport margin, more than water and air combined, and not covered by the
   first reply.
4. **Is 2023 continuous with the earlier series, or rebased?** Commodity shares
   move ~4× more across the SAS → AIES consolidation than in a normal year (mean
   1.4pp against 0.3pp; "Used household and office goods" 4.4% → 6.9%). Also
   asked how BEA handles a year like 2024 before the next AIES release.
5. **The water and air difficulty multiplier table** — the 1/2/3 assignment by
   commodity, or the rule at a finer grain than the examples given.

Deliberately **not** asked: access to the AAR Freight Commodity Statistics. It
is a commercial subscription BEA cannot grant, and the STB Commodity Revenue
Stratification Report they named looks like a public substitute — to be tested
before going back to them.

### What BEA said

On **"Other goods"** (question 1):

> We do not use the "other" commodity from SAS Table 8 since we have no
> information on what commodities it contains. Distributing it pro rata to the
> other 10 would not change the result since we are creating weights with the
> data to distribute our truck margins rather than explicitly using the values
> from SAS table 8.

On **which table, and the "sector" wording** (question 1, second part):

> Sorry, using the term "sector" was confusing. At BEA, we use (mostly) the same
> codes (NAICS) for both commodities and industries and refer to the level of
> detail for each as sector, summary and detail to distinguish levels of
> aggregation. When I used the term "sector," I was just referring to a very
> aggregated level of detail rather than relating the data to industries. The
> data are commodity data in SAS Table 8 and we do use the published data.

On the **group → I-O concordance** (question 2):

> The SAS group to IO items concordance is not published. It is internal to our
> database. We remap it every 5 years and we are in that process now.

On **pipeline** (question 3):

> Pipeline is a bit different from our other transportation margins in that
> there is no outside source that we need to tell us where to distribute the
> pipeline margins. The margin values, like the other transportation margins,
> come from Census and there are 4 pipeline margine items: crude oil pipe,
> natural gas pipe, refined petroleum pipe, and pipeline, not elsewhere
> classified (NEC). Natural gas pipelines margins go to natural gas commodity.
> Crude pipelines margins go to any crude oil commodity. Those two are the
> clearest. Refined petroleum margins go to refined fuels such as gasoline, jet
> fuel, kerosene and other refined oils and waxes. Pipeline, NEC margins are
> applied to dyes, pigments, toners, ammonia, Urea and like products that would
> be transported through a pipeline. These margins are all distributed
> proportionally to the commodities to which they are assigned.

On the **difficulty multipliers** (question 5):

> I can't share the table with you, but I can give you a push in the right
> direction. Air is simple, everything except animal is a 1 (animals is 3). For
> water, we see motorized vehicles and transport as the most difficult (highest
> multiplier). For everything else, if it would be palletized or put in a
> container, it would receive a 2 and if it sits loose on board, it receives a 1.
> That should get you pretty close.

### What it changed

1. **Closed the largest unknown in the transport chain, and cheaply.** "Other
   goods" — ~22% of all `TRANS` — is simply **not used**. BEA builds *weights*
   from the ten identified groups rather than spending Table 8's values, so
   dropping "other" and renormalising over the ten is not an approximation of
   BEA's method, it **is** BEA's method. The alternative treatment we would
   otherwise have had to choose between is confirmed equivalent: pro rata across
   the ten "would not change the result".
2. **Confirmed the source reading.** Table 8 is the right table, the eleven
   published groups are what BEA uses, there is no finer internal tabulation, and
   the data are commodity data. "Sector" was aggregation-level language, not an
   industry/commodity distinction — so the doubt recorded on 2026-08-14 is
   resolved in favour of what we already built.
3. **Made pipeline fully reproducible**, from nothing. 11.9% of `TRANS` moves
   from "no method stated" to a complete deterministic rule: four Census margin
   items, each with a named destination set, distributed proportionally. No
   external source is needed for it at all.
4. **Made the multipliers reproducible without the table.** Air is 1 everywhere
   except animals at 3. Water is 3 for motorized vehicles and transport, 2 for
   palletized or containerized cargo, 1 for cargo loose in the hold. Note this
   fixes air's value, which the first reply left unstated, and adds motorized
   vehicles to the first reply's "heavy machinery" at the top of the water scale.
5. **Left question 4 open**, which is now the only unanswered method question:
   the 2023 AIES splice moves commodity shares ~4× a normal year, and nothing
   here says whether that is real or a rebasing.

With this, **every mode of `TRANS` has a stated method**. What remains is not
method but sourcing: the group → I-O concordance BEA will not publish, rail's
revenue source, and the 2023 splice.

---

## 2026-08-19 — Follow-up, sent, unanswered: the within-group weight, and rebalancing

**Question put:** 2026-08-19, to William Nicolls. Two questions, both about truck,
which is 67.8% of `TRANS` and the only mode whose commodity detail we cannot
observe.

1. **What weights allocate a SAS group's margin among the specific commodities
   inside it?** BEA has an internal crosswalk from the SAS groups to Detail I-O
   commodities, but the crosswalk only says *which* commodities are in a group,
   not how the group's margin divides among them. Asked in the alternative:
   **are certain commodities manually excluded from certain modes?**
2. **Is there a rebalancing step at the end** - RAS-like or otherwise - that
   forces the commodity totals to equal the total transport margin?

**The evidence sent**, our 2017 results on BEA's own described methods, weighting
within a group by each commodity's total published `TRANS`:

| commodity | published | other 4 modes | truck | overshoot | truck % | SAS group |
|---|---:|---:|---:|---:|---:|---|
| Oil and gas extraction | 47,608 | 42,741 | 12,204 | −7,337 | 26% | Coal & petroleum |
| Grain farming | 13,602 | 6,227 | 11,920 | −4,545 | **88%** | Grains/alcohol/tobacco |
| Coal mining | 13,437 | **13,832** | 3,445 | −3,840 | 26% | Coal & petroleum |
| Other nonmetallic mineral mining | 7,556 | 4,276 | 6,518 | −3,239 | **86%** | Stone/minerals/ores |
| Iron and steel mills | 6,403 | 2,429 | 6,687 | −2,713 | **104%** | Base metal & machinery |
| Other basic organic chemicals | 5,489 | 4,514 | 3,581 | −2,606 | 65% | Pharma & chemical |
| Automobile manufacturing | 4,838 | 4,327 | 2,957 | −2,446 | 61% | Electronic/vehicles |
| Sawmills | 3,237 | 1,568 | 2,918 | −1,249 | **90%** | Wood/textiles |
| Paperboard mills | 2,782 | 1,498 | 2,508 | −1,224 | **90%** | Wood/textiles |

Two failure shapes are visible in that table and they need different answers.
Coal is over-allocated **before truck is added at all** - the other four modes
already exceed the published column - which is a question about the other modes
or about the column. Iron and steel is the opposite: truck alone claims 104% of
everything published, leaving nothing for rail, which certainly hauls steel.

### What turns on the answers

**Question 1 governs 67.8% of `TRANS`.** Truck's commodity detail comes almost
entirely from the within-group weight: ten identified groups span 258 receiving
commodities, so Table 8 fixes the group totals and the weight does everything
below that. Our default - each commodity's total published `TRANS` - is blind to
which modes already occupy a commodity, which is exactly why rail-heavy
commodities overshoot. An "excluded commodities" answer would work as well as a
weight, and would be easier to reproduce.

⚠️ **One group cannot hold its share on our mapping, so this may be upstream of
the weight.** "Base metal and machinery" demands 104% of the entire published
`TRANS` of every commodity we place in it, before any other mode takes a share,
and four more groups sit at 86-90%. No within-group weight can fix that: either
our group→commodity mapping is too narrow, or a group's revenue share is not
meant to carry that share of the whole truck margin.

**Question 2 decides whether a joint solve is ours to build.** Feasibility is
proven - every subset of modes has non-negative slack against the published
column, tested exactly over all 32 subsets - so a reconciled answer exists. What
is unknown is whether BEA reaches it by construction or by a final balancing
step. If they rebalance, we should rebalance the same way; if they do not, their
within-group rule must avoid the collision in a way ours does not, and that rule
is the thing to copy.

⚠️ **One error in the message as sent:** it says "8 commodity groups". SAS Table 8
publishes **eleven**, of which ten carry a commodity identity and "Other goods" is
the eleventh that BEA discards. Worth watching for in the reply, in case the
answer is framed around a different set.

---

## 2025-06-24 — Negative margins in the 2017 Margins table

**Answered by:** William (Billy) Jolliff, BEA.

Negative entries in the margin columns are **inventory timing**, not errors.
Margin is booked when inventory *builds*; a later drawdown carries an offsetting
negative, because the margin was already counted in the earlier period while
current-period commodity output is unchanged by the draw. All 31 negative rows
in the 2017 table are buyer `F03000`, totalling −8,076 million.

They must never be clipped, floored or absoluted, and rates must not be derived
from `F03000` rows — a negative margin over a change-in-inventories base is a
timing correction, not a rate. The verbatim quote and the three consequences are
in the plan, §Negative margins are inventory timing — never clip them.
