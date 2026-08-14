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

## 2026-08-14 — Follow-up, sent, unanswered

Five questions, numbered as sent. Questions 1 and 2 share a subject so they were
grouped; strict order of value would put pipeline second.

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
