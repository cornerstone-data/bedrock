# Plan — estimating the Margins table and the Supply margin columns

Step 4c of [`plan.md`](plan.md), issue
[#571](https://github.com/cornerstone-data/bedrock/issues/571). Covers the
transaction-level Margins dataset, which is also the Step 6d deliverable and
Step 6b's PUR→PRO input — **the highest fan-out of anything unbuilt.**

Method statements below are from the **BEA IO manual (2009), chapter 8** and the
2025 BEA correspondence with Billy Jolliff (National Economic Accounts), each
checked against the published 2017 detail tables.

---

## The object

`(buyer, commodity) × {Producers' Value, Transportation, Wholesale, Retail,
Purchasers' Value}`, BEA 2017 detail, buyers spanning industries *and*
final-demand codes. 56,619 rows in 2017, 421 buyers × 404 commodities.

The Supply columns are its aggregate, **with a tax term on the trade side**:

```
Σ_buyers ( Wholesale + Retail )  =  TRADE[c] + TOP[c]
Σ_buyers   Transportation        =  TRANS[c]
```

## The three margins are three different problems

Measured on the 2017 table:

| | transactions | commodities | PCE share | rate, PCE | rate, non-PCE |
|---|---:|---:|---:|---:|---:|
| Wholesale | 17,027 (30.1%) | 253 | 29.9% | 0.134 | 0.158 |
| **Retail** | **1,933 (3.4%)** | 188 | **87.5%** | **0.364** | **0.164** |
| Transportation | 12,252 (21.6%) | 257 | 20.4% | 0.020 | 0.039 |

Two notes on reading that table, both settled by Phase 1. The rate columns are
**margin ÷ purchasers' value** — a share of what the buyer pays, not the
cascading rate BEA computes and the build carries. And the counts are off the
*after*-redefinitions table; the anchor is now the **before**-redefinitions one
(§Phase 1), where the same counts are 17,419 / 2,106 / 12,516 transactions over
254 / 188 / 258 commodities. The structure the table describes is unchanged
either way.

Three consequences:

- **The buyer dimension is essentially a retail phenomenon.** Retail's PCE rate
  is **2.2×** its non-PCE rate; wholesale's differ by 1.2× and in the *opposite*
  direction. Step 6b's warning that a commodity-average rate misallocates is
  acute for retail and nearly harmless for wholesale.
- **Retail is sparse and concentrated** — 3.4% of transactions, 87.5% of its
  value in one buyer. Effort spent here is well targeted.
- Wholesale is 8.8× more widely distributed than retail, exactly as the manual
  says ("retail margins are distributed to only those transactions that move
  through retail establishments").

## What BEA does, and what of it we can reproduce

### Transportation

Total cost by mode → commodities, on the share of transport receipts from
moving that commodity, from the **Commodity Flow Survey**. CFS reports ton-miles
and value but *not revenue by commodity*, so BEA assumes **revenue per ton is
constant across commodities**.

That assumption is visible in the 2017 data — `TRANS`/`T013` orders almost
perfectly by weight-per-dollar:

| highest | | lowest | |
|---|---:|---|---:|
| Coal mining | 49.1% | Motion picture and video | 0.04% |
| Other nonmetallic mineral mining | 45.4% | Sound recording | 0.16% |
| Scrap | 40.6% | Magnetic/optical media | 0.82% |
| Cement | 20.2% | Ship building | 0.89% |

Ship building at 0.89% is the confirmation: heavy, but self-propelled, so it
consumes no freight. **Any rebuild must be ton-weighted, not value-weighted** —
inverting that would swap coal and pharmaceuticals.

**Reproducible, and improvable.** Use **FAF** rather than CFS: it is modelled to
annual years where CFS is quinquennial, and it publishes **ton-miles**, a better
freight-cost proxy than tons since cost scales with distance.

⚠️ **But volume does not predict the margin, and the fit test settles it against
the method rather than for it.** Measured on 2017, allocating each mode's margin
across commodities on that mode's share of the measure — BEA's own construction
— and scored against the published `TRANS` at SCTG level:

| source | measure | pearson | total abs share error |
|---|---|---:|---:|
| **FAF** | **ton-miles** | **0.370** | **68.2pp** |
| FAF | value | 0.310 | 69.3pp |
| FAF | tons | 0.099 | 92.9pp |
| CFS PUF | ton-miles | 0.099 | 79.9pp |
| CFS PUF | value | 0.122 | 76.7pp |
| CFS PUF | tons | 0.011 | 99.8pp |

Three findings, in order of how much they change the build:

**Ton-miles beats tons decisively, so the manual's assumption is the weaker
one.** Constant revenue per ton is a line-haul assumption, and `TRANS` is the
delivered margin. The residuals say so plainly: weight-based allocation
over-pays bulk (coal 35,609 predicted against 13,432 published, cereal grains
34,666 against 13,604) and starves light high-value goods (furniture 2,502
against 21,035, machinery 4,810 against 19,931, electronics 4,325 against
16,049). Furniture really does cost roughly eight times more per ton-mile to
deliver than coal.

**The CFS public use file is disqualified on scope, not on measure.** It carries
2,979 of FAF's 5,436 billion ton-miles, and the missing 45% is not spread
evenly: **crude petroleum, the single largest `TRANS` commodity at 47,613, has
zero CFS coverage**, live animals 8%, cereal grains 51%. CFS excludes farms,
fisheries, most construction and the domestic legs of imports — which is exactly
what FAF adds back. (This rules out the PUF for *our* use. BEA works from the
confidential CFS microdata with its own adjustments, so it says nothing about
BEA's method.)

**So do not allocate margin *by* ton-miles — use ton-miles to *move* the 2017
allocation.** This is the same anchor-and-move shape as everything else in this
plan, and it makes the weak cross-sectional fit irrelevant rather than
blocking:

```
rate[c]       = TRANS_2017[c] ÷ ton-miles_2017[c]
TRANS_year[c] = rate[c] × ton-miles_year[c], controlled to the annual total
```

**And it reduces further than it looks.** Ton-miles are published per SCTG, not
per commodity, so `ton-miles[c] = share[c] × ton-miles[sctg(c)]`. Hold `share[c]`
constant — there is no annual source that would move it — and it cancels:

```
TRANS_year[c] = TRANS_2017[c] × ton-miles_year[sctg] ÷ ton-miles_2017[sctg]
```

So the whole construction is **each commodity's published 2017 `TRANS`, moved by
the ton-mile growth of its SCTG group, controlled to the annual total**. Exact in
2017 by construction, so BEA's own commodity allocation is inherited rather than
approximated, and the commodity cost structure a volume weighting discards is
kept — furniture stays eight times dearer per ton-mile than coal because the
2017 table says so, not because a model inferred it.

Three things that follow, all of which shrink the work:

- **No balancing step.** An earlier draft fitted `margin_2017[c,m]` per commodity
  *and mode* biproportionally, which needs a RAS. The mode dimension never
  appears in the output — only in the derivation — so it can go. The transport
  column is an **input to** Step 5's RAS, not a product of one, and the choice of
  RAS implementation reverts to [#588](https://github.com/cornerstone-data/bedrock/issues/588)
  where it belongs.
- **No SCTG → commodity allocation, and so no 4a/4b dependency here.** Fixed
  within-SCTG shares cancel, so the interim-supply weighting BEA uses to split a
  product line across commodities is not needed for *this* column. It is still
  needed for the trade margins.
- **What is given up is modal-shift sensitivity.** A per-commodity rate cannot
  see freight moving from rail to truck, which raises cost per ton-mile. That is
  second order, and the mode-level form is the natural refinement to revisit once
  a balancer exists — the derivation is recorded above for that reason.

📬 **Asked of BEA, 2026-08-09, reply outstanding.** Whether the weighting is
tons or has moved to ton-miles or a commodity-varying revenue rate; whether the
distribution runs within each mode separately; how local delivery and
small-shipment cost are handled; and whether the commodity allocation still
comes from CFS directly or FAF now plays a part. **Nothing is blocked on the
answer** — the construction above anchors on the published `TRANS` rather than
reconstructing it, so it stands whichever way the weighting question falls.
What each answer would change: *tons confirmed* leaves ton-miles a deliberate,
documented deviation affecting only the mode-level refinement; *a
commodity-varying revenue rate* vindicates the anchor directly, since that
variation is what the 2017 implied rates preserve and a volume weighting
discards; *local delivery included in the margin* explains the 5-8x residual on
light high-value goods and says whether modal shift is the right second-order
correction; *FAF used as an input* would mean BEA moves with the same annual
source we do.

### Wholesale

Economic Census **product-line sales by kind of business (KB)**; the margin rate
of the *primary* wholesaler of a good is applied to that product line wherever
sold (the "most appropriate kind of business", MAKB, rate); scaled within each
KB to the output control; then product-line margins distributed to I-O
commodities **using interim supply — output plus imports — as weights**; then to
items using interim supply less transactions not receiving wholesale.

Note the treatment split, which explains why wholesalers give up 90–99% of
output rather than 100%: merchant wholesaler output, own-account agent/broker
output and manufacturers' sales *branch* expenses are margin, while **commission
sales and manufacturers' sales *office* expenses are services sold directly**.

### Retail

Same first two steps, then it diverges: product-line margins aggregate into
**retail categories (RCCs)**, which map to a hand-selected set of transactions
"moving through retail establishments"; the margin is split **PCE vs non-PCE**
using Economic Census **sales by class of customer**; and **two rates** are
computed per retail category.

**The manual's worked example is real-world calibrated** — table 8.15 gives
category X1 a PCE rate of .338 and non-PCE .163, against 0.364 / 0.164 measured
across the whole 2017 table. That is close enough to treat the two-rate
structure as the real mechanism, not an illustration.

**So the unpublished input may not be needed.** BEA's PCE/non-PCE split comes
from class-of-customer data we do not have — but the resulting rates are
readable straight off the 2017 Margins file.

## The residual decomposes `TOP` into producer-level and trade-level tax

The `Σ(W+R) = TRADE + TOP` identity leaves −1.29%. That residual is not noise —
it follows from where each tax sits:

> *"The nonmargin taxes (excise taxes) are embedded in the Producers value field
> of the Margins table. These would be captured in the 'Tax on products' field
> in the supply table. Your distinction is correct about sales tax vs excise
> taxes."* — B. Jolliff, BEA, 2025-06-16

Sales tax is inside the margin columns; excise is inside Producers' Value. Both
land in `TOP`. So algebraically the residual is the **producer-level** share:

```
producer_level_tax[c] = TRADE[c] + TOP[c] − Σ_buyers ( Wholesale + Retail )
```

⚠️ **Only meaningful for commodities that bear trade margin.** For a service with
no wholesale or retail, `Σ(W+R)` and `TRADE` are both zero and the expression
degenerates to `TOP` — which is why restaurants, electric power, insurance and
legal services otherwise appear at a spurious 100%. Restricted to the **202
margin-bearing commodities with `TOP` > 100**, producer-level tax is **9.9% of
`TOP`**, and it lands exactly where the tax law puts it:

| commodity | producer-level share of `TOP` | why |
|---|---:|---|
| Oil and gas extraction | 90.4% | severance tax, levied on the producer |
| Coal mining | 88.1% | severance tax |
| Tobacco manufacturing | 37.0% | federal excise, paid by the manufacturer |
| Distilleries | 37.0% | federal excise |
| Breweries | 35.4% | federal excise |
| Wineries | 15.9% | federal excise |
| **Petroleum refineries** | **0.2%** | **motor fuel tax is levied on distributors and collected at the pump — it behaves like a sales tax and sits in the margin columns** |

Petroleum is the case that looks anomalous and is not: its `TOP` of 99,047 is
almost entirely trade-level. The alcohol/tobacco cluster at 35–37% is the
manufacturer-paid excise the correspondence describes.

**Use for the build:** this gives an excise-vs-sales split per commodity from
published data alone, with no external tax source — the input needed if margins
are ever required in basic prices.

## The ratio is not a share — margin is added, not carved out

`TRADE`/`T013` exceeds 1 for **21 commodities**, carrying **21.1% of all
positive `TRADE`**. That is not an error, and it is worth stating because it
looks like one.

Margin is **additive**: `T016` (purchaser) `= T013` (basic) `+ T014` (margins)
`+ T015` (taxes). So a commodity's trade margin can be any multiple of its basic
value. Checked both ways:

| | commodities > 1 |
|---|---:|
| `TRADE` / `T013` | **21** |
| `TRADE` / `T016` | **0** (max 0.715) |

Apparel worked through: basic 96,779 + margins 182,974 + taxes 31,848 =
purchaser 311,601. **Basic value is 31% of what the buyer pays**, so
`TRADE`/`T013` = 1.84 while `TRADE`/`T016` = 0.57. A $20 shirt with $7 of goods
in it is exactly this.

The 21 are the high-markup consumer goods the intuition predicts — apparel,
leather, jewellery, dolls and toys, sporting goods, carpets, curtains, breweries,
distilleries, wineries. **`S00402` used and secondhand goods is the extreme at
16.02**, and instructively so: `T007` is zero because used goods have no
production, so the commodity is almost pure dealer margin.

**Two consequences for the build.** `T013` remains the correct *allocation*
base — three independent statements say so, and none of them claims the result
is a share. But a rate expressed on `T013` is unbounded, so **do not use "rate >
1" as a validation rule**; validate on `T016`, where the bound is real.

## The negative result that shapes the build

BEA computes **one rate per item**, uniform across all transactions receiving
that margin (manual table 8.10: *"the margin rate (0.10) is the same for all
items receiving margin"*). It is tempting to model one rate per commodity.

**Tested, and it does not hold at the published level.** Rate dispersion within
each commodity, on BEA's own cascading bases:

| | commodities | median CV | CV < 0.01 |
|---|---:|---:|---:|
| Wholesale / (Producers' + Transport) | 249 | **0.238** | 0 (0%) |
| Retail / (Producers' + Transport + Wholesale) | 56 | **0.696** | 0 (0%) |

Rebuilt in Phase 1 on the before-redefinitions anchor, over every commodity with
more than one rate-bearing transaction: wholesale **0.218** over 253 commodities
with **none** under 0.01, retail **0.776** over 131 with **one**, transportation
**0.226** over 257 with **three**. Four commodities out of 641 are effectively
uniform, which does not support a per-commodity rate anywhere else.

The reason is that BEA's "item" is **finer than the published commodity**, so
several item rates mix inside every published commodity, and the item level is
not recoverable from published data.

**Therefore: carry rates per (buyer, commodity, margin type) as
[#571](https://github.com/cornerstone-data/bedrock/issues/571) already
specifies.** Collapsing to a per-commodity rate discards real, measured
variation, and it cannot be justified from the source method.

## Sources — what exists

| Piece | Where | State |
|---|---|---|
| `BTS_FAF` extractor | **bedrock** `extract/bts/` | ✅ **Ported** (FAF5.7.1, 2017-2024). Emits tons, ton-miles and value |
| `Transport_Margins_2017.yaml` | flowsa **`margins`** branch | Not yet ported. Selects `Unit: ton-miles` and redistributes the non-primary modes proportionally |
| `NAICS_Crosswalk_FAF_Mode_and_SCTG.csv` | **bedrock** `utils/mapping/` | ✅ **Ported**, two stale BEA codes fixed, re-keyed on FAF's published names |
| `Crosswalk_SCTGtoBEA.csv`, mode→BEA crosswalks, `FAFData.R` | `cornerstone-data/stateior` | **Not needed** — the flowsa crosswalk already reaches 2017 detail. Its air mode code is also wrong (`48100`, five digits) |
| CFS 2017 public use file | Census | **Ruled out** — 55% of FAF's ton-miles and *zero* coverage of crude petroleum, the largest `TRANS` commodity |
| `Gross_Margins_2017.yaml` (Census AWTS + ARTS) | flowsa `margins` | Built. Annual **2012–2022**. Gives margin **by trade sector** |
| PCE / PEQ bridges | bedrock, `BEA_PCEBridge` | Same margin columns as the Margins table, but **benchmark years only** — workbook sheets are 2007, 2012, 2017 |
| `Census_EC_PxI` — Economic Census, Products by Industry | flowsa `margins` | Built, **2017**. This *is* BEA's product-line-by-KB input |
| `write_Crosswalk_NAPCS.py` — **MAKB identification** | flowsa `margins` | Built. Takes the max-value NAICS per NAPCS line — literally BEA's "most appropriate kind of business" rule |
| `NAICS_to_NAPCS_Crosswalk_2017.csv` | flowsa `margins` | Built, 29,098 rows |
| 2022 product lines | Census API `ecnnapcsprd` | **Published and live.** `NAICS2022` × `NAPCS2022` × `NAPCSDOL` × `NAICSALL_PCT` |
| Class of customer (PCE vs non-PCE split) | — | Not extracted — but the resulting rates read off the 2017 Margins file |
| NAPCS product line → **I-O commodity** concordance | — | **The one genuinely missing piece** |

**FAF covers 100% of the `TRANS` suppliers exactly** — truck `484000`, rail
`482000`, pipeline `486000`, water `483000`, air `481000`, together the whole
−415,570. The other three FAF modes map to commodities with zero `TRANS`
(postal, couriers, other), which the flowsa method already redistributes.

### The gap, stated precisely

AWTS/ARTS give the wholesale and retail margin *level*, not its allocation
across commodities. Transport is solved end-to-end by FAF. For trade, **most of
BEA's product-line machinery already exists** and only one link is missing.

What exists: `Census_EC_PxI` is the product-line-by-KB table BEA's steps (1)–(2)
consume, and `write_Crosswalk_NAPCS.py` already implements the **MAKB rule** —
it selects the maximum-value NAICS per NAPCS line, which is exactly "the margin
rate of the primary wholesaler of the good". The 2022 vintage is live on the
Census API as `ecnnapcsprd`, carrying `NAPCSDOL` (product-line revenue) and
`NAICSALL_PCT` (industry contribution to that line), so extending beyond 2017 is
a `years:` entry rather than new work.

**The one genuinely missing link is NAPCS product line → I-O commodity** —
BEA's wholesale step (4), where product-line margin is distributed to I-O
commodities using interim supply as weights. BEA builds that concordance
internally and does not publish it. Retail's RCC grouping (steps 3 and 5) is
likewise internal.

**The 2017 Margins file substitutes for both.** It yields the receiving sets,
the per-(buyer, commodity) rates and the PCE/non-PCE structure directly — the
output those concordances exist to produce, for the benchmark year. So the
concordance is needed only if we ever want to *re-run BEA's method* on newer
product-line data, rather than carry 2017-anchored rates forward.

## Approach

Anchor on 2017 structure, move with annual sources, control to identities —
the same shape as [`compensation_disaggregation_plan.md`](compensation_disaggregation_plan.md).

| phase | issue | depends on |
|---|---|---|
| 1 | ✅ [#610](https://github.com/cornerstone-data/bedrock/issues/610) 2017 rates and receiving sets — **built**, §Phase 1 below | — |
| 2 | [#611](https://github.com/cornerstone-data/bedrock/issues/611) port the FAF transport chain | #601 (merged) |
| 3 | [#612](https://github.com/cornerstone-data/bedrock/issues/612) AWTS/ARTS annual trade levels | — |
| 4 | [#613](https://github.com/cornerstone-data/bedrock/issues/613) apply and derive `TRADE`/`TRANS` | #610–#612, **#570 (4a), #579 (4b), #580 (4d)** |
| 5 | [#614](https://github.com/cornerstone-data/bedrock/issues/614) validate per commodity | #613 |
| — | [#615](https://github.com/cornerstone-data/bedrock/issues/615) re-run BEA's product-line method | **deferred, candidate for a Phase 3 of the project** |

**Phase 1 — 2017 rates and the receiving sets.** Built; see §Phase 1 below for
what it produced and what it settled.

**Phase 2 — port the transport chain.** In progress on `faf_transport_margins`.

✅ **Done.** `BTS_FAF` is ported (FAF5.7.1, the 2017-base version, 2017-2024 in
one archive) with two departures from the flowsa original: no `tabula`/PDF
scrape, since the SCTG and mode code tables ship inside the archive as
`FAF5_metadata.xlsx`; and **all trade types**, since FAF's `dms_*` fields already
describe only the domestic leg, so the `trade_type == 1` filter was discarding
20% of ton-miles concentrated in imports rather than avoiding a double count.
The mode and SCTG crosswalk is ported with its two stale BEA codes fixed and
re-keyed on the names FAF publishes.

✅ **Settled.** Ton-miles over tons, FAF over CFS, and the anchor-and-move
construction rather than direct allocation — §Transportation.

❌ **Remaining**, and less than the issue scoped. Per-SCTG ton-mile growth
factors from the FBA, applied to the published 2017 `TRANS` per commodity; the
annual control total; and the FBS method that gets ton-miles from the FBA to
commodity groups. No balancing step, no interim-supply split, no
`Crosswalk_SCTGtoBEA.csv` extension — the ported crosswalk already covers all
258 `TRANS`-receiving commodities.
⚠️ The FBS method uses `attribute_on: [Flowable, PrimarySector]`, which needs the
`retain_activity_columns` plumbing restored in
[#601](https://github.com/cornerstone-data/bedrock/pull/601) — it will raise
`KeyError: ['ActivityProducedBy']` without it. #601 is merged on `nowcast` and
present.

**Phase 3 — trade levels, and optionally BEA's own allocation.** Port
`Gross_Margins_2017.yaml` (AWTS/ARTS) as the annual wholesale and retail control
totals by trade sector. If the 2017-anchored rates from Phase 1 prove
insufficient, the rest of BEA's method is available: port `Census_EC_PxI` and
`write_Crosswalk_NAPCS.py` (which already implements MAKB), extend the FBA to
the 2022 `ecnnapcsprd` vintage, and build the one missing NAPCS → I-O commodity
concordance. **Do Phase 1 first and see whether that is needed** — it is a large
piece of work whose output the 2017 Margins file already approximates.

**Phase 4 — apply.** Rates from Phase 1 onto the nowcast base, levels controlled
to Phases 2–3. **The base needs 4a, 4b *and* 4d** — it is producer value less
the trade-level tax, `T013 + MDTY + SUB + producer-level TOP`, measured in
§Phase 1 against the published column at −0.003%. Two shortcuts to avoid, in
opposite directions: applying rates to `T007` alone drops the margin on imports
(three independent statements confirm imports are in the base — Jolliff's
"column OR", the wholesale method's "interim supply", and the transport method's
table 8.2), while applying them to full producer value double-counts the
trade-level tax already inside the rates.

**Phase 5 — validate per commodity, never in aggregate.** `T014` nets to **1**
against **7,361,003** of gross mass, so a totals check passes on *anything*. Use
the [#587](https://github.com/cornerstone-data/bedrock/issues/587) per-cell
picture; `supply_bridge_detail_sut` already covers `TRADE`/`TRANS`.
Bound-check rates on `T016`, not `T013` — see "the ratio is not a share".

Budget the effort on the **positive side**: the −3.68T negative side is 24
commodities giving up nearly all their own output (19 trade at 96.8%, eight
retail sectors at exactly −100%; 5 transport at 56.8%), which 4a produces
anyway. The work is allocating across the 255 receiving commodities.

## Phase 1 — built

[`bedrock/transform/iot/nowcast_margins.py`](../../transform/iot/nowcast_margins.py)
carries the structure;
[`margins_2017_baseline.py`](margins_2017_baseline.py) is the proof against the
published Supply columns, commodity by commodity, and `--check` fails if a count
regresses.

**Settled: the anchor is the *before*-redefinitions Margins table.** It matches
the MUT before-redefinitions tables and therefore the SUT — everything upstream
of Step 7 stays before redefinitions — and the published Supply table it
aggregates against is a before-redefinitions construct too.

Measured both ways, the after-redefinitions table closes the **trade identity**
— `Σ_buyers (Wholesale + Retail) = TRADE + TOP`, §The object — for 226 of the
255 commodities with positive `TRADE`, against 236. **That gap is redefinition, not framework**
— the SUT/MUT difference is what the `+ TOP` term handles, and it applies to
both redefinition states equally. Redefinition moves 6,007 million of trade
margin gross across 111 commodities, −5,879 net, and **`333914` pump and pumping
equipment is 4,139 of it on its own**: it is redefined out of the
after-redefinitions Margins table entirely, as a buyer and as a commodity, while
the before-redefinitions Supply table still carries `TRADE` = 3,965 for it. The
rest are small proportional shifts — the largest, `326190`, is 654 on 41,158 —
that push commodities across the 1% line. So the two tables do not disagree
about margins; they are indexed on different commodity content, and pairing
either one with the Supply table of the other state asks them to reconcile
anyway.

Note `derive_PRO_to_PUR_ratio` reaches for the after-redefinitions table via
`USAConfig`; these are different objects for different purposes.

**What it produces**, all keyed on the published `(buyer, commodity)` index:

| | transactions | commodities | buyers | margin, $M | carried as a rate | as a level |
|---|---:|---:|---:|---:|---:|---:|
| Wholesale | 17,419 (30.6%) | 254 | 413 | 1,894,329 | 17,168 | 251 |
| Retail | 2,106 (3.7%) | 188 | 336 | 1,761,765 | 2,098 | 8 |
| Transportation | 12,516 (22.0%) | 258 | 413 | 414,559 | 12,286 | 230 |

Rates are per (buyer, commodity, margin type) on BEA's cascading bases, and the
table carries a `base_share` and a `margin_share` alongside each — a nowcast
base arrives per commodity (`T013`) and has to be split across that commodity's
receiving transactions before a per-transaction rate can be applied, so keeping
both splits is what lets #613 use this without going back to the published
table.

**Two treatments, not one.** 489 of the (transaction × margin type) entries
carry a *level* instead of a rate: 470 `F03000` inventory entries (§Negative
margins below), and 19 across 13 transactions whose cascading base is zero or
negative — seven retail cells of exactly 1 million over a zero base, and six
over a negative one, of which `F02E00` buying `S00402` is essentially all the
value. A margin over a change-in-inventories base is a timing correction, not a
rate, and a rate over a negative base is not a number.

### Both identities, per commodity

| identity | population | commodities | holds within 1% | derived, $M | published, $M | net |
|---|---|---:|---:|---:|---:|---:|
| trade | `TRADE` > 0 | 255 | **236** | 3,656,094 | 3,697,260 | −1.11% |
| trade | all | 402 | 279 | | | |
| transport | `TRANS` > 0 | 258 | **180** | 414,559 | 415,580 | −0.25% |
| transport | all | 402 | 319 | | | |

⚠️ The two rows per identity are the same check on different populations, and
the difference is not small: 144 commodities bear no transportation margin on
either side and hold trivially, which is what turns 180 of 258 into 319 of 402.
Quote the positive population.

### Every residual, accounted for

**The 19 trade misses are the excise and severance goods, in order** — tobacco
−18%, oil and gas −33%, distilleries −12%, motion picture −35%, breweries −6%,
coal −57%, wineries −2%. Each misses by exactly its producer-level tax, which is
the decomposition above seen from the other end: the identity's tax term
corrects for *trade-level* tax, sitting inside Wholesale and Retail, while
excise sits in `Producers' Value` and never enters the left-hand side.
Producer-level tax is **9.6% of `TOP`** over the 203 margin-bearing commodities
with `TOP` > 100 million.

**The 78 transport misses are publication rounding, not method.** BEA publishes
the Margins table to the million and the smallest non-zero transportation cell
in it is exactly 1 million, so every cell that would round below half a million
is published as zero and drops out of the commodity sum. The shortfall is
therefore one-sided — 195 of 258 commodities short against 22 over — and scales
with how thinly the margin is spread:

| median published cell | commodities | holds | median relative difference |
|---|---:|---:|---:|
| ≤ 1M | 21 | 3 | −3.0% |
| 1–2M | 60 | 23 | −1.3% |
| 2–5M | 86 | 67 | −0.3% |
| 5–20M | 72 | 69 | −0.1% |
| > 20M | 19 | 18 | −0.02% |

The misses carry 7.6% of `TRANS`. Nothing to fix: a nowcast built from these
rates inherits the same rounding, and the alternative is inventing cells BEA
suppressed.

**The supplying side is in the Margins table after all** — this changes the
Phase 4 scoping. The 24 commodities with a negative Supply margin column were
written off above as "close to a function of trade/transport output, which 4a
produces anyway". They do not need 4a: the published `Purchasers' Value` is
**not** the sum of the four components on a trade or transport commodity's own
rows. PCE buys 254 billion of `441000` motor vehicle dealers at producers' value
and **zero** at purchasers' value, because that margin has been moved onto the
goods. `Σ_b (Producers' + Transportation + Wholesale + Retail − Purchasers')`
per commodity lands entirely on those 24, and:

- against `−TRANS` it is a **direct read** — all five transport commodities
  within 0.2%, 20 million out of 415,570;
- against `−TRADE` it runs 1.0× to 2.2× high, and the excess is the trade-level
  tax the receiving side is short by. The two sides measure that tax
  independently to within 0.3% — 391,761 million here against 391,162 million by
  which `Σ(Wholesale + Retail)` exceeds `TRADE` on the receiving side, 12.0% of
  positive `TRADE`. Petroleum wholesalers are the extreme at 2.2×, exactly as
  the motor-fuel-tax finding predicts.

⚠️ The same fact is a trap for Step 6b: **do not reconstruct `Purchasers' Value`
by summing the components** — on 14,655 rows the published table does not, and
the difference is the whole 4.07 trillion of margin.

### What the rates apply to — and why 4d comes first

The cascade starts from `Producers' Value`, so "what is the nowcast base" is a
question about *valuation*, and it has a measurable answer. Summing the Margins
table over buyers per commodity — **excluding `F05000`**, which carries
−2,626,305 of producers' value, no margin at all, and is a MUT-only negative
column — gives 37,093,944 against a derived producer value (`T013` + `T015`) of
37,094,432: **−0.0013%**. Basic `T013` is 36,398,867, off by 695,565. The
Margins table is a **producer**-value object, not a basic-value one.

Per commodity it is producer value **less the trade-level tax**, because that
tax is booked inside the margin columns of the commodity receiving the margin
and inside the `Producers' Value` of the trade commodity that collected it —
`424700` petroleum wholesalers carry 161,945 of producers' value against a
`T013` of 72,970. So the base a nowcast has to rebuild is:

```
base[c] = T013[c] + MDTY[c] + SUB[c] + producer_level_tax[c]
```

(`SUB` is stored negative in the Supply table, so it subtracts; a negative
producer-level residual is treated as zero.) Scored against the published
`Σ_buyers Producers' Value` over the 378 margin-receiving commodities:

| candidate base | within 1% | within 0.1% | net |
|---|---:|---:|---:|
| `T013` basic | 293 / 378 | 164 | +0.88% |
| full producer, `T013 + T015` | 201 / 378 | 125 | −1.19% |
| **producer less trade-level tax** | **377 / 378** | **364** | **−0.003%** |

**So the tax columns are a hard prerequisite of Phase 4, and only part of `TOP`
belongs in the base.** Adding all of `TOP` double-counts the trade-level share,
which is already inside the rates' numerators: 19% too high on petroleum
refineries, 36% on tobacco. The split that says how much to add is the
producer-level decomposition Phase 1 produced, which is the third use that
decomposition has now earned.

| the excise commodities, $M | published `Σ PV` | basic | full producer | the rule |
|---|---:|---:|---:|---:|
| Petroleum refineries | 530,433 | 530,094 | 629,277 | 530,435 |
| Tobacco manufacturing | 62,153 | 48,809 | 84,820 | 62,154 |
| Distilleries | 25,456 | 21,341 | 32,441 | 25,453 |
| Oil and gas extraction | 359,408 | 351,108 | 360,281 | 359,410 |

**What Phase 4 therefore depends on:**

| input | from | why |
|---|---|---|
| `T007` domestic output, basic | 4a [#570](https://github.com/cornerstone-data/bedrock/issues/570) | `T013` |
| `MCIF`, `MADJ` | 4b [#579](https://github.com/cornerstone-data/bedrock/issues/579) | `T013` — imported goods carry domestic margin |
| `MDTY` | 4b [#579](https://github.com/cornerstone-data/bedrock/issues/579) | in the base directly |
| `TOP`, `SUB` | **4d [#580](https://github.com/cornerstone-data/bedrock/issues/580)** | the producer-level slice of `TOP`, and `SUB` |
| `F03000` by commodity | 1e [#529](https://github.com/cornerstone-data/bedrock/issues/529) | moves the 470 inventory **level** rows, which no rate covers |
| buyer split | Phase 1's `base_share` | see below |

**Not** dependencies, and worth stating because both look like they should be.
Trade and transport commodity output is a *check* on the negative side, not an
input — the give-up derives from the positive side. And the Use table is
deliberately not one: distributing the commodity base across buyers uses the
2017 `base_share` rather than a nowcast Use matrix, because getting producer
value per Use cell is what Step 6b uses the margins *for*. Freezing the buyer
structure at 2017 is the price of breaking that circle; if Step 3 lands first,
the nowcast Use row is available to inform the split, but nothing here requires
it.

One limitation this makes explicit: the rate numerators contain trade-level
sales tax, so carrying 2017 rates forward carries 2017's effective sales-tax
rates with them. A statutory rate change in a nowcast year does not reach the
margins except through the Phase 3 control totals.

### The bound check, and one correction

| ratio | commodities > 0 | above 1 | max | share of value above 1 |
|---|---:|---:|---:|---:|
| `TRADE` / `T013` | 255 | 21 | 16.02 | 21.1% |
| `TRADE` / `T016` | 255 | **0** | 0.715 | — |
| `TRANS` / `T013` | 258 | 1 | 3.25 | 5.7% |
| `TRANS` / `T016` | 258 | **0** | 0.315 | — |

The warning above was written for the trade margin; it applies to
transportation too. `S00402` used and secondhand goods carries `TRANS`/`T013` of
3.25 — the same mechanism, a commodity with almost no basic value of its own.
Bound-check on `T016` for both.

## Negative margins are inventory timing — never clip them

All **31** negative rows in the 2017 table are buyer `F03000`, totalling
**−8,076 million**. Nothing else anywhere in the table is negative. BEA's
explanation of the mechanism:

> *"Margins are accounted for when the change in inventories are increasing.
> When we draw on inventories for intermediate consumption we would see
> intermediate inputs account for the margins even though in that particular
> case there was no trade margin for that specific item in the period. The
> negative margin from inventories accounts for the value of the inventory that
> was added in the previous period, but also in the reference period the
> commodity output (margin output as well) would be unchanged as a result of a
> draw on inventories."* — B. Jolliff, BEA, 2025-06-24

So margin is booked **when inventory builds**, and a drawdown carries the
offsetting negative because the margin was already counted in the earlier
period while current-period output is unchanged. The negative is doing real
accounting work: it keeps total purchasers' value consistent whether a material
is consumed immediately or stored and drawn later.

Three consequences:

- **Do not clip, floor or absolute these values.** ⚠️
  `_margin_negatives_treatment`'s `abs_negative_margin_columns` flag in
  `derive_PRO_to_PUR_ratio.py` would silently destroy exactly this signal —
  check it before reusing that module.
- **Do not derive rates from `F03000` rows.** A negative margin over a
  change-in-inventories base is not a rate; it is a timing correction. Exclude
  `F03000` when fitting the per-(buyer, commodity) rates in Phase 1, then carry
  its margin as a level.
- **Expect the sign to flip by year.** In a year of inventory build the same
  cells go positive. A nowcast that hard-codes the 2017 sign will be wrong
  whenever the inventory cycle turns.

## Open questions

1. ~~**Tons or ton-miles** for the transport allocation~~ — **settled on the 2017
   fit** (§Transportation): ton-miles, by a wide margin over tons, and FAF over
   the CFS public use file, which has zero coverage of the largest `TRANS`
   commodity. But neither measure *predicts* the margin, so ton-miles moves the
   2017 allocation forward rather than generating it: each commodity's published
   2017 `TRANS` grows with its SCTG group's ton-miles, controlled to the annual
   total. No balancing step and no interim-supply split are involved.
2. ~~**Extending `Crosswalk_SCTGtoBEA.csv` to BEA 2017 detail**~~ — **moot.**
   That was the stateior route; the flowsa crosswalk ported in Phase 2 already
   carried a BEA 2017 detail code on every SCTG row, and with two stale codes
   fixed it covers all 258 `TRANS`-receiving commodities and 100% of `TRANS`
   value. Note the codes sit in a `Note` column that the mapping machinery does
   not read — it joins on `Sector`, which is NAICS — so they become operative
   only when [#546](https://github.com/cornerstone-data/bedrock/issues/546)
   lands a BEA-code target schema. That is a column swap, not new work.
3. **Build the NAPCS → I-O commodity concordance?** Only needed to re-run BEA's
   actual method on 2022 product lines rather than carry 2017-anchored rates.
   Everything upstream of it now exists; this is the sole missing link.
   **Deferred to [#615](https://github.com/cornerstone-data/bedrock/issues/615)**,
   out of v0.5 and a candidate for a Phase 3 of the project. Reopen only if
   #614's validation shows the 2017-anchored allocation drifting.

Three earlier questions are now closed: the excise/sales split resolves into the
producer-level vs trade-level decomposition above; petroleum's near-zero
residual is correct behaviour rather than an anomaly; and `TRADE`/`T013` above 1
is correct because margin is additive, not a share.
