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
freight-cost proxy than tons since cost scales with distance. Test tons vs
ton-miles against the 2017 rates and keep the better fit — the 49%-to-0.04%
spread makes that a sharp test.

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

Zero commodities are uniform. The reason is that BEA's "item" is **finer than
the published commodity**, so several item rates mix inside every published
commodity, and the item level is not recoverable from published data.

**Therefore: carry rates per (buyer, commodity, margin type) as
[#571](https://github.com/cornerstone-data/bedrock/issues/571) already
specifies.** Collapsing to a per-commodity rate discards real, measured
variation, and it cannot be justified from the source method.

## Sources — what exists

| Piece | Where | State |
|---|---|---|
| `BTS_FAF` extractor + `Transport_Margins_2017.yaml` | flowsa **`margins`** branch | Built. Already selects `Unit: ton-miles` and redistributes the non-primary modes proportionally |
| `NAICS_Crosswalk_FAF_Mode_and_SCTG.csv` | flowsa `margins` | Built |
| `Crosswalk_SCTGtoBEA.csv`, mode→BEA crosswalks, `FAFData.R` | `cornerstone-data/stateior` | Built, **R**, and SCTG maps to **BEA 2012 detail** — no 2017 detail column |
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

**Phase 1 — 2017 rates and the receiving sets.** Depends on nothing; start here.
Derive per-(buyer, commodity, margin type) rates on BEA's cascading bases, and
the three receiving sets from non-zero values. Prove both identities reproduce
the published Supply columns commodity by commodity.

**Phase 2 — port the transport chain.** flowsa `margins` `BTS_FAF` +
`Transport_Margins_2017.yaml`. Extend `Crosswalk_SCTGtoBEA.csv` to 2017 detail.
Decide tons vs ton-miles on the 2017 fit.
⚠️ The method uses `attribute_on: [Flowable, PrimarySector]`, which needs the
`retain_activity_columns` plumbing restored in
[#601](https://github.com/cornerstone-data/bedrock/pull/601) — it will raise
`KeyError: ['ActivityProducedBy']` without it.

**Phase 3 — trade levels, and optionally BEA's own allocation.** Port
`Gross_Margins_2017.yaml` (AWTS/ARTS) as the annual wholesale and retail control
totals by trade sector. If the 2017-anchored rates from Phase 1 prove
insufficient, the rest of BEA's method is available: port `Census_EC_PxI` and
`write_Crosswalk_NAPCS.py` (which already implements MAKB), extend the FBA to
the 2022 `ecnnapcsprd` vintage, and build the one missing NAPCS → I-O commodity
concordance. **Do Phase 1 first and see whether that is needed** — it is a large
piece of work whose output the 2017 Margins file already approximates.

**Phase 4 — apply.** Rates from Phase 1 onto nowcast `T013`, levels controlled
to Phases 2–3. **`T013` = `T007` + `MCIF` + `MADJ`, so this needs 4a *and* 4b.**
Applying rates to `T007` alone is the tempting shortcut and drops the margin on
imports — three independent statements confirm the base: Jolliff's "column OR",
the wholesale method's "interim supply", and the transport method's table 8.2.

**Phase 5 — validate per commodity, never in aggregate.** `T014` nets to **1**
against **7,361,003** of gross mass, so a totals check passes on *anything*. Use
the [#587](https://github.com/cornerstone-data/bedrock/issues/587) per-cell
picture; `supply_bridge_detail_sut` already covers `TRADE`/`TRANS`.

Budget the effort on the **positive side**: the −3.68T negative side is 24
commodities giving up nearly all their own output (19 trade at 96.8%, eight
retail sectors at exactly −100%; 5 transport at 56.8%), which 4a produces
anyway. The work is allocating across the 255 receiving commodities.

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

1. **21 commodities have `TRADE`/`T013` > 1**, which should be impossible if
   `T013` is the allocation base. Resolve before relying on the rate.
2. **Tons or ton-miles** for the transport allocation — decide on the 2017 fit.
3. **Build the NAPCS → I-O commodity concordance?** Only needed to re-run BEA's
   actual method on 2022 product lines rather than carry 2017-anchored rates.
   Everything upstream of it now exists; this is the sole missing link.
4. **Extending `Crosswalk_SCTGtoBEA.csv` to BEA 2017 detail** — it currently
   carries 2012 detail and 2017 *summary* only. This is the real porting work
   on the transport chain, not the SCTG mapping itself.

Two earlier questions are now closed: the excise/sales split resolves into the
producer-level vs trade-level decomposition above, and petroleum's near-zero
residual is correct behaviour rather than an anomaly.
