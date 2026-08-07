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
| Economic Census product lines, class of customer | — | Not extracted. Quinquennial |
| RCC / product-line → I-O concordances | — | **BEA-internal, unpublished** |

**FAF covers 100% of the `TRANS` suppliers exactly** — truck `484000`, rail
`482000`, pipeline `486000`, water `483000`, air `481000`, together the whole
−415,570. The other three FAF modes map to commodities with zero `TRANS`
(postal, couriers, other), which the flowsa method already redistributes.

### The gap, stated precisely

**AWTS/ARTS give the wholesale and retail margin *level*, not its allocation
across commodities.** Transport is solved end-to-end by FAF; trade is not. The
missing piece is BEA's product-line machinery, whose concordances are
unpublished.

**The 2017 Margins file is the substitute.** It yields the receiving sets, the
per-transaction rates, and the PCE/non-PCE structure directly — everything the
unpublished concordances were needed to produce, for the benchmark year.

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

**Phase 3 — trade levels.** Port `Gross_Margins_2017.yaml` (AWTS/ARTS) as the
annual wholesale and retail control totals by trade sector.

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

## Open questions

1. **Does the excise/sales split need modelling?** The `TRADE + TOP` identity
   leaves −1.29%, concentrated on excise goods — tobacco, distilleries and
   breweries all near −0.37 of their `TOP`. Only matters if margins are needed
   in basic prices.
2. **Why is petroleum's residual ~0** (−206 on `TOP` 99,047) when fuel excise is
   large? Possibly booked at `447000` gasoline stations instead.
3. **21 commodities still have `TRADE`/`T013` > 1**, which should be impossible.
4. **Tons or ton-miles** for the transport allocation.
5. **Do Economic Census product lines survive 2022** in a usable form, if we
   ever want BEA's actual method rather than 2017-anchored rates?
6. **Negative margins are inventory timing** — all 31 are buyer `F03000`
   ([BEA, 2025-06-24](https://github.com/cornerstone-data/bedrock/issues/571)).
   ⚠️ `_margin_negatives_treatment`'s `abs_negative_margin_columns` flag in
   `derive_PRO_to_PUR_ratio.py` would destroy that signal — check before reuse.
