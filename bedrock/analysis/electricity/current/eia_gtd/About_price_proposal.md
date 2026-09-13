# Proposal: price the kWh, and make T&D the residual

**Status: draft for discussion.** Pairs a revision of the electricity G/T/D
disaggregation with a revision of the nowcast electricity Use column. Neither
works alone: the allocator's weights and the Use row it allocates over disagree,
and at present each is used to justify the other.

Companion measurement: `About_electricity_shares.md`, issue #894, PR #892.

## The defect

`allocate_purchaser_gtd` converts MWh to dollars with **one price for every
purchaser in the economy**:

```python
p = (p_share_2017 * electricity_purchases_total) / egrid_mwh   # a scalar
proportional = mwh.loc[members] * p
```

Inside manufacturing the MWh come from MECS Table 7.7 kWh shares, so a sector's
generation dollars are `its kWh share × the average price`. Where a sector buys
cheap power that product exceeds its entire electricity bill, water-fill caps it,
and the excess lands on other purchasers. Measured on the 2024 nowcast: **44
manufacturing sectors clip under MECS weights, zero under dollar weights**, and
**47 end with no T&D dollars at all** — modelled as buying generation and no
transmission or distribution, which no real purchaser does.

Stated precisely: the flat `p` is **4.67 ¢/kWh**, and the cheapest manufacturing
industry's *entire* electricity bill is **4.21 ¢/kWh** (325194 cyclic crudes).
The method charges some industries more for generation alone than they spend on
electricity in total.

## The governing idea

**Generation is a commodity; T&D is not.** Wholesale generation barely varies by
customer, while a smelter at transmission voltage pays almost no distribution. So
the price differential between industries is substantially a *T&D* differential,
and T&D should be derived as a residual rather than assumed as a fixed share.

The data bears this out. Holding generation at one price, implied T&D per kWh
orders exactly as the theory predicts:

| lowest T&D | ¢/kWh | | highest T&D | ¢/kWh |
|---|---:|---|---|---:|
| Cyclic crudes | 1.21 | | Asphalt paving | 8.83 |
| Newsprint mills | 1.43 | | Secondary aluminum | 8.54 |
| Petrochemicals | 1.60 | | Aircraft | 6.81 |
| Nonferrous smelting | 1.68 | | Beverages | 6.39 |
| Inorganic chemicals | 1.71 | | Aerospace parts | 5.76 |

Large continuous-process plants at transmission voltage at the top, small
distribution-connected plants at the bottom.

## The price source is MECS, not Census

MECS publishes the manufacturing price itself: **Table 7.2** as $/MMBtu and
**Table 7.10** as expenditure in dollars, on the same NAICS frame as 7.7's kWh.
They agree to within **0.124 ¢/kWh across all 82 published rows**, and the 31-33
aggregate is **6.86 ¢/kWh against EIA's published industrial 6.92** (−0.9%).

Census-derived prices are rejected for this role. Dividing Census `CSTELEC` by
MECS kWh lands at 6.57 (−5.0% against published), produces impossible tail values
(max 171 ¢/kWh against 11.83 on the MECS-internal series), diverges from MECS by
more than 25% on 15 of 81 rows, and is noise-dominated across years — rank
correlation 0.55-0.60 between 2018 and 2022, against **0.919** for MECS.

Census keeps the job it is good at and already does: the annual **dollar** index
on the Use row, via `nonmaterial_seed`'s existing `CSTELEC → 221100` mapping.

## A. Annual per-industry price

The level moves and the structure does not, and each is observed by the source
that sees it best:

| | 2018 → 2022 |
|---|---|
| price **level** | +23.6% (EIA published industrial +22.1%) |
| **relative** structure, rank correlation | **0.919** |
| kWh-weighted mean \|relative change\| | **6.8%** |
| industries with relative price moving <20% | **72 of 76 — 99% of kWh** |

So:

```
price_i(t) = relative_i(t) × published_industrial_price(t)
             └ interpolated, MECS 2018/2022 ┘   └ EIA, observed annually ┘
```

Outside the anchors, hold the relative structure and let the published level
carry it — the same "hold past the last observation" form the nowcast already
uses.

**Two tests fall out.** The kWh-weighted aggregate of `price_i(t)` must reproduce
the published industrial price for every year. At t = 2018 and 2022 it must
reproduce MECS exactly — the identity check every seed in this repo is held to.

⚠️ Four industries move their relative price more than 20% (1% of kWh). Name and
flag them; aluminum smelter closures are exactly this shape, and a real contract
change should not be smoothed away.

## B. Use column: MECS levels at the anchors, Census as the annual index

Realign manufacturing electricity levels to MECS at 2018 and 2022, and index
annually on Census between them — the same anchor-and-index pattern
`Census_ASM_Expenses` already describes itself as ("the annual bridge between the
two Economic Census materials breakouts").

Two constraints this must respect:

- ⚠️ **The column control rescales whatever is set.** `apply_column_control`
  rescales the block to `GO − VAPRO`, and `composed_seed` is explicit that "the
  dollar level here is never the estimate ... a seed changes *how a column
  divides*, nothing else". So realigning to MECS realigns the **shape** of the
  electricity row unless it is placed where the control does not override it.
- ✅ **Anchoring at 2018 rather than 2017 protects the benchmark match.** 2017
  keeps BEA's published cross-section. It does create a seam — the two
  cross-sections differ by roughly 13pp of shape — which should be stated, not
  smoothed.

Why realignment and not just indexing: the existing seed is
`seed[c,i] = Use2017[c,i] × survey[i,k,t] / survey[i,k,2017]`, which aligns how a
column *moves* and never how the row *divides*. That is why IO-vs-Census sits at
12.96pp **at 2017 itself** and stays 11-15pp across the span.

⚠️ Adopting Census's cross-section instead would import scope error: the
Census/BEA ratio is 0.866 in aggregate but runs p10 0.663 to p90 1.728, a
**BEA-weighted 18.1% mean deviation**. MECS is the tie-breaker — it disagrees with
Census precisely where Census looks wrong (primary batteries 26.5×, clothing
14.5×, computers 9.55× against BEA, and the same industries return impossible
prices).

## C. Disaggregation: generation at a class price, T&D as the residual

```
gen_i = kWh_i × p_gen(class)
T&D_i = electricity_i − gen_i     ( = kWh_i × (price_i − p_gen) )
```

Water-fill becomes an exception rather than a mechanism. At any uniform
`p_gen ≤ 4.21 ¢/kWh`, **no manufacturing industry goes negative**; at the current
4.67, three do (2.8% of kWh). Against 44 clipping sectors today.

**Commercial keeps a single class price.** CBECS 2018 Table C13 gives 9.19 to
11.22 ¢/kWh across all thirteen building activities — a **1.22× spread**, against
manufacturing's 1.8× at the deciles. One price per class is defensible there and
is not in manufacturing. The gap between classes (9.95 vs 6.86) is larger than
either internal spread, which is what one economy-wide `p` cannot represent.

⚠️ **CBECS cannot allocate.** It classifies by principal building activity and
the 2018 microdata codebook contains **zero NAICS references**. A building is not
an establishment and a multi-tenant office cannot be resolved to NAICS by any
mapping. CBECS prices a class; it does not distribute one.

## ⛔ Blocking precondition: the electricity row does not foot to EIA

Deriving kWh from `dollars ÷ price` requires the dollars on the price's basis.
They are not. The derived balanced Use SUT (purchaser price — the valuation the
nowcast seeds in) against EIA-861 published retail revenue, $bn:

| year | EIA comm | ours | gap | EIA ind | ours | gap | EIA resid | PCE | gap |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 145.0 | 190.4 | +31.3% | 67.7 | 77.0 | +13.8% | 177.7 | 177.8 | +0.1% |
| 2018 | 148.2 | 205.1 | +38.4% | 69.2 | 84.0 | +21.4% | 189.0 | 191.2 | +1.1% |
| 2019 | 146.0 | 207.5 | +42.1% | 68.3 | 83.4 | +22.1% | 187.4 | 188.1 | +0.4% |
| 2020 | 137.0 | 190.2 | +38.8% | 64.0 | 76.7 | +19.9% | 192.7 | 190.9 | −0.9% |
| 2021 | 149.7 | 250.6 | **+67.5%** | 71.8 | 95.6 | +33.1% | 200.8 | 196.8 | −2.0% |
| 2022 | 173.4 | 276.2 | +59.3% | 84.9 | 120.0 | **+41.4%** | 227.0 | 219.9 | −3.1% |
| 2023 | 178.2 | 242.7 | +36.2% | 81.2 | 97.2 | +19.8% | 232.0 | 242.0 | +4.3% |
| 2024 | 185.9 | 227.4 | +22.3% | 84.1 | 92.3 | +9.8% | 244.4 | 259.3 | +6.1% |

Commercial = services + trade + government + transportation; industrial =
manufacturing + mining + construction + agriculture, against the matching EIA
customer class.

### The base offset is BEA's own, not the nowcast's

The published **BEA 2017 SUT Use** carries the same gap before the nowcast
touches it: commercial-like $188.9bn against EIA's $145.7bn (**+29.7%**),
industrial-like $76.9bn against $67.7bn (**+13.6%**). The nowcast reproduces it at
2017 (190.4 / 77.0), which is what it is built to do. So the level difference is a
BEA-versus-EIA scope question and **not a defect to fix in this work** — but it
must be attributed before `kWh = dollars / price` is built on it, because that
derivation inherits it and will not reconcile against the eGRID MWh targets.

### ❌ Taxes are not the explanation

Published 2017 `T00TOP` on `221100` is **$23.44bn**, 5.1% of the $458.2bn
electricity row, of which the named public-utilities excise is $11.31bn.
Apportioned to the intermediate share of the row that is roughly $14bn against a
$64.4bn intermediate gap — **at most a fifth, and in the wrong direction** once
the control below is applied.

⚠️ **Residential refutes it outright.** A tax wedge applies to the whole
commodity row, yet **PCE matches EIA residential to +0.1% in 2017** and stays
within ±6% across the span, while commercial sits +31% at the same moment. A
uniform additive wedge cannot produce a 0% gap on one class and +31% on another.
Whatever the offset is, it is specific to the intermediate block.

Remaining candidates, in order: **scope of the 221100 commodity** beyond retail
sales (resale, transmission service, direct use of self-generated power — EIA's
`Table 2.2` reports Direct Use separately and the repo already loads it), and
**industry-versus-customer-class assignment**, which can move dollars between
commercial and industrial but cannot change the total and so cannot be the whole
story.

### The excursion is the nowcast's

The gap is not stable. It runs +31.3% at 2017, balloons to **+67.5% commercial in
2021** and **+41.4% industrial in 2022**, then falls back to +22.3% / +9.8% by
2024 — below the 2017 base on the industrial side. A scope difference does not do
that. The excursion coincides with the 2021-22 energy price surge: EIA commercial
revenue rose 149.7 → 173.4 while ours rose 250.6 → 276.2, so **the electricity row
over-responds to price movement**. This is the same excursion that puts 324110
refineries at 12.14% of manufacturing electricity in 2022 against 5.72% either
side, now visible at the aggregate.

⚠️ Note where the seeds are: manufacturing electricity is indexed on Census
`CSTELEC` through `nonmaterial_seed`, but the commercial band — the larger and
worse-behaved half — has no electricity seed and moves on the carry.

**Sections A and C are blocked** until the base offset is attributed and the
2021-22 excursion explained.

## Open questions

1. **`p_gen` is not free** — it is pinned by requiring generation dollars to foot
   to the UGO generation share, which yields 4.67, above the 4.21 ceiling. Three
   distinguishable resolutions: the generation share is wrong; `p_gen` should
   vary by class; or a few smelters genuinely buy below average wholesale on
   long-term contracts. Only 3 industries and 2.8% of kWh ride on it.
2. Does a class-varying `p` disturb the eGRID class-MWh targets?
3. Agriculture, mining and construction have neither a MECS nor a CBECS price.
4. MECS is quadrennial — the relative-price interpolation above is the proposed
   answer, but it is untested before 2018 and after 2022.

## Prerequisites

- ⚠️ **Add MECS Table 7.2 and 7.10 to the extractor.** Both are commented out of
  the `tables:` list in `EIA_MECS_Energy.yaml`; 7.10 has a 2018 layout and
  **neither has a 2022 layout**. The 2022 row offsets differ from 7.7's by one
  row (region markers at 15/98/181/264/347 against 16/99/182/265/348) and the RSE
  blocks differ again, so ranges must be **verified against the workbook, not
  derived**. Until then the only 7.10 on hand is the superseded
  `EIA_MECS_Energy_2018_v2.0.0_c31283d` parquet.
- Add `EIA_CBECS_Energy` for table C13. `EIA_CBECS_Land` and `EIA_CBECS_Water`
  give the extractor pattern; C13 sits at `ce/xls/c13.xlsx` rather than the
  `bc/` path those use.
- Re-run the clip and reassignment counts under #892's re-anchored shares.

## Why the nowcast column must move in tandem

Three defects sit in the Use column and none is caused by MECS:

- **322120 Paper mills carries $0 electricity in the pinned MUT
  `v0.3.0_4276083`, in 2021 and 2023** ($0.722bn / **$0** / $0.908bn / **$0** /
  $0.744bn over 2020-2024).
- **324110 refineries swing 5.72% to 12.14% of manufacturing electricity and
  back**, while both independent surveys stay inside 4.8-7.0%.
- The IO-vs-MECS gap **widens across the span**, 15.62pp to 20.22pp, against a
  MECS side that is frozen on one survey.

A priced allocator sitting on that column would attribute its instability to the
electricity method.
