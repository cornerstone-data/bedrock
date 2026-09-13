# Why the current electricity disaggregation fails

**The case for a third iteration.** The current method pairs two decisions that
are individually well-argued and jointly inconsistent: one national generation
price (methods Discussion **#88**, D0 and D4) and MECS physical kWh inside
manufacturing (Discussion **#90**, M4). Together they make a growing set of
manufacturing purchasers impossible to represent — 6 in 2017, **44 in 2024** —
and hand every one of them an electricity emission factor about **twice** what a
comparable purchaser gets.

Measured by `flat_price_clipping.py`. Companions: `About_electricity_shares.md`
(the three-source comparison), `About_price_proposal.md` (the proposed method),
`electricity_row_control.py` (what drives the row the allocator divides).

## Where the method came from

| | approach | headline outcome |
|---|---|---|
| **1st** (#85) | BEA gross output for the split; **EIA retail prices by end-use class** to turn dollars into MWh | ❌ Residential MWh came out **0.532×** EIA's. Non-electricity `N` moved ~14%. |
| **2nd** (#88 + #90) | EIA class shares set MWh; **one national generation price**; MECS kWh inside manufacturing | ✅ Class ratios 1.000. `N` moves ~4% (mixed units) / ~1.4% (reaggregated). |
| **3rd** (proposed) | keep every bill; **price generation by class**, T&D as the residual | — |

The flat price was adopted *specifically* to fix iteration 1. The results deck
is explicit: "Main reason for decrease is we using one price for monetary to
physical conversion for generation, whereas before we were using several
different prices ... This also caused the misallocation of MWh in the model
relative to EIA end use sectors."

**That diagnosis was right, and nothing here argues for going back.** What
follows is that the cure has a defect of its own, in a different place.

## 1. The cap is an identity, not an edge case

Generation dollars are `MWh_i × p`, capped at the purchaser's own electricity
bill, with the excess water-filled onto others in the same class (#88 D8). So:

```
gen_i = MWh_i × p,  capped at bill_i
gen_i ≤ bill_i   ⟺   bill_i / MWh_i ≥ p
```

**A purchaser is capped if and only if its own all-in electricity price is below
the single national generation price.** Not approximately — exactly. The clipped
set and the below-`p` set are the same set, every year, on every Industrial
purchaser:

| year | `p` ¢/kWh | purchasers | clipped | own price < `p` | sets agree on |
|---|---:|---:|---:|---:|---:|
| 2017 | 3.87 | 265 | 6 | 6 | **265 / 265** |
| 2018 | 4.05 | 264 | 10 | 10 | **264 / 264** |
| 2019 | 4.09 | 262 | 9 | 9 | **262 / 262** |
| 2020 | 4.00 | 260 | 11 | 11 | **260 / 260** |
| 2021 | 4.68 | 260 | 24 | 24 | **260 / 260** |
| 2022 | 5.15 | 260 | 27 | 27 | **260 / 260** |
| 2023 | 4.84 | 255 | 41 | 41 | **255 / 255** |
| 2024 | 4.67 | 265 | 44 | 44 | **265 / 265** |

This is the structural claim, and it needs no appeal to data quality: **a single
national generation price cannot represent any purchaser whose all-in price is
below it.** For such a purchaser the model must either charge more for
generation alone than the purchaser spends on electricity in total — impossible
— or cap generation at the bill. There is no third option. How many purchasers
that is depends only on where `p` sits in the price distribution, and in 2024 it
sits at the **17th percentile**.

## 2. Every capped purchaser is left with zero transmission and distribution

T&D is the residual of the bill after generation (#88 D8), so a cap that
consumes the whole bill leaves nothing. This is not a near-zero; it is zero, for
every capped purchaser in every year:

| year | clipped | **with zero T&D** | their bill | % of Industrial | median T&D share, uncapped |
|---|---:|---:|---:|---:|---:|
| 2017 | 6 | **6** | $3.87bn | 4.5% | 53.5% |
| 2018 | 10 | **10** | $1.97bn | 2.1% | 49.2% |
| 2019 | 9 | **9** | $1.04bn | 1.1% | 49.0% |
| 2020 | 11 | **11** | $2.60bn | 3.0% | 50.4% |
| 2021 | 24 | **24** | $2.38bn | 2.1% | 56.3% |
| 2022 | 27 | **27** | $2.14bn | 1.5% | 55.3% |
| 2023 | 41 | **41** | $5.75bn | 5.4% | 49.1% |
| 2024 | 44 | **44** | $6.39bn | 6.3% | 51.1% |

A comparable purchaser spends about half its electricity bill on delivery. These
44 are modelled as buying generation and no delivery at all, which no real
purchaser does.

**This is where the method contradicts itself.** #88's guiding principle 6 and
the results deck both say the price differential between purchasers is precisely
what shows up in T&D — "Differences across end uses (homes vs industry, etc.)
show up in the T&D part of the electricity purchases." The flat price sets T&D
to **zero** for exactly those purchasers whose prices sit furthest below
average. The mechanism that is supposed to carry the price difference is
switched off precisely where the difference is largest.

## 3. The consequence is a doubled emission factor on those sectors

Generation carries essentially all of the direct emissions — the deck puts the
three children at **7.197 / 0.226 / 0.0** kg CO₂e per dollar. A purchaser with
zero T&D therefore buys nothing but the high-intensity product, and its blended
electricity emission factor is the pure generation factor:

| year | median blended EF, uncapped | capped | distortion |
|---|---:|---:|---:|
| 2017 | 3.35 | 7.20 | **2.15×** |
| 2018 | 3.66 | 7.20 | 1.97× |
| 2019 | 3.68 | 7.20 | 1.96× |
| 2020 | 3.58 | 7.20 | 2.01× |
| 2021 | 3.15 | 7.20 | **2.28×** |
| 2022 | 3.22 | 7.20 | 2.23× |
| 2023 | 3.67 | 7.20 | 1.96× |
| 2024 | 3.52 | 7.20 | **2.04×** |

The 2024 list is not obscure: other basic inorganic chemicals ($1.25bn bill),
paper ($0.74bn), ready-mix concrete ($0.49bn), primary aluminium ($0.39bn), corn
products, pharmaceutical products, soap and cleaning compounds, fabric,
adhesives, paints. These are electricity-intensive industries — which is the
point, because **their intensity is what makes their price low, and their low
price is what gets them capped.** The method penalises the sectors it was
extended to represent better.

## 4. It is getting worse, and it is coupled to a dollar total we know is wrong

The capped set has grown **six** purchasers to **44**, and `p` has climbed from
the 2nd to the 17th percentile of the price distribution. `p` is not free:

```
p = (2017 UGO generation-dollar share × 221100 Use+Y dollars) / eGRID MWh
```

The numerator is the electricity row's **dollar total**. So any error in that
total moves `p`, and `p` decides who is representable. Two movements already
measured in `electricity_row_control.py` push in exactly this direction:

- **2021-22**: BEA's own gross output for the electricity industry rose 20.5%
  then 15.4% on a fuel-cost pass-through. `p` rose 4.00 → 4.68 → 5.15 and the
  capped set went 11 → 24 → 27.
- **2023-24**: the electricity row lost a fifth of its share of intermediate use
  (issue **#896**), which lowers manufacturing bills and therefore lowers every
  purchaser's implied price. `p` *fell* to 4.84 and 4.67, and the capped set
  still rose to 41 and 44 — the distribution moved underneath it.

A method whose representable set depends on the level of a dollar row this
unsettled is fragile independently of whether the row is right.

## 5. The published validation cannot see any of this

The results deck validates on class ratios — Residential 1.000, Com+Ind+Trans+
Exports 1.000, Total 1.000 — against iteration 1's 0.532. That comparison is
real and it is the right test of what iteration 1 got wrong.

⚠️ But those class totals are **imposed by construction**. #88 D0 sets each
class's MWh as its EIA share of (eGRID − exports). A ratio of 1.000 confirms the
constraint binds; it is not independent evidence about the allocation. Every
finding above is *within* class, where nothing is checked.

## What is NOT wrong, for the record

**Water-fill does not undo the MECS reallocation.** The obvious suspicion — that
the cap reverses what MECS was adopted to do — is wrong, and was tested. MECS
moves 138.8m MWh off the dollar allocation in 2024; after water-fill,
**129.4m MWh (93.2%) of that movement survives**. The MECS weighting is doing
roughly what #90 intended.

So the case against the current method is *not* "MECS is being overwritten". It
is that the flat price makes 44 purchasers unrepresentable, zeroes their
delivery costs, and doubles their emission factor — and that this is a property
of the arithmetic rather than of the MECS weights.

⚠️ #90 M5 anticipated this failure mode in the record, while arguing to defer
MECS Table 11.3: assigning plants more MWh than their bill can pay for means
"published mixed-units MWh then follow the capped dollars, so those plants would
not actually keep the extra onsite kWh." The same exposure applies to Table 7.7,
which was adopted. The argument was made and not carried across.

## Why the proposal is not iteration 1 again

This is the objection the proposal has to answer, and the distinction is sharp.

**Iteration 1 used prices to move dollars between purchasers.** It changed who
pays what for electricity, and that is what broke the MWh pattern against EIA.

**The proposal changes no bill.** #88 D8 — each purchaser keeps its own
electricity dollars — is retained exactly. What changes is that generation is
priced by class rather than by one national scalar, and T&D becomes the explicit
residual:

```
gen_i  = kWh_i × p_gen(class)
T&D_i  = bill_i − gen_i     ( = kWh_i × (price_i − p_gen) )
```

This is the current method's own logic applied *within* class instead of only
*between* classes. And the within-class spread is the larger of the two:
manufacturing runs **1.8×** between price deciles, while the gap between the
commercial and manufacturing class prices the method does represent is
9.95 vs 6.86, or **1.45×**. The method resolves the smaller difference and
discards the larger one.

At any uniform `p_gen ≤ 4.21 ¢/kWh` no manufacturing industry goes negative; at
the current 4.67, three do. Water-fill becomes an exception rather than a
mechanism.

## Acceptance tests for a third iteration

Stated up front, because the risk is regressing to iteration 1:

1. **Class ratios stay at 1.000.** Residential and Com+Ind+Trans+Exports must
   still reproduce the EIA class mix. This is the iteration-1 regression test
   and it is non-negotiable.
2. **No purchaser is left with zero T&D**, and the capped set is empty or
   near it.
3. **No purchaser's blended electricity EF is the pure generation factor**
   unless it genuinely buys only generation.
4. The kWh-weighted aggregate of the per-industry price reproduces EIA's
   published industrial price every year, and reproduces MECS exactly at the
   2018 and 2022 anchors.
5. Whatever replaces `p` must be **stated as a function of an observed price**,
   not of the electricity row's dollar total, so that finding 4's coupling is
   broken.

## Reproducing

```
python -m bedrock.analysis.electricity.current.eia_gtd.flat_price_clipping \
    --mut-vintage v0.3.0_4276083 --csv
    # --check asserts the two claims the case rests on:
    #   clipped is exactly the below-p set, all 8 years, every purchaser
    #   every clipped purchaser has zero T&D
```

`--mut-vintage` only saves a GCS probe; the pinned build is the current
production one. The UGO generation share (0.3417) and T/(T+D) (0.0592) are
pinned in the module because `p` depends on them and a guess would change who
clips.
