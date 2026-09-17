# Why the current electricity disaggregation fails

**The case for a third iteration.** The current method pairs two decisions that
are individually well-argued and jointly inconsistent: one national generation
price (methods Discussion **#88**, D0 and D4) and MECS physical kWh inside
manufacturing (Discussion **#90**, M4). Together they make a growing set of
manufacturing purchasers impossible to represent — 6 in 2017, **44 in 2024** —
and hand every one of them an electricity emission factor about **twice** what a
comparable purchaser gets.

Points 1 to 5 are about the allocator. **Point 6 is about the nowcast
electricity row it divides**, which turns out to be the larger of the two
problems: most of the growth in point 4 comes from electricity purchases, not from the
price. Both have to move, and in that order.

Measured by `flat_price_clipping.py`. Companions: `About_electricity_shares.md`
(the three-source comparison), `About_price_proposal.md` (the proposed method),
`electricity_row_control.py` (what drives the row the allocator divides).

## Where the method came from

| | approach | headline outcome |
|---|---|---|
| **1st** (#85) | BEA gross output for the split; **EIA retail prices by end-use class** to turn dollars into MWh | ❌ Residential MWh came out **0.532×** EIA's. Non-electricity `N` moved ~14%. |
| **2nd** (#88 + #90) | EIA class shares set MWh; **one national generation price**; MECS kWh inside manufacturing | ✅ Class ratios 1.000. `N` moves ~4% (mixed units) / ~1.4% (reaggregated). |
| **3rd** (proposed) | keep every purchaser's electricity purchases; **price generation by class**, T&D as the residual | — |

The flat price was adopted *specifically* to fix iteration 1. The results deck
is explicit: "Main reason for decrease is we using one price for monetary to
physical conversion for generation, whereas before we were using several
different prices ... This also caused the misallocation of MWh in the model
relative to EIA end use sectors."

**That diagnosis was right, and nothing here argues for going back.** What
follows is that the cure has a defect of its own, in a different place.

## 1. A purchaser hits the cap exactly when its own electricity is cheaper than the national price

### How the method assigns generation dollars

Each purchaser is given a quantity of electricity in MWh. That quantity is
turned into generation dollars by multiplying it by `p`, the one national
generation price. Whatever is left of the purchaser's electricity purchases after
those generation dollars are taken out is recorded as transmission and
distribution.

This can ask a purchaser to pay more for generation than it spends on
electricity in total. The method handles that by **capping generation at the
purchaser's electricity purchases**: the whole of what it spends is charged to
generation and nothing to delivery. The generation dollars that did not fit are
then moved onto other purchasers in the same customer class, spread across
whatever room each has left between its own generation charge and its own
electricity purchases.

> #88 D8 calls that redistribution step "water-fill", and the code calls a
> purchaser whose generation was capped "clipped". This document says **capped**
> throughout.

### When the cap binds

Generation fits inside those purchases when

```
MWh × p  ≤  purchases
```

Divide both sides by MWh:

```
p  ≤  purchases / MWh
```

`purchases / MWh` is simply what that purchaser pays per kWh for electricity in
total, generation and delivery together — call it **the purchaser's price per
kWh**. So the cap binds whenever a purchaser's price per kWh is below `p`.

**That is a fact about the arithmetic, not about the data.** It does not depend
on MECS being right, on electricity purchases being right, or on `p` being well chosen. Any
purchaser that pays less per kWh than `p` cannot be represented: the model must
either charge it more for generation alone than it spends on electricity
altogether, which is impossible, or cap generation at the purchases and leave it with
no delivery costs. There is no third option.

### It holds exactly, every year

If the reasoning above is right, then two lists should match: the purchasers the
allocator actually capped, and the purchasers whose price per kWh is below `p`. They
do — for every Industrial purchaser, in all eight years:

| year | `p` ¢/kWh | purchasers | capped | price per kWh below `p` | purchasers where the two agree |
|---|---:|---:|---:|---:|---:|
| 2017 | 3.87 | 265 | 6 | 6 | **265 of 265** |
| 2018 | 4.05 | 264 | 10 | 10 | **264 of 264** |
| 2019 | 4.09 | 262 | 9 | 9 | **262 of 262** |
| 2020 | 4.00 | 260 | 11 | 11 | **260 of 260** |
| 2021 | 4.68 | 260 | 24 | 24 | **260 of 260** |
| 2022 | 5.15 | 260 | 27 | 27 | **260 of 260** |
| 2023 | 4.84 | 255 | 41 | 41 | **255 of 255** |
| 2024 | 4.67 | 265 | 44 | 44 | **265 of 265** |

Not approximately, and not most of them — the same purchasers appear in both
columns every time.

So the number of purchasers the method cannot represent is decided by one thing
only: **how many of them pay less per kWh than `p`.** In 2024, `p` sits at the
**17th percentile** of the price distribution, so roughly one Industrial
purchaser in six falls below it.

### A worked example: primary aluminium, 2024

| | |
|---|---:|
| electricity purchases | **$389.0m** |
| MWh assigned to it (MECS Table 7.7 shares) | 11.254m MWh |
| its price per kWh — $389.0m ÷ 11.254m MWh | **3.46 ¢/kWh** |
| national generation price `p` | **4.67 ¢/kWh** |
| generation it is therefore asked to pay — 11.254m × $46.70 | **$525.6m** |

The method asks primary aluminium to pay **$525.6m for generation alone against
$389.0m of total electricity purchases** — $136.6m more than it spends on
electricity altogether. That cannot be recorded, so generation is capped at the full
$389.0m, transmission and distribution are set to **$0**, and the $136.6m is
moved onto other purchasers in the same class.

Compare primary iron and steel in the same year, which pays **6.13 ¢/kWh** —
above `p`. It is asked for $2,614.6m of generation against $3,434.1m of purchases,
which fits, so it keeps **$802.5m** of transmission and distribution and is not
capped.

Nothing distinguishes these two cases except which side of 4.67 ¢/kWh the
purchaser's price per kWh falls on.

⚠️ **The example is not cherry-picked — it is the industry the method was
extended to serve.** Discussion #90's guiding principle 2 justified adopting
MECS by naming three industries: "Aluminum, chemicals, and paper should get more
generation MWh than their share of the electricity purchases would imply." In 2024
**all three are capped**: primary aluminium, paper ($743.9m of purchases, 4.15 ¢/kWh),
and **13 chemical industries** including other basic inorganic chemicals
($1,252.7m of purchases, 4.56 ¢/kWh), pharmaceutical products, adhesives and paints.
Giving those industries more MWh is exactly what pushes their price per kWh
below `p`, which is exactly what gets them capped.

## 2. Every capped purchaser is left with zero transmission and distribution

T&D is the residual of purchases after generation (#88 D8), so a cap that
consumes the whole of it leaves nothing. This is not a near-zero; it is zero, for
every capped purchaser in every year:

| year | capped | **with zero T&D** | their purchases | % of Industrial | median T&D share, uncapped |
|---|---:|---:|---:|---:|---:|
| 2017 | 6 | **6** | $3.87bn | 4.5% | 53.5% |
| 2018 | 10 | **10** | $1.97bn | 2.1% | 49.2% |
| 2019 | 9 | **9** | $1.04bn | 1.1% | 49.0% |
| 2020 | 11 | **11** | $2.60bn | 3.0% | 50.4% |
| 2021 | 24 | **24** | $2.38bn | 2.1% | 56.3% |
| 2022 | 27 | **27** | $2.14bn | 1.5% | 55.3% |
| 2023 | 41 | **41** | $5.75bn | 5.4% | 49.1% |
| 2024 | 44 | **44** | $6.39bn | 6.3% | 51.1% |

A comparable purchaser spends about half its electricity purchases on delivery. These
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

The 2024 list is not obscure: other basic inorganic chemicals ($1.25bn of purchases),
paper ($0.74bn), ready-mix concrete ($0.49bn), primary aluminium ($0.39bn), corn
products, pharmaceutical products, soap and cleaning compounds, fabric,
adhesives, paints. These are electricity-intensive industries — which is the
point, because **their intensity is what makes their price low, and their low
price is what gets them capped.** The method penalises the sectors it was
extended to represent better.

## 4. It has grown from 6 purchasers to 44, and mostly not for the reason you would guess

**Over 2017 to 2024, on the same model.** These are eight runs of the same
method over the eight nowcast years, not a change of method. The capped count
goes 6, 10, 9, 11, 24, 27, 41, **44**.

From point 1, a purchaser is capped when its price per kWh is below `p`. So there
are only two ways the count can grow:

- **the bar rises** — `p` goes up while purchasers' prices stay put, or
- **the distribution falls** — purchasers' prices per kWh go down while `p` stays put.

These can be separated by counterfactual. Take the purchasers present in both
2017 and year *t*, so the changing sector list contributes nothing, and count
how many would be capped if only one of the two had moved:

| year | capped | if only `p` had moved | if only prices had moved | growth from `p` | growth from prices | interaction |
|---|---:|---:|---:|---:|---:|---:|
| 2017 | 6 | 6 | 6 | 0 | 0 | 0 |
| 2018 | 10 | 6 | 10 | 0 | +4 | 0 |
| 2019 | 9 | 6 | 8 | 0 | +2 | +1 |
| 2020 | 11 | 6 | 11 | 0 | +5 | 0 |
| 2021 | 24 | 7 | 8 | +1 | +2 | +15 |
| 2022 | 27 | 20 | 2 | **+14** | −4 | +11 |
| 2023 | 41 | 8 | 20 | +3 | **+15** | +18 |
| 2024 | 44 | 7 | 30 | +1 | **+24** | +13 |

**The dominant cause is purchasers' prices per kWh falling, not `p` rising.** Of the
38 extra capped purchasers in 2024, `p` moving explains **1**, prices moving
explains **24**, and the remaining 13 need both. `p` itself is barely higher
than in 2017 — 4.67 against 3.87 — and in 2023 and 2024 it is *falling* while
the capped count keeps climbing.

### What fell was the bottom of the distribution, not the middle

| | 2017 | 2020 | 2022 | 2024 |
|---|---:|---:|---:|---:|
| `p` | 3.87 | 4.00 | 5.15 | 4.67 |
| 10th percentile of prices per kWh | **5.63** | 4.41 | 5.12 | **3.87** |
| median price per kWh | 8.15 | 7.94 | 11.57 | 8.33 |

The median is flat across the whole span. The cheap tail drops by a third. That
is what pushes purchasers under the bar.

### Why the cheap tail falls

A purchaser's price per kWh is `purchases ÷ MWh`. Both terms are exposed:

- **The MWh are frozen in relative terms.** #90 M6 uses the 2022 MECS survey for
  every year 2018-2024 with no interpolation, so how manufacturing MWh divide
  between industries does not move at all after 2018. Only the size of the pool
  moves.
- **Electricity purchases move with the nowcast Use row**, which
  `electricity_row_control.py` shows is unsettled: it lost roughly a fifth of
  its share of all intermediate use across 2023-24 (issue **#896**). Falling
  purchases over frozen MWh shares lower the cheap tail directly — and 2023 and 2024
  are exactly where the "growth from prices" column jumps to +15 and +24.

`p` has its own exposure in the years where it does the work. It is

```
p = (2017 UGO generation-dollar share × 221100 Use+Y dollars) / eGRID MWh
```

so its numerator is the electricity row's **dollar total**. In 2022 — the one
year where `p` is the main driver, +14 — BEA's gross output for the electricity
industry had risen 20.5% then 15.4% on a fuel-cost pass-through, and `p` went
4.00 → 4.68 → 5.15 with it.

**Either way the conclusion is the same.** Which purchasers the method can
represent is set by the electricity row's dollar level — through `p` in 2022,
through electricity purchases in 2023-24 — and that level is not a settled quantity.

## 5. The published validation cannot see any of this

The results deck validates on class ratios — Residential 1.000, Com+Ind+Trans+
Exports 1.000, Total 1.000 — against iteration 1's 0.532. That comparison is
real and it is the right test of what iteration 1 got wrong.

⚠️ But those class totals are **imposed by construction**. #88 D0 sets each
class's MWh as its EIA share of (eGRID − exports). A ratio of 1.000 confirms the
constraint binds; it is not independent evidence about the allocation. Every
finding above is *within* class, where nothing is checked.

## 6. Fixing the allocator alone will not work — electricity purchases it divides are defective too

Everything above is about the **allocator** (`allocate_purchaser_gtd`, the EEIO
side, Discussions #88 and #90). But the allocator does not decide how much
electricity anyone buys. It takes each purchaser's electricity purchases as given and
only splits it into generation and delivery. Those purchases come from the
**nowcast's electricity Use row**, which is a separate piece of work with
separate defects.

Point 4 is what makes this load-bearing rather than a caveat. **Of the 38 extra
capped purchasers in 2024, 24 come from purchasers' prices per kWh falling, and a
purchaser's price per kWh is `purchases ÷ MWh`.** Electricity purchases are doing
most of the damage, and they are not the allocator's to fix.

What has to move on the nowcast side, measured in
`electricity_row_control.py` and `About_electricity_shares.md`:

1. **The row loses a fifth of its share of intermediate use across 2023-24**
   (issue **#896**) — 2.02% of all intermediate use in 2022 to 1.70% and 1.54%,
   after six years steady at 1.82-2.03%. That fall lowers manufacturing purchases,
   which lowers prices per kWh, which is the +15 and +24 in point 4's table. **#896
   is on the critical path for this method, not beside it.**
2. **The commercial band has no electricity seed at all** and moves on the
   carry, so the larger half of the row is unanchored. Manufacturing at least
   rides on Census `CSTELEC` through `nonmaterial_seed`.
3. **The row is a residual.** `intermediate = q − Y` with final demand anchored
   to EIA and commodity output not, so every dollar of movement in the output
   control lands on electricity purchases the allocator divides — including BEA's 2021-22
   fuel-cost pass-through.
4. **Individual cells are wrong.** 322120 paper mills carries **$0** electricity
   in 2021 and 2023 in the pinned MUT. Paper is one of the three industries #90
   named, and one of the capped ones in 2024.
5. **The row divides differently from both dollar surveys.** IO versus Census
   sits at 12.96pp of the manufacturing total **at 2017 itself** and 11-15pp
   across the span, because the existing seed aligns how a column *moves* and
   never how the row *divides*.

`About_price_proposal.md` is organised in three sections — **A** the price, **B**
electricity purchases, **C** the split — and its section **B** is the proposed answer to 2, 3 and 5 —
realign manufacturing electricity to MECS levels at the 2018 and 2022 anchors
and index annually on Census between them. It is written, and it is where the
detail belongs; this document's point is that **fixing electricity purchases is not optional
alongside fixing the split.** A correctly priced allocator dividing a row that sheds a
fifth of its share in two years will still produce a growing capped set, because
point 4 shows that is exactly what has been driving the growth.

⚠️ Sequencing matters and cuts the other way too: **electricity purchases must be fixed
first, or the allocator's improvement cannot be measured.** If both move in one
change, a reduced capped count cannot be attributed to either.

## What is NOT wrong, for the record

**Water-fill does not undo the MECS reallocation.** The obvious suspicion — that
the cap reverses what MECS was adopted to do — is wrong, and was tested. MECS
moves 138.8m MWh off the dollar allocation in 2024; after the cap and the
redistribution that follows it,
**129.4m MWh (93.2%) of that movement survives**. The MECS weighting is doing
roughly what #90 intended.

So the case against the current method is *not* "MECS is being overwritten". It
is that the flat price makes 44 purchasers unrepresentable, zeroes their
delivery costs, and doubles their emission factor — and that this is a property
of the arithmetic rather than of the MECS weights.

⚠️ #90 M5 anticipated this failure mode in the record, while arguing to defer
MECS Table 11.3: assigning plants more MWh than their purchases can pay for means
"published mixed-units MWh then follow the capped dollars, so those plants would
not actually keep the extra onsite kWh." The same exposure applies to Table 7.7,
which was adopted. The argument was made and not carried across.

## Why the proposal is not iteration 1 again

This is the objection the proposal has to answer, and the distinction is sharp.

**Iteration 1 used prices to move dollars between purchasers.** It changed who
pays what for electricity, and that is what broke the MWh pattern against EIA.

**The proposal changes no purchaser's electricity purchases.** #88 D8 — each purchaser keeps its own
electricity dollars — is retained exactly. What changes is that generation is
priced by class rather than by one national scalar, and T&D becomes the explicit
residual:

```
gen_i  = kWh_i × p_gen(class)
T&D_i  = purchases_i − gen_i     ( = kWh_i × (price_i − p_gen) )
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

Stated up front, because the risk is regressing to iteration 1.

**On the allocator** — `About_price_proposal.md` section C, the split:

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
   not of the electricity row's dollar total, so that point 4's coupling is
   broken.

**On the nowcast electricity row** — section B, electricity purchases the allocator divides,
which has to move too:

6. **The row's share of all intermediate use is stable across the span**, or its
   movement is attributed to something observed. The 2.02% → 1.70% → 1.54% break
   is what drives most of point 4's growth (**#896**).
7. **No purchaser carries $0 electricity in a year it operated** — 322120 paper
   mills in 2021 and 2023 today.
8. **The commercial band is seeded**, rather than carried, so the larger half of
   the row responds to something observed.
9. **IO versus Census closes materially below 12.96pp at 2017**, the point at
   which the row divides like neither dollar survey.

**On the order of work:**

10. Electricity purchases first, allocator second, each measured on its own. If
    both move in one change, a reduced capped count cannot be attributed to
    either.

## Reproducing

```
python -m bedrock.analysis.electricity.current.eia_gtd.flat_price_clipping \
    --mut-vintage v0.3.0_4276083 --csv
    # --check asserts the two claims the case rests on:
    #   the capped purchasers are exactly those priced below p, all 8 years
    #   every capped purchaser is left with zero T&D
```

It also prints the counterfactual decomposition behind point 4, and `--csv`
writes it to `flat_price_clipping_growth.csv`.

`--mut-vintage` only saves a GCS probe; the pinned build is the current
production one. The UGO generation share (0.3417) and T/(T+D) (0.0592) are
pinned in the module because `p` depends on them and a guess would change who
clips.
