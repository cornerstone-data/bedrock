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

⚠️ **Corrected against BEA's own description of the method** (W. Nicolls, Section
Chief for distributive services, 2026-08-11, replying to the question recorded in
`7488437`). What follows replaces the reading taken from the manual, which had a
single volume measure allocating every mode. Two of the four modes BEA described
are not allocated on volume at all.

**The target is a percentage split, not a level.** *"We really aren't looking for
a specific value since that is already measured in margin output, we are simply
trying to estimate the percentage of that margin output that is distributed to
each commodity."* The anchor-plus-growth construction therefore stands; what
changes is the vector that carries it.

**Modes are never combined.** *"We do not combine modes and we ignore multi-modal
reported data since we cannot differentiate those in margin output."* Each mode's
margin output is spread only over the freight moving by that mode, on its own
basis:

| mode | 2017 `TRANS` | share | BEA's basis | source | our status |
|---|---:|---:|---|---|---|
| Truck `484000` | −281,589 | **67.8%** | **revenue by product**, ten groups; "other" discarded | SAS Table 8 → AIES `miscsector` | source found; concordance to build |
| Rail `482000` | −68,590 | 16.5% | **revenue by product** | AAR Freight Commodity Statistics (purchased); STB Commodity Revenue Stratification Report | STB is the public route; not extracted |
| Pipeline `486000` | −49,660 | 11.9% | **four Census margin items → named commodity sets, proportional** | Census margin items only — no external source | **fully specified**, §Pipeline |
| Water `483000` | −9,506 | 2.3% | ton-miles **× difficulty 1/2/3** | BTS/Census CFS | rule known, table not needed |
| Air `481000` | −6,225 | 1.5% | ton-miles × multiplier (animals only) | BTS/Census CFS | rule known |

⚠️ **Every mode now has a stated method** (second Nicolls reply, 2026-08-17).
What remains is sourcing, not method: the group → I-O concordance BEA will not
publish, rail's revenue source, and the 2023 splice.

The difficulty multiplier weights ton-miles rather than replacing them, so the
water and air allocator is a weighted ton-mile share,
`m_c · tonmiles_c ÷ Σ m_i · tonmiles_i` with `m ∈ {1,2,3}`. It encodes how hard
cargo is to rearrange at port. **The table is unpublished but the rule is now
specified finely enough to reproduce it** — *"I can't share the table with you,
but I can give you a push in the right direction… That should get you pretty
close"*:

| | 1 | 2 | 3 |
|---|---|---|---|
| **air** | everything else | — | **animals** |
| **water** | sits loose in the hold (grain, oil) | palletized or containerized | **motorized vehicles and transport**; heavy machinery |

Note this **fixes air's multiplier at 3**, which the first reply left unstated,
and adds motorized vehicles to the first reply's "heavy machinery" at the top of
the water scale. The weights are refreshed every five years, so they are a
constant in any annual construction.

**This reorders the work.** Water and air together are 3.8% of `TRANS` and their
multipliers no longer block anything. Pipeline, 11.9%, went from the largest
unmethodded piece to fully reproducible with **no external source at all**. Truck
alone is more than two thirds, and after the "other goods" answer below its only
remaining unknown is a concordance we must build ourselves.

**No separate handling of local or small shipments.** *"We do not handle local or
small shipments separately… The specific cost of a commodity doesn't figure in to
the calculation, just the cost to ship it reported in the data."* This kills the
hypothesis that the published margin carries distribution cost that line-haul
volume cannot see. The under-allocation to light high-value commodities has a
different cause, given below.

The old reading is still half-visible in the 2017 data — `TRANS`/`T013` orders
almost perfectly by weight-per-dollar:

| highest | | lowest | |
|---|---:|---|---:|
| Coal mining | 49.1% | Motion picture and video | 0.04% |
| Other nonmetallic mineral mining | 45.4% | Sound recording | 0.16% |
| Scrap | 40.6% | Magnetic/optical media | 0.82% |
| Cement | 20.2% | Ship building | 0.89% |

Ship building at 0.89% is the confirmation: heavy, but self-propelled, so it
consumes no freight. **Any rebuild must be ton-weighted, not value-weighted** —
inverting that would swap coal and pharmaceuticals.

**But weight is not the allocator, and that is what the 2017 fit could not
see.** Ton-miles beat tons on the aggregate fit, and the conclusion drawn from
that — allocate everything on ton-miles — was wrong, because the fit pooled four
modes whose mechanisms differ and for the two largest the right answer is not a
volume measure at all. It was a contest between two wrong answers, so the winner's
margin told us nothing.

**That also explains the residual the reconstruction could not.** Rail and truck,
84% of `TRANS` between them, are allocated on **revenue**, and revenue per
ton-mile is far higher for light high-value freight than for bulk. A pure
ton-mile allocator starves exactly those commodities, by roughly the ratio of
their freight rate to the bulk rate — which is the 5–8× under-allocation observed,
and the reason it looked like a missing local-delivery term.

**FAF still has a job, a smaller one.** It remains the right ton-mile source for
water and air, where BEA does allocate on volume. Note the FAF-over-CFS choice was
made for an all-mode allocator that no longer exists — BEA uses CFS — so the "CFS
has zero coverage of the largest `TRANS` commodity" objection needs re-testing
against water and air alone, where it may not bite.

### The truck source, measured — and its two problems

SAS Table 8, *Estimated Revenue by Product and Class of Customer for Employer
Firms*, is the table BEA named. Measured directly:

- **Only NAICS 484 carries commodity rows.** Rail, water, air and pipeline are
  absent from Table 8 entirely, so BEA's use of four different sources is forced,
  not preferred. There is no single-source shortcut.
- **Eleven commodity groups, and they partition exactly.** They sum to
  `Total Motor Carrier Revenue` to the dollar in every unsuppressed year
  (2015–2021 gap 0; 2022 has one suppressed cell, pharmaceutical 18,004,
  recoverable by subtraction from the published total — the same treatment
  §Two gaps that are not bugs specifies for retail, which arrives with
  [#628](https://github.com/cornerstone-data/bedrock/pull/628)).
- **Commodity detail runs 2015–2022 only.** Nothing for 2013–14.
- **AIES continues it.** `timeseries/aies/miscsector` publishes the same eleven
  groups plus the total and hazardous materials as `RCPT_MOTR_*` variables, and
  they still sum exactly. But **2023 only** — every other year returns 204
  despite the catalogue advertising 1992–2023, exactly as for `aies/basic`. So
  **2024 has no truck source**, the same gap already noted for trade.

### ✅ Problem 1 — "Other goods" — closed: BEA discards it

"Other goods" runs 32–34% of the total in every year (30.6% in 2023), and against
truck's 67.8% of `TRANS` that is ~22% of the entire transport margin. It was the
single largest unknown in the transport chain. The answer is that there is no
method step to reproduce, because BEA does not use the bucket at all:

> *"We do not use the 'other' commodity from SAS Table 8 since we have no
> information on what commodities it contains. Distributing it pro rata to the
> other 10 would not change the result since we are creating weights with the
> data to distribute our truck margins rather than explicitly using the values
> from SAS table 8."* — W. Nicolls, BEA, 2026-08-17

**The reason it is harmless is the anchor-plus-growth construction, and it is
worth being explicit about why.** Table 8's values are never spent; only their
*shares* are. So the allocator is

```
share[g] = revenue[g] ÷ Σ_{g ≠ other} revenue[g]      over the ten identified groups
```

and renormalising over ten groups is arithmetically identical to distributing
"other" pro rata across them. BEA's own reply makes that equivalence explicit, so
**dropping "other" is not an approximation of the method — it is the method.**
The one assumption it carries is that "other goods" freight has the same
commodity mix as identified freight; BEA accepts that assumption rather than
avoiding it, and there is no published basis on which we could do better.

⚠️ **Do not reintroduce it later as a refinement.** A future source that gave
"other goods" a commodity identity would make our allocator *diverge* from BEA's
rather than improve on it, and the 2017 anchor is BEA's published column.

**Also confirmed: the source reading was right.** Table 8 is the table BEA means,
the eleven published groups are what BEA uses, and there is no finer internal
tabulation to ask for. "Basically at a sector level" in the first reply was
aggregation-level language — BEA uses NAICS codes for commodities and industries
alike and says sector/summary/detail for the levels — not a claim that the rows
are industries. *"The data are commodity data in SAS Table 8 and we do use the
published data."*

### ⚠️ Problem 2 — the shares step across the AIES splice, and this one stays open

**Asked, and not answered.** Question 4 of the 2026-08-14 follow-up put this
directly to BEA — whether 2023 is continuous with the earlier series or rebased,
and how they handle a year like 2024 before the next AIES release — and the
2026-08-17 reply does not address it. It is now the **only unanswered method
question** in the transport chain.

The level falls 17.8% (414,693 → 340,969), which does not matter because the
level comes from the 2017 anchor. The shares do matter, and they step:

| | mean \|Δpp\| | max \|Δpp\| |
|---|---:|---:|
| 2021 → 2022, within SAS | 0.34 | 1.06 |
| 2022 → 2023, across the splice | **1.39** | **3.21** |

Biggest movers are "Used household and office goods" (4.43% → 6.90%) and "Coal
and petroleum" (4.63% → 6.64%), against a taxonomy whose shares are otherwise
slow — "Other goods" moved only 32.4% → 34.3% across 2017–2021. Some of this is
plausibly the 2023 freight recession landing unevenly, but four times the normal
volatility located exactly at the survey consolidation is the same signature as
the retail rate step, and takes the same treatment: do not splice silently.

### The concordance is internal, and we have to build our own

**These groups are a bespoke Census taxonomy** — neither SCTG nor NAICS ("Grains,
alcohol, and tobacco products", "Base metal and machinery", "Used household and
office goods"). Asked whether the mapping is published:

> *"The SAS group to IO items concordance is not published. It is internal to our
> database. We remap it every 5 years and we are in that process now."*
> — W. Nicolls, BEA, 2026-08-17

So it is structurally the same missing link as NAPCS → I-O on the trade side
([#615](https://github.com/cornerstone-data/bedrock/issues/615)), with one large
practical difference: **this one is ten groups, not 29,098 rows.** Building it by
hand against BEA 2017 detail is an afternoon, not a project, and unlike the NAPCS
concordance there is no alternative — the 2017 anchor gives us the *commodity*
allocation but not the *group* it came from, so nothing substitutes for the map.

Two consequences worth recording now. The mapping is many-to-many and the groups
are coarse, so a group's margin must be spread across its member commodities on a
weight; interim supply is the natural one, matching what BEA's wholesale step (4)
does. And BEA is remapping *right now* on the five-year cycle, so a 2022-vintage
concordance built today will differ from theirs at the next benchmark — a reason
to keep ours in a reviewable CSV rather than in code.

### Pipeline — fully specified, and the only mode needing no external source

11.9% of `TRANS`, more than water and air combined, and the first reply did not
cover it. The second does, completely:

> *"Pipeline is a bit different from our other transportation margins in that
> there is no outside source that we need to tell us where to distribute the
> pipeline margins. The margin values, like the other transportation margins,
> come from Census and there are 4 pipeline margine items… These margins are all
> distributed proportionally to the commodities to which they are assigned."*
> — W. Nicolls, BEA, 2026-08-17

| Census margin item | goes to | BEA's words |
|---|---|---|
| Natural gas pipe | natural gas | *"Natural gas pipelines margins go to natural gas commodity"* — clearest |
| Crude oil pipe | any crude oil commodity | *"Crude pipelines margins go to any crude oil commodity"* — clearest |
| Refined petroleum pipe | refined fuels | *"gasoline, jet fuel, kerosene and other refined oils and waxes"* |
| Pipeline, NEC | chemicals moved by pipe | *"dyes, pigments, toners, ammonia, Urea and like products that would be transported through a pipeline"* |

### ✅ Pipeline — built

[`nowcast_transport_margins.py`](../../transform/iot/nowcast_transport_margins.py).

**The four margin items are the four detailed pipeline NAICS**, and Census
publishes them annually in **Service Annual Survey Table 2**: `4861` crude oil,
`4862` natural gas, `48691` refined petroleum products, `48699` all other. They
partition NAICS 486 to the dollar in every year — 10,529 + 27,393 + 8,898 + 372
= 47,192 in 2017 — and the loader raises if they stop, since that would mean the
published taxonomy had drifted off the items BEA named. Table 2 is a `sheets:`
entry, the same shape truck needs from Table 8.

⚠️ **Correction to the sizing above: this was not "Census margin data we already
reach".** SAS carried only Table 3 (expenses) before this; Table 2 had to be
added. The mode is still the cheapest — no ton-mile file and no negotiated
concordance — but the source was one step further away than first written.

**Crude and natural gas collapse onto one commodity**, and that is most of why
this mode is cheap. BEA 2017 detail carries a single `211000` *Oil and gas
extraction*, so the two clearest items — 80.4% of pipeline revenue — land
together and their split never affects the allocation. Only a three-way split
does any work. `221200` natural gas distribution is deliberately unmapped: it is
the utility, not the commodity, and receives no `TRANS`.

**✅ The two judgement calls, settled and agreed:**

- **`324121` asphalt paving and `324122` asphalt shingles are excluded** from the
  refined set. Both are refinery products and both receive `TRANS`, but neither
  moves by pipeline; including them would divert ~1,000 million.
- **"And like products" is not expanded** beyond BEA's three named exemplars
  (`325130` dyes and pigments, `325910` toners, `325310` ammonia and urea). The
  candidates left out — `325110`, `325120`, `325180`, `325190` — hold 13,917
  million of `TRANS` between them, so adding them would thin `48699` sharply for
  no stated reason. Worth ~391 million either way.

Both are recorded per row in
[`Crosswalk_Pipeline_Margin_Items_to_BEA_2017.csv`](../../utils/mapping/Crosswalk_Pipeline_Margin_Items_to_BEA_2017.csv),
which carries a `basis` column a test enforces.

### ⚠️ The published column is a ceiling, not an answer key

An earlier draft of this section said pipeline's 2017 `TRANS` by commodity "is
the answer key, so the weight can be fitted rather than assumed". **That is
wrong, and it matters for every mode, not just pipeline.** The published
`Transportation` column is summed over all five modes, so no single mode can be
fitted to it. What it gives is a *bound*: a mode's allocation may not exceed it,
because the remaining modes cannot contribute negative margin.

`pipeline_bound_check` reports it, and the 2017 fit is comfortable everywhere —
83.8% of `211000` (pipeline dominates crude and gas haulage), 34.3% of the
refinery commodities, 17.7% of the NEC chemicals, positive headroom throughout.

**So the five modes are one decomposition, and a mode built alone is
provisional.** For every commodity, `Σ_modes allocation = TRANS[c]`.
`mode_residual` makes that explicit: it reports what the unbuilt modes must
still supply and flags an over-allocation as a hard error. Pipeline is not final
until truck, rail, water and air are reconciled against the same column.

**Proportional to what?** BEA does not name the weight. The default is the
published column, which inherits BEA's own commodity allocation for the same
reason the anchor-and-move construction does; it is injectable so `T013` or FAF
pipeline-mode ton-miles can be substituted. ⚠️ The default does assume pipeline's
mix *within* a set matches the all-mode mix — the one place this leans on other
modes' behaviour — but it is bounded and measured: it cannot touch the 80.4%
going to `211000`, and across the whole column the published-vs-`T013` choice
moves at most **536 million, 1.1% of pipeline and 0.13% of `TRANS`**, all of it
between `324110` and `324190`.

⚠️ **This supersedes the closed PR's `output_ratio` treatment for pipeline**,
which existed only because a residual went negative — pipeline gives up 1.033 of
its own gross output. That was a workaround for a construction BEA does not use.

### Phase 2 — all five modes built, and the annual control solved

[`nowcast_transport_margins.py`](../../transform/iot/nowcast_transport_margins.py).
Each mode is allocated on the basis BEA uses for it, controlled to its own annual
total, and `transport_margin_column(year)` assembles the Supply `TRANS` column —
positive on receivers, negative on the five modes, **summing to zero**, which is
target T16. Wired into `derive_initial_supply_bridge` for **2017–2022**.

| mode | share | allocator | source |
|---|---:|---|---|
| truck | 67.8% | revenue by commodity group, ten used, "other" discarded | `Census_SAS` Table 8 |
| rail | 16.5% | revenue by product, 521 STCC5 codes | `STB_CRSR` |
| pipeline | 11.9% | four Census margin items to named commodity sets | `Census_SAS` Table 2 |
| water | 2.3% | ton-miles times difficulty 1/2/3 | `BTS_FAF` |
| air | 1.5% | ton-miles times difficulty 1/2/3 | `BTS_FAF` |

**The annual control is observed freight revenue, not a residual.** The manual
settles it: trade output *is* margin, but transport output is receipts and only
the part "unbundled and shifted forward" is margin — which for a freight mode is
essentially all of it. Measured, 2017 margin over freight revenue is 0.995 rail,
1.042 truck, 1.052 pipeline. The control is that ratio frozen at 2017 times
observed revenue, so 2017 is an identity and the level comes from the same source
as the shape.

This **replaces the retired chain's three treatments**, which agreed in 2017 and
spread 11.8% by 2022. It needs no Use matrix (no circularity with Step 6b), no
industry-versus-commodity output reconciliation, and carries no passenger
contamination.

⚠️ **Water and air ratios mean something different** — 0.478 and 0.584, because
their freight revenue includes international legs while the margin is the
domestic leg only. Bounded by their 3.8% share, and tested: deep sea, domestic
and all-freight water revenue have near-identical volatility, so the split is not
a distinct source of movement.

⚠️ **2023–24 are unsourced.** SAS stops at 2022; AIES `miscsector` carries 2023
and is not wired; 2024 is unpublished. Rail alone reaches 2024.

### ⚠️ The open defect — the five modes collide per commodity

Each mode's total is right and the column identity holds, but summed per
commodity the five exceed the published 2017 column on **97 of 258 commodities by
45,232 million, 10.9%**. Essentially all of it is truck: pipeline and rail alone
over-allocate nothing.

The cause is that truck's commodity detail comes from a *weight* — ten groups
span 258 commodities — and the default weight is each commodity's total published
`TRANS`, which is blind to the modes already occupying it. Where rail is heavy,
truck stacks on top. Two failure shapes: coal is over-allocated before truck is
added at all, while iron and steel takes 104% from truck alone.

⚠️ **One group cannot hold its share on the current mapping.** "Base metal and
machinery" demands 104% of the entire published `TRANS` of every commodity in it,
before any other mode takes a share; four more sit at 86–90%. No within-group
weight fixes that — either the mapping is too narrow, or a group's revenue share
is not meant to carry that share of the whole truck margin.

**A feasible allocation provably exists** — Hall's condition holds over all 32
subsets of modes, so the constraint system is satisfiable and the collision is an
artifact of independent construction, not of the data. Put to BEA on 2026-08-19
(see [`bea_correspondence.md`](bea_correspondence.md)): what weights allocate
within a SAS group, or which commodities are excluded from which modes, and
whether they rebalance at the end.

⚠️ **In a nowcast year there is no published column to violate**, so this does not
block the build. Whatever resolves it changes how the positive side is
distributed without changing its total, its signs, or the identity.

### `Margins_Transport_<year>` FBS — built, all five modes

[`bedrock/transform/margins/`](../../transform/margins/) (`README.md` there is
the reference). `Margins_Transport_2017` through `_2022`, one activity set per
mode, output as a **BEA commodity matrix**: `SectorProducedBy` is the commodity
that receives the margin, `SectorConsumedBy` the transport commodity that gives
it up, `Flowable` that commodity's name. Summed over `SectorConsumedBy` it is
the Supply `TRANS` column; kept apart it is the expanded margins file, which is
what the FBS exists for.

Every mode reproduces its published 2017 give-up **exactly**, and a nowcast year
reaches `mode_control_total` exactly — 2022 checked as well as 2017.

**The division of labour.** The *module* owns each mode's allocator series and
its control total: the suppressed SAS truck group recovered by subtraction, the
"other goods" group BEA discards, the STCC codes that name a service class, the
difficulty multiplier, and the 2017-anchored coverage ratio. None of that is
expressible in a yaml. The *FBS* owns the split from the source's own key to BEA
commodities — ten SAS groups, 498 STCC codes, four pipeline items and 42 SCTG
groups onto 258 receiving commodities — via the crosswalks and proportional
attribution against the published `TRANS` column. A test asserts the two agree,
which is what keeps the division honest. **This is the split `Margins_Trade`
will need too.**

⚠️ **The two paths agree to 0.3%, not exactly, and the reason matters.** The
within-group weight is BEA's published 2017 transport margin. The FBS reads it
off `BEA_Detail_Supply`'s `TRANS` column, published in whole millions; the module
sums it out of the Margins transaction table, which is finer. Same quantity,
different publication rounding.

**Water and air needed no exception after all.** The 1/2/3 multiplier is applied
to ton-miles in the clean function, on the source side, rather than dressed up as
an attribution source — which is the honest place for a weight the attribution
model has no slot for.

**Three things the build turned up**, each fixed:

- `Census_SAS` had **no `source_catalog.yaml` entry at all**. Added as
  `activity_schema: null`, `sector_hierarchy: flat` — its activities are NAICS in
  Table 2 and prose group names in Table 8, so neither schema covers it.
- `BTS_FAF` carried `activity_schema: None` as the **string**, which the legacy
  guard in `map_to_sectors` rejects. It had never been exercised by an FBS.
- `!include:` opens files **without an explicit encoding**, so an included yaml
  must stay ASCII on Windows. `Margins_Transport_common.yaml` therefore writes
  `CAUTION:` where the rest of the repo writes ⚠️.

**Crosswalks.** Three `Sector_Crosswalk_*` files, generated by
[`write_margins_sector_crosswalks.py`](../../utils/mapping/write_margins_sector_crosswalks.py)
from the existing `Crosswalk_*_to_BEA_2017.csv` files. They hold no judgement of
their own; a test fails if a source crosswalk is edited without re-running it.

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
| `BTS_FAF` extractor + `Transport_Margins_2017.yaml` | flowsa **`margins`** branch | Built, but ton-miles is the right basis for **water and air only** — see §Transportation |
| `NAICS_Crosswalk_FAF_Mode_and_SCTG.csv` | flowsa `margins` | Built |
| **SAS Table 8** — truck revenue by commodity | `Census_SAS`, sheet `Table 8` | **Published, 2015–2022.** The extractor reads sheet `Table 3` today; Table 8 is a `sheets:` entry away |
| **AIES `miscsector`** — truck revenue by commodity, continued | Census API `timeseries/aies/miscsector` | **Published, 2023 only.** `RCPT_MOTR_*_DVAL`, same eleven groups |
| **STB Commodity Revenue Stratification Report** — rail revenue by commodity | Surface Transportation Board | Public; **not extracted**. AAR's equivalent is proprietary |
| Pipeline allocation basis | — | **Nothing identified** |
| Water/air difficulty multipliers (1/2/3) | BEA, internal | **Table unpublished, rule obtained** 2026-08-17 — reproducible without it. Governs 3.8% of `TRANS` |
| Pipeline margin items (4) → commodity sets | Census margin data | **Method obtained** 2026-08-17. No external source needed; §Pipeline |
| SAS ten-group → I-O commodity concordance | — | **Missing, and BEA's is internal.** Must be built by hand; governs truck's 67.8% |
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
across commodities. ~~Transport is solved end-to-end by FAF.~~ **No longer true** —
see §Transportation: FAF covers the `TRANS` *suppliers* completely, but supplying
the allocation *within* each mode needs revenue data for truck and rail, and FAF
is the right source only for water and air. For trade, **most of BEA's
product-line machinery already exists** and only one link is missing.

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
| 2 | [#611](https://github.com/cornerstone-data/bedrock/issues/611) ~~port the FAF transport chain~~ → build a per-mode allocator; see §Transportation | #601 (merged) |
| 3 | [#612](https://github.com/cornerstone-data/bedrock/issues/612) AWTS/ARTS annual trade levels | — |
| 4 | [#613](https://github.com/cornerstone-data/bedrock/issues/613) apply and derive `TRADE`/`TRANS` | #610–#612, **#570 (4a), #579 (4b), #580 (4d)** |
| 5 | [#614](https://github.com/cornerstone-data/bedrock/issues/614) validate per commodity | #613 |
| — | [#615](https://github.com/cornerstone-data/bedrock/issues/615) re-run BEA's product-line method | **deferred, candidate for a Phase 3 of the project** |

**Phase 1 — 2017 rates and the receiving sets.** Built; see §Phase 1 below for
what it produced and what it settled.

**Phase 2 — build the transport allocator, one basis per mode.** ⚠️ **Rescoped.**
The first attempt ported flowsa's `BTS_FAF` + `Transport_Margins_2017.yaml` and
allocated every mode on ton-miles; BEA's reply retired it and the PR was closed
([#627](https://github.com/cornerstone-data/bedrock/pull/627), branch
`faf_transport_margins` retained at `b94913f`). What survives from it: the
`BTS_FAF` extractor fix, `NAICS_Crosswalk_FAF_Mode_and_SCTG.csv`, the per-mode
framing, and the anchor-plus-growth construction. What replaces the allocator, in
descending order of what it is worth:

1. **Pipeline, 11.9% — build this first.** Fully specified, no external source,
   no concordance to negotiate: four Census margin items onto named commodity
   sets, proportional within each (§Pipeline). It was the largest unmethodded
   piece a week ago and is now the cheapest to land, so it is the natural first
   increment and proves the per-mode framing end to end on a mode that needs
   nothing extracted.
2. **Truck, 67.8%** — revenue by commodity group. Source found: SAS Table 8
   (`Census_SAS`, sheet `Table 8`) for 2015–2022, continued by AIES
   `timeseries/aies/miscsector` (`RCPT_MOTR_*_DVAL`) for 2023. The "other goods"
   problem is closed — discard it and renormalise over the ten identified groups,
   which is BEA's own method. What is left is the ten-group → I-O concordance,
   which BEA will not publish and which we must build by hand.
3. **Water and air, 3.8%** — weighted ton-miles, `m ∈ {1,2,3}`, and the rule is
   now fine enough to reproduce without BEA's table (§Transportation). Promoted
   above rail because the `BTS_FAF` extractor and its crosswalk already exist on
   `faf_transport_margins` at `b94913f` and only need cherry-picking.
4. **Rail, 16.5%** — revenue by product, and now the **only mode with no
   identified public source**. AAR is proprietary; the STB Commodity Revenue
   Stratification Report is the untested public substitute. Last not because it
   is small but because it is the one piece that may not exist.

**What the closed PR established that a rebuild must not rediscover.** The
allocator was wrong; the plumbing under it was not, and these findings cost real
time to reach:

- **`BTS_FAF` needs two departures from the flowsa original.** No `tabula`/PDF
  scrape — the SCTG and mode code tables ship inside the archive as
  `FAF5_metadata.xlsx`. And **all trade types**, not `trade_type == 1`: FAF's
  `dms_*` fields already describe only the domestic leg, so filtering discarded
  20% of ton-miles, concentrated in imports, rather than avoiding a double count.
- **`faf_parse` filtered on a year `call_all_years: True` never passes it.** The
  generator parses once with `year=None` and slices each year out itself, so **no
  FBA could be generated at all** until that was fixed.
- **`BTS_FAF` was missing from `source_catalog.yaml`**, without which any FBS
  reading it fails on `data_format`.
- **Do not port flowsa's `move_SCB_to_flow`.** It overwrites `Flowable` with the
  NAICS sector, which would silently coarsen every growth factor.
- **The FBS method name determines its folder.** `return_folder_path` walks the
  name backwards on `_`, so `Margins_Transport_<year>` requires
  `transform/margins/` — and so will Phase 3's `Margins_Trade_<year>`.

The `BTS_FAF` extractor and `NAICS_Crosswalk_FAF_Mode_and_SCTG.csv` are retained
on `faf_transport_margins` at `b94913f` and should be cherry-picked rather than
rewritten when water and air are built.

**The BEA-code constraint is easing.** The closed PR had to work around
`SectorConsumedBy` resolving to NAICS (`31212`, `11111`) — an SCTG mapped to a
3-digit NAICS group would be spread across everything in it — by aggregating the
FBS back over its sector columns to the SCTG surviving in `Flowable` and joining
to BEA detail in Python.
[#631](https://github.com/cornerstone-data/bedrock/pull/631) lands a pure BEA
commodity target (`BEA_detail_commodity_target.yaml`,
`default_schema: bea`), so a transport method can now target BEA detail directly
and move that join into the FBS. This is the same dependency as open question 2:
the SCTG → BEA crosswalk codes only become operative once a BEA-code target
schema is available.

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

## Phase 3 — the trade levels, and what they turned out to be

The three extractors are built:
[`Census_AWTS`](../../extract/census/Census_AWTS.py),
[`Census_ARTS`](../../extract/census/Census_ARTS.py) and
[`Census_AIES`](../../extract/census/Census_AIES.py), giving a continuous annual
wholesale and retail gross margin for **2012–2023**.

**The annual economic surveys were consolidated.** From data year 2023 AWTS,
ARTS, ASM and SAS became the **Annual Integrated Economic Survey**, so the two
standalone surveys stop at 2022 and `timeseries/aies/basic` carries 2023 on the
Census API. The issue was written against flowsa's 2012–2022 picture and did not
know this.

⚠️ **The splice only works at the right type of operation.** AIES publishes
wholesale gross margin under `TYPOP` code `1X`, merchant wholesalers excluding
manufacturers' sales branches and offices, and **zero** under the all-types code
`00`; retail is the other way round, published under `00`. Reading either at the
wrong code returns a well-formed zero rather than an error, silently deleting one
side of the trade margin. `1X` is also exactly what the AWTS workbook is — its
`nomsbo` table — so the two are the same basis rather than merely similar.

### These are index numbers, not levels

| 2017, $M | Census survey | BEA published column | Census / BEA |
|---|---:|---:|---:|
| Wholesale, NAICS 42 published row | 1,100,925 | 1,894,329 | **0.581** |
| Retail, published `Total` row | 1,458,243 | 1,761,765 | **0.828** |

**Wholesale is barely half of BEA's column**, because AWTS covers merchant
wholesalers only while BEA's Wholesale margin also carries MSBOs and
agents/brokers. So Phase 3 hands Phase 4 a **growth factor applied to the 2017
BEA level**, not a replacement level — the same anchor-and-move construction
Phase 2 used for transport, and for the same reason. Substituting the Census
level directly would delete 42% of the wholesale margin.

⚠️ **Read the published total row, never the sum of sub-industries.** The
wholesale figure above was previously quoted as 1,043,789 (ratio 0.551), which is
the sum of the 18 four-digit codes, not the published NAICS 42 row. The two are
not a fixed distance apart, because suppression varies by year:

| 4-digit sum ÷ NAICS 42 row | 2012–16 | 2017 | 2018 | 2019–21 | **2022** |
|---|---:|---:|---:|---:|---:|
| wholesale | 1.000 | 0.948 | 0.941 | 1.000 | **0.839** |

That is not a rounding difference, it is a **17.8pp error in one year of the
growth factor**: on the 4-digit sum 2021→2022 reads −6.4%, while the published
row moves +11.4%. The retail side has the same hole in the same year, 0.940. So
#613 must build the index off the `Total` / NAICS 42 rows the extractors already
emit, and the "suppression subtracts from a control total" warning below is a
specification for the consumer, not a caveat about the extract.

### The 850,540 wholesale gap, decomposed — and why it stays open

Chased to the bottom, because "add the missing types of operation" is the
obvious move and it does not work.

| 2017, $M | sales | gross margin |
|---|---:|---:|
| Merchant wholesalers, AWTS `nomsbo` | 5,704,275¹ | 1,043,789² |
| Merchant wholesalers, Economic Census `ecnmargin` `TYPOP 10` | 5,700,967 | **1,563,667** |
| Sales branches, with stock (`TYPOP 21`) | 1,330,238 | **not published** |
| Sales offices, without stock (`TYPOP 22`) | 1,001,003 | **not published** |
| Electronic markets, agents and brokers (`425`) | 702,599 | 30,509 (commissions) |

¹ implied, `Gross margins ÷ Gross margins as a percent of sales`.
² the 18 four-digit codes; the published NAICS 42 row is 1,100,925.

**Agents and brokers close 3.6% of the gap and no more.** `ecncomm`/`ecnprofit`
`RCPCMRD` is 30,509 against a gap of 850,540, and the 2023 AIES equivalent
`RCPT_COMSN_EARN_VAL` is 37,866 on 856,428 of sales — a ~4.4% commission rate.
It is small for a structural reason, not a coverage one: agents and brokers
never take title, so they book a commission on someone else's sale rather than a
margin on their own. Before 2023 the only source is `ecncomm`, which is
**Economic Census — 2012, 2017 and 2022 only**, so it could contribute a level
correction but no annual movement. Adding an interpolated quinquennial series to
a growth index buys nothing.

**MSBO margin is not published in any vintage.** `ecnmargin` and `ecnprofit`
return a single row, `TYPOP 10`, which is merchant wholesalers — the 11–19
subtypes aggregated. There is no 21 or 22 row. `ecntypop` *does* break MSBOs out,
but only for sales, payroll, inventories and operating expenses; it carries no
margin item, and neither does AIES (`RCPT_GM_DVAL` is 0 at `TYPOP 2X`). MSBOs
have 2,331,241 of sales, enough to be most of the remaining gap at any plausible
rate, but that margin could only ever be **imputed** — merchant rate × MSBO
sales — never observed.

**Decision: the index stays merchant-wholesaler-only.** A consistent basis across
all years is what a growth factor needs; the scope difference cancels in the
ratio as long as MSBO margin grows roughly like merchant wholesale, and Phase 4
anchors the level on 2017 BEA regardless. Imputing MSBO margin would add a
component whose year-to-year movement is driven entirely by MSBO *sales*, which
is a modelling choice dressed as data.

**One place left to look, flagged rather than chased here.** An MSBO is the
manufacturer's own outlet, so the establishment is in wholesale but the parent is
a manufacturer — the markup may be visible from the *manufacturing* side even
though the wholesale tables do not carry it. The annual manufacturing survey
probe in [#564](https://github.com/cornerstone-data/bedrock/issues/564) is the
work that will be reading those tables closely, so the lookout is recorded there:
[`annual_survey_expense_sources.md`](annual_survey_expense_sources.md) §Watch for
while in here. `ecnclcust` (class of customer) is the most promising candidate. A
single credible 2017 MSBO markup rate would be enough, since only the annual
movement is needed from elsewhere.

⚠️ **Open, and larger than anything above: the Economic Census and AWTS disagree
by 42% about merchant wholesalers' own margin.** 1,563,667 against 1,100,925 at
NAICS 42, for the same year and very nearly the same sales base — 5,700,967
against an AWTS-implied 5,704,275, a 0.06% difference. So the disagreement is
entirely in cost of goods: EC's `CSTGS` is 4,124,842 where AWTS purchases are
4,621,765, about 497,000 apart. That is far too large to be inventory timing, and
it means **the choice of Census source moves the wholesale level by 42%**. Not
run down yet.

**Decision: anchor on the annual source — AWTS/ARTS, spliced to AIES — and not
on the Economic Census.** The Economic Census is the more authoritative source in
general, and that is the right reason to expect it to win a level dispute; it is
not the relevant criterion here. Phase 3 supplies **movement only** — the level
comes from the 2017 BEA column by construction (§These are index numbers) — and
the Economic Census is quinquennial, so it cannot supply annual movement at all.
Interpolating 2012/2017/2022 into a growth index would add modelled years dressed
as observations. What a growth factor needs is a basis held constant across every
year, and the annual surveys are that. The 42% gap therefore never enters the
build: it cancels in each year's ratio as long as it is stable over time, which
is the assumption now doing the work and the one to test.

### The splice is the seam that matters, and only one side of it is clean

With the level anchored on BEA and the movement taken from the annual series, the
one place a basis change can still corrupt the answer is **2022→2023, where AWTS
and ARTS hand over to AIES** — the survey consolidation re-engineered the
questionnaire, so it is exactly where the annual basis could shift toward the
Economic Census one. Measured on the published total rows, the margin *rate* is
the test, because it is invariant to coverage:

| gross margin ÷ sales | 2019 | 2020 | 2021 | 2022 | **2023 (AIES)** | step |
|---|---:|---:|---:|---:|---:|---:|
| wholesale, NAICS 42 | 19.3% | 19.7% | 20.7% | 20.1% | **20.4%** | **+0.3pp** ✅ |
| retail, `Total` | 29.4% | 29.5% | 30.8% | 31.3% | **34.2%** | **+2.9pp** ⚠️ |

**Wholesale splices cleanly.** The rate moves +0.29pp across the seam, inside its
own year-to-year range, and the level falls 1.0% on sales down 2.5% — an ordinary
2023 for goods wholesaling as inflation cooled. The `TYPOP 1X` = `nomsbo` basis
match holds up in the data, not just in the documentation.

**Retail does not, yet.** The rate steps 2.9pp in one year against a series that
had moved ~0.3pp/yr for a decade, on sales up only 1.5% — so nearly all of the
10.8% margin gain is repricing of the margin itself rather than more trade. It is
not a NAICS vintage artifact: AIES 2023 publishes the 2017-vintage retail
structure (`452` and `454` both present, no `455`–`459`), and its twelve 3-digit
codes sum to the published `44-45` total exactly.

⚠️ **So the EC-versus-annual question resurfaces inside the annual series.** If
AIES adopted the Economic Census basis, retail 2023 is on a different footing
from retail 2012–2022 and the growth factor carries a one-off 2.9pp jump that
never happened. **The discriminating test:** 2022 is an Economic Census year, so
compute the retail margin rate three ways — ARTS 2022, EC 2022 (`ecnmargin`), and
AIES 2023. If EC 2022 sits near 34% while ARTS sits at 31.3%, the step is a basis
change and 2023 must be rebased onto the ARTS rate before it enters the index. If
EC 2022 sits near 31.3%, the step is real and stands. Wholesale gets the same
test for free, and its answer is already predicted: the EC basis is 42% above
AWTS, so a wholesale rate that did *not* jump is evidence AIES stayed on the
annual basis.

Neither `ecnmargin` nor `ecnbasic` is extracted yet, so this is scoped work
rather than a lookup. It is a prerequisite of #613's control total, not of the
extractors, which emit the published rows either way.

### Why gross margin, and not sales

Gross output of wholesale and retail **excludes the cost of goods purchased for
resale**, so trade output essentially *is* the margin — the same fact that makes
the eight retail sectors give up exactly −100% of their output in §The negative
result. Sales is therefore the wrong series to carry: it contains COGS, which is
not the trade sector's revenue at all.

This is also why the port does not follow flowsa's `Gross_Margins_2017.yaml` in
relabelling AWTS's `Purchases` as `Sales`. Purchases *are* the COGS being
excluded; sales are roughly purchases plus the margin. Measured on 2017,
`Gross margins / Gross margins as a percent of sales` agrees with
`Purchases + Gross margins` to within 1% for every kind of business (0.978–1.000,
NAICS 42 at 0.997), the residual being inventory change. Relabelling understates
sales by the margin itself and so overstates any rate built on it by about a
fifth. The three published items are emitted under their published names and the
derivation is left to where its basis can be stated.

### Two gaps that are not bugs

**2024 has no source.** AIES returns 204 No Content for every year except 2023 —
it carries no back-years, and 2024 is not published. The nowcast window runs to
2024, so the final year's trade control total has to be extrapolated until the
next AIES release. Nothing upstream fixes this.

**Suppression subtracts from a control total.** 2022 retail sums to 2,036,590
against a published total of 2,167,261 — **130,671 short**, from suppressed
gasoline stations (`447`) and `44812`. Zeroing a suppressed cell is harmless in a
detail table and not harmless here, because the parts *are* the whole. The
published total row is in the same workbook, so the suppressed cell is recovered
by subtraction from it rather than by treating the zero as data; the `Suppressed`
flag is preserved on every such cell so that step can find them.

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

1. ~~**Tons or ton-miles** for the transport allocation~~ — **the wrong question.**
   BEA allocates rail and truck on revenue by product and only water and air on
   ton-miles, so the 2017 fit was choosing between two measures neither of which
   applies to 84% of `TRANS`. See §Transportation. Five questions replaced it, put
   to BEA on 2026-08-14; **four came back answered on 2026-08-17** (see
   [`bea_correspondence.md`](bea_correspondence.md)), numbered as sent:
   1. ~~**How is SAS Table 8's "Other goods" distributed?**~~ — **closed.** It is
      not distributed, because it is not used: BEA builds weights from the ten
      identified groups, so discarding "other" and renormalising *is* the method,
      and pro rata "would not change the result". The largest single unknown in
      the chain, closed at zero cost.
   2. ~~**Is the Table 8 group → I-O concordance published?**~~ — **closed, and
      the answer is no.** Internal to BEA's database, remapped every five years,
      being remapped now. We build our own; ten groups makes that tractable.
   3. ~~**What is the pipeline (`486000`) basis?**~~ — **closed, completely.**
      Four Census margin items onto named commodity sets, distributed
      proportionally, with no external source needed. §Pipeline.
   4. ⚠️ **Does the 2022→2023 share step survive scrutiny,** or is it a basis
      change to be rebased? **Not answered — the one method question still open.**
      2022 is an Economic Census year, so the three-way test §The splice is the
      seam that matters specifies for retail applies here too.
   5. ~~**The water and air difficulty multiplier table**~~ — **closed as a
      rule**, not as a table: BEA will not share the table but gave the assignment
      finely enough to reproduce ("should get you pretty close"). Air 1 except
      animals at 3; water 3 for motorized vehicles and transport, 2 for
      palletized or containerized, 1 for loose in the hold.

   **This inverts the build order.** Pipeline went from unmethodded to the
   cheapest mode to land and should be built first; rail, previously the only
   unblocked mode, is now last, because it is the only one whose public source is
   still unidentified. See §Approach.
2. **Extending `Crosswalk_SCTGtoBEA.csv` to BEA 2017 detail** — it currently
   carries 2012 detail and 2017 *summary* only. Now scoped to water and air only,
   3.8% of `TRANS`, since those are the sole modes still allocated on ton-miles.
3. **Build the SAS ten-group → I-O commodity concordance.** Unavoidable — BEA's
   is internal, and the 2017 anchor gives the commodity allocation but not the
   group it came from, so nothing substitutes for it. Governs truck's 67.8%.
4. **Build the NAPCS → I-O commodity concordance?** Only needed to re-run BEA's
   actual method on 2022 product lines rather than carry 2017-anchored rates.
   Everything upstream of it now exists; this is the sole missing link.
   **Deferred to [#615](https://github.com/cornerstone-data/bedrock/issues/615)**,
   out of v0.5 and a candidate for a Phase 3 of the project. Reopen only if
   #614's validation shows the 2017-anchored allocation drifting.

Three earlier questions are now closed: the excise/sales split resolves into the
producer-level vs trade-level decomposition above; petroleum's near-zero
residual is correct behaviour rather than an anomaly; and `TRADE`/`T013` above 1
is correct because margin is additive, not a share.
