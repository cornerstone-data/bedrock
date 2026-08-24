# Plan — estimating the Use table's intermediate block (Step 3)

Step 3 of [`plan.md`](plan.md) — the commodity × industry interior of the SUT Use
table, 402 × 402, **purchaser value, before redefinitions**. The last incomplete
block of the 2017 build and the only one with no section in
[`sections.py`](sections.py).

Issues: [#497](https://github.com/cornerstone-data/bedrock/issues/497) (the
method), [#577](https://github.com/cornerstone-data/bedrock/issues/577)
(agriculture), [#578](https://github.com/cornerstone-data/bedrock/issues/578)
(government), [#606](https://github.com/cornerstone-data/bedrock/issues/606)
(`S00300`), [#564](https://github.com/cornerstone-data/bedrock/issues/564)
(the annual-survey probe, answered in
[`annual_survey_expense_sources.md`](annual_survey_expense_sources.md)).

Everything numbered below is reproduced by
[`intermediate_structure_drift.py`](intermediate_structure_drift.py).

---

## The finding that reorganises this step

**Step 3 does not estimate any levels. It estimates a shape.**

Both margins of this block are already determined by other steps, and Step 5
imposes both:

| margin | value | where it comes from | Step 5 |
|---|---|---|---|
| industry column | `T005[j] = GO_producer[j] − VAPRO[j]` | UGO305-A gross output (Step 4a input) less Step 2 value added | **hard**, T1 |
| commodity row | `T001[c] = T016[c] − Σ_FD Y[c]` | Supply purchaser total (Step 4) less final demand (Step 1) | **hard**, T11 (`T016 = T019`) |

Both identities are exact in the published 2017 detail SUT, not approximate:

- `T005 + VABAS = T018` per industry, **max residual $1M on $34T** — the
  arithmetic behind the user-facing version of it, *total intermediate =
  reported gross output − calculated value added*.
- `T016 = T019` per commodity, **max residual $0M**, all 402 rows.

A biproportional balance with both margins fixed reproduces those margins
regardless of what it started from. So of everything Step 3 could produce, the
part that survives Step 5 is the **cross-structure** — the relative pattern
inside the matrix — and nothing else.

⚠️ **This is not an argument for doing less. It is an argument for spending the
effort somewhere different.** A source that delivers a column total is
delivering a number Step 5 already has. A source that delivers a *mix* is
delivering the only thing Step 3 can contribute. §Reconciling the sources below
re-sorts #577, #578 and #564 on exactly that line, and it changes their
priority order.

Two consequences worth stating before anything else:

1. **Scaling the seed's columns to `GO − VA` is free and should be done anyway.**
   It changes no within-column structure, so it costs no assumption, and it hands
   Step 5 a near-feasible starting point instead of one that is 44% too small by
   2024. It is the cheapest thing on this page.
   ⚠️ The catch is valuation: the exact identity is at **basic** value
   (`T018 = T005 + VABAS`), published gross output is at **producer** value, and
   the wedge — `T00TOP − T00SUB` by industry — is a Step 5 *output*, not an
   input (§Step 5 Decision 3). Use Step 2's 2017-ratio seed for the wedge, label
   it a seed, and let Step 5 re-solve. §The column control below.
2. **Step 3 must run before Step 6b, and 6b consumes its output.** The margins
   redistribution — each commodity's `TRADE`/`TRANS` moved onto the
   wholesale/retail/transport commodity *rows* in each buyer's column — is
   allocated by that buyer's purchases of the margined commodities. Those
   purchases are this block. §Margins below.

---

## How stale does a frozen 2017 structure get?

`--drift`. Published **summary** Use SUT, frozen 2017 column shares scored
against each later published year. One benchmark vintage, no revision seam,
exactly the nowcast horizon. The metric is the index of dissimilarity — the
share of a column's dollars sitting on the wrong commodity — dollar-weighted
across the 71 industry columns.

| year | dissimilarity | intermediate $M |
|---|---:|---:|
| 2018 | **0.031** | 15,847,995 |
| 2019 | **0.045** | 16,155,023 |
| 2020 | **0.074** | 15,358,118 |
| 2021 | **0.071** | 18,071,299 |
| 2022 | **0.084** | 20,339,157 |
| 2023 | **0.097** | 20,728,742 |
| 2024 | **0.102** | 21,438,541 |

**About 1.2 points a year, monotone apart from the 2020/2021 COVID pair, and
10.2% by 2024.** On 2024's 21.4 trillion of intermediate that is **2.2 trillion
of dollars on the wrong commodity** — at summary, where 71 columns hide every
reallocation happening *inside* a summary industry. The detail number is larger
by an unknown amount.

⚠️ **This is a floor, and the reason matters.** BEA's annual summary SUT is
itself an estimate: annual indicators applied over a carried-forward benchmark
structure. Wherever BEA also froze structure, "frozen" scores well here by
construction rather than by being right. The measurement below is the
non-circular one.

### The out-of-sample version

`--holdout`. The 2012 benchmark detail Use table carried to 2017 and scored
against the published 2017 detail table. Both ends are Economic-Census-anchored
*best-level* estimates, so neither was carried from the other — this is the same
design as [`mix_holdout_test.py`](mix_holdout_test.py) for Step 4a's commodity
mix, and it is the only measurement here that is out of sample at detail.

| variant | dissimilarity | columns improved |
|---|---:|---:|
| frozen 2012 structure | **0.195** | — / 402 |
| + commodity inflation | **0.187** | 230 / 402 |

**19.5% of every column's dollars land on the wrong detail commodity after five
years**, against 7-8% at summary over a comparable span. The gap between the two
is the within-summary reallocation the summary table cannot see, plus the 2012/2017
benchmark reconstruction difference — the two are not separable here.

⚠️ **After redefinitions, producer value.** BEA has moved the 2012 benchmark off
static download, so the only 2012 detail Use table available is the redefined one
in `CEDA6IO.xlsx`; it is paired with the 2017 redefined table so both sides sit in
one space. Step 3's own object is before-redefinitions at purchaser value, so
this is an analogue of the estimand, not the estimand.

---

## Does #497's inflation step earn its place?

`--inflation`. Same summary years, frozen structure against the same structure
carried on a commodity price index (bedrock's own detail industry PI,
`derive_industry_price_index`, output-weighted to summary).

| year | frozen | + inflation | change |
|---|---:|---:|---:|
| 2018 | 0.0314 | 0.0299 | **+4.9%** |
| 2019 | 0.0452 | 0.0449 | +0.7% |
| 2020 | 0.0743 | 0.0717 | +3.6% |
| 2021 | 0.0711 | 0.0709 | +0.3% |
| 2022 | 0.0838 | 0.0883 | **−5.4%** |
| 2023 | 0.0974 | 0.1083 | **−11.2%** |
| 2024 | 0.1019 | 0.1148 | **−12.6%** |

**Inflation helps slightly through 2021 and hurts from 2022 on — worst in the
years the price level moved most.** The detail holdout agrees on the magnitude
from the other side: +4.3% over 2012→2017, a span whose median cumulative price
ratio was 1.047.

**The mechanism is substitution, and it is not a bug in the price index.**
Carrying a *nominal* share vector on a price ratio assumes the physical input
mix is fixed — a Leontief quantity structure, elasticity zero. When a commodity's
price rises, buyers use less of it, so its nominal share rises by *less* than its
price. Inflating by the full ratio overshoots, and it overshoots in proportion to
the relative price dispersion, which is exactly what 2022-2024 supplied.

**Three readings, and only the third is a conclusion:**

1. **#497 is not wrong, it is small.** A ±5% adjustment on a 10-19% error is a
   rounding correction to the wrong problem. The nowcast's accuracy on this block
   is set by structural drift, not by price.
2. **The summary reference may be biased toward frozen** (above), which would
   flatter the frozen column. But it flatters the *inflated* column equally at
   2018-2021 and cannot explain a sign flip that tracks the inflation rate.
3. ✅ **Recommendation: keep the inflation carry, damp it, and stop treating it
   as the method.** Concretely — apply the price ratio with an exponent
   `θ ∈ [0, 1]` and fit θ on this same summary panel; θ = 1 is #497 today, θ = 0
   is a frozen `A`. The panel above says the fitted θ is well below 1 and may be
   near zero. A one-parameter fit on published data is a day of work and is the
   difference between a defensible carry and an assumed one.

⚠️ **What is measured here is a summary-level proxy for a detail method, on a
commodity axis built by output-weighting an industry PI.** A true purchaser-price
commodity index is a different object (§Margins). Redo the fit at detail once
Step 4c's margin rates are in — the sign of the 2022-2024 result is robust, the
size of θ is not.

---

## Where the drift actually sits

`--where`. 2024, summary, by dollars misplaced.

| industry | dissimilarity | column $M | misplaced $M |
|---|---:|---:|---:|
| `ORE` Other real estate | 0.141 | 1,216,705 | **171,923** |
| `GSLG` State and local general government | 0.127 | 1,223,742 | **155,977** |
| `42` Wholesale trade | 0.096 | 1,233,214 | **118,761** |
| `5412OP` Misc. professional, scientific | 0.114 | 827,267 | 94,444 |
| `81` Other services, except government | **0.193** | 467,465 | 90,196 |
| `23` Construction | 0.065 | 1,206,105 | 78,028 |
| `722` Food services and drinking places | 0.118 | 658,866 | 77,787 |
| `622` Hospitals | 0.129 | 569,310 | 73,394 |
| `GFGD` Federal general government (defense) | **0.192** | 342,461 | 65,887 |
| `513` Broadcasting and telecommunications | 0.133 | 437,017 | 58,080 |

Highest *rates*, among columns above $50B: `22` Utilities **0.224**, `81` Other
services 0.193, `GFGD` 0.192, `GFGN` 0.179, `493` Warehousing 0.159.

And on the row axis — each commodity's share of all intermediate use,
2017 → 2024, in percentage points:

| rising | pp | falling | pp |
|---|---:|---|---:|
| `ORE` Other real estate | **+1.25** | `324` Petroleum and coal | **−0.70** |
| `5412OP` Misc. professional | +1.09 | `513` Broadcasting/telecom | −0.62 |
| `532RL` Rental and leasing | +0.67 | `325` Chemicals | −0.42 |
| `523` Securities | +0.53 | `481` Air transportation | −0.35 |
| `524` Insurance | +0.50 | `521CI` Credit intermediation | −0.33 |
| `514` Data processing / info | +0.41 | `3361MV` Motor vehicles | −0.31 |

**A services-and-property shift out of goods and energy.** No price index
reproduces this — `324` falls despite petroleum prices rising over the span,
which is the substitution story of §Inflation stated on the row axis.

Three things follow for sourcing:

- **The three government columns are among the worst-drifting in the table**
  (`GFGD` 0.192, `GFGN` 0.179, `GSLG` 0.127, together 258 $B misplaced). #578's
  premise — *government needs a column total, not a mix, because `G*` is not
  commodity-specific* — is what the data contradicts. A total is what those
  columns least need.
- **Agriculture is not in this list at all.** `111CA` carries little of the drift.
  #577 remains correct and cheap but is a small prize; it should not be first.
- **`ORE`, `42`, `23`, `5412OP`, `81` and `722` are where the dollars are**, and
  none of them has an annual expense source (§Reconciling below). This is the
  honest shape of the problem.

---

## The column control — how to use `GO − VA`

The identity is exact and both inputs now exist, so use it. The only decision is
the valuation wedge.

```
T005[j]  =  T018[j]        −  VABAS[j]        (basic,    exact to $1M)
         =  GO_producer[j] −  VAPRO[j]        (producer, Step 5's T1)
VAPRO    =  VABAS + T00TOP − T00SUB
```

`GO_producer` is `BEA_Detail_GrossOutput_IO_<year>`, extracted for **2017-2024,
all 402 detail industries**, from UGO305-A — a straight read, no 2017 shares
(§Step 5 Decision 3). `VABAS` is Step 2's three rows. `T00TOP` / `T00SUB` by
industry are **not sourced** — Step 5 solves that split, deliberately, because a
fixed 2017 conversion ratio is what the whole design refuses to assume.

✅ **Decision: scale at Step 3 using Step 2's 2017-ratio `T00TOP`/`T00SUB` seed,
and label the result a seed.** Rationale:

- The scaling is a per-column scalar, so it injects **no structural assumption** —
  the only thing Step 3 contributes is untouched.
- Step 5 re-imposes T1 hard and re-solves the wedge, so a wrong wedge here is
  overwritten rather than propagated.
- Without it, the Step 3 candidate is unreadable as a diagnostic: a 2024 seed
  would be ~30% short economy-wide before Step 5 touches it, and every cell-level
  comparison would report that one number 160,000 times.

⚠️ **Do not let this control leak into the *sourcing* argument.** Having the
column total for free is precisely why a source that only supplies a column
total supplies nothing.

---

## Margins — two distinct jobs, and only one of them is Step 3's

The Step 4c/4d work gives, per commodity per year, the margins added between
basic and purchaser value (`TRADE`, `TRANS` on the Supply table). Two different
things want that data, and conflating them is easy:

### 1. Redistribution to the margin commodity rows — **Step 6b, not Step 3**

Converting the purchaser-priced SUT Use table to the producer-priced MUT Use
table strips each goods cell back to producers' value and books the stripped
wholesale / retail / transport amounts onto the margin-supplying commodity rows
*within the same buyer's column*. The allocator across buyers is each buyer's
purchases of the margined commodities — which is this block. So:

**Step 3 is an input to that conversion, not the place it happens.** The plan's
§Step 6b already specifies it at transaction level off the 4c Margins dataset.
What is worth adding there is the check the redistribution makes available:
after 6b, the margin commodity rows of the producer-priced Use table plus their
final-demand cells must recover the Supply `TRADE`/`TRANS` columns per commodity,
and `Σ TRADE = Σ TRANS = 0` (§Step 5 T15/T16).

Sanity check on the shape, 2017 detail: in the **purchaser**-priced SUT Use
table the trade rows are tiny — `4B0000` All other retail is **0** in the
intermediate block, `425000` wholesale agents 33,045 $M, `423A00` 4,436 $M —
because the margins are inside the goods rows. In the producer-priced MUT they
are among the largest rows in the table. That difference *is* the redistribution.

### 2. Margin-rate movement in the deflator — **this one is Step 3's**

A cell of this block is at **purchaser** value. Its correct price movement is
therefore the *purchaser*-price movement:

```
PUR_c(t) / PUR_c(2017)  ≈  [ basic price ratio ]  ×  [ 1 + m_c(t) ] / [ 1 + m_c(2017) ]
     where m_c = (TRADE_c + TRANS_c) / T013_c
```

#497 as written supplies only the first factor, off a **gross-output** price
index — a basic/producer-value index. The second factor is not small. Summary
Supply, `T014 / T013`, 2017 → 2024, over the 36 commodities with a margin rate
above 1%: **median absolute change 2.8pp, p90 12.1pp**, and in relative terms
`313TT` textiles 0.858 → 1.075 (+25%), `339` misc. manufacturing 0.910 → 1.068,
`337` furniture 0.740 → 0.894, `315AL` apparel 1.79 → 1.88.

**This is a concrete, cheap improvement to #497 that uses data Step 4c already
produces for its own reasons**, and it is the one place the margins data belongs
inside Step 3. It should be built and scored on the same summary panel as θ
(§Inflation) — the two are the same experiment with one more term.

⚠️ Ordering: this makes Step 3 depend on Step 4c/4d output. That is a real
dependency but not a circular one — 4c's rates are built from Census margin data
and the published Supply columns, not from the Use interior.

---

## Reconciling the sources — #497 vs #564's survivors

The conflict the plan currently carries is *"#497 says carry 2017 forward on
inflation; #564 found real annual sources for agriculture and government"*.
Sorted by the §Finding above — **does the source deliver mix, or only a total?**
— it mostly dissolves, and what is left is not what the doc expected.

| source | delivers | worth to Step 3 | verdict |
|---|---|---|---|
| **ERS FIWS** farm intermediate expenses (#577) | **mix**, 89-91% commodity-mappable, 11 named categories, 1910-**2025** | real structure, but on a column that barely drifts, and one farm sector against ~10 BEA industries | ✅ **build it, but not first** |
| **2022 Economic Census `MATFUEL`** (#564, unbuilt) | **mix**, the materials breakout for all of manufacturing | the only second observation of the 82% of manufacturing's column that annual data cannot see | ✅ **highest-value unbuilt item on this page** |
| **NIPA T31005** government intermediate purchases | totals — and they match `T005` **exactly** ($0 / $0 / $2M) | zero: Step 5 already has the column total | ⚠️ keep as a *validation* of the derived column, drop as a source |
| **Census `govslocalfin`** (#578) | function × object totals | a total, again — but the function detail is a *potential* mix proxy | ⚠️ **rescope**, see below |
| **NIPA T31105** defense by type | 14 leaves, one of which maps | one cell (`33299A` ammunition, −0.2%); the rest miss by 46-287% | ❌ control and hint only, as the plan already records |
| **ASM `CSTELEC`/`CSTFU`** 2018-2021 | **mix**, 2 cells, ~2.3% of the column, 6-digit, no suppression | small but real, and energy inputs matter downstream for EEIO | ⚠️ narrow; take it if the extractor is cheap |
| **SAS Table 3** service expenses | totals at 227 six-digit NAICS | a *detail* total inside a summary constraint — the one case where a total is not redundant | ⚠️ see below |
| **AIES `exp02`** | one row per service sector, 2023 only | nothing at BEA detail | ❌ as #564 found |

Three of those rows changed verdict, and each is a task:

**#578 needs rescoping, not cancelling.** Its stated premise is wrong in both
directions: the `G*` columns are the *worst-drifting* in the table, so a mix is
exactly what they need, and the column total it offers is already free (NIPA
matches `T005` to the dollar, so even the total is better sourced elsewhere).
What `govslocalfin` uniquely has is **function × object** — education, highways,
police, hospitals, utilities — and function is a plausible bridge to commodity
mix, because a highway department and a school district buy different things.
That bridge does not exist yet and is the actual work. Decide whether it is worth
it before building the extractor; the payoff is the 156 $B misplaced in `GSLG`.

**SAS Table 3 is the exception that proves the rule.** Step 5's column target is
detail gross output, which is already detail — so a detail service *total* adds
nothing after all, and the note in `annual_survey_expense_sources.md` calling it
"the most useful thing the probe found" should be revised. It was the most useful
thing *for a Step 3 that owned its column totals*. Step 3 does not.

**The 2022 Economic Census materials breakout is now the top external candidate.**
It is the only source that speaks to manufacturing's undifferentiated 82%, it is
a genuine second structural observation between 2017 and 2025, and interpolating
2017→2022 structure and extrapolating past it is a strictly better carry than
holding 2017 for eight years. #564 flagged it as a "consolation prize"; on the
§Finding above it is the main prize.

---

## Special commodities and columns

- **`S00300` Noncomparable imports** — 142,497 $M in the intermediate block
  (260,421 $M total), no domestic production, nothing on the Supply side to split
  it by. Owned by [#606](https://github.com/cornerstone-data/bedrock/issues/606)
  jointly with Step 1. Unchanged by anything here.
- **`S00401` Scrap** (49,126 $M) and **`S00402` Used and secondhand goods**
  (33,816 $M) — both live in the intermediate block, both have a Supply-side
  counterpart, and neither has a price index (`S00402` gets a neutral 1.0 in
  `get_cornerstone_industry_price_ratio`). ⚠️ **Not currently owned by any issue.**
  `Used` also has the largest margin-rate movement of any summary commodity
  (6.87 → 8.18 between 2017 and 2024), so it interacts with §Margins.
- **`S00102` / `S00203` government enterprises** — 1,234 and 34,545 $M as
  *commodity rows*; 8,492 and 148,806 $M as *industry columns*. ⚠️ These are the
  rows the Step 7 government-enterprise reallocation moves. Nothing here changes
  that ordering — reallocation sits at Step 7, never before the RAS.
- **The government industry columns are dense**: `S00500` fills 172 of 402 rows,
  `GSLGO` 229. They are not the sparse special-purpose columns the "not
  commodity-specific" framing suggests.
- **Negative cells**: 7 in the 2017 detail interior. GRAS handles sign; do not
  clip them in the seed.

---

## What to build, in order

**S0. Declare the section before the candidate exists.** `use_intermediate_detail_sut`
in [`sections.py`](sections.py) — 402 × 402, reference `_use_sut_detail()` interior,
`candidate=None`. This is the pattern `use_va_detail_sut` was declared under: the
reference, the frame and the tolerance are arguments about economics and can be
settled now. Cheap, and it puts Step 3 into the progress report.

**S1. The seed, per #497, plus the column control.** Seed from
`Use_SUT_Framework_2017_DET` (native SUT, native purchaser, native
before-redefinitions — no conversion round-trip), carry on the commodity price
index, scale each column to `GO_producer − VAPRO_seed`. This is #497 as scoped,
and it is what turns the section on. Do it first — everything below is measured
against it.

**S2. Fit θ, and add the margin-rate factor.** §Inflation and §Margins.2. One
experiment, two terms, scored on the published summary panel 2018-2024. The
deliverable is a number and a decision, not a new source dependency.

**S3. The 2022 Economic Census materials breakout.** §Reconciling. The largest
available structural signal, and the only one aimed at manufacturing.

**S4. `--where`-driven sourcing for the top drifters.** `ORE`, `GSLG`/`GFGD`,
`42`, `5412OP`, `81`. Currently unsourced and, on the evidence above, worth more
than everything in #564's survivor list combined. This is a research task before
it is a build task.

**S5. ERS agriculture (#577)** and, if the function→commodity bridge survives
scrutiny, **government finances (#578, rescoped)**.

Not in this step: `S00300` (#606, shared with Step 1), the margins redistribution
(Step 6b), the government-enterprise reallocation (Step 7), and the balance
itself (Step 5, #588).
