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
[`intermediate_structure_drift.py`](intermediate_structure_drift.py). Its
benchmark measurements read the **2007 / 2012 / 2017 detail SUT panel** from
`SUPPLY-USE_2026-08-24.zip`, which has no extractor yet — see §What to build S0a.

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

⚠️ **This is a floor twice over.** First, BEA's annual summary SUT is itself an
estimate — annual indicators applied over a carried-forward benchmark structure —
so wherever BEA also froze structure, "frozen" scores well here by construction
rather than by being right. Second, and this one is now measured rather than
asserted: **aggregating to summary hides 30% of the detail error**
(§The out-of-sample version). At detail the 2024 figure is nearer **0.146**.

### The out-of-sample version, on the estimand itself

`--holdout`. **`Use_SUT_Detail.xlsx` inside `SUPPLY-USE_2026-08-24.zip` carries
2007, 2012 and 2017 as three sheets — all on the 2017 code basis, all in the same
413 × 424 frame, purchaser value, before redefinitions, BEA detail.** That is
Step 3's estimand exactly, three times over, so each span is a real benchmark
holdout rather than an analogue: every year is its own Economic-Census-anchored
*best-level* estimate, and none is carried from another. Same design as
[`mix_holdout_test.py`](mix_holdout_test.py) for Step 4a's commodity mix, but
with a better reference than that test could get.

| span | detail | aggregated to summary | summary hides | + inflation | best θ |
|---|---:|---:|---:|---:|---:|
| 2007 → 2012 | **0.132** | 0.093 | 30% | — | — |
| 2012 → 2017 | **0.173** | 0.122 | 30% | 0.166 (+4.5%, 229/402 cols) | 1.00 |
| 2007 → 2017 | **0.214** | 0.142 | 33% | — | — |

(2007 spans carry no price ratio — `derive_industry_price_index` starts at 2012.)

Three things, and the third is the one that changes how the other measurements
should be read:

1. **17.3% of every column's dollars land on the wrong detail commodity after
   five years, 21.4% after ten.** Sub-linear: the second five years add 4.0
   points on top of the first five years' 13.2. Structure drifts fastest early
   and then settles, which is mildly good news for a nowcast that reruns off a
   fresh benchmark every five years and bad news for the tail of the span.
2. **The 2012→2017 span drifted more than 2007→2012** (0.173 against 0.132)
   — so 13% per five years is not a constant to plan around.
3. ⚠️ **Aggregating to summary hides 30% of the error, measured on identical
   data.** The same 2012→2017 comparison scores 0.173 at detail and 0.122 at
   summary. So the summary panel above is not just a proxy, it is a *systematically
   optimistic* one: the 10.2% at 2024 corresponds to roughly **0.146 at detail**
   if that ratio holds. Every summary number on this page should be read with
   that multiplier attached.

⚠️ **The benchmark panel has no extractor.** It is a local drop in the
`USA_AllTablesSUP` cache directory; `io_2017` still maps `Use_SUT_detail` to the
single-year `Use_SUT_Framework_2017_DET.xlsx`. Promoting it to a year-parameterised,
GCS-backed loader is a task on its own, and it is worth more than this diagnostic
— a 2007 and 2012 detail SUT in the same frame as 2017 is a second and third
observation of *every* structural question in the build, not just Step 3's.

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
years the price level moved most.** The detail benchmark holdout is the other
half of the picture: over 2012→2017, a span whose median cumulative price ratio
was 1.047, it helps by **+4.5%** and the fitted θ comes out at **1.00**. So the
carry is not broken; it is **regime-dependent**, and the regime is relative-price
dispersion. In a quiet span full inflation is right and worth about 4%. In
2022-2024 it is wrong and costs up to 13%.

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
3. ✅ **Recommendation: keep the inflation carry, make θ a fitted parameter
   rather than an assumed 1, and stop treating the carry as the method.** Apply
   the price ratio as `ratio ** θ`; θ = 1 is #497 today, θ = 0 is a frozen `A`.
   The two panels disagree about the value — detail 2012→2017 fits θ = 1.00,
   summary 2022-2024 wants θ well below it — and that disagreement **is** the
   finding: θ should be a function of relative-price dispersion in the span, not
   a constant. Fitting it needs both panels, and the detail one only became
   available with the benchmark SUT drop.

⚠️ **The commodity axis here is an output-weighted industry PI, not a true
purchaser-price commodity index** — a different object, and the missing piece is
the margin-rate term (§Margins). The sign of the 2022-2024 result is robust; the
size of θ will move once that term is in.

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

### The same picture at detail

The detail ranking is not the summary ranking, because summary hides 30% of the
error and hides it unevenly. `--where` also prints the 2012→2017 benchmark span
at BEA detail:

| industry | dissimilarity | column $M | misplaced $M |
|---|---:|---:|---:|
| `531ORE` Other real estate | 0.188 | 747,586 | **140,555** |
| `S00500` Federal general government (defense) | **0.383** | 218,672 | 83,851 |
| `622000` Hospitals | 0.225 | 360,026 | 81,132 |
| `GSLGO` State and local government (other services) | 0.221 | 356,483 | 78,926 |
| `484000` Truck transportation | **0.280** | 189,186 | 52,939 |
| `52A000` Monetary authorities and depository credit | 0.205 | 218,869 | 44,847 |
| `550000` Management of companies | 0.219 | 198,487 | 43,417 |
| `424200` Drugs and druggists' sundries | **0.371** | 107,524 | 39,931 |
| `221100` Electric power generation | **0.297** | 130,972 | 38,955 |
| `561300` Employment services | 0.280 | 137,423 | 38,479 |
| `324110` Petroleum refineries | 0.094 | 397,451 | 37,251 |
| `S00600` Federal general government (nondefense) | **0.308** | 108,824 | 33,538 |

**`S00500` federal defense is the worst-drifting large column in the table at
0.383 — 38% of its dollars on the wrong commodity after five years**, with
`S00600` at 0.308 right behind it and the three `GSLG*` columns between 0.164 and
0.221. The government block is not a quiet corner of this table; it is the
noisiest part of it.

`531ORE` tops both rankings, at both levels, in both eras.

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

- **The government columns are the worst-drifting in the table**, and at detail
  it is stark: `S00500` **0.383**, `S00600` **0.308**, `GSLGO` 0.221, `GSLGE`
  0.164 — 230 $B misplaced across the block over one five-year span, and 258 $B
  at summary by 2024. #578's premise — *government needs a column total, not a
  mix, because `G*` is not commodity-specific* — is what the data contradicts. A
  total is what those columns least need.
- **Agriculture is not in this list at all.** `111CA` carries little of the drift.
  #577 remains correct and cheap but is a small prize; it should not be first.
- **`ORE`, `42`, `23`, `5412OP`, `81` and `722` are where the dollars are**, and
  none of them has an annual expense source (§Reconciling below). This is the
  honest shape of the problem.

---

## The row control — what the trade estimates cost this block

The commodity row is not estimated, it is a residual: `T001[c] = T016[c] − Σ_FD Y[c]`,
imposed hard by Step 5. So error in the two least settled terms of that identity
lands in the intermediate block. Those terms are **`MCIF`** (imports, Step 4b) and
**`F04000`** (exports, Step 1d) — commodity output that leaves the country and so is
not available for domestic industry use. Both come from the same trade extract and
both still show `PARTIAL` / `MISS` / `EXTRA` cells against 2017.

Measured by [`row_control_exposure.py`](row_control_exposure.py). Per commodity,
`dT001 = err_MCIF − err_F04000`, weighted by how much of that commodity actually
goes to industry (`ι = T001/T019`), because an error on a commodity that is 95%
final demand mostly lands in final demand.

| | $M | % of intermediate |
|---|---:|---:|
| net error in the row control | −93,889 | −0.6% |
| gross error, Σ\|dT001\| | 1,164,084 | 7.8% |
| **landing in the intermediate block** | **488,408** | **3.3%** |
|  attributable to `MCIF` | 454,227 | 3.1% |
|  attributable to `F04000` | 313,910 | 2.1% |

**Three readings:**

1. **It is real but second-order.** 3.3% against the 17.3% of structural drift over
   five years (§The out-of-sample version). Worth fixing, not worth reordering the
   step around — and the *net* is only −0.6%, so this is misallocation across
   commodities rather than a level problem.
2. **Exports are two-thirds the size of imports here.** `F04000` contributes 313,910
   against `MCIF`'s 454,227. The user's instinct is right: the export column belongs
   in this accounting, and it is not a rounding term.
3. ✅ **It is extremely concentrated — one commodity is 29%, ten are 57%, twenty are
   71%.** This is a short list, not a research programme.

### The list

| commodity | ι | `MCIF` ref | `MCIF` err | | `F04000` ref | `F04000` err | | exposure $M |
|---|---:|---:|---:|---|---:|---:|---|---:|
| `S00300` Noncomparable imports | 0.55 | 260,421 | **−260,421** | miss | 0 | 0 | absent | **142,489** |
| `334418` Printed circuit assembly | 0.83 | 757 | **+19,833** | partial | 2,839 | −2,221 | partial | 18,309 |
| `52A000` Monetary authorities, depository credit | 0.58 | 6,646 | **+30,983** | partial | 30,778 | −35 | match | 18,076 |
| `325414` Biological products | 0.79 | 51,322 | −25,007 | partial | 22,574 | −2,327 | partial | 17,806 |
| `533000` Lessors of nonfinancial intangibles | 0.54 | **0** | **+33,285** | **extra** | 73,049 | +2,476 | partial | 16,588 |
| `336412` Aircraft engines and parts | 0.38 | 13,535 | +7,842 | partial | 35,982 | **−33,265** | partial | 15,571 |
| `334118` Computer terminals | 0.40 | 37,618 | −14,968 | partial | 6,060 | +21,063 | partial | 14,367 |
| `54151A` Other computer related services | 0.88 | 7,562 | +12,871 | partial | 8,269 | −1,595 | partial | 12,704 |
| `336411` Aircraft manufacturing | 0.23 | 8,839 | +4,943 | partial | 52,720 | **−50,141** | partial | 12,447 |
| `336413` Other aircraft parts | 0.62 | 14,753 | +1,720 | partial | 22,154 | −16,683 | partial | 11,377 |
| `331410` Nonferrous metal smelting | 0.87 | 18,967 | +11,807 | partial | 4,683 | +21,043 | partial | 8,062 |
| `492000` Couriers and messengers | 0.85 | 44 | −44 | miss | 9,411 | **−9,411** | **miss** | 8,008 |
| `336390` Other motor vehicle parts | 0.68 | 35,891 | +1,002 | partial | 27,336 | −10,127 | partial | 7,620 |
| `325411` Medicinal and botanical | 0.98 | 93 | **+9,841** | partial | 78 | +2,149 | partial | 7,512 |
| `541300` Architectural and engineering | 0.81 | 14,609 | −8,505 | partial | 33,253 | −18,298 | partial | 7,362 |
| `325910` Printing ink | 0.80 | 160 | **+8,642** | partial | 953 | +249 | partial | 6,951 |

Two more worth naming for their *rate* rather than their size: `334610` magnetic
media — error 217% of total use — and `325910` printing ink at 146%. Both are small
commodities where the trade extract is producing several times the published import
level.

⚠️ **`336111` / `336112` automobiles and light trucks look catastrophic on a raw
error basis (−90 and +88 billion) and are excluded here on purpose**: their `ι` is
essentially zero — `T001` is 14 and 1 $M — so the error lands in PCE and investment,
not in this block. It is a large problem for Step 1, and not this step's problem.

### The `EXTRA` list is a lead on `S00300`, not a separate bug

Five commodities carry `MCIF` where BEA publishes **zero**, 64,548 $M in all. The
top two are the interesting ones:

| commodity | our `MCIF` | BEA |
|---|---:|---:|
| `533000` Lessors of nonfinancial intangible assets | 33,285 | **0** |
| `483000` Water transportation | 28,361 | **0** |

✅ **Both are textbook `S00300`.** [#606](https://github.com/cornerstone-data/bedrock/issues/606)
defines noncomparable imports as *services produced and consumed abroad* (its own
example: airport expenditures by U.S. airlines in foreign countries) and *payments
for the rights to patents, copyrights, or industrial processes*. Licensing of
intangibles is the second category verbatim; foreign port and vessel services are the
first. So **61,646 $M of the `S00300` gap is plausibly not missing at all — it is
sitting on named service commodities in our extract**, and #606 is partly a
*reallocation* problem rather than a sourcing one. That is a cheap thing to test and
it accounts for 24% of `S00300`.

The other three extras are small and look like ordinary concordance noise: `425000`
wholesale electronic markets 2,234, `339116` dental laboratories 522, `33211A`
forging and stamping 146.

### What this changes

All four are now [#701](https://github.com/cornerstone-data/bedrock/issues/701), with the vehicle split split out to [#702](https://github.com/cornerstone-data/bedrock/issues/702).

- **[#606](https://github.com/cornerstone-data/bedrock/issues/606) is the single
  highest-value item in this area** — 29% of the exposure on its own — and the
  `EXTRA` lead above says where to start.
- **The aircraft cluster is an exports problem, not an imports one.** `336411` /
  `336412` / `336413` are short 100,089 $M of exports between them, 39,395 of
  exposure. One concordance, three commodities.
- **`52A000` is the largest pure-imports error**: +30,983 on a published 6,646, a
  5.7× overstatement of financial service imports. ⚠️ Worth checking against the
  unapplied ITA goods-and-services scale
  ([#647](https://github.com/cornerstone-data/bedrock/issues/647)) before treating it
  as a concordance fault.
- **`492000` couriers is `MISS` on both sides** — we produce neither its imports nor
  its 9,411 $M of exports. A commodity absent from both halves of the trade extract
  is a mapping gap, not an estimate.

⚠️ **This is 2017, where the answer is published.** It grades the *estimates*, not
the nowcast years. But the extract is the same extract, so a commodity that misses
here is a commodity to distrust in 2018-2025 — with no published table to catch it.

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

**Step 3 is an input to that conversion, not the place it happens.**
[#697](https://github.com/cornerstone-data/bedrock/issues/697) owns it, at
transaction level off the 4c Margins dataset.
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
| **2022 Economic Census `MATFUEL`** | **mix**, the materials breakout for all of manufacturing | the only second observation of the 82% of manufacturing's column that annual data cannot see | ✅ **built — see §The materials census** |
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

## The materials census — built, and it delivers

`Census_EC_MatFuel` now pulls `ecnmatfuel` for **2017 and 2022**, and
[`materials_structure.py`](materials_structure.py) measures what it buys. Both
questions came back positive.

### How much of the materials bill can be placed on a commodity

`MATFUEL` codes are 8-digit and NAICS-derived — `33110090` iron and steel ingot,
`33272203` bolts and nuts — so the longest NAICS prefix in
`NAICS_to_BEA_Crosswalk_2017.csv` resolves the material. The seller-not-maker
problem that made the trade concordance hard does not arise, because a material
is defined by what it *is*.

| | 2017 | 2022 |
|---|---:|---:|
| cost after suppression recovery | $2,906.6B | $3,875.7B |
| **`direct`** — one BEA detail commodity | **52.9%** | **54.1%** |
| **`group`** — a BEA group, needs a within-group split | 13.6% | 15.1% |
| **`residual`** — Census could not place it | 33.5% | 30.8% |
| **placeable** | **66.5%** | **69.2%** |
| distinct BEA commodities reached | 139 | 139 |
| industries × materials | 388 × 289 | 367 × 290 |
| withheld cells, recovered | 412 ($39.9B) | 330 ($87.5B) |

✅ **66.5% and 69.2% placeable, against 8.3% for the annual data** — an eight-fold
improvement on exactly the part of the column that matters most. The `group`
tier is placeable too: `322` paper reaches several BEA detail commodities, and
splitting within the group on 2017 Use shares is a far lighter benchmark
dependency than freezing the whole column.

⚠️ **A third is residual and that is the ceiling.** `00970099` "Cost of all
other materials, components, parts, containers and supplies" ($487B / $614B) and
`00971000` "Materials, ingredients, containers and supplies, nsk"
($453B / $530B) are the two large buckets. No amount of concordance work reaches
them.

⚠️ **`00772000` "Total Materials" is the industry total, not a material.** The
named codes sum to it exactly — median ratio 1.000 across 386 of 388 industries.
Summing the FBA unfiltered doubles the table. It is kept because it is the
control a suppression recovery has to subtract published children from, exactly
as NAICS `00` serves `Census_EC_PxI`.

### Suppression recovery, and why it changes what can be claimed

Census withholds **412 cells in 2017 and 330 in 2022**. They are not zero — they
sit inside each industry's published `00772000` total, and
`estimate_suppressed_ec_matfuel` fills them against it.

✅ **The control is exact, measured not assumed.** For every industry with
nothing withheld, the named materials sum to `00772000` to within 0.1% — **238 of
238 in 2017 and 247 of 247 in 2022** — and fuels have their own exact control in
`00772002`, present for every industry carrying fuel rows. After recovery all
**406** (2017) and **386** (2022) industry-by-kind controls close to within 0.1%,
and no negative cell is created.

⚠️ **But the fill is a placement, not a measurement, and the holdout says how
rough.** Masking published cells and recovering them gives a weighted absolute
percentage error of **0.60 in 2017 and 0.72 in 2022**. The *mass* is exact — the
residual is fixed by the published total — so **all** of that error is allocation
across materials within the column, which is exactly what a mix score measures.

The prior is chosen by that holdout rather than by argument: the residual is
shared over withheld cells in proportion to what the industry's **NAICS-3 peers**
publish for the same material. An economy-wide prior was tried first and is
visibly wrong — it hands an idiosyncratic industry the economy's shopping list,
and put **$8.6B of "motor vehicle seating" into aircraft manufacturing** while
cutting its aircraft engines from $15.5B to $0.5B. NAICS-3 scores 0.602 / 0.718
against economy-wide's 0.640 / 1.033.

⚠️ **Cross-vintage priors are deliberately not used**, though 2017 is by far the
best predictor of a withheld 2022 cell. Filling 2022 from 2017 would make the two
vintages more alike and bias the movement measurement — the headline finding —
toward zero. A recovery must not manufacture the answer the analysis is testing.

### How far the materials mix actually moved

| | full frame | **unsuppressed only** |
|---|---:|---:|
| industries | 345 | **193** |
| share of 2022 cost | 90.7% | 43.4% |
| **dissimilarity** | 0.1588 | **0.1330** |
| median column | 0.1529 | 0.1257 |
| columns > 0.10 | 261 | 133 |
| columns > 0.25 | 66 | **17** |

✅ **Quote 0.133, the unsuppressed subsample.** The full frame's 0.159 is
contaminated by the fill, and the collapse from 66 extreme columns to 17 shows
where: **most of the extremes were the recovery, not the economy.** An earlier
draft of this section quoted 0.153 on the full frame and called the materials
block as volatile as the whole column; the clean number does not support that
strong a claim.

⚠️ The clean subsample is not a random one — suppression correlates with having
few establishments, so it over-represents larger, more diversified industries.

**What survives, and it is enough:** materials mix moves **0.133 over five
years**, against **0.173 for the entire Use column over 2012→2017**. So the
largest and least-observed part of the manufacturing column moves substantially
— somewhat less than the column as a whole, not more — and **133 of 193 clean
industries move more than 10 points**. Freezing it from 2017 to 2025 discards a
reallocation of that size, and this source observes it. That is the argument.

### Where the movement sits

| industry | dissimilarity | materials 2022 $B | reallocated $B |
|---|---:|---:|---:|
| `324110` Petroleum refineries | 0.082 | 625.4 | **51.5** |
| `336411` Aircraft manufacturing | **0.592** | 34.2 | 20.2 |
| `326199` All other plastics products | 0.233 | 54.5 | 12.7 |
| `325199` All other basic organic chemicals | 0.239 | 51.1 | 12.2 |
| `331110` Iron and steel mills | 0.164 | 70.0 | 11.5 |
| `211120` Crude petroleum extraction | 0.311 | 35.9 | 11.2 |
| `325110` Petrochemicals | 0.337 | 31.5 | 10.6 |
| `211130` Natural gas extraction | 0.313 | 26.2 | 8.2 |
| `336390` Other motor vehicle parts | 0.179 | 43.2 | 7.8 |
| `311224` Soybean and other oilseed processing | 0.165 | 45.7 | 7.5 |

`324110` dominates on size rather than rate — 0.082 on a $625B materials bill,
which is crude oil composition moving under a very large column.

⚠️ **`336411` aircraft was the loudest column, and chasing it is what found the
recovery defect.** It scored 0.592, which is not credible as economics. The cause
is not a code reassignment: **most of its 2022 column is withheld**, so the score
was measuring the suppression fill. It is excluded from the clean subsample above,
which is the right treatment. It remains an industry to distrust in this source —
and it is separately the one the trade work flagged for a 100,089 $M export
shortfall (§The row control).

### What is not built yet

The remainder of [#698](https://github.com/cornerstone-data/bedrock/issues/698).

1. ✅ **Suppression recovery — built.** See above.
2. **The `group`-tier within-group split**, on 2017 Use shares.
3. **The vintage code diff** — `MATFUEL` and NAICS both change basis between
   2017 and 2022, and 10% of each year's cost is off the shared frame. The
   `336411` result above is why this matters.
4. **The interpolation itself** — two observations, and the span runs
   2018-2025. 2022 sits close to the middle, so linear interpolation between the
   two and extrapolation past 2022 is the obvious first form.

---

## Special commodities and columns

- **`S00300` Noncomparable imports** — 142,497 $M in the intermediate block
  (260,421 $M total), no domestic production, nothing on the Supply side to split
  it by. Owned by [#606](https://github.com/cornerstone-data/bedrock/issues/606)
  jointly with Step 1. Unchanged by anything here.
- **`S00401` Scrap** (49,126 $M) and **`S00402` Used and secondhand goods**
  (33,816 $M) — both live in the intermediate block, both have a Supply-side
  counterpart, and neither has a price index (`S00402` gets a neutral 1.0 in
  `get_cornerstone_industry_price_ratio`). Now [#703](https://github.com/cornerstone-data/bedrock/issues/703).
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

**S0a. Give the benchmark detail SUT panel an extractor** ([#700](https://github.com/cornerstone-data/bedrock/issues/700)). `Use_SUT_Detail.xlsx`
and `Supply_Detail.xlsx` in `SUPPLY-USE_2026-08-24.zip` carry **2007, 2012 and
2017 on one code basis in one frame**. Today they are a local drop that only this
diagnostic reads. A year-parameterised, GCS-backed loader beside
`_load_2017_detail_supply_use_usa` turns them into a second and third observation
of *every* structural question in the build — Step 3's input mix, Step 4a's
commodity mix, the margin rates, the FD splits — not just this one. **Cheapest
high-leverage item on the page**, and everything measured above depends on it.

**S0b. Declare the section before the candidate exists** ([#704](https://github.com/cornerstone-data/bedrock/issues/704)). `use_intermediate_detail_sut`
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

**S2. Fit θ, and add the margin-rate factor** ([#699](https://github.com/cornerstone-data/bedrock/issues/699)). §Inflation and §Margins.2. One
experiment, two terms, scored on the published summary panel 2018-2024. The
deliverable is a number and a decision, not a new source dependency.

**S3. The 2022 Economic Census materials breakout** — ✅ **extracted**; turning it
into a seed is [#698](https://github.com/cornerstone-data/bedrock/issues/698). §The materials census.

**S4. `--where`-driven sourcing for the top drifters** ([#705](https://github.com/cornerstone-data/bedrock/issues/705)). `ORE`, `GSLG`/`GFGD`,
`42`, `5412OP`, `81`. Currently unsourced and, on the evidence above, worth more
than everything in #564's survivor list combined. This is a research task before
it is a build task.

**S5. ERS agriculture (#577)** and, if the function→commodity bridge survives
scrutiny, **government finances (#578, rescoped to exactly that question)**.

Not in this step: `S00300` (#606, shared with Step 1), the margins redistribution
(Step 6b, #697), the government-enterprise reallocation (Step 7), and the balance
itself (Step 5, #588).
