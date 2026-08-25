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
benchmark measurements read the **2007 / 2012 / 2017 detail SUT panel** through
`io_2017.load_benchmark_detail_U_intermediate_usa` (§What to build S0a).

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
| 2018 | **0.031** | 15,847,998 |
| 2019 | **0.050** | 16,117,956 |
| 2020 | **0.083** | 15,339,322 |
| 2021 | **0.082** | 18,107,588 |
| 2022 | **0.086** | 20,556,623 |
| 2023 | **0.097** | 20,728,742 |
| 2024 | **0.102** | 21,438,541 |

✅ **Every row now reads the same workbook.** "No revision seam" used to be an
aspiration — the diagnostic went through `io_2017`'s year-pinned loader, which
changed vintage between 2022 and 2023. It now reads
`Use_Tables_Supply-Use_Framework_1997-2024_Summary.xlsx` for every year, so the
2019-2022 rows above are **0.005 to 0.011 higher** than the pinned series
reported and 2018, 2023 and 2024 are unchanged. ⚠️ The shape of the finding did
not move, but the middle of the series was understated: what looked like a
plateau across 2020-2022 (0.074, 0.071, 0.084) is a plateau at a higher level
(0.083, 0.082, 0.086). §The revision floor measures the seam directly.

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

`--holdout`. **BEA's benchmark detail Use SUT carries
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

✅ **The benchmark panel has an extractor** (§S0a). `io_2017` reads all three
years off the published panel, GCS-backed, and its 2017 sheets are checked cell
for cell against the single-year workbooks. That is worth more than this
diagnostic — a 2007 and 2012 detail SUT in the same frame as 2017 is a second and
third observation of *every* structural question in the build, not just Step 3's.

---

## Does #497's inflation step earn its place?

*What θ is mechanically — a scalar exponent on a commodity deflator, not a price
ratio or a valuation bridge — is in
[`About_the_price_carry.md`](About_the_price_carry.md). This section is about
whether the carry earns its place, not what it is.*

`--inflation`. Same summary years, frozen structure against the same structure
carried on a commodity price index (bedrock's own detail industry PI,
`derive_industry_price_index`, output-weighted to summary).

| year | frozen | + inflation | change |
|---|---:|---:|---:|
| 2018 | 0.0314 | 0.0299 | **+4.9%** |
| 2019 | 0.0504 | 0.0499 | +1.0% |
| 2020 | 0.0833 | 0.0797 | **+4.3%** |
| 2021 | 0.0823 | 0.0808 | +1.7% |
| 2022 | 0.0859 | 0.0922 | **−7.4%** |
| 2023 | 0.0974 | 0.1083 | **−11.2%** |
| 2024 | 0.1019 | 0.1148 | **−12.6%** |

(2019-2022 restated onto the single vintage, as above. The sign pattern is
unchanged and the 2022 loss is **larger** on one basis, −7.4% rather than
−5.4%.)

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
   the price ratio as `ratio ** θ`; θ = 1 is #497 today, θ = 0 is a frozen `A`,
   and ⚠️ **θ < 0 is where 2023 and 2024 actually fit** — see §θ goes negative.
   The two panels disagree about the value — detail 2012→2017 fits θ = 1.00,
   summary 2022-2024 wants θ well below it — and that disagreement **is** the
   finding.

❌ **This section then guessed that θ is a function of relative-price dispersion
in the span. It is not — see §S2, where that guess was tested and failed.** On
78 non-nested spans dispersion explains **1.4%** of the variance in fitted θ,
with the coefficient pointing the wrong way. What explains 61% is whether the
span crosses the 2021-22 surge. The substitution *mechanism* above survives
intact; what does not survive is dispersion as its measurable proxy.

⚠️ **The commodity axis here is an output-weighted industry PI, not a true
purchaser-price commodity index** — a different object, and the missing piece was
the margin-rate term (§Margins). ✅ **That term has since been built and the
result above survives it**: adding it leaves θ unchanged in six years of seven
and moves it *away* from 1.00 in the seventh, because it scales up the same goods
rows the price leg does. See §It was a competing explanation for θ. The sign of
the 2022-2024 result is robust, and so now is the size.

---

## What a column is worth — and it is not dollars

⚠️ **Every ranking in the rest of this plan is dollar-weighted, and dollars are
the wrong unit for an EEIO model.** Which columns "matter", which sources are
worth building, what "a small prize" means — all of it was sorted on dollars.
Cornerstone estimates kg CO2e, so that is the unit a priority ordering has to be
in.

✅ **The weighting is `N`, not `D`** (Wes, 2026-08-25).

- `D` = **direct** kg CO2e per dollar = characterised `B`.
- `N` = **direct + indirect** = `C B L`, with `L = (I − (Adom + Aimp))⁻¹` — the
  same inverse the shipped model builds its own `N` from.

**Why `N`.** A Use-table cell *is* an entry in `A`, so an error in it propagates
through the entire Leontief inverse. What getting a commodity row right is worth
is therefore the **total** emissions embodied in a dollar of it. Some sectors
have much higher `N` than `D` because their indirect is large — and their inputs
are still very important, which `D` cannot see.

**On the 2017 detail intermediate block, by commodity row:**

| commodity rows | dollars $B | dollar % | **`N` %** | (`D` %) |
|---|---:|---:|---:|---:|
| **manufacturing 31-33** | 4,699 | 31.6% | **44.7%** | 28.0% |
| **agriculture 111/112** | 413 | 2.8% | **16.4%** | 23.7% |
| *all other* (mostly services) | 8,260 | 55.6% | **14.3%** | 7.2% |
| utilities 22 | 350 | 2.4% | 13.7% | 26.1% |
| mining 21 | 510 | 3.4% | 8.0% | 11.5% |
| transportation 48-49 | 624 | 4.2% | 2.9% | 3.8% |

⚠️ **Manufacturing nearly doubles and utilities halves.** Electricity's
emissions are almost entirely *direct*, so `D` flatters it; a manufactured good
carries a long upstream chain that only `N` counts. ⚠️ **Services double**,
7.2% → 14.3% — the block §S4 dismissed is worth twice what the direct weighting
said.

Individual rows move harder still: `31161A` meat processing goes **0.10% →
2.61%**, a 26× change that is all upstream livestock; `531ORE` goes 1.48% →
3.97%. ⚠️ **The block is less concentrated on `N`** — the top 10 rows are 48.5%
against 65.3% on `D`, and the top 25 are 65.6% against 85.1%.

⚠️ **Any ranking in this plan dated before 2026-08-25 is a `D` ranking at best
and a dollar ranking usually.** Re-check before quoting one.

⚠️ **Rows and columns are different questions, and confusing them is easy.**
Agriculture *rows* are 16.4% of impact — what everyone buys **from** farms. The
13 agriculture *columns* are 6.9% — what farms **buy**. A seed on the
agriculture column moves the 6.9%, not the 16.4%.

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

✅ **This ranking survives the single-vintage rebuild unchanged — every figure in
the table above is identical to the digit.** It was worth checking, because
§The revision floor shows `GFGD` and `521CI` revise by more than `ORE` does and
`521CI` is not on this list at all. The reason it survives is that the seam sits
between 2022 and 2023: **2017 and 2018 are identical across the two vintages**,
and 2024 was only ever read from the current one, so this 2017-against-2024
comparison never crossed it. The columns named here — and therefore the
candidate list #705 tested against them — rest on one basis and always did.

⚠️ **The 2022 ranking is a different matter, and it does not survive.** Scored
on one vintage, four columns enter its top ten (`5412OP`, `81`, `23`, `513`) and
four leave (`521CI`, `311FT`, `4A0`, `561`), and `ORE` moves from second to
first as its 2022 dissimilarity goes 0.084 → **0.158**. Nothing on this page
ranks columns at 2022, so no verdict rests on it — but any future one must read
the current workbook, which the diagnostic now does.

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
- ❌ **"Agriculture is not in this list at all" is wrong**, and doubly so. It was
  a *dollar* judgement taken at *summary*. At **detail on `N`** the farm columns
  are among the **worst-drifting in the table** — `1111A0` 0.474, `1121A0`
  0.424, `1111B0` 0.311 — against an economy-wide 0.0606. `111CA` carries little
  of the *dollar* drift. ⚠️ **But "a small prize" was a dollar judgement and does not
  survive re-weighting** — see §What a column is worth. The 13 agriculture
  columns are **1.8% of intermediate dollars and 6.9% of impact on `N`**, and
  BEA's own `111CA` commodity mix drifts **0.059** (`N`-weighted) by 2022. It is
  a modest prize, not a negligible one, and ERS FIWS is the only source in this
  step that reaches **2025**.
- **`ORE`, `42`, `23`, `5412OP`, `81` and `722` are where the dollars are.**
  ⚠️ Each of them *does* have an expense source — BEA named one for every one of
  them at the benchmark — and §Sourcing the columns that actually drift tested
  each in turn. ⚠️ **That section's verdicts were also taken on dollars**, and
  two of them flip on `N`: see §The verdict, in one table.

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
here is a commodity to distrust in 2018-2023 — with no published table to catch it.

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
therefore the *purchaser*-price movement.

**The valuation chain, off the summary Supply table.** `T013` is total product
supply at **basic** prices, `T014` is total trade and transportation margins,
`T015` is total tax less subsidies on products, and `T016` is total product
supply at **purchaser** prices. ⚠️ `T014` is the margins *alone*, not a running
subtotal — the identity is `T016 = T013 + T014 + T015`, verified to $1M at
detail. So:

```
PRO_c = T013_c + T015_c                    (basic + net product taxes)
PUR_c = PRO_c + T014_c = T016_c

PUR_c(t) / PUR_c(2017)  ≈  [ producer price ratio ] × [ 1 + μ_c(t) ] / [ 1 + μ_c(2017) ]
     where  μ_c = T014_c / (T013_c + T015_c)
```

⚠️ **The denominator is producer value, not basic value.** BEA gross output is
valued at **producers' prices**, so the price ratio #497 carries already contains
the product-tax layer and only the margin layer is missing — one factor, not two.
Dividing margins by `T013` instead would overstate the rate by the tax wedge:
median **3.3%**, and worst on exactly the commodities that matter here —
`315AL` apparel 1.372 against 1.793 (**+31%**), `324` petroleum 0.246 against
0.289, `313TT` textiles 0.779 against 0.858, `311FT` food, beverage and tobacco
0.489 against 0.533.

#497 as written supplies only the first factor. The second is not small. Summary
Supply, 2017 → 2024, over the 26 commodities with `μ` above 1%: **median absolute
change 3.6pp, p90 12.7pp**, and in relative terms `313TT` textiles 0.779 → 0.949
(**+22%**), `337` furniture 0.690 → 0.809 (+17%), `339` misc. manufacturing
0.853 → 0.988 (+16%), `324` petroleum 0.246 → 0.275 (+12%), `315AL` apparel
1.372 → 1.466 (+7%).

⚠️ **Apply the factor only to margin-*receiving* commodities.** For the margin
*suppliers* `T014` is large and negative — the margin is allocated away from the
transport or trade commodity and onto the goods it carries, which is why
`Σ TRADE = Σ TRANS = 0` — so `μ` runs to −0.94 for `42` wholesale, −0.99 for
`486` pipeline, −0.88 for `482` rail. `1 + μ` is then 0.06, 0.01 and 0.12, and
the factor becomes a ratio of two near-zero numbers: unstable, and not the
economics of those rows anyway. **Set the factor to 1 wherever `μ_c ≤ 0`.** This
is nearly free rather than a compromise: in the *purchaser*-priced SUT Use table
the margin rows carry almost no intermediate dollars (`4B0000` is 0, see above),
precisely because their margins are sitting inside the goods rows.

On the rows where it does apply the denominator correction is real but
second-order, because a proportional level error largely divides out of a ratio:
across the 26 receiving commodities above $20B it moves the carry factor by a
median of **0.35pp**, p90 **1.4pp**, and at most 2.1pp among named commodities
(`313TT`). It is one line of code and worth having; it is not what makes or
breaks the term.

**This is a concrete, cheap improvement to #497 that uses data Step 4c already
produces for its own reasons**, and it is the one place the margins data belongs
inside Step 3.

✅ **Built, at detail** (`nowcast_intermediate.margin_rate_factor`), and the
piece that was not obvious is how an *annual* rate reaches 402 rows at all: BEA
publishes the Supply table at detail only for benchmark years. So the rate's
**level** is detail-observed at 2017 and only its **movement** is borrowed from
the summary parent — `μ_c(t) = μ_c(2017) × μ_P(t)/μ_P(2017)`.

✅ **S0a made that rule testable, and it is the better of the two.** 2012 is a
benchmark, so the true detail factor is observed there. Weighted by 2017
intermediate dollars, against the truth:

| rule | weighted MAE |
|---|---:|
| detail level × parent movement | **0.756pp** |
| the parent's factor, taken down unchanged | 1.010pp |
| no factor at all | 1.818pp |

So the shipped rule recovers about three-fifths of the observable movement and
the naive one about two-fifths. ⚠️ 2012→2017 is a quiet span; nothing here says
the rule holds up as well across the 2021-22 surge, and no detail observation
exists to check that.

⚠️ **A year with no published Supply table is refused, not carried.**
`MARGIN_YEARS` is 1997-2024 — BEA's Supply vintage — and it is a *separate*
constraint from `INTERMEDIATE_YEARS`, which is bounded by gross output. They
agree at 2024 today, so nothing is blocked; a 2025 build ([#707](https://github.com/cornerstone-data/bedrock/issues/707)) would reach a
year with a gross-output parquet and no Supply table, and `margin_rate_factor`
raises there. Falling through to a factor of 1.0 would read as "margins did not
move" and carrying the last published rate would read as a measurement — both
invisible in the built block. The caller either waits for BEA or passes
`margins=False` and says in writing that the deflator is the producer one.

⚠️ **One vintage, not the pinned one.** `_load_usa_summary_sut` picks the
workbook by year, which would take a ratio's numerator and denominator off
different vintages. The two disagree by a median 0.50pp on 2020's rates and
1.20pp on 2022's — but **exactly 0.00pp on 2017**, because BEA does not revise
the benchmark. So reading the current vintage throughout costs nothing at the
base and removes the seam at the target.

### ⚠️ It was a competing explanation for θ, and it has been tested and rejected

§Inflation reads the summary panel's low θ as substitution under relative-price
dispersion. A *missing deflator term* produces the same symptom, so the two were
separable by experiment rather than by argument: fit θ with and without this
factor (`--theta`), and see whether adding it pulls θ toward the detail panel's
1.00.

**It does not** — and running the experiment turned up something larger than the
question it was asked.

| year | θ, price only | θ, + margin leg | θ moves by |
|---|---:|---:|---:|
| 2018 | 0.75 | 0.75 | 0.00 |
| 2019 | 0.75 | 0.50 | **−0.25** |
| 2020 | 1.00 | 1.00 | 0.00 |
| 2021 | 0.50 | 0.50 | 0.00 |
| 2022 | 0.25 | 0.25 | 0.00 |
| 2023 | **−0.25** | **−0.25** | 0.00 |
| 2024 | **−0.50** | **−0.50** | 0.00 |

θ is unmoved in six years of seven and moves *away* from 1.00 in the seventh. The
score at the fitted θ differs by less than 0.001 either way — very slightly worse
with the term at 2018-2022, very slightly better at 2023-2024. ✅ **The
substitution reading stands, and it now survives a correct purchaser-price
deflator rather than merely predating one.**

⚠️ **This is not a null test.** The factor is live: 26 of 73 summary commodities
move, the range at 2024 is 0.975 to 1.099, and the dollar-weighted mean deviation
across the 2017 intermediate rows is 1.3%. It lands on the largest goods rows in
the table — `325` chemicals ×1.049 on a $705B row, `3361MV` motor vehicles
×1.047, `332` ×1.037, `334` ×1.044, `333` ×1.056.

**And that is why it cannot rescue θ.** The margin leg scales *up* the same goods
rows the price leg scales up — 22 of the 26 touched commodities have a factor
above 1 — while the observed data moves the other way: **17 of those 22 lost
intermediate share**, and the touched set lost **3.26pp** of the block between
2017 and 2024. Both legs are nominal-share-inflating terms on goods during a
period of real substitution away from goods, so a more correct deflator pushes
harder in the direction that was already overshooting.

✅ **Keep the term anyway.** It is the correct deflator for a purchaser-valued
cell, it costs one line, and the case for it never rested on this experiment.
What changes is the claim attached to it: it is a correctness fix worth a median
0.35pp on the carry factor, **not** a repair for θ.

### ⚠️ θ goes negative, and that is the finding

**2023 fits θ = −0.25 and 2024 fits θ = −0.50.** The frozen structure scores
*better* when commodity shares are moved **against** their own price movement.
This is not "inflation stops helping"; it is inflation running the wrong way, and
it is a stronger statement than anything §Inflation previously claimed.

⚠️ **The earlier draft could not see it.** `THETA_GRID` started at 0.0, so 2023
and 2024 both pinned to the floor and were reported as θ = 0.00 — "the carry
contributes nothing". The grid now runs to −1.0 and the floor is no longer
binding. The detail 2012→2017 span still fits **1.00**, interior to the old grid,
so nothing measured there moves.

The span from **+1.00 at 2020 to −0.50 at 2024** is monotone in the price regime
and is the §Inflation mechanism at full strength: when relative prices disperse
hard, nominal shares move *opposite* to price because the quantity response
outruns the price effect. Elasticity above one, in nominal terms.

⚠️ **Direct consequence for S1.** #497's θ = 1 is not merely too strong for the
target years — it is the **wrong sign**. A build that hardcodes θ = 1 and ships a
2024 table applies a correction pointing away from the answer on the largest
goods rows in the block.

✅ **What the build ships instead is θ = 0, not the fitted negative** — see §S2.
The fitted values are real and the sign is real, but the *gain* they buy over a
frozen `A` is under 1% of the score on these spans, and a negative θ asserts a
mechanism nobody has proposed. Rounding up to zero keeps the 12.6% that θ = 1
was giving away and declines the last 0.9%.

⚠️ **So the θ gap is still unexplained by anything measured.** Detail 2012→2017
fits 1.00; summary fits 0.75 falling to 0.00 by 2023. The regime story predicts
that, and nothing else on this page tests it. The next candidate is the one
§Inflation already names — θ as a function of relative-price dispersion in the
span — and it needs the benchmark panel (§S0a) to get a second detail span.

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
| **Census `govslocalfin`** (#578) | function × object totals | a total, again — the function detail looked like a *potential* mix proxy | ❌ **tested and rejected** — the function mix moves 0.046 in five years against a 0.201 commodity drift; §The government function bridge |
| **NIPA T31105** defense by type | 14 leaves, one of which maps | one cell (`33299A` ammunition, −0.2%); the rest miss by 46-287% | ❌ control and hint only, as the plan already records |
| **EC / ASM / AIES expenses** 2017-2023 | **level and coarse mix**, 14-19 cells at 6-digit — materials, fuels, electricity, contract work, resales and 9-12 named services | far more than the two cells this row used to claim: **6.4% of the column** seedable onto commodities (not the 11.7% first reported — see §S3b), plus the annual materials *level* that kills linear interpolation | ✅ **built** — `Census_EC_Expenses`, `Census_ASM_Expenses`, `Census_AIES_Expenses`; see §4 and §S3b |
| **SAS Table 3** service expenses | totals at 227 six-digit NAICS | a *detail* total inside a summary constraint — the one case where a total is not redundant | ⚠️ see below |
| **SAS Table 5** selected expenses | **mix**, ~35 items at 63 industries, 2-4 digit NAICS, **2013-2017 and 2020-2022** | the structural service source the probe looked past, and far more than Table 3 offers | ⚠️ **tested and mostly rejected** — the two eras sit on different Economic Census benchmarks; §Sourcing the columns that actually drift |
| **AWTS / ARTS Business Expenses Supplement** | **mix**, 13 items, 2017 and 2022, one benchmark | the trade counterpart, and BEA's own benchmark source for `42` and `4A0` | ❌ **suppression** — `4A0` loses every item; same section |
| **AIES `exp02`** | one row per service sector, 2023 only | nothing at BEA detail | ❌ as #564 found — **re-confirmed**: AIES publishes *no* expense cell for sectors 21, 22, 23 and 51-81 at **any** NAICS level, so it cannot source the worst drifters. Its expense block is manufacturing-only, which is why it appears above as a materials source and not as a services one |

Three of those rows changed verdict, and each is a task:

**#578 needed rescoping, and the rescoped version has now been tested and
rejected.** Its stated premise was wrong in both directions: the `G*` columns are
the *worst-drifting* in the table, so a mix is exactly what they need, and the
column total it offers is already free (NIPA matches `T005` to the dollar, so
even the total is better sourced elsewhere). What `govslocalfin` uniquely has is
**function × object** — education, highways, police, hospitals, utilities — and
function looked like a plausible bridge to commodity mix, because a highway
department and a school district buy different things.
❌ **They do buy different things, and it is not nearly enough.** The go/no-go
ran before the extractor did, which was the point of gating it: the function mix
moves only 0.046 in five years, government functions overlap by 36%, and
reweighting BEA's own function split delivers **+2.4%**. See §The government
function bridge — tested, and it is a no-go.

**SAS Table 3 is the exception that proves the rule.** Step 5's column target is
detail gross output, which is already detail — so a detail service *total* adds
nothing after all, and the note in `annual_survey_expense_sources.md` calling it
"the most useful thing the probe found" should be revised. It was the most useful
thing *for a Step 3 that owned its column totals*. Step 3 does not.

**The 2022 Economic Census materials breakout is now the top external candidate.**
It is the only source that speaks to manufacturing's undifferentiated 82%, it is
a genuine second structural observation inside the estimation span, and interpolating
2017→2022 structure and extrapolating past it is a strictly better carry than
holding 2017 for eight years. #564 flagged it as a "consolation prize"; on the
§Finding above it is the main prize.

---

## The materials census — built, and it delivers

`Census_EC_MatFuel` now pulls `ecnmatfuel` for **2017 and 2022**, and
[`inputs_structure.py`](inputs_structure.py) measures what it buys. Both
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

**What survives:** the materials mix moves **0.133 over five years** on
`MATFUEL` codes, and **133 of 193 clean industries move more than 10 points**.
Freezing it across the estimation span discards a reallocation of that size,
and this source observes it. That is the argument.

⚠️ **But do not compare 0.133 against the column's 0.173 — they are different
frames.** 0.173 is a *BEA detail commodity* score and 0.133 is a `MATFUEL`-code
one. On the same frame as 0.173 this block scores **0.094**. See §The mix score
on the frame Step 3 actually seeds, which is where the comparable number and the
corrected claim live.

### Where the movement sits

⚠️ **This table was regenerated after the suppression recovery landed; an
earlier draft of it quoted pre-recovery figures** (`336411` at 0.592 on a $34.2B
bill). The recovery moves both the scores and the weights, and these are the
current ones.

| industry | dissimilarity | materials 2022 $B | reallocated $B |
|---|---:|---:|---:|
| `324110` Petroleum refineries | 0.081 | 628.6 | **50.8** |
| `336411` Aircraft manufacturing | **0.655** | 51.5 | **33.8** |
| `326199` All other plastics products | 0.233 | 54.5 | 12.7 |
| `325199` All other basic organic chemicals | 0.239 | 52.7 | 12.6 |
| `211120` Crude petroleum extraction | 0.329 | 38.4 | 12.6 |
| `325110` Petrochemicals | 0.327 | 34.1 | 11.1 |
| `331110` Iron and steel mills | 0.148 | 71.8 | 10.6 |
| `336414` Guided missile and space vehicle mfg | **0.815** | 10.5 | 8.5 |
| `211130` Natural gas extraction | 0.312 | 26.2 | 8.2 |
| `336390` Other motor vehicle parts | 0.180 | 43.4 | 7.8 |
| `336415` Space vehicle propulsion units | **0.960** | 8.0 | 7.7 |
| `311224` Soybean and other oilseed processing | 0.165 | 45.7 | 7.5 |

`324110` dominates on size rather than rate — 0.081 on a $629B materials bill,
which is crude oil composition moving under a very large column.

⚠️ **The whole aerospace group is the loudest thing here, and none of it is
credible as economics.** `336411` scores 0.655, `336414` 0.815 and `336415`
**0.960** — a score of 0.96 means almost every dollar sits on a different
material in the two vintages, which no real industry does in five years. The
cause is not a code reassignment: all three are heavily withheld, so the score is
measuring the suppression fill rather than the economy. All three fall out of the
clean subsample above, which is the right treatment, and this is the clearest
single demonstration of why the clean subsample is the one to quote.

⚠️ `336411` is separately the industry the trade work flagged for a 100,089 $M
export shortfall (§The row control), so it is doubly one to distrust.

### What is now built, and what it changed

All three of the remaining [#698](https://github.com/cornerstone-data/bedrock/issues/698) items are built.
Two of them dissolved a problem the plan expected to fight; the third overturned
the interpolation form the plan had already chosen.

#### 2. ✅ The `group`-tier within-group split — built, and scored

A `group` cell is divided over the BEA detail commodities its NAICS could be, on
the purchasing industry's **own 2017 Use row** for those commodities. Scored the
way the suppression prior was: demote every `direct` cell to the group one
prefix higher, split it, and compare against the commodity Census actually named.

| prior | 2017 | 2022 |
|---|---:|---:|
| **column** — the industry's own 2017 Use row | **72.0%** | **72.9%** |
| economy — the commodities' economy-wide row totals | 46.9% | 49.5% |

*share of a split landing on the right commodity*

✅ **The column prior puts about 72% of the money where it belongs, against 47%
for an economy-wide one.** It is not close, and it settles the choice on
evidence rather than on which sounds more principled.

⚠️ **Accuracy falls off with group breadth**, so the tier is not uniform:

| group width | on right commodity | group-tier $B, 2017 / 2022 |
|---|---:|---:|
| 2-4 commodities | **79.8%** | 134.8 / 188.2 |
| 5-9 | 68.6-72.3% | 152.5 / 217.4 |
| 10-29 | 44.7-51.9% | 76.4 / 124.2 |
| 30+ | — | 2.5 / 0.6 |

About **79% of group-tier dollars sit in groups of nine or fewer**, where the
prior is 69-80% right.

✅ **The weak end used to be the bare `33` prefix — 136 BEA commodities, $29.6B /
$54.3B — and it turned out to be entirely purchased scrap.** Every `MATFUEL`
scrap code begins `33`, which is not a NAICS that maps to any single commodity,
so the prefix walk was filing metal scrap into a split across most of
manufacturing. BEA carries **`S00401` Scrap** for exactly this concept — $49.1B
into manufacturing in 2017 — and Census's "excluding home scrap" is the same
thing: bought in rather than generated on site. The five codes are now mapped
straight onto `S00401` ahead of the prefix walk, and the 30+ band collapses from
$32.1B / $54.9B to **$2.5B / $0.6B**, leaving one 59-member group.

| MATFUEL code | stream | 2017 $B | 2022 $B |
|---|---|---:|---:|
| `33000045` | Iron and steel scrap | 18.73 | 36.75 |
| `33000042` | Aluminum and aluminum-base alloy scrap | 5.37 | 9.00 |
| `33000046` | Copper and copper-base alloy scrap | 2.56 | 3.36 |
| `33000051` | Precious metal scrap | 1.50 | 3.11 |
| `33000053` | Other nonferrous scrap | 1.46 | 2.07 |

⚠️ **Scrap is metal and only metal in this source.** There is no wastepaper
code, no cullet, no plastic regrind and no textile rag code anywhere in
`ecnmatfuel`, so the scrap specificity available on the purchase side covers the
metal streams alone. See [cornerstone-data/methods#59](https://github.com/cornerstone-data/methods/issues/59)
for the fuller picture, including the output side, where `ecnpxi` does carry
paper, plastics and textiles as wholesale recyclable sales.

⚠️ **`331110` iron and steel mills moved from 0.328 to 0.440 scrap-intensity**
between the two censuses, which is a large structural move that a frozen 2017
benchmark cannot see.

⚠️ **The split uses 2017 structure in both vintages**, so it manufactures no
movement *inside* a group; only the group's total moves. That is a real
benchmark dependency, and it is still far lighter than freezing the column: the
group tier is 13.6% / 15.1% of cost and the ~54% that is `direct` moves freely.

#### 3. ✅ The vintage code diff — built, and it is not a diff

The plan expected to lose 10% of each year's cost to the 2017 → 2022 code
revision. It loses none.

✅ **The material axis was never affected.** The two vintages share **289 of 289
and 290** `MATFUEL` codes; the one code unique to either year carries $0.1B.
Every warning in this repo about "the vintages sit on different NAICS bases" was
really a warning about the *industry* axis.

✅ **And the industry axis reconciles completely.** All the off-frame cost is
NAICS 2022 merging pairs of 2017 codes. Taking connected components of the year
concordance and summing the 2017 side to the merged unit puts **100% of both
vintages** on one 365-industry basis, with no split assumption anywhere.

| NAICS 2017 | NAICS 2022 | 2017 $B | 2022 $B |
|---|---|---:|---:|
| `336111` + `336112` | `336110` | 226.7 | **271.2** |
| `322121` + `322122` | `322120` | 15.4 | 14.3 |
| `333314` + `333316` + `333318` | `333310` | 9.1 | 13.8 |
| `335911` + `335912` | `335910` | 5.4 | 13.5 |
| `321213` + `321214` | `321215` | 4.4 | 10.1 |

⚠️ **This does not rescue `336411`.** Aircraft manufacturing was on the shared
frame all along; its 0.592 is the suppression fill, not code churn. The plan's
guess that the vintage diff was "why `336411` matters" was wrong, and the
exclusion in §How far the materials mix actually moved remains the right
treatment.

#### 4. ❌ Linear interpolation **of the level** — tested, and rejected

⚠️ **This section is about the materials *level*, not the mix.** The two were
run together in an earlier draft and they have different answers: the level is
observed annually and so needs no interpolation at all, while the mix is observed
only at the two censuses and its form is settled separately, in
§The interpolation form — answered.

The plan called linear interpolation "the obvious first form". It is obvious and
it is wrong, and the way to see that is to stop treating 2018-2023 as unobserved.
**Manufacturing's materials bill is published every year the census misses** —
ASM through 2021, AIES from 2023 — and two new extractors now pull it:
`Census_ASM_Expenses` and `Census_AIES_Expenses`.

| year | source | linear $B | observed $B | gap | industry WAPE |
|---|---|---:|---:|---:|---:|
| 2017 | Economic Census | 2,830 | 2,830 | — | — |
| 2018 | ASM | 3,019 | 3,053 | −1.3% | 0.07 |
| 2019 | ASM | 3,207 | 2,966 | +8.1% | 0.11 |
| 2020 | ASM | 3,396 | **2,636** | **+28.8%** | **0.31** |
| 2021 | ASM | 3,584 | 3,109 | +15.3% | 0.17 |
| 2022 | Economic Census | 3,772 | 3,772 | — | — |
| 2023 | AIES | 3,961 | **3,517** | **+8.7%** | 0.14 |

❌ **Interpolating overstates 2020 by 28.8%**, because a straight line between
two points five years apart cannot bend around a pandemic.

❌ **Extrapolating past 2022 gets the sign wrong.** The materials bill *fell*
6.8% into 2023; the line says it rose 5.0%. That is the span the nowcast leans
on hardest and the span the straight line fails worst.

⚠️ **The observed panel ends at 2023.** ASM ends at 2021, the census is
quinquennial, and AIES 2024 still returns `204 No Content`. So the form is
fitted and scored on 2017-2023 and the span stops there — extending it to 2024
is [#707](https://github.com/cornerstone-data/bedrock/issues/707), and 2025 is out of scope.

✅ **What the annual surveys buy is the level and the coarse partition** —
materials, fuels, electricity, contract work, resales, and nine to twelve named
purchased-service cells. ⚠️ **They do not buy the commodity mix.** That remains a
two-point interpolation; what has changed is that its *form* is now an empirical
question with a scoreable answer instead of a default.

⚠️ **Match the scope, or read a definition as a growth rate.** The census
materials universe is `CSTMPRT + CSTFU` (`EXPS_MAT_DVAL + EXPS_FUEL_VAL` in
AIES) — **not** `CSTMTOT`, which also carries electricity, contract work and
resales. Against `CSTMTOT` the 2017-census-to-2018-ASM step is a median 1.181
across industries; on the matched scope it is 1.063, a year of materials
inflation.

### The mix score on the frame Step 3 actually seeds

The group split makes it possible to score the movement where it matters — BEA
detail commodities on the reconciled industry basis, rather than `MATFUEL` codes
against `MATFUEL` codes.

| frame | industries | commodities | 2022 $B | dissimilarity |
|---|---:|---:|---:|---:|
| `direct` only — no benchmark dependency at all | 361 | 138 | 2,151 | 0.0914 |
| `direct` + `group` | 363 | **200** | **2,681** | 0.1068 |
| **unsuppressed, `direct` + `group`** | 194 | 173 | 1,097 | **0.0949** |

✅ **The split is worth having on coverage alone**: it lifts the placeable bill
from $2,151B to $2,681B and the commodities reached from 138 to **200**. Those 62
extra commodities are ones the `direct` tier never touches.

⚠️ These figures are **after** the scrap fix above. Before it the `direct` tier
was 137 commodities and $2,097B, and `direct + group` reached 204 — but four of
those extra commodities were reached only because scrap was being smeared
across the bare `33` group, so losing them is the fix working rather than
coverage being lost.

⚠️ **And it revises the headline again, downward.** §How far the materials mix
actually moved quotes **0.133**, but that is a `MATFUEL`-code score, and the
0.173 it is compared against is a **BEA detail commodity** score. On the same
frame as 0.173, the clean subsample gives **0.0949**.

**So the corrected claim is:** the materials block moves **0.094 over five
years against 0.173 for the whole Use column over an equal-length span** — a
real reallocation, and roughly **half** the column's rate rather than "somewhat
less". Freezing it still discards something this source observes; the honest
size of that something is half what the previous draft implied.

⚠️ Two reasons the commodity score is lower, and both are properties of the seed
rather than of the economy: aggregating 289 materials onto ~200 commodities nets
off substitution *within* a commodity, and the group split holds 2017 structure
fixed inside each group. Neither is a correction to the 0.133 — they are
different questions, and 0.094 is the one Step 3 is asking.

### ⚠️ BEA has not used the 2022 Economic Census

BEA's 2022 and 2023 summary tables are still annual-survey updates carried over
the 2017 benchmark. **Nothing here should be validated against them.**
Aggregating a census-seeded intermediate block to summary and differencing it
against BEA's published 2022 Use is not a check — a gap is expected, and it is
not evidence of an error on this side. This bites hardest on **Intermediate Uses
and Intermediate Supply**, which is exactly what Step 3 builds.

That cuts both ways, and the second way is the point: **this is information BEA
has not yet incorporated**, which is the strongest available argument for
seeding from it rather than from a carried-forward 2017.

---

### ✅ The interpolation form — answered, and it is not the price path

The form left open above has been fitted, in
[`inputs_structure.py`](inputs_structure.py) (`--form`, `--seed`).

⚠️ **First, separate the two things that were being called "interpolation".**
The materials **level** is observed every year — ASM to 2021, AIES from 2023 —
so it never needed interpolating, and §4 above rules out doing so. The
**commodity mix** is observed only at 2017 and 2022, and *nothing* observes its
interior: the fuels share of the census universe sits between 0.98% and 1.14%
across the whole span, so the coarse annual partition constrains the mix hardly
at all. The open question was only ever about the mix.

#### ❌ The price-carried path is rejected

The candidate this plan named was a price-carried path, reasoning that #497
already carries a commodity price index and the observed level path is dominated
by price. Carrying the 2017 census mix to 2022 on `(p_c(2022)/p_c(2017))^θ` and
scoring against the observed 2022 census mix:

| frame | best θ | gain over frozen |
|---|---:|---:|
| `direct + group` | −0.25 | +0.4% |
| **unsuppressed, `direct + group`** | **0.00** | **0.0%** |

❌ **θ fits 0.00 on the frame this plan says to quote.** The price carry buys
*nothing* on the materials mix. **The level moves with price; the mix does not**
— which is the same lesson §Inflation learned on the summary panel, arriving
from a different direction.

#### ⚠️ The summary panel cannot arbitrate the form, and that is not a detail

The obvious test is to take the annually-published summary Use SUT, interpolate
between two years five apart and score against the years in between. ⚠️ **That
test is circular and its answer must not be used.** BEA's annual tables are the
last benchmark carried forward on annual indicators, so every interior year of a
summary span is *itself* an interpolation; scoring a candidate against it
measures agreement between two methods rather than the shape of the thing. And
because **BEA has not incorporated the 2022 Economic Census**, its 2022, 2023 and
2024 tables are still *2017*-benchmark carries — so a span with an endpoint
anywhere in the nowcast horizon is contaminated at the endpoint as well as in the
interior.

✅ **The benchmark detail panel can arbitrate it**, and this is a second use for
what S0a built: 2007, 2012 and 2017 are three *independent*
Economic-Census-anchored observations. Interpolate 2007 → 2017, score at the
observed 2012:

| form | manufacturing | whole table |
|---|---:|---:|
| frozen at 2007 | 0.0889 | 0.1323 |
| linear | 0.0764 | 0.1190 |
| **geometric** (log-linear in shares) | **0.0710** | **0.1164** |
| endpoint — adopt the 2017 mix at once | 0.1216 | 0.1712 |

✅ **Interpolating beats freezing by 20.1%**, ✅ **geometric beats linear by
7.1%** — the form is worth choosing rather than defaulting — and ❌ **adopting
the newer observation immediately is worse than freezing.** So *"just seed every
year from the 2022 census, it is nearer the target years"* is the one candidate
that loses to doing nothing.

#### ❌ But do not extend the trend past the last observation

Step 3's target years run past the last census, so the tail needs its own test.
From 2007 and 2012, reaching the observed 2017:

| form | manufacturing | whole table |
|---|---:|---:|
| **hold the 2012 mix** | **0.1232** | **0.1733** |
| linear trend extended | 0.1569 | 0.2455 |
| geometric trend extended | 0.1513 | 0.2546 |

❌ **Extending a mix trend one span past its last observation costs 27.4% on
manufacturing and 41.7% on the whole table.** A mix trend does not persist.

✅ **So S3 ships an asymmetry, and every leg of it is measured:** geometric
interpolation between the two censuses, and the 2022 mix **held flat** for 2023
and 2024.

⚠️ **One clean interior observation, on a ten-year span**, transferred to the
census's five-year one. It is the only clean test the data admits — not a large
sample — and the honest reading is that it settles *which* form rather than
proving the form is right.

#### What the seed moves

`materials_seed(year)` returns commodity × BEA detail industry in $M on the
benchmark Use axes, manufacturing columns only — the same shape `nonmaterial_seed`
returns, so the two compose. ✅ **The form is an index on the share, not a
substitution of the level**, for the reason §The 2017 anchor gives, and the
column total is held because Step 3 owns the level through `GO − VAPRO`.

| year | treatment | moved from frozen 2017 |
|---|---|---:|
| 2017 | census observed | 0.0000 |
| 2018 | interpolated | 0.0192 |
| 2019 | interpolated | 0.0378 |
| 2020 | interpolated | 0.0567 |
| 2021 | interpolated | 0.0770 |
| 2022 | census observed | 0.0979 |
| 2023 | held at 2022 | 0.0979 |
| 2024 | held at 2022 | 0.0979 |

⚠️ The seed runs on the **full, suppression-recovered** frame rather than the
unsuppressed subsample, because the seed wants every industry it can reach where
the *score* wants only clean ones — on the unsuppressed frame alone the same
table peaks at 0.0414. The 0.0979 at 2022 sits alongside §The mix score's
independently-computed 0.0949, which is the construction checking out.

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

## Sourcing the columns that actually drift — S4, answered

[#705](https://github.com/cornerstone-data/bedrock/issues/705) asked, per column, *what can source this*.
The answer is **one marginal go and four no-goes**, and the reason the no-goes
are no is not that the sources are missing — every one of them exists, and BEA
named it.

### What BEA itself used, and why that reframes the question

The 2017 benchmark methodology says, per column group:

| column group | BEA's stated source for the **input structure** |
|---|---|
| **Construction** | 2017 Economic Census construction data |
| **Wholesale, retail, accommodation and food** | Census **AWTS / ARTS quinquennial Business Expenses Supplement** — 13 named items, interpolated against the 2012 benchmark |
| **Services, transportation, utilities** | **2017 SAS operating expenses** — 14 named items; non-Census industries from Amtrak, STB, Alaska Rail, DOE, FRB |

and the annual updates move those columns on the Census **Value Put in Place**
(construction), **AWTS** (wholesale), **CPS/HVS** (housing) and the **Census
Annual Survey of State and Local Government Finances** (government).

Two things follow, and both change what S4 is.

1. ⚠️ **These columns are not "unsourced".** Each has a named source that BEA
   already used at the benchmark. The question is never *does a source exist* —
   it is **does a later vintage of the same source exist, on the same basis, and
   does using it beat holding BEA's 2017 answer**.
2. ✅ **The item lists BEA names are exactly the BES/SAS expense taxonomy** —
   electricity, fuels, repairs to machinery, repairs to buildings, rental of
   machinery, rental of buildings, advertising, professional and technical,
   data processing, communication, water/sewer/refuse, purchased transportation.
   The commodity mapping this step would need has therefore already been done
   once, by BEA, on the same names.

### How much of each column those named items can reach

Share of the **2017 detail** intermediate column sitting on BEA rows that a named
BES/SAS/`ecnbasic` item maps to. This is the ceiling on what any indexed seed
built from these sources can move, before any question of whether it should:

| column | column $M | reachable by named items | largest single item |
|---|---:|---:|---|
| `4A0` Other retail | 379,058 | **68.7%** | rent of land/buildings 24.6% |
| `GSLG` State and local | 724,015 | 66.2% | professional/technical 12.2% |
| `ORE` Other real estate | 747,586 | **62.1%** | rent of land/buildings 23.0% |
| `5412OP` Misc. professional | 505,468 | 59.4% | professional/technical 23.2% |
| `42` Wholesale | 877,730 | 53.6% | transport/warehousing 11.5% |
| `622` Hospitals | 360,026 | 51.8% | rent of land/buildings 17.4% |
| `81` Other services | 275,706 | 47.9% | rent of land/buildings 18.6% |
| `GFGD` Federal defense | 218,672 | 43.1% | professional/technical 17.0% |
| `722` Food services | 376,434 | 37.2% | rent of land/buildings 13.0% |
| `23` Construction | 737,745 | **25.0%** | professional/technical 7.0% |

**These are not manufacturing's numbers.** S3b could seed 6.4% of the
manufacturing column from named non-materials cells because 79% of that column is
materials. Here the same taxonomy reaches half to two-thirds of the column,
because these industries have no materials bill — which is why the question was
worth asking, and why a negative answer needs to be earned rather than assumed.

### The test, and the direction it is biased in

Every candidate below is scored the same way. For an industry `j` and an expense
item `k` mapped to commodities `C`:

    seed[c, j] = Use2017[c, j] × ( survey[j, k, t] / survey[j, k, 2017] ) / g[j, t]

where `g[j, t]` is the industry's **own** growth in the same items, so the index
carries only *relative* movement — the level is Step 5's job and the shape is
Step 3's (§The finding). The seeded column is renormalised and scored against
BEA's published summary Use for that year on the index of dissimilarity, beside
a frozen 2017 column. This is the form S3b settled on and the metric
[`intermediate_structure_drift.py`](intermediate_structure_drift.py) reports.

⚠️ **The test is biased against every candidate here, and knowing which way
matters more than the scores do.** BEA's 2020-2022 summary Use carries the 2017
benchmark structure forward on annual indicators, and BEA built that structure
**from the 2017 vintage of these very sources** — the 2017 BES and the 2017 SAS.
BEA has not used the 2022 Economic Census (§BEA has not used the 2022 Economic
Census) and has not used the 2022 BES. So a seed built from the *later* vintage
of the same survey is scored against a table that contains the *earlier* vintage
of it, and every real movement the new data reports is counted as error.

So: **a win here is strong evidence and a loss is weak evidence.** Where a
candidate loses, the verdict below rests on a defect that can be pointed at —
a benchmark seam, a suppression pattern, an item ratio that is not economics —
and not on the score alone.

### ⚠️ The revision floor — and the seam that used to sit under `--drift`

Everything scored above, and everything in §How stale does a frozen 2017
structure get, compares a 2017 base against a later published year. Those reads
**used to go through `_load_usa_summary_sut`, which pins the workbook by year**:
2017-2022 from `Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx`, 2023 and
2024 from `..._1997-2024_Summary.xlsx`. The pinning is deliberate and right for
FBA consumers — published FBAs must not move under BEA's revisions — and wrong
for a diagnostic that differences years against each other. ✅ **The diagnostic
now reads the current workbook for every year**, and `--revision` reproduces the
table below, which is what the seam was worth.

**Same year, both workbooks, dollar-weighted index of dissimilarity:**

| year | all columns | `ORE` | `ORE` level, old → new $M |
|---|---:|---:|---|
| 2017 | **0.0000** | 0.0000 | 747,583 → 747,583 |
| 2018 | **0.0000** | 0.0000 | 761,063 → 761,063 |
| 2019 | 0.0190 | 0.0461 | 894,373 → 843,500 |
| 2020 | 0.0375 | **0.0995** | 843,940 → 803,625 |
| 2021 | 0.0407 | **0.0963** | 937,328 → 987,639 |
| 2022 | **0.0557** | **0.0976** | 1,004,295 → 1,091,894 |

✅ **2017 and 2018 are identical across the two vintages**, so the base year of
every measurement on this page is safe and the drift numbers are not built on
sand.

⚠️ **But the revision floor at 2022 is 0.0557 against a same-basis drift of
0.0859** — nearly two thirds of the signal — and for individual columns it is
larger than the signal: `ORE` revises 0.098, `GFGD` 0.178, `521CI` 0.108, `22`
0.135. **A gap of two or three hundredths between a seeded column and a frozen
one is inside BEA's own revision noise**, which is the right way to read the
near-misses in the verdicts above: they are not evidence that the seed is worse,
only that this test cannot tell.

✅ **The 2018-2024 series no longer changes basis at 2023.** It used to: 2018-2022
came from the older vintage and 2023-2024 from the newer, so the published series
stepped rather than drifted at that join — which is where `525`'s intermediate
column appeared to jump +100.8% and `ORE` +16.8%. **That step was the workbook,
not the economy**, and it is gone; what it had been hiding was 0.005-0.011 of
real drift in 2019-2022 (§How stale does a frozen 2017 structure get).

#### What this does to `ORE` — it makes the problem bigger, not the answer

✅ **`ORE`'s 2022 drift against BEA's current numbers is 0.158 rather than 0.084**
— nearly double what the pinned loader reported, because the newer vintage moved
the column further from its 2017 base. (At 2024 it is 0.141 and always was: that
year never crossed the seam — see §Where the drift actually sits.) `ORE` is a worse column than §Where the
drift actually sits says, and the seed's gain against it is scored on this
vintage throughout (§`ORE` below).

⚠️ **But the mechanism has to be restated honestly.** BEA revised `ORE` back to
2019 and not to 2017 or 2018, and the revision lands on **the same two rows the
drift does**: `55` management of companies is +1.07pp of drift on the old vintage
and **+6.60pp on the new**, of which +5.53pp is pure revision; `561` is +2.92pp
old, +5.83pp new, +2.91pp revision. That is the shape of the **equity-REIT
reclassification** — a population moved into the real estate industry, bringing
its corporate-HQ and administrative purchasing with it.

So what the SAS index is tracking is not only a change in how a lessor buys; it
is partly **the same reclassified population showing up in Census's 531 frame**.
✅ That is a legitimate reason to use it — the nowcast should land on BEA's
current basis, and this moves it there — but it is not the input-substitution
story, and the seed should say so where it is written.

⚠️ **`561` as a *column* does not survive either**: it gained on the old vintage
at 2022 and loses on the new one. It was a vintage artefact, and `ORE` is the
only column left standing.

### `ORE` / `531ORE` Other real estate — ✅ **built, and not marginal after all**

⚠️ **The "marginal" verdict below was a dollar verdict and is superseded.** On
`N` (§What a column is worth) `ORE` gains **+25.1 / +32.6 / +37.2%** against a
frozen 2017 at 2020 / 2021 / 2022, and it carries **59-83% of the whole
services-and-transportation block's gain** on its own. The +4.5% figure quoted
throughout this section is the dollar-weighted one; read it as the *lower*
bound, not the result.

**First, two things the issue asked to establish.**

✅ **The drift is not the housing imputation.** BEA summary `ORE` is detail
`531ORE` alone; owner-occupied and tenant-occupied housing are `531HSO` and
`531HST`, which sit in the separate `HS` column. Nothing in `ORE`'s 0.141 is the
imputed series.

⚠️ **Part of it is a classification change, and that is now measured rather than
suspected.** BEA has reclassified **equity REITs out of funds, trusts and other
financial vehicles and into the real estate industry** (mortgage REITs stay), on
the grounds that the two have different production processes, and it lands in
`ORE`. §The revision floor below pins it: BEA revised `ORE` back to **2019 and no
further**, so the 2017 benchmark base is untouched while every later year moved,
and the revision sits on the same `55` and `561` rows the drift does. The
apparent +100.8% jump in `525` at 2023 is **the workbook changing under the
diagnostic, not a break in the economy** — see below. A reclassification is a
re-basing job rather than a seeding job, so read `ORE`'s drift as *part
reclassification* throughout what follows.

**What is left is still the largest drifting column, and it does have a source.**
SAS Table 5 publishes NAICS `531` — 2013-2017 and 2020-2022, now extracted as
[`Census_SAS_Expenses`](../../extract/census/Census_SAS_Expenses.yaml) and seeded
by [`services_transport_expense_seed.py`](services_transport_expense_seed.py). Nine items span the 2017
base and reach 45.7% of the detail column. Scored on BEA's **current** vintage:

| endpoint | frozen | seeded | gain |
|---|---:|---:|---:|
| 2020 | 0.1970 | 0.1883 | +4.4% |
| 2021 | 0.1642 | 0.1579 | +3.8% |
| 2022 | 0.1579 | 0.1508 | **+4.5%** |

✅ **Positive at all three endpoints, on a test biased against it** (§The test,
and the direction it is biased in), and no single item carries it — dropping
temporary staff, the largest contributor, leaves +2.2%.

⚠️ **But +4.5% is *at* the bar, not over it.** §Inflation puts the price carry at
about 4% in a quiet span. This clears what #564 could not, and it does not clear
it by much.

⚠️ **An earlier draft of this section reported +18.9 / +24.5 / +24.7% and those
numbers were wrong.** They came from applying each item's index to whole
**summary** rows rather than to the BEA detail rows the item actually names, and
the error was not a blur — it was an inflation. `Temporary staff and leased
employee expense` maps to `561300` employment services, **$8.6B of `ORE`'s
column**. At summary it multiplied all of `561`, **$97.8B**, three quarters of
which is `561700` services to buildings and dwellings — a lessor's janitorial and
landscaping bill, which the survey item does not describe and which moved for its
own reasons. The lesson generalises to every candidate on this page: **a coarse
commodity mapping applies a measured ratio to dollars the source never measured,
and it flatters the result rather than fuzzing it.** Build the seed at detail and
aggregate to score; never the reverse.

#### Why it is only 4% — the movement is on rows nothing names

`services_transport_expense_seed.py --reachable` splits `ORE`'s 2017 → 2022 share change
into what a SAS item can touch and what it cannot:

| row | movement pp | 2017 share | reachable |
|---|---:|---:|---|
| `55` Management of companies | **+6.60** | 0.95% | ❌ no item names it |
| `561` Administrative and support | +5.83 | 13.09% | ⚠️ only via `561300`, 1.15% of the column |
| `521CI` Credit intermediation | −3.00 | 10.92% | ❌ |
| `22` Utilities | −2.37 | 5.65% | ✅ |
| `ORE` Other real estate | −1.82 | 22.96% | ✅ |
| `23` Construction | −1.72 | 5.77% | ✅ |
| `524` Insurance | −1.72 | 7.55% | ❌ (published, but withheld for 531) |
| `562` Waste management | +1.58 | 4.02% | ❌ (item discontinued after 2017) |

**13.51pp of movement is reachable and 18.07pp is not**, and the single largest
mover has no counterpart question in the survey at all. That is the honest
ceiling on this source for this column, and it is why the gain is a twentieth
rather than a quarter.

⚠️ Two of the unreachable rows are unreachable for reasons worth separating.
`524` insurance **is** a SAS item — `Cost of insurance` — but it is withheld for
NAICS 531 in one of the two years, so the seed cannot span the base. `562` waste
is the `Water, sewer, refuse removal` item, one of the three discontinued after
2017. Neither is a gap in the questionnaire; both are gaps in what survives the
seam.

**Verdict: ⚠️ built, marginal, and not the thing to do next.** The extractor and
the seed exist and are measured, which is worth having — `Census_SAS_Expenses`
is the only annual observation of any service industry's purchased inputs, and
#707 will want it. But a 4% gain on one column does not compete with S1 or S2 for
the next hour of work, and the plan should not pretend otherwise.

---

### `42` Wholesale and `4A0` retail — ⚠️ **verdict under review**

⚠️ **BOTH the original test and the retest used the wrong answer key**, and the
retest's conclusion is therefore not safe. Agriculture was rejected the same way
and the rejection **reversed** once regraded — see §The answer key. The trade
BES has 2012 and 2017 vintages, so it *can* be regraded on the holdout, and that
has not been done yet. ❌ **Do not act on the no-go below until it is.**

⚠️ **Read the verdict at the end of this section, not the table in the middle.**
The table was scored at the wrong granularity, on an item set BEA did not use,
in dollars. The retest fixes all three and the answer is a firmer no.

**#564's "absent from AIES `exp02`" was true and was the wrong place to look.**
The trade expense detail is not in the annual survey; it is the **quinquennial
Business Expenses Supplement**, published in years ending in 2 and 7, and both
vintages are live:

- retail `arts/tables/2017/bes.xlsx` and `arts/tables/2022/bes.xlsx`
- wholesale `awts .../2017_awts_detailopex_table5.1_revised.xlsx` and
  `.../2022_awts_detailopex_table5.1.xlsx`

✅ **The item list is identical across the two vintages** — 13 mappable items,
same names, same order, no questionnaire change — and ✅ **both vintages are
benchmarked to the 2017 Economic Census**, so unlike SAS there is no basis seam.
⚠️ **The wholesale 2017 file only satisfies that if the *revised* one is used**:
the original `table5.1` is benchmarked to the **2012** Economic Census and the
revision exists precisely to move it to 2017. Reaching for the obvious filename
imports a rebenchmark as if it were economics.

**It fails on suppression, and the failure is total for the column that matters.**
Requiring an item to be published for every constituent NAICS in both years:

| column | items surviving | reachable share | frozen | seeded | gain |
|---|---:|---:|---:|---:|---:|
| `441` Motor vehicle dealers | 13 of 13 | 57.4% | 0.064 | 0.078 | −21.5% |
| `445` Food and beverage | 5 of 13 | 28.7% | 0.047 | 0.107 | −129.7% |
| `452` General merchandise | 12 of 13 | 43.4% | 0.055 | 0.218 | −299.2% |
| `4A0` Other retail | **0 of 13** | — | — | — | — |
| `42` Wholesale | 3 of 13 | **7.1%** | 0.062 | 0.068 | −9.3% |

⚠️ **`4A0` — 719 $B and the retail column the drift ranking names — loses every
single item**, because it spans nine three-digit NAICS and at least one of them
is withheld for every item in one of the two years. Ignoring that, and letting
the incomplete sums stand, is what produces the ratios near 0.3 that made the
first cut of this test look catastrophic; they were suppression, not economics.
And `452`'s surviving cells still carry contract labour ×4.45 against packaging
×0.54 and communication ×0.38, which is not a five-year change in how a
department store buys.

**Verdict: ❌ no-go, and ⚠️ do not build the suppression recovery.**

⚠️ **This section's own reasoning was wrong in three ways**, all corrected in
[`trade_expense_supplement.py`](trade_expense_supplement.py), and the answer
after correcting them is a *firmer* no:

1. ⚠️ **The test above is at BEA summary; Step 3 estimates BEA detail.** "`4A0`
   loses every item because it spans nine three-digit NAICS" is a summary
   artefact — at detail `4A0` is **six columns**, and **five of the six are a
   single three-digit NAICS**, needing no aggregation at all.
2. ⚠️ **It scored items BEA did not use.** The `452` account above rests on
   *contract labour ×4.45* — ❌ **contract labour is not one of BEA's thirteen
   items** for trade (C2). It is a BES column BEA did not take.
3. ⚠️ **It was dollar-weighted** (§What a column is worth).

**Re-scored at BEA detail, on BEA's own 13 items, at 2022:**

| column | dollar | **`N`** |
|---|---:|---:|
| `42` | −3.3% | −2.5% |
| `441` | −24.4% | **−43.6%** |
| `445` | −24.0% | +5.4% |
| `452` | −132.7% | **−183.6%** |
| `4A0` | −3.3% | −31.8% |

❌ **Every column loses on dollars; four of five lose on impact.**

✅ **`441` is why the suppression recovery must not be built.** It has **13 of
13 items published in both years**, needs no aggregation, and reaches **61.5%
of its dollars and 75.7% of its impact** — and it loses **−43.6%**. ⚠️ **Neither
suppression nor reach is the binding constraint**, so recovering suppressed
cells would buy nothing. That is a build saved.

❌ **The mechanism is absent**, and it is the same one that sank ERS
agriculture: each BES item's movement against its BEA commodities' published
share movement correlates **+0.06 pooled over 54 pairs** (`441` +0.32, `445`
+0.25, `4A0` +0.06, `452` −0.00), against ERS agriculture's +0.18. **A survey
can measure an industry's expenses well and still not describe how BEA moved
the commodity mix**, because BEA does not build the mix from item shares.

⚠️ **Some movements are not credible on their own terms.** `452` reports
building rent falling **$9,037M → $5,577M** — a 38% *nominal* fall in five
years — with professional services ×2.83 and communication ×0.38.

⚠️ **What still stands from the original verdict.** Suppression is real: at BEA
detail, retail has **1 of 9 columns with all 13 items and 3 with none**;
wholesale **0 of 9 and 4 with none**, and 2022 is far more suppressed than 2017
(42.5% of retail cells against 18.8%). ⚠️ **The percent-of-total column is
suppressed on exactly the same cells** — 0 of 471 recoverable by arithmetic.
⚠️ **`425000` ($20.5B) has no AWTS coverage at all**: AWTS surveys merchant
wholesalers, `425` is agents and brokers. And the annual AWTS/ARTS
operating-expense tables publish **a total**, which §The finding says Step 5
already has.

⚠️ **Nothing continues this after 2022** — AIES publishes no expense cell for
42 or 44-45 — so ✅ **no extractor was built**: a `Census_BES` source would
carry two observations and no future.

⚠️ **One caveat on the retest.** 2017 → 2022 is the only span the BES offers,
so the correlations rest on cross-sectional spread inside one interval, not on
repeated observation the way the seven-year agriculture test does.

⚠️ **Nothing continues this after 2022.** AIES publishes no expense cell for 42 or
44-45 at any NAICS level, so the trade BES pair ends at 2022 with no successor.

### `23` Construction — ❌ **no-go**

✅ **`ecnbasic` covers construction expenses fully, in both vintages, cleanly.**
Sector 23 populates eleven expense cells at NAICS-6 for 2017 and 2022 —
subcontracted work, electricity, rental of buildings and of machinery, and the
eight `PCH*` services — and every 2017 → 2022 ratio is between 1.12 and 2.00,
with no break of the kind SAS carries. This is the *best-behaved* pair on the
page.

**It still fails, on reach.** `CSTMPRT` materials is **$598.5B in 2017 and
$845.1B in 2022 — 51% of the column — and it is one undifferentiated cell.**
`ecnmatfuel` is manufacturing and mining only; construction has no materials
breakout anywhere. The named items reach 17.3% of the summary column, and the
seed scores **0.044 against frozen's 0.038**.

⚠️ **Construction's place in the `--where` ranking is column size, not drift
rate.** Its summary mix moves 0.038 over 2017 → 2022 and 0.065 by 2024 — the
lowest rate in the top ten. 78 $B is misplaced because 1.2 $T runs through the
column, not because the column is unstable.

### `5412OP`, `81`, `722`, `622` — ❌ **no-go, and the reason is a seam**

**#564's characterisation of the service sectors needs correcting, and the
correction does not rescue them.** "One row for the entire sector" is true of
AIES `exp02` in 2023. **SAS Table 5 publishes 63 industries at 2- to 4-digit
NAICS** — `5412`, `5413`, `5414`, `5415`, `5416`, `5417`, `5418`, `5419`
separately, `81`, `722`, `621`, `622`, `623` separately — with ~35 expense items.
And it is **annual, not quinquennial**: the item detail runs **2013-2017 and
2020-2022**. The claim in
[`annual_survey_expense_sources.md`](annual_survey_expense_sources.md) that
Table 5 is "63 industries, and 2020-2022 only" reads the latest workbook's
display window as the series; the 2017, 2018 and 2019 vintages carry the earlier
years.

**Two defects sit between that and a usable index, and together they are
decisive.**

⚠️ **1. The 2017 and 2022 observations are on different Economic Census
benchmarks.** The 2017 workbook states its estimates are adjusted to the **2012**
Economic Census; the 2022 workbook states the **2017** Economic Census. There is
no restated 2017 — the detailed-expense series *restarts* at 2020, with nothing
published for 2018 and 2019. So every 2017 → 2022 ratio the seed needs is a real
movement, a rebenchmark and a questionnaire change multiplied together.

⚠️ **2. Three mappable items were discontinued** after 2017 — rental of machinery,
purchased communication services, and water/sewer/refuse — so the two eras do not
even describe the same expense.

**What that produces is visible at the item level.** Broadcasting and telecom's
expensed equipment goes 43,232 → 494 ($M, ×0.01) while its temporary staff goes
617 → 9,141 (×14.8); transport support's repairs to transportation equipment go
×0.10 while its fuels for transportation equipment go ×7.91. These are the same
kind of implausible swing #564 flagged in ASM's `334614` and `322121`, and they
are reporting reallocations, not economics.

**Scored, on the weaker claim the issue asked about — sector movement applied to
detail sub-columns:**

| column | 2020 | 2021 | 2022 |
|---|---:|---:|---:|
| `5412OP` | −22.4% | −6.4% | −79.5% |
| `81` | +0.5% | +1.0% | −5.9% |
| `722` | +2.7% | +0.2% | −2.0% |
| `622` | −8.4% | −4.5% | −14.0% |
| **all 31 columns, dollar-weighted** | 0.090 → 0.120 | 0.088 → 0.096 | **0.099 → 0.131** |

**None of the four beats frozen at any endpoint**, and the aggregate loses at all
three. ⚠️ Two columns that are *not* on the drifter list do beat it —
`561` (+14.9 / +5.9 / +16.9%) and `484` (+10.5 / +9.4 / −5.0%) — which is worth
recording, but neither is where the dollars are.

### What the Economic Census cannot do here, stated once

⚠️ **`ecnbasic` publishes the expense cells for sectors 21, 23 and 31-33 and for
nothing else.** Every service and trade sector returns a zero for every expense
variable at every NAICS level, in both 2017 and 2022; wholesale gets `OPEX`, a
single total. **The route that worked for manufacturing in #698 does not extend
to the drifting columns** — the Economic Census asks those industries no expense
questions, which is why BEA had to reach for BES and SAS in the first place.

❌ **And the three narrow Economic Census datasets that looked like a way in are
not one.** `ecnpurmode`, `ecnpurelec` and `ecnpurgas` were worth checking because
`484000` drifts 0.280 and `22` carries the highest summary drift *rate* at 0.224.
Pulled for both vintages, all three fail, and each fails differently:

| dataset | what it actually publishes | against BEA 2017 |
|---|---|---|
| `ecnpurmode` | **one industry** — `48851` freight transportation arrangement — and its purchased transportation split five ways by mode | $134.7B against the whole `48A000` column of **$73.2B**; ⚠️ it never touches `484000` |
| `ecnpurelec` | two industries, one cell: purchased electricity **for resale** | $100.7B against BEA's `221100`→`221100` cell of **$9.5B**, a 10.6× scope gap |
| `ecnpurgas` | two industries, one cell: purchased natural gas **for resale** | $51.4B against a `221200` column of **$24.0B** and an own-row cell of **$1M** |

⚠️ **The mode mix does move, and that is what makes the answer instructive rather
than dull.** Truck goes 62.3% → 48.3% and water 15.4% → 23.6% between the two
vintages, an index of dissimilarity of **0.170** — larger than almost anything
measured on this page. There is simply no BEA cell entitled to receive it: BEA
books freight arrangement net, and the resale purchases utilities report are not
the intermediate rows BEA carries for those industries. **A real second
observation of a mix is worth nothing when the concept underneath it is not the
concept in the Use table**, which is §S3b's scope-factor lesson arriving from the
other direction — there the mismatch cancelled in a ratio, here there is no cell
to take the ratio against.

### The verdict, in one table

| column | misplaced $M | source found | verdict |
|---|---:|---|---|
| `ORE` | 171,923 | SAS Table 5, NAICS 531 | ⚠️ **built** — +4.5%, at the bar; 18 of 32pp of its movement is unreachable |
| `GSLG` / `GFGD` | 221,864 | `govslocalfin` function × object | ❌ **+2.4%** — the function mix moves 0.046 against a 0.201 drift; §The government function bridge |
| `42` | 118,761 | AWTS BES 2017 (revised) + 2022 | ❌ suppression: 3 items, 7% of the column |
| `5412OP` | 94,444 | SAS Table 5 | ❌ loses at every endpoint; benchmark seam |
| `81` | 90,196 | SAS Table 5 | ❌ no gain at any endpoint |
| `23` | 78,028 | `ecnbasic` sector 23 | ❌ clean pair, 17% reach, 51% of the column is one cell |
| `722` | 77,787 | SAS Table 5 | ❌ no gain at any endpoint |
| `622` | 73,394 | SAS Table 5 | ❌ loses at every endpoint |
| `4A0` | 57,765 | ARTS BES 2017 + 2022 | ❌ suppression: **0 usable items** |

❌ **S4's conclusion — "#564's negative result generalises" — did not survive,
and the table above is superseded.** It was true of the *sources* in three cases
(`42`, `4A0` and `23` still fail on suppression and reach) and false as a
generalisation, for two reasons measured in
[`services_transport_expense_resource.py`](services_transport_expense_resource.py):

1. ⚠️ **Every verdict above is dollar-weighted.** Re-scored on `N` — total kg
   CO2e per dollar, §What a column is worth — the SAS columns do not all lose.
2. ⚠️ **The AIES half of #564 was the wrong endpoint.** `timeseries/aies/basic`
   returns well-formed zeros for services; `exp02` publishes 41 expense
   variables for 13 sectors.

⚠️ **These are graded against BEA's carry-forward** (§The answer key) and are
therefore weak evidence in both directions. ⚠️ **`Census_SAS_Expenses` starts at
2013, so this block cannot be regraded on the 2012 holdout** without
substituting 2013 for the base and saying so.

**Re-scored on `N`**, by
[`services_transport_expense_seed.py`](services_transport_expense_seed.py)
`--by-column`, against a frozen 2017 at 2020 / 2021 / 2022:

| column | §S4 verdict | on `N` | now |
|---|---|---|---|
| `622` | ❌ loses at every endpoint | **+11.4 / +12.3 / +27.5%** | ✅ **overturned** |
| `722` | ❌ no gain at any endpoint | **+7.5 / +11.2 / +10.6%** | ✅ **overturned** |
| `81` | ❌ no gain at any endpoint | −0.8 / +2.9 / +0.6% | ⚠️ **stands** (flat) |
| `5412OP` | ❌ loses at every endpoint | −9.0 / −7.7 / −2.0% | ❌ **stands** |
| `ORE` | ⚠️ built, marginal | **+25.1 / +32.6 / +37.2%** | ✅ much better than "marginal" |

⚠️ **`81` is the cautionary one**: it wins consistently on `D` and is flat on
`N`. Changing the weighting decides verdicts, so a verdict is only as good as
the weighting it was taken under.

✅ **The whole block is built and it wins**, 100 BEA detail columns and $6,779B:
**+10.2 / +10.8 / +9.2%** on `N`, +0.5 / +1.4 / +2.3% on dollars.
⚠️ **`ORE` alone carries 59–83% of that**, and 22, 21 and 17 of 33 scored
columns individually win — the aggregate is carried by weight, not by count.

⚠️ **What does still stand from S4**: `42`, `4A0` and `23` are unchanged, the
suppression finding is unchanged, and the caution below is unchanged.

⚠️ **And one caution against reading these no-goes too hard.** Every score above
is against a BEA table built from the 2017 vintage of the same survey. They rule
out *seeding from these sources as they stand*; they do not establish that the
2017 mix is right. Where BEA later incorporates the 2022 BES and the 2022 SAS —
as it incorporated the 2017 ones — the sign of these tests can change, and
nothing here should be quoted as evidence that these columns are stable.

---

## The government function bridge — tested, and it is a no-go

The go/no-go this plan sequenced S5 behind has been run, in
[`gov_function_bridge.py`](gov_function_bridge.py)
(`uv run python -m bedrock.analysis.nowcasting.gov_function_bridge --all`).
❌ **The answer is no, and the reason is arithmetic rather than data quality.**

### The bound that decides it

The bridge #578 was rescoped to means one model: government functions have
stable input mixes, and what moves the column is the changing weight of
education against highways against police. For function shares `w` and
within-function commodity mixes `m`:

```
s_c(t) = Σ_f w_f(t) · m_{c|f}
```

Hold `m` fixed and the commodity movement that model can produce is bounded by
the **function** movement itself:

```
d(s(t), s(0)) = ½ Σ_c |Σ_f Δw_f · m_{c|f}|
             ≤ ½ Σ_f |Δw_f| · Σ_c m_{c|f}
              = ½ Σ_f |Δw_f| = d(w(t), w(0))
```

— because every `m_{c|f}` sums to 1 over commodities. The bound is attained only
if the function mixes are **disjoint**. ✅ **So the question is answerable without
building the bridge**: measure how far the function mix moves, and measure how
different government functions really are in what they buy. Both are observable
today.

### 1. The function mix barely moves

`govslocalfin`'s 33 state-and-local functions, dissimilarity against 2017:

| 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|---|
| 0.0074 | 0.0093 | 0.0155 | 0.0355 | **0.0464** | 0.0440 | 0.0385 |

**4.6% of government dollars changed function over five years**, and the COVID
bulge at 2021-22 is most of even that. On the current-operations basis the
2022 → 2024 movement is **0.0121**.

⚠️ **That 0.0464 is the ceiling**, against a **0.201** dollar-weighted commodity
drift in the same columns over a comparable span.

### 2. Government functions are not disjoint

BEA's own government columns, pairwise dissimilarity of their 2017 commodity
shares — the direct measurement of how much a function reweighting could matter:

| | `GSLGE` | `GSLGH` | `GSLGO` | `S00500` | `S00600` |
|---|---|---|---|---|---|
| `GSLGE` | — | 0.507 | 0.443 | 0.729 | 0.673 |
| `GSLGH` | 0.507 | — | 0.537 | 0.774 | 0.675 |
| `GSLGO` | 0.443 | 0.537 | — | 0.683 | 0.628 |

Mean off-diagonal **0.639** — government functions **overlap by 36%**, because a
school district and a highway department both buy electricity, professional
services, repair and supplies. So the realistic ceiling is `0.0464 × 0.639 =`
**0.030**, or **14.7%** of what has to be explained, still crediting the model
with a perfect within-function mix.

### 3. Reweighting real functions delivers 2.4%

The bound can be *realised* rather than argued, because BEA's three general
state-and-local columns are themselves a function split — education, hospitals
and health, everything else — whose weights **and** within-function commodity
mixes are both observed, at 2012 and at 2017. Freeze each column's 2012 mix, move
only the weights to 2017, score the aggregate:

| | index of dissimilarity |
|---|---|
| function-weight movement 2012 → 2017 | 0.0453 |
| frozen 2012 mix, scored at 2017 | 0.1946 |
| **function-reweighted, scored at 2017** | **0.1899** |
| | **gain +2.4%** |

⚠️ **97.6% of the drift in these columns is movement *within* a function**, which
no function reweighting can see. And note the weight movement: BEA's 3-function
split moves 0.0453 where `govslocalfin`'s 33 functions move 0.0464 — ✅ **a finer
function list does not help, because disaggregating functions does not move more
dollars between them.** The result is not specific to Census's function taxonomy.

For scale: the inflation carry gets ~4-5% in a quiet span and `ORE`'s built seed
gets +4.5%, both of which this plan already calls *at* the bar rather than over
it. A perfect function bridge lands at half that.

### Two independent defects, either of which would also sink it

⚠️ **`Current Operations` by function does not exist before 2022.** The
intermediate-consumption object — current operations net of the salaries and
wages exhibit — is published **by function only for 2022, 2023 and 2024**. For
2017-2021 it is a single economy-wide number. So there is **no 2017 observation
to anchor the bridge at the seed year**, and the annual movement the source was
wanted for is precisely what it does not carry. The row-count jump
`annual_survey_expense_sources.md` flagged — 137 codes to 232 at 2022 — *is* this
defect, not a coverage curiosity.

⚠️ **The only continuous function series is `Total Expenditure`, and its proxy
error exceeds the signal.** It carries salaries, capital outlay, assistance and
subsidies, and interest alongside the intermediate purchases. Against
`Current Operations` in the three years both exist it is **0.062, 0.062, 0.064**
dissimilar — **larger than the whole 0.0385 of function movement 2017 → 2024**
that it would have to transmit.

⚠️ **`govslocalfin` is state and local only**, and federal is where the worst
column is. Detail 2012 → 2017, the government block's misplaced dollars:

| tier | column $M | misplaced $M | share |
|---|---:|---:|---:|
| federal (`S00500`, `S00600`, `S00101`, `S00102`) | 345,303 | 124,647 | 43.1% |
| **state + local** (`GSLG*`, `S0020*`) | 933,736 | **164,683** | **56.9%** |

`S00500` alone — **83,851 $M misplaced at 0.383, the worst-drifting column in the
entire Use table** — is federal and out of reach of this source at any depth.

### What this closes, and what it does not

✅ **Closed: the function bridge as a route to the `G*` commodity mix.** #578's
rescoped premise was that function is a plausible proxy for what a government
buys. It is plausible; it is also far too small, and it fails on three
independent grounds before the bridge itself has to be judged.

❌ **Not closed: #578's underlying problem.** The `G*` columns still drift, the
drift is still 230-258 $B, and it is **within-function** — a school district
buying a different basket than it bought in 2012, not a shift from schools to
highways. Nothing in this step sources that, and nothing here should be read as
evidence those columns are stable.

⚠️ **Two by-products of the pull survive the no-go and belong elsewhere.**
`Salaries and Wages` is the government `V00100` that Step 2's row split needs,
and **`Capital Outlay` by function *is* published 2017-2024** — unlike current
operations — which makes it the natural check on the final-demand government
investment columns and on §Step 0's open SLG Equipment/Structures/IP attribution
bug. Neither is Step 3's, and neither is claimed here.

---

## The answer key, and why every earlier score used the wrong one

⚠️ **Scores in §S4, §S5 and the trade retest were graded against BEA's published
*summary* Use for 2018–2024. That is the wrong key**, for two reasons Wes
identified on 2026-08-25:

1. ⚠️ **BEA has not incorporated the 2022 Economic Census.** Its 2018–2024
   tables are the 2017 benchmark **carried forward** on BEA's own annual
   methods. ❌ So those tests measure agreement with an extrapolation, not
   accuracy — and a seed that correctly caught real structural change would
   *lose* against a key that has not seen it.
2. ⚠️ **Summary collapses the detail the seed moves.** Ten farm columns are
   summed into `111CA` before scoring; several survey items land on one summary
   code, so distinct movements are averaged away and different items get
   identical targets. ❌ **Any correlation computed that way is depressed by
   construction**, which withdraws the +0.18 and +0.06 figures below as
   evidence.

✅ **[`benchmark_holdout.py`](benchmark_holdout.py) is the key that has neither
problem.** BEA publishes **three detail benchmarks** — 2007, 2012, 2017 — and
`io_2017` reads all of them onto the **same 2017 code axis**. So:

> seed the observed **2012** block with a source's 2012 → 2017 movement,
> and score against the observed **2017** block

✅ Out of sample, ✅ at detail, ✅ against an observation rather than an
extrapolation, ✅ and nothing in the test derives from what is being tested.
⚠️ **Only the mix is on trial** — every column is renormalised to the observed
2017 total, because Step 3 observes the level through `GO − VAPRO`.

**Economy-wide 2012 → 2017 mix drift is 0.0606 impact-weighted**, and that is
the bar.

⚠️ **What the holdout cannot do.** It measures **2012 → 2017**, a span with no
counterpart to the 2021-22 price surge. A source needs a **2012 observation**:
ERS FIWS (1910–2025) and the trade BES (quinquennial) have one;
`Census_SAS_Expenses` **starts at 2013**, so the services and transportation
seed cannot be graded here without substituting 2013 and saying so. ⚠️ **Its
+10.2/+10.8/+9.2% therefore remains ungraded on a sound key.**

⚠️ And at benchmark years BEA built these columns from the same sources, so the
holdout shows a source reproduces BEA's **benchmark process** — the right target
for a nowcast, but not an independent check on the world.

---

## What is still held at the 2017 benchmark

⚠️ **A quarter of the block has no annual source at all** and holds its 2017
mix, carried only by the price step. Worth stating as a list rather than a
feeling, because it is the backlog:

| block | columns | $B | share |
|---|---:|---:|---:|
| census-seeded (§S3, §S3b) | 252 | 4,500 | 30.3% |
| survey-seeded (§S4, services + transportation) | 100 | 6,779 | 45.6% |
| ❌ **held at frozen 2017** | **50** | **3,577** | **24.1%** |

**The held 50, by BEA sector:**

| sector | columns | $B | why | issue |
|---|---:|---:|---|---|
| `G` government | 8 | 1,222 | `govslocalfin` bridge rejected | #578 |
| `42` wholesale | 11 | 878 | AWTS BES suppression, 3 of 13 items | — |
| `44RT` retail | 9 | 668 | ARTS BES suppression, **0 of 13 items** | — |
| `11` agriculture | 13 | 272 | ✅ **seed validated on the holdout, +17.9%** | #577 |
| `PROF` | 1 | 198 | no prefix match in either survey | — |
| `22` utilities | 3 | 160 | survey refused on the mechanism | #719 |
| `48TW` | 2 | 108 | no prefix match | — |

### The route back: rebenchmark at 2022, then interpolate

✅ **The form is settled** (§The interpolation form): **geometric interpolation
between the two observations, and hold the later one flat afterwards.** Fitted
by holding out the observed 2012 on a 2007→2017 span — geometric 0.0710 against
linear 0.0764 and frozen 0.0889.

⚠️ **Two guardrails, both measured:**
- ❌ **Do not extrapolate past the last observation.** Holding 2022 flat for
  2023-24 beats a trend by 27.4% on manufacturing and 41.7% on the whole table.
- ❌ **Do not adopt the newer census early.** Using the 2022 mix for every year
  scores 0.1216 — worse than freezing at 2007. It is the one candidate that
  loses to doing nothing.

⚠️ **But the 2022 source is not the same source for every held sector**, and
this is where the plan has to stop being generic. The Economic Census expense
breakout `ecnbasic` carries expenses for sectors **21, 23 and 31-33 only**, in
both 2017 and 2022 — which is exactly §S3's scope and none of the held block.

| held sector | its 2022 observation | status |
|---|---|---|
| `42` / `44RT` | AWTS / ARTS **Business Expenses Supplement**, 2017 + 2022 | ⚠️ **verdict under review** — rejected on the wrong answer key; 2012 vintage exists, holdout retest pending |
| `11` | ERS FIWS — **annual, not quinquennial**, and runs to 2025 | ✅ no interpolation needed at all |
| `22` | EIA 861/861M, form 176 — **annual** | ✅ no interpolation needed; #719 |
| `G` | — | ❌ `govslocalfin` rejected; nothing else identified |

❌ **So "rebenchmark at 2022 and interpolate" has no candidate left.** Trade was
the only held sector the shape fitted, and it has now been retested and rejected
— ⚠️ **and specifically the suppression recovery this plan pointed at should not
be built**, because the column with no suppression fails too. Agriculture and
utilities have *annual* sources and would be seeded directly rather than
interpolated; agriculture has since been rejected as well, leaving utilities
(#719) as the only live route into the held block.

---

## What to build, in order

**S0a. Give the benchmark detail SUT panel an extractor** ([#700](https://github.com/cornerstone-data/bedrock/issues/700)) — ✅ **built.**
`Use_SUT_Detail.xlsx` and `Supply_Detail.xlsx` carry **2007, 2012 and 2017 on one
code basis in one frame**, and they are now read through
[`io_2017.py`](../../extract/iot/io_2017.py)`._load_benchmark_detail_supply_use_usa(matrix, year)`,
GCS-backed like every other table, with `load_benchmark_detail_U_intermediate_usa`
and `load_benchmark_detail_supply_usa` as the typed 402 × 402 accessors. They are
a second and third observation of *every* structural question in the build —
Step 3's input mix, Step 4a's commodity mix, the margin rates, the FD splits —
not just this one.

✅ **The panel's 2017 sheets are the single-year workbooks, cell for cell.**
Both matrices agree exactly with `Use_SUT_Framework_2017_DET.xlsx` and
`Supply_2017_DET.xlsx` — 0 differing cells on 413 × 424 and 405 × 415 — so the
panel really is the published 2017 table plus two earlier observations of it, not
a different vintage of it. `assert_benchmark_panel_matches_2017()` is the check,
and it is a callable rather than a test because it needs both files.

⚠️ **The two loaders are kept separate anyway.**
`_load_2017_detail_supply_use_usa` is what `bea_parse` emits as the
`BEA_Detail_Use_SUT` / `BEA_Detail_Supply` FBAs, and those stay pinned to the
workbook BEA published them from. Extending the FBAs themselves to 2007 and 2012
is a separate question — the year is hardcoded on the detail branches of
`bea_parse` and the yaml declares `years: [2017]`.

⚠️ **BEA's subsidy sign convention holds on all three years**, not just 2017:
Use `T00SUB` positive, Supply `SUB` negative, agreeing to $1M in 2007
(109,169 vs −109,168) and exactly in 2012. `_assert_bea_subsidy_signs` runs on
every year the panel loader returns.

⚠️ **The 2007 span still cannot carry a θ.** `derive_industry_price_index`
starts at 2012, so of the three benchmark spans only 2012 → 2017 is fitted. The
panel gives a second *detail* observation of the mix but not yet a second detail
θ, which is what §Inflation's dispersion candidate needs.

**S0b. Declare the section before the candidate exists** ([#704](https://github.com/cornerstone-data/bedrock/issues/704)) — ✅ **declared, and it
did not stay candidate-less.** S1 landed in the same change, so
`use_intermediate_detail_sut` in [`sections.py`](sections.py) went in **runnable**:
402 × 402, reference `_use_sut_detail()` interior, candidate
`derive_initial_U_intermediate`, `Tolerance(rtol=0.01, atol=$0.5M, ramp=0.25)`.

**S1. The seed, per #497, plus the column control** — ✅ **built.**
[`nowcast_intermediate.py`](../../transform/iot/nowcast_intermediate.py) carries
the three moves and
[`nowcast.py`](../../transform/eeio/nowcast.py)`.derive_initial_U_intermediate`
is the entry point. Seed from `Use_SUT_Framework_2017_DET` (native SUT, native
purchaser, native before-redefinitions — no conversion round-trip), carry the
column shares on the commodity price ratio at **θ as an argument**, defaulting to
#497's 1.0, then scale each column to `GO_producer − VAPRO_seed`. Runs for
**2017-2024**; the binding constraint is `BEA_Detail_GrossOutput_IO_<year>`, not
the price index.

Reproduce with `intermediate_structure_drift --seed --control`.

✅ **The section passes at 2017 on every cell**: 44,281 populated cells, 366 row
totals and 400 column totals all inside tolerance, 100% coverage and 100%
accuracy. ⚠️ **That is the plumbing, not the movement** — at 2017 the carry
factor is 1.0 everywhere and the only move left is the column rescale.

⚠️ **The 2017 rescale is not the identity, and the residual is BEA's own
rounding.** Published `T005` is one rounded number; the interior sums 402
separately rounded cells to a different one — $350M on $14.9T, at most $13M on
any single column. A *small* column wears that as a large fraction: `334610` is
$482M of intermediates and carries $6M of the gap, so the largest **relative**
cell error is **1.05%** while the largest **absolute** one is **$6.0M**, on a
$19.2B cell. `atol` is what carries the small cells here, not `rtol`.

✅ **The seven published negatives survive** in all eight years, and the two
structurally empty columns — `4200ID` customs duties and `814000` private
households — stay empty. ⚠️ A control that puts real dollars on an empty column
raises rather than dropping them, and so does a seed column whose nonzero cells
cancel: an *empty* column has no structure to normalise, a *cancelling* one has
structure that cannot be written as shares of its own total, and collapsing both
to all-zero would lose that distinction.

| year | intermediate $B | vs frozen 2017 |
|---|---:|---:|
| 2017 | 14,856.0 | +0.00% |
| 2018 | 15,848.0 | +6.68% |
| 2019 | 16,118.0 | +8.50% |
| 2020 | 15,339.3 | +3.26% |
| 2021 | 18,107.6 | +21.89% |
| 2022 | 20,556.6 | +38.38% |
| 2023 | 20,728.8 | +39.53% |
| 2024 | 21,438.5 | **+44.31%** |

which is §The column control's "~30% short without it" restated from the other
side: a frozen 2017 level is 1/1.443 = **30.7% short** at 2024.

⚠️ **The level is θ's one blind spot.** Every column is renormalised before the
control is applied, so the block total is set entirely by `GO − VAPRO` — the
table above is identical to 12 decimal places at θ = 1 and at the fitted θ.
Read it as a check on the control, never as evidence about the carry.

### ✅ The column control is observed on both sides

⚠️ **This section described a `vapro_seed` that no longer exists.** The numbers
it carried — 2.3% economy-wide, 2.5→8.0% weighted MAE, `GSLG` 18.3% low at 2022
— are the **superseded** frozen-ratio seed and should not be quoted as current.

`T005 = GO_producer − VAPRO`, and **both sides are now observed annually**, from
`derived_intermediate_and_value_added`, which allocates BEA's
`UGO205-A`/`UVA205-A`/`UII205-A` underlying-industry tables down to the 402
detail industries. Aggregated to summary and scored against the published
summary `T005` (`--control`):

| year | level % | weighted MAE % | worst | worst % |
|---|---:|---:|---|---:|
| 2018 | 0.00001 | 0.00013 | `327` | −0.003 |
| 2019 | 0.00004 | 0.00013 | `22` | −0.001 |
| 2020 | −0.00004 | 0.00010 | `326` | −0.001 |
| 2021 | −0.00007 | 0.00012 | `111CA` | −0.001 |
| 2022 | −0.00001 | 0.00006 | `42` | −0.000 |
| 2023 | 0.00000 | 0.00023 | `333` | +0.001 |
| 2024 | −0.00000 | 0.00016 | `333` | −0.002 |

⚠️ **Read that as a consistency check, not a validation.** `UII205-A` and the
summary Use SUT's `T005` are the same BEA estimate published two ways, so
agreeing to a thousandth of a percent says the allocation is arithmetically
faithful — not that BEA is right.

✅ **The `GSLG` level problem is fixed.** It was the column *level* under a
frozen VA ratio, and the level is now read rather than seeded.

⚠️ **What is fixed is the government column's *level*, and #578 is untouched.**
They were always two problems, and the superseded seed made them easy to run
together:

| | what is wrong | status |
|---|---|---|
| the column *level*, `GO − VAPRO` | a frozen 2017 VA share could not see government's moving one | ✅ **fixed** — both sides observed |
| [#578](https://github.com/cornerstone-data/bedrock/issues/578), the commodity *mix* inside the column | `govslocalfin`'s function × object split, if that bridge exists | ❌ **still open — and `govslocalfin` is no longer the answer.** The go/no-go ran and the bridge buys +2.4%; §The government function bridge |

`govslocalfin` is the source for the second and does nothing for the first.
`GFGD` 0.192, `GFGN` 0.179 and `GSLG` 0.127 are still among the worst-drifting
columns in the table — that is #578's problem, and no amount of column control
touches it, because the control rescales a column without moving one share
inside it.

⚠️ **NIPA `T31005` is no longer worth wiring for the level.**
[`plan.md`](plan.md) §Step 3 records it matching the published `T005` at
**$0 / $0 / $2M** in 2017, and this page previously called it the exact annual
source sitting unused for the worst cell of the control table. It supplies a
column *total*, which is now had for free — the same reason §The column control
gives for why a total-only candidate adds nothing here.

⚠️ **And Step 5 rescues less of this than §The column control implies.** T1 is
hard and real for 2017-2024, but it constrains `T005 + VAPRO = GO` — the
column's **sum**, not the split. The split is pinned only by **T4** (`V00100` by
industry group) and **T6** (the tax rows, economy-wide), both **soft** and both
still `PLACEHOLDER`, with `va_row_targets` reading their values off the 2017
panel after `del year`. And **T5 — `T00OTOP` and `V00300` — is deliberately not
imposed**: they "enter the balance as seed only, which is the price of the test
being worth running", because the income side is the hold-back that keeps GDP
out-of-sample. `V00300` is **$7.873T**. So the balance cannot re-derive `VABAS`.

⚠️ **What is still missing from Step 2 is the row split, not the level.** There
is now a `VAPRO` for every year 1997-2024, so "the balance cannot run on 2024 at
all" is no longer true; what Step 2 still owes is the split of that total across
the five value-added rows, and T4/T6 remain soft placeholders reading 2017
values off `published_2017_panel` after `del year`.

**What that does and does not cost.** The estimand here is the column *shape*,
and the shape is untouched: it is seeded, carried and renormalised before the
control is applied, so a wrong control rescales a column without moving a single
share inside it.

**None of which changes what to build here.** Step 5 re-imposes T1 hard and
re-solves the `T00TOP`/`T00SUB` wedge, so a wrong wedge is overwritten rather
than propagated —
which is exactly the argument §The column control makes for allowing a 2017
ratio at Step 3 in the first place, and it is why `T31005` "as a *source* adds
nothing" there. It is recorded because two cheap alternatives are already
measured or identified: the published summary `T005`, observed annually and
spreadable within a summary parent on gross-output share, and `T31005` for the
government columns specifically. Whoever needs the column right per industry
before Step 5 runs should pass `column_control` explicitly rather than
re-deriving it.

**S2. Fit θ, and add the margin-rate factor** ([#699](https://github.com/cornerstone-data/bedrock/issues/699)) — ✅ **built, and the
number is not the one this page expected.** §Inflation and §Margins.2.

⚠️ **Every span on this page starts at 2017**, so elapsed years, cumulative
inflation, price dispersion and accumulated structural drift all move together
with the calendar and none can be told from the others. That is why §Inflation
could *name* "θ is a function of relative-price dispersion" but not test it. The
summary Use SUT publishes 1997-2024 and the price index reaches 2012, so **78
non-nested spans** — different bases, different lengths, different inflation —
are free. `--regime`:

| predictor of the fitted θ | R² |
|---|---:|
| **crosses the 2021-22 surge** | **0.613** |
| cumulative price level | 0.525 |
| elapsed years | 0.142 |
| relative-price **dispersion** | **0.014** |

❌ **Dispersion is dead**: 1.4% of the variance, coefficient the wrong sign.
❌ **So is elapsed time**: 14%, and adding it to the regime binary moves that
binary's coefficient to 0.002 and its R² not at all. ✅ **The regime reading
stands** — holding span length fixed, spans that cross the surge fit θ 0.0-0.5
and spans that do not fit 0.7-0.9, at *every* length from one to nine years.

⚠️ **And the decision matters much less than the diagnosis.** The median gain of
the *best* θ over a frozen `A` is **5.44%** of the score off the surge and
**0.59%** across it — and every year this build targets from 2022 on crosses it.
**In the target regime the carry is worth well under one percent however θ is
set.** What #497's θ = 1 cost was not a missed 0.6%, it was the 12.6% it gave
away by pointing the wrong way.

✅ **What ships** (`nowcast_intermediate.default_theta`): **θ = 0.75 off the
surge, θ = 0.0 across it** — fitted 0.755 and 0.141 on the 78 spans, and rounded
rather than carried to three digits. Rounding *up* to zero rather than to the
target spans' own −0.25/−0.50 is deliberate: a negative θ says nominal shares
move against their own price, which is a curve-fit rather than a mechanism, and
it buys 0.6%. ✅ The one detail span agrees in direction — 2012→2017 does not
cross the surge and fits 1.00 against the rule's 0.75, on a panel the rule was
not fitted on.

✅ **The margin-rate leg is built too** and the deflator is now the purchaser
one, price × margin rate. See §Margins.2 for the spreading rule and the test
S0a made possible. ⚠️ **It is inert in exactly the years this build targets**:
at θ = 0 every factor is raised to the zero power, so the leg moves 0.21% of the
2019 block and 0.54% of the 2021 block and **0.000% of the 2024 one**. It is
kept because it is the correct deflator for a purchaser-valued cell and because
θ is not 0 forever, not because it changes the current answer.

**S3. The 2022 Economic Census materials breakout** — ✅ **extracted, recovered,
placed on commodities and reconciled across vintages.** §The materials census.
What is left of [#698](https://github.com/cornerstone-data/bedrock/issues/698) is the seed itself, and it now has a shape:

- **the placement is done** — `place_on_commodities()` returns industry × BEA
  detail commodity for both vintages, 200 commodities and $2,681B in 2022;
- **the industry axis is done** — one 365-unit basis carrying 100% of both
  vintages;
- ✅ **the interpolation form is answered, and it is not the price path**
  (§The interpolation form — answered). The price-carried candidate this plan
  named fits **θ = 0.00** on the census span: the level moves with price, the
  mix does not. The form is chosen on the benchmark panel instead —
  **geometric interpolation** between the censuses (+20.1% over frozen, +7.1%
  over linear), and the 2022 mix **held flat** past the last observation, since
  extending the trend costs 27.4%;
- ✅ **the seed is built** — `materials_seed(year)`, an index on the share with
  the column total held, moving the block 0.0979 off frozen 2017 by 2022.

⚠️ **The estimation span ends at 2023, which is the last year observed.**
Extending it to 2024 is [#707](https://github.com/cornerstone-data/bedrock/issues/707) and is Phase 2 work tied to producing a
2024 table; **2025 is out of scope**. So the form is fitted and scored on
observed years only, and nothing here extrapolates past the data.

**S3b. The named non-materials cells** — ✅ **built.** It was expected to be the
cheap half of S3, and it was, but not for the reason given: the work turned out
to be a scope measurement rather than a mapping exercise.

⚠️ **The 11.7% and 91.0% quoted in the previous draft were both too generous,
and the correction is a definition rather than an error in the data.** The 11.7%
counted survey-side dollars, and its two largest entries are not purchases of a
commodity at all — resales (3.6% of the column) are goods bought and sold on
untransformed, which the Use table handles through trade margins, and contract
work (1.4%) is manufacturing services whose commodity is the *buyer's own*
industry rather than any fixed row. With the survey's own residual (5.1%) they
are reached as expense but cannot be placed. The honest split:

| | share of the $3,566.6B manufacturing column |
|---|---:|
| `Census_EC_MatFuel` materials | 79.4% |
| **named non-materials cells, and seedable** | **6.4%** |
| reached as expense, but not one commodity | 10.0% |
| neither | 4.2% |

So **85.8% is reachable**, not 91.0%, and the seedable non-materials block is
$228.6B across ten cells: electricity, repair, temporary staff, professional and
technical services, advertising, refuse, data processing, communication,
expensed software and expensed computers.

#### The 2017 anchor, and why the seed is an index

`Census_EC_Expenses` is new and is what makes the block seedable at all. The
Economic Census publishes these cells for 2017 and 2022 under **the same
variable names ASM uses**, so census and survey form one panel with no
crosswalk: 2017 census, ASM 2018-2021, 2022 census, AIES 2023. ✅ The splice is
continuous — 2017 electricity is $47.5B against ASM's $51.0B in 2018, repair
$52.7B against $55.2B — on 360 six-digit industries in both.

That 2017 observation matters because it is the year the benchmark Use table is
built on, and **the levels cannot be used**. Both sides are 2017 and both are
manufacturing's purchases, so matching definitions would give a ratio near one.
They do not:

| kind | census 2017 $B | BEA 2017 $B | survey/BEA |
|---|---:|---:|---:|
| `PCHPRTE` professional and technical | 33.4 | 83.1 | **0.40** |
| `PCHDAPR` data processing | 6.1 | 12.6 | 0.49 |
| `PCHCSVC` communication | 5.3 | 9.8 | 0.55 |
| `PCHADVT` advertising | 16.4 | 20.5 | 0.80 |
| `CSTELEC` electricity | 47.5 | 58.7 | 0.81 |
| `PCHTEMP` temporary staff | 38.0 | 21.6 | 1.76 |
| `PCHRFUS` refuse | 14.3 | 6.6 | 2.17 |
| `PCHRPR` repair | 52.7 | 13.8 | **3.84** |
| `PCHCMPQ` expensed computers | 5.7 | 1.4 | 3.97 |
| `PCHEXSO` expensed software | 4.7 | 0.6 | **8.01** |

The disagreements are structural. **Expensed software and computer hardware** are
operating expense to Census and mostly *investment* to BEA, so the intermediate
row is a fraction of the survey cell. **Repair** is one Census question against
four BEA rows, carrying parts that BEA books elsewhere. **Professional and
technical services** runs the other way — one Census question against BEA's
legal, accounting, engineering, consulting and R&D rows together.

✅ **So the seed moves BEA's cell rather than replacing it**, which cancels every
one of these because a constant scope factor divides out:

    seed[c, i] = Use2017[c, i] × survey[i, k, t] / survey[i, k, 2017]

BEA's own level and its own split across the commodities of a multi-row kind are
both preserved, and the survey supplies only movement. 2017 reproduces the
benchmark exactly, which is the check that the form is right.

| year | seeded $B | vs frozen 2017 |
|---|---:|---:|
| 2017 | 228.6 | 0.00% |
| 2018 | 237.3 | +3.79% |
| 2019 | 239.2 | +4.65% |
| 2020 | 224.7 | **−1.72%** |
| 2021 | 242.5 | +6.08% |
| 2022 | 263.3 | +15.18% |
| 2023 | 283.0 | **+23.80%** |

The block carries the same pandemic signature the materials bill does, and a
frozen 2017 understates it by 23.8% at the end of the observed span.

⚠️ **AIES publishes no telephony and no expensed software.** Both variables exist
in the 2023 table and both are zero in every one of its 883 rows, which is an
absence rather than an economy that stopped buying them. They are held at the
2022 census and the year is marked `held`, rather than seeded as a collapse to
zero.

✅ **The observed span runs 2017-2023 and the seed covers all of it**, so S3b
needs no extrapolation. `nonmaterial_seed()` raises for any later year rather
than inventing one; extending it is [#707](https://github.com/cornerstone-data/bedrock/issues/707).

**S4. `--where`-driven sourcing for the top drifters** ([#705](https://github.com/cornerstone-data/bedrock/issues/705)) — ✅ **researched, and the
answer is one marginal go and four no-goes.** §Sourcing the columns that
actually drift.
The columns were never unsourced: BEA named a source for every one of them at the
benchmark, and the later vintage of that source exists in every case. What
separates the two observations is a rebenchmark, a questionnaire change or a
suppression pattern — in every case but one.

✅ **`ORE` is built** — [`Census_SAS_Expenses`](../../extract/census/Census_SAS_Expenses.yaml)
splices SAS Table 5 across the two vintages that carry it, and
[`services_transport_expense_seed.py`](services_transport_expense_seed.py) indexes BEA's 2017 `531ORE`
column on it. ⚠️ **The gain is +4.5% at 2022 and +3.8-4.4% at the other two
endpoints — at the inflation carry's bar rather than over it**, because `ORE`'s
movement sits mostly on rows no survey item names: `55` management of companies
moves +6.60pp and has no counterpart question at all. Positive at every endpoint
on a test biased against it, so it is real; it is not large, and it does not
compete with S1 or S2 for what to do next.

⚠️ **Nothing else on the drifter list survives**, and `4A0` is the sharpest case:
its Business Expenses Supplement pair exists, on one benchmark and one item list,
and **not one of its thirteen items is published for all nine of its constituent
NAICS in both years**. Reopening `42` and `4A0` means building a suppression
recovery on the `ecnmatfuel` pattern first, not writing an extractor.

**S5. ERS agriculture (#577)** — ✅ **built, measured, and it earns its place.**

⚠️ **This section recorded a rejection on 2026-08-25 and the rejection was
wrong.** It was an artefact of the answer key, not a fact about ERS. See
§The answer key, and why every earlier score used the wrong one.

✅ **On the sound key** ([`benchmark_holdout.py`](benchmark_holdout.py)) — seed
the **observed 2012** benchmark detail block with ERS's 2012 → 2017 movement,
score against the **observed 2017** block, at detail:

| weighting | frozen 2012 | seeded | gain | columns won |
|---|---:|---:|---:|---:|
| dollar | 0.2269 | 0.2074 | **+8.6%** | 8 of 10 |
| **impact (`N`)** | 0.3286 | 0.2698 | **+17.9%** | **9 of 10** |

⚠️ **And the prize is much bigger than this plan says.** Farm columns are among
the **worst-drifting in the whole table** at detail on `N` — `1111A0` **0.474**,
`1121A0` **0.424**, `1111B0` 0.311, `112120` 0.308, against an economy-wide
0.0606. ❌ **"Agriculture is not in this list at all" was a dollar judgement
taken at summary and is wrong twice over.**

⚠️ `111400` greenhouse and nursery is the one column the seed worsens (−49.5%).

✅ **The extractor is the other yield**, below.

---

⚠️ **The source work, which stands regardless.**
[`USDA_ERS_FIWS`](../../extract/usda/USDA_ERS_FIWS.py) now emits intermediate
product expenses. ✅ It was already in bedrock and its parse was **filtering to
cash receipts alone**, which dropped the concept #577 is built on — the same
class of silent whitelist as `Census_AIES`'s `AIES_EXPENSE_FLOWS`. Three
separate silent filters were found and fixed; see the module.

✅ **This is the source BEA itself used.** Table C2: *"Inputs to the agriculture,
forestry, fishing, and hunting industries were estimated from BEA NIPA estimates
based on USDA ERS statistics for farm income, EIA data, and 2017 Economic Census
data."*

✅ **It reconciles.** 2017 intermediate product expenses, excl. operator
dwellings, is **$226,611M**, and farm origin $104,467M + manufactured inputs
$56,316M + other intermediate $65,828M sums to it exactly. Named categories are
**89.6–91.7%** of the total, as #577 claimed.

✅ **And it agrees with BEA year by year** — FIWS's own growth against BEA's
published `111CA` intermediate total runs **0.988 to 1.047** across 2018-2024,
the closest agreement of any source examined in this step (utilities' SAS panel
runs 0.63–0.89 on the same test).

⚠️ **But #577's stated rationale does not survive.** It says *"The value is in
the levels. Do not build this expecting a shifting mix."* The **level is already
observed** — Step 3 controls every column to `GO − VAPRO` — and FIWS agrees with
BEA on the level anyway, so the level is worth nothing here. **The value has to
be the mix, which is the thing #577 says not to expect.**

The mix is small and real:

| | dissimilarity |
|---|---:|
| FIWS's own category mix, 2017→2023 | 0.045 |
| BEA's `111CA` commodity mix at 2022 | 0.077 dollar / **0.059 `N`** |
| the 13 agriculture columns' weight | 1.8% dollars / **6.9% `N`** |

✅ **Two things still argue for building it**: ERS FIWS is the **only source in
this step that reaches 2025**, and the category→commodity mapping is close to
one-for-one (pesticide $15.7B against BEA's $16.9B, petroleum $12.8B against
$11.7B) — gaps a relative index divides out.

⚠️ **One farm sector against ten BEA farm columns.** FIWS has no NAICS-6 split,
so the index moves all ten identically and is validated only at the `111CA`
aggregate — the same limitation the services and transportation seed carries.
⚠️ **2025 is an ERS forecast**, and **irrigation is discontinued after 2023** —
18 categories in 2024-25 against 19 before, so its absence is not a zero.

❌ **Not government finances.** The function→commodity bridge was the gate on
#578 and it did not survive scrutiny: §The government function bridge — tested,
and it is a no-go. ⚠️ **That does not close #578**, whose columns are still the
worst-drifting in the table; it closes `govslocalfin` as the route to them, and
the drift it leaves behind is *within*-function, which no reallocation between
functions can reach.

**S6. Utilities from EIA ([#719](https://github.com/cornerstone-data/bedrock/issues/719))** — ⚠️ **new, and it exists because the
services seed had to refuse this column.** `22`'s three detail columns are
**held at the benchmark** (`NOT_SEEDED`), not seeded, because what utilities'
column does over 2017-2022 is a **price-driven reweighting** — `211` goes 7.7%
→ 23.3% of it as the column rises $160.4B → $258.4B — while the survey's
purchased-fuels line moves the *other way*, $102.5B → $79.6B.

⚠️ **A relative share index cannot carry that by construction**: dividing out
common movement is the one thing it does. Seeding it anyway made `22` the
largest single drag on the block, **−119%** of the aggregate gain at 2020.
⚠️ **BEA did use SAS for utilities** (C2), so "different universe" is not
available as an explanation.

✅ **The instrument is EIA, per C1**: forms 861/861M for electric power, form
176 and the *Natural Gas Monthly* for gas. Physical fuel receipts × price is
what tracks a fuel-price reweighting. ⚠️ **Holding 2017 is not a fix** — θ is
0.0 across exactly this surge, so the column is close to frozen and its drift
is unaddressed.

Not in this step: `S00300` (#606, shared with Step 1), the margins redistribution
(Step 6b, #697), the government-enterprise reallocation (Step 7), and the balance
itself (Step 5, #588).
