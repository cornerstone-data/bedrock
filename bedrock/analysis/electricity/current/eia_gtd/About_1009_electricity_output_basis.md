# Electricity gross output: what BEA sourced, and why the intra-industry cell is small

Issue #1009. Parent #990, grandparent #896.

The question #1009 opened with was whether BEA's electricity gross output is
defensible, and if it is, where the 2021-24 swing should live. Measurement since
then has answered the two sourcing questions outright and settled the treatment
ranking. This file records what was found, which treatment was chosen, and why
the other two lost, so the reasoning is not spread across issue comments.

## 1. What BEA sourced, cited

Both answers are in Tables C1 and C2 of
`bedrock/analysis/nowcasting/bea_2017_benchmark_sources.md`, transcribed from the
SCB November 2023 comprehensive-update article.

### Gross output — C1, Utilities row

| column | entry |
|---|---|
| 2017 benchmark source | Census Bureau **2017 Economic Census** |
| **nonbenchmark-year source** | **EIA forms 861 and 861M** |
| price index | BLS CPI and BLS PPI |

⚠️ **BEA's 2018-2024 gross output for electric power comes from EIA Form 861 —
the same form behind EIA's published retail revenue.** #1009 asked whether the
divergence is "a vintage or method difference rather than two independent
measurements". It is the former. BEA and EIA are not two independent readings of
the industry; they are two derivations from one survey.

✅ That makes the divergence more tractable and more serious at once. There is no
"which source do you believe" question to argue about. One of the two derivations
is taking something from Form 861 that the other is not — Form 861 reports
bundled, delivery-only and wholesale revenue separately from sales to ultimate
customers, and 861M is a monthly frame that gets revised — and the difference has
to be findable in that space rather than attributed to survey disagreement.

⚠️ It also weakens the framing in PR #1010. That PR is written as if EIA were an
independent check on BEA. It is not; it is the same source read differently. The
*measurements* in #1010 stand — the implied-price deviation and its improvement
are arithmetic — but "BEA says one thing, EIA says another" is the wrong sentence.

### Intermediate inputs — C2, "Services, transportation and warehousing, and utilities"

> For selected Census-covered industries, information from the **2017 SAS** on
> operating expenses was used. Detailed expense data used to estimate
> intermediate inputs included materials, parts, and supplies **(not for
> resale)**; purchased electricity; purchased fuels (except motor fuels); rental
> payments…; repairs and maintenance…; advertising; printing; data processing;
> communication; water, sewer, refuse removal, and other utilities; professional
> and technical services; and all other operating expenses.

⚠️ **There is no line for power purchased for resale**, and the one electricity
line is a general operating expense — our own map has
`SAS_ITEM_TO_BEA['Purchased electricity'] = ('221100',)`
(`bedrock/analysis/nowcasting/services_transport_expense_seed.py`).

The magnitude corroborates the reading. The `221100 × 221100` cell is
**$9.38bn in 2017 = 2.41% of the industry's gross output**, which is what station
service and office consumption looks like. It is not a $50-70bn wholesale power
market booked small; it is a different quantity entirely.

## 2. The column cannot hold a purchased-power constraint

This is what kills treatment 1, independently of the sourcing argument.

BEA's published 2017 figures for `221100`, which our build reproduces exactly:

| 2017 | $bn |
|---|---:|
| gross output | 389.4 |
| value added (`VAPRO`) | 258.5 — **66.4% of output** |
| **intermediate purchases (`T005`)** | **131.0** |
| of which `221100 × 221100` | 9.38 |

`T1` pins `T005 + VAPRO = GO` and `T18` pins `VAPRO`, and `221100` is alone in
underlying line 13, so the value-added allocation rescale is the identity and
that $258.5bn is BEA's own published number. `T005` is therefore *forced* at
$131.0bn by BEA's own output and value-added pair.

Against that, EIA Table 8.3 (FERC Form 1, major investor-owned utilities, 2017).
The hierarchy reconciles exactly, which is worth recording as a check on the
extract:

```
revenue: Electric Utility                263.26
expenses: Electric Utility               226.11  = Operation 142.00 + Maintenance 18.00
                                                  + Depreciation 30.32 + Taxes and Other 35.79
  Operation                              142.00  = Production 98.86 + Transmission 10.80
                                                  + Distribution 4.36 + Customer Accounts 4.79
                                                  + Customer Service 5.96 + Sales 0.21
                                                  + Administrative and General 17.02
    Production                            98.86  = Cost of Fuel 32.16 + Purchased Power 49.03
                                                  + Other 17.66
```

Investor-owned utilities are **67.6%** of the industry by revenue
(263.26 / 389.4), so their share of a $131.0bn `T005` is about **$88.54bn**.

⚠️ **FERC's fuel plus purchased power for those same utilities is $81.19bn.**
That leaves **$7.35bn** for maintenance, administration, transmission,
distribution, customer service and every purchased service — against
Administrative and General alone of **$17.02bn**. The constraint does not fit in
the column, and no scaling assumption is needed to see it.

## 3. A second finding the sizing turned up

⚠️ **Read §6 before acting on this section.** BEA consolidates generation and
distribution into one `221100`, which nets the internal sale. FERC's purchased
power is a transaction between entities BEA consolidates, so putting it in the
intermediate column below overstates FERC's intermediate and the direction of the
gap is not settled. The section is kept as measured; the interpretation is not.

Splitting the same $263.26bn of investor-owned revenue both ways:

| IOU share, 2017, $bn | BEA × 67.6% | FERC Form 1 | diff |
|---|---:|---:|---:|
| compensation | 40.97 | 40.97 | — |
| taxes on production | 33.93 | 35.79 | −1.86 |
| **gross operating surplus** | **99.82** | **67.47** | **+32.35** |
| **intermediate purchases** | **88.54** | **119.03** | **−30.49** |

Both columns total $263.26bn exactly, so this is one pie split two ways:
**+32.35 = −(−30.49) − 1.86.** BEA books 47.9% more gross operating surplus and a
matching shortfall in intermediate purchases.

⚠️ Electric-only operating income is used here, not Table 8.3's
`Net Utility Operating Income` of $46.46bn, which is all-utility. Electric-only
is `revenue: Electric Utility − expenses: Electric Utility` = 263.26 − 226.11 =
**$37.15bn**.

Two checks on whether that gap is an artefact:

- ✅ **Taxes reconcile to within 5%**, which says the 67.6% coverage ratio is not
  wildly wrong for at least one component.
- ✅ **Compensation is observed, not assumed.** BLS QCEW 2017 for NAICS 2211 gives
  **$43.95bn** of *private* annual payroll (plus $8.51bn government, which lines
  up with `S00101`/`S00202` sitting outside `221100`). BEA's $60.61bn implies a
  **37.9%** supplements ratio — high against the ~24% economy-wide figure but
  defensible for an industry carrying legacy defined-benefit pensions. **The gap
  is not a compensation error.**

⚠️ **The confound that remains is composition.** BEA's `221100` includes
independent power producers; FERC's "investor-owned utilities" does not. IPPs are
roughly a third of the industry's output and are structurally surplus-heavy —
merchant generators with heavy capital, almost no distribution or customer-service
overhead, and little purchased power. A flat 67.6% scaling assumes IPPs cost like
IOUs, which they do not. Using QCEW's sub-industry split (private payroll is
$19.06bn in generation `22111` and $24.89bn in transmission and distribution, and
IPPs do no T&D) puts IOU labour nearer $48bn of compensation, which moves FERC's
implied intermediate to ~$111bn and the gap to **~$22.7bn** rather than $30.49bn.

**The gap narrows by about a quarter and does not close.** It cannot be resolved
from Table 8.3 alone.

## 4. The three treatments

### ❌ Treatment 1 — constrain `221100 × 221100` against purchased power. **Rejected.**

Two independent reasons, either sufficient:

1. **It does not fit.** §2 — $81.19bn of investor-owned fuel and purchased power
   against an $88.54bn investor-owned share of `T005`.
2. **The source explains the small cell.** §1 — the SAS schedule BEA used has no
   line for power purchased for resale, and the cell's 2.41%-of-output magnitude
   is own consumption.

⚠️ Making the cell $72.5bn would require grossing gross output up by ~$63bn,
which changes every emission-factor denominator and moves opposite to the
treatment that was chosen. **It is not a cell fix; it is a gross-versus-net basis
change**, and nothing observed justifies making it.

### ❌ Treatment 2 — leave the cell and accept the row control as published. **Rejected.**

#1009 required "an argument for why residential consumption and trade should
absorb it". There is none, and the absorption is measured: the balance moved
**+$17.6bn** into `221100 × F01000` beyond what Step 3 seeded, roughly 26% of the
$67.6bn intermediate fall, and the Step-3 seed tracked EIA residential (+1.96%
against +2.20%) while the shipped product ran +10.06%. Unseeded trade columns take
more of it (#1005). Accepting the control means accepting a sink chosen by the
balancer rather than by evidence.

### ✅ Treatment 3 — rebase gross output on EIA volume and published price. **Chosen, PR #1010.**

#1009 ranked this lowest on three objections. Each was tested rather than argued
around:

| objection | what was done |
|---|---|
| Table 2.3 is retail-only, so using it as output would exclude wholesale and push the intermediate block down further | The 2017 base is **split**: output sold to ultimate customers moves on Table 2.3 revenue, output sold for resale moves on Table 8.3 purchased power. A single retail index was tested and **fails measurably** — it drives `T005` to $126.6bn in 2022, below the $129.5bn investor-owned utilities alone report spending on fuel and purchased power. |
| needs the government-enterprise carve-out handled explicitly | `S00101` and `S00202` are **held out**, with the measurement: apportioning on output share assumes equal resale intensity, which is false for federal power (largely Bonneville and TVA, selling mostly at wholesale). Applying it would move `S00101` −26% in 2021 on an assumption known to be wrong for it. |
| needs the same case made separately for `221200` | Made, and it **fails**. Gas distribution does not have this signature: the implied price per Mcf sold runs +3.4% → +34.4% as a monotone trend that never returns, driven by the share of deliveries the utility sells rather than transports falling 30.2% → 25.4%. The two-component split does not identify either — the 2017 residual is $2.3bn, about $0.13 per Mcf transported. |

Result: worst implied-price deviation falls from **+14.7% to +5.0%**, every year
off the base improves, and `T005` stays at 1.2-1.7x investor-owned
fuel-plus-purchased-power across the span.

⚠️ **The decomposition is an approximation, and §6 shows the exact one.** BEA
publishes the industry's component structure, and it is **generation versus
transmission and distribution**, not retail versus sales for resale. The measured
improvement stands, but the better route replaces the generation deflator using
BEA's own components rather than re-indexing the aggregate from outside.

⚠️ **Off by default** behind `rebase_utility_gross_output_on_eia`. Nothing shipped
moves until a config sets it.

## 5. What is still open

- ⚠️ **The surplus split (§3) is not an electricity-resale question** and is
  larger than the cell #1009 was chasing. If BEA's value added for the industry is
  too high, every fuel row in the `221100` column is depressed, not just the
  diagonal, and the route is `T18`/`VAPRO` rather than anything electricity
  specific. Filed separately.
- ⚠️ **§1 reframes PR #1010's narration.** BEA's nonbenchmark output already comes
  from EIA Form 861, so the divergence is a derivation difference inside one
  source. The arithmetic is unaffected; the explanatory sentence is not.
- **Which Form 861 series BEA takes** for nonbenchmark years — bundled,
  delivery-only, wholesale, or a combination — is the next question, and it is
  answerable from the form's own documentation rather than by inference.
- The balancer using residential PCE as a sink is #1008 / PR #1011.
- The trade electricity pin is #899 / #1005; the manufacturing shape is #898.

## Reproduce

```
detail_gross_output_panel(ec_adjusted=False).loc[['221100','221200','S00101','S00202']]
assemble_seeds(2017, fitted=True)['use']['221100']       # the column, T005 and VA
eia_table_8_3_line(2017, 'expenses: Purchased Power')    # and the other line items
BLS_QCEW_2017_*.parquet, ActivityProducedBy == '2211'    # payroll, national, by ownership
bedrock/analysis/nowcasting/bea_2017_benchmark_sources.md  # C1 line 14, C2 line 148
```

## 6. What BEA's detail electric power actually is

Found after §§1-5 were written, and it supersedes parts of them.

BEA's detail accounts do not carry `221100` as one industry. They carry **ten**:
eight generation technologies, bulk power transmission and control, and
distribution. `load_pi_detail()` and `load_go_detail()` in
`bedrock/extract/iot/gdp.py` expose both the price index and the nominal series
for each.

### The eight generation technologies share one price index

Price index, 2017 = 100:

| | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|
| Fossil fuel generation | 105.4 | 142.1 | **154.0** | 124.3 | **109.0** |
| Nuclear generation | 105.4 | 141.7 | 153.3 | 124.2 | 109.2 |
| Solar generation | 105.3 | 141.3 | **153.0** | 123.9 | **108.7** |
| Hydroelectric generation | 105.4 | 141.9 | 153.6 | 124.2 | 109.0 |
| Wind generation | 105.3 | 142.1 | 153.9 | 124.1 | 108.7 |
| Geothermal generation | 105.3 | 143.0 | 154.9 | 124.2 | 108.5 |
| Biomass generation | 105.3 | 142.8 | 154.6 | 124.2 | 108.5 |
| Other generation | 105.7 | 142.9 | 154.8 | 124.9 | 109.4 |
| **Bulk transmission** | 108.2 | 115.4 | 122.8 | 127.6 | **130.7** |
| **Distribution** | 101.2 | 109.9 | 126.2 | 125.5 | **127.7** |

⚠️ **Solar and hydroelectric output prices track fossil-fired generation to
within one index point.** Hydro has no fuel, solar has no fuel, and nuclear fuel
is contracted years ahead. **BEA is applying a single wholesale power price index
to all eight generation technologies.**

The real quantities agree from the other side. BEA has real solar output growing
**8.5%** from 2017 to 2024 and real wind **7.2%**, while actual US solar
generation roughly quadrupled and wind rose about 77%. Every one of the ten moves
on the same real index to within a point or two.

✅ **So the detail split is one aggregate divided on near-fixed 2017 shares**, not
ten independent estimates. Solar is 0.565% of electric power output in 2017 and
0.517% in 2024.

### The 2022-24 fall is entirely generation

Contribution to the −$53.7bn change in nominal output:

| | $bn | share |
|---|---:|---:|
| Fossil fuel generation | **−38.9** | 72.5% |
| Nuclear generation | **−15.0** | 27.9% |
| other generation | −8.2 | 15.3% |
| Bulk transmission | **+1.4** | — |
| Distribution | **+6.9** | — |

Transmission and distribution *rose* throughout. Distribution alone is **62%** of
the industry ($241.2bn of $389.4bn in 2017) and its price index runs 100 → 127.7,
close to EIA's published retail price path of +23.5%.

✅ **BEA and EIA are both right about different things.** BEA's generation half
carries the wholesale price collapse after the 2022 gas spike; EIA's retail
revenue carries regulated retail rates, which kept climbing. The row control takes
the consolidated aggregate, which mixes them.

### This explains the netting by consolidation

If BEA books $133.2bn of generation and $241.2bn of distribution as separate
detail industries, distribution buys its power from generation — a flow of order
$130bn. Consolidating the ten into one `221100` **nets that internal sale out**.
Which is why, all at once:

- output $389.4bn ≈ retail revenue $390.3bn — the consolidated industry sells to
  outsiders
- the diagonal is $9.38bn — only own consumption survives consolidation
- `T005` of $131.0bn excludes purchased power — it is internal
- FERC's $49.03bn of purchased power is a transaction *between* entities BEA
  consolidates

✅ §2's reading is confirmed and now has a mechanism rather than an inference.

### Consequences

1. ⚠️ **§3's surplus comparison has a broken premise.** It put FERC's purchased
   power in the intermediate column. Under consolidation that is internal and
   correctly absent from BEA's `T005`, which flips the sign of the intermediate
   gap. The measurements stand; the conclusion does not.
2. ⚠️ **§4's chosen treatment approximates the right fix.** The exact
   decomposition is generation versus transmission and distribution, published by
   BEA, and the defect is specifically the generation price index. Replacing that
   deflator is better founded than re-indexing the aggregate from outside.
3. ➡️ **Bigger than this issue for the GHG model**: the detail electricity
   industries carry **no technology mix movement at all**. Anything reading
   generation mix off the detail accounts is reading a frozen 2017 snapshot. That
   is #902's territory.

### Reproduce

```
from bedrock.extract.iot.gdp import load_pi_detail, load_go_detail
rows 21-30 of each; 'sector_name' carries the industry, year columns are strings
```
