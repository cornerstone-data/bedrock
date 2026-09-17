# What moves `B`, and how much of it is real

Findings from `B_change_diagnostics.py`, run 2026-09-16 against FBS vintage
`v0.3.0_796a6ca` and nowcast MUT vintage `v0.3.0_4276083`, nowcast models
2017-2024. Method and the diagnostic-to-approach mapping are in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md); this note holds the
numbers, and [`B_driver_investigation.md`](B_driver_investigation.md) tracks
which of them have been shown to be justified.

`B = (E / x) @ Vnorm`, so the three drivers the plan names — `E`, `x` and
`Vnorm` — are the three places a factor can move from. The sections below
measure how much each contributes, how to gate on a factor change in a way
that discriminates, which movements are worth smoothing at all, and five
mechanisms that produce movement with nothing underneath it.

⚠️ **Every figure here is industry emissions only.** `F01000` personal
consumption expenditures has no gross output, so it cannot enter `E / x`; the
production path drops it for the same reason.

---

## 1. Weighting the factor change by its share of `N`

The project's target is a steady `N`, so a move in a commodity's direct factor
matters in proportion to how much of `N` that factor drives. Weighting the
percentage change in `B` by its own-direct share of `N` cancels to something
simple:

```
delta_B_pct_of_N = pct_change_B * own_direct_share_of_N
                 = (dB / B) * (B * L[j,j] / N)
                 = dB * L[j,j] / N
```

`own_direct_share_of_N` is small for most commodities — **median 0.077**, so
for the typical commodity only 8% of its footprint is its own direct
emissions. Tenth percentile 0.019, ninetieth 0.499, maximum 0.988.

That is why an unweighted gate does not discriminate. Share of commodities a
5% gate admits, real-dollar factors, `B_change_real.csv`:

| year | commodities | on `abs_pct_change_B` | on `abs_delta_B_pct_of_N` | median `B` | median `N`-weighted |
|---|---:|---:|---:|---:|---:|
| 2018 | 402 | 48.8% | **5.5%** | 4.9% | 0.33% |
| 2019 | 402 | 48.3% | **5.2%** | 4.8% | 0.34% |
| 2020 | 402 | 68.7% | **10.9%** | 8.6% | 0.60% |
| 2021 | 400 | 85.5% | **22.5%** | 16.9% | 1.44% |
| 2022 | 401 | 71.6% | **10.0%** | 9.5% | 0.78% |
| 2023 | 401 | 67.6% | **10.2%** | 9.0% | 0.57% |
| 2024 | 400 | 64.5% | **5.8%** | 7.6% | 0.64% |

The unweighted gate admits 48-86% of commodities; the weighted one admits
5-23%, and 2021 is still visibly the worst year. The ranking also changes, not
just the count. For 2022:

| rank | on `abs_delta_B_pct_of_N` | own share | `pct_change_B` | `delta_B_pct_of_N` | `pct_change_N` |
|---|---|---:|---:|---:|---:|
| 1 | Fruit and tree nut farming | 0.67 | −52.6% | **−35.1%** | −48.0% |
| 2 | Iron, gold, silver, other metal ore mining | 0.77 | −44.0% | **−33.8%** | −33.1% |
| 3 | Vegetable and melon farming | 0.67 | +44.3% | **+29.5%** | +24.2% |
| 4 | Water transportation | 0.89 | −29.1% | **−25.8%** | −20.3% |

against the unweighted ranking, which leads with automotive equipment rental
(own share 0.13, `pct_change_B` +166%, `N` effect +21.4%) and puts internet
publishing fourth on a +105% factor move that shifts its `N` by 0.45%, because
its own direct emissions are 0.4% of its footprint.

Where the own share is high the weighted figure tracks the actual `N` move
closely, which is the check that it is measuring the right thing. It will not
always: motorcycle and bicycle manufacturing shows `delta_B_pct_of_N` of −21.2%
against `pct_change_N` of +81.7% in 2022, because its supply chain moved even
though its own factor fell. `pct_change_N` is carried in the table for exactly
that comparison.

### The 30 largest weighted factor movements

Every commodity-year in `B_change_real.csv`, ranked on `abs_delta_B_pct_of_N`,
highest first. `own share` is `own_direct_share_of_N`; the bolded column is the
gate metric; `pct_change_N` is what the commodity's total factor actually did.

⚠️ **`114000` and `327910` are excluded** pending
[#912](https://github.com/cornerstone-data/bedrock/issues/912). Their factors
go negative, so a percentage change has no interpretation for them; `114000`
otherwise took 6 of these 30 slots including first place, at a nominal +379%
that is entirely an artefact of crossing zero. Restore them to the ranking when
the defect is fixed, not before.

| year | commodity | name | own share | `pct_change_B` | `delta_B_pct_of_N` | `pct_change_N` |
|---|---|---|---:|---:|---:|---:|
| 2021 | `483000` | Water transportation | 0.79 | +111.5% | **+87.6%** | +87.3% |
| 2019 | `336991` | Motorcycle, bicycle, and parts manufacturing | 0.11 | +371.1% | **+41.3%** | +159.2% |
| 2020 | `611100` | Elementary and secondary schools | 0.53 | +71.9% | **+38.3%** | +30.2% |
| 2022 | `111300` | Fruit and tree nut farming | 0.67 | -52.6% | **-35.1%** | -47.9% |
| 2022 | `2122A0` | Iron, gold, silver, and other metal ore mining | 0.77 | -44.0% | **-33.8%** | -33.1% |
| 2022 | `111200` | Vegetable and melon farming | 0.67 | +44.3% | **+29.5%** | +24.2% |
| 2023 | `325310` | Fertilizer manufacturing | 0.89 | -32.0% | **-28.4%** | -30.8% |
| 2021 | `315000` | Apparel manufacturing | 0.32 | -87.7% | **-28.0%** | -13.0% |
| 2021 | `325110` | Petrochemical manufacturing | 0.64 | +41.6% | **+26.5%** | +25.2% |
| 2022 | `483000` | Water transportation | 0.89 | -29.1% | **-25.8%** | -20.3% |
| 2020 | `21311A` | Other support activities for mining | 0.47 | -46.5% | **-21.8%** | -32.9% |
| 2021 | `332114` | Custom roll forming | 0.15 | +148.0% | **+21.8%** | +38.7% |
| 2020 | `532100` | Automotive equipment rental and leasing | 0.29 | -74.3% | **-21.8%** | -37.7% |
| 2023 | `2123A0` | Other nonmetallic mineral mining and quarrying | 0.47 | +46.4% | **+21.8%** | +14.0% |
| 2022 | `532100` | Automotive equipment rental and leasing | 0.13 | +166.4% | **+21.4%** | +31.5% |
| 2022 | `336991` | Motorcycle, bicycle, and parts manufacturing | 0.23 | -92.4% | **-21.2%** | +81.7% |
| 2021 | `325180` | Other basic inorganic chemical manufacturing | 0.73 | -28.4% | **-20.7%** | -17.1% |
| 2021 | `325130` | Synthetic dye and pigment manufacturing | 0.41 | +50.2% | **+20.6%** | +25.2% |
| 2023 | `611100` | Elementary and secondary schools | 0.62 | -31.2% | **-19.3%** | -26.7% |
| 2020 | `481000` | Air transportation | 0.80 | +24.1% | **+19.3%** | +11.9% |
| 2021 | `325310` | Fertilizer manufacturing | 0.86 | +21.9% | **+18.9%** | +21.0% |
| 2021 | `325510` | Paint and coating manufacturing | 0.26 | +70.4% | **+18.1%** | +35.7% |
| 2021 | `312110` | Soft drink and ice manufacturing | 0.10 | +177.4% | **+18.1%** | +36.4% |
| 2021 | `331313` | Alumina refining and primary aluminum production | 0.68 | -26.5% | **-18.0%** | -13.3% |
| 2021 | `322110` | Pulp mills | 0.48 | +37.9% | **+18.0%** | +24.1% |
| 2022 | `327993` | Mineral wool manufacturing | 0.31 | +57.4% | **+17.8%** | +15.6% |
| 2021 | `332800` | Coating, engraving, heat treating and allied activities | 0.25 | +71.6% | **+17.6%** | +43.0% |
| 2021 | `233240` | Power and communication structures | 0.45 | -38.5% | **-17.3%** | +7.2% |
| 2021 | `2332D0` | Other nonresidential structures | 0.34 | -49.9% | **-16.9%** | +0.8% |
| 2023 | `212100` | Coal mining | 0.88 | +18.9% | **+16.7%** | +15.1% |

The list spans +87.6% down to 16.7% on the gate metric, and is led by
commodities that **are** their own footprint. Water transportation, fertilizer
manufacturing, fruit and nut farming, metal ore mining and petrochemical
manufacturing all sit at own-shares of 0.63 to 0.89 — sectors whose emissions
happen on their own site, so a move in their direct factor passes almost
undamped into what their buyers carry. Own-shares across the 30 run 0.10 to
0.89, so a low-share commodity can still make the list on a large enough move;
what it can no longer do is make it on a large move that barely touches `N`.
The unweighted ranking led with rental and leasing and internet publishing,
own-shares 0.13 and 0.004.

⚠️ **The span is not evenly represented, and the reason is not remediable.**
2021 supplies 14 of the 30 and 2022 seven; 2018 and 2024 supply none at all.
That is the COVID rebound showing up as a genuine one-year movement in real
output and in fuel use together, not a defect to smooth away. Prioritising on
this list alone would spend the project's effort on 2021.

### ⚠️ Negative emission factors in two commodities

Filed as [#912](https://github.com/cornerstone-data/bedrock/issues/912).

`114000` fishing, hunting and trapping and `327910` abrasive product
manufacturing carry a **negative** `B` in several years:

| | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `114000` E, Mt CO2e | 1.44 | 0.77 | 0.67 | **−0.18** | 0.68 | 0.34 | **−0.27** | **−0.42** |
| `114000` `B` real | 0.164 | 0.100 | 0.088 | **−0.014** | 0.093 | 0.056 | **−0.030** | **−0.040** |

`327910` is negative in 2020 through 2024. No other commodity is negative in
any year.

This is a defect, not a sink. The negative mass sits on fossil-combustion
inventory rows — `UMD_GHGIA_T_3_11.petroleum_industrial`,
`natural_gas_nonmanufacturing`, `ng_manufacturing`, and
`UMD_GHGIA_T_2_S1.carbonate_use` — and a negative quantity of burned petroleum
does not exist. For `114000` the petroleum row runs 1.178, 0.626, 0.536,
**−0.148**, 0.565, 0.276, **−0.217**, **−0.330** Mt.

Ruled out so far: gross output, which is positive for both commodities in every
year; and negative cells in the nowcast Use table for those columns, of which
there are none — `324110` petroleum refineries into `114000` is 256.3, 81.5 and
131.0 $M in 2017, 2020 and 2024. The nowcast Use table does carry 383 to 699
negative cells overall out of 170,910, but not in these columns, so that is
adjacent rather than causal.

The remaining candidate is a subtractive step inside the attribution — a
national total allocated net of an already-allocated portion, which goes
negative when the subtracted part exceeds the total. Not confirmed.

⚠️ Until this is resolved, `B_change_real.csv` rows for these two commodities
cannot be ranked or gated: a percentage off a negative base has no
interpretation, and `114000` otherwise takes 6 of the top 30 slots.

It also shrinks every count in the gate table above, because that table
filters on a positive prior-year factor. Of the 405 commodities, three carry
no direct emissions in any year and are always absent — `4200ID`, `814000`
and `S00402` — leaving 402. From the 2021 row the negative ones drop out too:
2021 loses both, 2022 and 2023 lose `327910`, 2024 loses both again. The
filter reads the **prior** year's factor, which is why negatives that start
in 2020 first show up as a smaller count in the 2021 row.

### Drift or oscillation: which movement is worth smoothing

![Drift against oscillation](images/sector_stratum_divergence.png)

`sector_stratum_divergence.png` and `sector_stratum_span.csv` place every
(sector, inventory table, attribution) cell by how much it moves against
whether the movement goes anywhere:

```
oscillation = 1 - |sum(divergence)| / sum(|divergence|)
```

0 means every year pointed the same way; 1 means the movements cancel and the
cell ends where it started. **That is the justified-versus-unjustified question
in measurable form**, and it does not fall out of magnitude alone.

The single largest cell on the span is electric power's own direct emissions
against `221100`, 609 Mt of gross movement, net −355 Mt. Direct intensity falls
from 3.82 to 3.06 kg CO2e per constant-2017 dollar, so the dominant direction
is real decarbonisation and smoothing the cell would erase it.

⚠️ **But it is not monotone, and its oscillation of 0.42 is the honest score.**
Two of the seven years reverse, for two different reasons:

| year | E growth | real `x` growth | divergence |
|---|---:|---:|---:|
| 2021 | **+7.0%** | +0.4% | **+96.3 Mt** |
| 2024 | +0.4% | **−1.8%** | **+30.9 Mt** |

In 2021 emissions rose 103 Mt on flat real output — intensity went *up* 6.6%,
from 3.19 to 3.40. In 2024 emissions were flat and real output contracted. Fuel
switching in the generation mix is the obvious candidate for 2021 and is not
measured here; what the diagnostic establishes is that the reversal is on the
emissions side, not the output side.

⚠️ 2021 is also the sharpest illustration of why the real series is the one to
read. Nominal electricity output rose **19.8%** that year, from $473bn to
$567bn, while real output rose 0.4%. On nominal `x` this cell would post a
large *negative* divergence in the very year its emissions rose.

Of the 63 cells carrying more than 10 Mt of gross movement, **33 oscillate
above 0.75 and carry 1,158 Mt between them**, against 7 trending cells below
0.25 carrying 270 Mt. The largest rocky cells:

| cell | gross | net | oscillation |
|---|---:|---:|---:|
| `211000` oil and gas \| UMD 3-11 natural_gas_nonmanufacturing → Use | 121.8 Mt | +3.9 | **0.97** |
| `211000` oil and gas \| UMD 3-11 petroleum_industrial → MECS | 106.6 Mt | +9.2 | **0.91** |
| `481000` air transport \| UMD 3-8 direct_jet → Direct | 99.5 Mt | −24.7 | 0.75 |
| `324110` refineries \| UMD 3-11 petroleum_industrial → MECS | 79.4 Mt | −4.6 | **0.94** |
| `1111B0` grain farming \| UMD 5-10 direct → EPA_GHGI_soils | 71.8 Mt | −10.6 | 0.85 |
| `1111B0` grain farming \| UMD 3-11 natural_gas_nonmanufacturing → Use | 60.2 Mt | −9.0 | 0.85 |

Oil and gas extraction appears twice at the top, moving 228 Mt between the two
cells and arriving 13 Mt from where it started. Both are industrial-fuel rows
attributed through the Use table or the MECS energy FBS, which is the same pair
of attribution routes behind most of this list.

⚠️ `oscillation` is unreliable where the gross is small — two rounding
movements that happen to cancel score 1.0 — so read it next to `total`. The
figures above are all filtered to cells above 10 Mt.

⚠️ **Net within the cell-year before taking absolute values.** The detail frame
is keyed on the raw `AttributionSources`, and a vintage-suffixed vector splits
one cell-year across two rows: the MECS energy FBS carries a year in its name,
so the 2018 pair has an `..._2017` row holding the `E_from` side and an
`..._2018` row holding the `E_to` side, each with a large divergence that
cancels against its sibling. Summing `|divergence|` over raw rows counts both
halves of that cancellation as movement — it put petroleum refineries' gas
combustion at 740 Mt of gross movement against an actual 21 Mt, a factor of 35,
and it put that cell top of the ranking. Same family of trap as the EPA table
renumbering.

Concentration, after that correction: the 1,991 cells carry 3,304 Mt of gross
movement between them, and the top 10 hold 40%, the top 25 hold 56%, the top
50 hold 69%. A top-N view is representative rather than a sample.

### `L` moves `N` more than the factors do, and is out of scope

`N = B @ L`, and `L` comes from each year's own `A` at that year's prices —
the nowcast configs set `apply_io_year_adjustments: False`, so nothing deflates
it. Holding `L` at the prior year isolates the part of the `N` move that the
emission factors explain; `pct_change_N_L_held` and `pct_change_N_L_effect`
carry the split. Medians over commodities:

| year | median \|Δ`N`\| | factors | `L` |
|---|---:|---:|---:|
| 2018 | 3.7% | 1.4% | 3.1% |
| 2019 | 9.1% | 3.0% | 6.2% |
| 2020 | 11.8% | 3.4% | 9.3% |
| 2021 | 18.2% | 3.8% | **14.9%** |
| 2022 | 5.7% | 3.9% | 5.0% |
| 2023 | 16.1% | 5.4% | **10.5%** |
| 2024 | 6.2% | 1.9% | 3.9% |

`L` accounts for two to four times what the factors do. Some of that is real
structural change and some is relative prices; the two are not separated here
because `A` is not deflated on this path.

⚠️ **`L` is out of scope for the smoothing project, by construction.**
`B = (E/x) @ Vnorm` has exactly three inputs and `L` is not one of them — it
enters only through `N`. `L` comes from `A = U_norm @ V_norm`, the nowcast's
own IO product, so no emissions-side change can move it. Diagnose it here,
remediate it in Nowcast Phase 2.

The same boundary applies to `Vnorm` for *remediation* even though it is a
direct term in `B`: its 26.6% share of `B` movement in 2023 is the nowcast Make
moving, and no GHG inventory or attribution change will touch it. This project
owns the `E` and `x` sides of the fix, and hands the Make and the `A` matrix
over.

⚠️ **What this means for the objective.** The stated intent is to reduce the
`N` change to justifiable changes. With `L` out of scope, this project can
deliver the factor-driven column above — 1.4% to 5.4% a year — and not the `L`
column. A flat median factor change should not later be read as "`N` is now
smooth".

⚠️ **A percentage ranking still does not answer "reduces cumulatively the
most".** This weighting fixes *within* a commodity — how much of its own `N`
its direct factor drives — not *across* commodities. A change in `B[j]` also
moves `N` for everything that buys from `j`, and a commodity nobody buys ranks
the same as electricity. The cross-commodity version is `dB[j]` propagated
through row `j` of `L`, output-weighted.

⚠️ Read any of these next to `B_from`. A commodity with a near-zero factor
posts a large percentage off a rounding-scale numerator. Percentages are NaN
rather than fabricated where the base is zero.

⚠️ `L` comes from each year's own `A`, at that year's prices, while a real `B`
is in constant first-year dollars. The ratios are unaffected because numerator
and denominator share the `B` basis, but `N` as a level is mixed-basis and
should not be compared across years as a level.

## 2. `Vnorm`: a tenth of the movement by mass, a quarter unweighted

Each year's `Vnorm` is built from that year's nowcast Make, so a commodity's
factor moves when the industry-to-commodity mix moves, with `E / x` flat.
Holding one side at a time and differencing gives the Make's share of gross `B`
movement — but the answer depends on whether commodities are weighted by how
much of them anyone buys. D13, `vnorm_share_of_B_movement.csv`:

| year | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|
| unweighted, every commodity equal | 8.2% | 7.7% | 10.0% | 5.4% | 5.8% | **35.7%** | **23.4%** |
| weighted by commodity output `q` | 3.9% | 5.6% | 6.7% | 3.4% | 2.7% | **12.0%** | **13.3%** |

⚠️ **These figures supersede an earlier set that could not be reproduced.** The
numbers previously printed here — 26.5% unweighted in 2023 against 35.7% now —
came from an ad-hoc calculation, and none of the three defensible index forms
reproduces them on the same span and the same vintages. The module's version is
now the source. The story is unchanged: a step up in 2023-24, and the weighted
figure roughly half the unweighted one.

⚠️ **The direction of the index matters more than it looks.** Moving `Vnorm`
against the current year's intensity gives 4.8% in 2021 and against the prior
year's gives 6.1%, a 27% spread on the same quantity. Neither year has a claim
to being the base, so the headline above is their average and both one-sided
readings are carried in the CSV.

⚠️ **Quote the weighted row.** The unweighted figure counts a kg/$ swing on a
commodity nobody buys the same as one on electricity, and roughly doubles the
Make's apparent importance. On the weighting that matters the Make is 3% to 6%
of gross `B` movement through 2022 and then 10.7% and 13.0% — still a clear
step up in 2023, still the one driver this project cannot remediate, but half
the size the unweighted number suggests.

In absolute terms the Make has moved **25 Mt CO2e onto a different commodity**
than the 2017 Make would have put it on, by 2024 — about 0.5% of total
emissions. It rises steadily to 25.9 Mt in 2022 and then flattens, 24.2 Mt in
2023 and 25.0 Mt in 2024, so the mix drift is a 2017-2022 story that has since
stalled rather than a continuing trend. That is the green line on
`E_vs_x_indexed.png`.

⚠️ That flattening sits oddly beside the Make's *share* of gross `B` movement,
which steps up in exactly those two years. Both are measured and neither is
wrong: the Make has stopped drifting further from the 2017 mix while the
year-on-year reshuffling within it got larger. Cumulative displacement and
annual churn are different quantities.

⚠️ **`Vnorm` has no level line, and `q` is not a substitute for one.** The
output-weighted total of `B` is total emissions by construction — measured,
5,023 Mt against 5,017 Mt in 2022, the 0.13% gap being the scrap correction —
and freezing `Vnorm` at 2017 moves that total by 0.1%. The Make redistributes
emissions across commodities; it does not create or destroy them, so an indexed
`Vnorm` line would sit flat at 100. `q` would not help either: `q` and `x` are
the same money counted along two axes and total to the dollar. The
redistribution is the only thing there is to plot, which is what
:func:`make_reallocation` measures.

For 234 commodity-years — 8.3% of the panel — `Vnorm` moved the factor more
than `E / x` did, `vnorm_dominant_commodities` in D13, between 21 and 49 a year.
Those cannot be remediated on either the emissions or the output side. (This
figure also supersedes an unreproducible earlier count of 133.) What changed in the 2023 Make is an open question and is its own
lead.

![E, x and the Make](images/E_vs_x_indexed.png)

⚠️ **The five coloured E lines are one quantity, not five.** They are total
emissions partitioned by *what vector spread them across sectors*, so they sum
to the red total line. None of them is an output series — `x` is the black and
grey pair, in dollars.

| line | what it is | share of E |
|---|---|---:|
| E: total GHG inventory | the sum of the five below | 100% |
| inventory names the sector directly | `Direct` — no attribution vector used | 54% |
| spread by a Use table row | each sector's purchases of a fuel commodity | 30% |
| spread by the MECS energy survey | `Energy_manufacturing_national_nowcast` | 9.5% |
| spread by another GHG inventory table | EPA soils and non-energy use tables | 6.4% |
| spread by gross output | `BEA_Detail_GrossOutput_IO` | **0.018%** |

⚠️ The last of those is 1.2 Mt. Drawn at equal weight it was the most dramatic
line on the chart, swinging on a fifth of a megatonne; it is now thin and
carries its share in the label. It is emissions attributed *using* gross output
as the weight — not gross output, which is `x`.

## 3. Prices are 73% of the 2021 and 2022 gap

The nowcast configs set `apply_io_year_adjustments: False`, so `x` is nominal:
the row sum of that year's Make in that year's dollars. Nominal gross output
runs $34.5T to $50.7T over the span against $40.9T in constant 2017 dollars.

`price_effect.csv` splits the E-versus-x gap into a price part and a real part;
the two close exactly on `total_gap_nominal == total_gap_real + price`:

| year | price share of the gap |
|---|---:|
| 2018 | 105% |
| 2019 | 21% |
| 2020 | 14% |
| 2021 | **74%** |
| 2022 | **73%** |
| 2023 | 19% |
| 2024 | 40% |

On the factor itself the same effect shows as a median absolute EF change of
13.4% nominal against 9.3% real in 2022. 2024 runs the other way: 3.8% nominal,
7.6% real.

⚠️ Any approach-1 ranking built on nominal `x` charges sources for inflation.
Use the `*_real` tables.

## 4. Five mechanisms that move `B` with nothing underneath

Separating justified from unjustified change is what the objective turns on.
Five mechanisms found so far:

1. **Vintage relabelling.** EPA renumbered its soils attribution tables
   mid-span — `T_5_17` to `T_5_18` for direct soils, `T_5_18` to `T_5_19` for
   indirect — and non-energy use moved `T_3_25b` to `T_3_25` for 2023, so
   `T_5_18` means *direct* in some years and *indirect* in others. Keyed on the
   raw table number, 2019 booked −297 Mt against +255 Mt while the emissions
   ran flat at ~290 Mt; 2023 did the same at ~90 Mt. Definitionally
   unjustified. Handled in the module via `ATTRIBUTION_ROLE_ALIAS`, which
   merges onto the role rather than the table number because the number alone
   does not identify the role. `divergence_by_attribution_raw.csv` keeps the
   renumbering visible.
2. **Prices in a nominal denominator.** Section 3.
3. **Attribution-vector churn.** A source attributed on a row of the Use table
   moves when the Use table moves, whether or not anything was emitted. Just
   under a third of `E` has its sector split derived from the IO tables,
   29.7% to 31.7% and steady across the span. `output_elasticity.csv`
   measures which sources actually tracked output; on real `x`, `Direct`
   comes out at 0.54 and
   `Nowcast_Detail_Use_AfterRedef` at 1.51, so neither behaves the way its
   label alone would suggest.
4. **`Vnorm` churn.** Section 2.
5. **Survey vintage steps.** A four-yearly survey applied with a hard cutoff
   moves the whole allocation on the cutoff year and nowhere else. Measured
   below for MECS.

### The MECS vintage cutoff lands between 2020 and 2021

Tracked as [#918](https://github.com/cornerstone-data/bedrock/issues/918);
tracker rows 1, 2 and 15.

EPA table 3-11, fossil fuel combustion, is **the largest driver family in the
panel**: 1,009 Mt of gross movement over 567 cells, 30.5% of all gross movement
in the panel, netting only −45 Mt. It reaches sectors by two different routes,
and the split is roughly 70/30:

| route | who it covers | gross | net | cells |
|---|---|---:|---:|---:|
| `Energy_manufacturing_national_nowcast` | manufacturing, via MECS | 706 Mt | −13 Mt | 525 |
| `Nowcast_Detail_Use_AfterRedef` | non-manufacturing, via the Use table | 303 Mt | −32 Mt | 42 |

`mecs_year` in `bedrock/transform/energy/Energy_manufacturing_national_nowcast_*.yaml`
holds the survey vintage behind each nowcast year, and it steps once:

| nowcast year | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|---|---|---|
| MECS survey | 2018 | 2018 | 2018 | 2018 | **2022** | 2022 | 2022 | 2022 |

Within a vintage the sector split moves only because the Use side moved. Across
the cutoff both move at once. Summing the absolute year-on-year change in every
sector's share of the table 3-11 total gives how much of the split was
reshuffled each year, in percentage points — and the non-MECS route is the
control, because it carries the same inventory table through the same years
with no survey vintage in it at all:

| year | MECS route (steps at 2021) | Use route (no MECS — control) |
|---|---:|---:|
| 2018 | 10.3 | 24.9 |
| 2019 | 4.5 | 11.7 |
| 2020 | 19.9 | 41.7 |
| **2021** | **27.6** | **39.6** |
| 2022 | 13.6 | 32.8 |
| 2023 | 11.4 | 26.7 |
| 2024 | 7.4 | 13.7 |
| mean excluding 2021 | 11.2 | 25.2 |
| **2021 as a multiple of that** | **2.47x** | 1.57x |

2021 is the largest reallocation year on both routes, so some of it is the COVID
rebound — but the control puts that at 1.57x, and the MECS route runs 2.47x.
Scaling the MECS baseline by the control's own amplification leaves roughly
**10 percentage points of 2021 reallocation that the cutoff explains and the
rebound does not.**

⚠️ **This bounds the effect; it does not isolate it.** The control is a
different sector population — non-manufacturing is intrinsically rockier, 25pp
a year against 11pp — so it is the wrong population to subtract, only the right
one to compare a *ratio* against. The clean measurement is the counterfactual
#918 already asks for: rerun the FBS with one MECS vintage held across the
cutoff and difference the two. Until that is run, the emissions effect of the
cutoff is unmeasured and rows 1, 2 and 15 stay `unresolved`.

### D15: what a facility-reported basis would carry, and what it would move

Tracked as [#923](https://github.com/cornerstone-data/bedrock/issues/923).

Every sector assignment in table 3-11 today is *derived* - a national total
spread by a purchase row or a survey share. GHGRP and NEI together report
stationary combustion facility by facility, so most of it need not be.
Best evidence first, deduplicated on `FRS_ID`: GHGRP subpart C, then NEI
combustion SCCs for the facilities below GHGRP's 25,000 tCO2e threshold.

**2022: 676.9 Mt over 17,266 facilities**, against an allocated table 3-11 of
808.8 Mt:

| `jurisdiction` | Mt | facilities | |
|---|---:|---:|---|
| `state` — 50 states and DC | 672.0 | 16,639 | |
| **`offshore` — `DM`** | **4.9** | **627** | Gulf of Mexico federal waters, **all of it oil and gas extraction** — 6.9% of that sector's facility basis |
| `territory` — `PR`, `VI` | 0.6 | 30 | **dropped** |

⚠️ **The boundary that governs is BEA's, not the GHG inventory's.** These
emissions become `B = E / x`, and `x` is BEA gross output. BEA's economic
territory for the NIPAs and the industry accounts runs past the 50 states to
everywhere the US holds exclusive economic rights — the Exclusive Economic Zone
and the Outer Continental Shelf — so **offshore oil and gas extraction is
already in the denominator.** Dropping it from the numerator would put the two
on different geographies and inflate the factor.

That cuts both ways, and `in_model_geography` applies it in both directions:

- **Offshore stays.** "Limit it to the 50 states and DC" reads like the obvious
  tidy-up and is wrong — it deletes 627 platforms in the one sector this
  investigation turns on.
- **Territories go.** Puerto Rico and the Virgin Islands are outside BEA's NIPA
  economic territory, so they have no `x` here, and emissions with no
  denominator would inflate whatever sector they landed in. 0.61 Mt over 30
  facilities, 0.09% of the union. Same boundary #913 raises for eGRID. Sector comes from the facility's own NAICS - a
combustion SCC names the equipment and fuel, "industrial boiler, natural gas",
and the same code appears in every industry, so no SCC-to-NAICS crosswalk can
place it.

| basis | sectors | our Mt | % of table 3-11 |
|---|---:|---:|---:|
| facility | 53 | 390.5 | 48.3% |
| facility + Use residual | 89 | 95.9 | 11.9% |
| facility, boundary to settle | 19 | 61.8 | 7.6% |
| **Use row — no facility data** | 91 | **260.6** | **32.2%** |

**67.8% of table 3-11 could be facility-backed.** The 32.2% that cannot is
agriculture (~95 Mt) and construction (~58 Mt), plus cement and coal mining -
dispersed, largely non-point sources, and precisely where MECS never applied
either, since MECS covers NAICS 31-33 only.

#### ⚠️ Fuel the facility made itself needs its own category

Combustion of *purchased* fuel can be spread by a row of the Use table, because
a purchase is what the Use table records. Combustion of **byproduct fuel** -
refinery still gas, coke oven gas, blast furnace gas - never appears as a
purchase anywhere. Those emissions are real and must be counted, but no Use row
can carry them, and attributing them with one is a category error rather than an
inaccuracy. NEI's SCC level 3 `007` identifies it: **37.0 Mt in 2022, 5.5% of
the union**, concentrated in petroleum refineries (22.4 Mt), petrochemicals and
primary metals.

⚠️ That 37.0 Mt depends on an imputation. `stewi` exposes GHGRP at subpart
granularity and subpart C does not name the fuel, so the process-gas share of a
matched NEI record is carried onto the GHGRP total. Without that step every
refinery lands in GHGRP unclassified and the figure collapses to 12.5 Mt. Read
it as a floor.

⚠️ **Self-supplied fuel is wider than byproduct gas.** An oil and gas producer
burning its own field gas is burning natural gas, and the SCC says natural gas,
so lease fuel is invisible to this split while having the same problem - which
is the level bias behind [#922](https://github.com/cornerstone-data/bedrock/issues/922).

#### Where the basis disagrees with the current split

Sectors with no facility data keep their current allocation, and the anchored
group keeps its current total share - only its internal distribution is restated
on facility evidence. Total absolute movement **31.9 percentage points**:

| sector | allocated Mt | facility Mt | now | hybrid | shift | facilities |
|---|---:|---:|---:|---:|---:|---:|
| `324110` petroleum refineries | 83.2 | 124.3 | 10.29% | 15.55% | **+5.25pp** | 147 |
| **`211000` oil and gas extraction** | 109.4 | 71.2 | 13.52% | 8.91% | **−4.61pp** | 1,461 |
| `325110` petrochemicals | 22.6 | 41.1 | 2.80% | 5.13% | +2.34pp | 52 |
| `331110` iron and steel | 21.5 | 38.1 | 2.66% | 4.76% | +2.11pp | 131 |
| `311221` wet corn milling | 7.4 | 20.4 | 0.91% | 2.55% | +1.63pp | 33 |
| `221200` natural gas distribution | 0.15 | 11.9 | 0.02% | 1.49% | +1.47pp | 60 |

⚠️ **Oil and gas extraction is the one large sector the facility basis marks
*down*, by 4.6 percentage points.** That is an independent corroboration of
#922, reached from facility records rather than from the coefficient, and it
points the same way: the Use-table basis over-allocates this sector.

⚠️ `221200` natural gas distribution carries 11.9 Mt of reported combustion
against 0.15 Mt allocated - its compressors are essentially unallocated today.

### D14: a physical floor under the fuel-combustion allocation

GHGRP subpart C is stationary fuel combustion reported facility by facility, for
facilities over the 25,000 tCO2e threshold. It is therefore a **lower bound** on
what a sector burned — whatever we allocate to a sector's table 3-11 should be
at least this much. It needs no answer key, no deflator and no benchmark year,
and it is published annually, which makes it the only external check in this
note that works on every year of the span.

Of the 26 sectors carrying a floor above 1 Mt in all seven years:

| | sectors | GHGRP mass 2022 | reading |
|---|---:|---:|---|
| clears the floor every year | 12 | 110 Mt | nothing to answer |
| **below it every year** | 7 | 181 Mt | a **constant** offset — a boundary definition difference, not rockiness |
| **intermittent** | 7 | 106 Mt | clears in some years, breaches in others — **only volatility can do this** |

⚠️ **The persistent group is not a defect list.** Petroleum refineries `324110`,
iron and steel `331110` and wet corn milling `311221` sit below the floor in all
seven years because GHGRP subpart C counts combustion of process-derived fuels —
refinery still gas, coke oven and blast furnace gas — that the GHG inventory
books outside table 3-11. A constant offset cannot make a factor rocky. Reading
those seven as errors would be the wrong conclusion from the right test.

The intermittent group is where the test bites, and one sector dominates it:

| sector | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | spread |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **`211000` oil and gas extraction** | **0.66** | 1.36 | 1.14 | **0.18** | 1.35 | 2.34 | 1.38 | **13.0x** |
| `322110` pulp mills | 0.77 | 0.86 | 0.88 | 0.91 | 1.14 | 1.78 | 1.49 | 2.3x |
| `2123A0` other nonmetallic mining | 1.09 | 1.11 | 1.12 | 0.95 | 0.72 | 0.76 | 1.02 | 1.6x |
| `324190` other petroleum and coal products | 0.95 | 1.04 | 1.07 | 0.75 | 1.01 | 1.08 | 1.22 | 1.6x |
| `325110` petrochemicals | 0.92 | 0.86 | 0.87 | 0.80 | 1.04 | 1.02 | 0.99 | 1.3x |

Oil and gas extraction is **5.7x more volatile against the floor than the next
worst sector**. In 2020 we allocate it 8.6 Mt where its own facilities reported
46.4 Mt — a factor of 5.4 below a measured lower bound, in a year its physical
combustion did not move at all.

⚠️ **2017 is below the floor too, at 0.66, and 2017 is matched to the published
benchmark.** So the level problem is not the nowcast's construction. The likely
cause is structural: oil and gas extraction burns a large share of its own
produced gas as lease and plant fuel, and self-supplied fuel is never a
*purchase*, so it cannot appear in the Use row this allocation is built from.
That separates into two defects — a level bias from purchase-based weights, and
the volatility measured above — which is how
[#922](https://github.com/cornerstone-data/bedrock/issues/922) now reads.

### Oil and gas extraction's petroleum coefficient, against a physical yardstick

Tracked as [#922](https://github.com/cornerstone-data/bedrock/issues/922);
tracker rows 1 and 16. This is the mechanism under the panel's second-largest
oscillating cell.

Once `petroleum_industrial` is known to be a Use-table vector, the question
becomes what moves the Use row. Purchases of refined petroleum per $100 of the
buying sector's own gross output isolates it: both sides are nominal, and every
buyer in the vector purchases the *same* commodity, so a uniform petroleum price
change scales every entry equally and **cannot move the shares at all**.

| buyer | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2017→2022 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **`211` oil and gas extraction** | **0.85** | 1.45 | 1.65 | 1.60 | 2.06 | **2.64** | 1.87 | 1.69 | **3.11x** |
| `230301` nonresidential construction | 2.03 | 2.24 | 1.93 | 1.61 | 2.22 | 2.62 | 1.88 | 1.82 | 1.29x |
| `2334A0` nonresidential construction | 1.39 | 1.55 | 1.33 | 1.11 | 1.52 | 1.75 | 1.31 | 1.28 | 1.27x |
| `32411` petroleum refineries | 1.26 | 1.43 | 1.77 | 1.35 | 1.03 | 1.10 | 0.93 | 0.64 | 0.87x |

Across all 182 buyers the median 2017→2022 move is **0.91x** and the 90th
percentile 1.53x. Oil and gas extraction is at **3.11x, the 98th percentile**;
its share of the whole allocation vector runs 3.60% → 17.17% → 11.41%. The
construction rows move together in a narrow band and fall back together in 2023,
which is what a common real effect looks like. `211` does not.

⚠️ The largest single jump is **2017→2018, +71%** — the first year off the
published benchmark, which the nowcast matches by construction.

**The external check.** GHGRP subpart C is facility-reported stationary fuel
combustion, so it moves only when fuel burned moves:

| | 2022 | 2023 | change |
|---|---:|---:|---:|
| GHGRP subpart C, NAICS `211` facilities | 46.7 Mt | 48.4 Mt | **+3.6%** |
| reporting facilities | 542 | 532 | −1.8% |
| our table 3-11 for `211000` | 109.3 Mt | 66.6 Mt | **−39.1%** |

Both years use the 2022 MECS survey, so the fuel share is constant and the whole
move is the Use table.

⚠️ **This bounds the verdict rather than proving it.** Subpart C covers 43-73%
of what we assign to this sector — the rest is below GHGRP's 25,000 tCO2e
threshold — so it is a partial yardstick. What it establishes is that the
*covered* portion did not move while our allocation share fell by a third.

⚠️ `32551` paint and coating manufacturing moves **17.69x** on the same measure
over the same span. It is smaller in mass but larger in ratio, and it is already
visible in the bottom-up ranking as a +70.4% factor move in 2021, so the two are
probably the same defect seen twice.

### The MECS split is fuel-only, and `petroleum_industrial` is not MECS

Two questions about the table 3-11 allocation, checked end to end in the method
and against the built FBS.

**Does the MECS allocation mix feedstock energy into combustion?** No. MECS
numbers its tables the opposite way round to intuition, and
`bedrock/extract/eia/EIA_MECS_Energy.yaml` records which is which in
`data_type`: **Table 2.1 and 2.2 are `nonfuel consumption`**, the feedstock
tables, and **Table 3.1 and 3.2 are `fuel consumption`**. Every combustion row
draws from the fuel side, and the feedstock vectors go to the non-energy-use
rows, where they belong:

| table 3-11 row | flowable and class | built from | feedstock |
|---|---|---|---|
| `ng_manufacturing` | `Natural Gas` / `Energy` | MECS Table 3.1 | excluded |
| `coal_manufacturing` | `Coal` / `Energy` | MECS Table 3.1 | excluded |
| `petroleum_industrial` | `Petroleum` / `Money` | BEA Use × MECS fuel share | scaled out |

Where money weights stand in for MECS quantities,
`multiply_bea_by_mecs_petroleum_energy_fraction` removes the feedstock share
explicitly, per sector, as `Table 3.1 / (Table 2.1 + Table 3.1)`. The
corresponding `Other`-class flows — `Natural Gas`, `Petroleum`, `HGL` — feed
`natural_gas_neu`, `petroleum_neu` and `petroleum_neu_hgl`. Two guards on that
separation are filed as
[#920](https://github.com/cornerstone-data/bedrock/issues/920) and
[#921](https://github.com/cornerstone-data/bedrock/issues/921); neither is a
live defect.

**What is `petroleum_industrial` actually allocated by?** Not MECS. Its
`MetaSources` is `Nowcast_Detail_Use_AfterRedef.petrol` — BEA Use purchases of
refined petroleum `324110` — and it carries the
`Energy_manufacturing_national_nowcast` label only because it is built inside
that FBS. The MECS fuel share is a scalar reweighting on top, and it is
**constant within a survey vintage**, so for 2018-19, 2019-20, 2022-23 and
2023-24 every year-on-year move in this vector is Use-table movement.

⚠️ **That re-reads the whole family.** Of the 706 Mt of gross movement the
route labels assign to MECS, 412 Mt is `petroleum_industrial`. On provenance
rather than label:

| driver | gross movement | share of table 3-11 |
|---|---:|---:|
| BEA Use table (`natural_gas_nonmanufacturing`, `coal_nonmanufacturing`, **and `petroleum_industrial`**) | ~716 Mt | **71%** |
| MECS survey quantities (`ng_manufacturing`, `coal_manufacturing`) | ~293 Mt | 29% |

So the Use table carries roughly seven tenths of the largest driver family in
the panel, against the 348 Mt that the route labels alone suggest. This is the
mass behind section 4's third mechanism, and it is why rows 1 and 2 of the
tracker move together: they are two Use-table cells, not one Use cell and one
MECS cell.

⚠️ **It also bounds what #918 can buy.** Smoothing the MECS vintage cutoff
changes a scalar multiplier on `petroleum_industrial`, not its shape, so it
cannot move the two largest rocky cells. #918 owns the ~293 Mt that MECS
quantities actually allocate.

⚠️ **A smoothed cutoff is not the only candidate fix.**
[#919](https://github.com/cornerstone-data/bedrock/issues/919) would replace the
source rather than interpolate it — GHGRP facility data aggregated to NAICS is
annual, so the four-yearly step disappears instead of being smoothed over. The
two issues are alternatives on the same rows, and #919 also reaches the 303 Mt
Use-table route that a MECS fix cannot touch.

The two sectors that lead this family are ones GHGRP reports on directly —
petroleum refineries `324110`, and petroleum and natural gas systems behind oil
and gas extraction `211000` — so coverage is **expected** to be strong exactly
where the movement is. That expectation is untested. #919 conditions the switch
on "full industry coverage available" and the coverage check is the open
question in it, not a formality: a partial-coverage GHGRP source substituted for
a complete MECS one trades a four-yearly step for a permanent level error.
**Check coverage for `211000` and `324110` first** — they are the two cells that
would justify the work on their own, and the cheapest place to find out whether
#919 is viable at all.

## 5. Where the emissions side sits

For the top-down ranking, the divergence decomposition attributes the
E-versus-x gap to the FBS rows that produced it, and is additive: every
`MetaSources` × `AttributionSources` term sums to the economy-wide gap, with a
residual of 1e-14 percent. `divergence_by_source_real.png` stacks it per year.

Of `E` itself, just over two thirds is attributed by something other than
the IO tables, and the largest single source-and-attribution pairs by
absolute divergence are
`UMD 2-S1 electric_power → Direct`, `UMD 5-10 direct → EPA_GHGI_soils` and
`UMD 3-11 ng_manufacturing → MECS`.
