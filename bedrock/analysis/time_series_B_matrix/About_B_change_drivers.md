# What moves `B`, and how much of it is real

Findings from `B_change_diagnostics.py`, run 2026-09-18 against FBS vintage
`v0.3.0_99655e9` and nowcast MUT vintage `v0.3.0_4276083`, nowcast models
2017-2024. The FBS is the one published build on GCS, and its hash is
reachable from `origin/main`.

⚠️ **Every figure here was restated on 2026-09-18.** The previous run resolved
to `v0.3.0_796a6ca`, a local-only build off a branch that no ref reaches, and
it moved up to 1.8% of total `E` between commodities — enough to reverse two
findings outright. What changed, and the resolver fix that stops it recurring,
are under "The negative emission factors were a build-vintage artefact" in
section 1.

Method and the diagnostic-to-approach mapping are in
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

`own_direct_share_of_N` is small for most commodities — **median 0.079**, so
for the typical commodity only 8% of its footprint is its own direct
emissions. Tenth percentile 0.020, ninetieth 0.511, maximum 0.987.

That is why an unweighted gate does not discriminate. Share of commodities a
5% gate admits, real-dollar factors, `B_change_real.csv`:

| year | commodities | on `abs_pct_change_B` | on `abs_delta_B_pct_of_N` | median `B` | median `N`-weighted |
|---|---:|---:|---:|---:|---:|
| 2018 | 402 | 46.5% | **4.2%** | 4.6% | 0.32% |
| 2019 | 402 | 49.3% | **4.2%** | 5.0% | 0.33% |
| 2020 | 402 | 65.7% | **10.9%** | 7.7% | 0.61% |
| 2021 | 402 | 81.8% | **18.4%** | 14.5% | 1.17% |
| 2022 | 402 | 64.2% | **8.0%** | 7.2% | 0.55% |
| 2023 | 402 | 64.9% | **9.5%** | 7.9% | 0.59% |
| 2024 | 402 | 59.2% | **7.0%** | 7.0% | 0.58% |

The unweighted gate admits 47-82% of commodities; the weighted one admits
4-18%, and 2021 is still visibly the worst year. The count is 402 in every
year: of the 405 commodities, three carry no direct emissions in any year and
are always absent — `4200ID`, `814000` and `S00402`.

The ranking also changes, not just the count. For 2022:

| rank | on `abs_delta_B_pct_of_N` | own share | `pct_change_B` | `delta_B_pct_of_N` | `pct_change_N` |
|---|---|---:|---:|---:|---:|
| 1 | Iron, gold, silver, other metal ore mining | 0.76 | −40.8% | **−31.2%** | −30.6% |
| 2 | Water transportation | 0.89 | −29.0% | **−25.8%** | −20.6% |
| 3 | Automotive equipment rental and leasing | 0.15 | +144.2% | **+22.3%** | +31.6% |
| 4 | Mineral wool manufacturing | 0.31 | +61.0% | **+18.7%** | +16.5% |

against the unweighted ranking, whose top four across the whole span are
motorcycle manufacturing, breweries, wineries and motorcycle manufacturing
again — own shares 0.072, 0.075, 0.048 and 0.080. Not one of the high-own-share
sectors that lead the weighted ranking appears in it. The sharpest case is
internet publishing, whose +85% factor move in 2022 shifts its `N` by 0.34%,
because its own direct emissions are 0.4% of its footprint.

Where the own share is high the weighted figure tracks the actual `N` move
closely, which is the check that it is measuring the right thing. It will not
always: motorcycle and bicycle manufacturing shows `delta_B_pct_of_N` of +15.0%
against `pct_change_N` of +136.8% in 2022, because its supply chain moved far
more than its own factor did. `pct_change_N` is carried in the table for
exactly that comparison.

### The 30 largest weighted factor movements

Every commodity-year in `B_change_real.csv`, ranked on `abs_delta_B_pct_of_N`,
highest first. `own share` is `own_direct_share_of_N`; the bolded column is the
gate metric; `pct_change_N` is what the commodity's total factor actually did.

| year | commodity | name | own share | `pct_change_B` | `delta_B_pct_of_N` | `pct_change_N` |
|---|---|---|---:|---:|---:|---:|
| 2021 | `483000` | Water transportation | 0.78 | +111.4% | **+87.0%** | +85.9% |
| 2021 | `114000` | Fishing, hunting and trapping | 0.63 | +103.0% | **+65.2%** | +108.6% |
| 2019 | `336991` | Motorcycle, bicycle, and parts manufacturing | 0.07 | +519.8% | **+37.7%** | +163.9% |
| 2021 | `111300` | Fruit and tree nut farming | 0.72 | -48.6% | **-35.2%** | -45.0% |
| 2022 | `2122A0` | Iron, gold, silver, and other metal ore mining | 0.76 | -40.8% | **-31.2%** | -30.6% |
| 2023 | `325310` | Fertilizer manufacturing | 0.89 | -32.0% | **-28.4%** | -30.8% |
| 2021 | `315000` | Apparel manufacturing | 0.32 | -87.7% | **-27.9%** | -13.4% |
| 2020 | `611100` | Elementary and secondary schools | 0.53 | +50.5% | **+26.7%** | +18.6% |
| 2023 | `111400` | Greenhouse, nursery, and floriculture production | 0.86 | +30.3% | **+26.0%** | +25.6% |
| 2022 | `483000` | Water transportation | 0.89 | -29.0% | **-25.8%** | -20.6% |
| 2021 | `111400` | Greenhouse, nursery, and floriculture production | 0.87 | -28.6% | **-25.0%** | -31.8% |
| 2021 | `325110` | Petrochemical manufacturing | 0.63 | +38.2% | **+24.1%** | +21.4% |
| 2021 | `111200` | Vegetable and melon farming | 0.75 | -31.4% | **-23.6%** | -27.0% |
| 2023 | `111300` | Fruit and tree nut farming | 0.70 | +32.4% | **+22.8%** | +28.8% |
| 2024 | `111300` | Fruit and tree nut farming | 0.72 | +31.4% | **+22.7%** | +25.9% |
| 2022 | `532100` | Automotive equipment rental and leasing | 0.15 | +144.2% | **+22.3%** | +31.6% |
| 2019 | `114000` | Fishing, hunting and trapping | 0.62 | -34.9% | **-21.6%** | -35.7% |
| 2023 | `611100` | Elementary and secondary schools | 0.57 | -36.6% | **-21.0%** | -29.1% |
| 2021 | `325180` | Other basic inorganic chemical manufacturing | 0.73 | -28.6% | **-20.8%** | -17.4% |
| 2021 | `332114` | Custom roll forming | 0.15 | +131.7% | **+20.4%** | +36.9% |
| 2020 | `532100` | Automotive equipment rental and leasing | 0.29 | -69.2% | **-19.7%** | -35.4% |
| 2021 | `325510` | Paint and coating manufacturing | 0.25 | +77.0% | **+19.6%** | +35.9% |
| 2020 | `481000` | Air transportation | 0.80 | +24.4% | **+19.5%** | +13.0% |
| 2021 | `325310` | Fertilizer manufacturing | 0.86 | +22.3% | **+19.1%** | +21.3% |
| 2023 | `114000` | Fishing, hunting and trapping | 0.56 | -33.6% | **-18.9%** | -40.5% |
| 2022 | `327993` | Mineral wool manufacturing | 0.31 | +61.0% | **+18.7%** | +16.5% |
| 2021 | `322110` | Pulp mills | 0.47 | +39.2% | **+18.6%** | +24.6% |
| 2021 | `312120` | Breweries | 0.07 | +248.5% | **+18.6%** | +36.9% |
| 2020 | `2122A0` | Iron, gold, silver, and other metal ore mining | 0.75 | -24.7% | **-18.5%** | -22.9% |
| 2022 | `111300` | Fruit and tree nut farming | 0.67 | -27.0% | **-18.1%** | -31.0% |

The list spans +87.0% down to 18.1% on the gate metric, and is led by
commodities that **are** their own footprint. Water transportation, fishing,
fertilizer manufacturing, fruit and nut farming, greenhouse production and
metal ore mining all sit at own-shares of 0.56 to 0.89 — sectors whose
emissions happen on their own site, so a move in their direct factor passes
almost undamped into what their buyers carry. Own-shares across the 30 run
0.07 to 0.89, so a low-share commodity can still make the list on a large
enough move; what it can no longer do is make it on a large move that barely
touches `N`. The unweighted ranking led with motorcycle manufacturing and
breweries, own shares 0.072 and 0.075.

Agriculture and fishing take 10 of the 30 slots between them, and `114000`
fishing, hunting and trapping takes 3 — on real movement rather than the
zero-crossing artefact the previous run reported; see below.

⚠️ **The span is not evenly represented, and the reason is not remediable.**
2021 supplies 13 of the 30, 2022 and 2023 five each, 2020 four, 2019 two and
2024 one; 2018 supplies none at all. That is the COVID rebound showing up as a
genuine one-year movement in real output and in fuel use together, not a defect
to smooth away. Prioritising on this list alone would spend the project's
effort on 2021.

### ⚠️ The negative emission factors were a build-vintage artefact

[#912](https://github.com/cornerstone-data/bedrock/issues/912) reported that
`114000` fishing, hunting and trapping and `327910` abrasive product
manufacturing carried a **negative** `B` in several years, on
fossil-combustion inventory rows where a negative has no physical meaning.
**It does not reproduce on the published FBS, and the defect was in which
build this module resolved to, not in the pipeline.**

`114000` E, Mt CO2e, on the two builds:

| FBS vintage | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `v0.3.0_99655e9` — published | 1.46 | 1.07 | 0.67 | 0.50 | 1.17 | 1.04 | 0.65 | 0.55 |
| `v0.3.0_796a6ca` — local only | 1.44 | 0.77 | 0.67 | **−0.18** | 0.68 | 0.34 | **−0.27** | **−0.42** |

On `99655e9` **no commodity carries a negative factor in any year of the
span**; on `796a6ca` five do — `114000`, `327910`, `33451A`, `339930` and
`5191A0`. The national total is identical between the two builds every year,
to the tonne, so the whole difference is a redistribution across sectors.

**What `796a6ca` is.** A commit dated 2026-09-04 on a branch that no longer
exists: `git branch --contains` finds no ref, so nobody can check out the code
that built it. Its tree does not even contain the per-year
`Energy_manufacturing_national_nowcast_<year>.yaml` methods that the parquet it
produced names in `AttributionSources`, so it was assembled from a mixed
working tree rather than from any commit. It was never uploaded; GCS holds one
nowcast FBS vintage, `v0.3.0_99655e9`, which is main's own build.

**Why it won.** `resolve_span_vintage` used to break ties on **file mtime**,
and the `796a6ca` parquets were written at 12:45 on 8 September, 33 minutes
after the published `99655e9` set. Newer file, older and unreachable code. The
resolver now ranks on `vintage_provenance` first — reachable from
`origin/main`, then some other ref, then dangling — and refuses to choose at
all when several span-covering vintages are on disk and none is on main.
`--list-vintages` prints the provenance of each so the check is one command.

⚠️ **The lesson generalises past this module.** Every artifact stem in
`transform/output_data` carries a git hash, and any of them can be shadowed the
same way by a rebuild done on a checked-out branch. An mtime is evidence about
when a file was written and none at all about what wrote it.

**What it cost.** Up to 1.8% of total `E` moved between commodities (2020;
0.9-1.2% in most other years), with 300-340 commodities shifting more than 1%.
Two findings reversed outright: `114000` is a real large mover rather than a
zero-crossing artefact and now ranks second in the whole panel, while `327910`
turns out to be unremarkable at rank 655 of 2,835. Two more moved materially —
oil and gas extraction's floor volatility in D14, and the facility-basis shift
in D15 — and are restated in their own sections below.

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
from 3.75 to 3.01 kg CO2e per constant-2017 dollar, so the dominant direction
is real decarbonisation and smoothing the cell would erase it.

⚠️ **But it is not monotone, and its oscillation of 0.42 is the honest score.**
Two of the seven years reverse, for two different reasons:

| year | E growth | real `x` growth | divergence |
|---|---:|---:|---:|
| 2021 | **+7.0%** | +0.4% | **+96.3 Mt** |
| 2024 | +0.4% | **−1.8%** | **+30.9 Mt** |

In 2021 emissions rose 101 Mt on flat real output — intensity went *up* 6.7%,
from 3.13 to 3.34. In 2024 emissions were flat and real output contracted. Fuel
switching in the generation mix is the obvious candidate for 2021 and is not
measured here; what the diagnostic establishes is that the reversal is on the
emissions side, not the output side.

⚠️ 2021 is also the sharpest illustration of why the real series is the one to
read. Nominal electricity output rose **19.8%** that year, from $473bn to
$567bn, while real output rose 0.4%. On nominal `x` this cell would post a
large *negative* divergence in the very year its emissions rose.

Of the 59 cells carrying more than 10 Mt of gross movement, **28 oscillate
above 0.75 and carry 860 Mt between them**, against 8 trending cells below
0.25 carrying 289 Mt. The largest cells above 10 Mt:

| cell | gross | net | oscillation |
|---|---:|---:|---:|
| `221100` electric power \| UMD 2-S1 electric_power → Direct | 609.2 Mt | −354.7 | 0.42 |
| `481000` air transport \| UMD 3-8 direct_jet → Direct | 99.5 Mt | −24.7 | 0.75 |
| `1111B0` grain farming \| UMD 5-10 direct → EPA_GHGI_soils | 88.1 Mt | −9.5 | 0.89 |
| `211000` oil and gas \| UMD 3-28 → Direct | 79.0 Mt | −70.0 | 0.11 |
| `211000` oil and gas \| UMD 3-11 natural_gas_nonmanufacturing → Use | 64.0 Mt | −1.1 | **0.98** |
| `211000` oil and gas \| UMD 3-25 → Direct | 62.9 Mt | −52.4 | 0.17 |
| `211000` oil and gas \| UMD 3-11 petroleum_industrial → MECS | 51.3 Mt | +6.6 | **0.87** |
| `212100` coal mining \| UMD 2-S1 direct → Direct | 42.9 Mt | −5.7 | **0.87** |

Oil and gas extraction takes four of the eight, and they split cleanly by
attribution route: its two `Direct` cells trend hard, oscillation 0.11 and
0.17, while its two industrial-fuel cells attributed through the Use table or
the MECS energy FBS move 115 Mt between them and arrive 6 Mt from where they
started. **The inventory-named part of this sector goes somewhere; the derived
part does not.** That contrast is the single clearest statement in the panel of
what attribution churn costs, and it is the mechanism sections below pursue.

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

Concentration, after that correction: the 1,991 cells carry 2,993 Mt of gross
movement between them, and the top 10 hold 39%, the top 25 hold 56%, the top
50 hold 70%. A top-N view is representative rather than a sample.

### `L` moves `N` more than the factors do, and is out of scope

`N = B @ L`, and `L` comes from each year's own `A` at that year's prices —
the nowcast configs set `apply_io_year_adjustments: False`, so nothing deflates
it. Holding `L` at the prior year isolates the part of the `N` move that the
emission factors explain; `pct_change_N_L_held` and `pct_change_N_L_effect`
carry the split. Medians over commodities:

| year | median \|Δ`N`\| | factors | `L` |
|---|---:|---:|---:|
| 2018 | 3.8% | 1.4% | 3.1% |
| 2019 | 9.1% | 2.9% | 6.2% |
| 2020 | 12.8% | 3.8% | 9.4% |
| 2021 | 18.4% | 3.3% | **14.8%** |
| 2022 | 5.3% | 3.6% | 5.0% |
| 2023 | 15.9% | 5.2% | **10.3%** |
| 2024 | 6.4% | 1.9% | 3.9% |

`L` accounts for two to four times what the factors do. Some of that is real
structural change and some is relative prices; the two are not separated here
because `A` is not deflated on this path.

⚠️ **`L` is out of scope for the smoothing project, by construction.**
`B = (E/x) @ Vnorm` has exactly three inputs and `L` is not one of them — it
enters only through `N`. `L` comes from `A = U_norm @ V_norm`, the nowcast's
own IO product, so no emissions-side change can move it. Diagnose it here,
remediate it in Nowcast Phase 2.

The same boundary applies to `Vnorm` for *remediation* even though it is a
direct term in `B`: its 12.2% share of `B` movement in 2023, output-weighted, is the nowcast Make
moving, and no GHG inventory or attribution change will touch it. This project
owns the `E` and `x` sides of the fix, and hands the Make and the `A` matrix
over.

⚠️ **What this means for the objective.** The stated intent is to reduce the
`N` change to justifiable changes. With `L` out of scope, this project can
deliver the factor-driven column above — 1.4% to 5.2% a year — and not the `L`
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
| unweighted, every commodity equal | 8.5% | 7.1% | 9.6% | 5.2% | 5.9% | **35.9%** | **22.8%** |
| weighted by commodity output `q` | 3.9% | 5.0% | 6.4% | 3.4% | 2.7% | **12.2%** | **11.9%** |

⚠️ **These figures supersede an earlier set that could not be reproduced.** The
numbers previously printed here — 26.5% unweighted in 2023 against 35.9% now —
came from an ad-hoc calculation, and none of the three defensible index forms
reproduces them on the same span and the same vintages. The module's version is
now the source. The story is unchanged: a step up in 2023-24, and the weighted
figure roughly half the unweighted one.

⚠️ **The direction of the index matters more than it looks.** Moving `Vnorm`
against the current year's intensity gives 4.7% in 2021 and against the prior
year's gives 5.8%, a 25% spread on the same quantity. Neither year has a claim
to being the base, so the headline above is their average and both one-sided
readings are carried in the CSV.

⚠️ **Quote the weighted row.** The unweighted figure counts a kg/$ swing on a
commodity nobody buys the same as one on electricity, and roughly doubles the
Make's apparent importance. On the weighting that matters the Make is 3% to 6%
of gross `B` movement through 2022 and then 12.2% and 11.9% — still a clear
step up in 2023, still the one driver this project cannot remediate, but a
third the size the unweighted number suggests.

In absolute terms the Make has moved **25 Mt CO2e onto a different commodity**
than the 2017 Make would have put it on, by 2024 — about 0.5% of total
emissions. It rises steadily to 25.9 Mt in 2022 and then flattens, 24.5 Mt in
2023 and 25.5 Mt in 2024, so the mix drift is a 2017-2022 story that has since
stalled rather than a continuing trend. That is the green line on
`E_vs_x_indexed.png`.

⚠️ That flattening sits oddly beside the Make's *share* of gross `B` movement,
which steps up in exactly those two years. Both are measured and neither is
wrong: the Make has stopped drifting further from the 2017 mix while the
year-on-year reshuffling within it got larger. Cumulative displacement and
annual churn are different quantities.

⚠️ **`Vnorm` has no level line, and `q` is not a substitute for one.** The
output-weighted total of `B` is total emissions by construction — measured,
5,026 Mt against 5,019 Mt in 2022, the 0.14% gap being the scrap correction —
and freezing `Vnorm` at 2017 moves that total by 0.1%. The Make redistributes
emissions across commodities; it does not create or destroy them, so an indexed
`Vnorm` line would sit flat at 100. `q` would not help either: `q` and `x` are
the same money counted along two axes and total to the dollar. The
redistribution is the only thing there is to plot, which is what
:func:`make_reallocation` measures.

For 263 commodity-years — 9.3% of the panel — `Vnorm` moved the factor more
than `E / x` did, `vnorm_dominant_commodities` in D13, between 22 and 51 a year.
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

## 3. Prices are 74% of the 2021 and 2022 gap

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
| 2022 | **74%** |
| 2023 | 19% |
| 2024 | 40% |

On the factor itself the same effect shows as a median absolute EF change of
10.8% nominal against 7.2% real in 2022. 2024 runs the other way: 3.4% nominal,
7.0% real.

⚠️ Any approach-1 ranking built on nominal `x` charges sources for inflation.
Use the `*_real` tables.

## 4. Five mechanisms that move `B` with nothing underneath

Separating justified from unjustified change is what the objective turns on.
Five mechanisms found so far:

1. **Vintage relabelling.** EPA renumbered its soils attribution tables
   mid-span — `T_5_17` to `T_5_18` for direct soils, `T_5_18` to `T_5_19` for
   indirect — and non-energy use moved `T_3_25b` to `T_3_25` for 2023, so
   `T_5_18` means *direct* in some years and *indirect* in others. Keyed on the
   raw table number, 2019 booked −301 Mt against +255 Mt while the emissions
   ran flat at ~290 Mt; 2023 did the same at −80 Mt against +86 Mt. Definitionally
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
   `Nowcast_Detail_Use_AfterRedef` at 1.02, so the inventory-named half tracks
   output barely more than half as fast as the Use-derived half, which tracks
   it one for one.
4. **`Vnorm` churn.** Section 2.
5. **Survey vintage steps.** A four-yearly survey applied with a hard cutoff
   moves the whole allocation on the cutoff year and nowhere else. Measured
   below for MECS.

### The MECS vintage cutoff lands between 2020 and 2021

Tracked as [#918](https://github.com/cornerstone-data/bedrock/issues/918);
tracker rows 1, 2 and 15.

EPA table 3-11, fossil fuel combustion, is **the largest driver family in the
panel**: 698 Mt of gross movement over 567 cells, 23.3% of all gross movement
in the panel, netting only −50 Mt. It reaches sectors by two different routes,
and the split is roughly 70/30:

| route | who it covers | gross | net | cells |
|---|---|---:|---:|---:|
| `Energy_manufacturing_national_nowcast` | manufacturing, via MECS | 504 Mt | −16 Mt | 525 |
| `Nowcast_Detail_Use_AfterRedef` | non-manufacturing, via the Use table | 194 Mt | −34 Mt | 42 |

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
| 2018 | 6.2 | 16.1 |
| 2019 | 5.2 | 7.6 |
| 2020 | 9.5 | 22.0 |
| **2021** | **19.6** | **27.3** |
| 2022 | 7.5 | 23.8 |
| 2023 | 7.4 | 24.2 |
| 2024 | 4.4 | 8.4 |
| mean excluding 2021 | 6.7 | 17.0 |
| **2021 as a multiple of that** | **2.92x** | 1.60x |

2021 is the largest reallocation year on both routes, so some of it is the COVID
rebound — but the control puts that at 1.60x, and the MECS route runs 2.92x.
Scaling the MECS baseline by the control's own amplification leaves roughly
**9 percentage points of 2021 reallocation that the cutoff explains and the
rebound does not.**

⚠️ **This bounds the effect; it does not isolate it.** The control is a
different sector population — non-manufacturing is intrinsically rockier, 17pp
a year against 7pp — so it is the wrong population to subtract, only the right
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
| facility | 49 | 281.5 | 34.8% |
| facility + Use residual | 91 | 102.7 | 12.7% |
| facility, boundary to settle | 19 | 141.0 | 17.4% |
| **Use row — no facility data** | 94 | **283.5** | **35.1%** |

**64.9% of table 3-11 could be facility-backed.** The 35.1% that cannot is
agriculture and construction, plus cement and coal mining -
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
on facility evidence. Total absolute movement **14.9 percentage points**:

| sector | allocated Mt | facility Mt | now | hybrid | shift | facilities |
|---|---:|---:|---:|---:|---:|---:|
| `324110` petroleum refineries | 76.1 | 124.3 | 9.40% | 14.90% | **+5.50pp** | 147 |
| **`211000` oil and gas extraction** | 90.1 | 71.2 | 11.14% | 8.54% | **−2.60pp** | 1,461 |
| `325110` petrochemicals | 22.3 | 41.0 | 2.76% | 4.92% | +2.16pp | 52 |
| `331110` iron and steel | 21.5 | 38.1 | 2.66% | 4.57% | +1.91pp | 131 |
| `311221` wet corn milling | 7.4 | 20.4 | 0.91% | 2.44% | +1.53pp | 33 |
| `221200` natural gas distribution | 0.15 | 11.9 | 0.02% | 1.42% | +1.41pp | 60 |
| `325180` other basic inorganic chemicals | 14.9 | 6.6 | 1.84% | 0.79% | −1.05pp | 91 |

⚠️ **Oil and gas extraction is the one large sector the facility basis marks
*down*, by 2.6 percentage points.** That is an independent corroboration of
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

The intermittent group is where the test bites, and oil and gas extraction
leads it:

| sector | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | spread |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **`211000` oil and gas extraction** | **0.66** | 1.16 | 1.12 | **0.81** | 1.29 | 1.93 | 1.21 | **2.93x** |
| `322110` pulp mills | 0.77 | 0.87 | 0.89 | 0.90 | 1.14 | 1.79 | 1.49 | 2.32x |
| `2123A0` other nonmetallic mining | 1.08 | 1.21 | 1.16 | 0.86 | 0.80 | 0.95 | 1.05 | 1.52x |
| `322120` paper mills | 1.11 | 1.10 | 1.14 | 1.06 | 0.86 | 1.02 | 1.06 | 1.32x |
| `327400` lime and gypsum | 0.84 | 0.82 | 0.82 | 0.83 | 1.05 | 0.98 | 0.98 | 1.28x |
| `325110` petrochemicals | 0.92 | 0.85 | 0.88 | 0.82 | 1.02 | 1.00 | 0.98 | 1.24x |

⚠️ **This is the finding the build vintage most distorted.** The previous run
put oil and gas at a 13.0x spread and **5.7x more volatile than the next worst
sector**, on a 2020 ratio of 0.18. On the published FBS the spread is 2.93x and
pulp mills sit at 2.32x, so it leads the group by 1.26x rather than running
away with it. It is still first, still breaches in two years, and its 2022 peak
of 1.93 is still the largest single-year overshoot in the group — but the
"one sector dominates" reading does not survive, and neither does the factor of
5.4 in 2020, which is now 1.23 the other way.

⚠️ **2017 is below the floor at 0.66, and 2017 is matched to the published
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
| our table 3-11 for `211000` | 90.1 Mt | 58.6 Mt | **−35.0%** |

Both years use the 2022 MECS survey, so the fuel share is constant and the whole
move is the Use table.

⚠️ **This bounds the verdict rather than proving it.** Subpart C runs 52% to
152% of what we assign to this sector across the span — below 100% where the
sub-threshold tail is what we are missing, above it in 2017 and 2020 where the
floor is breached — so it is a partial yardstick in both directions. What it
establishes is that the *covered* portion did not move while our allocation
fell by a third.

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

⚠️ **That re-reads the whole family.** Of the 504 Mt of gross movement the
route labels assign to MECS, 216 Mt is `petroleum_industrial`. On provenance
rather than label:

| driver | gross movement | share of table 3-11 |
|---|---:|---:|
| BEA Use table (`natural_gas_nonmanufacturing`, `coal_nonmanufacturing`, **and `petroleum_industrial`**) | ~409 Mt | **59%** |
| MECS survey quantities (`ng_manufacturing`, `coal_manufacturing`) | ~288 Mt | 41% |

So the Use table carries nearly six tenths of the largest driver family in the
panel, against the 194 Mt that the route labels alone suggest. This is the mass
behind section 4's third mechanism, and it is why rows 1 and 2 of the tracker
move together: they are two Use-table cells, not one Use cell and one MECS
cell.

⚠️ **It also bounds what #918 can buy.** Smoothing the MECS vintage cutoff
changes a scalar multiplier on `petroleum_industrial`, not its shape, so it
cannot move the largest rocky cells. #918 owns the ~288 Mt that MECS
quantities actually allocate.

⚠️ **A smoothed cutoff is not the only candidate fix.**
[#919](https://github.com/cornerstone-data/bedrock/issues/919) would replace the
source rather than interpolate it — GHGRP facility data aggregated to NAICS is
annual, so the four-yearly step disappears instead of being smoothed over. The
two issues are alternatives on the same rows, and #919 also reaches the 194 Mt
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
`UMD 2-S1 electric_power → Direct` at 609 Mt, then
`UMD 3-11 petroleum_industrial` and `UMD 5-10 direct → EPA_GHGI_soils` at
107 Mt each, and `UMD 3-11 ng_manufacturing → MECS` at 101 Mt.

### Reading the legend: the unit is the table, not the label

⚠️ **Neither half of a legend entry identifies a driver family.** An entry is
`<UMD inventory table>.<stratum> → <what spread it across sectors>`, and both
halves mislead if read as a category:

- **The attribution half is a route, not a provenance.** `UMD 3-11
  petroleum_industrial → MECS` is not a MECS vector. Its weights are BEA Use
  purchases of `324110` rescaled by a MECS fuel fraction that is constant
  within a survey vintage (section 4) — the largest Use-table cell in the panel
  wearing a MECS label.
- **A fuel name is not a combustion category.** `petroleum` appears on mobile
  combustion, on residential and commercial combustion, on industrial
  combustion, and on non-combustion fugitives.

What the tables are, from `UMD_GHGIA.yaml` and the activity-set comments in
`GHG_national_Cornerstone_nowcast_<year>.yaml`:

| UMD table | what it covers |
|---|---|
| **3-11** | stationary combustion — **industrial**. The whole of [#923](https://github.com/cornerstone-data/bedrock/issues/923) |
| 3-4 | stationary combustion — residential and commercial |
| 3-8 | **mobile** combustion, transportation end-use |
| 3-14 | non-energy use — carbon in feedstocks, never burned |
| 3-25, 3-26 | petroleum *systems* — fugitive and vented, not combustion |

So the #923 family on this figure is **every `UMD 3-11` entry and only those**:
`petroleum_industrial` 107.2 Mt, `ng_manufacturing` 101.4,
`natural_gas_nonmanufac` 48.5, plus `coal_manufacturing` 24.3 and
`coal_nonmanufacturing` 9.6 bundled inside `other` — **291.0 Mt, 14.5%** of the
panel. Filtering on the legend text instead — every entry naming MECS, plus
every `petroleum* → Use` — gets 418 Mt that is only half the right mass: it
catches 208.6 Mt of table 3-11, pulls in 209.5 Mt that is not (145.9 Mt of 3-8
mobile, 63.5 Mt of 3-4 commercial), and drops the 82.4 Mt of table 3-11 that is
natural gas and coal.

⚠️ **291.0 Mt here and the 698 Mt in tracker row 15 are the same family on two
aggregations, not a disagreement.** This figure sums sectors within a year
before taking the absolute value, so opposing sector movements net; D9 keeps
each (sector × source) cell separate. Quote 291.0 Mt against this figure and
697.9 Mt against `sector_stratum_span.csv`, and never mix them in one
comparison.

Table 3-4 is stationary combustion too, so #923's title reaches it while its
body scopes table 3-11 alone. Facility data would not anchor it either —
commercial buildings sit below GHGRP's 25,000 tCO2e threshold and are not NEI
point sources — so the 63.5 Mt on `3-4 petroleum_commercial` shares the
Use-table mechanism of tracker row 12
([#933](https://github.com/cornerstone-data/bedrock/issues/933)) but needs a
different fix, and is not covered by #923 as written.
