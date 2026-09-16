# What moves `B`, and how much of it is real

Findings from `derive_B_time_series.py`, run 2026-09-16 against FBS vintage
`v0.3.0_796a6ca` and nowcast MUT vintage `v0.3.0_4276083`, nowcast models
2017-2024. Method and the diagnostic-to-approach mapping are in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md); this note holds the
numbers.

`B = (E / x) @ Vnorm`, so the three drivers the plan names — `E`, `x` and
`Vnorm` — are the three places a factor can move from. The sections below
measure how much each contributes, how to gate on a factor change in a way
that discriminates, and four mechanisms that produce movement with nothing
underneath it.

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

## 2. `Vnorm` is a quarter of the movement in 2023

Each year's `Vnorm` is built from that year's nowcast Make, so a commodity's
factor moves when the industry-to-commodity mix moves, with `E / x` flat.
Holding one side at a time and differencing:

| year | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `Vnorm` share of gross `B` movement | 9.0% | 7.3% | 10.5% | 7.5% | 11.2% | **26.6%** | **21.6%** |

Under 12% through 2022, then 26.6% in 2023 and 21.6% in 2024. For 133
commodity-years — 4.7% of the panel — `Vnorm` moved the factor more than
`E / x` did. Those cannot be remediated on either the emissions or the output
side. What changed in the 2023 Make is an open question and is its own lead.

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

## 4. Four mechanisms that move `B` with nothing underneath

Separating justified from unjustified change is what the objective turns on.
Four mechanisms found so far:

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
