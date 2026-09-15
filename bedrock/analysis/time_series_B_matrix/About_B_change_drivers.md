# What moves `B`, and how much of it is real

Findings from `derive_B_time_series.py`, run 2026-09-15 against FBS vintage
`v0.3.0_796a6ca` and nowcast MUT vintage `v0.3.0_4276083`, nowcast models
2017-2024. Method and the diagnostic-to-approach mapping are in
[`B_matrix_smoothing_plan.md`](B_matrix_smoothing_plan.md); this note holds the
numbers.

`B = (E / x) @ Vnorm`, so the three drivers the plan names — `E`, `x` and
`Vnorm` — are the three places a factor can move from. The sections below
measure how much each contributes, what a 5% gate actually selects, and four
mechanisms that produce movement with nothing underneath it.

⚠️ **Every figure here is industry emissions only.** `F01000` personal
consumption expenditures has no gross output, so it cannot enter `E / x`; the
production path drops it for the same reason.

---

## 1. A 5% gate selects most of the economy

Interannual change in the real-dollar commodity factor, `B_change_real.csv`,
commodities with a non-negligible prior factor:

| year | commodities | over 5% | over 10% | over 20% | median |
|---|---:|---:|---:|---:|---:|
| 2018 | 402 | 48.8% | 27.6% | 6.0% | 4.9% |
| 2019 | 402 | 48.3% | 23.6% | 7.5% | 4.8% |
| 2020 | 402 | 68.7% | 42.5% | 19.4% | 8.6% |
| 2021 | 400 | **85.5%** | 70.2% | 42.8% | 16.9% |
| 2022 | 401 | 71.6% | 47.4% | 23.2% | 9.5% |
| 2023 | 401 | 67.6% | 46.1% | 20.9% | 9.0% |
| 2024 | 400 | 64.5% | 38.5% | 11.0% | 7.6% |

The median commodity moves about as much as the gate, so 2021 admits 342 of
400. Raising it to 20% admits 6-43% depending on year.

⚠️ **A percentage ranking does not answer "reduces cumulatively the most".** An
EF on a commodity nobody buys ranks the same as one on electricity. Rank on
`|delta_B| × q` — the change in the factor times the commodity's output — to
get the cumulative-impact ordering. `B_change_real.csv` carries `delta_B` and
`B_from`; the weight joins from `q` or final demand.

⚠️ Sort `B_change_real.csv` on `abs_pct_change` but filter on `B_from` first. A
commodity whose factor rounds to zero posts a large percentage off a
rounding-scale numerator: 2023 leads with watch and clock manufacturing at
−1208% off a 0.0004 Mt base.

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
