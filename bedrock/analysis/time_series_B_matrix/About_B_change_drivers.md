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
