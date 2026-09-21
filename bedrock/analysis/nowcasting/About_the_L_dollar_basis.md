# The dollar basis of L, and what it does to N

Findings for [#937](https://github.com/cornerstone-data/bedrock/issues/937),
handed to Nowcasting Phase 2 from the B smoothing diagnostics (tracker row 10,
diagnostic D6b). The code is [`L_dollar_basis.py`](L_dollar_basis.py); it runs
off the span `B_change_diagnostics` caches, so it needs no model build.

Every figure here is the run of 2026-09-21 on FBS `v0.3.0_99655e9` and MUT
`v0.3.0_4276083` — the same pinned vintages the B smoothing results carry, so
the two sets of numbers are directly comparable.

## What #937 asked, and the answer

> Establish how much of the `L` movement is real structural change and how much
> is relative prices. […] Decide whether `A` should be deflated for the purpose
> of a stable `N`.

**Deflating `A` changes `N` by nothing at all**, and the `L` effect #937 was
filed on is roughly half an artefact of a mixed dollar basis. Both fall out of
one identity, so take that first.

## The identity

A real input coefficient deflates the input and re-inflates the output —
`a_real[i, j] = a[i, j] * r_j / r_i`, where `r_j` is commodity *j*'s price ratio
from the base year. In matrix form that is `A_real = diag(1/r) A diag(r)`, and
because `I = diag(1/r) I diag(r)` as well, the transform carries straight
through the inverse:

```
I - A_real = diag(1/r) (I - A) diag(r)
L_real     = diag(1/r) L diag(r)          L_real[i, j] = L[i, j] * r_j / r_i
```

So `L` never has to be re-derived from a deflated `A` — which is also why this
analysis costs nothing. A real direct factor is `B_real = B * r`: the same
kilograms over fewer constant dollars. Put the two together:

```
N_real[j] = sum_i (B[i] * r_i) * (L[i, j] * r_j / r_i)
          = r_j * sum_i B[i] * L[i, j]
          = r_j * N[j]
```

**The `r_i` cancels.** A consistently deflated `N` is the nominal `N` with its
own denominator deflated and nothing else — which is exactly what
`inflation_adjust_ef_denom_to_new_base_year` already does on the reporting
path. `--check` verifies the cancellation at 6.5e-16.

⚠️ **The cancellation needs both sides or neither.** That is the whole finding.

## Where the 2-to-4x came from

`B_change_diagnostics` deflates `B` — correctly, because a nominal `E / x`
falls whenever prices rise — and used to leave `L` at each year's own prices.
Its `N_total` docstring already warned that the result was "a mixed-basis
level"; the D6b table that #937 quotes is built on it, and so is the 2-to-4x.
**Fixed on 2026-09-21 ([#957](https://github.com/cornerstone-data/bedrock/issues/957))**
— `N_total` now moves both sides together, so the middle column below is
history rather than current output.

Median absolute percent change in `N` over commodities, on each of the three
ways of holding `B` and `L`:

| year | nominal `B`, nominal `L` | **real `B`, nominal `L`** (the hybrid, pre-#957) | real `B`, real `L` (now) |
|---|---:|---:|---:|
| 2018 | 3.49 | 3.83 | **3.41** |
| 2019 | 8.18 | 9.08 | **7.02** |
| 2020 | 9.80 | 12.83 | **9.40** |
| 2021 | 6.70 | **18.39** | **8.14** |
| 2022 | 12.60 | 5.34 | **6.26** |
| 2023 | 10.64 | **15.94** | **8.87** |
| 2024 | 4.65 | 6.35 | **6.85** |

And the split into the part the emission factors explain and the part `L` does:

| year | factors (real) | `L` (real) | `L` ÷ factors | *pre-#957* |
|---|---:|---:|---:|---:|
| 2018 | 1.41 | 2.72 | 1.9 | *2.2* |
| 2019 | 2.87 | 4.49 | 1.6 | *2.1* |
| 2020 | 3.64 | 6.12 | 1.7 | *2.5* |
| 2021 | 3.25 | **5.95** | 1.8 | *4.5* |
| 2022 | 3.61 | 4.44 | 1.2 | *1.4* |
| 2023 | 4.62 | 5.09 | 1.1 | *2.0* |
| 2024 | 1.74 | 5.18 | 3.0 | *2.1* |

![N read on three dollar bases](images/L_dollar_basis.png)

Output-weighted rather than median, the real-basis ratio runs 1.3 to 3.0 and
the `L` effect 3.4 to 7.9 points — the same story with `L` a little stronger.

**So #937's direction stands and its magnitude does not.** `L` does lead the
emission factors in every year. It leads them by **1.1x to 3.0x**, not two to
four, and 2021 — the year that motivated the filing, at 4.5x — is the year the
hybrid basis distorts most.

### It is not a uniform overstatement

The hybrid does not inflate everything; it injects noise in both directions.
2022 and 2024 are *understated* by it. The year ranking changes: on the
reported series the worst years are 2021 and 2023, and on a consistent basis
the worst is **2020**, with 2021 third.

Per commodity-year it is worse than the medians suggest. The median absolute
gap between the two bases is 16.4 points in 2021 and 8.3 in 2023, and
**531 of 2,814 commodity-years — 18.9% — disagree on the *sign* of the `N`
move.** A commodity whose factor is reported as rising is falling on a
consistent basis in nearly one case in five. `L_basis_per_commodity.csv` ranks
them; the widest are `335110`, `335991` and `334220`, each above 40 points.

## Should `A` be deflated?

**For `N`: there is nothing to decide.** The cancellation is exact, so
deflating `A` is neither right nor wrong for a factor — it is a no-op as long
as `B` is deflated with it. What must not happen is deflating one and not the
other, and that is what is happening today in the diagnostics.

**For `N` stability: no, and it would not help anyway.** Deflating does not
quieten `L`. Gross year-on-year movement, `sum |dL|` over all cells:

| year | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---:|---:|---:|---:|---:|---:|---:|
| real ÷ nominal | 1.07 | 0.97 | 0.98 | 0.95 | 1.08 | 1.00 | **1.64** |

Within 9% of the nominal series every year but 2024, where the real `L` moves
**64% more**. Prices were masking real structural movement in 2024, not
creating it. Whatever is behind #938's unexplained 2023–24 step-up in the
Make's share of `B` movement, deflation does not make it go away — and on this
evidence 2024 gets worse, not better, when measured properly.

**For reading `A` or `L` as structure: yes, and it is not optional.** This is
the one place in the analysis where the deflation changes an answer, because
it reads `L` directly instead of feeding it to `N`. Column sums of `L` are
total requirements per dollar of final demand:

| | 2017 | 2024 | span change |
|---|---:|---:|---:|
| median, nominal | 2.033 | 1.963 | **−3.47%** |
| median, real | 2.033 | 2.028 | **−0.29%** |
| mean, nominal | 2.080 | 2.009 | −3.43% |
| mean, real | 2.080 | 2.043 | −1.76% |

On the nominal `L` the median commodity's supply chain shortens 3.5% over the
span. On the real one it is flat. Almost all of that apparent shortening is
relative prices — inputs getting cheaper relative to the output they go into,
with no change in what is bought. (On the unweighted mean about half survives
deflation, so state which statistic is being quoted.)

⚠️ This bears directly on [#905](https://github.com/cornerstone-data/bedrock/issues/905),
the 2012–2017 time series, and on the A-matrix leverage work: a longer span
means more accumulated relative price movement, so a nominal series would read
an ever-larger spurious shortening. It is also why
[#397](https://github.com/cornerstone-data/bedrock/issues/397) is worth closing
on its own terms rather than as a route to a stabler `N`.

## What this costs the B smoothing project

#937 was filed partly to bound what that project can deliver, and the bound is
real but smaller than stated. On a consistent basis the project's lever — the
emission factors — is worth **1.4 to 4.6 points** of median `N` movement a
year, against `L`'s 2.7 to 6.1. A flat factor column still must not be read as
"`N` is now smooth".

✅ **The correction was applied back.** D6b's `pct_change_N`,
`pct_change_N_L_held` and `pct_change_N_L_effect` were mixed-basis columns and
row 10 of the driver tracker quoted them; `N_total` now deflates both sides and
every figure that depended on it has been restated in place
([#957](https://github.com/cornerstone-data/bedrock/issues/957)).

⚠️ **One claim made when #957 was filed does not survive.** The gate
`delta_B_pct_of_N` is invariant to the choice of **base year** — the `r_j`
cancels between `dB` and `N` — but it is **not** invariant to the
hybrid-to-real switch, because that changes the *basis* and its `N` denominator
moves with it. Measured: a median shift of 0% (2018) to 15% (2023) in the
value, 99th percentile 50%. What does hold is the use it is put to — the
ranking keeps **29 or 30 of the top 30** in every year and pooled. So the
gate's history is not comparable across the fix, and its verdicts are.

## Method notes and caveats

**The deflator.** `x / x_real` straight off the cached span, so the price index
here is by construction the one the diagnostics already deflate `B` with.
Carried onto the commodity axis as a `V_norm`-weighted average of supplying
industries' ratios, with base-year weights, the same form as
`get_vnorm_adjusted_commodity_price_ratio` but built locally so no
`functools.cache` can carry a stale config across.

**It is not load-bearing.** Four choices — base-year `V_norm` weights, each
year's own weights, a flat 1:1 industry-to-commodity reindex, and chaining each
pair onto its own prior year instead of onto 2017 — all give the same answer:
the widest disagreement in the `L` effect in any year is **0.25 points**,
against an effect of 2.7 to 6.1. `--check` runs all four and fails if any of
them moves the answer by half a point.

⚠️ **One inconsistency remains in the real basis, and it is measured rather
than waved at.** `B_total(real=True)` divides each industry's emissions by that
industry's own deflated output and *then* maps to commodities through `V_norm`;
the `L` deflation must work on the commodity axis, because that is the axis `A`
is on. The two coincide only where industry prices are uniform within a
commodity's supplying mix. The relative gap between them is **0.02% to 0.14%
at the median** and reaches 36% at the maximum (2022). Immaterial for anything
read across the panel; **check `L_deflator_axis_gap.csv` before quoting a
single commodity's real `N`.**

❌ **Do not read the three columns of a split as a partition.** Each is a
separate median of an absolute value. A commodity whose factor and `L` effects
have opposite signs contributes to both without contributing to the total, so
they do not add up and the shortfall is not an error.

**Not addressed here.** Whether the nowcast's own construction should carry
prices differently — `theta` already sets how much of a commodity's price
movement passes into its nominal cost share, and at `theta = 0` the carried
shares are price-invariant by construction (see
[`About_the_price_carry.md`](About_the_price_carry.md)). The interaction
between that assumption and the deflation above is worth a look and is not
part of this measurement.

## Output

Seven tables and a plot under `output/`, which is gitignored:
`L_basis_median.csv`, `L_basis_output_weighted.csv`,
`L_basis_per_commodity.csv`, `L_supply_chain_length.csv`,
`L_gross_movement.csv`, `L_commodity_price_ratio.csv`,
`L_deflator_axis_gap.csv`, `L_dollar_basis.png`.
