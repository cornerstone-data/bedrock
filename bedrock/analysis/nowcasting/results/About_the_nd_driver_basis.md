# The N_d driver decomposition, restated on one dollar basis

Findings for [#958](https://github.com/cornerstone-data/bedrock/issues/958).
The code is [`nd_drivers.py`](nd_drivers.py); this note records what changed
when `rebase` started moving `A` and `L` onto the common dollar year alongside
`D`, and which of the standing driver findings survive it.

Run of 2026-09-21, nowcast models 2017-2024, all figures in 2024 dollars. The
before column is the run of 2026-09-11 that the results deck carries on slides
12-14, reproduced exactly from the same code before the fix.

⚠️ **Slides 12-14 of the results deck are stale.** They are outside this repo
and were not updated by this change.

## What was wrong

`rebase` deflated `D` and left `A` and `L` at each year's own prices, on the
stated grounds that

> `A` is a ratio of current dollars to current dollars and `L` follows from
> it, so neither is rebased — which is also why the structure effect is the
> term that survives the mistake intact.

A dollar-to-dollar ratio is not price-neutral. `a[i, j] = (p_i q_ij) / (p_j x_j)`
moves with the **relative** price `p_i / p_j`, and only uniform inflation
cancels. Over this span inflation was anything but uniform: the 10th-to-90th
percentile spread of annual industry price moves goes from 1.06 in 2018-20 to
1.18 in 2021 and 1.28 in 2024, and refined petroleum's own index runs
1.22 → 0.79 → 1.95 → 1.45.

**So the structure effect was not surviving the mistake. It was absorbing it.**

## The fix

`A` and `L` take the same transform as each other — deflate the input,
re-inflate the output — in the house price term `ρ`, the inflation adjustment
factor `ρ_ty = Π_by / Π_ty`. ⚠️ That is the **reciprocal** of the forward
ratio `D` is multiplied by. Writing `ρ̂` for the diagonalised vector as the US
methods paper does:

```
M_rebased = ρ̂ M ρ̂⁻¹                     M[i, j] · ρ_i / ρ_j
I - A_r   = ρ̂ (I - A) ρ̂⁻¹    =>    L_r = ρ̂ L ρ̂⁻¹
```

This is the inverse of the paper's own A transform `A_ty = ρ̂⁻¹ A_sy ρ̂`, which
inflates rather than deflates. It carries through the inverse, so `L` needs no
re-solve:

Each year's rebased pair still satisfies `L_r = (I - A_r)^-1`, which is what
keeps the cell partition `L₂ − L₁ = L₂ (A₂ − A₁) L₁` exact. Both identity
checks pass on all seven spans, and `rebase` now asserts the cancellation it
implies — with both sides rebased, `D_r @ L_r` must equal `(D @ L) / ρ`, which
fails the moment one side is left behind.

✅ **That retires the old warning against comparing levels with
[`nowcast_key_sector_nd_series`](nowcast_key_sector_nd_series.py).** That
module deflates the target's own denominator, which is exactly what a
consistent rebasing produces, so it was right all along and the two now agree
rather than being "close but not identical".

## What moved

The emissions side barely moves — `D` was already deflated — and the structure
side and the totals move a great deal:

| | median absolute shift | maximum |
|---|---:|---:|
| emissions effect | 0.09 pp | 1.53 pp |
| **structure effect** | **1.09 pp** | **13.87 pp** |
| total `N_d` change | 1.15 pp | 13.78 pp |

**11 of 70 sector-years flip the sign of the structure effect**, and 6 of 70
flip the sign of the total move.

⚠️ **The nominal basis was hiding structural movement, not inventing it.**
Mean absolute annual `L` column-sum move is **3.00 pp nominal against 5.40 pp
real** — 1.8x. Across all sectors the median absolute structure effect rises
from 1.78 to 2.14 pp, and the structure-to-emissions ratio from 0.41 to 0.49.
Deflating makes the structure side *larger*, because the energy sectors whose
prices swung hardest had their real input-mix movement masked by those swings.

## The investigation list, re-derived

| sector | span | total, before → after | structure, before → after |
|---|---|---:|---:|
| Automobiles | 2023→24 | −24.8% → **−18.5%** | −22.9 → **−16.6 pp** |
| Petroleum refineries | 2022→23 | −17.0% → −17.3% | −9.1 → **−10.0 pp** |
| Oil & gas extraction | 2022→23 | −17.6% → **−24.1%** | −5.6 → **−13.1 pp** |
| Petrochemicals | 2020→21 | +30.2% → **+44.0%** | +0.7 → **+13.3 pp** |

Item by item against what the tracker said:

- ⚠️ **Automobiles 2023→24 is smaller and no longer the largest structural
  year.** −16.6 pp rather than −22.9. It is still a large single-year
  structural move and still worth settling against
  [#850](https://github.com/cornerstone-data/bedrock/issues/850) — but the
  panel's largest is now **automobiles 2021→22 at −21.6 pp**, which the
  nominal basis put at −15.4 and which nobody was looking at.
- ✅ **Petroleum refineries 2022→23 stands**, −10.0 pp against −9.1. This is
  the one item the fix leaves essentially alone. ⚠️ Its *other* years do move,
  hard: 2021→22 goes −2.8 → **+8.7 pp** and 2017→18 +0.4 → **+7.7 pp**.
- ⚠️ **Oil & gas extraction 2022→23 more than doubles**, −5.6 → −13.1 pp, and
  its total move goes from −17.6% to −24.1%. It remains the largest single-year
  total move in the panel and the structural share of it is now far bigger than
  reported.
- ❌ **Petrochemicals 2020→21 is no longer "almost all emissions".** The
  tracker recorded +30.2% of which +29.5 emissions, with a *negative* structure
  effect. On one basis it is **+44.0% of which +13.3 pp is structure**. That
  finding is withdrawn.

The largest structural years on the corrected basis, in order:

| sector | span | structure | (was) | total |
|---|---|---:|---:|---:|
| Automobiles | 2021→22 | **−21.6 pp** | −15.4 | −27.0% |
| Automobiles | 2023→24 | −16.6 pp | −22.9 | −18.5% |
| Petrochemicals | 2020→21 | +13.3 pp | +0.7 | +44.0% |
| Oil & gas extraction | 2022→23 | −13.1 pp | −5.6 | −24.1% |
| Petroleum refineries | 2022→23 | −10.0 pp | −9.1 | −17.3% |
| Beef cattle | 2022→23 | +9.7 pp | +7.4 | +9.2% |
| Oil & gas extraction | 2021→22 | +9.6 pp | +3.0 | +4.9% |
| Petrochemicals | 2018→19 | −8.8 pp | −0.4 | −27.1% |
| Petroleum refineries | 2021→22 | +8.7 pp | −2.8 | +7.5% |

## The endpoint span, 2017-2024

The chained table above reads timing; the endpoint decomposition reads the
whole span, and it moves too. `L` column sum is the sector's total
requirements per dollar — how long its supply chain is:

| sector | `N_d` span, before → after | `L` column sum, before → after |
|---|---:|---:|
| Petroleum refineries | −26.9% → **−16.5%** | −4.5% → **+3.0%** |
| Automobiles | −53.0% → −57.2% | −19.1% → **−22.5%** |
| Oil & gas extraction | −33.7% → −34.6% | +7.8% → +6.6% |
| Coal mining | −15.0% → −13.8% | −3.8% → **+3.8%** |
| Truck transport | −5.3% → −3.0% | −1.5% → **+1.9%** |
| Air transport | −11.7% → −12.8% | −5.9% → −8.8% |
| Grain farming | −11.8% → −11.2% | −3.3% → −0.5% |
| Beef cattle | −21.7% → −21.6% | −6.8% → −5.7% |
| Petrochemicals | +5.7% → +5.0% | −9.2% → −10.1% |
| Electric power | −20.7% → −20.9% | −4.3% → −4.2% |

❌ **"Oil and gas extraction is the only sector that lengthens" is withdrawn.**
On a nominal `L` it was the only one of the ten with a rising column sum. On a
real one, **coal mining and truck transport rise too** — both were reading as
shortening purely because their inputs got cheaper relative to their own
output. Three of ten lengthen, not one.

⚠️ **Petroleum refineries' span `N_d` change is the single largest
restatement**, −26.9% → −16.5%. Its supply chain reads as *lengthening* 3.0%
in real terms where the nominal series had it shortening 4.5% — unsurprising
for the sector whose own price index runs 1.22 → 0.79 → 1.95 → 1.45, and a
reason to treat any refinery-adjacent nominal coefficient with suspicion.

## What does not change

- ❌ **The COVID denominator rule is untouched.** That is about `x`, not `A` —
  `N` is per dollar, so 2019→20 raises it wherever output fell faster than
  emissions. Still not a change in production, and the emissions effects move
  by a median 0.09 pp.
- ❌ **`N_d` is still not a footprint.** Domestic technology only; the import
  block is excluded throughout. See
  [`nowcast_key_sector_nd_series`](nowcast_key_sector_nd_series.py).
- ✅ **The cell attribution is still an exact partition**, not a ranking. The
  transform preserves `L_r = (I - A_r)^-1` per year, so the identity the
  partition rests on holds; both checks pass on all seven spans.
- ⚠️ **Structure still changes sign across years for most sectors**, so an
  endpoint decomposition still nets opposite-signed years and still must not
  be read as evidence of a stable structure. The fix makes that *more* true,
  not less: it raises the median absolute structure effect.

## Caveat

The deflator is the repo's per-sector price index read 1:1 onto the commodity
axis — the same one `inflation_adjust_ef_denom_to_new_base_year` applies to
`D`, deliberately, so the two sides cannot drift. A supply-mix-weighted
commodity ratio is the more careful construction and moves comparable figures
by under 0.02 percentage points; see
[`About_the_L_dollar_basis.md`](../About_the_L_dollar_basis.md), which measures
the choice four ways.

Related: [#937](https://github.com/cornerstone-data/bedrock/issues/937),
[#957](https://github.com/cornerstone-data/bedrock/issues/957) (the same defect
in the B smoothing diagnostics),
[#397](https://github.com/cornerstone-data/bedrock/issues/397).
