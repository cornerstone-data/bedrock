# Which sectors' L movement matters to B smoothing, and why

#937 established a bound — `L` moves `N` 1.1x to 3.0x as much as the emission
factors do, and the B smoothing project has no lever on it. A bound is not a
work list. This turns it into one.

Code: [`L_flux_priority.py`](L_flux_priority.py). Run of 2026-09-21 on FBS
`v0.3.0_99655e9` and MUT `v0.3.0_4276083`, real basis throughout.

## The measure

`pct_change_N_L_effect` from D6b is already the right quantity — the part of a
commodity's year-on-year `N` move that survives holding `L` at the prior year.
It has only ever been reported as a **median over commodities**. Ranking the
commodities themselves is all that was missing.

Per commodity, over 2017-2024:

- **gross** `Σ|L effect|` — how much `L` moved its `N` in total.
- **oscillation** `1 − |net| / gross` — near 1 means it moved and arrived back
  where it started, the signature of churn; near 0 means a trend.
- **weighted gross** — gross × mean share of commodity output. **This is the
  ranking.**

⚠️ **Weighting is not optional here.** The unweighted top 20 is almost entirely
micro-output manufacturing — motorcycles, magnetic media, computer storage
devices — with gross figures of 200-330% and output shares that round to zero.
They are real percentage moves on commodities nobody buys, and they would have
set the whole work list if the ranking had been left unweighted.

⚠️ **This ranking could not have been produced before [#957](https://github.com/cornerstone-data/bedrock/issues/957)
and [#958](https://github.com/cornerstone-data/bedrock/issues/958).** On the
nominal basis part of `L`'s movement is relative prices, and deflating turns
out to make the movement *larger* rather than smaller.

## The single biggest finding: it is mostly the electricity row

Of the 20 top-ranked commodities, the electricity row `221100` is a driving
`A` cell for **all 20**, and carries **30.9%** of all top-cell contribution
mass among them. Share of each commodity's own driving-cell mass:

| commodity | electricity share | oscillation |
|---|---:|---:|
| `518200` Data processing and hosting | **67.5%** | 0.47 |
| `441000` Motor vehicle and parts dealers | **57.5%** | 0.97 |
| `511200` Software publishers | 45.3% | 0.19 |
| `423A00` Other durable goods wholesalers | 42.7% | 0.93 |
| `531ORE` Other real estate | 38.8% | 0.28 |
| `424700` Petroleum and petroleum products | 38.0% | 0.66 |
| `541511` Custom computer programming | 36.3% | 0.06 |
| `52A000` Depository credit intermediation | 33.0% | 0.42 |
| `622000` Hospitals | 32.2% | 0.40 |
| `531HST` Tenant-occupied housing | 31.1% | 0.99 |

**This is [#896](https://github.com/cornerstone-data/bedrock/issues/896)**, the
electricity Use row, reached from a completely different direction. That issue
was filed from the electricity method's side — the flat generation price caps
purchasers whose own price falls below it, and the bills doing the moving are
this row. This analysis starts from `N` stability, ranks every commodity by how
much `L` destabilises it, and lands on the same row as the dominant cause.

⚠️ **Two independent routes to one defect raises its priority, and does not
double it.** #896 is already HIGH on the Phase 2 board and already named a
prerequisite for the electricity G/T/D revision. What is new is that it is also
the largest single lever on `L`-driven `N` instability, so fixing it pays the B
smoothing project as well as the electricity one.

The worst case is `441000` motor vehicle and parts dealers — gross 109.1,
oscillation **0.97**, and `L` moves its `N` **4.8x** as much as its own
emission factors do. Its own electricity coefficient:

| 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0.0150 | 0.0152 | 0.0131 | 0.0138 | 0.0169 | 0.0209 | 0.0150 | 0.0119 |

**+60% from its 2019 trough to its 2022 peak, then −43% by 2024**, ending 21%
below where it started. A car dealership's electricity per dollar of sales does
not do that.

## Churn: the defect candidates

Output-weighted gross with oscillation above 0.9 — large, and arriving where it
started:

| commodity | gross | osc | `L` ÷ own factors | worst year |
|---|---:|---:|---:|---|
| `441000` Motor vehicle and parts dealers | 109.1 | 0.97 | 4.80 | 2022, +27.8% |
| `811100` Automotive repair and maintenance | 80.4 | 1.00 | 3.44 | 2021, +37.8% |
| `423A00` Other durable goods wholesalers | 77.8 | 0.93 | 3.40 | 2021, +30.4% |
| `531HST` Tenant-occupied housing | 63.0 | 0.99 | 3.15 | 2020, +21.9% |
| `523900` Other financial investment activities | 61.9 | 0.95 | 1.85 | 2021, +20.5% |
| `423600` Household appliances and electronic goods | 60.8 | 0.99 | 2.82 | 2021, +26.6% |
| `48A000` Scenic and sightseeing transportation | 91.7 | 0.98 | 2.67 | 2021, +35.9% |
| `324110` Petroleum refineries | 38.9 | 0.98 | 1.69 | 2022, +10.1% |

⚠️ **Oscillation is a screen, not proof** — the same caveat the B driver
tracker carries. A large driver that oscillates near 1 is where to start an
investigation.

**`324110` petroleum refineries is a second convergence.** Its driving cells
are oil and gas extraction → refineries, 0.5469 → 0.6086 → 0.5588 → 0.6053 —
which is [#922](https://github.com/cornerstone-data/bedrock/issues/922), tracker
rows 1, 2 and 16, already carrying a **not justified** verdict on three external
checks. The same cell that makes the sector's *attribution* rocky also makes
its `L` rocky. One fix, two symptoms.

A cell not previously named: **pipeline transportation → oil and gas
extraction**, 0.0217 → 0.0481 → 0.0205 across 2022-23. A 2.2x jump and back in
two years, on the column that feeds refineries.

## Trend: leave alone

Oscillation below 0.25 and a monotone direction — these move `N` a lot and go
somewhere, which is what a real structural change looks like:

| commodity | gross | net | osc |
|---|---:|---:|---:|
| `531HSO` Owner-occupied housing | 29.8 | **−29.8** | **0.00** |
| `550000` Management of companies | 33.4 | **−33.4** | **0.00** |
| `541511` Custom computer programming | 63.2 | −59.3 | 0.06 |
| `541100` Legal services | 40.4 | −37.8 | 0.06 |
| `511200` Software publishers | 86.9 | −70.0 | 0.19 |
| `541512` Computer systems design | 60.5 | −47.8 | 0.21 |

Owner-occupied housing and management of companies oscillate at **exactly
zero** — every year moves the same way. Falling `N` for service commodities
whose supply chains are decarbonising is the expected shape, and smoothing here
would destroy signal.

⚠️ These are `justified`-looking, not `justified`. The verdict needs the
source-level check the tracker requires, and several of them also carry a large
electricity share — `541511` is 36.3% electricity with oscillation 0.06, so its
trend may be partly the electricity row trending rather than the sector's own
structure. Settle #896 first and re-read this table after.

## What to do, in order

1. **[#896](https://github.com/cornerstone-data/bedrock/issues/896) electricity
   Use row.** Already HIGH and a prerequisite for the electricity revision; this
   adds that it is the largest single driver of `L`-driven `N` instability
   across the highest-output churning commodities. Re-run this ranking after.
2. **[#922](https://github.com/cornerstone-data/bedrock/issues/922) /
   [#923](https://github.com/cornerstone-data/bedrock/issues/923) the oil and
   gas → refineries coefficient.** Already `not justified` on the B tracker; this
   adds the `L` symptom to the attribution one. Include the pipeline
   transportation → oil and gas cell in the same look.
3. **The remaining churners** — `811100`, `423A00`, `531HST`, `523900`,
   `423600`, `48A000` — re-read after 1 and 2, since all six carry an
   electricity share and much of their movement may go with it.

## Method

`A` is recovered from the cached `L` as `A = I − L⁻¹`, exact to 5.7e-16, so the
cell attribution needs no model build. The attribution is an exact **partition**
— contributions sum to the `L` effect, verified to 2.6e-13 — using the same
symmetric form as [`nd_drivers`](results/nd_drivers.py) with `d̄` replaced by
the current year's factors:

```
contribution of (i, j) to the L effect on c
    = ½ [ (B₂L₂)_i ΔA_ij (L₁)_jc + (B₂L₁)_i ΔA_ij (L₂)_jc ]
```

Both identities are asserted by `--check`.

Related: [`About_the_L_dollar_basis.md`](About_the_L_dollar_basis.md) (#937,
the basis), [`About_the_nd_driver_basis.md`](results/About_the_nd_driver_basis.md)
(#958), and row 10 of
[`B_driver_investigation.md`](../time_series_B_matrix/B_driver_investigation.md).
