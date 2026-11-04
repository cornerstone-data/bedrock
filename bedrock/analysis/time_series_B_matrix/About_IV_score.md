# IV score

The IV score gives each commodity's emission factor `B` one number over
2017-2024. It is high when `B` moves or jumps and when that `B` drives the `N`
values. It selects the factors to check as real-world change or as a modeling
problem. It does not say why a factor moved; `B_change_diagnostics.py` and
`About_B_change_drivers.md` are the next step for a flagged factor.

Code: `IV_score.py`. Results below use GHG FBS `v1.0.0_79273ed` and MUT
`v1.0.0_5514c3b`.

## Calculation

```
IV_j = I_j * V_j * 100

S_jk = B_j L_jk / N_k                 share of N_k from emissions at commodity j
I_j  = mean over years of sum_k w_k S_jk
V_j  = RMS over years of (d_jt - median over years of d_jt)
d_jt = (B_jt - B_jt-1) / mean(|B_jt|, |B_jt-1|), B in constant 2017 dollars
```

**Importance, `I`.** `S_jk` is the percent change in `N_k` for a 1% change in
`B_j`, and it covers every `N` the factor reaches, not only its own. `S_jk` sums
to 1 over `j` for each `k`, so `I` sums to 1 over all commodities. `S` is built
on nominal `B` and `L`. `w_k` sets how much each `N` counts and sums to 1 each
year.

**Volatility, `V`.** `d_jt` stays finite when a factor passes through zero.
Removing the median change means a steady trend scores 0 and a single jump is not
treated as trend. `V` has to be real, because a nominal factor falls whenever
prices rise. Test series give:

| `B` series | `V` |
|---|---|
| flat, or a steady trend | 0 |
| +/-10% alternating | 0.125 |
| one +50% step | 0.151 |
| one +50% one-year spike | 0.214 |

**Unit.** Percentage points of weighted `N`. With equal weights, `IV_j` reads as:
in a typical year, the move in this factor shifts the average commodity's `N` by
this many percentage points. The score has no pass or fail threshold; it is a
ranking.

## Weighting the N values

The weights are open, to reflect which `N` values matter to users.

| `--weights` | `w_k` proportional to | Effect |
|---|---|---|
| `equal` (default) | 1 | each published factor counts once |
| `final_demand` | net final demand, `y = L^-1 q`, negatives set to 0 | large consumer-facing commodities count more |
| `output` | commodity output `q` | large commodities count more, including intermediates |
| `footprint` | `y * N` | reduces to the emissions share `B_j q_j / sum(B q)`, so `L` drops out |

`--weights-csv` takes any other set, with columns `commodity` and `weight`, for
example a list of the `N` values users rely on most. `IV_score.csv` carries the
score under all four named weightings.

## Results, equal weights

The score is concentrated. The median is 0.004 across 405 commodities.

| Top commodities | Share of summed score |
|---|---|
| 1 | 22.6% |
| 10 | 48.3% |
| 30 | 69.6% |
| 100 | 87.7% |

The top 30 hold 69.8% of emissions on the `B q` basis. The top 15:

| Rank | Code | Commodity | IV | I | V | Peak year | Net change |
|---|---|---|---|---|---|---|---|
| 1 | 221100 | Electric power | 2.21 | 25.8% | 0.086 | 2021 | -0.26 |
| 2 | 2122A0 | Iron, gold, silver, other metal ore mining | 0.51 | 1.9% | 0.270 | 2022 | -0.70 |
| 3 | 325110 | Petrochemicals | 0.32 | 2.0% | 0.159 | 2019 | 0.30 |
| 4 | 325190 | Other basic organic chemicals | 0.30 | 2.4% | 0.127 | 2021 | -0.26 |
| 5 | 1111B0 | Grain farming | 0.26 | 2.9% | 0.090 | 2022 | 0.00 |
| 6 | 331110 | Iron and steel mills | 0.26 | 4.8% | 0.054 | 2022 | -0.14 |
| 7 | 325211 | Plastics and resin | 0.23 | 1.6% | 0.143 | 2021 | 0.47 |
| 8 | 211000 | Oil and gas extraction | 0.22 | 5.0% | 0.045 | 2020 | -0.48 |
| 9 | 325310 | Fertilizer | 0.21 | 1.2% | 0.178 | 2023 | -0.11 |
| 10 | 481000 | Air transportation | 0.19 | 1.6% | 0.120 | 2020 | -0.11 |
| 11 | 212100 | Coal mining | 0.17 | 1.6% | 0.110 | 2020 | -0.03 |
| 12 | 331313 | Alumina and primary aluminum | 0.16 | 1.2% | 0.137 | 2021 | -0.11 |
| 13 | 324110 | Petroleum refineries | 0.16 | 2.0% | 0.078 | 2020 | -0.09 |
| 14 | 325120 | Industrial gases | 0.15 | 1.2% | 0.121 | 2018 | -0.32 |
| 15 | 327310 | Cement | 0.13 | 1.8% | 0.072 | 2022 | -0.17 |

Net change is the sum of `d_jt` over the span.

Each year's share of the squared, `I`-weighted movement:

| | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|---|---|
| All commodities | 7.3% | 1.7% | 9.3% | 61.7% | 4.1% | 14.3% | 1.7% |
| Without electric power | 9.3% | 9.4% | 16.2% | 21.4% | 22.2% | 13.0% | 8.6% |

Electric power's 2021 move dominates the first row, so most of the 2021 share
belongs to one factor.

## How much the weighting matters

Electric power ranks first under all four weightings, and air transportation
ranks second under the three that are not equal.

| Weighting | Of the equal-weight top 30, kept | Entering the top 30 |
|---|---|---|
| `final_demand` | 22 | state and local government (education), depository credit, other financial investment, pipelines, lime and gypsum, warehousing, motor vehicle dealers, natural gas distribution |
| `output` | 23 | the same, without motor vehicle dealers |
| `footprint` | 24 | state and local government (education), beef cattle, pipelines, natural gas distribution, vegetable farming, waste combustors |

The commodities that leave are mostly small emitters whose rank came from their
own `N`, such as nonferrous metals, alumina, and forestry.

## Compared with the drivers note

`About_B_change_drivers.md` weights each factor by its own `N` only
(`delta_B_pct_of_N`, the `k = j` term). Summing that measure over the span and
ranking on it keeps 21 of this top 30.

- **Enter under IV**, because they reach other commodities' `N`: refineries (rank
  116 on the own-`N` measure, 13 here), other real estate (122, 20), truck
  transportation (93, 17), nonferrous metals (87, 27), iron and steel (73, 6),
  plastics (58, 7), organic chemicals (50, 4). Solid waste landfilling (47, 25)
  and cement (33, 15) also enter, though most of their reach is their own `N`.
- **Leave under IV**, because they move their own `N` but little else: vegetable
  farming (rank 4 on the own-`N` measure, 38 here), waste combustors (6, 32),
  fishing (7, 41), pulp mills (8, 31), schools (11, 50).

The decomposition in the drivers note can be rerun for any factor flagged here.

## Limits

- **A small commodity with no inputs can rank high under equal weights.** A
  commodity that buys nothing gets an own share of 1 at any size. Private
  households (`814000`) ranks 24 on 0.006 Mt, with a 133% move in its own `N` in
  2018. Its rank falls to 44, 66 and 390 under the three dollar or emissions
  weightings. Read `mean_emissions_Mt` alongside the score.
- **A steady trend scores 0.** Oil and gas extraction falls every year (net
  change -0.48), and only the departures from its median change count. A steady
  decline can still be a modeling problem, so `net_change` is reported next to
  the score.
- **Averaging over all `N` dilutes a factor that moves one `N` a long way.**
  `largest_N_move_pct`, `_year` and `_target` give the largest single move in any
  `N` from one factor's annual change, with `L` held at the prior year. Water
  transportation ranks 18 but moves its own `N` by 81% in 2021.
- **`jump_share` and `oscillation` describe the shape of the movement.**
  `jump_share` near 1 means one year dominates; `oscillation` near 1 means the
  series returns to where it started, and 0 means it moved one way.
- **The score covers `B` only.** Movement in `L` is not scored.
- **Excluded emissions.** `F01000` (about 1,450 Mt) has no commodity output, so
  it has no `B`. `531110` (about 0.01 Mt) is not on the 405-commodity `B` axis.

## Run

```
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --check
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --weights final_demand
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --weights-csv my_weights.csv
```

The vintages come from the year configs, which must agree across the span. The
span is cached under `output/` per FBS and MUT pair, and results go to
`output/IV_score/`.
