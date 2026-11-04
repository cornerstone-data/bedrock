# IV score

The IV score gives each commodity's emission factor `B` one number over
2017-2024. It is high when `B` moves, jumps or trends and when that `B` drives
the domestic total factors `N_d`. It selects the factors to check as real-world
change or as a modeling problem. It does not say why a factor moved;
`B_change_diagnostics.py` and `About_B_change_drivers.md` are the next step for a
flagged factor.

Code: `IV_score.py`. Results below use GHG FBS `v1.0.0_79273ed` and MUT
`v1.0.0_5514c3b`.

## Calculation

```
IV_j = I_j * V_j * 100

S_jk = B_j L_dom_jk / N_d_k           share of N_d_k from emissions at commodity j
N_d  = B L_dom, with L_dom = (I - Adom)^-1
I_j  = mean over years of sum_k w_k S_jk
m_j  = median over years of d_jt
V_j  = sqrt(mean over years of (d_jt - m_j)^2 + trend_weight * m_j^2)
d_jt = (B_jt - B_jt-1) / mean(|B_jt|, |B_jt-1|), B in constant 2017 dollars
```

**Importance, `I`.** `S_jk` is the percent change in `N_d_k` for a 1% change in
`B_j`, and it covers every `N_d` the factor reaches, not only its own. `S_jk`
sums to 1 over `j` for each `k`, so `I` sums to 1 over all commodities. `S` is
built on nominal `B` and `L_dom`. `w_k` sets how much each `N_d` counts and sums
to 1 each year.

**Why the domestic matrices.** A US `B` only applies to the domestic part of the
chain. The global model in `ceda` splits imported inputs by source country and
gives them those countries' factors, so running US `B` through imports with the
full `L` measures a path that users do not get. `IV_full_L` keeps the score on
the full `L` and `N` for comparison.

**Volatility, `V`.** `V` has two parts: movement around the median annual change,
and the median change itself, which is the trend. `--trend-weight` scales the
second part. At the default of 1, a steady change counts the same as year-to-year
noise of the same size. At 0, a steady trend scores 0. Because the trend is the
median, a single jump does not count as trend. `d_jt` stays finite when a factor
passes through zero. `V` has to be real, because a nominal factor falls whenever
prices rise. Test series give:

| `B` series | `V` | `V`, trend weight 0 |
|---|---|---|
| flat | 0 | 0 |
| steady -20% a year | 0.222 | 0 |
| +/-10% alternating | 0.157 | 0.125 |
| one +50% step | 0.151 | 0.151 |
| one +50% one-year spike | 0.214 | 0.214 |

The alternating series picks up a trend because its seven annual changes have one
more rise than fall, so the median is a rise.

**Unit.** Percentage points of weighted `N_d`. With equal weights, `IV_j` reads
as: in a typical year, the move in this factor shifts the average commodity's
`N_d` by this many percentage points. The score has no pass or fail threshold; it
is a ranking.

## Weighting the N values

The weights are open, to reflect which `N` values matter to users.

| `--weights` | `w_k` proportional to | Effect |
|---|---|---|
| `equal` (default) | 1 | each published factor counts once |
| `final_demand` | final demand met by domestic output, `y = L_dom^-1 q`, negatives set to 0 | large consumer-facing commodities count more |
| `output` | commodity output `q` | large commodities count more, including intermediates |
| `footprint` | `y * N_d` | reduces to the emissions share `B_j q_j / sum(B q)`, so `L_dom` drops out |

`--weights-csv` takes any other set, with columns `commodity` and `weight`, for
example a list of the `N` values users rely on most. `IV_score.csv` carries the
score under all four named weightings.

## Results, equal weights

The score is concentrated. The median is 0.006 across 405 commodities.

| Top commodities | Share of summed score |
|---|---|
| 1 | 26.4% |
| 10 | 47.2% |
| 30 | 66.9% |
| 100 | 85.6% |

The top 30 hold 74.4% of emissions on the `B q` basis. The top 15:

| Rank | Code | Commodity | IV | I | V | Trend per year | Peak year | Net change |
|---|---|---|---|---|---|---|---|---|
| 1 | 221100 | Electric power | 2.81 | 28.0% | 0.100 | -5.2% | 2021 | -0.26 |
| 2 | 2122A0 | Iron, gold, silver, other metal ore mining | 0.31 | 1.1% | 0.273 | -4.2% | 2022 | -0.70 |
| 3 | 211000 | Oil and gas extraction | 0.28 | 2.9% | 0.095 | -8.4% | 2020 | -0.48 |
| 4 | 325110 | Petrochemicals | 0.27 | 1.7% | 0.163 | +3.4% | 2019 | 0.30 |
| 5 | 331110 | Iron and steel mills | 0.27 | 4.5% | 0.059 | -2.3% | 2022 | -0.13 |
| 6 | 1111B0 | Grain farming | 0.26 | 2.8% | 0.091 | -0.7% | 2022 | 0.00 |
| 7 | 325190 | Other basic organic chemicals | 0.24 | 1.9% | 0.128 | +1.8% | 2021 | -0.26 |
| 8 | 325211 | Plastics and resin | 0.21 | 1.5% | 0.144 | +0.8% | 2021 | 0.47 |
| 9 | 481000 | Air transportation | 0.19 | 1.6% | 0.121 | -1.4% | 2020 | -0.11 |
| 10 | 212100 | Coal mining | 0.18 | 1.5% | 0.115 | -3.6% | 2020 | -0.03 |
| 11 | 324110 | Petroleum refineries | 0.16 | 2.0% | 0.083 | -2.9% | 2020 | -0.09 |
| 12 | 325120 | Industrial gases | 0.16 | 1.3% | 0.128 | -4.2% | 2018 | -0.31 |
| 13 | 325310 | Fertilizer | 0.15 | 0.9% | 0.178 | -0.2% | 2023 | -0.11 |
| 14 | 484000 | Truck transportation | 0.15 | 3.6% | 0.042 | -1.4% | 2021 | -0.03 |
| 15 | 531ORE | Other real estate | 0.14 | 1.1% | 0.128 | -8.2% | 2020 | -0.71 |

The trend is the median annual change, `median_change`. Net change is the sum of
`d_jt` over the span.

Each year's share of the squared, `I`-weighted movement around the median:

| | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|---|---|
| All commodities | 7.1% | 1.2% | 9.0% | 64.9% | 2.4% | 14.3% | 1.1% |
| Without electric power | 9.2% | 10.3% | 18.0% | 22.4% | 19.2% | 12.3% | 8.6% |

Electric power's 2021 move dominates the first row, so most of the 2021 share
belongs to one factor.

## Steady trends

The score counts a trend, but a steady trend in a low-importance factor still
ranks low. `IV_score_steady_trends.csv` ranks every commodity on the trend alone,
`IV_trend = I_j * |m_j| * 100`, so these can be justified as real change or
flagged. `years_with_trend` counts the years, of seven, moving the same way as
the median. The top of the list under equal weights:

| Code | Commodity | `IV_trend` | Trend per year | Years with trend |
|---|---|---|---|---|
| 221100 | Electric power | 1.46 | -5.2% | 5 |
| 211000 | Oil and gas extraction | 0.25 | -8.4% | 7 |
| 331110 | Iron and steel mills | 0.10 | -2.3% | 4 |
| 531ORE | Other real estate | 0.09 | -8.2% | 6 |
| 325110 | Petrochemicals | 0.06 | +3.4% | 5 |
| 324110 | Petroleum refineries | 0.06 | -2.9% | 6 |
| 1121A0 | Beef cattle ranching | 0.06 | -3.6% | 7 |
| 212100 | Coal mining | 0.06 | -3.6% | 4 |
| 325120 | Industrial gases | 0.05 | -4.2% | 5 |
| 322130 | Paperboard mills | 0.05 | +5.9% | 7 |

Counting the trend keeps 29 of the top 30 from a trend weight of 0. Oil and gas
extraction moves from rank 14 to 3, and beef cattle from 46 to 29.

## How much the basis matters

Electric power ranks first and 29 of the top 30 stay when the score is computed
on the full `L` and `N`, but scores change where the path runs through imports.
Oil and gas extraction and alumina fall to 59% of their full-`L` score, and metal
ore mining to 60%. Nonferrous metals leaves the top 30, from rank 26 to 141, and
waste combustors enters.

## How much the weighting matters

Electric power ranks first under all four weightings, and oil and gas extraction
ranks second under the three that are not equal.

| Weighting | Of the equal-weight top 30, kept | Entering the top 30 |
|---|---|---|
| `final_demand` | 22 | state and local government (education), depository credit, other financial investment, pipelines, motor vehicle dealers, nondepository credit, residential maintenance, lime and gypsum |
| `output` | 22 | depository credit, state and local government (education), other financial investment, warehousing, pipelines, nondepository credit, natural gas distribution, services to buildings |
| `footprint` | 24 | state and local government (education), pipelines, natural gas distribution, vegetable farming, fruit and tree nut farming, dairy cattle |

The commodities that leave are mostly small emitters whose rank came from their
own `N_d`, such as alumina, forestry and private households.

## Compared with the drivers note

`About_B_change_drivers.md` weights each factor by its own `N` only
(`delta_B_pct_of_N`, the `k = j` term). Summing that measure over the span on the
domestic basis and ranking on it keeps 18 of this top 30.

- **Enter under IV**, because they reach other commodities' `N_d`: other real
  estate (rank 141 on the own-`N_d` measure, 15 here), refineries (106, 11), truck
  transportation (103, 14), beef cattle (101, 29), iron and steel (80, 5), organic
  chemicals (51, 7), plastics (48, 8), oil and gas extraction (35, 3).
- **Leave under IV**, because they move their own `N_d` but little else: fishing
  (rank 4 on the own-`N_d` measure, 42 here), vegetable farming (5, 40), pulp
  mills (8, 39), schools (10, 52), storage batteries (16, 46).

The decomposition in the drivers note can be rerun for any factor flagged here.

## Limits

- **`N_d` leaves out the imported part of a footprint.** The published `N`
  includes it, and only `ceda` can measure it. A share of `N_d` therefore overstates
  a factor's importance to the full footprint of an import-heavy commodity. On
  bedrock's own matrices `N_d` is 79% of `N` on average, and 47% for computers and
  56% for apparel. That full `N` carries US factors on imports, so it is only a
  rough guide to the size of this gap.
- **A small commodity with no inputs can rank high under equal weights.** A
  commodity that buys nothing gets an own share of 1 at any size. Private
  households (`814000`) ranks 27 on 0.006 Mt, with a 133% move in its own `N_d` in
  2018. Its rank falls to 48, 78 and 391 under the three dollar or emissions
  weightings. Read `mean_emissions_Mt` alongside the score.
- **Averaging over all `N_d` dilutes a factor that moves one `N_d` a long way.**
  `largest_N_move_pct`, `_year` and `_target` give the largest single move in any
  `N_d` from one factor's annual change, with `L_dom` held at the prior year. Water
  transportation ranks 17 but moves its own `N_d` by 86% in 2021.
- **`jump_share` and `oscillation` describe the shape of the movement.**
  `jump_share` near 1 means one year dominates; `oscillation` near 1 means the
  series returns to where it started, and 0 means it moved one way. Both are
  measured around the median or on the annual changes, and neither depends on the
  trend weight.
- **The score covers `B` only.** Movement in `L_dom` is not scored.
- **Excluded emissions.** `F01000` (about 1,450 Mt) has no commodity output, so
  it has no `B`. `531110` (about 0.01 Mt) is not on the 405-commodity `B` axis.

## Run

```
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --check
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --weights final_demand
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --weights-csv my_weights.csv
uv run python -m bedrock.analysis.time_series_B_matrix.IV_score --trend-weight 0
```

The vintages come from the year configs, which must agree across the span. The
span and `L_dom` are cached under `output/` per FBS and MUT pair, and results go to
`output/IV_score/`.
