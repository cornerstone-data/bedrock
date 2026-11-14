# IV score

The IV score gives each commodity's emission factor `B` one number over
2017-2024. It is high when `B` moves, jumps or trends and when that `B` drives
the domestic total factors `N_d`. It selects the factors to check as real-world
change or as a modeling problem. It does not say why a factor moved;
[`About_sector_deep_dive.md`](About_sector_deep_dive.md) is the next step for a
flagged factor.

Code: `IV_score.py`. Results below use GHG FBS `v1.0.0_2b1164f` and MUT
`v1.0.0_2b1164f`, the v1.0 pin rebuild with #1303 / #1305 / #1306 / #1312 on.
How the ranking moved from earlier builds is under
[Change from the `e7715da` build](#change-from-the-e7715da-build) and
[Change from the `5514c3b` build](#change-from-the-5514c3b-build).

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

The score is concentrated. The median is 0.005 across 405 commodities. The
summed score is 8.28.

| Top commodities | Share of summed score |
|---|---|
| 1 | 17.1% |
| 10 | 39.5% |
| 30 | 61.6% |
| 100 | 83.2% |

The top 30 hold 76.7% of emissions on the `B q` basis. The top 15:

| Rank | Code | Commodity | IV | I | V | Trend per year | Peak year | Net change |
|---|---|---|---|---|---|---|---|---|
| 1 | 221100 | Electric power | 1.42 | 31.0% | 0.046 | -2.5% | 2021 | -0.26 |
| 2 | 331110 | Iron and steel mills | 0.26 | 4.6% | 0.057 | -1.7% | 2022 | -0.13 |
| 3 | 325110 | Petrochemicals | 0.26 | 1.7% | 0.154 | -0.2% | 2021 | 0.25 |
| 4 | 211000 | Oil and gas extraction | 0.25 | 2.8% | 0.088 | -7.9% | 2020 | -0.47 |
| 5 | 1111B0 | Grain farming | 0.25 | 2.7% | 0.090 | -0.9% | 2022 | 0.00 |
| 6 | 481000 | Air transportation | 0.18 | 1.5% | 0.118 | -1.3% | 2020 | -0.10 |
| 7 | 325190 | Other basic organic chemicals | 0.17 | 1.8% | 0.098 | -4.5% | 2021 | -0.20 |
| 8 | 212100 | Coal mining | 0.16 | 1.5% | 0.112 | -3.2% | 2020 | -0.04 |
| 9 | 324110 | Petroleum refineries | 0.16 | 1.9% | 0.082 | -2.7% | 2020 | -0.08 |
| 10 | 325211 | Plastics and resin | 0.15 | 1.7% | 0.088 | -0.2% | 2021 | 0.13 |
| 11 | 325120 | Industrial gases | 0.14 | 1.1% | 0.126 | -2.3% | 2020 | -0.29 |
| 12 | 531ORE | Other real estate | 0.13 | 1.1% | 0.119 | -6.9% | 2020 | -0.51 |
| 13 | 484000 | Truck transportation | 0.13 | 3.4% | 0.039 | -0.6% | 2019 | -0.05 |
| 14 | 562212 | Solid waste landfilling | 0.13 | 2.4% | 0.053 | -3.2% | 2023 | -0.22 |
| 15 | 327310 | Cement | 0.12 | 1.7% | 0.072 | -0.0% | 2022 | -0.17 |

The trend is the median annual change, `median_change`. Net change is the sum of
`d_jt` over the span.

Each year's share of the squared, `I`-weighted movement around the median:

| | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 |
|---|---|---|---|---|---|---|---|
| All commodities | 3.4% | 20.1% | 28.3% | 29.3% | 5.3% | 11.8% | 1.8% |
| Without electric power | 12.3% | 7.7% | 23.6% | 22.9% | 18.8% | 8.4% | 6.4% |

2021 is still the largest year; 2020 is close behind. Without electric power,
2020 leads and 2022 stays large.

## Steady trends

The score counts a trend, but a steady trend in a low-importance factor still
ranks low. `IV_score_steady_trends.csv` ranks every commodity on the trend alone,
`IV_trend = I_j * |m_j| * 100`, so these can be justified as real change or
flagged. `years_with_trend` counts the years, of seven, moving the same way as
the median. The top of the list under equal weights:

| Code | Commodity | `IV_trend` | Trend per year | Years with trend |
|---|---|---|---|---|
| 221100 | Electric power | 0.78 | -2.5% | 6 |
| 211000 | Oil and gas extraction | 0.22 | -7.9% | 7 |
| 325190 | Other basic organic chemicals | 0.08 | -4.5% | 4 |
| 531ORE | Other real estate | 0.08 | -6.9% | 5 |
| 562212 | Solid waste landfilling | 0.08 | -3.2% | 6 |
| 331110 | Iron and steel mills | 0.08 | -1.7% | 4 |
| 1121A0 | Beef cattle ranching | 0.06 | -3.7% | 7 |
| 322130 | Paperboard mills | 0.05 | +7.2% | 6 |
| 324110 | Petroleum refineries | 0.05 | -2.7% | 6 |
| 212100 | Coal mining | 0.05 | -3.2% | 4 |

Counting the trend keeps 28 of the top 30 from a trend weight of 0. Oil and gas
extraction moves from rank 16 to 4, and beef cattle from 46 to 26.

## How much the basis matters

Electric power ranks first and 28 of the top 30 stay when the score is computed
on the full `L` and `N`, but scores change where the path runs through imports.
Oil and gas extraction falls to 58% of its full-`L` score, alumina to 59% and
metal ore mining to 63%. Nonferrous metals leaves the top 30, from rank 29 to 158,
and fruit and tree nut farming from 28 to 33. Warehousing and waste combustors
enter.

## How much the weighting matters

Electric power ranks first under all four weightings, and oil and gas extraction
ranks second under the three that are not equal.

| Weighting | Of the equal-weight top 30, kept | Entering the top 30 |
|---|---|---|
| `final_demand` | 22 | state and local government (education), depository credit, other financial investment, funds and trusts, motor vehicle dealers, nondepository credit, residential maintenance, lime and gypsum |
| `output` | 23 | depository credit, state and local government (education), other financial investment, funds and trusts, nondepository credit, services to buildings, lime and gypsum |
| `footprint` | 23 | state and local government (education), vegetable farming, fruit and tree nut farming, dairy cattle, other animal production, mining support, natural gas distribution |

The commodities that leave are mostly small emitters whose rank came from their
own `N_d`, such as alumina, forestry and private households.

## Compared with ranking on a factor's own `N`

`B_change_diagnostics.py` reports `delta_B_pct_of_N`, which weights each factor by
its own `N` only, the `k = j` term. Summing that measure over the span on the
domestic basis and ranking on it keeps 18 of this top 30.

- **Enter under IV**, because they reach other commodities' `N_d`: electric power
  (rank 43 on the own-`N_d` measure, 1 here), other real estate (127, 13),
  paperboard mills (110, 27), refineries (101, 11), truck transportation (93, 14),
  beef cattle (90, 26), plastics (84, 9), iron and steel (81, 2), organic chemicals
  (63, 10).
- **Leave under IV**, because they move their own `N_d` but little else: fishing
  (rank 3 on the own-`N_d` measure, 36 here), vegetable farming (4, 35), pulp
  mills (5, 38), schools (7, 47), storage batteries (12, 44).

## Change from the `e7715da` build

Against GHG FBS / MUT `v1.0.0_e7715da` (#1249 / #1267), the equal-weight summed
score is 8.28 against 8.41, and 28 of the top 30 stay. The largest move:

| Code | Commodity | IV before | IV now | Rank before | Rank now | Why |
|---|---|---|---|---|---|---|
| 562212 | Solid waste landfilling | 0.22 | 0.13 | 6 | 14 | Calendar-2017 match_io waste weights (#1312) cut the 2017-2018 `B` jump. Peak year moves from 2022 to 2023; `V` falls from 0.089 to 0.053. The 2022 output step (#1269) remains. |

Most other top-15 scores move by a few hundredths. Share by year shifts toward
2020 and 2023 and away from 2021 and 2022.

## Change from the `5514c3b` build

The earlier results used GHG FBS `v1.0.0_79273ed` and MUT `v1.0.0_5514c3b`. On
the `e7715da` cut the summed score fell from 10.63 to 8.41, and 28 of the top 30
stayed. The largest moves on that cut:

| Code | Commodity | IV before | IV now | Rank before | Rank now | Why |
|---|---|---|---|---|---|---|
| 221100 | Electric power | 2.81 | 1.40 | 1 | 1 | Electricity is now split into generation and delivery, with output on EIA's level. `221100` is the generation part: $186bn of output in 2017, against $461bn before. Its real output no longer falls in 2021, so `V` falls from 0.100 to 0.045. |
| 2122A0 | Metal ore mining | 0.31 | 0.06 | 2 | 28 | Non-energy use emissions (UMD Table 3-14) no longer land on this commodity. They were 15.2 Mt in 2017 and swung between 4.9 and 17.8 Mt a year. Emissions in 2017 fall from 20.2 to 8.2 Mt. |
| 562212 | Solid waste landfilling | 0.09 | 0.22 | 21 | 6 | Emissions are unchanged. Industry output is now flat at about $9bn from 2017 to 2021 and steps up 25% in 2022, where it used to rise steadily. The waste sectors' output is now set from their Use; check the step in the deep dive. |
| 325190 | Other basic organic chemicals | 0.24 | 0.16 | 7 | 10 | 2024 real output is $126bn, against $136bn before, so the 2024 fall in `B` is smaller. |
| 325180 | Other basic inorganic chemicals | 0.11 | 0.04 | 18 | 39 | Output is unchanged. Emissions over 2017-2022 are now closer to the 2023-24 level, so the 2021 step is gone. |
| 324190 | Other petroleum and coal products | 0.08 | 0.04 | 23 | 42 | Output is unchanged. Emissions over 2017-2022 are now close to the 2023-24 level, so the 2023 step is gone. |

Not every difference comes from a methods change. The `5514c3b` build read
cached source tables built on older code, and the rebuild regenerated them.

## Limits

- **`N_d` leaves out the imported part of a footprint.** The published `N`
  includes it, and only `ceda` can measure it. A share of `N_d` therefore overstates
  a factor's importance to the full footprint of an import-heavy commodity. On
  bedrock's own matrices `N_d` is 79% of `N` on average, and 47% for computers and
  56% for apparel. That full `N` carries US factors on imports, so it is only a
  rough guide to the size of this gap.
- **A small commodity with no inputs can rank high under equal weights.** A
  commodity that buys nothing gets an own share of 1 at any size. Private
  households (`814000`) ranks 22 on 0.006 Mt, with a 133% move in its own `N_d` in
  2018. Its rank falls to 44, 77 and 391 under the three dollar or emissions
  weightings. Read `mean_emissions_Mt` alongside the score.
- **Averaging over all `N_d` dilutes a factor that moves one `N_d` a long way.**
  `largest_N_move_pct`, `_year` and `_target` give the largest single move in any
  `N_d` from one factor's annual change, with `L_dom` held at the prior year. Water
  transportation ranks 16 but moves its own `N_d` by 86% in 2021.
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
