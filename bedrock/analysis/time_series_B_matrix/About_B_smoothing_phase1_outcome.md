# B Smoothing Phase 1: outcome

Phase 1 did not meet its acceptance test. Year-to-year movement in the direct
emission factors (`B`) is about where it was when the project started: the
mean over 2018-2024 of the median `abs_delta_B_pct_of_N` went from 0.616 to
0.646, and the share of commodities over the 5% gate from 8.9% to 8.8%.

The emissions side did become steadier. What changed is that emissions and
output now come from more independent data, so they no longer move together
and cancel in `E / x`. The project made the factors more accurate without
making them steadier overall. Movement did fall in several of the largest
industrial emitters: iron and steel, paper, cement, fertilizer, industrial
gases and natural gas distribution.

Figures and tables come from
[`phase1_outcome.py`](phase1_outcome.py), which reads three runs of
[`B_change_diagnostics.py`](B_change_diagnostics.py) (vintages at the end).

## What the project set out to do

The [plan](B_matrix_smoothing_plan.md) set two acceptance measures, both on
real-dollar factors, to be recorded before remediation and re-run after:

- the median `abs_delta_B_pct_of_N` per year, the change in a commodity's
  direct factor as a share of its total factor `N`;
- the share of commodities whose `abs_delta_B_pct_of_N` is over 5%.

The main remediation was the facility-reported basis for industrial
combustion ([#923](https://github.com/cornerstone-data/bedrock/issues/923),
[#929](https://github.com/cornerstone-data/bedrock/issues/929)). The case for
it ([#924](https://github.com/cornerstone-data/bedrock/pull/924)) projected
that it would reshuffle EPA table 3-11 combustion between sectors about a
quarter as much as the purchase-based basis did (0.24 times).

## Result against the acceptance measures

Three runs separate the two changes made over the project. The control has
the v0.5 tables and the purchase-based GHG FBS, so the step from the baseline
is the new tables and the other emissions changes; the step from the control
to v0.5 is the facility data.

| Measure, mean of 2018-2024 | Phase 1 baseline | v0.5 without facility data | v0.5 |
|---|---|---|---|
| Median `abs_delta_B_pct_of_N` (%) | 0.616 | 0.673 | 0.646 |
| Commodities over 5% (%) | 8.9 | 9.6 | 8.8 |

The new tables raised both measures and the facility data brought them back
down, to about the baseline. The 2021 peak is higher than at the start (median
1.47 against 1.13), and comes from the tables: 1.76 in the control.

![Acceptance measures by year](images/phase1_acceptance_by_year.png)

## Why the factors did not steady

The emissions side delivered part of what was projected. The sector shares of
table 3-11 combustion moved a median of 6.6 percentage points a year in v0.5,
against 10.5 at the baseline (0.63 times, where 0.24 was projected). In the
pandemic years the reduction is larger: 13.1 against 21.2 in 2021, and 6.8
against 12.1 in 2020. Across all industries, emissions move 10% less.

The direct factor is `E / x`, and its movement depends on how far `E` and `x`
move together as well as on how much each moves. Pooled over 2018-2024 and
weighted by emissions:

| Root-mean-square year-on-year change | Phase 1 baseline | v0.5 without facility data | v0.5 |
|---|---|---|---|
| Emissions `E` (%) | 10.35 | 9.99 | 9.34 |
| Real output `x` (%) | 8.85 | 9.09 | 9.14 |
| Correlation of the two | 0.46 | 0.36 | 0.34 |
| Direct factor `E / x` (%) | 10.06 | 10.78 | 10.64 |

Emissions move less and output slightly more, but the correlation between
them falls from 0.46 to 0.34, so less of their movement cancels and the
factor moves 6% more than at the start.

At the baseline, about 30% of emissions were placed on sectors by rows of the
Use table, and those emissions moved one-for-one with output (tracker row 12,
[#933](https://github.com/cornerstone-data/bedrock/issues/933)). The Use
table and output come from the same tables, so part of the baseline's
stability was emissions and output moving together. Placing combustion where
facilities reported it, and replacing carried 2017 Use cells with observed
ones, removes that link. Each side is closer to what was measured, and the two
sides now move more independently.

Two examples:

- Petroleum refineries. Year-to-year movement in the sector's emissions fell
  from 9.6% to 3.1% with the facility data. Its output moves 6.3% a year in the
  v0.5 tables and no longer moves with its emissions, so the factor still
  moves 5.1% a year.
- Electric power. Its emissions move 5.2% a year in all three runs.
  Output in the v0.5 tables moves 3.5% a year against 1.9% at the baseline, and
  the factor's movement rose from 5.6% to 7.5%. This change is in the tables,
  not the emissions.

![Emissions, output and the factor](images/phase1_E_x_comovement.png)

## Where movement fell

Industries with facility or sector-specific data (manufacturing, mining, oil
and gas, utilities other than electric power) moved 3% less than at the
baseline on the emissions-weighted mean, and 5% less than the control.
Including electric power, the group moved 17% more, driven by electric
power's output. By group, the median fell in utilities, other mining,
agriculture, government and manufacturing, and rose most in trade, where
factors are small.

![Change by sector group](images/phase1_churn_by_group.png)

Of the 40 largest-emitting sectors in 2024, 22 moved less at the end than at
the start. Those with the largest reductions, as the mean
`abs_delta_B_pct_of_N` over 2018-2024:

| Sector | 2024 emissions (Mt CO2e) | Phase 1 baseline | v0.5 | Change | Main step |
|---|---|---|---|---|---|
| 322120 Paper mills | 14.1 | 3.81 | 1.70 | -55% | facility data |
| 331110 Iron and steel mills | 55.9 | 4.14 | 2.61 | -37% | tables |
| 322130 Paperboard mills | 14.0 | 2.59 | 1.96 | -25% | facility data |
| 1111B0 Grain farming | 167.2 | 7.29 | 5.72 | -22% | soils ([#934](https://github.com/cornerstone-data/bedrock/issues/934)) |
| 327310 Cement | 61.1 | 5.99 | 4.80 | -20% | facility data |
| 112120 Dairy | 92.0 | 1.58 | 1.34 | -15% | facility build |
| 325310 Fertilizer | 41.3 | 14.36 | 12.48 | -13% | facility data |
| 221200 Natural gas distribution | 37.2 | 3.41 | 3.02 | -11% | tables |
| 325120 Industrial gases | 37.4 | 11.73 | 10.71 | -9% | facility data |

Greenhouse and nursery production (111400) fell 64%, 14.16 to 5.04, most of it
with the facility data: the combustion placed on it by the Use table, the part
that moves with its purchases, is about half what it was.

The largest increases were in electric power (+34%), state and local
government education and other services (+38% and +39%), truck transportation
(+24%), all from the tables, and petroleum refineries (+47%), in both steps. Pipelines (+19%),
oilseed farming (+42%) and wet corn milling (+50%) rose with the facility
data. Refineries' industry factor moved less with the facility data (above),
but the commodity measure did not follow it; that difference is unexplained.

![The 25 largest-emitting sectors](images/phase1_churn_top_sectors.png)

## Conclusions

1. The acceptance measures cannot tell real movement from noise. The data
   added in v0.5 is more independent and closer to what was measured, and it
   raised movement in `B` by about as much as the facility data removed.
   Measured against output that moves on its own, steadier emissions do not
   give a steadier factor.
2. The emissions-side remediation worked where it applied. Table 3-11 sector
   shares move a third less, and most of the largest facility-covered
   emitters, other than electric power and refineries, have steadier factors.
3. The remaining movement is mostly in the output and the tables (`x`, and
   `Vnorm` through the Make), which were out of scope for this project, and in
   sectors with no facility data: transportation, agriculture outside grain,
   and services.

## Phase 2 candidates

For mid-October to December, if taken up. All are on the
[B Smoothing Backlog](https://github.com/orgs/cornerstone-data/projects/35)
unless noted.

1. A better acceptance measure: score factor movement against independent
   physical series (fuel use, production volumes) so that real movement is not
   counted as a defect.
2. Rescore after the 2017-2023 facility FBS is rebuilt with #1074; the v0.5 run
   here uses local builds.
3. Output movement in electric power and refineries, which now drives their
   factors (not yet filed; nowcast side).
4. Refineries' commodity measure rising while the industry factor steadies:
   check `Vnorm` and the own-share weighting.
5. The Use-table vector that still places combustion outside facility scope
   ([#933](https://github.com/cornerstone-data/bedrock/issues/933),
   [#975](https://github.com/cornerstone-data/bedrock/issues/975),
   [#1064](https://github.com/cornerstone-data/bedrock/issues/1064)).
6. Transportation: air ([#935](https://github.com/cornerstone-data/bedrock/issues/935)),
   water ([#936](https://github.com/cornerstone-data/bedrock/issues/936), the
   largest mover at 19.8), road ([#917](https://github.com/cornerstone-data/bedrock/issues/917)).
7. The Make's share of movement in 2023
   ([#938](https://github.com/cornerstone-data/bedrock/issues/938)).
8. Facility-data follow-ups: GHGRP subparts booked as process
   ([#986](https://github.com/cornerstone-data/bedrock/issues/986)), refinery
   catalyst coke ([#984](https://github.com/cornerstone-data/bedrock/issues/984)),
   waste and pipelines ([#955](https://github.com/cornerstone-data/bedrock/issues/955)).

## Runs and vintages

| Run | Output folder | Cache folder | GHG FBS | MUT |
|---|---|---|---|---|
| Phase 1 baseline (v0.4 build, 2026-09-18) | `baseline_v0.4` | `cache` | `v0.3.0_99655e9` | `v0.3.0_4276083` |
| v0.5 without facility data | `v05_nonfac_full` | `cache_v05_nonfac` | `v0.3.0_0e1b0f0` | `v0.3.0_3096818` |
| v0.5 | `v05_1074_full` | `cache_v05_1074` | `v0.3.0_a7206ca` (facilities, #1074, local build) | `v0.3.0_3096818` |

All three cover 2017-2024 on 402 commodities. The emissions-weighted means
use the v0.5 emissions as weights in every run. `phase1_outcome.py --check`
verifies each cache's vintages and that the RMS figures above satisfy
rms(E/x)² = rms(E)² + rms(x)² − 2·cov.
