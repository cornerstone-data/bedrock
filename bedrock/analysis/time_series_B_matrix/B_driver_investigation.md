# B driver investigation tracker

Sectors under review, their status, and the issues and PRs that hold the work.
Explanations go in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md), which
also keeps the findings from the earlier version of this tracker.

The list is every commodity with an IV score above 0.2, using spend weights for an
average spend-based organizational user, on GHG FBS `v1.0.0_e7715da` and MUT
`v1.0.0_e7715da`, the combined 2017-2024 rebuild (#1249). Statuses were set on
earlier builds (`edf6e0e`/`b17908f` and before) and are kept as they were.

**IV score** and **Volatility score** read initial → current. Initial is the first
run this tracker was built from, on GHG FBS `v1.0.0_79273ed` and MUT
`v1.0.0_5514c3b`; current is `v1.0.0_e7715da`. The volatility score is `V` in
[`About_IV_score.md`](About_IV_score.md): the year-to-year movement in the real
factor, with its trend.
Sectors that have dropped below 0.2 on these vintages stay at the end of the table
until their issues close. The spend file is not in the repo. See
[`About_IV_score.md`](About_IV_score.md).

**Driving factor** is how much of the movement in the commodity's `N_d` over
2017-2024 comes from `B` and how much from `L_dom`. Annual moves are summed as
absolute values, so moves in opposite directions don't cancel.

| Code | Sector | IV score | Volatility score | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|---|
| `221100` | Electric power generation, transmission, and distribution | 3.23 → 1.53 | 0.100 → 0.045 | B 82%, L 18% | justified: `N_d` and EIA's CO2 per kWh both end at 0.77 of 2017, and the 2021-23 swing in `q` is fixed (3.23 to 1.65). Open: `q` keeps BEA's 2017 level, which moves the factor's level and not its trend | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~, ~~[#894](https://github.com/cornerstone-data/bedrock/issues/894)~~, ~~[#898](https://github.com/cornerstone-data/bedrock/issues/898)~~, ~~[#901](https://github.com/cornerstone-data/bedrock/issues/901)~~, ~~[#1009](https://github.com/cornerstone-data/bedrock/issues/1009)~~, ~~[#1012](https://github.com/cornerstone-data/bedrock/issues/1012)~~, ~~[#1198](https://github.com/cornerstone-data/bedrock/issues/1198)~~, ~~[#1199](https://github.com/cornerstone-data/bedrock/issues/1199)~~, ~~[#1205](https://github.com/cornerstone-data/bedrock/issues/1205)~~ |
| `481000` | Air transportation | 0.40 → 0.40 | 0.121 → 0.119 | B 82%, L 18% | justified | ~~[#935](https://github.com/cornerstone-data/bedrock/issues/935)~~ |
| `211000` | Oil and gas extraction | 0.35 → 0.32 | 0.095 → 0.089 | B 51%, L 49% | Direct/`B` justified; `L` unresolved: flat GHGI `E`, GO matches EIA crude+gas $, real `q` up with volumes. Open: own-use 2021-22 | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~, ~~[#922](https://github.com/cornerstone-data/bedrock/issues/922)~~ |
| `562212` | Solid waste landfilling | 0.11 → 0.28 | 0.045 → 0.089 | B 96%, L 4% | unresolved | none yet |
| `484000` | Truck transportation | 0.31 → 0.28 | 0.042 → 0.039 | B 54%, L 46% | unresolved | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~ |
| `324110` | Petroleum refineries | 0.26 → 0.24 | 0.083 → 0.081 | B 48%, L 52% | unresolved | [#984](https://github.com/cornerstone-data/bedrock/issues/984), [#975](https://github.com/cornerstone-data/bedrock/issues/975), [#997](https://github.com/cornerstone-data/bedrock/issues/997), ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~ |
| `325110` | Petrochemical manufacturing | 0.25 → 0.24 | 0.163 → 0.154 | B 61%, L 39% | mostly justified: `E` follows ethylene and output follows total product tonnes. Refineries' falling share adds about 8 points, and the 2021 step is not justified | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), ~~[#1211](https://github.com/cornerstone-data/bedrock/issues/1211)~~, ~~[#1229](https://github.com/cornerstone-data/bedrock/issues/1229)~~, [#997](https://github.com/cornerstone-data/bedrock/issues/997), ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~, [#986](https://github.com/cornerstone-data/bedrock/issues/986), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |
| `531ORE` | Other real estate | 0.23 → 0.23 | 0.128 → 0.128 | B 51%, L 49% | unresolved | [#705](https://github.com/cornerstone-data/bedrock/issues/705), ~~[#1163](https://github.com/cornerstone-data/bedrock/issues/1163)~~, [#975](https://github.com/cornerstone-data/bedrock/issues/975) |
| `331110` | Iron and steel mills and ferroalloy manufacturing | 0.16 → 0.17 | 0.059 → 0.058 | B 38%, L 62% | justified: facility emissions and steel output support most of the decline. Open: coking coal line | ~~[#1202](https://github.com/cornerstone-data/bedrock/issues/1202)~~ |
| `325211` | Plastics material and resin manufacturing | 0.21 → 0.16 | 0.144 → 0.093 | B 56%, L 44% | split, below 0.2: the 2023 step is gone, and about a quarter of the rise is supported | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), [#670](https://github.com/cornerstone-data/bedrock/issues/670), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |
| `325190` | Other basic organic chemical manufacturing | 0.23 → 0.16 | 0.128 → 0.091 | B 57%, L 43% | split, below 0.2: the N2O decline is justified. The fall in fossil routes and the 2024 output jump, which overshoots production reports, are not | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), ~~[#1211](https://github.com/cornerstone-data/bedrock/issues/1211)~~, ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~, ~~[#898](https://github.com/cornerstone-data/bedrock/issues/898)~~, [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |

Status values:

| verdict | meaning |
|---|---|
| **justified** | a real movement in the world. Do not smooth; a smoothing change here would destroy signal. |
| **not justified** | movement with nothing underneath it. A defect, in scope, fix it. |
| **out of scope** | real driver, but this project has no lever on it. Hand over with the evidence. |
| **unresolved** | not yet investigated. Needs the source-level work before it can be ranked. |
| **split** | part of the movement is justified and part is not. The row says which. |

