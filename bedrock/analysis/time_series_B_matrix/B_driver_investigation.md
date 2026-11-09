# B driver investigation tracker

Sectors under review, their status, and the issues and PRs that hold the work.
Explanations go in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md), which
also keeps the findings from the earlier version of this tracker.

The list is every commodity with an IV score above 0.2, using spend weights for an
average spend-based organizational user, on GHG FBS `v1.0.0_edf6e0e` and MUT
`v1.0.0_b17908f`, with the v1.0 configs' electricity flags on (#1198, #1199, #1205).
Sectors that have dropped below 0.2 on these vintages stay at the end of the table
until their issues close. The spend file is not in the repo. See
[`About_IV_score.md`](About_IV_score.md).

**Driving factor** is how much of the movement in the commodity's `N_d` over
2017-2024 comes from `B` and how much from `L_dom`. Annual moves are summed as
absolute values, so moves in opposite directions don't cancel.

| Code | Sector | IV score | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|
| `221100` | Electric power generation, transmission, and distribution | 1.65 | B 87%, L 13% | justified: `N_d` and EIA's CO2 per kWh both end at 0.77 of 2017, and the 2021-23 swing in `q` is fixed (3.23 to 1.65). Open: `q` keeps BEA's 2017 level, which moves the factor's level and not its trend | [#890](https://github.com/cornerstone-data/bedrock/issues/890), [#894](https://github.com/cornerstone-data/bedrock/issues/894), [#898](https://github.com/cornerstone-data/bedrock/issues/898), [#901](https://github.com/cornerstone-data/bedrock/issues/901), [#1009](https://github.com/cornerstone-data/bedrock/issues/1009), [#1012](https://github.com/cornerstone-data/bedrock/issues/1012), [#1198](https://github.com/cornerstone-data/bedrock/issues/1198), [#1199](https://github.com/cornerstone-data/bedrock/issues/1199), [#1205](https://github.com/cornerstone-data/bedrock/issues/1205) |
| `481000` | Air transportation | 0.41 | B 83%, L 17% | justified | [#935](https://github.com/cornerstone-data/bedrock/issues/935) |
| `211000` | Oil and gas extraction | 0.35 | B 52%, L 48% | unresolved | [#890](https://github.com/cornerstone-data/bedrock/issues/890), [#922](https://github.com/cornerstone-data/bedrock/issues/922) |
| `484000` | Truck transportation | 0.30 | B 60%, L 40% | unresolved | [#890](https://github.com/cornerstone-data/bedrock/issues/890) |
| `325110` | Petrochemical manufacturing | 0.28 | B 66%, L 34% | mostly justified: `E` follows ethylene and output follows total product tonnes. Refineries' falling share adds about 8 points, and the 2021 step is not justified | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), [#1211](https://github.com/cornerstone-data/bedrock/issues/1211), [#1229](https://github.com/cornerstone-data/bedrock/issues/1229), [#997](https://github.com/cornerstone-data/bedrock/issues/997), [#1177](https://github.com/cornerstone-data/bedrock/issues/1177), [#986](https://github.com/cornerstone-data/bedrock/issues/986), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), [#1203](https://github.com/cornerstone-data/bedrock/issues/1203) |
| `324110` | Petroleum refineries | 0.25 | B 52%, L 48% | unresolved | [#984](https://github.com/cornerstone-data/bedrock/issues/984), [#975](https://github.com/cornerstone-data/bedrock/issues/975), [#997](https://github.com/cornerstone-data/bedrock/issues/997), [#1177](https://github.com/cornerstone-data/bedrock/issues/1177) |
| `531ORE` | Other real estate | 0.23 | B 43%, L 57% | unresolved | [#705](https://github.com/cornerstone-data/bedrock/issues/705), [#1163](https://github.com/cornerstone-data/bedrock/issues/1163), [#975](https://github.com/cornerstone-data/bedrock/issues/975) |
| `331110` | Iron and steel mills and ferroalloy manufacturing | 0.23 | B 48%, L 52% | justified: facility emissions and steel output support most of the decline. Open: coking coal line | [#1202](https://github.com/cornerstone-data/bedrock/issues/1202) |
| `325190` | Other basic organic chemical manufacturing | 0.18 | B 61%, L 39% | split, below 0.2: the N2O decline is justified. The fall in fossil routes and the 2024 output jump, which overshoots production reports, are not | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), [#1211](https://github.com/cornerstone-data/bedrock/issues/1211), [#1177](https://github.com/cornerstone-data/bedrock/issues/1177), [#898](https://github.com/cornerstone-data/bedrock/issues/898), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), [#1203](https://github.com/cornerstone-data/bedrock/issues/1203) |
| `325211` | Plastics material and resin manufacturing | 0.18 | B 59%, L 41% | split, below 0.2: the 2023 step is gone, and about a quarter of the rise is supported | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), [#670](https://github.com/cornerstone-data/bedrock/issues/670), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), [#1203](https://github.com/cornerstone-data/bedrock/issues/1203) |

Status values:

| verdict | meaning |
|---|---|
| **justified** | a real movement in the world. Do not smooth; a smoothing change here would destroy signal. |
| **not justified** | movement with nothing underneath it. A defect, in scope, fix it. |
| **out of scope** | real driver, but this project has no lever on it. Hand over with the evidence. |
| **unresolved** | not yet investigated. Needs the source-level work before it can be ranked. |
| **split** | part of the movement is justified and part is not. The row says which. |

