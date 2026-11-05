# B driver investigation tracker

Sectors under review, their status, and the issues and PRs that hold the work.
Explanations go in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md), which
also keeps the findings from the earlier version of this tracker.

The list is every commodity with an IV score above 0.2, using spend weights for an
average spend-based organizational user, on GHG FBS `v1.0.0_79273ed` and MUT
`v1.0.0_5514c3b`. The spend file is not in the repo. See
[`About_IV_score.md`](About_IV_score.md).

**Driving factor** is how much of the movement in the commodity's `N_d` over
2017-2024 comes from `B` and how much from `L_dom`. Annual moves are summed as
absolute values, so moves in opposite directions don't cancel.

| Code | Sector | IV score | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|
| `221100` | Electric power generation, transmission, and distribution | 3.23 | B 93%, L 7% | unresolved | 89#0, #894, #898,#901, #1012 |
| `481000` | Air transportation | 0.40 | B 82%, L 18% | justified | #935 |
| `211000` | Oil and gas extraction | 0.35 | B 54%, L 46% | unresolved | #890, #922 |
| `484000` | Truck transportation | 0.31 | B 65%, L 35% | unresolved | #890 |
| `324110` | Petroleum refineries | 0.26 | B 55%, L 45% | unresolved | #984, #975, #997, #1177 |
| `325110` | Petrochemical manufacturing | 0.25 | B 60%, L 40% | unresolved | #997, #1177, #986, #1196 |
| `531ORE` | Other real estate | 0.23 | B 48%, L 52% | unresolved | #705, #1163, #975 |
| `325190` | Other basic organic chemical manufacturing | 0.23 | B 62%, L 38% | unresolved | #1177, #898, #1196 |
| `325211` | Plastics material and resin manufacturing | 0.21 | B 55%, L 45% | unresolved | #670, #1196 |

Status values:

| verdict | meaning |
|---|---|
| **justified** | a real movement in the world. Do not smooth; a smoothing change here would destroy signal. |
| **not justified** | movement with nothing underneath it. A defect, in scope, fix it. |
| **out of scope** | real driver, but this project has no lever on it. Hand over with the evidence. |
| **unresolved** | not yet investigated. Needs the source-level work before it can be ranked. |

