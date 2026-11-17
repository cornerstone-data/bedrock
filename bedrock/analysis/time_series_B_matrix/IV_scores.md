# IV scores, spend weights

The `N_d` IV score for the 50 highest commodities, with spend weights for an
average spend-based organizational user, on GHG FBS `v1.0.0_2b1164f` and MUT
`v1.0.0_2b1164f`. [`About_IV_score.md`](About_IV_score.md) has the calculation.
The spend file is not in the repo.

The scores sum to 10.44: the spend-weighted average `N_d` moves
10.4 percentage points in a typical year. The top 50 hold
75.0% of that. Across all commodities, 32%
of the summed score comes from `B` and 68% from `L_dom`.

**Columns.**

| Column | Meaning |
|---|---|
| Weight | the commodity's spend weight |
| IV | `w * V * 100`, the commodity's part of the weighted average `N_d` movement |
| From `B`, from `L_dom` | the IV split by where the movement comes from, with each part's share. Annual moves are summed as absolute values, so moves in opposite directions don't cancel |
| Own `B` | share of the movement from the commodity's own `B` |
| Top `B` | the `B` that moves this `N_d` most, and its share of the movement from `B` |
| `N_d` volatility | `V` on the commodity's real `N_d` |
| Trend | the median annual change in real `N_d` |
| Emissions | mean Mt CO2e a year over 2017-2024 |
| `B` score rank | the commodity's rank on the `B` score under the same weights |

| Rank | Code | Commodity | Weight | IV | From `B` | From `L_dom` | Own `B` | Top `B` | `N_d` volatility | Trend | Peak year | Emissions | `B` score rank |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `511200` | Software publishers | 3.64% | 0.681 | 0.133 (19%) | 0.548 (81%) | 3% | `221100` Electricity generation (37%) | 0.187 | -12.7% | 2020 | 0.29 | 60 |
| 2 | `541800` | Advertising, public relations, and related services | 7.05% | 0.626 | 0.240 (38%) | 0.386 (62%) | 11% | `221100` Electricity generation (30%) | 0.089 | -6.8% | 2023 | 2.39 | 18 |
| 3 | `339112` | Surgical and medical instrument manufacturing | 3.77% | 0.439 | 0.106 (24%) | 0.333 (76%) | 10% | `221100` Electricity generation (24%) | 0.116 | -4.6% | 2020 | 0.33 | 29 |
| 4 | `48A000` | Scenic and sightseeing transportation and support activities | 2.01% | 0.423 | 0.072 (17%) | 0.351 (83%) | 5% | `221100` Electricity generation (24%) | 0.210 | -7.6% | 2021 | 0.82 | 53 |
| 5 | `5241XX` | Insurance carriers, except direct life | 2.42% | 0.346 | 0.091 (26%) | 0.255 (74%) | 8% | `221100` Electricity generation (25%) | 0.143 | -10.2% | 2024 | 0.82 | 44 |
| 6 | `541610` | Management consulting services | 2.85% | 0.316 | 0.082 (26%) | 0.233 (74%) | 7% | `221100` Electricity generation (24%) | 0.111 | -6.7% | 2021 | 0.76 | 39 |
| 7 | `325412` | Pharmaceutical preparation manufacturing | 1.21% | 0.279 | 0.035 (12%) | 0.244 (88%) | 6% | `221100` Electricity generation (30%) | 0.230 | -16.1% | 2023 | 1.88 | 61 |
| 8 | `5419A0` | All other miscellaneous professional, scientific, and technical services | 2.49% | 0.268 | 0.081 (30%) | 0.187 (70%) | 4% | `221100` Electricity generation (34%) | 0.108 | -8.9% | 2024 | 0.21 | 90 |
| 9 | `54151A` | Other computer related services, including facilities management | 1.76% | 0.267 | 0.080 (30%) | 0.187 (70%) | 11% | own (19%) | 0.151 | -14.0% | 2022 | 0.44 | 37 |
| 10 | `484000` | Truck transportation | 5.44% | 0.228 | 0.152 (67%) | 0.075 (33%) | 61% | own (63%) | 0.042 | -2.8% | 2021 | 167.27 | 4 |
| 11 | `334111` | Electronic computer manufacturing | 0.64% | 0.212 | 0.022 (10%) | 0.190 (90%) | 0% | `221100` Electricity generation (29%) | 0.331 | +9.1% | 2019 | 0.00 | 292 |
| 12 | `561300` | Employment services | 1.49% | 0.203 | 0.042 (20%) | 0.162 (80%) | 1% | `221100` Electricity generation (26%) | 0.136 | -7.9% | 2021 | 0.19 | 171 |
| 13 | `541100` | Legal services | 1.80% | 0.183 | 0.065 (36%) | 0.117 (64%) | 2% | `221100` Electricity generation (34%) | 0.101 | -9.2% | 2024 | 0.26 | 156 |
| 14 | `336991` | Motorcycle, bicycle, and parts manufacturing | 0.34% | 0.167 | 0.007 (4%) | 0.160 (96%) | 4% | `221100` Electricity generation (27%) | 0.491 | +8.4% | 2019 | 0.03 | 123 |
| 15 | `519130` | Internet publishing and broadcasting and Web search portals | 1.16% | 0.157 | 0.036 (23%) | 0.121 (77%) | 5% | `221100` Electricity generation (35%) | 0.135 | -6.8% | 2018 | 0.08 | 82 |
| 16 | `315000` | Apparel manufacturing | 0.75% | 0.156 | 0.059 (38%) | 0.097 (62%) | 41% | own (61%) | 0.207 | -8.2% | 2021 | 0.28 | 16 |
| 17 | `334413` | Semiconductor and related device manufacturing | 1.68% | 0.152 | 0.060 (40%) | 0.092 (60%) | 25% | `221100` Electricity generation (43%) | 0.090 | -7.9% | 2022 | 5.67 | 23 |
| 18 | `541512` | Computer systems design services | 0.97% | 0.151 | 0.048 (32%) | 0.103 (68%) | 15% | own (26%) | 0.155 | -14.3% | 2022 | 2.28 | 41 |
| 19 | `522A00` | Nondepository credit intermediation and related activities | 0.83% | 0.142 | 0.048 (34%) | 0.094 (66%) | 20% | own (32%) | 0.171 | -14.5% | 2018 | 3.42 | 36 |
| 20 | `512100` | Motion picture and video industries | 1.25% | 0.134 | 0.039 (29%) | 0.095 (71%) | 4% | `221100` Electricity generation (37%) | 0.107 | -6.0% | 2018 | 0.27 | 109 |
| 21 | `561400` | Business support services | 1.86% | 0.125 | 0.057 (46%) | 0.068 (54%) | 6% | `221100` Electricity generation (31%) | 0.067 | -4.9% | 2020 | 0.42 | 104 |
| 22 | `518200` | Data processing, hosting, and related services | 0.93% | 0.112 | 0.044 (39%) | 0.068 (61%) | 10% | `221100` Electricity generation (44%) | 0.120 | -9.7% | 2024 | 1.56 | 58 |
| 23 | `334118` | Computer terminals and other computer peripheral equipment manufacturing | 0.75% | 0.109 | 0.020 (18%) | 0.089 (82%) | 13% | own (28%) | 0.145 | -2.9% | 2018 | 0.08 | 65 |
| 24 | `337122` | Nonupholstered wood household furniture manufacturing | 0.35% | 0.105 | 0.010 (9%) | 0.096 (91%) | 13% | own (37%) | 0.301 | -6.5% | 2023 | 0.07 | 86 |
| 25 | `523A00` | Securities and commodity contracts intermediation and brokerage | 1.25% | 0.103 | 0.049 (47%) | 0.055 (53%) | 32% | own (30%) | 0.083 | -1.4% | 2020 | 1.29 | 32 |
| 26 | `324110` | Petroleum refineries | 1.48% | 0.099 | 0.050 (50%) | 0.049 (50%) | 24% | `211000` Oil and gas extraction (38%) | 0.067 | +0.6% | 2023 | 218.06 | 5 |
| 27 | `492000` | Couriers and messengers | 1.61% | 0.094 | 0.045 (48%) | 0.049 (52%) | 44% | own (44%) | 0.058 | -3.8% | 2020 | 12.71 | 22 |
| 28 | `334418` | Printed circuit assembly (electronic assembly) manufacturing | 0.67% | 0.092 | 0.028 (31%) | 0.063 (69%) | 15% | own (30%) | 0.136 | -11.2% | 2021 | 0.13 | 56 |
| 29 | `561900` | Other support services | 1.88% | 0.091 | 0.052 (57%) | 0.039 (43%) | 12% | `221100` Electricity generation (31%) | 0.048 | -3.8% | 2020 | 0.32 | 85 |
| 30 | `541200` | Accounting, tax preparation, bookkeeping, and payroll services | 0.68% | 0.082 | 0.025 (31%) | 0.057 (69%) | 8% | `221100` Electricity generation (26%) | 0.120 | -9.5% | 2022 | 0.56 | 101 |
| 31 | `325310` | Fertilizer manufacturing | 0.56% | 0.081 | 0.065 (80%) | 0.016 (20%) | 79% | own (91%) | 0.145 | -5.7% | 2022 | 39.43 | 12 |
| 32 | `541511` | Custom computer programming services | 0.59% | 0.078 | 0.022 (28%) | 0.056 (72%) | 14% | own (23%) | 0.131 | -12.1% | 2022 | 1.04 | 84 |
| 33 | `52A000` | Monetary authorities and depository credit intermediation | 0.39% | 0.075 | 0.023 (31%) | 0.052 (69%) | 24% | own (44%) | 0.192 | -13.6% | 2023 | 6.81 | 26 |
| 34 | `541300` | Architectural, engineering, and related services | 0.75% | 0.072 | 0.022 (30%) | 0.050 (70%) | 6% | `221100` Electricity generation (22%) | 0.096 | -5.4% | 2022 | 2.91 | 97 |
| 35 | `335999` | All other miscellaneous electrical equipment and component manufacturing | 1.10% | 0.071 | 0.028 (39%) | 0.044 (61%) | 6% | `221100` Electricity generation (39%) | 0.065 | -0.4% | 2024 | 0.14 | 137 |
| 36 | `334516` | Analytical laboratory instrument manufacturing | 0.42% | 0.071 | 0.010 (14%) | 0.061 (86%) | 7% | `221100` Electricity generation (27%) | 0.169 | -8.1% | 2023 | 0.12 | 140 |
| 37 | `621400` | Outpatient care centers | 0.70% | 0.071 | 0.028 (40%) | 0.043 (60%) | 6% | `221100` Electricity generation (41%) | 0.101 | -8.3% | 2020 | 1.07 | 162 |
| 38 | `33441A` | Other electronic component manufacturing | 0.96% | 0.070 | 0.022 (32%) | 0.047 (68%) | 6% | `221100` Electricity generation (34%) | 0.072 | -3.8% | 2024 | 0.12 | 111 |
| 39 | `481000` | Air transportation | 0.78% | 0.069 | 0.058 (83%) | 0.012 (17%) | 83% | own (90%) | 0.089 | -0.9% | 2021 | 156.00 | 2 |
| 40 | `314900` | Other textile product mills | 0.87% | 0.069 | 0.016 (24%) | 0.053 (76%) | 16% | `221100` Electricity generation (25%) | 0.079 | -4.6% | 2020 | 0.35 | 72 |
| 41 | `541700` | Scientific research and development services | 1.01% | 0.062 | 0.029 (46%) | 0.033 (54%) | 3% | `221100` Electricity generation (27%) | 0.062 | -2.4% | 2020 | 1.21 | 205 |
| 42 | `721000` | Accommodation | 0.42% | 0.061 | 0.016 (26%) | 0.045 (74%) | 9% | `221100` Electricity generation (46%) | 0.145 | -6.2% | 2022 | 4.72 | 73 |
| 43 | `339113` | Surgical appliance and supplies manufacturing | 0.48% | 0.059 | 0.014 (23%) | 0.045 (77%) | 2% | `221100` Electricity generation (26%) | 0.123 | -6.5% | 2020 | 0.16 | 223 |
| 44 | `517210` | Wireless telecommunications carriers (except satellite) | 0.47% | 0.059 | 0.015 (25%) | 0.044 (75%) | 3% | `221100` Electricity generation (32%) | 0.126 | -7.6% | 2019 | 0.51 | 184 |
| 45 | `561200` | Facilities support services | 0.99% | 0.059 | 0.029 (49%) | 0.030 (51%) | 13% | `327400` Lime and gypsum product manufacturing (21%) | 0.059 | -5.2% | 2024 | 0.89 | 89 |
| 46 | `326110` | Plastics packaging materials and unlaminated film and sheet manufacturing | 0.88% | 0.058 | 0.029 (50%) | 0.029 (50%) | 8% | `221100` Electricity generation (21%) | 0.066 | -3.0% | 2021 | 1.88 | 70 |
| 47 | `333318` | Other commercial and service industry machinery manufacturing | 0.79% | 0.055 | 0.017 (31%) | 0.038 (69%) | 18% | `221100` Electricity generation (23%) | 0.069 | -4.5% | 2023 | 0.64 | 87 |
| 48 | `493000` | Warehousing and storage | 0.46% | 0.053 | 0.024 (44%) | 0.030 (56%) | 43% | own (57%) | 0.116 | -7.4% | 2022 | 16.81 | 19 |
| 49 | `325414` | Biological product (except diagnostic) manufacturing | 0.17% | 0.052 | 0.007 (14%) | 0.045 (86%) | 7% | own (30%) | 0.307 | -20.1% | 2023 | 0.63 | 131 |
| 50 | `325620` | Toilet preparation manufacturing | 0.32% | 0.052 | 0.008 (16%) | 0.044 (84%) | 13% | own (22%) | 0.163 | -13.2% | 2023 | 1.00 | 122 |
