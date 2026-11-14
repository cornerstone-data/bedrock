# B driver investigation tracker

Sectors under review, their status, and the issues and PRs that hold the work.
Explanations go in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md), which
also keeps the findings from the earlier version of this tracker.

The list is every commodity with an IV score above 0.2, using spend weights for an
average spend-based organizational user, on GHG FBS `v1.0.0_2b1164f` and MUT
`v1.0.0_2b1164f`, the v1.0 pin rebuild with the services seed held at its last SAS
year (#1306), commercial kWh divided on survey electricity or other inputs
(#1305), summary trade holds (#1303), and calendar-2017 match_io waste weights
(#1312). Statuses were set on earlier builds (`e7715da`, `edf6e0e`/`b17908f` and
before) and are kept as they were.

**IV score** and **Volatility score** read initial → current. Initial is the first
run this tracker was built from, on GHG FBS `v1.0.0_79273ed` and MUT
`v1.0.0_5514c3b`; current is `v1.0.0_2b1164f`. Against `fd271dc`, the prior v1.0
pin, the spend-weighted IV scores sum to 7.76 against 7.97, with the same top 30;
solid waste landfilling (`562212`) drops below 0.2 after #1312. The volatility
score is `V` in [`About_IV_score.md`](About_IV_score.md): the year-to-year
movement in the real factor, with its trend.
Sectors that have dropped below 0.2 on these vintages stay at the end of the table
until their issues close. The spend file is not in the repo. See
[`About_IV_score.md`](About_IV_score.md).

**N_d volatility** is the same measure on the commodity's total domestic factor
`N_d`, in constant 2017 dollars, and reads initial → current like the scores. It
is the factor users get, so it is the one to steady. From the first run to now, the
median across all commodities goes from 0.089 to 0.081, and across the 142
commercial-class commodities (non-industrial two-digit prefixes) from 0.114 to
0.094. The volatility score only sees a commodity's own emissions: a service
sector's `B` is its own fuel and refrigerants, and what it buys, electricity above
all, moves only its `N_d`. Fixes to what services buy (#1305, #1306) show up here
and not in the volatility score: across those commercial-class commodities, the
median `N_d` volatility is 0.094 against 0.104 on `fd271dc`, lower for 102 and
higher for 37. Warehousing goes from 0.135 to 0.116, automotive repair from 0.168
to 0.090, cable programming from 0.169 to 0.132.

**Driving factor** is how much of the movement in the commodity's `N_d` over
2017-2024 comes from `B` and how much from `L_dom`. Annual moves are summed as
absolute values, so moves in opposite directions don't cancel.

| Code | Sector | IV score | Volatility score | N_d volatility | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|---|---|
| `221100` | Electricity generation | 3.23 → 1.55 | 0.100 → 0.046 | **0.105** → 0.055 | B 84%, L 16% | justified | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~, ~~[#894](https://github.com/cornerstone-data/bedrock/issues/894)~~, ~~[#898](https://github.com/cornerstone-data/bedrock/issues/898)~~, ~~[#901](https://github.com/cornerstone-data/bedrock/issues/901)~~, ~~[#1009](https://github.com/cornerstone-data/bedrock/issues/1009)~~, ~~[#1012](https://github.com/cornerstone-data/bedrock/issues/1012)~~, ~~[#1198](https://github.com/cornerstone-data/bedrock/issues/1198)~~, ~~[#1199](https://github.com/cornerstone-data/bedrock/issues/1199)~~, ~~[#1205](https://github.com/cornerstone-data/bedrock/issues/1205)~~ |
| `481000` | Air transportation | 0.40 → 0.39 | **0.121** → **0.118** | **0.101** → 0.089 | B 83%, L 17% | justified | ~~[#935](https://github.com/cornerstone-data/bedrock/issues/935)~~ |
| `211000` | Oil and gas extraction | 0.35 → 0.31 | 0.095 → 0.088 | 0.083 → 0.091 | B 50%, L 50% | Direct/`B` justified; `L` unresolved: flat GHGI `E`, GO matches EIA crude+gas $, real `q` up with volumes. Open: own-use 2021-22 | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~, ~~[#922](https://github.com/cornerstone-data/bedrock/issues/922)~~ |
| `484000` | Truck transportation | 0.31 → 0.28 | 0.042 → 0.039 | 0.041 → 0.042 | B 66%, L 34% | justified: `B` flat (within 5% of 2017, ends 2% lower); own use fixed in #890 (3.33 to 4.50 per $100, no 2023 step). Open: `L` (34%); 2021 diesel rise | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~ |
| `324110` | Petroleum refineries | 0.26 → 0.25 | 0.083 → 0.082 | 0.063 → 0.067 | B 50%, L 50% | unresolved | [#984](https://github.com/cornerstone-data/bedrock/issues/984), [#975](https://github.com/cornerstone-data/bedrock/issues/975), [#997](https://github.com/cornerstone-data/bedrock/issues/997), ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~ |
| `325110` | Petrochemical manufacturing | 0.25 → 0.24 | **0.163** → **0.154** | **0.157** → **0.151** | B 63%, L 37% | mostly justified: `E` follows ethylene and output follows total product tonnes. Open: 2024 refinery product-share fall (33% to 25%); 2021 non-energy step | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), ~~[#1211](https://github.com/cornerstone-data/bedrock/issues/1211)~~, ~~[#1229](https://github.com/cornerstone-data/bedrock/issues/1229)~~, [#997](https://github.com/cornerstone-data/bedrock/issues/997), ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~, [#986](https://github.com/cornerstone-data/bedrock/issues/986), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |
| `531ORE` | Other real estate | 0.23 → 0.22 | **0.128** → **0.119** | 0.093 → 0.084 | B 49%, L 51% | unresolved | [#705](https://github.com/cornerstone-data/bedrock/issues/705), ~~[#1163](https://github.com/cornerstone-data/bedrock/issues/1163)~~, [#975](https://github.com/cornerstone-data/bedrock/issues/975) |
| `325190` | Other basic organic chemical manufacturing | 0.23 → 0.17 | **0.128** → 0.098 | 0.066 → 0.050 | B 59%, L 41% | split, below 0.2: adipic acid N2O decline is justified. Open: fossil allocation fall; 2024 output rise vs production reports | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), ~~[#1211](https://github.com/cornerstone-data/bedrock/issues/1211)~~, ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~, ~~[#898](https://github.com/cornerstone-data/bedrock/issues/898)~~, [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |
| `331110` | Iron and steel mills and ferroalloy manufacturing | 0.16 → 0.16 | 0.059 → 0.057 | 0.091 → 0.090 | B 37%, L 63% | justified: facility emissions and crude steel match the 12% `B` fall; coking coal is steady (7.5 to 6.4 Mt). Open: `L` (63%); gas vs direct boundary | ~~[#1202](https://github.com/cornerstone-data/bedrock/issues/1202)~~ |
| `562212` | Solid waste landfilling | 0.11 → 0.16 | 0.045 → 0.053 | 0.050 → 0.053 | B 99%, L 1% | below 0.2 after calendar-2017 waste weights (#1312); unresolved: `E` flat; remaining `B` moves with `q` (2022 step). See #1269 | [#1269](https://github.com/cornerstone-data/bedrock/issues/1269), ~~[#1312](https://github.com/cornerstone-data/bedrock/pull/1312)~~ |
| `325211` | Plastics material and resin manufacturing | 0.21 → 0.15 | **0.144** → 0.088 | **0.117** → 0.090 | B 57%, L 43% | split, below 0.2: under a third of the rise is supported on facility emissions without adipic acid; non-energy use drives the rest | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), [#670](https://github.com/cornerstone-data/bedrock/issues/670), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |

### The rest of the top 30

The other commodities in the current top 30, ordered by current IV score, on the
same builds and weights as the table above. None has been assessed on these builds.
Grain farming and water transportation have earlier findings in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md).

| Code | Sector | IV score | Volatility score | N_d volatility | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|---|---|
| `325310` | Fertilizer manufacturing | 0.18 → 0.14 | **0.178** → **0.144** | **0.169** → **0.145** | B 81%, L 19% | unresolved | none yet |
| `1111B0` | Grain farming | 0.13 → 0.12 | 0.091 → 0.090 | **0.110** → **0.107** | B 74%, L 26% | unresolved | ~~[#934](https://github.com/cornerstone-data/bedrock/issues/934)~~ |
| `485000` | Transit and ground passenger transportation | 0.14 → 0.12 | **0.120** → **0.101** | **0.117** → 0.076 | B 59%, L 41% | unresolved | none yet |
| `212100` | Coal mining | 0.11 → 0.11 | **0.115** → **0.112** | 0.097 → 0.099 | B 89%, L 11% | unresolved | none yet |
| `315000` | Apparel manufacturing | 0.11 → 0.10 | **0.535** → **0.540** | **0.189** → **0.207** | B 37%, L 63% | unresolved | none yet |
| `325120` | Industrial gas manufacturing | 0.12 → 0.10 | **0.128** → **0.126** | **0.106** → 0.095 | B 71%, L 29% | unresolved | none yet |
| `541800` | Advertising, public relations, and related services | 0.11 → 0.10 | **0.156** → **0.146** | **0.112** → 0.089 | B 38%, L 62% | unresolved | none yet |
| `493000` | Warehousing and storage | 0.09 → 0.09 | 0.100 → **0.102** | **0.138** → **0.116** | B 43%, L 57% | unresolved | none yet |
| `483000` | Water transportation | 0.08 → 0.08 | **0.311** → **0.312** | **0.258** → **0.255** | B 89%, L 11% | unresolved | [#936](https://github.com/cornerstone-data/bedrock/issues/936) |
| `532100` | Automotive equipment rental and leasing | 0.07 → 0.07 | **0.217** → **0.220** | **0.150** → **0.148** | B 65%, L 35% | unresolved | none yet |
| `492000` | Couriers and messengers | 0.08 → 0.07 | 0.064 → 0.059 | 0.067 → 0.058 | B 47%, L 53% | unresolved | none yet |
| `334413` | Semiconductor and related device manufacturing | 0.08 → 0.07 | 0.071 → 0.075 | **0.100** → 0.090 | B 40%, L 60% | unresolved | none yet |
| `322130` | Paperboard mills | 0.06 → 0.07 | 0.069 → 0.083 | 0.069 → 0.065 | B 23%, L 77% | unresolved | none yet |
| `327310` | Cement manufacturing | 0.07 → 0.07 | 0.072 → 0.072 | 0.062 → 0.061 | B 90%, L 10% | unresolved | none yet |
| `52A000` | Monetary authorities and depository credit intermediation | 0.06 → 0.06 | **0.247** → **0.231** | **0.205** → **0.192** | B 31%, L 69% | unresolved | none yet |
| `327400` | Lime and gypsum product manufacturing | 0.06 → 0.06 | 0.062 → 0.062 | 0.055 → 0.053 | B 81%, L 19% | unresolved | none yet |
| `486000` | Pipeline transportation | 0.06 → 0.05 | 0.052 → 0.050 | 0.057 → 0.050 | B 86%, L 14% | unresolved | none yet |
| `339112` | Surgical and medical instrument manufacturing | 0.06 → 0.05 | **0.179** → **0.168** | **0.112** → **0.116** | B 25%, L 75% | unresolved | none yet |
| `230301` | Nonresidential maintenance and repair | 0.04 → 0.05 | 0.051 → 0.060 | 0.033 → 0.026 | B 51%, L 49% | unresolved | none yet |

### High-volatility factors

Every other commodity whose current volatility score is above 0.200, highest
first, on the same builds and weights as the tables above. Most rank low on the
IV score because they emit little and reach few other `N_d`, so read **Emissions**
(mean Mt CO2e a year over 2017-2024) alongside the volatility. None has been
assessed on these builds. Fishing and abrasives have earlier findings in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md).

| Code | Sector | IV score | Volatility score | N_d volatility | Emissions | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|---|---|---|
| `333316` | Photographic and photocopying equipment manufacturing | 0.01 → 0.00 | **0.651** → **0.651** | 0.097 → **0.102** | 0.02 | B 53%, L 47% | unresolved | none yet |
| `519130` | Internet publishing and broadcasting and Web search portals | 0.01 → 0.01 | **0.571** → **0.516** | **0.229** → **0.135** | 0.08 | B 22%, L 78% | unresolved | none yet |
| `532A00` | General and consumer goods rental | 0.01 → 0.01 | **0.515** → **0.503** | **0.151** → **0.146** | 0.49 | B 48%, L 52% | unresolved | none yet |
| `336111` | Automobile manufacturing | 0.00 → 0.00 | **0.465** → **0.486** | **0.168** → **0.689** | 0.73 | B 12%, L 88% | unresolved | none yet |
| `335991` | Carbon and graphite product manufacturing | 0.01 → 0.01 | **0.501** → **0.483** | **0.211** → **0.212** | 0.17 | B 19%, L 81% | unresolved | none yet |
| `425000` | Wholesale electronic markets and agents and brokers | 0.01 → 0.01 | **0.470** → **0.463** | **0.525** → **0.514** | 0.42 | B 11%, L 89% | unresolved | none yet |
| `333991` | Power-driven handtool manufacturing | 0.00 → 0.00 | **0.426** → **0.437** | 0.071 → 0.091 | 0.02 | B 35%, L 65% | unresolved | none yet |
| `336992` | Military armored vehicle, tank, and tank component manufacturing | 0.00 → 0.00 | **0.420** → **0.423** | **0.207** → **0.172** | 0.07 | B 20%, L 80% | unresolved | none yet |
| `335912` | Primary battery manufacturing | 0.00 → 0.01 | **0.284** → **0.407** | **0.180** → **0.367** | 0.02 | B 16%, L 84% | unresolved | none yet |
| `33299A` | Ammunition, arms, ordnance, and accessories manufacturing | 0.00 → 0.00 | **0.402** → **0.403** | **0.154** → **0.157** | 0.41 | B 30%, L 70% | unresolved | none yet |
| `335120` | Lighting fixture manufacturing | 0.01 → 0.01 | **0.428** → **0.398** | 0.067 → 0.083 | 0.20 | B 36%, L 64% | unresolved | none yet |
| `335110` | Electric lamp bulb and part manufacturing | 0.00 → 0.00 | **0.426** → **0.371** | 0.075 → **0.101** | 0.02 | B 38%, L 62% | unresolved | none yet |
| `325413` | In-vitro diagnostic substance manufacturing | 0.00 → 0.00 | **0.376** → **0.373** | **0.204** → **0.207** | 0.41 | B 33%, L 67% | unresolved | none yet |
| `332430` | Metal can, box, and other metal container (light gauge) manufacturing | 0.01 → 0.00 | **0.368** → **0.371** | 0.050 → 0.046 | 0.50 | B 34%, L 66% | unresolved | none yet |
| `312110` | Soft drink and ice manufacturing | 0.01 → 0.01 | **0.346** → **0.366** | 0.084 → 0.077 | 3.70 | B 74%, L 26% | unresolved | none yet |
| `334511` | Search, detection, and navigation instruments manufacturing | 0.00 → 0.00 | **0.376** → **0.357** | **0.187** → **0.171** | 0.44 | B 26%, L 74% | unresolved | none yet |
| `512200` | Sound recording industries | 0.00 → 0.00 | **0.364** → **0.358** | **0.314** → **0.182** | 0.01 | B 14%, L 86% | unresolved | none yet |
| `334111` | Electronic computer manufacturing | 0.00 → 0.00 | **0.350** → **0.362** | **0.298** → **0.331** | 0.00 | B 9%, L 91% | unresolved | none yet |
| `114000` | Fishing, hunting and trapping | 0.00 → 0.00 | **0.366** → **0.354** | **0.347** → **0.351** | 0.30 | B 40%, L 60% | unresolved | ~~[#912](https://github.com/cornerstone-data/bedrock/issues/912)~~ |
| `336360` | Motor vehicle seating and interior trim manufacturing | 0.00 → 0.00 | **0.326** → **0.348** | **0.156** → **0.210** | 0.69 | B 18%, L 82% | unresolved | none yet |
| `332310` | Plate work and fabricated structural product manufacturing | 0.01 → 0.01 | **0.347** → **0.345** | 0.070 → 0.066 | 1.35 | B 48%, L 52% | unresolved | none yet |
| `33641A` | Propulsion units and parts for space vehicles and guided missiles | 0.00 → 0.00 | **0.351** → **0.344** | **0.298** → **0.301** | 0.26 | B 20%, L 80% | unresolved | none yet |
| `334512` | Automatic environmental control manufacturing | 0.00 → 0.00 | **0.354** → **0.346** | **0.187** → **0.177** | 0.00 | B 16%, L 84% | unresolved | none yet |
| `311210` | Flour milling and malt manufacturing | 0.00 → 0.00 | **0.322** → **0.342** | 0.056 → 0.059 | 1.50 | B 72%, L 28% | unresolved | none yet |
| `333611` | Turbine and turbine generator set units manufacturing | 0.00 → 0.00 | **0.356** → **0.342** | 0.079 → 0.077 | 0.11 | B 46%, L 54% | unresolved | none yet |
| `327910` | Abrasive product manufacturing | 0.00 → 0.00 | **0.347** → **0.339** | **0.105** → 0.097 | 0.15 | B 31%, L 69% | unresolved | ~~[#912](https://github.com/cornerstone-data/bedrock/issues/912)~~ |
| `336413` | Other aircraft parts and auxiliary equipment manufacturing | 0.00 → 0.00 | **0.348** → **0.338** | **0.172** → **0.167** | 0.23 | B 16%, L 84% | unresolved | none yet |
| `335313` | Switchgear and switchboard apparatus manufacturing | 0.00 → 0.00 | **0.390** → **0.336** | 0.066 → 0.056 | 0.15 | B 38%, L 62% | unresolved | none yet |
| `336414` | Guided missile and space vehicle manufacturing | 0.00 → 0.00 | **0.337** → **0.328** | **0.204** → **0.197** | 0.15 | B 20%, L 80% | unresolved | none yet |
| `334290` | Other communications equipment manufacturing | 0.00 → 0.00 | **0.329** → **0.326** | **0.140** → **0.143** | 0.00 | B 17%, L 83% | unresolved | none yet |
| `335911` | Storage battery manufacturing | 0.01 → 0.01 | **0.291** → **0.327** | **0.121** → 0.064 | 1.26 | B 53%, L 47% | unresolved | none yet |
| `335312` | Motor and generator manufacturing | 0.01 → 0.01 | **0.380** → **0.325** | 0.066 → 0.051 | 0.12 | B 48%, L 52% | unresolved | none yet |
| `814000` | Private households | 0.00 → 0.00 | **0.317** → **0.317** | **0.317** → **0.317** | 0.01 | B 100%, L 0% | unresolved | none yet |
| `334112` | Computer storage device manufacturing | 0.01 → 0.01 | **0.231** → **0.303** | **0.302** → **0.351** | 0.02 | B 13%, L 87% | unresolved | none yet |
| `334610` | Manufacturing and reproducing magnetic and optical media | 0.00 → 0.00 | **0.310** → **0.304** | **0.464** → **0.431** | 0.00 | B 5%, L 95% | unresolved | none yet |
| `332420` | Metal tank (heavy gauge) manufacturing | 0.00 → 0.00 | **0.295** → **0.292** | **0.121** → **0.118** | 0.27 | B 43%, L 57% | unresolved | none yet |
| `332320` | Ornamental and architectural metal products manufacturing | 0.02 → 0.01 | **0.285** → **0.287** | 0.059 → 0.057 | 1.21 | B 49%, L 51% | unresolved | none yet |
| `335314` | Relay and industrial control manufacturing | 0.00 → 0.00 | **0.345** → **0.280** | 0.044 → 0.052 | 0.03 | B 48%, L 52% | unresolved | none yet |
| `336991` | Motorcycle, bicycle, and parts manufacturing | 0.01 → 0.01 | **0.273** → **0.279** | **0.580** → **0.491** | 0.03 | B 2%, L 98% | unresolved | none yet |
| `332996` | Fabricated pipe and pipe fitting manufacturing | 0.00 → 0.00 | **0.293** → **0.278** | 0.097 → 0.070 | 0.12 | B 53%, L 47% | unresolved | none yet |
| `334300` | Audio and video equipment manufacturing | 0.00 → 0.00 | **0.265** → **0.275** | **0.146** → **0.177** | 0.00 | B 15%, L 85% | unresolved | none yet |
| `334515` | Electricity and signal testing instruments manufacturing | 0.00 → 0.00 | **0.298** → **0.275** | **0.219** → **0.196** | 0.01 | B 14%, L 86% | unresolved | none yet |
| `335930` | Wiring device manufacturing | 0.00 → 0.00 | **0.315** → **0.272** | 0.073 → 0.060 | 0.16 | B 26%, L 74% | unresolved | none yet |
| `611B00` | Other educational services | 0.02 → 0.02 | **0.254** → **0.260** | **0.157** → **0.123** | 2.39 | B 64%, L 36% | unresolved | none yet |
| `532400` | Commercial and industrial machinery and equipment rental and leasing | 0.04 → 0.04 | **0.255** → **0.258** | **0.124** → **0.109** | 2.18 | B 41%, L 59% | unresolved | none yet |
| `333618` | Other engine equipment manufacturing | 0.00 → 0.00 | **0.264** → **0.258** | 0.073 → 0.065 | 0.25 | B 37%, L 63% | unresolved | none yet |
| `337122` | Nonupholstered wood household furniture manufacturing | 0.01 → 0.01 | **0.222** → **0.255** | **0.230** → **0.301** | 0.07 | B 9%, L 91% | unresolved | none yet |
| `327993` | Mineral wool manufacturing | 0.00 → 0.00 | **0.259** → **0.253** | 0.077 → 0.054 | 1.23 | B 45%, L 55% | unresolved | none yet |
| `811400` | Personal and household goods repair and maintenance | 0.00 → 0.00 | **0.199** → **0.253** | **0.114** → **0.103** | 0.18 | B 20%, L 80% | unresolved | none yet |
| `441000` | Motor vehicle and parts dealers | 0.01 → 0.01 | **0.272** → **0.249** | **0.231** → **0.221** | 6.47 | B 26%, L 74% | unresolved | none yet |
| `213111` | Drilling oil and gas wells | 0.00 → 0.00 | **0.245** → **0.244** | **0.191** → **0.205** | 1.25 | B 37%, L 63% | unresolved | none yet |
| `334210` | Telephone apparatus manufacturing | 0.00 → 0.00 | **0.249** → **0.243** | **0.139** → **0.150** | 0.00 | B 18%, L 82% | unresolved | none yet |
| `33451A` | Watch, clock, and other measuring and controlling device manufacturing | 0.00 → 0.00 | **0.242** → **0.241** | **0.168** → **0.135** | 0.01 | B 18%, L 82% | unresolved | none yet |
| `713900` | Other amusement and recreation industries | 0.01 → 0.01 | **0.223** → **0.231** | 0.053 → 0.066 | 4.50 | B 63%, L 37% | unresolved | none yet |
| `326160` | Plastics bottle manufacturing | 0.00 → 0.00 | **0.240** → **0.235** | 0.045 → 0.034 | 0.31 | B 49%, L 51% | unresolved | none yet |
| `523900` | Other financial investment activities | 0.03 → 0.03 | **0.238** → **0.235** | **0.130** → **0.133** | 7.32 | B 38%, L 62% | unresolved | none yet |
| `332720` | Turned product and screw, nut, and bolt manufacturing | 0.02 → 0.01 | **0.232** → **0.232** | 0.059 → 0.059 | 0.90 | B 54%, L 46% | unresolved | none yet |
| `48A000` | Scenic and sightseeing transportation and support activities | 0.03 → 0.03 | **0.224** → **0.230** | **0.213** → **0.210** | 0.82 | B 17%, L 83% | unresolved | none yet |
| `334510` | Electromedical and electrotherapeutic apparatus manufacturing | 0.00 → 0.00 | **0.204** → **0.230** | **0.154** → **0.152** | 0.17 | B 18%, L 82% | unresolved | none yet |
| `336611` | Ship building and repairing | 0.00 → 0.00 | **0.256** → **0.227** | **0.202** → **0.143** | 0.25 | B 21%, L 79% | unresolved | none yet |
| `336500` | Railroad rolling stock manufacturing | 0.00 → 0.00 | **0.235** → **0.226** | **0.128** → **0.122** | 0.08 | B 20%, L 80% | unresolved | none yet |
| `336310` | Motor vehicle gasoline engine and engine parts manufacturing | 0.00 → 0.00 | **0.226** → **0.224** | 0.080 → 0.067 | 0.58 | B 34%, L 66% | unresolved | none yet |
| `339930` | Doll, toy, and game manufacturing | 0.00 → 0.00 | **0.226** → **0.221** | 0.081 → 0.097 | 0.03 | B 23%, L 77% | unresolved | none yet |
| `333914` | Measuring, dispensing, and other pumping equipment manufacturing | 0.00 → 0.00 | **0.224** → **0.219** | 0.095 → 0.098 | 0.15 | B 24%, L 76% | unresolved | none yet |
| `541940` | Veterinary services | 0.00 → 0.00 | **0.435** → **0.219** | **0.273** → **0.328** | 0.08 | B 8%, L 92% | unresolved | none yet |
| `533000` | Lessors of nonfinancial intangible assets | 0.01 → 0.01 | **0.253** → **0.215** | **0.160** → **0.122** | 0.66 | B 22%, L 78% | unresolved | none yet |
| `336370` | Motor vehicle metal stamping | 0.00 → 0.00 | **0.212** → **0.211** | 0.099 → 0.099 | 0.59 | B 26%, L 74% | unresolved | none yet |
| `336350` | Motor vehicle transmission and power train parts manufacturing | 0.00 → 0.00 | **0.211** → **0.211** | 0.083 → 0.079 | 0.43 | B 20%, L 80% | unresolved | none yet |
| `511120` | Periodical Publishers | 0.00 → 0.00 | **0.240** → **0.210** | **0.152** → **0.136** | 0.01 | B 18%, L 82% | unresolved | none yet |
| `518200` | Data processing, hosting, and related services | 0.03 → 0.02 | **0.218** → **0.209** | **0.125** → **0.120** | 1.56 | B 38%, L 62% | unresolved | none yet |
| `325510` | Paint and coating manufacturing | 0.02 → 0.01 | **0.243** → **0.209** | 0.090 → 0.078 | 2.24 | B 36%, L 64% | unresolved | none yet |
| `335210` | Small electrical appliance manufacturing | 0.00 → 0.00 | **0.223** → **0.207** | 0.049 → 0.043 | 0.00 | B 44%, L 56% | unresolved | none yet |
| `336320` | Motor vehicle electrical and electronic equipment manufacturing | 0.00 → 0.00 | **0.205** → **0.206** | 0.049 → 0.055 | 0.21 | B 35%, L 65% | unresolved | none yet |
| `334418` | Printed circuit assembly (electronic assembly) manufacturing | 0.03 → 0.02 | **0.203** → **0.205** | **0.151** → **0.136** | 0.13 | B 32%, L 68% | unresolved | none yet |
| `5111A0` | Directory, mailing list, and other publishers | 0.00 → 0.00 | **0.200** → **0.202** | **0.135** → 0.092 | 0.02 | B 30%, L 70% | unresolved | none yet |
| `311920` | Coffee and tea manufacturing | 0.00 → 0.00 | **0.219** → **0.202** | 0.091 → 0.082 | 0.20 | B 31%, L 69% | unresolved | none yet |
| `336213` | Motor home manufacturing | 0.00 → 0.00 | **0.198** → **0.201** | 0.097 → **0.101** | 0.01 | B 60%, L 40% | unresolved | none yet |
| `335920` | Communication and energy wire and cable manufacturing | 0.00 → 0.00 | **0.228** → **0.201** | 0.059 → 0.022 | 0.12 | B 51%, L 49% | unresolved | none yet |

Status values:

| verdict | meaning |
|---|---|
| **justified** | a real movement in the world. Do not smooth; a smoothing change here would destroy signal. |
| **not justified** | movement with nothing underneath it. A defect, in scope, fix it. |
| **out of scope** | real driver, but this project has no lever on it. Hand over with the evidence. |
| **unresolved** | not yet investigated. Needs the source-level work before it can be ranked. |
| **split** | part of the movement is justified and part is not. The row says which. |

