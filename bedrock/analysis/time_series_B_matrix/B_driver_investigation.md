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
| `221100` | Electricity generation | 3.23 → 1.53 | 0.100 → 0.045 | B 82%, L 18% | justified| ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~, ~~[#894](https://github.com/cornerstone-data/bedrock/issues/894)~~, ~~[#898](https://github.com/cornerstone-data/bedrock/issues/898)~~, ~~[#901](https://github.com/cornerstone-data/bedrock/issues/901)~~, ~~[#1009](https://github.com/cornerstone-data/bedrock/issues/1009)~~, ~~[#1012](https://github.com/cornerstone-data/bedrock/issues/1012)~~, ~~[#1198](https://github.com/cornerstone-data/bedrock/issues/1198)~~, ~~[#1199](https://github.com/cornerstone-data/bedrock/issues/1199)~~, ~~[#1205](https://github.com/cornerstone-data/bedrock/issues/1205)~~ |
| `481000` | Air transportation | 0.40 → 0.40 | **0.121** → **0.119** | B 82%, L 18% | justified | ~~[#935](https://github.com/cornerstone-data/bedrock/issues/935)~~ |
| `211000` | Oil and gas extraction | 0.35 → 0.32 | 0.095 → 0.089 | B 51%, L 49% | Direct/`B` justified; `L` unresolved: flat GHGI `E`, GO matches EIA crude+gas $, real `q` up with volumes. Open: own-use 2021-22 | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~, ~~[#922](https://github.com/cornerstone-data/bedrock/issues/922)~~ |
| `562212` | Solid waste landfilling | 0.11 → 0.28 | 0.045 → 0.089 | B 96%, L 4% | unresolved: `E` flat; `B` moves with `q` (flat 2018-2021, step in 2022). See #1269 | [#1269](https://github.com/cornerstone-data/bedrock/issues/1269) |
| `484000` | Truck transportation | 0.31 → 0.28 | 0.042 → 0.039 | B 54%, L 46% | justified: `B` flat (within 5% of 2017, ends 2% lower); own use fixed in #890 (3.33 to 4.50 per $100, no 2023 step). Open: `L` (46%); 2021 diesel rise | ~~[#890](https://github.com/cornerstone-data/bedrock/issues/890)~~ |
| `324110` | Petroleum refineries | 0.26 → 0.24 | 0.083 → 0.081 | B 48%, L 52% | unresolved | [#984](https://github.com/cornerstone-data/bedrock/issues/984), [#975](https://github.com/cornerstone-data/bedrock/issues/975), [#997](https://github.com/cornerstone-data/bedrock/issues/997), ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~ |
| `325110` | Petrochemical manufacturing | 0.25 → 0.24 | **0.163** → **0.154** | B 61%, L 39% | mostly justified: `E` follows ethylene and output follows total product tonnes. Open: 2024 refinery product-share fall (33% to 25%); 2021 non-energy step | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), ~~[#1211](https://github.com/cornerstone-data/bedrock/issues/1211)~~, ~~[#1229](https://github.com/cornerstone-data/bedrock/issues/1229)~~, [#997](https://github.com/cornerstone-data/bedrock/issues/997), ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~, [#986](https://github.com/cornerstone-data/bedrock/issues/986), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |
| `531ORE` | Other real estate | 0.23 → 0.23 | **0.128** → **0.128** | B 51%, L 49% | unresolved | [#705](https://github.com/cornerstone-data/bedrock/issues/705), ~~[#1163](https://github.com/cornerstone-data/bedrock/issues/1163)~~, [#975](https://github.com/cornerstone-data/bedrock/issues/975) |
| `331110` | Iron and steel mills and ferroalloy manufacturing | 0.16 → 0.17 | 0.059 → 0.058 | B 38%, L 62% | justified: facility emissions and crude steel match the 12% `B` fall; coking coal is steady (7.5 to 6.4 Mt). Open: `L` (62%); gas vs direct boundary | ~~[#1202](https://github.com/cornerstone-data/bedrock/issues/1202)~~ |
| `325211` | Plastics material and resin manufacturing | 0.21 → 0.16 | **0.144** → 0.093 | B 56%, L 44% | split, below 0.2: under a third of the rise is supported on facility emissions without adipic acid; non-energy use drives the rest | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), [#670](https://github.com/cornerstone-data/bedrock/issues/670), [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |
| `325190` | Other basic organic chemical manufacturing | 0.23 → 0.16 | **0.128** → 0.091 | B 57%, L 43% | split, below 0.2: adipic acid N2O decline is justified. Open: fossil allocation fall; 2024 output rise vs production reports | [#1210](https://github.com/cornerstone-data/bedrock/issues/1210), ~~[#1211](https://github.com/cornerstone-data/bedrock/issues/1211)~~, ~~[#1177](https://github.com/cornerstone-data/bedrock/issues/1177)~~, ~~[#898](https://github.com/cornerstone-data/bedrock/issues/898)~~, [#1196](https://github.com/cornerstone-data/bedrock/issues/1196), ~~[#1203](https://github.com/cornerstone-data/bedrock/issues/1203)~~ |

### The rest of the top 30

The other commodities in the current top 30, ordered by current IV score, on the
same builds and weights as the table above. None has been assessed on these builds.
Grain farming and water transportation have earlier findings in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md).

| Code | Sector | IV score | Volatility score | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|---|
| `485000` | Transit and ground passenger transportation | 0.14 → 0.15 | **0.120** → **0.123** | B 66%, L 34% | unresolved | none yet |
| `325310` | Fertilizer manufacturing | 0.18 → 0.14 | **0.178** → **0.145** | B 80%, L 20% | unresolved | none yet |
| `1111B0` | Grain farming | 0.13 → 0.12 | 0.091 → 0.091 | B 74%, L 26% | unresolved | ~~[#934](https://github.com/cornerstone-data/bedrock/issues/934)~~ |
| `541800` | Advertising, public relations, and related services | 0.11 → 0.11 | **0.156** → **0.163** | B 35%, L 65% | unresolved | none yet |
| `212100` | Coal mining | 0.11 → 0.11 | **0.115** → **0.111** | B 87%, L 13% | unresolved | none yet |
| `315000` | Apparel manufacturing | 0.11 → 0.11 | **0.535** → **0.541** | B 43%, L 57% | unresolved | none yet |
| `325120` | Industrial gas manufacturing | 0.12 → 0.10 | **0.128** → **0.126** | B 71%, L 29% | unresolved | none yet |
| `493000` | Warehousing and storage | 0.09 → 0.09 | 0.100 → **0.102** | B 37%, L 63% | unresolved | none yet |
| `483000` | Water transportation | 0.08 → 0.07 | **0.311** → **0.311** | B 88%, L 12% | unresolved | [#936](https://github.com/cornerstone-data/bedrock/issues/936) |
| `492000` | Couriers and messengers | 0.08 → 0.07 | 0.064 → 0.061 | B 46%, L 54% | unresolved | none yet |
| `532100` | Automotive equipment rental and leasing | 0.07 → 0.07 | **0.217** → **0.217** | B 62%, L 38% | unresolved | none yet |
| `322130` | Paperboard mills | 0.06 → 0.07 | 0.069 → 0.083 | B 22%, L 78% | unresolved | none yet |
| `334413` | Semiconductor and related device manufacturing | 0.08 → 0.07 | 0.071 → 0.072 | B 42%, L 58% | unresolved | none yet |
| `327310` | Cement manufacturing | 0.07 → 0.07 | 0.072 → 0.072 | B 90%, L 10% | unresolved | none yet |
| `52A000` | Monetary authorities and depository credit intermediation | 0.06 → 0.06 | **0.247** → **0.250** | B 31%, L 69% | unresolved | none yet |
| `327400` | Lime and gypsum product manufacturing | 0.06 → 0.06 | 0.062 → 0.062 | B 81%, L 19% | unresolved | none yet |
| `486000` | Pipeline transportation | 0.06 → 0.05 | 0.052 → 0.050 | B 74%, L 26% | unresolved | none yet |
| `339112` | Surgical and medical instrument manufacturing | 0.06 → 0.05 | **0.179** → **0.171** | B 25%, L 75% | unresolved | none yet |
| `523A00` | Securities and commodity contracts intermediation and brokerage | 0.04 → 0.04 | **0.200** → **0.200** | B 47%, L 53% | unresolved | none yet |

### High-volatility factors

Every other commodity whose current volatility score is above 0.200, highest
first, on the same builds and weights as the tables above. Most rank low on the
IV score because they emit little and reach few other `N_d`, so read **Emissions**
(mean Mt CO2e a year over 2017-2024) alongside the volatility. None has been
assessed on these builds. Fishing and abrasives have earlier findings in
[`About_sector_trend_explanations.md`](About_sector_trend_explanations.md).

| Code | Sector | IV score | Volatility score | Emissions | Driving factor | Status | Issues and PRs |
|---|---|---|---|---|---|---|---|
| `333316` | Photographic and photocopying equipment manufacturing | 0.01 → 0.00 | **0.651** → **0.655** | 0.02 | B 54%, L 46% | unresolved | none yet |
| `519130` | Internet publishing and broadcasting and Web search portals | 0.01 → 0.01 | **0.571** → **0.573** | 0.08 | B 19%, L 81% | unresolved | none yet |
| `532A00` | General and consumer goods rental | 0.01 → 0.01 | **0.515** → **0.527** | 0.49 | B 41%, L 59% | unresolved | none yet |
| `335991` | Carbon and graphite product manufacturing | 0.01 → 0.01 | **0.501** → **0.483** | 0.16 | B 17%, L 83% | unresolved | none yet |
| `336111` | Automobile manufacturing | 0.00 → 0.00 | **0.465** → **0.465** | 0.76 | B 33%, L 67% | unresolved | none yet |
| `541940` | Veterinary services | 0.00 → 0.00 | **0.435** → **0.433** | 0.09 | B 9%, L 91% | unresolved | none yet |
| `336992` | Military armored vehicle, tank, and tank component manufacturing | 0.00 → 0.00 | **0.420** → **0.428** | 0.07 | B 20%, L 80% | unresolved | none yet |
| `333991` | Power-driven handtool manufacturing | 0.00 → 0.00 | **0.426** → **0.425** | 0.02 | B 35%, L 65% | unresolved | none yet |
| `335120` | Lighting fixture manufacturing | 0.01 → 0.01 | **0.428** → **0.404** | 0.19 | B 40%, L 60% | unresolved | none yet |
| `33299A` | Ammunition, arms, ordnance, and accessories manufacturing | 0.00 → 0.00 | **0.402** → **0.403** | 0.41 | B 30%, L 70% | unresolved | none yet |
| `335110` | Electric lamp bulb and part manufacturing | 0.00 → 0.00 | **0.426** → **0.395** | 0.02 | B 45%, L 55% | unresolved | none yet |
| `114000` | Fishing, hunting and trapping | 0.00 → 0.00 | **0.366** → **0.394** | 0.33 | B 43%, L 57% | unresolved | ~~[#912](https://github.com/cornerstone-data/bedrock/issues/912)~~ |
| `512200` | Sound recording industries | 0.00 → 0.00 | **0.364** → **0.373** | 0.01 | B 12%, L 88% | unresolved | none yet |
| `332430` | Metal can, box, and other metal container (light gauge) manufacturing | 0.01 → 0.00 | **0.368** → **0.371** | 0.51 | B 33%, L 67% | unresolved | none yet |
| `334511` | Search, detection, and navigation instruments manufacturing | 0.00 → 0.00 | **0.376** → **0.365** | 0.43 | B 25%, L 75% | unresolved | none yet |
| `325413` | In-vitro diagnostic substance manufacturing | 0.00 → 0.00 | **0.376** → **0.360** | 0.42 | B 31%, L 69% | unresolved | none yet |
| `312110` | Soft drink and ice manufacturing | 0.01 → 0.01 | **0.346** → **0.355** | 3.61 | B 73%, L 27% | unresolved | none yet |
| `336413` | Other aircraft parts and auxiliary equipment manufacturing | 0.00 → 0.00 | **0.348** → **0.353** | 0.23 | B 16%, L 84% | unresolved | none yet |
| `335313` | Switchgear and switchboard apparatus manufacturing | 0.00 → 0.00 | **0.390** → **0.352** | 0.14 | B 34%, L 66% | unresolved | none yet |
| `334111` | Electronic computer manufacturing | 0.00 → 0.00 | **0.350** → **0.351** | 0.00 | B 10%, L 90% | unresolved | none yet |
| `33641A` | Propulsion units and parts for space vehicles and guided missiles | 0.00 → 0.00 | **0.351** → **0.347** | 0.26 | B 20%, L 80% | unresolved | none yet |
| `327910` | Abrasive product manufacturing | 0.00 → 0.00 | **0.347** → **0.347** | 0.15 | B 31%, L 69% | unresolved | ~~[#912](https://github.com/cornerstone-data/bedrock/issues/912)~~ |
| `335312` | Motor and generator manufacturing | 0.01 → 0.01 | **0.380** → **0.346** | 0.11 | B 44%, L 56% | unresolved | none yet |
| `333611` | Turbine and turbine generator set units manufacturing | 0.00 → 0.00 | **0.356** → **0.343** | 0.11 | B 43%, L 57% | unresolved | none yet |
| `332310` | Plate work and fabricated structural product manufacturing | 0.01 → 0.01 | **0.347** → **0.342** | 1.36 | B 48%, L 52% | unresolved | none yet |
| `334512` | Automatic environmental control manufacturing | 0.00 → 0.00 | **0.354** → **0.339** | 0.00 | B 15%, L 85% | unresolved | none yet |
| `311210` | Flour milling and malt manufacturing | 0.00 → 0.00 | **0.322** → **0.337** | 1.49 | B 76%, L 24% | unresolved | none yet |
| `336414` | Guided missile and space vehicle manufacturing | 0.00 → 0.00 | **0.337** → **0.335** | 0.15 | B 19%, L 81% | unresolved | none yet |
| `334290` | Other communications equipment manufacturing | 0.00 → 0.00 | **0.329** → **0.330** | 0.00 | B 18%, L 82% | unresolved | none yet |
| `336360` | Motor vehicle seating and interior trim manufacturing | 0.00 → 0.00 | **0.326** → **0.324** | 0.68 | B 22%, L 78% | unresolved | none yet |
| `335911` | Storage battery manufacturing | 0.01 → 0.01 | **0.291** → **0.320** | 1.39 | B 50%, L 50% | unresolved | none yet |
| `814000` | Private households | 0.00 → 0.00 | **0.317** → **0.317** | 0.01 | B 100%, L 0% | unresolved | none yet |
| `335314` | Relay and industrial control manufacturing | 0.00 → 0.00 | **0.345** → **0.306** | 0.03 | B 46%, L 54% | unresolved | none yet |
| `332996` | Fabricated pipe and pipe fitting manufacturing | 0.00 → 0.00 | **0.293** → **0.293** | 0.13 | B 46%, L 54% | unresolved | none yet |
| `335930` | Wiring device manufacturing | 0.00 → 0.00 | **0.315** → **0.293** | 0.14 | B 26%, L 74% | unresolved | none yet |
| `334610` | Manufacturing and reproducing magnetic and optical media | 0.00 → 0.00 | **0.310** → **0.290** | 0.00 | B 6%, L 94% | unresolved | none yet |
| `332420` | Metal tank (heavy gauge) manufacturing | 0.00 → 0.00 | **0.295** → **0.288** | 0.27 | B 42%, L 58% | unresolved | none yet |
| `332320` | Ornamental and architectural metal products manufacturing | 0.02 → 0.01 | **0.285** → **0.284** | 1.23 | B 50%, L 50% | unresolved | none yet |
| `335912` | Primary battery manufacturing | 0.00 → 0.00 | **0.284** → **0.283** | 0.03 | B 23%, L 77% | unresolved | none yet |
| `334515` | Electricity and signal testing instruments manufacturing | 0.00 → 0.00 | **0.298** → **0.282** | 0.01 | B 13%, L 87% | unresolved | none yet |
| `441000` | Motor vehicle and parts dealers | 0.01 → 0.01 | **0.272** → **0.278** | 6.57 | B 25%, L 75% | unresolved | none yet |
| `336991` | Motorcycle, bicycle, and parts manufacturing | 0.01 → 0.01 | **0.273** → **0.274** | 0.03 | B 2%, L 98% | unresolved | none yet |
| `334300` | Audio and video equipment manufacturing | 0.00 → 0.00 | **0.265** → **0.268** | 0.00 | B 18%, L 82% | unresolved | none yet |
| `333618` | Other engine equipment manufacturing | 0.00 → 0.00 | **0.264** → **0.262** | 0.25 | B 35%, L 65% | unresolved | none yet |
| `533000` | Lessors of nonfinancial intangible assets | 0.01 → 0.01 | **0.253** → **0.257** | 0.64 | B 24%, L 76% | unresolved | none yet |
| `327993` | Mineral wool manufacturing | 0.00 → 0.00 | **0.259** → **0.254** | 1.23 | B 46%, L 54% | unresolved | none yet |
| `611B00` | Other educational services | 0.02 → 0.02 | **0.254** → **0.253** | 2.50 | B 58%, L 42% | unresolved | none yet |
| `532400` | Commercial and industrial machinery and equipment rental and leasing | 0.04 → 0.04 | **0.255** → **0.252** | 2.18 | B 35%, L 65% | unresolved | none yet |
| `334210` | Telephone apparatus manufacturing | 0.00 → 0.00 | **0.249** → **0.247** | 0.00 | B 19%, L 81% | unresolved | none yet |
| `446000` | Health and personal care stores | 0.00 → 0.00 | **0.233** → **0.245** | 0.57 | B 38%, L 62% | unresolved | none yet |
| `515200` | Cable and other subscription programming | 0.00 → 0.00 | **0.245** → **0.243** | 0.69 | B 61%, L 39% | unresolved | none yet |
| `515100` | Radio and television broadcasting | 0.04 → 0.04 | **0.239** → **0.240** | 1.49 | B 51%, L 49% | unresolved | none yet |
| `523900` | Other financial investment activities | 0.03 → 0.03 | **0.238** → **0.238** | 7.39 | B 39%, L 61% | unresolved | none yet |
| `33451A` | Watch, clock, and other measuring and controlling device manufacturing | 0.00 → 0.00 | **0.242** → **0.238** | 0.01 | B 16%, L 84% | unresolved | none yet |
| `334112` | Computer storage device manufacturing | 0.01 → 0.01 | **0.231** → **0.237** | 0.03 | B 16%, L 84% | unresolved | none yet |
| `511120` | Periodical Publishers | 0.00 → 0.00 | **0.240** → **0.236** | 0.01 | B 18%, L 82% | unresolved | none yet |
| `326160` | Plastics bottle manufacturing | 0.00 → 0.00 | **0.240** → **0.236** | 0.31 | B 50%, L 50% | unresolved | none yet |
| `336611` | Ship building and repairing | 0.00 → 0.00 | **0.256** → **0.234** | 0.24 | B 21%, L 79% | unresolved | none yet |
| `531HST` | Tenant-occupied housing | 0.00 → 0.00 | **0.199** → **0.233** | 0.10 | B 20%, L 80% | unresolved | none yet |
| `713900` | Other amusement and recreation industries | 0.01 → 0.01 | **0.223** → **0.233** | 4.59 | B 61%, L 39% | unresolved | none yet |
| `334510` | Electromedical and electrotherapeutic apparatus manufacturing | 0.00 → 0.00 | **0.204** → **0.232** | 0.18 | B 18%, L 82% | unresolved | none yet |
| `213111` | Drilling oil and gas wells | 0.00 → 0.00 | **0.245** → **0.228** | 1.29 | B 34%, L 66% | unresolved | none yet |
| `336310` | Motor vehicle gasoline engine and engine parts manufacturing | 0.00 → 0.00 | **0.226** → **0.228** | 0.58 | B 34%, L 66% | unresolved | none yet |
| `336500` | Railroad rolling stock manufacturing | 0.00 → 0.00 | **0.235** → **0.226** | 0.08 | B 20%, L 80% | unresolved | none yet |
| `332720` | Turned product and screw, nut, and bolt manufacturing | 0.02 → 0.01 | **0.232** → **0.226** | 0.90 | B 57%, L 43% | unresolved | none yet |
| `561300` | Employment services | 0.01 → 0.01 | **0.228** → **0.223** | 0.17 | B 20%, L 80% | unresolved | none yet |
| `518200` | Data processing, hosting, and related services | 0.03 → 0.02 | **0.218** → **0.222** | 1.57 | B 38%, L 62% | unresolved | none yet |
| `339930` | Doll, toy, and game manufacturing | 0.00 → 0.00 | **0.226** → **0.222** | 0.03 | B 26%, L 74% | unresolved | none yet |
| `333914` | Measuring, dispensing, and other pumping equipment manufacturing | 0.00 → 0.00 | **0.224** → **0.219** | 0.15 | B 23%, L 77% | unresolved | none yet |
| `312120` | Breweries | 0.00 → 0.00 | **0.215** → **0.215** | 1.60 | B 50%, L 50% | unresolved | none yet |
| `311920` | Coffee and tea manufacturing | 0.00 → 0.00 | **0.219** → **0.215** | 0.19 | B 31%, L 69% | unresolved | none yet |
| `48A000` | Scenic and sightseeing transportation and support activities | 0.03 → 0.03 | **0.224** → **0.213** | 0.86 | B 15%, L 85% | unresolved | none yet |
| `336370` | Motor vehicle metal stamping | 0.00 → 0.00 | **0.212** → **0.211** | 0.60 | B 26%, L 74% | unresolved | none yet |
| `336350` | Motor vehicle transmission and power train parts manufacturing | 0.00 → 0.00 | **0.211** → **0.210** | 0.43 | B 20%, L 80% | unresolved | none yet |
| `337122` | Nonupholstered wood household furniture manufacturing | 0.01 → 0.01 | **0.222** → **0.208** | 0.07 | B 8%, L 92% | unresolved | none yet |
| `611100` | Elementary and secondary schools | 0.00 → 0.00 | **0.207** → **0.207** | 7.27 | B 84%, L 16% | unresolved | none yet |
| `335210` | Small electrical appliance manufacturing | 0.00 → 0.00 | **0.223** → **0.206** | 0.00 | B 44%, L 56% | unresolved | none yet |
| `325510` | Paint and coating manufacturing | 0.02 → 0.01 | **0.243** → **0.206** | 2.24 | B 35%, L 65% | unresolved | none yet |
| `334418` | Printed circuit assembly (electronic assembly) manufacturing | 0.03 → 0.03 | **0.203** → **0.205** | 0.14 | B 32%, L 68% | unresolved | none yet |
| `333130` | Mining and oil and gas field machinery manufacturing | 0.00 → 0.00 | **0.203** → **0.201** | 0.40 | B 24%, L 76% | unresolved | none yet |
| `336320` | Motor vehicle electrical and electronic equipment manufacturing | 0.00 → 0.00 | **0.205** → **0.201** | 0.21 | B 36%, L 64% | unresolved | none yet |
| `621200` | Offices of dentists | 0.00 → 0.00 | **0.195** → **0.200** | 0.30 | B 26%, L 74% | unresolved | none yet |

Status values:

| verdict | meaning |
|---|---|
| **justified** | a real movement in the world. Do not smooth; a smoothing change here would destroy signal. |
| **not justified** | movement with nothing underneath it. A defect, in scope, fix it. |
| **out of scope** | real driver, but this project has no lever on it. Hand over with the evidence. |
| **unresolved** | not yet investigated. Needs the source-level work before it can be ranked. |
| **split** | part of the movement is justified and part is not. The row says which. |

