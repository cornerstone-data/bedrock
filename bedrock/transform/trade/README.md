# Trade FBS (BEA 2017 Detail)

National goods + services trade mapped to BEA 2017 Detail commodities.

## Methods

- `Trade_Exports_2017` — Census `ALL_VAL_YR` goods + BEA IntlServTrade exports. `SectorConsumedBy` is `F04000` after aggregation. 1:m Crosswalk rows are split in proportion to 2017 Use SUT `F04000`.
- `Trade_Imports_2017` — Census `GEN_CIF_YR` goods + BEA IntlServTrade imports (CIF-family mass for Supply `MCIF`). 1:m rows are split in proportion to 2017 Supply `MCIF`. Duties (`CAL_DUT_YR`) and charges (`GEN_CHA_YR`) stay selectable on the Census FBA via `FlowName`; they are not applied here.

Crosswalks: `Sector_Crosswalk_Census_USATrade.csv` and `Sector_Crosswalk_BEA_IEA.csv` (`SectorSourceName` `BEA_2017_Code`). IEA rows are deepest `TypeOfService` codes whose label names the Detail commodity (or a small family the type spans). Parent totals are omitted when children are mapped (`AllTypesOfService`, `Transport`, `ChargesForTheUseOfIpNie`, `TelecomCompAndInfo`, `OtherBusiness`, `ProfMgmtConsult`, `Travel`). Crosswalk membership is not chosen by closing 2017 Use/Supply cells. Methods include `BEA_detail_commodity_target.yaml` so FBS output stays on BEA 2017 Detail commodities. Calling FBAs are `TECHNOSPHERE_FLOW` with empty `ActivityConsumedBy`, so activity sets set `primary_action_type: Produced`. Import weights set `primary_action_type: Consumed` because Supply stores `MCIF` on `ActivityProducedBy` and the commodity on `ActivityConsumedBy`.

Do not copy `Cornerstone_2025_target.yaml` / `BEA_detail_target.yaml` here — those select NAICS leaves; these Crosswalks emit Detail. ITA G+S scale is not applied in these methods.

## 2017 nowcast scorecard

Scored from `use_fd_detail_sut` `F04000` and `supply_bridge_detail_sut` `MCIF` (USD), after Trade overlay and the `S00900` identity. Command: `uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail`. Outcome: **FAIL + inventory** vs the #557 bars (national ~2–3%; import Pearson ≳ 0.85 / export ≳ 0.75–0.85 on non-`S00*` codes; top-20 Jaccard ≳ 0.7 / ≳ 0.6).

| | National % | Pearson all / non-`S00*` | Spearman all / non-`S00*` | Top-20 Jaccard all / non-`S00*` |
|---|---|---|---|---|
| F040 exports | +6.15% | 0.93 / 0.89 | 0.87 / 0.87 | 0.60 / 0.60 |
| MCIF imports | +1.29% | 0.62 / 0.84 | 0.90 / 0.91 | 0.67 / 0.74 |

`S00900` / `F04000` matches published within 1 M USD via the Y identity. MCIF national is inside ~2–3%; F040 is not (ITA G+S scale is not applied). Export Pearson on non-specials clears the bar. Import Pearson on non-specials is just short of 0.85. Export Jaccard meets the lower bar; import non-`S00*` Jaccard clears 0.7.

## Residual / specials

Documented rules; FBS methods do not allocate onto these codes. Holes below are `MISS` cells with `|reference| ≥ 1` B USD.

- **Unmapped IEA types.** Hierarchy totals (`AllTypesOfService`, `Transport`, `Travel`, `OtherBusiness`, …) stay unmapped when children are mapped. Digitally deliverable cross-cuts (`PotIctEnServ*`) stay unmapped. `Travel` other than `TravelHealth`, transport port services (`TransportAirPort`, `TransportSeaPort`), `GovtGoodsAndServicesNie`, and `OthBusinessNie` have no Detail home on the Crosswalk. Those types do not appear on F040/MCIF.
- **Census `980000`.** No Detail sector in the goods Crosswalk; the NAICS row is omitted from `Sector_Crosswalk_Census_USATrade.csv` and stays unmapped.
- **`S00900` (rest of the world adjustment).** Set at Y assembly: `Y[S00900,F040] = −Y[S00900,F010] + Supply_T016[S00900]` (USD). Not an FBS extract.
- **`S00300` (noncomparable imports).** 260,421 M USD on Supply `MCIF`. No Crosswalk row. Hold-from-Supply / residual allocation is later wiring, not FBS generation.
- **Export `MISS` ≥ 1 B USD.** `492000` couriers (9,411 M), `550000` management of companies (4,296 M), `531ORE` other real estate (3,274 M), `221100` electric power (2,744 M), `511130` book publishers (2,598 M), `311810` bread and bakery (1,764 M), `511120` periodical publishers (1,236 M), `722110` full-service restaurants (1,177 M), `722211` limited-service restaurants (1,130 M). Rule: no Census NAICS-6 and no mapped IEA type hits that Detail commodity.
- **Import `MISS` ≥ 1 B USD besides `S00300`.** `311810` (4,710 M), `221100` (2,431 M), `511130` (1,786 M), `1121A0` beef cattle (1,659 M), `561300` employment services (1,588 M). Same rule.
- **Industry-only Crosswalk sectors.** Census `331314` is a 2017 industry code, not a 2017 SUT commodity (`331313` / `33131B`).
- **FAS vs PUR.** Census export `ALL_VAL_YR` is FAS-family; Use `F04000` is purchasers' value. These methods do not convert valuation basis commodity-by-commodity.
- **`MDTY` / `MADJ` / ITA scale.** Census duty and charge fields are in the FBA. National duty level, c.i.f./f.o.b. adjustment, and ITA G+S control wait for later wiring.

## IEA Crosswalk revisions

Candidates to revisit. Trigger is a classification or source argument, not a 2017 cell gap.

- **`DatabaseAndOthInfo` → 5111\*** — add newspaper/periodical/book/`5111A0` only if EBOPS–CPC–NAICS names publishing. File maps `5191A0` only.
- **`BusMgmtConsPubRel` → `550000`** — add management of companies only if that NAICS is in the type’s definition (consulting / PR). File maps `541610` / `5416A0` only.
- **`TransportRoadAndOth`** — extend to `485000` / `486000` / `492000` if “other modes” includes transit, pipeline, or couriers. File maps rail + truck (`482000`, `484000`).
- **`CipLicensesBroadcastLiveRecord`** — `515100` (broadcasting) or `711*` (live events) vs movies `512100` (file).
- **`GovtGoodsAndServicesNie`** — unmapped. Needs a commodity home that is government n.i.e., not federal electric (`S00101` / `S00102`) and not postal (`491000`).
- **`OthBusinessNie` → `561*`** — n.i.e. residual onto admin/support only if a concordance names those NAICS. Mass is several times 561\* F040/MCIF; file leaves the type unmapped.
- **Travel other than `TravelHealth`** — visitor spend (hotels, food, education travel) via BEA TTSA or leave unmapped. Not Use `F04000` mix (`721000` is 0 in the 2017 SUT).
- **Transport port services** — `TransportAirPort` / `TransportSeaPort` wait for a support-activities Detail code (no `488000` in 2017 commodities) or a written rule that port is the mode commodity (`481000` / `483000`).
- **1:m weights** — `attribution_method: equal`, or same-year `BEA_Detail_GrossOutput_IO`, vs frozen 2017 F040/MCIF. Equal/GO do not require a later Detail Use/Supply.
- **Goods coverage** — Census FBA has no `1121*` activity (`1121A0` import hole); `311824` maps to `3118A0` (not bakery `311810`); `980000` stays omitted.
- **ITA G+S scale** — national control (#567); does not change Crosswalk identity.
