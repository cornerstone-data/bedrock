# Trade FBS (BEA 2017 Detail)

National goods + services trade mapped to BEA 2017 Detail commodities.

## Methods

- `Trade_Exports_2017` — Census `ALL_VAL_YR` goods + BEA IntlServTrade exports. `SectorConsumedBy` is `F04000` after aggregation. 1:m Crosswalk rows are split in proportion to 2017 Use SUT `F04000`.
- `Trade_Imports_2017` — Census `GEN_CIF_YR` goods + BEA IntlServTrade imports (CIF-family mass for Supply `MCIF`). 1:m rows are split in proportion to 2017 Supply `MCIF`. Duties (`CAL_DUT_YR`) and charges (`GEN_CHA_YR`) stay selectable on the Census FBA via `FlowName`; they are not applied here.

Crosswalks: `NAICS_Crosswalk_Census_USATrade.csv` and `NAICS_Crosswalk_BEA_IEA.csv`. SUT weight FBAs already carry Detail commodity codes; attribution sources set `activity_schema: NAICS_2017_Code` so `map_to_sectors` does not apply `BEA_2017_Detail` (Detail→NAICS), and `Trade_detail_passthrough.yaml` keeps those codes from collapsing. Calling FBAs are `TECHNOSPHERE_FLOW` with empty `ActivityConsumedBy`, so activity sets set `primary_action_type: Produced`. Import weights set `primary_action_type: Consumed` because Supply stores `MCIF` on `ActivityProducedBy` and the commodity on `ActivityConsumedBy`.

`Trade_detail_passthrough.yaml` is a Trade-only `industry_spec` so Crosswalk Detail codes are not collapsed as NAICS. Swap it for a shared BEA Detail target when #567 / #568 land. Do not copy `Cornerstone_2025_target.yaml` / `BEA_detail_target.yaml` here — those select NAICS leaves; these Crosswalks emit Detail. ITA G+S scale is not applied in these methods.

## Residual / specials (documented; not applied in these methods)

- **Thin service map.** Only API `TypeOfService` codes present in `NAICS_Crosswalk_BEA_IEA` are mapped. Hierarchy totals such as `AllTypesOfService` are left unmapped. Travel, charges for the use of intellectual property (`533000`), and other unmapped service types remain in the residual vs Use `F04000` / Supply `MCIF`.
- **Census `980000`.** No Detail sector in the goods Crosswalk; the NAICS row is omitted from `NAICS_Crosswalk_Census_USATrade.csv` and stays unmapped.
- **`S00900` (rest of the world adjustment).** Derive from the SUT identity at Y assembly (PCE / Supply), not from this FBS.
- **`S00300` (noncomparable imports), wholesale, and other SUT holes.** No Crosswalk rows force mass onto these codes. Hold-from-Use or residual rules belong with nowcast wiring, not FBS generation.
- **Industry-only Crosswalk sectors.** Census `331314` and IEA `S00101` are 2017 industry codes, not 2017 SUT commodities; they will not match Use/Supply Detail rows.
- **FAS vs PUR.** Census export `ALL_VAL_YR` is FAS-family; Use `F04000` is purchasers' value. These methods do not convert valuation basis commodity-by-commodity.
- **`MDTY` / `MADJ` / ITA scale.** Census duty and charge fields are in the FBA. National duty level, c.i.f./f.o.b. adjustment, and ITA G+S control wait for later wiring.
