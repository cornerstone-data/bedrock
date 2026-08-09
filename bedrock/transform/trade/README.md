# Trade FBS (BEA 2017 Detail)

National goods + services trade mapped to BEA 2017 Detail commodities.

## Methods

- `Trade_Exports_2017` — Census `ALL_VAL_YR` goods + BEA IntlServTrade exports. `SectorConsumedBy` is `F04000` after aggregation. 1:m Crosswalk rows are split in proportion to 2017 Use SUT `F04000`.
- `Trade_Imports_2017` — Census `GEN_CIF_YR` goods + BEA IntlServTrade imports (CIF-family mass for Supply `MCIF`). 1:m rows are split in proportion to 2017 Supply `MCIF`. Duties (`CAL_DUT_YR`) and charges (`GEN_CHA_YR`) stay selectable on the Census FBA via `FlowName`; they are not applied here.

Crosswalks: `NAICS_Crosswalk_Census_USATrade.csv` and `NAICS_Crosswalk_BEA_IEA.csv`. SUT weight FBAs already carry Detail commodity codes; attribution sources set `activity_schema: NAICS_2017_Code` so `map_to_sectors` does not apply `BEA_2017_Detail` (Detail→NAICS), and `Trade_detail_passthrough.yaml` keeps those codes from collapsing. Calling FBAs are `TECHNOSPHERE_FLOW` with empty `ActivityConsumedBy`, so activity sets set `primary_action_type: Produced`. Import weights set `primary_action_type: Consumed` because Supply stores `MCIF` on `ActivityProducedBy` and the commodity on `ActivityConsumedBy`.

`Trade_detail_passthrough.yaml` is a Trade-only `industry_spec` so Crosswalk Detail codes are not collapsed as NAICS. Swap it for a shared BEA Detail target when #567 / #568 land. Do not copy `Cornerstone_2025_target.yaml` / `BEA_detail_target.yaml` here — those select NAICS leaves; these Crosswalks emit Detail. ITA G+S scale is not applied in these methods.

## 2017 nowcast scorecard

Scored from `use_fd_detail_sut` `F04000` and `supply_bridge_detail_sut` `MCIF` (USD), after Trade overlay and the `S00900` identity. Command: `uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail`. Outcome: **FAIL + inventory** vs the #557 bars (national ~2–3%; import Pearson ≳ 0.85 / export ≳ 0.75–0.85 on non-`S00*` codes; top-20 Jaccard ≳ 0.7 / ≳ 0.6).

| | National % | Pearson all / non-`S00*` | Spearman all / non-`S00*` | Top-20 Jaccard all / non-`S00*` |
|---|---|---|---|---|
| F040 exports | +4.06% | 0.84 / 0.71 | 0.85 / 0.84 | 0.38 / 0.43 |
| MCIF imports | +1.77% | 0.62 / 0.83 | 0.93 / 0.94 | 0.54 / 0.67 |

`S00900` / `F04000` matches published within 1 M USD via the Y identity. MCIF national is inside ~2–3%; F040 is not. Export Pearson on non-specials is below the bar (all-codes Pearson is lifted by `S00900`). Import Pearson on non-specials is just short of 0.85. Jaccard is below both bars.

## Residual / specials

Documented rules; FBS methods do not allocate onto these codes. Holes below are `MISS` cells with `|reference| ≥ 1` B USD.

- **Thin service map.** Only API `TypeOfService` codes present in `NAICS_Crosswalk_BEA_IEA` are mapped. Hierarchy totals such as `AllTypesOfService` are left unmapped. Unmapped IEA types (travel, IP, etc.) leave SUT mass on Detail codes with no FBS row.
- **Census `980000`.** No Detail sector in the goods Crosswalk; the NAICS row is omitted from `NAICS_Crosswalk_Census_USATrade.csv` and stays unmapped.
- **`S00900` (rest of the world adjustment).** Set at Y assembly: `Y[S00900,F040] = −Y[S00900,F010] + Supply_T016[S00900]` (USD). Not an FBS extract.
- **`S00300` (noncomparable imports).** 260,421 M USD on Supply `MCIF`. No Crosswalk row. Hold-from-Supply / residual allocation is later wiring, not FBS generation.
- **Export `MISS` ≥ 1 B USD.** `533000` lessors of nonfinancial intangibles (73,049 M) — IP / charges-for-use, unmapped IEA. `483000` water transportation (4,858 M), `550000` management of companies (4,296 M), `532400` commercial equipment rental (3,883 M), `531ORE` other real estate (3,274 M), `622000` hospitals (2,815 M), `221100` electric power (2,744 M), `425000` wholesale electronic markets (2,385 M), `532100` automotive rental (2,192 M), `311810` bread and bakery (1,764 M), `484000` truck transportation (1,629 M), `482000` rail (1,358 M), `722110` full-service restaurants (1,177 M), `722211` limited-service restaurants (1,130 M). Rule: no Census NAICS-6 and no mapped IEA type hits that Detail commodity; leave zero until a Crosswalk or residual rule exists.
- **Import `MISS` ≥ 1 B USD besides `S00300`.** `311810` (4,710 M), `622000` (4,439 M), `221100` (2,431 M), `1121A0` beef cattle (1,659 M). Same rule: unmapped activity, not a silent zero in the SUT.
- **Industry-only Crosswalk sectors.** Census `331314` and IEA `S00101` are 2017 industry codes, not 2017 SUT commodities; they will not match Use/Supply Detail rows.
- **FAS vs PUR.** Census export `ALL_VAL_YR` is FAS-family; Use `F04000` is purchasers' value. These methods do not convert valuation basis commodity-by-commodity.
- **`MDTY` / `MADJ` / ITA scale.** Census duty and charge fields are in the FBA. National duty level, c.i.f./f.o.b. adjustment, and ITA G+S control wait for later wiring.
