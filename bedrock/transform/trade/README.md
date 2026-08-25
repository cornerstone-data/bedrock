# Trade FBS (BEA 2017 Detail)

National goods + services trade mapped to BEA 2017 Detail commodities.

## Methods

- `Trade_Exports_2017` — Census `ALL_VAL_YR` goods + BEA IntlServTrade exports. `SectorConsumedBy` is `F04000` after aggregation. 1:m Crosswalk rows are split in proportion to 2017 Use SUT `F04000`.
- `Trade_Imports_2017` — Census `GEN_CIF_YR` goods + BEA IntlServTrade imports (CIF-family mass for Supply `MCIF`). 1:m rows are split in proportion to 2017 Supply `MCIF`. Duties (`CAL_DUT_YR`) and charges (`GEN_CHA_YR`) stay selectable on the Census FBA via `FlowName`; they are not applied here.

Crosswalks: `Sector_Crosswalk_Census_USATrade.csv` (shared by exports and imports) and direction-specific IEA files `Sector_Crosswalk_BEA_IEA_exports.csv` / `Sector_Crosswalk_BEA_IEA_imports.csv` (`SectorSourceName` `BEA_2017_Code`). `Trade_Exports_2017` selects `BEA_IEA_exports`; `Trade_Imports_2017` selects `BEA_IEA_imports`. Non-financial IEA rows are identical across both files. IEA rows are deepest `TypeOfService` codes whose label names the Detail commodity (or a small family the type spans). Parent totals are omitted when children are mapped (`AllTypesOfService`, `Transport`, `ChargesForTheUseOfIpNie`, `TelecomCompAndInfo`, `OtherBusiness`, `ProfMgmtConsult`, `Travel`, `Financial`, `FinExplicitAndOth`). Crosswalk membership is not chosen by closing 2017 Use/Supply cells. Methods include `BEA_detail_commodity_target.yaml` so FBS output stays on BEA 2017 Detail commodities. Calling FBAs are `TECHNOSPHERE_FLOW` with empty `ActivityConsumedBy`; activity sets select by `FlowName` (and IEA exclusions). Attribution weight sources select Use `F04000` / Supply `MCIF` via `selection_fields`.

### Direction-specific IEA crosswalks

Exports and imports share one Census goods Crosswalk. IEA uses two files because BEA books financial services trade asymmetrically in the 2017 Detail SUT: family `F04000` is spread across four financial commodities (~$132 B); family `MCIF` is almost entirely FISIM on `52A000` (~$6.6 B). IEA reports explicit financial subtypes on imports that have no matching `MCIF` cells.

**Non-financial rows match across both files except the `#606` noncomparable-imports block below** (imports route to `S00300`; exports keep the Detail commodity). Financial services also differ:

| IEA `TypeOfService` | Exports (`BEA_IEA_exports`) | Imports (`BEA_IEA_imports`) | Why they differ |
| --- | --- | --- | --- |
| `Financial` | 1:m → `52A000`, `522A00`, `523A00`, `523900`; split by 2017 Use `F04000` | *unmapped* | Parent total (~$38 B imports) has no SUT `MCIF` home; mapping it dumps mass onto `52A000` (5.7× SUT). |
| `FinFisim` | *unmapped* (included in parent `Financial`) | 1:1 → `52A000` | FISIM imports (~$6.6 B) match published `52A000` `MCIF`. |
| `CipLicensesOutcomesResearchAndDev` | → `533000` (F040-weighted family) | → **`S00300`** | Intangible licensing imports; `533000` `MCIF` is zero ([#606](https://github.com/cornerstone-data/bedrock/issues/606)). |
| `CipLicensesFranchiseFees` / `CipLicensesTrademarks` | → `533000` | → **`S00300`** | Same noncomparable-imports bucket on `MCIF`. |
| `TransportSeaFreight` | → `483000` | → **`S00300`** | Foreign port/vessel services consumed abroad; `483000` `MCIF` is zero ([#606](https://github.com/cornerstone-data/bedrock/issues/606)). |
| `TradeRelated` | → `425000` | → **`S00300`** | Wholesale electronic markets; `425000` `MCIF` is zero. |
| `FinExplicitAndOth` | *unmapped* (included in parent `Financial`) | *unmapped* | Explicit fees (~$31 B imports); no Detail `MCIF` home. |
| `FinCredCardOthCredRelated` | *unmapped* | *unmapped* | Explicit credit services; `522A00` `MCIF` is zero in SUT. |
| `FinSecBrokAndMM` | *unmapped* | *unmapped* | Explicit brokerage; `523A00` `MCIF` is ~$57 M in SUT vs ~$4.8 B in IEA. |
| `FinUwAndPP` | *unmapped* | *unmapped* | Explicit underwriting; no SUT `MCIF` cell. |
| `FinSecLendEftOth` | *unmapped* | *unmapped* | Securities lending / EFT; no SUT `MCIF` cell. |
| `FinFinMan` | *unmapped* | *unmapped* | Financial management; `523900` `MCIF` is zero in SUT. |
| `FinAdvCust` | *unmapped* | *unmapped* | Advisory / custody; `523900` `MCIF` is zero in SUT. |

**Export financial rule:** map parent `Financial` only; proportional split uses frozen 2017 `F04000` weights among the four Detail codes. **Import financial rule:** map `FinFisim` → `52A000` only (direct 1:1; no proportional step).

When adding a new IEA row, update **both** files unless the classification argument is direction-specific (financial services, `#606` / `S00300` imports).

Do not copy `Cornerstone_2025_target.yaml` / `BEA_detail_target.yaml` here — those select NAICS leaves; these Crosswalks emit Detail.

### Crosswalk experiments (rejected)

Prototypes scored on 2017 Trade FBS; not shipped.

**Census goods → `S00300` on imports (`339116`, `33211A`).** A direction-specific Census Crosswalk routed dental-lab and forging NAICS to `S00300` when Detail `MCIF` is zero (~$668 M). That cleared import EXTRA on those codes and moved mass onto `S00300` PARTIAL, but [#606](https://github.com/cornerstone-data/bedrock/issues/606) noncomparable imports covers services produced abroad and IP licensing — not Census merchandise. `intermediate_estimation_plan.md` treats `339116` / `33211A` as concordance noise, not textbook `S00300`. `339116` has nonzero export F040, so BEA books product trade on the export side. Rejected pending a [#658](https://github.com/cornerstone-data/bedrock/issues/658) / [#670](https://github.com/cornerstone-data/bedrock/issues/670) goods-allocation argument or an explicit import unmap.

**`TravelHealth` 1:m across the healthcare Detail family.** Split ~$1,098 M export / ~$639 M import `TravelHealth` across `621100`–`621900`, `622000`, `623A00`, `623B00` by frozen SUT F040/MCIF weights. Family candidate totals were unchanged; L1 error vs published healthcare F040/MCIF was unchanged ($2,704 M exports / $3,935 M imports). Ambulatory/residential MISS became PARTIAL by shifting mass off `622000` without improving fit. IEA `TravelHealth` covers ~29% of published healthcare export F040; the remainder has no IntlServTrade or Census source. TTSA or other `Travel*` decomposition is the path if pursued.

### Overlay (nowcast)

`bedrock.transform.eeio.nowcast._trade_fbs_commodity_vector` aggregates Trade FBS to Detail and drops non-SUT commodity codes (e.g. industry-only `331314`) before writing Use `F04000` and Supply `MCIF`. FBS parquet is the same mapped mass. `S00900` / `F04000` is the Y identity after overlay (not part of the Trade vector). `bedrock.transform.trade.scale.scale_amounts_to_ita` can multiply a Detail series to ITA G+S; nowcast does not call it (#647).

## 2017 nowcast scorecard

Scored from `use_fd_detail_sut` `F04000` and `supply_bridge_detail_sut` `MCIF` (USD), after Trade overlay and the `S00900` identity. Command: `uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail`.

**Overlay mass:** mapped Trade Detail (Census goods + BEA IEA services). `S00900` / `F04000` matches published within 1 M USD via the Y identity. **Benchmark vs SUT (#557 bars):** national ~2-3%; import Pearson >= 0.85 / export >= 0.75-0.85 on non-`S00*`; top-20 Jaccard >= 0.7 / >= 0.6.

Scored against the assembled nowcast columns (`use_fd_detail_sut` F04000 / `supply_bridge_detail_sut` MCIF), not raw Trade FBS vs raw SUT. Rerun: `uv run python -m bedrock.analysis.nowcasting.trade_data.score_2017_trade_detail` (requires GCS auth and Census API key for Inventories FBS).

| direction | National % vs SUT | Pearson all / non-`S00*` | Spearman all / non-`S00*` | Top-20 Jaccard all / non-`S00*` | n_miss |
| --- | --- | --- | --- | --- | --- |
| F040 exports | +12.20% | 0.96 / 0.94 | 0.89 / 0.89 | 0.67 / 0.67 | 40 |
| MCIF imports | +0.58% | 0.75 / 0.85 | 0.95 / 0.95 | 0.74 / 0.82 | 19 |

## Residual / specials

Documented rules; FBS methods do not allocate onto these codes. Holes below are `MISS` cells with `|reference| >= 1` B USD.

- **Unmapped IEA types.** Hierarchy totals (`AllTypesOfService`, `Transport`, `Travel`, `OtherBusiness`, `ProfMgmtConsult`, `TechTradeRelatedOth`, `PersonalCulturalAndRecreational`, ...) stay unmapped when children are mapped. Digitally deliverable cross-cuts (`PotIctEnServ*`) stay unmapped. `Travel` other than `TravelHealth`, transport port services (`TransportAirPort`, `TransportSeaPort`), `GovtGoodsAndServicesNie`, and `OthBusinessNie` have no Detail home on the Crosswalk. Those types do not appear on F040/MCIF.
- **Import EXTRA (goods, MCIF = 0).** `339116` dental laboratories (~522 M), `33211A` forging (~146 M). Census CIF mass lands on Detail codes with zero published `MCIF`. Not routed to `S00300` — see Crosswalk experiments (rejected). Track under [#658](https://github.com/cornerstone-data/bedrock/issues/658) / [#670](https://github.com/cornerstone-data/bedrock/issues/670).
- **Healthcare ambulatory / residential export MISS.** `621100`–`621900`, `623A00`, `623B00` (~$100–181 M F040 per leaf). Crosswalk maps `TravelHealth` → `622000` only (~$1,098 M vs ~$2,815 M hospital ref). No Census or additional IEA leaf hits the ambulatory/residential codes; a 1:m `TravelHealth` split by SUT weights did not improve family-level error (see Crosswalk experiments (rejected)).
- **1:m zero-weight concentration.** When a mapped IEA type fans out to several Detail codes and some have zero Use `F04000` / Supply `MCIF` weight, proportional attribution puts all mass on the positive-weight targets. Examples on the leaf Crosswalk: `MaintenanceAndRepairNie` -> `811100`/`811200`/`811300`/`811400` (import MCIF often only on `811400`); `Insurance` -> `524113`/`5241XX`/`524200` (mass concentrates on codes with positive weight). `OthPersonalCulturalAndRecreational` -> `712000`/`713900` can drop when Supply MCIF is zero on both (see generate warnings). Leaf parents such as `GovtGoodsAndServicesNie` remain **unmapped** (no `491000` row).
- **Census residual NAICS (`*X` / `*XX`).** Census USA Trade publishes suppressed-detail residuals alongside digit-6 NAICS (2017 exports: `33641X` 120,967 M, `31181X`, `31135X`, `31131X`, `11211X`, `1123XX`; imports: the same set except `33641X`). `census_usatrade_parse` keeps `\d{6}|\d{5}X|\d{4}XX`. `Sector_Crosswalk_Census_USATrade` maps each residual 1:m onto the Detail commodities of that family (`33641X` → `336411`/`336412`/`336413`/`336414`/`33641A` by Use `F04000` weights). Dropping residuals at parse left those Crosswalk rows unreachable and understated aerospace exports by ~$121 B.
- **Census `980000`.** No Detail sector in the goods Crosswalk; the NAICS row is omitted from `Sector_Crosswalk_Census_USATrade.csv` and stays unmapped.
- **`S00900` (rest of the world adjustment).** Set at Y assembly: `Y[S00900,F040] = -Y[S00900,F010] + Supply_T016[S00900]` (USD). Not an FBS extract.
- **`S00300` (noncomparable imports).** 260,421 M USD on Supply `MCIF`. Imports Crosswalk routes `CipLicensesOutcomesResearchAndDev`, `CipLicensesFranchiseFees`, `CipLicensesTrademarks`, `TransportSeaFreight`, and `TradeRelated` to `S00300` ([#606](https://github.com/cornerstone-data/bedrock/issues/606)); exports keep the Detail commodity targets. Remaining `S00300` mass is hold-from-Supply / residual allocation, not Trade FBS.
- **Export `MISS` >= 1 B USD.** `492000` couriers (9,411 M), `550000` management of companies (4,296 M), `531ORE` other real estate (3,274 M), `221100` electric power (2,744 M), `722110` full-service restaurants (1,177 M), `722211` limited-service restaurants (1,130 M). Rule: no Census NAICS-6 / residual and no mapped IEA type hits that Detail commodity. (`311810` bakery is filled from `31181X`.)
- **Import `MISS` >= 1 B USD besides `S00300`.** `221100` (2,431 M), `561300` employment services (1,588 M). Same rule. (`311810` and `1121A0` are filled from `31181X` / `11211X`.)
- **Large non-`MISS` gaps (PARTIAL, ~>=10x).** `325413` exports (~63x), `322130` exports (~19x), `325910` imports (~55x), `517110` imports (~36x), `334418` imports (~27x), `334610` imports (~21x), `333112` imports (~12x). These errors are driven by incorrect split/weighting rather than zeroing.
- **Outstanding goods-allocation hypothesis (`#670`).** Large non-`MISS` gaps in some directly mapped goods families suggest the NAICS-based Census goods Crosswalk may not fully reproduce BEA's product-level foreign-trade allocation. The aerospace cluster (`336411`/`336412`/`336413`) was primarily a dropped `33641X` residual, not an HTS surface mismatch — Trade FBS after residual recovery is within ~1.1–1.3× of 2017 F040 on those leaves. Track remaining evidence in [#670](https://github.com/cornerstone-data/bedrock/issues/670).
- **Utilities (no source in Census or IEA).** `221100` electric power (F040 2,744 M / MCIF 2,431 M), `221200` natural gas distribution (F040 559 M), `221300` water/sewage (F040 705 M). Cross-border electricity and gas pipeline flows are not in Census merchandise trade or BEA IntlServTrade. Requires an alternate source (EIA, BEA benchmark methodology); tracked separately.
- **Couriers `492000` (no IEA type).** F040 9,411 M. IEA `TransportPostal` covers postal services and maps cleanly to `491000` (710 M ref, 4% over). Courier/express services (UPS, FedEx, etc.) are not a distinct IEA TypeOfService. Requires an alternate source.
- **Construction maintenance/repair (no source).** `230301` nonresidential (F040 81 M), `230302` residential (18 M). Not merchandise trade; not in IEA services.
- **BEA Detail disaggregations without Census NAICS.** `332710` machine shops (F040 353 M), `332800` coating/engraving/heat treating (F040 179 M) -- standalone BEA Detail codes; no Census NAICS-6 on trade and no IEA type. `332996` fabricated pipe fittings (F040 97 M), `332114` custom roll forming (24 M), `326130`/`326140`/`326150` plastics foams/laminates (26 M combined) -- Census lumps these into parent Detail codes (`33299A`, `33211A`, `326190`); a SUT-proportional split from the parent is available but not applied given the small magnitudes (< 0.1 % of parent F040 for the plastics children).
- **FAS vs PUR.** Census export `ALL_VAL_YR` is FAS-family; Use `F04000` is purchasers' value. These methods do not convert valuation basis commodity-by-commodity.
- **`MDTY`.** Census effective duty rate (`CAL_DUT_YR` / `GEN_VAL_YR` mapped to Detail via `Sector_Crosswalk_Census_USATrade`, 1:m by Supply `MCIF`) times Census goods MCIF from `Trade_Imports`, leveled so the national sum matches NIPA T30500 `B235RC`. Calculated Census duty != collected duty (tariff-era gap). Wired in `derive_initial_supply_bridge` via `bedrock.transform.trade.duties.mdty_detail_usd`.
- **`MADJ`.** Census `GEN_CHA_YR` (import charges) mapped to Detail via the goods Crosswalk, then reassigned onto Detail codes with nonzero 2017 Supply `MADJ` in proportion to those published `MADJ` values (signed shares), leveled so the national sum matches published Supply `MADJ`. That destination mix matches BEA's transport/insurance booking of the wedge (including codes with zero SUT `MCIF`). Does not fill `T013`. Wired via `bedrock.transform.trade.madj.madj_detail_usd`.

## IEA Crosswalk revisions

Candidates to revisit. Trigger is a classification or source argument, not a 2017 cell gap.

- **`BusMgmtConsPubRel` -> `550000`** -- add management of companies only if that NAICS is in the type's definition (consulting / PR). File maps `541610` / `5416A0` only.
- **`CipLicensesBroadcastLiveRecord`** -- `515100` (broadcasting) or `711*` (live events) vs movies `512100` (file).
- **`GovtGoodsAndServicesNie`** -- unmapped. Needs a commodity home that is government n.i.e., not federal electric (`S00101` / `S00102`) and not postal (`491000`).
- **`OthBusinessNie` -> `561*`** -- n.i.e. residual onto admin/support only if a concordance names those NAICS. Mass is several times 561\* F040/MCIF; file leaves the type unmapped.
- **Travel other than `TravelHealth`** -- visitor spend (hotels, food, education travel) via BEA TTSA or leave unmapped. Not Use `F04000` mix (`721000` is 0 in the 2017 SUT).
- **`TravelHealth` → `622000` only** -- a 1:m split across the healthcare Detail family by SUT weights was rejected (see Crosswalk experiments (rejected)); does not close the gap vs published healthcare F040/MCIF totals.
- **Transport port services** -- `TransportAirPort` / `TransportSeaPort` wait for a support-activities Detail code (no `488000` in 2017 commodities) or a written rule that port is the mode commodity (`481000` / `483000`).
- **1:m weights** -- `attribution_method: equal`, or same-year `BEA_Detail_GrossOutput_IO`, vs frozen 2017 F040/MCIF. Equal/GO do not require a later Detail Use/Supply.
- **Financial services** -- direction-specific Crosswalks (`BEA_IEA_exports` vs `BEA_IEA_imports`). Exports: parent `Financial` 1:m by 2017 `F04000`. Imports: `FinFisim` → `52A000` only. Do not map parent `Financial` or explicit `Fin*` leaves onto `MCIF`.
- **Goods coverage** -- `11211X` / `31181X` and other Census residuals are kept in the FBA and mapped (see Residual / specials). `311824` maps to `3118A0` per BEA's NAICS-to-Detail crosswalk (dry pasta/dough/flour mixes, not bakery). `980000` (low-value shipments) stays omitted; no Detail sector.
