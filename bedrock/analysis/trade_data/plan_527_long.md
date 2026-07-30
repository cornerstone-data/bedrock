# Trade data for F04000 / F05000 — intended plan (long)

Working notes with validation detail (annual-summary IO check, `compare()` results). A shorter posting draft may live locally as `plan_527.md` (gitignored).

**Issue:** [#527](https://github.com/cornerstone-data/bedrock/issues/527) (parent [#526](https://github.com/cornerstone-data/bedrock/issues/526) → implement [#528](https://github.com/cornerstone-data/bedrock/issues/528))

**Decision:** Use **Option 1 — Census trade in goods + BEA trade in services** as the primary annual extract. Scale / constrain national totals to **BEA ITA** goods+services controls. **BACI is out of scope** for this F040/F050 work (no required validation or structure prior).

Background and 2017 test results: [`trade_data_source_options_527.md`](trade_data_source_options_527.md).

---

## What we are building

Annual initial estimates for Use-table **F04000 (exports)** and **F05000 (imports)** for nowcast years, aligned as closely as practical with the **2017 detailed SUT/MUT** (exports PUR; import valuation consistent with Supply **CIF / MCIF**), with national goods+services totals controlled to **ITA**.

## Source roles

| Role | Source | Use |
|---|---|---|
| **Primary extract (goods)** | Census International Trade (NAICS-6) | Imports **CIF** (`GEN_CIF_YR`) to match Supply CIF; exports FAS-family (`ALL_VAL_YR`). Customs (`GEN_VAL_YR`) available if combining with services needs a no-double-count diagnostic. |
| **Primary extract (services)** | BEA `IntlServTrade` | Imports and exports by type of service |
| **National totals control** | BEA ITA Tables 2.1 / 3.1 (+ NIPA foreign linkage as needed) | Scale or residual-constrain combined Census+BEA to ITA goods+services export/import totals |
| **Sector bridge** | USEEIO import-EF concordances (+ bedrock NAICS↔BEA Detail) | NAICS → BEA Detail (strong); service type → BEA Detail (thin today — extend) |
| **2017 truth / residuals** | Bedrock Use F040/F050, Supply MCIF | Benchmark shape and specials (`S00300`, `S00900`, wholesale/margins) |
| **Consider: annual summary Use** | BEA summary MUT `F040` / `F050` (~71 commodities), already in bedrock | Optional IO-framework commodity mix / cross-check for years with published annual IO (see note below) |

**Not used:** CEPII BACI / ceda BACI — FOB, goods-only, share-oriented; not needed for US F040/F050 levels or ITA-controlled totals.

### Note: annual summary IO trade columns — are YoY changes valid?

**Worth considering**, especially as an IO-aligned commodity-mix check for years BEA has already published annual summary Use tables. They are **not** a substitute for Census+BEA Detail structure or for ITA as the national totals control.

**Method (BEA):** [Concepts and Methods of the U.S. I-O Accounts](https://www.bea.gov/resources/methodologies/concepts-methods-io-accounts), Ch. 7 — ITAs are the **primary source** for I-O foreign trade; merchandise comes from Census FT-900 / HTS→I-O concordance; services from ITA categories (some service splits still use historical distributions when current industry detail is thin). I-O gross exports/imports are **lower** than NIPA/ITA because BEA removes reexports/reimports and certain overseas U.S. government activities so that gross trade reflects what enters/leaves domestic supply (net exports match NIPA).

**Do not confuse with the import *matrix*:** [BEA FAQ](https://www.bea.gov/help/faq/453) — annual changes in *industry use of imports* are imputed (proportional import / domestic-supply assumption). That caveat applies to `Import_summary` / U_imports structure, **not** to the Use final-demand **F040/F050 columns** themselves.

**Empirical check (bedrock `load_summary_Ytot_usa`, 2012–2023 vs NIPA T10105 exports/imports):**

| | Exports (F040 vs NIPA) | Imports (\|F050\| vs NIPA) |
|---|---|---|
| YoY % correlation | **~0.99** | **~0.99** |
| Level ratio (summary / NIPA) | ~0.77–0.81 | ~0.79–0.83 |

YoY moves show the expected trade shocks (2015 dollar strength, 2020 drop, 2021–22 rebound). Levels sit systematically below NIPA, consistent with the I-O adjustments above — so use summary tables for **direction and commodity mix**, not as a drop-in replacement for ITA control totals.

**Practical role in this plan:** Optional. For published annual IO years, compare Census+BEA→Detail rolled to summary against published `F040`/`F050`. Prefer ITA for totals control; prefer Census+BEA for Detail / post-publication nowcast years. Commodity-mix confidence is high for goods (HTS-driven), medium for services (some historical splits).

## Where to pull implementation from

| Piece | Pull from | Notes |
|---|---|---|
| Goods + services download (imports **and** exports) | [flowsa `imports`](https://github.com/cornerstone-data/flowsa/tree/imports) — `Census_USATrade`, `BEA_IEA` | Preferred. **Not** in bedrock or flowsa `master` yet. |
| ITA control totals | BEA ITA 2.1 (goods) + 3.1 (services); optional NIPA foreign-transactions linkage | No bedrock ITA loader yet — add or call BEA API for G+S totals |
| Legacy imports-only reference + concordances | [USEEIO `import_emission_factors/`](https://github.com/cornerstone-data/USEEIO/tree/master/import_emission_factors) — especially `concordances/Census_to_useeio2_sector_concordance.csv`, `BEA_service_to_useeio2_sector_concordance.csv` | Goods map ~97% of Census import value; service map only ~10 API types |
| Broader NAICS↔BEA Detail | `bedrock/utils/mapping/naics/NAICS_to_BEA_Crosswalk_2017.csv` | Already in bedrock |
| 2017 F040/F050 / MCIF | Bedrock IO extract / Use and Supply tables | Benchmark for structure and specials |

**Do not** treat USEEIO’s `download_imports_data.py` as the production extract — it is imports-only. Prefer flowsa `imports` FBAs, then map with USEEIO/bedrock concordances.

## Pipeline (conceptual)

```
Census goods (CIF imports, FAS exports) ─┐
                                         ├─→ map to BEA Detail ─→ commodity vectors
BEA IntlServTrade (imp + exp) ───────────┘         │
                                                   ├─ specials / margins from Use or residual policy
                                                   ├─ scale / constrain national totals to ITA G+S
                                                   └─ nowcast years via same recipe (#528)
```

## Known gaps to close before “recreates 2017”

1. **CIF goods + full services** can overshoot Use/MCIF (~12% in 2017 tests) — ITA totals control (and/or customs vs CIF) addresses the national overshoot; keep CIF when targeting Supply MCIF (#527) unless the control step replaces levels.
2. **Service→Detail map is thin** — export Use has large holes (`533000` IP-like, travel, etc.); extend concordance.
3. **Special IO / margin cells** (`S00300`, `S00900`, wholesale) — explicit hold-from-Use or residual policy.
4. **Exports PUR** — FAS ≠ PUR; margins/BOP bridge still open.
5. Wire flowsa `imports` + ITA totals into bedrock’s extract/FBA path, then FBA/FBS → nowcast.

### 2017 `compare()` check (matched vs unmatched)

Census+BEA→Detail vs Use MUT F040/F050 via `compare_NIPA_to_IOT.compare()` (code match). National totals can look close while **matched cells overshoot** and **Use-only holes** absorb the gap:

| | Imports | Exports |
|---|---|---|
| Extract vs Use total | ≈ −1% | ≈ −6% |
| Matched cells (by code) | 305 | 305 |
| Matched extract vs matched Use | **+12%** | **+30%** |
| Use mass with no extract code (reference-only) | **~12%** (~324B) | **~28%** (~573B) |
| Cells within 5% | 69/305 | 10/305 |

Largest **reference-only** (policy / map gaps, not NAICS failures):

- Imports: `S00300` noncomparable imports (~260B), `4200ID` customs duties (~39B)
- Exports: `S00900` RoW (~204B), `533000` IP/intangibles (~73B), wholesale `423*`/`424*`, truck/rail

Largest **matched-cell** disagreements: vehicle NAICS splits, aircraft under-fill on exports, thin service equal-splits (`541610`/`541700`), couriers / used goods overshoot.

**Implication:** Concordance coverage is fine. Remaining work is specials policy, richer service→Detail map, then ITA totals control — not a different goods source.

## Implementation order (#528)

1. Vendor/merge flowsa `Census_USATrade` + `BEA_IEA` into bedrock’s extract path; add ITA 2.1/3.1 totals for the control step.
2. Apply goods + services → BEA Detail maps; document valuation, specials, and ITA scale/residual rules.
3. Prove 2017 reconstruction: commodity shape vs Use F040/F050 (and MCIF as needed); national totals vs ITA.
4. Hook into nowcast initial Y for non-benchmark years.
