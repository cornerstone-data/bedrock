# International trade data options for initial F04000 / F05000 estimates

Working notes for [bedrock#527](https://github.com/cornerstone-data/bedrock/issues/527). Living in `bedrock/analysis/trade_data/` until a source decision lands in #528.

**Parent:** [#526](https://github.com/cornerstone-data/bedrock/issues/526) → implement in [#528](https://github.com/cornerstone-data/bedrock/issues/528)

**Decision (working):** **Option 1 — Census goods + BEA services** is the primary annual extract. **BEA ITA** is the national totals control. **BACI is out of scope** for F040/F050. Consolidated plan: [`plan_527_long.md`](plan_527_long.md).

**Context:** Nowcast initial Use-table final demand needs annual **exports (F04000)** and **imports (F05000)**. Target is close match to the **2017 detailed Use table (exports in PUR)** and Supply-side import valuation (**CIF / MCIF**), with national goods+services totals controlled to ITA.

---

## Intended plan (consolidated)

1. **Extract annually from Option 1:** Census NAICS-6 goods (imports **CIF** per #527 / Supply MCIF; exports FAS-family) + BEA `IntlServTrade` services (imports and exports).
2. **Control national totals to ITA** (Tables 2.1 / 3.1 goods+services) — scale or residual-constrain the combined extract; do not treat a raw Census+BEA sum as the final total.
3. **Implement extract from** [flowsa `imports`](https://github.com/cornerstone-data/flowsa/tree/imports) (`Census_USATrade`, `BEA_IEA`) — not yet in bedrock; USEEIO import-EF downloader is imports-only legacy. Add ITA totals loading as needed.
4. **Map to BEA Detail** with USEEIO Census→Detail and service→Detail concordances (extend the thin service map); bedrock `NAICS_to_BEA_Crosswalk_2017` as backup for goods.
5. **Reconcile structure to 2017 Use F040/F050** (and MCIF for imports): explicit rules for CIF+services overlap, specials (`S00300`/`S00900`), wholesale/margins.
6. **Ship in #528** as FBA/FBS → nowcast F040/F050 using that recipe. BACI is not part of the recipe.
7. **Optionally consider annual summary Use `F040`/`F050`** as an IO-framework commodity-mix cross-check for years BEA has published (YoY trade changes look valid — see [`plan_527_long.md`](plan_527_long.md) note); not a substitute for ITA totals or Census+BEA Detail.

2017 tests support this: goods Detail map is strong; combined totals are in the right ballpark but need ITA (or valuation fixes) to remove CIF+services overshoot; remaining work is valuation overlap, service Detail coverage, and specials — not a different primary source or BACI.

---

## Requirements (from #527)

| Requirement | Implication |
|---|---|
| Match 2017 detailed Use as closely as possible | Prefer BEA detail / NAICS→BEA concordance; reconcile totals to SUT/MUT F040/F050 |
| Exports in PUR (Use table) | Merchandise customs values ≠ purchaser; may need margins / BOP / NIPA bridges |
| Imports comparable to Supply CIF | Census CIF and Supply `MCIF` are CIF-family; customs value and BACI FOB are not |
| Annual series | All candidates publish annually (Census monthly→annual; ITA quarterly/annual; BACI annual) |
| Comparable to BACI | Goods HS/NAICS detail can be mapped; services and valuation still differ |
| Goods **and** services | No single candidate covers both at BEA-detail commodity resolution |

---

## Target accounting concepts (bedrock)

| Concept | Where it lives | Notes |
|---|---|---|
| F04000 Exports of goods and services | Use table FD column | Nowcast leaves F040/F050 zero until #526–#528 |
| F05000 Imports of goods and services | MUT Use FD column (MUT-only) | SUT Supply carries imports as **MCIF** (+ MADJ) in the supply identity |
| PUR Use cells | SUT Use intermediate + FD | Purchaser value; see `About_BEA_IOT_table_valuation_differences.md` |
| MCIF | SUT Supply column | Imports at CIF in the basic-supply bridge: `T013 = T007 + MCIF + MADJ` |
| Benchmark trade structure | BEA Import matrices + Use F040/F050 | Loaded in bedrock IO extract; not an annual trade API |

Bedrock does **not** yet extract Census trade, BEA ITA, or BACI for F040/F050.

---

## Option comparison (summary)

| | **1. Census goods + BEA services** | **2. BEA ITA + NIPA linkage** | **3. BACI (CEPII)** | **4. Combination** |
|---|---|---|---|---|
| **Goods** | Yes — NAICS-6 (partner optional) | Yes — end-use / BOP categories (coarse) | Yes — HS6 bilateral | Goods from 1 or 3; totals from 2 |
| **Services** | Yes — BEA `IntlServTrade` | Yes — ITA Table 3.1 | No | Services from BEA |
| **Imports valuation** | Census **CIF**; services BEA | BOP / national-accounts basis | **FOB** (CEPII reconciled) | CIF goods + BEA services; control to ITA/NIPA |
| **Exports** | Census FAS-family + BEA services (flowsa `imports` FBAs) | Yes (goods + services) | Yes (bilateral FOB) | Prefer Census+BEA extract; optional ITA totals |
| **Commodity detail** | NAICS-6; map to BEA Detail via existing concordances | Coarse without a structure prior | HS6→CEDA/BEA-like sectors | Detail from Census/BEA; structure from 2017 Use |
| **Countries** | Partner-level available | Country/area in some ITA tables; 2.1/3.1 are category series | ~148 CEDA-mapped ISO3 (+ ROW) | Partner detail optional for national FD |
| **Years (practical)** | Census/BEA APIs cover 2012–2023+ | Long annual/quarterly ITA history | CEDA wired **2022–2024** (HS22) | Align to nowcast years |
| **Existing code** | **Preferred extract:** flowsa `imports` FBAs; USEEIO IEF path is imports-only legacy | No bedrock ITA loader | Strong in **ceda**; unused in bedrock | Wire extract → bedrock FBA/FBS |
| **Best role** | **Chosen primary extract** | **National totals control** | Out of scope for F040/F050 | Shell: Opt 1 + ITA totals + Use structure |

---

## flowsa `imports` branch — extraction shifted here (not in bedrock yet)

Option 1 download logic was **reimplemented as flowsa FBAs** on [cornerstone-data/flowsa `imports`](https://github.com/cornerstone-data/flowsa/tree/imports) (diverged from flowsa `master`; **not merged**).

| FBA | What it pulls | vs USEEIO import-EF downloader |
|---|---|---|
| `Census_USATrade` | Census intl trade **imports and exports** by NAICS-6; imports CIF, exports FAS-family | USEEIO path is **imports only** |
| `BEA_IEA` | BEA `IntlServTrade` **imports and exports** by service type × country | USEEIO path is imports-only; flowsa includes exports |
| `Census_USATrade_Construction` | State NAICS / HS subset for construction materials | Side path; not needed for national F040/F050 |

### Is it pulled into bedrock?

**No.** No Census USA Trade or BEA IEA extractors under `bedrock/extract/`; nothing in NIPA FD FBS or nowcast consumes them. Local flowsa may track the remote `imports` branch without checking it out.

For #527/#528, treat **flowsa `imports`** as the preferred extract home for Census goods + BEA services. USEEIO remains useful for **concordances** and IEF history.

---

## USEEIO `imports` branch (legacy download path)

Tip commits on USEEIO `imports` (also present on cornerstone `master` for that file):

1. **Per-API country lists** — Census and BEA country codes loaded separately (not a shared tuple threaded through callers).
2. **NAICS retain option** — Census goods can stay at NAICS before aggregating to BEA Detail (better for BACI/HS joins and concordance diagnostics).

Still **imports-only** on that USEEIO path (no export twin). Prefer flowsa `imports` FBAs for F040/F050 extract work.

---

## Option 1 — Census Trade in Goods + BEA trade in services

### What it is

- **Goods:** Census International Trade API — NAICS-6; imports **CIF** (YTD through December); exports FAS-family totals.
- **Services:** BEA `IntlServTrade` by type of service (and optionally area/country).

### Coverage

| Dimension | Goods (Census) | Services (BEA) |
|---|---|---|
| Flow | Imports and exports (flowsa FBAs) | Imports and exports (flowsa FBAs) |
| Classification | NAICS-6; map to BEA Detail via concordance | Service types → BEA Detail via concordance (thin — see validation) |
| Geography | National aggregate or partner countries | AllCountries or partner/area |
| Time | Annual from monthly API | Annual |
| Valuation | Imports CIF; exports FAS-family | BEA service values (API million USD) |

### Existing tools

| Location | Role |
|---|---|
| flowsa `imports` — `Census_USATrade`, `BEA_IEA` | Preferred goods + services FBAs (imports **and** exports) |
| USEEIO import-EF concordances | NAICS → BEA Detail; service API type → BEA Detail |
| bedrock `NAICS_to_BEA_Crosswalk_2017` | Broader NAICS↔BEA Detail (also usable for goods) |

**Gaps:** flowsa branch not merged into bedrock’s flowsa revision; no PUR bridge; service→Detail concordance incomplete; no FBS yet for these FBAs.

### Fit to SUT

Validated locally for 2017 (see below): national totals within ~12–14% of Use F040/F050 before Detail mapping; after mapping, import totals align closely while structure is only moderate. Not a drop-in replacement for Use columns.

---

## Option 2 — BEA ITA accounts with NIPA linkage

### What it is

- [ITA Table 2.1](https://apps.bea.gov/iTable/?ReqID=62&step=1) — U.S. International Trade in **Goods** (BOP).
- [ITA Table 3.1](https://apps.bea.gov/iTable/?ReqID=62&step=1) — International Trade in **Services**.
- [Linkage table: ITA → NIPA foreign transactions](https://apps.bea.gov/iTable/?reqid=19&step=2&isuri=1&categories=survey).

See BEA’s [primer on U.S. international economic accounts](https://apps.bea.gov/scb/issues/2021/07-july/0721-iea-primer.htm).

### Coverage

| Dimension | Content |
|---|---|
| Goods + services | Both, BOP basis |
| Classification | Coarse vs BEA IO Detail |
| Geography | Detail in other ITA tables; 2.1/3.1 mainly category series |
| Time | Quarterly and annual |
| Valuation | Closest to **NIPA** foreign-transaction totals |

### Existing tools

bedrock NIPA extract covers PCE/gov/investment FBAs — **not** ITA trade tables. No dedicated ITA module in bedrock.

### Fit to SUT

Strong for **control totals**. Weak alone for 400-commodity F040/F050. Published ITA 2017 G+S totals already differ from Use F040/F050 (~8% exports, ~5% imports).

---

## Option 3 — BACI (CEPII), as used in ceda — **out of plan scope**

Evaluated and **not used** for F040/F050. Kept for the options record only.

CEPII **BACI**: bilateral merchandise trade, HS 6-digit; reconciled **FOB**. ceda loads HS22 for **exporter shares** (absolute USD discarded); years **2022–2024**; no services. Poor fit for US Use F040/F050 levels (FOB, goods-only, not ITA/IO-aligned). The intended plan does not depend on BACI for validation or structure.

---

## Option 4 — Combination (shell around Option 1)

Option 1 is the primary extract. The production shell adds **ITA as the national totals control** and Use F040/F050 as the 2017 structure/specials benchmark. BACI is not in the shell.

```
┌─────────────────────────────────────────────────────────────┐
│ Control totals: BEA ITA 2.1 / 3.1 (goods + services)        │
│ Structure / specials: 2017 Use F040/F050 (+ MCIF as needed) │
└────────────────────────────┬────────────────────────────────┘
                             │ scale / residual / structure prior
┌────────────────────────────▼────────────────────────────────┐
│ Annual commodity detail:                                    │
│   Goods: Census CIF imports + FAS-family exports (NAICS)    │
│   Services: BEA IntlServTrade                               │
│   Map to BEA Detail                                         │
└─────────────────────────────────────────────────────────────┘
```

Local 2017 tests support **Option 1 as the core extract** with **ITA totals control**: raw Census+BEA totals are in the right ballpark but systematically high on imports (CIF + services overlap); Detail mapping helps import totals but export **structure** remains weak without a richer service concordance or a Use-based prior for specials/margins.

---

## Existing tooling inventory

### bedrock (today)

| Asset | Trade relevance |
|---|---|
| IO 2017 extract / derived Y | Benchmark Use F040/F050, Import matrices |
| USEEIO nowcast `U_imports` | Import **Use** structure (not trade API) |
| Nowcast Y | F040/F050 placeholders |
| NIPA extract | Other FD — not ITA trade |
| Census extract | Domestic econ stats — not intl trade |
| `NAICS_to_BEA_Crosswalk_2017` | Goods NAICS → BEA Detail |

### ceda

| Asset | Trade relevance |
|---|---|
| `vanilla/baci/*`, OECD expansion | Bilateral shares for MRIO |
| HS → CEDA mappings | Goods concordance |

### flowsa (`imports` branch — not in bedrock)

| Asset | Trade relevance |
|---|---|
| `Census_USATrade`, `BEA_IEA` | Preferred annual goods+services extract |

### USEEIO

| Asset | Trade relevance |
|---|---|
| Import-EF download path | Legacy imports-only Census+BEA |
| Census → BEA Detail concordance | Strong goods map (~97% of 2017 Census import value) |
| Service API type → BEA Detail concordance | Thin (10 types; ~70% of AllTypesOfService) |

### Not found

- Census USA Trade / BEA IEA inside **bedrock** (or on flowsa `master`)
- bedrock FBA/FBS for ITA Tables 2.1/3.1
- BACI loaders inside bedrock
- Complete service-type → BEA Detail coverage for travel, IP charges, etc.

---

## 2017 validation — what was tested

Exact cell-for-cell match of Use F040/F050 from Census+BEA alone is **not** a realistic bar. Official BEA series already disagree with each other at the national total.

### Benchmark inventory (from bedrock IO tables)

Million USD, 2017:

| Series | Total | Basis |
|---|---:|---|
| Use MUT `F04000` (exports) | 2,082,970 | PRO |
| Use SUT `F04000` (exports) | 2,082,984 | PUR — national total ≈ PRO |
| Use MUT \|`F05000`\| (imports) | 2,626,305 | PRO |
| Supply `MCIF` | 2,649,430 | BAS / CIF-family |
| ITA goods+services exports | ~2,263,907 | BOP |
| ITA goods+services imports | ~2,764,352 | BOP |

Use F040 / ITA exports ≈ **0.92**; Use \|F050\| / ITA imports ≈ **0.95**; Use \|F050\| / MCIF ≈ **0.991**.

### National extract totals (Census goods + BEA services)

Pulled calendar-2017 national Census NAICS-6 goods (CIF imports, FAS-family exports) and BEA `IntlServTrade` `AllCountries` / `AllTypesOfService` (not a sum of hierarchical service rows).

| Series | M USD | vs Use \|F050\| | vs Use F040 |
|---|---:|---:|---:|
| Census goods imports (CIF) | 2,406,065 | 0.92 | — |
| BEA services imports | 534,852 | 0.20 | — |
| **Combined imports** | **2,940,917** | **1.12** | — |
| Use \|F050\| | 2,626,305 | 1.00 | — |
| Supply MCIF | 2,649,430 | 1.01 | — |
| ITA imports G+S | 2,764,352 | 1.05 | — |
| Census goods exports | 1,547,195 | — | 0.74 |
| BEA services exports | 836,352 | — | 0.40 |
| **Combined exports** | **2,383,547** | — | **1.14** |
| Use F040 | 2,082,970 | — | 1.00 |
| ITA exports G+S | 2,263,907 | — | 1.09 |

Findings:

- Combined extracts are in the right ballpark (~12–14% above Use columns), not noise.
- Combined **imports** exceed Use, MCIF, and ITA — consistent with **CIF goods + full services overlapping** freight/insurance (Census CIF includes charges that BOP often records in services).
- Combined **exports** are closer to ITA than to Use F040 (Use is the lower series).

### BEA Detail mapping (goods + services → F040/F050 vectors)

Mapped Census NAICS with the USEEIO Census→BEA Detail concordance, and BEA service API types with the USEEIO service→Detail concordance (equal split on 1:m, same rule as the USEEIO import-EF path). Compared commodity vectors to Use F040/F050.

**Concordance coverage (mapping success, not Use alignment)**

| Piece | Source M USD | Mapped share |
|---|---:|---:|
| Census goods imports | 2,406,065 | **0.970** (1 unmapped NAICS) |
| Census goods exports | 1,547,195 | **0.922** (2 unmapped) |
| BEA services in concordance (10 API types) | 374,115 of 534,852 AllTypes | 1.0 of those types; **~0.70 of AllTypes** (~0.65 on exports) |

Goods NAICS mapping is only mildly worse for exports (92% vs 97%). That is **not** why export Detail looks much worse overall.

**Vector vs Use** (mapped goods + mapped services only)

| Flow | Extract / Use | Pearson | Spearman | Top-20 Jaccard | Use mass with extract = 0 |
|---|---:|---:|---:|---:|---:|
| Imports | **1.008** | 0.60 | 0.69 | 0.54 | **10%** |
| Exports | **0.945** | 0.34 | 0.49 | 0.21 | **27.5%** |

Dropping special IO codes (`S00xxx`) raises import Pearson to ~0.81 and export Pearson only to ~0.53 — so the export gap is not just `S00900`.

### Why export Detail looks much worse than import Detail

**Short answer:** Export *NAICS concordance coverage* is fine. Export *alignment to Use F040* is weak because a large share of Use export dollars sits in sectors the Census goods extract + thin service map never fill — and services matter more for exports than for imports.

1. **Empty Use mass, not failed NAICS joins.** 27.5% of Use F040 value has extract = 0, vs 10% of Use |F050|. Imports' main hole is one special code (`S00300` noncomparable imports, ~260B). Exports have `S00900` (RoW, ~204B) **plus** a long tail of missing sectors.

2. **Largest Use export cells with no extract** include:
   - `S00900` — Rest-of-world / IO export bucket (~204B)
   - `533000` — lessors of intangible assets / IP-royalties-like (~73B) — service concordance does not cover charges for IP use
   - Wholesale trade (`423*`, `424A00`, …) and truck transport (`484000`) — margin / trade activity in Use that merchandise NAICS trade does not line up with 1:1

3. **Services are a larger share of exports.** Combined Census+BEA extract is ~35% services on exports vs ~18% on imports. The service→Detail map only has 10 API types and omits travel, IP charges, and other large export categories — so export Detail is starved where Use is rich.

4. **Goods-only structure is not the failure mode.** Census→Detail mapping works for both flows; the residual export miss after removing `S00xxx` is still services / wholesale / transport shape, not random NAICS failures.

Implication: improving confidence on 2017 recreation is mostly **fill the Use-export holes** (richer service map, treatment of special IO codes and margins) and **fix import valuation overlap** (CIF + services), not rebuild the goods NAICS concordance.

### `compare()` matched vs unmatched (same Detail extract)

Re-ran the Detail vectors against Use MUT F040/F050 with `compare_NIPA_to_IOT.compare()` (code match). This separates **matched-cell disagreement** from **unmatched mass** — which Pearson alone conflates.

| | Imports | Exports |
|---|---|---|
| Extract vs Use national total | ≈ −1% | ≈ −6% |
| Matched cells | 305 (all by code) | 305 (all by code) |
| Matched extract vs matched Use | **+12%** | **+30%** |
| Reference-only (Use, no extract code) | **~12%** of Use (~324B) | **~28%** of Use (~573B) |
| Cells within 5% | 69/305 | 10/305 |

Top reference-only: imports `S00300` (~260B) + `4200ID` duties (~39B); exports `S00900` (~204B), `533000` (~73B), wholesale / truck / rail. Worst matched cells include vehicle NAICS splits, aircraft under-fill (exports), thin service equal-splits, couriers / used goods.

**Read:** Code coverage is fine. Totals can look acceptable while matched commodities overshoot and specials/margins soak the gap — so ITA scale alone will not fix structure. Specials policy + service map first; then totals control.

### Confidence after these tests

| Option | Confidence for 2017 reproduction | Updated read |
|---|---|---|
| **1. Census + BEA services** | **Medium** totals; **medium** import structure; **low–medium** export structure | Right source family; goods map is strong; service map + special IO codes explain export Detail weakness |
| **2. ITA + NIPA alone** | High totals; low detail | Still needs a commodity prior |
| **3. BACI alone** | Low | Out of scope for F040/F050 plan; goods-only FOB |
| **4. Combination** | **High as shell around Opt 1** | Opt 1 extract + **ITA totals control** + Use structure/specials; fix service map |

**Recommendation for #527:** **Option 1 is the primary bet** (Census goods + BEA services), implemented from flowsa `imports` FBAs. **ITA Tables 2.1/3.1 control national goods+services totals.** Treat 2017 Use F040/F050 as the structure/specials benchmark; keep Census **CIF** when targeting Supply MCIF unless the ITA control step sets levels. Expand the service→Detail concordance and decide explicit rules for `S00300` / `S00900` / wholesale margins. **Do not rely on BACI.** See [`plan_527_long.md`](plan_527_long.md).

### Not yet tested

- Full partner-country FBA generation via flowsa `imports` (national aggregates were used for speed)
- ITA Tables 2.1/3.1 download into bedrock (needed for the totals control step)
- PUR conversion / margins on exports
- Customs-value goods imports (vs CIF) to reduce double-count with services
- Years other than 2017

---

## Path to confidence: recreate 2017 F040/F050

**Definition of "pretty confident"** (proposed acceptance bar, not yet met):

| Check | Target |
|---|---|
| National import total vs Use F050 (abs) and/or MCIF | Within ~2–3% after valuation fixes (or after explicit scale-to-Use) |
| National export total vs Use F040 | Within ~2–3% after same |
| Commodity import vector vs Use F050 | Pearson ≳ 0.85 on non-special codes; top-20 Jaccard ≳ 0.7; document residual on `S00300` |
| Commodity export vector vs Use F040 | Pearson ≳ 0.75–0.85 on non-special codes; top-20 Jaccard ≳ 0.6; document residual on `S00900` / margins |
| Known holes | Written rules for specials, unmapped services, CIF↔services overlap — not silent zeros |

If raw extract cannot hit those bars, confidence still holds if we **prove** a simple, documented pipeline (e.g. extract structure × scale to Use totals, or extract + Use residual on specials only) recovers 2017 within the same bars — that pipeline is then what nowcast carries forward.

### What to do next (ordered)

1. **Lock the target series.** Treat Use MUT F040/F050 as the 2017 structure/specials truth; treat **ITA G+S** as the national totals control. Record PRO vs PUR and Use-vs-ITA gaps (~8% exports / ~5% imports in 2017).

2. **Fix import valuation overlap, then apply ITA control.** Re-run national and Detail compares with Census **customs** (or CIF minus estimated freight/insurance) + BEA services as needed, then scale/constrain combined imports (and exports) to ITA. Goal: remove the systematic ~12% import overshoot before chasing commodity noise.

3. **Expand service→BEA Detail mapping.** Add the missing IntlServTrade types that drive Use exports (charges for IP use → `533000`-family, travel, etc.). Re-measure export Pearson / empty Use mass. Highest-leverage structural fix for exports.

4. **Explicit specials and margins policy.** Decide whether `S00300` / `S00900` are (a) taken from Use and held fixed in nowcast, (b) allocated from residuals after mapped commodities, or (c) out of scope for the trade extract. Same for wholesale / margin-heavy export cells that Census NAICS will not recreate.

5. **Goods-only and services-only diagnostics.** Split the Detail compare so goods and services are scored separately. Confirms goods are good enough and isolates remaining error to services/specials.

6. **Document the 2017 reconstruction recipe.** One page: sources, valuation, concordances, specials rules, **ITA scale/residual step**, and the acceptance metrics above. That recipe is what #528 implements for nowcast years.

7. **Only then wire production.** Merge/vendor flowsa `imports` FBAs + ITA totals into bedrock's path; FBA→FBS→nowcast F040/F050 using the documented recipe.

### What "keep working" would look like in practice

Stay in `bedrock/analysis/trade_data/` until the acceptance table is green (or the recipe+residual path is green). Parallel tracks:

- **Valuation / ITA track:** CIF vs customs, then scale to ITA G+S (mostly imports).
- **Services track:** richer TypeOfService → BEA Detail (mostly exports).
- **Specials track:** policy for `S00300`/`S00900`/wholesale so zeros are intentional.
- **Proof track:** one final 2017 reconstruction table (commodity-level extract vs Use, with metrics) in the notes.

Stop when a reviewer can reproduce 2017 Use trade columns from the recipe within the bars above without hand-editing cells.

---

## Decision criteria

| Criterion | Opt 1 (primary) | Opt 2 (ITA) | Opt 3 (BACI, unused) | Opt 4 (shell) |
|---|---|---|---|---|
| 2017 detail match | Medium (goods strong; services/specials weak on exports) | Low alone | Low–medium goods only | Same as Opt 1 + ITA totals + Use structure |
| Annual | High | High | High (recent years) | High |
| Goods+services | High | High (coarse) | Fail | High |
| CIF / PUR path | Best CIF imports; PUR still open | Best BOP/NIPA totals | FOB only | ITA controls totals |
| Code reuse | Highest (flowsa + concordances) | Low (no bedrock ITA loader yet) | High in ceda (unused here) | Medium |
| Role in plan | Primary extract | **Totals control** | Out of scope | Production recipe |

---

## Open questions before #528

1. **Benchmark-year initials:** Use 2017 F040/F050 as truth and only *nowcast* with Census+BEA structure, or rebuild 2017 from the extract?
2. **Import valuation vs ITA control:** Keep Census CIF for commodity shape and scale national totals to ITA, or switch customs / CIF-minus-freight before the ITA step?
3. **CIF + services double-count:** Handled primarily by ITA totals control; still choose customs vs CIF for the pre-control vector.
4. **Service→Detail concordance:** Extend the 10-type map (IP, travel, …) or allocate unmapped services with 2017 Use shares?
5. **Specials / margins:** Hold `S00300`/`S00900`/wholesale from Use, or force the extract to invent them?
6. **Export PUR:** Accept FAS/BOP provisionally, or apply margins before F040?
7. **Where code lands:** Merge flowsa `imports` into the flowsa revision bedrock uses; add ITA totals; then FBA/FBS in #528?

---

## Suggested next steps

1. Decide #527 source-of-record: **Option 1 primary + ITA totals control** (done as working decision); confirm acceptance bars in "Path to confidence."
2. Run the valuation + services + specials tracks until 2017 reconstruction meets those bars (commodity shape vs Use; national totals vs ITA).
3. Implement per [`plan_527_long.md`](plan_527_long.md) in #528 (flowsa `imports` FBAs → Detail map → ITA control → nowcast).

---

## References

- Issue [#527](https://github.com/cornerstone-data/bedrock/issues/527), [#526](https://github.com/cornerstone-data/bedrock/issues/526), [#528](https://github.com/cornerstone-data/bedrock/issues/528)
- Plan context: `.claude/plan/nipa_sut_nowcast.md`
- Valuation: `bedrock/analysis/compare_NIPA_to_IOT/About_BEA_IOT_table_valuation_differences.md`
- flowsa `imports`: [tree](https://github.com/cornerstone-data/flowsa/tree/imports)
- USEEIO import-EF concordances: [concordances/](https://github.com/cornerstone-data/USEEIO/tree/master/import_emission_factors/concordances)
- Census API: [International Trade datasets](https://www.census.gov/data/developers/data-sets/international-trade.html)
- CEPII BACI: [product page](https://www.cepii.fr/DATA_DOWNLOAD/baci/doc/baci_webpage.html)
- ceda BACI: `ceda/vanilla/baci/`
