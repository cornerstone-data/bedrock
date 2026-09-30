# Who-buys search (2018–2021 gap)

**search_date:** 2026-09-29

**Fitness target:** receipts (or revenue) by **class of customer** at **6-digit waste NAICS** (`562*`), comparable to workbook Step 3 / EC `ecnclcust`. Purchaser refuse toward aggregate `562`/`562000` fails. Waste×waste shipper→receiver mass is **intersection**, not who-buys/FD.

**outcome:** `freeze_confirmed`

**rationale:** Across all three required search legs, nothing meets quick-wire fitness for intercensal **2018–2021** (6-digit `562*` seller receipts by customer class that can land in this Flip PR). The only primary who-buys source remains EC `ecnclcust` (2012/2017/2022 already wired). Settled #3 for this gate = **bare EC 2017 freeze** for 2018–2021. Legs 2–3 were searched; all non-EC candidates fail or are N/A for who-buys.

---

## sources_checked

### (1) External / literature (re-check + new since 2026-09-22)

Re-verified table in `feasibility_multi_year_weights.md` § "Alternate who-buys search (2026-09-22)". Optional Phase 1 artifact `cache/who_buys_alt_sources_search.json` was not in the tree at search time; conclusions were re-checked live against the feasibility table and Census API where applicable.

| Source | Pass / fail / fit | Notes |
|--------|-------------------|--------|
| **EC `ecnclcust` (2017 / 2022)** | **Pass — primary (already wired)** | Census years only (ending in 2/7). Covers 2017 and 2022+ via `resolve_ec_year`; **does not fill 2018–2021**. |
| **AIES56CLASS (2023–2024)** | **Fail** | Live Census API query 2026-09-29 (`aiesmiscsector`, `RCPT_BUS_DVAL` / `RCPT_LEISURE_DVAL` / `RCPT_TOT_VAL`): **zero `562*` NAICS** in both 2023 and 2024; only `56133`, `56151`, `56152`, `561599` (matches FTP probe). Also wrong class taxonomy vs EC (business/leisure/billing — not household/fed/S&L/NFP). |
| **SAS Table 8** | **Fail** | Bedrock `Census_SAS.yaml`: Table 8 is truck (`484`) product × class-of-customer allocator only — not waste-child who-buys. |
| **QSS** | **Fail / absent** | No bedrock extract. Aggregate sector/subsector class-of-customer at best — not 6-digit `562*`. |
| **BLS QCEW** | **Fail** | Employment/wages; no receipts by customer class. |
| **Nowcast refuse / AIES `EXPS_REFUSE` / ASM/EC `PCHRFUS`** | **Fail** | Purchaser spend toward aggregate waste commodity — wrong economic object vs seller `ecnclcust`. |
| **BEA detail IO / GDP-by-industry** | **Fail** | Aggregate waste only. |
| **SUSB annual** | **Fail** (new check) | Receipts only in Economic Census years; annual files lack class-of-customer. |
| **CES-WP-25-66 (2025)** “Class of Customer” EC paper | **N/A / not a data wire** | Confirms EC class-of-customer is establishment-level and publicly tabulated for **selected EC sectors/years** — not an intercensal 2018–2021 microdata product we can wire. |

**New since 2026-09-22:** No public 6-digit waste who-buys series for 2018–2021 found. AIES56CLASS remains published for 2023/2024 but still has **no `562*`**.

### (2) Bedrock-already-extracted / used

| Source | Pass / fail / fit | Notes |
|--------|-------------------|--------|
| `Census_EC` / `ecnclcust` + `ec_waste_customer_class.py` | **Pass — baseline** | Wired 2012/2017/2022 → fixed 9 customer-class Use/FD codes. `resolve_ec_year(2018–2021)→2017`. |
| `Census_SAS` Tables 2/3 | **Fail for who-buys** | Industry-mix / revenue by child NAICS — not customer class. |
| `Census_SAS` Table 8 | **Fail** | Truck `484` only (see above). |
| `Census_AIES_Waste_Child_Expenses` (EXP01/BASIC) | **Fail for who-buys** | Child `EXPS_TOT_DVAL` = industry mix successor to SAS Table 3. |
| AIES56CLASS (via miscsector API; not a dedicated bedrock YAML) | **Fail** | Re-queried; no `562*` (leg 1). |
| `Census_AIES_Service_Expenses` / `Census_AIES_Expenses` / `Census_ASM_Expenses` / `Census_EC_Expenses` refuse lines | **Fail** | `EXPS_REFUSE_VAL` / `PCHRFUS` → aggregate purchaser refuse. |
| QSS | **Absent** | No extract under `bedrock/extract/**`. |
| Bundled 2017 weight CSVs / `derive_waste_weights` | **Fail as new who-buys** | Carry EC 2017 (or 2022 when resolved); no intercensal who-buys series beyond EC freeze. |
| Other extract/transform hits (`EPA_WFR`, food-waste FBS, `EPA_REI_waste`, CalRecycle) | **Fail / N/A** | Generation, composition, or disposition pathways — not national 6-digit seller receipts by customer class. |

### (3) Cornerstone stewi / stewiFBS inventories

Pinned package: `StEWI` from [`cornerstone-data/standardizedinventories`](https://github.com/cornerstone-data/standardizedinventories) (`pyproject.toml` git dep; installed env reports StEWI **1.2.2**).

| Source | Pass / fail / fit | Notes |
|--------|-------------------|--------|
| stewi `FLOWBYFACILITY` schema | **Fail for who-buys** | Fields: `FacilityID`, `FlowName`, `Compartment`, `FlowAmount`, `Unit`, `DataReliability`. **No** customer-class / buyer NAICS / class-of-customer columns. |
| stewi `FACILITY` schema | **N/A (producer NAICS only)** | Facility attributes include producer `NAICS`; no consumer class. |
| stewi `RCRAInfo.py` / BR path | **Fail for who-buys** (intersection only) | No `Shipper`/`Receiver`/`ActivityConsumedBy`/`SectorConsumedBy` strings in installed stewi package sources. Bedrock `rcra_waste_flows.py` builds waste×waste **intersection** from consolidated BR CSV shipper→receiver tons — **not** FD/who-buys. |
| CRHW national/state FBS (`CRHW_national_{2013,2015,2017,2019,2021}.yaml` via `stewiFBS_common.yaml:CRHW` → `stewi_to_sector`) | **Fail for who-buys** | `prepare_stewi_fbs` maps facility NAICS → `ActivityProducedBy` and assigns `ActivityConsumedBy=nan` when missing/all-null. Reconfirms generation-oriented FBS (empty consumed-by) — cannot proxy EC-style who-buys. Receivers in BR bypass ≠ customer-class FD shares. |
| Other stewi inventories bedrock pulls (e.g. eGRID via `egrid_generation.py`) | **N/A** | Electricity generation — no waste customer-class columns. |
| follow-on BR→FBS shipment edges | **Out of scope / not a who-buys find** | Would improve intersection provenance; still not seller receipts by customer class. |

---

## Outcome table

| Outcome | Chosen? | Meaning |
|---------|---------|---------|
| **`freeze_confirmed`** | **Yes** | Nothing usable across legs 1–3 meets quick-wire fitness for 2018–2021. Settled #3 = bare EC 2017 for 2018–2021. |
| **`quick_wire_in_phase`** | No | No source with 6-digit `562*` seller receipts by customer class that can validate→wire→regen in this PR. |

**Deferred notes (non-blocking, not flip gates):** SAS-scale of 2017 EC and §11 hardening remain optional hardening only. Follow-on BR→FBS remains a separate post-v0.5 PR.

**Status:** who-buys search complete with `outcome=freeze_confirmed`. Downstream Flip evidence (settled-doc cleanup, variance, Y2Y) and production Flip (2026-09-29) proceeded from this result — see [`production_gate.md`](production_gate.md).
