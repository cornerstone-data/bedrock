# Plan: E Matrix Disaggregation (CEDA → Cornerstone)

## Context

Currently, `derive_E_usa()` produces E in **CEDA v7** space (~400 sectors). To get E into Cornerstone space, it goes through an intermediate step: CEDA v7 → BEA 2017 → Cornerstone (via `bea_E()` which maps CEDA→BEA, then `expand_ghg_matrix` which maps BEA→Cornerstone).

We want a **direct CEDA v7 → Cornerstone** expansion path, which avoids the lossy round-trip through BEA.

## Key differences between taxonomies

| | BEA 2017 | CEDA v7 | Cornerstone |
|---|---|---|---|
| Aluminum | `33131B` | `331313` | `33131B` (kept) |
| Appliances | `335220` | `335221/222/224/228` (4 splits) | `335220` (kept) |
| Waste | `562000` | `562000` | 7 subsectors |
| Dropped | `S00401`, `S00300`, `S00900` | — | — |

So CEDA→Cornerstone needs to handle: appliances (4→1 aggregation), aluminum (code rename `331313`→`33131B`), waste (1→7 disaggregation), and the rest are 1:1.

## Steps

### 1. Create mapping file: `bedrock/utils/taxonomy/mappings/bea_ceda_v7__cornerstone_commodity.py`

- New function `load_ceda_v7_commodity_to_cornerstone_commodity() → Dict[CEDA_V7_SECTOR, List[COMMODITY]]`
- Mapping logic:
  - `331313` → `['33131B']` (aluminum rename)
  - `335221`, `335222`, `335224`, `335228` → `['335220']` (appliances aggregation, many-to-one)
  - `562000` → `WASTE_DISAGG_COMMODITIES['562000']` (waste disaggregation)
  - All other codes present in both → identity `[code]`
  - Codes in CEDA v7 but not in Cornerstone → `[]` (dropped)
- Follow the existing pattern: use `validate_mapping()` with `dangerously_skip_empty_mapping_check=True`

### 2. Add correspondence loader in `usa_taxonomy_correspondence_helpers.py`

- New function `load_ceda_v7_commodity__cornerstone_commodity_correspondence() → pd.DataFrame`
- Uses `create_correspondence_matrix()` with `domain=CEDA_V7_SECTORS`, `range=COMMODITIES`
- Same pattern as existing `load_usa_2017_commodity__cornerstone_commodity_correspondence()`

### 3. Rename in `cornerstone_expansion.py`

- `expand_ghg_matrix` → `expand_ghg_matrix_from_bea_to_cornerstone`
- Update all call sites in `derived_cornerstone.py` (2 calls: `derive_cornerstone_E` and `derive_cornerstone_B_via_vnorm`)

### 4. Add CEDA→Cornerstone expansion infrastructure in `cornerstone_expansion.py`

- New cached functions (mirroring the BEA ones):
  - `ceda_commodity_corresp_raw()` — loads the new correspondence
  - `ceda_commodity_corresp()` — column-normalized version
  - `cs_commodity_to_ceda_map()` — reverse map `{cornerstone_code: ceda_parent_code}`
- Reuse existing `CS_COMMODITY_LIST`

### 5. Create `expand_ghg_matrix_from_ceda_to_cornerstone` in `cornerstone_expansion.py`

- Signature: `(M: pd.DataFrame, target_col_codes: list[str], col_map: dict[str, str]) → pd.DataFrame`
- Same structure as `expand_ghg_matrix_from_bea_to_cornerstone` but using the CEDA→Cornerstone correspondence
- Key difference: the appliances many-to-one case (4 CEDA codes → 1 Cornerstone code) means columns should be **summed** not duplicated. The function should handle this aggregation.

### 6. Wire up for E matrix (future step, not in this PR)

- In `derived_cornerstone.py`, add an alternative `derive_cornerstone_E` path that calls `derive_E_usa()` directly (already in CEDA v7 space) and uses `expand_ghg_matrix_from_ceda_to_cornerstone` instead of going through the BEA intermediate

## Files touched

| File | Change |
|---|---|
| `bedrock/utils/taxonomy/mappings/bea_ceda_v7__cornerstone_commodity.py` | **NEW** — CEDA v7 → Cornerstone mapping |
| `bedrock/utils/taxonomy/usa_taxonomy_correspondence_helpers.py` | Add `load_ceda_v7_commodity__cornerstone_commodity_correspondence()` |
| `bedrock/transform/eeio/cornerstone_expansion.py` | Rename function + add CEDA correspondence loaders + new expand function |
| `bedrock/transform/eeio/derived_cornerstone.py` | Update import & 2 call sites for rename |

## Key design consideration

The CEDA→Cornerstone path has a **many-to-one** case (appliances 4→1) that the BEA→Cornerstone path doesn't have. `expand_ghg_matrix_from_ceda_to_cornerstone` needs to sum (not duplicate) when multiple source columns map to one target column. The column-normalized correspondence matrix handles this naturally if you use matrix multiplication (`M @ corresp`) rather than the current index-relabeling approach.
