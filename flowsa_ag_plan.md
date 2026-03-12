# Plan: FLOWSA Agriculture — Enteric Fermentation & Manure Management

## Goal

Switch manure management N2O from the CEDA approach (T_2_1 total → proportional attribution via T_5_7) to the FLOWSA approach (pull N2O directly from T_5_7 with direct attribution). Also align enteric fermentation animals with the full FLOWSA animal list. This simplifies the pipeline and aligns with FLOWSA's treatment of these sources.

## Background

### Current state (CEDA path)
- **Enteric fermentation CH4** (`EPA_GHGI_T_5_3`): direct attribution, 9 animals (American Bison, Beef Cattle, Dairy Cattle, Goats, Horses, Mules and Asses, Sheep, Swine, Poultry)
- **Manure management CH4** (`EPA_GHGI_T_5_7`): direct attribution, same 9 animals, but **CH4 only** (`FlowName: ["CH4"]`)
- **Manure management N2O**: pulled from `EPA_GHGI_T_2_1` as aggregate "Manure Management" N2O total, then proportionally distributed across animal sectors using T_5_7 N2O data as attribution source

### FLOWSA approach (m1/m2 common)
- `EPA_GHGI_T_5_3` and `EPA_GHGI_T_5_7` use the same config — all 9 animals, **all flows** (CH4 + N2O), direct attribution
- No need for T_2_1 for manure N2O at all

### Recommendation from writeup
- **Manure Management N2O**: Use FLOWSA path — pull directly from T_5_7 (both CH4 and N2O). Same values, simpler, no extra allocation step. FLOWSA includes American Bison, Mules and Asses (CEDA does not include Mules and Asses).
- **Enteric Fermentation CH4**: Align animals with FLOWSA animal list. The current CEDA/Cornerstone YAMLs already list all 9 FLOWSA animals for T_5_3, so no change is needed here.

---

## Implementation Plan

### Phase 1: New transitional FBS YAML

**File**: `bedrock/transform/ghg/GHG_national_Cornerstone_2023_ag_livestock.yaml`

Following the established pattern (see PR #247, mobile_combustion, etc.):

1. `!include:GHG_national_CEDA_2023.yaml` as base
2. Override `source_names` via `!include:GHG_national_CEDA_2023.yaml:source_names`
3. Override these source_names:

   **`EPA_GHGI_T_2_1`**: Disable the `manure_management_n2O` activity set by setting its `PrimaryActivity` to `PASS`. We must use `PASS` rather than simply removing it because the transitional YAML `!include`s all base activity sets from the CEDA YAML — without `PASS`, the included base would still bring `manure_management_n2O` in. This follows the same pattern used in PR #247 where `direct3` was set to `PASS` to disable it after consolidation.

   **`EPA_GHGI_T_5_7`**: Remove the `FlowName: ["CH4"]` filter so it loads **both CH4 and N2O** with direct attribution. This is the key change — N2O is now pulled directly from T_5_7 instead of T_2_1.

   **`EPA_GHGI_T_5_3`**: Already lists all 9 FLOWSA animals with direct attribution. No override needed.

### Phase 2: Crosswalk

No new crosswalk file is needed. The existing `NAICS_Crosswalk_EPA_GHGI_CEDA.csv` already maps all 9 animals (American Bison, Beef Cattle, Dairy Cattle, Goats, Horses, Mules and Asses, Sheep, Swine, Poultry) to their NAICS sectors for both T_5_3 and T_5_7. We are not adding new activities or changing sector mappings — we're only changing *which table* the N2O comes from and removing the FlowName filter. The `activity_to_sector_mapping: EPA_GHGI_CEDA` remains the same.

### Phase 3: Config flag + pipeline wiring

**`bedrock/utils/config/usa_config.py`**:
- Add new boolean: `update_enteric_fermentation_and_manure_management_ghg_method: bool = False  # DRI: mo.li`

**`bedrock/transform/allocation/derived.py`** (`load_E_from_flowsa()`):
- Add `elif usa.update_enteric_fermentation_and_manure_management_ghg_method:` branch in the method cascade
- Set `methodname = 'GHG_national_Cornerstone_2023_ag_livestock'`
- Position: The cascade is a priority-ordered if/elif chain where the most comprehensive method wins. Each flag represents an incremental GHG update that builds on the CEDA baseline. The current order (from `derived.py` lines 146–159) is:
  1. `new_ghg_method` → full Cornerstone (most comprehensive)
  2. `update_electricity_ghg_method` → electricity attribution
  3. `update_other_gases_ghg_method` → other gases tables
  4. `update_ghg_attribution_method_for_ng_and_petrol_systems` → petroleum/natgas
  5. `update_transportation_ghg_method` → mobile combustion
  6. `add_new_ghg_activities` → new GHGI activities
  7. default → `GHG_national_CEDA_2023`

  Our new flag goes at position 7 (just before the default CEDA fallback, after `add_new_ghg_activities`), since it's a similarly scoped incremental update. Each of these flags is mutually exclusive — only one method is loaded at a time.

**`bedrock/utils/config/configs/2025_usa_cornerstone_ghg_ag_livestock.yaml`**:
- New config YAML:
  ```yaml
  use_cornerstone_2026_model_schema: True
  load_E_from_flowsa: true
  update_enteric_fermentation_and_manure_management_ghg_method: true
  snapshot_version_or_git_sha: "ff3c5a0ea73b26cecd09fd0613b8b34e1f30bcdc"
  ```

### Phase 4: Update Cornerstone YAML

**`bedrock/transform/ghg/GHG_national_Cornerstone_2023.yaml`**:
- Apply the same changes directly:
  - `EPA_GHGI_T_5_7`: Remove `FlowName: ["CH4"]` filter, allow both CH4 and N2O
  - `EPA_GHGI_T_2_1.manure_management_n2O`: Remove this activity set entirely (unlike the transitional YAML, the Cornerstone YAML owns its config directly — no `!include` inheritance to contend with, so we can simply delete it)
- No crosswalk changes needed for `NAICS_Crosswalk_EPA_GHGI_Cornerstone.csv` either — the Cornerstone crosswalk already maps all 9 animals for T_5_3 and T_5_7, and we're not changing any activity-to-sector mappings.

---

## Testing & Validation

### What to test (from colleague's guidance)

1. **Run `run_method_diff.py`** (actually `bedrock.utils.config.diff_methods`) to compare:
   - Baseline: `GHG_national_m1_common` or `GHG_national_m2_common` (original FLOWSA approach)
   - Test: `GHG_national_Cornerstone_2023_ag_livestock` (new transitional method)

2. **Run `FlowBySector` comparison** using `compare_FBS()`:
   ```python
   from bedrock.transform.flowbysector import FlowBySector, getFlowBySector
   from bedrock.utils.config.schema import dq_fields

   fbs_baseline = getFlowBySector('GHG_national_CEDA_2023')  # or m1/m2 common
   fbs_test = getFlowBySector('GHG_national_Cornerstone_2023_ag_livestock')
   ```

3. **Expected differences**:
   - Manure management N2O should now appear as direct animal-level entries from T_5_7 instead of proportionally allocated from T_2_1
   - Total N2O from manure management should be the same (16.8 per the writeup) since both T_2_1 and T_5_7 report the same values
   - Small differences possible for Goats and Mules/Asses if T_2_1 suppressed these (FLOWSA path includes them)
   - American Bison and Mules and Asses should appear as explicit entries

4. **Use Ben's diff script** to generate before/after difference tables for PR description

---

## File Summary

| Action | File | Description |
|--------|------|-------------|
| **Create** | `bedrock/transform/ghg/GHG_national_Cornerstone_2023_ag_livestock.yaml` | Transitional FBS YAML overriding T_5_7 and T_2_1 |
| **Create** | `bedrock/utils/config/configs/2025_usa_cornerstone_ghg_ag_livestock.yaml` | Config to enable the new method |
| **Edit** | `bedrock/utils/config/usa_config.py` | Add `update_enteric_fermentation_and_manure_management_ghg_method` flag |
| **Edit** | `bedrock/transform/allocation/derived.py` | Add elif branch for new method |
| **Edit** | `bedrock/transform/ghg/GHG_national_Cornerstone_2023.yaml` | Update Cornerstone YAML directly |
