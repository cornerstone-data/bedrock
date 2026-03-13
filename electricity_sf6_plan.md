# Plan: Add SF6 Electricity T&D to the Electricity Model Flag

## Context

### What PR #239 Did
PR #239 (`update_electricity_ghg_method` flag) created a new GHG method file
`GHG_national_Cornerstone_2023_electricity.yaml` that:

1. Inherits from `GHG_national_CEDA_2023.yaml`
2. Overrides **only** the `electric_power` activity set in `EPA_GHGI_T_3_8` (CH4) and `EPA_GHGI_T_3_9` (N2O) for stationary combustion
3. Disaggregates "All activities Electric Power" into fuel-specific activities (Coal, Natural Gas, Fuel Oil, Wood Electric Power)
4. Maps them to granular NAICS codes via `EPA_GHGI_Cornerstone_electricity` mapping:
   - Coal/Fuel Oil/Natural Gas Electric Power → **221112** (Fossil Fuel Electric Power Generation)
   - Wood Electric Power → **221117** (Biomass Electric Power Generation)
5. Added config `2025_usa_ceda_ghg_electricity.yaml` with `update_electricity_ghg_method: true`

### What's Missing: SF6 from Electricity T&D

There is a separate emission source for **SF6 (Sulfur Hexafluoride) from Electrical Transmission & Distribution** equipment (switchgear, circuit breakers). This is handled via the `electricity_transmission` activity set under `EPA_GHGI_T_2_1` in the GHG YAML configs.

Currently in the base `GHG_national_CEDA_2023.yaml` (lines 70-76):
```yaml
electricity_transmission:
  selection_fields:
    PrimaryActivity:
      - Electrical Equipment  # SF6, updated name in 2024 EPA release
    FlowName: SF6
  attribution_method: direct
```

This uses the `EPA_GHGI_CEDA` activity-to-sector mapping, which maps:
- "Electrical Equipment" → NAICS **2211** (broad Electric Power sector)

The colleague's ask: **Override the `electricity_transmission` activity set in the electricity method** to use Cornerstone-specific mapping with a more granular NAICS code, consistent with the approach taken for `electric_power`.

## What Exists Today

### Two paths for SF6 electricity allocation

1. **FlowSA path** (`load_E_from_flowsa`): `electricity_transmission` activity set in `EPA_GHGI_T_2_1` → maps "Electrical Equipment" SF6 → NAICS 2211
2. **Other gases path** (legacy): `sf6_electricity.py` → allocates SF6 directly to sector **221100** (commodity code for 2211)

Both currently target the aggregate 2211/221100 level.

### Relevant NAICS codes for electricity sub-sectors
| NAICS | Description |
|-------|-------------|
| 2211  | Electric Power Generation, Transmission and Distribution (aggregate) |
| 221112 | Fossil Fuel Electric Power Generation |
| 221117 | Biomass Electric Power Generation |
| 221121 | Electric Bulk Power Transmission and Control |
| 221122 | Electric Power Distribution |

SF6 from T&D is specifically about **transmission and distribution infrastructure**, not generation. The most appropriate granular NAICS would be **221121** (Electric Bulk Power Transmission and Control) or a combination of **221121 + 221122**.

## Proposed Implementation

### Option A: Map SF6 T&D to 221121 (Transmission) only

Simplest approach. All SF6 from electrical equipment goes to the transmission sector.

### Option B: Map SF6 T&D to both 221121 + 221122 (Transmission + Distribution)

Split SF6 across both T&D sub-sectors. Would require proportional attribution or a fixed split.

### Option C: Keep SF6 at 2211 but ensure it's explicitly included

If granular disaggregation isn't needed for SF6, just ensure the electricity method explicitly includes it (no mapping change).

## Recommended Changes (assuming Option A or C - confirm with colleague)
comment: let's go with C

### 1. Update `GHG_national_Cornerstone_2023_electricity.yaml`

Add an override for `EPA_GHGI_T_2_1` to also change the `electricity_transmission` activity set:

```yaml
!include:GHG_national_CEDA_2023.yaml
  source_names: !include:GHG_national_CEDA_2023.yaml:source_names
    EPA_GHGI_T_2_1:
      !include:GHG_national_CEDA_2023.yaml:source_names:EPA_GHGI_T_2_1
      activity_sets:
        !include:GHG_national_CEDA_2023.yaml:source_names:EPA_GHGI_T_2_1:activity_sets
        electricity_transmission:
          activity_to_sector_mapping: EPA_GHGI_Cornerstone_electricity
          selection_fields:
            PrimaryActivity:
              - Electrical Equipment
            FlowName: SF6
          attribution_method: direct

    EPA_GHGI_T_3_8: &stationary_combustion
      # ... existing override from PR #239 ...
    EPA_GHGI_T_3_9: *stationary_combustion
```

### 2. Update `NAICS_Crosswalk_EPA_GHGI_Cornerstone_electricity.csv`

Add the SF6 T&D mapping row:

```csv
EPA_GHGI_Cornerstone_electricity,Electrical Equipment,NAICS_2017_Code,221121,,"SF6 from T&D equipment",EPA_GHGI_T_2_1
```

(Or 2211 if keeping aggregate — confirm target NAICS with colleague)

### 3. No config changes needed

The `update_electricity_ghg_method` flag already exists and gates the method selection. No new flag needed.

## Questions to Confirm with Colleague

1. **Which NAICS code for SF6 T&D?** Should it go to:
   - 221121 (Electric Bulk Power Transmission and Control)?
   - 221122 (Electric Power Distribution)?
   - Both (split)?
   - Stay at 2211 (just explicitly include in the method)? comment: this one
2. **Should the `sf6_electricity.py` (other gases path) also be updated** to use the same granular code, or is this only for the FlowSA path? comment: I don't quite follow this question, elaborate on it
3. **Does this also need to be reflected in the main `GHG_national_Cornerstone_2023.yaml`** (i.e., the `new_ghg_method` path), or only the `update_electricity_ghg_method` path? comment: only the update_electricity_ghg_method path

## Files to Modify

| File | Change |
|------|--------|
| `bedrock/transform/ghg/GHG_national_Cornerstone_2023_electricity.yaml` | Add `EPA_GHGI_T_2_1` override with `electricity_transmission` activity set |
| `bedrock/utils/mapping/activitytosectormapping/NAICS_Crosswalk_EPA_GHGI_Cornerstone_electricity.csv` | Add "Electrical Equipment" SF6 → target NAICS mapping row |
| (optional) `bedrock/transform/allocation/other_gases/sf6_electricity.py` | Update sector from 221100 to granular code if needed |
