# CEDA FBS vs Registry alignment (temporary evaluation)

This folder holds **temporary** scripts for aligning GHG_national_CEDA_2023 FBS with ALLOCATED_EMISSIONS_REGISTRY. It is not imported by production code and can be removed or refactored when alignment work is done.

## Usage

Run from the **repository root** (the directory that contains both `bedrock` and `scripts`):

```bash
# 1. Overlap assessment (section 1): FBS slices, registry sources, overlap report
python -m scripts.ceda_fbs_registry_eval.run_overlap_report

# 2. Batch comparison (section 2): uses only fbs_slice_to_registry_mapping.csv
#    Add (fbs_slice, emissions_source) pairs to the mapping file (from step 1 output), then:
python -m scripts.ceda_fbs_registry_eval.run_batch_compare

# 3. Run a single FBS source or activity set (section 3, Option B)
# In Python:
#   from scripts.ceda_fbs_registry_eval.run_single import run_single_fbs_slice
#   df = run_single_fbs_slice("EPA_GHGI_T_3_64")  # one source
#   df = run_single_fbs_slice("EPA_GHGI_T_2_1", activity_set="electricity_transmission")
```

## Outputs

- `output/` — created by run_overlap_report and run_batch_compare:
  - `fbs_slices.csv` — includes `primary_activities` from FBS selection_fields
  - `registry_sources.csv`
  - `overlap_report.csv` — each row is a candidate (fbs_slice, emissions_source) pair; columns include `match_quality` (`table_gas_and_activity` when FBS PrimaryActivity matches activity strings from the allocation module, else `table_gas_only`), `fbs_primary_activities`, and `registry_activities` for inspection. Sort by `match_quality` to prefer activity-based matches.
  - `fbs_slice_to_registry_mapping.csv` — **only source of truth for comparison**: list (fbs_slice, emissions_source) pairs to compare; batch comparison ignores the overlap report and runs only these pairs
  - `comparison_summary.csv` (after run_batch_compare)
- `temp_fbs_methods/` — temporary FBS method YAMLs from `run_single_fbs_slice` (do not commit)

## Validation

Before relying on batch comparison, validate on 2–3 known pairs (e.g. `EPA_GHGI_T_3_64` ↔ `ch4_natural_gas_systems`, and one activity-set slice). Then run the full batch and use the summary to prioritize alignment in GHG_national_CEDA_2023.
