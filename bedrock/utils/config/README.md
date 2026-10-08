# Config

## Source Catalog
The 'source_catalog.yaml' file should be manually updated with each new
Flow-By-Activity.

## Term descriptions
- _class_: list, classes such as "Water" found in the Flow-By-Activity
- _activity_schema_: `null` if activities are text-based (mapping CW supplies
  SectorSourceName), otherwise a dict keyed by schema (`naics` / `bea` / ...). Examples:

  ```yaml
  activity_schema:
    bea:
      year: 2017
      hierarchy: flat
  # SectorSourceName = BEA_2017_Code

  activity_schema:
    naics:
      hierarchy: parent-completeChild
      by_year:
        2010: 2007
        2017: 2017
  ```

  Hierarchy values: `flat`, `parent-completeChild`, or
  `parent-incompleteChild` (describes how this FBA's activity rows nest, not
  the global SECTOR_HIERARCHY_ORDER).

---

## Atomic FBS change testing

Use this workflow to test FBS method YAML changes in small, isolated steps: confirm only intended config keys changed, then confirm FBS output diff is limited to the right sources/activities.

### Workflow (3 steps)

1. **Config diff** — Compare resolved configs (baseline vs test method). Diff paths should match only your intended edit.
2. **Mapping diff** (optional) — Compare activity-to-sector mapping file usage and content for both methods.
3. **FBS diff** — Generate both methods, then compare FBS outputs to see numerical impact.

Steps 1–2 use the `diff_methods` CLI (no FBS run). Step 3 you run yourself: generate baseline and test FBS, then `compare_FBS(baseline_fbs, test_fbs)` from `bedrock.utils.validation.validation`.

### Setup

- **Baseline method:** A method name whose YAML is the “before” state (e.g. a copy of the test YAML before edits, with a different filename).
- **Test method:** The method you edited (e.g. `GHG_national_CEDA_2023_new`). Use `!include` for shared config and override only what you change.

Method name = filename without `.yaml` (e.g. `GHG_national_CEDA_2023_new` → `transform/ghg/GHG_national_CEDA_2023_new.yaml`).

### CLI: diff_methods

From repo root (with venv activated or `uv run`):

```bash
# Config diff only (fast)
python -m bedrock.utils.config.diff_methods <baseline_method> <test_method>

# Config + mapping diff
python -m bedrock.utils.config.diff_methods <baseline_method> <test_method> --mapping

# Write diffs to YAML (path optional; default: <baseline>_vs_<test>_diffs.yaml)
python -m bedrock.utils.config.diff_methods <baseline> <test> --output
python -m bedrock.utils.config.diff_methods <baseline> <test> -o my_diffs.yaml
```
