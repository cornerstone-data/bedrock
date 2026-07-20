---
name: dispatch-ef-diagnostics
description: Use this skill to dispatch EF (emission-factor) diagnostics runs — create and name empty diagnostics Google Sheets, trigger generate_diagnostics, and record a run index. Covers the a_matrix_time_series epic matrix and generic feature-config lists via bedrock.utils.validation.dispatch_diagnostics. Trigger when the user says "dispatch the EF diagnostics", "kick off the diagnostics runs", "create the diagnostics sheets and run them", "trigger generate_diagnostics", "re-dispatch the failed cells", or asks to run the model under several configs and produce per-run diagnostics sheets.
disable-model-invocation: false
argument-hint: [scenarios] [years] [approaches] (e.g. "bundle_v0_3 2019-2023" or "isolate_a_matrix useeio_nowcast 2023")
---

# Dispatch EF diagnostics runs

**Playbooks:** [`bedrock/utils/validation/evaluate_feature_impact.md`](../../bedrock/utils/validation/evaluate_feature_impact.md) (feature impact); [`bedrock/utils/config/feature_flag.md`](../../bedrock/utils/config/feature_flag.md) (flag + atomic YAML). Shared helpers + feature CLI: `bedrock.utils.validation.dispatch_diagnostics` (default Drive folder: v0.4 Diagnostics).

Fan out the `generate_diagnostics` GitHub Actions workflow to produce **one diagnostics Google Sheet per cell**. Each sheet gets the `N_and_diffs` / `D_and_diffs` / `D_and_N_significant_sectors` / `config_summary` tabs that the `plot-ef-diagnostics` skill consumes.

For the **A-matrix time-series epic**, the driver is `bedrock/analysis/a_matrix_time_series/dispatch_ef_time_series.py` (one sheet per `(scenario, approach, year)`). Per cell it: (1) **creates** a Sheet in the epic Drive folder with a deterministic title, (2) **triggers** `gh workflow run generate_diagnostics.yml`, (3) **records** a row in `output/results/ef_run_index.csv`. It is **idempotent** — cells already in the index are skipped, so re-running only fills gaps. Default baseline for that epic is **CEDA-US (v0)**.

## Prerequisites — verify, don't assume

| Check | How |
|---|---|
| `gh` auth + `workflow` scope | `gh auth status` |
| Workflow registered | `gh workflow list \| grep -i diagnostic` → expect `generate_diagnostics active` |
| Configs exist **on the ref** | `git fetch origin <ref>` then `git cat-file -e origin/<ref>:bedrock/utils/config/configs/<cfg>.yaml`. Don't trust a spec sheet's "on main?" column — confirm. |
| Google ADC + Drive | `gcloud auth application-default login` (Drive scope). Confirm read access + see what's already in the folder via `_drive_client().files().list(...)`. |
| USEEIO baseline pin | `bedrock/utils/snapshots/useeio_baseline_pin.json` (currently `USEEIOv2.6.0-phoebe-23`, a GCS Excel). With `use_useeio_baseline=true` the workflow benchmarks against this pin **in addition to** the always-on CEDA-v0 comparison. |

Workflow inputs (`generate_diagnostics.yml`): `config_name, sheet_id, use_useeio_baseline (bool), model_base_year, usa_ghg_data_year, pr_url`. The **git-ref is `gh workflow run --ref`, not an input** — it selects the model code the run executes against.

## Clarify first — ask before acting

Do **not** create any sheet or dispatch until these are confirmed. Ask via `AskUserQuestion`, offering the default as the recommended option. Treat anything still ambiguous as a blocker.

| Detail | Default |
|---|---|
| Drive folder | `1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s` (`EF_TIME_SERIES_DRIVE_FOLDER_ID`) |
| Scenarios | `bundle_v0_3` (or `isolate_a_matrix`, or both) |
| Approaches | all in the scenario (e.g. restrict to `useeio_nowcast`) |
| Years | `2019,2020,2021,2022,2023` — sets `model_base_year` + `usa_ghg_data_year` |
| Baseline | CEDA-only (ask whether to also tick USEEIO via `--use-useeio-baseline`) |
| git-ref | `main` |
| Throttle | `poll` (or `sleep:N` / `none`) |
| Sheet title | `[{date}, {year}, {baseline} based, {approach label}, {scenario}] EFs diagnostics` |
| Dry-run first? | yes |

Echo the resolved plan back (folder, cell count, baseline, git-ref) and get a go-ahead before the non-dry-run dispatch — it creates sheets and consumes CI minutes. **Always show the full list of sheet titles for review before creating any.**

## Bespoke config lists (the common real case)

Most real requests come as a **config-spec Google Sheet**, not the two canned scenarios. `SCENARIO_YAMLS` only knows `isolate_a_matrix` / `bundle_v0_3` (4 A-matrix YAMLs each) — an arbitrary release-progression list (`useeio_phoebe_23*`, `2025_usa_cornerstone_*`, `…_v0_3_*`) **can't be expressed via `--scenarios/--approaches`**. Don't force-fit it.

Instead drive the dispatcher's **helper functions** with the custom list (reuse, don't reinvent):

```python
from bedrock.utils.validation.dispatch_diagnostics import (
    create_sheet, trigger_workflow, wait_for_capacity,
    V04_DIAGNOSTICS_DRIVE_FOLDER_ID,
)
# A-matrix / release-progression folder stays on the epic dispatcher:
# from bedrock.analysis.a_matrix_time_series.dispatch_ef_time_series import (
#     EF_TIME_SERIES_DRIVE_FOLDER_ID,
# )
# per config: wait_for_capacity(...) → create_sheet(folder, title) → trigger_workflow(...) → persist a row
```

Or for a simple feature-config list use the validation CLI:

```bash
uv run python -m bedrock.utils.validation.dispatch_diagnostics \
    --git-ref main \
    --configs <cfg1>,<cfg2> \
    --baseline-label "Bedrock v0.3 snapshot based" \
    --dry-run
```

A spec sheet's observed layout has two baseline-grouped tables:
- **"compare to USEEIO Phoebe"** → run with `use_useeio_baseline=true`.
- **"compare to v0 ceda"** → CEDA-only (default).
- A config in **both** is the user's call: one run with the box on already includes CEDA-v0 columns (so a separate CEDA-only sheet is partly redundant) vs. two distinct sheets. Surface it; let them choose.
- These are **release snapshots → one run per config**, not the multi-year sweep. **Match each config's year to its YAML**: configs that set `v0_3_umd_2024_ghgia: true` or hard-code `model_base_year: 2024` (e.g. `…_2024_io_ghg`, `…_umd_2024_ghgia`, **and FINAL `…_v0_3`**) must run at 2024. When a config hard-codes its years, pass **no** `model_base_year`/`usa_ghg_data_year` override so the YAML wins.

Persist this batch to a **separate index CSV** (e.g. `output/results/ef_run_index_release_<label>.csv`), not the canonical `ef_run_index.csv`, so `compile_ef_diagnostics` isn't polluted. Write `sheet_id` on create and `triggered_at` on trigger so the run is **resumable** — re-running skips rows that already have `triggered_at`.

## Runtime & serialization

`generate_diagnostics.yml` sets `concurrency: { group: generate_diagnostics, cancel-in-progress: false }`, so GitHub runs these **one at a time**; firing many in quick succession **drops** the pending ones. So:
- The `poll` throttle is **mandatory**; the batch is effectively serial.
- Runs are **typically ~2–5 min each** (`timeout-minutes: 60` is a ceiling, not the norm) ⇒ a batch ≈ N × a few min; **24 runs ≈ 1–1.5 h**.
- Run the dispatcher **in the background** for anything beyond a handful of cells; report progress from its log + index.
- `_wait_for_capacity`'s default `timeout=1800` is usually fine; pass `timeout≈5400` as a safety margin in case a run nears the 60-min ceiling.
- Don't parallelize by firing fast — you'll lose runs to the concurrency group.

## Known failure modes (model-side, not dispatch)

These surface as a **failed GH run after a successful dispatch** — the sheet is created but stays empty. Pre-flight by grepping each config for year flags and snapshot pins.

- **Year / flag mismatch.** Overriding `usa_ghg_data_year`/`model_base_year` to a value a config forbids → e.g. `ValueError: usa_ghg_data_year=2023 is incompatible with v0_3_umd_2024_ghgia`. Catch up front: `grep -lE 'v0_3_umd_2024_ghgia:\s*[Tt]rue' …/configs/*.yaml` → those run at 2024. Fix: re-run at the required year **and rename the sheet's year** to match.
- **Interim configs pinned to an old snapshot.** A config that pins `snapshot_version_or_git_sha` and flips an interim flag (e.g. `2025_usa_cornerstone_full_model_v0_3_ghgi_mecs` + `update_mecs_method` against an old SHA) may fail to resolve its GHG FlowBySector method on `main` HEAD → `AttributeError: 'NoneType' object has no attribute 'pop'` in flowsa `generateFlowBySector`. Run these against their **pinned snapshot/branch**, not `main`. Often the effect is already folded into a later bundled config (MECS ⊂ `…_umd_2024_ghgia`), so the standalone cell can be skipped.

## Standard procedure

1. **Dry-run first** — prints the plan (titles + configs), creates/triggers nothing:
   ```bash
   python -m bedrock.analysis.a_matrix_time_series.dispatch_ef_time_series \
       --git-ref main --scenarios bundle_v0_3 --years 2019,2020,2021,2022,2023 --dry-run
   ```
2. **Dispatch for real** (drop `--dry-run`):
   ```bash
   python -m bedrock.analysis.a_matrix_time_series.dispatch_ef_time_series \
       --git-ref main \
       --scenarios isolate_a_matrix,bundle_v0_3 \
       --years 2019,2020,2021,2022,2023 \
       [--approaches useeio_nowcast] [--use-useeio-baseline] [--throttle poll|sleep:N|none]
   ```
3. **Wait** for GH Actions (~2–5 min per run, serial). Watch with `gh run list --workflow generate_diagnostics.yml`.
4. **Compile + plot** (reviewer path — see the `plot-ef-diagnostics` skill):
   ```bash
   python -m bedrock.analysis.a_matrix_time_series.compile_ef_diagnostics
   python -m bedrock.analysis.a_matrix_time_series.plot_ef_diagnostics
   ```

## Flags

| Flag | Meaning |
|---|---|
| `--git-ref` | **Required.** Branch/tag the workflow runs against (usually `main`). |
| `--scenarios` | Comma list: `isolate_a_matrix`, `bundle_v0_3` (default `bundle_v0_3`). |
| `--years` | Comma list (default `2019,2020,2021,2022,2023`). Sets `model_base_year` and `usa_ghg_data_year`. |
| `--approaches` | Optional filter, e.g. `useeio_nowcast`. Default = all approaches in the scenario. |
| `--use-useeio-baseline` | Add USEEIO baseline columns. Default = CEDA-only. |
| `--throttle` | `poll` (default; blocks until prior runs clear), `sleep:N`, or `none`. |
| `--dry-run` | Print the plan only. |
| `--re-dispatch-from-csv` | Re-trigger cells already in `ef_run_index.csv` (recovery; **reuses** existing sheets). |

## Conventions (don't reinvent)

- **Drive folder:** `EF_TIME_SERIES_DRIVE_FOLDER_ID = 1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s`.
- **Sheet title:** `[{YYYY-MM-DD}, {year}, {baseline} based, {approach label}, {scenario}] EFs diagnostics`.
- **Run index:** `output/results/ef_run_index.csv` — columns `scenario, approach, year, baseline, config_name, sheet_id, sheet_title, useeio_box_ticked, git_ref, triggered_at`.
- **Scenario → YAML** (in `dispatch_ef_time_series.py`), keyed by approach:
  - `isolate_a_matrix` (A-matrix method only, else v0 defaults): `2025_usa_cornerstone_A_{useeio,summary_tables,commodity_price_index,useeio_nowcast}`.
  - `bundle_v0_3` (full v0.3 stack + one A-matrix alternative): `2025_usa_cornerstone_v0_2_A_{…}`.
- **Approach labels** (title text): `useeio → "A matrix with 2017 benchmark A"`, `summary_tables → "A matrix with summary tables"`, `commodity_price_index → "A matrix with commodity price index"`, `useeio_nowcast → "A matrix from USEEIO nowcast"`.

## Recovery / utilities

- **Lost the local index?** Rebuild from Drive: `python -m bedrock.analysis.a_matrix_time_series.recover_ef_run_index --folder-id 1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s`.
- **Batch hit rate limits?** `--re-dispatch-from-csv` re-triggers without minting new sheets.

## Manual fallback (no driver)

For a one-off cell by hand: create a sheet in the Drive folder named per the template (`_create_sheet` or Drive UI), trigger the workflow, then append the index row yourself:
```bash
gh workflow run generate_diagnostics.yml --ref main \
  -f config_name=<config> -f sheet_id=<id> \
  -f model_base_year=<year> -f usa_ghg_data_year=<year> -f use_useeio_baseline=false
```

## Reference

- Driver (A-matrix epic): `bedrock/analysis/a_matrix_time_series/dispatch_ef_time_series.py`
- Shared helpers + feature CLI: `bedrock/utils/validation/dispatch_diagnostics.py`
- Workflow: `.github/workflows/generate_diagnostics.yml` → `bedrock/utils/validation/generate_diagnostics.py` (single-run entry) → `calculate_ef_diagnostics.py` (writes the tabs).
- Package overview + DAG: `bedrock/analysis/a_matrix_time_series/README.md`.
- Operator checklist: `bedrock/analysis/a_matrix_time_series/useeio_nowcast_ef_runbook.md`.
- Feature-flag playbook: `bedrock/utils/validation/evaluate_feature_impact.md`.
