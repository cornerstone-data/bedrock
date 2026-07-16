# Evaluate methodology feature impact

Playbook for measuring the EF impact of a methodology flag via diagnostics
Google Sheets. Assumes the flag, transform gate, and atomic config YAML
exist — see [`../config/feature_flag.md`](../config/feature_flag.md).

Post-run plotting and multi-run merges:
[`analysis/README.md`](analysis/README.md).

## Prerequisites

- Atomic (or intentional) `config_name` under `bedrock/utils/config/configs/`.
- Git ref that contains the flag wiring (branch or `main`).
- Empty Google Sheet the run can write to (Drive UI or API).
- For CI dispatch: `gh` authenticated with `workflow` scope
  (`gh auth status`).
- Optional USEEIO Excel baseline: pin file at
  [`../snapshots/useeio_baseline_pin.json`](../snapshots/useeio_baseline_pin.json).

## Choose a baseline

Three baseline identities map onto two loader modes:

| Baseline identity | How | What the sheet compares |
|---|---|---|
| **CEDA-US v0** | Leave `snapshot_version_or_git_sha` at its default, `'v0'`; leave the USEEIO box unticked | `N_old` / `D_old` are the legacy CEDA-US v0 snapshot EFs |
| **Bedrock / Cornerstone model snapshot** | Set `snapshot_version_or_git_sha` to the desired model snapshot SHA. For the latest accepted model, use the SHA in `bedrock/utils/snapshots/.SNAPSHOT_KEY`. Leave the USEEIO box unticked | `N_old` / `D_old` are the selected Bedrock / Cornerstone snapshot EFs |
| **USEEIO Excel baseline** | Tick `use_useeio_baseline` / pass `--useeio_baseline_pin_json` | `N_old` / `D_old` are the D/N rows from the workbook in `useeio_baseline_pin.json`. Emits `_purchaser` N columns (model-purchaser vs USEEIO-purchaser) |

CEDA-US v0 and Bedrock / Cornerstone snapshots both use
`diagnostics_baseline_source='gcs_snapshot'`; `snapshot_version_or_git_sha`
selects which snapshot. The USEEIO workbook uses
`diagnostics_baseline_source='gcs_useeio_xlsx'` and is pinned by URI, SHA-256,
and model-version label.

The baseline is **one per sheet**. Snapshot and USEEIO modes are mutually
exclusive. A USEEIO run does not also carry a CEDA-US or Bedrock / Cornerstone
snapshot comparison; run a separate sheet for each baseline. `_purchaser`
columns appear only on USEEIO-baseline sheets.

Use precise baseline labels in sheet titles and run indexes: `CEDA-US v0
based`, `Bedrock <release-or-short-SHA> snapshot based`, or `USEEIO
<model-version-label> based`. Do not use bare `CEDA based` for a Bedrock /
Cornerstone snapshot.

Trust `config_summary` for resolved years and baseline fields, not the
dispatch override alone.

## Sheets, Drive folders, and naming

Diagnostics write into an existing Google Sheet. Creating that Sheet is a
separate step from running the model.

### Where sheets go

| Folder | Drive ID | Defined in | Use for |
|---|---|---|---|
| **v0.4 Diagnostics** | `1W6I4q2ssfgaaVz6eLNICNiETP05dhrCK` | `dispatch_diagnostics.V04_DIAGNOSTICS_DRIVE_FOLDER_ID` | Methodology feature evaluations |
| EF time-series / v0.3 release progression | `1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s` | `a_matrix_time_series.dispatch_ef_time_series.EF_TIME_SERIES_DRIVE_FOLDER_ID` | A-matrix time-series and v0.3 step-by-step release sheets |
| v0.3 waterfall | `107RNHx1OUGN6roYdRi3BbdCSrMNFhl6u` | `dispatch_diagnostics.V03_WATERFALL_DRIVE_FOLDER_ID` | Wholesale G1→G3 / FINAL waterfall only |

Default feature-impact sheets to the **v0.4 Diagnostics** folder. The
A-matrix time-series folder ID stays on its epic dispatcher. Do not place
one-off feature runs in the time-series or waterfall folders unless they
belong to those registered progressions.

### How to create the sheet

| Path | When |
|---|---|
| **Drive UI** | One-off; paste the sheet ID into the workflow / CLI |
| **Agent / script via Drive API** | `create_sheet(folder_id, title)` from `bedrock.utils.validation.dispatch_diagnostics` |
| **Feature dispatcher** | `uv run python -m bedrock.utils.validation.dispatch_diagnostics --configs …` (creates sheet, triggers workflow, appends run index) |
| **Epic dispatcher** | Batch cells (time-series, release, waterfall) that import the same helpers |

Confirm folder ID and full title list before any agent create or batch
dispatch. Dry-run batch dispatchers first.

### Title convention

```text
[{YYYY-MM-DD}, bedrock repo, {model_year}, {baseline label}, {feature or config label}] EFs diagnostics
```

Examples:

```text
[2026-07-16, bedrock repo, 2024, Bedrock v0.3 snapshot based, update_foo_method] EFs diagnostics
[2026-07-16, bedrock repo, 2024, CEDA-US v0 based, update_foo_method] EFs diagnostics
[2026-07-16, bedrock repo, 2024, USEEIO USEEIOv2.6.0-phoebe-23 based, update_foo_method] EFs diagnostics
```

`model_year` is the config’s intrinsic `model_base_year` (or an intentional
year override). Baseline labels follow the identities above — never use bare
`CEDA based` for a Bedrock / Cornerstone snapshot.

Epic title variants keep the same bracketed shape but swap the middle label
for approach / release-step text (see `dispatch_ef_time_series`,
`dispatch_ef_release_v0_3`, `dispatch_ef_v03_waterfall`).

### Shared create / trigger helpers

Create-sheet, `gh workflow run`, and serial throttle helpers live in
[`dispatch_diagnostics.py`](dispatch_diagnostics.py). Epic dispatchers
(time-series, release, waterfall) import from there. Feature evaluation uses
the module CLI (default folder = v0.4 Diagnostics):

```powershell
uv run python -m bedrock.utils.validation.dispatch_diagnostics `
  --git-ref <branch-or-main> `
  --configs <config_stem> `
  --baseline-label "Bedrock v0.3 snapshot based" `
  --dry-run
```

Omit `--dry-run` once titles are reviewed. Pass `--use-useeio-baseline` when
the baseline is the USEEIO pin. Run-index default:
`bedrock/utils/validation/output/ef_run_index_feature.csv`.

## One-off diagnostics run

### GitHub Actions

1. Create an empty Google Sheet in the v0.4 Diagnostics folder (UI or API);
   copy its ID from the URL.
2. Dispatch against the ref that has the flag wiring:

```powershell
gh workflow run generate_diagnostics.yml --ref <branch-or-main> `
  -f config_name=<config_stem> `
  -f sheet_id=<google_sheet_id> `
  -f use_useeio_baseline=false
```

Optional inputs: `pr_url`, `model_base_year`, `usa_ghg_data_year`,
`use_useeio_baseline=true`.

Leave year overrides empty when the YAML already sets the correct
`model_base_year` / `usa_ghg_data_year`. Overriding a year a flag forbids
fails at validation (for example `v0_3_umd_2024_ghgia` with an incompatible
`usa_ghg_data_year`).

Watch: `gh run list --workflow generate_diagnostics.yml`.

The workflow is serial (`concurrency.group: generate_diagnostics`). Firing
many runs without waiting drops pending ones.

### Local

```powershell
uv run python -m bedrock.utils.validation.generate_diagnostics `
  --sheet_id <google_sheet_id> `
  --config_name <config_stem>
```

USEEIO pin:

```powershell
uv run python -m bedrock.utils.validation.generate_diagnostics `
  --sheet_id <google_sheet_id> `
  --config_name <config_stem> `
  --useeio_baseline_pin_json bedrock/utils/snapshots/useeio_baseline_pin.json
```

Requires Google application-default credentials with Sheets write access.

## What the sheet contains

Typical EF tabs (see `calculate_ef_diagnostics`):

| Tab | Role |
|---|---|
| `N_and_diffs` | Total EF (N) run vs baseline; `%` diffs; inflated / purchaser columns when eligible |
| `D_and_diffs` | Direct EF (D); same inflation rules as N. No purchaser-price D columns |
| `D_and_N_significant_sectors` | Sectors called out as significant movers |
| `config_summary` | Resolved `USAConfig` fields + git metadata + baseline key used |
| `BLy_new_vs_BLy_old` | Per-sector BLy when the baseline path provides it |

**Producer-price N % diff** (`N_perc_diff`) is
`(N_new − N_old_inflated) / |N_old_inflated|` when inflated columns exist,
where `N_old` is whichever baseline the mode selected (snapshot or USEEIO).

**Purchaser-price N % diff** (USEEIO pin path only):
`(N_new_purchaser − N_old_purchaser) / |N_old_purchaser|` — model vs USEEIO
in purchaser prices.

Cross-sheet comparisons of raw `N_new` are valid only when dollar years match
(`model_base_year`, `usa_io_data_year`, `usa_ghg_data_year` in each
`config_summary`). Otherwise use inflation-adjusted columns.

## Plot a single run

```powershell
uv run python -m bedrock.utils.validation.analysis.diagnostics_plots `
  --sheet-id <google_sheet_id> --tag <label>
```

Outputs land under `analysis/output/<tag>/` (see
[`analysis/README.md`](analysis/README.md)). `--sheet-id` can fall back to
`BEDROCK_DIAGNOSTICS_SHEET_ID`.

## Multiple runs

1. Create one sheet per config × baseline in the v0.4 Diagnostics folder
   (or the epic folder for a registered progression).
2. Dispatch `generate_diagnostics` serially (poll for capacity between runs).
3. Merge with `combine_ef_diagnostics` when a workbook across runs is needed
   (`--combo` picks a registered `ComboSpec` in
   `bedrock.utils.validation.analysis.combinations`).
4. Register a combo only for a progression that will be re-run.

Epic matrices (approach × year, v0.3 release / waterfall steps) live under
`bedrock/analysis/` — see
[`../../analysis/a_matrix_time_series/README.md`](../../analysis/a_matrix_time_series/README.md)
and [`../../analysis/v0_3/README.md`](../../analysis/v0_3/README.md). This
guide covers the single-flag (or small list) path.

## Interpret checklist

- Do movers concentrate in the sector families the flag should affect?
- How large are % and absolute changes on `D_and_N_significant_sectors`
  versus the full distribution?
- Does `config_summary` show the intended flag, years, and baseline
  (`snapshot_version_or_git_sha` / USEEIO pin fields)?
- Are apparent agreements methodological tautologies (shared frozen structure
  with the baseline) rather than independent validation?

## Known failure modes

- **Year / flag mismatch.** CLI year overrides disagree with a flag that
  locks years → `ValueError` during config validation. Prefer the YAML’s
  years; align the sheet title year if it was wrong.
- **Interim config pinned to an old snapshot.** A YAML that sets
  `snapshot_version_or_git_sha` and flips an interim flag may fail to
  resolve methods on current `main`. Run against the ref that matches the
  pin, or fold the effect into a bundled config that `main` supports.
- **Empty sheet after a “successful” dispatch.** The workflow triggered but
  the model job failed; check the Actions log. Re-dispatch reuses the same
  `sheet_id`.
- **Dropped concurrent runs.** Wait for capacity (or use a dispatcher that
  polls) before the next `gh workflow run`.

## End-to-end path

1. Add flag + gate + atomic YAML — [`../config/feature_flag.md`](../config/feature_flag.md).
2. Dispatch `generate_diagnostics` for that `config_name` (this doc).
3. Plot with `diagnostics_plots`; combine only if comparing several runs.
4. Answer the interpret checklist before enabling the flag on a release
   config.
