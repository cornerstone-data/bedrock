# Snapshots & releases

Snapshots are the source of truth for what the bedrock pipeline produced at a specific commit.
They are:

- The fixtures behind the snapshot integration tests in `bedrock/transform/__tests__/test_usa.py` (run twice every weekday on `main` via `test_integration.yml`)
- Parquet sources for diagnostics comparison baselines — selected at dispatch via `--baseline` (see [`../validation/evaluate_feature_impact.md`](../validation/evaluate_feature_impact.md)); allowed snapshot keys live on `USAConfig.snapshot_version_or_git_sha`
- A reproducibility guarantee: every prior release remains independently reproducible because old snapshots are never deleted

When the pipeline changes in a way that legitimately changes its outputs, the integration tests will diff against the previous snapshot and fail. That signal is intentional, and the fix is to regenerate the snapshot and bump `.SNAPSHOT_KEY`. Tagging a release is a separate event that may bundle the snapshot bump together with non-output-changing work (docs, polish, bug fixes). This document is the team-wide playbook for both flows.

## Snapshot SHA vs. release tag SHA

These are **independent** concepts and frequently point at different commits:

- **Snapshot SHA** = the commit on `main` whose pipeline outputs were captured into `gs://cornerstone-default/snapshots/<sha>/`. Stored in `.SNAPSHOT_KEY`. Immutable per release.
- **Release tag SHA** = the commit on `main` that the `vX.Y.Z` tag points at. Marks "what users get when they check out this release." May include the snapshot bump *plus* any docs / polish / non-output-changing PRs that landed before the tag.

**Invariants** at any tagged commit:

- `.SNAPSHOT_KEY` is already the snapshot SHA for that release (the snapshot bump PR is an ancestor of the tagged commit). `git log <snapshot_sha>..<tag_sha>` gives you the docs/polish included in the release.
- `[project].version` in `pyproject.toml` equals the tag without the `v` prefix (tag `v0.5.0` → `0.5.0`).

## Concepts

| Thing | Where | What it is |
|---|---|---|
| Snapshot artifacts | `gs://cornerstone-default/snapshots/<git_sha>/*.parquet` | The 11 parquet outputs of the canonical pipeline run at `<git_sha>`. See [`SNAPSHOT_NAMES`](names.py). |
| Snapshot key | [`.SNAPSHOT_KEY`](.SNAPSHOT_KEY) | The single SHA that integration tests load via `load_current_snapshot(...)`. Bumping this is what "uses the new snapshot" means. |
| Release tag | annotated git tag `v0.X.Y` | Marks a snapshot/release boundary so methodology evolution is easy to read in history. |
| Package version | [`pyproject.toml`](../../../pyproject.toml) `[project].version` | Same number as the release tag without the `v` prefix (e.g. tag `v0.5.0` → `0.5.0`). `return_pkg_version` in [`settings.py`](../config/settings.py) stamps FBA/FBS filenames (`{method}_v{tool_version}_{git_hash}.parquet`) from this value. Bumped in Phase B before the tag. |
| Named release | [`releases.py`](releases.py) | Release label → snapshot SHA map, each entry commented with the config stem it was built from and the output change that made it necessary. Imported by [`diagnostics_baseline`](../validation/diagnostics_baseline.py) so `--baseline v0.3` (etc.) resolves; update in Phase A alongside `.SNAPSHOT_KEY`. |
| Diagnostics baseline alias | [`diagnostics_baseline.py`](../validation/diagnostics_baseline.py) `NAMED_BASELINES` | Short operator names (`ceda-v0`, `v0.3`, …) → `releases.py` constants. Add an entry when a new release is a common comparison target. Raw SHAs skip this map if they are already on the `Literal`. |
| Allowed snapshot keys | `USAConfig.snapshot_version_or_git_sha` | `Literal[...]` of SHAs diagnostics may load as `N_old` / `D_old`. Every released snapshot SHA must appear here. YAML default is `'v0'`; dispatch `--baseline` overrides for that run only. |
| Canonical config | [`2025_usa_cornerstone_v0_5.yaml`](../config/configs/2025_usa_cornerstone_v0_5.yaml) | The single config used to generate the snapshots that back `.SNAPSHOT_KEY`. Atomic configs are not snapshotted here; see "Adhoc snapshots" below. |
| Cornerstone GHG FBS pin | [`cornerstone_ghg_fbs_2024_pin.json`](cornerstone_ghg_fbs_2024_pin.json) | Pins one `GHG_national_Cornerstone_nowcast_facilities_2024` parquet in `transform/output_data` (filename + SHA256). Guards FBS regeneration in [`test_fbs.py`](../../transform/__tests__/test_fbs.py). See "Cornerstone GHG FBS pin" below. |
| Nowcast balanced SUT pin | [`nowcast_balanced_sut_2024_pin.json`](nowcast_balanced_sut_2024_pin.json) | Pins the 2024 balanced Supply and Use parquets in `flowsa/BalancedSUT` (filename + SHA256). Guards a `balance_year(2024)` rebuild in [`test_nowcast_balanced_sut_pin.py`](../../transform/__tests__/test_nowcast_balanced_sut_pin.py). See "Nowcast balanced SUT pin" below. |

### Pins vs runtime loaders

| Artifact | Pins | Consumed by |
|---|---|---|
| [`.SNAPSHOT_KEY`](.SNAPSHOT_KEY) | Full pipeline outputs at a git SHA | `test_usa.py` (`eeio_integration`) |
| [`cornerstone_ghg_fbs_2024_pin.json`](cornerstone_ghg_fbs_2024_pin.json) | One `GHG_national_Cornerstone_nowcast_facilities_2024` parquet | `test_fbs.py` (`eeio_integration`) |
| [`nowcast_balanced_sut_2024_pin.json`](nowcast_balanced_sut_2024_pin.json) | 2024 balanced Supply and Use | `test_nowcast_balanced_sut_pin.py` (`nowcast_integration`) |
| `_load_cornerstone_ghg_fbs_from_gcs` in [`derived.py`](../../transform/allocation/derived.py) | Nothing — selects via `_select_cornerstone_ghg_fbs_base_name` (newest upload for that base_name) | `load_E_from_flowsa()` for Cornerstone GHG |

The FBS pin and the runtime loader are independent: bumping the pin updates the regeneration test golden file; production still follows the latest GCS upload until `load_E_from_flowsa` is wired to the pin.

| Store | Releases (`v0`, `v0.1`, `v0.2`, `v0.3`, `v0.4`, `v0.5`) | Test-only SHAs |
|---|---|---|
| [`.SNAPSHOT_KEY`](.SNAPSHOT_KEY) | current release SHA | — |
| [`releases.py`](releases.py) | `v0`, `v0_1`, `v0_2`, `v0_3_0`, … | `TEST_*` (intermediate bumps, not release labels) |
| [`diagnostics_baseline.py`](../validation/diagnostics_baseline.py) `NAMED_BASELINES` | `ceda-v0`, `v0.3`, … | — |
| `USAConfig.snapshot_version_or_git_sha` | `'v0'`, release SHAs with `# v0.x` comments | `2ebb51f7...`, `9fe22d9a...` `# test` |

**Atomic config** — a YAML config that changes a single methodology flag relative to the baseline, used to measure that change in isolation. See [`../config/feature_flag.md`](../config/feature_flag.md). Diagnostics against a chosen snapshot or USEEIO baseline: [`../validation/evaluate_feature_impact.md`](../validation/evaluate_feature_impact.md).

`v0.2` is an earlier release snapshot. `v0` and `v0.1` predate it. The two `TEST_*` SHAs are intermediate bumps kept in the Literal so atomic configs and test fixtures can pin them for comparison.

## Versioning

Tags follow `v<major>.<minor>.<patch>`. The choice between patch and minor is **mechanical**: it depends only on whether `.SNAPSHOT_KEY` changed since the previous tag, not on a judgment call about methodology.

| Bump | When | Examples |
|---|---|---|
| **patch** (`v0.2.0` → `v0.2.1`) | `.SNAPSHOT_KEY` is unchanged from the previous tag. Used for docs, polish, refactors, bug fixes, and any other work that does not change pipeline outputs. | Docs update; CI tweak; comment cleanup; non-output-affecting refactor |
| **minor** (`v0.2.x` → `v0.3.0`) | `.SNAPSHOT_KEY` changed since the previous tag. Any cause — methodology choice flipped, upstream data refresh, dependency bump that perturbs floating-point arithmetic, etc. | Enabling waste disaggregation; switching GHG attribution method; FBA data refresh |
| **major** (`v0.x.y` → `v1.0.0`) | Reserved for the first official Cornerstone U.S. release. After that, breaking changes to the artifact contract (schema/shape changes that downstream consumers must adapt to). | First public release; output schema redesign |

A reviewer can verify the patch-vs-minor decision by inspecting the diff between tags: `git diff <prev_tag>..<new_tag> -- bedrock/utils/snapshots/.SNAPSHOT_KEY`.

Every Phase B release (patch or minor) also bumps `[project].version` in `pyproject.toml` to match the tag. That field is independent of `.SNAPSHOT_KEY`; leaving it stale does not break snapshot tests, but new FBA/FBS artifacts keep stamping the old `tool_version` until it is updated.

## When to cut a new snapshot (Phase A trigger)

A snapshot bump is **always initiated by a human** — never automatically — because an integration-test failure can be either an intended methodology change or an accidental regression. The trigger is usually one of:

1. **Integration tests start failing on `main`** after a PR merges. A Slack alert in `#alerts-bedrock` fires.
2. **A methodology PR is about to merge** and the author knows it will change outputs.
3. **An upstream data or dependency change** lands that perturbs outputs.

In every case, follow the decision flow:

```text
integration tests red?
        │
        ▼
   investigate: is the diff EXPECTED and CORRECT?
        │
   ┌────┴────┐
   no        yes
   │         │
revert     proceed to "Phase A" below
or fix
```

Do not bump `.SNAPSHOT_KEY` to silence a failure you have not diagnosed.

## When to cut a release (Phase B trigger)

Phase B is **scheduled**, not reactive. Cut a release when any of these is true and `main` is green:

- A methodology change has landed and its snapshot bump has merged — ship a **minor** release.
- A meaningful body of docs / polish / fixes has accumulated on `main` since the previous tag — ship a **patch** release.
- Downstream consumers need a stable reference point (e.g., a paper draft, an external review).

There is no requirement to tag every snapshot bump; small bumps can wait and be batched with docs into a single release.

## Release workflow

The release is split into two **independent** phases:

- **Phase A — Snapshot regeneration.** Runs whenever pipeline outputs change. May fire multiple times during a release cycle, or never. Produces a bump to `.SNAPSHOT_KEY`. **Does not tag.**
- **Phase B — Cutting the release.** Runs whenever the team decides to ship. Tags the current head of `main` with `vX.Y.Z`. Bundles whatever has landed (snapshot bump, docs, polish) into a release.

A patch release (`v0.2.0` → `v0.2.1`) skips Phase A entirely — only Phase B runs. A minor release (`v0.2.x` → `v0.3.0`) runs both.

### Roles

- **Release driver** — either the author of the pipeline-changing PR (for Phase A) or a designated release manager (for Phase B). Owns end-to-end execution.
- **Reviewer** — any team member with merge rights on the snapshot bump PR and on the release tag.

### Phase A — Snapshot regeneration

**A1. Land the change on `main`.**
Merge the methodology / pipeline-change PR. Note the merge commit SHA on `main`; this is what the snapshots will be generated against and what `.SNAPSHOT_KEY` will pin.

**A2. Generate snapshots in CI.**
Trigger the `generate_snapshots` workflow manually:

- GitHub → Actions → **generate_snapshots** → Run workflow
- Branch: `main` (or the specific SHA from step A1 if newer commits have landed)
- `config_name`: leave as the default `2025_usa_cornerstone_v0_5` (the canonical config)
- Leave `snapshot_prefix_override` blank so the prefix is the commit SHA

Wait for the success notification in `#alerts-bedrock`. Artifacts will be at `gs://cornerstone-default/snapshots/<sha>/`.

**A3. Open the snapshot bump PR.**
On a new branch, make exactly these changes (and nothing else — keep the bump PR mechanical and reviewable in under a minute):

- [ ] [`bedrock/utils/snapshots/.SNAPSHOT_KEY`](.SNAPSHOT_KEY) — replace the file's only line with the new SHA.
- [ ] [`bedrock/utils/snapshots/releases.py`](releases.py) — add the new release constant (e.g. `v0_4_0 = "<sha>"`) with a trailing `# config: <stem>` comment (the `generate_snapshots --config_name` value). Leave prior release entries in place. Use underscores in the Python identifier; the git tag uses dots. Do **not** add entries for patch-only releases. Register the snapshot's EF dollar year (``B`` / ``D`` / ``N`` intensity year) in ``EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY`` in the same edit. Add an `# Output change:` comment above the constant naming the work that moved the outputs (with the PR number), so the release is identifiable without reconstructing it from `git log --follow` on `.SNAPSHOT_KEY`.
- [ ] [`bedrock/utils/config/usa_config.py`](../config/usa_config.py) — extend the `snapshot_version_or_git_sha: Literal[...]` to include the new SHA, with a trailing comment noting the release label (e.g. `# v0.4.0`). Do **not** remove old SHAs — atomic configs and test fixtures may still reference them.
- [ ] [`bedrock/utils/validation/diagnostics_baseline.py`](../validation/diagnostics_baseline.py) — when the release is a common diagnostics comparison target, add a short alias to `NAMED_BASELINES` (e.g. `'v0.4': releases.v0_4_0`, `'v0.4.0': releases.v0_4_0`). Operators can always pass the raw SHA via `--baseline` once the `Literal` includes it; the alias is for convenience (`--baseline v0.4`).
- [ ] Title: `release: snapshot bump (anticipated v0.X.Y)`
- [ ] Description: short summary of the output delta vs. the previous snapshot, plus the GCS URL `gs://cornerstone-default/snapshots/<new_sha>/`.

**A4. Verify before merging.**
On the bump PR's branch, the integration tests should pass against the new snapshot:

```bash
uv run pytest bedrock/transform/__tests__/test_usa.py -m eeio_integration
```

Re-run locally if CI runners hit transient flakes. The bump PR is the only safe place to verify because once `.SNAPSHOT_KEY` changes, the test pass/fail signal is what guards the release.

**A5. Merge the bump PR.**
At this point `main` carries the new snapshot. No tag yet. Other PRs (docs, polish, fixes) can continue to merge until you're ready for Phase B.

### Phase B — Cutting the release

**B1. Decide patch vs. minor.**
From a fresh `main`:

```bash
git fetch --tags
git diff <latest_tag>..main -- bedrock/utils/snapshots/.SNAPSHOT_KEY
```

- Diff is empty → **patch** bump (`v0.X.<Y+1>`).
- Diff is non-empty → **minor** bump (`v0.<X+1>.0`).

If you're cutting `v1.0.0` or a later major release, that's a deliberate methodology + comms decision separate from this flow.

**B2. Confirm `main` is shippable.**
Integration tests green on the most recent scheduled run. CI on `main` is green. Any docs PRs intended for this release have already merged.

**B3. Bump the package version.**
On a small PR (or the same PR as any last docs/polish for the release), set `[project].version` in [`pyproject.toml`](../../../pyproject.toml) to the version you are about to tag — no `v` prefix (e.g. tag `v0.5.0` → `version = "0.5.0"`). Run `uv lock` so [`uv.lock`](../../../uv.lock) records the same `bedrock` version, and commit both files. Merge before tagging. Do this for every release, patch or minor. Already-uploaded FBA/FBS objects keep their old `tool_version` in the filename; only new regenerations pick up the bumped value via `return_pkg_version`.

**B4. Tag `main`.**

```bash
git checkout main && git pull
SNAP=$(cat bedrock/utils/snapshots/.SNAPSHOT_KEY)
git tag -a v0.X.Y -m "Bedrock release v0.X.Y

Snapshot SHA:  $SNAP
GCS prefix:    gs://cornerstone-default/snapshots/$SNAP/
Canonical config: 2025_usa_cornerstone_v0_5

Highlights since previous tag:
- <bullet>
- <bullet>
"
git push origin v0.X.Y
```

The tag is annotated (not lightweight) so the release notes show up in `git log` and `git tag -n`. Confirm `pyproject.toml` on the tagged commit reads `version = "0.X.Y"` before pushing the tag.

**B5. Create a GitHub Release.**

- GitHub → Releases → Draft new release → pick tag `v0.X.Y`
- Title: `v0.X.Y`
- Body: paste the tag message, plus a generated "what changed" section. The easiest way is `git log <prev_tag>..v0.X.Y --oneline` and group entries into Methodology / Docs / Fixes / Other.

**B6. Announce in Slack.**
Post in `#alerts-bedrock` (and any other relevant channel):

> :package: **Bedrock release `v0.X.Y`**
> Tag SHA: `<short_tag_sha>` ([compare](https://github.com/cornerstone-data/bedrock/compare/v0.X.<prev>...v0.X.Y))
> Snapshot SHA: `<short_snapshot_sha>` (unchanged from previous release / new since `v0.X.<prev>`)
> Canonical config: `2025_usa_cornerstone_v0_5`
> Highlights: <one or two lines>
> Downstream impact: <e.g. use `--baseline v0.X` (or the new SHA) on diagnostics dispatch when comparing to this release>

**B7. (Optional) Announce diagnostics baseline for the release.**
Model config YAMLs leave `snapshot_version_or_git_sha` at `'v0'`. To compare diagnostics against the new release snapshot, pass `--baseline v0.X` on `generate_diagnostics` / the workflow `baseline` input, or the raw SHA once it is on the `Literal`. See [`../validation/evaluate_feature_impact.md`](../validation/evaluate_feature_impact.md) (§ Choose a baseline). Do not flip YAML defaults solely to change the comparison target.

## Anatomy of the Phase A snapshot bump PR

A clean bump PR diff looks roughly like this (anticipating release `v0.3.0`):

```diff
--- a/bedrock/utils/snapshots/.SNAPSHOT_KEY
+++ b/bedrock/utils/snapshots/.SNAPSHOT_KEY
-7372464249c434c9bebb172c065a4d0e3702176e
+<new_sha>

--- a/bedrock/utils/snapshots/releases.py
+++ b/bedrock/utils/snapshots/releases.py
 v0_2 = "7372464249c434c9bebb172c065a4d0e3702176e"  # config: 2025_usa_cornerstone_v0_2
+v0_3_0 = "<new_sha>"  # config: 2025_usa_cornerstone_v0_3
+
+ EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY: dict[str, int] = {
+     ...
+     v0_3_0: 2024,
+ }

--- a/bedrock/utils/config/usa_config.py
+++ b/bedrock/utils/config/usa_config.py
     snapshot_version_or_git_sha: ta.Literal[
         'v0',
         '1bda811e0169436ae90fd356fbef512ce7518ccb',  # v0.1
         '2ebb51f7190c3a62b5d8b2420bff9b20f57282fc',  # test
         '9fe22d9afdfdb6806397b2356eb3cf4c4c346744',  # test: snapshot from 2025_usa_cornerstone_fbs_schema
         '7372464249c434c9bebb172c065a4d0e3702176e',  # v0.2
+        '<new_sha>',                                  # v0.4.0
     ] = 'v0'

--- a/bedrock/utils/validation/diagnostics_baseline.py
+++ b/bedrock/utils/validation/diagnostics_baseline.py
 NAMED_BASELINES: dict[str, str] = {
     'ceda-v0': releases.v0,
     'v0.3': releases.v0_3_0,
+    'v0.4': releases.v0_4_0,
+    'v0.4.0': releases.v0_4_0,
 }
```

Four mechanical files (`.SNAPSHOT_KEY`, `releases.py` including ``EF_DOLLAR_YEAR_BY_SNAPSHOT_KEY``, `usa_config.py`, and optionally `diagnostics_baseline.py` when adding a named alias). If anything else needs to change, it belongs in a separate PR.

## Rolling back

The two phases roll back independently.

**Rolling back a snapshot bump (Phase A).** Snapshots are immutable in GCS, so a rollback is just a revert of the bump PR — that restores the previous `.SNAPSHOT_KEY` value. Old SHAs in `USAConfig.snapshot_version_or_git_sha` and `releases.py` are kept around precisely so historical baselines remain queryable. If a tag has already been cut on top of the bump PR, see "rolling back a release" below.

**Rolling back a release (Phase B).** Delete the tag locally and on origin (`git tag -d v0.X.Y && git push origin :refs/tags/v0.X.Y`) and delete the corresponding GitHub Release. The commits on `main` stay. This is safe because the tag is just a label; downstream consumers who already pulled it should be notified explicitly. Prefer cutting a new tag with the fix rather than deleting and re-creating the same tag.

If a bad snapshot accidentally got uploaded under a SHA you want to keep, you can regenerate by running the workflow again with `--snapshot_prefix_override` set to the same SHA; existing files in the GCS prefix will be overwritten on re-upload. Prefer not to do this — cut a new release instead so the audit trail stays clean.

## Adhoc snapshots

The `generate_snapshots.py` script supports `--adhoc`, which uploads to `gs://cornerstone-default/snapshots/<sha>/adhoc/` instead of the top-level SHA folder. Use this for:

- Snapshotting an atomic config (anything other than `2025_usa_cornerstone_v0_5`)
- Local experimentation where you don't want to pollute the canonical snapshot prefix

Adhoc snapshots are never wired into `.SNAPSHOT_KEY` or `releases.py`. They exist for ad-hoc diagnostic comparisons only.

## Cornerstone GHG FBS pin

[`cornerstone_ghg_fbs_2024_pin.json`](cornerstone_ghg_fbs_2024_pin.json) pins one `GHG_national_Cornerstone_nowcast_facilities_2024` FlowBySector parquet under `gs://cornerstone-default/transform/output_data/`. `use_facility_ghg_attribution` selects that method stem. This JSON is the weekday regeneration golden. A config's `cornerstone_ghg_fbs_filename`, when set, names the parquet that config loads. [`test_fbs.py`](../../transform/__tests__/test_fbs.py) regenerates the method from YAML + GCS sources (including the stewi facility pin) and compares the frame to the pinned file (via [`fbs_pin.py`](fbs_pin.py)). The test runs on the weekday `test_integration` schedule alongside `test_usa.py`.

### When to bump the pin

Bump the pin when the team **intentionally** ships a new `GHG_national_Cornerstone_nowcast_facilities_2024` FBS to GCS, for example:

- Changes to [`GHG_national_Cornerstone_nowcast_facilities_2024.yaml`](../../transform/ghg/GHG_national_Cornerstone_nowcast_facilities_2024.yaml) or its upstream FBAs / facility attribution path
- A new stewi or facilitymatcher stem adopted in [`stewi_facility_pin.json`](stewi_facility_pin.json) that changes facilities FBS output
- A refreshed Energy FBS input that changes Hybrid manufacturing shares

Do **not** bump the pin to silence a failing test without diagnosing the diff. A red `test_generate_nowcast_facilities_ghg_fbs_2024_matches_pinned_reference` means either regeneration is broken or the pin is stale relative to an upload that was already blessed.

A pin bump is separate from a `.SNAPSHOT_KEY` bump. Snapshot tests cover the full pipeline; the FBS pin covers only whether `generateFlowBySector('GHG_national_Cornerstone_nowcast_facilities_2024')` still reproduces the committed golden parquet.

### How to bump the pin

**1. Regenerate and upload.**

Confirm the stewi facility pin preflight passes (`uv run python -m bedrock.utils.snapshots.stewi_facility_pin`), then run `FlowBySector.generateFlowBySector('GHG_national_Cornerstone_nowcast_facilities_2024', download_sources_ok=True)` locally (or via an internal job). Upload the parquet and metadata JSON to `gs://cornerstone-default/transform/output_data/`. Filenames follow `{method}_v{tool_version}_{git_hash}.parquet`.

**2. Compute SHA256** of the uploaded parquet:

```powershell
uv run python -c "import hashlib, sys; p=sys.argv[1]; h=hashlib.sha256(open(p,'rb').read()).hexdigest(); print(h)" path\to\GHG_national_Cornerstone_nowcast_facilities_2024_....parquet
```

**3. Update** [`cornerstone_ghg_fbs_2024_pin.json`](cornerstone_ghg_fbs_2024_pin.json):

- `filename` — exact GCS object name
- `sha256` — 64-char hex digest from step 2
- `method` and `gcs_sub_bucket` stay `GHG_national_Cornerstone_nowcast_facilities_2024` and `transform/output_data` unless the bucket layout changes

**4. Verify** on the pin-bump branch:

```powershell
uv run pytest bedrock/transform/__tests__/test_fbs.py -m eeio_integration -v
```

**5. Merge** the pin bump — in the same PR as the FBS regen/upload when possible, or immediately after. Keep the diff mechanical: the JSON pin file (and any upload-related method changes) only.

After upload, `_load_cornerstone_ghg_fbs_from_gcs` picks up the new parquet on the next run (newest `base_name` match) even before the pin bump merges; the pin bump aligns CI with the blessed file.

## Nowcast balanced SUT pin

[`nowcast_balanced_sut_2024_pin.json`](nowcast_balanced_sut_2024_pin.json) pins the 2024 balanced Supply and Use tables under `gs://cornerstone-default/flowsa/BalancedSUT/`. Filename and SHA256 live in that file. The pin does not cover the Step 6 MUT or the after-redefinition MUT. v0.5 still loads the after-redefinition tables named by `nowcast_mut_vintage`.

The pin is independent of `nowcast_mut_vintage` and `.SNAPSHOT_KEY`. [`test_nowcast_balanced_sut_pin.py`](../../transform/__tests__/test_nowcast_balanced_sut_pin.py) calls `balance_year(2024)` under `2025_usa_cornerstone_v0_6`, with FBS reads and writes pointed at an empty directory, and compares the frames `save_balance` writes (Use after the residue sweep) to the pinned parquets. Missing FlowByActivity inputs are loaded from `flowsa/FlowByActivity` before any Census, EIA, or BEA call. The test is marked `nowcast_integration` and runs as its own job on the weekday `test_integration` schedule. One year is about 15-17 minutes when inputs are already local; a cold rebuild is longer, and the job timeout is 120 minutes.

### When to bump the pin

Bump the pin when a new balanced 2024 Supply and Use upload is the table the rebuild should match. Do not bump the pin to silence a failing test without diagnosing the diff. A red `test_balance_2024_matches_pinned_sut` means either regeneration is broken or the pin names a vintage the current code no longer reproduces.

A pin bump is separate from a `.SNAPSHOT_KEY` bump and from the FBS pin bump. Do not point the test at `latest_nowcast_mut_vintage` or at `nowcast_mut_vintage`.

### How to bump the pin

The golden files are objects already on GCS. After the balanced Supply and Use for 2024 are uploaded:

1. Set `filename` to the exact object names under `flowsa/BalancedSUT`.
2. Set `sha256` to the digest of each parquet.
3. Leave `gcs_sub_bucket` as `flowsa/BalancedSUT` and `usa_config` as `2025_usa_cornerstone_v0_6` unless the rebuild's methodology config changes. `PIN_USA_CONFIG` in `nowcast_balanced_sut_pin.py` must match that name.

```powershell
uv run python -c "import hashlib, sys; p=sys.argv[1]; print(hashlib.sha256(open(p,'rb').read()).hexdigest())" path\to\Balanced_Detail_Use_SUT_2024_....parquet
```

Verify:

```powershell
uv run pytest bedrock/transform/__tests__/test_nowcast_balanced_sut_pin.py -m nowcast_integration -v
```

Merge the pin bump with the upload when they ship together. Keep the diff mechanical: the JSON pin file, unless the same change is what moved the balanced tables.

## File map

| File | Role |
|---|---|
| [`.SNAPSHOT_KEY`](.SNAPSHOT_KEY) | The pinned SHA that integration tests load |
| [`../../../pyproject.toml`](../../../pyproject.toml) | `[project].version` stamped onto FBA/FBS artifacts; bump in Phase B to match the tag |
| [`generate_snapshots.py`](generate_snapshots.py) | CLI that builds the 11 parquet snapshots and uploads to GCS |
| [`loader.py`](loader.py) | `load_current_snapshot`, `load_configured_snapshot`, GCS download helpers |
| [`names.py`](names.py) | `SnapshotName` literal type and `SNAPSHOT_NAMES` list |
| [`releases.py`](releases.py) | Release label → snapshot SHA map; imported by `diagnostics_baseline` |
| [`../validation/diagnostics_baseline.py`](../validation/diagnostics_baseline.py) | Maps `--baseline` labels (`ceda-v0`, `v0.3`, …) to snapshot keys |
| [`../config/usa_config.py`](../config/usa_config.py) | `USAConfig.snapshot_version_or_git_sha` `Literal` of allowed baseline SHAs |
| [`../../../.github/workflows/generate_snapshots.yml`](../../../.github/workflows/generate_snapshots.yml) | `workflow_dispatch` CI that runs `generate_snapshots.py` |
| [`../../../.github/workflows/test_integration.yml`](../../../.github/workflows/test_integration.yml) | Weekday CI: `eeio_integration` against `.SNAPSHOT_KEY`, and `nowcast_integration` against the balanced SUT pin |
| [`cornerstone_ghg_fbs_2024_pin.json`](cornerstone_ghg_fbs_2024_pin.json) | Pinned `GHG_national_Cornerstone_nowcast_facilities_2024` parquet for FBS regen test |
| [`fbs_pin.py`](fbs_pin.py) | Load pin JSON, download from GCS, verify SHA256 |
| [`nowcast_balanced_sut_2024_pin.json`](nowcast_balanced_sut_2024_pin.json) | Pinned 2024 balanced Supply and Use |
| [`nowcast_balanced_sut_pin.py`](nowcast_balanced_sut_pin.py) | Load balanced SUT pin JSON, download from GCS, verify SHA256 |
| [`../../transform/__tests__/test_nowcast_balanced_sut_pin.py`](../../transform/__tests__/test_nowcast_balanced_sut_pin.py) | `nowcast_integration` test: `balance_year(2024)` vs pinned Supply and Use |
| [`stewi_facility_pin.json`](stewi_facility_pin.json) | Pinned stewi GHGRP/NEI + facilitymatcher outputs for facilities FBS |
| [`stewi_facility_pin.py`](stewi_facility_pin.py) | Load pin, download from GCS, preflight check |
| [`../../transform/__tests__/test_fbs.py`](../../transform/__tests__/test_fbs.py) | `eeio_integration` test: regen vs pinned FBS |

## Stewi + facilitymatcher pin

[`stewi_facility_pin.json`](stewi_facility_pin.json) pins the **produced** stewi inventory parquets (`flowbyprocess`, `facility`) and facilitymatcher outputs (`FacilityMatchList_forStEWI`, `FRS_NAICSforStEWI`) used by facilities GHG FBS builds. It does not pin the FRS national zip or other matcher raw inputs.

Objects live under `gs://cornerstone-default/stewi/` and `gs://cornerstone-default/facilitymatcher/`. [`stewi_facility_pin.py`](stewi_facility_pin.py) downloads missing pinned files before `build_facility_combustion` when `BEDROCK_STEWI_FACILITY_PIN` is enabled (default on; set to `off` to skip).

### When to bump the pin

Bump when the team intentionally adopts new stewi inventory stems or a new facilitymatcher build for release facilities FBS work. Upload the new parquets (and metadata JSON) to GCS first, then edit the pin JSON stems.

### How to bump / verify

```powershell
# After uploading new stems to GCS and editing stewi_facility_pin.json:
uv run python -m bedrock.utils.snapshots.stewi_facility_pin
```

Preflight fails if a pinned parquet is missing locally and cannot be downloaded, or if a competing local version exists for the same inventory year.
