# Snapshots & releases

Snapshots are the source of truth for what the bedrock pipeline produced at a specific commit.
They are:

- The fixtures behind the snapshot integration tests in `bedrock/transform/__tests__/test_usa.py` (run twice every weekday on `main` via `test_integration.yml`)
- The baseline that diagnostics runs compare against (`USAConfig.snapshot_version_or_git_sha`)
- A reproducibility guarantee: every prior release remains independently reproducible because old snapshots are never deleted

When the pipeline changes in a way that legitimately changes its outputs, the integration tests will diff against the previous snapshot and fail. That signal is intentional, and the fix is to regenerate the snapshot and bump `.SNAPSHOT_KEY`. Tagging a release is a separate event that may bundle the snapshot bump together with non-output-changing work (docs, polish, bug fixes). This document is the team-wide playbook for both flows.

## Snapshot SHA vs. release tag SHA

These are **independent** concepts and frequently point at different commits:

- **Snapshot SHA** = the commit on `main` whose pipeline outputs were captured into `gs://cornerstone-default/snapshots/<sha>/`. Stored in `.SNAPSHOT_KEY`. Immutable per release.
- **Release tag SHA** = the commit on `main` that the `vX.Y.Z` tag points at. Marks "what users get when they check out this release." May include the snapshot bump *plus* any docs / polish / non-output-changing PRs that landed before the tag.

**Invariant**: at any tagged commit, the value of `.SNAPSHOT_KEY` is already the snapshot SHA for that release (i.e. the snapshot bump PR is an ancestor of the tagged commit). `git log <snapshot_sha>..<tag_sha>` gives you the docs/polish included in the release.

## Concepts

| Thing | Where | What it is |
|---|---|---|
| Snapshot artifacts | `gs://cornerstone-default/snapshots/<git_sha>/*.parquet` | The 10 parquet outputs of the canonical pipeline run at `<git_sha>`. See [`SNAPSHOT_NAMES`](names.py). |
| Snapshot key | [`.SNAPSHOT_KEY`](.SNAPSHOT_KEY) | The single SHA that integration tests load via `load_current_snapshot(...)`. Bumping this is what "uses the new snapshot" means. |
| Release tag | annotated git tag `v0.X.Y` | Marks a snapshot/release boundary so methodology evolution is easy to read in history. |
| Named release | [`releases.py`](releases.py) | Reference-only map of release labels → snapshot SHAs. Not imported by runtime code; update in Phase A alongside `.SNAPSHOT_KEY`. |
| Diagnostic baseline pin | `USAConfig.snapshot_version_or_git_sha` | `Literal[...]` of SHAs that any config may point at as its diagnostic baseline. Every released snapshot SHA must be added here. |
| Canonical config | [`2025_usa_cornerstone_v0_3.yaml`](../config/configs/2025_usa_cornerstone_v0_3.yaml) | The single config used to generate the snapshots that back `.SNAPSHOT_KEY`. Atomic configs are not snapshotted here; see "Adhoc snapshots" below. |

### Where snapshot SHAs live

| Store | Releases (`v0`, `v0.1`, `v0.2`) | Test-only SHAs |
|---|---|---|
| [`.SNAPSHOT_KEY`](.SNAPSHOT_KEY) | `7372464249...` (v0.2) | — |
| [`releases.py`](releases.py) | `v0`, `v0_1`, `v0_2` | `TEST_*` (intermediate bumps, not release labels) |
| `USAConfig.snapshot_version_or_git_sha` | `'v0'`, `1bda811e...` `# v0.1`, `7372464249...` `# v0.2` | `2ebb51f7...`, `9fe22d9a...` `# test` |

**Atomic config** — a YAML config that changes a single methodology flag relative to the baseline, used to measure that change in isolation.

`v0.2` is the current integration baseline. `v0` and `v0.1` are earlier release snapshots. The two `TEST_*` SHAs are intermediate bumps kept in the Literal so atomic configs and test fixtures can pin them for comparison.

## Versioning

Tags follow `v<major>.<minor>.<patch>`. The choice between patch and minor is **mechanical**: it depends only on whether `.SNAPSHOT_KEY` changed since the previous tag, not on a judgment call about methodology.

| Bump | When | Examples |
|---|---|---|
| **patch** (`v0.2.0` → `v0.2.1`) | `.SNAPSHOT_KEY` is unchanged from the previous tag. Used for docs, polish, refactors, bug fixes, and any other work that does not change pipeline outputs. | Docs update; CI tweak; comment cleanup; non-output-affecting refactor |
| **minor** (`v0.2.x` → `v0.3.0`) | `.SNAPSHOT_KEY` changed since the previous tag. Any cause — methodology choice flipped, upstream data refresh, dependency bump that perturbs floating-point arithmetic, etc. | Enabling waste disaggregation; switching GHG attribution method; FBA data refresh |
| **major** (`v0.x.y` → `v1.0.0`) | Reserved for the first official Cornerstone U.S. release. After that, breaking changes to the artifact contract (schema/shape changes that downstream consumers must adapt to). | First public release; output schema redesign |

A reviewer can verify the patch-vs-minor decision by inspecting the diff between tags: `git diff <prev_tag>..<new_tag> -- bedrock/utils/snapshots/.SNAPSHOT_KEY`.

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
- `config_name`: leave as the default `2025_usa_cornerstone_v0_3` (the canonical config)
- Leave `snapshot_prefix_override` blank so the prefix is the commit SHA

Wait for the success notification in `#alerts-bedrock`. Artifacts will be at `gs://cornerstone-default/snapshots/<sha>/`.

**A3. Open the snapshot bump PR.**
On a new branch, make exactly these changes (and nothing else — keep the bump PR mechanical and reviewable in under a minute):

- [ ] [`bedrock/utils/snapshots/.SNAPSHOT_KEY`](.SNAPSHOT_KEY) — replace the file's only line with the new SHA.
- [ ] [`bedrock/utils/snapshots/releases.py`](releases.py) — add the new release constant (e.g. `v0_3_0 = "<sha>"`) with a trailing `# config: <stem>` comment (the `generate_snapshots --config_name` value). Leave prior release entries in place. Use underscores in the Python identifier; the git tag uses dots. Do **not** add entries for patch-only releases.
- [ ] [`bedrock/utils/config/usa_config.py`](../config/usa_config.py) — extend the `snapshot_version_or_git_sha: Literal[...]` to include the new SHA, with a trailing comment noting the release label (e.g. `# v0.3.0`). Do **not** remove old SHAs — atomic configs and test fixtures may still reference them.
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

**B3. Tag `main`.**

```bash
git checkout main && git pull
SNAP=$(cat bedrock/utils/snapshots/.SNAPSHOT_KEY)
git tag -a v0.X.Y -m "Bedrock release v0.X.Y

Snapshot SHA:  $SNAP
GCS prefix:    gs://cornerstone-default/snapshots/$SNAP/
Canonical config: 2025_usa_cornerstone_v0_3

Highlights since previous tag:
- <bullet>
- <bullet>
"
git push origin v0.X.Y
```

The tag is annotated (not lightweight) so the release notes show up in `git log` and `git tag -n`.

**B4. Create a GitHub Release.**

- GitHub → Releases → Draft new release → pick tag `v0.X.Y`
- Title: `v0.X.Y`
- Body: paste the tag message, plus a generated "what changed" section. The easiest way is `git log <prev_tag>..v0.X.Y --oneline` and group entries into Methodology / Docs / Fixes / Other.

**B5. Announce in Slack.**
Post in `#alerts-bedrock` (and any other relevant channel):

> :package: **Bedrock release `v0.X.Y`**
> Tag SHA: `<short_tag_sha>` ([compare](https://github.com/cornerstone-data/bedrock/compare/v0.X.<prev>...v0.X.Y))
> Snapshot SHA: `<short_snapshot_sha>` (unchanged from previous release / new since `v0.X.<prev>`)
> Canonical config: `2025_usa_cornerstone_v0_3`
> Highlights: <one or two lines>
> Downstream impact: <e.g. diagnostics baselines should bump to this SHA when ready>

**B6. (Optional) Update downstream `snapshot_version_or_git_sha` defaults.**
If you want existing configs to compare against the new baseline by default, follow up with a PR that flips their `snapshot_version_or_git_sha`. This is intentionally a separate PR so diagnostic baseline moves are explicit.

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

--- a/bedrock/utils/config/usa_config.py
+++ b/bedrock/utils/config/usa_config.py
     snapshot_version_or_git_sha: ta.Literal[
         'v0',
         '1bda811e0169436ae90fd356fbef512ce7518ccb',  # v0.1
         '2ebb51f7190c3a62b5d8b2420bff9b20f57282fc',  # test
         '9fe22d9afdfdb6806397b2356eb3cf4c4c346744',  # test: snapshot from 2025_usa_cornerstone_fbs_schema
         '7372464249c434c9bebb172c065a4d0e3702176e',  # v0.2 (current .SNAPSHOT_KEY)
+        '<new_sha>',                                  # v0.3.0
     ] = 'v0'
```

Three files, no other code touched. If anything else needs to change, it belongs in a separate PR.

## Rolling back

The two phases roll back independently.

**Rolling back a snapshot bump (Phase A).** Snapshots are immutable in GCS, so a rollback is just a revert of the bump PR — that restores the previous `.SNAPSHOT_KEY` value. Old SHAs in `USAConfig.snapshot_version_or_git_sha` and `releases.py` are kept around precisely so historical baselines remain queryable. If a tag has already been cut on top of the bump PR, see "rolling back a release" below.

**Rolling back a release (Phase B).** Delete the tag locally and on origin (`git tag -d v0.X.Y && git push origin :refs/tags/v0.X.Y`) and delete the corresponding GitHub Release. The commits on `main` stay. This is safe because the tag is just a label; downstream consumers who already pulled it should be notified explicitly. Prefer cutting a new tag with the fix rather than deleting and re-creating the same tag.

If a bad snapshot accidentally got uploaded under a SHA you want to keep, you can regenerate by running the workflow again with `--snapshot_prefix_override` set to the same SHA; existing files in the GCS prefix will be overwritten on re-upload. Prefer not to do this — cut a new release instead so the audit trail stays clean.

## Adhoc snapshots

The `generate_snapshots.py` script supports `--adhoc`, which uploads to `gs://cornerstone-default/snapshots/<sha>/adhoc/` instead of the top-level SHA folder. Use this for:

- Snapshotting an atomic config (anything other than `2025_usa_cornerstone_v0_3`)
- Local experimentation where you don't want to pollute the canonical snapshot prefix

Adhoc snapshots are never wired into `.SNAPSHOT_KEY` or `releases.py`. They exist for ad-hoc diagnostic comparisons only.

## File map

| File | Role |
|---|---|
| [`.SNAPSHOT_KEY`](.SNAPSHOT_KEY) | The pinned SHA that integration tests load |
| [`generate_snapshots.py`](generate_snapshots.py) | CLI that builds the 10 parquet snapshots and uploads to GCS |
| [`loader.py`](loader.py) | `load_current_snapshot`, `load_configured_snapshot`, GCS download helpers |
| [`names.py`](names.py) | `SnapshotName` literal type and `SNAPSHOT_NAMES` list |
| [`releases.py`](releases.py) | Reference-only release label → snapshot SHA map (not imported by code) |
| [`../config/usa_config.py`](../config/usa_config.py) | `USAConfig.snapshot_version_or_git_sha` `Literal` of allowed baseline SHAs |
| [`../../../.github/workflows/generate_snapshots.yml`](../../../.github/workflows/generate_snapshots.yml) | `workflow_dispatch` CI that runs `generate_snapshots.py` |
| [`../../../.github/workflows/test_integration.yml`](../../../.github/workflows/test_integration.yml) | Scheduled CI that diffs current pipeline output against `.SNAPSHOT_KEY` |
