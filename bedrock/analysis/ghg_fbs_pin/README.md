# Cornerstone GHG FBS pin bump

Operator toolkit for rebuilding the Cornerstone GHG FBS and drafting a pin
bump (upload + JSON promote remain manual). See also
`bedrock/utils/snapshots/README.md` (Bump cascade by change type, Phase A A2,
Cornerstone GHG FBS pin).

When canonical `nowcast_mut_vintage` (or other GHG FBS inputs) moves, run this
toolkit and promote the pin **before** `generate_snapshots` / a `.SNAPSHOT_KEY`
bump. The next snapshot loads E from `cornerstone_ghg_fbs_filename`; it does
not regenerate the FBS.

Prefer a green `nowcast_integration` /
`test_balance_2024_matches_pinned_sut` when the balanced SUT pin was updated
for this nowcast rebuild. GHG Use attribution follows the rebuild config's
`nowcast_mut_vintage`, not the SUT pin filename — SUT-pin lag is not an
automatic blocker when the balance gate is deliberately on a prior
methodology config. Still require Step 7 `--check` / upload confidence for
the MUT named on the config before promoting FBS.

## Gas totals vs current pin

```powershell
$env:Path = "$env:USERPROFILE\.local\bin;$env:Path"
uv run python -m bedrock.analysis.ghg_fbs_pin.compare_gas_e --rebuild
```

Rebuild under the USA config the new pin will name in `usa_config` (canonical
for a release rebuild).

## Upstream input pin (before rebuild)

Locks (1) exact Energy FBS filenames and (2) NowcastMUT vintage per inventory
year. Nowcast Use FBAs are not pinned as separate objects: they are built from
the after-redefinition Use table under ``flowsa/NowcastMUT``. Align
[`upstream_inputs_pin.json`](upstream_inputs_pin.json) so MUT vintages match
the config you are rebuilding under (for a canonical v1.0 rebuild, the same
vintage as ``nowcast_mut_vintage`` on that config). Refresh Energy FBS
filenames the same way when those inputs move.

The rebuild config's ``nowcast_mut_vintage`` is the intended MUT source for
that Use FBA (falling back to newest GCS only when the config names none).
Until the Nowcast Use FBA parse is fully config-aware, quarantine rival MUT
vintages and stale Nowcast Use FBAs so regen cannot load the wrong table.
The preflight requires the pinned MUT tables locally, no competing MUT
vintages for that year/stage, and no stale Nowcast Use FBA in ``FBA_DIR``.

```powershell
# Stewi / facilitymatcher (also invoked by the upstream check when enabled)
uv run python -m bedrock.utils.snapshots.stewi_facility_pin

# Energy FBS + NowcastMUT; fails on missing files, rivals, or stale Nowcast FBAs
uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs

# Optional: fetch missing pinned objects from GCS (still fails on rivals)
# uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs --download

# Move Energy FBS rivals, other MUT vintages, and Nowcast Use FBAs to _quarantine/
# uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs --quarantine-rivals

# Optional: fill empty Energy FBS filenames from newest local match, then save
# uv run python -m bedrock.analysis.ghg_fbs_pin.check_upstream_inputs --discover --write
```

## Rebuild + draft pin JSON

```powershell
uv run python -m bedrock.analysis.ghg_fbs_pin.prepare_pin
```

Writes under `output/` (gitignored if covered by analysis patterns; otherwise local-only):

- the rebuilt `{method}_v{version}_{hash}.parquet`
- `cornerstone_ghg_fbs_2024_pin.DRAFT.json` with `method`, `filename`, `sha256`,
  and `usa_config` (the active config stem used for the rebuild)

Then upload the parquet to `gs://cornerstone-default/transform/output_data/`,
promote the draft pin (keep `usa_config`), and set `cornerstone_ghg_fbs_filename`
on the canonical USA config (and matching year series) to the new filename.
Frozen waterfall / `COMBINATIONS` decks move only with a deliberate deck
decision. Confirm `fbs_regeneration`, then cut the USA snapshot (Phase A A3).
