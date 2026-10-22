# Cornerstone GHG FBS pin bump

Operator toolkit for rebuilding the Cornerstone GHG FBS and drafting a pin
bump (upload + JSON promote remain manual). See also
`bedrock/utils/snapshots/README.md` (Cornerstone GHG FBS pin).

## Gas totals vs current pin

```powershell
$env:Path = "$env:USERPROFILE\.local\bin;$env:Path"
uv run python -m bedrock.analysis.ghg_fbs_pin.compare_gas_e --rebuild
```

## Upstream input pin (before rebuild)

Locks (1) exact Energy FBS filenames and (2) NowcastMUT vintage per inventory
year. Nowcast Use FBAs are not pinned: they are melted from MUT tables under
``flowsa/NowcastMUT``, and FBS regen uses ``latest_nowcast_mut_vintage`` (not
USAConfig), so the preflight requires the pinned vintage to be what GCS would
serve as latest and that no stale Nowcast Use FBA sits in ``FBA_DIR``.

2017 pins MUT ``v0.5.0_011f41f`` (compensation fix, #1123); 2018-2024 pin
``v0.5.0_a166975``. Edit [`upstream_inputs_pin.json`](upstream_inputs_pin.json)
when refreshing Energy inputs or MUT vintages.

```powershell
# Stewi / facilitymatcher (also invoked by the upstream check when enabled)
uv run python -m bedrock.utils.snapshots.stewi_facility_pin

# Energy FBS + NowcastMUT; fails on missing, rivals, stale Nowcast FBAs, or
# latest MUT vintage mismatch
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
- `cornerstone_ghg_fbs_2024_pin.DRAFT.json` with `method`, `filename`, `sha256`

Then upload the parquet to `gs://cornerstone-default/transform/output_data/`,
promote the draft pin, and set `cornerstone_ghg_fbs_filename` on the canonical
USA config (and matching waterfall pins) to the new year-stem filename.
