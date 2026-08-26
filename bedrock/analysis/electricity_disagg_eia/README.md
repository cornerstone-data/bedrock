# Electricity disaggregation — prior-production freeze

Snapshot of the previous G/T/D split and mixed-units conversion, taken before
replacing those functions with EIA-anchored generation / transmission /
distribution allocation.

Do not freeze footing or reallocation configs.

## Run

From the repo root:

```text
python -m bedrock.analysis.electricity_disagg_eia.snapshot_current_production
```

Writes under `output/baseline_current_production/<config>/`. Re-run only on
the pre-replacement code; overwriting with the new allocator would destroy
the comparison baseline.

## Files (per config)

| File | Contents |
|---|---|
| `q.parquet` | Published commodity output |
| `x.parquet` | Industry GO used as B's `x` (`derive_cornerstone_x_after_redefinition`) |
| `use_y_generation.parquet` | Generation commodity Use+Y by purchaser |
| `intersection_3x3.parquet` | Electricity Use 3×3 (`Udom` and `Uimp`) |
| `electricity_rows_y.parquet` | Electricity commodity rows of Use and Y |
| `E.parquet` / `D.parquet` / `N.parquet` / `BLy.parquet` | Live E, D, N, BLy |
| `class_generation_mwh.parquet` | Class generation (MWh under mixed units; $ under G/T/D-only). Exports = `F04000` sliced out of Commercial. HH = `F01000`. |
| `c_row.parquet` | Mixed-units only: class-varying generation row factors |
| `run_metadata.json` | git SHA, datetime, flags, how `p` is recorded on the frozen path |

G/T/D-only `p` is `N/A`. Mixed-units freeze records implied `q_$[221110] / eGRID`
(the frozen `1/c_col`) plus class-varying `c_row`. That implied price is not
the EIA-anchored generation price used in production today.
