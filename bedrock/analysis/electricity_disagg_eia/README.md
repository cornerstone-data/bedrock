# Electricity disaggregation — current-production freeze (P0)

Snapshot of **today's** 3-way split and mixed-units conversion, taken before
replacing those functions with the EIA-anchored G/T/D method ([Discussion #88](https://github.com/cornerstone-data/methods/discussions/88)).

Do not freeze footing or reallocation configs.

## Run

From the repo root:

```text
python -m bedrock.analysis.electricity_disagg_eia.snapshot_current_production
```

Writes under `output/baseline_current_production/<config>/`. Re-run only on
today's (pre-replacement) code.

## Files (per config)

| File | Contents |
|---|---|
| `q.parquet` | Published commodity output |
| `x.parquet` | Industry GO used as B's `x` (`derive_cornerstone_x_after_redefinition`) |
| `use_y_generation.parquet` | Generation commodity Use+Y by purchaser |
| `intersection_3x3.parquet` | Electricity Use 3×3 (`Udom` and `Uimp`) |
| `electricity_rows_y.parquet` | Electricity commodity rows of Use and Y |
| `E.parquet` / `D.parquet` / `N.parquet` / `BLy.parquet` | Live E, D, N, BLy |
| `class_generation_mwh.parquet` | Class generation (MWh under mixed units; $ under 3-way-only). Exports = `F04000` sliced out of Commercial. HH = `F01000`. |
| `c_row.parquet` | Mixed-units only: class-varying generation row factors |
| `run_metadata.json` | git SHA, datetime, flags, how `p` is recorded on today's path |

3-way-only `p` is `N/A`. Mixed-units freeze records implied `q_$[221110] / eGRID` (today's `1/c_col`) plus class-varying `c_row` — implied, not D0 `p`.
