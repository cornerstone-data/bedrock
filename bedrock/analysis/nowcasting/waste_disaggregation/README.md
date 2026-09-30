# Waste disaggregation (nowcasting)

Year-varying waste disaggregation weights on the nowcast EEIO path.

## Guiding document

[`.cursor/plans/waste_disagg_nowcasting_update.plan.md`](../../../../.cursor/plans/waste_disagg_nowcasting_update.plan.md) — implementation plan (phases live only there).

## Contents

| File | Role |
|------|------|
| [`feasibility_multi_year_weights.md`](feasibility_multi_year_weights.md) | Feasibility report |
| [`implementation_plan.md`](implementation_plan.md) | Stub → Cursor plan |
| [`impact_nowcast_updated_weights.md`](impact_nowcast_updated_weights.md) | 2024 impact note |
| [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md) | By-year GCS: year-aligned weights vs 2017 production |
| [`production_gate.md`](production_gate.md) | **FLIP** recorded; post-merge re-snapshot still required |
| [`who_buys_search.md`](who_buys_search.md) | Who-buys search (`freeze_confirmed`) |
| [`waste_n_variance.md`](waste_n_variance.md) | Absolute N/D + dominating weight slices |
| [`waste_y2y_comparison.md`](waste_y2y_comparison.md) | Control vs treatment year-to-year |
| [`flip_release_note.md`](flip_release_note.md) | Flip release note + snapshot checklist |
| [`impact_pins.py`](impact_pins.py) | Locked GCS MUT vintage + config name helpers |
| [`configs/`](configs/) | Analysis-only control/treatment YAMLs (GCS pin; year-aligned vs 2017) |
| `scripts/` | Probe, impact runner, GCS drivers, Y2Y / weight-delta |
| `figures/` | Impact / preview charts |
| `cache/` | Probe JSON, year×vintage impact dirs, `y2y_waste_*.csv` |

## Commands

```bash
# GCS vintage gate (writes cache/gcs_mut_vintage.json)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.probe_gcs_mut_vintage

# Single-year impact (year×vintage cache)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs --year 2024

# All years on verified GCS pin (year-aligned vs 2017)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_gcs_impacts

# Y2Y panels (reads existing impact caches)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.summarize_waste_y2y_panels

# Weight deltas for extreme years
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.fill_weight_delta_2017_vs_2024 --year 2021
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.fill_weight_delta_2017_vs_2024 --year 2022
```

## Status

- Feasibility / initial wiring: **done** ([#977](https://github.com/cornerstone-data/bedrock/pull/977))
- GCS year-aligned impact package: **merged** ([#1029](https://github.com/cornerstone-data/bedrock/pull/1029)) — pin `v0.3.0_92b7a8a`; year-aligned vs 2017; `rcra_path=br_bypass`; local-MUT evidence retired
- Production Flip: **FLIP** — who-buys / variance / Y2Y evidence accepted; canonical nowcast on `match_io`; release note [`flip_release_note.md`](flip_release_note.md); snapshot / waterfall re-dispatch after merge
- Follow-on BR→FBS (bedrock BR→FBS): **post-v0.5**, separate PR
