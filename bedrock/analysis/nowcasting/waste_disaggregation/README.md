# Waste disaggregation (nowcasting)

Year-varying waste disaggregation weights on the nowcast EEIO path.

## Guiding document

[`.cursor/plans/waste_disagg_nowcasting_update.plan.md`](../../../../.cursor/plans/waste_disagg_nowcasting_update.plan.md) (lone complete reference for Phases 1–4 / 3.2).

## Contents

| File | Role |
|------|------|
| [`feasibility_multi_year_weights.md`](feasibility_multi_year_weights.md) | Phase 1 report |
| [`implementation_plan.md`](implementation_plan.md) | Stub → Cursor plan |
| [`impact_nowcast_updated_weights.md`](impact_nowcast_updated_weights.md) | Phase 2 / Phase 3.1 2024 impact |
| [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md) | By-year GCS: year-aligned weights vs 2017 production |
| [`phase3_production_gate.md`](phase3_production_gate.md) | Flip recorded (Phase 3.2.D); snapshot ops remaining |
| [`phase32_who_buys_search.md`](phase32_who_buys_search.md) | Phase 3.2.A who-buys search (`freeze_confirmed`) |
| [`phase32_waste_n_variance.md`](phase32_waste_n_variance.md) | Phase 3.2.B absolute N/D + dominating slices |
| [`phase32_y2y_comparison.md`](phase32_y2y_comparison.md) | Phase 3.2.C control vs treatment Y2Y |
| [`phase32_flip_release_note.md`](phase32_flip_release_note.md) | Phase 3.2.D Flip release note + snapshot checklist |
| [`phase3_pins.py`](phase3_pins.py) | Locked GCS MUT vintage + config name helpers |
| [`configs/`](configs/) | Analysis-only control/treatment YAMLs (GCS pin; year-aligned vs 2017) |
| `scripts/` | Probe, impact runner, GCS drivers, Phase 3.2 Y2Y / weight-delta |
| `figures/` | Impact / preview charts |
| `cache/` | Probe JSON, year×vintage impact dirs, `phase32_y2y_waste_*.csv` |

## Phase 3 commands

```bash
# GCS vintage gate (writes cache/phase3_gcs_mut_vintage.json)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.probe_gcs_mut_vintage

# Single-year impact (year×vintage cache)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs --year 2024

# All years on verified GCS pin (year-aligned vs 2017)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_phase3_gcs_impacts

# Phase 3.2.C Y2Y panels (reads existing impact caches)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.summarize_phase32_y2y_panels

# Phase 3.2.B weight deltas for extreme years
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.fill_weight_delta_2017_vs_2024 --year 2021
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.fill_weight_delta_2017_vs_2024 --year 2022
```

## Status

- Phase 1–2: **done** ([#977](https://github.com/cornerstone-data/bedrock/pull/977))
- Phase 3 + 3.1: **merged** ([#1029](https://github.com/cornerstone-data/bedrock/pull/1029)) — GCS pin `v0.3.0_92b7a8a`; year-aligned vs 2017; `rcra_path=br_bypass`; Track C retired; production still on 2017
- Phase 3.2: **FLIP** — 3.2.A–C evidence accepted; canonical nowcast on `match_io`; release note [`phase32_flip_release_note.md`](phase32_flip_release_note.md); snapshot / waterfall re-dispatch after merge
- Phase 4 (bedrock BR→FBS): **post-v0.5**, separate PR
