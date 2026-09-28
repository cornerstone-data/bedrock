# Waste disaggregation (nowcasting)

Year-varying waste disaggregation weights on the nowcast EEIO path.

## Guiding document

[`.cursor/plans/waste_disagg_nowcasting_update.plan.md`](../../../../.cursor/plans/waste_disagg_nowcasting_update.plan.md)
(short summary: `waste_disagg_nowcasting_update_64a133fe.plan.md`).

## Contents

| File | Role |
|------|------|
| [`feasibility_multi_year_weights.md`](feasibility_multi_year_weights.md) | Phase 1 report |
| [`implementation_plan.md`](implementation_plan.md) | Stub → Cursor plan |
| [`impact_nowcast_updated_weights.md`](impact_nowcast_updated_weights.md) | Phase 2 / Phase 3.1 2024 impact |
| [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md) | Phase 3.3 by-year GCS (+ Track C) |
| [`phase3_production_gate.md`](phase3_production_gate.md) | Phase 3.4 hold vs flip |
| [`phase3_pins.py`](phase3_pins.py) | Locked GCS MUT vintage + config name helpers |
| [`configs/`](configs/) | Analysis-only control/treatment YAMLs (Track A + Track C local) |
| `scripts/` | Probe, impact runner, Track A/B drivers |
| `figures/` | Impact / preview charts |
| `cache/` | Probe JSON, year×vintage impact dirs |

## Phase 3 commands

```bash
# Track A vintage gate (writes cache/phase3_gcs_mut_vintage.json)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.probe_gcs_mut_vintage

# Track B local MUT rebuild 2018-2024 (~2 h; no --gcs)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_phase3_local_mut_rebuild

# Single-year impact (year×vintage cache)
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_waste_weight_impact_efs --year 2024

# All Track A years on verified GCS pin
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.scripts.run_phase3_gcs_impacts
```

## Status

- Phase 1–2: **done** ([#977](https://github.com/cornerstone-data/bedrock/pull/977))
- Phase 3: **in progress** — GCS pin `v0.3.0_f709829`; pinned control/treatment YAMLs 2018–2024; BR bypass recorded as `rcra_path=br_bypass`
- Phase 4 (bedrock BR→FBS): **out of this PR**
