# Waste disaggregation (nowcasting)

Phase 1 feasibility for **year-varying waste disaggregation weights** on the
nowcast EEIO path. Phase 2 code is gated on **Decision 7** in the guiding plan.

## Guiding document (single source)

[`.cursor/plans/waste_disagg_two_phases_18d3c084.plan.md`](../../../../.cursor/plans/waste_disagg_two_phases_18d3c084.plan.md)
— Phase 1 status, locked decisions, and **full Phase 2** implementation plan.

## Contents (this folder)

| File | Role |
|------|------|
| [`feasibility_multi_year_weights.md`](feasibility_multi_year_weights.md) | **Phase 1 report** (stakeholders) |
| [`implementation_plan.md`](implementation_plan.md) | Stub → Cursor plan Phase 2 |
| [`preview_sas_rcra_shares.py`](preview_sas_rcra_shares.py) | §4.1.4 SAS (and attempted RCRA) share-drift preview |
| `figures/` | Preview charts |
| `cache/` | Probe/search JSON + small intermediates |

## Run the Phase 1 preview

```bash
.venv\Scripts\python.exe -m bedrock.analysis.nowcasting.waste_disaggregation.preview_sas_rcra_shares
```

## Status

- Phase 1 report + Decisions 1–7: **locked** (Decision 7 = **2024 pilot-only**)
- Phase 2: **authorized for 2024 only** (Phases A–C); multi-year 2018–2023 blocked until after 2024 impact review
