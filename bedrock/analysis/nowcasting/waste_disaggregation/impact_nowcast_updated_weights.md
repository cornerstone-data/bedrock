# Impact report — 2024 waste-weight pilot (control vs treatment)

**Status:** Phase 3.1 Track A complete (GCS pin `v0.3.0_f709829`, both arms).  
**Plan:** `.cursor/plans/waste_disagg_nowcasting_update.plan.md` (Phase 3 dual-MUT).  
**By-year:** [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md).

## Context

Production still freezes waste weight **shares at 2017**. This run rebuilds 2024 weights with **both** arms on the same verified GCS MUT vintage (Track A gate).

| Arm | Config | Waste weights | MUT vintage | Electricity |
|-----|--------|---------------|-------------|-------------|
| Control | `…_nowcast_2024_waste_weights_control.yaml` | Bundled **2017** | **`v0.3.0_f709829`** | off |
| Treatment | `…_nowcast_2024_waste_weights_match_io.yaml` | `match_io` → 2024 | **same** | off |

**Treatment provenance:** AIES 2024 BASIC; SAS 2022 row-sum; EC 2022; RCRA 2021 BR with `rcra_path=br_bypass`.

Do **not** treat the Sep-22 unpinned-control pilot alone as flip evidence.

## Economy-wide results (Track A)

| Metric | Total EF (N) | Direct EF (D) |
|--------|--------------|---------------|
| Median % change | **−0.23%** | **0.0%** |
| 95th percentile of \|% change\| | **1.13%** | **0.0%** |
| Share \|N %\| > 1% | **8.1%** | — |
| Share \|N %\| > 5% | **1.5%** | — |

Artifacts: `cache/impact_2024_v0.3.0_f709829/`; figures `figures/impact_2024_v0.3.0_f709829_*.png`.

## Waste-sector N / D % Δ

| Sector | N % Δ | D % Δ |
|--------|-------|-------|
| 562111 | −7.7% | −5.7% |
| 562HAZ | +7.8% | +50.6% |
| 562212 | +8.9% | +10.0% |
| 562213 | +63.8% | +65.4% |
| 562910 | −3.1% | −15.1% |
| 562920 | +10.9% | +8.9% |
| 562OTH | −2.5% | −4.8% |

## Production gate

Default **HOLD** — [`phase3_production_gate.md`](phase3_production_gate.md). Prefer Track C if local MUT deltas differ from GCS.
