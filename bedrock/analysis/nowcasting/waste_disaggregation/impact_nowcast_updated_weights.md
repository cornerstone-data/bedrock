# Impact report — 2024 waste-weight pilot (control vs treatment)

**Status:** GCS year-aligned vs 2017 on pin `v0.3.0_92b7a8a`.  
**Plan:** `.cursor/plans/waste_disagg_nowcasting_update.plan.md`.  
**By-year:** [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md).

## Context

Production still freezes waste weight **shares at 2017**. This run rebuilds 2024 weights with **both** arms on the same verified GCS MUT vintage.

| Arm | Config | Waste weights | MUT vintage | Electricity |
|-----|--------|---------------|-------------|-------------|
| Control | `…_nowcast_2024_waste_weights_control.yaml` | Bundled **2017** | **`v0.3.0_92b7a8a`** | off |
| Treatment | `…_nowcast_2024_waste_weights_match_io.yaml` | `match_io` → 2024 | **same** | off |

**Treatment provenance:** AIES 2024 BASIC; SAS 2022 row-sum; EC 2022; RCRA 2021 BR with `rcra_path=br_bypass`.

Do **not** treat the Sep-22 unpinned-control pilot alone as flip evidence.

## Weight shares: 2017 CSVs vs 2024 derive

<!-- AUTO:WEIGHT_DELTA_2017_VS_2024 -->

Comparison of **bundled 2017 production weight CSVs** (control) vs **2024 derived** weights (treatment `match_io`). Values are percent shares among the seven Cornerstone waste children (each vector sums to ~100%). Δ is 2024 − 2017 in percentage points.

_2024 derive provenance:_ RCRA=2021, EC=2022, SAS=2022, AIES=2024/BASIC; notes: SAS Table 2 revenue waste suppression recovery (equal_residual under NAICS 562): parent=136674000000, published_detail=132211000000, residual=4463000000, n_suppressed=2, fill_each=2231500000, recovered_naics=['562112', '562213']; rcra_path=br_bypass; RCRA intersection from BR shipper->receiver rows (year=2021); bypasses CRHW FBS - temporary diagnostics path; follow-on BR→FBS work owns FBS replacement; BR intersection stats: rows_seen=1829582, rows_used_received=1248765, rows_used_shipped=311228, rows_skipped_missing_ids=21699, rows_with_tons_or_ids=1559993, rows_skipped_non_waste_endpoint=1401094, rows_used_waste_intersection=158899.

**Industry mix — Use column sum (industry output)**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 47.60% | 45.59% | -2.01 pp |
| 562HAZ | 12.10% | 10.01% | -2.10 pp |
| 562212 | 8.27% | 7.03% | -1.24 pp |
| 562213 | 1.51% | 0.77% | -0.74 pp |
| 562910 | 16.60% | 19.86% | +3.25 pp |
| 562920 | 4.48% | 4.77% | +0.29 pp |
| 562OTH | 9.43% | 11.97% | +2.54 pp |

**Commodity mix — Use row sum (commodity output)**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 46.30% | 48.98% | +2.68 pp |
| 562HAZ | 10.30% | 8.13% | -2.17 pp |
| 562212 | 9.80% | 8.22% | -1.58 pp |
| 562213 | 1.50% | 1.63% | +0.13 pp |
| 562910 | 16.60% | 16.17% | -0.43 pp |
| 562920 | 7.53% | 5.89% | -1.64 pp |
| 562OTH | 7.97% | 10.97% | +3.00 pp |

**Make column sum**

| Child | 2017 CSV | 2024 derive | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 47.91% | 45.59% | -2.32 pp |
| 562HAZ | 12.80% | 10.01% | -2.80 pp |
| 562212 | 8.15% | 7.03% | -1.12 pp |
| 562213 | 1.35% | 0.77% | -0.58 pp |
| 562910 | 14.60% | 19.86% | +5.25 pp |
| 562920 | 5.42% | 4.77% | -0.66 pp |
| 562OTH | 9.76% | 11.97% | +2.21 pp |

**Use waste×waste intersection (shipper→receiver shares)**

Max abs cell Δ (context only; not dominating-slice input): `562HAZ`←`562HAZ` = +21.73 pp (2017=57.98%, 2024=79.72%).

Diagonal cells (receiver = shipper):

| Child | 2017 diag | 2024 diag | Δ (pp) |
|-------|----------:|----------:|-------:|
| 562111 | 0.00% | 0.00% | +0.00 pp |
| 562HAZ | 57.98% | 79.72% | +21.73 pp |
| 562212 | 0.06% | 0.13% | +0.07 pp |
| 562213 | 0.00% | 0.00% | +0.00 pp |
| 562910 | 0.13% | 0.05% | -0.08 pp |
| 562920 | 0.00% | 0.05% | +0.05 pp |
| 562OTH | 1.33% | 0.13% | -1.20 pp |

<!-- /AUTO:WEIGHT_DELTA_2017_VS_2024 -->

## Economy-wide results (GCS)

| Metric | Total EF (N) | Direct EF (D) |
|--------|--------------|---------------|
| Median % change | **−0.01%** | **0.0%** |
| 95th percentile of \|% change\| | **0.08%** | **0.0%** |
| Share \|N %\| > 1% | **1.7%** | — |
| Share \|N %\| > 5% | **1.0%** | — |

Artifacts: `cache/impact_2024_v0.3.0_92b7a8a/` (regenerated after SAS Table 2/3 suppression recovery). Figures: [#1031](https://github.com/cornerstone-data/bedrock/issues/1031) (local PNGs untracked).

## Waste-sector N / D % Δ

| Sector | N % Δ | D % Δ |
|--------|-------|-------|
| 562111 | −7.5% | −5.7% |
| 562HAZ | −0.5% | +50.6% |
| 562212 | +9.1% | +10.1% |
| 562213 | +63.9% | +65.4% |
| 562910 | −3.5% | −15.1% |
| 562920 | +10.9% | +8.9% |
| 562OTH | −3.1% | −4.8% |

## Production gate

**FLIP** — [`production_gate.md`](production_gate.md); release note [`flip_release_note.md`](flip_release_note.md). Evidence = GCS year-aligned vs 2017.
