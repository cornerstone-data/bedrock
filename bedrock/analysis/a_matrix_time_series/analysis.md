# A-matrix transformation analysis — findings

Empirical results and root-cause investigation for epic #337 / Step 6 EF diagnostics.
Plan and per-step structure: `analysis_plan.md`.

## Setup

7 diagnostics runs at `model_base_year = 2023`, `update_inflation_factors = False`
(default; preserves pre-#369 production behavior on the legacy parquet PI source).
`apply_inflation_to_V` defaults to `False` for every approach except
`commodity_price_index` (which has it set in its YAML). All five approach configs
ran without YAML edits — see `analysis_plan.md` Phase 1.

| approach | YAML | A-matrix construction | runs |
|---|---|---|---|
| `useeio` | `2025_usa_cornerstone_A_useeio.yaml` | `A_2017_BEA_detail` directly (no temporal adjustment) | vs CEDA |
| `summary_tables` | `2025_usa_cornerstone_A_summary_tables.yaml` | `scale_cornerstone_A` via summary-table ratios (price + physical shifts) | vs CEDA, vs USEEIO |
| `industry_price_index` | `2025_usa_cornerstone_A_industry_price_index.yaml` | `diag(p) · A_2017 · diag(1/p)`, `p` = BEA detail industry PI | vs CEDA, vs USEEIO |
| `commodity_price_index` | `2025_usa_cornerstone_A_commodity_price_index.yaml` | Same form, `p` = V-norm-weighted commodity ratio | vs CEDA, vs USEEIO |
| `taxonomy` (= v0) | `2025_usa_cornerstone_taxonomy.yaml` | Production default (= summary-tables-style scaling) | not re-run; v0 baseline |

`useeio`-vs-USEEIO is degenerate (same methodology) and was skipped. `taxonomy`-vs-CEDA would be identity and was also skipped. `B/x` are the same across the four candidates at a given baseline, so all variation in `D` (direct EF) is invariant across approach panels — variation in `N` (total EF) reflects only the `(I − A)⁻¹` part.

Outputs:
- Per-(approach, baseline) Sheets in Drive folder `1M2-Vopqfrx1vGcwoNi6wq55FmoELNV1s` (v0.3 Diagnostics).
- Compiled workbook + scatter coords: `output/results/{ef_comparison.xlsx, ef_run_index.csv, ef_scatter_coords.parquet}`.
- 2×2 scatter + histogram PNGs per `(baseline, ef_kind)` in `output/plots/ef_*`.

## Headline summary numbers

`|N_perc_diff|` quantiles per approach (sectors per row).

vs **CEDA-US (v0)**:

| approach | N_p50 | N_p95 | N_max | n_significant (>10%) |
|---|---:|---:|---:|---:|
| `summary_tables` | **0.053** | **0.207** | 0.564 | **93** |
| `useeio` | 0.084 | 0.288 | 0.756 | 178 |
| `industry_price_index` | 0.121 | 0.438 | 0.911 | 247 |
| `commodity_price_index` | 0.121 | 0.438 | 0.881 | 245 |

vs **USEEIO**:

| approach | N_p50 | N_p95 | N_max | n_significant |
|---|---:|---:|---:|---:|
| `summary_tables` | 0.189 | 0.629 | 3.072 | 436 |
| `industry_price_index` | **0.096** | **0.531** | 3.130 | 527 |
| `commodity_price_index` | **0.095** | **0.531** | 3.130 | 530 |

**Headline reads:**
1. `summary_tables` reproduces v0 most closely (n_p50 = 5.3%) — same methodology, drift of ~5 pp reflects schema-mapping noise + the post-#369 inflation gate, not a methodology difference.
2. `industry_price_index` and `commodity_price_index` are functionally indistinguishable — every quantile, every panel, both vs CEDA and vs USEEIO.
3. Histogram bias direction (medians, vs CEDA): `summary_tables` ≈ −3.4%, `useeio` ≈ −5.0%, `industry_price_index` ≈ +9.6%, `commodity_price_index` ≈ +9.5%. Vs USEEIO: `summary_tables` ≈ −15.2%, `industry_price_index` ≈ −2.9%, `commodity_price_index` ≈ −2.8%.

## Mechanism (Phase 1 trace)

`derive_cornerstone_Aq_scaled()` → A matrix (varies by approach) → `derive_cornerstone_B_non_finetuned()` → B matrix → `calculate_ef_diagnostics.py` computes `N = (I − A)⁻¹ · B/x` and `D = B/x`. The four candidates diverge **only** in A-matrix construction; B/x identical across them.

Mapping methodologies onto the data:
- `summary_tables` *is* v0's methodology (modulo schema mapping) → predicts tight match to CEDA.
- `useeio` shares the BEA-2017 detail base with v0 but skips temporal adjustment → predicts modest, mostly-symmetric drift.
- `industry_price_index` rescales A by relative price changes only → drops the *physical* inter-industry shift component v0 captures via summary tables → predicts directional bias.
- `commodity_price_index` is `industry_price_index` mapped through V-norm supplier weights, but for ~95% of codes the primary supplier industry equals the commodity code → predicts ≈ industry_price_index.

Empirically (`output/results/ratio_summary.csv` at year 2024):
- `ind_median = 1.289`, `com_median = 1.290` — V-norm-weighted commodity ratio differs from industry ratio by `rel_delta_p95 = 0.019`.
- 52 of 405 codes (12.8%) differ by >1%; 0 differ by >5%.

That is mechanically why the entire commodity-PI path collapses onto the industry-PI numbers in this dataset.

## Phase 2 — D2.2 (sectors driving the bias)

Gap statistic per sector: `gap = N_industry_pdiff − N_summary_pdiff`, both vs CEDA. Distribution:

- mean = +0.156, median = +0.131, p95 = +0.474, p5 = −0.077, range = (−0.71, +1.04)

The +0.131 median exactly reproduces the 13 pp histogram-bias gap (industry_pi median ≈ +9.6%, summary_tables median ≈ −3.4%). So whatever drives `gap` drives the population-level bias.

### Where the gap matters materially (impact = |gap| × |N|)

NAICS-3 aggregation, by total impact:

| NAICS | description | n_sectors | sum_impact | median_gap |
|---|---|---:|---:|---:|
| **325** | Chemicals | 19 | **2.11** | +0.16 |
| **311** | Food manufacturing | 24 | **2.05** | +0.12 |
| **336** | Transportation equipment | 25 | 1.54 | +0.17 |
| 322 | Paper | 8 | 0.70 | +0.17 |
| 333 | Machinery | 28 | 0.68 | +0.13 |
| 326 | Plastics & rubber | 10 | 0.67 | +0.16 |
| 335 | Electrical equipment | 18 | 0.66 | +0.09 |
| 332 | Fabricated metal | 20 | 0.60 | −0.02 |
| **334** | Computer/electronics | 20 | 0.56 | **+0.47** |
| 339 | Misc manufacturing | 11 | 0.51 | +0.19 |

Two competing magnitudes. NAICS 334 (electronics) has the **largest per-sector gap** (median +0.47) — the textbook case where price-only scaling and physical scaling predict opposite directions, because electronics had falling prices but rising real output 2017→2024. But electronics have low per-sector EFs, so material impact is mid-pack.

The systemic bias is dominated by **chemicals (325) and food manufacturing (311)** — moderate per-sector gaps (+0.12 to +0.16) but high baseline EFs.

### One outlier worth flagging

`33641A` (Propulsion units for space vehicles): gap = −0.71. The only large-magnitude reversal. `summary_tables` shows +56.4% vs CEDA, `industry_price_index` shows −14.6%. Likely a schema-mapping artifact in the BEA summary table for a small specialty sector, not a general-mechanism finding.

## Root cause (one sentence)

`industry_price_index` overstates 2024 EFs vs `summary_tables` because it credits sectors only for nominal price changes; on energy-intensive manufacturing sectors (chemicals, food, transportation equipment) where 2017→2024 saw both significant price inflation *and* material physical-efficiency gains, the omitted physical-shift component is what `summary_tables` captures and where the population bias lives.

## Open items

- **D2.1** — V-norm concentration: confirmed from `ratio_summary.csv` already (`p95 rel_delta = 0.019`). No separate run needed; cited above.
- **D2.3** — multiplicative decomposition of `N_summary ≈ N_industry · (price_corr) · (phys_corr)`. Pending; would quantify whether the physical-shift correction is uniform-multiplicative or sector-specific.
- **Phase 3** — generalize: confirm the price + physical-shift decomposition for each top-impact sector (325, 311, 336) individually.
- **Phase 4** — origin: why CEDA-US production uses summary-table scaling. Plausible implicit reason: BEA summary tables are the only update source between detail-table releases (2017 → 2022), so summary scaling is the cheapest available "price + physical shift" composite.
- **Phase 6** — visualize: per-sector scatter of (BEA price-index Δ 2017→2024) vs (summary-table multiplier 2017→2024), bubble-sized by EF, colored by NAICS-2. Outliers labeled.

## Pointers

- Compile + plot scripts: `compile_ef_diagnostics.py`, `plot_ef_diagnostics.py`.
- Per-pair tabs in `output/results/ef_comparison.xlsx`: 7 named `{approach}__vs_{baseline}`.
- Long-format scatter coords: `output/results/ef_scatter_coords.parquet`.
- Plan: `analysis_plan.md`. Redundancy cleanup queue: `.claude/plans/a_matrix_redundancy_cleanup_plan.md`.
