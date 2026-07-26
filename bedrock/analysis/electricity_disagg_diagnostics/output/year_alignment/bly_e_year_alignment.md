# BLy vs E under mixed units — year alignment probe

Mixed-units model only. Compares attributed production `BLy = diag(D) @ L_dom @ y_nab` to inventory `E`, nationally and for the electricity block (221110/221121/221122).

## Setup

### Baseline (production v0.2 mixed)

- Config: `2025_usa_cornerstone_v0_2_electricity_mixed_units`
- `model_base_year=2023`, `usa_io_data_year=2022`, `usa_ghg_data_year=2023`
- A/q: default scale 2017→IO year then inflate→model year (no dedicated `scale_a_matrix_*` flag set on v0.2)
- E: hardcoded **2023 eGRID FBS** whenever electricity disaggregation is on
- `x` in B: GHG-year industry GO (`use_E_data_year_for_x_in_B=True`)

### Single-year 2017 attempt

- `model_base_year=2017`, `usa_io_data_year=2017`, `usa_ghg_data_year=2017`
- `scale_a_matrix_with_useeio_method=True` → A/q stay on 2017 detail base (no summary-ratio scale / no price inflation)
- E: **`GHG_national_Cornerstone_2017`** on GCS; this probe bypasses the production eGRID branch and splits aggregate `221100` → G/T/D via `split_electricity_e_for_disaggregated_b` (SF₆→transmission; other gases→generation)
- Mixed-units MWh: stewi eGRID has **no 2017** inventory (available: 2016, 2018–2023). Proxy: **eGRID 2018** net generation for `c_col` / `c_row`
- Table 2.4 retail prices: **2017** EIA EPA values are available

**Blockers to a true fully single-year 2017 mixed model in production code:**

1. `usa_ghg_data_year` Literal excludes 2017 (breaks `reconciling_data_years/model1.yaml`)
2. `load_E_from_flowsa` always loads `GHG_national_Cornerstone_2023_egrid` when electricity disaggregation is on (ignores GHG year)
3. No stewi eGRID 2017 inventory for physical MWh

## Results

### Baseline

| Scope | E (Mt) | BLy (Mt) | BLy−E | BLy/E | Σ D·q (Mt) | BLy/(Σ D·q) | Σ D_USD·x (Mt) | ‖q−L_dom@y‖/‖q‖ | median x/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| national | 4871.67 | 4939.87 | +68.20 | 1.0140 | 4939.87 | 1.0000000000 | 4887.30 | 2.185e-16 | 0.9845 |
| electricity_block | 1471.03 | 1649.54 | +178.50 | 1.1213 | 1649.54 | 1.0000000000 | 1471.03 | 1.873e-16 | 1.1194 |

### 2017 single-year attempt

| Scope | E (Mt) | BLy (Mt) | BLy−E | BLy/E | Σ D·q (Mt) | BLy/(Σ D·q) | Σ D_USD·x (Mt) | ‖q−L_dom@y‖/‖q‖ | median x/q |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| national | 5121.81 | 5103.77 | -18.04 | 0.9965 | 5103.77 | 1.0000000000 | 5144.14 | 3.521e-16 | 1.0000 |
| electricity_block | 1761.53 | 1738.11 | -23.42 | 0.9867 | 1738.11 | 1.0000000000 | 1761.53 | 4.886e-16 | 1.0135 |

## Correct identity: `BLy ≈ Σ_j D_j q_j` (not `BLy = E`)

`BLy = diag(D) @ L_dom @ y_nab`. With balanced domestic IO the operative identities are:

1. `L_dom @ y_nab ≈ q`
2. therefore `BLy ≈ Σ_j D_j q_j`

`BLy = E` is **not** an accounting identity of this pipeline — even without A/q scale/inflate (2017 attempt).

### Identity checks

| Scenario | Scope | ‖q−L_dom@y‖/‖q‖ | BLy (Mt) | Σ D·q (Mt) | BLy−Σ D·q | BLy/(Σ D·q) | E (Mt) | BLy/E |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| Baseline | national | 2.185e-16 | 4939.87 | 4939.87 | -9.095e-12 | 1.0000000000 | 4871.67 | 1.0140 |
| Baseline | electricity_block | 1.873e-16 | 1649.54 | 1649.54 | -7.276e-12 | 1.0000000000 | 1471.03 | 1.1213 |
| 2017 attempt | national | 3.521e-16 | 5103.77 | 5103.77 | +5.366e-11 | 1.0000000000 | 5121.81 | 0.9965 |
| 2017 attempt | electricity_block | 4.886e-16 | 1738.11 | 1738.11 | +5.139e-11 | 1.0000000000 | 1761.53 | 0.9867 |

Both checks hold to numerical precision in baseline and the 2017 attempt (`‖q−L_dom@y‖/‖q‖` ~ 1e-16; `BLy/(Σ D·q)` = 1). `BLy/E` stays away from 1, especially in the electricity block under production years.

### Why `BLy ≠ E`

1. **`D` uses industry `x`, not commodity `q`.** `B = (E / x) @ Vnorm`, `D = sum_g B`. Then `D·q` equals `E` only if `q = x` (and Vnorm maps cleanly). Empirically `median(x/q)` is not 1; compare `Σ D·q` vs `E` vs `Σ D_USD·x` in the Results tables.
2. **Electricity undilution / split.** Child-sector `D_110` is large; `BLy_110 ≈ D_110 · q_110` can diverge from the electricity inventory slice when `q_110` and `x_110` diverge — same mechanism as in `electricity_full_trace.md`.
3. **2017 proxy gaps.** The single-year attempt still uses eGRID 2018 MWh for mixed units and a gas-row split of 2017 aggregate electricity E (not facility eGRID), so it is not a pure same-source year.

Remaining in 2017 for A/q removes price scale/inflate drift and tightens `BLy/E` nationally, but **does not make `BLy = E`**.

## How year changes are handled (relevant components)

| Component | Current mixed unit model implementation | Relevant disaggregation step(s) | Notes |
|---|---|---|---|
| **Detail IO (V, U, A base, q base)** | BEA 2017 detail (`usa_detail_original_year=2017`) | Reallocation, 3-way split | Disagg Make/Use in Cornerstone space when waste/elec disagg on |
| **A, q (scaled)** | Scale detail→`usa_io_data_year` (2022) with summary ratios; inflate→`model_base_year` (2023) via industry PI (default branch); then mixed-units rewrite of gen row/column and `q_110` | Reallocation, 3-way split, mixed units | `scale_a_matrix_with_useeio_method` skips scale/inflate (2017 probe) |
| **y_nab** | Backcomputed from scaled/mixed `Adom` and `q` → same dollar/unit year as A/q | Reallocation, 3-way split, mixed units | Mixed: gen row/column physical |
| **L / L_dom** | `(I−A)^−1` from scaled/mixed A | Reallocation, 3-way split, mixed units | Year enters only through A |
| **E** | `usa_ghg_data_year` FBS **unless** electricity disaggregation → **forced 2023 eGRID FBS** | 3-way split | 2017 FBS exists but is unused in production disagg path |
| **x (B denominator)** | BEA GO at `usa_ghg_data_year` when `use_E_data_year_for_x_in_B` | 3-way split | Not the same series as scaled commodity `q` |
| **Vnorm** | From uninflated V and `q =` column sums of that V | 3-way split | Maps industry E/x → commodity B. Current elec-disagg flags: `apply_inflation_to_V=False`, `use_scaled_x_and_scaled_Vnorm_for_B=False` |
| **B** | `(E/x) @ Vnorm`, then mixed-units `/ c_col` on gen column | 3-way split, mixed units | Intensity year = E and x year. Current elec-disagg flags: `use_E_data_year_for_x_in_B=True`, `deflate_x_to_detail_io_year_for_B=False`, `use_scaled_x_and_scaled_Vnorm_for_B=False` (no post-hoc B scale/inflate) |
| **D** | Column sums of B (gen: kg/MWh after mixed) | 3-way split, mixed units | Follows B |
| **N** | `D`-weighted Leontief (`B @ L` characterized) | Reallocation, 3-way split, mixed units | Mixes B year with A/L year |
| **BLy** | `diag(D) @ L_dom @ y_nab` | Reallocation, 3-way split, mixed units | Couples B/D year to A/q year |
| **Mixed `c_col`/`c_row`** | MWh from eGRID@`model_base_year`; prices Table 2.4@`usa_ghg_data_year` | mixed units | 2017 MWh missing in stewi |

### Why D7 GO correction does not force electricity `q ≈ x`

A common expectation is that because electricity child A/q rows are corrected with BEA detail GO (UGO305), scaled `q` should match the BEA GO `x` used in `B = (E / x) @ Vnorm`. That is not what D7 does.

**D7 does not set electricity `q` to 2023 BEA GO.** It only adjusts the 2017→2022 **scale** step so each child's *growth ratio* matches UGO305 GO growth instead of the flat Utilities `"22"` summary ratio. After that, `q` is still **inflated 2022→2023 with industry PI** (shared `221100` factor), which is not the same as pinning levels to 2023 GO.

Three mismatches remain:

1. **Ratio correction ≠ level matching.** D7 multiplies already-scaled 2017 detail `q` by `(GO_i[2022]/GO_i[2017]) / (q_22[2022]/q_22[2017])`. Absolute `q_i` still comes from the disaggregated Make/Use structure, not `q_i := GO_i`.
2. **Wrong year endpoint for GO.** Scale/D7 target is `usa_io_data_year=2022`. The B denominator `x` is **2023** GHG-year industry GO (`usa_ghg_data_year`). The last hop to 2023 on `q` is PI inflate, not another UGO305 GO update.
3. **Commodity `q` vs industry `x`.** Even same-year, `x` is industry GO (aggregate `221100` expanded/split with V shares), while `q` is commodity output after scale/inflate. For generation, `BLy` uses `D·q` and inventory recovery is `D·x` (`Σ D_USD·x = E` in the elec block) — so the diagnostic gap is still **`q ≠ x`**, not a failure of `L_dom @ y ≈ q`.

In short: GO correction makes G/T/D **scale differently from each other** through 2022; it does **not** force model-year commodity `q` onto the same BEA GO vector used as `x` in `B`. That residual `q`–`x` wedge is what opens electricity-block `BLy/E` under production scale/inflate.

### eGRID MWh and EIA prices vs inflated A/q year

Numerically yes for the current config — both are 2023 — but they are not the same config field.

| Input | Year source | Current value |
|---|---|---:|
| Inflated A/q | `model_base_year` | 2023 |
| eGRID MWh (`c_col`) | `model_base_year` | 2023 |
| EIA Table 2.4 prices (`c_row`) | `usa_ghg_data_year` | 2023 |

eGRID matches the inflated A/q year by construction. Prices match only because the baseline also sets `usa_ghg_data_year=2023`; if GHG year diverged from `model_base_year`, prices would follow GHG, not A/q.

## What it would take to build D & N for 2017–2023 (outline only)

Do **not** implement here; requirements:

1. **Config / API**
   - Extend `usa_ghg_data_year` to include 2017–2018 (FBS already on GCS).
   - Parameterize electricity E source: eGRID FBS by year when available; else year GHG FBS + documented G/T/D split.
   - Parameterize eGRID MWh year (or accept nearest-year proxy with flags).

2. **Per-year inputs**
   - GHG FBS `GHG_national_Cornerstone_{y}` for each y.
   - Industry `x(y)` from BEA GO (already time-series capable).
   - A/q(y): either freeze 2017 structure (`useeio` method), or run scale/inflate / nowcast / summary-ratio path to each `model_base_year=y`.
   - Table 2.4 prices(y); eGRID MWh(y) where stewi supports it (gap at 2017).

3. **Per-year compute**
   - For each y: build mixed-units A/q (if desired), B(y)=E(y)/x(y)@Vnorm, D(y), L(y), N(y).
   - Decide whether V/U/disagg weights are frozen at 2017 or year-specific (GO shares / Table 8.3).

4. **Comparability rules**
   - Report D/N in USD-equiv (apply `c_col` back-conversion for gen) for cross-year charts.
   - Document dollar year of denominators (GHG-year x vs model-year q).
   - Separate structural change (A) from inventory change (E) and price change (inflation).

5. **Validation**
   - Track `BLy` vs `E` and `Σ D·q` each year (expect persistent gap).
   - Smoke-test 2017/2018/2022/2023 against known v0.2 mixed anchors.

## Reproduce

```
python -m bedrock.analysis.electricity_disagg_diagnostics.year_alignment.year_alignment_bly_e
```

Writes `C:/Users/jvend/Documents/CodeProjects/bedrock/bedrock/analysis/electricity_disagg_diagnostics/output/year_alignment/bly_e_year_alignment.md` and `C:/Users/jvend/Documents/CodeProjects/bedrock/bedrock/analysis/electricity_disagg_diagnostics/output/year_alignment/bly_e_year_alignment.json`.

Re-render markdown only (from existing JSON):

```
python -m bedrock.analysis.electricity_disagg_diagnostics.year_alignment.year_alignment_bly_e --report-only
```
