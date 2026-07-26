# Electricity full trace across v0.2 chain

Comparison of IO anchors, emissions inventory **E**, direct EF **D**, total EF **N**, and **BLy** for the electricity block.

Configs: **v0.2** footing → **reallocation** (PR2) → **3-way split** (PR3) → **unit conversion** (PR4 mixed units).

Rows labeled **221100\*** after PR3 are re-aggregated values for **221110 + 221121 + 221122**. They retain the report's existing aggregate calculation (sums for additive metrics; output-weighted values for EFs). The individual child-sector rows are also shown.

Mixed units only change the pipeline starting at **A/q** (and matrices derived from them: **L**, then **B**/**D**/**N** for generation). Make, Use, VA, Y, Vnorm, and industry output **x** remain monetary even at the unit-conversion step. Physical generation q and `c_col` are shown in the mixed-units detail table below.

| Metric | Electricity sector | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| x industry output (USD) | 221100 | $461.90 B | $455.76 B | N/A | N/A | Unit conversion: monetary (Make-based industry x; not converted via c_col). PR2 clears 221100 co-production off-diagonals onto the diagonal; industry gross output is reshaped but national totals are preserved. |
|  | 221100* | N/A | N/A | $455.76 B | $455.76 B |  |
|  | 221110 | N/A | N/A | $155.74 B | $155.74 B |  |
|  | 221121 | N/A | N/A | $17.77 B | $17.77 B |  |
|  | 221122 | N/A | N/A | $282.25 B | $282.25 B |  |
| q commodity output (USD-equiv) | 221100 | $595.09 B | $595.09 B | N/A | N/A | Unit conversion: monetary scaled_q (pre-conversion A/q); not q_MWh/c_col. Physical generation q is in the mixed-units detail table. |
|  | 221100* | N/A | N/A | $593.81 B | $593.81 B |  |
|  | 221110 | N/A | N/A | $230.42 B | $230.42 B |  |
|  | 221121 | N/A | N/A | $20.93 B | $20.93 B |  |
|  | 221122 | N/A | N/A | $342.46 B | $342.46 B |  |
| V Make diagonal sum (USD) | 221100 | $454.19 B | $455.76 B | N/A | N/A | Unit conversion: monetary Make table (unchanged by mixed units). |
|  | 221100* | N/A | N/A | $455.76 B | $455.76 B |  |
|  | 221110 | N/A | N/A | $155.74 B | $155.74 B |  |
|  | 221121 | N/A | N/A | $17.77 B | $17.77 B |  |
|  | 221122 | N/A | N/A | $282.25 B | $282.25 B |  |
| Udom column sum (USD) | 221100 | $163.73 B | $161.54 B | N/A | N/A | Unit conversion: monetary Use table (unchanged by mixed units). Largest change at reallocation (1.3% vs v0.2). |
|  | 221100* | N/A | N/A | $161.54 B | $161.54 B |  |
|  | 221110 | N/A | N/A | $84.35 B | $84.35 B |  |
|  | 221121 | N/A | N/A | $6.29 B | $6.29 B |  |
|  | 221122 | N/A | N/A | $70.90 B | $70.90 B |  |
| Uimp column sum (USD) | 221100 | $7.56 B | $7.46 B | N/A | N/A | Unit conversion: monetary Use table (unchanged by mixed units). Largest change at reallocation (1.3% vs v0.2). |
|  | 221100* | N/A | N/A | $7.46 B | $7.46 B |  |
|  | 221110 | N/A | N/A | $5.90 B | $5.90 B |  |
|  | 221121 | N/A | N/A | $0.10 B | $0.10 B |  |
|  | 221122 | N/A | N/A | $1.46 B | $1.46 B |  |
| Udom row sum (USD) | 221100 | $276.29 B | $276.29 B | N/A | N/A | Unit conversion: monetary Use table (unchanged by mixed units). |
|  | 221100* | N/A | N/A | $276.29 B | $276.29 B |  |
|  | 221110 | N/A | N/A | $96.29 B | $96.29 B |  |
|  | 221121 | N/A | N/A | $11.35 B | $11.35 B |  |
|  | 221122 | N/A | N/A | $168.65 B | $168.65 B |  |
| VA column sum (USD) | 221100 | $288.81 B | $285.00 B | N/A | N/A | Unit conversion: monetary VA table (unchanged by mixed units). Largest change at reallocation (1.3% vs v0.2). |
|  | 221100* | N/A | N/A | $286.76 B | $286.76 B |  |
|  | 221110 | N/A | N/A | $65.49 B | $65.49 B |  |
|  | 221121 | N/A | N/A | $11.38 B | $11.38 B |  |
|  | 221122 | N/A | N/A | $209.89 B | $209.89 B |  |
| Y row sum (USD) | 221100 | $177.97 B | $177.97 B | N/A | N/A | Unit conversion: monetary final-demand table (unchanged by mixed units). |
|  | 221100* | N/A | N/A | $177.97 B | $177.97 B |  |
|  | 221110 | N/A | N/A | $58.93 B | $58.93 B |  |
|  | 221121 | N/A | N/A | $6.36 B | $6.36 B |  |
|  | 221122 | N/A | N/A | $112.68 B | $112.68 B |  |
| Vnorm diagonal mean | 221100 | 0.996549 | 1.000000 | N/A | N/A | Unit conversion: monetary Make-normalized Vnorm (unchanged by mixed units). |
|  | 221100* | N/A | N/A | 1.000000 | 1.000000 |  |
|  | 221110 | N/A | N/A | 1.000000 | 1.000000 |  |
|  | 221121 | N/A | N/A | 1.000000 | 1.000000 |  |
|  | 221122 | N/A | N/A | 1.000000 | 1.000000 |  |
| A diagonal mean (scaled) | 221100 | 0.070663 | 0.070666 | N/A | N/A | Unit conversion: physical mixed-units A (generation column in MWh basis). Largest change at unit conversion (149.6% vs v0.2). |
|  | 221100* | N/A | N/A | 0.153758 | 0.176389 |  |
|  | 221110 | N/A | N/A | 0.178976 | 0.246869 |  |
|  | 221121 | N/A | N/A | 0.275100 | 0.275100 |  |
|  | 221122 | N/A | N/A | 0.007199 | 0.007199 |  |
| L diagonal mean | 221100 | 1.085308 | 1.085313 | N/A | N/A | Unit conversion: L rebuilt from mixed-units A (physical generation path). Largest change at unit conversion (14.6% vs v0.2). |
|  | 221100* | N/A | N/A | 1.205387 | 1.243411 |  |
|  | 221110 | N/A | N/A | 1.226796 | 1.340866 |  |
|  | 221121 | N/A | N/A | 1.379745 | 1.379745 |  |
|  | 221122 | N/A | N/A | 1.009620 | 1.009622 |  |
| x for B denominator (USD) | 221100 | $600.89 B | $600.89 B | N/A | N/A | Unit conversion: monetary industry output used as B's E/x denominator. |
|  | 221100* | N/A | N/A | $600.89 B | $600.89 B |  |
|  | 221110 | N/A | N/A | $205.33 B | $205.33 B |  |
|  | 221121 | N/A | N/A | $23.43 B | $23.43 B |  |
|  | 221122 | N/A | N/A | $372.12 B | $372.12 B |  |

### Mixed-units detail (unit conversion step)

| Metric | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- |
| q generation MWh (221110) | — | — | — | 4,190,970,937 MWh |  |
| c_col (MWh per USD gen) | — | — | — | 0.018189 MWh/USD |  |

## E inventory (absolute, kg CO₂e)

*Units: kg CO₂e*

| GHG | Electricity sector | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Total GHG | 221100 | 1,438.07 MtCO₂e | 1,438.07 MtCO₂e | N/A | N/A | E reloads from eGRID FBS at child sectors; CO₂ rises ~3% vs aggregate FBS. |
|  | 221100* | N/A | N/A | 1,471.03 MtCO₂e | 1,471.03 MtCO₂e |  |
|  | 221110 | N/A | N/A | 1,465.76 MtCO₂e | 1,465.76 MtCO₂e |  |
|  | 221121 | N/A | N/A | 5.27 MtCO₂e | 5.27 MtCO₂e |  |
|  | 221122 | N/A | N/A | 0.00 MtCO₂e | 0.00 MtCO₂e |  |

## E inventory (shares of total)

| GHG | Electricity sector | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Total GHG | 221100 | 100.00% | 100.00% | N/A | N/A | Total GHG is 100% by definition. |
|  | 221100* | N/A | N/A | 100.00% | 100.00% |  |
|  | 221110 | N/A | N/A | 100.00% | 100.00% |  |
|  | 221121 | N/A | N/A | 100.00% | 100.00% |  |
|  | 221122 | N/A | N/A | 100.00% | 100.00% |  |

## D — direct EF (kg CO₂e / USD-equiv)

Block `221100*` is an **x-weighted** average of child-sector D (`sum(D_s · x_s) / sum(x_s)`, with `x` = industry GO used in `E/x`). See [x-weighted D/N aggregation](#x-weighted-summary-dn) in the walkthrough.

*Units: kg/USD*

| GHG | Electricity sector | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Total GHG | 221100 | 2.385903 | 2.393242 | N/A | N/A | Largest change at 3-way split (2.6% vs v0.2); reflects IO and/or unit-basis shift. |
|  | 221100* | N/A | N/A | 2.448100 | 2.448100 |  |
|  | 221110 | N/A | N/A | 7.138494 | 7.138494 |  |
|  | 221121 | N/A | N/A | 0.225045 | 0.225045 |  |
|  | 221122 | N/A | N/A | 0.000000 | 0.000000 |  |

## D — shares of total direct EF

| GHG | Electricity sector | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Total GHG | 221100 | 100.00% | 100.00% | N/A | N/A | Total GHG is 100% by definition. |
|  | 221100* | N/A | N/A | 100.00% | 100.00% |  |
|  | 221110 | N/A | N/A | 100.00% | 100.00% |  |
|  | 221121 | N/A | N/A | 100.00% | 100.00% |  |
|  | 221122 | N/A | N/A | 100.00% | 100.00% |  |

## N — total EF (kg CO₂e / USD-equiv)

Block `221100*` is an **x-weighted** average of child-sector N (`sum(N_s · x_s) / sum(x_s)`). See [x-weighted D/N aggregation](#x-weighted-summary-dn) in the walkthrough.

*Units: kg/USD*

| GHG | Electricity sector | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Total GHG | 221100 | 2.776318 | 2.783909 | N/A | N/A | Block N is x-weighted in USD-equiv; mixed units change A/L for generation but national BLy is unchanged. |
|  | 221100* | N/A | N/A | 3.216261 | 3.510145 |  |
|  | 221110 | N/A | N/A | 9.213431 | 10.070114 |  |
|  | 221121 | N/A | N/A | 0.424045 | 0.426374 |  |
|  | 221122 | N/A | N/A | 0.082942 | 0.084643 |  |

## N — shares of total EF

| GHG | Electricity sector | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Total GHG | 221100 | 100.00% | 100.00% | N/A | N/A | Total GHG is 100% by definition. |
|  | 221100* | N/A | N/A | 100.00% | 100.00% |  |
|  | 221110 | N/A | N/A | 100.00% | 100.00% |  |
|  | 221121 | N/A | N/A | 100.00% | 100.00% |  |
|  | 221122 | N/A | N/A | 100.00% | 100.00% |  |

## BLy attribution

| Metric | v0.2 | reallocation | 3-way split | unit conversion | Notes |
| --- | --- | --- | --- | --- | --- |
| BLy (MtCO2e) | 1,419.84 | 1,424.21 | 1,649.54 | 1,649.54 | Child-sector BLy sum exceeds parent due to IO restructuring; E also shifts to eGRID FBS. |
| y_nab (USD) | $211.86 B | $211.86 B | $204.65 B | $144.12 B | Largest change at 3-way split (3.4% vs v0.2). Unit conversion: block total mixes MWh (221110) and USD (T/D); not comparable to prior USD totals — see walkthrough. |

---

## Walkthrough: reallocation to 3-way split (D, N, BLy)

This section explains why **D** rises from **2.393** to **2.448 kg/USD** and **N** from **2.784** to **3.216 kg/USD**, and why **BLy** rises from **1,424** to **1,650 MtCO₂e**.

### Formulas (production diagnostics path)

| Step | Formula |
|------|---------|
| B_ind | `E / x` (industry gross output at GHG year) |
| B | `B_ind @ Vnorm` |
| D | `sum_g B[g, sector]` (kg CO₂e / USD commodity) |
| A | `Adom + Aimp` (year-scaled) |
| L | `(I - A)^-1` (total); `L_dom = (I - Adom)^-1` for BLy |
| M | `B @ L` |
| N | `sum_g M[g, sector]` |
| y_nab | `backcompute_y_from_A_and_q(Adom, q)` |
| **BLy** | **`diag(D) @ L_dom @ y_nab`** (per sector: `BLy_j = D_j * (L_dom @ y_nab)_j`) |

Block **D** and **N** in the summary tables are **x-weighted** across electricity sectors: `sum(D_s * x_s) / sum(x_s)` and the same for N ([details](#x-weighted-summary-dn)).

### x and q (scaled USD) in the walkthrough tables

These tables report two different output concepts. They are not required to match.

- **`q (scaled USD)`** is **commodity** output from `derive_cornerstone_Aq_scaled()` (`scaled_q`). On the v0.2 footing used here (`model_base_year=2023`, `usa_io_data_year=2022`, detail IO year **2017**), detail Make-based `q` is year-scaled **2017 → 2022** with summary IO ratios, then inflated **2022 → 2023** with industry price indexes. "Scaled" means that A/q year-scaling/inflation pipeline — **not** the later PR4 mixed-units (MWh) conversion.
- **`x (B denominator)`** is **industry** gross output used in `E / x` for B. With `use_E_data_year_for_x_in_B`, it comes from `derive_cornerstone_x_after_redefinition()` at **`usa_ghg_data_year` = 2023** (BEA gross-output time series expanded into Cornerstone). It is **year-selected industry GO**, not the same summary-ratio `scale_cornerstone_q` path as `q`.

Both values are therefore on a **2023** footing, but from different sources: commodity Make → scale/inflate vs industry BEA GO@GHG year. A small gap (here `q` slightly below `x`) is expected. The walkthrough identity that is forced is `(L_dom @ y_nab) ≈ q`, not `x ≈ q`. Make-derived industry `compute_x(V)` in the IO summary table above is a third series and is not the B denominator.

### Side-by-side: reallocation vs 3-way split

PR3 reloads **E** from `GHG_national_Cornerstone_2023_egrid` and splits IO. Almost all inventory lands on **221110** with a much smaller **x**.

Column groups:

- **Reallocation:** `221100` is the pre-split aggregate; `221100*` re-aggregates the three child sectors for comparison with that aggregate.
- **3-way split:** individual `221110` / `221121` / `221122` values.

`221100*` aggregation markers:

- `+` — sum of child sectors
- `E/x` — block direct intensity `sum(E_s) / sum(x_s)` (**D** in this table)
- `x̄` — x-weighted average across child sectors: `sum(v_s * x_s) / sum(x_s)` (**N** in this table)
- `q̄` — q-weighted average across child sectors: `sum(v_s * q_s) / sum(q_s)` (**L** diagonals in this table; q-weighted D/N are shown for comparison in the worked calculations)

Per-sector **D** values are `E/x` with `Vnorm=1`. In this walkthrough table, `221100*` **D** uses block `E/x` and `221100*` **N** uses the x-weighted child average — the same **x-weighting** as the summary D/N tables above ([anchor](#x-weighted-summary-dn)). Both q- and x-weighted D/N are shown in the worked calculations below.

| Quantity | Unit | Reallocation | | 3-way split | | |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
|  |  | 221100 | 221100* | 221110 | 221121 | 221122 |
| E | MtCO₂e | 1438.07 | 1471.03 (+) | 1465.76 | 5.27 | 0.00 |
| x (B denominator) | $billions | $600.89 | $600.89 (+) | $205.33 | $23.43 | $372.12 |
| q (scaled USD) | $billions | $595.09 | $593.81 (+) | $230.42 | $20.93 | $342.46 |
| D (kg/USD) | kg/USD | 2.3932 | 2.4481 (E/x) | 7.1385 | 0.2250 | 0.0000 |
| L_dom diagonal | USD/USD | 1.0833 | 1.1054 (q̄) | 1.2235 | 1.3770 | 1.0094 |
| L_total diagonal | USD/USD | 1.0853 | 1.1069 (q̄) | 1.2268 | 1.3797 | 1.0096 |
| N (kg/USD) | kg/USD | 2.7839 | 3.2163 (x̄) | 9.2134 | 0.4240 | 0.0829 |
| y_nab ($billions) | $billions | $211.86 | $204.65 (+) | $61.30 | $4.20 | $139.15 |
| (L_dom @ y_nab) ($billions) | $billions | $595.09 | $593.81 (+) | $230.42 | $20.93 | $342.46 |
| BLy (MtCO₂e) | MtCO₂e | **1424.21** | **1649.54** (+) | **1644.82** | **4.71** | **0.00** |

### Worked calculations — Step: reallocation (221100)

These identities use the single aggregate electricity sector before the 3-way split.

```
D[221100] = E/x = 1438.07e12 / 600.89e9 = 2.3932
N[221100] = sum(B @ L) = 2.7839
BLy[221100] = D * (L_dom @ y_nab)_221100
          = 2.3932 * $595.09B
          = 1424.21 MtCO2e
```

### Worked calculations — Step: 3-way split (221110 + 221121 + 221122)

These identities use the three child sectors. `221100*` in the table above is the re-aggregation of these values (`+` / `E/x` / `x̄` / `q̄` as marked).

#### Block D — q-weighted vs x-weighted / `E/x`

<a id="x-weighted-summary-dn"></a>

**Included in the side-by-side table and summary D (`221100*`):** block `E/x` = `sum(E)/sum(x)` = **2.4481** kg/USD (equals x-weighted child D when `Vnorm≈1`).

```
D[221100*] (E/x / x-weighted, in summary + table) = sum(E_s) / sum(x_s)
                           = 1471.03 / 600.89 = 2.448100 kg/USD

D[221100*] (q-weighted; comparison only) =
  (D_110*q_110 + D_121*q_121 + D_122*q_122) / sum(q)
  = (7.1385*230.42 + 0.2250*20.93 + 0.0000*342.46) / 593.81
  = 1649.54 / 593.81 = 2.777902 kg/USD

D[221100*] (x-weighted children) =
  (D_110*x_110 + D_121*x_121 + D_122*x_122) / sum(x)
  = (7.1385*205.33 + 0.2250*23.43 + 0.0000*372.12) / 600.89
  = 1471.03 / 600.89 = 2.448100 kg/USD
```

With diagonal Make (`Vnorm≈1`), child `D_s ≈ E_s/x_s`, so the x-weighted child average equals block `E/x`. The q-weighted value differs because `q ≠ x` and emissions concentrate on generation.

#### Block N — q-weighted vs x-weighted

**Included in the side-by-side table and summary N (`221100*`):** x-weighted child N = **3.2163** kg/USD.

```
N[221100*] (x-weighted, in summary + table) =
  (N_110*x_110 + N_121*x_121 + N_122*x_122) / sum(x)
  = (9.2134*205.33 + 0.4240*23.43 + 0.0829*372.12) / 600.89
  = 1932.61 / 600.89 = 3.216261 kg/USD

N[221100*] (q-weighted; comparison only) =
  (N_110*q_110 + N_121*q_121 + N_122*q_122) / sum(q)
  = (9.2134*230.42 + 0.4240*20.93 + 0.0829*342.46) / 593.81
  = 2160.21 / 593.81 = 3.637895 kg/USD
```

#### Block BLy (sum over electricity sectors) — matches `221100*` table row `+`

```
BLy_block = BLy_110 + BLy_121 + BLy_122
BLy_j     = D_j * (L_dom @ y_nab)_j

BLy[221110] = 7.1385 * $230.42B = 1644.82 MtCO2e
BLy[221121] = 0.2250 * $20.93B = 4.71 MtCO2e
BLy[221122] = 0.0000 * $342.46B = 0.00 MtCO2e
BLy_block = 1644.82 + 4.71 + 0.00 = 1649.54 MtCO2e
```

### Delta summary (reallocation → 3-way split)

| Metric | Reallocation | 3-way split | Change | Primary driver |
|--------|-------------:|------------:|-------:|----------------|
| D_block (kg/USD) | 2.3932 | 2.4481 | +0.0549 | eGRID E on 221110 / small x_gen |
| N_block (kg/USD) | 2.7839 | 3.2163 | +0.4324 | Higher D_gen + higher L_gen |
| BLy_block (Mt) | 1424.21 | 1649.54 | +225.33 | BLy_110 jumps with D_110 and L_dom @ y_nab |
| E_block (Mt) | 1438.07 | 1471.03 | +32.96 | eGRID FBS vs aggregate FBS |
| y_nab block (B) | 211.86 | 204.65 | -7.21 | IO split reallocates domestic demand |

**Why BLy rises more than E (+33 Mt):** BLy is not E. It is **attributed production** through the IO identity. Generation BLy uses `D_110 = 7.14` (not the aggregate 2.39) times `(L_dom @ y_nab)_110` ($230B). Transmission adds 4.7 MtCO₂e; distribution has D≈0 so BLy≈0. The block sum **1,650 Mt** exceeds inventory **1,471 Mt** because BLy counts attributed production through domestic final demand, not raw FBS totals.

---

## Walkthrough: 3-way split to unit conversion (D, N, BLy)

### Summary — why **D** is unchanged (USD-equiv)

1. **Why does D stay flat for electricity between the 3-way split and unit conversion?**
2. This walkthrough compares the **3-way split** (all USD) with **unit conversion** (generation in MWh; T/D still USD).
3. **D is unchanged in kg/USD-equivalent** because PR4 rescales generation **D** and **q** by the **same** `c_col`: `D_MWh = D_USD / c_col`, `q_MWh = q_USD × c_col` → USD-equivalent intensity is `D_MWh × c_col = D_USD`. Here `c_col = 0.018189` MWh/USD, so `D_110` goes 7.1385 kg/USD → 392.4687 kg/MWh and back-converts to the same USD-equiv value. Block D stays **2.4481** kg/USD-equiv. **E** and T/D **D** are untouched.
4. Absolute attributed emissions (**BLy**) are likewise invariant (`BLy = D·q`); what *does* move is **N**, via rewritten **A**/**L** (3.2163 → 3.5101 kg/USD-equiv).

**Takeaway:** Mixed units re-express generation on a physical basis; they do not change USD-equivalent direct EF or block `BLy`, because `c_col` cancels in `D·q`.

This section explains what PR4 (mixed units) changes — and why **electricity-block BLy stays at 1,649.54 MtCO₂e** (Δ = -7.73e-12 Mt).

### What PR4 does

PR4 converts **only generation (221110)** from USD to physical MWh via `c_col = 0.018189 MWh/USD` (eGRID net generation ÷ monetary `q_110`):

| Object | Conversion |
|--------|------------|
| `q_110` | `q_MWh = q_USD × c_col` |
| `B[:, 110]` (and thus `D_110`) | `B_MWh = B_USD / c_col` → `D` in kg/MWh |
| `Adom`/`Aimp` gen row & column | Rescaled with `c_col` / `c_row` so IO balance holds in mixed units |
| `E`, `x`, T/D sectors (221121/221122) | **Unchanged** |

### How `c_col` and `c_row` are calculated

Mixed units need two kinds of conversion factors for generation (**221110**):

| Factor | Role | Units |
|--------|------|-------|
| **`c_col`** | Converts the **generation column** (output `q_110`, inputs into gen, `B[:,110]`) from USD to MWh | MWh / USD |
| **`c_row`** | Converts the **generation sales row** (`Adom[110, ·]`, `Aimp[110, ·]`, and FD purchases of gen) from USD to MWh, **by purchaser** (end-use class) | MWh / USD (per column) |

`c_col` is a single national average intensity. `c_row` varies by purchaser because residential, commercial, industrial, and transportation buyers face different retail electricity prices (EIA EPA Table 2.4).

#### `c_col` — output / column factor

```
c_col = MWh_eGRID / q_USD_221110
```

- **MWh_eGRID** = U.S. total net generation from eGRID for model_base_year **2023** = **4,190,970,937 MWh** (4.1910 × 10⁹).
- **q_USD_221110** = scaled commodity output of generation = **$230.4162 B**.

```
c_col = 4,190,970,937 / 230,416,233,538.80
      = 0.018189 MWh/USD
```

Interpretation: each dollar of generation output corresponds to **0.0182 MWh** on average. Applying `q_MWh = q_USD × c_col` and `B_MWh = B_USD / c_col` keeps `B·q` (kg CO₂e) unchanged.

#### `c_row` — sales-row factors by purchaser class

Purchaser column `j` is mapped to an EPA end-use class (Residential / Commercial / Industrial / Transportation) via `build_end_use_map()`, then priced with Table 2.4 retail rates (cents/kWh, GHG year **2023**):

| End-use class | Table 2.4 price (¢/kWh) |
|---------------|------------------------:|
| Residential | 16.00 |
| Commercial | 12.59 |
| Industrial | 8.04 |
| Transportation | 12.77 |

Domestic generation-row USD flows are intermediate sales `A_110,j · q_j` plus model-year final-demand purchases `y_110,f`. Define a price-weighted denominator and a scalar **λ** that forces total converted MWh to equal eGRID generation:

```
denom = Σ_j (A_110,j · q_j) / p_j  +  Σ_f y_110,f / p_f
λ     = MWh_eGRID / denom
c_j   = λ / p_j     # for every purchaser column j (and FD category f)
```

Here `p_j` is the Table 2.4 price for `j`'s end-use class. **λ** absorbs unit consistency between USD flows and ¢/kWh prices so that

```
Σ_j (A_110,j · q_j · c_j) + Σ_f (y_110,f · c_f) = MWh_eGRID
```

exactly (row MWh identity). Numerically for this run:

```
denom = 2.0777e+10
λ     = 4,190,970,937 / denom = 0.201711
c_row ranges [0.012607, 0.025088] MWh/USD across 427 columns (median 0.025088)
```

**Example — intermediate purchaser** (221110, Industrial):

```
p_221110 = 8.04 ¢/kWh  (Industrial)
A_110,221110 = 0.178055
q_221110 = $230.42 B
flow_USD = A · q = $41.03 B
c_221110 = λ / p = 0.201711 / 8.04 = 0.025088 MWh/USD
```

**Another intermediate example** (531ORE, Commercial): `p = 12.59` ¢/kWh → `c = 0.016022` MWh/USD (flow $13.01 B).

**Example — final demand** (F01000, Residential):

```
y_110,F01000 = $61.20 B
p = 16.00 ¢/kWh (Residential)
c = λ / p = 0.201711 / 16.00 = 0.012607 MWh/USD
```

In `A`, the generation **row** is multiplied by `c_j` (USD sales → MWh sales) and the generation **column** is divided by `c_col` (inputs per $ → inputs per MWh). Cheaper industrial power gets a **larger** `c_j` than residential for the same λ, so a dollar of industrial purchases maps to more MWh.

### Side-by-side sector table

3-way (all USD) vs unit conversion (221110 in MWh; T/D still USD). `(L_dom @ y)` equals reported `q` under row balance.

| Sector | E (Mt) | D (3-way) | D (mixed) | q (3-way, USD B) | q (mixed) | L@y (3-way) | L@y (mixed) | BLy 3-way (Mt) | BLy mixed (Mt) |
|--------|-------:|----------:|----------:|-----------------:|----------:|-----------:|------------:|---------------:|---------------:|
| 221110 | 1465.76 | 7.1385 kg/USD | 392.4687 kg/MWh | $230.42 | 4.1910 B MWh | $230.42 | 4.1910 B MWh | **1644.82** | **1644.82** |
| 221121 | 5.27 | 0.2250 kg/USD | 0.2250 kg/USD | $20.93 | $20.93 B | $20.93 | $20.93 B | **4.71** | **4.71** |
| 221122 | 0.00 | 0.0000 kg/USD | 0.0000 kg/USD | $342.46 | $342.46 B | $342.46 | $342.46 B | **0.00** | **0.00** |
| **Sum** | **1471.03** | — | — | **$593.81** | — | — | — | **1649.54** | **1649.54** |

### Why BLy is unchanged (the key identity)

Per sector, attributed emissions are

```
BLy_j = D_j * (L_dom @ y_nab)_j
```

With a balanced domestic IO, `L_dom @ y_nab = q`, so **`BLy_j = D_j * q_j`**.

For generation, PR4 multiplies `q` by `c_col` and divides `D` by the **same** `c_col`:

```
c_col = 0.018189 MWh/USD
D_110_USD  = 7.138494 kg/USD
q_110_USD  = $230.4162 B
BLy_110    = 7.138494 * $230.4162B = 1644.82 Mt

D_110_MWh  = D_110_USD / c_col = 392.468673 kg/MWh
q_110_MWh  = q_110_USD * c_col = 4.190971 B MWh
BLy_110    = 392.468673 * 4.190971B = 1644.82 Mt
```

The `c_col` factors cancel: `(D/c_col) * (q·c_col) = D·q`. Transmission and distribution never change units, so their `BLy` is identical. Therefore the **block sum is identical** at Mt precision — not an accident of rounding, but the design of the mixed-units transform.

National total U.S. BLy is likewise unchanged for the same reason: only the generation column's intensity and activity units flip together; inventory `E` and all other sectors' `(D, q)` pairs are untouched.

### What *does* change (and what does not)

| Metric | 3-way split | Unit conversion | Change | Why |
|--------|------------:|----------------:|-------:|-----|
| E_block (Mt) | 1471.03 | 1471.03 | +0.00 | FBS inventory not recomputed |
| D_block (kg/USD-equiv) | 2.4481 | 2.4481 | -0.0000 | USD-equivalent D uses `D_MWh × c_col` for gen; stable |
| N_block (kg/USD-equiv) | 3.2163 | 3.5101 | +0.2939 | `L` changes with mixed `A`; total EF intensities move |
| BLy_block (Mt) | 1649.54 | 1649.54 | -7.73e-12 | `D·q` invariant under `c_col` |

**Takeaway:** Mixed units re-express generation on a physical activity basis (`kg/MWh` × MWh). Absolute attributed emissions (`BLy`) are invariant; Leontief total intensities (`N`) need not be, because `A`/`L` are rewritten.

---

## Walkthrough: y_nab block changes (reallocation → unit conversion)

Block **y_nab** in the summary table is the sum of `y_nab` over electricity sector(s): **221100** before PR3, **221110 + 221121 + 221122** after. Values are **not** obtained by splitting aggregate `y_nab[221100]`; each step recomputes from the domestic IO balance:

```
y_nab_i = q_i - sum_j(Adom_ij * q_j)   # backcompute_y_from_A_and_q(Adom, q)
```

### Reallocation → 3-way split: $211.86B → $204.65B (−$7.21B)

| | sum(q) | sum(Adom·q) | sum(y_nab) |
|---|---:|---:|---:|
| reallocation (221100) | $595.09 B | $383.23 B | **$211.86 B** |
| 3-way split (3 sectors) | $593.81 B | $389.16 B | **$204.65 B** |

The drop decomposes as:

```
Δy_nab = Δq - Δ(Adom·q) = (-1.29B) - (+5.92B) = -7.21B
```

PR3 disaggregates V/U/VA (not a proportional carve of aggregate `y_nab`). That raises domestic intermediate flows in `Adom` (block mean A diagonal ~0.071 → ~0.154) and slightly lowers electricity `q` ($595.09B → $593.81B). More of each child's `q` is explained by domestic purchases (`Adom·q`), so less remains as `y_nab`.

Per-sector backcompute at 3-way split:

| Sector | q | Adom·q (row) | y_nab |
|--------|---:|---:|---:|
| 221110 | $230.42 B | $169.11 B | **$61.30 B** |
| 221121 | $20.93 B | $16.74 B | **$4.20 B** |
| 221122 | $342.46 B | $203.31 B | **$139.15 B** |
| **Sum** | **$593.81 B** | **$389.16 B** | **$204.65 B** |

### 3-way split → unit conversion: $204.65B → 144.12 B mixed (-60.53 B)

PR4 only changes **221110** (generation → MWh). 221121 and 221122 are unchanged in USD:

| Sector | y_nab (3-way, USD) | y_nab (mixed) | Δ |
|--------|-------------------:|--------------:|--:|
| 221110 | $61.30 B | 0.77 B (mixed units) | -60.53 B (mixed units) |
| 221121 | $4.20 B | $4.20 B | $0.00 B |
| 221122 | $139.15 B | $139.15 B | $0.00 B |

Under mixed units, `q_221110` goes from **$230.42B (USD)** to **4.19 B (mixed units, MWh)** and `Adom` is rescaled for the generation row/column. `y_nab` is backcomputed again on that mixed `A`/`q`. The summary-table block total **144.12 B (mixed units)** is **not comparable USD**; only 221121 and 221122 remain monetary. The ~60.5B drop is almost entirely **221110's `y_nab` collapsing under MWh units** ($61.30 B → 0.77 B (mixed units)).

| | sum(q) | sum(Adom·q) | sum(y_nab) |
|---|---:|---:|---:|
| 3-way split (USD) | $593.81 B | $389.16 B | **$204.65 B** |
| unit conversion (mixed) | 367.58 B (mixed units) | 223.47 B (mixed units) | **144.12 B (mixed units)** |

### y_nab is not split by w_row

The **Y / use-commodity-row** split uses compensating weights **`w_row`** (from GO shares and Table 8.3 intersection). That splits commodity rows in **Y** and **U** — not `y_nab`. `y_nab` follows **`q − Adom·q`** on the disaggregated IO.

| Sector | w_row | naive (w_row × $211.86B) | actual backcompute |
|--------|------:|-------------------------:|-------------------:|
| 221110 | 0.3311 | $70.15 B | **$61.30 B** |
| 221121 | 0.0357 | $7.57 B | **$4.20 B** |
| 221122 | 0.6331 | $134.14 B | **$139.15 B** |

**w_row** shares: 221110 **33.11%**, 221121 **3.58%**, 221122 **63.31%**.
