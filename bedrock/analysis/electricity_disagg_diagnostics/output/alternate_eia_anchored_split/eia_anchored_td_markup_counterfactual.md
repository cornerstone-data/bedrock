# Alternate EIA-anchored split — uniform generation, T&D markup

Diagnostics-only counterfactual for a redesigned **PR3 + PR4** electricity path. **No production EEIO code is modified.**

## Design (as specified)

1. **MWh anchor:** EIA Table 2.2 sales by class; within-class ∝ post-reallocation 221100 USD.
2. **Generation row:** p_uniform = (post–3-way 221110 Use+Y $) / eGRID net generation MWh; purchaser gen $ = allocated EIA sales MWh_j × p_uniform; clip gen down to Table 2.4 retail bill when T&D residual would be negative.
3. **T&D residual:** T&D_j = max(0, MWh_j×p_retail_class − gen_j); split T/D with UGO305 T/(T+D), D/(T+D).
4. **Make last:** Use+Y row totals as Make commodity weights (reported, not applied).

T/(T+D) national split: transmission **5.9%**, distribution **94.1%** (from UGO305).

## Key constructed quantities

| Item | Value |
|---|---:|
| Production `221110` Use+Y (numerator) | $232.37 B |
| eGRID net generation (denominator) | 4,191.0 TWh |
| Uniform gen price `p = 221110 $/eGRID` | $55.44/MWh (5.54 ¢/kWh) |
| Table 2.4 Industrial (reference) | 8.04 ¢/kWh |
| Allocated EIA sales MWh | 3,874.3 TWh |
| eGRID − EIA sales MWh | 316.7 TWh |
| Counterfactual gen $ on sales MWh | $214.81 B |
| Gen $ − production `221110` Use+Y | $-17.56 B |
| Counterfactual T&D $ | $276.50 B (T $16.38 B / D $260.12 B) |
| All-in (gen+T&D) $ | $491.31 B |
| `221100` Use+Y (MWh-weight source only) | $566.26 B |
| Purchasers with gen clipped to retail | 0 (0.0% of MWh) |
| Implied gen $/MWh min / median / max | 55.44 / 55.44 / 55.44 |
| MWh-weighted avg implied gen $/MWh | $55.44/MWh |

Uniform gen price (~5.5 ¢/kWh) is **below** all Table 2.4 class rates (Industrial 8.04 ¢/kWh), so clip rule B should rarely bind and **all classes keep positive T&D** markup above generation. Gen $ recovered on EIA sales MWh is below production `221110` Use+Y by the eGRID−sales gap × p.

## Alternate gen $ vs EIA Table 2.2 (implied prices)

Same layout as the production deck table (3-way `221110` $ / EIA TWh → implied ¢/kWh vs Table 2.4), but with **this counterfactual**: uniform `p = 221110 Use+Y / eGRID` (5.54 ¢/kWh) applied to EIA sales MWh. Implied gen prices are therefore flat across classes; the gap to Table 2.4 is absorbed as T&D markup.

| Class | Alt gen $ (B) (`221110`) | Alt T $ (B) (`221121`) | Alt D $ (B) (`221122`) | EIA TWh values | Implied gen prices, ¢/kWh | EIA Table 2.4, ¢/kWh |
|---|---:|---:|---:|---:|---:|---:|
| Residential | 80.40 | 8.98 | 142.63 | 1,450.03 | 5.54 | 16.00 |
| Commercial | 78.07 | 5.88 | 93.33 | 1,408.11 | 5.54 | 12.59 |
| Industrial | 55.96 | 1.49 | 23.69 | 1,009.26 | 5.54 | 8.04 |
| Transportation | 0.38 | 0.03 | 0.47 | 6.86 | 5.54 | 12.77 |
| **Total** | **214.81** | **16.38** | **260.12** | **3,874.25** | **5.54** | |

All-in (gen+T&D) $/MWh recovers Table 2.4 by construction for every class (clip rule B unused here).

## Class totals vs EIA and vs current production

| Class | EIA 2.2 (TWh) | Alt gen MWh (TWh) | Production mixed gen (TWh) | Alt gen $ (B) | Alt T&D $ (B) | Prod. pre-mix gen $ (B) |
|---|---:|---:|---:|---:|---:|---:|
| Residential | 1,450.0 | 1,450.0 | 771.5 | 80.40 | 151.61 | 61.20 |
| Commercial | 1,408.1 | 1,408.1 | 1,390.6 | 78.07 | 99.21 | 86.79 |
| Industrial | 1,009.3 | 1,009.3 | 1,963.1 | 55.96 | 25.19 | 78.25 |
| Transportation | 6.9 | 6.9 | 79.3 | 0.38 | 0.50 | 5.02 |

By construction, **alternate gen MWh class totals match EIA Table 2.2 sales** (within-class IO allocation only reshuffles inside the class). Current production **does not**.

### Household FD (`F01000`)

| | Alternate | Current production (mixed) |
|---|---:|---:|
| Gen-row MWh | 1,450.0 TWh | 771.5 TWh |
| vs EIA Residential | 100.0% | 53.2% |
| Gen $ | $80.40 B | (in Residential class USD) |
| T&D $ | $151.61 B | n/a (price in gen `c_row`) |
| Implied gen ¢/kWh | 5.54 | class-varying |
| Implied all-in ¢/kWh | 16.00 | ≈ Table 2.4 Residential |
| Gen clipped? | False | — |

## Make-last weights vs current UGO305 GO weights

| Commodity | Alt Use+Y share (Make-last) | UGO GO share | Δ (alt − UGO) |
|---|---:|---:|---:|
| 221110 | 43.7% | 34.2% | +9.5% |
| 221121 | 3.3% | 3.9% | -0.6% |
| 221122 | 52.9% | 61.9% | -9.0% |

With gen priced off **`221110`/eGRID** (below retail), T&D absorbs most of the Table 2.4 markup for every class. Make-last Use+Y shares are therefore **much more T&D-heavy** than the prior mistaken `221100`/sales uniform-price run, and closer in spirit to UGO’s distribution weight — though the exact split still differs from UGO.

## Expected effects on existing diagnostics

### 1. Household vs interindustry MWh (`hh_vs_interindustry`)

- **Generation-row MWh** by end-use class tracks EIA Table 2.2 by construction, fixing the current ~0.53× Residential shortfall on `F01000` for the gen commodity.
- Intermediate gen MWh class totals match EIA sales (self-use still inside Industrial if so mapped).
- **T&D** carries class retail markups above the low uniform gen price.

### 2. Class-price driver (decomposition §B)

- Production today puts Table 2.4 into **`c_row` on 221110**.
- Alternate: gen $/MWh is ~uniform at `221110`/eGRID; class price gaps move to **221121/221122**. With `D_T&D ≈ 0`, that mainly rewrites monetary `A`/`L`, not T&D direct EF.

### 3. Consumer `N` undilution (`n_variance_explained`)

- **Undilution of `D_221110`** can remain if eGRID E stays on generation — the +271 MMT / median `N` rise is not automatically gone.
- **Who** inherits it shifts with EIA MWh shares (more Residential-mapped, less Industrial overweight vs production).
- Industrial `%ΔN` boost from cheap gen `c_row` should shrink.

### 4. National BLy / full_trace

- Block BLy still ≈ `D_110·q_110` if E stays on generation.
- Make-last from gen+T&D Use+Y puts substantial weight on T&D; need consistent VA/x/E rules with Make-last ordering.
- Mixed-units `c_col` can stay eGRID/`q_110`; `c_row` becomes nearly flat (uniform gen price).

### 5. Feasibility flags

- **Gen $ on EIA sales vs production `221110` Use+Y:** $-17.56 B (eGRID−sales × p).
- **All-in vs `221100` Use+Y (weight source):** $-74.95 B.
- **Clip rule B:** 0 purchasers / 0.0% of MWh.
- Direct Use / losses sit in the eGRID−sales gap, not in allocated purchaser MWh.
- Industry-column / fuel / VA steps are not rebuilt here.

## Reproduce

```
python -m bedrock.analysis.electricity_disagg_diagnostics.alternate_eia_anchored_split
```

Writes `output\alternate_eia_anchored_split\eia_anchored_td_markup_counterfactual.md` and the JSON companion.
