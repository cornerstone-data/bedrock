# Household vs interindustry — MWh demand and BLy (mixed units)

Config: `2025_usa_cornerstone_v0_2_electricity_mixed_units` (`model_base_year=2023`, `usa_ghg_data_year=2023`).

Household final demand is BEA **`F01000`** (Personal consumption expenditures (households)). In conversation this is the “F001” / household PCE bucket; Cornerstone/BEA code is `F01000`, not a literal `F001` column.

## Framing

Total attributed production is `BLy = diag(D) @ L_dom @ y_nab`. In EEIO, **all** `BLy` is attributed to final demand; intermediate transactions are endogenous in `L_dom @ y`. So:

1. **Household emissions** = `BLy` from the `F01000` piece of `y_nab`.
2. **“Interindustry” emissions** here = `BLy` from **all other final-demand** columns (investment, government, exports, …). Those FD vectors induce interindustry electricity and other purchases through `L_dom`.
3. **Related MWh** for generation commodity `221110`: intermediate row uses `Adom[221110] ⊙ q` vs FD uses of `221110` split with 2017 Y shares (including `F01000`).

## 1–2. Model results

### National BLy split (full economy)

| Bucket | BLy (Mt CO2e) | Share of total BLy |
|---|---:|---:|
| Household (`F01000`) | 2,851.63 | 57.7% |
| Other final demand (induces interindustry) | 2,088.24 | 42.3% |
| **Total BLy** | **4,939.87** | **100%** |

Reconciliation `total − hh − other` = 0.000e+00 Mt (should be ~0).

Method: `y_hh = y_nab * (Y_2017[F01000] / sum_domestic_FD Y_2017)_i; BLy_hh = 1^T diag(D) L_dom y_hh; other = y_nab - y_hh`.

### Generation commodity (221110) MWh uses

| Use of 221110 | MWh | TWh | Share of uses |
|---|---:|---:|---:|
| Intermediate (all industries) | 3,417,736,048 | 3,417.7 | 81.5% |
| Household FD (`F01000`) | 761,473,815 | 761.5 | 18.2% |
| Other final demand | 11,761,074 | 11.8 | 0.3% |
| **Uses total** | **4,190,970,937** | **4,191.0** | **100%** |
| `q_221110` (mixed-units output) | 4,190,970,937 | 4,191.0 | — |
| `q − uses` | 0 | 0.0 | — |

### Generation-sector BLy allocated by those MWh shares

This is **not** the national BLy split above; it only apportions `BLy_221110 = 1,644.82` Mt by 221110 MWh use shares.

| Allocation of BLy_221110 | Mt CO2e |
|---|---:|
| Intermediate MWh | 1,341.35 |
| Household `F01000` MWh | 298.85 |
| Other FD MWh | 4.62 |

Note: Splits commodity-221110 BLy by share of 221110 MWh uses (intermediate vs F01000 vs other FD), not the full national BLy split.

## 3. Comparison to EIA Table 2.2 (2023)

Source already in-model: `EIA_ElectricPowerAnnual Table 2.2 (Total Electric Industry)` via `getFlowByActivity('EIA_ElectricPowerAnnual', year)`.

| EIA sector | MWh | TWh |
|---|---:|---:|
| Residential | 1,450,025,184 | 1,450.0 |
| Commercial | 1,408,108,755 | 1,408.1 |
| Industrial | 1,009,255,634 | 1,009.3 |
| Transportation | 6,863,789 | 6.9 |
| Direct use | 136,918,155 | 136.9 |
| **Total end use** | **4,011,171,517** | **4,011.2** |

### Model vs EIA

| Comparison | Model | EIA | Model/EIA |
|---|---:|---:|---:|
| Household `F01000` MWh vs Residential | 761.5 TWh | 1,450.0 TWh | 0.525 |
| Intermediate MWh vs Com+Ind+Trans sales | 3,417.7 TWh | 2,424.2 TWh | 1.410 |
| `q_221110` vs Total end use | 4,191.0 TWh | 4,011.2 TWh | 1.045 |

### Comparability notes

- EIA **Residential** ≈ model household electricity purchases, but PCE `F01000` is an IO final-demand construct (producer/purchaser and margin treatment differ from utility sales).
- EIA **Commercial + Industrial (+ Transportation)** is the closest published sales analog to model **intermediate** 221110 use; IO intermediate also includes electricity used by utilities and other sectors that EIA may classify differently, and excludes some FD electricity (gov, investment).
- Model `q_221110` is mixed-units generation **output** (eGRID net generation scaled via `c_col`), while EIA total end use is **sales + direct use** — related but not identical (losses, exports/imports, self-generation).

## Reproduce

```
python -m bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry.hh_vs_interindustry
```

Writes `C:/Users/jvend/Documents/CodeProjects/bedrock/bedrock/analysis/electricity_disagg_diagnostics/output/hh_vs_interindustry/hh_vs_interindustry_mwh_bly.md` and `C:/Users/jvend/Documents/CodeProjects/bedrock/bedrock/analysis/electricity_disagg_diagnostics/output/hh_vs_interindustry/hh_vs_interindustry_mwh_bly.json`.
