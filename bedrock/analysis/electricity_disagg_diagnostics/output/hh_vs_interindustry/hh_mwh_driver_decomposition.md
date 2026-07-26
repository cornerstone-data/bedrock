# Household vs intermediate electricity — A–E driver decomposition

Config: `2025_usa_cornerstone_v0_2_electricity_mixed_units`; model year **2023**; EIA price year **2023**.

Pipeline steps referenced below follow the electricity diagnostics sequence **footing → reallocation → 3-way split → unit conversion**. Monetary tables use `derive_cornerstone_Aq_scaled()` (post–3-way split, still USD). Physical MWh tables apply generation-row conversion factors to those same flows (the unit-conversion step), either with production class prices or a uniform-price diagnostic counterfactual.

## Executive conclusion

- The post–3-way-split monetary IO assigns **26.6%** of 221110 uses to `F01000` versus EIA Residential's **37.4%** share of retail sales. A substantial mismatch therefore exists **before mixed units**.
- Class prices and mapping move household electricity from **1,113.1 TWh** under uniform prices to **771.5 TWh** in production. They add **50.3%** of the final direct-row household shortfall versus EIA Residential—about half, not a negligible adjustment.
- Intermediate electricity moves from **3,075.9 TWh** to **3,417.7 TWh**; class prices add **34.4%** of its final excess versus EIA nonresidential sales.
- **30.1%** of direct intermediate 221110 MWh is purchased by the three electricity industries themselves. That is IO-intermediate supply-chain throughput, not an EIA ultimate-customer classification.
- External BEA methodology confirms that NIPA electricity PCE is anchored to EIA Residential, but NIPA PCE is delivered purchaser value while this diagnostic follows the **generation-only 221110 row**. The direct-row F01000/EIA ratio is therefore diagnostic, not an apples-to-apples validation.

## A: Are USD uses already skewed vs EIA before unit conversion?

**Disaggregation step: 3-way split.** Table values are monetary generation-row uses from `derive_cornerstone_Aq_scaled()` after reallocation and the three-way electricity split, **before** mixed-unit / unit conversion.

| 221110 use | USD (billions) | Share |
|---|---:|---:|
| Intermediate: `221110` use of `221110` | 41.03 | 17.8% |
| Intermediate: other industries | 128.08 | 55.6% |
| Household `F01000` | 61.20 | 26.6% |
| Other final demand | 0.11 | 0.0% |
| **Total** | **230.42** | **100.0%** |

These are monetary shares from the 3-way split. Intermediate is split into generation’s own use of the generation commodity versus all other industry purchasers. The uniform-price scenario in B converts the same flows to physical MWh without changing the shares, making the EIA comparison dimensionally valid while preserving the pre-conversion structure.

## B: How much do class prices and end-use mapping move the mixed MWh split?

**Disaggregation step: unit conversion** (applied to the same 3-way-split monetary flows from A). The **Uniform price** column is a diagnostic counterfactual (one MWh/USD for every purchaser). The **Production class prices** column is the production unit-conversion path (`c_row = λ / p_class`). Both columns hold total generation-row MWh fixed to eGRID.

| Bucket | Uniform price (TWh) | Production class prices (TWh) | Change (TWh) |
|---|---:|---:|---:|
| Intermediate | 3,075.9 | 3,417.7 | +341.8 |
| Household `F01000` | 1,113.1 | 771.5 | -341.6 |
| Other final demand | 2.0 | 1.7 | -0.2 |
| **Total** | 4,191.0 | 4,191.0 | -0.0 |

Both scenarios hold total generation-row uses fixed to **4,191.0 TWh**. Therefore production minus uniform isolates **class prices plus the mapping that assigns each purchaser to a class**.

| Gap decomposition | Household shortfall vs Residential | Intermediate excess vs nonresidential |
|---|---:|---:|
| Present under uniform prices | 336.9 TWh | 651.7 TWh |
| Added by class prices + map | 341.6 TWh | 341.8 TWh |
| Final production gap | 678.5 TWh | 993.5 TWh |
| Share added by prices + map | 50.3% | 34.4% |

This is an accounting decomposition, not a causal structural model: the uniform residual combines the IO structure, the 2017 final-demand share proxy, and remaining definition/boundary differences.

### Reconciliation to the earlier household report

The direct cell conversion gives **771.5 TWh** for `F01000`. The earlier `hh_vs_interindustry` convention gives **761.5 TWh** because it reallocates total mixed final demand with clipped, domestic-only 2017 shares. The A–E decomposition uses direct converted cells so the production and uniform scenarios differ only in `c_row`.

The direct scenario converts each raw model-year Y cell with its class factor. The earlier report instead allocates total mixed y_nab using nonnegative domestic 2017 Y shares (imports excluded).

## C: Which intermediate purchasers hold residential-like MWh?

**Disaggregation step: unit conversion (production class prices).** MWh tables below are the production column from B: 3-way-split monetary flows converted with Table 2.4 / end-use-map `c_row`. The USD column in the top-25 table is still the pre-conversion 3-way-split monetary flow.

| Assigned class | Intermediate TWh | FD TWh | Total TWh |
|---|---:|---:|---:|
| Residential | 0.0 | 771.5 | 771.5 |
| Commercial | 1,375.4 | 1.7 | 1,377.1 |
| Industrial | 1,963.1 | 0.0 | 1,963.1 |
| Transportation | 79.3 | 0.0 | 79.3 |

### Electricity-supply-chain purchases

| Direct intermediate purchaser | TWh of 221110 |
|---|---:|
| `221110` | 1,029.3 |
| `221121` | 0.0 |
| `221122` | 0.0 |
| **Three electricity industries** | **1,029.3** |
| Other intermediate purchasers | 2,388.4 |

These are generation-row purchases by electricity industries. They are intermediate in IO accounting but are not themselves ultimate EIA customer classes; their downstream destination requires supply-chain tracing and cannot be inferred from the direct 221110 row.

### Top 25 intermediate purchasers

| Rank | Code | Name | Assigned class | USD (B) | TWh | Housing keyword |
|---:|---|---|---|---:|---:|:---:|
| 1 | `221110` | Electric power generation | Industrial | 41.03 | 1,029.3 |  |
| 2 | `531ORE` | Other real estate | Commercial | 13.01 | 208.4 | yes |
| 3 | `211000` | Unrefined oil and gas | Industrial | 3.54 | 88.8 |  |
| 4 | `324110` | Gasoline, fuels, and by-products of petroleum refining | Industrial | 3.51 | 88.0 |  |
| 5 | `722211` | Limited-service restaurants | Commercial | 5.39 | 86.3 |  |
| 6 | `445000` | Food and beverage stores | Commercial | 4.67 | 74.8 |  |
| 7 | `452000` | General merchandise stores | Commercial | 3.93 | 62.9 |  |
| 8 | `622000` | Hospitals | Commercial | 3.56 | 57.1 |  |
| 9 | `722110` | Full-service restaurants | Commercial | 3.50 | 56.1 |  |
| 10 | `447000` | Gasoline stations | Commercial | 3.19 | 51.1 |  |
| 11 | `550000` | Company and enterprise management | Commercial | 2.34 | 37.5 |  |
| 12 | `518200` | Data processing and hosting | Commercial | 2.11 | 33.7 |  |
| 13 | `4B0000` | Other retail | Commercial | 2.02 | 32.3 |  |
| 14 | `484000` | Truck transport | Transportation | 1.93 | 30.4 |  |
| 15 | `721000` | Hotels and campgrounds | Commercial | 1.90 | 30.4 |  |
| 16 | `517110` | Telecommunications | Commercial | 1.90 | 30.4 |  |
| 17 | `722A00` | All other food and drinking places | Commercial | 1.88 | 30.0 |  |
| 18 | `441000` | Vehicles and parts sales | Commercial | 1.77 | 28.4 |  |
| 19 | `331110` | Primary iron, steel, and ferroalloy products | Industrial | 1.13 | 28.2 |  |
| 20 | `454000` | Nonstore retailers | Commercial | 1.69 | 27.0 |  |
| 21 | `424A00` | Other nondurable goods merchant wholesalers | Commercial | 1.63 | 26.1 |  |
| 22 | `444000` | Building material and garden equipment and supplies dealers | Commercial | 1.39 | 22.3 |  |
| 23 | `446000` | Health and personal care stores | Commercial | 1.38 | 22.1 |  |
| 24 | `326190` | Other plastic products | Industrial | 0.88 | 22.0 |  |
| 25 | `423A00` | Other durable goods merchant wholesalers | Commercial | 1.35 | 21.6 |  |

Keyword flag is descriptive only; it does not reclassify an IO industry or prove that its electricity serves households.

## D: How sensitive are Model/EIA ratios to alternate bucket pairings?

**Disaggregation step: unit conversion (production class prices)** for all model TWh columns; EIA columns are published Table 2.2 sales / end use (not an IO pipeline step). Model buckets reuse the same production unit-conversion MWh vectors as C.

| Model bucket | Model TWh | EIA bucket | EIA TWh | Model/EIA |
|---|---:|---|---:|---:|
| F01000 only | 771.5 | EIA Residential | 1,450.0 | 0.532 |
| All Residential-mapped final demand | 771.5 | EIA Residential | 1,450.0 | 0.532 |
| All intermediate | 3,417.7 | EIA Com+Ind+Trans sales | 2,424.2 | 1.410 |
| Intermediate excluding electricity-industry purchasers | 2,388.4 | EIA Com+Ind+Trans sales | 2,424.2 | 0.985 |
| Intermediate + all other final demand | 3,419.5 | EIA Com+Ind+Trans sales + Direct Use | 2,561.1 | 1.335 |
| All model uses mapped Residential | 771.5 | EIA Residential | 1,450.0 | 0.532 |
| All model uses mapped Commercial | 1,377.1 | EIA Commercial | 1,408.1 | 0.978 |
| All model uses mapped Industrial | 1,963.1 | EIA Industrial | 1,009.3 | 1.945 |
| All model uses mapped Transportation | 79.3 | EIA Transportation | 6.9 | 11.548 |
| All model 221110 uses | 4,191.0 | EIA Total sales | 3,874.3 | 1.082 |
| All model 221110 uses | 4,191.0 | EIA Total End Use | 4,011.2 | 1.045 |

Mapped class comparisons test the current end-use mapping; they do not make IO industries identical to utility customer classes. The electricity-excluded row is only a boundary sensitivity: it does not trace those MWh to their eventual customers.

## E: What do BEA PCE and EIA sales-by-sector notes imply about A–D?

**Disaggregation step: none for the source table** (external BEA/EIA methodology). Model quantities cited in the sanity check are from the **unit conversion (production class prices)** result for `F01000`, i.e. the same production path as B–D.

Evidence reviewed **2026-07-24**.

| Organization | Source | Finding used here |
|---|---|---|
| BEA | [NIPA Handbook, Chapter 5 — Personal Consumption Expenditures](https://www.bea.gov/resources/methodologies/nipa-handbook/pdf/chapter-05.pdf) | PCE electricity uses EIA residential revenue and residential kWh/price data, adjusted by BEA from a billing to a usage basis. |
| BEA | [NIPA Table 2.4.5, Household utilities: Electricity (DELCRC)](https://fred.stlouisfed.org/series/DELCRC1A027NBEA) | Current-dollar PCE electricity was $236.748 billion in 2023. |
| BEA | [FAQ 84 — detail beyond PCE Table 2.4.5U](https://www.bea.gov/help/faq/84) | Table 2.4.5U is the most detailed time series; benchmark IO PCE Bridge files provide the IO commodity composition of each PCE category. |
| BEA | [Historical Benchmark Input-Output Tables / PCE Bridge](https://www.bea.gov/industry/historical-benchmark-input-output-tables) | PCE Bridge tables reconcile NIPA PCE with IO commodities at producer and purchaser prices; they are the appropriate bridge, not total F01000. |
| EIA | [Electric Power Annual Table 2.2](https://www.eia.gov/electricity/annual/html/epa_02_02.html) | Reports sales to ultimate customers by Residential, Commercial, Industrial, and Transportation, plus Direct Use and Total End Use. |
| EIA | [Form EIA-861 instructions](https://www.eia.gov/survey/form/eia_861/instructions.pdf) | Residential includes private households and apartment buildings where electricity is consumed for household purposes; Commercial includes nonmanufacturing businesses, institutions, government, and lighting. |
| EIA | [Guide to EIA Electric Power Data](https://www.eia.gov/electricity/data/guide/pdf/guide.pdf) | EIA-861 is a census of utilities and other sellers; its sectors are customer/end-use classes, not IO purchaser industries. |

### Independent PCE-dollar sanity check

```text
BEA PCE electricity (2023) = $236.748 B
EIA residential price      = 16.00 cents/kWh
Implied MWh = PCE dollars / (price × $10/MWh per cent/kWh)
            = 1,479,675,000 MWh
```

The implied **1,479.7 TWh** is **102.0%** of EIA Residential (1,450.0 TWh), while the model `F01000` result is only **52.1%** of that implied quantity. This confirms that BEA PCE electricity and EIA Residential are closely aligned. It does **not** make direct `221110 × F01000` comparable to delivered sales: the latter include transmission and distribution, while generation sold through those industries remains intermediate in the direct IO row.

Caveat: PCE dollars are a national-accounting purchaser-value measure and EIA price is utility revenue per kWh. Their quotient is a sanity check, not an exact BEA published physical series; BEA also adjusts billing to usage.

### A–D conclusions after E

1. **A identifies a real direct-row structure, but not ultimate use:** some generation is routed through electricity industries before reaching the EIA customer.
2. **B is quantitatively important:** class prices account for about half the direct household shortfall and about one-third of the intermediate excess; they are not a complete explanation.
3. **C reveals the central classification limit:** EIA classes ultimate customers by use, whereas the direct IO row records purchaser industries and supply-chain throughput.
4. **D is essential:** generation output, utility end-use sales, direct use, and IO domestic uses have different boundaries.

## Reproduce

```
python -m bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry.hh_mwh_driver_decomposition
```

Writes `C:/Users/jvend/Documents/CodeProjects/bedrock/bedrock/analysis/electricity_disagg_diagnostics/output/hh_vs_interindustry/hh_mwh_driver_decomposition.md` and `C:/Users/jvend/Documents/CodeProjects/bedrock/bedrock/analysis/electricity_disagg_diagnostics/output/hh_vs_interindustry/hh_mwh_driver_decomposition.json`.
