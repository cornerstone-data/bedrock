# Counterfactual: unit conversion anchored to EIA Table 2.2 MWh

Config: `2025_usa_cornerstone_v0_2_electricity_mixed_units` (model year 2023).

## Setup

Keep the post–3-way-split monetary 221110 uses and the same end-use map. Replace Table 2.4 / eGRID row conversion with class factors

```text
c_class = EIA_Table_2_2_MWh[class] / USD_221110[class]
implied_price = USD_221110[class] / EIA_Table_2_2_MWh[class]
```

so every IO/FD column mapped to that class shares one price / MWh-per-dollar.

EIA sales total = **3,874.3 TWh**; eGRID net gen = **4,191.0 TWh**; gap = **316.7 TWh**.

## 1. Implied prices by end-use class

| Class | Model USD (B) | EIA 2.2 TWh | Implied $/MWh | Implied ¢/kWh | Table 2.4 ¢/kWh | Implied / 2.4 |
|---|---:|---:|---:|---:|---:|---:|
| Residential | 61.20 | 1,450.0 | 42.20 | 4.22 | 16.00 | 0.26 |
| Commercial | 85.96 | 1,408.1 | 61.04 | 6.10 | 12.59 | 0.48 |
| Industrial | 78.25 | 1,009.3 | 77.53 | 7.75 | 8.04 | 0.96 |
| Transportation | 5.02 | 6.9 | 731.11 | 73.11 | 12.77 | 5.73 |

Implied price = monetary 221110 USD in class / EIA Table 2.2 MWh for class. Units: $/MWh = USD/MWh; ¢/kWh = ($/MWh)/10.

### Read of the prices

- **Residential** implied ¢/kWh is far **below** Table 2.4 (4.2 vs 16.0) because model F01000 USD is too small relative to EIA Residential MWh.
- **Commercial** is also low (6.1 vs 12.6).
- **Industrial** lands near Table 2.4 (7.8 vs 8.0) only because large electricity-industry self-use inflates Industrial-mapped USD; that is a coincidence of mapping, not evidence the Industrial map is “right.”
- **Transportation** is an extreme outlier (73 vs 13 ¢/kWh): tiny EIA MWh vs non-trivial mapped model USD.

## 2. Impacts on D and N

### Analytic expectations

- D for generation depends on c_col (B_gen / c_col), not c_row. Keeping eGRID c_col leaves electricity D essentially unchanged; switching c_col to EIA sales changes D_221110 inversely with c_col.
- Non-electricity D is unchanged: only the generation column of B is scaled by c_col.
- N for electricity moves with L through the generation sales row (c_row) and with D_gen when c_col changes.
- Non-electricity N changes because A[221110, j] · c_j alters electricity requirements in L for every purchaser.

### Quantitative counterfactuals vs production (Table 2.4 + eGRID)

| Variant | Elec D mean Δ | Elec N mean Δ | Non-elec D mean Δ | Non-elec N mean Δ | Notes |
|---|---:|---:|---:|---:|---|
| A. Implied prices, keep eGRID λ | +0.00% | -13.08% | +0.00% | -9.96% | relative prices only; total still eGRID |
| B. Strict 2.2 row, c_col = eGRID | +0.00% | -14.16% | +0.00% | -11.07% | q=eGRID; backcomputed FD=1,769 TWh |
| C. Strict 2.2 row, c_col = EIA sales | +8.17% | -5.99% | +0.00% | -9.96% | q=sales; backcomputed FD=1,452 TWh |

### Electricity sectors under variant C (illustrative)

| Sector | D prod | D alt C | ΔD | N prod | N alt C | ΔN |
|---|---:|---:|---:|---:|---:|---:|
| 221110 | 392.4687 | 424.5527 | +8.17% | 553.6468 | 520.4881 | -5.99% |
| 221121 | 0.2250 | 0.2250 | +0.00% | 0.4264 | 0.4190 | -1.73% |
| 221122 | 0.0000 | 0.0000 | +nan% | 0.0846 | 0.0793 | -6.35% |

## 3. Production feasibility / IO balance

Electricity-industry purchases are **52.4%** of Industrial-mapped 221110 USD — a core reason Industrial implied prices look nothing like Table 2.4.

- **row_vs_output_identity** (high): EIA sales total (3874.3 TWh) ≠ eGRID generation (4191.0 TWh). After A conversion, y is backcomputed so q − uses ≈ 0 always; the real failure is compositional: with c_col=eGRID, backcomputed FD becomes 1769 TWh, not the EIA-class FD implied by the Table 2.2 anchors.
- **direct_use_and_losses** (high): Table 2.2 Direct Use and grid losses / plant use are not in the four sales classes. The mapping has nowhere to put them without an extra residual class or changing c_col/q.
- **electricity_self_use_in_industrial** (high): Electricity-industry intermediate purchases are 52.4% of Industrial-mapped USD. Forcing Industrial MWh = EIA Industrial assigns utility/self-generation throughput into the EIA industrial customer bucket.
- **delivered_vs_generation_commodity** (high): EIA classes are delivered sales; 221110 is generation only. Matching them forces generation-row MWh to equal delivered customer-class MWh.
- **import_row_and_margins** (medium): Aimp generation row would need the same class factors; purchaser-price vs producer-price and margin treatments still differ from utility revenue/sales.
- **negative_or_tiny_class_usd** (medium): A class with near-zero or negative net USD (inventory / scrap quirks in Y) makes c_class unstable.
- **ef_units_and_downstream** (medium): Changing c_col changes D_gen units (per MWh scale). N for non-electricity sectors moves with L even when D does not, so footprint tables and BLy attributions shift.

### Bottom line

1. Implied prices are well-defined from USD_class / EIA_2.2_MWh_class, but they are **accounting residuals**, not retail tariffs — especially Residential (too low) and Transportation (too high).
2. **Non-electricity D is unchanged**; **non-electricity N moves** with the generation sales row. Electricity **D moves only if c_col changes**; electricity **N moves with both c_row and c_col**.
3. A full production implementation is awkward: eGRID vs EIA sales, Direct Use/losses, generation-vs-delivered scope, and electricity self-use inside Industrial. Algebraic row balance can be forced by backcomputing y, but then class MWh no longer match Table 2.2.

## Reproduce

```
python -m bedrock.analysis.electricity_disagg_diagnostics.table_2_2_unit_conversion_counterfactual
```
