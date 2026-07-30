# Methods #86 — Three-path toy analysis (`Adom` + `Aimp` + `Atot`)

Shared 3×3 toy with domestic/import Use split. All sections use:

- `Adom = Unorm(Udom) @ Vnorm`, `Aimp = Unorm(Uimp) @ Vnorm`
- `Atot = Adom + Aimp`
- Detail year = `2017`; model year = `2023`

| Section | Path | Mixed units? |
| --- | --- | --- |
| **1** | Main branch (`derive_cornerstone_Aq_scaled` → BLy/D/N) | No — all USD |
| **2** | Analysis: scaled IO rebuild → mixed `V`/`U`/`Y` → rederive `A` | Yes — IO level |
| **3** | PR4 (`build_electricity_mixed_units_aq`) direct on scaled `A`/`q` | Yes — matrix level |

## Section 1 — Current production path (main branch)

Mirrors **main** through `derive_cornerstone_Aq_scaled` → monetary `B`, `L`, `y_nab` → BLy / D / N.
No mixed units; no `V`/`U` rebuild in production after inflation.

### 1.1 Monetary IO tables (detail year)

**`V` [USD]:**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| 221110 |      500 |    0 |    0 |
| C1     |        0 |  100 |    0 |
| C2     |        0 |    0 |  100 |

**`Udom` [USD]:**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| 221110 |       25 |    2 |    1 |
| C1     |        5 |   10 |    2 |
| C2     |        5 |    1 |    8 |

**`Uimp` [USD]:**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| 221110 |        0 |    8 |    4 |
| C1     |        0 |    0 |    0 |
| C2     |        0 |    0 |    0 |

**`Y` [USD]:**

|        |   F01000 |
|:-------|---------:|
| 221110 |      472 |
| C1     |       83 |
| C2     |       86 |

**`VA` [USD]:**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| V00100 |      200 |   30 |   25 |
| V00200 |       80 |   10 |    8 |
| V00300 |      120 |   20 |   15 |

### 1.2 Derive `Adom`, `Aimp`, `Atot`, `q`

**Equations:**

- `q = column-sum(V)`, `x = row-sum(V)`
- `Vnorm = V / q`, `Unorm = U / x`
- `Adom = Unorm(Udom) @ Vnorm`, `Aimp = Unorm(Uimp) @ Vnorm`
- `Atot = Adom + Aimp`

**`Adom`:**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| 221110 |     0.05 | 0.02 | 0.01 |
| C1     |     0.01 | 0.1  | 0.02 |
| C2     |     0.01 | 0.01 | 0.08 |

**`Aimp`:**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| 221110 |        0 | 0.08 | 0.04 |
| C1     |        0 | 0    | 0    |
| C2     |        0 | 0    | 0    |

**`Atot`:**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| 221110 |     0.05 | 0.1  | 0.05 |
| C1     |     0.01 | 0.1  | 0.02 |
| C2     |     0.01 | 0.01 | 0.08 |

**`q` [USD]:**

|        |   q | units   |
|:-------|----:|:--------|
| 221110 | 500 | USD     |
| C1     | 100 | USD     |
| C2     | 100 | USD     |

### 1.3 Scale + inflate to model year

**Scale** (``scale_cornerstone_A`` / ``scale_cornerstone_q``):

- `A_scaled = A_detail ⊙ ratio_A`
- `q_scaled = q_detail ⊙ ratio_q`

**Inflate** (``inflate_cornerstone_A_matrix_with_commodity_pi``):

- `A_target = diag(p) @ A_scaled @ diag(1/p)`
- `q_target = q_scaled ⊙ p`

**`ratio_q`:**

|        |   value |
|:-------|--------:|
| 221110 |    1.12 |
| C1     |    1.05 |
| C2     |    1.03 |

**`Adom_target` [model-year USD]:**

|        |    221110 |         C1 |        C2 |
|:-------|----------:|-----------:|----------:|
| 221110 | 0.054     | 0.025      | 0.0122685 |
| C1     | 0.009152  | 0.1        | 0.0203704 |
| C2     | 0.0088992 | 0.00981818 | 0.08      |

**`Aimp_target` [model-year USD]:**

|        |   221110 |       C1 |        C2 |
|:-------|---------:|---------:|----------:|
| 221110 |        0 | 0.104545 | 0.0518519 |
| C1     |        0 | 0        | 0         |
| C2     |        0 | 0        | 0         |

**`Atot_target` [model-year USD]:**

|        |    221110 |         C1 |        C2 |
|:-------|----------:|-----------:|----------:|
| 221110 | 0.054     | 0.129545   | 0.0641204 |
| C1     | 0.009152  | 0.1        | 0.0203704 |
| C2     | 0.0088992 | 0.00981818 | 0.08      |

**`q_target` [USD]:**

|        |      q | units   |
|:-------|-------:|:--------|
| 221110 | 700    | USD     |
| C1     | 115.5  | USD     |
| C2     | 111.24 | USD     |

### 1.4 Rebuilt scaled flows (analysis illustration only — not in production)

Production stops at scaled `A`/`q`. For identity checks in this section we rebuild
diagonal-Make flows from scaled `Atot`/`q`/`y_nab`:

**`V_scaled` [USD]:**

|        |   221110 |    C1 |     C2 |
|:-------|---------:|------:|-------:|
| 221110 |      700 |   0   |   0    |
| C1     |        0 | 115.5 |   0    |
| C2     |        0 |   0   | 111.24 |

**`Udom_scaled` [USD]:**

|        |   221110 |      C1 |      C2 |
|:-------|---------:|--------:|--------:|
| 221110 | 37.8     |  2.8875 | 1.36475 |
| C1     |  6.4064  | 11.55   | 2.266   |
| C2     |  6.22944 |  1.134  | 8.8992  |

**`Uimp_scaled` [USD]:**

|        |   221110 |     C1 |    C2 |
|:-------|---------:|-------:|------:|
| 221110 |        0 | 12.075 | 5.768 |
| C1     |        0 |  0     | 0     |
| C2     |        0 |  0     | 0     |

**`Y_scaled` [USD]:**

|        |   F01000 |
|:-------|---------:|
| 221110 | 640.105  |
| C1     |  95.2776 |
| C2     |  94.9774 |

**`VA_scaled` [USD]:**

|        |   221110 |    C1 |      C2 |
|:-------|---------:|------:|--------:|
| V00100 |      280 | 34.65 | 27.81   |
| V00200 |      112 | 11.55 |  8.8992 |
| V00300 |      168 | 23.1  | 16.686  |

### 1.5 BLy, D, N (monetary)

**Context (§1):** All values in USD (monetary).

**Equations:**

- `y_nab = q − rowsum(Adom ⊙ q)` (production uses `Adom`, not `Atot`)
- `L_tot = (I − Atot_target)⁻¹`
- `L_dom = (I − Adom)⁻¹`
- `D = rowsum(B)`, `M = B @ L_dom`, `N = rowsum(M)`
- `BLy = diag(D) @ L_dom @ y_nab`

**`Atot` used for `L_tot` (`Atot_target`):**

|        |    221110 |         C1 |        C2 |
|:-------|----------:|-----------:|----------:|
| 221110 | 0.054     | 0.129545   | 0.0641204 |
| C1     | 0.009152  | 0.1        | 0.0203704 |
| C2     | 0.0088992 | 0.00981818 | 0.08      |

**`L_tot = (I − Atot)⁻¹`** [dimensionless; USD/USD monetary]:

|        |    221110 |        C1 |        C2 |
|:-------|----------:|----------:|----------:|
| 221110 | 1.05929   | 0.153316  | 0.0772232 |
| C1     | 0.0110064 | 1.11297   | 0.0254102 |
| C2     | 0.010364  | 0.0133606 | 1.08797   |

**`y_nab`:**

|        |        q | units   |
|:-------|---------:|:--------|
| 221110 | 657.948  | USD     |
| C1     |  95.2776 | USD     |
| C2     |  94.9774 | USD     |

**`D`:**

|        |    q | units   |
|:-------|-----:|:--------|
| 221110 | 0.8  | USD     |
| C1     | 0.5  | USD     |
| C2     | 0.45 | USD     |

**`N`:**

|        |        q | units   |
|:-------|---------:|:--------|
| 221110 | 0.856155 | USD     |
| C1     | 0.584939 | USD     |
| C2     | 0.513499 | USD     |

**`BLy` [kg CO₂e]:**

|        |   value |
|:-------|--------:|
| 221110 | 560     |
| C1     |  57.75  |
| C2     |  50.058 |

### 1.6 Output identities (scaled monetary flows)

**Commodity identity** `q ≈ (Udom + Uimp)·1 + y_d`:

|        | units   |      q |   U·1 + y_d |   q − (U·1 + y_d) |
|:-------|:--------|-------:|------------:|------------------:|
| 221110 | USD     | 700    |      700    |       0           |
| C1     | USD     | 115.5  |      115.5  |       1.42109e-14 |
| C2     | USD     | 111.24 |      111.24 |       0           |

Diagnostic: commodity output and domestics use plus exports
Status: PASSED
Tolerance (rtol): 0.0000
Max normalized residual: 0.0000 (pass if <= 1.0)
Failing sectors: None

**Leontief identity** `q ≈ L_tot @ y_nab`:

|        | units   |      q |   L @ y_nab |   q − L @ y_nab |
|:-------|:--------|-------:|------------:|----------------:|
| 221110 | USD     | 700    |      700    |     0           |
| C1     | USD     | 115.5  |      115.5  |     0           |
| C2     | USD     | 111.24 |      111.24 |     2.84217e-14 |

Diagnostic: compare output and L * y
Status: PASSED
Tolerance (rtol): 0.0000
Max normalized residual: 0.0000 (pass if <= 1.0)
Failing sectors: None

## Section 2 — Mixed units at scaled IO tables (analysis path)

After scaling (§1.3), rebuild `V`/`Udom`/`Uimp`/`Y`/`VA`, apply mixed conversion
to generation-sector flows, then rederive `Adom`/`Aimp`/`Atot`/`q`.
**Not implemented in production.**

### 2.1 Scaled monetary IO (same as §1.4)

**`V_scaled` [USD]:**

|        |   221110 |    C1 |     C2 |
|:-------|---------:|------:|-------:|
| 221110 |      700 |   0   |   0    |
| C1     |        0 | 115.5 |   0    |
| C2     |        0 |   0   | 111.24 |

**`Udom_scaled` [USD]:**

|        |   221110 |      C1 |      C2 |
|:-------|---------:|--------:|--------:|
| 221110 | 37.8     |  2.8875 | 1.36475 |
| C1     |  6.4064  | 11.55   | 2.266   |
| C2     |  6.22944 |  1.134  | 8.8992  |

**`Uimp_scaled` [USD]:**

|        |   221110 |     C1 |    C2 |
|:-------|---------:|-------:|------:|
| 221110 |        0 | 12.075 | 5.768 |
| C1     |        0 |  0     | 0     |
| C2     |        0 |  0     | 0     |

**`Y_scaled` [USD]:**

|        |   F01000 |
|:-------|---------:|
| 221110 | 640.105  |
| C1     |  95.2776 |
| C2     |  94.9774 |

**`VA_scaled` [USD]:**

|        |   221110 |    C1 |      C2 |
|:-------|---------:|------:|--------:|
| V00100 |      280 | 34.65 | 27.81   |
| V00200 |      112 | 11.55 |  8.8992 |
| V00300 |      168 | 23.1  | 16.686  |

### 2.2 Conversion factors at target year

`c_col = 0.01` MWh/$; eGRID anchor = `7` MWh.

**`c_j`:**

|        |      value |
|:-------|-----------:|
| 221110 | 0.0102859  |
| C1     | 0.0060001  |
| C2     | 0.00720012 |
| F01000 | 0.0102859  |

Flow rules: scale `V[221110,221110]`, `Udom[221110,·]`, `Uimp[221110,·]`, `Y[221110,·]`;
leave `U[·,221110]` purchases in USD.

### 2.3 Mixed IO tables

**`V_mixed` (`221110` diagonal in MWh):**

|        |   221110 |    C1 |     C2 |
|:-------|---------:|------:|-------:|
| 221110 |        7 |   0   |   0    |
| C1     |        0 | 115.5 |   0    |
| C2     |        0 |   0   | 111.24 |

**`Udom_mixed` (hybrid USD/MWh):**

|        |   221110 |         C1 |         C2 |
|:-------|---------:|-----------:|-----------:|
| 221110 | 0.388806 |  0.0173253 | 0.00982636 |
| C1     | 6.4064   | 11.55      | 2.266      |
| C2     | 6.22944  |  1.134     | 8.8992     |

**`Uimp_mixed`:**

|        |   221110 |        C1 |        C2 |
|:-------|---------:|----------:|----------:|
| 221110 |        0 | 0.0724512 | 0.0415303 |
| C1     |        0 | 0         | 0         |
| C2     |        0 | 0         | 0         |

**`Y_mixed`:**

|        |   F01000 |
|:-------|---------:|
| 221110 |  6.58404 |
| C1     | 95.2776  |
| C2     | 94.9774  |

### 2.4 Rederive `Adom`, `Aimp`, `Atot`, `q`

**Equations:** same as §1.2 on mixed flows.

**`Adom_mixed`:**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000150002 | 8.83348e-05 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**`Aimp_mixed`:**

|        |   221110 |          C1 |          C2 |
|:-------|---------:|------------:|------------:|
| 221110 |        0 | 0.000627283 | 0.000373339 |
| C1     |        0 | 0           | 0           |
| C2     |        0 | 0           | 0           |

**`Atot_mixed`:**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000777285 | 0.000461674 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**`q_mixed`:**

|        |      q | units   |
|:-------|-------:|:--------|
| 221110 |   7    | MWh     |
| C1     | 115.5  | USD     |
| C2     | 111.24 | USD     |

### 2.5 BLy, D, N (mixed)

**Context (§2):** Hybrid units: `221110` in MWh / kg per MWh where applicable; others in USD.

**Equations:**

- `y_nab = q − rowsum(Adom ⊙ q)` (production uses `Adom`, not `Atot`)
- `L_tot = (I − Atot_mixed)⁻¹`
- `L_dom = (I − Adom)⁻¹`
- `D = rowsum(B)`, `M = B @ L_dom`, `N = rowsum(M)`
- `BLy = diag(D) @ L_dom @ y_nab`

**`Atot` used for `L_tot` (`Atot_mixed`):**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000777285 | 0.000461674 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**`L_tot = (I − Atot_mixed)⁻¹`** [dimensionless; computed from hybrid-unit `Atot_mixed`]:

|        |   221110 |         C1 |          C2 |
|:-------|---------:|-----------:|------------:|
| 221110 |  1.06022 | 0.00092169 | 0.000552449 |
| C1     |  1.10161 | 1.11234    | 0.0251819   |
| C2     |  1.03732 | 0.0127623  | 1.08776     |

**`y_nab`:**

|        |        q | units   |
|:-------|---------:|:--------|
| 221110 |  6.58404 | MWh     |
| C1     | 95.2776  | USD     |
| C2     | 94.9774  | USD     |

**`D`:**

|        |        q | units   |
|:-------|---------:|:--------|
| 221110 | 1.86066  | MWh     |
| C1     | 0.109968 | USD     |
| C2     | 0.100459 | USD     |

**`N`:**

|        |        q | units   |
|:-------|---------:|:--------|
| 221110 | 2.1957   | MWh     |
| C1     | 0.123776 | USD     |
| C2     | 0.112146 | USD     |

**`BLy` [kg CO₂e]:**

|        |   value |
|:-------|--------:|
| 221110 | 13.0246 |
| C1     | 12.7013 |
| C2     | 11.175  |

### 2.6 Output identities (row-balanced `U`/`Y` from rederived mixed `A`/`q`)

Raw mixed IO tables need not row-balance exactly; identities use
`Udom = Adom ⊙ q`, `Uimp = Aimp ⊙ q`, `y_nab` from `Atot`/`q`.

**Commodity identity** `q ≈ (Udom + Uimp)·1 + y_d`:

|        | units   |      q |   U·1 + y_d |   q − (U·1 + y_d) |
|:-------|:--------|-------:|------------:|------------------:|
| 221110 | MWh     |   7    |        7    |       0           |
| C1     | USD     | 115.5  |      115.5  |       1.42109e-14 |
| C2     | USD     | 111.24 |      111.24 |       0           |

Diagnostic: commodity output and domestics use plus exports
Status: PASSED
Tolerance (rtol): 0.0000
Max normalized residual: 0.0000 (pass if <= 1.0)
Failing sectors: None

**Leontief identity** `q ≈ L_tot @ y_nab`:

|        | units   |      q |   L @ y_nab |   q − L @ y_nab |
|:-------|:--------|-------:|------------:|----------------:|
| 221110 | MWh     |   7    |        7    |     8.88178e-16 |
| C1     | USD     | 115.5  |      115.5  |    -1.42109e-14 |
| C2     | USD     | 111.24 |      111.24 |     1.42109e-14 |

Diagnostic: compare output and L * y
Status: PASSED
Tolerance (rtol): 0.0000
Max normalized residual: 0.0000 (pass if <= 1.0)
Failing sectors: None

## Section 3 — Mixed units directly on scaled `A`, `q` (PR4 / `jv_PR4`)

Mirrors **`build_electricity_mixed_units_aq(derive_cornerstone_Aq_scaled())`**:
apply `apply_electricity_unit_conversion_to_A/q/B` to scaled blocks —
no `V`/`U` rebuild, no flow-table round-trip.

### 3.1 Scaled target-year `A` and `q` (from §1.3)

**`Adom_target` [USD]:**

|        |    221110 |         C1 |        C2 |
|:-------|----------:|-----------:|----------:|
| 221110 | 0.054     | 0.025      | 0.0122685 |
| C1     | 0.009152  | 0.1        | 0.0203704 |
| C2     | 0.0088992 | 0.00981818 | 0.08      |

**`Aimp_target` [USD]:**

|        |   221110 |       C1 |        C2 |
|:-------|---------:|---------:|----------:|
| 221110 |        0 | 0.104545 | 0.0518519 |
| C1     |        0 | 0        | 0         |
| C2     |        0 | 0        | 0         |

**`q_target` [USD]:**

|        |      q | units   |
|:-------|-------:|:--------|
| 221110 | 700    | USD     |
| C1     | 115.5  | USD     |
| C2     | 111.24 | USD     |

### 3.2 Conversion factors

`c_col = 0.01` MWh/$; eGRID anchor = `7` MWh.

**`c_j`:**

|        |      value |
|:-------|-----------:|
| 221110 | 0.0102859  |
| C1     | 0.0060001  |
| C2     | 0.00720012 |
| F01000 | 0.0102859  |

**Direct transform on `Adom`/`Aimp`/`q`:**

- `A[gen,j] *= c_j`; `A[gen,gen] *= c_gen/c_col`; `A[i,gen] /= c_col`
- `q[gen] *= c_col`
- `B[·,gen] /= c_col`

### 3.3 Mixed `Adom`, `Aimp`, `Atot`, `q`

**`Adom_mixed`:**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000150002 | 8.83348e-05 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**`Aimp_mixed`:**

|        |   221110 |          C1 |          C2 |
|:-------|---------:|------------:|------------:|
| 221110 |        0 | 0.000627283 | 0.000373339 |
| C1     |        0 | 0           | 0           |
| C2     |        0 | 0           | 0           |

**`Atot_mixed`:**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000777285 | 0.000461674 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**`q_mixed`:**

|        |      q | units   |
|:-------|-------:|:--------|
| 221110 |   7    | MWh     |
| C1     | 115.5  | USD     |
| C2     | 111.24 | USD     |

### 3.4 BLy, D, N (mixed)

**Context (§3):** Hybrid units: `221110` in MWh / kg per MWh where applicable; others in USD.

**Equations:**

- `y_nab = q − rowsum(Adom ⊙ q)` (production uses `Adom`, not `Atot`)
- `L_tot = (I − Atot_mixed)⁻¹`
- `L_dom = (I − Adom)⁻¹`
- `D = rowsum(B)`, `M = B @ L_dom`, `N = rowsum(M)`
- `BLy = diag(D) @ L_dom @ y_nab`

**`Atot` used for `L_tot` (`Atot_mixed`):**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000777285 | 0.000461674 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**`L_tot = (I − Atot_mixed)⁻¹`** [dimensionless; computed from hybrid-unit `Atot_mixed`]:

|        |   221110 |         C1 |          C2 |
|:-------|---------:|-----------:|------------:|
| 221110 |  1.06022 | 0.00092169 | 0.000552449 |
| C1     |  1.10161 | 1.11234    | 0.0251819   |
| C2     |  1.03732 | 0.0127623  | 1.08776     |

**`y_nab`:**

|        |        q | units   |
|:-------|---------:|:--------|
| 221110 |  6.58404 | MWh     |
| C1     | 95.2776  | USD     |
| C2     | 94.9774  | USD     |

**`D`:**

|        |     q | units   |
|:-------|------:|:--------|
| 221110 | 80    | MWh     |
| C1     |  0.5  | USD     |
| C2     |  0.45 | USD     |

**`N`:**

|        |         q | units   |
|:-------|----------:|:--------|
| 221110 | 85.7431   | MWh     |
| C1     |  0.575411 | USD     |
| C2     |  0.510104 | USD     |

**`BLy` [kg CO₂e]:**

|        |   value |
|:-------|--------:|
| 221110 | 560     |
| C1     |  57.75  |
| C2     |  50.058 |

### 3.5 Comparison with §2

Side-by-side comparison of the closing mixed-unit objects from §2 and §3.
On this diagonal-Make toy the paths agree numerically (differences ≈ 0).

#### `Atot_mixed`

**Section 2 (V/U rederived mixed)**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000777285 | 0.000461674 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**Section 3 (direct A/q mixed)**

|        |    221110 |          C1 |          C2 |
|:-------|----------:|------------:|------------:|
| 221110 | 0.0555438 | 0.000777285 | 0.000461674 |
| C1     | 0.9152    | 0.1         | 0.0203704   |
| C2     | 0.88992   | 0.00981818  | 0.08        |

**Difference (Section 2 − Section 3):**

|        |      221110 |   C1 |   C2 |
|:-------|------------:|-----:|-----:|
| 221110 | 6.93889e-18 |    0 |    0 |
| C1     | 0           |    0 |    0 |
| C2     | 0           |    0 |    0 |

#### `q_mixed`

**Section 2 (V/U rederived mixed)**

|        |      q | units   |
|:-------|-------:|:--------|
| 221110 |   7    | MWh     |
| C1     | 115.5  | USD     |
| C2     | 111.24 | USD     |

**Section 3 (direct A/q mixed)**

|        |      q | units   |
|:-------|-------:|:--------|
| 221110 |   7    | MWh     |
| C1     | 115.5  | USD     |
| C2     | 111.24 | USD     |

**Difference (Section 2 − Section 3):**

|        |   q | units   |
|:-------|----:|:--------|
| 221110 |   0 | MWh     |
| C1     |   0 | USD     |
| C2     |   0 | USD     |

#### `L_tot`

**Section 2 (V/U rederived mixed)**

|        |   221110 |         C1 |          C2 |
|:-------|---------:|-----------:|------------:|
| 221110 |  1.06022 | 0.00092169 | 0.000552449 |
| C1     |  1.10161 | 1.11234    | 0.0251819   |
| C2     |  1.03732 | 0.0127623  | 1.08776     |

**Section 3 (direct A/q mixed)**

|        |   221110 |         C1 |          C2 |
|:-------|---------:|-----------:|------------:|
| 221110 |  1.06022 | 0.00092169 | 0.000552449 |
| C1     |  1.10161 | 1.11234    | 0.0251819   |
| C2     |  1.03732 | 0.0127623  | 1.08776     |

**Difference (Section 2 − Section 3):**

|        |   221110 |   C1 |   C2 |
|:-------|---------:|-----:|-----:|
| 221110 |        0 |    0 |    0 |
| C1     |        0 |    0 |    0 |
| C2     |        0 |    0 |    0 |
