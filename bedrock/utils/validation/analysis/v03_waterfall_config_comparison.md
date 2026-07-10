# v03 waterfall config comparison

Flag reference for wholesale v0→v0.3 EF diagnostics. Values are **resolved**
after loading each yaml through `USAConfig` (unset keys inherit defaults from
`bedrock.utils.config.usa_config`).

Configs live in `bedrock/utils/config/configs/v03_waterfall_*.yaml`. Dispatch
scripts and registries:

- USEEIO path: `bedrock.analysis.v0_3.dispatch_ef_v03_waterfall` (default baseline)
- CEDA path: `bedrock.analysis.v0_3.dispatch_ef_v03_waterfall --baseline ceda`
- Registries: `release_v0_v03_useeio_groups.py`, `release_v0_v03_ceda_groups.py`

All waterfall endpoints use **IO@2024 producer** footing (`model_base_year: 2024`,
`price_type: producer`) unless noted.

## Progressions

**USEEIO baseline** (G1 → G2 → G3 → FINAL):

| Step | Config stem | Marginal change |
|------|-------------|-----------------|
| Baseline | *(pinned USEEIO Excel)* | Comparison anchor |
| G1 | `v03_waterfall_useeio_g1_schema_ghg` | Pinned USEEIO → Cornerstone schema + GHG (USEEIO A/margins retained) |
| G2 | `v03_waterfall_g2_methods` | G1 → CEDA A/price, cornerstone margins, inflation |
| G3 | `v03_waterfall_g3_data` | G2 → 2024 UMD GHG / IO data |
| FINAL | `v03_waterfall_final` | Shipped v0.3 mix (`2025_usa_cornerstone_v0_3` equivalent) |

**CEDA baseline** (G1a → G1b → G2 → G3 → FINAL):

| Step | Config stem | Marginal change |
|------|-------------|-----------------|
| Baseline | `v8_ceda_2025_usa` | CEDA v0 parquet snapshot |
| G1a | `v03_waterfall_ceda_g1a_schema_ghg` | CEDA v0 → Cornerstone schema + GHG (no waste) |
| G1b | `v03_waterfall_ceda_g1b_waste_disagg` | G1a → waste disaggregation |
| G2 | `v03_waterfall_g2_methods` | G1b → CEDA A/price, cornerstone margins, inflation |
| G3 | `v03_waterfall_g3_data` | G2 → 2024 UMD GHG / IO data |
| FINAL | `v03_waterfall_final` | Shipped v0.3 mix |

G2, G3, and FINAL share the same yaml on both baselines. Only the diagnostics
comparison anchor differs (USEEIO Excel vs CEDA v0 parquet).

## Baseline anchors

### Pinned USEEIO baseline

Not a Bedrock yaml config. Diagnostics loads a pinned Excel workbook when
dispatch passes `use_useeio_baseline=True` and
`bedrock/utils/snapshots/useeio_baseline_pin.json`:

| Field | Value |
|-------|-------|
| Artifact | `USEEIOv2.6.0-phoebe-23.xlsx` |
| GCS URI | `gs://cornerstone-default/snapshots/USEEIOv2.6.0-phoebe-23/USEEIOv2.6.0-phoebe-23.xlsx` |
| Model label | `USEEIOv2.6.0-phoebe-23` |
| `diagnostics_baseline_source` at run time | `gcs_useeio_xlsx` |
| Combine / merge column name | `pinned_useeio_baseline` |
| Source in merged workbooks | G1 sheet `D_old_inflated` / `N_old_inflated` |

Provenance: `bedrock/utils/snapshots/useeio_baseline_pin.provenance.md`.

### CEDA v0 baseline (`v8_ceda_2025_usa`)

Parquet snapshot release config. Serves as the G1a comparison anchor on the
CEDA waterfall path (`CEDA_V0_BASELINE` in `release_v0_3_progression.py`).

| Field | Value |
|-------|-------|
| Config stem | `v8_ceda_2025_usa` |
| `model_base_year` | 2023 |
| `usa_ghg_data_year` | 2023 |
| `diagnostics_baseline_source` | `gcs_snapshot` |
| `ceda_margins` | true |
| `cornerstone_industry_avg_margins` | false |
| `ipcc_ar_version` | AR6 |
| Other methodology flags | `USAConfig` defaults (legacy CEDA v0 stack) |

## Footing and data selection

| Step | Config | `model_base_year` | `price_type` | `usa_ghg_data_year` | `iot_before_or_after` |
|------|--------|-------------------|--------------|---------------------|------------------------|
| Pinned USEEIO | *(Excel)* | ~2023 | purchaser (Excel) | ~2023 | — |
| CEDA v0 | `v8_ceda_2025_usa` | 2023 | producer | 2023 | after |
| USEEIO G1 | `v03_waterfall_useeio_g1_schema_ghg` | 2024 | producer | 2023 | **before** |
| CEDA G1a | `v03_waterfall_ceda_g1a_schema_ghg` | 2024 | producer | 2023 | after |
| CEDA G1b | `v03_waterfall_ceda_g1b_waste_disagg` | 2024 | producer | 2023 | after |
| G2 | `v03_waterfall_g2_methods` | 2024 | producer | 2023 | after |
| G3 | `v03_waterfall_g3_data` | 2024 | producer | **2024** | after |
| FINAL | `v03_waterfall_final` | 2024 | producer | **2024** | after |

## Schema and GHG methodology

| Step | `use_cornerstone_2026_model_schema` | `load_E_from_flowsa` | `new_ghg_method` | `v0_3_umd_2024_ghgia` | `use_E_data_year_for_x_in_B` |
|------|-------------------------------------|----------------------|------------------|-----------------------|------------------------------|
| CEDA v0 | false | false | false | false | false |
| USEEIO G1 | true | true | true | false | true |
| CEDA G1a | true | true | true | false | false |
| CEDA G1b | true | true | true | false | false |
| G2 | true | true | true | false | true |
| G3 | true | true | false | **true** | true |
| FINAL | true | true | false | **true** | true |

G1/G1a/G1b/G2 use the Cornerstone GHG FBS path (`new_ghg_method`). G3 and FINAL
switch to the UMD GHGIA 2024 inventory (`v0_3_umd_2024_ghgia`), matching
`2025_usa_cornerstone_v0_3`.

## Waste disaggregation

| Step | `implement_waste_disaggregation` |
|------|----------------------------------|
| CEDA v0 | false |
| USEEIO G1 | true |
| CEDA G1a | false |
| CEDA G1b | true |
| G2 | true |
| G3 | true |
| FINAL | true |

## A matrix, margins, and inflation

| Step | `useeio_margins` | `ceda_margins` | `cornerstone_industry_avg_margins` | `scale_a_matrix_with_useeio_method` | `scale_a_matrix_with_ceda_method_as_fallback` | `adjust_summary_A_and_q_dollar_year` | `update_inflation_factors` |
|------|------------------|----------------|------------------------------------|---------------------------------------|-----------------------------------------------|--------------------------------------|------------------------------|
| CEDA v0 | false | **true** | false | false | false | false | false |
| USEEIO G1 | **true** | false | false | **true** | false | false | false |
| CEDA G1a | false | false | true | false | false | false | false |
| CEDA G1b | false | false | true | false | false | false | false |
| G2 | false | false | true | false | **true** | **true** | **true** |
| G3 | false | false | true | false | true | true | true |
| FINAL | false | false | true | false | true | true | true |

## USEEIO G1-only flags

| Flag | USEEIO G1 | All other waterfall configs |
|------|-----------|------------------------------|
| `deflate_x_to_detail_io_year_for_B` | true | false |
| `use_ghg_national_2023_m2` | false | false |
| `skip_scrap_adjustment_in_vnorm` | true | false |
| `use_useeio_schema` | false | false |

## Diagnostics baseline at dispatch

| Path | Dispatch flag | `diagnostics_baseline_source` on sheet | Old-side EF source |
|------|---------------|----------------------------------------|--------------------|
| USEEIO waterfall | `use_useeio_baseline=True` | `gcs_useeio_xlsx` | Pinned USEEIO Excel (`D_old_*` / `N_old_*`) |
| CEDA waterfall | `use_useeio_baseline=False` | `gcs_snapshot` | CEDA v0 parquet (`v8_ceda_2025_usa` snapshot) |

Yaml files do not set `diagnostics_baseline_source`; the diagnostics workflow
overrides it when the USEEIO pin JSON is supplied.

## Combine net-diff chains

| Combo | First-step target | Chain |
|-------|-------------------|-------|
| `v0_to_v03_useeio_groups` | `pinned_useeio_baseline` | G1 → G2 → G3 (marginals); FINAL verifies |
| `v0_to_v03_ceda_groups` | `v8_ceda_2025_usa` | G1a → G1b → G2 → G3 (marginals); FINAL verifies |

Both combos use `n_price_type='producer'` in `combinations.py`.

## B denominator and EF dollar year (CEDA path)

| Step | `use_E_data_year_for_x_in_B` | `usa_ghg_data_year` | New-side `B` construction | Old-side (in-sheet diff) |
|------|------------------------------|---------------------|---------------------------|--------------------------|
| CEDA v0 | false | 2023 | Legacy scale + PI to `model_base_year` (2023) | — |
| G1a | false | 2023 | Legacy scale + PI to `model_base_year` (2024) | v0 snapshot inflated 2023→2024 |
| G1b | false | 2023 | Same as G1a | Prior run snapshot inflated to 2024 |
| G2 | true | 2023 | Vnorm; gross output at `usa_ghg_data_year` | Prior run snapshot inflated to 2024 |
| G3 | true | 2024 | Vnorm; gross output at `usa_ghg_data_year` | Prior run snapshot inflated to 2024 |
| FINAL | true | 2024 | Same as G3 | Prior run snapshot inflated to 2024 |

Waterfall plots and combine use `ref_2023_sheet_id=None` (no extra cross-sheet
2023→2024 inflation). Cross-sheet marginals use each run's `D_new` / `N_new`
columns as written on the diagnostics sheet.
