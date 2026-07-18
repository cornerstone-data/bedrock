
# About Reconciling Data Years

The study is for a conference paper and presentation at IIOA 2026 conference.

Four 4 models are defined and for each matrices are prepared over the time period.

## Model config parameters used in the analysis

### Model 1


Here are the config fields actually consumed in the model 1 pipeline:

| Config field | Where used | Via |
|---|---|---|
| `usa_ghg_data_year` | `derive_cornerstone_x_after_redefinition` | selects the gross output year |
| `iot_before_or_after_redefinition` | `derive_cornerstone_x_after_redefinition` | before/after redefinition source |
| `implement_waste_disaggregation` | `derive_cornerstone_x_after_redefinition`, `derive_cornerstone_Aq` | `get_waste_disagg_weights()` |
| `eeio_waste_disaggregation` | `derive_cornerstone_x_after_redefinition`, `derive_cornerstone_Aq` | `get_waste_disagg_weights()` |
| `usa_base_io_data_year` | `derive_cornerstone_Vnorm_scrap_corrected` | inflation conditional |
| `update_inflation_factors` | `derive_cornerstone_Vnorm_scrap_corrected`, `deflate_ef` | `get_cornerstone_industry_price_ratio()` |
| `apply_inflation_to_V` | `deflate_ef` → `get_vnorm_adjusted_commodity_price_ratio` | inflation gate |
| `model_base_year` | `deflate_ef` → `get_vnorm_adjusted_commodity_price_ratio` | inflation base year |

### Notes

- **`usa_ghg_data_year`** is used by `derive_cornerstone_x_after_redefinition` to pick the gross output year. For model1, `_set_config` does not set this field (only non-model1 branches do), so model1's x vector uses the yaml default rather than the loop year. This may or may not be intentional.
- **`model_base_year`** is set to the loop year and flows into `deflate_ef` via `get_vnorm_adjusted_commodity_price_ratio`, which is the correct inflation source year.
