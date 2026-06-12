# Export useeior MultiYearIndustryCPI, MultiYearCommodityCPI, MarketShares, and Rho.
# Run from repo root: Rscript bedrock/analysis/rho/export_useeior_cpi_rho.R
#
# Uses USEEIOv2.3-GHG + WasteDisaggregationDetail2017 as the closest packaged
# analogue to phoebe (2017 detail commodity, waste disagg). The pinned phoebe-23
# workbook Rho is compared separately in compare_rho_paths.py.

suppressPackageStartupMessages({
  library(useeior)
})

out_dir <- file.path("bedrock", "analysis", "rho", "output")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

model_specs <- file.path(
  system.file("extdata", "modelspecs", package = "useeior"),
  "USEEIOv2.3-GHG.yml"
)
disagg_specs <- file.path(
  system.file("extdata", "disaggspecs", package = "useeior"),
  "WasteDisaggregationDetail2017.yml"
)

logging::basicConfig()

model <- buildIOModel(
  modelname = "USEEIOv2.3-GHG",
  configpaths = c(model_specs, disagg_specs)
)

io_year <- as.integer(model$specs$IOYear)

# Market shares (industry x commodity) from IO-year Make table (also stored as V_n)
D <- model$V_n

write.csv(
  model$MultiYearIndustryCPI,
  file.path(out_dir, "useeior_MultiYearIndustryCPI.csv"),
  row.names = TRUE
)
write.csv(
  model$MultiYearCommodityCPI,
  file.path(out_dir, "useeior_MultiYearCommodityCPI.csv"),
  row.names = TRUE
)
write.csv(
  D,
  file.path(out_dir, "useeior_market_shares.csv"),
  row.names = TRUE
)
write.csv(
  model$Rho,
  file.path(out_dir, "useeior_Rho.csv"),
  row.names = TRUE
)

cat("IOYear:", io_year, "\n")
cat("Industry CPI dims:", paste(dim(model$MultiYearIndustryCPI), collapse = "x"), "\n")
cat("Commodity CPI dims:", paste(dim(model$MultiYearCommodityCPI), collapse = "x"), "\n")
cat("Rho dims:", paste(dim(model$Rho), collapse = "x"), "\n")
cat("Wrote CSVs to", normalizePath(out_dir), "\n")
