# About Bedrock v0.2: The Cornerstone U.S. Model

## What v0.2 means

Bedrock v0.2 is the first release in which **USEEIO and CEDA-US are merged** into a single, configurable U.S. national EEIO (Environmentally-Extended Input-Output) model. USEEIO is a US specific legacy EEIO model originally developed by the US EPA. CEDA-US (Comprehensive Environmental Data Archive) is the US portion of Watershed's proprietary Multi-Regional EEIO (MRIO) model. v0.2 brings in methodological contributions and improvements from both which includes a reworked sector schema, an updated approach to reconcile data years between the E matrix (environmental extensions) and B matrix (per-dollar emissions intensities), and a merged attribution model for per-sector greenhouse-gas (GHG) emissions.

v0.2 is a **waypoint**, not the end state. Cornerstone's global MRIO model, targeted for release in **October 2026**, will combine the best of USEEIO and CEDA. v0.2 produces emission factors (EFs) in the unit of dollar of output in **USD 2023, producer prices** (set by `model_base_year: 2023` and `price_type: producer` in `USAConfig`). Users should deflate/inflate or convert to purchaser prices outside `bedrock` if a different basis is needed.

The config-driven patterns v0.2 establishes, including typed methodology flags, YAML-as-experiment, and snapshot-validated output, will carry forward into the development of Cornerstone's 2026 MRIO release, so effort invested in v0.2 is long-term and reusable.

> *Companion documents:* the **[Methodology for Cornerstone U.S. Model](https://github.com/cornerstone-data/papers/blob/v0.2model/us-methods/us-methods.md)** covers the *what* (equations, data sources, methodological choices adopted from USEEIO vs CEDA). The **Cornerstone Technical Architecture Vision** covers the *why* (an Extract-Transform-Load (ETL) monorepo architecture, global MRIO end state). This document — `bedrock` as the *how* — walks the v0.2 config surface (§1), release notes (§2), and diagnostic shifts against v0.1 and USEEIO (§3).

---

## 1. Methodology choices captured in configuration

The headline claim of v0.2 is that **every methodological decision involved in merging USEEIO and CEDA-US is validated using a configuration specifically `USAConfig`** (`bedrock/utils/config/usa_config.py`). Changes to the configuration adjusts the methodology so individual changes can be validated via snapshots.

Configurations are organized into five themes:

| Group | Representative flags | Paper section |
|---|---|---|
| Schema / taxonomy | `use_cornerstone_2026_model_schema`, `implement_waste_disaggregation`, `eeio_waste_disaggregation` | Waste-sector 1→7 disaggregation |
| Economic IOT (input-output tables) | `use_E_data_year_for_x_in_B`, `iot_before_or_after_redefinition` | Preparing the GHG Data for use with the IOT |
| GHG — sector methods | `update_transportation_ghg_method`, `update_electricity_ghg_method`, `update_ghg_coa_allocation`, `update_ghg_attribution_method_for_ng_and_petrol_systems`, `update_enteric_fermentation_and_manure_management_ghg_method`, `update_liming_and_fertilizer_ghg_method` | Per-sector GHG attribution |
| GHG — gas coverage | `update_flowsa_refrigerant_method`, `update_other_gases_ghg_method`, `add_new_ghg_activities`, `new_ghg_method` | GHG inventory construction |
| Data vintage | `model_base_year`, `usa_base_io_data_year`, `usa_ghg_data_year`, `ipcc_ar_version` (IPCC Assessment Report version, e.g. AR5 / AR6) | Data sources |

### 1.1. Configuration files
`bedrock/utils/config/configs/` ships two kinds of YAML:
- A single **full-model** config (`2025_usa_cornerstone_full_model.yaml`) — the v0.2 recommended configuration, now the default in `get_usa_config()`. It turns on the full set of improvements that land with v0.2.
- [pending codebase cleanup] **Per-flag ablation configs** (e.g. `..._ghg_electricity.yaml`, `..._a_price_index.yaml`, `..._taxonomy_and_B_transformation.yaml`) — each isolates a single change against the v0.1 baseline so the marginal impact of that methodological choice can be measured.

A separate `snapshot_version_or_git_sha` field enumerates the v0.1 baseline (`1bda811…`) and the v0.2 SHAs (`2ebb51f…`, `9fe22d9…`), so diagnostic runs can compare current output against any released baseline.

### 1.2. Validation and reproducibility
Pandera schemas guard DataFrames at module boundaries; snapshot tests under `bedrock/transform/__tests__/` (marker `eeio_integration`) cover every config in `configs/`, and continuous integration (CI) blocks accidental drift. A `uv` lockfile pins Python and dependencies, and Google Cloud Platform (GCP)-backed artifact storage carries the resolved `USAConfig.to_dict()` with every output — the matrices are self-describing with respect to the methodology that produced them.

## 2. Releases: v0 → v0.2

- **v0** - baseline
- **v0.1** — CEDA v7 schema, legacy GHG methodology largely inherited from CEDA-US, methodology flags mostly absent or defaulted off. The first reproducible reference point.
- **v0.2** — default config flipped to `2025_usa_cornerstone_full_model.yaml`. It brings:
  - Cornerstone 2026 schema (405 sectors, compared to 411 in USEEIO and 400 in CEDA-US) on by default.
  - Waste-sector disaggregation (1 → 7) with explicit weights.
  - A new methodology reconciling data years between E and B matrices, an upgrade from USEEIO and CEDA-US.
  - Overhauled GHG methods for transportation, electricity, agricultural soils, petroleum & natural-gas systems, refrigerants, and "other" gases — each individually gated and individually snapshot-tested.
  - Per-config diagnostic comparison against the v0.1 baseline via `snapshot_version_or_git_sha`.

The internal-review draft of the methods paper corresponds to the v0.2 configuration; readers who open the paper alongside `2025_usa_cornerstone_full_model.yaml` will see the full picture, subject to the paper/code coverage caveats above.

## 3. Diagnostic results: v0.2 vs USEEIO and CEDA-US benchmarks

v0.2 is compared against two external benchmarks in parallel:
- **USEEIO** — USEEIOv2.6.0-phoebe-23 on [Zenodo](https://zenodo.org/records/17457336).
- **CEDA-US** — US portion of the multi-regional CEDA model.

### 3.1. Headline numbers
| Metric | vs USEEIO | vs CEDA-US |
|---|---|---|
| Median EFs percentage change | *TBD* | *TBD* |
| Median EFs absolute change | *TBD* | *TBD* |
| Number of sectors shifting > *20%* in EFs | *TBD* | *TBD* |
| Top 3 flags by impact on B-matrix entries | *TBD* | *TBD* |

### 3.2. Figures (each rendered once per benchmark, as side-by-side panels)

1. **Histograms** — (a) emission-factor (EF) %-change, (b) EF absolute change.
2. **x–y plots** — (a) EF size vs EF %-change, (b) EF absolute change vs EF %-change.
3. **Stacked-bar / waterfall chart** — attribution of the delta by flag group.

