# Bedrock v0.2: The Cornerstone U.S. EEIO Model

## What v0.2 means

Bedrock v0.2 is the first release in which **USEEIO and CEDA-US are merged** into a single, configurable U.S. national EEIO (Environmentally-Extended Input-Output) model. USEEIO is a US specific legacy EEIO model originally developed by the US EPA. CEDA-US (Comprehensive Environmental Data Archive) is the US portion of Watershed's proprietary Multi-Regional EEIO (MRIO) model. v0.2 brings in methodological contributions and improvements from both which includes a reworked sector schema, an updated approach to reconcile data years between the E matrix (environmental extensions) and B matrix (per-dollar emissions intensities), and a merged attribution model for per-sector greenhouse-gas (GHG) emissions.

v0.2 is a **waypoint**, not the end state. Cornerstone's global MRIO model, targeted for release in **October 2026**, will combine the best of USEEIO and CEDA.

v0.2 produces emission factors (EFs) in the unit of dollar of output in **USD 2023, producer prices** (set by `model_base_year: 2023` and `price_type: producer` in `USAConfig`). Downstream consumers should deflate/inflate or convert to purchaser prices outside `bedrock` if a different basis is needed.

v0.2 is stable for use now. The config-driven patterns it establishes — typed methodology flags, YAML-as-experiment, snapshot-validated output — carry forward into the MRIO release, so effort invested in v0.2 is not throwaway.

> *Companion documents:* the **Cornerstone U.S. National Model: Methodology Overview** covers the *what* (equations, data sources, methodological choices adopted from USEEIO vs CEDA). The **Cornerstone Technical Architecture Vision** covers the *why* (an Extract-Transform-Load (ETL) monorepo architecture, global MRIO end state). This document — `bedrock` as the *how* — walks the v0.2 config surface (§1), release notes (§2), diagnostic shifts against v0.1 and USEEIO (§3), and the path toward the October 2026 MRIO release (§4–5).

---

## 1. Methodology as configuration

The headline claim of v0.2 is that **every methodological decision involved in merging USEEIO and CEDA-US is a typed flag on `USAConfig`** (`bedrock/utils/config/usa_config.py`). The methods paper is the narrative; the config is the executable contract. Flipping a flag is a methodological change — tracked in git, testable via snapshots, and recorded as provenance metadata on every output artifact.

Flags group into five themes, each mapping to a section of the methods paper:

| Group | Representative flags | Paper section |
|---|---|---|
| Schema / taxonomy | `use_cornerstone_2026_model_schema`, `implement_waste_disaggregation`, `eeio_waste_disaggregation` | Waste-sector 1→7 disaggregation |
| Economic IOT (input-output tables) | `use_E_data_year_for_x_in_B`, `iot_before_or_after_redefinition` | Preparing the GHG Data for use with the IOT |
| GHG — sector methods | `update_transportation_ghg_method`, `update_electricity_ghg_method`, `update_ghg_coa_allocation`, `update_ghg_attribution_method_for_ng_and_petrol_systems`, `update_enteric_fermentation_and_manure_management_ghg_method`, `update_liming_and_fertilizer_ghg_method` | Per-sector GHG attribution |
| GHG — gas coverage | `update_flowsa_refrigerant_method`, `update_other_gases_ghg_method`, `add_new_ghg_activities`, `new_ghg_method` | GHG inventory construction |
| Data vintage | `model_base_year`, `usa_base_io_data_year`, `usa_ghg_data_year`, `ipcc_ar_version` (IPCC Assessment Report version, e.g. AR5 / AR6) | Data sources |

### 1.1. Config-as-experiment
`bedrock/utils/config/configs/` ships two kinds of YAML:
- A single **full-model** config (`2025_usa_cornerstone_full_model.yaml`) — the v0.2 recommended configuration, now the default in `get_usa_config()`. It turns on the full set of improvements that land with v0.2.
- [pending codebase cleanup] **Per-flag ablation configs** (e.g. `..._ghg_electricity.yaml`, `..._a_price_index.yaml`, `..._taxonomy_and_B_transformation.yaml`) — each isolates a single change against the v0.1 baseline so the marginal impact of that methodological choice can be measured.

A separate `snapshot_version_or_git_sha` field enumerates the v0.1 baseline (`1bda811…`) and the v0.2 SHAs (`2ebb51f…`, `9fe22d9…`), so diagnostic runs can compare current output against any released baseline.

### 1.2. Paper vs. code coverage
The internal-review draft of the methods paper covers the majority but not all v0.2 flags; a handful of GHG-method updates and A-matrix scaling alternatives are currently **ahead of the paper**. Each flag in this document is annotated `(in paper)` or `(ahead of paper; documentation in progress)` so readers can tell peer-reviewed material from code-only material.

### 1.3. Validation and reproducibility
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

## 3. Diagnostic results: v0.2 vs USEEIO benchmark

*[Headline numbers pending from the most recent per-flag ablation runs.]*
- Overall shift in total economy-wide GHG footprint from v0.2 full model, measured against USEEIOv2.6.0-phoebe-23 released on [Zenodo](https://zenodo.org/records/17457336).
- Top three flags by impact — which single methodological change moves the matrix the most.
- Sectors whose emissions intensity shifts by more than a stated threshold.

*[Figures]*:

(1) histogram
  - (1a) emission-factor (EF) %-change
  - (1b) EF absolute change

(2) x–y plots vs USEEIO
  - (2a) EF size (x-axis) vs EF %-change (y-axis)
  - (2b) EF absolute change (x-axis) vs EF %-change (y-axis)

(3) stacked-bar chart showing where the differences are from.

A compact table will show, per flag group, the direction and rough magnitude of effect on B-matrix entries (↑ / ↓ / ≈) with a one-line interpretation, and columns for both the v0.1 and USEEIO baselines. These numbers are internally consistent — generated against pinned snapshot baselines — and should be read as "how the method moved" rather than as a claim of improved accuracy; the latter requires external validation. Readers who want to reproduce an individual comparison can run the corresponding per-flag config YAML.

## 4. How `bedrock` fits into Cornerstone

`bedrock` is the U.S. national implementation of the ETL pipeline set out in the Architecture Vision. v0.2 is the module to complete the USEEIO and CEDA-US merge where the best methodologies from both models are adopted. The config-driven methodology pattern utilized in `bedrock` v0.2 will extend to additional geographies in the global MRIO framework to faciliate traceability of methodological choices at the global level.
