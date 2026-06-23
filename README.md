# bedrock

`bedrock` is a pipeline for building environmentally-extended input-output (EEIO) models and related data artifacts, like emission factors.

`bedrock` merges USEEIO and CEDA-US to build the Cornerstone U.S. model, a waypoint toward the Cornerstone global Multi-Regional EEIO model targeted for release in October 2026.
USEEIO is [a US specific legacy EEIO model](https://github.com/USEPA/USEEIO) originally developed by the US EPA.
CEDA-US is the US portion of the [CEDA Multi-Regional EEIO model](https://openceda.org/solutions/ceda), developed by Watershed.
`bedrock` brings in methodological contributions and improvements from both models and establishes a config-driven system that will be carried forward to the development of Cornerstone's global MRIO model outside of `bedrock`.

**Companion documents:**
- **[Methodology for Cornerstone U.S. Model](https://github.com/cornerstone-data/papers/blob/f2372a0d72def7a9961279ab22982adac07e5f67/us-methods/us-methods.md)** covers the equations, data sources, and methodology choices adopted from USEEIO vs CEDA.
- **[Cornerstone Technical Architecture Vision](https://github.com/cornerstone-data/papers/blob/449b7981b537be419dc861b49fac559547b09bae/architecture_vision/Cornerstone_Architecture_Vision.md)** describes the monorepo architecture adopted in `bedrock`.

## Methodology choices captured in configuration

Every methodological decision involved in merging USEEIO and CEDA-US is governed by the configuration system defined in [`USAConfig`](bedrock/utils/config/usa_config.py).
Each flag in the configuration represents a discrete methodology choice, and changes from the baseline can be validated in isolation.

These flags are grouped into themes:

| Theme | Example flags |
|---|---|
| Schema / taxonomy | `use_cornerstone_2026_model_schema`, `implement_waste_disaggregation`, `implement_electricity_reallocation` |
| Economic IOT (input-output tables) | `use_E_data_year_for_x_in_B`, `iot_before_or_after_redefinition` |
| GHG attribution — sector methods | `update_transportation_ghg_method`, `update_electricity_ghg_method` |
| GHG attribution — gas coverage | `update_flowsa_refrigerant_method`, `add_new_ghg_activities` |
| Data vintage | `model_base_year`, `usa_base_io_data_year`, `ipcc_ar_version` |

See [`USAConfig`](bedrock/utils/config/usa_config.py) for the full list.

### Configuration files

All configuration files are in [`bedrock/utils/config/configs/`](bedrock/utils/config/configs/), where:
- A single *full-model* config represents a full set of methodology choices made for a data release. [`2025_usa_cornerstone_v0_3.yaml`](bedrock/utils/config/configs/2025_usa_cornerstone_v0_3.yaml) is the default config in `get_usa_config()`. [`2025_usa_cornerstone_v0_2.yaml`](bedrock/utils/config/configs/2025_usa_cornerstone_v0_2.yaml) is the v0.2 methodology stack for historical comparison.
- Several *atomic configs* each isolate a single methodological change from the baseline so the impact of each choice can be measured independently. For example, [`2025_usa_cornerstone_taxonomy.yaml`](bedrock/utils/config/configs/2025_usa_cornerstone_taxonomy.yaml) is the config for a specific choice to use Cornerstone taxonomy.

A separate `snapshot_version_or_git_sha` field specifies the baseline SHA, so diagnostic runs can compare current output against any released baseline.

### Validation and reproducibility

Snapshot tests in [`bedrock/transform/__tests__/test_usa.py`](bedrock/transform/__tests__/test_usa.py) validate the pipeline's main outputs: `B`, `Adom`, `Aimp`, `ytot`, `ydom`, `yimp`, `y_nab`, `scaled_q`, `exports`.
Each test re-derives one output from scratch and compares it to a reference parquet file in [Google Cloud Storage](https://console.cloud.google.com/storage/browser/cornerstone-default/snapshots?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))), ensuring 100% numerical reproducibility.
The tests run twice every weekday on `main` and alert the Cornerstone team on any failure or snapshot diff, guarding against refactors or upstream data updates that silently change output values in ways no unit test would catch.

Snapshots are stored in GCS, one folder per git SHA, and [`bedrock/utils/snapshots/.SNAPSHOT_KEY`](bedrock/utils/snapshots/.SNAPSHOT_KEY) pins the SHA currently under test.
When methodology changes, snapshots are regenerated and uploaded under the new SHA, and the pin is bumped.
Old snapshots are retained, so every prior release remains independently reproducible.

See [`bedrock/utils/snapshots/README.md`](bedrock/utils/snapshots/README.md) for the team-wide workflow for cutting a new snapshot, bumping the pin, and tagging a release (`v0.X.Y`).

### Diagnostics of model results compared to USEEIO and CEDA-US baselines

A work-in-progress diagnostics report comparing `bedrock` outputs against USEEIO and CEDA-US can be found in [here](https://github.com/cornerstone-data/papers/blob/internal-review-diagnostics-report/diagnostics_report/bedrock_diagnostics.md) in Cornerstone's papers repository.


## License
See the [LICENSE](/LICENSE.txt) on appropriate use of bedrock and [ATTRIBUTION.md](/ATTRIBUTION.md) for the attribution requirement.

## Setup
After cloning the repository, in the root directory:

1. **Install google-cloud-sdk:**
   See [Google Cloud documentation](https://docs.cloud.google.com/sdk/docs/install) for instructions.

2. **Install uv, Python, and dependencies:**
   ```bash
   # Install uv (if not already installed)
   ./scripts/install-uv

   # Install Python dependencies
   ./scripts/install-deps
   ```

   Note: `uv` will automatically use the Python version in `uv.lock` when running `uv sync`. The Python version is installed within a virtual env managed by `uv`.

3. **Authenticate with GCP:**
   ```bash
   ./scripts/google-login
   ```
   This will open a browser. Log in with your Cornerstone email.

4. **Confirm setup successful:**

    ```bash
    uv run pytest bedrock/utils/__tests__/test_gcp.py
    ```

## Test EEIO matrix derivation

```bash
uv run pytest bedrock/transform/__tests__/test_usa.py -m eeio_integration
```

## Outputs

EEIO matrices for each `bedrock` release will be uploaded at a future date for sharing.
