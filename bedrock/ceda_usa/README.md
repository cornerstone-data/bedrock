# CEDA USA

CEDA, or the Comprehensive Environmental Data Archive, is a multi-regional
EEIO (Environmentally-Extended Input-Output) model developed by Watershed 
([learn more about CEDA](https://openceda.org/solutions/ceda)).

`ceda-usa` is the Python repository for building the single-region 
CEDA USA EEIO matrices. The EEIO matrices derived within `ceda-usa` 
have the domestic technology assumption (i.e., they do not account for emissions 
from foreign manufacturing of goods or services). The `ceda-usa` outputs are
consumed by [Watershed](https://watershed.com/) to produce the full, 
multi-regional EEIO release of CEDA with an in-house data pipeline at Watershed.

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
    uv run pytest ceda_usa/utils/__tests__/test_gcp.py
    ```

## Test CEDA matrix derivation

```bash
uv run pytest bedrock.transform/__tests__/test_usa.py -m eeio_integration
```

## Outputs

EEIO matrices for each `ceda-usa` release will be uploaded at a
future date for sharing.

## License
Copyright 2025 Watershed, ERG, Stanford. All rights reserved. License terms to be released soon.
