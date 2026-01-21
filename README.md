# bedrock

## License
See the [LICENSE](/LICENSE) on appropriate use of bedrock and [ATTRIBUTION.md](/ATTRIBUTION.md) for the attribution requirement.

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
