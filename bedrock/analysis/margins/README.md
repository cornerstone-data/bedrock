# Margins and Phi analysis

Manual validation scripts for BEA margins, PRO:PUR (Phi), and supply-chain emission
factors. Outputs land in `output/` (gitignored).

## Tracked scripts

| Script | Purpose |
|--------|---------|
| `compare_phi_to_reference.py` | Bedrock Phi vs pinned USEEIO workbook (IO year + 2024) and CEDA workbook |
| `compare_margin_approaches.py` | PRO:PUR across useeior / Cornerstone / CEDA margin configs at IO year and 2024 |
| `compare_sef_zenodo_useeio_code.py` | Bedrock SEF without-margins vs [Zenodo v1.4.0](https://doi.org/10.5281/zenodo.17202747) on Reference USEEIO Code | `output/sef_zenodo_useeio_code_comparison.csv` |

```powershell
uv run python -m bedrock.analysis.margins.compare_phi_to_reference
uv run python -m bedrock.analysis.margins.compare_margin_approaches
uv run python -m bedrock.analysis.margins.compare_sef_zenodo_useeio_code `
    --config_name useeio_phoebe_23 --dollar_year 2024
```

Run after SEF publish at target dollar year (e.g. `--dollar_year 2024` on
`useeio_phoebe_23`). Uses purchaser Phi with useeior-style Rho margin inflation when
`useeio_margins` is active.

## Optional local scripts (not required in CI)

| Script | Purpose |
|--------|---------|
| `compare_margins_bedrock_vs_useeior.py` | Bedrock `getMarginsTable` parity vs exported `model$Margins` (needs R export) |
| `compare_phi_useeior_filter_parity.py` | Phi sensitivity to documented vs imports-bug margin filters |
| `check_useeior_export_parity.py` | PRO column parity vs useeior CSV |

Keep one-off probes (`_probe_*`, `diagnose_*`, `explore_*`) out of this folder unless
promoted to a repeatable check with a stable output contract.

## Related CI

- `bedrock/publish/__tests__/test_sef_vs_useeio_baseline.py` — Phi@2017 vs phoebe workbook
- `bedrock/utils/economic/__tests__/test_inflation_helpers_cornerstone.py` — Rho ratio helper
