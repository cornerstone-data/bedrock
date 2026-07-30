# Margins and Phi analysis

Manual validation scripts for BEA margins, PRO:PUR (Phi), and supply-chain emission
factors. All outputs land in `output/`, which is gitignored
(`bedrock/analysis/**/output/`).

## Scripts

| Script | Purpose | Outputs |
| ------ | ------- | ------- |
| `compare_phi_to_reference.py` | Bedrock Phi vs the pinned USEEIO workbook (IO year + 2024) and the CEDA 2025 workbook (IO year only) | `output/plots/phi_comparison.png`, `output/phi_comparison_useeio_<year>.csv`, `output/phi_comparison_ceda.csv` |
| `compare_margin_approaches.py` | PRO:PUR across useeior / Cornerstone / CEDA margin configs at IO year and 2024 | `output/plots/margin_approach_comparison_<year>.png`, `output/margin_approach_comparison.csv` |
| `compare_sef_margins_sources.py` | Margin (and without-margin) SEF side-by-side vs [Zenodo v1.4.0](https://doi.org/10.5281/zenodo.17202747): `useeio_phoebe_23`, `2025_usa_cornerstone_v0_3` (2024 purchaser), joined on Reference USEEIO Code | `output/sef_margins_zenodo_phoebe_v0_3.csv` |

```powershell
uv run python -m bedrock.analysis.margins.compare_phi_to_reference
uv run python -m bedrock.analysis.margins.compare_margin_approaches
uv run python -m bedrock.analysis.margins.compare_sef_margins_sources --dollar_year 2024
```

`compare_sef_margins_sources.py` is self-contained: it owns the Zenodo download and
loaders, the SEF CSV loader, and `publish_sef`.

## Notes

- `compare_sef_margins_sources.py` publishes both configs at the target dollar year
  when no CSV path is passed; pass `--phoebe-sef-csv` / `--v0-3-sef-csv` to reuse an
  existing export instead of republishing.
- The Zenodo workbook is downloaded on first use and cached under
  `bedrock/utils/snapshots/data/zenodo_sef_v1.4.0/`; override with `--zenodo-xlsx`.
- Zenodo rows whose Reference USEEIO Code lists multiple codes are dropped; the rest
  collapse to one value per code (mean across NAICS rows).
- Purchaser Phi uses useeior-style Rho margin inflation when `useeio_margins` is
  active.
- Keep one-off probes (`_probe_*`, `diagnose_*`, `explore_*`) out of this folder
  unless promoted to a repeatable check with a stable output contract.

## Related CI

- `bedrock/publish/__tests__/test_sef_vs_useeio_baseline.py` — Phi@2017 vs phoebe workbook
- `bedrock/utils/economic/__tests__/test_inflation_helpers_cornerstone.py` — Rho ratio helper
