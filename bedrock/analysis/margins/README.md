# Margins and Phi analysis

Manual validation scripts for BEA margins, PRO:PUR (Phi), and supply-chain emission
factors. All outputs land in `output/`, which is gitignored
(`bedrock/analysis/**/output/`).

## Scripts

| Script | Purpose | Outputs |
| ------ | ------- | ------- |
| `compare_phi_to_reference.py` | Bedrock Phi vs the pinned USEEIO workbook (IO year + 2024) and the CEDA 2025 workbook (IO year only) | `output/plots/phi_comparison.png`, `output/phi_comparison_useeio_<year>.csv`, `output/phi_comparison_ceda.csv` |
| `compare_margin_approaches.py` | PRO:PUR across useeior / Cornerstone / CEDA margin configs at IO year and 2024 | `output/plots/margin_approach_comparison_<year>.png`, `output/margin_approach_comparison.csv` |
| `compare_sef_margins_sources.py` | Margin (and without-margin) SEF side-by-side vs [Zenodo v1.4.0](https://doi.org/10.5281/zenodo.17202747): `useeio_phoebe_23`, `2025_usa_cornerstone_v0_3`, `2025_usa_cornerstone_v0_6` (2024 purchaser), joined on Reference USEEIO Code. The summary compares each model's margin SEF with Zenodo, v0.3 with phoebe, and v0.6 with v0.3 | `output/sef_margins_comparison.csv`, `output/sef_margins_summary.csv` |

```powershell
uv run python -m bedrock.analysis.margins.compare_phi_to_reference
uv run python -m bedrock.analysis.margins.compare_margin_approaches
uv run python -m bedrock.analysis.margins.compare_sef_margins_sources --dollar_year 2024 --phoebe-sef-csv <pinned phoebe SEF CSV>
```

`compare_sef_margins_sources.py` is self-contained: it owns the Zenodo download and
loaders, the SEF CSV loader, and `publish_sef`.

## Snapshot handoff (`A_margin`)

`A_margin` is a first-class snapshot name (`bedrock.utils.snapshots.names`). It is the
Cornerstone commodity × commodity margin allocation matrix from
`derive_cornerstone_A_margin` (non-zero only on margin-provider rows). Downstream
models that supply their own total requirements factors apply:

`N_margin = N @ A_margin`

(Bedrock's published SEF margins column uses Bedrock `N`; CEDA multiplies by CEDA
`N` after mapping Cornerstone codes.) Phi is a separate snapshot in the same SHA
prefix (`PRO:PUR` purchaser scale). Until the next full snapshot cut includes
`A_margin.parquet` at the top-level SHA, the interim GCS sidecar is documented under
[Interim object sidecars](../../utils/snapshots/README.md#interim-object-sidecars).

## Notes

- `compare_sef_margins_sources.py` needs a pinned phoebe SEF CSV (`--phoebe-sef-csv`),
  because the `useeio_phoebe_23` config is retired. It publishes v0.3 and v0.6 at the
  target dollar year unless `--v0-3-sef-csv` / `--v0-6-sef-csv` point to existing
  exports.
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
