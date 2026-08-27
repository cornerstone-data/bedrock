# Electricity analysis

Two eras, one tree.

- **`current/`** — analyses of **today’s** EIA-anchored generation / transmission /
  distribution production (YAML flags `implement_electricity_disaggregation` /
  `implement_electricity_mixed_units`).
- **`historical/`** — published original vs EIA-anchored (pre-MECS) comparison
  tables and figures (not live production).
- **`shared/`** — helpers used by both (eGRID vs national totals; 221100
  reallocation matrix export).

| Path | What it is |
|---|---|
| `current/diagnostics/` | BLy / EF / full-trace / year-alignment / F01000 BLy attribution / comparison deck |
| `current/eia_gtd/` | Results-deck G/T/D tables (`EIAPurchaserAllocation`) |
| `historical/original_vs_eia_anchored_deck/` | PPTX extract: class MWh, D/N tables, vs-footing histogram PNGs |

Do not regenerate freeze parquets into this tree. Archived dumps live under Dropbox
`Documentation/Archive/`.
