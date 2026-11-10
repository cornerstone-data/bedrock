# Electricity analysis

Two eras, plus two live config ladders under `current/diagnostics/`, plus
Electricity v3 stack analysis under `v3/`.

- **`current/`** — analyses of **today’s** EIA-anchored generation / transmission /
  distribution production (YAML flags `implement_electricity_disaggregation` /
  `implement_electricity_mixed_units` / `implement_electricity_reaggregation`).
- **`historical/`** — published original vs EIA-anchored (pre-MECS) comparison
  tables and figures (not live production).
- **`shared/`** — helpers used by both (eGRID vs national totals; 221100
  reallocation matrix export).
- **`v3/`** — Electricity v3 (generation + delivery margin / `42ELEC`) PR-scoped
  analysis; see `v3/README.md`.

**Live ladders** (pick one, then run an analysis — see `current/diagnostics/`):

| Ladder id | Terminal | Use when |
|---|---|---|
| `bea_v03_mixed_units` (default) | mixed units | BEA v0.3.1 research chain → physical generation |
| `nowcast_2024_reaggregation` | reaggregation | Nowcast-2024 / v0.4 chain → monetary 221100 |

| Path | What it is |
|---|---|
| `current/diagnostics/` | BLy / EF / full-trace / year-alignment / F01000 BLy attribution / comparison deck |
| `current/eia_gtd/` | Results-deck G/T/D tables (`EIAPurchaserAllocation`) |
| `historical/original_vs_eia_anchored_deck/` | PPTX extract: class MWh, D/N tables, vs-footing histogram PNGs |

Do not regenerate freeze parquets into this tree. Archived dumps live under Dropbox
`Documentation/Archive/`.
