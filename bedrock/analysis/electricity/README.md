# Electricity analysis

Two eras, one tree.

- **`current/`** — analyses of **today’s** EIA-anchored generation / transmission /
  distribution production (YAML flags `implement_electricity_disaggregation` /
  `implement_electricity_mixed_units`).
- **`historical/`** — immutable freeze of the **original** UGO / Table 8.3 /
  Table 2.4 electricity-disagg implementation. Do not run historical Python
  against current production.
- **`shared/`** — helpers used by both (eGRID vs national totals; 221100
  reallocation matrix export).

| Path | What it is |
|---|---|
| `current/diagnostics/` | BLy / EF / full-trace / year-alignment / F01000 BLy attribution |
| `current/eia_gtd/` | Stub for results-deck G/T/D tables (`EIAPurchaserAllocation`) |
| `current/vs_original_elec_disagg/` | Live EIA path vs the original-implementation freeze |
| `current/vs_pre_mecs_industrial_weights/` | Live MECS Industrial weights vs dollar-weight freeze |
| `current/eia_gtd_code_impl_f8f73b01.plan.md` | Design note for current production (internal names are stale) |
| `historical/original_elec_disagg_implementation/` | Read-only freeze (`q`, `x`, E/D/N/BLy, …) |
| `historical/pre_mecs_industrial_weights/` | EIA G/T/D mixed units with dollar manufacturing weights |

Do not invent a snapshotter for the freeze. If freeze files are missing, they
are missing or not tracked.
