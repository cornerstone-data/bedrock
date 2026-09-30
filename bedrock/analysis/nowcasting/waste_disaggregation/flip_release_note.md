# Flip release note (year-aligned waste weights)

**Decision:** **FLIP** (2026-09-29) — production nowcast uses year-matched waste disaggregation weight shares (`waste_weights_year: match_io`) instead of frozen 2017 bundled CSVs.

**Evidence package (accepted):**

- [`who_buys_search.md`](who_buys_search.md) — who-buys `freeze_confirmed`
- [`waste_n_variance.md`](waste_n_variance.md) — waste-N variance extremes + SAS Table 2/3 suppression recovery
- [`waste_y2y_comparison.md`](waste_y2y_comparison.md) — control vs treatment Y2Y
- [`impact_nowcast_updated_weights_by_year.md`](impact_nowcast_updated_weights_by_year.md) — GCS paired panel on pin below

---

## What changed for users / downstream EFs

- **National / typical-sector total EFs (N)** may shift slightly from waste weights alone (GCS paired medians roughly −0.01% to −0.4%; tail ~0.1–2%). This is **not** a wholesale economy rewrite.
- **Waste-child** EFs can move by tens of percent (occasionally more) in some years — concentrated, explained structure updates (including Census suppression recovery and SAS→AIES industry-mix handoff).
- Direct EFs (D) for most non-waste sectors stay near unchanged in the paired evidence.

## Settled methodology path (Flip does not wait on these)

| Item | Production Flip path |
|------|----------------------|
| GCS MUT vintage under Flip evidence | **`v0.3.0_92b7a8a`** |
| Waste×waste Use intersection | **`rcra_path=br_bypass`** (Biennial Report shipper→receiver) **until follow-on BR→FBS work** retires the bypass with bedrock BR→FBS |
| RCRA vintage on treatment | **≥2017** BR (not workbook 2012 embedded in 2017 CSVs) |
| Who-buys / FD for model years 2018–2021 | **Bare Economic Census 2017** (`freeze_confirmed`) — no intercensal 6-digit substitute |
| Who-buys for 2022+ | Economic Census **2022** `ecnclcust` |
| Industry mix ≤2022 | SAS Table 3 expenses **with equal-residual suppression recovery** under NAICS `562` |
| Industry mix 2023–2024 | AIES `EXPS_TOT_DVAL` (EXP01 / BASIC) |
| Commodity / Use-row ≤2022 (carry 2022 for 2023–2024) | SAS Table 2 revenue **with the same suppression recovery** |

Follow-on BR→FBS work, §11 hardening, and who-buys SAS-scale of 2017 EC are **not** Flip prerequisites.

## Configs flipped

Canonical nowcast YAMLs now set `waste_weights_year: match_io` (resolves to `usa_base_io_data_year`):

- `2025_usa_cornerstone_v0_4.yaml` (canonical snapshot config)
- `2025_usa_cornerstone_v0_4_nowcast_{2017–2024}.yaml` (+ 2024 electricity diagnostic variants that enable waste disaggregation)
- `2025_usa_cornerstone_v0_5.yaml` and `2025_usa_cornerstone_v0_5_{2017–2024}.yaml`

USAConfig field default remains `2017` for non-nowcast / unset configs. Analysis-only control YAMLs under `waste_disaggregation/configs/` stay on `2017` for A/B comparison.

## Snapshot / waterfall ops (on Flip branch)

**Snapshot (done on branch `jv_waste_disagg_nowcast_flip_decision`):**

- Workflow: [generate_snapshots #36665300634](https://github.com/cornerstone-data/bedrock/actions/runs/36665300634) (`config_name=2025_usa_cornerstone_v0_4`)
- SHA / `.SNAPSHOT_KEY`: `60c8a6b8568b3002a73cdf8569114b2250571e94`
- GCS: `gs://cornerstone-default/snapshots/60c8a6b8568b3002a73cdf8569114b2250571e94/`
- Prior v0.4.0 SHA kept as `releases.v0_4_0_pre_waste_match_io` for comparison

**Diagnostics dispatched against this git-ref** (indexes under `cache/`):

| Sheet / config | Baseline | Folder |
|----------------|----------|--------|
| `2025_usa_cornerstone_v0_4` | ceda-v0 | v0.4 Diagnostics |
| `2025_usa_cornerstone_v0_5` | ceda-v0 | v0.4 Diagnostics |
| `2025_usa_cornerstone_v0_4` | v0.3 | v0.4 Diagnostics |
| `2025_usa_cornerstone_v0_5` | v0.3 | v0.4 Diagnostics |
| `v03_waterfall_ceda_g1b_waste_disagg` | ceda-v0 | v0.3 waterfall |

Note: historical G1b waterfall YAML still defaults to frozen 2017 waste shares (`match_io` would resolve to `usa_base_io_data_year=2017` on that footing). Flip effect is on the nowcast v0.4 / v0.5 cells above.

Run indexes: `cache/ef_run_index_flip.csv`, `cache/ef_run_index_flip_waterfall.csv`.

After merge to `main`, re-run `generate_snapshots` on the **merge commit** if it differs from this branch tip, and point `.SNAPSHOT_KEY` at that final SHA.

## Gate

See [`production_gate.md`](production_gate.md) — status **FLIP**.
