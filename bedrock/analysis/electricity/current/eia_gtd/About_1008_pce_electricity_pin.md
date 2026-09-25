# About #1008 — constrain `221100 × F01000` (PCE electricity pin)

Diagnostic + production wiring for
[issue #1008](https://github.com/cornerstone-data/bedrock/issues/1008)
(parent [#990](https://github.com/cornerstone-data/bedrock/issues/990),
grandparent [#896](https://github.com/cornerstone-data/bedrock/issues/896)).

**Problem.** `F01000` is Tier 2 (column total soft-targeted; commodity split free),
so Step 5 GRAS can move electricity into residential PCE. The #990 close-out
graded pre-balance `derive_initial_Y_pur` (+1.96% YoY 2022→23 vs EIA +2.20%);
shipped product ran +10.06%. Attributed was true of the **target**, not the
**product**.

**Wes coord** ([comment](https://github.com/cornerstone-data/bedrock/issues/1008#issuecomment-5835477936)):
dual-arm `rebase_utility_gross_output_on_eia`; baseline narrative **`f709829`**
(not `4276083`); flag-gate default off; name other-`F01000` sinks.

## Run

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin \
    measure|grade [--years 2017-2024] [--csv] [--check] \
    [--rebase-eia both] [--baseline-vintage f709829] [--require-rebase-on]
```

- **measure** — ranks every `F01000` commodity seed → `balance_year(..., 'none')`
  Δ (Step-5 isolation). Side columns `mut_usd` / `delta_vs_mut` from in-memory
  `mut_from_balanced` (not a ranking gate).
- **grade** — runs `tier1_fixed` / `row_side_target` / `eia_band` vs `'none'`
  baseline on T11, shipped MUT EIA YoY band (`_published_band_ok`), intermediate
  bands + other `F01000` sinks. Winner rule in code / below.
- **`--rebase-eia`** — dual-arm with #1009. When
  `rebase_utility_gross_output_on_eia` is absent, the True arm soft-skips
  (`__arm_skip__` summary). Ship `selected` is always on the rebase-off arm.

`--check` (grade only): schema, 0 or 1 `selected` on False-arm ok rows, never
`selected` on True arm, nonzero electricity PCE seed. Full-span Acceptance run
passed with `check: 0 failure(s)`.

## Production wiring (ship switch)

**Ship switch** = USAConfig `constrain_electricity_pce_cell` (default **False**)
+ `electricity_pce_constraint_mode` (winner string; flag stays False until
explicit ship).

`DEFAULT_PCE_CONSTRAINT` in `nowcast_mask.py` stays **`'none'` forever** — it is
the Python unconstrained sentinel / analysis baseline, **not** the production
ship switch. Do not flip it.

| Surface | Role |
|---|---|
| `fixed_value_mask` / `build_sut_mask` | Candidate A Tier-1 hold |
| `assemble_targets` → `T1008` | Candidate B soft cell target |
| `engine(..., pce_constraint=, pce_eia_band_m=)` | B/C closers after T4, before T18 |
| `assemble` / `balance_year` → `resolve_pce_constraint` | Explicit mode wins; config when both kwargs None |

**Candidate B:** T1008 is closer-only (like T4) — excluded from `_use_vectors`
so it does not double-spend soft T2. Weight = `WEIGHTS['T2']` (0.8).

**Candidate C:** in-loop level collar = EIA residential $bn→$M ± $5bn; grade
metric uses full YoY `_published_band_ok` ($5bn abs or 15% rel). Level collar
≠ YoY band when BEA PCE and EIA levels disagree — that is a valid grade fail,
not a closer bug to “fix” with an in-loop YoY collar.

**Product EIA cell:** in-memory `mut_from_balanced` on that run’s balanced
frames — never disk `build(year)`. Step 7 copies FD unchanged; grade uses
Step-6 MUT Use.

## Published-counterpart survey (Work §1)

Hardcoded lookup (`counterpart_survey`) for v1 — About records the survey;
only electricity has a named series today:

| Commodity | Has annual cell-level PCE counterpart? | Series |
|---|---|---|
| `221100` | **yes** | EIA Form 861 / EPA Table 2.3 residential |
| all others | **no** (hardcoded map) | — |

**Top-10 movers (measure CSV, union 2022 ∪ 2023 by `|Δ|`):** **17 commodities,
0 with annual cell-level PCE counterparts.** Electricity (`221100`) is **not**
in that union (ranks 12 / 25) but **does** have EIA residential via the survey
map. Document these separately — do not collapse into a single `1/N` that mixes
survey map with top-10 union. Fix stays **cell-specific** (do not freeze all of
`F01000`).

## Winner rule

1. Eligible = T11 residual ≤ engine atol ($100M) and `skipped` empty, all years
   (False arm only for ship).
2. Prefer `eia_all_spans_ok` among eligible.
3. Prefer smaller \|Δ\| onto `trade` + `trade_unseeded` bands.
4. Tie-break: `tier1_fixed`.
5. None eligible → leave flag False / mode `'none'` (Acceptance-valid).

Wes prefers a narrow hold or soft target over re-deriving upstream Y. Smoke
(2022→23) selected B over A on displacement while C lost that span’s EIA —
consistent with that preference on the short window. Full-span Acceptance
disagrees (below); mode string follows Phase-6.

`selected` only when `rebase_eia=False` and `arm_status=='ok'`. True arm is
advisory for #1009 coordination.

## Measure findings

### Smoke 2022–2023

| year | `221100` rank | seed $bn | balanced $bn | Step-5 Δ $bn |
|---|---:|---:|---:|---:|
| 2022 | **12** | 232.24 | 219.90 | **−12.34** |
| 2023 | **25** | 236.80 | 242.03 | **+5.23** |

### Full span 2017–2024 (rebase-off; `baseline_vintage=f709829`)

From `pce_electricity_pin_measure_2017_2024.csv`:

| year | `221100` rank | Step-5 Δ $bn |
|---|---:|---:|
| 2017 | 41 | +0.14 |
| 2018 | 27 | +0.64 |
| 2019 | 30 | +0.80 |
| 2020 | 48 | +0.73 |
| 2021 | 33 | −1.84 |
| 2022 | **12** | **−12.34** |
| 2023 | 25 | +5.23 |
| 2024 | 23 | +6.19 |

Largest Step-5 electricity PCE move is 2022 (−$12.3bn). True arm soft-skipped
(`rebase_utility_gross_output_on_eia` absent). Documentary vintage label only —
not a GCS checkout.

## Grade matrix

### Smoke 2022→2023 (provisional)

| Candidate | T11 | EIA band | Trade \|Δ\| bn | Selected |
|---|---|---|---:|---|
| `tier1_fixed` | ok | **ok** (+$4.56bn vs EIA +$5.00bn) | 0.830 | no |
| `row_side_target` | ok | **ok** (+$4.56bn vs EIA +$5.00bn) | **0.072** | **yes** |
| `eia_band` | ok | **fail** (+$12.50bn vs +$5.00bn) | 0.028 | no |

**Smoke provisional winner: `row_side_target`.**

### Full span 2017–2024 (Acceptance) — rebase-off

CSVs: `pce_electricity_pin_{t11,eia,displacement,pce_sink,summary}_2017_2024.csv`.
`--check` → 0 failures.

| Candidate | T11 all years | EIA all spans | Trade \|Δ\| bn | Selected |
|---|---|---|---:|---|
| `tier1_fixed` | ok | **fail** (2021→22: +$33.5bn vs EIA +$26.2bn) | 1.783 | no |
| `row_side_target` | ok | **fail** (same 2021→22 miss) | 0.144 | no |
| `eia_band` | ok | **fail** (2022→23: +$12.5bn vs EIA +$5.0bn) | **0.081** | **yes** |

**Acceptance winner: `eia_band`.** No candidate has `eia_all_spans_ok` on
2017–2024 (A/B miss 2021→22; C misses the smoke span 2022→23). Among T11-eligible,
smaller trade-band displacement selects C. Smoke’s `row_side_target` remains the
correct short-window pick; full-span mode string follows C.

**Primary PCE sinks (winner):** hospitals (`622000`), tenant housing (`531HST`),
pharma (`325412`), limited-service restaurants (`722211`), petroleum (`324110`).

### Dual-arm

| Arm | Status |
|---|---|
| `rebase_eia=False` | Full grade; sole ship selector |
| `rebase_eia=True` | `__arm_skip__` / `skipped_flag_absent` until #1009 lands |

When #1009 lands, re-run `--rebase-eia both`; rebase-on may shrink PCE sinks —
advisory unless `--require-rebase-on`.

## Production posture

- `constrain_electricity_pce_cell` defaults **False** (not enabled on this PR).
- Graded winner string is **`eia_band`** (documented here; USAConfig field
  default stays `'none'` so release YAML stays waterfall-bracket-clean).
  When explicitly shipping, set both the flag **and**
  `electricity_pce_constraint_mode: eia_band` together (atomic YAML or release
  edit) — do not leave flag on with mode `'none'`.
- Never co-enable with #1009 rebase in one unattributed rebuild.
- Do **not** flip `DEFAULT_PCE_CONSTRAINT`.

## Out of scope

#1009 (BEA vs EIA GO), #899/#1005 (trade pin), #902 (G/T/D allocator).
