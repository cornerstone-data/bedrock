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
# 1) Warm FBSs for the current git hash (assemble only; jobs must be 1).
#    Do not commit between warm and measure/grade — cache is hash-keyed.
python -m bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin \
    warm --years 2017-2024 --rebase-eia both --jobs 1

# 2) Measure / grade — pass --jobs 8 explicitly for Acceptance (default is 1).
#    Fallback --jobs 4 if memory-constrained.
python -m bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin \
    measure|grade [--years 2017-2024] [--csv] [--check] \
    [--rebase-eia both] [--baseline-vintage f709829] [--require-rebase-on] \
    [--jobs 8]
```

Confirm `bedrock/transform/output_data/*_{GIT_HASH}.parquet` covers 2017–2024
before a dual-arm Acceptance grade. Mid-run commits invalidate later years
(pathological multi-hour `assemble`); logs are hygiene only, not a speed lever.

- **warm** — `assemble` only under each rebase arm (no GRAS). Rejects `--jobs > 1`.
- **measure** — ranks every `F01000` commodity seed → `balance_year(..., 'none')`
  Δ (Step-5 isolation). Side columns `mut_usd` / `delta_vs_mut` from in-memory
  `mut_from_balanced` (not a ranking gate).
- **grade** — runs `tier1_fixed` / `row_side_target` / `eia_band` vs `'none'`
  baseline on T11, shipped MUT EIA YoY (reported `$5bn/15%` band), continuous
  weighted EIA miss, intermediate bands + other `F01000` sinks. Winner rule below.
- **`--rebase-eia`** — dual-arm with #1009. When
  `rebase_utility_gross_output_on_eia` is absent, the True arm soft-skips
  (`__arm_skip__` summary). Ship `selected` is always on the rebase-off arm.

`--check` (grade only): schema, 0 or 1 `selected` on False-arm ok rows, never
`selected` on True arm, nonzero electricity PCE seed. Full-span Acceptance run
passed with `check: 0 failure(s)` (first-pass; see Winner rule).

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

**Authoritative (Phase 11):** among False-arm eligible candidates
(`t11_all_years_ok`, `allow_select`):

1. Minimize `eia_weighted_abs_miss` =
   Σ wᵧ·|YoYᵧ − EIAᵧ| / Σ wᵧ with wᵧ = |Step-5 Δ| of `221100×F01000` on
   mode `'none'` at span-end year **y** (USD; same helpers as measure).
2. Tie-break: smaller \|Δ\| onto `trade` + `trade_unseeded` bands.
3. Tie-break: `tier1_fixed`.
4. None eligible → leave flag False / mode `'none'` (Acceptance-valid).

The `$5bn / 15%` `_published_band_ok` gate remains a **reported** column
(`eia_all_spans_ok`); it does **not** filter the selection pool.

**Old rule (Phase 6 / first-pass 9b — non-shipping):** T11 → prefer binary
`eia_all_spans_ok` → trade \|Δ\| → `tier1_fixed`. That collapsed gate picked
`eia_band` on ~$63m trade noise after all candidates failed the band; held per
[Wes review](https://github.com/cornerstone-data/bedrock/pull/1011#pullrequestreview-5324073406).
Matrices below that say “Ship-intent: `eia_band`” are **provisional under the
old rule** until a post–Phase-10+11 warm re-grade.

Wes prefers a narrow hold or soft target over re-deriving upstream Y. Smoke
(2022→23) selected B over A on displacement while C lost that span’s EIA —
consistent with that preference on the short window.

`selected` only when `rebase_eia=False` and `arm_status=='ok'`. True arm is
advisory (still computes `eia_weighted_abs_miss`; never `eligible`/`selected`).

## Measure findings

### Smoke 2022–2023

| year | `221100` rank | seed $bn | balanced $bn | Step-5 Δ $bn |
|---|---:|---:|---:|---:|
| 2022 | **12** | 232.24 | 219.90 | **−12.34** |
| 2023 | **25** | 236.80 | 242.03 | **+5.23** |

### Full span 2017–2024 (`baseline_vintage=f709829`; both arms)

From `pce_electricity_pin_measure_2017_2024.csv` after stacking on #1010:

| year | rank (off) | Step-5 Δ $bn (off) | rank (on) | Step-5 Δ $bn (on) |
|---|---:|---:|---:|---:|
| 2017 | 41 | +0.14 | 41 | +0.14 |
| 2018 | 27 | +0.64 | 56 | −0.23 |
| 2019 | 30 | +0.80 | 37 | −0.60 |
| 2020 | 48 | +0.73 | 91 | +0.31 |
| 2021 | 33 | −1.84 | 16 | −4.95 |
| 2022 | **12** | **−12.34** | **10** | **−15.44** |
| 2023 | 25 | +5.23 | 29 | +4.53 |
| 2024 | 23 | +6.19 | 20 | +6.95 |

Largest Step-5 electricity PCE move remains 2022; rebase-on increases that
swing (−$15.4bn vs −$12.3bn). Documentary vintage label only — not a GCS checkout.

## Grade matrix

### Smoke 2022→2023 (provisional)

| Candidate | T11 | EIA band | Trade \|Δ\| bn | Selected |
|---|---|---|---:|---|
| `tier1_fixed` | ok | **ok** (+$4.56bn vs EIA +$5.00bn) | 0.830 | no |
| `row_side_target` | ok | **ok** (+$4.56bn vs EIA +$5.00bn) | **0.072** | **yes** |
| `eia_band` | ok | **fail** (+$12.50bn vs +$5.00bn) | 0.028 | no |

**Smoke provisional winner: `row_side_target`.**

### Full span 2017–2024 — dual-arm Acceptance (Phase 9b)

CSVs: `pce_electricity_pin_{t11,eia,displacement,pce_sink,summary}_2017_2024.csv`
(both `rebase_eia` values). `--check --require-rebase-on` → **0 failures**.

**False arm (ship selector):**

| Candidate | T11 | EIA all spans | Trade \|Δ\| bn | Selected |
|---|---|---|---:|---|
| `tier1_fixed` | ok | fail | 1.783 | no |
| `row_side_target` | ok | fail | 0.144 | no |
| `eia_band` | ok | fail | **0.081** | **yes** |

**True arm (advisory; all `eligible=selected=False`):**

| Candidate | T11 | EIA all spans | Trade \|Δ\| bn |
|---|---|---|---:|
| `tier1_fixed` | ok | fail | 3.216 |
| `row_side_target` | ok | fail | 0.230 |
| `eia_band` | ok | fail | **0.034** |

**Ship-intent (old rule / non-shipping):** `eia_band`. Same False-arm winner as
pre-#1010 Phase-6 under the collapsed binary gate. True-arm trade ranking also
preferred C. Keep Candidate C code (9c keep rule). No candidate has
`eia_all_spans_ok` on either arm. **Authoritative ship-intent awaits Phase 11
selector + warm dual-arm re-grade (9b).**

**Primary PCE sinks (False-arm old-rule winner):** hospitals (`622000`), tenant housing
(`531HST`), pharma (`325412`), limited-service restaurants (`722211`), petroleum
(`324110`).

## Production posture

- `constrain_electricity_pce_cell` defaults **False** (not enabled on this PR).
- Do **not** treat first-pass / Phase-6 `eia_band` as the mode to flip. After
  Phase 11 + authoritative 9b, record the False-arm winner in About; set flag +
  mode together only at explicit ship (atomic YAML) — do not leave flag on with
  mode `'none'`.
- Never co-enable with #1009/#1010 rebase in one unattributed rebuild.
- Do **not** flip `DEFAULT_PCE_CONSTRAINT`.

## Out of scope

Deleting Candidate C; #899/#1005; #902; re-deriving `derive_initial_Y_pur`.
