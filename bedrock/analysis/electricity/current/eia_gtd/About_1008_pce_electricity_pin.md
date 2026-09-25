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

## Run

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.pce_electricity_pin \
    measure|grade [--years 2017-2024] [--csv] [--check]
```

- **measure** — ranks every `F01000` commodity seed → `balance_year(..., 'none')`
  Δ (Step-5 isolation). Side columns `mut_usd` / `delta_vs_mut` from in-memory
  `mut_from_balanced` (not a ranking gate).
- **grade** — runs `tier1_fixed` / `row_side_target` / `eia_band` vs `'none'`
  baseline on T11, shipped MUT EIA YoY band (`_published_band_ok`), intermediate
  bands + other `F01000` sinks. Winner rule in code / below.

`--check` (grade only): schema, 0 or 1 `selected`, nonzero electricity PCE seed.

## Production wiring

Shared `PceConstraint` / `DEFAULT_PCE_CONSTRAINT` in `nowcast_mask.py`
(default **`'none'`** until grade picks a winner). Threaded through:

| Surface | Role |
|---|---|
| `fixed_value_mask` / `build_sut_mask` | Candidate A Tier-1 hold |
| `assemble_targets` → `T1008` | Candidate B soft cell target |
| `engine(..., pce_constraint=, pce_eia_band_m=)` | B/C closers after T4, before T18 |
| `assemble` / `balance_year` | Full kwarg path (avoids #1005 threading gap) |

**Candidate B:** T1008 is closer-only (like T4) — excluded from `_use_vectors`
so it does not double-spend soft T2. Weight = `WEIGHTS['T2']` (0.8).

**Candidate C:** in-loop level collar = EIA residential $bn→$M ± $5bn; grade
metric uses full YoY `_published_band_ok` ($5bn abs or 15% rel).

**Product EIA cell:** in-memory `mut_from_balanced` on that run’s balanced
frames — never disk `build(year)`. Step 7 copies FD unchanged; grade uses
Step-6 MUT Use.

## Published-counterpart survey (Work §1)

Union of top movers will be filled from the measure CSV. Hardcoded lookup
today (`counterpart_survey`):

| Commodity | Has annual cell-level PCE counterpart? | Series |
|---|---|---|
| `221100` | **yes** | EIA Form 861 / EPA Table 2.3 residential |
| all others surveyed | **no** | — |

**Count:** `1 / N` with counterparts → fix stays **cell-specific** (do not
freeze all of `F01000` in this PR).

## Winner rule

1. Eligible = T11 residual ≤ engine atol ($100M) and `skipped` empty, all years.
2. Prefer `eia_all_spans_ok` among eligible.
3. Prefer smaller \|Δ\| onto `trade` + `trade_unseeded` bands.
4. Tie-break: `tier1_fixed`.
5. None eligible → keep `DEFAULT_PCE_CONSTRAINT='none'` (Acceptance-valid).

## Measure findings (2022–2023 smoke; expand to 2017–2024 for Acceptance)

From `pce_electricity_pin_measure_2022_2023.csv` (seed → balanced `'none'`):

| year | `221100` rank | seed $bn | balanced $bn | Step-5 Δ $bn |
|---|---:|---:|---:|---:|
| 2022 | **12** | 232.24 | 219.90 | **−12.34** |
| 2023 | **25** | 236.80 | 242.03 | **+5.23** |

Matches the #1008 reopen table. Electricity is **not** the largest F01000
mover in either year (top movers are retail / housing / autos / hospitals /
etc., none with a published cell-level annual PCE counterpart).

**Counterpart survey:** only `221100` has EIA Form 861 residential. Among the
union of top-10 movers 2022–23: **0 / N** have counterparts → fix stays
**cell-specific**.

`mut_usd` ≈ `balanced_usd` for `221100×F01000` (thin margins); product grade
still uses `mut_from_balanced` for Acceptance.

## Grade matrix (2022→2023 smoke)

| Candidate | T11 | EIA band | Trade \|Δ\| bn | Selected |
|---|---|---|---:|---|
| `tier1_fixed` | ok (~0) | **ok** (+$4.56bn vs EIA +$5.00bn) | 0.830 | no |
| `row_side_target` | ok (~0) | **ok** (+$4.56bn vs EIA +$5.00bn) | **0.072** | **yes** |
| `eia_band` | ok (~0) | **fail** (+$12.50bn vs +$5.00bn) | 0.028 | no |

**Winner: `row_side_target`.** Both Tier-1 and row-side track EIA; row-side
moves less mass onto trade bands. `eia_band` loses because collaring the
**level** to EIA residential dollars does not fix **YoY** when BEA PCE and EIA
retail levels disagree — the closer still leaves a +$12.5bn shipped YoY.

**Displacement (winner):** small intermediate-band moves; primary sinks inside
`F01000` are hospitals (`622000`), tenant housing (`531HST`), pharma
(`325412`), petroleum (`324110`), limited-service restaurants (`722211`).

**Production default:** leave `DEFAULT_PCE_CONSTRAINT='none'` until
`grade --years 2017-2024` confirms; crisis-span evidence recommends flipping to
`row_side_target`.

## Out of scope

#1009 (BEA vs EIA GO), #899/#1005 (trade pin), #902 (G/T/D allocator).
