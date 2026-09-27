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

**Wes coord** ([issue](https://github.com/cornerstone-data/bedrock/issues/1008#issuecomment-5835477936);
[design review](https://github.com/cornerstone-data/bedrock/pull/1011#pullrequestreview-5322512131);
[selection review](https://github.com/cornerstone-data/bedrock/pull/1011#pullrequestreview-5324073406);
[runtime](https://github.com/cornerstone-data/bedrock/pull/1011#issuecomment-5845859763)):
dual-arm `rebase_utility_gross_output_on_eia`; baseline narrative **`f709829`**;
flag-gate default off; name other-`F01000` sinks; weighted EIA selector; warm
then year-parallel before Acceptance.

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

`--check` (grade only): schema, exactly 1 `selected` on False-arm when
`--require-rebase-on --rebase-eia both`, never `selected` on True arm, nonzero
electricity PCE seed. Authoritative full-span run (hash `314bc2d4`):
`check: 0 failure(s)`.

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

Two selection rules appear in this write-up:

- **Initial rule** — binary EIA / trade sequence: among T11-eligible candidates,
  prefer those with `eia_all_spans_ok`, then smaller trade \|Δ\|
  (`trade` + `trade_unseeded`), then `tier1_fixed`. Once every candidate failed
  the `$5bn/15%` band, ranking collapsed to trade \|Δ\| and picked `eia_band` on
  ~$63m of noise. See [Wes selection
  review](https://github.com/cornerstone-data/bedrock/pull/1011#pullrequestreview-5324073406).
- **Updated rule** — weighted EIA miss (ship selector on the False arm): among
  eligible candidates (`t11_all_years_ok`, `allow_select`):
  1. Minimize `eia_weighted_abs_miss` =
     Σ wᵧ·|YoYᵧ − EIAᵧ| / Σ wᵧ with wᵧ = |Step-5 Δ| of `221100×F01000` on
     mode `'none'` at span-end year **y** (USD; same helpers as measure).
  2. Tie-break: smaller \|Δ\| onto `trade` + `trade_unseeded` bands.
  3. Tie-break: `tier1_fixed`.
  4. None eligible → leave flag False / mode `'none'` (Acceptance-valid).

  The `$5bn / 15%` `_published_band_ok` gate remains a **reported** column
  (`eia_all_spans_ok`); it does **not** filter the selection pool.

Ship-intent below uses the **updated rule** on the warm dual-arm re-grade. The
initial rule is retained only as historical contrast.

Wes prefers a narrow hold or soft target over re-deriving upstream Y. Smoke
(2022→23) still selects B under the updated rule (C’s single-span miss
dominates that window). Full-span updated-rule selection picks **C** — see
Acceptance decision.

`selected` only when `rebase_eia=False` and `arm_status=='ok'`. True arm is
advisory (still computes `eia_weighted_abs_miss`; never `eligible`/`selected`).

## Measure findings

### Smoke 2022–2023

| year | `221100` rank | seed $bn | balanced $bn | Step-5 Δ $bn |
|---|---:|---:|---:|---:|
| 2022 | **12** | 232.24 | 219.90 | **−12.34** |
| 2023 | **25** | 236.80 | 242.03 | **+5.23** |

### Full span 2017–2024 (`baseline_vintage=f709829`; both arms)

From `pce_electricity_pin_measure_2017_2024.csv` after authoritative warm grade
on hash `314bc2d4` (stacked on #1010):

| year | rank (off) | Step-5 Δ $bn (off) | rank (on) | Step-5 Δ $bn (on) |
|---|---:|---:|---:|---:|
| 2017 | 41 | +0.14 | 41 | +0.14 |
| 2018 | 27 | +0.64 | 29 | +0.50 |
| 2019 | 30 | +0.80 | 29 | +0.92 |
| 2020 | 48 | +0.73 | 45 | +0.74 |
| 2021 | 33 | −1.84 | 36 | −1.66 |
| 2022 | **12** | **−12.34** | **17** | **−9.10** |
| 2023 | 25 | +5.23 | 26 | +5.20 |
| 2024 | 23 | +6.19 | 22 | +5.68 |

Largest Step-5 electricity PCE move remains **2022** on both arms. Rebase-on
**shrinks** that swing here (−$9.1bn vs −$12.3bn off) — earlier first-pass
docs that showed a larger on-arm 2022 swing are superseded by this hash’s
measure CSV. Documentary vintage label only — not a GCS checkout.

## Grade matrix

### Smoke 2022→2023 (updated rule; timing probe)

| Candidate | T11 | EIA band | Weighted miss $bn | Trade \|Δ\| bn | Selected |
|---|---|---|---:|---:|---|
| `tier1_fixed` | ok | **ok** | 0.443 | (higher) | no |
| `row_side_target` | ok | **ok** | **0.443** | (lower) | **yes** |
| `eia_band` | ok | **fail** | 7.494 | (lowest) | no |

**Smoke winner under the updated rule: `row_side_target`.** A/B tie on
weighted miss; trade \|Δ\| picks B. C loses this one-span window hard
(+$12.5bn YoY vs EIA +$5.0bn).

### Full span 2017–2024 — dual-arm Acceptance

**What was run** (hash `314bc2d4`; **no commits** from warm through grade):

```bash
python -m ...pce_electricity_pin warm --years 2017-2024 --rebase-eia both --jobs 1 --require-rebase-on
python -m ...pce_electricity_pin measure --years 2017-2024 --csv --rebase-eia both \
  --baseline-vintage f709829 --require-rebase-on --jobs 8
python -m ...pce_electricity_pin grade --years 2017-2024 --csv --check --rebase-eia both \
  --baseline-vintage f709829 --require-rebase-on --jobs 8
```

| Step | Wall |
|---|---:|
| Warm (16 year×arm assembles, jobs=1) | **~1 h 11 min** |
| Measure (both arms, jobs=8) | **~33 min** |
| Grade (both arms, jobs=8) | **~1 h 0 min** |
| **Total** | **~2 h 44 min** |

CSVs: `pce_electricity_pin_{measure,t11,eia,displacement,pce_sink,summary}_2017_2024.csv`.
`--check --require-rebase-on` → **0 failures**. Both arms `arm_status=ok`
(no `__arm_skip__`).

**False arm (ship selector, updated rule):**

| Candidate | T11 | EIA all spans (reported) | Weighted miss $bn | Trade \|Δ\| bn | Selected |
|---|---|---|---:|---:|---|
| `tier1_fixed` | ok | fail | 4.412 | 1.783 | no |
| `row_side_target` | ok | fail | 4.412 | 0.144 | no |
| `eia_band` | ok | fail | **2.184** | **0.081** | **yes** |

**True arm (advisory; all `eligible=selected=False`):**

| Candidate | T11 | EIA all spans (reported) | Weighted miss $bn | Trade \|Δ\| bn |
|---|---|---|---:|---:|
| `tier1_fixed` | ok | fail | 4.056 | 1.622 |
| `row_side_target` | ok | fail | 4.056 | 0.095 |
| `eia_band` | ok | fail | **2.524** | **0.093** |

**Ship-intent mode string: `eia_band`.** Production flag stays
**False** on this PR. True-arm ranking under the updated rule also prefers C
(advisory only).

**Primary PCE sinks (False-arm winner C):** hospitals (`622000`), tenant housing
(`531HST`), pharma (`325412`), limited-service restaurants (`722211`), petroleum
(`324110`).

## Acceptance decision — why the updated rule selects C (and how reviews are answered)

### Selection review
([#5324073406](https://github.com/cornerstone-data/bedrock/pull/1011#pullrequestreview-5324073406))

Wes rejected shipping `eia_band` on the **initial rule**: once every candidate
failed the binary `$5bn/15%` gate, ranking fell to trade \|Δ\| and C won by
**$63m** — noise on a $30tn+ table. He asked for a continuous EIA score weighted
by `|Step-5 Δ|`, with the band as a **reported** gate only, and held the
mode-string flip until dual-arm + that score existed. He expected **B**, and
wrote he was happy to be wrong if the weighted score still returned C.

**This Acceptance grades under the updated rule and returns C.** Reasons, in
order:

1. **Binary gate still fails everyone** — no candidate has `eia_all_spans_ok`.
   Under the updated rule that no longer decides anything; it is only reported.
2. **Weighted miss (False arm):** A=B **4.412 bn**, C **2.184 bn**. C wins
   without needing the trade tie-break.
3. **Where the mass sits:** span-end weight `w_2022 = $12.34bn` (largest Step-5
   year). On **2021→22** A/B miss **+$7.38bn (~28%)** while C misses only
   **+$1.02bn** (level collar tracks EIA through the crisis year). On
   **2022→23** C misses **+$7.49bn (~150%)** while A/B miss **+$0.44bn** — but
   that span’s weight is only `$5.23bn`. Absolute miss × Step-5 weight therefore
   prefers C: A/B’s crisis-year miss dominates the numerator; C’s relative
   disaster is on a lighter-weight span.
4. **Smoke vs full span:** the 2022→23-only probe selects B (C’s single miss
   is the whole window). Full span includes the heavy 2021→22 weight where C
   is better. Recording both is intentional — same honesty Wes called out for
   smoke vs initial-rule full-span disagreement.
5. **Relative vs absolute:** Wes’s table correctly shows A/B five times better
   on *relative* miss for their failing span. The updated rule uses
   **absolute** `|YoY−EIA|` × `|Step-5 Δ|`. That is what returns C. If a future
   review wants relative miss in the score, that is a separate rule change —
   not how this Acceptance was graded.
6. **2021→22 and #1009:** Wes hypothesized A/B’s 2021→22 miss might shrink once
   the row is rebased. **True-arm measure** still shows a large 2022 Step-5 move
   (−$9.1bn); True-arm scores under the updated rule still rank C best
   (2.52 vs A/B 4.06). Rebase does **not** hand the updated-rule decision to B.
7. **Trade \|Δ\|** still ranks C lowest (0.081 False / 0.093 True) but is
   **not** the decider here — the updated rule’s weighted EIA miss already
   separates C.

**Candidate C disposition:** False-arm ship-intent is `eia_band` **and** True-arm
lowest weighted miss is `eia_band` → **keep** the `eia_band` code path.
Demotion / deletion does not apply on this result.

### Runtime review
([#5845859763](https://github.com/cornerstone-data/bedrock/pull/1011#issuecomment-5845859763))

Wes timed cold `assemble(2023)` at **~9 h** vs warm `assemble(2022)` at **~9 s**,
attributed to **FBS git-hash cache invalidation** from mid-run commits — not
GRAS and not log I/O. Advice: warm every year before grade; do not commit during
a span; then year-parallel; logs are hygiene; N-candidate re-`assemble` (~27 s/yr
warm) is deferrable cleanliness.

**This Acceptance followed that discipline:**

| Wes point | What we did |
|---|---|
| Cost is FBS regen, not logs | `*.log` already gitignored / untracked; not treated as a speed fix |
| Warm all years for current hash | `warm --years 2017-2024 --rebase-eia both --jobs 1` before measure/grade |
| Never commit mid-span | Hash `314bc2d4` held from warm through `--check` |
| Then parallelize years | `measure` / `grade` with **`--jobs 8`** (CLI default remains 1) |
| Reject parallel warm | `warm` errors if `jobs != 1` |
| Arms sequential | False then True; each arm years parallel |
| Seed reuse across candidates | Still deferred (~27 s/yr); not attempted |

Wall times above (~2 h 45 min warm+measure+grade) match the “warm then ~15–20
min/arm × 2 at 8-way” order of magnitude once FBSs are hot; the warm leg still
dominates when many year×arm pairs miss the hash cache.

### Earlier design / sequencing review
([#5322512131](https://github.com/cornerstone-data/bedrock/pull/1011#pullrequestreview-5322512131))

Addressed on the stack before this Acceptance: `sign_flex` on the PCE
redistributor; CI black/mypy sites; PR stacked on `#1010` by **merge** (not
rebase onto `main`); dual-arm grade with rebase off/on; `DEFAULT_PCE_CONSTRAINT`
remains `'none'`; production flag off until explicit ship.

## Production posture

- `constrain_electricity_pce_cell` defaults **False** (not enabled on this PR).
- **Ship-intent mode string is `eia_band`** (False-arm `selected` on this
  Acceptance under the updated rule). USAConfig field default stays `'none'`
  so release YAML stays waterfall-bracket-clean. When explicitly shipping, set
  **both** the flag and `electricity_pce_constraint_mode: eia_band` together
  (atomic YAML) — do not leave flag on with mode `'none'`.
- Never co-enable with #1009/#1010 rebase in one unattributed rebuild.
- Do **not** flip `DEFAULT_PCE_CONSTRAINT`.

## Next steps

1. **PR / issue comments** — post this Acceptance (False/True matrices, weighted
   scores, timings, why C wins under the updated rule) on #1011 / #1008 / #1010.
2. **Reviewer confirm** — especially that absolute×Step-5 weighting (not
   relative miss) is the accepted ship rule, given it returned C contrary to
   the pre-score expectation of B.
3. **Explicit ship (separate edit)** — only after review sign-off: atomic
   `constrain_electricity_pce_cell: true` + `electricity_pce_constraint_mode:
   eia_band` on the intended release config; do not combine with the GO rebase
   flag in one unattributed rebuild.
4. **Optional follow-ups (out of this Acceptance):** in-process seed reuse
   across candidates (~27 s/yr); relative-miss variant of the score if product
   owners want it; delete Candidate C only in a later PR if demotion criteria
   ever apply (they do not now).

## Out of scope

Deleting Candidate C; #899/#1005; #902; re-deriving `derive_initial_Y_pur`;
FBS git-hash redesign; candidate-level assemble seed cache.
