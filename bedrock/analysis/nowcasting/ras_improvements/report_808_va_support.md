# #808 VA-dominated Use column support plateau

Standing report for [#808](https://github.com/cornerstone-data/bedrock/issues/808).
Detail CSVs live under `bedrock/analysis/nowcasting/output/va_support/`
(gitignored). Soft-only census — no soft-vs-hard dual protocol.

---

## 1. What

Soft Step-5 support and residual census for the **named** Use industries in
#808: housing (`531HSO`, `531ORE`), state/local gov enterprises (`GSLGE`,
`GSLGO`, `GSLGH`), federal (`S00600`), and private households (`814000`).

For each year the CLI assembles once, runs
`split → offset → soft engine → restore`, then measures per-industry hard
**T1 / T17 / T18** abs residuals on **pre-offset** targets, closer support
(free `V00300` / `V00100`, free sign-flex non-VA mass), and a baseline
`b1_gate` for `814000`. Does **not** call `balance_year`.

---

## 2. Why

#808: T11 closes, but T1/T17/T18 **plateau** on VA-dominated columns
(support-limited, not iteration-limited). Product choice here: pursue
**fixes** (selective freing + special-case), not Phase-1 accept-all — unless
baseline shows residuals already gone.

### Referenced work

| Item | Role |
|------|------|
| [#808](https://github.com/cornerstone-data/bedrock/issues/808) | This defect |
| [#839](https://github.com/cornerstone-data/bedrock/issues/839) | Hygiene Later-additions; short pointer only after decision |
| [#915](https://github.com/cornerstone-data/bedrock/pull/915) | Post-balance hygiene (merged) |
| [#755](https://github.com/cornerstone-data/bedrock/issues/755) / [#943](https://github.com/cornerstone-data/bedrock/pull/943) | RAS movement diagnosis pattern this census mirrors |
| [#809](https://github.com/cornerstone-data/bedrock/issues/809) | T15 accepted as Phase-1 residual (**working assumption**); **≠** auto-accept #808 |
| [#767](https://github.com/cornerstone-data/bedrock/issues/767) | Motor-vehicle Use rows — **out of scope** |

---

## 3. Metrics

Units: **BEA $M**. Residuals:
`|Target.evaluate(restored) − values|` on pre-offset assemble targets.

| Field | Meaning |
|-------|---------|
| `t1_abs` / `t17_abs` / `t18_abs` | Per-industry abs residual after soft balance |
| `t1_minus_t18_usd_m` | `T1.values − T18.values` at assemble (= implied intermediates / T005 scale) |
| `free_v00300` / `free_v00100` | Mask free flags on that Use column |
| `n_free_signflex_non_va` / `abs_sum_seed_offsets` | Free sign-flex non-VA support; **seed** Σ\|cell\| (proxy for live closer `abs_sum` on Z) |
| `t18_skip_gate` | Generic `_apply_t18_closer` label: `v00300_frozen` \| `abs_sum_zero` \| `none` |
| `b1_gate` | Baseline classifier for `814000`: `allow_b1` \| `skip_immaterial` \| `needs_decision` |
| `b2_candidate_cells` / `b2_cells_opened` | Structural zeros in commodity×`VA_OPEN_SUPPORT_INDUSTRIES` before clear; cells cleared by helper (0 pre-B2) |

**`b1_gate` (first match):** year error / missing → `needs_decision`; not
`free_v00100` → `needs_decision`; `t18_abs ≤ 50` → `skip_immaterial`;
`|t1−t18| ≤ max(50, 0.05·t18)` and `t18 > 50` → `allow_b1`; else
`needs_decision`.

**`allow_b1` economics:** small GO−VAPRO gap ⇒ implement no-offset B1
(`+d` on `V00100×814000`); post-fix requires **`t18_abs ≤ 50` only** — T1
absorbs ~former T18 by design. Auto-B1 only if **2022** is `allow_b1`.

Reproduce::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.va_support_census \
        --years 2018,2021,2022,2023 --force

~15–17 min/year; not CI.

---

## 4. Results

### Baseline (pre-fix)

Soft census on current `main` before B1/B2. Anatomy matches #808: housing/gov
carry T1+T17; `814000` carries T18 alone with `t18_skip_gate=v00300_frozen`.
`t1_minus_t18_usd_m = 0` for `814000` every year (GO≡VAPRO at assemble).
**2022 `b1_gate = allow_b1`** → auto B1 + B2.

| Year | named Σ\|T1\| | named Σ\|T17\| | named Σ\|T18\| | b1_gate (814000) | b2_cand | b2_opened | hygiene |
|------|-------------:|--------------:|--------------:|------------------|--------:|----------:|---------|
| 2018 | 1,855 | 1,814 | 7 | skip_immaterial | 1,419 | 0 | ok |
| 2021 | 7,390 | 7,583 | 26 | skip_immaterial | 1,419 | 0 | ok |
| 2022 | **28,013** | **28,111** | **94** | **allow_b1** | 1,419 | 0 | ok |
| 2023 | 5,382 | 5,583 | 17 | skip_immaterial | 1,419 | 0 | ok |

**814000 detail (baseline)**

| Year | t18_abs | t1_abs | t1−t18 | free_v00100 | free_v00300 | t18_skip_gate | b1_gate |
|------|--------:|-------:|-------:|:-----------:|:-----------:|---------------|---------|
| 2018 | 7 | 7 | 0 | yes | no | v00300_frozen | skip_immaterial |
| 2021 | 26 | 26 | 0 | yes | no | v00300_frozen | skip_immaterial |
| 2022 | **94** | **94** | 0 | yes | no | v00300_frozen | **allow_b1** |
| 2023 | 17 | 17 | 0 | yes | no | v00300_frozen | skip_immaterial |

**Housing / gov T1 (baseline, 2022)** — T18 already hits (`t18_abs=0`, skip gate `none`); residual is T1/T17 support:

| Industry | t1_abs | t17_abs |
|----------|-------:|--------:|
| 531HSO | 7,675 | 7,663 |
| 531ORE | 6,880 | 6,807 |
| GSLGE | 5,037 | 5,065 |
| GSLGH | 1,527 | 1,580 |
| GSLGO | 4,661 | 4,749 |
| S00600 | 2,141 | 2,152 |

### Post-fix (B1 + B2)

Soft census after `_apply_t18_closer` special-case for `814000` and
`clear_va_open_support_structural_zeros` in `build_sut_mask`.

| Year | named Σ\|T1\| | named Σ\|T17\| | named Σ\|T18\| | b2_cand / opened | hygiene | t11 |
|------|-------------:|--------------:|--------------:|------------------:|---------|-----|
| 2018 | 1,848 | 1,807 | **0** | 1,419 / 1,419 | ok | ~0 |
| 2021 | 7,364 | 7,557 | **0** | 1,419 / 1,419 | ok | ~0 |
| 2022 | 27,920 | 28,017 | **0** | 1,419 / 1,419 | ok | ~0 |
| 2023 | 5,364 | 5,565 | **0** | 1,419 / 1,419 | ok | ~0 |

**2022 before → after (named industries)**

| Industry | t1 base → post | t17 base → post | t18 base → post |
|----------|---------------:|----------------:|----------------:|
| 814000 | 94 → **0** | 94 → **0** | 94 → **0** |
| 531HSO | 7,675 → 7,675 | 7,663 → 7,663 | 0 → 0 |
| 531ORE | 6,880 → 6,880 | 6,807 → 6,807 | 0 → 0 |
| GSLGE | 5,037 → 5,037 | 5,065 → 5,065 | 0 → 0 |
| GSLGH | 1,527 → 1,527 | 1,580 → 1,580 | 0 → 0 |
| GSLGO | 4,661 → 4,661 | 4,749 → 4,749 | 0 → 0 |
| S00600 | 2,141 → 2,141 | 2,152 → 2,152 | 0 → 0 |

**B1 bar:** `814000` `t18_abs ≤ 50` — **PASS** (0). Because assemble
`t1_minus_t18 = 0`, no-offset `+d` on `V00100` also cleared T1/T17 on that
column (column sum moved onto GO).

**B2 bars (frozenset T1/T17 ≥50% drop or ≤$200M each; migration ≤+$500M):**
**FAIL.** Frozenset Σ\|T1\| 27,920 → 27,920 (drop ≈ 0%). Newly freed zeros
were not used by GRAS — residuals bit-identical to baseline within float
noise. Per plan: document and **do not** broaden the frozenset.

---

## 5. Takeaways

1. **Anatomy confirmed on current main.** Housing/gov dominate T1+T17; `814000`
   alone for T18 (`v00300_frozen`). Soft≈hard not tested here (soft-only by
   design). Soft targets are not the lever.
2. **B1 works.** 2022 `allow_b1` → special-case T18 via `V00100` + T4
   accounting freeze. Post-fix `814000` T18 (and T1) are **0** every year in
   the span. No `V00300×814000` writes.
3. **B2 does not move the housing/gov T1 plateau.** Those columns already had
   free sign-flex non-VA support (`t18_skip_gate=none`, large seed offsets).
   Clearing 1,419 extra Tier-0 commodity cells did not reduce T1/T17. Stop —
   do not expand freing.
4. **#808 status (partial).** Households T18 mechanism is fixed. Remaining
   mass (~$28B named T1 in 2022) is the housing/gov support story and needs a
   different product decision (accept Phase-1 residual for those columns, or a
   new mechanism) — not more structural-zero freing.
5. **Hygiene / T11.** Green and exact on all post-fix years.
6. **#809 / #839.** T15 accept assumption unchanged; short #839 pointer in
   [`report_839_hygiene.md`](report_839_hygiene.md) — no new hygiene gate from
   this work.

### Close readiness (A5)

| Predicate | Status |
|-----------|--------|
| B1: `814000` `t18_abs ≤ 50` (2022) | **Met** (0) |
| B2: ≥50% frozenset T1/T17 drop or ≤$200M/col; migration ≤+$500M | **Not met** (~0% T1 drop) |
| T11 exact + hygiene green | **Met** |

**Do not close #808** until a product decision on the housing/gov T1/T17
residual (accept, or a different mechanism). B1 may ship alone.

Reproduce post-fix::

    uv run python -m bedrock.analysis.nowcasting.ras_improvements.va_support_census \
        --years 2018,2021,2022,2023 --force
