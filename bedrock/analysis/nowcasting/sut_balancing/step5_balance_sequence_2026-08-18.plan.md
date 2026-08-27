---
name: Step 5 balance sequence
overview: "Three stacked PRs for #588 after scaffolding (#659): GRAS kernel, SUT orchestration, then KRAS-style softness. This document is interfaces and order only — not the implementer spec for any PR."
todos:
  - id: pr1-kernel
    content: "PR1 — gras_balance ndarray kernel (spec: gras_kernel_plan_2026-08-18.plan.md)"
    status: pending
  - id: pr2-wrapper
    content: "PR2 — engine(free, residual, masks); spec: sut_orchestration_03e8183c.plan.md (hard only: T1, T11–T17)"
    status: pending
  - id: pr3-kras
    content: PR3 — KRAS-style soft layer; spec: kras_soft_layer_c61e0083.plan.md (mechanism; WEIGHTS uncalibrated)
    status: pending
isProject: false
---

# Step 5 balance sequence (three PRs)

**Parent issue:** [#588](https://github.com/cornerstone-data/bedrock/issues/588). **Already on `nowcast`:** mask, targets, offset, precheck ([#659](https://github.com/cornerstone-data/bedrock/pull/659)). **Git base for PR1:** `nowcast`. Stack PR2 on PR1, PR3 on PR2.

This file is **boundaries**. Implementer detail lives in a per-PR plan. Do not put GRAS clamp rules or KRAS weight tables here.

| PR | Plan | Ships | Does not ship |
|---|---|---|---|
| **1. Engine** | [`gras_kernel_plan_2026-08-18.plan.md`](gras_kernel_plan_2026-08-18.md) | `gras_balance` / `GrasBalanceResult` in `bedrock/utils/economic/balance/gras.py`. One signed matrix, row/col vectors, `free_mask`, `sign_flex`. Hand-checkable numpy tests. | `engine(free, residual, masks)`; `Target` / `SutMask` in the public function; T11–T17; 2017 SUT; KRAS |
| **2. SUT orchestration** | [`sut_orchestration_03e8183c.plan.md`](sut_orchestration_03e8183c.plan.md) | `engine(free, residual, masks)` calling `gras_balance` per block. **Hard only (T1, T11–T17).** Soft/placeholder T2/T4/T6–T9 skipped. T11 on 400 commodities. | T4 aggregators; KRAS; rewriting `gras_balance` to take `TargetSet` |
| **3. KRAS** | [`kras_soft_layer_c61e0083.plan.md`](kras_soft_layer_c61e0083.plan.md) | Soft T2/T4/T7 around the same `engine` / `gras_balance`. T6/T8/T9 whole-name deferred when T12–T14 occupy a slot. Starting `WEIGHTS`, not calibrated. | QP; softness inside `gras.py`; calibrating `WEIGHTS` |

```text
#659 scaffolding (merged)
    → PR1  gras_balance(Z, row_t, col_t, free_mask, sign_flex)
        → PR2  engine(free, residual, masks)  # calls gras_balance per block
            → PR3  KRAS-style soft layer        # still calls gras_balance
                → PR4  initial_sut(year)         # seed it from the nowcast, not 2017
```

✅ **PR4 added 2026-08-27** — [`initial_sut_assembly_2026-08-27.plan.md`](initial_sut_assembly_2026-08-27.plan.md).
PR1-PR3 built the balance and proved it on the **published 2017 panel**; PR4 gives it the
nowcast's own blocks for 2017-2023 and shows the RAS converges on them. ⚠️ **It is blocked on
`F03000` change in inventories for 2018-2023** — `derive_initial_Y_pur` has `if year == 2017`
and every other year is all-zero, which would converge and mean nothing.

[`plan.md`](../plan.md) recommendation: GRAS + KRAS-style soft layer, mask via offset. PR1 is GRAS. PR2 is the caller (**hard T1, T11–T17**). PR3 is the soft **mechanism** (blends + T4 closer). **Calibrated weights are not PR3** — starting `WEIGHTS` in `nowcast_targets.py` ship as-is.

## Interfaces PR1 must not break

- **Public kernel stays ndarray-only.** Do not add `gras_balance(free, residual, masks)`. That collapse is why PR2 exists.
- **Participation:** later wrapper passes `mask.free.to_numpy()` (`True` = may move), never `mask.frozen`. `sign_flex=(mask.sign_lock.to_numpy() == 0)` must be passed explicitly (`None` is all-False, stricter than `SutMask`).
- **Output:** `~free_mask` cells are 0 so `restore_fixed` can round-trip. Structural zeros are not caught by `restore_fixed`; the kernel still zeros them via `free_mask`.
- **Raise vs return:** `ValueError` on bad shapes / nonzero target with no free cell; `converged=False` on stall or exhausted iterations. Do not reuse `InfeasibleBalance`.
- **No scipy.** PR3 must not assume a QP solver in the kernel.

## Interfaces PR2 must not break

- Call `gras_balance`; do not fork a second dense loop.
- Keep `engine(free, residual, masks)` as the SUT entry. Cross-block identities (T11–T14, T17) live here, not in `gras.py`. **Hard only in this PR** (T1, T11–T17). T4 aggregators and other `hard=False` targets are skipped; PR3 imposes them.
- Pass order is locked in the PR2 plan: Use then Supply, outer loop on T11, unconstrained margins hold at current sums.

## Interfaces PR3 must not break

- Softness is **constraint** policy (who gives way), not a new scale formula. Prefer wrapping or iterating `gras_balance` over editing the GRAS split.
- Hard vs soft already exists on `Target.hard`. PR3 uses it; PR1 does not read `TargetSet`.

## When to write the next full plan

- PR3 spec is written: [`kras_soft_layer_c61e0083.plan.md`](kras_soft_layer_c61e0083.plan.md).
- After NIPA/ITA values replace placeholders: calibrate `WEIGHTS` (not a fourth stacked PR until those sources exist).
