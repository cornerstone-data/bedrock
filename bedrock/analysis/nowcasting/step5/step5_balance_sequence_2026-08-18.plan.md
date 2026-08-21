---
name: Step 5 balance sequence
overview: "Three stacked PRs for #588 after scaffolding (#659): GRAS kernel, SUT orchestration, then KRAS-style softness. This document is interfaces and order only — not the implementer spec for any PR."
todos:
  - id: pr1-kernel
    content: "PR1 — gras_balance ndarray kernel (spec: gras_kernel_plan_2026-08-18.plan.md)"
    status: pending
  - id: pr2-wrapper
    content: "PR2 — engine(free, residual, masks); spec: sut_orchestration_plan_2026-08-18.plan.md (hard only: T1, T11–T17)"
    status: pending
  - id: pr3-kras
    content: PR3 — KRAS-style soft constraints around the same kernel; write a full plan when this PR is next (needs sourced weights)
    status: pending
isProject: false
---

# Step 5 balance sequence (three PRs)

**Parent issue:** [#588](https://github.com/cornerstone-data/bedrock/issues/588). **Already on `nowcast`:** mask, targets, offset, precheck ([#659](https://github.com/cornerstone-data/bedrock/pull/659)). **Git base for PR1:** `nowcast`. Stack PR2 on PR1, PR3 on PR2.

This file is **boundaries**. Implementer detail lives in a per-PR plan. Do not put GRAS clamp rules or KRAS weight tables here.

| PR | Plan | Ships | Does not ship |
|---|---|---|---|
| **1. Engine** | [`gras_kernel_plan_2026-08-18.plan.md`](gras_kernel_plan_2026-08-18.plan.md) | `gras_balance` / `GrasBalanceResult` in `bedrock/utils/economic/balance/gras.py`. One signed matrix, row/col vectors, `free_mask`, `sign_flex`. Hand-checkable numpy tests. | `engine(free, residual, masks)`; `Target` / `SutMask` in the public function; T11–T17; 2017 SUT; KRAS |
| **2. SUT orchestration** | [`sut_orchestration_plan_2026-08-18.plan.md`](sut_orchestration_plan_2026-08-18.plan.md) | `engine(free, residual, masks)` calling `gras_balance` per block. **Hard only (T1, T11–T17).** Soft/placeholder T2/T4/T6–T9 skipped. T11 on 400 commodities. | T4 aggregators; KRAS; rewriting `gras_balance` to take `TargetSet` |
| **3. KRAS** | write when PR3 is next | Soft sourced targets (weights already on `Target`); identities stay hard. Around the **same** kernel, not a new inner loop. | Replacing GRAS with QP; putting softness inside `gras_balance` |

```text
#659 scaffolding (merged)
    → PR1  gras_balance(Z, row_t, col_t, free_mask, sign_flex)
        → PR2  engine(free, residual, masks)  # calls gras_balance per block
            → PR3  KRAS-style soft layer        # still calls gras_balance
```

[`plan.md`](bedrock/analysis/nowcasting/plan.md) recommendation: GRAS + KRAS-style soft layer, mask via offset. PR1 is GRAS. PR2 is the missing caller. PR3 is the soft layer. Decision 3’s placeholder NIPA/ITA values can land on PR2’s shape; **calibrated weights** are PR3.

## Interfaces PR1 must not break

- **Public kernel stays ndarray-only.** Do not add `gras_balance(free, residual, masks)`. That collapse is why PR2 exists.
- **Participation:** later wrapper passes `mask.free.to_numpy()` (`True` = may move), never `mask.frozen`. `sign_flex=(mask.sign_lock.to_numpy() == 0)` must be passed explicitly (`None` is all-False, stricter than `SutMask`).
- **Output:** `~free_mask` cells are 0 so `restore_fixed` can round-trip. Structural zeros are not caught by `restore_fixed`; the kernel still zeros them via `free_mask`.
- **Raise vs return:** `ValueError` on bad shapes / nonzero target with no free cell; `converged=False` on stall or exhausted iterations. Do not reuse `InfeasibleBalance`.
- **No scipy.** PR3 must not assume a QP solver in the kernel.

## Interfaces PR2 must not break

- Call `gras_balance`; do not fork a second dense loop.
- Keep `engine(free, residual, masks)` as the SUT entry. Cross-block identities (T11–T14, T17) and T4 aggregators live here, not in `gras.py`.
- Pass order (Use vs Supply, how many iterations to joint `T016 = T019`) is a PR2 design, not a PR1 leftover.

## Interfaces PR3 must not break

- Softness is **constraint** policy (who gives way), not a new scale formula. Prefer wrapping or iterating `gras_balance` over editing the GRAS split.
- Hard vs soft already exists on `Target.hard`. PR3 uses it; PR1 does not read `TargetSet`.

## When to write the next full plan

- After PR1 merges: SUT-orchestration plan (pass order, how T11/T17 become kernel calls, 2017 replay with `allow_placeholders=True`).
- After sourced targets are real enough to calibrate: KRAS plan (which weights, how violation is charged, diagnostics). Placeholder `WEIGHTS` in `nowcast_targets.py` are not that calibration.
