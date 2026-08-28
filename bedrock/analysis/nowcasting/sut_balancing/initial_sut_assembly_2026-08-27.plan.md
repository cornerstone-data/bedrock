---
name: Initial SUT assembly and RAS convergence
overview: "PR4 of the Step 5 stack: seed engine() from the nowcast blocks instead of the published 2017 panel, so 2017-2023 initial SUTs exist and the RAS can be shown to converge on them. Convergence only — accuracy evaluation is deliberately out of scope."
todos:
  - id: f03000-timeseries
    content: "PREREQUISITE — Change in inventories F03000 for 2018-2023 (Step 1C). Currently `if year == 2017`; every other year is all-zero"
    status: pending
  - id: assembler
    content: "initial_sut(year) in transform/, returning {'use','supply'} on the balance's labels and sign convention"
    status: pending
  - id: convergence
    content: "Drive engine() for 2017-2023 and record iterations and residuals. NOT an accuracy evaluation"
    status: pending
isProject: false
---

# Initial SUT assembly and RAS convergence (PR4)

**Follows** PR1 `gras_balance` → PR2 `engine` → PR3 KRAS soft layer, all on `nowcast`.
This file is **boundaries and prerequisites**, in the shape of
[`step5_balance_sequence_2026-08-18.plan.md`](step5_balance_sequence_2026-08-18.plan.md).

⚠️ **The deliverable is *built and converging*, not *good*.** Accuracy evaluation
is explicitly a later step. Scoring the result is the most tempting way to spend
this work and the wrong one — and per
[`intermediate_estimation_plan.md`](../intermediate_estimation_plan.md) §BEA has
not used the 2022 Economic Census, the obvious yardstick is not a check anyway.

## The machinery already exists, and only the seed is 2017-locked

`mask_layer_feasibility._engine_hard_residuals(year)` already assembles a real
year, builds real masks and targets, runs `engine()` and restores the frozen
blocks. It is a **replay**: it seeds from `nowcast_mask.published_2017_panel`.

```python
seeds   = {block: published_2017_panel(block) for block in BLOCKS}   # the only 2017 lock
masks   = build_sut_masks(year)      # already year-parameterised
targets = build_target_set(year)     # already year-parameterised
frozen, free = split_fixed_blocks(seeds, masks)
out     = engine(free, offset_targets(targets, frozen), masks)
```

So PR4 is **not** "write a RAS driver". It is "produce the seed that driver
should have been given", plus a run harness.

## Prerequisite: `F03000` change in inventories, 2018-2023

**This blocks the stated 2017-2023 range and nothing else does.** Every other
block already spans it:

| block | covered | source |
|---|---|---|
| Use interior | 2017-2024 | `INTERMEDIATE_YEARS` |
| Use value added | 2017-2024 | `VALUE_ADDED_YEARS` |
| Use FD — exports/imports | 2017-2024 | `TRADE_OVERLAY_YEARS` |
| Supply `TRANS` | **2017-2023** | `TRANSPORT_MARGIN_YEARS` — the ceiling, and it matches the range |
| Supply `TOP`/`SUB` | 2017-2024 | `TOP_YEARS` |
| **Use FD — `F03000`** | **2017 only** | `if year == 2017` in `derive_initial_Y_pur` |

Only `Inventories_2017.yaml` exists, so **2018-2023 carry an all-zero change in
inventories**. That is not a small omission dressed as a zero: `F03000` is the
one final-use column whose total is free rather than controlled, it changes sign
between years, and a zeroed column silently pushes its mass onto whatever the
balance can move.

⚠️ **Do not run the RAS on 2018-2023 before this lands.** It would converge — a
zero column is perfectly balanceable — and the convergence would mean nothing.

Source is settled and mostly extracted (§`F03000` of [`plan.md`](../plan.md),
[`inventories_estimation_plan.md`](../inventories_estimation_plan.md));
`U50705BU1` is in the extract list and unused.
[#529](https://github.com/cornerstone-data/bedrock/issues/529) /
[#530](https://github.com/cornerstone-data/bedrock/issues/530) /
[#531](https://github.com/cornerstone-data/bedrock/issues/531).

## What PR4 ships

**`initial_sut(year) -> dict[str, pd.DataFrame]`**, in `transform/`, not in an
analysis module.

⚠️ **It belongs in `transform/` because it is a build artefact, not a
diagnostic.** The repo does not unit-test analysis scripts, and this feeds next
week's MUT integration and the `nowcast` → `main` PR. Leaving it in
`mask_layer_feasibility` would ship the milestone's central object untested and
then require moving it.

| panel | assembled from |
|---|---|
| Use interior | `derive_initial_U_intermediate(year)` |
| Use VA rows | `derive_initial_value_added(year)` |
| Use FD columns | `derive_initial_Y_pur(year)` |
| Supply output | `Detail_Supply_<year>` FBS |
| Supply bridge | `derive_initial_supply_bridge(year)` |

⚠️ **`published_2017_panel` is the shape specification**, not a fallback. Same
labels, same sign convention, same structurally-empty corners — the value-added
by final-demand corner of the Use panel stays zero. If the assembled panel does
not match its shape, the masks and targets misalign silently rather than raising.

**The 2017 assembly is checkable**: it should reproduce the published panel to
the tolerances each step already records, because at 2017 every carry factor is
1.0. That is the plumbing test, exactly as `reproduction_check` is for Step 3.

## What PR4 does not ship

- **Any accuracy evaluation.** Not scores, not dissimilarity against published
  tables, not a verdict on the estimates.
- Calibrated `WEIGHTS`. Still the starting values from PR3.
- 2024. `TRANS` stops at 2023 and no partial fill is honest — 79.7% of the
  column is truck and pipeline.
- MUT redefinition and the Cornerstone EF/diagnostics wiring. That is the week
  after, ending in the `nowcast` → `main` PR.

## Run harness

A CLI flag on the analysis side, per the repo's convention for diagnostics — no
unit tests for analysis scripts, make the check a flag. It reports per year:
iterations, converged true/false, and max absolute residual per hard target.

⚠️ **`converged=False` is a result, not a failed run.** The point is to find out
which years solve and which do not, and a year that stalls is the finding.
