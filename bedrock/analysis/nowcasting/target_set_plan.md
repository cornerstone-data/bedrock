# The Step 5 target set — what constrains the balance, and what builds it

Settles [#591](https://github.com/cornerstone-data/bedrock/issues/591) (Step 5
Decision 3) and specifies the code that produces it. Companion to
[`mask_layer_plan.md`](mask_layer_plan.md) — **the mask and the target set are
one decision seen from two sides**, because a source spent on a cell cannot also
be spent on a margin. Read that document first.

Numbers below are reproduced by
[`mask_layer_feasibility.py --check`](mask_layer_feasibility.py).

---

## 1. What changed since #591 was written

**#591's property 1 does not hold.** It says *"detail gross output for 2018-2024
is nowcast by Step 4a — imposing it on the balance is circular"*, and concludes
the industry constraint can only be imposed at the level GDP-by-industry
publishes. That premise is refuted on two independent grounds:

- **The series is published at detail.** `BEA_Detail_GrossOutput_IO_<year>` is
  already extracted in bedrock for **2017-2024, all 402 detail industries**,
  from BEA's *Underlying* GDP-by-Industry table **UGO305-A**.
  [`derive_gross_output_before_redefinition`](../../transform/iot/derived_gross_industry_output.py)
  is a **straight read** of it — no 2017 shares anywhere. Only the
  *after*-redefinition variant applies 2017 co-production ratios, and the SUT is
  before redefinitions throughout (§Step 3, settled).
- **It behaves like an estimate, not a rescaling.** `GO_i(t)/GO_i(2017)` takes
  **402 distinct values across 402 industries** in every year. Dispersion
  2017→2024 runs 0.67× to 5.25×, and the spread lives *inside* summary
  industries: `335911` storage batteries 4.70× against `335110` electric lamps
  0.67×, both inside "Electrical equipment, appliances, and components". A
  shares-based series would show one ratio per summary industry.

**Consequences.** The industry-column target is imposed at **detail**, for every
Phase 1 year. The per-year target set survives, but it is now near-constant
across 2018-2024, and #591's Phase 1 / Phase 2 asymmetry on this point
dissolves — 2025 is not better constrained, it is the same. Aggregate-level
constraints are **still required**, but for the *value-added rows*, not for
gross output.

**And a new dependency appears.** Published gross output is at **producer**
prices; the SUT column identity is at **basic**. §4 below.

## 2. The refined target set

Legend — **H** hard (an identity, or a source we will not trade away),
**S**`w` soft with weight `w`, **mask** imposed cell-wise instead, **—** not
imposed.

| # | Margin | Target | Source | Level | Mode |
|---|---|---|---|---|---|
| T1 | Supply + Use industry columns | gross output, **basic prices** | UGO305-A less the industry product-tax wedge (§4) | **detail, 402** | **H** |
| T2 | Use FD columns ×13 | NIPA column total, one per code | §3 | column total | **S**`0.8` |
| T3 | Use FD columns ×6 | — | — | — | **mask** |
| T4 | Use VA row `V00100` | compensation of employees | NIPA T60200D | **industry group, aggregated** | **S**`0.6` |
| T5 | Use VA rows `T00OTOP`, `V00300` | — | — | — | **—** *(held back, §6)* |
| T6 | Use VA rows `T00TOP`, `T00SUB` | economy-wide totals | T30500, T31300 | scalar | **S**`0.7` |
| T7 | Supply `MCIF` | imports total | BEA ITA goods + services | column total | **S**`0.8` |
| T8 | Supply `MDTY` | customs duties total | NIPA T30500 | column total | **S**`0.7` |
| T9 | Supply `TOP`, `SUB` | product taxes / subsidies totals | T30500, T31300 | column total | **S**`0.7` |
| T10 | Supply `MADJ`, `TRADE`, `TRANS` | — | ours (Step 4b/4c) | — | **—** |
| T11 | Commodity rows | `T016 = T019` | identity | detail, 402 | **H** |

**T1 and T11 are the only hard constraints.** Everything sourced is an estimate
from an account with its own vintage; a set held entirely hard is infeasible by
construction, which is the argument for the KRAS-style soft layer in #588
Decision 2. The weights above are a **starting proposal to be calibrated**, not
a result — the ordering (identity > gross output > expenditure > income >
allocation) is the part worth defending.

### Why these and not the obvious alternatives

**Not summary SUT** — struck in #591 and it stays struck: derived, late, and it
aggregates away the dimension being estimated. It belongs to the test set
(#573).

**Not commodity gross output.** `T016 = T019` already determines it. Adding it
as a target would over-determine the commodity margin.

**Not `MADJ`, `TRADE`, `TRANS`.** These are our own Step 4b/4c output. A target
we produced is a preference with extra steps — #591's property 1, applied where
it actually bites. They stay free, with sign locks (§`mask_layer_plan.md` Tier 3).

**Six FD columns become mask, not target.** `F06C00` and `F07C00` carry exactly
one nonzero commodity row; `F10C00`, `F06N00`, `F07N00`, `F10N00` carry three or
four. For those, masking the cells and targeting the column total are the same
constraint written twice. 17 cells, 5.1% of the Use panel's mass.

## 3. The thirteen FD column targets

Remaining after the six that moved to the mask:

| Code | NIPA source | Note |
|---|---|---|
| `F01000` | PCE, T2.4.5U via the PCE bridge total | 259 commodity rows — the target adds a lot over the cells |
| `F02E00` | nonres. equipment, T5.5.5U | ⚠️ the PEQ bridge disagrees with the Use table by ~$15B (#547) — decide which the target is |
| `F02N00` | nonres. IP, T5.5.5U | |
| `F02R00` | residential, T5.4.5U | ⚠️ `F02R00` is short `S00402` by 1,883 (#633/#635) |
| `F02S00` | nonres. structures, T5.4.5U | |
| `F03000` | change in private inventories, T5.7.5B | ⚠️ **can be negative** — −37,568 in 2020, and it swings 1,248% year over year. Lowest weight in the set |
| `F04000` | exports, ITA / trade step | |
| `F06E00`, `F06S00` | federal defense equipment, structures | Section 3 |
| `F07E00`, `F07S00` | federal nondefense equipment, structures | Section 3 |
| `F10E00`, `F10S00` | S&L equipment, structures | Section 3 |

⚠️ **A negative column target is not an edge case here.** Any engine that
clamps targets non-negative silently produces a wrong `F03000` for 2020 — see
`mask_layer_plan.md` §2.

## 4. Gross output: producer → basic

Published gross output is at producer prices. The SUT industry column total is
at basic prices. Measured on 2017 detail, the wedge is exact per industry:

```
GO(producer)  =  T007(basic)  +  T00TOP  -  T00SUB
```

Maximum residual **$4 million per industry on a $34 trillion total**; zero
industries off by more than 1%. Skip the conversion and 86 industries are more
than 1% wrong and the economy total is 695,632 high — which is precisely taxes
on products less subsidies.

⚠️ **This is a new data dependency, and it is the weakest link in T1.** The
conversion needs `T00TOP` and `T00SUB` **by industry**, which Step 2 currently
allocates with 2017 ratios. So the hardest constraint in the set rests on an
allocation assumption. Two ways out, and they should be compared before T1 is
held hard:

- **(a) Convert the target.** Build `GO_basic = GO_producer − T00TOP + T00SUB`
  with the Step 2 industry split. Simple, but imports the allocation error into
  a hard constraint.
- **(b) State the target in producer prices** and let the `T00TOP`/`T00SUB` rows
  carry the conversion inside the balance — i.e. constrain
  `T005 + VABAS + T00TOP − T00SUB` rather than `T005 + VABAS`. The allocation
  then becomes something the balance solves rather than something it assumes.

**Recommend (b)**, with (a) as the fallback if the extra coupling destabilises
convergence. Either way the choice must be recorded, because it decides whether
the product-tax industry split is an input or an output of Step 5.

## 5. Aggregate-level constraints are still required — for value added

Gross output no longer needs them. `V00100` does: NIPA T60200D publishes
compensation by **industry group**, not by 402 detail industries. The truthful
constraint is *"these N detail industries sum to the published group"*, which
needs an aggregator matrix `G` and a target `G @ row_sum(Uva)`.

This is `sut_ras`'s `G_va` machinery, and it is what ceda's row/column-vector
API cannot express. Combined with `mask_layer_plan.md` §2 — ceda's mask is the
wrong kind of mask — **the two capabilities Option A was buying have both now
failed to hold up.** #588 Decision 1 should be re-run against that.

## 6. What is deliberately held back

#591 requires naming the aggregates left unimposed, so the testing strategy
keeps real out-of-sample content. **The income side is the hold-back.**

**Imposed, therefore by construction and no longer evidence:**
gross output by detail industry; the thirteen FD column totals; the six masked
FD columns; `T00TOP`/`T00SUB` economy-wide; imports, duties; `T016 = T019`;
compensation by industry group.

**Held back, and therefore real tests:**

| Held back | Tests what |
|---|---|
| `V00300` gross operating surplus | the residual the whole system lands on — its NIPA construction (T61200D + T61400D + T61500D + T61700D + T61300D + T62200D) is never imposed |
| `T00OTOP` other taxes on production | T30500's non-product portion, unimposed |
| **`VAPRO` → T10105 GDP** | the income-side total against a system anchored on the expenditure side and gross output. **The statistical discrepancy is the expected gap** — ~$67.9B in 2017, 0.35% of GDP — so this test has a known, interpretable tolerance rather than an arbitrary one |
| `VABAS` → T10305 | same, at basic prices |
| NIPA T1.14 gross value added **by sector** | a different cut (business / households / government) from the industry cut we impose |
| Summary SUT, all years | #573, entirely in the test set |

⚠️ **Note what this costs.** Leaving `V00300` and `T00OTOP` unimposed means
Step 2's value-added block enters Step 5 as a **seed only** for those two rows.
That is the price of keeping GDP as evidence, and it is worth paying: with every
income-side aggregate imposed, a green reconciliation run would prove nothing
beyond "the solver ran".

## 7. What to build

Two layers, mirroring the engine/orchestration split in #588.

### 7a. Generic, in `bedrock/utils/economic/balance/`

```
targets.py      Target, TargetSet, Aggregator — a target is (block, axis,
                values, aggregator|None, hard, weight, source, allow_negative)
mask.py         SutMask — the three layers kept separate: structural zeros,
                fixed values, sign locks. Never one boolean.
offset.py       split_fixed(X, mask) -> (F, Z);  offset_targets(targets, F)
                subtracting frozen mass, including R @ F @ Cᵀ for aggregates
feasibility.py  precheck(seed, mask, targets) -> list[Infeasibility]
```

**`feasibility.py` is the piece that turns `mask_layer_feasibility.py` into
production code.** Before the balance runs it must report, per margin: frozen
mass, free mass, leverage, and whether a nonzero residual target faces zero free
mass. That last case is infeasible and must **raise**, not converge to something
meaningless. A leverage threshold (start at 10×) warns.

⚠️ Three properties this layer must have, all learned from
`mask_layer_plan.md` §2:

- **A fixed cell is held at its value**, not zeroed. The offset method does
  this; a participation mask does not.
- **Targets keep their sign.** Residual targets can go negative even where the
  original was positive, and `F03000` is negative outright in 2020.
- **`F` is excluded from the seed**, not merely flagged — passing the full
  matrix *and* the full targets double-counts the frozen mass.

### 7b. Sourcing, in `bedrock/transform/iot/`

```
nowcast_targets.py   industry_output_targets(year)   -> T1, incl. the §4 conversion
                     fd_column_targets(year)         -> T2
                     va_row_targets(year)            -> T4, T6, with the aggregator
                     supply_column_targets(year)     -> T7, T8, T9
                     build_target_set(year)          -> TargetSet
nowcast_mask.py      structural_zero_mask()          -> from the 2017 pattern
                     fixed_value_mask(year)          -> Tier 1, the six FD columns
                     sign_lock_mask()                -> Tier 3
                     build_sut_mask(year)            -> SutMask
```

**Per-year configuration**, following the FBS convention: a shared
`ras_targets_common.yaml` carrying the table above plus thin
`ras_targets_<year>.yaml` files including it. Near-constant across 2018-2024 now
that detail gross output is observed for all of them — but the structure has to
exist, because weights and hold-backs are the things most likely to move.

**Excluded from the balance entirely** (`mask_layer_plan.md` §3): `S00900`, held
out and re-derived from `−F010 + Supply T016` afterwards; `4200ID`, empty in
every block.

## 8. Open, and what each blocks

1. **Producer vs basic — (a) or (b) in §4.** Blocks T1, the hardest constraint
   in the set.
2. **Is the published detail GO before or after redefinitions?** The straight
   read is used as "before" in
   [`derived_gross_industry_output.py`](../../transform/iot/derived_gross_industry_output.py),
   which is consistent with the SUT — confirm against BEA documentation rather
   than inferring it from our own function name.
3. **Weight calibration.** The ordering in §2 is defensible; the numbers are
   not yet. Calibrate on the 2017 replay, where the answer is published.
4. **Does the Tier-1 mask hold for 2018-2024?** The 1:1 line→commodity mapping
   is asserted from the 2017 crosswalks.
5. **`F02E00`'s target** — the PEQ bridge and the Use table disagree by ~$15B
   (#547). Whichever is chosen becomes the constraint.
