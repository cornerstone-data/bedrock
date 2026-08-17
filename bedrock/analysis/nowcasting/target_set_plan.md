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
| T1 | Supply + Use industry columns | gross output, **producer prices** — the column margin is `T005 + VAPRO` (§4) | UGO305-A, unconverted | **detail, 402** | **H** |
| T2 | Use FD columns ×13 | NIPA column total, one per code | §3 | column total | **S**`0.8` |
| T3 | Use FD columns ×6 | — | — | — | **mask** |
| T4 | Use VA row `V00100` | compensation of employees | NIPA T60200D | **industry group, aggregated** | **S**`0.6` |
| T5 | Use VA rows `T00OTOP`, `V00300` | — | — | — | **—** *(held back, §6)* |
| T6 | Use VA rows `T00TOP`, `T00SUB` | economy-wide totals | T30500, T31300 | scalar | **S**`0.7` |
| T7 | Supply `MCIF` | imports total | BEA ITA goods + services | column total | **S**`0.8` |
| T8 | Supply `MDTY` | customs duties total | NIPA T30500 | column total | **S**`0.7` |
| T9 | Supply `TOP`, `SUB` | product taxes / subsidies totals | T30500, T31300 | column total | **S**`0.7` |
| T10 | Supply `MADJ`, `TRADE`, `TRANS` | — *(distribution free; but see T15/T16)* | ours (Step 4b/4c) | — | **—** |
| T11 | Commodity rows | `T016 = T019` | identity | detail, 402 | **H** |
| T12 | Use `T00SUB` ↔ Supply `SUB` | `Σ T00SUB + Σ SUB = 0` | identity (§2a) | scalar | **H** |
| T13 | Use `T00TOP` ↔ Supply `TOP` + `MDTY` | `Σ T00TOP = Σ TOP + Σ MDTY` | identity (§2a) | scalar | **H** |
| T14 | Use `T00TOP[4200ID]` ↔ Supply `MDTY` | equal | identity (§2a) | scalar | **H** |
| T15 | Supply `TRADE` column | `Σ TRADE = 0` | identity (§2b) | scalar | **H** |
| T16 | Supply `TRANS` column | `Σ TRANS = 0` | identity (§2b) | scalar | **H** |

**The hard constraints are T1 and T11–T16** — all identities, none of which
spends a source. Everything *sourced* is an estimate from an account
with its own vintage; a set held entirely hard is infeasible by construction,
which is the argument for the KRAS-style soft layer in #588 Decision 2. The
weights above are a **starting proposal to be calibrated**, not a result — the
ordering (identity > gross output > expenditure > income > allocation) is the
part worth defending.

⚠️ **T1 cannot bind the Supply column for `4200ID`.** Its Use column is 38,513
of customs duties while its Supply column is **zero**, because duties are not
output at basic prices. Stated at producer prices, the gross-output target
binds the Use side only for this industry. `mask_layer_plan.md` §3.

### 2a. Three cross-block identities the target set was paying for

**Measured on the 2017 detail SUT.** These tie the Use table's product-tax rows
to the Supply table's product-tax columns. Both sides are *inside* the balance,
so — like `T016 = T019` — they cost no source at all:

| Identity | Use side | Supply side | Residual |
|---|---:|---:|---:|
| Subsidies | `T00SUB` 59,876 | `−SUB` 59,876 | **0 — exact** |
| Taxes, naive | `T00TOP` 755,451 | `TOP` 716,926 | **38,525** ✗ |
| **Taxes, correct** | `T00TOP` 755,451 | `TOP + MDTY` 755,433 | 18 (0.0024%) ✓ |

⚠️ **`T00TOP = TOP` is wrong; `T00TOP = TOP + MDTY` is right.** Customs duties
are a tax on products that the Supply table books in its own column while the
Use table folds it into `T00TOP` — and `4200ID` is exactly the hinge:

```
Use T00TOP[4200ID]      =  38,513   ≈  Supply MDTY total   38,507   (residual 6)
Use T00TOP less 4200ID  = 716,938   ≈  Supply TOP total   716,926   (residual 12)
```

The residuals of 6, 12 and 18 are BEA's $1M publication rounding — the same
effect that produces the transport-margin shortfall in Step 4c.

**Why this changes T6, T8 and T9.** Those three spend NIPA T30500 and T31300 on
*both* sides of the same accounting quantity. The identities make one side
redundant: impose T12-T14 hard, and the NIPA totals become anchors on the
**level** only, rather than doing double duty on the split. Strictly more
constraint for strictly less source — and per §6 it leaves more of T30500
unspent as evidence.

**This is also what makes the sign normalisation load-bearing.** Stored as BEA
publishes them, T12 is a sum-to-zero across two opposite conventions; stored
negative on both sides (§4's ⚠️), it is a plain equality that a `{0,1}`
aggregator can express. A signed aggregator would be needed otherwise.

⚠️ **Verified on 2017 only.** These are accounting identities and should hold
every year, but `MDTY` is nowcast annually from Census duty rates levelled to
NIPA `B235RC`, so T14 doubles as a **free consistency check on that estimate** —
and would be the first place a duty-rate error shows up.

### 2b. Margins are a redistribution, so their columns sum to zero

The single most useful constraint on our *own* Step 4c output, and it costs
nothing. Measured on 2017:

| Column | added to goods | given up | **total** |
|---|---:|---:|---:|
| `TRADE` | 3,264,932 | −3,264,931 | **1** |
| `TRANS` | 415,580 | −415,570 | **10** |

A margin is not value created, it is value **moved**: the wholesale, retail and
transport commodities give up exactly what is added onto the goods they carry.
The give-up side is the 19 wholesale/retail commodities and the 5 transport
ones — the same negative cells Tier 3's sign locks protect.

**This is the line between the two families of Supply column**, and it is why
only these two are zero-sum:

| Column | Behaviour | Column total 2017 |
|---|---|---:|
| `TRADE`, `TRANS` | **redistribution** — zero-sum | 1, 10 |
| `MCIF`, `MDTY`, `TOP` | **addition** to domestic supply | 2,649,430 / 38,507 / 716,926 |
| `MADJ`, `SUB` | addition, signed negative | −23,116 / −59,876 |

**Why T15/T16 matter more than they look.** T10 leaves `TRADE` and `TRANS`
deliberately unimposed, because they are our own Step 4b/4c output and a target
we produced is a preference with extra steps. But *unimposed* left them with no
constraint at all — and the failure mode is silent: if the nowcast adds 3.3T of
margin onto goods while the trade commodities give up 3.1T, the table simply
does not balance and nothing says so until much later. T15/T16 close that
without touching the **distribution**, which stays free. It is a constraint on
our own work rather than a source, which is exactly what makes it legitimate.

### The trade and transport rows reconcile differently, and it is not a defect

Following the give-up side through to total supply:

```
TRADE   make 3,373,416 − giveup 3,264,931 = 108,485  ≈  T016 108,478  ≈  uses 108,465
TRANS   make   731,885 − giveup   415,570 = 316,315  ≠  T016 364,350
```

Trade closes directly because its 19 commodities carry **zero** `MCIF`, `MADJ`,
`TOP`, `SUB` and `MDTY` — wholesale and retail services are neither imported nor
taxed. Transport does not, because they are both:

```
316,315 + MCIF 46,393 + MADJ −19,056 + TOP 23,246 + SUB −2,548 = 364,350 = T016 ✓
```

With `MADJ` included the transport rows close to **residual 0** — exactly, not
approximately. So there is **no separate trade/transport row identity to
impose**: it is the ordinary Supply row identity (`mask_layer_plan.md` §8) plus
T11. The new content is entirely in the column sums, T15 and T16.

### `MADJ` is transport and insurance, but it is still a level

All six nonzero `MADJ` cells are transport or insurance commodities — the
c.i.f./f.o.b. wedge is freight plus insurance, and it lands nowhere else:

| Commodity | `MADJ` | in the `TRANS` give-up set? |
|---|---:|---|
| `483000` water transport | −12,794 | yes |
| `484000` truck transport | −4,900 | yes |
| `492000` couriers | −3,361 | no |
| `481000` air transport | −950 | yes |
| `5241XX` insurance carriers | −699 | no |
| `482000` rail transport | −412 | yes |

That is an independent check on the destination-reassignment method built for
`MADJ` in [#644](https://github.com/cornerstone-data/bedrock/pull/644), which
reassigns Census `GEN_CHA_YR` onto the 2017 `MADJ` destinations.

⚠️ **But it earns no identity of its own.** `MADJ` totals **−23,116**, not zero:
it *reduces* total supply to move imports from a c.i.f. to an f.o.b. basis,
rather than moving value between commodities. It belongs to the addition family
with `MCIF` and `MDTY`, not to the zero-sum family with `TRADE` and `TRANS` —
so T10 leaves it unconstrained, and it is the one Step 4b/4c output with no
internal check at all.

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

## 4. Gross output stays at producer prices — decided

Published gross output is at producer prices. The SUT industry column total is
at basic prices. Measured on 2017 detail, the wedge is exact per industry:

```
GO(producer)  =  T007(basic)  +  T00TOP  -  T00SUB
```

Maximum residual **$4 million per industry on a $34 trillion total**; zero
industries off by more than 1%. Skip the conversion and 86 industries are more
than 1% wrong and the economy total is 695,632 high — which is precisely taxes
on products less subsidies.

Converting the target to basic prices would need `T00TOP` and `T00SUB` **by
industry**, which Step 2 allocates with 2017 ratios — putting an allocation
assumption underneath the hardest constraint in the set.

✅ **Decided 2026-08-17: do not convert. The target is producer prices, and the
balance solves the allocation.** We cannot assume a fixed 2017 ratio for the
conversion, so the product-tax industry split becomes an **output** of Step 5
rather than an input to it. Concretely, the industry column margin is

```
T005  +  VAPRO   =   GO(producer)          verified to $1 per industry, 2017
```

i.e. intermediate inputs plus **all five** value-added rows, rather than
`T005 + VABAS`. `T00TOP` and `T00SUB` keep their economy-wide totals as soft
targets (T6), which anchors the level while leaving the industry distribution
free for the balance to determine.

### ⚠️ The sign trap this exposes

**BEA stores subsidies with opposite signs in the two tables**, and the column
margin is wrong by `2 × T00SUB` if that goes unnoticed:

| | stored | negative cells |
|---|---:|---:|
| Use table `T00SUB` row | **+59,876** | 0 of 402 |
| Supply table `SUB` column | **−59,876** | 15 of 15 |

So the producer-price column margin is **not a plain sum of the column's
cells** — `T00SUB` enters with coefficient **−1**. A plain five-row sum misses
`VAPRO` by up to **38,943 on a single industry**.

Two consequences for #653:

- **The target machinery needs signed coefficients**, not just a boolean
  aggregator matrix. `R @ X` with `R ∈ {0,1}` cannot express this margin.
- **Or** normalise on the way in: store `T00SUB` negative internally, matching
  the Supply side, so every margin is a plain sum. **Prefer this** — a signed
  storage convention is checked once at load, whereas a signed aggregator is a
  chance to get it wrong at every call site. Whichever is chosen, assert it, and
  assert it at the boundary where BEA's tables are read.

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

**Excluded from the balance** (`mask_layer_plan.md` §3), ⚠️ **on the commodity
axis only**: `S00900`, held out and re-derived from `−F010 + Supply T016`
afterwards; `4200ID`, whose commodity row is empty. **`4200ID` stays an
industry** — its column is customs duties, `T00TOP` = `VAPRO` = 38,513, and
dropping it would delete a hard constraint. Implemented in
[`nowcast_mask.py`](../../transform/iot/nowcast_mask.py) as
`balance_commodities()` (400) against `balance_industries()` (402).

## 8. Weight calibration on the 2017 replay

The ordering in §2 — identity > gross output > expenditure > income >
allocation — is defensible from what each source is. The numbers are not.

**What a weight actually decides.** The target set is *mutually inconsistent by
construction*: NIPA, GDP-by-industry and the trade accounts are three accounts
on three vintages, and they will not reconcile to the dollar. So the balance
cannot satisfy all of them, and something has to absorb the gap. **A weight does
not say "how accurate is this number" — it says "when the accounts disagree, who
gives way."** High weight means the balance moves everything else first; low
weight means this target absorbs the inconsistency. Weights are meaningful only
*relative to each other*, so normalise before reading anything into a value.

**Why 2017 and not a nowcast year.** 2017 is the only year with a published
detail SUT. So the replay is a supervised experiment with a known answer:

1. build the seed exactly as a nowcast year would — Steps 1-4 on 2017 inputs,
   no peeking at the published SUT
2. build the target set from the 2017 vintages of the same sources
3. balance under a candidate weight vector
4. diff the result against BEA's published 2017 detail SUT, cell by cell and on
   the margins, using #587's comparison engine
5. repeat, and keep the vector that minimises the discrepancy

**Do this in two stages, not as a grid search over eight weights.** First run
with all soft weights *equal* and read which targets the balance has to violate
most — that names the handful that actually bind. Then tune only those. Most of
the eight will turn out not to matter, and finding out which is cheaper than
optimising all of them.

⚠️ **Two things that make this less clean than it sounds, both worth stating
before anyone reports a calibrated number:**

- **2017 is a benchmark year, so it is unrepresentative in exactly the way that
  matters.** BEA had the Economic Census; the sources are unusually good *and
  unusually consistent with each other*. Weights fitted to 2017 will be too
  tight for a year like 2020. Treat the replay as a check on the **ordering** —
  does the ranking survive? — rather than as a numeric fit, and sanity-check the
  implied residual distribution on a nowcast year before shipping.
- **A weight should be defensible as a statement about the source**, so
  calibration is a test of prior belief rather than a free parameter fit. If the
  replay says `F03000` deserves the *highest* weight, that is a signal something
  upstream is wrong — not a result to adopt.

## 9. Open, and what each blocks

1. ✅ ~~Producer vs basic~~ — **decided**: producer, and the balance solves the
   allocation. §4.
2. ✅ ~~`F02E00`'s target~~ — **decided**: the **Use table** is the constraint,
   not the PEQ bridge. See below.
3. **Is the published detail GO before or after redefinitions?** The straight
   read is used as "before" in
   [`derived_gross_industry_output.py`](../../transform/iot/derived_gross_industry_output.py),
   which is consistent with the SUT — confirm against BEA documentation rather
   than inferring it from our own function name.
4. **Does the Tier-1 mask hold for 2018-2024?** The 1:1 line→commodity mapping
   is asserted from the 2017 crosswalks.

### `F02E00` — the Use table is the constraint

`F02E00` reproduces the PEQ bridge cell for cell (#631) while disagreeing with
the Use table by ~$15B at the column total (#547). ✅ **Decided 2026-08-17: the
Use table is the target.** The bridge is an **allocation device** — it says how a
column distributes across commodities — not an authority on how large the column
is. The SUT we are building has to be a SUT, so the column total has to be on the
Use table's basis.

⚠️ **This settles which, not how, and the gap does not close itself.** For 2017
the Use-table column total is simply readable. For 2018-2024 there is no detail
Use table — that is what we are building — so the remaining work is to establish
**which NIPA aggregate reproduces the Use-table basis rather than the bridge
basis**, and to explain the ~$15B. Until that is answered, `F02E00`'s nowcast
target is on the wrong basis by roughly a percent of the column. **#547.**
