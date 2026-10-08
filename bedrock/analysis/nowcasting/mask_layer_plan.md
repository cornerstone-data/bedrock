# The Step 5 mask layer — what it is, whether it fits, and what goes in it

Companion to [`plan.md`](plan.md) §Step 5 and to
[#588](https://github.com/cornerstone-data/bedrock/issues/588) Decision 2
("what is held fixed entirely?") and
[#591](https://github.com/cornerstone-data/bedrock/issues/591) (the target set).
Every number below is reproduced by
[`mask_layer_feasibility.py`](mask_layer_feasibility.py), which carries the
findings as `--check` assertions.

The mask and the target set are the same decision seen from two sides, so they
have to be settled together. **A source can be spent on a cell or on a margin,
never on both.**

---

## 1. What the mask is

A per-cell boolean over every SUT block: *the balancer may not change this
value*. It is **not** any of the three things it keeps getting confused with.

| | What it is | Where it lives |
|---|---|---|
| **Structural zero** | cell is zero and must stay zero | a *pattern*: both engines get it free — diagonal scaling can never make a zero nonzero |
| **Fixed value** ← *this is the mask* | cell is nonzero, directly measured, must come out unchanged | neither engine has it today |
| **Sign lock** | cell may move but not across zero | `sut_ras`'s `sign_flex`, not a mask |

Conflating the first two is the mistake ceda's API invites, and it is why the
mask looked free. It is not free — see §3.

## 2. Feasibility — can either engine express it?

### ceda `ras_balancing.py`: no, and for three independent reasons

`free_mask` is a **participation** mask, not a value mask. The dense path zeroes
every non-free cell rather than holding it:

```python
mask = np.asarray(_prepare_free_mask(matrix, free_mask), dtype=bool)
masked = np.where(mask, matrix, 0.0)     # ras_balancing.py:573 — value lost
seed = np.maximum(masked, 0.0)           # :579 — negative seed clamped
```

and the targets never learn about the frozen mass. Three blockers, all inside
the first ten lines of the dense path:

1. **`:573`** — a masked cell is set to `0.0`, so a fixed nonzero value cannot
   be expressed at all. The docstring says "held at their seed values (typically
   zero)"; the code holds them at zero, full stop.
2. **`:570-571`** — `np.maximum(row_targets, 0.0)` / `np.maximum(col_targets,
   0.0)`. A negative column target is silently replaced by zero. **This is not
   hypothetical: `F03000` is −37,568 in 2020** and swings 1,248% year over year,
   the largest move of any FD column by an order of magnitude.
3. **`:579`** — negative seed mass is clamped and merely logged. Our seed has
   70 negative cells in the FD block alone (61 of them in `F03000`), plus every
   `SUB` cell (15/15), every `MADJ` cell (6/6), 19 negative `TRADE` and 5
   negative `TRANS`.

⚠️ **This moves #588's Decision 1.** ceda's mask machinery was one of the two
things Option A was buying. It turns out to be the wrong mask, and the two
clamps are load-bearing invariants rather than lines to delete. What remains
genuinely valuable in ceda is the *convergence, stall-projection and diagnostics*
work — not the mask.

### `sut_ras.py`: no mask at all, but nothing fights one

GRAS carries signed targets (`gras_internal` takes `target` unsigned nowhere)
and preserves negative cells by construction. There is no mask to fix, only one
to add.

### The mask reduces to a participation mask — the offset method

This is the finding that makes the mask feasible under *either* engine, and it
is about twenty lines rather than an engine rewrite. Split the block into a
fixed part `F` and a free part `Z`, balance `Z` against **residual** targets,
then add `F` back:

```
X  = F + Z                    F zero off the mask, Z zero on it
r' = r - F @ 1                row targets, less the frozen row mass
c' = c - 1ᵀ @ F               column targets, less the frozen column mass
A' = A - R @ F @ Cᵀ           and the same for any aggregate-level target
```

The engine then only ever sees a participation mask, which is the thing both
engines already have. Two consequences worth stating before anyone writes it:

- **Residual targets can change sign** even where the original target was
  positive. GRAS handles that; RAS does not. Another point for Decision 2.
- **`F` must be excluded from the seed**, not merely flagged. Passing the full
  matrix with a mask and *also* the full targets double-counts the frozen mass.

## 3. What a mask costs — measured on the 2017 detail SUT

The metric is **leverage** = `|margin total| / |free mass in margin|`. Leverage
10 means the free cells must move 10% to deliver a 1% change in the target;
leverage `inf` means the margin cannot move at all.

| Mask | frozen cells | **frozen % of mass** | rows immovable | rows lev>10 | cols immovable |
|---|---:|---:|---:|---:|---:|
| S0 structural zeros only | 0 | 0.0% | 0 | 0 | 0 |
| S1 + 1:1 NIPA→commodity FD cells | 17 | **5.1%** | 5 | 5 | 6 |
| S2 + whole FD block | 1,253 | **39.9%** | 27 | 78 | 19 |
| S3 + whole FD block + VA block | 2,806 | **74.2%** | 27 | 78 | 21 |

**Count cells and the mask looks cheap; count dollars and it does not.** The FD
block is **2.7% of the Use panel's 47,087 nonzero cells and 39.9% of its
dollars** — a fifteenfold difference between the two ways of reading the same
mask.

**Freezing whole blocks is not affordable.** Under S2, 27 commodity rows lose
every degree of freedom on the Use side and 51 more sit above 10× leverage —
78 of 402 commodities, a fifth of the table. The extremes are not marginal
commodities: `336112` light trucks has $439,551 of row mass and **$1** of free
mass; `621100` physicians' offices, $492,108 against $617.

**But read leverage across both tables, not one.** `T016 = T019` can close on
either side, so a frozen Use row is only fatal if the Supply row is frozen too.
Of the 27, **26 have a Supply row that absorbs the whole adjustment at a ratio
of 1.00**. Exactly one commodity is genuinely stuck.

⚠️ So the real cost of freezing final demand is not infeasibility — it is that
**the balance silently relocates onto the Supply table for a fifth of
commodities**. Housing, government, education, health and construction stop
being estimated on the Use side at all. That may be the right modelling choice;
it must not be an accidental one.

### Two commodities need handling outside the mask

- **`S00900`** (noncomparable imports / rest-of-world adjustment) — its Use row
  is 100% final demand ($405,436, all exports) and its Supply side can absorb
  $3,494, a joint free share of 0.9%. It is already *derived* from an identity
  (`−F010 + Supply T016`, [`plan.md`](plan.md) §Step 1B). **Hold it out of the
  balance and re-derive it afterwards** rather than asking RAS to move it.
- **`4200ID`** — ⚠️ **corrected 2026-08-17. The earlier text here said "zero in
  every block, on both sides", and that is wrong on the industry axis.**
  `4200ID` is **customs duties**, and it means different things on the two axes:

  | axis | content |
  |---|---|
  | commodity (row) | genuinely empty — it produces nothing |
  | **industry (column)** | **live**: no intermediate purchases and no commodity output, but `T00TOP` = `VAPRO` = **38,513** |

  That column total is the Supply table's `MDTY` total (38,507, residual 6 —
  BEA's $1M publication rounding), and it is *exactly* the published detail
  gross output for `4200ID`. So **exclude it as a commodity and keep it as an
  industry.** Excluding it from both axes drops a $38.5B hard constraint and
  the cleanest instance of the §7 producer-versus-basic wedge, which is
  `GO(producer) = T007(basic) + T00TOP − T00SUB` = `0 + 38,513 − 0` here.

  ⚠️ **Its Supply column is genuinely zero**, because duties are not output at
  basic prices. A gross-output target stated at producer prices can bind the
  Use column but **not** the Supply one — see `target_set_plan.md` §2, T1.

### The two axes are not the same set

The panel is **not square**, and nothing downstream may assume it is. 398 codes
are in both lists; four are industry-only (`331314`, `S00101`, `S00201`,
`S00202` — all live, 5,100 to 33,922 of intermediates each) and four
commodity-only (`S00300`, `S00401`, `S00402`, `S00900`).

⚠️ **Two commodities have no domestic make at all**, so a structural-zero mask
freezes their make row completely and their entire Supply-side freedom is in
the bridge columns:

| commodity | Supply make | bridge | composition |
|---|---:|---:|---|
| `S00300` noncomparable imports | 0 | 260,421 | **all `MCIF`** — nothing domestic produces it |
| `S00402` used and secondhand goods | 0 | 164,495 | 117,563 `TRADE`, 23,869 `TRANS` |

Neither is infeasible — both keep ample bridge freedom — but **leverage on those
two rows must be read on the bridge, not on the make**. `S00402` reaching
buyers through margins rather than production is also why its margin rate on
basic value looks impossible (§`margins_estimation_plan.md`, `TRANS/T013` of
3.25).

## 4. The rule for what goes in the mask

> **Mask a cell only if the source reports *that cell*. If the source reports
> the margin, it belongs to the target set instead. Never both.**

This is what rules out the obvious-looking move of freezing the FD block. `PCE`
and equipment reproduce their BEA bridges cell for cell (#630, #631) — but the
bridge is a **2017 commodity split applied to a current-year NIPA line**. The
line total is observed; the split is an assumption, and it is exactly the
assumption the balance exists to correct. Freeze it and the nowcast can never
learn that the commodity mix moved.

The collision is sharpest where a column is nearly a single cell:

| FD column | nonzero rows | mask vs target |
|---|---:|---|
| `F06C00`, `F07C00` | 1 | **identical** — masking the cells *is* the column target |
| `F10C00`, `F06N00`, `F07N00`, `F10N00` | 3–4 | near-identical |
| `F02N00`…`F02S00`, `F0*E00`, `F0*S00` | 8–20 | target adds a little |
| `F01000` (259), `F04000` (341), `F03000` (258) | 250+ | target adds a lot |

Spend the government consumption and IP columns as **mask**; spend PCE, exports,
inventories and the investment columns as **targets**.

## 5. The proposed mask

**Tier 0 — structural zeros, everywhere.** The 2017 sparsity pattern per block,
except where a nowcast source legitimately creates a cell. Free, and it is what
both engines already do. Note this is a real assumption on the Supply side,
which is only 3.1% dense: *no industry produces a commodity it did not produce
in 2017*.

**Tier 1 — masked, hard.** Cells where one NIPA line lands on one commodity:
`F06C00`, `F07C00`, `F10C00`, `F06N00`, `F07N00`, `F10N00`. **17 cells, 5.1% of
the Use panel's mass, and it costs 5 commodity rows their Use-side freedom.**
These six columns then leave the target set — they are already imposed.

**Tier 2 — not masked; their totals go in the target set.** Everything that
reaches its commodities through a bridge or a share: `F01000`, `F02E00`,
`F02N00`, `F02R00`, `F02S00`, `F03000`, `F04000`, and the equipment/structures
government columns. The NIPA column total is the constraint; the split stays
free.

**Tier 3 — sign locks, not masks.** `SUB` (15/15 negative), `MADJ` (6/6), the
margin give-up side, and `TOP` (non-negative). These need to move; they must not
cross zero.

**Tier 4 — held out of the balance, on the commodity axis only.** `S00900` and
`4200ID` leave the *commodity* list. `4200ID` **stays** in the industry list —
see §3, it is customs duties and its column carries 38,513.

**Deliberately unimposed, as out-of-sample evidence** (#591's requirement that
some aggregates stay unspent): NIPA T1.14, and `VAPRO` → T1.1.5 GDP. The VA
block is not masked and the VA row totals are targets, so those two remain the
only aggregates a green run actually proves something about.

## 6. What this changes upstream

- **#588 Decision 1** — ceda's mask is not the mask Step 5 needs, and its two
  clamps are invariants rather than lines to delete. The offset method means the
  mask is no longer a reason to prefer ceda. What ceda still offers is
  convergence, stall projection and diagnostics.
- **#588 Decision 2** — the negative `F03000` target and the sign-change
  behaviour of residual targets are now measured, not asserted. Plain RAS/IPFP
  is out on evidence.
- **#591** — six FD columns move from the target set into the mask. And see §7.

## 7. Detail gross output is observed, and it is at producer prices

Two findings that belong to #591 but surfaced here.

**It is not nowcast.** `BEA_Detail_GrossOutput_IO_<year>` is already extracted
in bedrock for **2017–2024, all 402 detail industries**. #591's property 1 says
"detail gross output for 2018-2024 is nowcast by Step 4a — imposing it on the
balance is circular", and concludes the industry constraint can only honestly be
imposed at summary level. **That premise needs re-checking**: if the published
series is genuinely BEA's own detail estimate, the Phase 1 years can carry a
detail-level industry constraint, and the Phase 1 / Phase 2 distinction in
§Step 5 largely dissolves. What must still be confirmed is whether BEA derives
those detail years from the same 2017 relationships Step 4a would use — if so,
the circularity is real but relocated, not absent.

**It is at producer prices; the SUT column identity is at basic prices.** The
wedge is exact, per industry:

```
GO(producer)  =  T007(basic)  +  T00TOP  -  T00SUB
```

Verified on 2017: maximum residual **$4 million per industry** on a $34 trillion
total; zero industries off by more than 1%. Ignore the conversion and 86
industries are more than 1% wrong and the economy total is 695,632 high — which
is precisely taxes on products less subsidies.

⚠️ **Consequence for the target set:** imposing published gross output on the
SUT column requires `T00TOP`/`T00SUB` **by industry**, which Step 2 currently
allocates with 2017 ratios. The constraint is only as observed as that
allocation. The alternative is to state the target in producer prices and let
the product-tax rows carry the conversion inside the balance.

## 8. The column identity, and why margins are not in it

Measured on 2017 detail, both to $1M:

```
T018 (industry output, basic)  =  T005 (intermediate, purchaser)  +  VABAS
VABAS                          =  V00100 + T00OTOP + V00300
```

`T00TOP` and `T00SUB` are **not** in the industry-output identity — they are the
`VABAS` → `VAPRO` wedge. And the Use table's industry column total equals the
Supply table's industry column sum to **$4M on $33.77T**.

So the intermediate column target is `intermediate = GO(basic) − VABAS`, and it
**needs no margin data**. The Use table is at purchaser prices throughout, and
the basic→purchaser wedge sits on the Supply table as commodity *rows*
(`TRADE`, `TRANS`, `MDTY`, `TOP`, `SUB`) rather than as a per-cell conversion
inside the Use columns. Margins enter Step 5 only through those Supply columns,
at commodity level, which is Step 4c's existing output. The per-transaction
margin detail is a **Step 6b** requirement (PUR→PRO), not a Step 5 one.

## 9. Open

1. **Does BEA derive published detail gross output for 2018-2024 independently,
   or from 2017 relationships?** Settles whether the detail industry constraint
   is circular. Blocks #591.
2. **`T00TOP`/`T00SUB` by industry** — needed for the producer↔basic conversion
   above, and currently a 2017-ratio allocation.
3. **Does the Tier-1 mask hold for 2018-2024?** The 1:1 line→commodity mapping
   is asserted from the 2017 crosswalks; confirm the government consumption and
   IP columns stay single-commodity across the window.
4. **Supply-side mask** — this note measures the Use panel. `MCIF` has a 2017
   candidate and `MDTY` a sourced method; whether either is fixed rather than
   targeted is not yet decided.
