# BEA-style redefinitions reconstruction report

This report records what the original BEA-style Step 7 implementation did, how well it reconstructed the published 2017 after-redefinitions tables, and why production is moving to a different approach (see [`../redefinitions_ratio_implementation/ratio-plan.md`](../redefinitions_ratio_implementation/ratio-plan.md)).

---

## Methods summary (original implementation)

BEA publishes two versions of the detailed Make, Use, import, and margins tables for 2017: one **before** “redefinitions” and one **after**. Redefinitions move some secondary production from the industry that made it to another industry treated as the primary producer, and they reshuffle the inputs (and value added) that go with that move.

The original bedrock attempt followed BEA’s written method as closely as it could. In plain terms it:

1. **Found which Make cells redefine** — compared the 2017 before and after Make tables; every off-diagonal cell that shrinks by more than a small dollar threshold is a redefinition; recorded how much of that cell moved (as a share of the before value) and which industry absorbed the increase.
2. **Moved Make output** — for each such pair, took that share times the year’s Make cell and subtracted it from the source industry / added it to the destination industry.
3. **Reallocated Use and value added (default rule)** — for most pairs, took the destination industry’s input mix and moved that mix, scaled by the dollars redefined, out of the source industry column and into the destination column.
4. **Applied a few special cases** — wholesale/retail margin cases (mostly compensation and depreciation), own-account software (a fixed five-input strip), named large customs (pair-specific recipes), and a repair when the default mix would drive a source input negative.
5. **Moved import and margins** in proportion to how much of each Use cell actually left the source industry.
6. **Filled the rest with a residual table** — cell-by-cell corrections so the published 2017 after tables match exactly.

That work was organized in the [implementation plan](bea-style-plan.md) as three build stages:

| Stage | Name in the plan | What it does (one sentence) |
| --- | --- | --- |
| **A** | 2017 movement census | Compares published 2017 before vs after tables, writes down which Make pairs redefine and which special input recipes apply, and checks the known Use-movement magnitudes. |
| **B** | Chapter 9 core transform | Runs the default method: move Make with those shares, then reallocate Use / value added / imports / margins using the destination industry’s input mix (plus the negative-input repair when needed). |
| **C** | Residual rules for the 2017 match | Adds the named special cases on top of **B**, then a final residual correction layer so Use, value added, imports, and margins match the published 2017 after tables cell by cell. |

Stages **D** (diagnostics wiring) and **E** (other years) in the plan are not separate algorithms; **D** scores the result and **E** was never the point of this reconstruction test.

---

## Where the archived implementation lives

This BEA-style stack is **out of production**. The archived copy is under:

`bedrock/analysis/nowcasting/after_redef_MUTs/BEA_style_redefinitions_implementation/code/`

That includes the transform module, census/overlay CLI, classification / recipes / residual artifacts, and their reference tests—kept as analysis only. Planning docs (this report, [bea-style-plan.md](bea-style-plan.md), README) stay in the parent folder. Production Step 7 must not import or call any of the archived code; it uses [`../redefinitions_ratio_implementation/`](../redefinitions_ratio_implementation/) instead.

---

## BEA-style redefinitions reconstruction report findings

The numbers below come from running the implemented **A → B → C** stack against the published 2017 before/after tables (dollar bar: half a million USD per cell).

### 1. Did we recreate the 2017 after-redefinitions tables?

**Yes for Use, value added, imports, and margins — but only with the full stack (B + all of C, including the residual layer).**

With every stage turned on, those four tables match the published 2017 after tables cell by cell:

| Table | Cells matched | Accuracy |
| --- | ---: | ---: |
| Use intermediate | 48,054 | 100% |
| Value added | 1,191 | 100% |
| Import | 18,505 | 100% |
| Margins | 128,911 | 100% |

That is the acceptance bar issue #572 asks for on those tables.

**Make also passes**, under the softer Make check from the plan (not “every cell on the whole grid must match”). Stage **A** found **1,880** redefinition pairs; all **1,880** active source cells that actually move match the published after Make to the half-million bar. Commodity column totals can still be off by up to about **$10M** (plan cap $11M). Destination cells and small leftover gaps are expected; there is no Make residual layer.

As a sanity check on stage **A**, the published before→after Use movement still matches the About-doc figures exactly: **5,740** cells change, **553,635** million dollars of gross movement, largest single cell **42,893**, net **−7**.

### 2. How well does each stage match on its own?

Almost every pair stays on stage **B**’s default destination mix. Stage **C**’s named specials touch only **13** of **1,880** pairs (five wholesale/retail margin, one software, seven named large customs). The negative-input repair runs at apply time when needed; it is not a stored pair label.

**Stage B alone (default destination mix on every pair)** does **not** recreate the published after Use / value added / import / margins tables. Against published after:

| Table | Cells differing | Gross absolute error (million USD) | Largest cell (M) |
| --- | ---: | ---: | ---: |
| **Use** | 7,463 | **558,744** | 42,894 |
| Value added | 1,110 | 1,626,873 | 228,811 |
| Import | 1,430 | 24,612 | 2,053 |
| Margins | 18,201 | 4,532,043 | 228,358 |

On Use alone, published movement is **5,740** cells / **553,635M** gross, while **B** alone is wrong on **7,463** cells / **558,744M** gross — similar dollar scale, but the wrong cells and wrong amounts. **B** does get Make right on the classified source cells from **A**; it does not get the other four tables right.

**Stage B + C’s named specials, still without the residual layer,** does not close that gap. On Use it makes the gap **worse**. The residual layer is defined as “published after minus the algorithm without the residual,” so its size is exactly what remains after the specials:

| Table | Residual cells (above half-million) | Residual gross (million USD) |
| --- | ---: | ---: |
| **Use** | 26,466 | **695,997** |
| Value added | 1,126 | 823,778 |
| Import | 5,123 | 39,397 |
| Margins | 57,685 | 4,771,953 |
| **Total** | — | **~6.33 trillion** |

Use error by stage:

| Stage | Cells off published | Gross absolute error (M) |
| --- | ---: | ---: |
| **B** only | 7,463 | 558,744 |
| **B** + named specials in **C** (no residual) | 26,466 | 695,997 |

The named specials raise Use residual gross by about **25%** vs **B** alone. They help a few economically distinct pairs, but for 2017 as a whole they are not a general fix—most pairs stay on **B**, and the specials can still move the wrong pattern on the pairs they touch.

Where the residual layer spends its Use effort:

| Category | Cell count | Residual gross (M) |
| --- | ---: | ---: |
| Published moved **and** residual corrects | 4,902 | 424,373 (~77% of published 553,635M) |
| Published moved, residual ≈ 0 (algorithm already right) | 838 | — |
| **Algorithm moved a cell BEA left alone** | **21,564** | **260,132** |

Most Use residual cells (**21,564**) undo **spurious** moves from **B** / the named specials—not merely patch published changes the algorithm missed.

**The residual layer in stage C is essential for the 2017 match.** Without it, all four tables have large errors (Use alone ~696B gross). With it, they match exactly at the half-million bar. That layer is a fit to the published 2017 after tables, not a rule from BEA’s manual. It absorbs: **B** failing on the ~1,867 default pairs; named specials not matching BEA’s actual pattern; later balancing and other post-reallocation steps the manual does not encode; and spurious cell movement on cells BEA did not change.

### Bottom line

| Question | Answer |
| --- | --- |
| **Recreate 2017 after tables?** | **Yes** — Use / value added / imports / margins to half a million with full **A→B→C** (including the residual); Make passes the classified-source check from **A**/**B**. |
| **Stage B alone?** | **Necessary but insufficient.** Make sources match; the other four tables do not. Use alone: ~559B gross error. |
| **Named specials in C?** | **Small footprint** (13/1,880 pairs). Useful for a few cases; **do not** improve overall 2017 fit — Use error grows after applying them. |
| **Residual layer in C?** | **Carries almost all of the 2017 match.** ~696B gross on Use, ~6.3T across all four tables. Most Use residual cells (21,564) undo wrong algorithm moves rather than patch published deltas. |

**Interpretation:** the published 2017 after-redef tables can be rebuilt from before-redef inputs **only with a learned residual**. Stages **A** and **B** plus the named specials in **C** explain *structure* (which pairs redefine; a few special patterns) but not the cell-level after values. That matches the plan’s expectation that later balancing forces a residual for exact 2017 reconstruction.

### 3. What should we do instead?

**Mostly: stop treating B + named specials + residual as the production method for Use / value added / imports / margins.** Keep what **A** taught about *which* Make cells redefine; do not pretend the Chapter 9 default mix is the published operator.

Running **B** without the residual on later years would inject large wrong Use / value added / import / margins movement. Carrying the residual forward is already “learned 2017 cell movement”—closer to what issue #572 and `plan.md` Step 7 asked for (per-cell ratios from the 2017 before/after pair, applied to all four deliverables, with cell-by-cell 2017 equality).

That ratio path meets the written acceptance bar by construction, matches the style of the existing gross-output helper on `main`, and is much smaller than census → classify → default mix → specials → residual.

Tradeoff (still true): a 2017 ratio carry **describes** 2017 and freezes that year’s movement structure; it is **not** BEA’s production-function rule for another year. The reconstruction does **not** prove which frozen 2017 structure extrapolates best when there is no after-redef truth—only that the default destination mix is a poor description of the published tables, and that the residual (or an explicit ratio table doing the same job) is doing the real matching work.

Practical summary:

| Piece | Recommendation |
| --- | --- |
| **2017 acceptance** | Either path can pass; #572-style ratios pass cheaper and without pretending stage **B** works alone. |
| **Use / value added / imports / margins for later years** | Prefer **2017-learned cell movement** (ratio or residual carry) over **B** + named specials. |
| **Make for later years** | This reconstruction found pair list + share of that year’s secondary Make cell worked well; the follow-on ratio plan may still choose one GO-based idiom everywhere for consistency—see [`../redefinitions_ratio_implementation/ratio-plan.md`](../redefinitions_ratio_implementation/ratio-plan.md). |
| **Named specials in C** | Drop or keep as analysis only. Too few pairs, no overall 2017 benefit. |
| **Residual layer in C** | Treat it honestly as the main Use-side operator, or replace it with an explicit #572-style ratio table — same job, clearer story. |

So: **yes**, stop treating **B + C** as the method you believe in for Use-side tables, and realign Step 7 with the **#572 / `plan.md` ratio-carry** story. **No**, do not discard everything from **A**—knowing which Make pairs redefine was the part that behaved well.
