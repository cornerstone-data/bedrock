# Nowcast progress report — blocks against the published 2017 detail SUT

Generated from `bedrock.analysis.nowcasting.sections`
([#587](https://github.com/cornerstone-data/bedrock/issues/587)). Reference is
the published 2017 detail SUT throughout — the benchmark year's answer in the
framework we are building in.

Regenerate with:

```
uv run python -m bedrock.analysis.nowcasting.plots
uv run python -m bedrock.analysis.nowcasting.plots \
    --dpi 110 --out-dir bedrock/analysis/nowcasting/images --no-report   # the copies below
```

**Snapshot date:** 2026-08-26 for Steps 1 and 4, re-run after the AIES-sourced
`TRANS` and `TOP`'s purchaser-price residual (#611/#580) and the NAICS-2022
goods Crosswalk (#734); **Step 3 is unchanged from the 2026-08-24 run** and **Step 2
from the 2026-08-23 run**.

**Step 3 has landed ([#497](https://github.com/cornerstone-data/bedrock/issues/497)),** so the 402 × 402 Use interior is in
the report for the first time. Step 1 is a live `derive_initial_Y_pur`
(`NIPA_final_dom_uses_2017`, Trade `F04000`, `Inventories_2017` on `F03000`).
**Step 2 is live for the first time** ([#538](https://github.com/cornerstone-data/bedrock/issues/538)):
all three value-added rows are sourced, so every section in the diagnostic now
has a candidate and none carries `candidate=None`. Step 4 is a live
`derive_initial_supply_bridge`, and with **`TOP` and `SUB` (#580)** it populates
**all twelve bridge columns** for 2017. No component of that block is unsourced,
so `T013`, `T014`, `T015` and `T016` are evaluable rather than NaN.

⚠️ **Step 2's 100% / 100% is not comparable to the other rows of that
table.** Its methods take their within-group distribution from the 2017
benchmark, which is the reference, so a 2017 run tests the plumbing and not the
estimate. Read the Step 2 section before reading its numbers as quality.

---

## Where the build stands

| block | step | shape | reference populates | reference total | candidate | coverage | accuracy |
|---|---|---|---:|---:|---|---:|---:|
| `use_fd_detail_sut` | 1 — final demand | 402 × 19 | 1,253 cells | $22.24T | live | **96.0%** | **54.6%** |
| `use_va_detail_sut` | 2 — value added | 3 × 402 | 1,189 cells | $18.92T | live (all 3 rows) | **100.0%** | **100.0%** |
| `use_intermediate_detail_sut` | 3 — intermediate interior | 402 × 402 | 44,281 cells | $14.86T | live | **100.0%** | **100.0%** |
| `supply_bridge_detail_sut` | 4 — supply bridge | 402 × 12 | 3,202 cells | $111.28T | live (all 12 columns) | **99.2%** | **62.1%** |

**coverage** = of the cells the reference populates, how many we populate.
**accuracy** = of the cells we populate, how many land within tolerance.

The first three of those blocks are the whole of what a published 2017 detail
reference supports *outside* the two 402 × 402 interiors, and Step 3 is one of
those interiors. The reference columns are the denominator: they are what "done"
looks like, and they are known before the corresponding step is built.

⚠️ **Step 3's 100%/100% is not comparable to the other rows, and it must not be
read as "Step 3 is finished".** Steps 1 and 4 build their blocks from
independent sources and are scored against an answer they never saw. Step 3 is
*seeded from this very reference*, so at 2017 the carry factor is 1.0 everywhere
and the only move left is the column rescale. A perfect score here says the
plumbing is right, and says nothing at all about the years that matter. The
movement is scored on the published summary panel by
[`intermediate_structure_drift`](intermediate_structure_drift.py), not here.
✅ **§Which cells actually carry annual data is the answer to what this row
cannot say** — the same block coloured by where each cell's movement comes from,
where the honest number is **33.8% of dollars observed**, not 100%.

⚠️ **Coverage and accuracy moved in opposite directions this snapshot, and that
is the honest result.** `F03000` landing adds 256 populated cells against a
column total that is right to 2.3% — coverage 75.3% → 95.5% — while almost none
of those cells land within tolerance, so accuracy falls 69.8% → 55.1%. Nothing
that was matching stopped matching. A column moved from "not attempted" to
"attempted, and mostly wrong per commodity", which is progress that reads as a
regression on one of the two numbers.

---

## Step 1 — Use table, final-demand columns

`use_fd_detail_sut` · 402 commodities × 19 final-demand codes · tolerance
`rtol=0.013, atol=5e5, ramp=0.25`

![Step 1 final demand match](images/use_fd_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 6,383 | 660 | 537 | 56 | 2 |
| row totals | 22 | 116 | 260 | 4 | 0 |
| column totals | 0 | 17 | 2 | 0 | 0 |

| | |
|---|---:|
| coverage | 95.5% |
| accuracy | 55.1% |
| candidate grand total | $22.36T |
| reference grand total | $22.24T |
| grand total error | 0.57% |
| residual outside the frame | none |

### What the picture says that the totals do not

**No column is a whole-column `miss` any more.** All 19 final-demand codes are
sourced. Two are outside tolerance at the column total: `F04000` (+6.15% vs
published, #528) and `F03000` (−2.28%, #529). The other seventeen reconcile at
the column level, including the twelve government columns, which land cell for
cell.

**The two axes disagree, which is the point.** 264 of 402 row totals are outside
tolerance against 2 of 19 column totals — a column total can be right in
aggregate while its commodity split is wrong, and only the row strip shows it.
`F03000` is now the clearest instance of that in the frame.

**What moved since 2026-08-15** — coverage 75.3% → 95.5%, accuracy 69.8% →
55.1%, whole-column misses 1 → 0.

- **`F03000` is live** ([#529](https://github.com/cornerstone-data/bedrock/issues/529),
  merged in #666). The Inventories FBS generates, attributes and reaches
  `derive_initial_Y_pur`, replacing a hardcoded all-zero column.
- **A regression in `F02E00` was caught and fixed by this run.** The
  `Census_EC_PxI` catalog entry added in #666 was inserted *inside* the
  `BEA_PEQBridge` entry in `source_catalog.yaml`, taking `BEA_PEQBridge`'s
  `activity_schema: {bea: 2017, flat}` with it. Without a BEA activity schema
  the PEQ bridge stopped being sector-like, and the whole `F02E00` column
  collapsed onto `S00402` — 166 rows to 22, and $986B of a $978B column on used
  and secondhand goods, with every real equipment commodity at zero. `F02R00`
  lost $15B the same way. Restoring the block to `BEA_PEQBridge` returns both
  columns to their previous cell-for-cell state. `Census_EC_PxI` keeps
  `activity_schema: null`, which was a duplicate key under the broken layout and
  so had never taken effect — the `null` that does the work is the one on the
  attribution source in `Inventories_2017.yaml`. **The diagnostic found this, and
  no test did**, which is the failure mode this report exists to make visible.

### `F03000` — read the column, not its total

| | ours | published |
|---|---:|---:|
| column total | 31,936 | 32,682 |
| commodities populated | 256 | 258 |
| gross mass (sum of absolute cells) | 92,459 | 98,764 |

Sign agreement on the 254 commodities both sides populate: **69.7%**. Absolute
error against published gross: **101%**.

**The total is the one thing that is free here** — it equals NIPA CIPI by
construction — while gross mass is 3× net across 61 negative commodities. So
−2.28% at the column total says almost nothing about the allocation, and the
per-commodity numbers above are the real score. The largest cells outstanding
are all previously scoped rather than new:

| commodity | ours | published | why |
|---|---:|---:|---|
| `336411` aircraft | −288 | −6,314 | manufacturing branch needs the industry's own stage split (#664) |
| `S00402` used goods | 380 | 3,969 | used-goods value sits in wholesale lines routing to `S00401` (#665) |
| `211000` oil and gas | −4,754 | −7,577 | mining is still an equal-split placeholder (#660) |
| `325414` biological products | 41 | 2,484 | trade-branch product-line split |

**`F04000` still dominates the worst cells.** All ten worst cells in the frame
are trade: `336411`, `S00402`, `336412`, `339910`, `336111`. With the
NIPA-sourced columns reconciled and inventories in, the remaining Step 1 error
is concentrated in the trade column's commodity split and in `F03000`'s.

**`F01000` is mostly `match`.** Personal consumption is the densest live column,
and its remaining amber is genuine bridge-vs-Use disagreement rather than
attribution error: `F01000` reproduces `BEA_PCEBridge` cell for cell, with the
totals agreeing to $2M.

Longer form: [`About_table_match.md`](About_table_match.md).

⚠️ **One warning surfaces in every current run and has not been chased:** `Some
rows from BEA_NIPA assigned to multiple activity sets` for `U20405` line 342
(`DNPIRC`, NPISH) in `FD_PCE_npish`. It is a double-count risk inside `F01000`.

---

## Step 2 — Use table, value-added rows

`use_va_detail_sut` · 3 rows × 402 industries · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

![Step 2 value added match](images/use_va_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 17 | 1,189 | 0 | 0 | 0 |
| row totals | 0 | 3 | 0 | 0 | 0 |
| column totals | 1 | 401 | 0 | 0 | 0 |

| | |
|---|---:|
| coverage | **100.0%** |
| accuracy | **100.0%** |
| candidate grand total | $18.9165T |
| reference grand total | $18.9165T |
| grand total error | **0.000016%** |
| residual outside the frame | none |

All three rows are sourced ([#538](https://github.com/cornerstone-data/bedrock/issues/538)):
`V00100` from `NIPA_VA_compensation_2017`, `T00OTOP` from `NIPA_VA_othertax_2017`,
`V00300` from `NIPA_VA_surplus_2017`, stacked by `derive_initial_value_added`.

### Read this picture differently from the other two

⚠️ **A solid green block is the floor here, not an achievement**, and it would be
a mistake to read it as Step 2 being three times better than Step 4. Every one
of the three methods takes its *level* from NIPA and its *within-group
distribution* from the 2017 benchmark — which is the reference. With 2017 as
both anchor and target the shares are the identity, so the only things this can
catch are plumbing defects: mass lost between attribution groups, a row written
to the wrong axis, a sign dropped. It catches those, and it has nothing to say
about the movement series, because there isn't one yet.

What the block therefore certifies is narrow and worth having: the orientation
transpose, the `BEA_2017_Code` identity crosswalk rows, the 69-way compensation
control set, and the eight-line `V00300` assembly all carry mass end to end
without losing or misrouting any of it. The grand total is off by
**0.000016%** — three million dollars on eighteen trillion, which is BEA's own
rounding stacked three rows deep.

**The test with real content is the same picture for a later year**, where the
shares stop being the identity. It cannot be drawn yet: no 2018-2024 files
exist, because compensation's QCEW movement series is blocked on the
`FBS_outside_flowsa` attribution-source gap. Until then this section is a
regression guard.

### The 17 absent cells are the structural zeros, and one of them was a bug

Neither side populates them, so they are genuinely nothing to say — but the list
is worth reading, because it is an accounting statement rather than a data gap:

| row | absent on | why |
|---|---|---|
| `V00100` | `4200ID`, `531HSO` | customs duties is a synthetic industry with no employees; owner-occupied housing has no wage bill |
| `V00300` | `4200ID`, `814000` | private households have no operating surplus |
| `T00OTOP` | `4200ID`, `814000`, and **11** government codes | a tax levied by government and remitted by a government producer nets out |

⚠️ **Eleven government codes, not the canonical ten** — and finding that is what
this picture bought on its first run. `tax_axis_conversion`'s prefix rule
(`S00`, `G`) names ten; the eleventh is the US Postal Service, `491000`, a
federal government enterprise whose BEA code is shaped like an industry's. Its
published `T00OTOP` is zero like the other ten, so it was landing on zero only
because the benchmark weight happened to be zero, not because the rule excluded
it. Now named in `write_value_added_crosswalk.py`. **No 2017 number moves** —
which is precisely why it would not have been found by a total.

### The valuation, which is still the thing to get right

`T00OTOP` is *other* taxes on production less subsidies at basic prices. It is
**not** the MUT's `V00200`, which is taxes on production *and imports* at
producer prices. The section deliberately does not alias the two, so a candidate
built on `V00200` would show as a `MISS`/`EXTRA` pair rather than a bad match.
⚠️ Note that `BEA_GDPbyIndustry`, added alongside these methods, publishes
exactly `V00200` — its taxes column is not this row.

---

## Step 3 — Use table, the intermediate interior

`use_intermediate_detail_sut` · 402 commodities × 402 industries · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

![Step 3 intermediate block match](images/use_intermediate_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 117,323 | 44,281 | 0 | 0 | 0 |
| row totals | 36 | 366 | 0 | 0 | 0 |
| column totals | 2 | 400 | 0 | 0 | 0 |

Grand total $14.856T against $14.856T, off by **0.002%**.

**Read the picture as a coverage map, not a score.** The candidate is
`derive_initial_U_intermediate`: the published 2017 interior column-normalised,
carried on the detail commodity price ratio at θ = 1, and rescaled to
`GO_producer − VAPRO_seed`. At 2017 every carry factor is 1.0, so the block is
the reference put through a per-column rescale — everything green is what that
should look like, and anything else would have been a plumbing bug.

⚠️ **The rescale is not the identity, and the residual is BEA's own rounding.**
Published `T005` is one rounded number; the interior sums 402 separately rounded
cells to a different one — $350M on $14.9T, at most $13M on a column. A *small*
column wears that as a large fraction, so `atol` is what carries those cells
rather than `rtol`: `334610` is $482M of intermediates and is rescaled by 1.05%,
the largest relative error in the block, while the largest absolute error is
$6.0M on a $19.2B cell.

✅ **The seven published negative cells survive**, and the two structurally empty
columns — `4200ID` customs duties and `814000` private households — stay empty.

⚠️ **The column control's `VAPRO` is no longer a seed.** It was, when this
section was first written and Step 2 was unbuilt; Step 2 now supplies `VAPRO`'s
split for 2017-2024 (#538, merged) and Step 5 pins it as T18. ⚠️ **`GSLG` at
18.3% low in 2022 was the column *level*, not
[#578](https://github.com/cornerstone-data/bedrock/issues/578)** — #578 is the
commodity *mix* inside the government columns and is a separate, later job. And
**θ is no longer #497's 1.0**: it is fitted per span, 0.75 off the 2021-22 surge
and 0.0 across it ([#699](https://github.com/cornerstone-data/bedrock/issues/699)).

### Which cells actually carry annual data

The match picture above says the block reproduces 2017. It says nothing about
where the block's movement *away* from 2017 comes from, and that is the only
question Step 3 is answerable on.

![Step 3 seed coverage](images/intermediate_seed_coverage_2022.png)

**How to read it.** This is the same 402 × 402 intermediate block as the match
picture — commodities down the side, the industries that buy them across the
top — but coloured by **provenance** rather than by score. Both axes are
regrouped into contiguous sector bands, in the order the sectors fall in the
table, because BEA's detail order interleaves them enough that the seeds would
otherwise read as scattered speckle rather than as the blocks they are. Every
cell is in one of three states:

- **white** — no cell. The 2017 benchmark is zero here, so there is nothing to
  seed and nothing to carry. This is 73% of the raster and it is why the
  cell-count share and the dollar share are so far apart.
- **grey** — carried. The cell keeps its 2017 structure and moves only on the
  commodity price carry. No annual source observes it.
- **green** — seeded. An annual survey observes this cell's movement.

**The green is graded, and that is the part of the picture that does the work.**
"Seeded" is not one thing. A survey datum almost never lands on a single cell:
`Purchased freight transportation` is one number in the Service Annual Survey
that has to be spread over 8 BEA commodities, and ERS publishes one farm sector
whose index drives all 10 farm columns at once. So the green carries `N` — **how
many cells share the single observation behind this cell** — computed as the
commodities a datum is split across times the industry columns the same index
drives. The darkest green is `N = 1`, where the datum *is* the cell: the
Economic Census reported that material, for that industry, and it resolves to
exactly one BEA commodity. The ramp is logarithmic and saturates at 64.

Reporting a cell fed by a number shared with 79 others in the same colour as one
fed by a number about it alone would flatter the coverage badly, which is the
whole reason for the gradation rather than a flat green.

**What to look at first.** The dark diagonal through manufacturing is the
Economic Census materials detail — the densest cell-specific evidence in the
block. The pale green horizontal bands low in the picture are services rows
reaching across nearly every buying industry on a small number of shared survey
items: wide, but weak per cell. And the three solid grey column blocks —
construction, trade, government — are where no annual source reaches at all.

⚠️ **One figure, not one per year.** The mappings behind `N` do not depend on
the year, so the map is nearly year-invariant; measured, the seeded share moves
**1.9 points across 2020-2023** (35.7 / 35.7 / 33.8 / 34.6), which is inside the
band where one picture stands for the span. ⚠️ **2018 and 2019 are the
exception and are excluded on purpose** — the SAS expense panel jumps straight
from 2017 to 2020, with no 2018 or 2019 vintage in it, so services and
transportation hold their 2017 columns entirely and the block reads **19.5%**
rather than 33.8%. A 2018 figure would be showing the missing SAS vintages, not
the seeds. `--check-years` re-measures this rather than trusting it.

Dollar-weighted, at 2022:

| band | columns | $M | seeded | of which N = 1 | median N |
|---|---:|---:|---:|---:|---:|
| agriculture | 13 | 272,102 | 78.7% | 0.0% | 20 |
| mining | 8 | 195,477 | 31.5% | 22.4% | 1.5 |
| utilities | 3 | 160,406 | 22.3% | 0.0% | 3 |
| construction | 12 | 737,745 | — | — | — |
| manufacturing | 231 | 3,561,508 | **67.1%** | **47.4%** | 4 |
| trade | 20 | 1,545,723 | — | — | — |
| transportation | 9 | 591,808 | 40.9% | 19.2% | 8 |
| services | 94 | 6,494,741 | 31.5% | 5.3% | 9 |
| government | 8 | 1,222,147 | — | — | — |
| **total** | **402** | **14,856,988** | **33.8%** | | **7** |

**33.8% of the block's dollars are observed; 9,905 of 44,281 non-empty cells.**

⚠️ **Read the dollars, not the cell count, and read this against the column
count rather than instead of it.** 330 of 402 *columns* move off the 2017 shape
— that is the number in the plan and it is the optimistic reading, because a
seeded column is not a column of seeded cells. `materials_seed` returns the
whole manufacturing column renormalised, but the census only *observes* the
materials rows; the rest of that column is the 2017 mix rescaled, which is
carried. 33.8% of dollars is the honest reading of the same fact.

Three things the picture says that the totals do not:

1. ✅ **Manufacturing is the strong block and it is strong in the right way** —
   67.1% of its dollars observed and **47.4% at `N = 1`**, the only block where
   most of the evidence is cell-specific. That is the Economic Census materials
   detail, and it is visible as the dark diagonal.
2. ⚠️ **Agriculture's 78.7% is the widest coverage and the weakest evidence** —
   median `N` of 20, nothing at all at `N = 1`. ERS publishes one farm sector,
   so every farm column moves identically in the commodities ERS names. High
   coverage and low specificity are not the same property.
3. ❌ **Construction, trade and government are entirely grey** — $3.5T, 24% of
   the block, with no annual observation of their input mix at all. Those are
   recorded verdicts rather than gaps in the work: trade is a measured no-go on
   the benchmark holdout, construction has no Census product that splits its one
   undifferentiated 51% cell, and government is #578. Each verdict's reopening
   condition is in
   [`intermediate_estimation_plan.md`](intermediate_estimation_plan.md).

Reproduced by [`seed_coverage.py`](seed_coverage.py) — `--check` re-asserts the
map, `--check-years` re-measures the one-figure claim, and `--check-palette`
re-runs the colour separation (worst pair dE 29.0 against a floor of 27, binding
on grey against the light end of the ramp under deuteranopia).

---

## Step 4 — Supply table, bridge to purchaser value

`supply_bridge_detail_sut` · 402 commodities × 12 bridge codes · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

![Step 4 supply bridge match](images/supply_bridge_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 1,592 | 1,958 | 1,211 | 33 | 30 |
| row totals | 3 | 200 | 192 | 1 | 6 |
| column totals | 0 | 8 | 2 | 2 | 0 |

| | |
|---|---:|
| coverage | 99.0% |
| accuracy | 61.8% |
| candidate grand total | $111.39T |
| reference grand total | $111.28T |
| grand total error | **0.092%** |

### What moved since 2026-08-24

Coverage 99.0% → **99.2%**, accuracy 61.8% → **62.1%**, grand total error
0.092% → **0.048%**, and `T013`/`T016` lost their last whole-column miss. None
of that is Step 4a: it is the **Trade FBSs rebuilt on the NAICS-2022 goods
Crosswalk** ([#734](https://github.com/cornerstone-data/bedrock/issues/734)),
which moved `MCIF` (misses 27 → 22, coverage 90.9% → 92.6%) and carried through
`T013` and `T016`.

Two method changes landed with it and neither shows in the 2017 scores, because
both are replays in the benchmark year:

- **`TRANS` reaches 2023** ([#611](https://github.com/cornerstone-data/bedrock/issues/611)),
  sourced from **AIES**, which continues SAS Table 8 under the same eleven group
  names. ⚠️ 2024 is a *release date*, not an engineering gap — truck and pipeline
  are 79.7% of the column and the AIES 2024 endpoints return 204/400.
- **`TOP`'s residual stopped being frozen** ([#580](https://github.com/cornerstone-data/bedrock/issues/580)).
  The other 70.2% of the column now moves on a purchaser-price base `T013 + T014`
  rather than on 2017 shares. ⚠️ **So `TOP` now depends on margin coverage**, and
  2024 holds 2023's shares. The two constructions differ by 6.3% of the 2020
  column and 7.3% of the 2024 one.

### What moved since 2026-08-21

Coverage 65.0% → **99.0%**, accuracy 54.1% → **61.8%**, whole-column misses
6 → 2, grand total error 34.5% → **0.092%**. The block went from two thirds
sourced to complete. Two columns landed, and with them the four subtotals:

- **`TOP` is sourced** ([#580](https://github.com/cornerstone-data/bedrock/issues/580),
  this branch) — NIPA T30500 taxes on products less customs duties for the
  annual total, ten named NIPA product lines placed on their own commodities
  (29.8% of the column), and the general-sales-tax residual on frozen 2017
  shares. **2017-2024.** (⚠️ The residual stopped being frozen on 2026-08-26 —
  see the entry above.)
- **`SUB` is sourced** (same issue) — NIPA T31300 for the total, each commodity
  anchored on its published 2017 value and moved by its own NIPA type line.
  ⚠️ 2020 and 2021 replace the `other` type — 84% of the column in those years —
  with BEA's published allocation of PPP across industries, because moving the
  2017 `other` vector would put ~377bn of pandemic support on insurance carriers
  and freezing the whole column would put ~420bn of it on housing. **2017-2024.**

⚠️ **The 339 of 339 and 15 of 15 are replays, not scores.** Both columns are
anchored on the published 2017 table, so 2017 reconstructs it by construction
and these rows check arithmetic, not accuracy — the same caveat `TRADE` carries
for the same reason. 2018-2024 have no published column to score against at all.
What the construction buys in those years is that the parts move on NIPA's own
annual measurement instead of on the column's growth rate: for `TOP` that is
29.8% of the column, differing from the default proposal by 3.7% of 2020 and
5.8% of 2024; for `SUB` it is the whole column, and the difference there is the
420bn above.

### Per-column, which is the only way to read this block

| code | absent | match | partial | miss | extra | coverage | accuracy |
|---|---:|---:|---:|---:|---:|---:|---:|
| `T007` | 3 | **399** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `MCIF` | 102 | 26 | 250 | 22 | 2 | 92.6% | 9.4% |
| `MADJ` | 396 | **6** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `T013` | 1 | 225 | 176 | 0 | 0 | **100.0%** | 56.1% |
| `TRADE` | 128 | **274** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `TRANS` | 139 | 6 | 257 | 0 | 0 | **100.0%** | 2.3% |
| `T014` | 120 | 132 | 150 | 0 | 0 | **100.0%** | 46.8% |
| `MDTY` | 194 | 67 | 119 | 4 | 18 | 97.9% | 32.8% |
| `TOP` | 63 | **339** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `SUB` | 387 | **15** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `T015` | 59 | 279 | 63 | 0 | 1 | **100.0%** | 81.3% |
| `T016` | 3 | 205 | 188 | 0 | 6 | **100.0%** | 51.4% |

**`T007`, `TRADE` and `TOP` are exact.** 399 of 399, 274 of 274 and 339 of 339
populated cells land inside tolerance — not "close", but every cell. `T007`'s
column total is 33,772,550 against a published 33,772,566: **$16M apart on
$33.8T**, which is BEA's own rounding grain. `TOP`'s is 716,925 against 716,926,
and that one is NIPA's reading of the same quantity rather than a fit.

**`TRANS` is the opposite shape, and that is the finding.** It reaches every
commodity the reference populates — 100% coverage, zero misses — but only 6 of
263 cells land inside tolerance. The transport margin is arriving at the right
*commodities* and the wrong *amounts*. Neither coverage nor the column total can
see this; the per-cell picture is the only thing that can, and this is the
column to work next.

**⚠️ `TRADE` and `TRANS` score as whole-column `miss` while being right at the
cell level, and the column total is the wrong test for them.** Both columns net
to **exactly zero** by construction — a margin is a redistribution, so it must.
The published columns net to $1M and $10M, which is rounding residue on a
$33.8T table, and `atol` is $0.5M. So the column-total strip marks a
cell-for-cell exact column as a miss. Read the per-column table above, not the
column-total strip, for these two.

**The four subtotals are now evaluable, and they inherit rather than add.**
`T015` is the strongest of them at 279 exact of 342 — it consumes `MDTY`, `TOP`
and `SUB`, two of which are exact, so its 63 `partial` cells are `MDTY`'s. `T013`
and `T016` carry `MCIF`'s +1.29% into 184 and 192 `partial` cells respectively.
None of the four introduces error of its own, which is what the identity check
is for.

**Only two whole-column misses remain, and both are the zero-sum margin columns**
— `TRADE` and `TRANS` net to exactly zero by construction against published
residues of $1M and $10M, so the column-total strip marks a cell-for-cell exact
column as a miss. `MCIF` is the one genuine `partial` at the column total, and
its 27 cell misses are the only real misses left in the block.

The right-hand block of the Supply table: imports, margins, taxes and the
subtotals carrying a commodity from domestic output at basic value to total
supply at purchaser value. Reference totals, in millions:

| code | description | 2017 total |
|---|---|---:|
| `T007` | Total commodity output (domestic, basic) | 33,772,566 |
| `MCIF` | Imports of goods and services, CIF | 2,649,430 |
| `MADJ` | CIF/FOB adjustment on imports | −23,116 |
| `T013` | **Total supply, basic** | **36,398,867** |
| `TRADE` | Trade margins | 1 |
| `TRANS` | Transportation costs | 10 |
| `T014` | **Total margins** | **1** |
| `MDTY` | Import duties | 38,507 |
| `TOP` | Taxes on products | 716,926 |
| `SUB` | Subsidies (stored negative) | −59,876 |
| `T015` | **Taxes less subsidies** | **695,565** |
| `T016` | **Total supply, purchaser** | **37,094,434** |

**`T014` sums to 1 across the whole economy.** Margins net to nothing in
aggregate while being large and offsetting per commodity. No scalar check on
this block can work; a per-commodity picture is the only thing that can. This
block is the strongest case in the project for the diagnostic existing.

Subtotals are kept in the frame rather than stripped, because they are the
Supply identities and a subtotal disagreeing with its own components is exactly
what is worth seeing:

```
T013 = T007 + MCIF + MADJ
T014 = TRADE + TRANS
T015 = MDTY + TOP + SUB
T016 = T013 + T014 + T015
```

---

## Caveats

**`F04000` / `MCIF` do not clear the #557 bars.** National F040 is +6.15%;
import Pearson on non-specials is 0.84 vs ≳ 0.85. Hole rules sit on #528.
Whether to apply a national ITA (or other) control is #647.

**`F03000` is sourced but not validated per commodity.** The column total is
right by construction; the allocation is at 69.7% sign agreement and 101%
absolute error against published gross. Mining and farm are equal-split
placeholders (#660), manufacturing needs per-industry stage shares (#664), and
`S00402` is an order of magnitude short (#665). Treat the column as a first
pass, not as a solved block.

**`TRANS` lands on the right commodities and the wrong amounts.** 100% coverage,
zero misses, but 6 of 263 cells inside tolerance. The receiving sets and the
per-mode allocation bases are doing their job; the levels are not. This is the
next Step 4c column to work, and it is invisible to every check except the
per-cell one.

**The column-total strip cannot score a margin column.** `TRADE` and `TRANS`
net to exactly zero by construction, so they read as whole-column misses against
a published total that is $1M/$10M of rounding residue. Two of the eight
remaining whole-column misses are this artefact rather than unsourced columns.

**Nothing here is a rollup.** Every number is BEA 2017 detail. Margins and
redefinitions net out at summary and above, so an aggregate view of these blocks
would pass on data these pictures show to be broken.
