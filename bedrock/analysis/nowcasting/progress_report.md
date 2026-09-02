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

**Snapshot date: 2026-08-28.** All five sections were re-run together, at
`f02be4f8`, so every number below comes from one build rather than from three
snapshots taken a week apart. That is the first time this report has been
internally consistent, and two of the four previously reported rows moved
enough that the old mixed-vintage table would have been misleading.

**Updated 2026-09-02: the build passed the seeds.** Between the snapshot above
and this update, Step 5 balanced all seven years, Step 6 converted every
balanced year to the before-redefinitions MUT quartet, and Step 7 converted
those to after-redefinitions — all stored, identity-gated, and on GCS. The
five seed sections below still carry the 2026-08-28 snapshot (the seeds have
not changed); §Step 5 and the new §Steps 6 and 7 carry the 2026-09-02 state,
including the first product-level check — industry output from the stored
tables against the BEA gross-output series.

**Regenerated 2026-09-02, second pass: the section pictures now score the
FINAL balanced product.** The five SUT sections' candidates switched from the
seed derivations to the newest stored `Balanced_Detail_*` parquet (soft
protocol, vintage `163db0e`) — the seeds represented the state after earlier
steps, and the report's question is what the shipped table looks like. The
seed candidates stay importable in `sections.py` for seed-level diagnosis.
The regenerated 2017 scores:

| block | seed cov/acc (08-28) | balanced cov/acc (09-02) | total diff |
|---|---:|---:|---:|
| Step 1 final demand | 88.5% / 59.1% | 89.9% / 50.9% | 0.071% |
| Step 2 value added | 99.9% / 79.4% | 99.9% / 67.7% | 0.000% |
| Step 3 intermediate | 100.0% / 100.0% | 100.0% / **57.2%** | 0.063% |
| Step 4a domestic output | 100.0% / 99.6% | 100.0% / **74.5%** | 0.009% |
| Step 4 supply bridge | 99.2% / 62.3% | 99.4% / 49.5% | 0.018% |

⚠️ **Read the two 100%→57%/75% drops as the RAS doing its job, not breaking.**
The seed interiors reproduced published 2017 because they were seeded from it;
the balance then moved interior cells to absorb the seed's real gaps (the
supply-use identity closed from a 2.3% fitted gap to exact, F03000/F04000's
misses spread into the interior). Totals and identities are exact everywhere —
what these scores now measure is **where the balance put the reconciliation**,
which is #755's question, made visible per cell for the first time. The
per-section narratives below retain the 2026-08-28 seed-run numbers where
they discuss seed behaviour; the pictures are the balanced product.

**Three things are new in this run.**

1. **Step 4a is in the report for the first time.** `supply_output_detail_sut`
   has been a registered section since #570 merged on 2026-08-21 and was never
   drawn here. It is the 402 × 402 Supply interior and it is the strongest
   block in the project — 100.0% coverage, 99.6% accuracy.
2. **Step 2 is five rows, not three.** #740 added `T00TOP` and `T00SUB`, and
   the block is no longer the solid green this report described. It scores
   79.4%, and **every cell of the shortfall is `T00TOP`**.
3. **Step 1 moved as predicted, and the prediction can now be replaced with the
   measurement.** `F03000`'s rebuild (#746) took coverage 95.5% → **88.5%** and
   accuracy 55.1% → **59.1%**.

**Every section has a live candidate and none carries `candidate=None`.**
Step 1 is a live `derive_initial_Y_pur` (`NIPA_final_dom_uses_2017`, Trade
`F04000`, `Inventories_2017` on `F03000`). Step 2 is
`derive_initial_value_added`, all five rows
([#538](https://github.com/cornerstone-data/bedrock/issues/538),
[#740](https://github.com/cornerstone-data/bedrock/pull/740)). Step 3 is
`derive_initial_U_intermediate`
([#497](https://github.com/cornerstone-data/bedrock/issues/497),
[#742](https://github.com/cornerstone-data/bedrock/pull/742)). Step 4a is the
`Detail_Supply_Mix_2017` FBS
([#570](https://github.com/cornerstone-data/bedrock/issues/570)). Step 4 is
`derive_initial_supply_bridge`, and with `TOP` and `SUB` (#580) it populates all
twelve bridge columns, so `T013`, `T014`, `T015` and `T016` are evaluable rather
than NaN.

⚠️ **Two of the five scores are circular and one is near-circular; read §Where
the build stands before ranking the blocks by them.** Step 3 is seeded from this
very reference, Step 4a shares its detail mix with it, and three of Step 2's
five rows take their within-group distribution from it. A 2017 run of those
tests the plumbing, not the estimate.

✅ **Step 5 has now run on the nowcast seeds — the span is balanced.** The
warning that stood here through 2026-08-28 ("never been run on a nowcast
seed") is retired: see §Step 5 for the balance and §Steps 6 and 7 for the
stored tables it produced.

---

## Where the build stands

| block | step | shape | reference populates | reference total | candidate | coverage | accuracy |
|---|---|---|---:|---:|---|---:|---:|
| `use_fd_detail_sut` | 1 — final demand | 402 × 19 | 1,253 cells | $22.24T | live | 88.5% | 59.1% |
| `use_va_detail_sut` | 2 — value added | 5 × 402 | 1,553 cells | $19.61T | live (all 5 rows) | **99.9%** | 79.4% |
| `use_intermediate_detail_sut` | 3 — intermediate interior | 402 × 402 | 44,281 cells | $14.86T | live | **100.0%** | **100.0%** |
| `supply_output_detail_sut` | 4a — domestic output interior | 402 × 402 | 5,080 cells | $33.77T | live | **100.0%** | **99.6%** |
| `supply_bridge_detail_sut` | 4 — supply bridge | 402 × 12 | 3,202 cells | $111.28T | live (all 12 columns) | **99.2%** | 62.3% |

**All five blocks now have a live candidate**, and between them they cover the
whole of what a published 2017 detail reference supports: both 402 × 402
interiors and all three trailing blocks.

⚠️ **Read the two bolded 100% rows and the 99.6% as three different claims.**
Step 3 is *seeded from this very reference*, so its 100% is circular. Step 4a is
*near*-circular — the same 2017 detail mix appears on both sides, so its 99.6%
says the build has not broken, not that the method is right. Only Steps 1, 2 and
4 are scored against an answer they never saw, which is why their numbers are
the low ones. **The ranking of these five rows is close to the reverse of the
ranking of how much each is actually known.**

**coverage** = of the cells the reference populates, how many we populate.
**accuracy** = of the cells we populate, how many land within tolerance.

### Which years each block reaches

The scores above are all 2017, because 2017 is the only year with a published
detail answer. The question the build schedule turns on is a different one —
how far forward each block runs at all:

| block / column | years | what stops it |
|---|---|---|
| Step 1 — NIPA final domestic uses | 2017-2024 | — |
| Step 1 — `F04000` exports | 2017-2024 | — |
| Step 1 — **`F03000` inventories** | **2017-2023** | census-vintage mix; `U50705BU1` |
| Step 2 — value added, all 5 rows | 2017-2024 | — |
| Step 3 — intermediate interior | 2017-2024 | — |
| Step 4a — domestic output | 2017-2024 | — |
| Step 4 — `T007` `MCIF` `MADJ` `MDTY` `TOP` `SUB` | 2017-2024 | — |
| Step 4 — **`TRADE`** | **2017-2023** | Census annual trade release |
| Step 4 — **`TRANS`** | **2017-2023** | AIES release (204/400 for 2024) |
| ⇒ `T014`, `T016` | **2017-2023** | inherited from the two margin columns |

✅ **So the SUT series is 2017-2023, and every one of the three binding
constraints is a publication date rather than an engineering gap.** Nothing in
the list is waiting on work; all three wait on a data release. 2024 is a Phase 2
question, alongside the BEA annual update.

⚠️ **`F03000` only joined this list on 2026-08-28.** `Inventories_<year>.yaml`
shipped 2017-2023 with #746, but `derive_initial_Y_pur` still gated on
`if year == 2017`, so the column was all-zero for 2018-2023 — the method was
built and its consumer was never switched on. Now wired. See §`F03000` as a
series.

The reference columns are the denominator: they are what "done" looks like, and
they are known before the corresponding step is built.

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
where the honest number is **35.7% of dollars observed**, not 100%.

⚠️ **Coverage and accuracy have now traded places twice, in opposite senses
each time, and both moves come from the same column.** On
2026-08-24, `F03000` landing took coverage 75.3% → 95.5% and accuracy 69.8% →
55.1%: a column went from "not attempted" to "attempted, and mostly wrong per
commodity". On 2026-08-28 its rebuild (#746) took coverage 95.5% → **88.5%** and
accuracy 55.1% → **59.1%**: the same column went from "attempted everywhere on
an equal split" to "attempted only where a source observes it".

✅ **Neither move is a quality change, and reading either number alone would get
the sign wrong.** Nothing that was matching stopped matching in either run. The
pair only makes sense read together, which is the argument for keeping both
numbers rather than blending them into one score.

---

---

## Where the numbers come from — provenance across all five blocks

New on 2026-08-28, from
[`block_provenance.py`](block_provenance.py). §Step 3 has carried a provenance
map since 2026-08-27 — the 402 × 402 intermediate block coloured by where each
cell's movement comes from. This extends the same question to the other four
blocks, so the whole SUT pair can be read on one axis.

**The states**, generalising Step 3's three:

| | |
|---|---|
| **primary** | a source observes *this cell*. `k = 1` — the datum **is** the cell |
| **allocated** | a source observes an aggregate *containing* this cell, spread onto it by a weight. `k > 1` |
| **carried** | no annual source; the cell holds its 2017 structure |
| **missing** | the reference has a value and we produce none, or the method declines to allocate a line it can name |
| absent | neither side populates it |

`k` counts **how many cells share the single source datum behind this cell**.
It is the same measurement as Step 3's `N`, and primary-vs-allocated is just its
two ends.

✅ **Derived, not asserted, for three of the four blocks.** The FlowBySector
rows for final demand, value added and inventories retain `Table` / `Line` /
`Code` — the NIPA table, line and series each row was built from. That triple
*is* the datum's identity, so `k` is counted off the build itself. ⚠️
`Detail_Supply_Mix` and `Trade_Exports` carry only `MetaSources` with no per-row
key, so `supply_mix`'s `k` is measured off the summary→detail crosswalk instead
and `F04000`'s is declared. `--check` prints which blocks are which.

![Use table provenance](images/use_table_provenance_2017.png)

![Supply table provenance](images/supply_table_provenance_2017.png)

**How to read them.** Each table is drawn in its own geometry: the Use table
puts final demand to the right of the intermediate interior and value added
underneath it; the Supply table puts the bridge to the right of the
domestic-output mix. Both axes are grouped into contiguous sector bands, as
§Step 3's map is. ⚠️ **The thin blocks are not to scale** — value added is 5
rows drawn at the height of about 20, or it would be invisible.

**What to look at first.** The `V00300` and `T00OTOP` rows of the value-added
strip are a flat, uniform pale green across all 402 industries: one datum each,
spread everywhere. `V00100` directly above them is mottled with dark cells,
which is a movement series with cell-specific evidence in it. In the final
demand panel the government equipment columns are solid pale blocks, and
`F03000` is the one column carrying visible purple.

### The table

Subtotals (`T013`-`T016`) are excluded: they are their components summed, so
counting them would count the same evidence twice.

| block | populated cells | primary | allocated | carried | missing | observed $ | **primary $** | median `k` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| final demand | 1,258 | 78 | 1,036 | 0 | 144 | 100% | **48.1%** | 4 |
| value added | 1,586 | 17 | 1,568 | 0 | 1 | 100% | **7.9%** | **29** |
| supply mix | 5,080 | 273 | 4,807 | 0 | 0 | 100% | **20.4%** | 14 |
| supply bridge | 1,804 | 0 | 1,505 | 274 | 25 | 85.4% | **0.0%** | 4 |
| intermediate (§Step 3) | 44,281 | — | — | 33,883 | 0 | 35.7% | — | 7 |

⚠️ **"Observed" is a much weaker word than it looks in the first four rows.**
Every populated cell of final demand, value added and the supply mix traces to
*some* source, so all three read 100%. That is not a quality claim — it is the
statement that nothing is invented. The column that carries the information is
**primary $**, and it falls as low as **7.9%** on value added and **0%** on the
bridge.

### ⚠️ The match table and this table disagree about which columns are good

**And both are right.** A column allocated from one NIPA line onto the 2017
benchmark mix reproduces 2017 *exactly* — the mix is what it was built from — so
it scores 100% on the match while resting on a single observation.

The clearest cases are the columns §Step 1 reports as flawless:

| column | match accuracy | median `k` | what that means |
|---|---:|---:|---|
| `F10E00` | **100%** | **72** | one NIPA datum spread over 72 commodities |
| `F07E00` | **100%** | **68** | one datum, 68 commodities |
| `F06E00` | **100%** | **54** | one datum, 54 commodities |
| `F10S00`, `F06S00` | **100%** | 11 | |
| `F01000` PCE | 99.6% | 3 | 61.4% of its dollars are primary |
| `F02S00` | 90.9% | **1** | the most specific column in the block |

**`F01000` is the honest inversion.** PCE scores *worse* on the match than the
government equipment columns and is built on far better evidence — median `k` of
3 against 72, and **61.4% of its dollars primary** against 0%.

⚠️ **Value added is the extreme case, and it is the block this report has twice
called a solid green floor.** Now it has a number:

| row | match accuracy | median `k` | primary $ |
|---|---:|---:|---:|
| `V00300` operating surplus | **100%** | **400** | 0% |
| `T00OTOP` other taxes | **100%** | **389** | 0% |
| `V00100` compensation | **100%** | 11 | **15.0%** |
| `T00SUB` | **100%** | 8 | 0% |
| `T00TOP` | 8.1% | 20 | 0% |

✅ **`V00300` is one NIPA line spread across 400 industries, and it scores
100%/100%.** That is the sharpest statement available of why a 2017 match score
is not a quality measure — and it is consistent with what §Step 2 says in
prose, that `V00300`'s shares are frozen at 2017 and drift **12.51%** by 2022.
`V00100` is the only value-added row with cell-specific evidence anywhere in it,
which is exactly what a QCEW-driven movement series should look like.

⚠️ **`T00TOP` is the one row where the two tables agree** — worst on the match
at 8.1% *and* uninformative on provenance. When a row is both, the problem is
the method, not the grader.

### Final demand, on its own

![Final demand provenance](images/final_demand_provenance_2017.png)

| code | primary | allocated | missing | absent | $M | median `k` | primary $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| `F01000` PCE | **56** | 203 | 0 | 143 | 13,718,607 | 3 | **61%** |
| `F02E00` | 1 | 106 | 0 | 295 | 1,373,369 | 7 | 1% |
| `F02N00` | 1 | 13 | 1 | 387 | 906,246 | 3 | 48% |
| `F02R00` | 4 | 15 | 1 | 382 | 772,526 | 11 | **77%** |
| `F02S00` | **9** | 2 | 1 | 390 | 600,073 | **1** | **84%** |
| **`F03000`** | 2 | 159 | **98** | 143 | 91,193 | 7 | 1% |
| **`F04000`** | 0 | 302 | **43** | 57 | 2,211,333 | 4 | 0% |
| `F06C00` | 1 | 0 | 0 | 401 | 599,358 | **1** | **100%** |
| `F06E00` | 0 | 54 | 0 | 348 | 77,264 | **54** | 0% |
| `F06N00` | 1 | 3 | 0 | 398 | 63,714 | 3 | 79% |
| `F06S00` | 0 | 11 | 0 | 391 | 7,964 | 11 | 0% |
| `F07C00` | 1 | 0 | 0 | 401 | 379,143 | **1** | **100%** |
| `F07E00` | 0 | 68 | 0 | 334 | 19,203 | **68** | 0% |
| `F07N00` | 1 | 3 | 0 | 398 | 109,262 | 3 | 74% |
| `F07S00` | 0 | 8 | 0 | 394 | 15,527 | 8 | 0% |
| `F10C00` | 0 | 3 | 0 | 399 | 1,737,213 | 3 | 0% |
| `F10E00` | 0 | 72 | 0 | 330 | 73,200 | **72** | 0% |
| `F10N00` | 1 | 3 | 0 | 398 | 44,768 | 3 | 48% |
| `F10S00` | 0 | 11 | 0 | 391 | 304,902 | 11 | 0% |

⚠️ **The two columns with missing cells are the two the match table also
faults** — `F03000`'s 98 and `F04000`'s 43 are the 141 of 144 misses §Step 1
attributes to them. This is the one place the two views agree.

✅ **`F02S00` and the two `*C00` structures columns are the best-evidenced in
the block** — median `k = 1`, 84-100% of dollars primary. They are also small.

⚠️ **`F10C00` is 1.7T at `k = 3` with 0% primary**, the largest column in the
block after PCE, and every dollar of it is allocated.

### Value added, on its own

![Value added provenance](images/value_added_provenance_2017.png)

Transposed relative to the others — the 402 industries run down the page and the
five value-added codes across it.

| row | primary | allocated | missing | absent | $M | median `k` | primary $ |
|---|---:|---:|---:|---:|---:|---:|---:|
| `V00100` compensation | **17** | 383 | 0 | 2 | 10,434,980 | 11 | **15%** |
| `T00OTOP` other taxes | 0 | 389 | 0 | 13 | 608,533 | **389** | 0% |
| `V00300` surplus | 0 | 400 | 0 | 2 | 7,946,864 | **400** | 0% |
| `T00TOP` product taxes | 0 | 380 | 1 | 21 | 755,438 | 20 | 0% |
| `T00SUB` subsidies | 0 | 16 | 0 | 386 | 59,875 | 8 | 0% |

✅ **`V00100` is the only column in the whole block with a visible texture**,
and the figure shows why: its shading varies band by band, because the 69 NIPA
control groups are coarse over manufacturing and fine over parts of services and
transportation. Every other row is a flat wash.

⚠️ **`T00OTOP` and `V00300` are literally uniform** — one datum each, `k = 389`
and `k = 400`. There is no cell in either row that any source observes
individually, and both score 100% on the 2017 match.

### The supply bridge, on its own

⚠️ **The bridge is a tenth of the supply figure's width, which is enough to see
that a column is grey and not enough to read which.** Drawn on its own, at the
width its twelve codes need:

![Supply bridge provenance](images/supply_bridge_provenance_2017.png)

| code | primary | allocated | carried | missing | absent | $M | median `k` | what it is |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `T007` | 0 | **399** | 0 | 0 | 3 | 33,772,550 | 3 | allocated — row margin of the supply mix |
| `MCIF` | 0 | 279 | 0 | **21** | 102 | 2,669,377 | 4 | allocated — Census CIF + IEA, 1:m on frozen 2017 |
| `MADJ` | 0 | 6 | 0 | 0 | 396 | 23,116 | 8 | allocated — Census charges reassigned |
| `T013` | 0 | 0 | **401** | 0 | 1 | 36,418,811 | — | **subtotal** — adds no observation |
| `TRADE` | 0 | 0 | **274** | 0 | 128 | 6,529,862 | — | **carried** — receiving split frozen at 2017 |
| `TRANS` | 0 | 263 | 0 | 0 | 139 | 831,096 | 11 | allocated — eleven AIES/SAS groups |
| `T014` | 0 | 0 | **282** | 0 | 120 | 7,360,958 | — | **subtotal** |
| `MDTY` | 0 | 204 | 0 | **4** | 194 | 38,513 | **2** | allocated — Census duty rate × NIPA level |
| `TOP` | 0 | 339 | 0 | 0 | 63 | 716,925 | 6 | allocated — 29.8% on named NIPA lines |
| `SUB` | 0 | 15 | 0 | 0 | 387 | 59,875 | 12 | allocated — NIPA type lines, 2017 anchor |
| `T015` | 0 | 0 | **343** | 0 | 59 | 793,922 | — | **subtotal** |
| `T016` | 0 | 0 | **399** | 0 | 3 | 37,114,377 | — | **subtotal** |

**Three things the picture says that the table does not.**

1. ⚠️ **`MCIF`'s 21 missing cells are not scattered — they are a block in
   services**, visible as the one solid purple run in the figure. That is a
   mapping gap in one place, not thin coverage everywhere, and it is the
   `BEA_IEA_imports` crosswalk dropping `TransportRoadAndOth`,
   `OthPersonalCulturalAndRecreational` and `OperatingLeasing` — about $7.2B a
   year attributed to nothing ([#670](https://github.com/cornerstone-data/bedrock/issues/670)).
2. ✅ **`MDTY` is the most specific column in the block** at median `k = 2`, and
   its dark band sits exactly on manufacturing — the Census duty rate is
   collected at NAICS-6, which is close to BEA detail for goods. It is also
   tiny, $38.5B, so the block's best evidence carries almost none of its money.
3. ⚠️ **Four of the twelve columns are grey subtotals**, and a fifth (`TRADE`)
   is genuinely carried. Reading the figure without that distinction makes the
   bridge look half-unsourced when the truth is that half of it is arithmetic.

### What this says about the bridge

**The supply bridge is the only block with no primary data at all**, and the
only one where a whole column is carried:

- **`TRADE` is carried** — the 2017 published column moved by a Census gross
  margin, with the receiving split frozen at the 2017 mix
  ([#672](https://github.com/cornerstone-data/bedrock/issues/672)). 14.6% of the
  block's component dollars.
- **`TOP` is the closest thing to primary** — ten named NIPA product lines sit
  on their own commodities, 29.8% of the column at `k = 1`; the sales-tax
  residual on the other 70.2% is not.
- **25 missing cells**, all `MCIF` and `MDTY`.

⚠️ **`T007` is 33.8T of the block's 44.6T and it is allocated**, at median
`k = 3` — it is the row margin of the supply mix, so it inherits that block's
provenance rather than adding evidence of its own.

### How to read this against the pedigree scores

§Data quality scores the intermediate block on reliability and technological
correlation, 1 best and 5 worst. This section is deliberately **not** that: it
counts fan-out and stops. A datum can be highly reliable *and* spread across 400
cells — `V00300`'s NIPA level is about as reliable as US statistics get, and its
industry split is still one number over 400 industries. **Reliability and
specificity are independent, and collapsing them is what makes a 100% match
score look like a finished block.**

⚠️ **Not yet extended:** the pedigree indicators are only computed for the
intermediate block. The FlowBySector rows for these four carry flowsa's own
`DataReliability` and `TechnologicalCorrelation` columns already, so scoring
them is a next step rather than new research.

## Step 1 — Use table, final-demand columns

`use_fd_detail_sut` · 402 commodities × 19 final-demand codes · tolerance
`rtol=0.013, atol=5e5, ramp=0.25`

![Step 1 final demand match](images/use_fd_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 6,380 | 655 | 454 | 144 | 5 |
| row totals | 21 | 115 | 261 | 4 | 1 |
| column totals | 0 | 17 | 2 | 0 | 0 |

| | |
|---|---:|
| coverage | 88.5% |
| accuracy | 59.1% |
| candidate grand total | $22.362T |
| reference grand total | $22.238T |
| grand total error | 0.555% |
| residual outside the frame | none |

### Per column — two columns carry the entire error

New in this run, and it changes how the block should be read. The 59.1% is not
an even spread of error across nineteen columns:

| code | absent | match | partial | miss | extra | coverage | accuracy |
|---|---:|---:|---:|---:|---:|---:|---:|
| `F01000` PCE | 143 | 258 | 1 | 0 | 0 | **100.0%** | **99.6%** |
| `F02E00` | 295 | 106 | 1 | 0 | 0 | **100.0%** | **99.1%** |
| `F02N00` | 387 | 11 | 3 | 1 | 0 | 93.3% | 78.6% |
| `F02R00` | 382 | 18 | 1 | 1 | 0 | 95.0% | **94.7%** |
| `F02S00` | 390 | 10 | 1 | 1 | 0 | 91.7% | 90.9% |
| **`F03000`** | 143 | **0** | 160 | **98** | 1 | **62.0%** | **0.0%** |
| **`F04000`** | 57 | 11 | 287 | **43** | 4 | 87.4% | **3.7%** |
| `F06C00` | 401 | 1 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F06E00` | 348 | 54 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F06N00` | 398 | 4 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F06S00` | 391 | 11 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F07C00` | 401 | 1 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F07E00` | 334 | 68 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F07N00` | 398 | 4 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F07S00` | 394 | 8 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F10C00` | 399 | 3 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F10E00` | 330 | 72 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F10N00` | 398 | 4 | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `F10S00` | 391 | 11 | 0 | 0 | 0 | **100.0%** | **100.0%** |

**`F03000` and `F04000` carry 141 of the block's 144 cell misses**, and between
them they are the whole of the accuracy shortfall. The twelve government
columns and both investment-change columns are **exact, cell for cell** — 241
cells, no partials, no misses. `F01000` is one partial cell in 259.

⚠️ **`F03000` scores 0.0% accuracy: not one of its 160 populated cells lands
inside tolerance.** That is not a contradiction of §`F03000`'s "both scores
improved" — the two are measuring different things. Sign agreement and absolute
error against published gross improved; the share of cells landing within
`rtol=1.3%` of a published value did not, and on a column running 3× gross to
net across 61 negative commodities it was never going to. **The per-commodity
scores in §`F03000` are the ones to read for this column**, and the tolerance
here is the wrong instrument for it.

### What the picture says that the totals do not

**No column is a whole-column `miss` any more.** All 19 final-demand codes are
sourced. Two are outside tolerance at the column total: `F04000` (+6.15% vs
published, #528) and `F03000` (**−10.8%** as of 2026-08-28, #529 — it was
−2.28% when this was written; the whole difference is the `Other industries`
line now being carried as visibly unallocated rather than smeared, see §`F03000`). The other seventeen reconcile at
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

Re-scored 2026-08-28, after every activity set was moved onto a measured weight
(#529). **Both scores improved and the commodity count fell, and the second is
the reason for the first:**

| | 2026-08-24 | now | published |
|---|---:|---:|---:|
| column total | 31,936 | **29,144** | 32,682 |
| commodities populated | 256 | **161** | 258 |
| gross mass (sum of absolute cells) | 92,459 | **91,193** | 98,764 |
| sign agreement | 69.7% | **75.6%** | |
| absolute error vs published gross | 101% | **80%** | |

⚠️ **The drop from 256 commodities to 161 is the improvement, not a
regression.** Three activity sets - farm, manufacturing, and
mining/utilities/construction - were falling through to flowsa's default and
being spread **equally across every target sector**, which populated cells no
source observes. Each now attributes on something that measures what those
industries actually sell, so the mass is concentrated where there is evidence
for it. 161 commodities with a defensible weight beats 256 with an equal split.

⚠️ **The column total is 3,538 short of published, and all of it is one named
line.** `Other industries` (3,537 in 2017) has no sub-detail in `U50705BU1` and
no crosswalk row, so it is carried in the method as **visibly unallocated**
rather than silently dropped.

**The total is NOT free any more, and that is deliberate.** It used to equal
NIPA CIPI by construction, which is why −2.28% at the column total said almost
nothing about the allocation. It is now −10.8%, and every dollar of the
difference is one named line the method declines to allocate. Gross mass is
still 3× net across 61 negative commodities, so the per-commodity numbers above
remain the real score. The largest cells outstanding
are all previously scoped rather than new:

| commodity | ours | published | why |
|---|---:|---:|---|
| `336411` aircraft | −312 | −6,314 | manufacturing branch needs the industry's own stage split (#664) |
| `324110` petroleum refineries | −2,362 | −7,387 | sits in the mining/utilities/construction line, which is one number for three sectors |
| `211000` oil and gas | −12,137 | −7,577 | same line; now over rather than under, because the PxI mix concentrates it |
| `336991` motorcycles etc. | −4,268 | 186 | sign disagreement inside the manufacturing branch |
| `S00402` used goods | 380 | 3,969 | used-goods value sits in wholesale lines routing to `S00401` (#665) |

⚠️ **`211000` moved from −4,754 to −12,137 while its published value is
−7,577** — it was under, and is now over by more. That is the
mining/utilities/construction line being concentrated by a product mix rather
than smeared: better placed in principle, and still one published number
covering three sectors with no stage split, which is open question 2.
| `325414` biological products | 41 | 2,484 | trade-branch product-line split |

### `F03000` as a series, 2017-2023

New on 2026-08-28. The column is now attached for every year the method
covers, not only the benchmark:

| year | total $M | commodities | negative | gross $M |
|---:|---:|---:|---:|---:|
| 2017 | 29,144 | 161 | 39 | 91,193 |
| 2018 | 52,125 | 161 | 31 | 107,681 |
| 2019 | 69,930 | 161 | 17 | 107,333 |
| 2020 | **−43,392** | 161 | 90 | 112,725 |
| 2021 | 18,972 | 161 | 65 | 209,486 |
| 2022 | **184,885** | 158 | 15 | 227,047 |
| 2023 | 50,108 | 158 | 72 | 175,360 |
| 2024 | 0 | 0 | — | — |

✅ **The series has the shape the period should have.** 2020 is the only
negative year — the pandemic drawdown, and the only year where a majority of
commodities (90 of 161) run negative — and 2022 is the restocking spike at 6.3×
the 2017 level. Neither is imposed; both come from NIPA's own annual CIPI.

⚠️ **Gross mass more than doubles across the span** (91,193 → 227,047) while the
net total moves without a trend. That is the column behaving as designed — it is
a *change* series whose net is small and whose offsetting components are not —
and it is why every check on this column has to be per commodity.

⚠️ **2024 is zero and should be.** There is no `Inventories_2024` method: the
trade branch's Economic Census product mix is interpolated between the 2017 and
2022 vintages and held, and `U50705BU1` supplies the level. 2024 waits on the
same release the transport margin does.

⚠️ **The per-commodity allocation is unvalidated for 2018-2023.** There is no
published detail SUT to score those years against, so the 75.6% sign agreement
and 80% absolute error above are 2017 figures and do not carry forward. What the
series buys is that the column exists and moves on measured annual data, not
that its split is right.

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

`use_va_detail_sut` · 5 rows × 402 industries · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

![Step 2 value added match](images/use_va_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 424 | 1,233 | 319 | 1 | 33 |
| row totals | 0 | **5** | 0 | 0 | 0 |
| column totals | 0 | 354 | 48 | 0 | 0 |

| | |
|---|---:|
| coverage | **99.9%** |
| accuracy | 79.4% |
| candidate grand total | $19.6121T |
| reference grand total | $19.6121T |
| grand total error | **0.000046%** |
| residual outside the frame | none |

⚠️ **This section used to read 3 × 402, 100% / 100%, and a solid green block.
That description is dead.** [#740](https://github.com/cornerstone-data/bedrock/pull/740)
added `T00TOP` and `T00SUB`, taking the block from three rows to five and from
$18.92T to $19.61T. The two new rows are not two more of the same thing, and the
block's score fell because one of them is a seed.

### Per row — the shortfall is one row, and only one

| row | absent | match | partial | miss | extra | coverage | accuracy | what it is |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `V00100` compensation | 2 | 400 | 0 | 0 | 0 | **100.0%** | **100.0%** | estimate (QCEW movement, 69 NIPA groups) |
| `V00300` surplus | 2 | 400 | 0 | 0 | 0 | **100.0%** | **100.0%** | seed; residual `T18` hands the balance |
| `T00OTOP` other taxes | 13 | 389 | 0 | 0 | 0 | **100.0%** | **100.0%** | level plus two lookups |
| `T00SUB` subsidies | 386 | 16 | 0 | 0 | 0 | **100.0%** | **100.0%** | converted from Supply `SUB` |
| **`T00TOP` product taxes** | 21 | **28** | **319** | **1** | **33** | 99.7% | **8.1%** | converted from Supply `TOP` — **industry split is a seed** |

✅ **All five row totals MATCH.** The levels are right on every row, to
0.000046% on the block. What `T00TOP` gets wrong is not how much product tax
there is — it is **which industries remit it**.

⚠️ **`T00TOP` is 8.1% accurate and it is the only row that is not exact.** 319
of its 347 populated cells are outside tolerance. Four of the five rows are
still the plumbing test this section has always been; the fifth is a real
estimate being scored for the first time, and it is failing in a specific,
already-diagnosed way.

### Why `T00TOP` fails, and why it is the interesting row

**A product tax is remitted by whoever *sells* the good, not by whoever makes
it.** The fourteen worst cells in the block are all trade industries:

| industry | ours | published | |
|---|---:|---:|---|
| `452000` general merchandise | 17,296 | 37,990 | −20,694 |
| `441000` motor vehicle dealers | 28,790 | 45,947 | −17,157 |
| `424200` drugs wholesalers | 18,857 | 2,783 | **+16,074** |
| `423600` electrical goods wholesale | 19,990 | 6,991 | +12,999 |
| `424400` grocery wholesalers | 14,846 | 2,363 | +12,483 |
| `444000` building material dealers | 10,396 | 21,701 | −11,305 |

Retail is short and wholesale is long, by roughly the same money. The seed
places the tax with the producer; the published row places it with the seller,
and 55.7% of the row sits in wholesale and retail because of it. This is the
same seller-not-maker structure `F03000` exposes on the inventories column —
see [`inventories_estimation_plan.md`](inventories_estimation_plan.md).

⚠️ **The 33 extras are a second, separate defect, and ten of them are one
family.** `T00TOP` puts money on ten agriculture industries (`1111A0`, `1111B0`,
`111200`, `111300`, `111400`, `111900`, `112120`, `1121A0`, `112300`, `112A00`)
where BEA publishes exactly zero — the largest is $16.1M on `1121A0`. Farms are
not retailers, so a market-share operator putting sales tax on them is the
mechanism failing in the direction the correlation already predicts.

⚠️ **`491000` is an extra here, and this report named it two runs ago for the
opposite reason.** The US Postal Service was found on the `T00OTOP` row as an
eleventh government code whose published value is zero, landing on zero only
because the benchmark weight happened to be zero. On `T00TOP` the same code now
carries $0.5M against a published zero. **The prefix rule that was fixed for
`T00OTOP` was not applied to `T00TOP`**, which is a one-line follow-on rather
than a modelling question.

✅ **The single `miss` is `S00203`** — $538M of published product tax on a
government enterprise our conversion puts nothing on. Same family as the extras:
a government producer that the industry-axis rule does not reach.

### What the four exact rows still certify

Unchanged, and still worth having. `V00100`, `V00300` and `T00OTOP` take their
*level* from NIPA and their within-group distribution from the 2017 benchmark —
which is the reference — so at 2017 the shares are the identity and these rows
test plumbing, not estimates. They catch mass lost between attribution groups, a
row written to the wrong axis, a sign dropped. **A solid green on those three is
the floor, not an achievement.**

`T00SUB` is different again: it is *converted* from the Supply `SUB` column by
`nowcast_va_taxes`, so its level carries no modelling content at all. It
reproduces the published row exactly. `T00TOP` is converted the same way and
does not, because only one of the two has an industry split that has to be
estimated.

⚠️ **The test with real content is still the same picture for a later year**,
where the shares stop being the identity — and for `T00TOP` that test is
available now, because its 2017 failure is not a plumbing artefact.

### The 17 structural zeros on the three original rows

Neither side populates them, so they are genuinely nothing to say — but the list
is an accounting statement rather than a data gap:

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
which is precisely why it would not have been found by a total. ⚠️ **And the
same rule is still missing on `T00TOP`**, see above.

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

![Step 3 seed coverage](images/intermediate_seed_coverage_2021.png)

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

⚠️ **One figure, not one per year, and 2021 is chosen by measurement.** The
mappings behind `N` do not depend on the year, so the map is nearly
year-invariant; measured, the seeded share moves **1.9 points across 2020-2023**
(35.7 / 35.7 / 33.8 / 34.6), inside the band where one picture stands for the
span. `best_year` ranks 2021 first — see §Data quality below for why that is a
weaker claim than it sounds. ⚠️ **2018 and 2019 are excluded on purpose** — the
SAS expense panel jumps straight from 2017 to 2020, with no 2018 or 2019 vintage
in it, so services and transportation hold their 2017 columns entirely and the
block reads **19.5%**. A 2018 figure would be showing the missing SAS vintages,
not the seeds. `--check-years` re-measures this rather than trusting it.

Dollar-weighted, at 2021:

| band | columns | $M | seeded | of which N = 1 | median N |
|---|---:|---:|---:|---:|---:|
| agriculture | 13 | 272,102 | 78.7% | 0.0% | 20 |
| mining | 8 | 195,477 | 31.7% | 22.5% | 1 |
| utilities | 3 | 160,406 | 22.3% | 0.0% | 3 |
| construction | 12 | 737,745 | — | — | — |
| manufacturing | 231 | 3,561,508 | **71.4%** | **51.2%** | 4 |
| trade | 20 | 1,545,723 | — | — | — |
| transportation | 9 | 591,808 | 42.8% | 19.9% | 8 |
| services | 94 | 6,494,741 | 33.3% | 6.6% | 9 |
| government | 8 | 1,222,147 | — | — | — |
| **total** | **402** | **14,856,988** | **35.7%** | | **7** |

**35.7% of the block's dollars are observed; 10,398 of 44,281 non-empty cells.**

⚠️ **Read the dollars, not the cell count, and read this against the column
count rather than instead of it.** 330 of 402 *columns* move off the 2017 shape
— that is the number in the plan and it is the optimistic reading, because a
seeded column is not a column of seeded cells. `materials_seed` returns the
whole manufacturing column renormalised, but the census only *observes* the
materials rows; the rest of that column is the 2017 mix rescaled, which is
carried. 35.7% of dollars is the honest reading of the same fact.

Three things the picture says that the totals do not:

1. ✅ **Manufacturing is the strong block and it is strong in the right way** —
   71.4% of its dollars observed and **51.2% at `N = 1`**, the only block where
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

### What was tested to get there, and what was rejected

The map says where the block is observed. It does not say what was graded to
arrive at that, and without it the three grey blocks read as unfinished work
rather than as the recorded verdicts they are.

**Six seeds are wired**, composed cell-wise by `composed_seed` (never added —
summing `materials_seed` and `nonmaterial_seed` double-counts the 23
non-materials rows and put `334111` 55% above the benchmark at 2017). They
partition the 402 columns, checked in code rather than assumed:

| block | columns | source | graded |
|---|---:|---|---|
| manufacturing | 232 | Economic Census materials, then the survey index on the non-materials cells | **ships ungraded**, by decision |
| services / transportation | 100 | `Census_SAS_Expenses` / AIES | ✅ **GO**, +11.5% on `N` |
| agriculture | 10 | ERS | ✅ **GO**, +17.9%, 9 of 10 columns |
| utilities | 3 | EIA 923 fuel receipts | ✅ **GO**, +16.0% on `N`, 3 of 3 columns |
| mining | 6 | Economic Census materials, sector 21 | **ships ungraded**, as manufacturing does |
| — | 51 | hold 2017 | nothing observes their movement |

⚠️ **Two blocks ship ungraded on purpose, and that is a policy rather than an
oversight.** Manufacturing and mining are seeded from the Economic Census
materials detail, which is *the only observation of those cells that exists*.
Grading it against a holdout would compare the census to itself. The gate is
overridden where the source is the only observation; the continuity and
coverage checks still run.

### What is held, and on what verdict

$3.5T — 24% of the block — carries no annual observation. **Every part of it is
a measured verdict except one**, and the distinction matters because a verdict
does not get re-litigated and a gap does.

| held block | verdict | the measurement |
|---|---|---|
| **trade** (`42`, `4A0`) | ❌ **no-go**, survived a regrade | a wash: dollar **+0.3%** (9 of 18 columns), impact `N` **−5.6%** (8 of 18). It tracks the items that carry no `N`. |
| **construction** (`23`) | ❌ **no-go**, closed 2026-08-27 | 51% of the column is one undifferentiated cell (`CSTMPRT`) and no Census product splits it — `ecnmatfuel` is manufacturing and mining only. The seed already **loses** to frozen, 0.044 against 0.038. |
| **government** (`G*`) | ❌ function bridge rejected (#578) | the function mix moves **0.046** in five years against a **0.201** commodity drift — an exact ceiling, and far too small. |
| `221200` gas distribution | ❌ EIA form 176 rejected | $29.4B holds, with `221300` |
| `221300` water | ⚠️ **no source** — a gap, not a verdict | |
| `213111`, `21311A` mining support | ✅ held deliberately | they buy labour and services, not the materials the census measures; coverage agrees rather than leads (6.7% / 15.6% against 25-34%). **$53.0B holds.** |

⚠️ **Government is the one to read carefully.** What is closed is the *function
bridge as a route to the mix*. What is **not** closed is the underlying problem:
the `G*` columns still drift by **230-258 $B**, and the drift is
**within-function** — a school district buying a different basket than it bought
in 2012, not a shift from schools to highways. Nothing here sources that, and
nothing here should be read as evidence those columns are stable. `S00500` alone
is **83,851 $M misplaced at 0.383, the worst-drifting column in the Use table**,
and it is federal and out of reach of that source at any depth.

⚠️ **Two things were tested and are recorded as "do not build".** The trade BES
**suppression recovery** — suppression is real, but recovering it does not buy
the verdict back — and **EIA form 176** for `221200`, where a 2014 change to the
form's establishment split manufactured a spurious **+29%** win. Both are the
kind of plausible next step that would otherwise be attempted twice.

### How the verdicts are graded — and why one of them reversed

⚠️ **The answer key is not BEA's published 2018-2024 tables.** Those carry the
2017 benchmark mix forward by construction, so grading a seed against them
scores the seed on how well it reproduces *a frozen mix* — which is the thing
the seed exists to move away from. A seed that correctly moves the mix loses.

✅ **[`benchmark_holdout.py`](benchmark_holdout.py) is the sound key**: seed the
**observed 2012** detail block with the source's own 2012 → 2017 movement, and
score against the **observed 2017** detail block. Both endpoints are published
benchmarks, so the movement being tested is the movement that actually happened.

**This has already changed the answer.** Agriculture was a no-go on the
carried-forward key and **reverses to a GO** on the holdout (+17.9%, 9 of 10
columns). Trade was rejected the same way and does **not** reverse — but its
reasons were rewritten, and the old −43.6% figure is withdrawn. The size of the
answer-key effect is measurable on one column: `441` scores **−16.8%** on the
sound key against −43.6% on BEA's carry-forward.

⚠️ **And the weighting matters as much as the key.** Verdicts are ranked on `N`,
total emission factor (direct **and** indirect, `C B L`), not on dollars and not
on direct EF. Manufacturing roughly doubles between the two, services double,
utilities halve. **Two SAS verdicts flipped** when the dollar ranking was
replaced with the `N` ranking, which is why the table above quotes `N` wherever
one exists.

### Data quality — pedigree scores

The map says *whether* a cell is observed. This scores *how well*, on a variant
of the EPA LCA data-quality pedigree: **1 is best, 5 is worst**, and the two
indicators are kept separate rather than blended into one number.

**Reliability** is how the data were collected — complete mandatory enumeration
(1), a mandatory plant-level filing (2), a probability sample or a cell Census
withheld and we re-estimated (3), modelled national estimates (4), no
observation at all (5).

**Technological correlation** is whether the datum is about *this* cell, on two
axes that fail independently. *Commodity*: 1 when the reported item is that BEA
commodity, degrading as one item is spread over more of them. *Industry*: 1 when
the source reports that specific BEA industry, degrading on the **worse** of two
things — collecting at an aggregation BEA splits finer (a 2-digit survey NAICS
against a 6-digit BEA industry) or one index driving many columns. The two are
combined by the mean, rounded up.

By source, at 2021, dollar-weighted:

| source | cells | $M | % of block | reliability | tc commodity | tc industry | tc |
|---|---:|---:|---:|---:|---:|---:|---:|
| carried | 33,883 | 9,550,627 | 64.3% | 5.00 | 5.00 | 5.00 | 5.00 |
| SAS | 1,538 | 2,414,907 | 16.3% | 3.00 | 1.97 | 3.83 | 3.17 |
| Economic Census materials | 4,122 | 2,335,200 | 15.7% | **1.00** | 1.76 | **1.00** | **1.44** |
| ASM expenses | 4,300 | 222,289 | 1.5% | 3.00 | 2.33 | 1.00 | 1.85 |
| ERS | 179 | 214,053 | 1.4% | 4.00 | 2.09 | 5.00 | 3.72 |
| EIA 923 | 11 | 68,772 | 0.5% | 2.00 | 1.00 | 3.00 | 2.00 |
| Census, suppression recovered | 248 | 51,140 | 0.3% | 3.00 | 1.80 | 1.00 | 1.46 |

**The two aggregations**, by band. `$` weights each cell by its dollars — what a
dollar of this block is worth as evidence. `N$` weights by `N × dollars`, which
deliberately *up*-weights the spread-thin evidence, so **the gap between the two
columns is a direct read on how much of a band's quality rests on data that had
to be allocated**:

| band | seeded | reliability `$` | tc `$` | reliability `N$` | tc `N$` |
|---|---:|---:|---:|---:|---:|
| agriculture | 78.7% | 4.21 | 4.00 | 4.01 | 3.89 |
| mining | 31.7% | 3.76 | 3.88 | 2.14 | 3.34 |
| utilities | 22.3% | 4.33 | 4.33 | 3.61 | 3.61 |
| construction | — | 5.00 | 5.00 | 5.00 | 5.00 |
| manufacturing | 71.4% | **2.30** | **2.49** | 1.50 | 2.82 |
| trade | — | 5.00 | 5.00 | 5.00 | 5.00 |
| transportation | 42.8% | 4.14 | 4.35 | 3.48 | 4.14 |
| services | 33.3% | 4.33 | 4.38 | 3.36 | 4.03 |
| government | — | 5.00 | 5.00 | 5.00 | 5.00 |
| **total** | **35.7%** | **3.98** | **4.05** | **3.04** | **3.76** |

⚠️ **The whole-block score is poor and it should be** — two thirds of the
block's dollars are carried and score 5 on both indicators by construction.
The number that says something about the *work* is manufacturing's **2.30 / 2.49**,
and the Economic Census materials row's **1.00 / 1.44**: where this pipeline has
a census, the evidence is close to the best the pedigree allows.

⚠️ **Reliability and tc disagree about agriculture, and that is the point of
keeping them apart.** ERS scores 4 on reliability (modelled national estimates)
*and* 5 on industry correlation (one farm sector driving ten columns) — it is
weak on both counts for different reasons. SAS is the mirror image: reliability
3, commodity correlation 1.97 — the items are close to BEA commodities — but
industry correlation 3.83, because the survey NAICS are coarser than BEA detail.
Blending these into one number would hide which fix would help.

⚠️ **Two things are deliberately not scored.** **Temporal correlation** is a
third pedigree indicator, scored on data age, and is not computed yet (Wes).
It is the indicator that would separate the candidate years: `materials_seed`
interpolates the census mix between the 2017 and 2022 vintages and holds it
afterwards, so **2022 is the only year whose largest seeded block is read in the
year the census actually ran** — but on reliability and tc alone the four years
separate by under 0.06, so what picks 2021 is coverage. Expect the presentation
year to move to 2022 once temporal correlation lands. **Data collection** (share
of establishments represented) is also unscored.

⚠️ **The ladders are choices, not findings.** The 1/2/3-4/5-9/≥10 steps and the
per-source reliability assignments sit in one table at the top of
[`seed_coverage.py`](seed_coverage.py) so a disagreement is a one-line change.
The degrade-on-mapping-down principle follows
[`dqi.py`](../../utils/mapping/dqi.py), which already applies it repo-wide.

Reproduced by [`seed_coverage.py`](seed_coverage.py) — `--check` re-asserts the
map, `--check-years` re-measures the one-figure claim, and `--check-palette`
re-runs the colour separation (worst pair dE 29.0 against a floor of 27, binding
on grey against the light end of the ramp under deuteranopia).

#### Where this stopped — 2026-08-27

Shipped and checked: the provenance map at 2021, the prose figure description,
and the two pedigree indicators with both aggregations. `--check`,
`--check-years`, `--check-palette` and `--best-year` all pass.

Open, in the order they were set down:

1. ▶️ **Letters on the figure.** Wes: annotate regions with letters keyed to
   descriptions in the caption, rather than text drawn on the raster. Deferred
   deliberately — the prose description above was to come first and is the
   thing to judge before adding marks.
2. ▶️ **Temporal correlation**, the third pedigree indicator, scored on data age
   (the EPA rubric bands roughly 1-25 years). Not started, by decision. ⚠️ It is
   the indicator that decides the presentation year: without it 2021 wins on
   coverage, with it 2022 should win because its census mix is read in-year.
   ⚠️ **Do not fold data age into reliability** — that crosses DQ categories,
   and an earlier pass of this work did exactly that and had to be reverted.
3. ▶️ **Data collection**, a fourth indicator (share of establishments
   represented), also unscored.
4. ⚠️ **`Census_EC_Expenses` 2022 covers 222 of 232 BEA manufacturing
   industries** where `Census_ASM_Expenses` 2021 covers all 232, for all ten
   expense kinds. That 4.3% shortfall is most of why the census year has *lower*
   coverage than the sample years around it. Not diagnosed — it may be
   suppression, or an extraction gap.
5. ⚠️ **The SAS expense panel has no 2018 or 2019 vintage**, so those years read
   19.5% against 35.7%. The vintages exist and are not fetched.

The rubric's ladders and per-source reliability scores are judgements, collected
at the top of [`seed_coverage.py`](seed_coverage.py) so they can be argued with
in one place rather than re-derived.

---

## Step 4a — Supply table, the domestic-output interior

`supply_output_detail_sut` · 402 commodities × 402 industries · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

**New to this report.** The section has been registered since
[#570](https://github.com/cornerstone-data/bedrock/issues/570) merged on
2026-08-21 and was never drawn here. It is the second of the two 402 × 402
interiors, and with it in the report **every block of the SUT pair now has a
picture**.

![Step 4a domestic output match](images/supply_output_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 156,524 | **5,059** | 21 | **0** | **0** |
| row totals | 3 | 399 | 0 | 0 | 0 |
| column totals | 1 | 401 | 0 | 0 | 0 |

| | |
|---|---:|
| coverage | **100.0%** |
| accuracy | **99.6%** |
| candidate grand total | $33,772,550M |
| reference grand total | $33,772,482M |
| grand total error | **0.0002%** |
| residual outside the frame | none |

**No misses, no extras, and no margin outside tolerance on either axis.** 5,059
of 5,080 populated cells land exact; the 21 partials are the whole of the
disagreement, and the largest of them is $1.5M on a $77M cell.

⚠️ **The block is 96.9% structurally empty, and that is why the coverage figure
needs its denominator stated.** 5,080 cells of 161,604 are present, because an
industry makes a handful of commodities, not 402. A match rate computed over all
cells would read ~99.99% for any build at all, including an empty one. The
tolerance leaves both-zero cells as `absent` deliberately, so the 100% / 99.6%
above is over present cells only.

⚠️ **2017 is close to circular — do not read this as the best-verified block.**
The candidate is the `Detail_Supply_Mix_2017` FBS, which disaggregates the published
*summary* domestic-output block onto the 2017 *detail* mix. The same detail mix
appears on both sides of the comparison, so a green result means the
disaggregation has not broken, not that the method predicts anything. **What
carries the method is two other measurements, neither of them in this picture:**

1. **The held-out mix test** — carrying a 2017 mix forward cost **0.94%**
   economy-wide over five years, which is the bar this section's tolerance was
   set from rather than a default.
2. **`annual_mix_test.py`** — no annual survey can move the mix *between*
   censuses; the signal sits at the noise floor. So holding the mix is a
   measured verdict, not an unfinished task.

**From 2022 the mix itself moves.** Economic Census product lines drive 133 of
178 columns (`pxi_mix_test.py`), and 45 columns are held at the 2017 mix. ⚠️ The
2022 Economic Census is an observation **BEA has not used** — the only
independent 2022-vintage answer key available — which is what makes the
post-2022 half of this block scoreable at all. See
[`bea_2017_benchmark_sources.md`](bea_2017_benchmark_sources.md).

⚠️ **Open:** detail *industry* output from Economic Census source data is
[#724](https://github.com/cornerstone-data/bedrock/issues/724), and
`325412` pharmaceutical preparation builds at 0.52 of published commodity
output ([#676](https://github.com/cornerstone-data/bedrock/issues/676)).

---

## Step 4 — Supply table, bridge to purchaser value

`supply_bridge_detail_sut` · 402 commodities × 12 bridge codes · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

![Step 4 supply bridge match](images/supply_bridge_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 1,595 | 1,980 | 1,197 | 25 | 27 |
| row totals | 3 | 206 | 187 | 0 | 6 |
| column totals | 0 | 9 | 1 | 2 | 0 |

| | |
|---|---:|
| coverage | 99.2% |
| accuracy | 62.3% |
| candidate grand total | $111.343T |
| reference grand total | $111.283T |
| grand total error | **0.054%** |
| residual outside the frame | none |

### What moved since 2026-08-26

Accuracy 62.1% → **62.3%**, cell misses 33 → **25**, extras 30 → **27**, and the
row-total `miss` is gone (1 → **0**). That is the **vehicle split fix**
([#702](https://github.com/cornerstone-data/bedrock/issues/702), merged in
[#748](https://github.com/cornerstone-data/bedrock/pull/748)): Census publishes
its own `336111`/`336112` breakdown through 2022 and only the parent `336110`
from 2023, and taking Census's child split put **$111B on the wrong commodity**
and produced an **$80B discontinuity at 2022→2023**. Relabelling both children
onto `336110` and letting the 1:m family split it moved imports
`pearson_non_special` **0.850 → 0.972**, and both cells `PARTIAL` → `MATCH`.

⚠️ **The exports half of #702 is still open** and the issue stays open for it.
Exports improved less and is still ~$18.6B off the other way, because its 1:m
weight is same-year `T007` domestic output — what we *produce*, not what we
*export*.

### What did not move, and was measured rather than assumed

⚠️ **`MCIF`'s 1:m weight stays the frozen 2017 `MCIF` column, and
[#729](https://github.com/cornerstone-data/bedrock/issues/729) is closed as a
no-go.** The issue proposed swapping it to Use `T019` total uses. Built, graded
and reverted 2026-08-28: imports spearman **0.941 → 0.893**, n_match **26 →
19**.

✅ **The zeros in published `MCIF` are BEA's allocation, not a sparse column** —
`524200` insurance brokerages, `524113`, `5416A0`, `811100`, `484000`, `532100`
and `713900` are published at exactly 0, because imported insurance is *carrier*
service, not brokerage. `611A00` and `611B00` match the current build **exactly**
(983/983, 748/748) and both break under `T019`.

✅ **Circularity is not the reason** — `derive_initial_Y_pur` and
`derive_initial_U_intermediate` read no import column, so total uses is
computable without reading `MCIF`. It simply does not help.

⚠️ **The goods import error is sized, and it is a crosswalk problem, not a
concordance replacement.** $501,870M, **23.0%** of published 2017 goods `MCIF`.
The top 10 commodities carry 38.8% of it and the top 50 carry 80.5%, so it is
**~50 crosswalk decisions**. The worst misroute is `334418` printed circuit
assembly: 757 published against 20,590 ours, **27×**.
([#670](https://github.com/cornerstone-data/bedrock/issues/670))

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
| `MCIF` | 102 | 28 | 249 | 21 | 2 | 93.0% | 10.1% |
| `MADJ` | 396 | **6** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `T013` | 1 | 227 | 174 | 0 | 0 | **100.0%** | 56.6% |
| `TRADE` | 128 | **274** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `TRANS` | 139 | 6 | 257 | 0 | 0 | **100.0%** | 2.3% |
| `T014` | 120 | 132 | 150 | 0 | 0 | **100.0%** | 46.8% |
| `MDTY` | 194 | 67 | 119 | 4 | 18 | 97.9% | 36.0% |
| `TOP` | 63 | **339** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `SUB` | 387 | **15** | 0 | 0 | 0 | **100.0%** | **100.0%** |
| `T015` | 59 | 281 | 61 | 0 | 1 | **100.0%** | 82.2% |
| `T016` | 3 | 206 | 187 | 0 | 6 | **100.0%** | 52.4% |

⚠️ **`S00300` noncomparable imports is the single worst row in the block**, and
it is the same commodity that dominates the Step 5 pre-balance gap. Ours is
236,328 against a published 781,263 — **−69.8%**, and it lands on `MCIF`,
`T013` and `T016` at once because the three inherit it. At $181.6B on each of
those three columns it is the largest absolute cell error in the frame by a
factor of seven. It has no source
([#606](https://github.com/cornerstone-data/bedrock/issues/606)).

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
its 21 cell misses, with `MDTY`'s 4, are the only real misses left in the
block.

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

## Step 5 — balancing the SUT

No section picture: Step 5 does not produce a block to compare, it produces a
*balanced* version of the four above. What follows is its state as of
2026-08-28, because it is the step the rest of the report is feeding.

### ✅ What is established

**The engine converges on the published 2017 panel.** `nowcast_sut_gras.engine`
reaches a **max hard residual of 0.57** against a tolerance of 100, across `T1`
and `T11`–`T18`, with and without soft targets. Re-run it with
`mask_layer_feasibility --check-engine`.

The hard targets hold on the published tables too:

| target | what it is | cells | max residual |
|---|---|---:|---:|
| `T11` | commodity identity (`T016` = `T019`) | 400 | 21 |
| `T17` | basic-to-producer wedge | 402 | 12 |
| `T1` | industry gross output | 402 | 13 |
| `T18` | value added | 402 | 2 |

### ✅ Established 2026-09-01/02, superseding what stood here

**The engine has now run to convergence on the nowcast seeds for every year
2017-2023**, under both the hard and soft protocols, with the T11
supply-equals-use identity closing exactly in each year. The balanced Supply
and Use SUT products are persisted as versioned parquet
(`Balanced_Detail_Supply` / `Balanced_Detail_Use_SUT` per year) with metadata
sidecars, locally and on GCS. The "never run on a nowcast seed" warning below
this point described the 2026-08-28 state and is kept only in the history of
this file. The pre-balance gap table that follows is likewise historical: the
interior fit (#794) took the seed's T11 from 20.0% to a named 2.3% before the
RAS closed it exactly. The full precondition ledger is
[`About_step5_preconditions.md`](About_step5_preconditions.md).

### The measured pre-balance gap

`T11` on the **2017 nowcast seed** — Supply row minus Use row, per commodity:

| | total | commodities over $10B | worst |
|---|---:|---:|---|
| nowcast seed | **$972,971M** | 20 | `S00300` −173,948 ([#606](https://github.com/cornerstone-data/bedrock/issues/606)) |
| published panel | $1,044M | 0 | — |

After `S00300`, the gap is the aerospace trio `336411`/`336412`/`336413`
([#701](https://github.com/cornerstone-data/bedrock/issues/701)). ⚠️ `S00300`
alone is 18% of it, and it is the same commodity that carries the single worst
row in the Step 4 bridge (−69.8%, see below) — **one unsourced commodity is the
largest single obstacle to the balance.**

### Blockers, in [#749](https://github.com/cornerstone-data/bedrock/issues/749)

1. **58 cells are nonzero where the mask calls them a structural zero.** Fixed
   for the trade-flow columns in
   [PR #750](https://github.com/cornerstone-data/bedrock/pull/750), which adds
   27 `NEVER_IMPORTED_COMMODITIES` in two tiers — 20 structural (a trade margin
   does not cross a border) and 7 by BEA convention. ⚠️ The build is clean on
   all 27 today but **by accident**: the frozen `MCIF` weight is itself zero
   there, so the weight has been doing the guard's job.
2. **A `TRADE` / `TRADE ` label mismatch.** `nowcast.py` labels the column
   `'TRADE'`; the balance panel expects BEA's trailing space, and reindexing
   drops it silently. ⚠️ **Latent, not active** — nothing in production joins
   the bridge to the panel yet, so whoever wires Step 5 hits it first.
3. ✅ **`F03000` was all-zero for 2018-2023 in the seed — fixed 2026-08-28.**
   `Inventories_<year>.yaml` had shipped 2017-2023
   ([#746](https://github.com/cornerstone-data/bedrock/pull/746)) while
   `derive_initial_Y_pur` still gated on `if year == 2017`, so the method was
   built and the consumer was never switched on. Now an `INVENTORIES_YEARS`
   membership test, mirroring `TRADE_OVERLAY_YEARS`. See §`F03000` as a series.


## Steps 6 and 7 — the stored MUTs, and the first product check

Added 2026-09-02. Downstream of the balance, two conversion layers now run per
year and store their products under `flowsa/NowcastMUT` (flat versioned
parquet + sidecar, four tables per year per stage):

- **Step 6** (`transform/iot/nowcast_mut.py`, #824): the balanced SUT to the
  **before-redefinitions** quartet — Make, producer-price Use, import matrix,
  and the hyper-detailed Margins table (one column per margin commodity — the
  per-buyer seller placement — beside the five published columns). Five
  identity gates at $1M refuse the save on any breach; $5.56tn of margin mass
  places with a $0.000M conservation gap.
- **Step 7** (`transform/iot/nowcast_redefinitions.py`, #830): the
  before-redefinitions quartet to **after-redefinitions**. Every design
  choice was measured before it was made — the Make movement is a whole-cell
  pattern (1,850 of 1,880 moving 2017 cells transfer at ~100%), the Use
  interior is a cell-ratio carry with commodity-row closure, and the measured
  verdicts (including the ones that rejected recipe-based reconstruction and
  two closure alternatives) are in
  [`About_method_probes.md`](after_redef_MUTs/method_probes/About_method_probes.md).

### Industry output from the stored tables vs the BEA gross-output series

The first check taken *on the product* rather than on a seed: industry output
computed from the stored before-redefinitions Make tables (row sums, producer
prices, latest build on disk), against the published BEA detail gross-output
series the repository currently consumes
(`derive_gross_output_before_redefinition`), and against the EC-adjusted
panel the balance was actually asked to hit. Weighted difference is the sum
of absolute per-industry gaps over total output. Reproduced by
[`go_vs_nowcast_mut.py`](go_vs_nowcast_mut.py).

Distribution statistics are over the industries the published series carries
at $100M or more, so a percent difference is never quoted over a near-zero
base.

| year | nowcast $B | BEA GO $B | total diff | weighted diff | median \|diff\| | max \|diff\| (industry) | industries >5% off | vs imposed panel |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 34,468 | 34,468 | 0.00% | 0.00% | 0.00% | 0.1% (`335912`) | 0 | 0.00% |
| 2018 | 36,509 | 36,505 | +0.01% | 0.06% | 0.00% | 0.9% (`452000`) | 0 | 0.06% |
| 2019 | 37,664 | 37,658 | +0.01% | 0.06% | 0.00% | 0.9% (`452000`) | 0 | 0.06% |
| 2020 | 36,744 | 36,715 | +0.08% | 0.10% | 0.01% | 4.7% (`S00201`) | 0 | 0.10% |
| 2021 | 41,902 | 41,833 | +0.17% | 0.19% | 0.01% | 10.9% (`S00201`) | 2 | 0.19% |
| 2022 | 46,706 | 46,611 | +0.20% | **2.20%** | **2.26%** | **38.1%** (`54151A`) | **118** | 0.23% |
| 2023 | 48,558 | 48,540 | +0.04% | **2.11%** | **2.24%** | **38.0%** (`54151A`) | **112** | 0.06% |

![Nowcast vs BEA GO by industry](images/go_vs_nowcast_mut.png)

**How to read the figure** (regenerate with
`uv run python -m bedrock.analysis.nowcasting.go_vs_nowcast_mut --figure`).
Left: every scored industry's percent difference, one jittered column per
year — blue above the published series, red below, dashed lines at ±5%. The
2017-2021 columns are a flat line at zero (the median industry is within
0.01%); the 2022 fan-out is the census break arriving, and 2023 keeps its
shape. Right: 2023's largest absolute divergences in dollars, the percent in
the label — professional services, wholesale, and pharma, i.e. the sectors
where the 2022 Economic Census disagrees most with BEA's carried-forward
detail mix.

**The story splits at 2022, and both halves are the intended behaviour.**
Through 2021 the nowcast tracks the published series to 0.06-0.19% weighted —
the balance reproducing its own controls plus tax-wedge rounding. In
2022-2023 the weighted difference jumps to ~2.2% with over a hundred
industries more than 5% apart — **that is the 2022 Economic Census
conditioning (#724), not drift**: within each summary group the detail mix
follows the census rather than BEA's carried-forward 2017 split, which the
EC-2022 audit measured as 5.1% off value-weighted. The last column proves the
distinction — against the census-conditioned panel the balance was actually
asked to hit, every year stays at or under 0.23%.

⚠️ **Read the 2022-2023 rows as a deliberate disagreement with BEA, on
census evidence.** The largest 2023 dollar divergences from the published
series are the census corrections by name: scientific R&D services `541700`
+13.7% (+$65B), advertising `541800` -18.3%, architectural and engineering
services `541300` -7.6%, pharmaceutical preparations `325412` -13.4%; the
largest *relative* move is custom computer programming `54151A` at +38%.
Totals agree to 0.2% in every year; what moves is the detail mix, which is
exactly what the census observes and the carried-forward split does not.

⚠️ **The pre-census maxima are a different, smaller story worth one line.**
Through 2021 the worst single industry is `S00201` state-and-local transit
(4.7% in 2020, 10.9% in 2021) — a pandemic-era government enterprise whose
published series and our balance disagree on the recovery path — followed by
`452000` general merchandise at under 1%. Everything else sits at the
rounding floor.

## The balanced SUT vs the published summary tables, annually

Added 2026-09-02. The only published tables the nowcast *years* can be
compared against at all: the balanced detail SUT rolled up to BEA summary on
the taxonomy crosswalks, against the published summary Supply and Use SUT for
the same year (sections `supply_summary_sut` / `use_summary_sut`; regenerate
any year with
`uv run python -m bedrock.analysis.nowcasting.plots --section supply_summary_sut --section use_summary_sut --year <Y>`).

⚠️ **Close-not-exact is the design, so read the trend, not the level.** The
balance held its own observed aggregates — detail gross output, the
final-demand totals, the tax rows — not every published summary cell, and
from 2022 it deliberately disagrees with BEA on census evidence. The
published summary tables are themselves BEA's annual estimates, not an
answer key of the #746 kind.

| year | Supply cov. | Supply acc. | Use cov. | Use acc. | Supply total diff | Use total diff |
|---:|---:|---:|---:|---:|---:|---:|
| 2017 | 99.2% | 55.5% | 99.6% | 41.5% | 0.012% | 0.012% |
| 2018 | 99.0% | 50.0% | 99.5% | 15.6% | 0.011% | 0.009% |
| 2019 | 98.7% | 42.2% | 99.5% | 12.6% | 0.058% | 0.008% |
| 2020 | 98.4% | 25.0% | 99.5% | 7.8% | 0.067% | 0.185% |
| 2021 | 98.1% | 17.5% | 99.1% | 6.9% | 0.340% | 0.586% |
| 2022 | 98.4% | 12.0% | 98.7% | 6.3% | **1.033%** | **1.038%** |
| 2023 | 98.4% | 35.9% | 98.6% | 6.3% | 0.017% | 0.016% |

Three things the span says. **Coverage holds at ~99% every year** — the
nowcast populates what BEA populates. **Cell-level agreement decays with
distance from the anchor** (Supply 55.5% → 12.0%, Use 41.5% → 6.3% at the
1%-of-cell bar), which is two real estimates diverging, not one degrading:
our interior moves on census and survey evidence, BEA's summary moves on
theirs. **The grand totals stay within 0.07% through 2019, spread to ~1% at
2022** — the census conditioning year — **and close back to 0.02% at 2023.**

The figures, one per table per year — green where the two agree within 1%,
amber ramping with severity, the margins strips along the edges:

![Supply summary 2017](images/supply_summary_sut_2017.png)
![Use summary 2017](images/use_summary_sut_2017.png)
![Supply summary 2018](images/supply_summary_sut_2018.png)
![Use summary 2018](images/use_summary_sut_2018.png)
![Supply summary 2019](images/supply_summary_sut_2019.png)
![Use summary 2019](images/use_summary_sut_2019.png)
![Supply summary 2020](images/supply_summary_sut_2020.png)
![Use summary 2020](images/use_summary_sut_2020.png)
![Supply summary 2021](images/supply_summary_sut_2021.png)
![Use summary 2021](images/use_summary_sut_2021.png)
![Supply summary 2022](images/supply_summary_sut_2022.png)
![Use summary 2022](images/use_summary_sut_2022.png)
![Supply summary 2023](images/supply_summary_sut_2023.png)
![Use summary 2023](images/use_summary_sut_2023.png)

## Caveats

✅ **Retired 2026-09-02: "Step 5 has not been run on a nowcast seed."** The
span is balanced under both protocols with products stored — see §Step 5 and
§Steps 6 and 7. The five seed sections above still describe the seeds as of
2026-08-28, which remains their latest joint snapshot.

**`F04000` / `MCIF` do not clear the #557 bars.** National F040 is +6.16%.
✅ Import Pearson on non-specials **now clears it** — 0.972 against a bar of
≳ 0.85, after the vehicle split fix (#702 / #748), up from 0.850. Hole rules sit
on #528. Whether to apply a national ITA (or other) control is #647.

**`F03000` and `F04000` are the whole of Step 1's error.** Between them they
carry 141 of the block's 144 cell misses; the other seventeen columns are at
100% coverage and the twelve government columns are exact cell for cell. See
§Per column.

**`F03000` is sourced, and a time series 2017-2023, but still not validated
per commodity.** ⚠️ The column total is **no longer right by construction** —
it is −10.8%, and the whole difference is the `Other industries` line carried as
visibly unallocated rather than smeared. The allocation is at **75.6% sign
agreement and 80% absolute error** against published gross (was 69.7% / 101%),
and **0.0% of its cells land inside this section's tolerance**. Farm now moves
on ERS inventory change; mining/utilities/construction is still one published
number for three sectors (#660), manufacturing needs per-industry stage shares
(#664), and `S00402` is an order of magnitude short (#665). Treat the column as
a first pass, not as a solved block.

✅ **Wired for 2017-2023 on 2026-08-28** — it had been all-zero for 2018-2023
in `derive_initial_Y_pur` despite the method shipping those years. See
§`F03000` as a series.

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
