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

**Snapshot date:** 2026-08-19. Step 1 is a live `derive_initial_Y_pur`
(`NIPA_final_dom_uses_2017`, Trade `F04000`, and — new this snapshot —
`Inventories_2017` on `F03000`). Step 4 is a live `derive_initial_supply_bridge`
(`MCIF` from `Trade_Imports_2017`; `MDTY` and `MADJ` sourced for 2017; other
bridge columns unsourced).

---

## Where the build stands

| block | step | shape | reference populates | reference total | candidate | coverage | accuracy |
|---|---|---|---:|---:|---|---:|---:|
| `use_fd_detail_sut` | 1 — final demand | 402 × 19 | 1,253 cells | $22.24T | live | **95.5%** | **55.1%** |
| `use_va_detail_sut` | 2 — value added | 3 × 402 | 1,189 cells | $18.92T | *none yet* | — | — |
| `supply_bridge_detail_sut` | 4 — supply bridge | 402 × 12 | 3,202 cells | $111.28T | live (MCIF, MADJ, MDTY) | **14.5%** | **21.2%** |

**coverage** = of the cells the reference populates, how many we populate.
**accuracy** = of the cells we populate, how many land within tolerance.

Those three blocks are the whole of what a published 2017 detail reference
supports outside the two 402 × 402 interiors. The reference columns are the
denominator: they are what "done" looks like, and they are known before the
corresponding step is built.

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
`rtol=0.01, atol=5e5, ramp=0.25` · **no candidate yet**

The reference, the frame and the bar are settled; Step 2 supplies the candidate
and the picture appears. The target:

| row | description | reference total |
|---|---|---:|
| `V00100` | Compensation of employees | $10.435T |
| `T00OTOP` | Other taxes on production, less subsidies | $0.609T |
| `V00300` | Gross operating surplus | $7.873T |
| | **total (`VABAS`)** | **$18.917T** |

1,189 of 1,206 cells are populated in the reference, so this block is nearly
dense — unlike final demand, there is almost nowhere for a miss to hide as a
structural zero.

**The valuation is the thing to get right.** `T00OTOP` is *other* taxes on
production less subsidies at basic prices. It is **not** the MUT's `V00200`,
which is taxes on production *and imports* at producer prices. A candidate built
on `V00200` will not match this reference, and the section deliberately does not
alias the two — the gap shows as a `MISS`/`EXTRA` pair rather than a bad match.

Turning it on: point `Section.candidate` at the Step 2 output.

---

## Step 4 — Supply table, bridge to purchaser value

`supply_bridge_detail_sut` · 402 commodities × 12 bridge codes · tolerance
`rtol=0.01, atol=5e5, ramp=0.25`

![Step 4 supply bridge match](images/supply_bridge_detail_sut_2017.png)

| scope | absent | match | partial | miss | extra |
|---|---:|---:|---:|---:|---:|
| cells | 1,599 | 98 | 365 | 2,739 | 23 |
| row totals | 9 | 0 | 279 | 114 | 0 |
| column totals | 0 | 2 | 1 | 9 | 0 |

| | |
|---|---:|
| coverage | 14.5% |
| accuracy | 21.2% |
| candidate grand total | $2.70T |
| reference grand total | $111.28T |
| grand total error | 97.6% |

**Unchanged this snapshot.** The Step 4c margin work since 2026-08-15 — #610
(2017 rates), #612 (annual wholesale and retail levels), #611 (the pipeline
transport margin, and BEA's replies) — is derivation and sourcing that has not
yet been written back into the bridge columns, so the picture is the same one.
`TRADE`/`TRANS` land here at #613.

`MCIF`, `MADJ`, and `MDTY` are sourced for 2017 (`Trade_Imports_2017` mapped
Detail mass; Census `GEN_CHA_YR` reassigned onto Supply `MADJ` destinations and
leveled to Supply `MADJ`; Census duty rates leveled to NIPA `B235RC`). Column
totals for `MADJ` and `MDTY` match the published national (within the section
tolerance). `MCIF` is `partial` at the column total (+1.29% vs published).
`T007`, `TRADE`/`TRANS`/`T014`, `TOP`/`SUB`/`T015`, and the `T013`/`T016`
identities are unsourced.

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

**Nothing here is a rollup.** Every number is BEA 2017 detail. Margins and
redefinitions net out at summary and above, so an aggregate view of these blocks
would pass on data these pictures show to be broken.
