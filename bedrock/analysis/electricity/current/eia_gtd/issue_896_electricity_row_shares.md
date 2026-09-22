# Issue #896 — electricity Use row share collapse (plain language)

Companion to the implementation plan for
[issue #896](https://github.com/cornerstone-data/bedrock/issues/896).
**Schemas, gate thresholds, and file-level steps** live in the Cursor plan
`issue_896_electricity_row_2dccb22e` and in
[`About_896_row_gate.md`](About_896_row_gate.md). Method background:
`About_why_the_flat_price_fails.md` (point 6) and `About_price_proposal.md`.

Reproduce the original measurement with:

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_control \
    --mut-vintage v0.3.0_4276083 --csv
```

Phase 1 diagnostic gate (bands / target gate / stages / seed status / AIES seam):

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 \
    --mode all --mut-vintage v0.3.0_4276083 --csv --check
```

## Why this work exists

The model’s electricity **bills** (who spends how much on electricity) look wrong
in 2023–24: electricity’s share of all intermediate purchases fell sharply.
Later work that splits each bill into generation vs delivery (G/T/D v3) cannot
be judged until those bills are fixed and measured on their own. The plan does
that in order: **find where the dollars went wrong → fix the bills → only then
change the splitter**.

## Phase 1 — Figure out *what* broke (diagnostics only)

**In plain terms:** Build better measurement tools. Do not change the production
model yet.

We already know the electricity row shrank as a share of the economy in 2023–24.
We do *not* yet know whether:

1. **The total budget for intermediate electricity was cut** (the “target”
   fell — supply of electricity minus final demand), so the whole row was scaled
   down together, or
2. **The total was fine, but the dollars were reshuffled** among industries
   (especially sectors with weak or missing survey anchors).

Phase 1 answers that with concrete checks:

| Sub-step | Plain question |
|---|---|
| **1.1 Bands** | Of the lost dollars, how much came from manufacturing vs services vs wholesale/retail vs other? |
| **1.2 Target gate** | Did the *allowed* intermediate total for electricity fall, or only the *realized* Use row? That decides the next phase. |
| **1.3 IPF vs GRAS** | Did the shrink happen in the first “fit the margins” step (whole-row rescale) or later when balancing free cells? |
| **1.4 Seed/mask status** | For the industries that lost the most: were they surveyed, held at 2017, or free for the balancer to move? |
| **1.5 AIES seam** | For services (restaurants, hospitals, etc.), did the 2023 survey switch (SAS → AIES) jolt purchased-electricity indexes? |
| **1.6 Docs** | Write down how to run and read these results so the decision is reproducible. |

**Exit:** A clear label — “target collapse” or “allocation problem” — plus CSVs.
Still no production table change.

## Phase 1 results (MUT `v0.3.0_4276083`, run 2026-09-22)

**Bottom line:** The 2023–24 share drop is primarily a **target collapse** — the
*allowed* intermediate-electricity budget (`T016 − ΣY`) fell in step with the
realized Use row. That is the Phase 1 gate’s cause label. Unseeded trade and
services then absorbed most of the column-level share-effect dollars when the
row was fitted down. The SAS→AIES electricity seam is **not** the driver.

Reproduce:

```bash
python -m bedrock.analysis.electricity.current.eia_gtd.electricity_row_896 \
    --mode all --mut-vintage v0.3.0_4276083 --csv --check
```

Artifacts: `electricity_row_896_*.csv` in this folder. How to read schemas:
[`About_896_row_gate.md`](About_896_row_gate.md).

### 1.2 Target gate — what broke

| Year | Target $bn | Realized Use $bn | Realized share of all intermediate | Supply−use gap $bn |
|---:|---:|---:|---:|---:|
| 2022 | 419.6 | 418.9 | **2.02%** | 0.7 |
| 2023 | 358.3 | 352.0 | **1.70%** | 6.3 |
| 2024 | 337.6 | 330.3 | **1.54%** | 7.3 |

| Span | Gate classification | Target fraction of level move | Shares move together? |
|---|---|---:|---|
| **2022→23** | **`target_collapse`** | 0.92 | yes |
| **2023→24** | **`target_collapse`** | 0.95 | yes |

In plain terms: almost all of the dollar decline in the Use row is mirrored by a
decline in the **row target** itself (supply of electricity less final demand).
This is **not** “the total was fine and RAS stole from commercial columns”
as the first-order story — though those columns still show large share effects
once the smaller total is imposed (below).

**Track:** Phase **2T** first (trace / fix `T016` and `ΣY` for `221100`), then
Phase 2A for residual band defects. Sensitivity cuts at 0.60 and 0.80 give the
same label on both crisis spans.

### 1.1 Bands — where the share-effect dollars landed

Share effect = electricity changing its bite of each purchaser’s input column
(the part the electricity method “owns”). Crisis spans, $bn:

| Band | 2022→23 | 2023→24 |
|---|---:|---:|
| **trade** (seed-set wholesale/retail) | **−17.3** | −3.8 |
| **services_transport** | −13.8 | **−19.4** |
| manufacturing | −9.3 | −2.9 |
| held_2017 (construction, gov, etc.) | −8.0 | −1.4 |
| utilities | −3.8 | −1.2 |
| mining | −2.1 | −0.8 |
| agriculture | −0.4 | ~0 |
| trade_unseeded | −0.3 | ~0 |

So: **2022→23** the largest bite is **trade with no electricity seed**;
**2023→24** the largest bite is **services**. That matches the “commercial
band on the carry” intuition for *where* the rescale shows up — but the gate
says the rescale is driven by a **smaller national intermediate budget**, not
by those seeds alone.

### 1.3 Stages (IPF vs GRAS) — when in the pipeline

On the top losers, moving from the Step-3 seed to the IPF fit
(`assemble_use_seed` fitted=False → True) cuts 2023 cells by on the order of
**$1–2bn each**, while GRAS (pinned MUT vs that IPF) is usually small
(tens of millions; hotels are an exception). In plain terms: the shrink happens
when the interior is **fitted to the lower row target**, not mainly in the later
free-cell balancer.

### 1.4 Seed / mask status — top losers 2022→23

| Industry | Band | Electricity in seed? | Cell free for balancer? | Share effect $bn |
|---|---|---|---|---:|
| 221100 utilities | utilities | no | yes | −3.6 |
| 445000 food/beverage stores | **trade** | **no** | yes | −3.1 |
| 722211 limited-service restaurants | services | yes | yes | −3.0 |
| 622000 hospitals | services | yes | yes | −2.4 |
| 452000 general merchandise | **trade** | **no** | yes | −2.3 |
| 325190 organic chemicals | manufacturing | yes | yes | −2.1 |
| 550000 management of companies | **held_2017** | **no** | yes | −2.1 |

Several of the biggest losers have **no electricity overlay** (trade, held_2017,
utilities) and free Use cells — exactly the columns that can give up dollars when
the row target falls.

### 1.5 AIES seam — not the cause of the decline

For the large services losers with a measurable SAS-2022 vs AIES-2023 purchased-
electricity index, the index **rose** (restaurants ~+7%, hospitals ~+4%). Holding
the 2022 index would have made those cells *smaller*, not larger — so the
SAS→AIES electricity handoff does **not** explain their share-effect losses.
Trucking’s index jumped sharply and its share effect went *up*. Services-band
share effect for 2022→23 is **−$13.8bn** (under the plan’s $15bn seam-fix
trigger); the seam counterfactual does not clear the “explains ≥25% of the band”
bar either.

### What to do next

1. **Phase 2T** — explain and/or fix the 2022→24 drop in the electricity
   intermediate **target** (Supply `T016` and/or final demand `Y` for `221100`).
2. **Then Phase 2A** — seed trade (#899), fix paper zeros (#900), and only touch
   the services electricity seam if a post-2T re-measure still shows a material
   services-band defect.
3. Do **not** change the G/T/D allocator until a new MUT clears the row gate.

## Phase 2T — If the *total* electricity budget fell

**In plain terms:** Phase 1 classified both crisis spans as **`target_collapse`**,
so this track is **required**, not optional. The bug is upstream of industry
seeding: how we build electricity’s total supply and/or final demand for
2023–24 (data handoffs, anchors, controls). Fix that source so the allowed
intermediate total stops breaking — or document that the movement is real and
observed. Rebuild tables, re-run Phase 1, then clean up any leftover
industry-level issues with Phase 2A.

## Phase 2A — If industries are mis-allocated (default expected path)

**In plain terms:** The total may be roughly right, but who gets how much
electricity inside the table is wrong. Fix that in small, separately measured
steps. **Do not change the G/T/D splitter yet**, so any improvement in “capped
purchasers” can be credited to better bills.

| Sub-step | Plain meaning |
|---|---|
| **2A.1 (#900)** | Paper mills sometimes show **$0** electricity in years they clearly operate. Fix that cell and add a guard against “zero in the middle of a normal series.” Small dollars; needed for trust and acceptance. |
| **2A.2 Services seam** | If Phase 1 shows services own a big chunk of the loss, fix only the **purchased electricity** path across the 2023 survey change — don’t rewrite the whole services seed. |
| **2A.3 (#899) Trade** | Wholesale/retail largely has **no electricity survey seed** today, so those columns can absorb balancer pressure. Index trade electricity cells on **QCEW payroll by BEA detail** (`qcew_detail_payroll`), graded before wiring. Do not use the quinquennial BES as the 2023–24 production seed. |
| **2A.4 Re-measure** | Rebuild a MUT vintage; re-check share of intermediate use and capped-purchaser counts with the splitter unchanged. |

**#896 is “done” when:** electricity’s share is stable again (or explained by a
real series), no bogus $0 bills, trade is seeded on an annual observed series,
and the services electricity seam is fixed if Phase 1 showed it owned material
dollars. Construction/government may stay on the carry for this issue.
Manufacturing’s long-standing shape gap vs Census/MECS is **not** required for
this exit — that is Phase 3.

## Phase 3 — Manufacturing shape (#898)

**In plain terms:** After the 2023–24 share crisis is handled, improve *how
manufacturing electricity is divided among industries* — align to MECS at survey
anchors (2018/2022) and use Census costs as the year-to-year index. This is a
shape fix, not the same as the 2023–24 share collapse, and it comes after #896
so the two improvements stay separable.

## Phase 4 — Unlock the allocator (#902)

**In plain terms:** Only a checklist in this plan. Once bills are fixed and
pinned in a new table vintage, a *separate* project can replace the flat national
generation price with class prices and residual T&D. This plan deliberately does
**not** implement that splitter change — so you never mix “better bills” and
“better split” in one opaque release.

## How the phases fit together

```text
Phase 1: Measure  →  (2T if total budget broke)  →  2A: Fix who gets the bills
                                                      ↓
                                              #896 closed
                                                      ↓
                                         Phase 3: Manufacturing shape
                                                      ↓
                                         Phase 4: Change the splitter
```

**One rule throughout:** change one thing, measure one thing. That is how you
prove the 2023–24 problem was the Use row — and later prove that G/T/D v3
actually helped.
