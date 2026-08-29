# What the seed owes the balance, and which issue closes each one

The standing triage list for the two **hard cross-block identities**, kept here rather than in a pull
request so it survives the merge that produced it. Sibling of
[`trade_data/About_row_exposure.md`](trade_data/About_row_exposure.md), which does the same job for the
trade columns alone.

⚠️ **Regenerate before quoting.** Every number below is a measurement of the artifacts on disk at the
time it was taken, and the artifacts change:

```bash
uv run python -m bedrock.analysis.nowcasting.control_residuals --check
uv run python -m bedrock.utils.validation.stale_artifacts --name Detail_Supply_   # are they current?
```

**State reflected:** the seed at `44c1dd6` — `nowcast` plus [#766](https://github.com/cornerstone-data/bedrock/pull/766)'s
`S00300` sourcing, open at the time of measurement. `#766` moves `MCIF` and `F02N00` on one commodity and
does not touch either identity's shape.

## How to read it

⚠️ **2017 is not evidence, and that is the whole finding.** Whole blocks are anchored on, or rescaled to,
the published 2017 tables, so they reproduce 2017 by construction. Against published 2017 they score:

| block | gross \|error\|, 2017, $M | why |
|---|---:|---|
| `TRADE` | 1 | anchored on the published give-up |
| `TOP`, `SUB` | 1 | anchored on the published 2017 columns |
| `MADJ` | 0 | levelled to published `MADJ` |
| `T007` | 291 | the published detail mix on both sides |
| Use interior | 1,017 | the published interior, rescaled |
| `F01000` PCE, all twelve government columns | 0–79 | one datum spread over many commodities |

Both identities avoid this because **both sides of each are our own seed** — no published answer key is
involved, so they run on 2018–2023 where no detail SUT exists to compare against:

```
T11   per commodity   T016[c]  =  T019[c]
T17   per industry    supply.col[i] + T00TOP[i] + T00SUB[i]  =  use.col[i]
```

Read them as a **trend away from the anchor**, never as a level at 2017.

## Open

### → [#724](https://github.com/cornerstone-data/bedrock/issues/724) · the Supply industry column

`Detail_Supply_<year>` disaggregates a published *summary* control onto a detail mix, and that summary
block is its only control — so the detail **industry** axis is unconstrained.
[`nowcast_targets`](../../transform/iot/nowcast_targets.py) records that `T17` is *"the only constraint the
Supply industry columns have — without it that whole axis is free"*, so the entire residual is absorbed by
the Supply interior and reaches commodity rows from there through the make mix.

| year | gross \|T17\| $M | % of `T005` | ind >1% | >25% | >50% |
|---|---:|---:|---:|---:|---:|
| **2017** | *210,493* | *1.4%* | *47* | *3* | *0* |
| 2018 | 754,586 | 4.8% | 344 | 32 | 10 |
| 2019 | 1,177,727 | 7.3% | 369 | 60 | 18 |
| 2020 | 2,105,893 | 13.7% | 382 | 124 | 47 |
| 2021 | 2,678,422 | 14.8% | 384 | 135 | 46 |
| 2022 | 3,464,064 | 16.9% | 385 | 154 | 65 |
| 2023 | **3,299,045** | **15.9%** | **366** | **157** | **73** |

**It breaks in 2018**, four years before the 2022 Economic Census mix — so the unconstrained axis is the
cause and [#570](https://github.com/cornerstone-data/bedrock/issues/570)'s mix change only aggravates it.
The 385-above-1% for 2022 independently reproduces #724's own *367 of 401*, measured through a ratio to
BEA detail GO rather than through the identity.

`T11` behaves the same way: **4.9%** of `T001` at 2017 and **21–23%** in every nowcast year.

**RESOLVED in `step4a_go_control_724`** (2026-08-29):
[`nowcast_supply_go_control`](../../transform/iot/nowcast_supply_go_control.py) pins each detail industry
column to its share of BEA detail GO within its summary group, biproportionally, preserving every published
summary Supply cell to <$1M. After the control:

| year | t17_pct before | after | ind >50% before | after |
|---|---:|---:|---:|---:|
| 2018 | 4.8% | **1.3%** | 10 | 0 |
| 2020 | 13.7% | **5.5%** | 47 | 2 |
| 2022 | 16.9% | **6.7%** | 65 | 4 |
| 2023 | 15.9% | **1.4%** | 73 | 1 |

Three things the after-numbers mean, so they are not re-litigated later:

1. **The pre-control residual was BEA's detail GO moving, not our seed drifting** — decomposed, 2023 was
   97.9% within-summary-group, and of that the GO mix moved 1,714,908 $M (half-gross) against our mix's
   128,532. The control imports BEA's within-group movement; it does not invent one.
2. **What remains is the between-group term** — BEA's own summary Supply and detail GO series disagreeing
   at group level — and it peaks in 2020–2022, the years
   [`summary_axis_audit`](summary_axis_audit.py) shows the pinned 2017–2022 summary workbook diverging
   from the newer vintage the GO panel reads (gross workbook-to-workbook diff 9.1tn on Supply 2022,
   confined to 2019–2022; 2017 and 2018 identical in both). No within-group operator can reduce it;
   re-pinning the summary vintage is a separate decision with its own blast radius.
3. **Solvency (#769) moved both ways and is NOT resolved**: negative mass fell (2023 −530,409 → −389,570,
   `454000` alone recovered ~146,000) but 3–4 small givers tipped marginally negative as `T007`
   redistributed (insolvent count 8 → 11 in 2023). #769's guard is still owed.

`T11` is expected to improve with the same change (`T007` is the controlled block's row margin) but was
not re-measured here; the 21–23% figure above predates the control.

### → [#769](https://github.com/cornerstone-data/bedrock/issues/769) · the trade margin give-up

Not an accuracy question — a **feasibility** one. Trade output essentially *is* margin, so the 19 givers
hand over 90.8–100% of their own `T007` in the anchor year. The give-up then moves on a Census margin index
× a frozen 2017 coverage ratio while the output moves on `Detail_Supply`: two independent series
differenced on a knife edge.

| year | insolvent givers | negative total supply, $M | max give-up |
|---|---:|---:|---:|
| **2017** | *2 / 19* | *−2* | *100.0%* |
| 2018 | 7 / 19 | −31,180 | 104.2% |
| 2019 | 6 / 19 | −33,076 | 107.8% |
| 2020 | 6 / 19 | −137,720 | 132.4% |
| 2021 | 7 / 19 | −240,204 | 125.9% |
| 2022 | 11 / 19 | −313,777 | 125.7% |
| 2023 | **8 / 19** | **−530,409** | **151.1%** |

Worst in 2023: `454000` nonstore retailers **−186,501**, `441000` motor vehicle dealers −118,261,
`424700` petroleum wholesale −51,273. The give-up grows **+54.5%** across the span, 3,264,931 → 5,045,737 $M.

❌ **A negative supply row is a hard stop, not a residual.** `T11` would demand a negative *Use* row for a
trade commodity, the sign locks refuse it, and GRAS works multiplicatively on positive mass.
[`trade_margin_column`](../../transform/iot/nowcast_trade_margins.py) checks that the column sums to zero —
target `T16` — and nothing else. There is no solvency guard.

⚠️ [#749](https://github.com/cornerstone-data/bedrock/issues/749) ran the seed at **2017 only** and saw the
*opposite* symptom: the `TRADE` column empty, so every trade commodity carried its full output on the
Supply side. That has since been fixed. The fix is right at 2017 and unsafe in every other year.

Sequence with #724 — trade output *is* margin, so the two are measuring the same dollars from opposite axes.

### → [#770](https://github.com/cornerstone-data/bedrock/issues/770) · no Use interior for 2018 or 2019

`derive_initial_U_intermediate(2018)` and `(2019)` **raise**. The refusal inside
[`services_transport_expense_seed`](services_transport_expense_seed.py) is correct — neither SAS vintage
publishes the detailed items and the two years sit on the sas-17/sas-22 benchmark seam. The defect is that
[`composed_seed`](../../transform/iot/nowcast_intermediate.py) calls it *unconditionally*, so one block's
honest refusal takes down the whole 402 × 402 interior including the five blocks that **do** have 2018–19
observations.

⚠️ The earlier reading that "every block spans 2017–2023 and none is year-gated below the milestone" was a
check of the **year constants**, not of whether the builders run.

### → [#771](https://github.com/cornerstone-data/bedrock/issues/771) · the services export mix

2017 is a genuine test here — the export column is built from its own outside source, not anchored.

| half | ours | published | net | gross \|err\| | exposure |
|---|---:|---:|---:|---:|---:|
| goods `1–3xxxxx` | 1,469,725 | 1,241,361 | +18.4% | 474,753 | 210,038 |
| **services `4–8xxxxx`** | **824,562** | **841,623** | **−2.0%** | **209,998** | **123,629** |

The goods `+18.4%` reproduces #762's measured `+18.1%`, which is what establishes the comparison is sound.
The services half is a **2% level error carrying a 25% gross error** — a pure redistribution across
commodity rows that #762's re-export correction cannot reach, because it is goods-only. At 123,629 $M this
is the largest unowned row-axis exposure after the two trade blocks that already have owners.

Signature is a concordance fault: repair services fabricated (`811200` 815×, `811400` 701×, `811100` 251×,
`811300` 246×), the `541x` family short to match (`541300` 0.4×, `541800` 0.7×, `541512` 0.7×), whole rows
dropped (`550000` 0 against 4,296; `531ORE` 0 against 3,274).

### → [#772](https://github.com/cornerstone-data/bedrock/issues/772) · the transport margin mix

`TRANS` is built per mode from each mode's own observed freight revenue and never touches the published
column, so 2017 is a real test for it too.

Level right to **0.008%** (415,548 against 415,580), the same **263** receiving commodities, `T16` holds —
and **89,639 $M = 21.6% of the freight bill** on the wrong rows, exposure 55,660. Systematically biased
toward bulk: `211000` oil and gas +15%, `1111B0` grain +33%, `212100` coal +29%, `2123A0` nonmetallic
mining +43%, `331110` iron and steel +42%, `325190` organic chemicals +47%; against `324110` refined
petroleum −25% and `1121A0` beef −24%. That is the shape of a tonnage basis standing in for a revenue one.

Those rows carry ι of 0.8–1.0, so the error lands in the technology matrix rather than in final demand, and
they sit in the highest-`N` sectors — rank it on total EF, not direct.

❌ **Not [#672](https://github.com/cornerstone-data/bedrock/issues/672)**, which replaces the
`Margins_Transport` within-group weight because it depends on the published Supply table. This is about
which commodities the freight bill lands on, and survives that change.

## Deferred, with the reason

The 2017 ι-weighted exposure ranking, $M — the error times the share of the row that goes to industry,
which is what actually reaches the technology matrix:

| block | exposure | owner |
|---|---:|---|
| `MCIF` imports | 259,265 | #670 #763 #701 |
| `F04000` goods exports | 210,038 | #762 |
| **`F04000` services exports** | **123,629** | **#771** |
| **`TRANS`** | **55,660** | **#772** |
| `F03000` inventories | 44,097 | #665 #660 — deferred |
| `F02N00` (`S00300`) | 5,422 | #767 |

⚠️ **`F03000` has the worst ratio on the board and stays deferred.** Its 2017 error is 96,302 $M against a
column whose own gross size is 65,723 — **146.5%** — but only 44,097 of exposure, because the column is
small and signed. **Relative severity is not the promotion test; what the hard control *moves* is.** That
contrast is the clearest available statement of what the test actually is, and it is the same arithmetic
that keeps #772 above #665/#660.

## Two instruments, not interchangeable

The span figures (`T17`, `T11`, solvency) are **internal-consistency** residuals between our own seed
blocks. The exposure figures are measured **against the published detail SUT** and exist only for 2017.
Findings on `T11` and `T17` both feed the row identity, so their exposures are **not additive**.
