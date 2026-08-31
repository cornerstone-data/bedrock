# What the seed owes the balance, and which issue closes each one

The standing triage list for the two **hard cross-block identities**, kept here rather than in a pull
request so it survives the merge that produced it. Sibling of
[`trade_data/About_row_exposure.md`](trade_data/About_row_exposure.md), which does the same job for the
trade columns alone.

⚠️ **Regenerate before quoting.** Every number below is a measurement of the artifacts on disk at the
time it was taken, and the artifacts change:

```bash
uv run python -m bedrock.analysis.nowcasting.control_residuals --check
uv run python -m bedrock.utils.validation.stale_artifacts --name Detail_Supply_Mix_   # are they current?
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

`Detail_Supply_Mix_<year>` disaggregates a published *summary* control onto a detail mix, and that summary
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

`T11` was re-measured at 2023 after the control: **21.5% — unchanged.** That is the expected result, not
a disappointment: the pre-ship comparison showed the industry pin closes only ~7% of the commodity-row
gap, and the measured worst rows are the row-side owners already filed — `531ORE` (residual 291,382,
exposure 252,540), the #769 insolvencies (`424200` −118,277, `424700` −45,764 with negative `T016`),
`533000`, `522A00`. `T17` was the identity this change owned; `T11` belongs to the row-side issues.

**Level 4 of the stack (`ec_mfg_go_adjustment_724`)**: manufacturing 2022–24 detail output is now
conditioned on EC 2022 inside `detail_gross_output_panel` (`ec_go_adjustment.py`) — ~145–153bn/yr of
within-group reallocation, group totals BEA, `334118` held in `PENDING_REVIEW` for Wes. The slot is a
registry (`SECTOR_CONDITIONERS`); next candidates construction (VIP, 130.6bn), agriculture (ERS, 32.1bn),
utilities (EIA, 23.5bn). Oil and gas is structurally out: `211` is a single-industry group. ⚠️ Step 5's
assembly must inject `adjusted_gross_output_usd` into T1 for 2022+ — `published_gross_output` reads the
unadjusted FBA parquet.

### → [#769](https://github.com/cornerstone-data/bedrock/issues/769) · the trade margin give-up

Not an accuracy question — a **feasibility** one. Trade output essentially *is* margin, so the 19 givers
hand over 90.8–100% of their own `T007` in the anchor year. The give-up then moves on a Census margin index
× a frozen 2017 coverage ratio while the output moves on `Detail_Supply_Mix`: two independent series
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

**RESOLVED in `trade_giveup_referee_769`** (2026-08-30), in three moves the referee analysis
([`trade_data/giveup_solvency.py`](trade_data/giveup_solvency.py)) licensed:

1. **The give-up level now comes from the published summary `Trade` cells, per giver group and year** —
   the same workbook `T007` comes from, so the two sides of `T016` share one source. The referee showed
   ours was the wrong side: BEA's implied coverage ratio ran 1.561 → 1.435–1.489 (wholesale, 2021–23)
   and 0.966 (retail 2023, the AIES splice's rate step), and the frozen ratio turned that into −341bn of
   give-up overstatement by 2023. Census keeps what it is evidence for: the within-group split and the
   tax index. The frozen-ratio construction survives as `census_index_control_total`, retired.
2. **Within a group the split is water-filled under each giver's own output** — published group `T016 ≥ 0`
   guarantees capacity; `454000`'s excess parks on its group-mates until the EC-2022 work (next on this
   stack) re-observes its output.
3. **`check_giveup_solvency` runs inside `derive_initial_supply_bridge`** — any giver beyond rounding
   dust fails the build; the baseline's `insolvent` column is a backstop, not the instrument.

After: **0 substantively insolvent givers in every year 2017–2023** (dust of a few $M where a published
group give-up exceeds group output by rounding); 2017 anchor preserved to $2M on 3,264,931.

### → [#770](https://github.com/cornerstone-data/bedrock/issues/770) · no Use interior for 2018 or 2019

**RESOLVED in `sas_cut_list_770`** (2026-08-30). The refusal's premise was wrong in a useful way: 2018–19
are not unobserved, they are published on a **cut item list** in `sas-19.xlsx` — 63 NAICS × 23 items,
fully populated — which this repo simply never fetched. Census's consolidation notes (via Wes) define the
bridge: twelve detailed items absorbed into `All other operating expenses`, two expensed items merged,
fringe merged (compensation, irrelevant here).

The fix is the directive it implements — **constrain at the published aggregate, assign within it on 2017
proportions**: `_cut_list_panel` synthesises each absorbed member at its sas-17 2017 value × the
aggregate's ratio taken *inside* `sas-19`, and the existing `relative_index`/`industry_growth` machinery
runs unchanged. Three facts that made it clean:

1. **`sas-19` is 2017-EC-benchmarked and restates 2017**, so the ratios cross no seam — these two years
   are in that respect *cleaner* than 2020–22, whose base is sas-17.
2. The two merged expensed items were both unmappable already (denominator-only), and fringe was already
   `NOT_INTERMEDIATE` — so the numerator bridge is one group.
3. The three discontinued items (`517*`, `221300`/`562000`, `532100`/`532400`) revive for exactly these
   two years: their 2017 bases exist and they sit inside the absorbed set.

Result: 83 (2018) and 81 (2019) of the 100 service/transport columns move;
`derive_initial_U_intermediate` builds **15.85T (2018)** and **16.12T (2019)**, bracketing sensibly
against 14.86T (2017) and the 15.34T COVID-dip (2020). The FBA carve keeps every existing year
byte-identical (max |diff| 0 on regeneration). Coarser than a full-list year — the absorbed members share
one ratio per industry — and the earlier caution stands recorded: the year-constant check was not a check
that the builders run.

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

## The stack, summed (2026-08-30, branch `supply_mix_rename_cap`)

The first measurement of the whole span on one tree — every stack level live, all blocks rebuilt:

| year | industry-output inconsistency (% of intermediate inputs) | trade commodities with negative total supply | supply-equals-use gap per commodity (% of intermediate use) |
|---|---:|---:|---:|
| 2017 | 1.4 | 2 (rounding dust) | 4.9 |
| 2018 | 1.3 *(was 4.8)* | 1 *(was 7)* | **9.1** *(unmeasurable before)* |
| 2019 | 1.9 *(was 7.3)* | 1 *(was 6)* | **11.6** *(unmeasurable before)* |
| 2020 | 5.5 *(was 13.7)* | 1 *(was 6)* | 19.3 |
| 2021 | 5.7 *(was 14.8)* | 0 *(was 7)* | 18.1 |
| 2022 | 6.7 *(was 16.9)* | 1 *(was 11)* | 20.0 *(was ~22)* |
| 2023 | 1.4 *(was 15.9)* | 0 *(was 8, −530bn)* | 20.4 *(was 21.5)* |

What the stack targeted, it fixed: the industry columns are consistent to near the anchor year's own
level, and no trade commodity's margin exceeds its output anywhere. What it never targeted — **the
commodity rows** — is now the one standing blocker: an 18–20% supply-equals-use gap in 2020–2023,
moved only ~2pp by everything above because its owners are the row-side estimates. The 2023 exposure
ranking says where it lives: the two real-estate rows (`531ORE` 252,479 of exposure, `533000` 78,686),
banking (`522A00` 69,303), the professional-services family (`541610`/`541800`/`54151A` ~147,000
together), services exports (#771) and the transport mix (#772) behind them. That ranking is the queue
for the row-side work, and the supply-equals-use gap is now in the `--check` baseline so it cannot
drift unremarked.

## Two instruments, not interchangeable

The span figures (`T17`, `T11`, solvency) are **internal-consistency** residuals between our own seed
blocks. The exposure figures are measured **against the published detail SUT** and exist only for 2017.
Findings on `T11` and `T17` both feed the row identity, so their exposures are **not additive**.
