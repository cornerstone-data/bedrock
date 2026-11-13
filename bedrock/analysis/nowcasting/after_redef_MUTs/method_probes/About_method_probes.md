# What the 2017 tables and the summary span say about how to do Step 7

Three probe scripts, run 2026-09-02, that picked the Step 7 method. Each is
standalone: `uv run python <script>.py`. Data: published detail before/after
2017 MUTs, and the published summary before-redefinitions (2017–2024) and
after-redefinitions (2024 vintage) tables — the loaders live in
`bedrock/extract/iot/io_2017.py`, ported from #775.

## 1. `redef_2017_probe.py` — the Make movement is a pattern, not a rate

At 2017 detail, redefinition movement is strictly off-diagonal → the same
commodity's diagonal (no reverse moves, no diagonal losses), and commodity
output is invariant to $10M — publication rounding. Of the 1,880 moving
cells, **1,850 move at ~100%** of the before-redefinitions cell value
($1,283bn of $1,325bn). The 30 partial cells ($42bn) are structured activity
splits, not noise — e.g. 541512→541511 moves 3.4%, 622000→621600 moves 37%,
541200→541610 moves 82% — and become a small learned fraction-exception list.

The same script also rejects recipe-based Use reconstruction: moving the
donor's or the receiver's average input recipe with the output scores **worse
than leaving the table alone** (cell L1 7.6% / 6.4% against 3.7% for
do-nothing), because…

## 2. `redef_2017_probe2.py` — the moved activities are input-light

Intermediate inputs moved per dollar of moved output are **0.24** in
aggregate — roughly half an average industry recipe — and industry-specific
to the point of sign flips (541511 gains $67bn of output with *negative* net
intermediates; 722A00 is 0.63; 713200, 811400, 233240 are ~0). Redefined
activities (trade margins, own-account construction, secondary retail) are
labor- and value-added-heavy. No average recipe can stand in for them.

## 3. `redef_span_probe.py` — the out-of-sample verdict, and a split crown

Learn everything at summary 2017, apply to each year's own published
before-redefinitions table, score against the published after tables
2018–2024. Cell L1 as % of the after-table's mass:

| year | V do-nothing | V pattern | V ratio-carry | U do-nothing | U ratio-carry | U flow |
|-----:|---:|---:|---:|---:|---:|---:|
| 2018 | 7.57% | **0.05%** | 0.16% | 3.24% | **0.27%** | 0.44% |
| 2021 | 7.36% | **0.08%** | 0.49% | 2.52% | **0.99%** | 1.31% |
| 2024 | 7.68% | **0.12%** | 0.49% | 2.79% | **1.06%** | 1.33% |

- **Make: the pattern method wins 3–4× over the ratio carry, every year**
  (98.4–99.3% of the redefinition error removed through 2024). Moved amount =
  2017 fraction × the year's own cell.
- **Use: the cell-ratio carry wins** (#775's construction, applied to the
  year's own before table): fewer bad cells too (5 vs 26 at 2018, 61 vs 91 at
  2024). The multiplicative form rides each cell's own nominal level; an
  additive 2017-composition delta cannot.
- A hybrid (ratio cells rescaled to flow-predicted column totals) is worse
  than both (2.38% at 2021) — rejected.

## The method this picks

**Make** from the pattern: zero the mapped (donor, commodity) cells of the
year's own before-redefinitions Make, credit the commodity's diagonal, with
the 2017 fractions kept for the small partial list. **Use** from the
cell-ratio carry on the year's own before-redefinitions table. The frozen-
ratio critique in #775's review lands hardest on the Make side, which the
pattern replaces; the Use-side ratios are vindicated on this instrument.

Rules context (methods discussion #3, "Background on Redefinitions"): BEA
always redefines construction, manufacturing-in-nonmanufacturing,
trade-in-nontrade (never wholesale↔retail), rental, and
services-in-nonservice — the observed 2017 pattern is largely these rules
made concrete. Industry output moves; commodity output never does — a per-year
identity gate the implementation must carry.

Still to design: the value-added rows, the after-redefinitions margins and
import tables, and the S00102/S00203 government-enterprise reallocation.

## 4. `redef_span_closure_probe.py` — close each row on the cells that moved its way

The Use carry overshoots or undershoots each commodity row, and the row is
closed back to its before total. The first closure spread the net change over
every changed cell by |delta|, so an overshooting row also took from the cells
the carry had nearly emptied. At detail in 2024 that left 146 negative Use
cells (-$7.6bn) against 8 from the carry alone. In legal services `541100`,
aerospace `33641A` went from $1,808M to $15M (2017 ratio 0.008) and then to
-$447M (#1270).

Closing on the same-sign cells only (an overshooting row gives back from the
cells that gained, an undershooting row from the cells that lost) keeps every
cell between its before and carried values. Same instrument as section 3,
cell L1 as % of after-table mass:

| year | carry, no closure | all-delta closure | same-sign closure |
|-----:|---:|---:|---:|
| 2018 | 0.269% | **0.255%** | 0.261% |
| 2019 | 0.406% | 0.352% | **0.323%** |
| 2020 | 0.798% | 0.740% | **0.659%** |
| 2021 | 0.985% | 0.806% | **0.678%** |
| 2022 | 0.824% | 0.717% | **0.664%** |
| 2023 | 0.937% | 0.828% | **0.732%** |
| 2024 | 1.062% | 0.947% | **0.774%** |

Same-sign wins 2019-2024 by 8-18%, with 2018 a tie, and it has fewer bad cells
(off by >10% and >$100M) in every year after 2018. Negative cells fall from
13-15 to 5-7 a year from 2020. At detail on the `e7715da` before tables, it
leaves only the 8 negative cells already in Step 6's tables.
