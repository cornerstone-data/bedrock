# nimble_compare

Check a candidate dataset against a BEA reference table — cell by cell and in
total — without first building an exact crosswalk.

Built for reconnaissance. Alignment cascades from codes to names to fuzzy names
and reports which pass produced each pair, so you can see the weak links instead
of having them buried. When a number needs to be defensible, promote the
comparison to a real crosswalk in `bedrock/utils/taxonomy/`.

## Use

```python
from bedrock.analysis.nimble_compare import bea_matrix_row, compare, nipa_sheet

result = compare(
    candidate=nipa_sheet(SECTION6_XLSX, 'T60200D-A', 2017).leaves(),
    reference=bea_matrix_row('V00100'),          # Use SUT detail, compensation row
    rollup='industry_to_summary',                # 402 detail -> 71 summary industries
)
print(result.report())
result.to_csv('out/compensation.csv')
```

Everything is a `LabeledSeries`: a tidy `code / name / value / level` frame. The
loaders build them, `compare` consumes two of them.

| Reference (BEA) | |
|---|---|
| `bea_matrix_row('V00100')` | a row of a 2017 detail matrix, across industries |
| `bea_matrix_column('230301')` | a column, down commodities |
| `bea_summary_sut_row('V00100', 2017)` | the summary Use SUT, no rollup needed |

| Candidate (anything) | |
|---|---|
| `nipa_sheet(path, sheet, year)` | a year column of a NIPA `SectionNall_xls.xlsx` sheet |
| `fba_series(source, year, ...)` | anything already generated as a FlowByActivity |
| `table_series(path, value=..., name=...)` | an arbitrary csv/xlsx |
| `frame_series(df, value=..., name=...)` | an in-memory frame |

## Granularity: the reference stays the detail table

`rollup` aggregates the reference *before matching*; it does not switch to BEA's
published summary table. The detail table remains the source, and the comparison
happens at the granularity the candidate can actually address — which for a NIPA
industry sheet is roughly summary, since those sheets have no "Oilseed farming"
row to compare against.

That choice is reported, not implied. Every cell keeps its composition:

| column | |
|---|---|
| `n_detail` | how many reference codes the cell sums (`1` = not aggregated) |
| `detail_members` | those codes and values, `211000=29293;...` |

```python
result.one_to_one()      # cells that are a single detail code -> true detail comparison
result.aggregated()      # cells that sum two or more
result.detail('111CA')   # the codes behind one cell
```

The distinction matters when reading results: a cell with `n_detail == 1` is
evidence about one detail industry, while `n_detail == 29` is evidence about an
aggregate and says nothing about its parts. Don't let the two read alike.

## Reading the report

Totals are split three ways — matched candidate, matched reference, and the
unmatched remainder on each side — because the two ways these checks fail need
different responses:

- **matched cells disagree** → a real data difference, look at `worst()`
- **rows go unmatched** → the two tables partition the economy differently

The second is the common one, and it is not a bug. NIPA splits wholesale trade
into durable/nondurable where BEA summary does not; BEA splits real estate into
housing/other where NIPA does not; the two carve federal government along
different seams entirely (civilian/military vs defense/nondefense).

Three knobs close those gaps, all by code or name:

```python
candidate.leaves(keep=['N4037C'], drop=['N4038C', 'N4039C'])   # use NIPA's parent
compare(..., merge_reference={'RE': ['HS', 'ORE']})            # sum BEA's parts
compare(..., overrides={'N4055C': '511'})                      # just say it
```

## Gotchas

**Hierarchy.** NIPA sheets interleave subtotals with leaves. Summing one as
published double counts, so call `.leaves()` — it uses the sheet's own
indentation, and reports how many rows it dropped.

**Fuzzy name matching is opt-in** (`on='fuzzy'`), and tightly cut off (0.88) even
then. BEA builds classifications by splitting a name into "Other X" and "All
other X", so a label that reads almost like another is usually one *part* of it.
Across granularity this is actively dangerous — "Support activities for mining"
scores 0.90 against detail code `323120`, "Support activities for **printing**",
an industry off by a factor of 20. Unmatched rows invite you to look; a
plausible wrong pair does not. Prefer `overrides` to enabling fuzzy.

`on` values: `'auto'` (default, all exact passes), `'fuzzy'`, `'code'`, `'name'`.

**Units.** BEA publishes every table reachable here in millions of dollars, and
nothing is rescaled implicitly. `BEA_NIPA` FBAs are the exception — they store
dollars, so pass `scale_candidate=1e-6`. A wildly wrong total is usually this.

**Signs.** NIPA's "Less:" lines are summed as published, not negated.

## Worked example

```
uv run python -m bedrock.analysis.nimble_compare.examples.nipa_compensation_vs_sut_v00100
```

NIPA table 6.2D compensation of employees against Use SUT detail row `V00100`,
2017, in three stages:

- **stage 0** — the 402 detail codes untouched. Only 17 of 74 NIPA rows find a
  partner and 385 detail codes go unmatched: the evidence that 6.2D has nothing
  at detail granularity. Those 17 agree exactly.
- **stage 1** — rolled up to the 71 summary groups NIPA can address. 61 pair on
  name alone, all within BEA's rounding, and 17 are 1:1 with a single detail code.
- **stage 2** — the ten partition mismatches reconciled, about twenty lines,
  closing to 69/69 cells and −1 million on a $10.4 trillion total.
