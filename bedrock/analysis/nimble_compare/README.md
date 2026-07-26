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

## When there are no cells to compare

Some pairs share no correspondence at all — NIPA 3.5 is organized by level of
government and kind of tax, the Use table's `T00OTOP` by industry. There is
nothing to match, and the totals *are* the answer. Select the part of the
candidate that corresponds and read the totals block:

```python
sheet = nipa_sheet(SECTION3, 'T30500-A', 2017)
compare(sheet.select(['LA000237', 'LA000365']), bea_matrix_row('T00OTOP')).totals
```

| | |
|---|---|
| `select(labels)` | just these rows, by code or name |
| `subtree(label, include_parent=, leaves_only=)` | everything nested under a row, following the sheet's indentation |

Forcing this shape into cells is worse than useless: BEA's detail industry list
has a row genuinely named `Customs duties` (`4200ID`), so a name match pairs
NIPA's federal customs receipts with the other-taxes row of the customs-duties
*industry*, which is zero. A `matched cells: 0` line is the honest output.

## Gotchas

**Hierarchy.** NIPA sheets interleave subtotals with leaves. Summing one as
published double counts, so call `.leaves()` — it uses the sheet's own
indentation, and reports how many rows it dropped.

**Hierarchy is read from labels, before any similarity is computed.** BEA carves
a residual out of a parent and names it after the parent, so the label declares
the relationship: `Ambulatory health care services` contains detail code `621900`
`Other ambulatory health care services`. A hierarchy pass strips registered
residual markers and, when the remainder is a name the *other side* actually
uses, records a parent/child **relation** instead of a match — and takes both
rows out of the fuzzy pass, which would otherwise score them as near-identical.

Relations are reported, never silently summed:

```
parent_name  parent_value  n_children  children_sum     diff  child_codes
Real estate      112429.0           1       93508.0  18921.0  ORE
```

That gap of 18,921 is the missing `HS` (Housing, 18,920) — the report points at
its own fix, which you apply with `merge_reference` rather than a name match.

Markers are registered **per source dialect** in [hierarchy.py](hierarchy.py),
because the conventions differ. "All other X" occurs 11 times in the BEA detail
industry list and never in the summary list or a NIPA industry stub, so it is a
reliable detail marker. Bare "Other X" is *not* self-evidently a marker — five
BEA summary industries are really named `Other retail`, `Other real estate`,
`Other transportation equipment`. Two things keep that safe: the pass runs after
exact name matching, and a strip only counts if the remainder matches the
opposite side exactly. Loaders tag their dialect (`nipa`, `bea_io_detail`,
`bea_io_summary`) automatically.

**Fuzzy name matching is opt-in** (`on='fuzzy'`) and gated twice even then: a
0.88 cutoff, plus `token_relation`, which rejects any pair differing by a
substituted content word. "Support activities for mining" vs "Support activities
for **printing**" scores 0.90 on `difflib` and is an industry off by a factor of
20; token-wise it is a substitution, so it is refused. Prefer `overrides` to
enabling fuzzy.

`on` values: `'auto'` (default, all exact passes + hierarchy), `'fuzzy'`,
`'code'`, `'name'`.

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

```
uv run python -m bedrock.analysis.nimble_compare.examples.nipa_taxes_vs_sut_t00otop
```

NIPA table 3.5 against Use SUT detail rows `T00OTOP` and `T00TOP`, 2017 — the
totals-only shape, and the selection needed to split taxes on products from
other taxes on production:

| | NIPA 3.5 | Use SUT | diff |
|---|---|---|---|
| other taxes on production | 608,533 | 608,542 | −9 (−0.0015%) |
| taxes on products | 755,438 | 755,451 | −13 (−0.0017%) |
