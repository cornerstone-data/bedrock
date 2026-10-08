# compare_NIPA_to_IOT

Check a candidate dataset (currently from BEA NIPA) against a BEA reference table — cell by cell and in
total — without first building an exact crosswalk.  Comparison to 2017 data is the default.

Matching cascades from codes to names to fuzzy names. Every pair records which
pass produced it. When a number needs to be defensible, promote the comparison to
a crosswalk in `bedrock/utils/taxonomy/`.

## Example Use

```python
from bedrock.analysis.nowcasting.compare_NIPA_to_IOT import bea_matrix_row, compare, nipa_flat_table

result = compare(
    candidate=nipa_flat_table('T60200D', 2017).leaves(),   # NIPA table 6.2D
    reference=bea_matrix_row('V00100'),          # Use SUT detail, compensation row
    rollup='industry_to_summary',                # 402 detail -> 71 summary industries
)
print(result.report())
result.to_csv('out/compensation.csv')
```
Note that either an IOT row or column can be passed from any BEA IOT that is in bedrock.

Intermediate data are a `LabeledSeries`: a tidy `code / name / value / level` frame. The
loaders build them, `compare` consumes two of them.

| Reference (BEA) | |
|---|---|
| `bea_matrix_row('V00100')` | a row of a 2017 detail matrix, across industries |
| `bea_matrix_column('230301')` | a column, down commodities |

| Candidate (anything) | |
|---|---|
| `nipa_flat_table('T60200D', 2017)` | a NIPA table out of BEA's `FlatFiles.ZIP` |
| `nipa_sheet(path, sheet, year)` | the same table out of a `SectionNall_xls.xlsx` workbook |
| `fba_series(source, year, ...)` | anything already generated as a FlowByActivity |
| `table_series(path, value=..., name=...)` | an arbitrary csv/xlsx |
| `frame_series(df, value=..., name=...)` | an in-memory frame |

## Reading a NIPA table

`nipa_flat_table` reads
`bedrock/extract/input_data/BEA_NIPA/FlatFiles.ZIP` — the same archive
[`BEA_NIPA.py`](../../../extract/bea/BEA_NIPA.py) parses — so a comparison and the
FBA it is checking cannot end up on different BEA vintages. Pass `path=` to read
a copy from elsewhere.

`nipa_sheet` reads one of the bundled excel files of the NIPA data which should be stored 
locally in 1 of 2 zips, `bedrock/extract/input_data/BEA_NIPA/NIPA Survey ALL.ZIP` or `bedrock/extract/input_data/BEA_NIPA/NIPA All Underlying.ZIP`. 

Values, codes, line order and hierarchy are the same from either loader. Two
things differ:

- **Labels.** The flat files give `SeriesLabel`, the series' own name, rather
  than its stub in this table. Table 3.5's "Federal" line is `Taxes on
  production and imports`, and 6.2D's "General government" is `Compensation of
  general government employees`. Codes are the same either way, so code-matched
  and `overrides`-driven comparisons are unaffected; name-matched ones pair
  differently.
- **Footnotes.** The flat files have none. They ship `nipadata{A,Q,M}.txt`,
  `SeriesRegister.txt` and `TablesRegister.txt`, none of which carry the footnote
  block, so `annotated()` returns empty. Use `nipa_sheet` for footnote text.

`level` comes from `SeriesCodeParents`, narrowed to the parents that are also
lines of the requested table. It matches the indentation the workbooks use.

## Granularity: the reference stays the detail table

`rollup` aggregates the reference *before matching*; it does not switch to BEA's
published summary table. The detail table remains the source, and the comparison
happens at the granularity the candidate can actually address — which for a NIPA
industry sheet is roughly summary, since those sheets have no "Oilseed farming"
row to compare against.

Rolling the detail table up reproduces the published summary table to BEA's
rounding, and keeps each cell's composition:

| column | |
|---|---|
| `n_detail` | how many reference codes the cell sums (`1` = not aggregated) |
| `detail_members` | those codes and values, `211000=29293;...` |

```python
result.one_to_one()      # cells that are a single detail code -> true detail comparison
result.aggregated()      # cells that sum two or more
result.detail('111CA')   # the codes behind one cell
```

A cell with `n_detail == 1` is evidence about one detail industry. A cell with
`n_detail == 29` is evidence about an aggregate and says nothing about its parts.

## Reading the report

Totals are split three ways — matched candidate, matched reference, and the
unmatched remainder on each side — because the two failure modes need different
responses:

- **matched cells disagree** → a data difference, look at `worst()`
- **rows go unmatched** → the two tables partition the economy differently

The second is common and is not a bug. NIPA splits wholesale trade into
durable/nondurable where BEA summary does not; BEA splits real estate into
housing/other where NIPA does not; the two carve federal government along
different seams (civilian/military vs defense/nondefense).

Three knobs close those gaps, all by code or name:

```python
candidate.leaves(keep=['N4037C'], drop=['N4038C', 'N4039C'])   # use NIPA's parent
compare(..., merge_reference={'RE': ['HS', 'ORE']})            # sum BEA's parts
compare(..., overrides={'N4055C': '511'})                      # just say it
```

## Which framework? BEA publishes two

Both the Supply-Use (SUT) and Make-Use (MUT) frameworks publish a "detail Use
table", so "compare it to the detail Use table" is ambiguous, and picking the
wrong one raises no error.

Three axes decide whether two BEA tables are comparable — **framework**,
**valuation** and **redefinition**. The matrix names state them, and
`describe_matrices()` lists all eight with their notes.

| matrix | framework | valuation | redefinition |
|---|---|---|---|
| `Use_SUT_detail` *(default)* | Supply-Use | purchaser cells, basic + producer totals | — |
| `Supply_SUT_detail` | Supply-Use | basic cells, basic + purchaser totals | — |
| `Use_MUT_detail_after_redef` | Make-Use | producer | after |
| `Make_MUT_detail_after_redef` | Make-Use | producer | after |
| `Import_MUT_detail_after_redef` | Make-Use | producer | after |
| `Use_MUT_detail_before_redef` | Make-Use | producer | before |
| `Make_MUT_detail_before_redef` | Make-Use | producer | before |
| `Import_MUT_detail_before_redef` | Make-Use | producer | before |

Every name states all three axes, and there are no framework-silent aliases. A
bare `Use_detail` does not say which of the two detail Use tables it means, and a
bare `Use_MUT_detail` does not say which redefinition it carries, so neither
resolves; both raise a `KeyError` listing the eight names. (`bedrock` uses the
short names elsewhere, in `matrix_mappings.py`. That is a separate namespace and
is unaffected.)

For what differs between the tables, see
[notes on MUT and SUT differences](About_BEA_IOT_table_valuation_differences.md).

`.framework`, `.valuation` and `.redefinition` ride on every reference series and
print in the report header. `valuation` is the basis of the table's *cells*; the
matrix `note` covers totals published on another basis. Asking a table for a row
it does not have reports where that row does live:

```
>>> bea_matrix_row('V00200')
KeyError: 'V00200' is not a row of Use_SUT_detail [Supply-Use framework,
purchaser value]. It is a row of Use_MUT_detail_after_redef (Make-Use framework,
producer value, after redefinition); ... -- whose rows do not correspond
one-for-one with this table.

>>> where_is('V00100')          # a row of three tables, meaning three things
{'Use_SUT_detail':              'Supply-Use framework, purchaser value',
 'Use_MUT_detail_after_redef':  'Make-Use framework, producer value, after redefinition',
 'Use_MUT_detail_before_redef': 'Make-Use framework, producer value, before redefinition'}
```

`where_is` covers the case that never raises: a code present in several tables
with a different meaning in each. Call it for any code not used before.

### Purchaser value

bedrock has no purchaser-value Use table. BEA publishes
`IOUse_Before_Redefinitions_PUR_2017_Detail.xlsx`, but it is in neither
`USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING` nor the extract bucket. Asking
for `Use_MUT_detail_before_redef_PUR` reports that rather than an unknown name.
Adding it takes the file plus one `MatrixSpec`.

The purchaser-value figures that are available are the Supply table's bridge
columns: `T013` total supply at basic, plus `MDTY`/`TOP`/`SUB` and the margin
columns, giving `T016` at purchaser value. For a derived PRO conversion see
`bedrock/transform/iot/derive_PRO_to_PUR_ratio.py`.

## When there are no cells to compare

Some pairs share no correspondence at all — NIPA 3.5 is organized by level of
government and kind of tax, the Use table's `T00OTOP` by industry. There is
nothing to match, and the totals *are* the answer. Select the part of the
candidate that corresponds and read the totals block:

```python
table = nipa_flat_table('T30500', 2017)
compare(table.select(['LA000237', 'LA000365']), bea_matrix_row('T00OTOP')).totals
```

| | |
|---|---|
| `select(labels)` | just these rows, by code or name |
| `subtree(label, include_parent=, leaves_only=)` | everything nested under a row |

Forcing this shape into cells produces matches that mean nothing: BEA's detail
industry list has a row named `Customs duties` (`4200ID`), so a name match pairs
NIPA's federal customs receipts with the other-taxes row of the customs-duties
*industry*, which is zero. `matched cells: 0` is the correct output.

## Gotchas

**Hierarchy.** NIPA tables interleave subtotals with leaves. Summing one as
published double counts, so call `.leaves()` — it uses the table's own hierarchy
(`SeriesCodeParents` from the flat files, indentation from a workbook) and
reports how many rows it dropped.

**Hierarchy is read from labels, before any similarity is computed.** BEA carves
a residual out of a parent and names it after the parent, so the label declares
the relationship: `Ambulatory health care services` contains detail code `621900`
`Other ambulatory health care services`. A hierarchy pass strips registered
residual markers and, when the remainder is a name the other side uses, records a
parent/child **relation** instead of a match. Both rows are then held out of the
fuzzy pass, which would otherwise score them as near-identical.

Relations are reported, never silently summed:

```
parent_name  parent_value  n_children  children_sum     diff  child_codes
Real estate      112429.0           1       93508.0  18921.0  ORE
```

The gap of 18,921 is the missing `HS` (Housing, 18,920). Close it with
`merge_reference`, not a name match.

Markers are registered **per source dialect** in [hierarchy.py](hierarchy.py),
because the conventions differ. "All other X" occurs 11 times in the BEA detail
industry list and never in the summary list or a NIPA industry stub, so it is a
reliable detail marker. Bare "Other X" is not: five BEA summary industries are
named `Other retail`, `Other real estate`, `Other transportation equipment`. Two
rules keep that safe — the pass runs after exact name matching, and a strip
counts only if the remainder matches the opposite side exactly. Loaders tag their
dialect (`nipa`, `bea_io_detail`, `bea_io_summary`) automatically.

**Fuzzy name matching runs by default, as the last pass.** It only sees rows the
exact passes and the hierarchy pass left over, and it is gated twice: a 0.88
`difflib` cutoff, plus `token_relation`, which rejects any pair differing by a
substituted content word. "Support activities for mining" and "Support activities
for printing" score 0.90 and are industries a factor of 20 apart; token-wise that
is a substitution, so the pair is refused.

Every fuzzy pair is labelled `fuzzy` in the `method` column, carries its `score`,
and is counted separately in the report's `MATCHED BY` block. Use `overrides` for
pairs you want stated rather than inferred, and `on='code'` or `on='name'` to
exclude the pass entirely.

`on` values: `'auto'` (default, every pass), `'fuzzy'` (synonym for `'auto'`),
`'code'`, `'name'`.

**Units.** BEA publishes every table reachable here in millions of dollars, and
nothing is rescaled implicitly. `BEA_NIPA` FBAs are the exception — they store
dollars, so pass `scale_candidate=1e-6`.

**Signs.** NIPA's "Less:" lines are summed as published, not negated.

## Worked examples

```
uv run python -m bedrock.analysis.nowcasting.compare_NIPA_to_IOT.examples.nipa_compensation_vs_sut_v00100
```

NIPA table 6.2D compensation of employees against Use SUT detail row `V00100`,
2017, in three stages:

- **stage 0** — the 402 detail codes untouched. 17 of 74 NIPA rows match, 385
  detail codes do not, because 6.2D has no rows at detail granularity.
- **stage 1** — rolled up to the 71 summary groups. 61 pair on name alone, all
  within BEA's rounding; 17 are 1:1 with a single detail code.
- **stage 2** — the ten partition mismatches reconciled in about twenty lines,
  giving 69/69 cells and −1 million on a $10.4 trillion total.

```
uv run python -m bedrock.analysis.nowcasting.compare_NIPA_to_IOT.examples.nipa_taxes_vs_sut_t00otop
```

NIPA table 3.5 against Use SUT detail rows `T00OTOP` and `T00TOP`, 2017: the
totals-only shape, and the selection that splits taxes on products from other
taxes on production.

| | NIPA 3.5 | BEA | diff |
|---|---|---|---|
| other taxes on production | 608,533 | Use `T00OTOP` 608,542 | −9 (−0.0015%) |
| taxes on products | 755,438 | Use `T00TOP` 755,451 | −13 (−0.0017%) |
| ⤷ customs duties | 38,513 | Supply `MDTY` 38,507 | +6 |
| ⤷ other product taxes | 716,925 | Supply `TOP` 716,926 | −1 |

The last two rows are a two-cell comparison rather than a total: Use SUT carries
product taxes as one row, the Supply table splits the same money into import
duties and everything else, and NIPA reports customs duties on its own line.

## NIPA Footnotes Can be Extracted using the `nipa_sheets()` method

NIPA documents residual lines in footnotes. Table 3.5's federal "Other" excise
taxes is 9,338 million with no further description in the table; footnote 1 gives
its content as "largely taxes on telephone services, tires, coal, nuclear fuel,
trucks, indoor tanning services". For residual lines this is the only description
available, and so the only basis for mapping them onto commodities.

`nipa_sheet` parses the footnote block and records which notes each row cites
(`\7,8\` → both). The marker is stripped from `name`, so it does not affect
matching. `nipa_flat_table` cannot supply any of this — BEA's flat files publish
no note block, and on a series loaded from them all three calls return empty.

```python
sheet.footnotes                          # {'1': 'Consists largely of taxes on ...'}
sheet.notes_for('B2006C')                # the texts that row cites
sheet.annotated(composition_only=True)   # rows beside their notes
```

`composition_only` keeps notes saying what a line contains and drops those saying
only when it changed ("Prior to 1988, included in line 43"). In table 3.5 every
composition note lands on a line named "Other" or "n.i.e", covering 31,268
million, 2.3% of the table.
