# compare_NIPA_to_IOT

Check a candidate dataset against a BEA reference table — cell by cell and in
total — without first building an exact crosswalk.

Built for reconnaissance. Matching cascades from codes to names to fuzzy names
and reports which pass produced each pair, so you can see the weak links instead
of having them buried. When a number needs to be defensible, promote the
comparison to a real crosswalk in `bedrock/utils/taxonomy/`.

## Use

```python
from bedrock.analysis.compare_NIPA_to_IOT import bea_matrix_row, compare, nipa_flat_table

result = compare(
    candidate=nipa_flat_table('T60200D', 2017).leaves(),   # NIPA table 6.2D
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
| `nipa_flat_table('T60200D', 2017)` | a NIPA table out of BEA's `FlatFiles.ZIP` |
| `nipa_sheet(path, sheet, year)` | the same table out of a `SectionNall_xls.xlsx` workbook |
| `fba_series(source, year, ...)` | anything already generated as a FlowByActivity |
| `table_series(path, value=..., name=...)` | an arbitrary csv/xlsx |
| `frame_series(df, value=..., name=...)` | an in-memory frame |

## Two ways in to a NIPA table

`nipa_flat_table` is the one to reach for. It reads
`bedrock/extract/input_data/BEA_NIPA/FlatFiles.ZIP` — the same archive
[`BEA_NIPA.py`](../../extract/bea/BEA_NIPA.py) parses — so a comparison and the
FBA it is checking cannot end up on different BEA vintages. Pass `path=` to read
a copy from elsewhere.

Values, codes, line order and hierarchy are identical between the two loaders;
both worked examples produce the same figures either way. Two things do differ:

- **labels.** `SeriesLabel` is the series' own name, not its stub in this table.
  Table 3.5's "Federal" line is `Taxes on production and imports` in the archive,
  and 6.2D's "General government" is `Compensation of general government
  employees`. Codes are the same, so a code-matched or `overrides`-driven
  comparison is unaffected — a name-matched one pairs differently.
- **footnotes.** The archive has none. It ships `nipadata{A,Q,M}.txt`,
  `SeriesRegister.txt` and `TablesRegister.txt`, and the footnote block is in no
  register, so `annotated()` comes back empty. Read the workbook with
  `nipa_sheet` when you need the prose (see [Footnotes](#footnotes) below).

Hierarchy survives the switch because the flat files state it outright:
`SeriesCodeParents` gives each series its parents, narrowed to the ones that are
also lines of this table, which is the same `level` the workbooks encode as
leading spaces.

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
aggregate and says nothing about its parts. 

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

## Which framework? BEA publishes two

Both the Supply-Use (SUT) and Make-Use (MUT) frameworks publish a "detail Use
table", so *"compare it to the detail Use table"* is ambiguous — and picking the
wrong one is quiet, not loud.

Three axes decide whether two BEA tables are comparable — **framework**,
**valuation** and **redefinition** — so the matrix names state them and
`describe_matrices()` lists the lot.

| matrix | framework | valuation | redefinition | alias |
|---|---|---|---|---|
| `Use_SUT_detail` *(default)* | Supply-Use | basic | — | — |
| `Supply_SUT_detail` | Supply-Use | basic → purchaser cols | — | `Supply_detail` |
| `Use_MUT_detail` | Make-Use | producer | after | `Use_detail` |
| `Make_MUT_detail` | Make-Use | producer | after | `Make_detail` |
| `Import_MUT_detail` | Make-Use | producer | after | `Import_detail` |
| `Use_MUT_detail_before_redef` | Make-Use | producer | before | — |
| `Make_MUT_detail_before_redef` | Make-Use | producer | before | — |
| `Import_MUT_detail_before_redef` | Make-Use | producer | before | — |

What differs, all checkable in the 2017 data:

- **Framework.** `V00100`, `V00300` and `T005` are rows of both Use tables with
  different values — compensation by 3 million, gross operating surplus by 16.
  Nothing errors; you silently get the other number. The tax rows don't
  correspond at all: SUT splits `T00OTOP` (608,542), `T00TOP` (755,451) and
  `T00SUB` (59,876) where MUT carries one net `V00200` (1,304,104). `T018`,
  `VABAS`, `VAPRO` are SUT-only; `T006`, `T008` MUT-only; `F05000` (imports) is a
  MUT-only *column*.
- **Valuation.** SUT is basic value, MUT producer, and the gap is exactly the
  product taxes: `T018` 33,772,568 + `T00TOP` − `T00SUB` = 34,468,143 against MUT
  `T008` 34,468,137.
- **Redefinition** moves money *between cells while preserving every total*, so a
  totals check cannot tell you that you picked the wrong one. Of the 161,604
  intermediate cells, 5,740 differ, 553,635 million moves gross and the largest
  single cell shifts 42,893 — for a net of −7. This is the axis to get right
  before any cell-by-cell comparison.

### Purchaser value

There is **no purchaser-value Use table in bedrock**. BEA publishes
`IOUse_Before_Redefinitions_PUR_2017_Detail.xlsx`, but it is in neither
`USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING` nor the extract bucket. Asking
for `Use_MUT_detail_PUR` says exactly that and what to do about it, rather than
reporting an unknown name — wiring it up later is the file plus one `MatrixSpec`.

Meanwhile the purchaser-value figures that *are* available are the Supply table's
bridge columns: `T013` total supply at basic 36,398,867, plus `MDTY`/`TOP`/`SUB`,
giving `T016` 37,094,434 at purchaser value. For a derived PUR conversion see
`bedrock/transform/iot/derive_PRO_to_PUR_ratio.py`.

All three ride on every reference series as `.framework` / `.valuation` /
`.redefinition`, print in the report header, and asking the wrong table for a row
tells you where it actually lives:

```
>>> bea_matrix_row('V00200')
KeyError: 'V00200' is not a row of Use_SUT_detail [Supply-Use framework, basic
value]. It is a row of Use_MUT_detail (Make-Use framework, producer value, after
redefinition); ... -- whose rows do not correspond one-for-one with this table.

>>> where_is('V00100')          # a row of three tables, meaning three things
{'Use_SUT_detail':              'Supply-Use framework, basic value',
 'Use_MUT_detail':              'Make-Use framework, producer value, after redefinition',
 'Use_MUT_detail_before_redef': 'Make-Use framework, producer value, before redefinition'}
```

`where_is` is the only thing that catches the `V00100` case, since that one never
raises — call it for any code you have not used before.

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
| `subtree(label, include_parent=, leaves_only=)` | everything nested under a row, following the table's own hierarchy |

Forcing this shape into cells is worse than useless, and it is one label away
from looking like a result. BEA's detail industry list has a row genuinely named
`Customs duties` (`4200ID`), zero in `T00OTOP`. The workbook calls NIPA's line
`Customs duties` too, so a name match pairs 38,513 million of federal customs
receipts with that empty industry row; the flat file calls the same line `Customs
and other import duties (G1151)` and so happens to miss. A `matched cells: 0`
line is the honest output — not something to trust to a naming accident.

## Gotchas

**Hierarchy.** NIPA tables interleave subtotals with leaves. Summing one as
published double counts, so call `.leaves()` — it uses the table's own hierarchy
(`SeriesCodeParents` from the flat files, indentation from a workbook) and
reports how many rows it dropped.

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
uv run python -m bedrock.analysis.compare_NIPA_to_IOT.examples.nipa_compensation_vs_sut_v00100
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
uv run python -m bedrock.analysis.compare_NIPA_to_IOT.examples.nipa_taxes_vs_sut_t00otop
```

NIPA table 3.5 against Use SUT detail rows `T00OTOP` and `T00TOP`, 2017 — the
totals-only shape, and the selection needed to split taxes on products from
other taxes on production:

| | NIPA 3.5 | BEA | diff |
|---|---|---|---|
| other taxes on production | 608,533 | Use `T00OTOP` 608,542 | −9 (−0.0015%) |
| taxes on products | 755,438 | Use `T00TOP` 755,451 | −13 (−0.0017%) |
| ⤷ customs duties | 38,513 | Supply `MDTY` 38,507 | +6 |
| ⤷ other product taxes | 716,925 | Supply `TOP` 716,926 | −1 |

The last two are a real two-cell comparison rather than a total: Use SUT carries
product taxes as one row, but the Supply table splits the same money into import
duties and everything else, and NIPA reports customs duties on its own line.

## Footnotes

NIPA documents residual lines in footnotes, because the label cannot carry it.
For example, Table 3.5's federal "Other" excise taxes is 9,338 million of nothing in
particular until footnote 1 says it is *"largely taxes on telephone services,
tires, coal, nuclear fuel, trucks, indoor tanning services"*. That sentence is
the only description of the line's content, and so the only basis for ever
mapping it onto commodities.

`nipa_sheet` parses the footnote block and records which notes each row cites
(`\7,8\` → both). The marker never reaches `name`, so it cannot disturb matching.
**This is the one thing `nipa_flat_table` cannot give you** — BEA's flat files
publish no note block, so on a series loaded from them these three all come back
empty. It is the reason to keep a workbook around even once everything else
reads from the archive.

```python
sheet.footnotes                          # {'1': 'Consists largely of taxes on ...'}
sheet.notes_for('B2006C')                # the texts that row cites
sheet.annotated(composition_only=True)   # rows beside their notes
```

`composition_only` keeps notes saying what a line *contains* and drops those
saying only when it changed ("Prior to 1988, included in line 43"). In table 3.5
every composition note lands on a line named "Other" or "n.i.e" — the same
pattern the hierarchy pass exploits, one level down — covering 31,268 million,
2.3% of the table.
