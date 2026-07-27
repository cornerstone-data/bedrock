"""Loaders that turn a data source into a :class:`~.series.LabeledSeries`.

Two families:

*reference* -- BEA tables bedrock already knows how to fetch
  :func:`bea_matrix_row`, :func:`bea_matrix_column`, :func:`bea_summary_sut_row`

*candidate* -- whatever you are checking against them
  :func:`nipa_sheet` (a NIPA "SectionNall_xls.xlsx" sheet), :func:`fba_series`
  (anything already generated as a FlowByActivity), :func:`table_series`
  (an arbitrary csv/xlsx you hand over), :func:`frame_series` (an in-memory frame)

All of them accept ``label`` and ``unit`` so reports name the sides usefully.
BEA publishes every table touched here in millions of dollars, so the default
unit is ``'Million USD'`` throughout and no rescaling happens implicitly --
:meth:`LabeledSeries.scale` is there when a candidate arrives in other units.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Literal, cast

import pandas as pd

from bedrock.analysis.compare_NIPA_to_IOT.series import LabeledSeries

MILLION_USD = 'Million USD'

# ---------------------------------------------------------------- BEA reference


def _bea_detail_use_sut() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa

    return _load_2017_detail_supply_use_usa('Use_SUT_detail')


def _bea_detail_supply() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa

    return _load_2017_detail_supply_use_usa('Supply_detail')


def _bea_detail_use_redef() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import _load_2017_detail_make_use_usa

    return _load_2017_detail_make_use_usa('Use_detail')


def _bea_detail_make_redef() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import _load_2017_detail_make_use_usa

    return _load_2017_detail_make_use_usa('Make_detail')


"""BEA publishes 2017 detail in two frameworks, and they are not interchangeable.

The Supply-Use (SUT) framework and the Make-Use (MUT) framework both have a
"detail Use table", so "compare it to the detail Use table" is ambiguous, and
picking the wrong one is quiet rather than loud:

* ``V00100``, ``V00300`` and ``T005`` exist in **both** Use tables with different
  values (SUT is basic value, MUT producer value; compensation differs by 3
  million, gross operating surplus by 16).  Nothing errors -- you just get the
  other framework's number.
* The tax rows do not correspond at all.  SUT splits them into ``T00OTOP``
  (608,542), ``T00TOP`` (755,451) and ``T00SUB`` (59,876); MUT carries the single
  net ``V00200`` (1,304,104).  Comparing a gross NIPA total to ``V00200`` is
  wrong by the subsidies.
* ``T018``/``VABAS``/``VAPRO`` are SUT-only, ``T006``/``T008`` MUT-only, and
  ``F05000`` (imports) is a MUT-only column.

So the matrix names here say which framework they belong to, the framework is
recorded on every series, and asking for a row that lives in the other framework
says so instead of reporting a bare KeyError.
"""

#: user-facing name -> (bedrock's loader key, framework tag)
BEA_MATRICES: dict[str, tuple[str, str]] = {
    'Use_SUT_detail': ('Use_SUT_detail', 'SUT'),
    'Supply_SUT_detail': ('Supply_detail', 'SUT'),
    'Use_MUT_detail': ('Use_detail', 'MUT'),
    'Make_MUT_detail': ('Make_detail', 'MUT'),
}

#: bedrock's own, framework-silent names, still accepted at call sites
MATRIX_ALIASES = {
    'Supply_detail': 'Supply_SUT_detail',
    'Use_detail': 'Use_MUT_detail',
    'Make_detail': 'Make_MUT_detail',
}

FRAMEWORK_NAMES = {
    'SUT': 'Supply-Use framework',
    'MUT': 'Make-Use framework (after redefinition)',
}

BeaMatrix = Literal[
    'Use_SUT_detail',
    'Supply_SUT_detail',
    'Use_MUT_detail',
    'Make_MUT_detail',
    # framework-silent aliases
    'Supply_detail',
    'Use_detail',
    'Make_detail',
]

_BEA_MATRIX_LOADERS: dict[str, Callable[[], pd.DataFrame]] = {
    'Use_SUT_detail': _bea_detail_use_sut,
    'Supply_detail': _bea_detail_supply,
    'Use_detail': _bea_detail_use_redef,
    'Make_detail': _bea_detail_make_redef,
}


def resolve_matrix(matrix: str) -> tuple[str, str, str]:
    """Canonical name, loader key and framework tag for a matrix name."""
    canonical = MATRIX_ALIASES.get(matrix, matrix)
    if canonical not in BEA_MATRICES:
        raise KeyError(
            f'{matrix!r} is not a known BEA matrix; '
            f'choose from {sorted(BEA_MATRICES)} '
            f'(aliases: {sorted(MATRIX_ALIASES)})'
        )
    loader_key, framework = BEA_MATRICES[canonical]
    return canonical, loader_key, framework


def _load_matrix(matrix: str) -> tuple[pd.DataFrame, str, str]:
    canonical, loader_key, framework = resolve_matrix(matrix)
    return _BEA_MATRIX_LOADERS[loader_key](), canonical, framework


def where_is(code: str, axis: Literal['row', 'column'] = 'row') -> dict[str, str]:
    """Which matrices contain ``code``, mapped to their framework.

    The direct answer to "is this the SUT table's row or the MUT table's?", and
    worth calling whenever a comparison is being set up against a code you have
    not used before -- ``V00100`` is in both and means something slightly
    different in each.
    """
    found = {}
    for canonical, (loader_key, framework) in BEA_MATRICES.items():
        df = _BEA_MATRIX_LOADERS[loader_key]()
        labels = df.index if axis == 'row' else df.columns
        if code in labels:
            found[canonical] = framework
    return found


def _missing_label_error(
    code: str, matrix: str, framework: str, axis: Literal['row', 'column']
) -> KeyError:
    """A KeyError that names the framework the code actually lives in."""
    elsewhere = {k: v for k, v in where_is(code, axis).items() if k != matrix}
    hint = (
        f' It is a {axis} of {", ".join(f"{k} [{v}]" for k, v in elsewhere.items())}'
        f' -- a different framework, whose {axis}s do not correspond one-for-one.'
        if elsewhere
        else ''
    )
    return KeyError(
        f'{code!r} is not a {axis} of {matrix} [{FRAMEWORK_NAMES[framework]}].{hint}'
    )


def _industry_names() -> dict[str, str]:
    from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_DESC

    return dict(USA_2017_INDUSTRY_DESC)


def _commodity_names() -> dict[str, str]:
    from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_DESC

    return dict(USA_2017_COMMODITY_DESC)


def summary_industry_names() -> dict[str, str]:
    """BEA 2017 *summary* industry code -> description."""
    from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
        USA_2017_SUMMARY_INDUSTRY_DESC,
    )

    return dict(USA_2017_SUMMARY_INDUSTRY_DESC)


def detail_industry_to_summary() -> dict[str, list[str]]:
    """BEA 2017 detail industry code -> summary code(s); feed to ``rollup``."""
    from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
        load_bea_v2017_industry_to_bea_v2017_summary,
    )

    return cast('dict[str, list[str]]', load_bea_v2017_industry_to_bea_v2017_summary())


def bea_matrix_row(
    row_code: str,
    matrix: BeaMatrix = 'Use_SUT_detail',
    *,
    across: Literal['industry', 'commodity'] = 'industry',
    label: str | None = None,
) -> LabeledSeries:
    """One row of a 2017 BEA detail matrix, as a series across its columns.

    The value-added rows are the reason this exists: ``bea_matrix_row('V00100')``
    is compensation of employees by detail industry out of the Use SUT table.
    Columns are restricted to the canonical code list for ``across``, which
    drops the totals and final-demand columns that would otherwise double count.

    ``matrix`` defaults to the **SUT** framework's Use table.  Pass
    ``'Use_MUT_detail'`` for the Make-Use one; see the note above on why the two
    are not interchangeable.
    """
    df, canonical, framework = _load_matrix(matrix)
    if row_code not in df.index:
        raise _missing_label_error(row_code, canonical, framework, 'row')
    names = _industry_names() if across == 'industry' else _commodity_names()
    codes = [c for c in df.columns if c in names]
    row = df.loc[row_code].reindex(codes)
    frame = pd.DataFrame(
        {'code': codes, 'name': [names[c] for c in codes], 'value': row.to_numpy()}
    )
    return LabeledSeries(
        frame,
        label or f'{canonical}:{row_code}',
        MILLION_USD,
        {
            'source': canonical,
            'framework': framework,
            'row': row_code,
            'across': across,
            'dialect': 'bea_io_detail',
        },
    )


def bea_matrix_column(
    column_code: str,
    matrix: BeaMatrix = 'Use_SUT_detail',
    *,
    across: Literal['industry', 'commodity'] = 'commodity',
    label: str | None = None,
) -> LabeledSeries:
    """One column of a 2017 BEA detail matrix, as a series down its rows.

    ``matrix`` defaults to the **SUT** framework's Use table; note ``F05000``
    (imports) is a column of the Make-Use table only.
    """
    df, canonical, framework = _load_matrix(matrix)
    if column_code not in df.columns:
        raise _missing_label_error(column_code, canonical, framework, 'column')
    names = _industry_names() if across == 'industry' else _commodity_names()
    codes = [c for c in df.index if c in names]
    col = df[column_code].reindex(codes)
    frame = pd.DataFrame(
        {'code': codes, 'name': [names[c] for c in codes], 'value': col.to_numpy()}
    )
    return LabeledSeries(
        frame,
        label or f'{canonical}:{column_code}',
        MILLION_USD,
        {
            'source': canonical,
            'framework': framework,
            'column': column_code,
            'across': across,
            'dialect': 'bea_io_detail',
        },
    )


def bea_summary_sut_row(
    row_code: str,
    year: int = 2017,
    *,
    label: str | None = None,
) -> LabeledSeries:
    """One row of the BEA Use SUT *summary* table, across summary industries.

    Useful when the candidate is itself near-summary granularity and you would
    rather not roll the detail table up.
    """
    from bedrock.extract.iot.io_2017 import _load_usa_summary_sut

    df = _load_usa_summary_sut('Use_SUT_summary', year)  # type: ignore[arg-type]
    if row_code not in df.index:
        raise KeyError(f'{row_code!r} is not a row of Use_SUT_summary {year}')
    names = summary_industry_names()
    codes = [c for c in df.columns if c in names]
    row = df.loc[row_code].reindex(codes)
    frame = pd.DataFrame(
        {'code': codes, 'name': [names[c] for c in codes], 'value': row.to_numpy()}
    )
    return LabeledSeries(
        frame,
        label or f'Use_SUT_summary:{row_code}@{year}',
        MILLION_USD,
        {
            'source': 'Use_SUT_summary',
            'row': row_code,
            'year': year,
            'dialect': 'bea_io_summary',
        },
    )


# ------------------------------------------------------------------- candidates

_INDENT = re.compile(r'^(\s*)')
_FOOTNOTE_DEF = re.compile(r'^(\d+)\.\s+(.*)$')
_FOOTNOTE_REF = re.compile(r'\\([\d,\s]+)\\')


def _parse_footnotes(column: list[object]) -> dict[str, str]:
    """Pull ``N. text`` definitions out of the block below a NIPA table."""
    notes: dict[str, str] = {}
    for cell in column:
        if not isinstance(cell, str):
            continue
        match = _FOOTNOTE_DEF.match(cell.strip())
        if match:
            notes[match.group(1)] = match.group(2).strip()
    return notes


def _footnote_refs(label: str) -> str:
    """The footnote numbers a label cites, e.g. ``Other\\7,8\\`` -> ``'7;8'``."""
    refs: list[str] = []
    for group in _FOOTNOTE_REF.findall(label):
        refs.extend(n.strip() for n in group.split(',') if n.strip())
    return ';'.join(refs)


def nipa_sheet(
    path: str,
    sheet: str,
    year: int,
    *,
    label: str | None = None,
    unit: str = MILLION_USD,
) -> LabeledSeries:
    """A single year column out of a NIPA ``SectionNall_xls.xlsx`` sheet.

    These sheets are laid out ``Line | label | series code | <year> ...`` under
    a handful of title rows, with hierarchy encoded as leading spaces in the
    label.  That indentation is preserved as ``level`` so
    :meth:`LabeledSeries.leaves` can drop the subtotal rows -- which you almost
    always want, since summing a NIPA sheet as published double counts.

    The retained ``code`` is BEA's NIPA series code (``N4013C``); it will not
    match IO codes, so these comparisons align on ``name``.
    """
    raw = pd.read_excel(path, sheet_name=sheet, header=None)

    header_idx = None
    for i in range(min(len(raw), 30)):
        if str(raw.iat[i, 0]).strip() == 'Line':
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f'no "Line" header row found in {sheet} of {path}')

    header = raw.iloc[header_idx]
    year_cols = {}
    for j, val in enumerate(header):
        try:
            year_cols[int(float(val))] = j
        except (TypeError, ValueError):
            continue
    if year not in year_cols:
        raise KeyError(f'{sheet} has no {year} column; available: {sorted(year_cols)}')

    body = raw.iloc[header_idx + 1 :]
    labels = [s if isinstance(s, str) else '' for s in body.iloc[:, 1]]
    # Every data row is numbered under "Line". Requiring that rules out the
    # legend and footnote block below the table, whose prose can otherwise land
    # in the label column and read as an industry.
    line_nos = pd.to_numeric(body.iloc[:, 0], errors='coerce').to_numpy()
    keep = [
        i for i, name in enumerate(labels) if name.strip() and not pd.isna(line_nos[i])
    ]
    # two spaces per NIPA indent step
    levels = [len(_INDENT.match(labels[i]).group(1)) // 2 for i in keep]  # type: ignore[union-attr]

    # BEA indents the stub head of these sheets as if it were a detail line, so
    # the grand total (line 1, e.g. "Compensation of employees") looks deeper
    # than the "Domestic industries" line it actually contains.  Left alone it
    # survives `leaves()` and doubles the candidate total.  Reseat it above
    # everything so it is recognized as the subtotal it is.
    if len(levels) > 1 and levels[0] > levels[1]:
        levels[0] = levels[1] - 1

    frame = pd.DataFrame(
        {
            'code': body.iloc[keep, 2].to_numpy(),
            'name': [labels[i] for i in keep],
            'value': body.iloc[keep, year_cols[year]].to_numpy(),
            'level': levels,
            # captured from the raw label, since the marker is stripped out of
            # `name` before it reaches anything that compares names
            'footnote_refs': [_footnote_refs(labels[i]) for i in keep],
        }
    )

    return LabeledSeries(
        frame,
        label or f'{sheet}@{year}',
        unit,
        {
            'source': path,
            'sheet': sheet,
            'year': year,
            'dialect': 'nipa',
            'footnotes': _parse_footnotes(raw.iloc[:, 0].tolist()),
        },
    )


def fba_series(
    source: str,
    year: int,
    *,
    code: str | None = None,
    name: str | None = 'ActivityProducedBy',
    value: str = 'FlowAmount',
    query: str | None = None,
    label: str | None = None,
    unit: str = MILLION_USD,
    **fba_kwargs: Any,
) -> LabeledSeries:
    """A series pulled from a generated FlowByActivity.

    ``query`` is applied first (a pandas query string, e.g.
    ``"Table == 'T60200D' and Line > 3"``), then the remaining rows are summed
    per code/name pair.  Note that :func:`bedrock.extract.bea.BEA_NIPA` FBAs
    store values in dollars, not millions -- pass ``unit='USD'`` and
    ``.scale(1e-6, 'Million USD')`` when comparing against an IO table.
    """
    from bedrock.extract.flowbyactivity import getFlowByActivity

    df = getFlowByActivity(source, year, **fba_kwargs)
    if query:
        df = df.query(query)
    return frame_series(
        df,
        code=code,
        name=name,
        value=value,
        label=label or f'{source}@{year}',
        unit=unit,
    )


def table_series(
    path: str,
    *,
    value: str,
    code: str | None = None,
    name: str | None = None,
    sheet: str | int | None = None,
    query: str | None = None,
    label: str | None = None,
    unit: str = MILLION_USD,
    **read_kwargs: Any,
) -> LabeledSeries:
    """A series read straight out of a csv/xlsx file you hand over."""
    if str(path).lower().endswith(('.xlsx', '.xls', '.xlsm')):
        df = pd.read_excel(path, sheet_name=sheet or 0, **read_kwargs)
    else:
        df = pd.read_csv(path, **read_kwargs)
    if query:
        df = df.query(query)
    return frame_series(
        df, code=code, name=name, value=value, label=label or str(path), unit=unit
    )


def frame_series(
    df: pd.DataFrame,
    *,
    value: str,
    code: str | None = None,
    name: str | None = None,
    label: str = 'frame',
    unit: str = MILLION_USD,
) -> LabeledSeries:
    """Wrap an in-memory frame, summing duplicate code/name pairs."""
    if code is None and name is None:
        raise ValueError('give at least one of code=, name=')
    keys = [c for c in (code, name) if c is not None]
    grouped = df.groupby(keys, as_index=False, dropna=False)[value].sum()
    frame = pd.DataFrame({'value': grouped[value]})
    frame['code'] = grouped[code] if code else ''
    frame['name'] = grouped[name] if name else ''
    return LabeledSeries(frame, label, unit)
