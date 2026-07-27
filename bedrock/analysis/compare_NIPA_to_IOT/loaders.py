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
from dataclasses import dataclass
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


def _bea_detail_import_redef() -> pd.DataFrame:
    from bedrock.extract.iot.io_2017 import _load_2017_detail_make_use_usa

    return _load_2017_detail_make_use_usa('Import_detail')


def _bea_detail_before_redef(key: str) -> Callable[[], pd.DataFrame]:
    """Raw before-redefinition workbook, read the same way as the after ones.

    ``io_2017``'s before-redef loaders return a trimmed commodity x industry
    block; comparisons here need the whole sheet, value-added rows included, so
    the workbook is read directly.
    """

    def load() -> pd.DataFrame:
        from bedrock.extract.iot.io_2017 import (
            GCS_USA_MAKE_USE_DIR,
            LOCAL_USA_MAKE_USE_DIR,
        )
        from bedrock.utils.io.gcp import load_from_gcs
        from bedrock.utils.taxonomy.bea.matrix_mappings import (
            USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING,
        )

        df = (
            load_from_gcs(
                name=USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING[key],
                sub_bucket=GCS_USA_MAKE_USE_DIR,
                local_dir=LOCAL_USA_MAKE_USE_DIR,
                loader=lambda pth: pd.read_excel(
                    pth, sheet_name='2017', skiprows=5, dtype={'Code': str}
                ),
            )
            .set_index('Code')
            .fillna(0)
        )
        df.columns = df.columns.astype(str)
        return df

    return load


@dataclass(frozen=True)
class MatrixSpec:
    """What a reference matrix is, along the axes that make tables incomparable.

    :param loader_key: key into :data:`_BEA_MATRIX_LOADERS`
    :param framework: ``'SUT'`` (Supply-Use) or ``'MUT'`` (Make-Use)
    :param valuation: ``'BAS'`` basic, ``'PRO'`` producer, ``'PUR'`` purchaser
    :param redefinition: ``'after'``, ``'before'``, or ``''`` where it does not apply
    :param note: anything a caller should know before comparing against it
    """

    loader_key: str
    framework: str
    valuation: str
    redefinition: str = ''
    note: str = ''

    @property
    def summary(self) -> str:
        bits = [FRAMEWORK_NAMES[self.framework], VALUATION_NAMES[self.valuation]]
        if self.redefinition:
            bits.append(f'{self.redefinition} redefinition')
        return ', '.join(bits)


FRAMEWORK_NAMES = {'SUT': 'Supply-Use framework', 'MUT': 'Make-Use framework'}

VALUATION_NAMES = {
    'BAS': 'basic value',
    'PRO': 'producer value',
    'PUR': 'purchaser value',
}

#: The 2017 detail reference tables, keyed by names that state their framework
#: and, where it applies, their redefinition treatment.  Three axes make two
#: tables incomparable even when they share row codes, and all three are
#: verifiable in the 2017 data:
#:
#: *framework* -- SUT and MUT both publish a "detail Use table".  ``V00100``,
#:   ``V00300`` and ``T005`` are rows of both with different values; the tax rows
#:   have no counterpart at all (SUT splits ``T00OTOP`` 608,542 / ``T00TOP``
#:   755,451 / ``T00SUB`` 59,876 where MUT carries the net ``V00200`` 1,304,104).
#:
#: *valuation* -- SUT is basic value, MUT producer value, and the difference is
#:   exactly the product taxes: SUT ``T018`` 33,772,568 + ``T00TOP`` - ``T00SUB``
#:   = 34,468,143, against MUT ``T008`` 34,468,137.
#:
#: *redefinition* -- reallocates between cells while preserving totals, so a
#:   totals check cannot tell you that you picked the wrong one.  Across the
#:   161,604 intermediate cells, 5,740 differ, 553,635 million moves gross and
#:   the largest single cell shifts 42,893 -- for a net of -7.
BEA_MATRICES: dict[str, MatrixSpec] = {
    'Use_SUT_detail': MatrixSpec('Use_SUT_detail', 'SUT', 'BAS'),
    'Supply_SUT_detail': MatrixSpec(
        'Supply_detail',
        'SUT',
        'BAS',
        note=(
            'trailing columns bridge to purchaser value: T013 total supply at '
            'basic 36,398,867, + MDTY/TOP/SUB = T016 37,094,434 at purchaser'
        ),
    ),
    'Use_MUT_detail': MatrixSpec('Use_detail', 'MUT', 'PRO', 'after'),
    'Make_MUT_detail': MatrixSpec('Make_detail', 'MUT', 'PRO', 'after'),
    'Import_MUT_detail': MatrixSpec('Import_detail', 'MUT', 'PRO', 'after'),
    'Use_MUT_detail_before_redef': MatrixSpec(
        'Use_detail_before_redef', 'MUT', 'PRO', 'before'
    ),
    'Make_MUT_detail_before_redef': MatrixSpec(
        'Make_detail_before_redef', 'MUT', 'PRO', 'before'
    ),
    'Import_MUT_detail_before_redef': MatrixSpec(
        'Import_detail_before_redef', 'MUT', 'PRO', 'before'
    ),
}

#: Purchaser-value detail Use table.  BEA publishes one
#: (``IOUse_Before_Redefinitions_PUR_2017_Detail.xlsx``) but bedrock does not
#: carry it: it is absent from ``USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING``
#: and from the extract input bucket.  Named here so asking for it explains the
#: gap rather than reporting an unknown matrix, and so wiring it up later is a
#: matter of adding the file plus one :class:`MatrixSpec`.
UNAVAILABLE_MATRICES = {
    'Use_MUT_detail_PUR': (
        'BEA publishes IOUse_Before_Redefinitions_PUR_2017_Detail.xlsx, but it is '
        'not in bedrock: add it to USA_2017_DETAIL_IO_BEFORE_REDEF_MATRIX_MAPPING '
        'and upload it to the USA_AllTables_MakeUse extract bucket. The nearest '
        'available purchaser-value figures are the Supply table bridge columns '
        '(Supply_SUT_detail: MDTY, TOP, SUB, T016).'
    ),
}

#: bedrock's own, framework-silent names, still accepted at call sites
MATRIX_ALIASES = {
    'Supply_detail': 'Supply_SUT_detail',
    'Use_detail': 'Use_MUT_detail',
    'Make_detail': 'Make_MUT_detail',
    'Import_detail': 'Import_MUT_detail',
}

BeaMatrix = Literal[
    'Use_SUT_detail',
    'Supply_SUT_detail',
    'Use_MUT_detail',
    'Make_MUT_detail',
    'Import_MUT_detail',
    'Use_MUT_detail_before_redef',
    'Make_MUT_detail_before_redef',
    'Import_MUT_detail_before_redef',
    # framework-silent aliases
    'Supply_detail',
    'Use_detail',
    'Make_detail',
    'Import_detail',
]

_BEA_MATRIX_LOADERS: dict[str, Callable[[], pd.DataFrame]] = {
    'Use_SUT_detail': _bea_detail_use_sut,
    'Supply_detail': _bea_detail_supply,
    'Use_detail': _bea_detail_use_redef,
    'Make_detail': _bea_detail_make_redef,
    'Import_detail': _bea_detail_import_redef,
    'Use_detail_before_redef': _bea_detail_before_redef('Use_detail_before_redef'),
    'Make_detail_before_redef': _bea_detail_before_redef('Make_detail_before_redef'),
    'Import_detail_before_redef': _bea_detail_before_redef(
        'Import_detail_before_redef'
    ),
}


def resolve_matrix(matrix: str) -> tuple[str, MatrixSpec]:
    """Canonical name and :class:`MatrixSpec` for a matrix name or alias."""
    canonical = MATRIX_ALIASES.get(matrix, matrix)
    if canonical in UNAVAILABLE_MATRICES:
        raise KeyError(
            f'{canonical!r} is not available. {UNAVAILABLE_MATRICES[canonical]}'
        )
    if canonical not in BEA_MATRICES:
        raise KeyError(
            f'{matrix!r} is not a known BEA matrix; '
            f'choose from {sorted(BEA_MATRICES)} '
            f'(aliases: {sorted(MATRIX_ALIASES)})'
        )
    return canonical, BEA_MATRICES[canonical]


def describe_matrices() -> pd.DataFrame:
    """Every reference matrix beside its framework, valuation and redefinition."""
    return pd.DataFrame(
        [
            {
                'matrix': name,
                'framework': spec.framework,
                'valuation': spec.valuation,
                'redefinition': spec.redefinition or '-',
                'note': spec.note,
            }
            for name, spec in BEA_MATRICES.items()
        ]
    )


def _load_matrix(matrix: str) -> tuple[pd.DataFrame, str, MatrixSpec]:
    canonical, spec = resolve_matrix(matrix)
    return _BEA_MATRIX_LOADERS[spec.loader_key](), canonical, spec


def where_is(code: str, axis: Literal['row', 'column'] = 'row') -> dict[str, str]:
    """Which matrices contain ``code``, mapped to a description of each.

    The direct answer to "is this the SUT table's row or the MUT table's?", and
    worth calling for any code not used before -- ``V00100`` is a row of four of
    these tables and means something slightly different in each.
    """
    found = {}
    for canonical, spec in BEA_MATRICES.items():
        df = _BEA_MATRIX_LOADERS[spec.loader_key]()
        labels = df.index if axis == 'row' else df.columns
        if code in labels:
            found[canonical] = spec.summary
    return found


def _missing_label_error(
    code: str, matrix: str, spec: MatrixSpec, axis: Literal['row', 'column']
) -> KeyError:
    """A KeyError that names the table the code actually lives in."""
    elsewhere = {k: v for k, v in where_is(code, axis).items() if k != matrix}
    hint = (
        f' It is a {axis} of {"; ".join(f"{k} ({v})" for k, v in elsewhere.items())}'
        f' -- whose {axis}s do not correspond one-for-one with this table.'
        if elsewhere
        else ''
    )
    return KeyError(f'{code!r} is not a {axis} of {matrix} [{spec.summary}].{hint}')


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
    df, canonical, spec = _load_matrix(matrix)
    if row_code not in df.index:
        raise _missing_label_error(row_code, canonical, spec, 'row')
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
            'framework': spec.framework,
            'valuation': spec.valuation,
            'redefinition': spec.redefinition,
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
    df, canonical, spec = _load_matrix(matrix)
    if column_code not in df.columns:
        raise _missing_label_error(column_code, canonical, spec, 'column')
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
            'framework': spec.framework,
            'valuation': spec.valuation,
            'redefinition': spec.redefinition,
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
