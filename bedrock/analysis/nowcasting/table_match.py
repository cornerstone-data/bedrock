"""Cell-by-cell agreement between a nowcast table and its reference table.

The question this answers, for any section of a Supply or Use table: *how much
of this table do we actually have, and how good is it* -- per cell, and equally
per row total and column total.

No plotting lives here.  :func:`compare_tables` returns a
:class:`TableMatch`, which is a status matrix plus relative errors plus
machine-readable counts, so the same comparison is an assertion in a test
(:meth:`TableMatch.assert_ok`) and the input to the renderer in
``bedrock.analysis.nowcasting.plots``.

Five statuses, not three
------------------------

"No data" is really two different things and they are not equally bad, so
:class:`CellStatus` splits them:

=========== ============================================================
``MATCH``   both sides present, agreeing within tolerance
``PARTIAL`` both sides present, disagreeing -- graded by :attr:`~TableMatch.severity`
``MISS``    the reference has a value and we produced nothing -- a failure
``EXTRA``   we produced a value where the reference has none -- mass in the wrong place
``ABSENT``  neither side has a value -- genuinely nothing to say
=========== ============================================================

``MISS`` and ``EXTRA`` are both silent under a totals check, and they cancel
each other, which is exactly the failure this module exists to surface.

Why the margins are first-class
-------------------------------

``T014`` nets to ~1 economy-wide and redefinition preserves every total by
construction, so **a totals check passes on broken data**.  The row and column
totals are therefore classified by the same rules as the interior and reported
alongside it: a green interior with yellow column totals (or the reverse)
localises an error that no scalar check can see.

One tolerance scale per comparison
----------------------------------

:class:`Tolerance` is supplied once per comparison and is applied identically
to every cell and to both margins.  It never varies by row or by column: a
colour has to mean the same thing everywhere in a picture, or the picture
cannot be read.  What *is* per-comparison is the bar itself -- PCE reconciles
to ~1.3%, PEQ to 0.22%, the Supply identities exactly -- so the caller states
it rather than the renderer inventing one.

::

    from bedrock.analysis.nowcasting.table_match import (
        Tolerance, compare_tables,
    )

    match = compare_tables(candidate, reference, tolerance=Tolerance(rtol=0.013))
    print(match.report())
    match.assert_ok(max_miss=0)
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import IntEnum

import numpy as np
import pandas as pd


class CellStatus(IntEnum):
    """Per-cell verdict.  Order is the rendering order, not a severity order."""

    ABSENT = 0
    MATCH = 1
    PARTIAL = 2
    MISS = 3
    EXTRA = 4


#: Lower-case names, used as column labels in :meth:`TableMatch.counts` and as
#: the keys of :meth:`TableMatch.summary`.
STATUS_NAMES: dict[CellStatus, str] = {
    CellStatus.ABSENT: 'absent',
    CellStatus.MATCH: 'match',
    CellStatus.PARTIAL: 'partial',
    CellStatus.MISS: 'miss',
    CellStatus.EXTRA: 'extra',
}

_STATUS_NAME_BY_CODE = {int(code): name for code, name in STATUS_NAMES.items()}


@dataclass(frozen=True)
class Tolerance:
    """The green/yellow boundary, and the scale of the yellow ramp.

    One instance governs a whole comparison -- every cell, every row total,
    every column total -- so that a shade means the same thing wherever it
    appears.  A cell is :attr:`~CellStatus.MATCH` when
    ``|candidate - reference| <= atol + rtol * |reference|``.

    :param rtol: relative tolerance as a fraction (``0.013`` for the ~1.3% PCE
        bar, ``0.0022`` for PEQ, ``0.0`` for an identity that must hold exactly)
    :param atol: absolute tolerance in the table's own units, which keeps cells
        with a near-zero reference from failing on rounding alone
    :param ramp: relative error at which the yellow ramp saturates.  Severity is
        ``0`` at the tolerance boundary and ``1`` at ``ramp`` and beyond, so the
        shading has a stated scale that reports can quote.
    :param presence: ``|value| <= presence`` counts as *no data on that side*.
        The default treats an exact zero as absent, which is what a zero means
        in a published BEA cell.
    """

    rtol: float = 0.0
    atol: float = 0.0
    ramp: float = 0.10
    presence: float = 0.0

    def __post_init__(self) -> None:
        if self.ramp <= self.rtol:
            raise ValueError(
                f'ramp ({self.ramp}) must exceed rtol ({self.rtol}); the ramp is '
                'the relative error at which the shading saturates, so it has to '
                'start where the tolerance ends'
            )
        for name in ('rtol', 'atol', 'presence'):
            if getattr(self, name) < 0:
                raise ValueError(f'{name} must be >= 0, got {getattr(self, name)}')

    def describe(self) -> str:
        parts = [f'rtol={self.rtol:.4g}']
        if self.atol:
            parts.append(f'atol={self.atol:,.4g}')
        parts.append(f'ramp={self.ramp:.4g}')
        if self.presence:
            parts.append(f'presence={self.presence:,.4g}')
        return ', '.join(parts)


#: The bar for a comparison that is an accounting identity rather than a
#: reconciliation against an independently built series.
EXACT = Tolerance(rtol=0.0, atol=0.0, ramp=0.01)


def classify(
    candidate: pd.DataFrame | pd.Series,
    reference: pd.DataFrame | pd.Series,
    tolerance: Tolerance,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Status, relative error and severity for two already-aligned objects.

    Split out from :func:`compare_tables` because the margins run through the
    exact same rules as the interior, and reusing one implementation is what
    makes that claim true rather than aspirational.

    :return: ``(status, rel_error, severity)``, each shaped like the inputs.
        ``rel_error`` is ``|c - r| / |r|`` as a fraction, ``NaN`` where the
        reference is absent.  ``severity`` is ``0``-``1`` on ``PARTIAL`` cells
        and ``NaN`` elsewhere.
    """
    is_frame = isinstance(candidate, pd.DataFrame)
    c = np.asarray(candidate, dtype=float)
    r = np.asarray(reference, dtype=float)
    if c.shape != r.shape:
        raise ValueError(f'shape mismatch: candidate {c.shape} vs reference {r.shape}')

    c_here = np.isfinite(c) & (np.abs(c) > tolerance.presence)
    r_here = np.isfinite(r) & (np.abs(r) > tolerance.presence)
    both = c_here & r_here

    with np.errstate(invalid='ignore', divide='ignore'):
        safe_c = np.where(both, c, 0.0)
        safe_r = np.where(both, r, 0.0)
        within = np.abs(safe_c - safe_r) <= tolerance.atol + tolerance.rtol * np.abs(
            safe_r
        )
        rel = np.where(
            r_here,
            np.abs(np.nan_to_num(c) - r) / np.abs(np.where(r_here, r, 1.0)),
            np.nan,
        )

    status = np.full(c.shape, int(CellStatus.ABSENT), dtype=np.int8)
    status[both & within] = int(CellStatus.MATCH)
    status[both & ~within] = int(CellStatus.PARTIAL)
    status[r_here & ~c_here] = int(CellStatus.MISS)
    status[c_here & ~r_here] = int(CellStatus.EXTRA)

    span = tolerance.ramp - tolerance.rtol
    with np.errstate(invalid='ignore'):
        sev = np.clip((rel - tolerance.rtol) / span, 0.0, 1.0)
    severity = np.where(status == int(CellStatus.PARTIAL), sev, np.nan)

    if is_frame:
        assert isinstance(candidate, pd.DataFrame)
        index, columns = candidate.index, candidate.columns

        def wrap(a: np.ndarray) -> pd.DataFrame:
            return pd.DataFrame(a, index=index, columns=columns)

    else:
        assert isinstance(candidate, pd.Series)
        index = candidate.index

        def wrap(a: np.ndarray) -> pd.Series:  # type: ignore[misc]
            return pd.Series(a, index=index)

    return wrap(status), wrap(rel), wrap(severity)


@dataclass
class Margin:
    """One axis of totals, classified by the same rules as the interior."""

    #: ``candidate``, ``reference``, ``diff``, ``rel_error``, ``severity``,
    #: ``status`` (``int8`` :class:`CellStatus`) and ``status_name``, indexed by
    #: the axis labels.
    table: pd.DataFrame
    #: ``'row'`` or ``'column'``
    axis: str

    @property
    def status(self) -> pd.Series:
        return self.table['status']

    @property
    def severity(self) -> pd.Series:
        return self.table['severity']

    def counts(self) -> pd.Series:
        return _counts(self.table['status'].to_numpy())

    def failures(self) -> pd.DataFrame:
        """Totals that are neither ``MATCH`` nor ``ABSENT``, worst first."""
        bad = self.table.loc[
            ~self.table['status'].isin([int(CellStatus.MATCH), int(CellStatus.ABSENT)])
        ]
        return bad.reindex(bad['diff'].abs().sort_values(ascending=False).index)


@dataclass
class Residual:
    """Candidate mass sitting on labels the comparison frame does not contain.

    A section pins its frame to the reference's own code space, so a candidate
    row or column outside it -- a NAICS code where a BEA detail commodity
    belongs, say -- is not drawn.  It is still counted here, because dropping
    it silently is exactly how mass in the wrong place goes unnoticed: it would
    otherwise vanish from the cells, the margins and the grand total at once.
    """

    #: Off-frame row label -> candidate total across all its columns
    rows: pd.Series
    #: Off-frame column label -> candidate total across all its rows
    columns: pd.Series
    #: All candidate mass outside the frame, counted once
    total: float

    def __bool__(self) -> bool:
        return bool(len(self.rows) or len(self.columns))

    def describe(self, n: int = 10) -> list[str]:
        if not self:
            return []
        lines = [
            f'RESIDUAL OUTSIDE THE FRAME  {self.total:,.0f}  '
            f'({len(self.rows)} rows, {len(self.columns)} columns not drawn)'
        ]
        for axis, series in (('row', self.rows), ('column', self.columns)):
            if not len(series):
                continue
            ranked = series.reindex(series.abs().sort_values(ascending=False).index)
            lines += [
                f'  {axis} {label}: {value:,.0f}'
                for label, value in ranked.head(n).items()
            ]
            if len(ranked) > n:
                rest = float(ranked.iloc[n:].sum())
                lines.append(f'  ... and {len(ranked) - n} more {axis}s, {rest:,.0f}')
        return lines


@dataclass
class TableMatch:
    """Per-cell status, per-margin status, and the counts that make it a test."""

    #: candidate on the comparison frame (``NaN`` where it had no row or column
    #: at all, which is different from a zero it actually produced)
    candidate: pd.DataFrame
    #: reference on the same frame
    reference: pd.DataFrame
    #: :class:`CellStatus` codes, ``int8``
    status: pd.DataFrame
    #: ``|c - r| / |r|`` as a fraction; ``NaN`` where the reference is absent
    rel_error: pd.DataFrame
    #: ``0``-``1`` across the tolerance-to-ramp band, ``NaN`` off ``PARTIAL``
    severity: pd.DataFrame
    row_totals: Margin
    col_totals: Margin
    tolerance: Tolerance
    #: Candidate mass the frame excluded.  Empty unless ``rows``/``columns``
    #: pinned the frame and the candidate had labels outside it.
    residual: Residual
    label: str = ''

    # ------------------------------------------------------------------ counts

    def counts(self) -> pd.DataFrame:
        """Counts per status for the cells and for each margin -- the test output.

        Rows ``cells`` / ``row_totals`` / ``col_totals``, columns the five
        :data:`STATUS_NAMES`.
        """
        return pd.DataFrame(
            {
                'cells': _counts(self.status.to_numpy()),
                'row_totals': self.row_totals.counts(),
                'col_totals': self.col_totals.counts(),
            }
        ).T

    @property
    def grand_total(self) -> pd.Series:
        c = float(np.nansum(self.candidate.to_numpy()))
        r = float(np.nansum(self.reference.to_numpy()))
        return pd.Series(
            {
                'candidate': c,
                'reference': r,
                'diff': c - r,
                'rel_error': abs(c - r) / abs(r) if r else float('nan'),
            }
        )

    @property
    def coverage(self) -> float:
        """Share of the cells the reference populates that we also populate.

        ``MATCH + PARTIAL`` over ``MATCH + PARTIAL + MISS`` -- "how much of this
        table do we have", asked before "how good is it".
        """
        n = self.counts().loc['cells']
        have = n['match'] + n['partial']
        want = have + n['miss']
        return float(have / want) if want else float('nan')

    @property
    def accuracy(self) -> float:
        """Share of the cells we do populate that land within tolerance."""
        n = self.counts().loc['cells']
        have = n['match'] + n['partial']
        return float(n['match'] / have) if have else float('nan')

    def summary(self) -> dict[str, float | str]:
        """A flat, machine-readable digest -- what CI records and compares."""
        n = self.counts()
        out: dict[str, float | str] = {'label': self.label}
        for scope in n.index:
            for name in n.columns:
                out[f'{scope}.{name}'] = int(n.loc[scope, name])
        out['coverage'] = self.coverage
        out['accuracy'] = self.accuracy
        gt = self.grand_total
        out['grand_total.candidate'] = float(gt['candidate'])
        out['grand_total.reference'] = float(gt['reference'])
        out['grand_total.rel_error'] = float(gt['rel_error'])
        out['residual.total'] = float(self.residual.total)
        out['residual.rows'] = len(self.residual.rows)
        out['residual.columns'] = len(self.residual.columns)
        out['tolerance'] = self.tolerance.describe()
        return out

    # ------------------------------------------------------------------- cells

    def cells(self, statuses: Iterable[CellStatus] | None = None) -> pd.DataFrame:
        """Long form: one row per cell, with both values, the error and the status.

        :param statuses: keep only these statuses; the default drops ``ABSENT``,
            which is almost always the overwhelming majority of a Use table and
            carries no information.
        """
        keep = (
            {int(s) for s in statuses}
            if statuses is not None
            else {int(s) for s in CellStatus if s is not CellStatus.ABSENT}
        )
        stack = lambda df: df.stack(future_stack=True)  # noqa: E731
        long = pd.DataFrame(
            {
                'candidate': stack(self.candidate),
                'reference': stack(self.reference),
                'rel_error': stack(self.rel_error),
                'severity': stack(self.severity),
                'status': stack(self.status).astype(int),
            }
        )
        long.index.names = [
            self.candidate.index.name or 'row',
            self.candidate.columns.name or 'column',
        ]
        long['diff'] = long['candidate'].fillna(0.0) - long['reference'].fillna(0.0)
        long['status_name'] = long['status'].map(_STATUS_NAME_BY_CODE)
        return long.loc[long['status'].isin(keep)].reset_index()

    def worst(self, n: int = 15) -> pd.DataFrame:
        """The ``n`` cells contributing most absolute error, worst first.

        Ranked on absolute difference rather than relative, because a 90% miss
        on a rounding-sized cell is noise and a 2% miss on PCE is not.
        """
        cells = self.cells()
        return cells.reindex(
            cells['diff'].abs().sort_values(ascending=False).index
        ).head(n)

    # ------------------------------------------------------------------- gates

    def ok(
        self,
        *,
        max_partial: int | None = None,
        max_miss: int = 0,
        max_extra: int = 0,
        max_margin_partial: int | None = None,
        min_coverage: float | None = None,
    ) -> bool:
        """Whether the comparison clears the given gates.  See :meth:`assert_ok`."""
        return not self._violations(
            max_partial=max_partial,
            max_miss=max_miss,
            max_extra=max_extra,
            max_margin_partial=max_margin_partial,
            min_coverage=min_coverage,
        )

    def assert_ok(
        self,
        *,
        max_partial: int | None = None,
        max_miss: int = 0,
        max_extra: int = 0,
        max_margin_partial: int | None = None,
        min_coverage: float | None = None,
    ) -> None:
        """Raise ``AssertionError`` with the report if any gate is breached.

        Every gate is an explicit count, so a test states the budget it holds a
        step to instead of asserting a scalar that redefinition preserves
        anyway.

        :param max_partial: cells allowed outside tolerance; ``None`` for no cap
        :param max_miss: cells the reference has and we do not
        :param max_extra: cells we have and the reference does not
        :param max_margin_partial: row *and* column totals allowed outside
            tolerance, counted together; ``None`` for no cap
        :param min_coverage: floor on :attr:`coverage`
        """
        violations = self._violations(
            max_partial=max_partial,
            max_miss=max_miss,
            max_extra=max_extra,
            max_margin_partial=max_margin_partial,
            min_coverage=min_coverage,
        )
        if violations:
            raise AssertionError('\n'.join([*violations, '', self.report(n_worst=10)]))

    def _violations(
        self,
        *,
        max_partial: int | None,
        max_miss: int,
        max_extra: int,
        max_margin_partial: int | None,
        min_coverage: float | None,
    ) -> list[str]:
        n = self.counts()
        cells = n.loc['cells']
        out: list[str] = []
        where = f'{self.label}: ' if self.label else ''
        for name, cap in (
            ('partial', max_partial),
            ('miss', max_miss),
            ('extra', max_extra),
        ):
            if cap is not None and cells[name] > cap:
                out.append(f'{where}{cells[name]} {name} cells, budget {cap}')
        if max_margin_partial is not None:
            margin_partial = int(
                float(n.loc['row_totals', 'partial'])  # type: ignore[arg-type]
                + float(n.loc['col_totals', 'partial'])  # type: ignore[arg-type]
            )
            if margin_partial > max_margin_partial:
                out.append(
                    f'{where}{margin_partial} row/column totals outside tolerance, '
                    f'budget {max_margin_partial}'
                )
        if min_coverage is not None and not self.coverage >= min_coverage:
            out.append(
                f'{where}coverage {self.coverage:.3f} below floor {min_coverage:.3f}'
            )
        return out

    # ------------------------------------------------------------------ output

    def report(self, n_worst: int = 15, n_margins: int = 10) -> str:
        n = self.counts()
        gt = self.grand_total
        rows, cols = self.status.shape
        lines = [
            f'{self.label or "table match"}  ({rows} x {cols} = {rows * cols:,} cells)',
            f'tolerance: {self.tolerance.describe()}',
            '',
            'STATUS COUNTS',
            *('  ' + ln for ln in n.to_string().splitlines()),
            '',
            f'  coverage  {self.coverage:6.1%}   (of the cells the reference '
            'populates, how many we do)',
            f'  accuracy  {self.accuracy:6.1%}   (of the cells we populate, how '
            'many land within tolerance)',
            '',
            'GRAND TOTAL',
            f'  candidate  {gt["candidate"]:>20,.0f}',
            f'  reference  {gt["reference"]:>20,.0f}',
            f'  difference {gt["diff"]:>20,.0f}  ({gt["rel_error"]:.3%})',
            '  (this is the number that passes on broken data -- read the margins)',
        ]
        if self.residual:
            lines += ['', *self.residual.describe()]
        for margin in (self.row_totals, self.col_totals):
            bad = margin.failures()
            lines += [
                '',
                f'{margin.axis.upper()} TOTALS OUTSIDE TOLERANCE ({len(bad)})',
            ]
            if len(bad):
                shown = bad.head(n_margins)[
                    ['candidate', 'reference', 'diff', 'rel_error', 'status_name']
                ]
                lines += ['  ' + ln for ln in shown.to_string().splitlines()]
                if len(bad) > n_margins:
                    lines.append(f'  ... and {len(bad) - n_margins} more')
            else:
                lines.append('  (none)')
        worst = self.worst(n_worst)
        if len(worst):
            lines += ['', f'WORST {len(worst)} CELLS BY ABSOLUTE DIFFERENCE', '']
            shown = worst[
                [
                    *worst.columns[:2],
                    'candidate',
                    'reference',
                    'diff',
                    'rel_error',
                    'status_name',
                ]
            ]
            lines += ['  ' + ln for ln in shown.to_string(index=False).splitlines()]
        return '\n'.join(lines)


def _counts(status: np.ndarray) -> pd.Series:
    flat = np.asarray(status).ravel()
    return pd.Series(
        {name: int((flat == int(code)).sum()) for code, name in STATUS_NAMES.items()},
        dtype=int,
    )


def _margin(
    candidate: pd.DataFrame,
    reference: pd.DataFrame,
    tolerance: Tolerance,
    axis: str,
) -> Margin:
    """Totals along one axis, classified by the same rules as the interior.

    Sums skip ``NaN`` -- a row we never produced contributes nothing to its own
    total rather than poisoning it -- but the *presence* of the total is then
    judged on the summed value, so a row of pure misses lands on ``MISS`` and
    not on ``ABSENT``.
    """
    if axis == 'row':
        c, r = candidate.sum(axis=1, min_count=1), reference.sum(axis=1, min_count=1)
    else:
        c, r = candidate.sum(axis=0, min_count=1), reference.sum(axis=0, min_count=1)
    status, rel, sev = classify(c, r, tolerance)
    table = pd.DataFrame(
        {
            'candidate': c,
            'reference': r,
            'diff': c.fillna(0.0) - r.fillna(0.0),
            'rel_error': rel,
            'severity': sev,
            'status': status.astype('int8'),
        }
    )
    table['status_name'] = table['status'].map(_STATUS_NAME_BY_CODE)
    return Margin(table, axis)


def compare_tables(
    candidate: pd.DataFrame,
    reference: pd.DataFrame,
    *,
    tolerance: Tolerance = EXACT,
    rows: Sequence[str] | pd.Index | None = None,
    columns: Sequence[str] | pd.Index | None = None,
    row_aliases: Mapping[str, str] | None = None,
    column_aliases: Mapping[str, str] | None = None,
    scale_candidate: float = 1.0,
    label: str = '',
) -> TableMatch:
    """Compare two tables cell by cell, and on their row and column totals.

    Both frames are put on one comparison frame before anything is classified.
    By default that frame is the union of both axes, so a row only one side has
    stays visible as ``MISS`` or ``EXTRA`` instead of being silently dropped --
    which is the whole point, since those two cancel in every total.

    :param candidate: what we built, rows x columns, in the reference's units
    :param reference: the published table to check against
    :param tolerance: the green/yellow boundary and ramp, applied uniformly to
        every cell and to both margins
    :param rows: force the row axis to exactly these labels, in this order --
        pass a section's declared code list to pin the picture to a fixed frame
        instead of letting the data decide it
    :param columns: the same for the column axis
    :param row_aliases: candidate row label -> reference row label, applied
        before alignment.  For codes naming the same concept in two frameworks
        (the MUT's ``V00200`` against the SUT's ``T00OTOP``); leaving them
        unaliased is also a valid choice, and shows up honestly as a
        ``MISS``/``EXTRA`` pair.
    :param column_aliases: the same for columns
    :param scale_candidate: multiply the candidate, for unit reconciliation
        (``1e6`` to lift a millions-of-dollars candidate into dollars)
    :param label: names the comparison in reports and assertion messages
    """
    if row_aliases:
        candidate = candidate.rename(index=dict(row_aliases))
    if column_aliases:
        candidate = candidate.rename(columns=dict(column_aliases))
    if scale_candidate != 1.0:
        candidate = candidate * scale_candidate

    for name, frame in (('candidate', candidate), ('reference', reference)):
        for axis, labels in (('index', frame.index), ('columns', frame.columns)):
            if labels.has_duplicates:
                dupes = labels[labels.duplicated()].unique().tolist()
                raise ValueError(f'{name} has duplicate {axis} labels: {dupes}')

    row_axis = (
        pd.Index(rows)
        if rows is not None
        else candidate.index.union(reference.index, sort=False)
    )
    col_axis = (
        pd.Index(columns)
        if columns is not None
        else candidate.columns.union(reference.columns, sort=False)
    )
    # A caller that pins the frame may also name it (a section does); only fall
    # back to the input frames' own names when it did not.
    if row_axis.name is None:
        row_axis.name = candidate.index.name or reference.index.name
    if col_axis.name is None:
        col_axis.name = candidate.columns.name or reference.columns.name

    residual = _residual(candidate, row_axis, col_axis)

    c = candidate.reindex(index=row_axis, columns=col_axis).astype(float)
    r = reference.reindex(index=row_axis, columns=col_axis).astype(float)

    status, rel, sev = classify(c, r, tolerance)
    return TableMatch(
        candidate=c,
        reference=r,
        status=status.astype('int8'),
        rel_error=rel,
        severity=sev,
        row_totals=_margin(c, r, tolerance, 'row'),
        col_totals=_margin(c, r, tolerance, 'column'),
        tolerance=tolerance,
        residual=residual,
        label=label,
    )


def _residual(
    candidate: pd.DataFrame, row_axis: pd.Index, col_axis: pd.Index
) -> Residual:
    """Candidate mass on labels the frame leaves out.  See :class:`Residual`."""
    off_rows = candidate.index.difference(row_axis, sort=False)
    off_cols = candidate.columns.difference(col_axis, sort=False)
    if not len(off_rows) and not len(off_cols):
        empty = pd.Series(dtype=float)
        return Residual(rows=empty, columns=empty.copy(), total=0.0)

    values = candidate.astype(float)
    kept = values.reindex(
        index=values.index.intersection(row_axis),
        columns=values.columns.intersection(col_axis),
    )
    # Subtracting the kept block counts a cell that is off-frame on both axes
    # once, which summing the two strips would not.
    total = float(np.nansum(values.to_numpy())) - float(np.nansum(kept.to_numpy()))
    return Residual(
        rows=values.loc[off_rows].sum(axis=1),
        columns=values[off_cols].sum(axis=0),
        total=total,
    )
