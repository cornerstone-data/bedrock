"""The single entry point: :func:`compare`, plus the report it returns.

::

    from bedrock.analysis.nowcasting.compare_NIPA_to_IOT import (
        compare, bea_matrix_row, nipa_flat_table
    )

    result = compare(
        candidate=nipa_flat_table('T60200D', 2017).leaves(),
        reference=bea_matrix_row('V00100'),
        rollup='industry_to_summary',
    )
    print(result.report())

The reconciliation printed by :meth:`Comparison.report` is the point of the
whole package: it separates "the two tables disagree about a cell" from "the two
tables cut the economy differently", which are the two ways these checks fail
and which a plain cell diff conflates.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import pandas as pd

from bedrock.analysis.nowcasting.compare_NIPA_to_IOT.loaders import (
    detail_industry_to_summary,
    summary_industry_names,
)
from bedrock.analysis.nowcasting.compare_NIPA_to_IOT.matching import (
    DEFAULT_FUZZY_CUTOFF,
    Alignment,
    align,
)
from bedrock.analysis.nowcasting.compare_NIPA_to_IOT.series import LabeledSeries

#: Named rollups, applied to the *reference* before matching, for the common
#: case of a detail BEA table against a coarser candidate.
ROLLUPS = {
    # mapping loader, names loader, and the label dialect the result speaks --
    # rolling detail up to summary also swaps in summary labels, so the residual
    # markers that apply change with it
    'industry_to_summary': (
        detail_industry_to_summary,
        summary_industry_names,
        'bea_io_summary',
    ),
}


@dataclass
class Comparison:
    """Cell-level and total-level agreement between two series."""

    cells: pd.DataFrame
    alignment: Alignment
    candidate: LabeledSeries
    reference: LabeledSeries

    # ------------------------------------------------------------------ totals

    @property
    def totals(self) -> pd.Series:
        matched_c = float(self.cells['candidate'].sum())
        matched_r = float(self.cells['reference'].sum())
        unmatched_c = float(self.alignment.candidate_only['value'].sum())
        unmatched_r = float(self.alignment.reference_only['value'].sum())
        return pd.Series(
            {
                'candidate_total': matched_c + unmatched_c,
                'reference_total': matched_r + unmatched_r,
                'matched_candidate': matched_c,
                'matched_reference': matched_r,
                'unmatched_candidate': unmatched_c,
                'unmatched_reference': unmatched_r,
                'matched_diff': matched_c - matched_r,
                'matched_pct_diff': _pct(matched_c, matched_r),
                'total_diff': (matched_c + unmatched_c) - (matched_r + unmatched_r),
                'total_pct_diff': _pct(
                    matched_c + unmatched_c, matched_r + unmatched_r
                ),
                'n_matched': len(self.cells),
                'n_candidate_only': len(self.alignment.candidate_only),
                'n_reference_only': len(self.alignment.reference_only),
            }
        )

    def worst(self, n: int = 15, by: str = 'abs_diff') -> pd.DataFrame:
        """The ``n`` cells that agree least, ranked by ``abs_diff`` or ``pct_diff``."""
        key = 'abs_diff' if by == 'abs_diff' else 'abs_pct_diff'
        return self.cells.nlargest(n, key)[
            [
                'candidate_name',
                'reference_code',
                'n_detail',
                'candidate',
                'reference',
                'diff',
                'pct_diff',
                'method',
            ]
        ]

    def close(self, tol_pct: float = 1.0) -> pd.DataFrame:
        """Cells agreeing within ``tol_pct`` percent."""
        return self.cells.loc[self.cells['abs_pct_diff'] <= tol_pct]

    def one_to_one(self) -> pd.DataFrame:
        """Cells whose reference side is a single un-aggregated code.

        When the reference was rolled up, these are the rows where the coarse
        group happens to contain exactly one original code -- so they are true
        detail-granularity comparisons, not aggregates, and can be read as such.
        """
        return self.cells.loc[self.cells['n_detail'] == 1]

    def aggregated(self) -> pd.DataFrame:
        """Cells whose reference side sums two or more codes."""
        return self.cells.loc[self.cells['n_detail'] > 1]

    def detail(self, reference_code: str) -> pd.DataFrame:
        """The codes and values composing one reference cell."""
        rows = self.reference.members.get(reference_code, [])
        return pd.DataFrame(rows, columns=['code', 'value'])

    # ------------------------------------------------------------------ output

    def report(
        self, n_worst: int = 15, tol_pct: float = 1.0, n_unmatched: int = 15
    ) -> str:
        t = self.totals
        methods = self.alignment.match_counts
        lines = [
            f'candidate : {self.candidate.label}  (n={len(self.candidate)}, '
            f'unit={self.candidate.unit or "?"})'
            + (
                f'  [{self.candidate.n_missing} blank values]'
                if self.candidate.n_missing
                else ''
            ),
            f'reference : {self.reference.label}  (n={len(self.reference)}, '
            f'unit={self.reference.unit or "?"})'
            # BEA publishes several detail Use tables, differing by framework,
            # valuation and redefinition, so a report that does not name which
            # one is ambiguous about what it just compared against.
            + (f'  [{self.reference.provenance}]' if self.reference.provenance else '')
            + (
                f'  [{self.reference.n_missing} blank values]'
                if self.reference.n_missing
                else ''
            ),
            '',
            'TOTALS',
            f'  candidate total     {t["candidate_total"]:>18,.0f}',
            f'  reference total     {t["reference_total"]:>18,.0f}',
            f'  difference          {t["total_diff"]:>18,.0f}  '
            f'({_fmt_pct(t["total_pct_diff"])})',
            '',
            f'  matched cells       {int(t["n_matched"]):>18,d}',
            f'  matched candidate   {t["matched_candidate"]:>18,.0f}',
            f'  matched reference   {t["matched_reference"]:>18,.0f}',
            f'  matched difference  {t["matched_diff"]:>18,.0f}  '
            f'({_fmt_pct(t["matched_pct_diff"])})',
            '',
            f'  candidate-only      {t["unmatched_candidate"]:>18,.0f}  '
            f'({int(t["n_candidate_only"])} rows)',
            f'  reference-only      {t["unmatched_reference"]:>18,.0f}  '
            f'({int(t["n_reference_only"])} rows)',
            '',
            'MATCHED BY',
        ]
        lines += [f'  {k:<12} {v:>5,d}' for k, v in methods.items()] or ['  (none)']

        within = len(self.close(tol_pct))
        n = len(self.cells)
        median = self.cells['abs_pct_diff'].dropna().median() if n else float('nan')
        lines += [
            '',
            f'CELL AGREEMENT  ({within}/{n} within {tol_pct:g}%'
            + (f', median |diff| {median:.2f}%)' if pd.notna(median) else ')'),
        ]
        # State the granularity the comparison actually ran at. A cell whose
        # reference side is one code is a detail-level comparison; one that sums
        # several is a claim about an aggregate, and the two should not be read
        # as if they were the same evidence.
        if n and self.reference.members:
            exact = len(self.one_to_one())
            agg = self.aggregated()
            lines += [
                f'  {exact} of {n} cells are 1:1 with a single reference code',
                (
                    f'  {len(agg)} aggregate 2-{int(agg["n_detail"].max())} codes'
                    if len(agg)
                    else '  0 cells are aggregates'
                ),
                '  (per-cell composition in the detail_members column of the csv)',
            ]
        if n:
            lines += ['', f'WORST {min(n_worst, n)} CELLS BY ABSOLUTE DIFFERENCE', '']
            lines += [
                '  ' + ln
                for ln in self.worst(n_worst).to_string(index=False).splitlines()
            ]

        for side, frame in (
            ('CANDIDATE', self.alignment.candidate_only),
            ('REFERENCE', self.alignment.reference_only),
        ):
            if not len(frame):
                continue
            # Comparing tables with no cell correspondence leaves hundreds of
            # rows over, and dumping all of them buries the totals that are the
            # actual answer. Largest first, since those are what move a total.
            shown = frame.reindex(
                frame['value'].abs().sort_values(ascending=False).index
            ).head(n_unmatched)
            lines += ['', f'UNMATCHED {side} ROWS ({len(frame)} total)', '']
            lines += [
                '  ' + ln
                for ln in shown[['code', 'name', 'value']]
                .to_string(index=False)
                .splitlines()
            ]
            if len(frame) > n_unmatched:
                rest = frame['value'].sum() - shown['value'].sum()
                lines.append(
                    f'  ... and {len(frame) - n_unmatched} more, totalling {rest:,.0f}'
                )
        relations = self.alignment.relations
        if len(relations):
            lines += [
                '',
                'HIERARCHY RELATIONS (label says part-of, so not matched)',
                '',
            ]
            shown = relations[
                [
                    'parent_side',
                    'parent_name',
                    'parent_value',
                    'n_children',
                    'children_sum',
                    'diff',
                    'child_codes',
                ]
            ]
            lines += ['  ' + ln for ln in shown.to_string(index=False).splitlines()]
            lines += [
                '  (a small diff means the children account for the parent; act on'
                ' it with',
                '   merge_reference= or leaves(keep=, drop=) rather than trusting a'
                ' name match)',
            ]

        if self.alignment.ambiguous:
            lines += [
                '',
                'AMBIGUOUS KEYS (duplicated on one side, left unmatched)',
                '  ' + ', '.join(self.alignment.ambiguous),
            ]
        return '\n'.join(lines)

    def to_csv(self, path: str) -> str:
        """Write the cell table, plus ``*_unmatched.csv`` for the leftovers."""
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.cells.to_csv(path, index=False)
        stem = os.path.splitext(path)[0]
        unmatched = pd.concat(
            [
                self.alignment.candidate_only.assign(side='candidate'),
                self.alignment.reference_only.assign(side='reference'),
            ],
            ignore_index=True,
        )
        unmatched.to_csv(f'{stem}_unmatched.csv', index=False)
        return path


def _pct(a: float, b: float) -> float:
    return float('nan') if b == 0 else (a - b) / abs(b) * 100.0


def _fmt_pct(v: float) -> str:
    return 'n/a' if pd.isna(v) else f'{v:+.3f}%'


def compare(
    candidate: LabeledSeries,
    reference: LabeledSeries,
    *,
    on: str = 'auto',
    rollup: str | dict[str, list[str]] | dict[str, str] | None = None,
    crosswalk: dict[str, list[str]] | dict[str, str] | None = None,
    overrides: dict[str, str] | None = None,
    merge_reference: dict[str, list[str]] | None = None,
    merge_candidate: dict[str, list[str]] | None = None,
    merge_names: dict[str, str] | None = None,
    fuzzy_cutoff: float = DEFAULT_FUZZY_CUTOFF,
    scale_candidate: float = 1.0,
) -> Comparison:
    """Compare a candidate series against a reference series, cell and total.

    :param rollup: aggregate the *reference* to coarser codes before matching --
        a key of :data:`ROLLUPS` (e.g. ``'industry_to_summary'``) or your own
        ``{reference_code: [target_code]}`` mapping.  Needed whenever the
        reference is finer-grained than the candidate, which is the usual shape
        when checking a NIPA-style table against a BEA detail matrix: the detail
        table is still the source, but the comparison happens at the granularity
        the candidate can actually support.  Every cell keeps its detail
        composition in ``n_detail``/``detail_members``, and
        :meth:`Comparison.one_to_one` picks out the cells no aggregation touched.
    :param on: which alignment passes to run, see :func:`~.matching.align`.
        Fuzzy name matching runs last and is included by default; pass
        ``on='code'`` or ``on='name'`` to exclude it.
    :param crosswalk: candidate code -> reference code, tried after exact codes
    :param overrides: hand-written pairs that win over everything
    :param merge_reference: ``{new_code: [codes]}`` summed on the reference side
        after ``rollup``, for industries the two tables partition differently
        (BEA's ``HS``/``ORE`` against one NIPA "Real estate" line)
    :param merge_candidate: the same, on the candidate side
    :param merge_names: labels for the synthetic codes the two merges introduce,
        so name matching can still reach them
    :param scale_candidate: multiply candidate values, for unit reconciliation
    """
    if scale_candidate != 1.0:
        candidate = candidate.scale(scale_candidate)

    if rollup is not None:
        if isinstance(rollup, str):
            mapping_fn, names_fn, dialect = ROLLUPS[rollup]
            reference = reference.rollup(mapping_fn()).with_names(names_fn())
            reference.label = f'{reference.label} [{rollup}]'
            reference.meta['dialect'] = dialect
        else:
            reference = reference.rollup(rollup)

    if merge_reference:
        reference = reference.merge_codes(merge_reference, merge_names)
    if merge_candidate:
        candidate = candidate.merge_codes(merge_candidate, merge_names)

    alignment = align(
        candidate,
        reference,
        on=on,
        crosswalk=crosswalk,
        overrides=overrides,
        fuzzy_cutoff=fuzzy_cutoff,
    )

    cells = alignment.pairs.rename(
        columns={'candidate_value': 'candidate', 'reference_value': 'reference'}
    )
    cells['diff'] = cells['candidate'] - cells['reference']
    cells['pct_diff'] = [
        _pct(c, r) for c, r in zip(cells['candidate'], cells['reference'])
    ]
    cells['abs_diff'] = cells['diff'].abs()
    cells['abs_pct_diff'] = cells['pct_diff'].abs()

    # Carry the reference's pre-aggregation composition onto each cell, so a
    # comparison made at a coarse granularity still says what it was made of --
    # and n_detail == 1 marks the cells that are genuinely 1:1 with a single
    # detail code, i.e. not aggregated at all.
    members = reference.members
    cells['n_detail'] = [len(members.get(code, ())) for code in cells['reference_code']]
    cells['detail_members'] = [
        ';'.join(f'{c}={v:g}' for c, v in members.get(code, ()))
        for code in cells['reference_code']
    ]

    cells = cells.sort_values('abs_diff', ascending=False).reset_index(drop=True)

    return Comparison(cells, alignment, candidate, reference)
