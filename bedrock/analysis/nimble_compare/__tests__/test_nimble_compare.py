"""Tests for nimble_compare.

Everything here is synthetic -- no GCS, no BEA workbooks -- so the alignment
cascade and the hierarchy handling are pinned down independently of whether the
reference data happens to be cached.
"""

from __future__ import annotations

import os
import tempfile

import pandas as pd

from bedrock.analysis.nimble_compare import (
    LabeledSeries,
    align,
    compare,
    frame_series,
    nipa_sheet,
    normalize_code,
    normalize_name,
)


def _series(rows: list[tuple[str, str, float]], **kw: object) -> LabeledSeries:
    frame = pd.DataFrame(rows, columns=['code', 'name', 'value'])
    return LabeledSeries(frame, **kw)  # type: ignore[arg-type]


class TestNormalize:
    def test_name_folds_case_punctuation_and_oxford_comma(self) -> None:
        assert normalize_name(
            'Electrical equipment, appliances, and components'
        ) == normalize_name('Electrical Equipment, Appliances & Components')

    def test_name_strips_both_footnote_styles(self) -> None:
        assert normalize_name('Other retail\\2\\') == normalize_name('Other retail')
        assert normalize_name('Accommodations (104)') == normalize_name(
            'Accommodations'
        )

    def test_name_expands_nec(self) -> None:
        assert normalize_name('Electrical equipment, n.e.c.') == normalize_name(
            'Electrical equipment, not elsewhere classified'
        )

    def test_code_ignores_case_and_separators(self) -> None:
        assert normalize_code('311-FT') == normalize_code('311ft')


class TestLabeledSeries:
    def test_drops_rows_with_neither_code_nor_name(self) -> None:
        s = _series([('42', 'Wholesale trade', 1.0), ('', '', 2.0)])
        assert len(s) == 1

    def test_leaves_drops_parents_by_indent_level(self) -> None:
        frame = pd.DataFrame(
            {
                'code': ['P', 'C1', 'C2', 'L'],
                'name': ['parent', 'child 1', 'child 2', 'loner'],
                'value': [3.0, 1.0, 2.0, 5.0],
                'level': [0, 1, 1, 0],
            }
        )
        leaves = LabeledSeries(frame).leaves()
        assert sorted(leaves.frame['code']) == ['C1', 'C2', 'L']
        assert leaves.total == 8.0

    def test_leaves_keep_and_drop_trade_granularity(self) -> None:
        frame = pd.DataFrame(
            {
                'code': ['P', 'C1', 'C2'],
                'name': ['parent', 'child 1', 'child 2'],
                'value': [3.0, 1.0, 2.0],
                'level': [0, 1, 1],
            }
        )
        # keep the parent and drop its children: the reference does not split it
        leaves = LabeledSeries(frame).leaves(keep=['P'], drop=['C1', 'C2'])
        assert list(leaves.frame['code']) == ['P']

    def test_leaves_selects_by_name_as_well_as_code(self) -> None:
        frame = pd.DataFrame(
            {
                'code': ['P', 'C1'],
                'name': ['parent', 'child 1'],
                'value': [3.0, 1.0],
                'level': [0, 1],
            }
        )
        leaves = LabeledSeries(frame).leaves(keep=['parent'], drop=['child 1'])
        assert list(leaves.frame['code']) == ['P']

    def test_leaves_is_a_noop_without_levels(self) -> None:
        s = _series([('a', 'A', 1.0), ('b', 'B', 2.0)])
        assert len(s.leaves()) == 2

    def test_merge_codes_sums_group_and_passes_rest_through(self) -> None:
        s = _series([('HS', 'Housing', 1.0), ('ORE', 'Other', 2.0), ('42', 'WT', 5.0)])
        merged = s.merge_codes({'RE': ['HS', 'ORE']}, {'RE': 'Real estate'})
        by_code = dict(zip(merged.frame['code'], merged.frame['value']))
        assert by_code == {'RE': 3.0, '42': 5.0}
        assert 'Real estate' in list(merged.frame['name'])

    def test_rollup_aggregates_and_reports_unmapped(self) -> None:
        s = _series([('111', '', 1.0), ('112', '', 2.0), ('999', '', 9.0)])
        rolled = s.rollup({'111': ['AG'], '112': ['AG']})
        assert dict(zip(rolled.frame['code'], rolled.frame['value'])) == {'AG': 3.0}
        assert rolled.meta['unmapped_codes'] == ['999']

    def test_rollup_splits_a_multi_target_code_evenly(self) -> None:
        s = _series([('111', '', 10.0)])
        rolled = s.rollup({'111': ['A', 'B']})
        assert dict(zip(rolled.frame['code'], rolled.frame['value'])) == {
            'A': 5.0,
            'B': 5.0,
        }

    def test_scale_converts_units(self) -> None:
        s = _series([('a', 'A', 2e6)], unit='USD')
        scaled = s.scale(1e-6, 'Million USD')
        assert scaled.total == 2.0
        assert scaled.unit == 'Million USD'


class TestAlign:
    def test_cascade_prefers_code_then_name_then_fuzzy(self) -> None:
        candidate = _series(
            [
                ('42', 'Wholesale trade', 1.0),
                ('X1', 'Educational services', 2.0),
                ('X2', 'Ambulatory health care services', 3.0),
                ('X3', 'Nothing like it', 4.0),
            ]
        )
        reference = _series(
            [
                ('42', 'Wholesale', 1.0),
                ('61', 'Educational Services', 2.0),
                # near-miss wording, so only the fuzzy pass reaches it
                ('621', 'Ambulatory health care service', 3.0),
                ('999', 'Utterly different', 4.0),
            ]
        )
        pairs = align(candidate, reference, on='fuzzy').pairs
        methods = dict(zip(pairs['candidate_code'], pairs['method']))
        assert methods['42'] == 'code'
        assert methods['X1'] == 'name'
        assert methods['X2'] == 'fuzzy'
        assert 'X3' not in methods

    def test_fuzzy_is_opt_in(self) -> None:
        # the pass that mistook mining support for printing support does not run
        # unless asked for
        candidate = _series([('X', 'Ambulatory health care services', 1.0)])
        reference = _series([('621', 'Ambulatory health care service', 1.0)])
        assert len(align(candidate, reference).pairs) == 0
        assert len(align(candidate, reference, on='fuzzy').pairs) == 1

    def test_fuzzy_cutoff_rejects_a_whole_vs_its_part(self) -> None:
        # even opted in, the cutoff stops this pairing: matching them would
        # silently drop the nondefense half
        candidate = _series([('c', 'Federal general government', 1.0)])
        reference = _series([('GFGD', 'Federal general government (defense)', 1.0)])
        assert len(align(candidate, reference, on='fuzzy').pairs) == 0
        assert len(align(candidate, reference, on='fuzzy', fuzzy_cutoff=0.8).pairs) == 1

    def test_rejects_an_unknown_on_value(self) -> None:
        try:
            align(_series([('a', 'A', 1.0)]), _series([('a', 'A', 1.0)]), on='exact')
        except ValueError as exc:
            assert 'fuzzy' in str(exc)
        else:
            raise AssertionError("expected a ValueError for on='exact'")

    def test_duplicate_keys_are_reported_not_guessed(self) -> None:
        candidate = _series(
            [('A', 'Government enterprises', 1.0), ('B', 'Government enterprises', 2.0)]
        )
        reference = _series([('GFE', 'Government enterprises', 3.0)])
        for on in ('auto', 'fuzzy'):
            result = align(candidate, reference, on=on)
            # the fuzzy pass must not resolve what the exact pass refused to
            assert len(result.pairs) == 0, on
            assert 'government enterprises' in result.ambiguous

    def test_overrides_win_and_accept_either_key(self) -> None:
        candidate = _series([('N4055C', 'Publishing industries', 1.0)])
        reference = _series([('511', 'Something else entirely', 1.0)])
        by_code = align(candidate, reference, overrides={'N4055C': '511'})
        by_name = align(
            candidate,
            reference,
            overrides={'Publishing industries': 'Something else entirely'},
        )
        assert by_code.pairs['method'].tolist() == ['override']
        assert by_name.pairs['method'].tolist() == ['override']

    def test_matching_is_one_to_one(self) -> None:
        candidate = _series([('a', 'Same name', 1.0)])
        reference = _series([('x', 'Same name', 1.0), ('y', 'Same name', 2.0)])
        # ambiguous on the reference side, so nothing is claimed
        assert len(align(candidate, reference, on='fuzzy').pairs) == 0


class TestDetailComposition:
    """A comparison run at a coarse granularity still reports its detail."""

    #: three detail codes; D1 alone becomes group G1, D2+D3 become G2
    MAPPING = {'D1': ['G1'], 'D2': ['G2'], 'D3': ['G2']}

    def _reference(self) -> LabeledSeries:
        return _series([('D1', 'One', 5.0), ('D2', 'Two', 3.0), ('D3', 'Three', 4.0)])

    def test_rollup_records_the_codes_behind_each_group(self) -> None:
        rolled = self._reference().rollup(self.MAPPING)
        assert rolled.members == {
            'G1': [('D1', 5.0)],
            'G2': [('D2', 3.0), ('D3', 4.0)],
        }

    def test_cells_carry_n_detail_and_members(self) -> None:
        candidate = _series([('G1', 'g one', 5.0), ('G2', 'g two', 7.0)])
        cells = compare(
            candidate, self._reference(), rollup=self.MAPPING, on='code'
        ).cells.set_index('reference_code')
        assert cells.at['G1', 'n_detail'] == 1
        assert cells.at['G1', 'detail_members'] == 'D1=5'
        assert cells.at['G2', 'n_detail'] == 2
        assert cells.at['G2', 'detail_members'] == 'D2=3;D3=4'

    def test_one_to_one_separates_unaggregated_cells(self) -> None:
        candidate = _series([('G1', 'g one', 5.0), ('G2', 'g two', 7.0)])
        result = compare(candidate, self._reference(), rollup=self.MAPPING, on='code')
        assert result.one_to_one()['reference_code'].tolist() == ['G1']
        assert result.aggregated()['reference_code'].tolist() == ['G2']
        assert result.detail('G2')['code'].tolist() == ['D2', 'D3']

    def test_merge_codes_composes_through_an_earlier_rollup(self) -> None:
        # G1 and G2 are themselves rollups, so merging them must report the
        # original detail codes rather than the intermediate group codes
        candidate = _series([('M', 'merged', 12.0)])
        result = compare(
            candidate,
            self._reference(),
            rollup=self.MAPPING,
            merge_reference={'M': ['G1', 'G2']},
            merge_names={'M': 'merged'},
        )
        assert result.cells.at[0, 'n_detail'] == 3
        assert result.cells.at[0, 'detail_members'] == 'D1=5;D2=3;D3=4'

    def test_no_rollup_means_no_composition_claimed(self) -> None:
        result = compare(_series([('D1', 'One', 5.0)]), self._reference(), on='code')
        assert result.cells.at[0, 'n_detail'] == 0
        assert result.cells.at[0, 'detail_members'] == ''


class TestCompare:
    def test_totals_separate_matched_from_unmatched(self) -> None:
        candidate = _series([('a', 'A', 10.0), ('only', 'Only here', 7.0)])
        reference = _series([('a', 'A', 8.0), ('other', 'Other', 5.0)])
        totals = compare(candidate, reference, on='code').totals
        assert totals['n_matched'] == 1
        assert totals['matched_candidate'] == 10.0
        assert totals['matched_reference'] == 8.0
        assert totals['matched_diff'] == 2.0
        assert totals['matched_pct_diff'] == 25.0
        assert totals['unmatched_candidate'] == 7.0
        assert totals['unmatched_reference'] == 5.0
        assert totals['candidate_total'] == 17.0
        assert totals['reference_total'] == 13.0

    def test_cells_are_ranked_by_absolute_difference(self) -> None:
        candidate = _series([('a', 'A', 100.0), ('b', 'B', 1.0)])
        reference = _series([('a', 'A', 90.0), ('b', 'B', 0.5)])
        cells = compare(candidate, reference, on='code').cells
        assert cells['candidate_code'].tolist() == ['a', 'b']
        assert cells['diff'].tolist() == [10.0, 0.5]

    def test_pct_diff_is_nan_against_a_zero_reference(self) -> None:
        cells = compare(
            _series([('a', 'A', 5.0)]), _series([('a', 'A', 0.0)]), on='code'
        ).cells
        assert pd.isna(cells['pct_diff'].iloc[0])

    def test_scale_candidate_reconciles_units(self) -> None:
        candidate = _series([('a', 'A', 5e6)], unit='USD')
        reference = _series([('a', 'A', 5.0)])
        totals = compare(candidate, reference, on='code', scale_candidate=1e-6).totals
        assert totals['matched_diff'] == 0.0

    def test_merge_reference_closes_a_partition_mismatch(self) -> None:
        candidate = _series([('c', 'Real estate', 3.0)])
        reference = _series([('HS', 'Housing', 1.0), ('ORE', 'Other real estate', 2.0)])
        result = compare(
            candidate,
            reference,
            merge_reference={'RE': ['HS', 'ORE']},
            merge_names={'RE': 'Real estate'},
        )
        assert result.totals['n_matched'] == 1
        assert result.totals['matched_diff'] == 0.0

    def test_report_renders_with_no_matches_at_all(self) -> None:
        text = compare(
            _series([('a', 'Alpha', 1.0)]), _series([('z', 'Zulu', 2.0)])
        ).report()
        assert 'UNMATCHED CANDIDATE ROWS' in text
        assert 'UNMATCHED REFERENCE ROWS' in text

    def test_to_csv_writes_cells_and_unmatched(self) -> None:
        result = compare(
            _series([('a', 'A', 1.0), ('b', 'B', 1.0)]),
            _series([('a', 'A', 1.0)]),
            on='code',
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = result.to_csv(os.path.join(tmp, 'sub', 'cells.csv'))
            assert len(pd.read_csv(path)) == 1
            unmatched = pd.read_csv(os.path.join(tmp, 'sub', 'cells_unmatched.csv'))
            assert unmatched['side'].tolist() == ['candidate']


class TestNipaSheet:
    """The NIPA reader, against a sheet shaped like BEA's but written here."""

    ROWS = [
        ['Table 9.9D. Something by Industry', None, None, None, None],
        ['[Millions of dollars]', None, None, None, None],
        [None, None, None, None, None],
        ['Line', None, None, 2016, 2017],
        # BEA indents the stub head as if it were a detail line
        [1, '      Grand total', 'A000RC', 90.0, 100.0],
        [2, 'Domestic industries', 'A001C', 85.0, 95.0],
        [3, '  Manufacturing', 'N002C', 50.0, 60.0],
        [4, '    Durable goods', 'N003C', 20.0, 25.0],
        [5, '    Nondurable goods', 'N004C', 30.0, 35.0],
        [6, '  Utilities', 'N005C', 35.0, 35.0],
        [7, 'Rest of the world', 'A006C', 5.0, 5.0],
        [None, None, None, None, None],
        ['Legend / Footnotes:', None, None, None, None],
        # unnumbered prose in the label column, below the table
        [None, '1. Consists of some explanatory note.', None, None, None],
    ]

    def _write(self, tmp: str) -> str:
        path = os.path.join(tmp, 'SectionXall_xls.xlsx')
        pd.DataFrame(self.ROWS).to_excel(
            path, sheet_name='T90900D-A', index=False, header=False
        )
        return path

    def test_reads_the_requested_year_and_finds_the_header(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            s = nipa_sheet(self._write(tmp), 'T90900D-A', 2017)
            by_code = dict(zip(s.frame['code'], s.frame['value']))
            assert by_code['N003C'] == 25.0
            assert by_code['A000RC'] == 100.0
            # the unnumbered footnote block below the data is not an industry
            assert len(s) == 7
            assert not any('explanatory' in n for n in s.frame['name'])

    def test_leaves_excludes_the_grand_total_despite_its_indent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            leaves = nipa_sheet(self._write(tmp), 'T90900D-A', 2017).leaves()
            assert sorted(leaves.frame['code']) == ['A006C', 'N003C', 'N004C', 'N005C']
            # 25 + 35 + 35 + 5 -- the published total, counted once
            assert leaves.total == 100.0

    def test_reports_a_missing_year_with_the_available_ones(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._write(tmp)
            try:
                nipa_sheet(path, 'T90900D-A', 1999)
            except KeyError as exc:
                assert '2016' in str(exc) and '2017' in str(exc)
            else:
                raise AssertionError('expected a KeyError for the missing year')


class TestFrameSeries:
    def test_sums_duplicate_keys(self) -> None:
        df = pd.DataFrame({'ind': ['a', 'a', 'b'], 'v': [1.0, 2.0, 4.0]})
        s = frame_series(df, code='ind', value='v')
        assert dict(zip(s.frame['code'], s.frame['value'])) == {'a': 3.0, 'b': 4.0}
