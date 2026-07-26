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
    split_residual,
    token_relation,
)


def _series(rows: list[tuple[str, str, float]], **kw: object) -> LabeledSeries:
    frame = pd.DataFrame(rows, columns=['code', 'name', 'value'])
    return LabeledSeries(frame, **kw)  # type: ignore[arg-type]


def _dialect_series(rows: list[tuple[str, str, float]], dialect: str) -> LabeledSeries:
    frame = pd.DataFrame(rows, columns=['code', 'name', 'value'])
    return LabeledSeries(frame, meta={'dialect': dialect})


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


class TestSelection:
    """Picking part of a table -- the totals-only comparison shape."""

    #: shaped like NIPA 3.5: two branches, one of which is a childless subtotal
    FRAME = pd.DataFrame(
        {
            'code': ['TOP', 'FED', 'FTP', 'FTP1', 'FTP2', 'FOTH', 'SL', 'SOTH', 'SO1'],
            'name': [
                'Taxes total',
                'Federal',
                'Taxes on product',
                'Excise',
                'Customs',
                'Other taxes on production',
                'State and local',
                'Other taxes on production',
                'Property taxes',
            ],
            'value': [100.0, 30.0, 28.0, 20.0, 8.0, 2.0, 70.0, 70.0, 70.0],
            'level': [0, 1, 2, 3, 3, 2, 1, 2, 3],
        }
    )

    def _series(self) -> LabeledSeries:
        return LabeledSeries(self.FRAME)

    def test_select_picks_named_rows_by_code_or_name(self) -> None:
        picked = self._series().select(['FOTH', 'SOTH'])
        assert picked.total == 72.0
        assert self._series().select(['Property taxes']).total == 70.0

    def test_subtree_follows_indentation(self) -> None:
        sub = self._series().subtree('FTP')
        assert sorted(sub.frame['code']) == ['FTP1', 'FTP2']
        assert sub.total == 28.0

    def test_subtree_stops_at_the_next_sibling(self) -> None:
        # must not run on into the "State and local" branch
        assert sorted(self._series().subtree('FED').frame['code']) == [
            'FOTH',
            'FTP',
            'FTP1',
            'FTP2',
        ]

    def test_subtree_of_a_childless_row_is_empty(self) -> None:
        assert len(self._series().subtree('FOTH')) == 0
        # ... but with the parent kept, leaves() treats it as the leaf it is
        kept = self._series().subtree('FOTH', include_parent=True, leaves_only=True)
        assert kept.total == 2.0

    def test_subtree_leaves_only_drops_intermediate_subtotals(self) -> None:
        leaves = self._series().subtree('FED', include_parent=True, leaves_only=True)
        assert sorted(leaves.frame['code']) == ['FOTH', 'FTP1', 'FTP2']
        assert leaves.total == 30.0

    def test_subtree_rejects_an_unknown_label(self) -> None:
        try:
            self._series().subtree('NOPE')
        except KeyError as exc:
            assert 'NOPE' in str(exc)
        else:
            raise AssertionError('expected a KeyError')


class TestMissingValues:
    def test_blank_values_are_counted_not_dropped(self) -> None:
        s = _series([('a', 'A', 1.0), ('b', 'B', float('nan'))])
        assert len(s) == 2
        assert s.n_missing == 1
        assert s.total == 1.0

    def test_report_survives_an_all_nan_pct_column(self) -> None:
        # a zero reference makes pct_diff undefined. The cell may legitimately
        # print as NaN, but the summary line must not report a NaN median.
        text = compare(
            _series([('a', 'A', 5.0)]), _series([('a', 'A', 0.0)]), on='code'
        ).report()
        agreement = next(ln for ln in text.splitlines() if 'CELL AGREEMENT' in ln)
        assert 'nan' not in agreement.lower()
        assert 'median' not in agreement

    def test_report_caps_long_unmatched_listings(self) -> None:
        candidate = _series([(f'c{i}', f'cand {i}', float(i)) for i in range(40)])
        reference = _series([('z', 'Zulu', 1.0)])
        text = compare(candidate, reference).report(n_unmatched=5)
        assert 'UNMATCHED CANDIDATE ROWS (40 total)' in text
        assert '... and 35 more' in text
        # largest first, so the rows that move a total are the ones shown
        assert 'cand 39' in text and 'cand 1 ' not in text


class TestFootnotes:
    """NIPA footnotes, which are where residual lines are actually defined."""

    ROWS = [
        ['Table 9.9. Something', None, None, None],
        ['Line', None, None, 2017],
        [1, 'Total', 'T000RC', 100.0],
        [2, '  Gasoline', 'G001RC', 60.0],
        [3, '  Other\\1,3\\', 'O001RC', 40.0],
        [4, '  Discontinued\\2\\', 'D001RC', None],
        [None, None, None, None],
        [
            '1. Consists largely of taxes on tires, coal, and nuclear fuel.',
            None,
            None,
            None,
        ],
        ['2. Prior to 1988, included in line 3', None, None, None],
        ['3. Includes indoor tanning services.', None, None, None],
    ]

    def _sheet(self, tmp: str) -> LabeledSeries:
        path = os.path.join(tmp, 'SectionXall_xls.xlsx')
        pd.DataFrame(self.ROWS).to_excel(
            path, sheet_name='T90900-A', index=False, header=False
        )
        return nipa_sheet(path, 'T90900-A', 2017)

    def test_parses_the_footnote_block(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            notes = self._sheet(tmp).footnotes
            assert set(notes) == {'1', '2', '3'}
            assert notes['1'].startswith('Consists largely of taxes on tires')

    def test_resolves_multi_reference_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            texts = self._sheet(tmp).notes_for('O001RC')
            assert len(texts) == 2, 'both 1 and 3 from "Other\\1,3\\"'
            assert any('tanning' in t for t in texts)

    def test_marker_is_kept_out_of_the_comparable_name(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            names = list(self._sheet(tmp).frame['name'])
            assert 'Other' in names, 'the footnote marker must not survive into name'

    def test_composition_only_drops_vintage_notes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sheet = self._sheet(tmp)
            assert set(sheet.annotated()['code']) == {'O001RC', 'D001RC'}
            # "Prior to 1988, included in line 3" says nothing about content
            assert set(sheet.annotated(composition_only=True)['code']) == {'O001RC'}

    def test_no_footnotes_is_not_an_error(self) -> None:
        s = _series([('a', 'A', 1.0)])
        assert s.footnotes == {}
        assert s.notes_for('a') == []
        assert len(s.annotated()) == 0


class TestHierarchyVocabulary:
    """Label conventions, read directly rather than inferred from similarity."""

    def test_splits_residual_markers(self) -> None:
        for label, parent in [
            ('All other miscellaneous manufacturing', 'miscellaneous manufacturing'),
            (
                'Other ambulatory health care services',
                'ambulatory health care services',
            ),
            ('Electrical equipment, n.e.c.', 'electrical equipment'),
        ]:
            split = split_residual(label, 'bea_io_detail')
            assert split is not None, label
            assert split[0] == normalize_name(parent)

    def test_returns_none_when_no_marker_applies(self) -> None:
        assert split_residual('Wholesale trade', 'bea_io_detail') is None

    def test_nipa_dialect_does_not_treat_bare_other_as_a_marker(self) -> None:
        # five BEA summary industries are genuinely named "Other <something>",
        # and NIPA uses the same stubs, so stripping there would be wrong
        assert split_residual('Other retail', 'nipa') is None
        assert split_residual('Other transportation equipment', 'nipa') is None
        # but a detail-list convention still applies
        assert split_residual('All other wood products', 'nipa') is not None

    def test_token_relation_reads_the_four_cases(self) -> None:
        assert (
            token_relation(
                'Other transportation equipment', 'Other Transportation Equipment'
            )
            == 'equal'
        )
        assert (
            token_relation(
                'Ambulatory health care services',
                'Other ambulatory health care services',
            )
            == 'residual'
        )
        assert (
            token_relation(
                'Publishing industries (includes software)',
                'Publishing industries, except internet (includes software)',
            )
            == 'qualified'
        )
        assert (
            token_relation(
                'Support activities for mining', 'Support activities for printing'
            )
            == 'different'
        )

    def test_inflection_is_not_a_difference(self) -> None:
        assert token_relation('Warehousing and storage', 'Warehousing & storage') == (
            'equal'
        )
        assert token_relation('Legal service', 'Legal services') == 'equal'


class TestHierarchyPass:
    def test_records_a_relation_instead_of_matching_a_residual(self) -> None:
        candidate = _dialect_series(
            [('P', 'Ambulatory health care services', 100.0)], 'nipa'
        )
        reference = _dialect_series(
            [
                ('621900', 'Other ambulatory health care services', 20.0),
                ('621100', 'Offices of physicians', 80.0),
            ],
            'bea_io_detail',
        )
        result = align(candidate, reference, on='fuzzy')
        assert len(result.pairs) == 0, 'a residual is not its parent'
        rel = result.relations
        assert len(rel) == 1
        assert rel.at[0, 'parent_name'] == 'Ambulatory health care services'
        assert rel.at[0, 'child_codes'] == '621900'
        assert rel.at[0, 'children_sum'] == 20.0

    def test_groups_several_residual_children_under_one_parent(self) -> None:
        candidate = _dialect_series([('P', 'Wood products', 30.0)], 'nipa')
        reference = _dialect_series(
            [
                ('A', 'Other wood products', 10.0),
                ('B', 'All other wood products', 20.0),
            ],
            'bea_io_detail',
        )
        rel = align(candidate, reference).relations
        assert rel.at[0, 'n_children'] == 2
        assert rel.at[0, 'children_sum'] == 30.0
        assert rel.at[0, 'diff'] == 0.0

    def test_fuzzy_cannot_pair_a_substituted_content_word(self) -> None:
        # the mining/printing failure, which scored 0.90 and was 1857% wrong
        candidate = _series([('c', 'Support activities for mining', 30000.0)])
        reference = _series([('323120', 'Support activities for printing', 1531.0)])
        assert len(align(candidate, reference, on='fuzzy').pairs) == 0

    def test_exact_names_still_match_before_any_hierarchy_logic(self) -> None:
        # "Other transportation equipment" is a real category name on both sides
        candidate = _dialect_series(
            [('N', 'Other transportation equipment', 5.0)], 'nipa'
        )
        reference = _dialect_series(
            [('3364OT', 'Other transportation equipment', 5.0)], 'bea_io_summary'
        )
        result = align(candidate, reference)
        assert result.pairs['method'].tolist() == ['name']
        assert len(result.relations) == 0


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
