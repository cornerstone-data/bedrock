"""Tests for diff_methods: config diff and mapping content diff."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import cast

import yaml

from bedrock.utils.config.diff_methods import (
    _default_output_path,
    _list_diff_summary,
    _parse_output_path,
    _to_yaml_safe,
    collect_activity_to_sector_mapping_names,
    diff_mapping_file_contents,
    diff_resolved_configs,
)


def test_diff_resolved_configs_identical_dicts() -> None:
    left: dict[str, object] = {'a': 1, 'b': 'x'}
    right: dict[str, object] = {'a': 1, 'b': 'x'}
    out = diff_resolved_configs(left, right)
    assert out == []


def test_diff_resolved_configs_empty_dicts() -> None:
    left: dict[str, object] = {}
    right: dict[str, object] = {}
    out = diff_resolved_configs(left, right)
    assert out == []


def test_diff_resolved_configs_one_key_added() -> None:
    left: dict[str, object] = {'a': 1}
    right: dict[str, object] = {'a': 1, 'b': 2}
    out = diff_resolved_configs(left, right)
    assert len(out) == 1
    assert out[0]['path'] == 'b'
    assert out[0]['kind'] == 'added'
    assert out[0].get('right') == 2


def test_diff_resolved_configs_one_key_removed() -> None:
    left: dict[str, object] = {'a': 1, 'b': 2}
    right: dict[str, object] = {'a': 1}
    out = diff_resolved_configs(left, right)
    assert len(out) == 1
    assert out[0]['path'] == 'b'
    assert out[0]['kind'] == 'removed'
    assert out[0].get('left') == 2


def test_diff_resolved_configs_one_key_changed() -> None:
    left: dict[str, object] = {'a': 1, 'b': 10}
    right: dict[str, object] = {'a': 1, 'b': 20}
    out = diff_resolved_configs(left, right)
    assert len(out) == 1
    assert out[0]['path'] == 'b'
    assert out[0]['kind'] == 'changed'
    assert out[0].get('left') == 10
    assert out[0].get('right') == 20


def test_diff_resolved_configs_nested_dict() -> None:
    left = cast(dict[str, object], {'x': {'a': 1, 'b': 2}})
    right = cast(dict[str, object], {'x': {'a': 1, 'b': 99}})
    out = diff_resolved_configs(left, right)
    assert len(out) == 1
    assert out[0]['path'] == 'x.b'
    assert out[0]['kind'] == 'changed'


def test_diff_resolved_configs_list_set_like_same() -> None:
    left = cast(dict[str, object], {'k': [1, 2, 3]})
    right = cast(dict[str, object], {'k': [3, 2, 1]})
    out = diff_resolved_configs(left, right)
    assert out == []


def test_diff_resolved_configs_list_set_like_different() -> None:
    left = cast(dict[str, object], {'k': [1, 2]})
    right = cast(dict[str, object], {'k': [1, 3]})
    out = diff_resolved_configs(left, right)
    assert len(out) == 1
    assert out[0]['path'] == 'k'
    assert out[0]['kind'] == 'changed'


def test_diff_resolved_configs_ignore_underscore_prefix_keys() -> None:
    """Keys starting with '_' are ignored and not reported in the diff."""
    left = cast(dict[str, object], {'a': 1, '_internal': 'x'})
    right = cast(dict[str, object], {'a': 1, '_internal': 'y'})
    out = diff_resolved_configs(left, right)
    assert out == []
    # Also: added/removed underscore keys are ignored
    left2 = cast(dict[str, object], {'a': 1})
    right2 = cast(dict[str, object], {'a': 1, '_skip': 99})
    out2 = diff_resolved_configs(left2, right2)
    assert out2 == []


def test_collect_activity_to_sector_mapping_names_empty() -> None:
    config: dict[str, object] = {}
    assert collect_activity_to_sector_mapping_names(config) == set()


def test_collect_activity_to_sector_mapping_names_no_source_names() -> None:
    config: dict[str, object] = {'other_key': 'x'}
    assert collect_activity_to_sector_mapping_names(config) == set()


def test_collect_activity_to_sector_mapping_names_top_level() -> None:
    config = cast(
        dict[str, object],
        {
            'source_names': {
                'Src1': {'activity_to_sector_mapping': 'CEDA_2025'},
                'Src2': {'activity_to_sector_mapping': 'EPA_GHGI_CEDA'},
            },
        },
    )
    out = collect_activity_to_sector_mapping_names(config)
    assert out == {'CEDA_2025', 'EPA_GHGI_CEDA'}


def test_collect_activity_to_sector_mapping_names_nested_source_names() -> None:
    config = cast(
        dict[str, object],
        {
            'source_names': {
                'Outer': {
                    'activity_to_sector_mapping': 'OuterMap',
                    'source_names': {
                        'Inner': {'activity_to_sector_mapping': 'InnerMap'},
                    },
                },
            },
        },
    )
    out = collect_activity_to_sector_mapping_names(config)
    assert out == {'OuterMap', 'InnerMap'}


def test_diff_mapping_file_contents_report_shape() -> None:
    """Report has mapping_name, path, summary; missing CSV yields 'missing file'."""
    # Use a mapping name that does not exist under crosswalkpath
    config_baseline = cast(
        dict[str, object],
        {
            'source_names': {
                'X': {'activity_to_sector_mapping': 'NonexistentMappingName_NoCsv'},
            },
        },
    )
    config_test = cast(
        dict[str, object],
        {
            'source_names': {
                'X': {'activity_to_sector_mapping': 'NonexistentMappingName_NoCsv'},
            },
        },
    )
    report = diff_mapping_file_contents(config_baseline, config_test)
    assert len(report) >= 1
    for r in report:
        assert 'mapping_name' in r
        assert 'path' in r
        assert 'summary' in r
    # At least one entry should be missing file (our nonexistent mapping)
    summaries = [r['summary'] for r in report]
    assert 'missing file' in summaries or any('missing' in s.lower() for s in summaries)


def test_diff_mapping_file_contents_only_in_baseline() -> None:
    config_baseline = cast(
        dict[str, object],
        {'source_names': {'A': {'activity_to_sector_mapping': 'MapA'}}},
    )
    config_test: dict[str, object] = {}
    report = diff_mapping_file_contents(config_baseline, config_test)
    assert any(
        r['path'] == 'only in baseline' and r['mapping_name'] == 'MapA' for r in report
    )


def test_diff_mapping_file_contents_only_in_test() -> None:
    config_baseline: dict[str, object] = {}
    config_test = cast(
        dict[str, object],
        {'source_names': {'B': {'activity_to_sector_mapping': 'MapB'}}},
    )
    report = diff_mapping_file_contents(config_baseline, config_test)
    assert any(
        r['path'] == 'only in test' and r['mapping_name'] == 'MapB' for r in report
    )


# --- _list_diff_summary -------------------------------------------------------


def test_list_diff_summary_empty_both() -> None:
    summary = _list_diff_summary([], [])
    assert summary['only_in_baseline'] == []
    assert summary['only_in_test'] == []


def test_list_diff_summary_only_in_baseline() -> None:
    summary = _list_diff_summary(['a', 'b', 'c'], ['a', 'b'])
    assert summary['only_in_baseline'] == ['c']
    assert summary['only_in_test'] == []


def test_list_diff_summary_only_in_test() -> None:
    summary = _list_diff_summary(['a', 'b'], ['a', 'b', 'x'])
    assert summary['only_in_baseline'] == []
    assert summary['only_in_test'] == ['x']


def test_list_diff_summary_both_sides() -> None:
    summary = _list_diff_summary(
        ['removed_only', 'common'],
        ['common', 'added_only'],
    )
    assert summary['only_in_baseline'] == ['removed_only']
    assert summary['only_in_test'] == ['added_only']


def test_list_diff_summary_set_like_order_independent() -> None:
    summary = _list_diff_summary([1, 2, 3], [3, 2, 1])
    assert summary['only_in_baseline'] == []
    assert summary['only_in_test'] == []


# --- _parse_output_path / _default_output_path -------------------------------


def test_parse_output_path_with_path() -> None:
    argv, path, requested = _parse_output_path(
        ['base', 'test', '--output', 'out.yaml']
    )
    assert argv == ['base', 'test']
    assert path == 'out.yaml'
    assert requested is True


def test_parse_output_path_without_path() -> None:
    argv, path, requested = _parse_output_path(['base', 'test', '--output'])
    assert argv == ['base', 'test']
    assert path is None
    assert requested is True


def test_parse_output_path_short_flag() -> None:
    argv, path, requested = _parse_output_path(['base', 'test', '-o', 'diffs.yaml'])
    assert path == 'diffs.yaml'
    assert requested is True


def test_default_output_path() -> None:
    out = _default_output_path('CEDA_2023_national', 'GHG_national_m1')
    assert out == 'CEDA_2023_national_vs_GHG_national_m1_diffs.yaml'


def test_default_output_path_sanitize() -> None:
    out = _default_output_path('a/b', 'c\\d')
    assert '/' not in out and '\\' not in out
    assert 'a_b' in out and 'c_d' in out


# --- YAML output and list_summary / callables ---------------------------------


def test_to_yaml_safe_callable() -> None:
    assert _to_yaml_safe(lambda x: x) == '(callable)'


def test_to_yaml_safe_nested_callable() -> None:
    data = {'a': 1, 'b': lambda: None}
    out = _to_yaml_safe(data)
    assert out == {'a': 1, 'b': '(callable)'}


def test_yaml_output_list_summary_path() -> None:
    """YAML output for a list change uses list_summary and no raw left/right."""
    config_diff_for_yaml = [
        {
            'path': 'source_names.X.activity_sets.direct.PrimaryActivity',
            'kind': 'changed',
            'left': ['only_baseline', 'common'],
            'right': ['common', 'only_test'],
        },
    ]
    entry = dict(config_diff_for_yaml[0])
    left_val = entry.get('left')
    right_val = entry.get('right')
    assert isinstance(left_val, list) and isinstance(right_val, list)
    entry['list_summary'] = _list_diff_summary(left_val, right_val)
    entry.pop('left', None)
    entry.pop('right', None)

    data = {
        'baseline_method': 'Base',
        'test_method': 'Test',
        'config_diff': [entry],
        'mapping_diff': [],
    }
    safe = _to_yaml_safe(data)
    with tempfile.NamedTemporaryFile(
        mode='w', suffix='.yaml', delete=False
    ) as f:
        yaml.dump(
            safe,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        path = f.name
    try:
        with open(path, encoding='utf-8') as f:
            loaded = yaml.safe_load(f)
        assert loaded['baseline_method'] == 'Base'
        assert loaded['test_method'] == 'Test'
        assert len(loaded['config_diff']) == 1
        diff_entry = loaded['config_diff'][0]
        assert diff_entry['path'] == 'source_names.X.activity_sets.direct.PrimaryActivity'
        assert diff_entry['kind'] == 'changed'
        assert 'list_summary' in diff_entry
        assert diff_entry['list_summary']['only_in_baseline'] == ['only_baseline']
        assert diff_entry['list_summary']['only_in_test'] == ['only_test']
        assert 'left' not in diff_entry
        assert 'right' not in diff_entry
    finally:
        Path(path).unlink(missing_ok=True)
