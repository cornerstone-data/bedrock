"""
Utilities to diff resolved FBS method configs and activity-to-sector mapping file contents.
Used for atomic FBS change testing: verify only intended config/mapping changes, then compare FBS output.

Workflow: (1) Config diff → (2) Mapping diff (optional, --mapping) → (3) FBS generate + compare_FBS.
Steps 1–2 are run via this module (CLI). Step 3 is user-driven (generate both methods, then
compare_FBS(baseline_fbs, test_fbs) from bedrock.utils.validation.validation).
"""

from __future__ import annotations

import json
import sys
from typing import Literal, NotRequired, TypedDict, cast

import pandas as pd
import yaml

from bedrock.utils.config.common import get_flowsa_base_name, load_yaml_dict
from bedrock.utils.config.settings import crosswalkpath, transformpath
from bedrock.utils.validation.exceptions import FlowsaMethodNotFoundError

# --- Config diff types (no Any/Unknown) -------------------------------------

# Values that we compare and store in diff entries (JSON-like; callables are skipped)
DiffableValue = dict[str, object] | list[object] | str | int | float | bool | None


class ConfigDiffEntry(TypedDict):
    path: str
    kind: Literal['added', 'removed', 'changed']
    left: NotRequired[DiffableValue]
    right: NotRequired[DiffableValue]


def _is_callable_or_non_diffable(val: object) -> bool:
    """True if we skip this value (callable or not comparable)."""
    return callable(val)


def _item_canonical(item: object) -> str:
    """Canonical string for one list item (for set membership)."""
    if isinstance(item, (dict, list)):
        try:
            return json.dumps(item, sort_keys=True, default=str)
        except (TypeError, ValueError):
            return repr(item)
    return repr(item)


def _list_as_set_canonical(lst: list[object]) -> frozenset[str]:
    """Normalize list to a comparable set (for set-like list comparison)."""
    return frozenset(_item_canonical(item) for item in lst)


def _lists_set_equal(left: list[object], right: list[object]) -> bool:
    """Set-like list comparison (order-independent)."""
    return _list_as_set_canonical(left) == _list_as_set_canonical(right)


def _diff_resolved_configs_rec(
    left: object,
    right: object,
    path_prefix: str,
    out: list[ConfigDiffEntry],
) -> None:
    """Recursive deep walk; appends to out."""
    if _is_callable_or_non_diffable(left) and _is_callable_or_non_diffable(right):
        return
    if _is_callable_or_non_diffable(left) or _is_callable_or_non_diffable(right):
        # One is callable, treat as same if key present (do not report)
        return

    if isinstance(left, dict) and isinstance(right, dict):
        all_keys = set(left) | set(right)
        for key in sorted(all_keys):
            if key.startswith('_'):
                continue
            sub_path = f'{path_prefix}.{key}' if path_prefix else key
            if key not in left:
                out.append(
                    {
                        'path': sub_path,
                        'kind': 'added',
                        'right': right[key]
                        if not _is_callable_or_non_diffable(right[key])
                        else None,
                    }
                )
            elif key not in right:
                out.append(
                    {
                        'path': sub_path,
                        'kind': 'removed',
                        'left': left[key]
                        if not _is_callable_or_non_diffable(left[key])
                        else None,
                    }
                )
            else:
                lv, rv = left[key], right[key]
                if _is_callable_or_non_diffable(lv) and _is_callable_or_non_diffable(
                    rv
                ):
                    continue
                if _is_callable_or_non_diffable(lv) or _is_callable_or_non_diffable(rv):
                    continue
                if isinstance(lv, dict) and isinstance(rv, dict):
                    _diff_resolved_configs_rec(lv, rv, sub_path, out)
                elif isinstance(lv, list) and isinstance(rv, list):
                    if not _lists_set_equal(lv, rv):
                        out.append(
                            {
                                'path': sub_path,
                                'kind': 'changed',
                                'left': lv,
                                'right': rv,
                            }
                        )
                else:
                    if lv != rv:
                        out.append(
                            {
                                'path': sub_path,
                                'kind': 'changed',
                                'left': lv,
                                'right': rv,
                            }
                        )
        return

    if isinstance(left, list) and isinstance(right, list):
        if not _lists_set_equal(left, right):
            out.append(
                {
                    'path': path_prefix or '.',
                    'kind': 'changed',
                    'left': left,
                    'right': right,
                }
            )
        return

    if left != right:
        out.append(
            {
                'path': path_prefix or '.',
                'kind': 'changed',
                'left': cast(DiffableValue, left),
                'right': cast(DiffableValue, right),
            }
        )


def diff_resolved_configs(
    left: dict[str, object],
    right: dict[str, object],
) -> list[ConfigDiffEntry]:
    """
    Deep diff of two resolved method configs. Returns path-style differences.
    Lists compared set-like (order-independent). Callables skipped.
    """
    out: list[ConfigDiffEntry] = []
    _diff_resolved_configs_rec(left, right, '', out)
    return out


# --- Mapping diff types and collection ---------------------------------------


class MappingDiffReportEntry(TypedDict):
    mapping_name: str
    path: str
    summary: str


def _collect_activity_to_sector_mapping_names_rec(
    config: object,
    seen: set[str],
) -> None:
    """Recursively collect activity_to_sector_mapping values under source_names."""
    if not isinstance(config, dict):
        return
    for key, val in config.items():
        if key == 'activity_to_sector_mapping' and isinstance(val, str):
            seen.add(val)
        if key == 'source_names' and isinstance(val, dict):
            for name, source_config in val.items():
                if isinstance(source_config, dict):
                    if 'activity_to_sector_mapping' in source_config:
                        v = source_config['activity_to_sector_mapping']
                        if isinstance(v, str):
                            seen.add(v)
                    _collect_activity_to_sector_mapping_names_rec(source_config, seen)


def collect_activity_to_sector_mapping_names(
    config: dict[str, object],
) -> set[str]:
    """Collect activity_to_sector_mapping names from resolved config (recursive under source_names)."""
    names: set[str] = set()
    _collect_activity_to_sector_mapping_names_rec(config, names)
    return names


def resolve_mapping_name_to_csv_path(mapping_name: str) -> str | None:
    """
    Resolve mapping name to CSV path under crosswalkpath (repo-only; no external paths).
    Returns path string if file exists, else None.
    """
    mapfn = f'NAICS_Crosswalk_{mapping_name}'
    base_name = get_flowsa_base_name(crosswalkpath, mapfn, 'csv')
    p = crosswalkpath / f'{base_name}.csv'
    return str(p) if p.is_file() else None


def diff_mapping_file_contents(
    config_baseline: dict[str, object],
    config_test: dict[str, object],
) -> list[MappingDiffReportEntry]:
    """
    Compare activity-to-sector mapping file contents used by baseline vs test config.
    Returns list of report entries: mapping_name, path, summary.
    """
    names_baseline = collect_activity_to_sector_mapping_names(config_baseline)
    names_test = collect_activity_to_sector_mapping_names(config_test)
    all_names = names_baseline | names_test
    report: list[MappingDiffReportEntry] = []

    for name in sorted(all_names):
        csv_path = resolve_mapping_name_to_csv_path(name)
        in_baseline = name in names_baseline
        in_test = name in names_test

        if not in_baseline:
            report.append(
                {
                    'mapping_name': name,
                    'path': 'only in test',
                    'summary': 'mapping only in test config',
                }
            )
            continue
        if not in_test:
            report.append(
                {
                    'mapping_name': name,
                    'path': 'only in baseline',
                    'summary': 'mapping only in baseline config',
                }
            )
            continue

        if csv_path is None:
            report.append(
                {
                    'mapping_name': name,
                    'path': '',
                    'summary': 'missing file',
                }
            )
            continue

        try:
            df = pd.read_csv(csv_path, dtype={'Activity': str, 'Sector': str})
        except OSError:
            report.append(
                {
                    'mapping_name': name,
                    'path': csv_path,
                    'summary': 'missing file',
                }
            )
            continue

        # Single CSV used by both; no second file to compare. Report row count.
        n = len(df)
        report.append(
            {
                'mapping_name': name,
                'path': csv_path,
                'summary': f'{n} rows (same mapping file for both)',
            }
        )

    return report


# --- CLI ---------------------------------------------------------------------


def _list_diff_summary(
    left: list[object], right: list[object]
) -> dict[str, list[object]]:
    """Set-like diff of two lists: items only in left (baseline), only in right (test)."""
    left_canon = _list_as_set_canonical(left)
    right_canon = _list_as_set_canonical(right)
    right_set = set(right_canon)
    left_set = set(left_canon)
    only_in_baseline = [x for x in left if _item_canonical(x) not in right_set]
    only_in_test = [x for x in right if _item_canonical(x) not in left_set]
    return {'only_in_baseline': only_in_baseline, 'only_in_test': only_in_test}


def _to_yaml_safe(obj: object) -> object:
    """Convert to YAML-serializable form; callables become placeholder."""
    if callable(obj):
        return '(callable)'
    if hasattr(obj, 'item') and callable(getattr(obj, 'item')):
        return obj.item()
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _to_yaml_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_yaml_safe(v) for v in obj]
    return str(obj)


def _format_config_diff_entries(entries: list[ConfigDiffEntry]) -> str:
    lines: list[str] = []
    for e in entries:
        path = e['path']
        kind = e['kind']
        line = f'  {path}  [{kind}]'
        if 'left' in e and e['left'] is not None:
            line += f'  left={e["left"]!r}'
        if 'right' in e and e['right'] is not None:
            line += f'  right={e["right"]!r}'
        lines.append(line)
    return '\n'.join(lines) if lines else '  (no differences)'


def _run_config_diff(
    baseline_method: str, test_method: str
) -> tuple[
    int,
    list[ConfigDiffEntry],
    dict[str, object] | None,
    dict[str, object] | None,
]:
    """
    Load both configs, run diff, print. Returns (exit_code, diffs, left, right).
    On failure returns (1, [], None, None).
    """
    try:
        config_baseline = load_yaml_dict(
            baseline_method, 'FBS', filepath=str(transformpath)
        )
    except FlowsaMethodNotFoundError as e:
        print(f'Error: {e}', file=sys.stderr)
        return (1, [], None, None)
    except Exception as e:
        print(f"Error loading baseline '{baseline_method}': {e}", file=sys.stderr)
        return (1, [], None, None)

    try:
        config_test = load_yaml_dict(test_method, 'FBS', filepath=str(transformpath))
    except FlowsaMethodNotFoundError as e:
        print(f'Error: {e}', file=sys.stderr)
        return (1, [], None, None)
    except Exception as e:
        print(f"Error loading test '{test_method}': {e}", file=sys.stderr)
        return (1, [], None, None)

    left = cast(dict[str, object], dict(config_baseline))
    right = cast(dict[str, object], dict(config_test))
    diffs = diff_resolved_configs(left, right)
    print('Config diff (baseline vs test):')
    print(_format_config_diff_entries(diffs))
    return (0, diffs, left, right)


def _parse_output_path(argv: list[str]) -> tuple[list[str], str | None, bool]:
    """
    Remove --output / -o and optional path from argv.
    Return (remaining_argv, output_path or None, output_requested).
    When output_requested and no path given, caller should use default path.
    """
    out: list[str] = []
    output_path: str | None = None
    output_requested = False
    i = 0
    while i < len(argv):
        if argv[i] in ('--output', '-o'):
            output_requested = True
            if i + 1 < len(argv) and not argv[i + 1].startswith('-'):
                output_path = argv[i + 1]
                i += 2
                continue
            i += 1
            continue
        out.append(argv[i])
        i += 1
    return (out, output_path, output_requested)


def _default_output_path(baseline_method: str, test_method: str) -> str:
    """Logical default YAML path from method names (safe for filesystem)."""
    base = f'{baseline_method}_vs_{test_method}_diffs.yaml'
    return base.replace('/', '_').replace('\\', '_')


def main() -> int:
    """CLI entry: diff_methods baseline_method test_method [--mapping] [--output FILE]"""
    argv = sys.argv[1:]
    argv, output_path, output_requested = _parse_output_path(argv)
    if not argv or len(argv) < 2:
        print(
            'Usage: python -m bedrock.utils.config.diff_methods <baseline_method> <test_method> [--mapping] [--output FILE]',
            file=sys.stderr,
        )
        return 1
    baseline_method = argv[0]
    test_method = argv[1]
    include_mapping = '--mapping' in argv

    if output_requested and output_path is None:
        output_path = _default_output_path(baseline_method, test_method)

    exit_code, diffs, left, right = _run_config_diff(baseline_method, test_method)
    if exit_code != 0:
        return exit_code

    mapping_reports: list[MappingDiffReportEntry] = []
    if include_mapping and left is not None and right is not None:
        mapping_reports = diff_mapping_file_contents(left, right)
        print('\nMapping file content diff:')
        for r in mapping_reports:
            print(
                f'  {r["mapping_name"]}: path={r["path"]!r}  summary={r["summary"]!r}'
            )

    if output_path:
        config_diff_for_yaml: list[dict[str, object]] = []
        for e in diffs:
            entry = dict(e)
            left_val = e.get('left')
            right_val = e.get('right')
            if (
                e.get('kind') == 'changed'
                and isinstance(left_val, list)
                and isinstance(right_val, list)
            ):
                entry['list_summary'] = _list_diff_summary(left_val, right_val)
                # Omit full lists to keep YAML compact; list_summary has the delta
                entry.pop('left', None)
                entry.pop('right', None)
            config_diff_for_yaml.append(entry)
        data: dict[str, object] = {
            'baseline_method': baseline_method,
            'test_method': test_method,
            'config_diff': config_diff_for_yaml,
            'mapping_diff': [dict(r) for r in mapping_reports],
        }
        safe_data = _to_yaml_safe(data)
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(
                safe_data,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        print(f'\nDiffs written to {output_path}', file=sys.stderr)

    return 0


if __name__ == '__main__':
    sys.exit(main())
