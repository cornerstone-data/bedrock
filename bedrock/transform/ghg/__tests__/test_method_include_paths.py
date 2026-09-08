"""Cross-file ``!include:`` references into the shared target resolve.

Method YAMLs inherit most structure, but an overlay that *adds* to a shared
block has to restate the path to it. Those restated paths are the one part
inheritance cannot keep in sync: when ``Cornerstone_2025_target.yaml`` moved
the NAICS lists under ``industry_spec.naics``, every method that restated
``industry_spec:NAICS_6`` was migrated except one, which went unnoticed
because a dangling ``!include:`` path is not a syntax error.

Scoped to ``Cornerstone_2025_target.yaml`` because it is plain YAML with no
includes of its own, so a reference into it can be resolved exactly. Paths into
method files cannot -- those keys may be inherited rather than present.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

GHG_DIR = Path(__file__).resolve().parents[1]
TARGET_NAME = 'Cornerstone_2025_target.yaml'
TARGET_PATH = GHG_DIR.parent / 'common' / TARGET_NAME

#: ``!include:<file>.yaml`` optionally followed by ``:key`` segments.
_INCLUDE = re.compile(r'!include:([A-Za-z0-9_.\-]+\.yaml)((?::[A-Za-z0-9_]+)*)')


def _target() -> dict[str, Any]:
    return yaml.safe_load(TARGET_PATH.read_text(encoding='utf-8'))


def _references() -> list[tuple[str, str]]:
    """``(method filename, colon-joined key path)`` for refs into the target."""
    found: list[tuple[str, str]] = []
    for path in sorted(GHG_DIR.glob('*.yaml')):
        for line in path.read_text(encoding='utf-8').splitlines():
            if line.lstrip().startswith('#'):
                continue
            for filename, keys in _INCLUDE.findall(line):
                if filename == TARGET_NAME and keys:
                    found.append((path.name, keys.lstrip(':')))
    return found


def test_target_references_exist() -> None:
    target = _target()
    missing: list[str] = []
    for method, key_path in _references():
        node: Any = target
        for key in key_path.split(':'):
            if not isinstance(node, dict) or key not in node:
                missing.append(f'{method}: {TARGET_NAME}:{key_path}')
                break
            node = node[key]
    assert not missing, (
        'dangling !include paths into '
        + TARGET_NAME
        + ':\n'
        + '\n'.join(sorted(set(missing)))
    )


def test_references_were_found() -> None:
    """Guard the regex: a silent zero-match would make the check vacuous."""
    if not _references():
        pytest.fail(f'no !include references into {TARGET_NAME} were parsed')
