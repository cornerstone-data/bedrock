"""Manifest loader and config_summary validation for diagnostics runs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from bedrock.analysis.electricity_disagg_diagnostics.paths import (
    MANIFEST_PATH,
    V02_SNAPSHOT_SHA,
)
from bedrock.utils.validation.analysis.combine_ef_diagnostics import (
    _read_config_summary_map,
)
from bedrock.utils.validation.analysis.fetch import load_tab

PLACEHOLDER_PREFIX = '<'


@dataclass(frozen=True)
class FootingSpec:
    label: str
    sheet_id: str
    config: str


@dataclass(frozen=True)
class StepSpec:
    label: str
    sheet_id: str
    config: str


@dataclass(frozen=True)
class FinalSpec:
    label: str
    sheet_id: str
    config: str


@dataclass(frozen=True)
class Manifest:
    meta: dict[str, str]
    footing: FootingSpec
    steps: list[StepSpec]
    final: FinalSpec


@dataclass(frozen=True)
class RunExpectation:
    config_name: str
    implement_electricity_reallocation: bool | None
    implement_electricity_disaggregation: bool | None
    implement_electricity_mixed_units: bool | None
    snapshot_sha: str | None


def _coerce_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {'true', '1'}


def _expect_flag(
    summary: dict[str, object],
    field: str,
    expected: bool | None,
) -> None:
    actual = _coerce_bool(summary.get(field))
    if expected is None:
        if actual:
            raise ValueError(
                f'{field} must be absent or false, got {summary.get(field)!r}'
            )
    elif actual is not expected:
        raise ValueError(f'{field} expected {expected}, got {summary.get(field)!r}')


def load_manifest(path: Path | None = None) -> Manifest:
    manifest_path = path or MANIFEST_PATH
    raw = yaml.safe_load(manifest_path.read_text(encoding='utf-8'))
    footing = FootingSpec(**raw['footing'])
    steps = [StepSpec(**step) for step in raw['steps']]
    final = FinalSpec(**raw['final'])
    meta = {str(k): str(v) for k, v in (raw.get('meta') or {}).items()}
    manifest = Manifest(meta=meta, footing=footing, steps=steps, final=final)
    _validate_sheet_ids(manifest)
    return manifest


def _validate_sheet_ids(manifest: Manifest) -> None:
    for label, sid in (
        ('footing', manifest.footing.sheet_id),
        *((f'step:{s.label}', s.sheet_id) for s in manifest.steps),
        ('final', manifest.final.sheet_id),
    ):
        if not sid or sid.startswith(PLACEHOLDER_PREFIX):
            raise ValueError(
                f'Manifest sheet_id for {label!r} is unset ({sid!r}). '
                'Run generate_diagnostics for each config and update manifest.yaml.'
            )


def validate_run_config(
    sheet_id: str,
    expected: RunExpectation,
    *,
    refresh: bool = False,
) -> None:
    summary = _read_config_summary_map(sheet_id, expected.config_name, refresh=refresh)
    actual_name = str(summary.get('config_name', '')).strip()
    if actual_name != expected.config_name:
        raise ValueError(
            f'config_name mismatch for {sheet_id}: expected {expected.config_name!r}, '
            f'got {actual_name!r}'
        )
    _expect_flag(
        summary,
        'implement_electricity_reallocation',
        expected.implement_electricity_reallocation,
    )
    _expect_flag(
        summary,
        'implement_electricity_disaggregation',
        expected.implement_electricity_disaggregation,
    )
    _expect_flag(
        summary,
        'implement_electricity_mixed_units',
        expected.implement_electricity_mixed_units,
    )
    if expected.snapshot_sha is not None:
        sha = str(summary.get('snapshot_version_or_git_sha', '')).strip()
        if sha != expected.snapshot_sha:
            raise ValueError(
                f'snapshot_version_or_git_sha expected {expected.snapshot_sha!r}, got {sha!r}'
            )
        baseline = str(summary.get('diagnostics_baseline_source', '')).strip()
        if baseline != 'gcs_snapshot':
            raise ValueError(
                f'diagnostics_baseline_source expected gcs_snapshot, got {baseline!r}'
            )
    load_tab(sheet_id, 'BLy_new_vs_BLy_old', refresh=refresh)


def expectations_for_manifest(manifest: Manifest) -> list[tuple[str, RunExpectation]]:
    footing = RunExpectation(
        config_name=manifest.footing.config,
        implement_electricity_reallocation=None,
        implement_electricity_disaggregation=None,
        implement_electricity_mixed_units=None,
        snapshot_sha=None,
    )
    out: list[tuple[str, RunExpectation]] = [
        (manifest.footing.sheet_id, footing),
    ]
    step_flags: list[tuple[bool, bool, bool]] = [
        (True, False, False),
        (True, True, False),
        (True, True, True),
    ]
    for step, flags in zip(manifest.steps, step_flags, strict=True):
        out.append(
            (
                step.sheet_id,
                RunExpectation(
                    config_name=step.config,
                    implement_electricity_reallocation=flags[0],
                    implement_electricity_disaggregation=flags[1],
                    implement_electricity_mixed_units=flags[2],
                    snapshot_sha=V02_SNAPSHOT_SHA,
                ),
            )
        )
    if manifest.final.sheet_id != manifest.steps[-1].sheet_id:
        out.append(
            (
                manifest.final.sheet_id,
                RunExpectation(
                    config_name=manifest.final.config,
                    implement_electricity_reallocation=True,
                    implement_electricity_disaggregation=True,
                    implement_electricity_mixed_units=True,
                    snapshot_sha=V02_SNAPSHOT_SHA,
                ),
            )
        )
    return out


def validate_manifest(manifest: Manifest, *, refresh: bool = False) -> None:
    for sheet_id, expected in expectations_for_manifest(manifest):
        validate_run_config(sheet_id, expected, refresh=refresh)
