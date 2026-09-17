"""Config-ladder registry for electricity diagnostics."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Literal

Terminal = Literal['mixed_units', 'reaggregation']


@dataclass(frozen=True)
class LadderSpec:
    id: str
    steps: tuple[tuple[str, str], ...]
    terminal: Terminal
    plain_baseline: str | None = None
    production_baseline: str | None = None
    nowcast_mut_vintage: str | None = None
    snapshot_sha: str | None = None

    def config_for_step(self, step_id: str) -> str:
        for sid, config in self.steps:
            if sid == step_id:
                return config
        available = ', '.join(self.step_ids)
        raise ValueError(
            f"ladder {self.id!r} has no step {step_id!r}; available: {available}"
        )

    @property
    def step_ids(self) -> tuple[str, ...]:
        return tuple(sid for sid, _ in self.steps)

    @property
    def terminal_config(self) -> str:
        return self.config_for_step(self.terminal)


_REGISTRY: dict[str, LadderSpec] = {}


def register(ladder: LadderSpec) -> LadderSpec:
    if ladder.id in _REGISTRY:
        raise ValueError(f'duplicate ladder id: {ladder.id!r}')
    _REGISTRY[ladder.id] = ladder
    return ladder


def get_ladder(ladder_id: str) -> LadderSpec:
    if ladder_id not in _REGISTRY:
        available = ', '.join(list_ladders()) or '(none)'
        raise ValueError(f'unknown ladder {ladder_id!r}; available: {available}')
    return _REGISTRY[ladder_id]


def list_ladders() -> list[str]:
    return sorted(_REGISTRY)


def add_ladder_arg(
    parser: argparse.ArgumentParser,
    *,
    default: str = 'bea_v03_mixed_units',
) -> None:
    parser.add_argument(
        '--ladder',
        default=default,
        metavar='ID',
        help=f'Config ladder id (default: {default}).',
    )


def resolve_ladder(args: argparse.Namespace) -> LadderSpec:
    try:
        return get_ladder(args.ladder)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
