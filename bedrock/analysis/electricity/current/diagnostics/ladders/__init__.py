"""Electricity diagnostics config ladders."""

from __future__ import annotations

from bedrock.analysis.electricity.current.diagnostics.ladders import (  # noqa: F401
    bea_v03_mixed_units,
    bea_v03_reaggregation,
    nowcast_2024_reaggregation,
)
from bedrock.analysis.electricity.current.diagnostics.ladders.registry import (
    LadderSpec,
    add_ladder_arg,
    get_ladder,
    list_ladders,
    register,
    resolve_ladder,
)

__all__ = [
    'LadderSpec',
    'add_ladder_arg',
    'get_ladder',
    'list_ladders',
    'register',
    'resolve_ladder',
]
