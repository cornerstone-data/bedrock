"""Load published class-MWh / D / N grids from the original-vs-EIA PPTX extract."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

import yaml

from bedrock.analysis.electricity.historical.original_vs_eia_anchored_deck.paths import (
    TABLES_YAML,
)

PUBLISHED_IMPLS = frozenset({'original', 'eia_gtd'})


@lru_cache(maxsize=1)
def load_tables() -> dict[str, Any]:
    payload = yaml.safe_load(TABLES_YAML.read_text(encoding='utf-8'))
    if not isinstance(payload, dict) or 'implementations' not in payload:
        raise ValueError(f'{TABLES_YAML} missing implementations')
    return payload


def impl_tables(impl_id: str) -> dict[str, Any]:
    implementations = load_tables()['implementations']
    if impl_id not in implementations:
        raise KeyError(f'no published tables for {impl_id!r}')
    return implementations[impl_id]


def published_ef(
    impl_id: str,
    kind: str,
    sector: str,
    step_id: str,
) -> float | None:
    """Return the published kg/USD value, or ``None`` when the PPTX cell is N/A."""
    block = impl_tables(impl_id)[kind]
    if sector not in block:
        return None
    raw = block[sector].get(step_id)
    if raw is None:
        return None
    return float(raw)


def published_class_mwh_rows(impl_id: str) -> list[tuple[float, float, str]]:
    """``(model_mwh, target_mwh, label)`` rows including Total."""
    rows = []
    for row in impl_tables(impl_id)['class_mwh']:
        rows.append(
            (float(row['model_mwh']), float(row['target_mwh']), str(row['label']))
        )
    return rows
