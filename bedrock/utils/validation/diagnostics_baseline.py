"""Resolve diagnostics ``--baseline`` into config overrides.

Named targets: ``ceda-v0``, ``useeio``, ``v0.3``. Any snapshot key allowed on
``USAConfig.snapshot_version_or_git_sha`` also works (older or future
releases). Add a short alias to ``NAMED_BASELINES`` when a new release
becomes a common comparison target.
"""

from __future__ import annotations

import typing as ta
from pathlib import Path

from bedrock.utils.config.usa_config import USAConfig
from bedrock.utils.snapshots import releases
from bedrock.utils.validation.useeio_excel_baseline import (
    load_useeio_baseline_pin_overrides,
)

DEFAULT_USEEIO_BASELINE_PIN_JSON = str(
    Path(__file__).resolve().parent.parent / 'snapshots' / 'useeio_baseline_pin.json'
)

# Short operator names → snapshot key. ``useeio`` is handled separately.
NAMED_BASELINES: dict[str, str] = {
    'ceda-v0': releases.v0,
    'ceda': releases.v0,
    'v0': releases.v0,
    'v0.3': releases.v0_3_0,
    'v0.3.0': releases.v0_3_0,
}


def baseline_cli_overrides(
    baseline: str,
    *,
    useeio_pin_json: str | None = None,
) -> dict[str, object]:
    """Build ``diagnostics_cli_overrides`` for ``--baseline``."""
    key = baseline.strip()
    if not key:
        raise ValueError('baseline must be a non-empty string')

    if key.lower() == 'useeio':
        pin_path = useeio_pin_json or DEFAULT_USEEIO_BASELINE_PIN_JSON
        overrides: dict[str, object] = dict(
            load_useeio_baseline_pin_overrides(pin_path)
        )
        overrides['diagnostics_baseline_source'] = 'gcs_useeio_xlsx'
        return overrides

    snap = NAMED_BASELINES.get(key)
    if snap is None:
        allowed = set(
            ta.get_args(
                USAConfig.model_fields['snapshot_version_or_git_sha'].annotation
            )
        )
        if key not in allowed:
            named = ', '.join(['useeio', *sorted(NAMED_BASELINES)])
            raise ValueError(
                f'Unknown diagnostics baseline {key!r}. '
                f'Use one of: {named}; or an allowed snapshot SHA.'
            )
        snap = key

    return {
        'diagnostics_baseline_source': 'gcs_snapshot',
        'snapshot_version_or_git_sha': snap,
    }
