"""Total attributed emissions (ΣBLy) for one config — waterfall regression helper.

The v0→v0.3 release waterfall's bedrock-side steps toggle the bucket flags
incrementally (GHG model allocation → waste disaggregation → IO year
adjustments → US data update). Each step's bar is the change in total
attributed emissions ΣBLy = Σ diag(d) L y. This module computes that total
for a single config in a fresh process, so successive steps cannot leak
``functools.cache`` state into each other.

CLI (used by ``test_waterfall_progression`` via subprocess):

    uv run python -m bedrock.utils.validation.waterfall_progression <config_name>
    uv run python -m bedrock.utils.validation.waterfall_progression --v0-baseline

Prints a JSON object ``{"bly_total_mt": <float>}`` (MtCO2e) on stdout.
"""

from __future__ import annotations

import argparse
import json
import sys

KG_PER_MT = 1e9


def bly_total_mt_for_config(config_name: str) -> float:
    """ΣBLy (MtCO2e) for *config_name*, derived live."""
    import bedrock.utils.config.common as common  # noqa: PLC0415
    from bedrock.utils.config.usa_config import (  # noqa: PLC0415
        reset_usa_config,
        set_global_usa_config,
    )

    common.download_fba_on_api_error = True
    # This runs in a fresh interpreter, but the parent test process exports
    # USA_CONFIG_FILE into the inherited environment; clear it so the step
    # uses exactly the requested config.
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)

    # Late-binding imports — depend on the global config.
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
        derive_y_for_national_accounting_balance_usa,
    )
    from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (  # noqa: PLC0415
        _compute_bly_series,
    )

    bly = _compute_bly_series(
        B=derive_B_usa_non_finetuned(),
        Adom=derive_Aq_usa().Adom,
        y=derive_y_for_national_accounting_balance_usa(),
    )
    return float(bly.sum()) / KG_PER_MT


def bly_total_mt_v0_baseline() -> float:
    """ΣBLy (MtCO2e) recomputed from the frozen CEDA v0 snapshots."""
    import pandas as pd  # noqa: PLC0415

    from bedrock.utils.snapshots.loader import load_snapshot  # noqa: PLC0415
    from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (  # noqa: PLC0415
        _compute_bly_series,
    )

    y_raw = load_snapshot('y_nab_USA', 'v0')
    y_old: pd.Series = y_raw.iloc[:, 0] if isinstance(y_raw, pd.DataFrame) else y_raw
    bly = _compute_bly_series(
        B=load_snapshot('B_USA_non_finetuned', 'v0'),
        Adom=load_snapshot('Adom_USA', 'v0'),
        y=y_old.astype(float),
    )
    return float(bly.sum()) / KG_PER_MT


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('config_name', nargs='?', default=None)
    group.add_argument(
        '--v0-baseline',
        action='store_true',
        help='ΣBLy from the frozen CEDA v0 snapshots instead of a live config',
    )
    args = parser.parse_args(argv)

    total = (
        bly_total_mt_v0_baseline()
        if args.v0_baseline
        else bly_total_mt_for_config(args.config_name)
    )
    json.dump({'bly_total_mt': total}, sys.stdout)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
