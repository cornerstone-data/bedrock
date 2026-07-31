"""q-weighted average N for one config — waterfall regression helper.

The v0→v0.3 release waterfall's bedrock-side steps toggle the bucket flags
incrementally (GHG model allocation → waste disaggregation → IO year
adjustments → US data update). Each step's level is the q-weighted average
total emission factor

    wavg N = Σᵢ Nᵢ qᵢ / Σᵢ qᵢ,   N = 1ᵀ M,  M = B L

with each step weighted by its own run's gross-output vector q (the release
waterfall convention: every bar is a true quantity of that state). This
module computes that level for a single config in a fresh process, so
successive steps cannot leak ``functools.cache`` state into each other.

CLI (used by ``test_waterfall_progression`` via subprocess):

    uv run python -m bedrock.utils.validation.waterfall_progression <config_name>
    uv run python -m bedrock.utils.validation.waterfall_progression --v0-baseline

Prints a JSON object ``{"weighted_avg_n_kg_per_usd": <float>}`` (kgCO2e per
USD of gross output, in that run's ``model_base_year`` dollars) on stdout.
"""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd


def _weighted_avg_n(
    B: pd.DataFrame,
    Adom: pd.DataFrame,
    Aimp: pd.DataFrame,
    q: 'pd.Series[float]',
) -> float:
    """Σ(N·q)/Σq with N = 1ᵀ B L, L = (I − (Adom+Aimp))⁻¹."""
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
    )

    N = compute_n(M=compute_M_matrix(B=B, L=compute_L_matrix(A=Adom + Aimp)))
    q_aligned = q.reindex(N.index).astype(float)
    return float((N * q_aligned).sum() / q_aligned.sum())


def weighted_avg_n_for_config(config_name: str) -> float:
    """q-weighted average N (kgCO2e/USD) for *config_name*, derived live."""
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
    )

    aq = derive_Aq_usa()
    return _weighted_avg_n(
        B=derive_B_usa_non_finetuned(),
        Adom=aq.Adom,
        Aimp=aq.Aimp,
        q=aq.scaled_q,
    )


def weighted_avg_n_v0_baseline() -> float:
    """q-weighted average N recomputed from the frozen CEDA v0 snapshots."""
    from bedrock.utils.snapshots.loader import load_snapshot  # noqa: PLC0415

    q_raw = load_snapshot('scaled_q_USA', 'v0')
    q: pd.Series = q_raw.iloc[:, 0] if isinstance(q_raw, pd.DataFrame) else q_raw
    return _weighted_avg_n(
        B=load_snapshot('B_USA_non_finetuned', 'v0'),
        Adom=load_snapshot('Adom_USA', 'v0'),
        Aimp=load_snapshot('Aimp_USA', 'v0'),
        q=q.astype(float),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('config_name', nargs='?', default=None)
    group.add_argument(
        '--v0-baseline',
        action='store_true',
        help='weighted average N from the frozen CEDA v0 snapshots instead of a live config',
    )
    args = parser.parse_args(argv)

    level = (
        weighted_avg_n_v0_baseline()
        if args.v0_baseline
        else weighted_avg_n_for_config(args.config_name)
    )
    json.dump({'weighted_avg_n_kg_per_usd': level}, sys.stdout)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
