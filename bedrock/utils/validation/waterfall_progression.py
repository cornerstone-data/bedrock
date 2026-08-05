"""Live reproducibility of v0.3 waterfall configs (q-weighted N).

Rebuilds every ``v03_waterfall_*`` config on the USEEIO and CEDA group
registries and checks that live ``1ᵀ B L``, weighted by canonical v0.3
``scaled_q_USA``, matches the diagnostics sheet ``N_new`` for that config.

Out of scope (no live rebuild)
------------------------------
* Pinned USEEIO baseline (``N_old_inflated`` on the USEEIO G1 sheet)
* Pinned CEDA v0 baseline (frozen snapshots / Excel baseline)

Configs under test (unique ``config_name`` union of both registries)
--------------------------------------------------------------------
* ``v03_waterfall_useeio_g1_schema_ghg``
* ``v03_waterfall_ceda_g1a_schema_ghg``
* ``v03_waterfall_ceda_g1b_waste_disagg``
* ``v03_waterfall_g2_methods`` (shared)
* ``v03_waterfall_g3_data`` (shared)
* ``v03_waterfall_final`` (shared)

Expected floats are sheet ``N_new`` × canonical q (not assessment
``N_*_inflated`` columns). Recompute::

    uv run python -m bedrock.utils.validation.waterfall_progression --sheet-n-new

Optional: ``--assessment-useeio`` recomputes ceda assessment USEEIO-track bars
(pin / G1 inflated / G2 / G3) for plot alignment; that path does not rebuild.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass

import pandas as pd

from bedrock.utils.config.usa_config import CANONICAL_USA_CONFIG
from bedrock.utils.validation.analysis import (
    release_v0_v03_ceda_groups as ceda_groups,
)
from bedrock.utils.validation.analysis import (
    release_v0_v03_useeio_groups as useeio_groups,
)


@dataclass(frozen=True)
class AssessmentNLevel:
    """One q-weighted N bar on the assessment USEEIO→bedrock chain."""

    key: str
    sheet_id: str
    n_column: str
    config_name: str | None
    note: str


# Keys / sheet IDs must match ceda ``USEEIO_TRACK`` + ``ef_combos.US_VS_USEEIO``.
# Sheet objects: ``release_v0_v03_useeio_groups``.
ASSESSMENT_USEEIO_BEDROCK_LEVELS: tuple[AssessmentNLevel, ...] = (
    AssessmentNLevel(
        key='pinned_useeio_baseline',
        sheet_id='1AaMWSXaHfyTHWfdNvICQ13RDA-CXEQir5W77RcntQRE',
        n_column='N_old_inflated',
        config_name=None,
        note='USEEIO pin from G1 diagnostics (combine_ef pin_source=bedrock_us)',
    ),
    AssessmentNLevel(
        key='G1',
        sheet_id='1AaMWSXaHfyTHWfdNvICQ13RDA-CXEQir5W77RcntQRE',
        n_column='N_new_inflated',
        config_name='v03_waterfall_useeio_g1_schema_ghg',
        note='USEEIO_TRACK[ghg]; prefer N_new_inflated (2024$)',
    ),
    AssessmentNLevel(
        key='G2',
        sheet_id='1ooNKUDndc3mOdBVt0HBFzIk1SNA1OjGTfh-3uJq1iUc',
        n_column='N_new',
        config_name='v03_waterfall_g2_methods',
        note='USEEIO_TRACK[io]',
    ),
    AssessmentNLevel(
        key='G3',
        sheet_id='1LXt5cZTsXFKG6l09Hw56zkrxIhnnqwXDxMjMOERrE6c',
        n_column='N_new',
        config_name='v03_waterfall_g3_data',
        note='USEEIO_TRACK[us_data]; bedrock endpoint before MRIO steps',
    ),
)


def live_waterfall_configs() -> tuple[str, ...]:
    """Unique waterfall config names across USEEIO + CEDA group registries."""
    seen: set[str] = set()
    ordered: list[str] = []
    for sheet in (
        *useeio_groups.V0_V03_USEEIO_GROUP_SHEETS,
        *ceda_groups.V0_V03_CEDA_GROUP_SHEETS,
    ):
        if sheet.config_name not in seen:
            seen.add(sheet.config_name)
            ordered.append(sheet.config_name)
    return tuple(ordered)


def _primary_sheet_id_for_config(config_name: str) -> str:
    """Diagnostics sheet used to pin live ``N_new`` for *config_name*.

    Shared G2/G3/FINAL names prefer the USEEIO-track sheet (CEDA-track sheets
    for those configs carry the same ``N_new`` for the metric here).
    """
    for sheet in (
        *useeio_groups.V0_V03_USEEIO_GROUP_SHEETS,
        *ceda_groups.V0_V03_CEDA_GROUP_SHEETS,
    ):
        if sheet.config_name == config_name:
            return sheet.sheet_id
    raise KeyError(f'no waterfall ProgressionSheet for config {config_name!r}')


def _series_from_snapshot_frame(frame: pd.DataFrame | pd.Series) -> pd.Series[float]:
    if isinstance(frame, pd.Series):
        return frame.astype(float)
    squeezed = frame.squeeze()
    if isinstance(squeezed, pd.DataFrame):
        squeezed = squeezed.iloc[:, 0]
    return squeezed.astype(float)


def load_canonical_v0_3_q() -> pd.Series[float]:
    """Shipped v0.3 commodity ``q`` (``scaled_q_USA`` at ``.SNAPSHOT_KEY``)."""
    from bedrock.utils.snapshots.loader import load_current_snapshot  # noqa: PLC0415

    q = _series_from_snapshot_frame(load_current_snapshot('scaled_q_USA'))
    q.index = q.index.astype(str)
    return q


def weighted_avg_n_from_vectors(
    N: pd.Series[float],
    q: pd.Series[float],
) -> float:
    """Σ(N·q)/Σq on the intersection of N and q indices (finite, nonzero q)."""
    n = N.astype(float)
    n.index = n.index.astype(str)
    q_aligned = q.reindex(n.index).astype(float)
    mask = n.notna() & q_aligned.notna() & (q_aligned != 0)
    n = n.loc[mask]
    q_aligned = q_aligned.loc[mask]
    denom = float(q_aligned.sum())
    if denom == 0.0:
        raise ValueError('canonical q has zero sum on sectors overlapping N')
    return float((n * q_aligned).sum() / denom)


# Back-compat alias used by earlier call sites / scratch scripts.
_weighted_avg_n_from_vectors = weighted_avg_n_from_vectors


def _n_series_from_diagnostics_tab(
    sheet_id: str,
    n_column: str,
    *,
    refresh: bool = False,
) -> pd.Series[float]:
    """Load one N column from a diagnostics ``N_and_diffs`` tab."""
    from bedrock.utils.validation.analysis.fetch import load_tab  # noqa: PLC0415

    df = load_tab(sheet_id, 'N_and_diffs', refresh=refresh)
    sector_col = 'sector' if 'sector' in df.columns else 'index'
    if n_column not in df.columns:
        # Mirror ceda combine_ef._bedrock_value_col fallback.
        if n_column == 'N_new_inflated' and 'N_new' in df.columns:
            n_column = 'N_new'
        else:
            raise KeyError(
                f'sheet {sheet_id!r} N_and_diffs missing {n_column!r}; '
                f'columns={list(df.columns)}'
            )
    n = pd.to_numeric(df[n_column], errors='coerce')
    series = pd.Series(n.to_numpy(), index=df[sector_col].astype(str), dtype=float)
    return series


def sheet_n_new_levels(*, refresh: bool = False) -> dict[str, float]:
    """q-weighted sheet ``N_new`` for every live waterfall config."""
    q = load_canonical_v0_3_q()
    return {
        config_name: weighted_avg_n_from_vectors(
            _n_series_from_diagnostics_tab(
                _primary_sheet_id_for_config(config_name),
                'N_new',
                refresh=refresh,
            ),
            q,
        )
        for config_name in live_waterfall_configs()
    }


def weighted_avg_n_from_assessment_level(
    level: AssessmentNLevel,
    *,
    q: pd.Series[float] | None = None,
    refresh: bool = False,
) -> float:
    """q-weighted N for one assessment bar, from the pinned diagnostics sheet."""
    if q is None:
        q = load_canonical_v0_3_q()
    n = _n_series_from_diagnostics_tab(level.sheet_id, level.n_column, refresh=refresh)
    return weighted_avg_n_from_vectors(n, q)


def assessment_useeio_bedrock_levels(
    *,
    refresh: bool = False,
) -> dict[str, float]:
    """Recompute all bedrock-owned USEEIO-track assessment bars from sheets."""
    q = load_canonical_v0_3_q()
    return {
        level.key: weighted_avg_n_from_assessment_level(level, q=q, refresh=refresh)
        for level in ASSESSMENT_USEEIO_BEDROCK_LEVELS
    }


def assert_useeio_track_sheet_ids_match_registry() -> None:
    """Fail if ``ASSESSMENT_*`` sheet IDs drift from ``release_v0_v03_useeio_groups``."""
    from bedrock.utils.validation.analysis import (  # noqa: PLC0415
        release_v0_v03_useeio_groups as reg,
    )

    expected = {
        'G1': reg.G1_SCHEMA_GHG.sheet_id,
        'G2': reg.G2_METHODS.sheet_id,
        'G3': reg.G3_DATA.sheet_id,
    }
    by_key = {lvl.key: lvl.sheet_id for lvl in ASSESSMENT_USEEIO_BEDROCK_LEVELS}
    for key, sheet_id in expected.items():
        if by_key[key] != sheet_id:
            raise AssertionError(
                f'ASSESSMENT_USEEIO_BEDROCK_LEVELS[{key!r}].sheet_id='
                f'{by_key[key]!r} != release_v0_v03_useeio_groups {sheet_id!r} '
                '(ceda USEEIO_TRACK must stay aligned with the registry)'
            )
    pin = by_key['pinned_useeio_baseline']
    if pin != expected['G1']:
        raise AssertionError(
            'USEEIO pin sheet must be the G1 diagnostics sheet '
            f'(got pin={pin!r}, G1={expected["G1"]!r})'
        )


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
    return weighted_avg_n_from_vectors(N, q)


def weighted_avg_n_for_config(config_name: str) -> float:
    """Live ``1ᵀ B L`` for *config_name*, weighted by canonical v0.3 ``q``.

    Matches diagnostics sheet ``N_new`` for that config when the model is
    reproducible. Does not apply the diagnostics ``N_new_inflated`` PI rebase.
    """
    import bedrock.utils.config.common as common  # noqa: PLC0415
    from bedrock.utils.config.usa_config import (  # noqa: PLC0415
        reset_usa_config,
        set_global_usa_config,
    )

    common.download_fba_on_api_error = True
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)

    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )

    aq = derive_Aq_usa()
    return _weighted_avg_n(
        B=derive_B_usa_non_finetuned(),
        Adom=aq.Adom,
        Aimp=aq.Aimp,
        q=load_canonical_v0_3_q(),
    )


def weighted_avg_n_v0_baseline() -> float:
    """CEDA v0 snapshot N, weighted by canonical v0.3 ``q`` (pinned baseline)."""
    from bedrock.utils.snapshots.loader import load_snapshot  # noqa: PLC0415

    return _weighted_avg_n(
        B=load_snapshot('B_USA_non_finetuned', 'v0'),
        Adom=load_snapshot('Adom_USA', 'v0'),
        Aimp=load_snapshot('Aimp_USA', 'v0'),
        q=load_canonical_v0_3_q(),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('config_name', nargs='?', default=None)
    group.add_argument(
        '--v0-baseline',
        action='store_true',
        help=(
            'N from frozen CEDA v0 snapshots, weighted by canonical v0.3 '
            f'scaled_q_USA (config {CANONICAL_USA_CONFIG})'
        ),
    )
    group.add_argument(
        '--sheet-n-new',
        action='store_true',
        help=(
            'recompute q-weighted sheet N_new for every live waterfall config '
            '(refresh EXPECTED_LIVE_N_NEW pins)'
        ),
    )
    group.add_argument(
        '--assessment-useeio',
        action='store_true',
        help=(
            'recompute bedrock-owned USEEIO-track assessment bars from '
            'diagnostics sheets (ceda combine_ef column rules + canonical q)'
        ),
    )
    parser.add_argument(
        '--refresh-sheets',
        action='store_true',
        help='bypass analysis/.cache and re-fetch Google Sheet tabs',
    )
    args = parser.parse_args(argv)

    if args.sheet_n_new:
        levels = sheet_n_new_levels(refresh=args.refresh_sheets)
        json.dump({'sheet_n_new': levels}, sys.stdout)
        return 0

    if args.assessment_useeio:
        assert_useeio_track_sheet_ids_match_registry()
        levels = assessment_useeio_bedrock_levels(refresh=args.refresh_sheets)
        json.dump({'assessment_useeio_bedrock': levels}, sys.stdout)
        return 0

    level = (
        weighted_avg_n_v0_baseline()
        if args.v0_baseline
        else weighted_avg_n_for_config(args.config_name)
    )
    json.dump({'weighted_avg_n_kg_per_usd': level}, sys.stdout)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
