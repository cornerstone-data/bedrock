"""UGO305 differentiated scaling simulation for Decision 7."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.d_85.disagg_scenarios import run_scenario
from bedrock.analysis.electricity.d_85.disagg_weights import ugo305_go_weights
from bedrock.analysis.electricity.d_85.scenario_ef_pipeline import (
    apply_config_scaling,
    derive_Aq_from_scenario,
)
from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult
from bedrock.extract.iot.gdp import SECTOR_NAME_COL, load_go_detail
from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_Aq_scaled
from bedrock.transform.eeio.electricity_disaggregation import (
    DISTRIBUTION_GO_SECTOR_NAME,
    GENERATION_GO_SECTOR_NAMES,
    TRANSMISSION_GO_SECTOR_NAME,
    _normalize_sector_name,
    _resolve_go_year_column,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

ELEC = list(ELECTRICITY_DISAGG_SECTORS)


def _go_levels_for_year(year: int) -> pd.Series[float]:
    go = load_go_detail()
    year_col = _resolve_go_year_column(go, year)
    name_to_value = {
        _normalize_sector_name(str(n)): float(v)
        for n, v in zip(go[SECTOR_NAME_COL], go[year_col], strict=True)
    }
    gen_total = sum(
        name_to_value[_normalize_sector_name(n)] for n in GENERATION_GO_SECTOR_NAMES
    )
    trans = name_to_value[_normalize_sector_name(TRANSMISSION_GO_SECTOR_NAME)]
    dist = name_to_value[_normalize_sector_name(DISTRIBUTION_GO_SECTOR_NAME)]
    return pd.Series(
        {'221110': gen_total, '221121': trans, '221122': dist},
        dtype=float,
    )


def build_ugo305_detail_ratios(
    base_year: int = 2017,
    target_year: int | None = None,
) -> pd.Series[float]:
    cfg = get_usa_config()
    tgt = target_year if target_year is not None else cfg.usa_io_data_year
    go_base = _go_levels_for_year(base_year)
    go_tgt = _go_levels_for_year(tgt)
    ratios = go_tgt / go_base.replace(0, float('nan'))
    return ratios.fillna(1.0).reindex(ELEC)


def build_anchored_overrides(
    ratios: pd.Series[float],
    *,
    w_base: pd.Series[float] | None = None,
) -> dict[str, float]:
    """Anchored variant: preserve weighted mean vs Utilities summary ratio."""
    from bedrock.analysis.electricity.d_85.scenario_ef_pipeline import (  # noqa: PLC0415
        _summary_utilities_ratio,
    )

    cfg = get_usa_config()
    util_ratio = _summary_utilities_ratio(
        cfg.usa_io_data_year, cfg.usa_detail_original_year, 'dom'
    )
    w = (ugo305_go_weights() if w_base is None else w_base).reindex(ELEC).astype(float)
    weighted_mean = float((ratios * w).sum() / w.sum())
    anchor = util_ratio / weighted_mean if weighted_mean else 1.0
    return {code: float(ratios[code]) * anchor for code in ELEC}


def run_d7_scenario(variant: str) -> tuple[DisaggScenarioResult, dict[str, float]]:
    baseline = run_scenario('baseline')
    ratios = build_ugo305_detail_ratios()
    if variant == 'd7_pure':
        overrides = {code: float(ratios[code]) for code in ELEC}
    elif variant == 'd7_anchored':
        overrides = build_anchored_overrides(ratios)
    else:
        raise ValueError(f'Unknown D7 variant: {variant}')
    return baseline, overrides


def compare_q_trajectories() -> pd.DataFrame:
    """Side-by-side scaled q for baseline vs D7 variants."""
    baseline = run_scenario('baseline')
    rows: list[dict[str, object]] = []
    aq_base = derive_Aq_from_scenario(baseline)
    aq_scaled_base = apply_config_scaling(aq_base)
    prod_aq = derive_cornerstone_Aq_scaled()
    for label, aq in (
        ('scenario_baseline', aq_scaled_base),
        ('production_baseline', prod_aq),
    ):
        for code in ELEC:
            rows.append(
                {'source': label, 'sector': code, 'q': float(aq.scaled_q.get(code, 0))}
            )
    for variant in ('d7_pure', 'd7_anchored'):
        _, overrides = run_d7_scenario(variant)
        aq = apply_config_scaling(
            derive_Aq_from_scenario(baseline), electricity_ratio_overrides=overrides
        )
        for code in ELEC:
            rows.append(
                {
                    'source': variant,
                    'sector': code,
                    'q': float(aq.scaled_q.get(code, 0)),
                }
            )
    return pd.DataFrame(rows)


def ratio_table() -> pd.DataFrame:
    ratios = build_ugo305_detail_ratios()
    cfg = get_usa_config()
    from bedrock.analysis.electricity.d_85.scenario_ef_pipeline import (  # noqa: PLC0415
        _summary_utilities_ratio,
    )

    util = _summary_utilities_ratio(
        cfg.usa_io_data_year, cfg.usa_detail_original_year, 'dom'
    )
    anchored = build_anchored_overrides(ratios)
    records = []
    for code in ELEC:
        records.append(
            {
                'sector': code,
                'ratio_k': float(ratios[code]),
                'utilities_summary_ratio': util,
                'd7_pure_override': float(ratios[code]),
                'd7_anchored_override': anchored[code],
            }
        )
    return pd.DataFrame(records)
