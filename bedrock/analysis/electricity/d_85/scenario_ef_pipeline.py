"""Scenario IO → A/q/B → L/M/N/D for d_85 EF comparison."""

from __future__ import annotations

import typing as ta
from typing import cast

import numpy as np
import pandas as pd
import pandera.typing as pt

from bedrock.analysis.electricity.d_85.scenario_types import DisaggScenarioResult
from bedrock.extract.iot.io_2017 import load_2017_V_usa
from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.cornerstone_expansion import industry_corresp
from bedrock.transform.eeio.cornerstone_year_scaling import (
    USA_SUMMARY_MUT_YEARS,
    scale_cornerstone_A,
    scale_cornerstone_q,
)
from bedrock.transform.eeio.derived_2017 import derive_summary_q_usa
from bedrock.transform.eeio.electricity_disaggregation import (
    split_electricity_e_for_disaggregated_b,
)
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_cornerstone import (
    inflate_cornerstone_A_matrix_with_industry_pi,
    inflate_cornerstone_q_or_y_with_industry_pi,
)
from bedrock.utils.math.formulas import (
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
)
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
    validate_cornerstone,
)
from bedrock.utils.schemas.single_region_schemas import AMatrix
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet

ELEC = list(ELECTRICITY_DISAGG_SECTORS)


def _scenario_aq_matrix_set(
    Adom: pd.DataFrame,
    Aimp: pd.DataFrame,
    scaled_q: pd.Series[float],
) -> SingleRegionAqMatrixSet:
    validate_cornerstone(Adom, 'A')
    validate_cornerstone(Aimp, 'A')
    validate_cornerstone(scaled_q, 'Q')
    return SingleRegionAqMatrixSet(
        Adom=cast(pt.DataFrame[AMatrix], Adom),
        Aimp=cast(pt.DataFrame[AMatrix], Aimp),
        scaled_q=scaled_q,
    )


def scenario_vnorm(
    scenario: DisaggScenarioResult,
    *,
    apply_inflation: bool | None = None,
) -> pd.DataFrame:
    """Scrap-corrected Vnorm from scenario V/q (mirrors production formula)."""
    _ = apply_inflation  # scenario V is always 2017 nominal in this analysis path
    V = scenario.V
    q = scenario.q
    x = scenario.x
    Vnorm = compute_Vnorm_matrix(V=V, q=q)
    scrap_2017 = load_2017_V_usa().loc[:, 'S00401']
    scrap_fraction = industry_corresp() @ scrap_2017
    parent_scrap = float(scrap_fraction.get(ELECTRICITY_AGGREGATE_SECTOR, 0.0))
    scrap_fraction = scrap_fraction.drop(ELECTRICITY_AGGREGATE_SECTOR, errors='ignore')
    for code in ELECTRICITY_DISAGG_SECTORS:
        scrap_fraction.loc[code] = parent_scrap
    scrap_fraction = scrap_fraction.reindex(V.index, fill_value=0.0)
    x_aligned = x.reindex(V.index, fill_value=0.0)
    denom = 1.0 - (scrap_fraction / x_aligned).fillna(0.0)
    return Vnorm.divide(denom, axis=0).reindex(
        index=V.index, columns=V.columns, fill_value=0.0
    )


def derive_Aq_from_scenario(scenario: DisaggScenarioResult) -> SingleRegionAqMatrixSet:
    """Unscaled 2017 A/q from scenario IO."""
    Vnorm = scenario_vnorm(
        scenario, apply_inflation=get_usa_config().apply_inflation_to_V
    )
    x = scenario.x
    Adom = compute_Unorm_matrix(U=scenario.Udom, x=x) @ Vnorm
    Aimp = compute_Unorm_matrix(U=scenario.Uimp, x=x) @ Vnorm
    Adom.index.name = 'sector'
    Adom.columns.name = 'sector'
    Aimp.index.name = 'sector'
    Aimp.columns.name = 'sector'
    return _scenario_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=scenario.q.copy())


def apply_config_scaling(
    aq: SingleRegionAqMatrixSet,
    *,
    electricity_ratio_overrides: dict[str, float] | None = None,
) -> SingleRegionAqMatrixSet:
    """Mirror default ``derive_cornerstone_Aq_scaled`` branch (io_year then industry PI)."""
    cfg = get_usa_config()
    io_year = cfg.usa_io_data_year
    detail_year = cfg.usa_detail_original_year
    model_year = cfg.model_base_year

    Adom = scale_cornerstone_A(
        aq.Adom,
        target_year=io_year,
        original_year=detail_year,
        dom_or_imp_or_total='dom',
    )
    Aimp = scale_cornerstone_A(
        aq.Aimp,
        target_year=io_year,
        original_year=detail_year,
        dom_or_imp_or_total='imp',
    )
    q = scale_cornerstone_q(aq.scaled_q, target_year=io_year, original_year=detail_year)

    if electricity_ratio_overrides:
        Adom = _apply_electricity_row_overrides(
            Adom, electricity_ratio_overrides, io_year, detail_year
        )
        Aimp = _apply_electricity_row_overrides(
            Aimp, electricity_ratio_overrides, io_year, detail_year
        )
        q = _apply_electricity_q_overrides(
            q, electricity_ratio_overrides, io_year, detail_year
        )

    Adom = inflate_cornerstone_A_matrix_with_industry_pi(
        Adom, original_year=io_year, target_year=model_year
    )
    Aimp = inflate_cornerstone_A_matrix_with_industry_pi(
        Aimp, original_year=io_year, target_year=model_year
    )
    q = inflate_cornerstone_q_or_y_with_industry_pi(
        q, original_year=io_year, target_year=model_year
    )
    return _scenario_aq_matrix_set(Adom=Adom, Aimp=Aimp, scaled_q=q)


def _summary_utilities_ratio(
    io_year: int,
    detail_year: int,
    dom_or_imp: ta.Literal['dom', 'imp'],
) -> float:
    _ = dom_or_imp
    io = cast(USA_SUMMARY_MUT_YEARS, io_year)
    detail = cast(USA_SUMMARY_MUT_YEARS, detail_year)
    ratio = (derive_summary_q_usa(io) / derive_summary_q_usa(detail)).fillna(1.0)
    val = float(ratio.get('22', 1.0))
    return val if np.isfinite(val) else 1.0


def _apply_electricity_row_overrides(
    A: pd.DataFrame,
    overrides: dict[str, float],
    io_year: int,
    detail_year: int,
) -> pd.DataFrame:
    """Rescale electricity child rows from Utilities summary ratio to overrides."""
    base_ratio = _summary_utilities_ratio(io_year, detail_year, 'dom')
    out = A.copy()
    for code, new_ratio in overrides.items():
        if code not in out.index:
            continue
        factor = new_ratio / base_ratio if base_ratio else 1.0
        out.loc[code] = out.loc[code] * factor
    return out


def _apply_electricity_q_overrides(
    q: pd.Series[float],
    overrides: dict[str, float],
    io_year: int,
    detail_year: int,
) -> pd.Series[float]:
    base_ratio = _summary_utilities_ratio(io_year, detail_year, 'dom')
    out = q.copy()
    for code, new_ratio in overrides.items():
        if code not in out.index:
            continue
        factor = new_ratio / base_ratio if base_ratio else 1.0
        out.loc[code] = float(out.loc[code]) * factor
    return out


def derive_B_from_scenario(scenario: DisaggScenarioResult) -> pd.DataFrame:
    """B = (E/x) @ Vnorm using scenario x (2017 Make-derived)."""
    E = split_electricity_e_for_disaggregated_b(derive_E_usa())
    x = scenario.x.reindex(E.columns, fill_value=np.nan)
    x = x.fillna(1.0)
    Vnorm = scenario_vnorm(
        scenario, apply_inflation=get_usa_config().apply_inflation_to_V
    )
    return (E.div(x, axis=1)) @ Vnorm


def probe_e_source() -> dict[str, str]:
    """Report whether eGRID FBS or GCS fallback was used for E."""
    cfg = get_usa_config()
    if not (cfg.new_ghg_method and cfg.implement_electricity_disaggregation):
        return {'source': 'standard', 'note': 'electricity disagg flag off'}
    try:
        from flowsa import getFlowBySector as get_flowsa_fbs  # noqa: PLC0415

        get_flowsa_fbs(
            methodname='GHG_national_Cornerstone_2023_egrid',
            download_FBS_if_missing=False,
        )
        return {'source': 'egrid_fbs', 'note': 'eGRID FBS load succeeded'}
    except Exception as exc:
        return {'source': 'national_gcs', 'note': str(exc)}


def compute_ef_vectors(
    scenario: DisaggScenarioResult,
    *,
    electricity_ratio_overrides: dict[str, float] | None = None,
) -> dict[str, pd.Series[float] | pd.DataFrame]:
    """Build D, N, L-related outputs from scenario IO."""
    aq = derive_Aq_from_scenario(scenario)
    aq_scaled = apply_config_scaling(
        aq, electricity_ratio_overrides=electricity_ratio_overrides
    )
    B = derive_B_from_scenario(scenario)
    A = aq_scaled.Adom + aq_scaled.Aimp
    L = compute_L_matrix(A=A)
    M = compute_M_matrix(B=B, L=L)
    D = compute_d(B=B)
    N = compute_n(M=M)
    return {'D': D, 'N': N, 'A': A, 'q': aq_scaled.scaled_q, 'B': B}
