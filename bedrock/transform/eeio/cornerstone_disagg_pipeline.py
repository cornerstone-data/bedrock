"""Cornerstone sector-disaggregation Orchestration of disagg when one
or more disagg-related config flags are true.

Returns uninflated 2017-chain-dollar IO matrices only. Public entry points in
``derived_cornerstone`` apply inflation and year-scaling after routing here.
"""

from __future__ import annotations

import functools
import pathlib
from dataclasses import dataclass
from typing import cast

import pandas as pd
import pandera.typing as pt

from bedrock.extract.disaggregation import disagg_weights as _disagg_weights
from bedrock.extract.disaggregation.disagg_weights import DisaggWeights
from bedrock.extract.disaggregation.waste_weight_config import (
    effective_waste_disagg_config,
)
from bedrock.extract.iot.io_2017 import (
    load_2017_Uimp_usa,
    load_2017_Utot_usa,
    load_2017_V_usa,
    load_2017_value_added_usa,
    load_2017_Ytot_usa,
)
from bedrock.transform.eeio.cornerstone_expansion import (
    commodity_corresp,
    industry_corresp,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    apply_electricity_unit_conversion_to_A,
    apply_electricity_unit_conversion_to_B,
    apply_electricity_unit_conversion_to_q,
    disaggregate_electricity_commodity_row_in_y,
    disaggregate_electricity_make_use_va,
    distribute_electricity_aggregate_x_using_v_row_shares,
    electricity_class_row_factors,
    electricity_output_factor,
    get_electricity_commodity_row_weights,
    reallocate_electricity_coproduction,
)
from bedrock.transform.eeio.electricity_end_use_mapping import (
    END_USE_MAPPING_REVIEW_STATUS,  # noqa: F401 — re-export
    build_end_use_map,
    build_end_use_map_resolved,  # noqa: F401 — re-export
    classify_industry_end_use,  # noqa: F401 — re-export
    table_2_4_prices_cents_kwh,
)
from bedrock.transform.eeio.waste_disaggregation import (
    apply_waste_disagg_to_U,
    apply_waste_disagg_to_V,
    apply_waste_disagg_to_VA,
    apply_waste_disagg_to_Ytot,
)
from bedrock.utils.config.usa_config import EEIOWasteDisaggConfig, get_usa_config
from bedrock.utils.math.formulas import backcompute_y_from_A_and_q, compute_x
from bedrock.utils.schemas.single_region_schemas import AMatrix
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet
from bedrock.utils.taxonomy.cornerstone.commodities import WASTE_DISAGG_COMMODITIES
from bedrock.utils.taxonomy.cornerstone.value_added import VALUE_ADDEDS

_BEDROCK_PKG_ROOT = pathlib.Path(__file__).resolve().parents[2]

WASTE_ORIGINAL_CODE = '562000'
WASTE_NEW_CODES: list[str] = list(WASTE_DISAGG_COMMODITIES[WASTE_ORIGINAL_CODE])

# Backward-compat aliases (re-exported from derived_cornerstone with leading underscore).
_WASTE_ORIGINAL_CODE = WASTE_ORIGINAL_CODE
_WASTE_NEW_CODES = WASTE_NEW_CODES


def _resolve_waste_cfg_paths(cfg: EEIOWasteDisaggConfig) -> EEIOWasteDisaggConfig:
    """Return a copy of *cfg* with weight file paths resolved to absolute paths."""

    def _resolve(p: str) -> str:
        path = pathlib.Path(p)
        if path.is_absolute():
            return p
        resolved = _BEDROCK_PKG_ROOT / path
        return str(resolved)

    return EEIOWasteDisaggConfig(
        use_weights_file=_resolve(cfg.use_weights_file),
        make_weights_file=_resolve(cfg.make_weights_file),
        year=cfg.year,
        source_name=cfg.source_name,
    )


@functools.cache
def cornerstone_sector_disagg_active() -> bool:
    """True when sector disaggregation (waste and/or electricity) is enabled.

    Electricity disaggregation requires waste (``USAConfig`` validator), so
    ``implement_waste_disaggregation`` alone is sufficient for routing.
    """
    return get_usa_config().implement_waste_disaggregation


@functools.cache
def electricity_disaggregation_enabled() -> bool:
    return get_usa_config().implement_electricity_disaggregation


@functools.cache
def get_waste_disagg_weights() -> DisaggWeights | None:
    """Return waste disaggregation weights if the feature is enabled, else None."""
    cfg = get_usa_config()
    if not cfg.implement_waste_disaggregation:
        return None
    resolved_cfg = _resolve_waste_cfg_paths(effective_waste_disagg_config(cfg))
    return _disagg_weights.load_disagg_weights(
        resolved_cfg,
        original_code=_WASTE_ORIGINAL_CODE,
        new_codes=_WASTE_NEW_CODES,
        disagg_sectors=_WASTE_NEW_CODES,
        va_row_codes=list(VALUE_ADDEDS),
    )


@functools.cache
def electricity_reallocation_enabled() -> bool:
    cfg = get_usa_config()
    return cfg.implement_electricity_reallocation


@dataclass
class CornerstoneDisaggIOBundle:
    V: pd.DataFrame
    Udom: pd.DataFrame
    Uimp: pd.DataFrame
    VA: pd.DataFrame


def derive_cornerstone_V_after_waste() -> pd.DataFrame:
    V_2017 = load_2017_V_usa()
    V = industry_corresp() @ V_2017 @ commodity_corresp().T
    V.index.name = 'sector'
    V.columns.name = 'sector'
    weights = get_waste_disagg_weights()
    if weights is not None:
        V = apply_waste_disagg_to_V(V, weights)
        V.index.name = 'sector'
        V.columns.name = 'sector'
    return V


def derive_cornerstone_U_after_waste() -> tuple[pd.DataFrame, pd.DataFrame]:
    Utot = load_2017_Utot_usa()
    Uimp = load_2017_Uimp_usa()
    Udom = Utot - Uimp

    com_c = commodity_corresp()
    ind_c = industry_corresp()

    Udom_cs = com_c @ Udom @ ind_c.T
    Uimp_cs = com_c @ Uimp @ ind_c.T

    for df in (Udom_cs, Uimp_cs):
        df.index.name = 'sector'
        df.columns.name = 'sector'

    weights = get_waste_disagg_weights()
    if weights is not None:
        Udom_cs, Uimp_cs = apply_waste_disagg_to_U(Udom_cs, Uimp_cs, weights)
        for df in (Udom_cs, Uimp_cs):
            df.index.name = 'sector'
            df.columns.name = 'sector'

    return Udom_cs, Uimp_cs


def _derive_y_before_electricity_disagg() -> pd.DataFrame:
    """Correspondence-mapped Y after waste disagg, before electricity row split."""
    ytot_orig = load_2017_Ytot_usa()
    ytot = commodity_corresp() @ ytot_orig
    ytot.index.name = 'sector'
    weights = get_waste_disagg_weights()
    if weights is not None:
        ytot = apply_waste_disagg_to_Ytot(ytot, weights)
        ytot.index.name = 'sector'
    return ytot


def derive_cornerstone_VA_after_waste() -> pd.DataFrame:
    VA = load_2017_value_added_usa() @ industry_corresp().T
    VA.columns.name = 'sector'
    weights = get_waste_disagg_weights()
    if weights is not None:
        VA = apply_waste_disagg_to_VA(VA, weights)
        VA.columns.name = 'sector'
    return VA


@functools.cache
def derive_disagg_io_bundle() -> CornerstoneDisaggIOBundle:
    """Correspondence + waste (+ optional electricity). Uninflated 2017 chain dollars."""
    V = derive_cornerstone_V_after_waste()
    Udom, Uimp = derive_cornerstone_U_after_waste()
    VA = derive_cornerstone_VA_after_waste()
    if electricity_reallocation_enabled():
        V, Udom, Uimp, VA = reallocate_electricity_coproduction(V, Udom, Uimp, VA)
    if electricity_disaggregation_enabled():
        V, Udom, Uimp, VA = disaggregate_electricity_make_use_va(V, Udom, Uimp, VA)
    return CornerstoneDisaggIOBundle(V=V, Udom=Udom, Uimp=Uimp, VA=VA)


@functools.cache
def derive_disagg_Ytot_with_trade() -> pd.DataFrame:
    """Correspondence-mapped Y with optional waste and electricity disagg."""
    Ytot_orig = load_2017_Ytot_usa()
    Ytot = commodity_corresp() @ Ytot_orig
    Ytot.index.name = 'sector'
    weights = get_waste_disagg_weights()
    if weights is not None:
        Ytot = apply_waste_disagg_to_Ytot(Ytot, weights)
        Ytot.index.name = 'sector'
    if electricity_disaggregation_enabled():
        w_row = get_electricity_commodity_row_weights()
        Ytot = disaggregate_electricity_commodity_row_in_y(Ytot, w_row)
        Ytot.index.name = 'sector'
    return Ytot


def distribute_waste_parent_x_using_v_row_shares(
    x_cs: pd.Series[float],
) -> pd.Series[float]:
    """Split duplicated BEA parent gross output across waste children using V row shares."""
    if get_waste_disagg_weights() is None:
        x = x_cs
    else:
        x = x_cs.copy()
        x_v = compute_x(V=derive_disagg_io_bundle().V)
        present = [
            c
            for c in _WASTE_NEW_CODES
            if c in x.index and c in x_v.index and pd.notna(x_v.loc[c])
        ]
        if present:
            xv_w = x_v.reindex(present).astype(float)
            total_v = float(xv_w.sum())
            if total_v > 0:
                parent_go = float(x.loc[present[0]])
                shares = xv_w / total_v
                for code in present:
                    x.loc[code] = parent_go * float(shares.loc[code])
    if electricity_disaggregation_enabled():
        return distribute_electricity_aggregate_x_using_v_row_shares(
            x, derive_disagg_io_bundle().V
        )
    return x


@functools.cache
def electricity_mixed_units_enabled() -> bool:
    return get_usa_config().implement_electricity_mixed_units


def _model_year_y_row_221110(aq_scaled: SingleRegionAqMatrixSet) -> pd.Series[float]:
    """Model-year 221110 FD row from backcompute total × 2017 share split."""
    y_2017 = derive_disagg_Ytot_with_trade().loc[GENERATION_SECTOR]
    y_total = float(
        backcompute_y_from_A_and_q(A=aq_scaled.Adom, q=aq_scaled.scaled_q).loc[
            GENERATION_SECTOR
        ]
    )
    y_sum = float(y_2017.sum())
    if y_sum <= 0:
        raise ValueError(
            'model_year_y_row_221110: 2017 Y row for 221110 sums to zero or negative'
        )
    return cast(pd.Series, y_total * (y_2017 / y_sum))


def electricity_conversion_factors(
    aq_scaled: SingleRegionAqMatrixSet,
) -> tuple[float, pd.Series[float]]:
    """Return (c_col, c_row) for generation sector unit conversion."""
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        us_total_net_generation_mwh,
    )

    cfg = get_usa_config()
    q_usd = float(aq_scaled.scaled_q[GENERATION_SECTOR])
    mwh = float(us_total_net_generation_mwh(cfg.model_base_year))
    c_col = electricity_output_factor(q_usd, mwh)
    prices = cast(dict[str, float], table_2_4_prices_cents_kwh(cfg.usa_ghg_data_year))
    end_use_map = build_end_use_map()
    y_row = _model_year_y_row_221110(aq_scaled)
    adom_row = cast(pd.Series, aq_scaled.Adom.loc[GENERATION_SECTOR])
    c_row = electricity_class_row_factors(
        adom_row,
        aq_scaled.scaled_q,
        y_row,
        prices,
        end_use_map,
        mwh,
    )
    return c_col, c_row


def build_electricity_mixed_units_aq(
    aq_scaled: SingleRegionAqMatrixSet,
) -> SingleRegionAqMatrixSet:
    """Return mixed-unit A/q when gate is on; else pass-through."""
    if not electricity_mixed_units_enabled():
        return aq_scaled
    c_col, c_row = electricity_conversion_factors(aq_scaled)
    Adom = apply_electricity_unit_conversion_to_A(
        aq_scaled.Adom, c_col=c_col, c_row=c_row
    )
    Aimp = apply_electricity_unit_conversion_to_A(
        aq_scaled.Aimp, c_col=c_col, c_row=c_row
    )
    scaled_q = apply_electricity_unit_conversion_to_q(aq_scaled.scaled_q, c_col)
    return SingleRegionAqMatrixSet(
        Adom=cast(pt.DataFrame[AMatrix], Adom),
        Aimp=cast(pt.DataFrame[AMatrix], Aimp),
        scaled_q=scaled_q,
    )


def build_electricity_mixed_units_b(
    b: pd.DataFrame,
    c_col: float,
) -> pd.DataFrame:
    """Return mixed-unit B when gate is on; else pass-through."""
    if not electricity_mixed_units_enabled():
        return b
    return apply_electricity_unit_conversion_to_B(b, c_col)
