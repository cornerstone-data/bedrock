"""Three Methods #86 toy paths: production (main), flow-mixed, direct-mixed (PR4)."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from bedrock.analysis.electricity.d_86.mixed_flow_conversion import (
    apply_direct_mixed_transform,
    apply_mixed_conversion_to_flows,
    compute_toy_conversion_factors,
    default_toy_mwh,
)
from bedrock.analysis.electricity.d_86.toy_dom_imp import (
    ToyDomImpMonetaryTables,
    ToyDomImpScaledInflated,
    build_toy_dom_imp_monetary_tables,
    derive_adom_aimp_from_flows,
    scale_and_inflate_dom_imp,
)
from bedrock.analysis.electricity.d_86.toy_tables import (
    toy_end_use_map,
    toy_prices_cents_kwh,
)
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.math.formulas import (
    backcompute_y_from_A_and_q,
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
)
from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
    _compute_bly_series,
)
from bedrock.utils.validation.eeio_diagnostics import (
    DiagnosticResult,
    compare_commodity_output_to_domestics_use_plus_exports,
    compare_output_vs_leontief_x_demand,
)


@dataclass(frozen=True)
class ToyScaledFlows:
    """Target-year flow tables rebuilt from scaled ``Adom`` / ``Aimp`` / ``q``."""

    v: pd.DataFrame
    udom: pd.DataFrame
    uimp: pd.DataFrame
    y: pd.DataFrame
    va: pd.DataFrame


@dataclass(frozen=True)
class ToyEfOutputs:
    """BLy, D, N and optional output-identity diagnostics."""

    b: pd.DataFrame
    l_dom: pd.DataFrame
    l_tot: pd.DataFrame
    y_nab: pd.Series[float]
    d: pd.Series[float]
    n: pd.Series[float]
    bly: pd.Series[float]
    commodity_identity: DiagnosticResult | None = None
    leontief_identity: DiagnosticResult | None = None


@dataclass(frozen=True)
class Section1ProductionResult:
    """Main-branch path: monetary scaled ``A`` / ``q`` → BLy, D, N (all USD)."""

    monetary: ToyDomImpMonetaryTables
    scaled: ToyDomImpScaledInflated
    ef: ToyEfOutputs


@dataclass(frozen=True)
class Section2FlowMixedResult:
    """Analysis path: scaled flows → mixed IO tables → rederived mixed ``A`` / ``q``."""

    monetary: ToyDomImpMonetaryTables
    scaled: ToyDomImpScaledInflated
    scaled_flows: ToyScaledFlows
    c_col: float
    c_row: pd.Series[float]
    mwh_221110: float
    mixed_flows: ToyScaledFlows
    adom: pd.DataFrame
    aimp: pd.DataFrame
    atot: pd.DataFrame
    q: pd.Series[float]
    ef: ToyEfOutputs


@dataclass(frozen=True)
class Section3DirectMixedResult:
    """PR4 path: mixed units applied directly to scaled ``Adom`` / ``Aimp`` / ``q``."""

    monetary: ToyDomImpMonetaryTables
    scaled: ToyDomImpScaledInflated
    c_col: float
    c_row: pd.Series[float]
    mwh_221110: float
    adom: pd.DataFrame
    aimp: pd.DataFrame
    atot: pd.DataFrame
    q: pd.Series[float]
    ef: ToyEfOutputs


def _target_year_y_frame(
    atot: pd.DataFrame,
    q: pd.Series[float],
    fd_columns: pd.Index,
) -> pd.DataFrame:
    y_nab = backcompute_y_from_A_and_q(A=atot, q=q)
    y = pd.DataFrame(0.0, index=atot.index, columns=fd_columns)
    y.iloc[:, 0] = y_nab.reindex(y.index).astype(float)
    return y


def rebuild_scaled_dom_imp_flows(
    monetary: ToyDomImpMonetaryTables,
    scaled: ToyDomImpScaledInflated,
) -> ToyScaledFlows:
    """Rebuild ``V`` / ``Udom`` / ``Uimp`` / ``Y`` / ``VA`` at model-year USD from scaled ``A`` / ``q``."""
    si = scaled
    y = _target_year_y_frame(si.atot_target, si.q_target, monetary.Y.columns)
    codes = list(si.q_target.index)
    v = pd.DataFrame(0.0, index=codes, columns=codes)
    for code in codes:
        v.loc[code, code] = float(si.q_target[code])
    udom = si.adom.a_target.multiply(si.q_target, axis=1)
    uimp = si.aimp.a_target.multiply(si.q_target, axis=1)
    q_ratio = (si.q_target / monetary.q).reindex(monetary.VA.columns, fill_value=1.0)
    va = monetary.VA.multiply(q_ratio, axis=1)
    return ToyScaledFlows(v=v, udom=udom, uimp=uimp, y=y, va=va)


def _generation_y_row(y: pd.DataFrame) -> pd.Series[float]:
    row = y.loc[GENERATION_SECTOR]
    if isinstance(row, pd.DataFrame):
        return row.iloc[0].astype(float)
    return row.astype(float)


def _conversion_factors(
    *,
    adom_target: pd.DataFrame,
    q_target: pd.Series[float],
    y_row: pd.Series[float],
    mwh: float | None,
) -> tuple[float, pd.Series[float], float]:
    mwh_val = (
        default_toy_mwh(float(q_target[GENERATION_SECTOR])) if mwh is None else mwh
    )
    c_col, c_row = compute_toy_conversion_factors(
        a=adom_target,
        q=q_target,
        y_row=y_row,
        mwh_221110=mwh_val,
        prices_by_class=toy_prices_cents_kwh(),
        end_use_map=toy_end_use_map(),
    )
    return c_col, c_row, mwh_val


def _compute_ef_outputs(
    *,
    b: pd.DataFrame,
    adom: pd.DataFrame,
    atot: pd.DataFrame,
    q: pd.Series[float],
    udom: pd.DataFrame | None = None,
    uimp: pd.DataFrame | None = None,
    y_flow: pd.DataFrame | None = None,
    identity_tolerance: float = 1e-9,
    run_identities: bool = True,
) -> ToyEfOutputs:
    """Compute L, D, N, BLy; optionally run commodity / Leontief checks on ``Atot``."""
    y_nab = backcompute_y_from_A_and_q(A=adom, q=q)
    l_dom = compute_L_matrix(A=adom)
    l_tot = compute_L_matrix(A=atot)
    d = compute_d(B=b)
    n = compute_n(M=compute_M_matrix(B=b, L=l_dom))
    bly = _compute_bly_series(B=b, Adom=adom, y=y_nab)

    commodity_identity = None
    leontief_identity = None
    if run_identities and udom is not None and uimp is not None and y_flow is not None:
        u_total = udom + uimp
        y_d = y_flow.sum(axis=1).astype(float)
        commodity_identity = compare_commodity_output_to_domestics_use_plus_exports(
            q=q,
            U_d=u_total,
            y_d=y_d,
            tolerance=identity_tolerance,
            include_details=True,
        )
        leontief_identity = compare_output_vs_leontief_x_demand(
            output=q,
            L=l_tot,
            y=backcompute_y_from_A_and_q(A=atot, q=q),
            tolerance=identity_tolerance,
            include_details=True,
        )
    return ToyEfOutputs(
        b=b,
        l_dom=l_dom,
        l_tot=l_tot,
        y_nab=y_nab,
        d=d,
        n=n,
        bly=bly,
        commodity_identity=commodity_identity,
        leontief_identity=leontief_identity,
    )


def run_section1_production(
    *,
    identity_tolerance: float = 1e-9,
) -> Section1ProductionResult:
    """
    Main-branch toy path (no mixed units):

    monetary IO → ``Adom`` / ``Aimp`` / ``Atot`` → scale + inflate →
    monetary ``B``, ``L``, ``y_nab`` → BLy, D, N.
    """
    monetary = build_toy_dom_imp_monetary_tables()
    scaled = scale_and_inflate_dom_imp(monetary)
    flows = rebuild_scaled_dom_imp_flows(monetary, scaled)
    ef = _compute_ef_outputs(
        b=monetary.B,
        adom=scaled.adom.a_target,
        atot=scaled.atot_target,
        q=scaled.q_target,
        udom=flows.udom,
        uimp=flows.uimp,
        y_flow=flows.y,
        identity_tolerance=identity_tolerance,
    )
    return Section1ProductionResult(monetary=monetary, scaled=scaled, ef=ef)


def run_section2_flow_mixed(
    *,
    mwh_221110: float | None = None,
    identity_tolerance: float = 1e-9,
) -> Section2FlowMixedResult:
    """
    Analysis path: rebuild scaled flows, apply mixed conversion at IO level,
    rederive ``Adom`` / ``Aimp`` / ``Atot`` / ``q``, then mixed BLy / D / N.
    """
    monetary = build_toy_dom_imp_monetary_tables()
    scaled = scale_and_inflate_dom_imp(monetary)
    scaled_flows = rebuild_scaled_dom_imp_flows(monetary, scaled)

    y_row = _generation_y_row(scaled_flows.y)
    c_col, c_row, mwh = _conversion_factors(
        adom_target=scaled.adom.a_target,
        q_target=scaled.q_target,
        y_row=y_row,
        mwh=mwh_221110,
    )

    v_m, udom_m, y_m = apply_mixed_conversion_to_flows(
        v=scaled_flows.v,
        u=scaled_flows.udom,
        y=scaled_flows.y,
        c_col=c_col,
        c_row=c_row,
    )
    _, uimp_m, _ = apply_mixed_conversion_to_flows(
        v=scaled_flows.v,
        u=scaled_flows.uimp,
        y=scaled_flows.y,
        c_col=c_col,
        c_row=c_row,
    )
    adom, aimp, q, _ = derive_adom_aimp_from_flows(v_m, udom_m, uimp_m)
    atot = adom + aimp

    # Row-balanced Use / Y implied by rederived mixed A and q (for identities).
    udom_bal = adom.multiply(q, axis=1)
    uimp_bal = aimp.multiply(q, axis=1)
    y_bal = _target_year_y_frame(atot, q, monetary.Y.columns)

    mixed_flows = ToyScaledFlows(
        v=v_m,
        udom=udom_m,
        uimp=uimp_m,
        y=y_m,
        va=scaled_flows.va,
    )

    b_m, _, _ = apply_direct_mixed_transform(
        a=scaled.adom.a_target,
        q=scaled.q_target,
        b=monetary.B,
        c_col=c_col,
        c_row=c_row,
    )
    ef = _compute_ef_outputs(
        b=b_m,
        adom=adom,
        atot=atot,
        q=q,
        udom=udom_bal,
        uimp=uimp_bal,
        y_flow=y_bal,
        identity_tolerance=identity_tolerance,
    )
    return Section2FlowMixedResult(
        monetary=monetary,
        scaled=scaled,
        scaled_flows=scaled_flows,
        c_col=c_col,
        c_row=c_row,
        mwh_221110=mwh,
        mixed_flows=mixed_flows,
        adom=adom,
        aimp=aimp,
        atot=atot,
        q=q,
        ef=ef,
    )


def run_section3_direct_mixed(
    *,
    mwh_221110: float | None = None,
) -> Section3DirectMixedResult:
    """
    PR4 production path: ``apply_electricity_unit_conversion_to_*`` on scaled
    ``Adom`` / ``Aimp`` / ``q`` with no flow-table round-trip.
    """
    monetary = build_toy_dom_imp_monetary_tables()
    scaled = scale_and_inflate_dom_imp(monetary)
    flows = rebuild_scaled_dom_imp_flows(monetary, scaled)

    y_row = _generation_y_row(flows.y)
    c_col, c_row, mwh = _conversion_factors(
        adom_target=scaled.adom.a_target,
        q_target=scaled.q_target,
        y_row=y_row,
        mwh=mwh_221110,
    )

    adom, q, b_m = apply_direct_mixed_transform(
        a=scaled.adom.a_target,
        q=scaled.q_target,
        b=monetary.B,
        c_col=c_col,
        c_row=c_row,
    )
    aimp, _, _ = apply_direct_mixed_transform(
        a=scaled.aimp.a_target,
        q=scaled.q_target,
        b=monetary.B,
        c_col=c_col,
        c_row=c_row,
    )
    atot = adom + aimp

    ef = _compute_ef_outputs(
        b=b_m,
        adom=adom,
        atot=atot,
        q=q,
        udom=None,
        uimp=None,
        y_flow=None,
        run_identities=False,
    )
    return Section3DirectMixedResult(
        monetary=monetary,
        scaled=scaled,
        c_col=c_col,
        c_row=c_row,
        mwh_221110=mwh,
        adom=adom,
        aimp=aimp,
        atot=atot,
        q=q,
        ef=ef,
    )


def assert_section2_matches_section3(
    s2: Section2FlowMixedResult,
    s3: Section3DirectMixedResult,
) -> None:
    """Flow-mixed and direct-mixed ``Atot`` / ``q`` should agree on the toy model."""
    np.testing.assert_allclose(
        s2.atot.to_numpy(),
        s3.atot.to_numpy(),
        rtol=1e-9,
        atol=1e-9,
    )
    pd.testing.assert_series_equal(s2.q, s3.q, atol=1e-9, rtol=0.0)
    assert float(s2.q[GENERATION_SECTOR]) == s2.mwh_221110
    assert s2.ef.commodity_identity is not None
    assert s2.ef.leontief_identity is not None
    assert s2.ef.commodity_identity.passed
    assert s2.ef.leontief_identity.passed
