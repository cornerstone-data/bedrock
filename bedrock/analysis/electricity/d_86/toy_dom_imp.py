"""Toy model with separate domestic and import Use tables (Adom + Aimp)."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from bedrock.analysis.electricity.d_86.toy_scaling import (
    ToyScaledInflatedAq,
    toy_scale_and_inflate_aq,
    toy_summary_a_imp_ratio,
)
from bedrock.analysis.electricity.d_86.toy_tables import (
    TOY_SECTORS,
    build_toy_monetary_tables,
)
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.utils.math.formulas import (
    backcompute_y_from_A_and_q,
    compute_A_matrix,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
    compute_x,
)


@dataclass(frozen=True)
class ToyDomImpMonetaryTables:
    """3×3 monetary tables with ``Udom`` and ``Uimp`` split."""

    V: pd.DataFrame
    Udom: pd.DataFrame
    Uimp: pd.DataFrame
    VA: pd.DataFrame
    Y: pd.DataFrame
    B: pd.DataFrame
    Adom: pd.DataFrame
    Aimp: pd.DataFrame
    Atot: pd.DataFrame
    q: pd.Series[float]
    x: pd.Series[float]
    y_d: pd.Series[float]
    y_nab: pd.Series[float]


def derive_adom_aimp_from_flows(
    v: pd.DataFrame,
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series[float], pd.Series[float]]:
    """Derive ``Adom``, ``Aimp``, ``q``, ``x`` from Make and split Use tables."""
    q = compute_q(V=v)
    x = compute_x(V=v)
    vnorm = compute_Vnorm_matrix(V=v, q=q)
    adom = compute_A_matrix(
        U_norm=compute_Unorm_matrix(U=udom, x=x),
        V_norm=vnorm,
    )
    aimp = compute_A_matrix(
        U_norm=compute_Unorm_matrix(U=uimp, x=x),
        V_norm=vnorm,
    )
    return adom, aimp, q, x


def build_toy_dom_imp_monetary_tables() -> ToyDomImpMonetaryTables:
    """Build monetary tables with additive import flows on the generation row."""
    base = build_toy_monetary_tables()
    uimp = pd.DataFrame(0.0, index=TOY_SECTORS, columns=TOY_SECTORS)
    uimp.loc[GENERATION_SECTOR, 'C1'] = 8.0
    uimp.loc[GENERATION_SECTOR, 'C2'] = 4.0
    udom = base.U.copy()

    adom, aimp, q, x = derive_adom_aimp_from_flows(base.V, udom, uimp)
    atot = adom + aimp
    y_nab = backcompute_y_from_A_and_q(A=atot, q=q)

    return ToyDomImpMonetaryTables(
        V=base.V,
        Udom=udom,
        Uimp=uimp,
        VA=base.VA,
        Y=base.Y,
        B=base.B,
        Adom=adom,
        Aimp=aimp,
        Atot=atot,
        q=q,
        x=x,
        y_d=base.y_d,
        y_nab=y_nab,
    )


@dataclass(frozen=True)
class ToyDomImpScaledInflated:
    """Scaled and inflated domestic/import/total A blocks."""

    adom: ToyScaledInflatedAq
    aimp: ToyScaledInflatedAq
    atot_target: pd.DataFrame
    q_target: pd.Series[float]


def scale_and_inflate_dom_imp(
    tables: ToyDomImpMonetaryTables,
) -> ToyDomImpScaledInflated:
    """Scale ``Adom`` and ``Aimp`` with separate summary ratios; ``Atot = Adom + Aimp``."""
    adom_si = toy_scale_and_inflate_aq(tables.Adom, tables.q)
    aimp_si = toy_scale_and_inflate_aq(
        tables.Aimp,
        tables.q,
        a_scale_ratio=toy_summary_a_imp_ratio(),
    )
    atot = adom_si.a_target + aimp_si.a_target
    return ToyDomImpScaledInflated(
        adom=adom_si,
        aimp=aimp_si,
        atot_target=atot,
        q_target=adom_si.q_target,
    )


def dom_imp_y_row(y: pd.DataFrame) -> pd.Series[float]:
    row = y.loc[GENERATION_SECTOR]
    if isinstance(row, pd.DataFrame):
        return row.iloc[0].astype(float)
    return row.astype(float)


__all__ = [
    'ToyDomImpMonetaryTables',
    'ToyDomImpScaledInflated',
    'build_toy_dom_imp_monetary_tables',
    'derive_adom_aimp_from_flows',
    'dom_imp_y_row',
    'scale_and_inflate_dom_imp',
]
