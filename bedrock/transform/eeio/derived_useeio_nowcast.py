"""Cornerstone A matrices derived from the upstream USEEIO nowcast.

Loaders live in [bedrock.extract.iot.useeio_nowcast](../../extract/iot/useeio_nowcast.py).
This module mirrors ``derive_cornerstone_Aq()``'s waste-disagg-aware path
applied to USEEIO's year-specific V/U:

1. Map BEA-detail V/U/Uimp to Cornerstone schema via
   ``industry_corresp() @ V @ commodity_corresp().T`` (and the U analog).
2. If ``implement_waste_disaggregation`` is enabled, apply
   ``apply_waste_disagg_to_V`` and ``apply_waste_disagg_to_U`` — both use
   the 2017 benchmark weights (``WasteDisaggregationDetail2017``) to split
   the BEA ``562000`` row/col into 5 Cornerstone child codes.
3. Clip GRAS reconciliation negatives in U to 0 (~0.27% of ``|U_dom|``;
   magnitude logged).
4. Build Vnorm with year-specific scrap correction (``scrap / x`` from the
   year's V, mapped to Cornerstone industries).
5. Compute ``A = Unorm @ Vnorm`` per matrix; apply the 0.98 column cap.
6. Return year-specific q derived directly from the year's V (Cornerstone
   space).

Settled policies (see [implement_useeio_nowcast_plan.md](../../analysis/a_matrix_time_series/docs/implement_useeio_nowcast_plan.md)):

- **2017 identity vs bedrock's BEA Aq is NOT enforced.** USEEIO and bedrock load
  different BEA 2017 vintages; expect ~9-22% L1 deviation.
- **Negatives in intermediate U are clipped to 0** (reconciliation noise).
- **0.98 column cap is applied per matrix** (Adom, Aimp separately).
- **Year-specific Vnorm + q** — V from each year drives both, so input
  shares AND commodity output reflect the year's nowcasted Make.
- **Waste disagg uses 2017 weights** when ``implement_waste_disaggregation``
  is True — splits ``562000`` into 5 Cornerstone children with the same
  weights the other approaches use.
"""

from __future__ import annotations

import functools
import logging

import pandas as pd
import pandera.typing as pt

from bedrock.extract.iot.useeio_nowcast import (
    USEEIO_NOWCAST_YEARS,
    load_useeio_nowcast_Uimp_intermediate_usa,
    load_useeio_nowcast_Utot_intermediate_usa,
    load_useeio_nowcast_V_usa,
)
from bedrock.transform.eeio.cornerstone_expansion import (
    commodity_corresp,
    industry_corresp,
)
from bedrock.transform.eeio.waste_disaggregation import (
    apply_waste_disagg_to_U,
    apply_waste_disagg_to_V,
)
from bedrock.utils.math.formulas import (
    compute_A_matrix,
    compute_q,
    compute_Unorm_matrix,
    compute_Vnorm_matrix,
    compute_x,
)
from bedrock.utils.schemas.cornerstone_schemas import CornerstoneAMatrix
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet

logger = logging.getLogger(__name__)

COLUMN_CAP = 0.98  # matches bedrock.transform.eeio.cornerstone_year_scaling


# =========================================================================
# Public API
# =========================================================================


@functools.cache
def derive_useeio_nowcast_Aq_cornerstone(year: int) -> SingleRegionAqMatrixSet:
    """Cornerstone-space (Adom, Aimp, q) for the given nowcast year.

    Available years: ``USEEIO_NOWCAST_YEARS`` (2017–2023). 2024 raises.

    Cached because ``derive_cornerstone_Aq_scaled()`` calls this from a
    ``@functools.cache``-decorated function; the analysis driver clears
    the cache between (approach, year) iterations.
    """
    if year not in USEEIO_NOWCAST_YEARS:
        raise ValueError(
            f"USEEIO nowcast not available for {year}. "
            f"Available years: {USEEIO_NOWCAST_YEARS}"
        )

    # Lazy import — ``get_waste_disagg_weights`` lives in derived_cornerstone,
    # which transitively pulls flowsa via allocation.derived at module import.
    # Keeping this lazy lets the analysis driver populate parquets without
    # initializing the flowsa log handler at module-import time.
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        get_waste_disagg_weights,
    )

    # 1. Load USEEIO inputs (BEA detail). U_out includes 3 VA rows
    # (V00100/V00200/V00300) that U_imports_out doesn't — align both to the
    # commodity axis of V (402 BEA detail commodities) before subtracting.
    V_bea = load_useeio_nowcast_V_usa(year)
    U_int_bea = load_useeio_nowcast_Utot_intermediate_usa(year)
    Uimp_int_bea = load_useeio_nowcast_Uimp_intermediate_usa(year)
    commodity_idx = V_bea.columns
    industry_idx = V_bea.index
    U_int_bea = U_int_bea.reindex(
        index=commodity_idx, columns=industry_idx, fill_value=0.0
    )
    Uimp_int_bea = Uimp_int_bea.reindex(
        index=commodity_idx, columns=industry_idx, fill_value=0.0
    )

    # 2. Map BEA detail → Cornerstone schema via correspondence multiplication
    # (mirrors derive_cornerstone_V / derive_cornerstone_U_with_negatives).
    ind_corresp = industry_corresp()
    com_corresp = commodity_corresp()
    V_cs = ind_corresp @ V_bea @ com_corresp.T
    Udom_bea = U_int_bea - Uimp_int_bea
    Udom_cs = com_corresp @ Udom_bea @ ind_corresp.T
    Uimp_cs = com_corresp @ Uimp_int_bea @ ind_corresp.T
    for df in (V_cs, Udom_cs, Uimp_cs):
        df.index.name = "sector"
        df.columns.name = "sector"

    # 3. Apply waste disagg if enabled (mirrors derive_cornerstone_V +
    # derive_cornerstone_U_with_negatives). Uses 2017 benchmark weights.
    weights = get_waste_disagg_weights()
    if weights is not None:
        V_cs = apply_waste_disagg_to_V(V_cs, weights)
        Udom_cs, Uimp_cs = apply_waste_disagg_to_U(Udom_cs, Uimp_cs, weights)
        for df in (V_cs, Udom_cs, Uimp_cs):
            df.index.name = "sector"
            df.columns.name = "sector"

    # 4. Log negatives, clip to 0.
    _log_and_clip_negatives(year, Udom_cs, Uimp_cs)
    Udom_cs = Udom_cs.clip(lower=0)
    Uimp_cs = Uimp_cs.clip(lower=0)

    # 5. Vnorm with year-specific scrap correction (mirrors
    # derive_cornerstone_Vnorm_scrap_corrected, but with year-specific V).
    x = compute_x(V=V_cs)
    q = compute_q(V=V_cs)
    Vnorm = compute_Vnorm_matrix(V=V_cs, q=q)
    scrap_bea = V_bea.loc[:, "S00401"]
    scrap_fraction = ind_corresp @ scrap_bea
    Vnorm = Vnorm.divide((1.0 - (scrap_fraction / x).fillna(0.0)), axis=0)

    # 6. A = U_norm @ Vnorm, per matrix.
    Adom = compute_A_matrix(U_norm=compute_Unorm_matrix(U=Udom_cs, x=x), V_norm=Vnorm)
    Aimp = compute_A_matrix(U_norm=compute_Unorm_matrix(U=Uimp_cs, x=x), V_norm=Vnorm)

    # 7. Column cap.
    Adom = _apply_column_cap(Adom, label=f"Adom_{year}")
    Aimp = _apply_column_cap(Aimp, label=f"Aimp_{year}")

    assert not Adom.isna().values.any(), f"Adom has NaN at year={year}"
    assert not Aimp.isna().values.any(), f"Aimp has NaN at year={year}"
    assert (Adom.values >= 0).all(), f"Adom has negatives at year={year}"
    assert (Aimp.values >= 0).all(), f"Aimp has negatives at year={year}"

    return SingleRegionAqMatrixSet(
        Adom=pt.DataFrame[CornerstoneAMatrix](Adom),  # type: ignore[arg-type]
        Aimp=pt.DataFrame[CornerstoneAMatrix](Aimp),  # type: ignore[arg-type]
        scaled_q=q,
    )


# =========================================================================
# Private helpers
# =========================================================================


def _log_and_clip_negatives(
    year: int, U_dom: pd.DataFrame, Uimp_int: pd.DataFrame
) -> None:
    """Report the magnitude of negative cells before they get clipped to 0."""
    n_neg_dom = int((U_dom < 0).sum().sum())
    n_neg_imp = int((Uimp_int < 0).sum().sum())
    total_abs = float(U_dom.abs().sum().sum())
    neg_abs = float(U_dom.where(U_dom < 0).abs().sum().sum())
    pct = 100.0 * neg_abs / total_abs if total_abs else 0.0
    logger.info(
        "[%d] U_dom negatives: %d cells, |sum|=%.3e (%.4f%% of |U_dom|); "
        "U_imp negatives: %d cells. Clipping all to 0.",
        year,
        n_neg_dom,
        neg_abs,
        pct,
        n_neg_imp,
    )


def _apply_column_cap(A: pd.DataFrame, label: str) -> pd.DataFrame:
    """Cap any column whose sum > 1 down to ``COLUMN_CAP``, mirroring
    ``scale_cornerstone_A``'s post-processing."""
    A = A.copy()
    col_sums = A.sum(axis=0)
    over = col_sums[col_sums > 1.0].sort_values(ascending=False)
    if len(over):
        logger.info(
            "[%s] %d column(s) > 1.0 capped to %.2f: %s",
            label,
            len(over),
            COLUMN_CAP,
            over.head(5).to_dict(),
        )
        for col, total in over.items():
            A[col] *= COLUMN_CAP / total
    return A
