"""Published USA derive entrypoints (A, B, y, C, D).

Two lanes:

- **Published** — functions in this module (``derive_*_usa``). Snapshots,
  waterfall, EF diagnostics, and other callers that want the configured
  electricity-reaggregation switch use these.
- **Deep helpers** — ``bedrock.transform.eeio.usa_matrices``
  (``derive_*``). V, U, VA, x, q, mixed-units A/B, and other
  intermediates with no facade twin.

There is no parallel CEDA / USEEIO full-matrix derive path behind these
facades; they always call the Cornerstone-schema implementation.
"""

from __future__ import annotations

import functools

import pandas as pd

from bedrock.transform.eeio.usa_matrices import (
    derive_Aq_reaggregated,
    derive_Aq_scaled,
    derive_B,
    derive_B_reaggregated,
    derive_Y_and_trade_scaled,
    derive_y_nab,
    derive_y_nab_reaggregated,
    derive_ydom_and_yimp,
)
from bedrock.utils.emissions.characterization import build_ghg_characterization_matrix
from bedrock.utils.emissions.ghg import GHG
from bedrock.utils.schemas.single_region_types import (
    SingleRegionAqMatrixSet,
    SingleRegionYtotAndTradeVectorSet,
    SingleRegionYVectorSet,
)


def _reaggregation_published() -> bool:
    from bedrock.transform.eeio.sector_disagg_pipeline import (  # noqa: PLC0415
        electricity_reaggregation_enabled,
    )

    return electricity_reaggregation_enabled()


def _collapse_commodity_vector(values: pd.Series[float]) -> pd.Series[float]:
    from bedrock.transform.eeio.sector_disagg_pipeline import (  # noqa: PLC0415
        collapse_electricity_children_vector,
    )
    from bedrock.utils.schemas.cornerstone_schemas import (  # noqa: PLC0415
        CORNERSTONE_COMMODITIES,
    )

    return collapse_electricity_children_vector(values, codes=CORNERSTONE_COMMODITIES)


@functools.cache
def derive_B_usa() -> pd.DataFrame:
    """USA B for snapshots / diagnostics.

    With ``implement_electricity_reaggregation``, this is the published 405
    q-weighted collapse (G/T/D → 221100), matching Excel/SEF.
    """
    if _reaggregation_published():
        return derive_B_reaggregated()
    return derive_B()


def derive_Y_and_trade_usa() -> SingleRegionYtotAndTradeVectorSet:
    """Detail Y and trade vectors (summary→detail disaggregate + inflate).

    With ``implement_electricity_reaggregation``, collapses electricity
    children on each vector to match the published 405 commodity set.
    """
    yset = derive_Y_and_trade_scaled()
    if not _reaggregation_published():
        return yset
    return SingleRegionYtotAndTradeVectorSet(
        ytot=_collapse_commodity_vector(yset.ytot),
        exports=_collapse_commodity_vector(yset.exports),
        imports=_collapse_commodity_vector(yset.imports),
    )


@functools.cache
def derive_y_for_national_accounting_balance_usa() -> pd.Series[float]:
    """y for the national accounting balance: y_nab = (y_tot - y_imp) + exports."""
    if _reaggregation_published():
        return derive_y_nab_reaggregated()
    return derive_y_nab()


def derive_ydom_and_yimp_usa() -> SingleRegionYVectorSet:
    """ydom and yimp split, used to populate diagonal/off-diagonal Y_oecd."""
    yset = derive_ydom_and_yimp()
    if not _reaggregation_published():
        return yset
    return SingleRegionYVectorSet(
        ydom=_collapse_commodity_vector(yset.ydom),
        yimp=_collapse_commodity_vector(yset.yimp),
    )


@functools.cache
def derive_Aq_usa() -> SingleRegionAqMatrixSet:
    """Year-scaled and inflated A matrices and q (model_base_year USD).

    With ``implement_electricity_reaggregation``, this is the published 405
    collapse, matching Excel/SEF and snapshot parquets.
    """
    if _reaggregation_published():
        return derive_Aq_reaggregated()
    return derive_Aq_scaled()


@functools.cache
def derive_C_usa() -> pd.DataFrame:
    """Trivial `(1, |GHG|)` row-summer for cornerstone B (already in CO2e).

    KNOWN DIVERGENCE FROM USEEIOR: bedrock B is in `kgCO2e/USD` while
    useeior B is in physical `kg gas/USD`. See
    `bedrock.utils.emissions.characterization` for the resolution path.
    """
    return build_ghg_characterization_matrix(list(GHG))


@functools.cache
def derive_D_usa() -> pd.DataFrame:
    """Direct impact per commodity = `C @ B` -- here equivalent to `B.sum(axis=0)`.

    KNOWN DIVERGENCE FROM USEEIOR: bedrock's D is structurally a single
    `Greenhouse Gases` indicator over commodities because bedrock's B is
    already CO2e. See `bedrock.utils.emissions.characterization`.
    """
    D = derive_C_usa() @ derive_B_usa()
    D.columns.name = 'sector'
    return D
