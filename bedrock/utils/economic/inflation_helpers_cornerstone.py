"""Cornerstone-specific inflation helpers.

Mirrors inflate_to_target_year.py but reindexes the CEDA v7 price index to
cornerstone commodity codes (405).

Codes that exist only in cornerstone and were disaggregated from a CEDA v7 parent
(e.g. waste 562111 ← 562000) inherit the parent's price ratio.  Codes with no
identifiable parent (e.g. S00402 used goods) receive a neutral ratio of 1.0.
"""

from __future__ import annotations

import functools
import typing as ta

_StrKey = ta.TypeVar("_StrKey", bound=str)

import numpy as np
import pandas as pd

from bedrock.transform.iot.derived_price_index import derive_industry_price_index
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.economic.inflation_helpers_ceda import (
    obtain_inflation_factors_from_reference_data,
)
from bedrock.utils.math.formulas import (
    compute_commodity_mix_matrix,
    compute_Vnorm_matrix,
    compute_x,
)
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_GROSS_INDUSTRY_OUTPUT_YEARS
from bedrock.utils.taxonomy.bea.v2017_commodity_sector import (
    BEA_2017_SECTOR_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_industry_summary import (
    USA_2017_SUMMARY_INDUSTRY_CODES,
)
from bedrock.utils.taxonomy.bea_v2017_to_ceda_v7_helpers import (
    load_bea_v2017_summary_to_cornerstone,
)
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITIES
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRIES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__cornerstone_commodity import (
    load_bea_v2017_commodity_to_cornerstone_commodity,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_sector__cornerstone_commodity import (
    load_bea_v2017_sector_commodity_to_cornerstone_commodity,
)

CORNERSTONE_INDUSTRIES: list[str] = list(INDUSTRIES)
CORNERSTONE_COMMODITIES: list[str] = list(COMMODITIES)


@functools.cache
def _cornerstone_to_ceda_v7_parent() -> dict[str, str]:
    """Map cornerstone-only codes back to their CEDA v7 parent for price lookup.

    Built from the BEA 2017 → cornerstone commodity mapping.  For any BEA code
    that maps to multiple cornerstone codes, each child inherits the BEA code
    (which is also the CEDA v7 code) as its price-index parent.  1:1 mapped
    codes already exist in the CEDA v7 index and need no override.
    """
    mapping = load_bea_v2017_commodity_to_cornerstone_commodity()
    parent: dict[str, str] = {}
    for bea_code, corner_codes in mapping.items():
        if len(corner_codes) > 1:
            for child in corner_codes:
                parent[child] = bea_code
    return parent


@functools.cache
def get_cornerstone_industry_price_ratio(
    original_year: int, target_year: int
) -> pd.Series[float]:
    """Price ratio reindexed to cornerstone industry codes.

    Cornerstone-only child codes (e.g. waste subsectors) inherit their CEDA v7
    parent's price ratio so that inflation is applied consistently.
    """
    cfg = get_usa_config()
    if cfg.update_inflation_factors:
        price_index = derive_industry_price_index()
        target_codes = CORNERSTONE_INDUSTRIES
    else:
        # Reindex to commodities so downstream `diag(p) @ A @ diag(1/p)`
        # aligns positionally against a commodity-indexed A. INDUSTRIES and
        # COMMODITIES order diverges at 351/405 positions, so the target
        # list is load-bearing despite the function's name.
        price_index = obtain_inflation_factors_from_reference_data()
        target_codes = CORNERSTONE_COMMODITIES
    pi_ratio: pd.Series[float] = price_index[target_year] / price_index[original_year]

    # Start with direct reindex (codes shared with CEDA v7 get their own ratio)
    ratio = pi_ratio.reindex(target_codes, fill_value=np.nan)

    # Fill cornerstone-only children with their CEDA v7 parent's ratio
    parent_map = _cornerstone_to_ceda_v7_parent()
    for child, parent_code in parent_map.items():
        if child in ratio.index and pd.isna(ratio[child]):
            if parent_code in pi_ratio.index:
                ratio[child] = pi_ratio[parent_code]

    # Anything still NaN (truly no parent, e.g. S00402) gets neutral 1.0
    ratio = ratio.fillna(1.0)
    return ratio


def inflate_cornerstone_A_matrix_with_industry_pi(
    A: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_ratio = get_cornerstone_industry_price_ratio(original_year, target_year)
    return pd.DataFrame(
        (np.diag(price_ratio) @ A @ np.diag(1 / price_ratio)).values,
        index=A.index,
        columns=A.columns,
    )


def inflate_cornerstone_q_or_y_with_industry_pi(
    q_or_y: pd.Series[float], original_year: int, target_year: int
) -> pd.Series[float]:
    price_ratio = get_cornerstone_industry_price_ratio(original_year, target_year)
    return q_or_y * price_ratio.reindex(q_or_y.index, fill_value=1.0)


@functools.cache
def get_vnorm_adjusted_commodity_price_ratio(
    original_year: int, target_year: int
) -> pd.Series[float]:
    """V-norm-weighted commodity price ratio.

    The price index is industry-level. ``get_cornerstone_industry_price_ratio``
    reindexes those industry ratios onto cornerstone commodity codes 1:1,
    treating each industry as its own primary commodity. This helper instead
    produces a commodity ratio that reflects the actual mix of industries
    supplying each commodity, weighted by V_norm:

        r_com[j] = sum_i V_norm[i, j] * r_ind[i]

    i.e. a column-weighted average of industry price ratios across the
    industries that supply commodity j. This is a ratio-of-ratios
    approximation of the (theoretically purer) ratio of V-norm-aggregated
    price levels; the two coincide when industry prices are uniform within
    a commodity's supplying mix.

    V is inflated to ``cfg.model_base_year`` when ``cfg.apply_inflation_to_V``
    is set; the V-norm weights then reflect supplier mix at the model year
    rather than at ``cfg.usa_base_io_data_year``.
    """
    # local import to avoid a circular dependency on transform.eeio
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Vnorm_scrap_corrected,
    )

    cfg = get_usa_config()
    industry_ratio = get_cornerstone_industry_price_ratio(original_year, target_year)
    Vnorm = derive_cornerstone_Vnorm_scrap_corrected(
        apply_inflation=cfg.apply_inflation_to_V,
        target_year=cfg.model_base_year,
    )
    aligned = industry_ratio.reindex(Vnorm.index, fill_value=1.0)

    # Normalize V_norm columns to sum to 1 so the dot-product is a true weighted
    # *average*, not a weighted *sum*. Scrap correction in
    # `derive_cornerstone_Vnorm_scrap_corrected` is a row-axis scaling that
    # leaves column sums slightly > 1 — using the un-renormalized form would
    # produce a non-1 ratio at year=year (off by the column-sum drift, ~5–7%).
    # Done locally here rather than upstream because A/B matrix consumers want
    # the un-renormalized scrap-corrected V_norm.
    column_sums = Vnorm.sum(axis=0)
    weights = Vnorm.divide(column_sums.where(column_sums > 1e-9, 1.0), axis=1)
    commodity_ratio = aligned @ weights

    # Commodities with no industry coverage (V_norm column ≈ 0, e.g. S00402
    # used goods) yield a 0 weighted-average, which would inject zeros into
    # diag(p) @ A @ diag(1/p). Fall back to the industry ratio, mirroring the
    # neutral 1.0 default in get_cornerstone_industry_price_ratio.
    no_coverage = column_sums < 1e-9
    fallback = industry_ratio.reindex(commodity_ratio.index, fill_value=1.0)
    commodity_ratio = commodity_ratio.where(~no_coverage, fallback)

    return commodity_ratio.reindex(CORNERSTONE_COMMODITIES, fill_value=1.0)


def inflate_cornerstone_A_matrix_with_commodity_pi(
    A: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    """Same `diag(p) @ A @ diag(1/p)` form as ``inflate_cornerstone_A_matrix``,
    but with the V-norm-derived commodity price ratio.
    """
    price_ratio = get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)
    return pd.DataFrame(
        (np.diag(price_ratio) @ A @ np.diag(1 / price_ratio)).values,
        index=A.index,
        columns=A.columns,
    )


def inflate_cornerstone_q_or_y_with_commodity_pi(
    q_or_y: pd.Series[float], original_year: int, target_year: int
) -> pd.Series[float]:
    price_ratio = get_vnorm_adjusted_commodity_price_ratio(original_year, target_year)
    return q_or_y * price_ratio.reindex(q_or_y.index, fill_value=1.0)


def inflate_cornerstone_B_matrix_with_industry_pi(
    B: pd.DataFrame, original_year: int, target_year: int
) -> pd.DataFrame:
    price_ratio = get_cornerstone_industry_price_ratio(target_year, original_year)
    return B * price_ratio.reindex(B.columns, fill_value=1.0).values


# ---------------------------------------------------------------------------
# Summary-level commodity price ratio + summary A/q dollar-year adjustment
#
# Used to rebase a BEA summary A (or q) from one dollar-year basis to another
# before forming the structural cross-year ratio in scale_cornerstone_A /
# scale_cornerstone_q. Without this, the ratio mixes inter-sector relative
# price changes with real input-share changes.
#
# The adjustment is direction-agnostic: deflating when from_year > to_year,
# inflating when from_year < to_year.
#
# Summary commodity PI is built ITA-style (Industry-Technology Assumption):
# the 2017 commodity-mix matrix C_m = V/x is held fixed, and BEA's published
# year-y industry gross output drives year-specificity for q and V_norm. See
# `derive_cornerstone_q_and_vnorm_for_year` below.
# ---------------------------------------------------------------------------


def adjust_summary_A_dollar_year(
    A_summary: pd.DataFrame,
    from_year: int,
    to_year: int,
) -> pd.DataFrame:
    """Rebase a summary A matrix from ``from_year`` USD to ``to_year`` USD via
    ``diag(1/p) @ A @ diag(p)``, where ``p`` is the summary commodity price
    ratio (from_year → to_year).

    Real A: ``A_to[i, j] = A_from[i, j] * p[j] / p[i]`` — row-axis is input
    deflation, column-axis is output re-inflation, so the ratio of deflated
    input to deflated output recovers a real-quantity coefficient.

    Direction-agnostic: deflates when from_year > to_year, inflates when
    from_year < to_year.
    """
    p = get_summary_commodity_price_ratio(original_year=to_year, target_year=from_year)
    p_row = np.asarray(p.reindex(A_summary.index, fill_value=1.0).to_numpy(dtype=float))
    p_col = np.asarray(
        p.reindex(A_summary.columns, fill_value=1.0).to_numpy(dtype=float)
    )
    return pd.DataFrame(
        np.diag(1.0 / p_row) @ A_summary.to_numpy() @ np.diag(p_col),
        index=A_summary.index,
        columns=A_summary.columns,
    )


def adjust_summary_q_dollar_year(
    q_summary: pd.Series[float],
    from_year: int,
    to_year: int,
) -> pd.Series[float]:
    """Rebase a summary q (commodity gross output) vector from ``from_year``
    USD to ``to_year`` USD by elementwise division by the summary commodity
    price ratio.

    Direction-agnostic: deflates when from_year > to_year, inflates when
    from_year < to_year.
    """
    p = get_summary_commodity_price_ratio(original_year=to_year, target_year=from_year)
    return q_summary / p.reindex(q_summary.index, fill_value=1.0)


@functools.cache
def get_summary_commodity_price_ratio(
    original_year: int, target_year: int
) -> pd.Series[float]:
    """Cross-year ratio of summary commodity price indices, indexed on
    ``USA_2017_SUMMARY_INDUSTRY_CODES`` (commodity and industry summary codes
    overlap at BEA 2017 summary granularity).

    Built as the ratio of two ITA-based Paasche summary commodity PIs (see
    ``get_summary_commodity_price_index``).
    """
    pi_orig = get_summary_commodity_price_index(original_year)
    pi_targ = get_summary_commodity_price_index(target_year)
    ratio = pi_targ / pi_orig
    return ratio.where(np.isfinite(ratio), 1.0).fillna(1.0)


def _aggregate_commodity_pi(
    pi_com_detail: pd.Series,
    q_y: pd.Series,
    code_to_children: ta.Mapping[_StrKey, ta.Sequence[str]],
) -> dict[str, float]:
    """q-weighted mean of cornerstone commodity PI over each group's children.

    Falls back to an unweighted mean when all children have zero q weight.
    Groups with no children present in ``pi_com_detail`` are omitted.
    """
    out: dict[str, float] = {}
    for code, children in code_to_children.items():
        children_in_idx = [c for c in children if c in pi_com_detail.index]
        if not children_in_idx:
            continue
        w = q_y.reindex(children_in_idx, fill_value=0.0)
        p = pi_com_detail.reindex(children_in_idx, fill_value=100.0)
        wsum = float(w.sum())
        out[str(code)] = float((p * w).sum() / wsum) if wsum > 0 else float(p.mean())
    return out


def _cornerstone_commodity_pi_for_year(year: int) -> tuple[pd.Series, pd.Series]:
    """Return ``(q_y, pi_com_detail)`` — the two inputs needed for aggregation.

    Shared by ``get_summary_commodity_price_index`` and
    ``get_sector_commodity_price_index``.
    """
    pi_year_industry = _cornerstone_indexed_industry_pi(year)
    q_y, V_norm_y = derive_cornerstone_q_and_vnorm_for_year(year)
    pi_ind_aligned = pi_year_industry.reindex(V_norm_y.index, fill_value=100.0)
    pi_com_detail = V_norm_y.T @ pi_ind_aligned  # (cornerstone commodities,)
    return q_y, pi_com_detail


@functools.cache
def get_summary_commodity_price_index(year: int) -> pd.Series[float]:
    """Summary commodity price index for ``year``, ITA-based Paasche
    construction. Indexed on ``USA_2017_SUMMARY_INDUSTRY_CODES``.

    Construction::

        PI_industry_detail[y]      ← cornerstone-reindexed PI for year y
        PI_commodity_detail[y]     = V_norm[y].T @ PI_industry_detail[y]
        PI_commodity_summary[K, y] = (q[y]-weighted mean of PI_commodity_detail
                                      over cornerstone children of K)

    `V_norm[y]` and `q[y]` come from ``derive_cornerstone_q_and_vnorm_for_year``
    — constructed under ITA (2017 commodity-mix matrix held fixed, year-y
    industry GO drives year-specificity). See that helper's docstring for what's
    frozen vs. year-specific.

    Fallbacks: summary codes with no cornerstone children fill 100 (neutral
    flat trajectory → ratio = 1 against any other year's neutral fill);
    summary blocks where all children have zero ``q[y]`` weight fall back to
    an unweighted mean of children's PI.
    """
    q_y, pi_com_detail = _cornerstone_commodity_pi_for_year(year)
    out = _aggregate_commodity_pi(
        pi_com_detail, q_y, load_bea_v2017_summary_to_cornerstone()
    )
    return pd.Series(out, dtype=float).reindex(
        USA_2017_SUMMARY_INDUSTRY_CODES, fill_value=100.0
    )


@functools.cache
def get_sector_commodity_price_index(year: int) -> pd.Series[float]:
    """Sector-level commodity price index for ``year``, same ITA-based Paasche
    construction as ``get_summary_commodity_price_index`` but aggregated to
    BEA 2017 sector codes. Indexed on ``BEA_2017_SECTOR_COMMODITY_CODES``.

    Useful for inflating margin components: Transportation (48TW),
    Wholesale (42), Retail (44RT).
    """
    q_y, pi_com_detail = _cornerstone_commodity_pi_for_year(year)
    out = _aggregate_commodity_pi(
        pi_com_detail, q_y, load_bea_v2017_sector_commodity_to_cornerstone_commodity()
    )
    return pd.Series(out, dtype=float).reindex(
        BEA_2017_SECTOR_COMMODITY_CODES, fill_value=100.0
    )


@functools.cache
def get_sector_commodity_price_ratio(
    original_year: int, target_year: int
) -> pd.Series[float]:
    """Cross-year ratio of sector commodity price indices, indexed on
    ``BEA_2017_SECTOR_COMMODITY_CODES``.

    Built as the ratio of two ITA-based Paasche sector commodity PIs (see
    ``get_sector_commodity_price_index``).
    """
    pi_orig = get_sector_commodity_price_index(original_year)
    pi_targ = get_sector_commodity_price_index(target_year)
    ratio = pi_targ / pi_orig
    return ratio.where(np.isfinite(ratio), 1.0).fillna(1.0)


@functools.cache
def derive_cornerstone_q_and_vnorm_for_year(
    year: int,
) -> tuple[pd.Series[float], pd.DataFrame]:
    """Year-y ``(q, V_norm)`` at cornerstone detail granularity.

    Constructed under the **Industry-Technology Assumption (ITA)**: each
    industry's commodity-output split (the commodity-mix matrix
    ``C_m = V / x``) is held constant at 2017; year-y industry size ``x[y]``
    (BEA's published gross-output time series) drives year-specificity. This
    is the only way to get year-y ``q`` / ``V_norm`` at detail granularity
    because BEA only publishes detail Make tables for benchmark years.
    Identity used::

        q[y] = C_m[2017] @ x[y]      (exact when C_m, x are from the same V;
                                       used in eeio_diagnostics.py:414)

    V[y] is reconstructed as ``C_m[2017].T · diag(x[y])`` (i.e., each industry
    row of 2017's V is scaled by ``x[y, i] / x[2017, i]``), and V_norm[y] is
    then ``V[y] / q[y]`` (column-normalized, market shares).

    Returns ``(q_y, V_norm_y)`` with cornerstone commodity / industry indices.
    At ``year == cfg.usa_base_io_data_year`` both reduce to their 2017
    counterparts (regression-tested).
    """
    # Local import: derived_cornerstone imports from this module, mirroring the
    # existing pattern at the top of `get_vnorm_adjusted_commodity_price_ratio`.
    from bedrock.transform.eeio.cornerstone_expansion import (  # noqa: PLC0415
        CS_INDUSTRY_LIST,
        cs_industry_to_bea_map,
        expand_vector,
    )
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        _distribute_waste_parent_x_using_v_row_shares,
        derive_cornerstone_V,
    )
    from bedrock.transform.iot.derived_gross_industry_output import (  # noqa: PLC0415
        derive_gross_output,
    )

    cfg = get_usa_config()
    V_2017 = derive_cornerstone_V()
    x_2017 = compute_x(V=V_2017)
    # C_m: commodity × industry, each row sums to 1 (within-industry split).
    C_m = compute_commodity_mix_matrix(V=V_2017, x=x_2017)

    x_bea_y = derive_gross_output(
        # `derive_gross_output` types target_year as the Literal of supported
        # years; this helper's caller passes a runtime int, so we cast.
        target_year=ta.cast(USA_GROSS_INDUSTRY_OUTPUT_YEARS, year),
        iot_before_or_after_redefinition=cfg.iot_before_or_after_redefinition,
    )
    x_y = expand_vector(x_bea_y, CS_INDUSTRY_LIST, cs_industry_to_bea_map())
    # When waste disagg is on, expand_vector duplicates the 562000 parent GO
    # across waste children; redistribute via 2017 V row shares so the
    # disaggregated industries get the right share. No-op when waste disagg
    # is off (the helper short-circuits on missing weights).
    x_y = _distribute_waste_parent_x_using_v_row_shares(x_y)
    x_y = x_y.reindex(x_2017.index, fill_value=0.0)

    q_y = C_m @ x_y
    q_y = q_y.reindex(V_2017.columns, fill_value=0.0)
    V_y = C_m.T.mul(x_y, axis=0).reindex(
        index=V_2017.index, columns=V_2017.columns, fill_value=0.0
    )
    V_norm_y = compute_Vnorm_matrix(V=V_y, q=q_y)
    return q_y, V_norm_y


@functools.cache
def _cornerstone_indexed_industry_pi(year: int) -> pd.Series[float]:
    """Cornerstone-industry-indexed PI for one year, with CEDA-v7-parent
    fallback for cornerstone-only codes (mirrors the per-year half of
    ``get_cornerstone_industry_price_ratio``).

    Always indexed on ``CORNERSTONE_INDUSTRIES`` regardless of
    ``update_inflation_factors`` (V_norm.T @ pi_industry in the ITA flow needs
    industry granularity; the existing dispatch's commodity branch returns
    commodity-indexed values for the legacy ``diag(p) @ A @ diag(1/p)`` flow,
    which we don't want here).

    Codes with no parent in the upstream PI fall back to 100 (BEA convention
    2017 = 100); any ratio against another year that also falls back is 1.0.
    """
    cfg = get_usa_config()
    price_index = (
        derive_industry_price_index()
        if cfg.update_inflation_factors
        else obtain_inflation_factors_from_reference_data()
    )

    pi_year: pd.Series[float] = price_index[year]
    series = pi_year.reindex(CORNERSTONE_INDUSTRIES).astype(float)
    parent_map = _cornerstone_to_ceda_v7_parent()
    for child, parent_code in parent_map.items():
        if (
            child in series.index
            and pd.isna(series.loc[child])
            and parent_code in pi_year.index
        ):
            series.loc[child] = float(pi_year.loc[parent_code])
    return series.fillna(100.0)
