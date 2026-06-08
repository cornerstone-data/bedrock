"""Excel publisher for the bedrock EEIO model.

Mirrors the shape of `useeior::writeModeltoXLSX` -- one workbook with one
sheet per model object plus metadata sheets. Each registry entry pairs a
sheet name with a getter callable that returns the object to write; if a
getter returns `None`, the sheet is omitted (matches `WriteModel.R` "skip
if NULL" behavior).

KNOWN DIVERGENCE FROM USEEIOR
-----------------------------
Bedrock `B` is in `kgCO2e / USD` (AR6 GWPs already baked in upstream at
FBS aggregation in `bedrock.transform.allocation.derived`). Useeior `B`
is in physical `kg gas / USD`, with a real GWP `C` matrix applying
characterization.

Practical consequences:
  * The bedrock `B` sheet is NOT numerically comparable to the useeior
    `B` sheet -- they have different row dimensions AND different
    per-row units.
  * `M` and `M_d` are `B @ L` / `B @ L_d`, so they inherit B's
    `kgCO2e / USD` units. They are NOT numerically comparable to
    useeior's physical-mass `M` / `M_d` either.
  * The bedrock `C`, `D`, `N`, `N_d` sheets are emitted for sheet-name
    parity only. `C` is a trivial row-summer (single "Greenhouse Gases"
    indicator over the 7 GHGs, all values = 1.0), so `D = C @ B`
    reduces to `B.sum(axis=0)`, `N = C @ M` to `M.sum(axis=0)`, and
    `N_d = C @ M_d` to `M_d.sum(axis=0)`. See
    `bedrock/utils/emissions/characterization.py` for resolution paths.

The `model_info` sheet carries `b_units`, `b_characterized`, `gwp_set`,
`c_kind`, and `divergence_from_useeior` fields so downstream consumers
cannot mistake bedrock B for useeior B.

Location suffix
---------------
Following useeior's convention, BEA-style numeric codes (e.g. `211000`)
are emitted with a `/US` location suffix so Excel does not coerce them
to integers (and so the codes are unambiguous in MRIO settings).
`_with_loc_suffix` is applied automatically to any axis whose
`Index.name == 'sector'`. The extended-U axes carry mixed
sector/VA/final-demand codes; we tag them as `'sector'` anyway so the
suffix applies uniformly (matches useeior's effective behavior).

Extended U / U_d block layout
-----------------------------
`U` mirrors useeior's extended use matrix -- value-added rows
(`V00100..V00300`) below the commodity-by-industry intermediate block,
and final-demand columns (`F01000..F10S00`) to the right of it. The
VA x FD corner is zero (structural padding). The VA block is identical
between U and U_d since value added is intrinsically domestic.

`U_d`'s FD block is currently truncated: bedrock has no `Ydom` matrix
at FD-category resolution (only the `ydom` vector via
`derive_cornerstone_ydom_and_yimp`), so we emit `U_d` as
`(commodity + VA) x industry` only -- VA rows but no FD columns. The
`model_info` sheet carries a `u_d_extended` field so consumers see
this. Future work: add a `derive_cornerstone_Ydom_matrix()` and
re-extend `U_d` to match `U`'s shape.
"""

# ruff: noqa: PLC0415
from __future__ import annotations

import datetime
import functools
import logging
import os
from collections.abc import Callable, Iterable
from typing import NamedTuple

import pandas as pd

from bedrock.utils.config.settings import GIT_HASH_LONG
from bedrock.utils.config.usa_config import get_usa_config
from bedrock.utils.math.formulas import compute_L_matrix, compute_M_matrix

logger = logging.getLogger(__name__)

# Useeior-style location suffix. Keeps numeric BEA codes string-typed in
# Excel and matches useeior workbook conventions.
_LOCATION: str = 'US'


class SheetSpec(NamedTuple):
    name: str
    getter: Callable[[], pd.DataFrame | pd.Series | None]


def _with_loc_suffix(idx: pd.Index) -> pd.Index:
    """Append `/US` to a sector-named axis; otherwise return unchanged.

    Detection by `Index.name` keeps non-sector axes (e.g. `ghg`,
    `indicator`) untouched.
    """
    if idx.name != 'sector':
        return idx
    out = [f'{str(v)}/{_LOCATION}' for v in idx]
    return pd.Index(out, name=idx.name)


def _apply_loc_suffix(obj: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
    out = obj.copy()
    if isinstance(out, pd.DataFrame):
        out.index = _with_loc_suffix(out.index)
        out.columns = _with_loc_suffix(out.columns)
    else:
        out.index = _with_loc_suffix(out.index)
    return out


def _build_config_summary_df(config_name: str) -> pd.DataFrame:
    """USAConfig -> long-form (config_field, value) DataFrame."""
    return get_usa_config().to_dataframe(config_name=config_name)


def _build_model_info_df(config_name: str) -> pd.DataFrame:
    """Workbook-level metadata, including the bedrock-vs-useeior divergence flags."""
    cfg = get_usa_config()
    rows: list[dict[str, object]] = [
        {'field': 'config_name', 'value': config_name},
        {'field': 'bedrock_git_sha', 'value': GIT_HASH_LONG or 'unknown'},
        {
            'field': 'published_at',
            'value': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        },
        {'field': 'model_base_year', 'value': cfg.model_base_year},
        {'field': 'usa_io_data_year', 'value': cfg.usa_io_data_year},
        {'field': 'usa_ghg_data_year', 'value': cfg.usa_ghg_data_year},
        {'field': 'ipcc_ar_version', 'value': cfg.ipcc_ar_version},
        {'field': 'location_suffix', 'value': _LOCATION},
        # Divergence-from-useeior fields. DO NOT REMOVE without resolving the
        # underlying semantics gap -- see writer module docstring and
        # bedrock/utils/emissions/characterization.py.
        {'field': 'b_units', 'value': 'kg CO2e / USD'},
        {'field': 'b_characterized', 'value': True},
        {'field': 'gwp_set', 'value': 'AR6'},
        {
            'field': 'c_kind',
            'value': (
                'trivial_row_summer (bedrock B is pre-characterized; '
                'useeior-shape parity only)'
            ),
        },
        {
            'field': 'divergence_from_useeior',
            'value': (
                'B is in CO2e (pre-characterized); useeior B is in physical '
                'kg of gas. M and M_d inherit those kgCO2e/USD units '
                '(M = B @ L). C is a trivial row-summer so D, N, N_d collapse '
                'to a single "Greenhouse Gases" indicator equal to '
                'B.sum(axis=0) / M.sum(axis=0) / M_d.sum(axis=0) '
                'respectively. Resolve before treating bedrock XLSX as a '
                'useeior drop-in. See '
                'bedrock/utils/emissions/characterization.py.'
            ),
        },
        {
            'field': 'u_extended',
            'value': (
                'full: VA rows (V001/V002/V003) and FD cols (F01000..F10S00) '
                'attached to commodity-by-industry intermediate. VA x FD '
                'corner is zero (structural padding). Matches useeior `U`.'
            ),
        },
        {
            'field': 'u_d_extended',
            'value': (
                'partial: VA rows included, FD cols truncated. Bedrock has '
                'no Ydom matrix at FD-category resolution today (only the '
                'ydom vector via derive_cornerstone_ydom_and_yimp). U_d '
                'shape diverges from useeior U_d until a derive_cornerstone'
                '_Ydom_matrix exists. See writer module docstring.'
            ),
        },
    ]
    return pd.DataFrame(rows)


def _build_commodities_meta_df() -> pd.DataFrame:
    from bedrock.utils.taxonomy.cornerstone.commodities import (
        COMMODITIES,
        COMMODITY_DESC,
    )

    return pd.DataFrame(
        [
            {
                'Code': code,
                'Code_Loc': f'{code}/{_LOCATION}',
                'Location': _LOCATION,
                'Name': COMMODITY_DESC[code],
            }
            for code in COMMODITIES
        ]
    )


def _build_industries_meta_df() -> pd.DataFrame:
    from bedrock.utils.taxonomy.cornerstone.industries import (
        INDUSTRIES,
        INDUSTRY_DESC,
    )

    return pd.DataFrame(
        [
            {
                'Code': code,
                'Code_Loc': f'{code}/{_LOCATION}',
                'Location': _LOCATION,
                'Name': INDUSTRY_DESC[code],
            }
            for code in INDUSTRIES
        ]
    )


def _build_final_demand_meta_df() -> pd.DataFrame:
    from bedrock.utils.taxonomy.cornerstone.final_demand import (
        FINAL_DEMAND_DESC,
        FINAL_DEMANDS,
    )

    return pd.DataFrame(
        [{'Code': code, 'Name': FINAL_DEMAND_DESC[code]} for code in FINAL_DEMANDS]
    )


def _build_value_added_meta_df() -> pd.DataFrame:
    from bedrock.utils.taxonomy.cornerstone.value_added import (
        VALUE_ADDED_DESC,
        VALUE_ADDEDS,
    )

    return pd.DataFrame(
        [{'Code': code, 'Name': VALUE_ADDED_DESC[code]} for code in VALUE_ADDEDS]
    )


def _build_flows_df() -> pd.DataFrame:
    """useeior-style `flows` table -- bedrock-aggregated GHGs (7 rows).

    Useeior emits ~2000 rows of elementary flows. Bedrock B is already
    aggregated to the 7 IPCC AR6 GHGs upstream, so this sheet lists those
    7 (NOT directly comparable to a useeior flows sheet). See the
    workbook docstring on `bedrock.publish.excel.writer` for the units
    divergence.
    """
    from bedrock.utils.emissions.ghg import GHG

    return pd.DataFrame(
        [
            {
                'Flowable': name,
                'Context': 'emission/air',
                'Unit': 'kg CO2e',
            }
            for name in GHG
        ]
    )


def _build_indicators_df() -> pd.DataFrame:
    """useeior-style `indicators` table -- one row for `Greenhouse Gases`.

    Mirrors the single-indicator C matrix returned by `derive_C_usa()`.
    """
    from bedrock.utils.emissions.characterization import GREENHOUSE_GASES_INDICATOR

    return pd.DataFrame(
        [
            {
                'Name': GREENHOUSE_GASES_INDICATOR,
                'Code': 'GHG',
                'Group': 'Impact Potential',
                'Unit': 'kg CO2e',
                'SimpleName': GREENHOUSE_GASES_INDICATOR,
                'SimpleUnit': 'kg CO2e',
            }
        ]
    )


# ---------------------------------------------------------------------------
# Matrix registry
# ---------------------------------------------------------------------------
#
# Sheet ordering mirrors useeior's `writeModeltoXLSX` (see `WriteModel.R`):
#
#   1. Matrices in the order declared in useeior's `matrices` vector:
#        V, U, U_d, A, A_d, A_m, B, C, D, L, L_d,
#        M, M_d, M_m, N, N_d, N_m, Rho, Phi, Tau
#   2. q, x (commodity and industry output)
#   3. demand vectors (one sheet per named demand vector; not yet wired)
#   4. metadata block: demands, flows, indicators, commodities_meta /
#      industries_meta, final_demand_meta, value_added_meta, SectorCrosswalk
#   5. bedrock-only extensions: config_summary, model_info
#
# Useeior writes only one of commodities_meta / industries_meta depending
# on `CommodityorIndustryType`. Bedrock has both taxonomies and writes
# both. config_summary and model_info are bedrock-only.
#
# The getters below are consumed only by the registry. If a second
# non-publish caller appears, promote to a config-aware `derive_*_usa()`
# in `bedrock/transform/eeio/derived.py`.
#
# Math for derived matrices (Leontief inverse, B @ L, etc.) is sourced
# from `bedrock.utils.math.formulas` -- don't inline `np.linalg.inv` or
# similar here.
#
# Lazy imports of `bedrock.transform.eeio.derived(_cornerstone)` inside
# each getter keep `import bedrock.publish.excel.writer` cheap;
# `formulas` is imported at module top because it's just math primitives.
#
# `@functools.cache` on each getter lets `clear_publish_caches()` reset
# all of them in one call between integration-test configs.


def _require_cornerstone() -> None:
    if not get_usa_config().use_cornerstone_2026_model_schema:
        raise NotImplementedError(
            'bedrock.publish.excel only supports cornerstone-schema configs '
            '(use_cornerstone_2026_model_schema=True). Wiring legacy paths '
            'into the publish pipeline is a separate task.'
        )


def _assemble_extended_U(
    intermediate: pd.DataFrame,
    fd: pd.DataFrame | None,
    va: pd.DataFrame,
) -> pd.DataFrame:
    """Stack intermediate / FD / VA blocks into a useeior-style extended U.

    Layout when `fd` is provided:

        +-----------------+-----------+
        |  intermediate   |    fd     |
        +-----------------+-----------+
        |       va        |  zeros    |
        +-----------------+-----------+

    With `fd=None`, the right-hand column block (FD + VA x FD zeros) is
    omitted -- used for `U_d` until a domestic Y matrix at FD-category
    granularity exists in bedrock (see
    `derive_cornerstone_ydom_and_yimp`).

    Both axes of the returned frame are tagged `Index.name = 'sector'`
    so `_apply_loc_suffix` adds `/US` to every label.

    Inputs must be aligned: `intermediate.columns == va.columns` and
    (when present) `intermediate.index == fd.index`.
    """
    if not intermediate.columns.equals(va.columns):
        raise ValueError(
            '_assemble_extended_U: intermediate.columns and va.columns must match '
            f'(got {len(intermediate.columns)} vs {len(va.columns)} labels)'
        )
    if fd is not None and not intermediate.index.equals(fd.index):
        raise ValueError(
            '_assemble_extended_U: intermediate.index and fd.index must match '
            f'(got {len(intermediate.index)} vs {len(fd.index)} labels)'
        )

    if fd is None:
        out = pd.concat([intermediate, va], axis=0)
    else:
        top = pd.concat([intermediate, fd], axis=1)
        va_zeros = pd.DataFrame(0.0, index=va.index, columns=fd.columns)
        bottom = pd.concat([va, va_zeros], axis=1)
        out = pd.concat([top, bottom], axis=0)

    out.index.name = 'sector'
    out.columns.name = 'sector'
    return out


@functools.cache
def _get_V() -> pd.DataFrame:
    from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_V

    return derive_cornerstone_V()


@functools.cache
def _get_U() -> pd.DataFrame:
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
        derive_disagg_Ytot_with_trade,
    )
    from bedrock.transform.eeio.derived_cornerstone import (
        derive_cornerstone_U_set,
        derive_cornerstone_VA,
    )
    from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

    uset = derive_cornerstone_U_set()
    intermediate = uset.Udom + uset.Uimp
    # Reindex by canonical FINAL_DEMANDS order so column layout matches
    # useeior's `U` regardless of source column ordering.
    fd_block = derive_disagg_Ytot_with_trade()[list(FINAL_DEMANDS)]
    return _assemble_extended_U(
        intermediate=intermediate,
        fd=fd_block,
        va=derive_cornerstone_VA(),
    )


@functools.cache
def _get_Udom() -> pd.DataFrame:
    # FD block intentionally truncated; bedrock has no Ydom matrix at
    # FD-category resolution today. See writer module docstring.
    from bedrock.transform.eeio.derived_cornerstone import (
        derive_cornerstone_U_set,
        derive_cornerstone_VA,
    )

    return _assemble_extended_U(
        intermediate=derive_cornerstone_U_set().Udom,
        fd=None,
        va=derive_cornerstone_VA(),
    )


@functools.cache
def _get_x() -> pd.Series:
    from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_x

    return pd.Series(derive_cornerstone_x(), name='x')


@functools.cache
def _get_A() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_Aq_usa

    aq = derive_Aq_usa()
    A = aq.Adom + aq.Aimp
    A.index.name = 'sector'
    A.columns.name = 'sector'
    return A


@functools.cache
def _get_Adom() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_Aq_usa

    return derive_Aq_usa().Adom


@functools.cache
def _get_L() -> pd.DataFrame:
    return compute_L_matrix(A=_get_A())


@functools.cache
def _get_Ldom() -> pd.DataFrame:
    return compute_L_matrix(A=_get_Adom())


@functools.cache
def _get_B() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_B_usa_non_finetuned

    return derive_B_usa_non_finetuned()


@functools.cache
def _get_C() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_C_usa

    return derive_C_usa()


@functools.cache
def _get_D() -> pd.DataFrame:
    from bedrock.transform.eeio.derived import derive_D_usa

    return derive_D_usa()


# Emission multipliers M = B @ L, M_d = B @ L_d. Both inherit B's
# kgCO2e/USD units -- see the module docstring's divergence-from-useeior
# callout.
@functools.cache
def _get_M() -> pd.DataFrame:
    M = compute_M_matrix(B=_get_B(), L=_get_L())
    M.columns.name = 'sector'
    return M


@functools.cache
def _get_Mdom() -> pd.DataFrame:
    M_d = compute_M_matrix(B=_get_B(), L=_get_Ldom())
    M_d.columns.name = 'sector'
    return M_d


# Impact multipliers N = C @ M, N_d = C @ M_d. In bedrock semantics
# these collapse to per-sector sums of M/M_d because C is the trivial
# row-summer (see `bedrock.utils.emissions.characterization`).
@functools.cache
def _get_N() -> pd.DataFrame:
    N = _get_C() @ _get_M()
    N.columns.name = 'sector'
    return N


@functools.cache
def _get_Ndom() -> pd.DataFrame:
    N_d = _get_C() @ _get_Mdom()
    N_d.columns.name = 'sector'
    return N_d


@functools.cache
def _get_q() -> pd.Series:
    from bedrock.transform.eeio.derived import derive_Aq_usa

    return pd.Series(derive_Aq_usa().scaled_q, name='q')


_CACHED_GETTERS: tuple[Callable[[], pd.DataFrame | pd.Series], ...] = (
    _get_V,
    _get_U,
    _get_Udom,
    _get_x,
    _get_A,
    _get_Adom,
    _get_L,
    _get_Ldom,
    _get_B,
    _get_C,
    _get_D,
    _get_M,
    _get_Mdom,
    _get_N,
    _get_Ndom,
    _get_q,
)


def clear_publish_caches() -> None:
    """Clear all `@functools.cache`-d getters in this module.

    Upstream `derive_*` caches in `bedrock.transform.eeio.derived(_cornerstone)`
    must also be cleared between configs -- see
    `bedrock/publish/__tests__/_helpers.py`.
    """
    for fn in _CACHED_GETTERS:
        fn.cache_clear()  # type: ignore[attr-defined]


def _build_matrix_registry(config_name: str) -> list[SheetSpec]:
    return [
        # --- useeior matrices (order matches `matrices` in WriteModel.R) ---
        SheetSpec('V', _get_V),
        SheetSpec('U', _get_U),
        SheetSpec('U_d', _get_Udom),
        SheetSpec('A', _get_A),
        SheetSpec('A_d', _get_Adom),
        # A_m requires real import emission factors (B_imp); not yet in bedrock.
        SheetSpec('A_m', lambda: None),
        SheetSpec('B', _get_B),
        SheetSpec('C', _get_C),
        SheetSpec('D', _get_D),
        SheetSpec('L', _get_L),
        SheetSpec('L_d', _get_Ldom),
        SheetSpec('M', _get_M),
        SheetSpec('M_d', _get_Mdom),
        SheetSpec('M_m', lambda: None),  # requires B_imp
        SheetSpec('N', _get_N),
        SheetSpec('N_d', _get_Ndom),
        SheetSpec('N_m', lambda: None),  # requires B_imp
        # Rho, Phi, Tau are useeior valuation-adjustment matrices with no
        # direct bedrock analogue. Leave as TODO until a design call is made.
        SheetSpec('Rho', lambda: None),
        SheetSpec('Phi', lambda: None),
        SheetSpec('Tau', lambda: None),
        # --- outputs (useeior writes these after the matrices block) ---
        SheetSpec('q', _get_q),
        SheetSpec('x', _get_x),
        # --- metadata block (useeior order: demands, flows, indicators,
        #     <sectors>_meta, final_demand_meta, value_added_meta,
        #     SectorCrosswalk) ---
        # TODO: `demands` -- needs a design call about which named demand
        # vectors to emit (y_nab? y_dom? y_imp?) and how to encode the long
        # form expected by useeior.
        SheetSpec('demands', lambda: None),
        SheetSpec('flows', lambda: _build_flows_df()),
        SheetSpec('indicators', lambda: _build_indicators_df()),
        SheetSpec('commodities_meta', lambda: _build_commodities_meta_df()),
        SheetSpec('industries_meta', lambda: _build_industries_meta_df()),
        SheetSpec(
            'final_demand_meta',
            lambda: _build_final_demand_meta_df(),
        ),
        SheetSpec(
            'value_added_meta',
            lambda: _build_value_added_meta_df(),
        ),
        # TODO: SectorCrosswalk getter once we settle on which crosswalk
        # to expose (BEA summary <-> CEDA <-> NAICS?).
        SheetSpec('SectorCrosswalk', lambda: None),
        # --- bedrock-only extensions ---
        SheetSpec(
            'config_summary',
            lambda: _build_config_summary_df(config_name),
        ),
        SheetSpec(
            'model_info',
            lambda: _build_model_info_df(config_name),
        ),
    ]


def _materialize(
    registry: Iterable[SheetSpec],
) -> list[tuple[str, pd.DataFrame | pd.Series]]:
    out: list[tuple[str, pd.DataFrame | pd.Series]] = []
    seen: set[str] = set()
    for spec in registry:
        if spec.name in seen:
            raise RuntimeError(
                f'publish: duplicate sheet name {spec.name!r} in registry; '
                'each SheetSpec.name must be unique'
            )
        seen.add(spec.name)
        result = spec.getter()
        if result is None:
            logger.info('publish: skipping sheet %r (getter returned None)', spec.name)
            continue
        if isinstance(result, pd.Series):
            out.append((spec.name, _apply_loc_suffix(result)))
        elif isinstance(result, pd.DataFrame):
            out.append((spec.name, _apply_loc_suffix(result)))
        else:
            raise TypeError(
                f'publish: sheet {spec.name!r} getter returned unsupported type '
                f'{type(result).__name__}; expected DataFrame, Series, or None'
            )
    return out


def write_model_to_xlsx(out_path: str, *, config_name: str) -> None:
    """Write the bedrock EEIO model workbook to `out_path`.

    Sheets are produced from `_build_matrix_registry(config_name)`. Any
    registry entry whose getter returns `None` is omitted from the
    workbook (useeior-style "skip if NULL").

    Raises `NotImplementedError` on legacy (non-cornerstone) configs --
    publish is wired only for cornerstone-schema configs today.
    """
    _require_cornerstone()
    registry = _build_matrix_registry(config_name)
    materialized = _materialize(registry)
    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    logger.info(
        'publish: writing %d sheets to %s',
        len(materialized),
        out_path,
    )
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        for name, obj in materialized:
            if isinstance(obj, pd.Series):
                obj.to_frame().to_excel(writer, sheet_name=name)
            else:
                obj.to_excel(writer, sheet_name=name)
