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
  * The bedrock `C` and `D` sheets are emitted for sheet-name parity
    only. `C` is a trivial row-summer (single "Greenhouse Gases"
    indicator over the 7 GHGs, all values = 1.0) and `D = C @ B`
    reduces to `B.sum(axis=0)`. See
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
`Index.name == 'sector'`.
"""

# ruff: noqa: PLC0415
from __future__ import annotations

import datetime
import logging
import os
from collections.abc import Callable, Iterable
from typing import NamedTuple

import pandas as pd

from bedrock.utils.config.settings import GIT_HASH_LONG
from bedrock.utils.config.usa_config import get_usa_config

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
                'kg of gas. Resolve before treating bedrock XLSX as a useeior '
                'drop-in. See bedrock/utils/emissions/characterization.py.'
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
# Real getters: B, C, D, q, and metadata sheets noted below. Other
# entries are `lambda: None` placeholders; their sheets are omitted from
# the workbook ("skip if NULL", matching useeior `WriteModel.R`).
# Resolving each placeholder requires either a config-aware
# `derive_*_usa()` wrapper or, for the `*_m` family, a real `B_imp`
# (import emission factors) which bedrock does not yet produce.
# Concrete TODOs are inlined next to each placeholder.


def _get_B() -> pd.DataFrame | None:
    from bedrock.transform.eeio.derived import derive_B_usa_non_finetuned

    return derive_B_usa_non_finetuned()


def _get_C() -> pd.DataFrame | None:
    from bedrock.transform.eeio.derived import derive_C_usa

    return derive_C_usa()


def _get_D() -> pd.DataFrame | None:
    from bedrock.transform.eeio.derived import derive_D_usa

    return derive_D_usa()


def _get_q() -> pd.Series | None:
    from bedrock.transform.eeio.derived import derive_Aq_usa

    q = derive_Aq_usa().scaled_q
    return pd.Series(q, name='q')


def _build_matrix_registry(config_name: str) -> list[SheetSpec]:
    return [
        # --- useeior matrices (order matches `matrices` in WriteModel.R) ---
        # TODO: derive_V_usa() wrapping derive_cornerstone_V().
        SheetSpec('V', lambda: None),
        # TODO: derive_U_usa() wrapping derive_cornerstone_U_set().Udom + Uimp.
        SheetSpec('U', lambda: None),
        SheetSpec('U_d', lambda: None),
        # TODO: derive_A_usa() wrapping derive_Aq_usa().Adom + Aimp.
        SheetSpec('A', lambda: None),
        SheetSpec('A_d', lambda: None),
        # A_m requires real import emission factors (B_imp); not yet in bedrock.
        SheetSpec('A_m', lambda: None),
        SheetSpec('B', _get_B),
        SheetSpec('C', _get_C),
        SheetSpec('D', _get_D),
        # TODO: derive_L_usa() = (I - A)^-1; cheap once derive_A_usa() exists.
        SheetSpec('L', lambda: None),
        SheetSpec('L_d', lambda: None),
        # TODO: derive_M_usa() = B @ L; cheap once L exists.
        SheetSpec('M', lambda: None),
        SheetSpec('M_d', lambda: None),
        SheetSpec('M_m', lambda: None),  # requires B_imp
        # TODO: derive_N_usa() = C @ M = M.sum(axis=0) given bedrock semantics.
        SheetSpec('N', lambda: None),
        SheetSpec('N_d', lambda: None),
        SheetSpec('N_m', lambda: None),  # requires B_imp
        SheetSpec('Rho', lambda: None),
        SheetSpec('Phi', lambda: None),
        SheetSpec('Tau', lambda: None),
        # --- outputs (useeior writes these after the matrices block) ---
        SheetSpec('q', _get_q),
        SheetSpec('x', lambda: None),  # TODO: derive_x_usa()
        # --- metadata block (useeior order: demands, flows, indicators,
        #     <sectors>_meta, final_demand_meta, value_added_meta,
        #     SectorCrosswalk) ---
        # TODO: demands, flows, indicators, SectorCrosswalk getters once
        # we have config-aware accessors for those tables.
        SheetSpec('demands', lambda: None),
        SheetSpec('flows', lambda: None),
        SheetSpec('indicators', lambda: None),
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


# Sheet names treated as model "data" (matrices/vectors). At least one of
# these must be non-None or `write_model_to_xlsx` raises -- otherwise we'd
# silently publish a metadata-only workbook.
_DATA_SHEETS: frozenset[str] = frozenset(
    {
        'V',
        'U',
        'U_d',
        'A',
        'A_d',
        'A_m',
        'B',
        'C',
        'D',
        'L',
        'L_d',
        'M',
        'M_d',
        'M_m',
        'N',
        'N_d',
        'N_m',
        'Rho',
        'Phi',
        'Tau',
        'q',
        'x',
    }
)


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
    workbook (useeior-style "skip if NULL"). If zero data sheets
    materialize, raises `RuntimeError` -- a metadata-only workbook is
    treated as a silent failure, not a valid state.
    """
    registry = _build_matrix_registry(config_name)
    materialized = _materialize(registry)
    data_count = sum(1 for name, _ in materialized if name in _DATA_SHEETS)
    if data_count == 0:
        raise RuntimeError(
            'publish: no data sheets materialized; refusing to write a '
            'metadata-only workbook. Check that derive_* getters in '
            'MATRIX_REGISTRY are wired and that the requested config '
            'supports them.'
        )
    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    logger.info(
        'publish: writing %d sheets (%d data, %d metadata) to %s',
        len(materialized),
        data_count,
        len(materialized) - data_count,
        out_path,
    )
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        for name, obj in materialized:
            if isinstance(obj, pd.Series):
                obj.to_frame().to_excel(writer, sheet_name=name)
            else:
                obj.to_excel(writer, sheet_name=name)
