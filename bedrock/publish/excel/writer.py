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
import logging
import os
from collections.abc import Iterable

import pandas as pd

from bedrock.publish.model_objects import (
    PUBLISH_LOCATION,
    SheetSpec,
    apply_loc_suffix,
    get_A,
    get_Adom,
    get_B,
    get_C,
    get_D,
    get_L,
    get_Ldom,
    get_M,
    get_Mdom,
    get_N,
    get_Ndom,
    get_Phi,
    get_q,
    get_Rho,
    get_U,
    get_Udom,
    get_V,
    get_x,
    require_cornerstone_config,
)
from bedrock.utils.config.settings import GIT_HASH_LONG
from bedrock.utils.config.usa_config import get_usa_config

logger = logging.getLogger(__name__)


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
        {'field': 'location_suffix', 'value': PUBLISH_LOCATION},
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
                'Code_Loc': f'{code}/{PUBLISH_LOCATION}',
                'Location': PUBLISH_LOCATION,
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
                'Code_Loc': f'{code}/{PUBLISH_LOCATION}',
                'Location': PUBLISH_LOCATION,
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
# Matrix registry (getters live in `bedrock.publish.model_objects`)
# ---------------------------------------------------------------------------


def _build_matrix_registry(config_name: str) -> list[SheetSpec]:
    return [
        # --- useeior matrices (order matches `matrices` in WriteModel.R) ---
        SheetSpec('V', get_V),
        SheetSpec('U', get_U),
        SheetSpec('U_d', get_Udom),
        SheetSpec('A', get_A),
        SheetSpec('A_d', get_Adom),
        # A_m requires real import emission factors (B_imp); not yet in bedrock.
        SheetSpec('A_m', lambda: None),
        SheetSpec('B', get_B),
        SheetSpec('C', get_C),
        SheetSpec('D', get_D),
        SheetSpec('L', get_L),
        SheetSpec('L_d', get_Ldom),
        SheetSpec('M', get_M),
        SheetSpec('M_d', get_Mdom),
        SheetSpec('M_m', lambda: None),  # requires B_imp
        SheetSpec('N', get_N),
        SheetSpec('N_d', get_Ndom),
        SheetSpec('N_m', lambda: None),  # requires B_imp
        SheetSpec('Rho', get_Rho),
        SheetSpec('Phi', get_Phi),
        SheetSpec('Tau', lambda: None),
        # --- outputs (useeior writes these after the matrices block) ---
        SheetSpec('q', get_q),
        SheetSpec('x', get_x),
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
            out.append((spec.name, apply_loc_suffix(result)))
        elif isinstance(result, pd.DataFrame):
            out.append((spec.name, apply_loc_suffix(result)))
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
    require_cornerstone_config()
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
