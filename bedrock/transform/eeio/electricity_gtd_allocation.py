"""EIA-anchored generation / transmission / distribution purchaser allocation.

Pure allocator plus the 2017 cached getter and Use/Y/A/q writers.
Table 2.2 / 2.14 / 3.1 loaders live in ``egrid_generation``.

Table 7.7 purchased kWh is read from the ``EIA_MECS_Energy`` **FBA** parquet.
This repo does not publish a standalone MECS Energy FBS: Tables 2.2 / 3.2
also stay FBA and enter GHG only as attribution sources during FBS build.
7.7 is the same pattern with a different consumer (G/T/D purchaser weights,
not sector-attributed flows). An FBS pass would apply
``estimate_suppressed_mecs_energy`` and the generic NAICS crosswalk; this
path needs 3-digit Q/D residual fill and the Cornerstone MECS 3.1 hand map.

Manufacturing NAICS→BEA IO uses ``CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_*``,
the same maps as GHG industrial coal/gas combustion. Residual Industrial
purchasers are ``NON_MECS_INDUSTRIES`` (dollar bills here; BEA fuel Use in
GHG). Multi-IO mapping keys split 7.7 kWh by electricity bills; GHG splits
fuel by BEA Use of that commodity.
"""

from __future__ import annotations

import functools
import logging
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, cast

import numpy as np
import pandas as pd
import pandera.typing as pt

from bedrock.extract.disaggregation.egrid_generation import (
    egrid_mwh_for_io_year,
    eia_table_2_2_end_use_mwh,
    eia_table_2_14_export_mwh,
    eia_table_2_14_year_for_egrid_year,
)
from bedrock.transform.eeio.electricity_end_use_mapping import build_end_use_map
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.schemas.single_region_schemas import AMatrix
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

logger = logging.getLogger(__name__)

_REANCHORED_ELECTRICITY_Q: pd.Series | None = None
_REANCHORED_EIA_PURCHASER_ALLOCATION: EIAPurchaserAllocation | None = None


def _cell_float(frame: pd.DataFrame, row: str, col: str) -> float:
    return float(cast(float, frame.at[row, col]))


def _as_float_series(obj: object) -> pd.Series:
    if isinstance(obj, pd.DataFrame):
        squeezed = obj.squeeze()
        if isinstance(squeezed, pd.Series):
            return squeezed.astype(float)
        if isinstance(squeezed, pd.DataFrame):
            return squeezed.iloc[:, 0].astype(float)
        raise TypeError(f'expected a Series, got {type(squeezed)}')
    if not isinstance(obj, pd.Series):
        raise TypeError(f'expected a Series, got {type(obj)}')
    return obj.astype(float)


def set_reanchored_electricity_q(q: pd.Series) -> None:
    """Record published G/T/D ``q`` after A/q reanchor, for the GHG-year ``x`` split."""
    global _REANCHORED_ELECTRICITY_Q
    _REANCHORED_ELECTRICITY_Q = (
        q.reindex(ELECTRICITY_DISAGG_SECTORS).astype(float).copy()
    )


def set_reanchored_eia_purchaser_allocation(allocation: EIAPurchaserAllocation) -> None:
    """Record the model-year purchaser split used to rewrite published A/q."""
    global _REANCHORED_EIA_PURCHASER_ALLOCATION
    _REANCHORED_EIA_PURCHASER_ALLOCATION = allocation


def get_reanchored_eia_purchaser_allocation() -> EIAPurchaserAllocation | None:
    return _REANCHORED_EIA_PURCHASER_ALLOCATION


def clear_reanchored_electricity_q() -> None:
    global _REANCHORED_ELECTRICITY_Q, _REANCHORED_EIA_PURCHASER_ALLOCATION
    _REANCHORED_ELECTRICITY_Q = None
    _REANCHORED_EIA_PURCHASER_ALLOCATION = None


def reanchored_electricity_q_shares() -> pd.Series | None:
    if _REANCHORED_ELECTRICITY_Q is None:
        return None
    total = float(_REANCHORED_ELECTRICITY_Q.sum())
    if total <= 0:
        return None
    return _REANCHORED_ELECTRICITY_Q / total


ELECTRICITY_AGGREGATE = '221100'
GENERATION_SECTOR = '221110'
TRANSMISSION_SECTOR = '221121'
DISTRIBUTION_SECTOR = '221122'
EXPORT_FD_CODE = 'F04000'
IMPORT_FD_CODE = 'F05000'

_TABLE_2_2_CLASSES: tuple[str, ...] = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
)

_WATER_FILL_ATOL = 1e-9

IndustrialWeighting = Literal['mecs', 'dollars']

MECS_7_7_NAICS_OVERLAY: dict[str, str] = {
    '322121': '322120',
    '322122': '322120',
    '336111': '336110',
    '336112': '336110',
}

TABLE_7_7_DESCRIPTION = 'Table 7.7'
TABLE_7_7_ELECTRICITY_TOTAL = 'Electricity total'
MANUFACTURING_TOTAL_NAICS = '31-33'
_MKWH_TO_KWH = 1.0e6
_THREE_DIGIT_RESIDUAL_ATOL_KWH = 1.0e6
_Q_D_CODES = frozenset({'Q', 'D'})


@dataclass(frozen=True)
class EIAPurchaserAllocation:
    """Per-purchaser generation / T&D split aligned to ``bills.index``."""

    bill: pd.Series
    end_use_class: pd.Series
    mwh: pd.Series
    gen_dollars: pd.Series
    t_dollars: pd.Series
    d_dollars: pd.Series
    clipped: pd.Series
    p: float
    egrid_mwh: float
    td_share: float


def _go_p_and_td_shares() -> tuple[float, float]:
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        build_electricity_disagg_go_weights,
    )

    w = build_electricity_disagg_go_weights()
    p_share = float(w[GENERATION_SECTOR])
    if not np.isfinite(p_share) or p_share <= 0:
        raise ValueError(
            'UGO generation share is missing or non-positive; '
            'Table 8.3 is not a p backup'
        )
    td_den = float(w[TRANSMISSION_SECTOR]) + float(w[DISTRIBUTION_SECTOR])
    if td_den <= 0:
        raise ValueError('UGO T+D share is non-positive')
    td_share = float(w[TRANSMISSION_SECTOR]) / td_den
    return p_share, td_share


def _class_mwh_targets(
    eia_year: int,
    egrid_mwh: float,
) -> dict[str, float]:
    t22 = eia_table_2_2_end_use_mwh(eia_year)
    # Table 2.14 (Canada/Mexico trade) can lag eGRID; resolve the table year
    # here instead of substituting inside the loader.
    table_2_14_year = eia_table_2_14_year_for_egrid_year(eia_year)
    export_mwh = eia_table_2_14_export_mwh(table_2_14_year)
    teu = float(t22['Total End Use'])
    if teu <= 0:
        raise ValueError(f'Table 2.2 Total End Use non-positive for {eia_year}')
    pools = {
        'Residential': float(t22['Residential']),
        'Commercial': float(t22['Commercial']),
        'Industrial': float(t22['Industrial']) + float(t22['Direct Use']),
        'Transportation': float(t22['Transportation']),
    }
    remaining = egrid_mwh - export_mwh
    targets = {cls: (pools[cls] / teu) * remaining for cls in _TABLE_2_2_CLASSES}
    targets['Exports'] = export_mwh
    return targets


def _end_use_classes(bills: pd.Series, self_use_key: str) -> pd.Series:
    end_use_map = build_end_use_map()
    classes = pd.Series(index=bills.index, dtype=object)
    for key in bills.index:
        if str(key) == self_use_key:
            classes[key] = 'Industrial'
        else:
            classes[key] = end_use_map.get(str(key), 'Commercial')
    return classes


def _water_fill_gen(
    bills: pd.Series,
    proportional: pd.Series,
    members: list[str],
) -> tuple[pd.Series, pd.Series]:
    """Clip gen to positive bills; put overflow on remaining slack in class."""
    gen = pd.Series(0.0, index=members, dtype=float)
    clipped = pd.Series(False, index=members, dtype=bool)
    cap = {j: max(float(bills[j]), 0.0) for j in members}
    overflow = 0.0
    for j in members:
        prop = float(proportional[j])
        if float(bills[j]) <= 0:
            gen[j] = 0.0
            overflow += prop
            continue
        take = min(prop, cap[j])
        gen[j] = take
        cap[j] -= take
        if prop > take + _WATER_FILL_ATOL:
            clipped[j] = True
            overflow += prop - take
    while overflow > _WATER_FILL_ATOL:
        slack_js = [j for j in members if cap[j] > _WATER_FILL_ATOL]
        if not slack_js:
            break
        slack_total = sum(cap[j] for j in slack_js)
        assigned = 0.0
        for j in slack_js:
            share = overflow * (cap[j] / slack_total)
            take = min(share, cap[j])
            gen[j] = float(gen[j]) + take
            cap[j] -= take
            assigned += take
        if assigned <= _WATER_FILL_ATOL:
            break
        overflow -= assigned
    return gen, clipped


def mecs_year_for_eia_year(eia_year: int) -> Literal[2018, 2022]:
    """Map an EIA/eGRID year to the MECS survey used for purchased-kWh shares.

    2017 and earlier use the 2018 survey; later years (including
    ``model_base_year``) use 2022. Production callers are ``eia_year=2017``
    and ``eia_year=model_year``. No interpolation between survey years.
    """
    return 2018 if eia_year <= 2017 else 2022


def industrial_manufacturing_pool() -> frozenset[str]:
    """Cornerstone IO codes in flattened 3.1 mapping keys ∪ subtraction keys.

    Manufacturing purchasers inside the Industrial end-use class are
    this set intersected with that class's bill columns.
    """
    from bedrock.transform.allocation.mappings.cornerstone.mecs import (  # noqa: PLC0415
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    )
    from bedrock.transform.allocation.utils import flatten_items  # noqa: PLC0415

    codes = set(flatten_items(CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.keys()))
    codes.update(
        flatten_items(CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.keys())
    )
    return frozenset(str(c) for c in codes)


def _dedupe_overlay(naics: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for code in naics:
        mapped = MECS_7_7_NAICS_OVERLAY.get(str(code), str(code))
        if mapped not in seen:
            seen.add(mapped)
            out.append(mapped)
    return tuple(out)


def _resolve_7_7_naics(code: str, available: set[str]) -> str:
    """Prefer the 7.7 overlay code when that vintage publishes it.

    2022 collapses 322121/322122 → 322120 and 336111/336112 → 336110; 2018
    still lists the older 6-digit rows.
    """
    mapped = MECS_7_7_NAICS_OVERLAY.get(str(code), str(code))
    if mapped in available:
        return mapped
    if str(code) in available:
        return str(code)
    raise ValueError(
        f'Table 7.7 US Electricity total missing NAICS {code!r} (overlay {mapped!r})'
    )


def _overlay_naics_tuple(
    naics: tuple[str, ...],
    available: set[str],
) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for code in naics:
        resolved = _resolve_7_7_naics(str(code), available)
        if resolved not in seen:
            seen.add(resolved)
            out.append(resolved)
    return tuple(out)


def _overlaid_3_1_mapping(
    available: set[str],
) -> dict[tuple[str, ...], tuple[str, ...]]:
    from bedrock.transform.allocation.mappings.cornerstone.mecs import (  # noqa: PLC0415
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
    )

    return {
        k: _overlay_naics_tuple(v, available)
        for k, v in CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.items()
    }


def _overlaid_3_1_subtraction(
    available: set[str],
) -> dict[tuple[str, ...], tuple[tuple[str, ...], tuple[str, ...]]]:
    from bedrock.transform.allocation.mappings.cornerstone.mecs import (  # noqa: PLC0415
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    )

    return {
        k: (
            _overlay_naics_tuple(parent, available),
            _overlay_naics_tuple(children, available),
        )
        for k, (parent, children) in (
            CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.items()
        )
    }


def _required_7_7_naics(available: set[str] | None = None) -> frozenset[str]:
    from bedrock.transform.allocation.mappings.cornerstone.mecs import (  # noqa: PLC0415
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING,
        CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING,
    )

    raw: set[str] = set()
    for vals in CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.values():
        raw.update(str(c) for c in vals)
    for (
        parent,
        children,
    ) in CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.values():
        raw.update(str(c) for c in parent)
        raw.update(str(c) for c in children)
    if available is None:
        needed = set(raw)
        needed.update(MECS_7_7_NAICS_OVERLAY.get(c, c) for c in raw)
        needed.update(MECS_7_7_NAICS_OVERLAY.values())
        return frozenset(needed)
    needed = set()
    for vals in CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_MAPPING.values():
        needed.update(_overlay_naics_tuple(vals, available))
    for (
        parent,
        children,
    ) in CORNERSTONE_INDUSTRY_TO_MECS_3_1_NAICS_SUBTRACTION_MAPPING.values():
        needed.update(_overlay_naics_tuple(parent, available))
        needed.update(_overlay_naics_tuple(children, available))
    return frozenset(needed)


def _is_three_digit_naics(code: str) -> bool:
    return len(code) == 3 and code.isdigit()


def _fill_three_digit_suppressed(kwh: pd.Series, suppressed: pd.Series) -> pd.Series:
    """Impute 3-digit Q/D on Electricity total from manufacturing total minus published."""
    out = kwh.astype(float).copy()
    if MANUFACTURING_TOTAL_NAICS not in out.index:
        raise ValueError(
            'Table 7.7 US Electricity total is missing manufacturing total '
            f'{MANUFACTURING_TOTAL_NAICS!r}'
        )
    total = float(out.loc[MANUFACTURING_TOTAL_NAICS])
    three = [str(c) for c in out.index if _is_three_digit_naics(str(c))]
    published: list[str] = []
    qd: list[str] = []
    for code in three:
        letter = ''
        if code in suppressed.index and pd.notna(suppressed.loc[code]):
            letter = str(suppressed.loc[code]).strip()
        val = float(out.loc[code])
        if letter in _Q_D_CODES:
            qd.append(code)
        elif np.isfinite(val):
            published.append(code)
    pub_sum = float(out.loc[published].sum()) if published else 0.0
    leftover = total - pub_sum
    if abs(leftover) > _THREE_DIGIT_RESIDUAL_ATOL_KWH and len(qd) != 1:
        raise ValueError(
            'Table 7.7 US Electricity total 3-digit residual '
            f'{leftover / _MKWH_TO_KWH:.6g} million kWh is not ~0 and '
            f'there are {len(qd)} Q/D 3-digit industries {qd!r} (need 0 or 1)'
        )
    if abs(leftover) > _THREE_DIGIT_RESIDUAL_ATOL_KWH and len(qd) == 1:
        out.loc[qd[0]] = leftover
    return out.fillna(0.0)


@functools.cache
def _mecs_purchased_kwh_cached(mecs_year: int) -> pd.Series:
    from bedrock.extract.flowbyactivity import getFlowByActivity  # noqa: PLC0415
    from bedrock.utils.mapping.location import US_FIPS  # noqa: PLC0415

    df = getFlowByActivity('EIA_MECS_Energy', int(mecs_year))
    desc = df['Description'].astype(str)
    loc = df['Location'].astype(str)
    flow = df['FlowName'].astype(str)
    sub = df.loc[
        (desc == TABLE_7_7_DESCRIPTION)
        & (loc == US_FIPS)
        & (flow == TABLE_7_7_ELECTRICITY_TOTAL)
    ].copy()
    if sub.empty:
        raise ValueError(
            f'EIA_MECS_Energy {mecs_year} has no US Table 7.7 '
            f'{TABLE_7_7_ELECTRICITY_TOTAL!r} rows'
        )
    naics = sub['ActivityConsumedBy'].astype(str).str.strip()
    amount_mkwh = pd.to_numeric(sub['FlowAmount'], errors='coerce')
    suppressed = sub['Suppressed']
    is_star = suppressed.astype(str).str.strip() == '*'
    amount_mkwh = amount_mkwh.mask(is_star, 0.0)
    kwh = pd.Series(
        amount_mkwh.to_numpy(dtype=float) * _MKWH_TO_KWH,
        index=pd.Index(naics.to_numpy(), name='naics'),
        dtype=float,
    )
    supp = pd.Series(
        suppressed.to_numpy(),
        index=kwh.index,
    )
    if kwh.index.has_duplicates:
        kwh = kwh.groupby(level=0).sum()
        supp = supp.groupby(level=0).first()
    kwh = _fill_three_digit_suppressed(kwh, supp)
    available = set(kwh.index.astype(str))
    _required_7_7_naics(available)
    return kwh.astype(float)


def mecs_purchased_kwh(mecs_year: int) -> pd.Series:
    """US Table 7.7 Electricity-total purchased kWh by MECS NAICS.

    Filter Description == ``Table 7.7``, Location == US FIPS, FlowName ==
    ``Electricity total``. After * → 0 and 3-digit Q/D residual fill;
    remaining non-3-digit Q/D → 0. Index is MECS NAICS (includes ``31-33``
    for the residual identity only — do not put ``31-33`` into manufacturing
    shares).     Hard-error if the US Electricity-total frame is empty after
    filters. Do not materialize an FBS: 7.7 is FBA-only purchaser
    weights, not ``SectorConsumedBy`` flows. A present Energy FBS
    without Table 7.7 rows is not FBANotAvailableError and must not
    dollar-fallback. Do not call ``estimate_suppressed_mecs_energy``.

    Cached by year; each call returns a copy so callers cannot mutate the
    cached Series.
    """
    return _mecs_purchased_kwh_cached(int(mecs_year)).copy()


def _split_kwh_across_io(
    io_codes: tuple[str, ...],
    kwh: float,
    bills: pd.Series,
) -> dict[str, float]:
    if not io_codes:
        return {}
    if len(io_codes) == 1:
        return {io_codes[0]: float(kwh)}
    weights = bills.reindex(list(io_codes)).astype(float).clip(lower=0.0).fillna(0.0)
    wsum = float(weights.sum())
    if wsum <= 0:
        return {c: 0.0 for c in io_codes}
    return {c: float(kwh) * float(weights[c]) / wsum for c in io_codes}


def io_manufacturing_purchased_kwh(bills: pd.Series, mecs_year: int) -> pd.Series:
    """Map Table 7.7 purchased kWh onto manufacturing IO columns.

    Overlay 7.7 NAICS, split shared rows by clip0 bills, clamp negative
    parent−child leftover to 0. Returns kWh on IO index (not class MWh).
    """
    naics_kwh = mecs_purchased_kwh(mecs_year)
    available = set(naics_kwh.index.astype(str))
    mapping = _overlaid_3_1_mapping(available)
    subtraction = _overlaid_3_1_subtraction(available)
    mapped_io = {str(c) for c in _flatten_io_keys(mapping)}
    subtracted_io = {str(c) for c in _flatten_io_keys(subtraction)}
    both = mapped_io & subtracted_io
    if both:
        raise ValueError(
            f'IO codes assigned by both 3.1 mapping and subtraction: {sorted(both)}'
        )
    naics_to_io: dict[str, list[str]] = {}
    for io_key, naics_vals in mapping.items():
        for n in naics_vals:
            bucket = naics_to_io.setdefault(n, [])
            for io in io_key:
                code = str(io)
                if code not in bucket:
                    bucket.append(code)
    assigned: dict[str, float] = {}
    for n, io_list in naics_to_io.items():
        if n not in naics_kwh.index:
            raise ValueError(f'Table 7.7 {mecs_year} missing NAICS {n!r} after overlay')
        chunk = _split_kwh_across_io(tuple(io_list), float(naics_kwh.loc[n]), bills)
        for io, val in chunk.items():
            assigned[io] = assigned.get(io, 0.0) + val
    for io_key, (parent, children) in subtraction.items():
        parent_kwh = sum(float(naics_kwh.loc[p]) for p in parent)
        child_kwh = sum(float(naics_kwh.loc[c]) for c in children)
        leftover = max(0.0, parent_kwh - child_kwh)
        chunk = _split_kwh_across_io(tuple(str(c) for c in io_key), leftover, bills)
        for io, val in chunk.items():
            assigned[io] = assigned.get(io, 0.0) + val
    return pd.Series(assigned, dtype=float)


def _flatten_io_keys(mapping: Mapping[tuple[str, ...], object]) -> list[str]:
    out: list[str] = []
    for key in mapping:
        out.extend(str(c) for c in key)
    return out


def _industrial_mecs_class_mwh(
    bills: pd.Series,
    members: list[str],
    target_mwh: float,
    eia_year: int,
) -> pd.Series:
    """Two-pool Industrial MWh: MECS shares inside manufacturing, dollars in residual.

    Direct Use in the Industrial class target rides on Table 7.7 purchase
    shares (manufacturing) and electricity-bill shares (residual). After this
    assignment, class-wide water-fill can move generation dollars from clipped
    energy-intensive manufacturers onto residual ag/mining/construction.
    """
    mfg_pool = industrial_manufacturing_pool()
    mfg = [m for m in members if m in mfg_pool]
    res = [m for m in members if m not in mfg_pool]
    clip_all = bills.reindex(members).astype(float).clip(lower=0.0).fillna(0.0)
    den = float(clip_all.sum())
    if den <= 0:
        raise ValueError(
            'Industrial electricity bills clip(0) sum is 0; cannot split '
            'manufacturing vs residual pools'
        )
    clip_mfg = bills.reindex(mfg).astype(float).clip(lower=0.0).fillna(0.0)
    pool_mfg = float(target_mwh) * float(clip_mfg.sum()) / den
    pool_res = float(target_mwh) - pool_mfg
    clip_res = bills.reindex(res).astype(float).clip(lower=0.0).fillna(0.0)
    res_sum = float(clip_res.sum())
    if res_sum <= 0 and pool_res > _WATER_FILL_ATOL:
        logger.info(
            'Industrial residual bills clip(0) sum is 0 with residual pool '
            '%.6g MWh; adding that pool to manufacturing',
            pool_res,
        )
        pool_mfg += pool_res
        pool_res = 0.0
    out = pd.Series(0.0, index=members, dtype=float)
    if mfg:
        mecs_year = mecs_year_for_eia_year(eia_year)
        io_kwh = (
            io_manufacturing_purchased_kwh(bills, mecs_year).reindex(mfg).fillna(0.0)
        )
        ksum = float(io_kwh.sum())
        if ksum <= 0:
            wsum = float(clip_mfg.sum())
            if wsum > 0:
                out.loc[mfg] = pool_mfg * (clip_mfg / wsum)
            else:
                out.loc[mfg] = 0.0
        else:
            out.loc[mfg] = pool_mfg * (io_kwh / ksum)
    if res and pool_res > 0 and res_sum > 0:
        out.loc[res] = pool_res * (clip_res / res_sum)
    return out


def allocate_purchaser_gtd(
    bills: pd.Series,
    *,
    self_use_key: str,
    eia_year: int,
    p_share_2017: float,
    td_share_2017: float,
    industrial_weights: IndustrialWeighting = 'mecs',
) -> EIAPurchaserAllocation:
    """Allocate domestic electricity bills to generation, transmission, and distribution.

    ``bills`` is domestic Use columns union Y columns. Do not pass Uimp —
    imported Use is written onto the generation row separately.
    ``self_use_key`` is always ``'221100'``. Class dollar weights use
    ``clip(lower=0)`` for shares only; if ``bill <= 0``, ``gen = 0``.
    ``industrial_weights='mecs'`` (default) uses Table 7.7 purchased kWh
    inside manufacturing and dollar weights for residual Industrial.
    ``industrial_weights='dollars'`` keeps clip0 bills for the whole
    Industrial class (diagnostics and tests that must not load Table 7.7).
    """
    if self_use_key != ELECTRICITY_AGGREGATE:
        raise ValueError(
            f'self_use_key must be {ELECTRICITY_AGGREGATE!r}, got {self_use_key!r}'
        )
    if industrial_weights not in ('mecs', 'dollars'):
        raise ValueError(
            f'industrial_weights must be mecs or dollars, got {industrial_weights!r}'
        )
    if not np.isfinite(p_share_2017) or p_share_2017 <= 0:
        raise ValueError(
            'UGO generation share is missing or non-positive; '
            'Table 8.3 is not a p backup'
        )
    if IMPORT_FD_CODE in bills.index:
        bills = bills.drop(index=IMPORT_FD_CODE)
    bills = bills.astype(float)
    classes = _end_use_classes(bills, self_use_key)
    egrid_mwh = egrid_mwh_for_io_year(eia_year)
    class_mwh = _class_mwh_targets(eia_year, egrid_mwh)
    bill_total = float(bills.sum())
    p = (p_share_2017 * bill_total) / egrid_mwh if egrid_mwh else float('nan')
    if not np.isfinite(p) or p <= 0:
        raise ValueError(f'non-positive generation price p={p!r}')

    mwh = pd.Series(0.0, index=bills.index, dtype=float)
    gen = pd.Series(0.0, index=bills.index, dtype=float)
    clipped = pd.Series(False, index=bills.index, dtype=bool)

    for cls, target_mwh in class_mwh.items():
        members = [str(k) for k in classes.index if classes[k] == cls]
        if not members:
            continue
        if cls == 'Industrial' and industrial_weights == 'mecs':
            mwh.loc[members] = _industrial_mecs_class_mwh(
                bills, members, float(target_mwh), eia_year
            )
        else:
            weights = bills.loc[members].clip(lower=0.0)
            wsum = float(weights.sum())
            if wsum > 0:
                mwh.loc[members] = target_mwh * (weights / wsum)
        class_bill_sum = float(bills.loc[members].sum())
        class_needed = p * float(target_mwh)
        if class_needed > _WATER_FILL_ATOL and class_bill_sum < class_needed:
            scale = class_bill_sum / class_needed
            mwh.loc[members] = mwh.loc[members] * scale
            logger.info(
                'nibble class %s: bills %.6g < p × MWh %.6g; scale=%.6g',
                cls,
                class_bill_sum,
                class_needed,
                scale,
            )
        proportional = mwh.loc[members] * p
        gen_cls, clip_cls = _water_fill_gen(bills, proportional, members)
        gen.loc[members] = gen_cls
        clipped.loc[members] = clip_cls

    n_clipped = int(clipped.fillna(False).to_numpy().sum())
    logger.info(
        'G/T/D water-fill: %s clipped purchasers of %s; class nibble logged above if any',
        n_clipped,
        int(len(clipped)),
    )

    leftover = bills - gen
    t_dollars = leftover * float(td_share_2017)
    d_dollars = leftover * (1.0 - float(td_share_2017))
    return EIAPurchaserAllocation(
        bill=bills,
        end_use_class=classes,
        mwh=mwh.astype(float),
        gen_dollars=gen.astype(float),
        t_dollars=t_dollars.astype(float),
        d_dollars=d_dollars.astype(float),
        clipped=clipped,
        p=float(p),
        egrid_mwh=float(egrid_mwh),
        td_share=float(td_share_2017),
    )


def _electricity_column_dollars(frame: pd.DataFrame, col: str) -> float:
    """Import bill on ``221100`` if still present, else the G/T/D child sum.

    After correspondence, the aggregate row is often gone and the bill sits
    on the children. Prefer the aggregate when it is non-zero so the two
    are not added together.
    """
    if ELECTRICITY_AGGREGATE in frame.index and col in frame.columns:
        agg_val = _cell_float(frame, ELECTRICITY_AGGREGATE, col)
        if np.isfinite(agg_val) and agg_val != 0.0:
            return agg_val
    children = [GENERATION_SECTOR, TRANSMISSION_SECTOR, DISTRIBUTION_SECTOR]
    present = [r for r in children if r in frame.index]
    if not present or col not in frame.columns:
        return 0.0
    return float(frame[col].loc[present].astype(float).sum())


def collapse_electricity_imports_onto_generation(imports: pd.Series) -> pd.Series:
    """Put all G/T/D import dollars on generation; zero T/D import rows.

    Does not change ``q`` or ``Aimp``. Extra import MWh is this generation
    dollar total divided by ``p``.
    """
    out = imports.astype(float).copy()
    present = [c for c in ELECTRICITY_DISAGG_SECTORS if c in out.index]
    if not present:
        return out
    total = float(out.reindex(present).fillna(0.0).sum())
    for code in present:
        out.loc[code] = 0.0
    if GENERATION_SECTOR in out.index:
        out.loc[GENERATION_SECTOR] = total
    return out


def _ensure_index_codes(frame: pd.DataFrame, codes: list[str]) -> pd.DataFrame:
    out = frame.copy()
    if out.columns.empty:
        return out
    for code in codes:
        if code not in out.index:
            out.loc[code] = 0.0
    return out


def _ensure_column_codes(frame: pd.DataFrame, codes: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for code in codes:
        if code not in out.columns:
            out[code] = 0.0
    return out


def write_purchaser_gtd_use_and_y(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    Y: pd.DataFrame,
    allocation: EIAPurchaserAllocation,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Write G/T/D commodity rows; leave ``U[221100,221100]`` on the aggregate.

    Applies to Use columns other than ``221100`` and to all Y columns.
    Imported Use is written onto the generation row only for those columns.
    Does not drop ``221100``.
    """
    agg = ELECTRICITY_AGGREGATE
    children = list(ELECTRICITY_DISAGG_SECTORS)
    Udom = _ensure_index_codes(Udom, children)
    Uimp = _ensure_index_codes(Uimp, children)
    Y = _ensure_index_codes(Y, children)

    for col in Udom.columns:
        col_s = str(col)
        if col_s == agg:
            continue
        orig_imp = 0.0
        if agg in Uimp.index and col in Uimp.columns:
            orig_imp = _cell_float(Uimp, agg, str(col))
        if col_s in allocation.bill.index:
            Udom.at[GENERATION_SECTOR, col] = float(allocation.gen_dollars[col_s])
            Udom.at[TRANSMISSION_SECTOR, col] = float(allocation.t_dollars[col_s])
            Udom.at[DISTRIBUTION_SECTOR, col] = float(allocation.d_dollars[col_s])
        else:
            orig_dom = _cell_float(Udom, agg, str(col)) if agg in Udom.index else 0.0
            Udom.at[GENERATION_SECTOR, col] = orig_dom
            Udom.at[TRANSMISSION_SECTOR, col] = 0.0
            Udom.at[DISTRIBUTION_SECTOR, col] = 0.0
        if agg in Udom.index:
            Udom.at[agg, col] = 0.0
        if col in Uimp.columns:
            Uimp.at[GENERATION_SECTOR, col] = orig_imp
            Uimp.at[TRANSMISSION_SECTOR, col] = 0.0
            Uimp.at[DISTRIBUTION_SECTOR, col] = 0.0
            if agg in Uimp.index:
                Uimp.at[agg, col] = 0.0

    for col in Y.columns:
        col_s = str(col)
        orig = _cell_float(Y, agg, str(col)) if agg in Y.index else 0.0
        if col_s == IMPORT_FD_CODE:
            Y.at[GENERATION_SECTOR, col] = _electricity_column_dollars(Y, str(col))
            Y.at[TRANSMISSION_SECTOR, col] = 0.0
            Y.at[DISTRIBUTION_SECTOR, col] = 0.0
        elif col_s in allocation.bill.index:
            Y.at[GENERATION_SECTOR, col] = float(allocation.gen_dollars[col_s])
            Y.at[TRANSMISSION_SECTOR, col] = float(allocation.t_dollars[col_s])
            Y.at[DISTRIBUTION_SECTOR, col] = float(allocation.d_dollars[col_s])
        else:
            Y.at[GENERATION_SECTOR, col] = orig
            Y.at[TRANSMISSION_SECTOR, col] = 0.0
            Y.at[DISTRIBUTION_SECTOR, col] = 0.0
        if agg in Y.index:
            Y.at[agg, col] = 0.0
    return Udom, Uimp, Y


def write_gtd_use_intersection(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    allocation: EIAPurchaserAllocation,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Materialize the G/T/D 3×3. ``Uimp[G,G]`` only; no leftover T/D on Uimp."""
    agg = ELECTRICITY_AGGREGATE
    children = list(ELECTRICITY_DISAGG_SECTORS)
    Udom = _ensure_index_codes(Udom, children)
    Uimp = _ensure_index_codes(Uimp, children)
    Udom = _ensure_column_codes(Udom, children)
    Uimp = _ensure_column_codes(Uimp, children)

    t_dom = float(allocation.bill[agg])
    gen_self = float(allocation.gen_dollars[agg])
    leftover = t_dom - gen_self
    t_self = leftover * float(allocation.td_share)
    d_self = leftover * (1.0 - float(allocation.td_share))
    uimp_gg = 0.0
    if agg in Uimp.index and agg in Uimp.columns:
        uimp_gg = _cell_float(Uimp, agg, agg)

    for i in children:
        for j in children:
            Udom.at[i, j] = 0.0
            Uimp.at[i, j] = 0.0
    Udom.at[GENERATION_SECTOR, GENERATION_SECTOR] = gen_self
    Udom.at[TRANSMISSION_SECTOR, TRANSMISSION_SECTOR] = t_self
    Udom.at[DISTRIBUTION_SECTOR, DISTRIBUTION_SECTOR] = d_self
    Uimp.at[GENERATION_SECTOR, GENERATION_SECTOR] = uimp_gg
    if agg in Udom.index and agg in Udom.columns:
        Udom.at[agg, agg] = 0.0
    if agg in Uimp.index and agg in Uimp.columns:
        Uimp.at[agg, agg] = 0.0
    return Udom, Uimp


def make_last_weights_from_domestic_use_y(
    Udom: pd.DataFrame,
    allocation: EIAPurchaserAllocation,
) -> pd.Series:
    """Domestic Use+Y G/T/D row-total shares (not Uimp)."""
    fd = set(FINAL_DEMANDS)
    totals: dict[str, float] = {}
    field = {
        GENERATION_SECTOR: allocation.gen_dollars,
        TRANSMISSION_SECTOR: allocation.t_dollars,
        DISTRIBUTION_SECTOR: allocation.d_dollars,
    }
    for code in ELECTRICITY_DISAGG_SECTORS:
        u = float(Udom.loc[code].sum()) if code in Udom.index else 0.0
        y = float(field[code].loc[field[code].index.isin(fd)].sum())
        totals[code] = u + y
    weights = pd.Series(totals, dtype=float)
    total = float(weights.sum())
    if total <= 0:
        raise ValueError('domestic Use+Y electricity row totals are non-positive')
    return weights / total


@functools.cache
def get_2017_eia_purchaser_allocation() -> EIAPurchaserAllocation:
    """Cached 2017 domestic bills → G/T/D. Does not call the IO bundle or Ytot."""
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        _derive_post_reallocation_checkpoint_for_disagg,
        _derive_y_before_electricity_disagg_lazy,
    )

    _v, udom, _uimp, _va = _derive_post_reallocation_checkpoint_for_disagg()
    y = _derive_y_before_electricity_disagg_lazy()
    agg = ELECTRICITY_AGGREGATE
    use_row = (
        _as_float_series(udom.loc[agg]) if agg in udom.index else pd.Series(dtype=float)
    )
    y_row = _as_float_series(y.loc[agg]) if agg in y.index else pd.Series(dtype=float)
    bills = use_row.add(y_row, fill_value=0.0)
    if IMPORT_FD_CODE in bills.index:
        bills = bills.drop(index=IMPORT_FD_CODE)
    p_share, td_share = _go_p_and_td_shares()
    return allocate_purchaser_gtd(
        bills,
        self_use_key=ELECTRICITY_AGGREGATE,
        eia_year=2017,
        p_share_2017=p_share,
        td_share_2017=td_share,
    )


def apply_purchaser_allocation_to_y(Y: pd.DataFrame) -> pd.DataFrame:
    """Split the 2017 Y electricity row from the shared 2017 allocation."""
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        reindex_y_commodities_to_elec_schema,
    )

    allocation = get_2017_eia_purchaser_allocation()
    children = list(ELECTRICITY_DISAGG_SECTORS)
    Y = _ensure_index_codes(Y, children)
    agg = ELECTRICITY_AGGREGATE
    for col in Y.columns:
        col_s = str(col)
        orig = _cell_float(Y, agg, str(col)) if agg in Y.index else 0.0
        if col_s == IMPORT_FD_CODE:
            Y.at[GENERATION_SECTOR, col] = _electricity_column_dollars(Y, str(col))
            Y.at[TRANSMISSION_SECTOR, col] = 0.0
            Y.at[DISTRIBUTION_SECTOR, col] = 0.0
        elif col_s in allocation.bill.index:
            Y.at[GENERATION_SECTOR, col] = float(allocation.gen_dollars[col_s])
            Y.at[TRANSMISSION_SECTOR, col] = float(allocation.t_dollars[col_s])
            Y.at[DISTRIBUTION_SECTOR, col] = float(allocation.d_dollars[col_s])
        else:
            Y.at[GENERATION_SECTOR, col] = orig
            Y.at[TRANSMISSION_SECTOR, col] = 0.0
            Y.at[DISTRIBUTION_SECTOR, col] = 0.0
        if agg in Y.index:
            Y.at[agg, col] = 0.0
    Y = Y.drop(index=[agg], errors='ignore')
    return reindex_y_commodities_to_elec_schema(Y)


def _spill_generation_nonfuel(
    Udom: pd.DataFrame,
    Uimp: pd.DataFrame,
    *,
    x_g: float,
    td_share: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Move non-fuel Use from G to T/D until VA_G would be 0."""
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        GENERATION_FUEL_COMMODITIES,
    )
    from bedrock.utils.taxonomy.cornerstone.value_added import (  # noqa: PLC0415
        VALUE_ADDEDS,
    )

    agg = ELECTRICITY_AGGREGATE

    skip = set(ELECTRICITY_DISAGG_SECTORS) | {agg} | set(VALUE_ADDEDS)
    inputs_g = float(Udom[GENERATION_SECTOR].sum()) + float(
        Uimp[GENERATION_SECTOR].sum()
    )
    va_g = x_g - inputs_g
    if va_g >= 0:
        return Udom, Uimp
    deficit = -va_g
    spillable = [
        str(row)
        for row in Udom.index
        if row not in skip and row not in GENERATION_FUEL_COMMODITIES
    ]
    available = 0.0
    amounts: dict[str, float] = {}
    for row in spillable:
        amt = _cell_float(Udom, row, GENERATION_SECTOR) + _cell_float(
            Uimp, row, GENERATION_SECTOR
        )
        if amt > 0:
            amounts[row] = amt
            available += amt
    if available <= 0:
        warnings.warn(
            f'Negative VA total for electricity sub-industry {GENERATION_SECTOR}: '
            f'{va_g}',
            stacklevel=2,
        )
        return Udom, Uimp
    move = min(deficit, available)
    t_frac = float(td_share)
    d_frac = 1.0 - t_frac
    for row, amt in amounts.items():
        share = amt / available
        take = move * share
        for U in (Udom, Uimp):
            cell = _cell_float(U, row, GENERATION_SECTOR)
            if cell <= 0:
                continue
            row_take = take * (cell / amt)
            U.at[row, GENERATION_SECTOR] = cell - row_take
            U.at[row, TRANSMISSION_SECTOR] = (
                _cell_float(U, row, TRANSMISSION_SECTOR) + row_take * t_frac
            )
            U.at[row, DISTRIBUTION_SECTOR] = (
                _cell_float(U, row, DISTRIBUTION_SECTOR) + row_take * d_frac
            )
    inputs_after = float(Udom[GENERATION_SECTOR].sum()) + float(
        Uimp[GENERATION_SECTOR].sum()
    )
    va_after = x_g - inputs_after
    if va_after < -1.0:
        warnings.warn(
            f'Negative VA total for electricity sub-industry {GENERATION_SECTOR}: '
            f'{va_after}',
            stacklevel=2,
        )
    return Udom, Uimp


def _scaled_export_fd_bill(
    *,
    original_year: int,
    target_year: int,
    model_year: int,
    use_commodity_pi: bool,
    pre_q: pd.Series,
) -> float:
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        derive_disagg_Ytot_with_trade,
    )
    from bedrock.transform.eeio.derived_cornerstone import (  # noqa: PLC0415
        derive_cornerstone_Aq,
    )
    from bedrock.utils.economic.inflation_helpers_cornerstone import (  # noqa: PLC0415
        inflate_cornerstone_q_or_y_with_commodity_pi,
        inflate_cornerstone_q_or_y_with_industry_pi,
    )

    y2017 = derive_disagg_Ytot_with_trade()
    elec = list(ELECTRICITY_DISAGG_SECTORS)
    if EXPORT_FD_CODE not in y2017.columns:
        raise ValueError(f'{EXPORT_FD_CODE} missing from 2017 Y')
    slice_2017 = y2017.loc[elec, EXPORT_FD_CODE].astype(float)
    q_2017 = derive_cornerstone_Aq().scaled_q.astype(float).reindex(elec)
    pre = pre_q.astype(float).reindex(elec)
    ratio = (pre / q_2017.replace(0.0, np.nan)).fillna(1.0)
    scaled = slice_2017 * ratio
    if use_commodity_pi:
        inflated = inflate_cornerstone_q_or_y_with_commodity_pi(
            scaled, original_year=original_year, target_year=model_year
        )
    else:
        inflated = inflate_cornerstone_q_or_y_with_industry_pi(
            scaled, original_year=target_year, target_year=model_year
        )
    return float(pd.Series(inflated).sum())


def _inflate_summary_year_scaled_aq(
    *,
    original_year: int,
    target_year: int,
    model_year: int,
    use_commodity_pi: bool,
) -> tuple[pd.DataFrame, pd.Series]:
    from bedrock.transform.eeio.cornerstone_year_scaling import (  # noqa: PLC0415
        get_summary_year_scaled_aq,
    )
    from bedrock.utils.economic.inflation_helpers_cornerstone import (  # noqa: PLC0415
        inflate_cornerstone_A_matrix_with_commodity_pi,
        inflate_cornerstone_A_matrix_with_industry_pi,
        inflate_cornerstone_q_or_y_with_commodity_pi,
        inflate_cornerstone_q_or_y_with_industry_pi,
    )

    pre = get_summary_year_scaled_aq(original_year, target_year)
    if use_commodity_pi:
        adom = inflate_cornerstone_A_matrix_with_commodity_pi(
            pre.Adom, original_year=original_year, target_year=model_year
        )
        q = inflate_cornerstone_q_or_y_with_commodity_pi(
            pre.q, original_year=original_year, target_year=model_year
        )
    else:
        adom = inflate_cornerstone_A_matrix_with_industry_pi(
            pre.Adom, original_year=target_year, target_year=model_year
        )
        q = inflate_cornerstone_q_or_y_with_industry_pi(
            pre.q, original_year=target_year, target_year=model_year
        )
    return adom, q


def _purchaser_bills_from_aq(
    adom_bills: pd.DataFrame,
    q_bills: pd.Series,
) -> pd.Series:
    from bedrock.transform.eeio.cornerstone_disagg_pipeline import (  # noqa: PLC0415
        derive_disagg_Ytot_with_trade,
    )
    from bedrock.utils.math.formulas import backcompute_y_from_A_and_q  # noqa: PLC0415

    elec = list(ELECTRICITY_DISAGG_SECTORS)
    a_elec = adom_bills.loc[elec].sum(axis=0).astype(float)
    industry_bills = a_elec * q_bills.reindex(a_elec.index).astype(float)
    self_bill = float(industry_bills.reindex(elec).fillna(0.0).sum())
    industry_bills = industry_bills.drop(labels=elec, errors='ignore')
    industry_bills[ELECTRICITY_AGGREGATE] = self_bill

    y_snap = backcompute_y_from_A_and_q(A=adom_bills, q=q_bills)
    y_elec_total = float(y_snap.reindex(elec).fillna(0.0).sum())
    y2017 = derive_disagg_Ytot_with_trade()
    y2017_elec = y2017.loc[elec].sum(axis=0).astype(float)
    y2017_sum = float(y2017_elec.sum())
    fd_bills = pd.Series(dtype=float)
    if y2017_sum > 0:
        for col, val in y2017_elec.items():
            col_s = str(col)
            if col_s in (EXPORT_FD_CODE, IMPORT_FD_CODE):
                continue
            fd_bills[col_s] = y_elec_total * (float(val) / y2017_sum)
    bills = industry_bills.add(fd_bills, fill_value=0.0)
    return bills.astype(float)


def reanchor_electricity_aq_after_year_scaling(
    aq: SingleRegionAqMatrixSet,
    *,
    original_year: int,
    target_year: int,
    model_year: int,
    use_commodity_pi: bool,
) -> SingleRegionAqMatrixSet:
    """Rewrite published A/q electricity G/T/D after price-index inflation."""
    from bedrock.transform.eeio.electricity_disaggregation import (  # noqa: PLC0415
        GENERATION_FUEL_COMMODITIES,
    )
    from bedrock.utils.math.formulas import backcompute_y_from_A_and_q  # noqa: PLC0415

    adom = aq.Adom.copy()
    aimp = aq.Aimp.copy()
    q = aq.scaled_q.astype(float).copy()
    elec = list(ELECTRICITY_DISAGG_SECTORS)

    adom_bills, q_bills = _inflate_summary_year_scaled_aq(
        original_year=original_year,
        target_year=target_year,
        model_year=model_year,
        use_commodity_pi=use_commodity_pi,
    )
    from bedrock.transform.eeio.cornerstone_year_scaling import (  # noqa: PLC0415
        get_summary_year_scaled_aq,
    )

    pre = get_summary_year_scaled_aq(original_year, target_year)
    bills = _purchaser_bills_from_aq(adom_bills, q_bills)
    bills[EXPORT_FD_CODE] = _scaled_export_fd_bill(
        original_year=original_year,
        target_year=target_year,
        model_year=model_year,
        use_commodity_pi=use_commodity_pi,
        pre_q=pre.q,
    )
    p_share, td_share = _go_p_and_td_shares()
    allocation = allocate_purchaser_gtd(
        bills,
        self_use_key=ELECTRICITY_AGGREGATE,
        eia_year=model_year,
        p_share_2017=p_share,
        td_share_2017=td_share,
    )
    set_reanchored_eia_purchaser_allocation(allocation)

    udom = pd.DataFrame(adom.multiply(q, axis=1))
    uimp = pd.DataFrame(aimp.multiply(q, axis=1))
    y = backcompute_y_from_A_and_q(A=adom, q=q)

    for col in udom.columns:
        col_s = str(col)
        if col_s in elec:
            continue
        if col_s in allocation.bill.index:
            udom.at[GENERATION_SECTOR, col] = float(allocation.gen_dollars[col_s])
            udom.at[TRANSMISSION_SECTOR, col] = float(allocation.t_dollars[col_s])
            udom.at[DISTRIBUTION_SECTOR, col] = float(allocation.d_dollars[col_s])
        uimp_tot = float(uimp.loc[elec, col].sum()) if col in uimp.columns else 0.0
        uimp.at[GENERATION_SECTOR, col] = uimp_tot
        uimp.at[TRANSMISSION_SECTOR, col] = 0.0
        uimp.at[DISTRIBUTION_SECTOR, col] = 0.0

    t_dom = float(allocation.bill[ELECTRICITY_AGGREGATE])
    gen_self = float(allocation.gen_dollars[ELECTRICITY_AGGREGATE])
    leftover = t_dom - gen_self
    uimp_gg = float(uimp.loc[elec, elec].sum().sum())
    for i in elec:
        for j in elec:
            udom.at[i, j] = 0.0
            uimp.at[i, j] = 0.0
    udom.at[GENERATION_SECTOR, GENERATION_SECTOR] = gen_self
    udom.at[TRANSMISSION_SECTOR, TRANSMISSION_SECTOR] = leftover * td_share
    udom.at[DISTRIBUTION_SECTOR, DISTRIBUTION_SECTOR] = leftover * (1.0 - td_share)
    uimp.at[GENERATION_SECTOR, GENERATION_SECTOR] = uimp_gg

    y = y.astype(float)
    for code in elec:
        y.loc[code] = 0.0
    fd_keys = [k for k in allocation.bill.index if k in set(FINAL_DEMANDS)]
    for col in fd_keys:
        if col == IMPORT_FD_CODE:
            continue
        y.loc[GENERATION_SECTOR] = float(y.loc[GENERATION_SECTOR]) + float(
            allocation.gen_dollars[col]
        )
        y.loc[TRANSMISSION_SECTOR] = float(y.loc[TRANSMISSION_SECTOR]) + float(
            allocation.t_dollars[col]
        )
        y.loc[DISTRIBUTION_SECTOR] = float(y.loc[DISTRIBUTION_SECTOR]) + float(
            allocation.d_dollars[col]
        )

    q = q.copy()
    for code in elec:
        q.loc[code] = float(udom.loc[code].sum()) + float(y.loc[code])

    w = q.reindex(elec).astype(float)
    w_sum = float(w.sum())
    if w_sum <= 0:
        raise ValueError('reanchored electricity q totals are non-positive')
    w = w / w_sum

    skip_rows = set(elec)
    for U in (udom, uimp):
        collapsed = U[elec].sum(axis=1)
        for row in U.index:
            if row in skip_rows:
                continue
            val = float(collapsed.loc[row])
            if row in GENERATION_FUEL_COMMODITIES:
                U.at[row, GENERATION_SECTOR] = val
                U.at[row, TRANSMISSION_SECTOR] = 0.0
                U.at[row, DISTRIBUTION_SECTOR] = 0.0
            else:
                for code in elec:
                    U.at[row, code] = val * float(w[code])

    x_g = float(q.loc[GENERATION_SECTOR])
    udom, uimp = _spill_generation_nonfuel(udom, uimp, x_g=x_g, td_share=td_share)

    q_safe = q.replace(0.0, np.nan)
    adom_out = udom.divide(q_safe, axis=1).fillna(0.0)
    aimp_out = uimp.divide(q_safe, axis=1).fillna(0.0)
    set_reanchored_electricity_q(q)
    return SingleRegionAqMatrixSet(
        Adom=cast(pt.DataFrame[AMatrix], adom_out),
        Aimp=cast(pt.DataFrame[AMatrix], aimp_out),
        scaled_q=q,
    )
