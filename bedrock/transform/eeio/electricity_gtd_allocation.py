"""EIA-anchored generation / transmission / distribution purchaser allocation.

Pure allocator plus the 2017 cached getter and Use/Y/A/q writers.
Table 2.2 / 2.14 / 3.1 loaders live in ``egrid_generation``.
"""

from __future__ import annotations

import functools
import logging
import warnings
from dataclasses import dataclass
from typing import cast

import numpy as np
import pandas as pd
import pandera.typing as pt

from bedrock.extract.disaggregation.egrid_generation import (
    egrid_mwh_for_io_year,
    eia_table_2_2_end_use_mwh,
    eia_table_2_14_export_mwh,
)
from bedrock.transform.eeio.electricity_end_use_mapping import build_end_use_map
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.schemas.single_region_schemas import AMatrix
from bedrock.utils.schemas.single_region_types import SingleRegionAqMatrixSet
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

logger = logging.getLogger(__name__)

_REANCHORED_ELECTRICITY_Q: pd.Series | None = None


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


def clear_reanchored_electricity_q() -> None:
    global _REANCHORED_ELECTRICITY_Q
    _REANCHORED_ELECTRICITY_Q = None


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


@dataclass(frozen=True)
class PurchaserAllocation:
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
    export_mwh = eia_table_2_14_export_mwh(eia_year)
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


def allocate_purchaser_gtd(
    bills: pd.Series,
    *,
    self_use_key: str,
    eia_year: int,
    p_share_2017: float,
    td_share_2017: float,
) -> PurchaserAllocation:
    """Allocate domestic electricity bills to generation, transmission, and distribution.

    ``bills`` is domestic Use columns union Y columns. Do not pass Uimp —
    imported Use is written onto the generation row separately.
    ``self_use_key`` is always ``'221100'``. Class dollar weights use
    ``clip(lower=0)`` for shares only; if ``bill <= 0``, ``gen = 0``.
    """
    if self_use_key != ELECTRICITY_AGGREGATE:
        raise ValueError(
            f'self_use_key must be {ELECTRICITY_AGGREGATE!r}, got {self_use_key!r}'
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

    leftover = bills - gen
    t_dollars = leftover * float(td_share_2017)
    d_dollars = leftover * (1.0 - float(td_share_2017))
    return PurchaserAllocation(
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
    allocation: PurchaserAllocation,
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
            Y.at[GENERATION_SECTOR, col] = orig
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
    allocation: PurchaserAllocation,
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
    allocation: PurchaserAllocation,
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
def get_2017_purchaser_allocation() -> PurchaserAllocation:
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

    allocation = get_2017_purchaser_allocation()
    children = list(ELECTRICITY_DISAGG_SECTORS)
    Y = _ensure_index_codes(Y, children)
    agg = ELECTRICITY_AGGREGATE
    for col in Y.columns:
        col_s = str(col)
        orig = _cell_float(Y, agg, str(col)) if agg in Y.index else 0.0
        if col_s == IMPORT_FD_CODE:
            Y.at[GENERATION_SECTOR, col] = orig
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
