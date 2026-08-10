"""Alternate EIA-anchored 3-way split + uniform-gen / T&D-markup design.

Constructs a **diagnostics-only** counterfactual (does not change production EEIO)
for a redesigned PR3+PR4 electricity path. Year rule:

- **Reallocation + 3-way construction** use IO account year
  (``usa_base_io_data_year``, 2017): unscaled A/q and Y, EIA Tables 2.2 / 2.4,
  eGRID for ``p_uniform`` (2018 when no 2017 inventory), and UGO305 GO shares.
- **Unit conversion** (not applied here) targets model year
  (``model_base_year``, 2024): after year-scaling, ``c_col`` / ``c_row`` would
  use model-year eGRID and (under this design) a near-flat gen price, with
  class retail markups already in T&D dollars.

Design steps (monetary, 2017 chain):

1. Allocate EIA Table 2.2 class MWh to IO purchasers ∝ post-reallocation
   aggregate ``221100`` $ (within class).
2. Uniform gen price ``p = (production 221110 Use+Y $) / eGRID MWh``; purchaser
   gen $ = allocated sales MWh × p (Σ gen $ on sales sits below 221110 $ by
   the eGRID−sales gap × p).
3. T&D residual recovers Table 2.4 class retail; clip gen down if residual
   would be negative.
4. Split T&D $ with UGO305 T/(T+D) shares; report Make-last Use+Y shares
   (comparison only).

Compares to the **current** mixed-units production path (model-year unit
conversion).

Run:
    python -m bedrock.analysis.electricity_disagg_diagnostics.alternate_eia_anchored_split
"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping, cast

import numpy as np
import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.full_trace.full_trace import (
    _clear_model_caches,
)
from bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry.hh_vs_interindustry import (
    HH_FD_CODE,
    _eia_table_2_2_sales_mwh,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR
from bedrock.extract.disaggregation.egrid_generation import (
    egrid_inventory_years,
    us_total_net_generation_mwh,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    _model_year_y_row_221110,
    derive_disagg_Ytot_with_trade,
    electricity_conversion_factors,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq,
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    build_electricity_disagg_go_weights,
)
from bedrock.transform.eeio.electricity_end_use_mapping import (
    build_end_use_map,
    electricity_end_use_retail_prices_cents_kwh,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)

logger = logging.getLogger(__name__)

REALLOC_CONFIG = '2025_usa_cornerstone_v0_3_electricity_reallocation'
SPLIT_CONFIG = '2025_usa_cornerstone_v0_3_electricity_disaggregation'
MIXED_CONFIG = '2025_usa_cornerstone_v0_3_electricity_mixed_units'
TRANS_SECTOR = '221121'
DIST_SECTOR = '221122'

OUT_SUBDIR = OUT_DIR / 'alternate_eia_anchored_split'
REPORT_MD = OUT_SUBDIR / 'eia_anchored_td_markup_counterfactual.md'
REPORT_JSON = OUT_SUBDIR / 'eia_anchored_td_markup_counterfactual.json'
FIGURE_GTD_COLUMNS = OUT_SUBDIR / 'figure_gtd_columns_after_uc_mixed.png'

CENTS_PER_KWH_TO_USD_PER_MWH = 10.0
SALES_CLASSES: tuple[str, ...] = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
)

# Columns for post–unit-conversion G/T/D comparison heatmaps.
HEATMAP_INDUSTRY_COLS: tuple[str, ...] = ('484000', '324110')
HEATMAP_FD_COLS: tuple[str, ...] = (HH_FD_CODE,)
HEATMAP_COL_LABELS: dict[str, str] = {
    '484000': 'Truck transport\n484000',
    '324110': 'Petroleum refining\n324110',
    HH_FD_CODE: 'HH FD\nF01000',
}
HEATMAP_ROW_CODES: tuple[str, ...] = (
    GENERATION_SECTOR,
    TRANS_SECTOR,
    DIST_SECTOR,
)
HEATMAP_ROW_LABELS: dict[str, str] = {
    GENERATION_SECTOR: 'Gen\n221110\n(TWh)',
    TRANS_SECTOR: 'Trans\n221121\n($B)',
    DIST_SECTOR: 'Dist\n221122\n($B)',
}


def _egrid_year_for_io_account(io_year: int) -> int:
    """Resolve eGRID inventory year for IO-account-year anchors.

    eGRID has no 2017 release; for ``usa_base_io_data_year=2017`` use **2018**
    (not EPA's preceding-year default of 2016).
    """
    available = set(egrid_inventory_years(io_year - 2, io_year + 2))
    if io_year in available:
        return io_year
    if io_year == 2017 and 2018 in available:
        return 2018
    later = sorted(y for y in available if y > io_year)
    if later:
        return later[0]
    earlier = sorted((y for y in available if y < io_year), reverse=True)
    if earlier:
        return earlier[0]
    raise ValueError(f'No eGRID inventory near IO account year {io_year}')


def _safe_div(num: float, den: float) -> float:
    return num / den if den else float('nan')


def _pct(x: float) -> str:
    return f'{100.0 * x:.1f}%' if np.isfinite(x) else 'n/a'


def _fmt_twh(mwh: float) -> str:
    return f'{mwh / 1e6:,.1f}'


def _fmt_b(usd: float) -> str:
    return f'{usd / 1e9:,.2f}'


def _fmt_price(usd_per_mwh: float) -> str:
    return f'{usd_per_mwh:,.2f}'


def _fmt_twh_precise(mwh: float) -> str:
    return f'{mwh / 1e6:,.2f}'


def _usd_per_mwh_to_cents_kwh(usd_per_mwh: float) -> float:
    return usd_per_mwh / CENTS_PER_KWH_TO_USD_PER_MWH


def _install(config: str) -> None:
    reset_usa_config()
    _clear_model_caches()
    set_global_usa_config(config)


def _agg_electricity_usd_flows() -> tuple[pd.Series, pd.Series, int, int, int]:
    """Post-reallocation / pre–3-way ``221100`` intermediate + FD USD (2017 chain).

    Used only for **within-class MWh allocation weights**, not for p_uniform.
    Dollars are unscaled IO-account-year (``usa_base_io_data_year``) flows.
    """
    _install(REALLOC_CONFIG)
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    cfg = get_usa_config()
    # Unscaled A/q: reallocation + 3-way redesign sit on 2017-chain IO, before
    # model-year scaling and before unit conversion.
    aq = derive_cornerstone_Aq()
    adom = aq.Adom.copy()
    aimp = aq.Aimp.copy()
    q = aq.scaled_q.astype(float)
    adom.index = adom.index.astype(str)
    adom.columns = adom.columns.astype(str)
    aimp.index = aimp.index.astype(str)
    aimp.columns = aimp.columns.astype(str)
    q.index = q.index.astype(str)
    agg = ELECTRICITY_AGGREGATE_SECTOR
    a_tot = adom.add(aimp, fill_value=0.0)
    intermediate = (a_tot.loc[agg].astype(float) * q).astype(float)
    intermediate = intermediate.clip(lower=0.0)

    Y = derive_disagg_Ytot_with_trade().copy()
    Y.index = Y.index.astype(str)
    Y.columns = Y.columns.astype(str)
    y_row = cast(pd.Series, Y.loc[agg].astype(float).clip(lower=0.0))
    return (
        cast(pd.Series, intermediate),
        y_row,
        int(cfg.usa_base_io_data_year),
        int(cfg.model_base_year),
        int(cfg.usa_ghg_data_year),
    )


def _production_221110_use_y_usd() -> tuple[float, float, float]:
    """Post–3-way monetary ``221110`` intermediate + FD USD and q (2017 chain).

    Uses the disaggregation (not mixed-units) config so dollars are still USD,
    and unscaled A/q + 2017 Y so the counterfactual matches the pre–unit-conversion
    IO account year.
    """
    _install(SPLIT_CONFIG)
    aq = derive_cornerstone_Aq()
    adom = aq.Adom.copy()
    aimp = aq.Aimp.copy()
    q = aq.scaled_q.astype(float)
    adom.index = adom.index.astype(str)
    adom.columns = adom.columns.astype(str)
    aimp.index = aimp.index.astype(str)
    aimp.columns = aimp.columns.astype(str)
    q.index = q.index.astype(str)
    gen = GENERATION_SECTOR
    a_tot = adom.add(aimp, fill_value=0.0)
    intermediate = float((a_tot.loc[gen].astype(float) * q).clip(lower=0.0).sum())
    y_2017 = derive_disagg_Ytot_with_trade().loc[gen].astype(float).clip(lower=0.0)
    y_usd = float(y_2017.sum())
    q_gen = float(q.loc[gen])
    return intermediate, y_usd, q_gen


def _combined_purchaser_usd(
    intermediate: pd.Series,
    y_row: pd.Series,
) -> pd.Series:
    """Stack intermediate industry columns and FD columns (unique codes)."""
    inter = intermediate.copy()
    inter.index = inter.index.map(lambda c: ('U', str(c)))
    fd = y_row.copy()
    fd.index = fd.index.map(lambda c: ('Y', str(c)))
    return pd.concat([inter, fd]).astype(float)


def _allocate_mwh_from_eia(
    purchaser_usd: pd.Series,
    end_use_map: Mapping[str, str],
    eia_mwh: Mapping[str, float],
) -> pd.Series:
    """Within-class proportional allocation of Table 2.2 sales MWh."""
    out = pd.Series(0.0, index=purchaser_usd.index, dtype=float)
    for end_use in SALES_CLASSES:
        class_mwh = float(eia_mwh.get(end_use, 0.0))
        mask = []
        for key in purchaser_usd.index:
            _kind, code = key
            mask.append(end_use_map.get(str(code)) == end_use)
        cols = purchaser_usd.index[np.array(mask, dtype=bool)]
        usd = purchaser_usd.reindex(cols).fillna(0.0)
        total_usd = float(usd.sum())
        if class_mwh <= 0 or total_usd <= 0:
            continue
        out.loc[cols] = usd / total_usd * class_mwh
    return out


def _td_national_shares() -> tuple[float, float]:
    """UGO305 transmission / distribution shares of (T+D) only."""
    w = build_electricity_disagg_go_weights()
    t = float(w[TRANS_SECTOR])
    d = float(w[DIST_SECTOR])
    s = t + d
    return t / s, d / s


def build_counterfactual_rows(
    purchaser_usd: pd.Series,
    mwh: pd.Series,
    end_use_map: Mapping[str, str],
    prices_cents: Mapping[str, float],
    p_uniform_usd_per_mwh: float,
    w_trans: float,
    w_dist: float,
) -> pd.DataFrame:
    """Build per-purchaser gen / T / D USD under option A + clip rule B."""
    rows: list[dict[str, Any]] = []
    for key, usd0 in purchaser_usd.items():
        kind, code = cast(tuple[str, str], key)
        end_use = end_use_map.get(str(code), 'Commercial')
        mwh_j = float(mwh.get(key, 0.0))
        p_retail = float(prices_cents.get(end_use, prices_cents['Total']))
        p_retail_usd_mwh = p_retail * CENTS_PER_KWH_TO_USD_PER_MWH
        gen_target = mwh_j * p_uniform_usd_per_mwh
        retail_bill = mwh_j * p_retail_usd_mwh
        clipped = gen_target > retail_bill + 1e-9 and mwh_j > 0
        if clipped:
            gen_usd = retail_bill
            td_usd = 0.0
        else:
            gen_usd = gen_target
            td_usd = max(0.0, retail_bill - gen_usd)
        trans_usd = td_usd * w_trans
        dist_usd = td_usd * w_dist
        rows.append(
            {
                'kind': kind,
                'code': str(code),
                'end_use': end_use,
                'baseline_221100_USD': float(usd0),
                'mwh': mwh_j,
                'gen_USD': gen_usd,
                'trans_USD': trans_usd,
                'dist_USD': dist_usd,
                'td_USD': td_usd,
                'all_in_USD': gen_usd + td_usd,
                'p_uniform_USD_per_MWh': p_uniform_usd_per_mwh,
                'p_retail_USD_per_MWh': p_retail_usd_mwh,
                'implied_gen_USD_per_MWh': _safe_div(gen_usd, mwh_j),
                'implied_all_in_USD_per_MWh': _safe_div(gen_usd + td_usd, mwh_j),
                'gen_clipped_to_retail': bool(clipped),
                'is_hh_fd': kind == 'Y' and str(code) == HH_FD_CODE,
            }
        )
    return pd.DataFrame(rows)


def _sum_by_end_use(df: pd.DataFrame, value_col: str) -> dict[str, float]:
    out: dict[str, float] = {eu: 0.0 for eu in SALES_CLASSES}
    for end_use, g in df.groupby('end_use'):
        if end_use in out:
            out[str(end_use)] = float(g[value_col].sum())
    return out


def _production_gen_by_class() -> dict[str, Any]:
    """Current mixed-units 221110 MWh and pre-conversion USD by end-use class."""
    _install(MIXED_CONFIG)
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    cfg = get_usa_config()
    end_use_map = build_end_use_map()
    aq_scaled = derive_cornerstone_Aq_scaled()
    inter_usd = (
        aq_scaled.Adom.loc[GENERATION_SECTOR].astype(float) * aq_scaled.scaled_q
    ).astype(float)
    # Match production FD construction used elsewhere.
    y_usd = _model_year_y_row_221110(aq_scaled).astype(float)

    c_col, c_row = electricity_conversion_factors(aq_scaled)
    aq_m = derive_cornerstone_Aq_mixed_units()
    inter_mwh = (aq_m.Adom.loc[GENERATION_SECTOR].astype(float) * aq_m.scaled_q).astype(
        float
    )
    # FD MWh: apply same class factors to model-year Y row.
    y_mwh = y_usd * c_row.reindex(y_usd.index).astype(float)

    def by_class(inter: pd.Series, fd: pd.Series) -> dict[str, float]:
        result: dict[str, float] = {}
        for eu in SALES_CLASSES:
            i_cols = [c for c in inter.index if end_use_map.get(str(c)) == eu]
            f_cols = [c for c in fd.index if end_use_map.get(str(c)) == eu]
            result[eu] = float(inter.reindex(i_cols).sum() + fd.reindex(f_cols).sum())
        return result

    return {
        'model_base_year': int(cfg.model_base_year),
        'c_col': float(c_col),
        'c_row_min': float(c_row.min()),
        'c_row_max': float(c_row.max()),
        'usd_by_class': by_class(
            cast(pd.Series, inter_usd.clip(lower=0.0)),
            cast(pd.Series, y_usd.clip(lower=0.0)),
        ),
        'mwh_by_class': by_class(
            cast(pd.Series, inter_mwh.clip(lower=0.0)),
            cast(pd.Series, y_mwh.clip(lower=0.0)),
        ),
        'hh_fd_mwh': float(y_mwh.get(HH_FD_CODE, 0.0)),
        'q_gen_mwh': float(aq_m.scaled_q.loc[GENERATION_SECTOR]),
    }


def _model_year_agg_electricity_usd_flows() -> tuple[pd.Series, pd.Series]:
    """Model-year-scaled post-reallocation ``221100`` intermediate + FD USD."""
    from bedrock.utils.math.formulas import backcompute_y_from_A_and_q  # noqa: PLC0415

    _install(REALLOC_CONFIG)
    aq = derive_cornerstone_Aq_scaled()
    adom = aq.Adom.copy()
    aimp = aq.Aimp.copy()
    q = aq.scaled_q.astype(float)
    adom.index = adom.index.astype(str)
    adom.columns = adom.columns.astype(str)
    aimp.index = aimp.index.astype(str)
    aimp.columns = aimp.columns.astype(str)
    q.index = q.index.astype(str)
    agg = ELECTRICITY_AGGREGATE_SECTOR
    a_tot = adom.add(aimp, fill_value=0.0)
    intermediate = (a_tot.loc[agg].astype(float) * q).astype(float).clip(lower=0.0)

    y_2017 = derive_disagg_Ytot_with_trade().loc[agg].astype(float).clip(lower=0.0)
    y_2017.index = y_2017.index.astype(str)
    y_total = float(backcompute_y_from_A_and_q(A=adom, q=q).loc[agg])
    y_sum = float(y_2017.sum())
    if y_sum <= 0:
        raise ValueError('model-year 221100 Y: 2017 Y row sums to zero or negative')
    y_row = cast(pd.Series, y_total * (y_2017 / y_sum))
    return cast(pd.Series, intermediate), y_row


def _cf_after_unit_conversion(
    model_year: int,
    production: Mapping[str, Any],
) -> dict[str, Any]:
    """Gen-row MWh by class after unit conversion under this CF (model year).

    Re-applies the EIA Table 2.2 sales MWh anchor at ``model_year`` using
    model-year-scaled post-reallocation ``221100`` $ as within-class weights.
    Class totals therefore match EIA ``model_year`` sales. Current production
    mixed-units MWh (same year) are included for comparison. National
    ``q_221110`` remains eGRID (``model_year``) via ``c_col``.
    """
    intermediate, y_row = _model_year_agg_electricity_usd_flows()
    end_use_map = build_end_use_map()
    eia = _eia_table_2_2_sales_mwh(model_year)
    egrid_mwh = float(us_total_net_generation_mwh(model_year))
    purchaser_usd = _combined_purchaser_usd(intermediate, y_row)
    mwh = _allocate_mwh_from_eia(purchaser_usd, end_use_map, eia)

    # Stack as DataFrame for HH / class sums
    rows = []
    for key, mwh_j in mwh.items():
        kind, code = cast(tuple[str, str], key)
        rows.append(
            {
                'kind': kind,
                'code': str(code),
                'end_use': end_use_map.get(str(code), 'Commercial'),
                'mwh': float(mwh_j),
                'is_hh_fd': kind == 'Y' and str(code) == HH_FD_CODE,
            }
        )
    detail = pd.DataFrame(rows)
    by_class = _sum_by_end_use(detail, 'mwh')
    eia_by_class = {c: float(eia.get(c, float('nan'))) for c in SALES_CLASSES}
    prod_by_class = {c: float(production['mwh_by_class'][c]) for c in SALES_CLASSES}
    hh_mwh = (
        float(detail.loc[detail['is_hh_fd'], 'mwh'].sum())
        if detail['is_hh_fd'].any()
        else float('nan')
    )
    eia_res = float(eia_by_class['Residential'])
    return {
        'model_base_year': model_year,
        'method': (
            'Allocate EIA Table 2.2 (model year) sales MWh within class ∝ '
            'model-year-scaled post-reallocation 221100 $; flat gen c_row would '
            'realize these MWh on the generation Use/Y row'
        ),
        'eia_table_2_2_sales_MWh': eia_by_class,
        'eia_sales_total_MWh': sum(eia_by_class.values()),
        'counterfactual_gen_MWh': by_class,
        'counterfactual_gen_total_MWh': float(mwh.sum()),
        'production_mixed_gen_MWh': prod_by_class,
        'production_gen_total_MWh': sum(prod_by_class.values()),
        'egrid_net_generation_MWh': egrid_mwh,
        'egrid_minus_eia_sales_MWh': egrid_mwh - sum(eia_by_class.values()),
        'hh_f01000': {
            'counterfactual_mwh': hh_mwh,
            'production_mwh': float(production['hh_fd_mwh']),
            'eia_residential_mwh': eia_res,
            'cf_vs_eia_residential': _safe_div(hh_mwh, eia_res),
            'prod_vs_eia_residential': _safe_div(
                float(production['hh_fd_mwh']), eia_res
            ),
        },
        'c_row_note': (
            'Under this CF, gen-row c_row is ~flat (uniform gen $/MWh); class retail '
            'markups stay in T&D $. Production today uses Table 2.4-varying c_row.'
        ),
    }


def _model_year_y_row_for_sector(
    aq_scaled: Any,
    sector: str,
) -> pd.Series:
    """Model-year FD row for one commodity: backcompute total × 2017 Y shares."""
    from bedrock.utils.math.formulas import backcompute_y_from_A_and_q  # noqa: PLC0415

    y_2017 = derive_disagg_Ytot_with_trade().loc[sector].astype(float)
    y_2017.index = y_2017.index.astype(str)
    y_total = float(
        backcompute_y_from_A_and_q(A=aq_scaled.Adom, q=aq_scaled.scaled_q).loc[sector]
    )
    y_sum = float(y_2017.sum())
    if y_sum <= 0:
        raise ValueError(f'model-year Y row for {sector}: 2017 sum non-positive')
    return cast(pd.Series, y_total * (y_2017 / y_sum))


def _production_mixed_gtd_column_matrix() -> pd.DataFrame:
    """Production after UC: gen in MWh, T/D in USD for heatmap columns."""
    _install(MIXED_CONFIG)
    aq_s = derive_cornerstone_Aq_scaled()
    aq_m = derive_cornerstone_Aq_mixed_units()
    c_col, c_row = electricity_conversion_factors(aq_s)
    del c_col  # used implicitly via mixed A/q

    a_tot = aq_m.Adom.add(aq_m.Aimp, fill_value=0.0)
    a_tot.index = a_tot.index.astype(str)
    a_tot.columns = a_tot.columns.astype(str)
    q = aq_m.scaled_q.astype(float)
    q.index = q.index.astype(str)

    data: dict[str, dict[str, float]] = {r: {} for r in HEATMAP_ROW_CODES}
    for col in HEATMAP_INDUSTRY_COLS:
        for row in HEATMAP_ROW_CODES:
            data[row][col] = float(a_tot.loc[row, col]) * float(q.loc[col])

    y_gen = _model_year_y_row_221110(aq_s).astype(float)
    y_gen.index = y_gen.index.astype(str)
    y_trans = _model_year_y_row_for_sector(aq_s, TRANS_SECTOR)
    y_dist = _model_year_y_row_for_sector(aq_s, DIST_SECTOR)
    for fd in HEATMAP_FD_COLS:
        data[GENERATION_SECTOR][fd] = float(y_gen.get(fd, 0.0)) * float(
            c_row.get(fd, 0.0)
        )
        data[TRANS_SECTOR][fd] = float(y_trans.get(fd, 0.0))
        data[DIST_SECTOR][fd] = float(y_dist.get(fd, 0.0))

    cols = list(HEATMAP_INDUSTRY_COLS) + list(HEATMAP_FD_COLS)
    return pd.DataFrame(
        {c: {r: data[r][c] for r in HEATMAP_ROW_CODES} for c in cols}
    ).reindex(index=list(HEATMAP_ROW_CODES), columns=cols)


def _cf_after_uc_gtd_column_matrix(
    model_year: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """CF after UC: gen MWh from EIA anchor; T/D $ from retail residual (model year).

    Also returns implied ¢/kWh by end-use class from (gen$+T$+D$)/gen MWh.
    """
    intermediate, y_row = _model_year_agg_electricity_usd_flows()
    end_use_map = build_end_use_map()
    eia = _eia_table_2_2_sales_mwh(model_year)
    prices = cast(
        dict[str, float], electricity_end_use_retail_prices_cents_kwh(model_year)
    )
    purchaser_usd = _combined_purchaser_usd(intermediate, y_row)
    mwh = _allocate_mwh_from_eia(purchaser_usd, end_use_map, eia)

    # Model-year uniform gen price from production 221110 Use+Y $ / eGRID.
    _install(MIXED_CONFIG)
    aq_s = derive_cornerstone_Aq_scaled()
    a_tot_s = aq_s.Adom.add(aq_s.Aimp, fill_value=0.0)
    a_tot_s.index = a_tot_s.index.astype(str)
    a_tot_s.columns = a_tot_s.columns.astype(str)
    q_s = aq_s.scaled_q.astype(float)
    q_s.index = q_s.index.astype(str)
    gen_inter = float(
        (a_tot_s.loc[GENERATION_SECTOR].astype(float) * q_s).clip(lower=0.0).sum()
    )
    gen_fd = float(_model_year_y_row_221110(aq_s).astype(float).clip(lower=0.0).sum())
    egrid = float(us_total_net_generation_mwh(model_year))
    p_uniform = _safe_div(gen_inter + gen_fd, egrid)
    w_trans, w_dist = _td_national_shares()

    cf = build_counterfactual_rows(
        purchaser_usd,
        mwh,
        end_use_map,
        prices,
        p_uniform,
        w_trans,
        w_dist,
    )

    cols = list(HEATMAP_INDUSTRY_COLS) + list(HEATMAP_FD_COLS)
    out = pd.DataFrame(0.0, index=list(HEATMAP_ROW_CODES), columns=cols)
    for col in HEATMAP_INDUSTRY_COLS:
        row = cf.loc[(cf['kind'] == 'U') & (cf['code'] == col)]
        if row.empty:
            continue
        r0 = row.iloc[0]
        out.loc[GENERATION_SECTOR, col] = float(r0['mwh'])
        out.loc[TRANS_SECTOR, col] = float(r0['trans_USD'])
        out.loc[DIST_SECTOR, col] = float(r0['dist_USD'])
    for fd in HEATMAP_FD_COLS:
        row = cf.loc[(cf['kind'] == 'Y') & (cf['code'] == fd)]
        if row.empty:
            continue
        r0 = row.iloc[0]
        out.loc[GENERATION_SECTOR, fd] = float(r0['mwh'])
        out.loc[TRANS_SECTOR, fd] = float(r0['trans_USD'])
        out.loc[DIST_SECTOR, fd] = float(r0['dist_USD'])

    by_class: dict[str, dict[str, float]] = {}
    for eu in SALES_CLASSES:
        g = cf.loc[cf['end_use'] == eu]
        mwh_eu = float(g['mwh'].sum())
        gen_usd = float(g['gen_USD'].sum())
        td_usd = float(g['td_USD'].sum())
        all_in = float(g['all_in_USD'].sum())
        by_class[eu] = {
            'gen_MWh': mwh_eu,
            'gen_USD': gen_usd,
            'td_USD': td_usd,
            'all_in_USD': all_in,
            'gen_cents_per_kWh': _usd_per_mwh_to_cents_kwh(_safe_div(gen_usd, mwh_eu)),
            'all_in_cents_per_kWh': _usd_per_mwh_to_cents_kwh(
                _safe_div(all_in, mwh_eu)
            ),
            'table_2_4_cents_per_kWh': float(prices[eu]),
        }
    implied = {
        'model_base_year': model_year,
        'p_uniform_USD_per_MWh': p_uniform,
        'p_uniform_cents_per_kWh': p_uniform / CENTS_PER_KWH_TO_USD_PER_MWH,
        'method': (
            'After UC, gen-row MWh by class = EIA Table 2.2 sales. Monetary gen $ '
            'are still MWh × p_uniform (p = model-year 221110 Use+Y $ / eGRID); '
            'T&D $ = class retail bill − gen $ (Table 2.4). Implied all-in ¢/kWh = '
            '(gen$ + T$ + D$) / gen MWh.'
        ),
        'by_end_use_class': by_class,
    }
    return out, implied


def _display_units_matrix(raw: pd.DataFrame) -> pd.DataFrame:
    """Gen MWh→TWh; T/D USD→$B for annotation."""
    out = raw.astype(float).copy()
    out.loc[GENERATION_SECTOR] = out.loc[GENERATION_SECTOR] / 1e6
    out.loc[TRANS_SECTOR] = out.loc[TRANS_SECTOR] / 1e9
    out.loc[DIST_SECTOR] = out.loc[DIST_SECTOR] / 1e9
    return out


def plot_gtd_columns_after_uc_figure(
    production: pd.DataFrame,
    counterfactual: pd.DataFrame,
    *,
    model_year: int,
) -> Any:
    """Two-panel mixed-unit heatmap: production vs CF after unit conversion."""
    import matplotlib.pyplot as plt  # noqa: PLC0415

    prod_d = _display_units_matrix(production)
    cf_d = _display_units_matrix(counterfactual)
    # Shared color norms across panels so intensities are comparable.
    combined = pd.concat([prod_d, cf_d], axis=1)
    gen_max = float(combined.loc[GENERATION_SECTOR].max())
    td_max = float(combined.loc[[TRANS_SECTOR, DIST_SECTOR]].to_numpy().max())

    def color_with_shared(display: pd.DataFrame) -> np.ndarray:
        arr = display.to_numpy(dtype=float)
        color = np.zeros_like(arr)
        if gen_max > 0:
            color[0, :] = arr[0, :] / gen_max
        if td_max > 0:
            color[1:, :] = arr[1:, :] / td_max
        return color

    panels = [
        ('Current production (mixed units)', prod_d, color_with_shared(prod_d)),
        ('This CF after unit conversion', cf_d, color_with_shared(cf_d)),
    ]
    col_labels = [HEATMAP_COL_LABELS[c] for c in prod_d.columns]
    row_labels = [HEATMAP_ROW_LABELS[r] for r in prod_d.index]

    fig, axes = plt.subplots(1, 2, figsize=(8.2, 4.0))
    im = None
    for ax, (title, display, color) in zip(axes, panels, strict=True):
        im = ax.imshow(color, cmap='YlOrRd', vmin=0.0, vmax=1.0)
        ax.set_xticks(range(len(col_labels)))
        ax.set_yticks(range(len(row_labels)))
        ax.set_xticklabels(col_labels, fontsize=7)
        ax.set_yticklabels(row_labels, fontsize=7)
        ax.set_xlabel('Purchaser column', fontsize=8)
        ax.set_ylabel('Commodity row', fontsize=8)
        ax.set_title(title, fontsize=9, pad=8)
        arr = display.to_numpy(dtype=float)
        for i in range(arr.shape[0]):
            for j in range(arr.shape[1]):
                val = arr[i, j]
                if not np.isfinite(val) or val <= 0:
                    ax.text(
                        j, i, '—', ha='center', va='center', fontsize=7, color='#666'
                    )
                    continue
                row_code = display.index[i]
                if row_code == GENERATION_SECTOR:
                    label = f'{val:.3f}' if val < 1 else f'{val:.2f}'
                elif row_code == TRANS_SECTOR:
                    # Keep $B units; use enough decimals that small T cells are not 0.00.
                    label = f'{val:.3f}' if val < 0.1 else f'{val:.2f}'
                else:
                    label = f'{val:.2f}'
                ax.text(
                    j,
                    i,
                    label,
                    ha='center',
                    va='center',
                    fontsize=7,
                    color='black' if color[i, j] < 0.55 else 'white',
                )

    fig.suptitle(
        f'G/T/D Use+Y by purchaser after unit conversion ({model_year})\n'
        'Gen in TWh; T&D in $B (color scaled separately within each unit type)',
        fontsize=10,
        weight='bold',
        y=1.05,
    )
    if im is not None:
        cbar = fig.colorbar(im, ax=axes, fraction=0.03, pad=0.02)
        cbar.set_label('Relative intensity\n(within Gen or within T&D)', fontsize=7)
    fig.subplots_adjust(top=0.78, wspace=0.35)
    return fig


def analyze() -> dict[str, Any]:
    intermediate, y_row, io_year, model_year, ghg_year = _agg_electricity_usd_flows()
    gen_inter_usd, gen_fd_usd, q_gen_usd = _production_221110_use_y_usd()
    gen_use_y_usd = gen_inter_usd + gen_fd_usd
    # Pre–unit-conversion anchors: IO account year (2017), not model year.
    # eGRID has no 2017 inventory → resolve to 2018 (see helper).
    egrid_year = _egrid_year_for_io_account(io_year)
    egrid_mwh = float(us_total_net_generation_mwh(egrid_year))

    end_use_map = build_end_use_map()
    prices = cast(
        dict[str, float], electricity_end_use_retail_prices_cents_kwh(io_year)
    )
    eia = _eia_table_2_2_sales_mwh(io_year)
    w_go = build_electricity_disagg_go_weights()
    w_trans, w_dist = _td_national_shares()

    purchaser_usd = _combined_purchaser_usd(intermediate, y_row)
    baseline_221100_usd = float(purchaser_usd.sum())
    mwh = _allocate_mwh_from_eia(purchaser_usd, end_use_map, eia)
    allocated_mwh = float(mwh.sum())
    eia_sales_total = sum(float(eia.get(c, 0.0)) for c in SALES_CLASSES)
    p_uniform = _safe_div(gen_use_y_usd, egrid_mwh)

    cf = build_counterfactual_rows(
        purchaser_usd,
        mwh,
        end_use_map,
        prices,
        p_uniform,
        w_trans,
        w_dist,
    )

    gen_total = float(cf['gen_USD'].sum())
    trans_total = float(cf['trans_USD'].sum())
    dist_total = float(cf['dist_USD'].sum())
    td_total = float(cf['td_USD'].sum())
    all_in_total = float(cf['all_in_USD'].sum())
    n_clipped = int(cf['gen_clipped_to_retail'].sum())
    clipped_mwh = float(cf.loc[cf['gen_clipped_to_retail'], 'mwh'].sum())

    # Make-last weights from Use+Y row totals
    make_last = {
        '221110': gen_total,
        '221121': trans_total,
        '221122': dist_total,
    }
    make_sum = sum(make_last.values())
    make_last_shares = {k: _safe_div(v, make_sum) for k, v in make_last.items()}
    ugo_shares = {str(k): float(v) for k, v in w_go.items()}

    cf_mwh_by_class = _sum_by_end_use(cf, 'mwh')
    cf_gen_usd_by_class = _sum_by_end_use(cf, 'gen_USD')
    cf_trans_usd_by_class = _sum_by_end_use(cf, 'trans_USD')
    cf_dist_usd_by_class = _sum_by_end_use(cf, 'dist_USD')
    cf_td_usd_by_class = _sum_by_end_use(cf, 'td_USD')
    cf_all_in_by_class = _sum_by_end_use(cf, 'all_in_USD')

    hh = cf.loc[cf['is_hh_fd']].iloc[0] if (cf['is_hh_fd']).any() else None

    production = _production_gen_by_class()
    eia_res = float(eia.get('Residential', float('nan')))
    after_uc = _cf_after_unit_conversion(model_year, production)
    prod_gtd = _production_mixed_gtd_column_matrix()
    cf_gtd, implied_prices_uc = _cf_after_uc_gtd_column_matrix(model_year)

    # Implied gen price dispersion after clip rule B
    positive = cf.loc[cf['mwh'] > 0]
    implied_gen = positive['implied_gen_USD_per_MWh']
    mwh_weighted_avg_gen_price = _safe_div(gen_total, allocated_mwh)

    payload: dict[str, Any] = {
        'design': {
            'scope': 'PR3+PR4 diagnostics counterfactual (no production code changes)',
            'mwh_anchor': (
                'start from EIA Table 2.2 (IO account year) sales MWh by end-use '
                'class; within each class, give IO sectors MWh in proportion to '
                'their monetary electricity purchases after reallocation '
                '(aggregate 221100, 2017-chain $)'
            ),
            'gen_price': (
                'charge every purchaser the same generation price: production '
                '221110 Use+Y dollars (2017 chain) ÷ eGRID net generation MWh '
                f'(inventory {egrid_year}; IO account year {io_year} has no eGRID); '
                'purchaser gen $ = allocated sales MWh × that price; if that would '
                'exceed the class retail bill (Table 2.4, IO account year), cut gen $ '
                'down to the bill so T&D is not negative'
                if egrid_year != io_year
                else (
                    'charge every purchaser the same generation price: production '
                    '221110 Use+Y dollars (2017 chain) ÷ eGRID net generation MWh '
                    '(IO account year); purchaser gen $ = allocated sales MWh × that '
                    'price; if that would exceed the class retail bill (Table 2.4, '
                    'IO account year), cut gen $ down to the bill so T&D is not negative'
                )
            ),
            'td_rule': (
                'T&D $ is the leftover needed to reach the class retail bill after '
                'generation; split that leftover into transmission vs distribution '
                'using national UGO T/(T+D) shares (IO account year GO)'
            ),
            'make_last': (
                'report Make commodity weights from the resulting gen / T / D '
                'purchase totals (shown for comparison only; not applied)'
            ),
            'w_trans_of_td': w_trans,
            'w_dist_of_td': w_dist,
            'p_uniform_denominator': (
                f'eGRID US net generation MWh (inventory year {egrid_year}; '
                f'IO account year {io_year} has no eGRID release)'
                if egrid_year != io_year
                else f'eGRID US net generation MWh (IO account year {io_year})'
            ),
            'p_uniform_numerator': (
                f'production 221110 Use+Y USD (3-way monetary, {io_year} chain)'
            ),
        },
        'years': {
            'io_account_year': io_year,
            'model_base_year': model_year,
            'usa_ghg_data_year': ghg_year,
            'counterfactual_external_years': {
                'eia_table_2_2': io_year,
                'eia_table_2_4': io_year,
                'egrid_for_p_uniform': egrid_year,
                'egrid_requested_io_year': io_year,
                'ugo_go_weights': io_year,
                'note': (
                    'Reallocation / 3-way counterfactual construction uses IO '
                    f'account year ({io_year}). eGRID has no {io_year} inventory; '
                    f'p_uniform uses eGRID {egrid_year}. Unit conversion comparison '
                    'uses model year.'
                ),
            },
            'reallocation_config': REALLOC_CONFIG,
            'split_config': SPLIT_CONFIG,
            'production_compare_config': MIXED_CONFIG,
        },
        'baseline_221100_for_mwh_weights': {
            'intermediate_USD': float(intermediate.sum()),
            'final_demand_USD': float(y_row.sum()),
            'total_USD': baseline_221100_usd,
            'note': 'Used only to allocate EIA class MWh within IO sectors',
        },
        'production_221110_use_y': {
            'intermediate_USD': gen_inter_usd,
            'final_demand_USD': gen_fd_usd,
            'use_y_total_USD': gen_use_y_usd,
            'q_USD': q_gen_usd,
            'use_y_minus_q_USD': gen_use_y_usd - q_gen_usd,
        },
        'egrid_net_generation_MWh': egrid_mwh,
        'eia_table_2_2_sales_MWh': {
            c: float(eia.get(c, float('nan'))) for c in SALES_CLASSES
        },
        'eia_sales_total_MWh': eia_sales_total,
        'allocated_mwh_total': allocated_mwh,
        'egrid_minus_allocated_sales_MWh': egrid_mwh - allocated_mwh,
        'p_uniform_USD_per_MWh': p_uniform,
        'p_uniform_cents_per_kWh': p_uniform / CENTS_PER_KWH_TO_USD_PER_MWH,
        'table_2_4_cents_per_kWh': {k: float(v) for k, v in prices.items()},
        'counterfactual_totals': {
            'gen_USD': gen_total,
            'trans_USD': trans_total,
            'dist_USD': dist_total,
            'td_USD': td_total,
            'all_in_USD': all_in_total,
            'gen_vs_221110_use_y_USD': gen_total - gen_use_y_usd,
            'gen_vs_221110_implied_on_sales_MWh_USD': gen_total
            - allocated_mwh * p_uniform,
            'all_in_minus_221100_USD': all_in_total - baseline_221100_usd,
            'mwh_weighted_avg_implied_gen_USD_per_MWh': mwh_weighted_avg_gen_price,
            'n_purchasers_clipped': n_clipped,
            'clipped_mwh': clipped_mwh,
            'clipped_mwh_share': _safe_div(clipped_mwh, allocated_mwh),
        },
        'implied_gen_price_dispersion_USD_per_MWh': {
            'min': float(implied_gen.min()) if len(implied_gen) else float('nan'),
            'median': float(implied_gen.median()) if len(implied_gen) else float('nan'),
            'max': float(implied_gen.max()) if len(implied_gen) else float('nan'),
            'mwh_weighted_avg': mwh_weighted_avg_gen_price,
            'note': (
                'Equals p_uniform except where clip rule B lowers gen to the retail bill'
            ),
        },
        'by_end_use_class': {
            'counterfactual_MWh': cf_mwh_by_class,
            'counterfactual_gen_USD': cf_gen_usd_by_class,
            'counterfactual_trans_USD': cf_trans_usd_by_class,
            'counterfactual_dist_USD': cf_dist_usd_by_class,
            'counterfactual_td_USD': cf_td_usd_by_class,
            'counterfactual_all_in_USD': cf_all_in_by_class,
            'production_mixed_MWh': production['mwh_by_class'],
            'production_pre_mixed_gen_USD': production['usd_by_class'],
        },
        'after_unit_conversion': after_uc,
        'implied_prices_after_uc': implied_prices_uc,
        'gtd_column_heatmap': {
            'model_base_year': model_year,
            'columns': list(HEATMAP_INDUSTRY_COLS) + list(HEATMAP_FD_COLS),
            'rows': list(HEATMAP_ROW_CODES),
            'units': {
                GENERATION_SECTOR: 'MWh (plotted as TWh)',
                TRANS_SECTOR: 'USD (plotted as $B)',
                DIST_SECTOR: 'USD (plotted as $B)',
            },
            'production_mixed_raw': prod_gtd.to_dict(),
            'counterfactual_after_uc_raw': cf_gtd.to_dict(),
            'production_mixed_display_TWh_or_USD_B': _display_units_matrix(
                prod_gtd
            ).to_dict(),
            'counterfactual_after_uc_display_TWh_or_USD_B': _display_units_matrix(
                cf_gtd
            ).to_dict(),
            'figure_path': str(FIGURE_GTD_COLUMNS),
        },
        'hh_f01000': (
            None
            if hh is None
            else {
                'mwh': float(hh['mwh']),
                'gen_USD': float(hh['gen_USD']),
                'td_USD': float(hh['td_USD']),
                'implied_gen_USD_per_MWh': float(hh['implied_gen_USD_per_MWh']),
                'implied_all_in_USD_per_MWh': float(hh['implied_all_in_USD_per_MWh']),
                'clipped': bool(hh['gen_clipped_to_retail']),
                'vs_eia_residential_mwh': _safe_div(float(hh['mwh']), eia_res),
            }
        ),
        'production_compare': {
            'hh_fd_mwh': production['hh_fd_mwh'],
            'hh_vs_eia_residential': after_uc['hh_f01000']['prod_vs_eia_residential'],
            'eia_residential_model_year_MWh': after_uc['hh_f01000'][
                'eia_residential_mwh'
            ],
            'eia_residential_io_year_MWh': eia_res,
            'q_gen_mwh': production['q_gen_mwh'],
            'c_row_range_MWh_per_USD': [
                production['c_row_min'],
                production['c_row_max'],
            ],
        },
        'make_last_vs_ugo': {
            'make_last_row_total_USD': make_last,
            'make_last_shares': make_last_shares,
            'ugo_go_shares': ugo_shares,
            'delta_shares_make_minus_ugo': {
                k: make_last_shares[k] - ugo_shares[k] for k in make_last_shares
            },
        },
        'pipeline_steps': {
            'reallocation': {
                'year_basis': io_year,
                'does': (
                    'Report-only: post-reallocation aggregate 221100 intermediate '
                    'and final-demand dollars (2017 $). No redesign at this step.'
                ),
                'inputs_this_cf': [
                    f'post-reallocation 221100 $ ({io_year} A×q and Y) for within-class MWh weights only',
                ],
            },
            'three_way_split': {
                'year_basis': io_year,
                'does': (
                    'Split aggregate 221100 into 221110 / 221121 / 221122 on the '
                    'same 2017-chain monetary tables (before year scaling)'
                ),
                'inputs_this_cf': [
                    f'EIA Table 2.2 sales MWh ({io_year})',
                    f'EIA Table 2.4 retail ¢/kWh ({io_year})',
                    (
                        f'eGRID US net generation MWh ({egrid_year}; no {io_year} inventory) '
                        f'for p_uniform'
                        if egrid_year != io_year
                        else f'eGRID US net generation MWh ({io_year}) for p_uniform'
                    ),
                    f'production 221110 Use+Y $ ({io_year} chain) for p_uniform numerator',
                    f'UGO305 GO T/(T+D) shares ({io_year} column)',
                ],
            },
            'unit_conversion': {
                'year_basis': model_year,
                'applied_in_this_report': False,
                'production_today': (
                    f'After year-scaling to model year {model_year}, '
                    f'c_col = eGRID({model_year}) / q_221110_$ and '
                    f'c_row_j = λ / Table2.4_class(p_j) with prices from '
                    f'usa_ghg_data_year={ghg_year}; only the gen row/column '
                    'convert to MWh/$'
                ),
                'if_this_counterfactual_implemented': (
                    f'Year scaling still targets model year {model_year}. Then: '
                    f'(1) c_col = eGRID({model_year}) / q_221110_$; '
                    '(2) gen-row c_row is nearly flat (~1/p_uniform) because class '
                    'retail gaps already sit in 221121/221122 $; '
                    '(3) T&D rows stay monetary; '
                    f'(4) gen-row MWh by class match EIA Table 2.2 ({model_year}) '
                    'sales (re-applied sales anchor at model year); '
                    f'(5) q_221110 still equals eGRID({model_year}) — the '
                    'eGRID−sales gap is outside allocated purchaser MWh.'
                ),
            },
        },
        'purchaser_detail_top25_by_mwh': cf.sort_values('mwh', ascending=False)
        .head(25)
        .to_dict(orient='records'),
    }
    return payload


def render_report(p: dict[str, Any]) -> str:
    d = p['design']
    cf = p['counterfactual_totals']
    by = p['by_end_use_class']
    make = p['make_last_vs_ugo']
    hh = p['hh_f01000']
    disp = p['implied_gen_price_dispersion_USD_per_MWh']
    g110 = p['production_221110_use_y']
    years = p['years']
    steps = p['pipeline_steps']
    after = p['after_unit_conversion']
    implied_prices_uc = p['implied_prices_after_uc']
    io_y = years['io_account_year']
    model_y = years['model_base_year']
    ext = years['counterfactual_external_years']
    egrid_y = ext['egrid_for_p_uniform']
    ind_cents = float(p['table_2_4_cents_per_kWh']['Industrial'])
    hh_uc = after['hh_f01000']

    lines: list[str] = [
        '# Alternate EIA-anchored split — uniform generation, T&D markup',
        '',
        'Diagnostics-only counterfactual for a redesigned **PR3 + PR4** electricity '
        'path. **No production EEIO code is modified.**',
        '',
        '## Current production (brief)',
        '',
        '1. **Reallocation:** clean co-production off-diagonals on aggregate '
        '`221100` Make (with matching Use/VA transfers); Y unchanged.',
        '2. **Three-way split:** split `221100` → `221110`/`221121`/`221122` with '
        '**UGO GO** (Make intersection), **Table 8.3** diagonal (Use intersection), '
        'industry columns/VA ∝ UGO, and a compensating commodity-row / Y split so '
        'Use+Y tracks UGO.',
        '3. **Year scaling:** scale detail G/T/D to the model year (including '
        'detail GO growth).',
        '4. **Unit conversion:** convert only generation — `c_col = eGRID / q$` '
        'and class-varying `c_row ∝ 1 / Table 2.4` — so gen-row MWh sum to eGRID '
        'and retail price gaps sit in the gen row, not in T&D $.',
        '',
        '## Design (as specified)',
        '',
        f'1. **MWh anchor (3-way):** {d["mwh_anchor"]}.',
        f'2. **Generation row (3-way):** {d["gen_price"]}.',
        f'3. **T&D residual (3-way):** {d["td_rule"]}.',
        f'4. **Make last (report only):** {d["make_last"]}.',
        '',
        f'T/(T+D) national split: transmission **{_pct(d["w_trans_of_td"])}**, '
        f'distribution **{_pct(d["w_dist_of_td"])}** (UGO305, {io_y}).',
        '',
        f'### G/T/D by purchaser after unit conversion ({model_y})',
        '',
        'Two panels compare **commodity rows** Gen / Trans / Dist for three '
        'purchaser columns — truck transportation (`484000`, large gen-MWh '
        'over-assignment under production), petroleum refining (`324110`, large '
        'industrial electricity purchaser), and household FD (`F01000`) — under '
        'current production mixed units vs this counterfactual after unit '
        'conversion. Generation is shown in **TWh**; T&D stay monetary and are '
        'shown in **$B** (transmission labels use extra decimals when values '
        'are small). Color intensity is scaled separately for Gen vs T&D '
        '(shared across panels) so mixed units are not forced onto one axis.',
        '',
        f'![G/T/D Use+Y by purchaser after unit conversion ({model_y})]('
        f'{FIGURE_GTD_COLUMNS.name})',
        '',
        '**How to read it:** production packs class price variation into the gen '
        'row (`c_row`), so HH gen MWh is low relative to EIA Residential while '
        'Transportation-mapped industries can be heavily over-assigned gen MWh. '
        'This CF anchors gen MWh to EIA sales within class and moves retail markup '
        'into T&D $, so HH gen rises toward Residential sales and trucking’s gen '
        'MWh falls sharply.',
        '',
        f'## After unit conversion ({model_y}): gen MWh by class vs EIA',
        '',
        f'Target year for unit conversion is **model year {model_y}**. Under this '
        'counterfactual the EIA sales MWh anchor is re-applied at that year '
        '(within-class weights from model-year-scaled post-reallocation `221100` $). '
        'Class gen-row MWh therefore match EIA Table 2.2; current production does not.',
        '',
        f'| Class | EIA 2.2 {model_y} (TWh) | Alt CF after UC (TWh) | '
        f'Current production mixed (TWh) | Alt / EIA | Prod / EIA |',
        '|---|---:|---:|---:|---:|---:|',
    ]
    for eu in SALES_CLASSES:
        eia_m = float(after['eia_table_2_2_sales_MWh'][eu])
        alt_m = float(after['counterfactual_gen_MWh'][eu])
        prod_m = float(after['production_mixed_gen_MWh'][eu])
        lines.append(
            f'| {eu} | {_fmt_twh(eia_m)} | {_fmt_twh(alt_m)} | {_fmt_twh(prod_m)} | '
            f'{_pct(_safe_div(alt_m, eia_m))} | {_pct(_safe_div(prod_m, eia_m))} |'
        )
    eia_tot = float(after['eia_sales_total_MWh'])
    alt_tot = float(after['counterfactual_gen_total_MWh'])
    prod_tot = float(after['production_gen_total_MWh'])
    lines.append(
        f'| **Total** | **{_fmt_twh(eia_tot)}** | **{_fmt_twh(alt_tot)}** | '
        f'**{_fmt_twh(prod_tot)}** | **{_pct(_safe_div(alt_tot, eia_tot))}** | '
        f'**{_pct(_safe_div(prod_tot, eia_tot))}** |'
    )
    lines.extend(
        [
            '',
            f'eGRID {model_y} net generation: {_fmt_twh(after["egrid_net_generation_MWh"])} '
            f'TWh (q_221110 target via `c_col`). eGRID − EIA sales: '
            f'{_fmt_twh(after["egrid_minus_eia_sales_MWh"])} TWh.',
            '',
            f'### Household FD (`F01000`), {model_y}',
            '',
            f'| | Alt CF after UC | Current production | EIA Residential {model_y} |',
            '|---|---:|---:|---:|',
            f'| Gen-row MWh | {_fmt_twh(hh_uc["counterfactual_mwh"])} TWh | '
            f'{_fmt_twh(hh_uc["production_mwh"])} TWh | '
            f'{_fmt_twh(hh_uc["eia_residential_mwh"])} TWh |',
            f'| vs EIA Residential | {_pct(hh_uc["cf_vs_eia_residential"])} | '
            f'{_pct(hh_uc["prod_vs_eia_residential"])} | 100% |',
            '',
            f'### Implied prices after unit conversion ({model_y}), ¢/kWh',
            '',
            'For each EIA end-use class, take the CF monetary gen + T + D dollars '
            '(model-year retail residual construction) and divide by that class’s '
            'gen-row MWh (EIA Table 2.2 sales). Gen $ are still '
            f'`MWh × p_uniform` with '
            f'`p_uniform = ${implied_prices_uc["p_uniform_USD_per_MWh"]:.2f}/MWh` '
            f'({implied_prices_uc["p_uniform_cents_per_kWh"]:.2f} ¢/kWh) = model-year '
            '`221110` Use+Y $ / eGRID; T&D $ fill out the Table 2.4 class bill. '
            'All-in ¢/kWh therefore matches Table 2.4 by construction; gen alone is '
            'flat at `p_uniform`.',
            '',
            '| Class | Gen ¢/kWh | All-in (G+T+D) ¢/kWh | Table 2.4 ¢/kWh |',
            '|---|---:|---:|---:|',
        ]
    )
    for eu in SALES_CLASSES:
        ip = implied_prices_uc['by_end_use_class'][eu]
        lines.append(
            f'| {eu} | {ip["gen_cents_per_kWh"]:.2f} | '
            f'{ip["all_in_cents_per_kWh"]:.2f} | '
            f'{ip["table_2_4_cents_per_kWh"]:.2f} |'
        )
    lines.extend(
        [
            '',
            '---',
            '',
            f'## Step 1 — Reallocation ({io_y})',
            '',
            'This step is **report-only**. The counterfactual does not change '
            'reallocation. The table below is production’s post-reallocation '
            f'aggregate `221100` intermediate + final demand in {io_y} $. Those '
            'dollars are used later only as within-class weights when allocating '
            'EIA sales MWh (Steps 2 and after unit conversion).',
            '',
            '| Item | Value |',
            '|---|---:|',
            f'| `221100` intermediate ({io_y}) | '
            f'${_fmt_b(p["baseline_221100_for_mwh_weights"]["intermediate_USD"])} B |',
            f'| `221100` final demand ({io_y}) | '
            f'${_fmt_b(p["baseline_221100_for_mwh_weights"]["final_demand_USD"])} B |',
            f'| `221100` Use+Y total ({io_y}) | '
            f'${_fmt_b(p["baseline_221100_for_mwh_weights"]["total_USD"])} B |',
            '',
            '---',
            '',
            f'## Step 2 — Three-way split ({io_y}; eGRID {egrid_y} as {io_y} proxy)',
            '',
            steps['three_way_split']['does'] + '.',
            '',
            'Inputs for this counterfactual redesign:',
            '',
        ]
    )
    for item in steps['three_way_split']['inputs_this_cf']:
        lines.append(f'- {item}')

    lines.extend(
        [
            '',
            f'### Constructed quantities ({io_y} chain)',
            '',
            '| Item | Value |',
            '|---|---:|',
            f'| Production `221110` Use+Y ({io_y}) | '
            f'${_fmt_b(g110["use_y_total_USD"])} B |',
            f'| eGRID net generation ({egrid_y}) | '
            f'{_fmt_twh(p["egrid_net_generation_MWh"])} TWh |',
            f'| Uniform gen price `p = 221110 $/eGRID` | '
            f'${_fmt_price(p["p_uniform_USD_per_MWh"])}/MWh '
            f'({p["p_uniform_cents_per_kWh"]:.2f} ¢/kWh) |',
            f'| Table 2.4 Industrial ({io_y}) | {ind_cents:.2f} ¢/kWh |',
            f'| Allocated EIA sales MWh ({io_y}) | '
            f'{_fmt_twh(p["allocated_mwh_total"])} TWh |',
            f'| eGRID − EIA sales MWh | '
            f'{_fmt_twh(p["egrid_minus_allocated_sales_MWh"])} TWh |',
            f'| Counterfactual gen $ on sales MWh | ${_fmt_b(cf["gen_USD"])} B |',
            f'| Gen $ − production `221110` Use+Y | '
            f'${_fmt_b(cf["gen_vs_221110_use_y_USD"])} B |',
            f'| Counterfactual T&D $ | ${_fmt_b(cf["td_USD"])} B '
            f'(T ${_fmt_b(cf["trans_USD"])} B / D ${_fmt_b(cf["dist_USD"])} B) |',
            f'| All-in (gen+T&D) $ | ${_fmt_b(cf["all_in_USD"])} B |',
            f'| Purchasers with gen clipped to retail | {cf["n_purchasers_clipped"]} '
            f'({_pct(cf["clipped_mwh_share"])} of MWh) |',
            f'| Implied gen $/MWh min / median / max | '
            f'{_fmt_price(disp["min"])} / {_fmt_price(disp["median"])} / '
            f'{_fmt_price(disp["max"])} |',
            '',
            f'Uniform gen price (~{p["p_uniform_cents_per_kWh"]:.1f} ¢/kWh) is '
            f'**below** all Table 2.4 ({io_y}) class rates (Industrial '
            f'{ind_cents:.2f} ¢/kWh), so clip rule B does not bind and **all classes '
            'keep positive T&D** markup. Gen $ on EIA sales is below production '
            '`221110` Use+Y by the eGRID−sales gap × p.',
            '',
            f'### Gen / T / D $ vs EIA Table 2.2 and Table 2.4 ({io_y})',
            '',
            '| Class | Alt gen $ (B) | Alt T $ (B) | Alt D $ (B) | '
            f'EIA 2.2 {io_y} (TWh) | Implied gen ¢/kWh | Table 2.4 {io_y} ¢/kWh |',
            '|---|---:|---:|---:|---:|---:|---:|',
        ]
    )
    prices_24 = p['table_2_4_cents_per_kWh']
    for eu in SALES_CLASSES:
        gen_usd = float(by['counterfactual_gen_USD'][eu])
        trans_usd = float(by['counterfactual_trans_USD'][eu])
        dist_usd = float(by['counterfactual_dist_USD'][eu])
        mwh_eu = float(by['counterfactual_MWh'][eu])
        implied = _usd_per_mwh_to_cents_kwh(_safe_div(gen_usd, mwh_eu))
        lines.append(
            f'| {eu} | {_fmt_b(gen_usd)} | {_fmt_b(trans_usd)} | {_fmt_b(dist_usd)} | '
            f'{_fmt_twh_precise(mwh_eu)} | {implied:.2f} | {float(prices_24[eu]):.2f} |'
        )
    tot_gen = float(cf['gen_USD'])
    tot_trans = float(cf['trans_USD'])
    tot_dist = float(cf['dist_USD'])
    tot_mwh = float(p['allocated_mwh_total'])
    lines.append(
        f'| **Total** | **{_fmt_b(tot_gen)}** | **{_fmt_b(tot_trans)}** | '
        f'**{_fmt_b(tot_dist)}** | **{_fmt_twh_precise(tot_mwh)}** | '
        f'**{_usd_per_mwh_to_cents_kwh(_safe_div(tot_gen, tot_mwh)):.2f}** | |'
    )
    lines.extend(
        [
            '',
            f'All-in (gen+T&D) $/MWh recovers Table 2.4 ({io_y}) by construction.',
            '',
            f'### Class gen MWh vs EIA ({io_y} only)',
            '',
            f'| Class | EIA 2.2 {io_y} (TWh) | Alt gen MWh (TWh) | Alt gen $ (B) | '
            'Alt T&D $ (B) |',
            '|---|---:|---:|---:|---:|',
        ]
    )
    for eu in SALES_CLASSES:
        lines.append(
            f'| {eu} | {_fmt_twh(p["eia_table_2_2_sales_MWh"][eu])} | '
            f'{_fmt_twh(by["counterfactual_MWh"][eu])} | '
            f'{_fmt_b(by["counterfactual_gen_USD"][eu])} | '
            f'{_fmt_b(by["counterfactual_td_USD"][eu])} |'
        )
    lines.extend(
        [
            '',
            f'By construction, alternate gen MWh class totals match EIA Table 2.2 '
            f'({io_y}).',
            '',
        ]
    )
    if hh is not None:
        lines.extend(
            [
                f'### Household FD (`F01000`), {io_y} three-way only',
                '',
                '| | Alternate 3-way CF |',
                '|---|---:|',
                f'| Gen-row MWh | {_fmt_twh(hh["mwh"])} TWh |',
                f'| vs EIA Residential {io_y} | {_pct(hh["vs_eia_residential_mwh"])} |',
                f'| Gen $ | ${_fmt_b(hh["gen_USD"])} B |',
                f'| T&D $ | ${_fmt_b(hh["td_USD"])} B |',
                f'| Implied gen ¢/kWh | '
                f'{hh["implied_gen_USD_per_MWh"] / CENTS_PER_KWH_TO_USD_PER_MWH:.2f} |',
                f'| Implied all-in ¢/kWh | '
                f'{hh["implied_all_in_USD_per_MWh"] / CENTS_PER_KWH_TO_USD_PER_MWH:.2f} |',
                f'| Gen clipped? | {hh["clipped"]} |',
                '',
            ]
        )
    lines.extend(
        [
            f'### Make-last weights vs UGO305 GO ({io_y})',
            '',
            '| Commodity | Alt Use+Y share (Make-last) | UGO GO share | Δ (alt − UGO) |',
            '|---|---:|---:|---:|',
        ]
    )
    for code in ELECTRICITY_DISAGG_SECTORS:
        lines.append(
            f'| {code} | {_pct(make["make_last_shares"][code])} | '
            f'{_pct(make["ugo_go_shares"][code])} | '
            f'{make["delta_shares_make_minus_ugo"][code]:+.1%} |'
        )
    lines.extend(
        [
            '',
            'With gen priced off `221110`/eGRID (below retail), T&D absorbs most of '
            f'the Table 2.4 ({io_y}) markup. Make-last shares are closer to UGO’s '
            'distribution weight than a gen-heavy uniform-price-on-`221100` design, '
            'but still differ from UGO.',
            '',
            f'- Gen $ on EIA sales vs production `221110` Use+Y: '
            f'${_fmt_b(cf["gen_vs_221110_use_y_USD"])} B.',
            f'- All-in vs `221100` Use+Y (weight source): '
            f'${_fmt_b(cf["all_in_minus_221100_USD"])} B.',
            f'- Clip rule B: {cf["n_purchasers_clipped"]} purchasers / '
            f'{_pct(cf["clipped_mwh_share"])} of MWh.',
            '',
            '---',
            '',
            f'## Step 3 — Unit conversion ({model_y})',
            '',
            'Unit conversion is **not applied** in the Step 2 tables above. This '
            'section describes production today and what implementing this '
            'counterfactual would change. Headline MWh results are in '
            f'[After unit conversion ({model_y})](#after-unit-conversion-'
            f'{model_y}-gen-mwh-by-class-vs-eia) above.',
            '',
            '### Production today',
            '',
            steps['unit_conversion']['production_today'] + '.',
            '',
            '| Item | Value |',
            '|---|---:|',
            f'| `c_row` range (MWh/$) | '
            f'{p["production_compare"]["c_row_range_MWh_per_USD"][0]:.4g} – '
            f'{p["production_compare"]["c_row_range_MWh_per_USD"][1]:.4g} |',
            f'| `q_221110` after conversion | '
            f'{_fmt_twh(p["production_compare"]["q_gen_mwh"])} TWh |',
            f'| HH `F01000` gen MWh / EIA Residential | '
            f'{_pct(p["production_compare"]["hh_vs_eia_residential"])} |',
            '',
            '### If this counterfactual were implemented',
            '',
            steps['unit_conversion']['if_this_counterfactual_implemented'],
            '',
            after['c_row_note'],
            '',
            '### Pre-conversion gen $ by class (production, model-year scaled)',
            '',
            '| Class | Production pre-mix gen $ (B) | Production mixed gen (TWh) |',
            '|---|---:|---:|',
        ]
    )
    for eu in SALES_CLASSES:
        lines.append(
            f'| {eu} | {_fmt_b(by["production_pre_mixed_gen_USD"][eu])} | '
            f'{_fmt_twh(by["production_mixed_MWh"][eu])} |'
        )
    lines.extend(
        [
            '',
            '## Expected effects on existing diagnostics',
            '',
            '- **`hh_vs_interindustry`:** after UC, HH gen MWh tracks EIA Residential '
            f'({model_y}); today’s ~0.53× shortfall from class-varying `c_row` goes away.',
            '- **Class-price driver:** Table 2.4 leaves gen `c_row`; markups sit in T&D $.',
            '- **`N` undilution:** `D_221110` undilution can remain if E stays on gen; '
            'who inherits it follows EIA MWh shares.',
            '- **BLy / full_trace:** block BLy still ≈ `D_110·q_110` if E on gen; '
            '`c_col` stays eGRID/`q_110`, `c_row` near-flat.',
            '',
            '## Open questions for production implementation',
            '',
            'This counterfactual redesigns only the **purchaser-side Use+Y G/T/D '
            'split** (EIA MWh + flat gen price + T&D markup) and a '
            '**post–unit-conversion MWh comparison**. It does **not** rebuild Make, '
            'Use intersection, industry columns/VA, compensating `w_row`, year '
            'scaling, or E/B. Shipping a production version requires resolving at '
            'least the following.',
            '',
            '### 1. Use intersection (3×3), including gen self-use',
            '',
            'How should the Use-table G/T/D × G/T/D intersection be split when this '
            'design puts a large share of generation commodity dollars (and '
            'Industrial-class MWh) onto the generation industry column—especially '
            'the gen→gen cell—without treating that as “delivered sales” the way '
            'EIA Industrial MWh is treated?',
            '',
            'Production today is **diagonal-only** Table 8.3 '
            '(`disaggregate_use_intersection`). This CF never builds that block; '
            'the gen industry is just another Industrial purchaser. Unresolved '
            'options: keep diagonal 8.3; allow off-diagonal (d_85-style hybrid); '
            'carve self-use / station use out of EIA Industrial; or fund the '
            'intersection from the eGRID−sales residual instead of sales.',
            '',
            '### 2. Make: Make-last vs UGO',
            '',
            'Make-last shares are **reported only**. Production still splits Make '
            'with UGO GO shares. Apply Make-last to V, keep UGO Make (accept '
            'Make≠Use mix), or something else—and how that interacts with Step 3 '
            'GO targets `x_s = w_go · x_agg`.',
            '',
            '### 3. Industry columns + VA',
            '',
            'Fuels→gen, other inputs ∝ `w_go`, and VA = residual are untouched. If '
            'commodity-row / Make totals change, do column structures and VA stay '
            'UGO-based? Risk of thin T&D columns / negative VA.',
            '',
            '### 4. Compensating `w_row` and market clearing',
            '',
            'Production uses compensating `w_row` so Use+Y tracks UGO while '
            'intersection uses Table 8.3. This CF replaces that with EIA+retail '
            'purchaser shares; all-in $ ≠ `221100` Use+Y, and gen $ on sales ≠ '
            'production `221110` Use+Y. Drop `w_row`? Use column-specific weights? '
            'Which identity is sacred—BEA $, EIA retail×sales, UGO GO, or eGRID MWh?',
            '',
            '### 5. Unit conversion with flat gen price',
            '',
            'CF intent: flat `c_row ≈ 1/p_uniform`, retail markup in T&D $, gen-row '
            'class MWh = **EIA sales**. Production: `c_row = λ / Table 2.4` so '
            'gen-row Σ MWh = **eGRID**. How to implement flat `c_row` without the '
            'eGRID row identity; how the gen self-use column uses `c_row`/`c_col`; '
            'domestic vs import rows.',
            '',
            '### 6. eGRID − sales residual',
            '',
            'Hundreds of TWh sit outside allocated purchasers; gen $ shortfall ≈ '
            'gap × `p_uniform`. Attribute to losses, plant use, Direct Use, '
            'exports, unallocated FD, or a residual sector—vs changing `c_col` '
            'away from full eGRID.',
            '',
            '### 7. Year scaling (2017 → 2024)',
            '',
            'CF money is built on the 2017 chain; unit-conversion MWh re-anchors '
            'with 2024 EIA + scaled `221100` $. Detail GO growth (D7) is not '
            're-run under the new split. Re-run the full CF after D7? Align EIA '
            '2.2/2.4, eGRID (2018 as 2017 proxy), and UGO years consistently?',
            '',
            '### 8. E / B still on generation',
            '',
            'Non-SF₆ E stays on `221110`; unit conversion only rescales gen B by '
            '`c_col`. Likely keep that, but redefine who inherits undiluted '
            '`D_221110` / BLy when gen MWh follows EIA shares and T&D carry retail '
            'markup.',
            '',
            '### 9. Other',
            '',
            'Imports/margins vs retail purchaser prices; clip rule B in other years '
            'or price regimes; end-use map still drives within-class weights; '
            'Leontief / Y backcompute if gen-row Σ = sales not eGRID.',
            '',
            '**Bottom line:** a production implementation needs a coherent package '
            'for Make + Use intersection (especially gen self-use) + industry/VA + '
            'clearing/`w_row` + eGRID−sales under `c_col` + 2017→2024 child '
            'scaling—not just swapping the commodity-row split and flattening '
            '`c_row`.',
            '',
            '## Reproduce',
            '',
            '```',
            'python -m bedrock.analysis.electricity_disagg_diagnostics.'
            'alternate_eia_anchored_split',
            '```',
            '',
            f'Writes `{REPORT_MD.relative_to(OUT_DIR.parent)}` and the JSON companion.',
            '',
        ]
    )
    return '\n'.join(lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    payload = analyze()
    OUT_SUBDIR.mkdir(parents=True, exist_ok=True)
    hm = payload['gtd_column_heatmap']
    prod = pd.DataFrame(hm['production_mixed_raw'])
    cf = pd.DataFrame(hm['counterfactual_after_uc_raw'])
    # DataFrames from to_dict() need reindex to restore row/column order.
    prod = prod.reindex(index=hm['rows'], columns=hm['columns']).astype(float)
    cf = cf.reindex(index=hm['rows'], columns=hm['columns']).astype(float)
    fig = plot_gtd_columns_after_uc_figure(
        prod, cf, model_year=int(hm['model_base_year'])
    )
    import matplotlib.pyplot as plt  # noqa: PLC0415

    fig.savefig(FIGURE_GTD_COLUMNS, dpi=160, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    REPORT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding='utf-8')
    REPORT_MD.write_text(render_report(payload), encoding='utf-8')
    print(f'Wrote {REPORT_MD}')
    print(f'Wrote {REPORT_JSON}')
    print(f'Wrote {FIGURE_GTD_COLUMNS}')


if __name__ == '__main__':
    main()
