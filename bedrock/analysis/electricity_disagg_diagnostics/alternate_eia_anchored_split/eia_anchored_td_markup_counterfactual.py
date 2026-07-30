"""Alternate EIA-anchored 3-way split + uniform-gen / T&D-markup design.

Constructs a **diagnostics-only** counterfactual (does not change production EEIO)
for a redesigned PR3+PR4 electricity split:

1. Allocate EIA Table 2.2 class MWh to IO purchasers in proportion to
   post-reallocation / pre–3-way aggregate ``221100`` monetary purchases.
2. Set the **221110** Use/Y row at a **uniform** $/MWh:
   ``p_uniform = (production 221110 Use+Y $) / eGRID net generation MWh``.
   Purchaser gen $ = allocated EIA MWh × p_uniform (so Σ gen $ on sales MWh
   is below 221110 $ by the eGRID−sales gap).
3. Set each purchaser's T&D residual so
   ``(gen + T&D)_j / MWh_j`` recovers Table 2.4 class retail $/MWh; if that
   residual would be negative, **lower gen $** to the retail bill (clip T&D at 0).
4. Split T&D dollars between ``221121`` / ``221122`` with fixed national shares
   from UGO305 T/(T+D) and D/(T+D).
5. Report implied **Make-last** commodity weights from the resulting Use+Y
   row totals (vs today's UGO GO weights).

Compares the counterfactual generation-row MWh / $ structure and diagnostic
implications to the **current** mixed-units production path.

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
    us_total_net_generation_mwh,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    _model_year_y_row_221110,
    derive_disagg_Ytot_with_trade,
    electricity_conversion_factors,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    build_electricity_disagg_go_weights,
)
from bedrock.transform.eeio.electricity_end_use_mapping import (
    build_end_use_map,
    table_2_4_prices_cents_kwh,
)
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.schemas.cornerstone_schemas import (
    ELECTRICITY_AGGREGATE_SECTOR,
    ELECTRICITY_DISAGG_SECTORS,
)

logger = logging.getLogger(__name__)

REALLOC_CONFIG = '2025_usa_cornerstone_v0_2_electricity_reallocation'
SPLIT_CONFIG = '2025_usa_cornerstone_v0_2_electricity_disaggregation'
MIXED_CONFIG = '2025_usa_cornerstone_v0_2_electricity_mixed_units'
TRANS_SECTOR = '221121'
DIST_SECTOR = '221122'

OUT_SUBDIR = OUT_DIR / 'alternate_eia_anchored_split'
REPORT_MD = OUT_SUBDIR / 'eia_anchored_td_markup_counterfactual.md'
REPORT_JSON = OUT_SUBDIR / 'eia_anchored_td_markup_counterfactual.json'

CENTS_PER_KWH_TO_USD_PER_MWH = 10.0
SALES_CLASSES: tuple[str, ...] = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
)


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


def _agg_electricity_usd_flows() -> tuple[pd.Series, pd.Series, int, int]:
    """Post-reallocation / pre–3-way ``221100`` intermediate + FD USD.

    Used only for **within-class MWh allocation weights**, not for p_uniform.
    """
    _install(REALLOC_CONFIG)
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    cfg = get_usa_config()
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
    intermediate = (a_tot.loc[agg].astype(float) * q).astype(float)
    intermediate = intermediate.clip(lower=0.0)

    Y = derive_disagg_Ytot_with_trade().copy()
    Y.index = Y.index.astype(str)
    Y.columns = Y.columns.astype(str)
    y_row = cast(pd.Series, Y.loc[agg].astype(float).clip(lower=0.0))
    return (
        cast(pd.Series, intermediate),
        y_row,
        int(cfg.model_base_year),
        int(cfg.usa_ghg_data_year),
    )


def _production_221110_use_y_usd() -> tuple[float, float, float]:
    """Post–3-way monetary ``221110`` intermediate + FD USD and q.

    Uses the disaggregation (not mixed-units) config so dollars are still USD.
    """
    _install(SPLIT_CONFIG)
    aq = derive_cornerstone_Aq_scaled()
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
    y_usd = float(_model_year_y_row_221110(aq).astype(float).clip(lower=0.0).sum())
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


def analyze() -> dict[str, Any]:
    intermediate, y_row, model_year, ghg_year = _agg_electricity_usd_flows()
    gen_inter_usd, gen_fd_usd, q_gen_usd = _production_221110_use_y_usd()
    gen_use_y_usd = gen_inter_usd + gen_fd_usd
    egrid_mwh = float(us_total_net_generation_mwh(model_year))

    end_use_map = build_end_use_map()
    prices = cast(dict[str, float], table_2_4_prices_cents_kwh(ghg_year))
    eia = _eia_table_2_2_sales_mwh(model_year)
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

    # Implied gen price dispersion after clip rule B
    positive = cf.loc[cf['mwh'] > 0]
    implied_gen = positive['implied_gen_USD_per_MWh']
    mwh_weighted_avg_gen_price = _safe_div(gen_total, allocated_mwh)

    payload: dict[str, Any] = {
        'design': {
            'scope': 'PR3+PR4 diagnostics counterfactual (no production code changes)',
            'mwh_anchor': (
                'EIA Table 2.2 sales by class; within-class ∝ post-reallocation '
                '221100 USD'
            ),
            'gen_price': (
                'p_uniform = (post–3-way 221110 Use+Y $) / eGRID net generation MWh; '
                'purchaser gen $ = allocated EIA sales MWh_j × p_uniform; '
                'clip gen down to Table 2.4 retail bill when T&D residual would be negative'
            ),
            'td_rule': (
                'T&D_j = max(0, MWh_j×p_retail_class − gen_j); '
                'split T/D with UGO305 T/(T+D), D/(T+D)'
            ),
            'make_last': 'Use+Y row totals as Make commodity weights (reported, not applied)',
            'w_trans_of_td': w_trans,
            'w_dist_of_td': w_dist,
            'p_uniform_denominator': 'eGRID US net generation MWh',
            'p_uniform_numerator': 'production 221110 Use+Y USD (3-way monetary)',
        },
        'years': {
            'model_base_year': model_year,
            'usa_ghg_data_year': ghg_year,
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
            'hh_vs_eia_residential': _safe_div(production['hh_fd_mwh'], eia_res),
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
    prod = p['production_compare']
    hh = p['hh_f01000']
    disp = p['implied_gen_price_dispersion_USD_per_MWh']
    g110 = p['production_221110_use_y']
    ind_cents = float(p['table_2_4_cents_per_kWh']['Industrial'])

    lines: list[str] = [
        '# Alternate EIA-anchored split — uniform generation, T&D markup',
        '',
        'Diagnostics-only counterfactual for a redesigned **PR3 + PR4** electricity '
        'path. **No production EEIO code is modified.**',
        '',
        '## Design (as specified)',
        '',
        f'1. **MWh anchor:** {d["mwh_anchor"]}.',
        f'2. **Generation row:** {d["gen_price"]}.',
        f'3. **T&D residual:** {d["td_rule"]}.',
        f'4. **Make last:** {d["make_last"]}.',
        '',
        f'T/(T+D) national split: transmission **{_pct(d["w_trans_of_td"])}**, '
        f'distribution **{_pct(d["w_dist_of_td"])}** (from UGO305).',
        '',
        '## Key constructed quantities',
        '',
        '| Item | Value |',
        '|---|---:|',
        f'| Production `221110` Use+Y (numerator) | '
        f'${_fmt_b(g110["use_y_total_USD"])} B |',
        f'| eGRID net generation (denominator) | '
        f'{_fmt_twh(p["egrid_net_generation_MWh"])} TWh |',
        f'| Uniform gen price `p = 221110 $/eGRID` | '
        f'${_fmt_price(p["p_uniform_USD_per_MWh"])}/MWh '
        f'({p["p_uniform_cents_per_kWh"]:.2f} ¢/kWh) |',
        f'| Table 2.4 Industrial (reference) | {ind_cents:.2f} ¢/kWh |',
        f'| Allocated EIA sales MWh | {_fmt_twh(p["allocated_mwh_total"])} TWh |',
        f'| eGRID − EIA sales MWh | '
        f'{_fmt_twh(p["egrid_minus_allocated_sales_MWh"])} TWh |',
        f'| Counterfactual gen $ on sales MWh | ${_fmt_b(cf["gen_USD"])} B |',
        f'| Gen $ − production `221110` Use+Y | '
        f'${_fmt_b(cf["gen_vs_221110_use_y_USD"])} B |',
        f'| Counterfactual T&D $ | ${_fmt_b(cf["td_USD"])} B '
        f'(T ${_fmt_b(cf["trans_USD"])} B / D ${_fmt_b(cf["dist_USD"])} B) |',
        f'| All-in (gen+T&D) $ | ${_fmt_b(cf["all_in_USD"])} B |',
        f'| `221100` Use+Y (MWh-weight source only) | '
        f'${_fmt_b(p["baseline_221100_for_mwh_weights"]["total_USD"])} B |',
        f'| Purchasers with gen clipped to retail | {cf["n_purchasers_clipped"]} '
        f'({_pct(cf["clipped_mwh_share"])} of MWh) |',
        f'| Implied gen $/MWh min / median / max | '
        f'{_fmt_price(disp["min"])} / {_fmt_price(disp["median"])} / '
        f'{_fmt_price(disp["max"])} |',
        f'| MWh-weighted avg implied gen $/MWh | '
        f'${_fmt_price(disp["mwh_weighted_avg"])}/MWh |',
        '',
        f'Uniform gen price (~{p["p_uniform_cents_per_kWh"]:.1f} ¢/kWh) is '
        f'**below** all Table 2.4 class rates (Industrial {ind_cents:.2f} ¢/kWh), '
        'so clip rule B should rarely bind and **all classes keep positive T&D** '
        'markup above generation. Gen $ recovered on EIA sales MWh is below '
        'production `221110` Use+Y by the eGRID−sales gap × p.',
        '',
        '## Alternate gen $ vs EIA Table 2.2 (implied prices)',
        '',
        'Same layout as the production deck table (3-way `221110` $ / EIA TWh → '
        'implied ¢/kWh vs Table 2.4), but with **this counterfactual**: uniform '
        f'`p = 221110 Use+Y / eGRID` ({p["p_uniform_cents_per_kWh"]:.2f} ¢/kWh) '
        'applied to EIA sales MWh. Implied gen prices are therefore flat across '
        'classes; the gap to Table 2.4 is absorbed as T&D markup.',
        '',
        '| Class | Alt gen $ (B) (`221110`) | Alt T $ (B) (`221121`) | '
        'Alt D $ (B) (`221122`) | EIA TWh values | Implied gen prices, ¢/kWh | '
        'EIA Table 2.4, ¢/kWh |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
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
            'All-in (gen+T&D) $/MWh recovers Table 2.4 by construction for every '
            'class (clip rule B unused here).',
            '',
            '## Class totals vs EIA and vs current production',
            '',
            '| Class | EIA 2.2 (TWh) | Alt gen MWh (TWh) | Production mixed gen (TWh) | '
            'Alt gen $ (B) | Alt T&D $ (B) | Prod. pre-mix gen $ (B) |',
            '|---|---:|---:|---:|---:|---:|---:|',
        ]
    )
    for eu in SALES_CLASSES:
        lines.append(
            f'| {eu} | {_fmt_twh(p["eia_table_2_2_sales_MWh"][eu])} | '
            f'{_fmt_twh(by["counterfactual_MWh"][eu])} | '
            f'{_fmt_twh(by["production_mixed_MWh"][eu])} | '
            f'{_fmt_b(by["counterfactual_gen_USD"][eu])} | '
            f'{_fmt_b(by["counterfactual_td_USD"][eu])} | '
            f'{_fmt_b(by["production_pre_mixed_gen_USD"][eu])} |'
        )

    lines.extend(
        [
            '',
            'By construction, **alternate gen MWh class totals match EIA Table 2.2 '
            'sales** (within-class IO allocation only reshuffles inside the class). '
            'Current production **does not**.',
            '',
        ]
    )

    if hh is not None:
        lines.extend(
            [
                '### Household FD (`F01000`)',
                '',
                '| | Alternate | Current production (mixed) |',
                '|---|---:|---:|',
                f'| Gen-row MWh | {_fmt_twh(hh["mwh"])} TWh | '
                f'{_fmt_twh(prod["hh_fd_mwh"])} TWh |',
                f'| vs EIA Residential | {_pct(hh["vs_eia_residential_mwh"])} | '
                f'{_pct(prod["hh_vs_eia_residential"])} |',
                f'| Gen $ | ${_fmt_b(hh["gen_USD"])} B | (in Residential class USD) |',
                f'| T&D $ | ${_fmt_b(hh["td_USD"])} B | n/a (price in gen `c_row`) |',
                f'| Implied gen ¢/kWh | '
                f'{hh["implied_gen_USD_per_MWh"] / CENTS_PER_KWH_TO_USD_PER_MWH:.2f} | '
                f'class-varying |',
                f'| Implied all-in ¢/kWh | '
                f'{hh["implied_all_in_USD_per_MWh"] / CENTS_PER_KWH_TO_USD_PER_MWH:.2f} | '
                f'≈ Table 2.4 Residential |',
                f'| Gen clipped? | {hh["clipped"]} | — |',
                '',
            ]
        )

    lines.extend(
        [
            '## Make-last weights vs current UGO305 GO weights',
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
            'With gen priced off **`221110`/eGRID** (below retail), T&D absorbs '
            'most of the Table 2.4 markup for every class. Make-last Use+Y shares '
            'are therefore **much more T&D-heavy** than the prior mistaken '
            '`221100`/sales uniform-price run, and closer in spirit to UGO’s '
            'distribution weight — though the exact split still differs from UGO.',
            '',
            '## Expected effects on existing diagnostics',
            '',
            '### 1. Household vs interindustry MWh (`hh_vs_interindustry`)',
            '',
            '- **Generation-row MWh** by end-use class tracks EIA Table 2.2 by '
            'construction, fixing the current ~0.53× Residential shortfall on '
            '`F01000` for the gen commodity.',
            '- Intermediate gen MWh class totals match EIA sales (self-use still '
            'inside Industrial if so mapped).',
            '- **T&D** carries class retail markups above the low uniform gen price.',
            '',
            '### 2. Class-price driver (decomposition §B)',
            '',
            '- Production today puts Table 2.4 into **`c_row` on 221110**.',
            '- Alternate: gen $/MWh is ~uniform at `221110`/eGRID; class price '
            'gaps move to **221121/221122**. With `D_T&D ≈ 0`, that mainly '
            'rewrites monetary `A`/`L`, not T&D direct EF.',
            '',
            '### 3. Consumer `N` undilution (`n_variance_explained`)',
            '',
            '- **Undilution of `D_221110`** can remain if eGRID E stays on '
            'generation — the +271 MMT / median `N` rise is not automatically gone.',
            '- **Who** inherits it shifts with EIA MWh shares (more Residential-'
            'mapped, less Industrial overweight vs production).',
            '- Industrial `%ΔN` boost from cheap gen `c_row` should shrink.',
            '',
            '### 4. National BLy / full_trace',
            '',
            '- Block BLy still ≈ `D_110·q_110` if E stays on generation.',
            '- Make-last from gen+T&D Use+Y puts substantial weight on T&D; need '
            'consistent VA/x/E rules with Make-last ordering.',
            '- Mixed-units `c_col` can stay eGRID/`q_110`; `c_row` becomes nearly '
            'flat (uniform gen price).',
            '',
            '### 5. Feasibility flags',
            '',
            f'- **Gen $ on EIA sales vs production `221110` Use+Y:** '
            f'${_fmt_b(cf["gen_vs_221110_use_y_USD"])} B (eGRID−sales × p).',
            f'- **All-in vs `221100` Use+Y (weight source):** '
            f'${_fmt_b(cf["all_in_minus_221100_USD"])} B.',
            f'- **Clip rule B:** {cf["n_purchasers_clipped"]} purchasers / '
            f'{_pct(cf["clipped_mwh_share"])} of MWh.',
            '- Direct Use / losses sit in the eGRID−sales gap, not in allocated '
            'purchaser MWh.',
            '- Industry-column / fuel / VA steps are not rebuilt here.',
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
    REPORT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding='utf-8')
    REPORT_MD.write_text(render_report(payload), encoding='utf-8')
    print(f'Wrote {REPORT_MD}')
    print(f'Wrote {REPORT_JSON}')


if __name__ == '__main__':
    main()
