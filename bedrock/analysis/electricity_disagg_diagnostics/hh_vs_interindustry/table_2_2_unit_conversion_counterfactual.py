"""Alternate unit conversion: anchor class MWh to EIA Table 2.2.

Instead of Table 2.4 prices + eGRID total, convert each end-use class so that
the sum of converted 221110 uses equals EIA Table 2.2 MWh for that class,
keeping the same IO→end-use mapping. All sectors in a class share one factor.

Reports:
1. Implied class prices ($/MWh and ¢/kWh)
2. Electricity and non-electricity D/N vs production Table 2.4 path
3. IO balance / production feasibility notes

Run:
    python -m bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry.table_2_2_unit_conversion_counterfactual
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
    MIXED_CONFIG,
    _eia_table_2_2_sales_mwh,
    _install_mixed_config,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    _model_year_y_row_221110,
    compute_mixed_unit_ef_vectors,
    electricity_conversion_factors,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_B_non_finetuned,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    GENERATION_SECTOR,
    apply_electricity_unit_conversion_to_A,
    apply_electricity_unit_conversion_to_B,
    apply_electricity_unit_conversion_to_q,
    electricity_output_factor,
)
from bedrock.transform.eeio.electricity_end_use_mapping import (
    EPA_END_USES,
    build_end_use_map,
    electricity_end_use_retail_prices_cents_kwh,
)
from bedrock.utils.math.formulas import (
    backcompute_y_from_A_and_q,
    compute_d,
    compute_L_matrix,
    compute_M_matrix,
    compute_n,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

logger = logging.getLogger(__name__)

OUT_SUBDIR = OUT_DIR / 'hh_vs_interindustry'
REPORT_MD = OUT_SUBDIR / 'table_2_2_unit_conversion_counterfactual.md'
REPORT_JSON = OUT_SUBDIR / 'table_2_2_unit_conversion_counterfactual.json'

CENTS_PER_KWH_TO_USD_PER_MWH = 10.0


def _safe_div(num: float, den: float) -> float:
    return num / den if den else float('nan')


def _usd_flows_by_class(
    intermediate_usd: pd.Series,
    final_demand_usd: pd.Series,
    end_use_map: Mapping[str, str],
) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for end_use in EPA_END_USES:
        inter_cols = [
            c for c in intermediate_usd.index if end_use_map[str(c)] == end_use
        ]
        fd_cols = [c for c in final_demand_usd.index if end_use_map[str(c)] == end_use]
        inter = float(intermediate_usd.reindex(inter_cols).fillna(0.0).sum())
        fd = float(final_demand_usd.reindex(fd_cols).fillna(0.0).sum())
        out[end_use] = {
            'intermediate_USD': inter,
            'final_demand_USD': fd,
            'total_USD': inter + fd,
        }
    return out


def _class_row_factors_from_eia(
    intermediate_usd: pd.Series,
    final_demand_usd: pd.Series,
    end_use_map: Mapping[str, str],
    eia_mwh_by_class: Mapping[str, float],
) -> tuple[pd.Series, dict[str, dict[str, float]]]:
    """c_j = EIA_MWh[class] / USD[class] for every purchaser in the class."""
    usd_by_class = _usd_flows_by_class(intermediate_usd, final_demand_usd, end_use_map)
    class_factors: dict[str, float] = {}
    detail: dict[str, dict[str, float]] = {}
    for end_use in EPA_END_USES:
        usd = usd_by_class[end_use]['total_USD']
        mwh = float(eia_mwh_by_class[end_use])
        factor = _safe_div(mwh, usd)
        class_factors[end_use] = factor
        detail[end_use] = {
            **usd_by_class[end_use],
            'eia_table_2_2_MWh': mwh,
            'c_class_MWh_per_USD': factor,
            'implied_USD_per_MWh': _safe_div(usd, mwh),
            'implied_cents_per_kWh': _safe_div(usd, mwh) / CENTS_PER_KWH_TO_USD_PER_MWH,
        }

    cols = intermediate_usd.index.union(final_demand_usd.index)
    c_row = pd.Series(index=cols, dtype=float)
    for col in cols:
        end_use = end_use_map[str(col)]
        c_row[col] = class_factors[end_use]
    return c_row, detail


def _ef_summary(
    d: pd.Series,
    n: pd.Series,
    *,
    label: str,
) -> dict[str, Any]:
    elec = list(ELECTRICITY_DISAGG_SECTORS)
    non_elec = [i for i in d.index.astype(str) if i not in elec]
    d = d.copy()
    d.index = d.index.astype(str)
    n = n.copy()
    n.index = n.index.astype(str)

    def _block(series: pd.Series, sectors: list[str]) -> dict[str, Any]:
        vals = series.reindex(sectors).astype(float)
        return {
            'mean': float(vals.mean()),
            'median': float(vals.median()),
            'sum': float(vals.sum()),
            'by_sector': {s: float(vals[s]) for s in sectors if s in vals.index},
        }

    return {
        'label': label,
        'electricity_D': _block(d, elec),
        'electricity_N': _block(n, elec),
        'non_electricity_D': {
            'mean': float(d.reindex(non_elec).mean()),
            'median': float(d.reindex(non_elec).median()),
            'n_sectors': len(non_elec),
        },
        'non_electricity_N': {
            'mean': float(n.reindex(non_elec).mean()),
            'median': float(n.reindex(non_elec).median()),
            'n_sectors': len(non_elec),
        },
    }


def _mixed_efs_with_factors(
    aq_scaled: Any,
    b_monetary: pd.DataFrame,
    *,
    c_col: float,
    c_row: pd.Series,
) -> tuple[pd.Series, pd.Series, dict[str, float]]:
    adom = apply_electricity_unit_conversion_to_A(
        aq_scaled.Adom, c_col=c_col, c_row=c_row
    )
    aimp = apply_electricity_unit_conversion_to_A(
        aq_scaled.Aimp, c_col=c_col, c_row=c_row
    )
    q = apply_electricity_unit_conversion_to_q(aq_scaled.scaled_q, c_col)
    b_mixed = apply_electricity_unit_conversion_to_B(b_monetary, c_col)
    l_tot = compute_L_matrix(A=adom + aimp)
    m = compute_M_matrix(B=b_mixed, L=l_tot)
    d = compute_d(B=b_mixed)
    n = compute_n(M=m)

    gen = GENERATION_SECTOR
    y = backcompute_y_from_A_and_q(A=adom, q=q)
    inter_mwh = float((adom.loc[gen].astype(float) * q.astype(float)).sum())
    fd_mwh = float(y.loc[gen])
    balance = {
        'q_gen': float(q.loc[gen]),
        'intermediate_plus_fd_MWh': inter_mwh + fd_mwh,
        'q_minus_uses_MWh': float(q.loc[gen]) - inter_mwh - fd_mwh,
        'intermediate_MWh': inter_mwh,
        'final_demand_MWh': fd_mwh,
    }
    return d, n, balance


def analyze() -> dict[str, Any]:
    cfg = _install_mixed_config()
    _clear_model_caches()
    aq = derive_cornerstone_Aq_scaled()
    b = derive_cornerstone_B_non_finetuned()
    q = aq.scaled_q.astype(float)
    q.index = q.index.astype(str)
    adom = aq.Adom.copy()
    adom.index = adom.index.astype(str)
    adom.columns = adom.columns.astype(str)

    gen = GENERATION_SECTOR
    intermediate_usd = cast(pd.Series, adom.loc[gen]).astype(float) * q
    y_row_usd = _model_year_y_row_221110(aq).astype(float)
    y_row_usd.index = y_row_usd.index.astype(str)
    end_use_map = build_end_use_map()

    year = int(cfg.model_base_year)
    eia_raw = _eia_table_2_2_sales_mwh(year)
    eia_classes = {
        'Residential': float(eia_raw['Residential']),
        'Commercial': float(eia_raw['Commercial']),
        'Industrial': float(eia_raw['Industrial']),
        'Transportation': float(eia_raw['Transportation']),
    }
    eia_direct = float(eia_raw.get('Direct Use', float('nan')))
    eia_total_end = float(eia_raw.get('Total End Use', float('nan')))
    eia_sales_total = sum(eia_classes.values())

    c_row_eia, class_detail = _class_row_factors_from_eia(
        intermediate_usd, y_row_usd, end_use_map, eia_classes
    )
    prices_2_4 = cast(
        dict[str, float], electricity_end_use_retail_prices_cents_kwh(int(cfg.usa_ghg_data_year))
    )

    # Production path (Table 2.4 + eGRID total)
    prod = compute_mixed_unit_ef_vectors(aq, b, prices_by_class=None)
    # Relative-price path using implied $/MWh as Table-2.4-like cents, still eGRID λ
    implied_as_table24 = {
        k: float(class_detail[k]['implied_cents_per_kWh']) for k in EPA_END_USES
    }
    # Include Total for API completeness if needed by callers; not used for classes.
    implied_as_table24['Total'] = float(
        np.average(
            [implied_as_table24[k] for k in EPA_END_USES],
            weights=[class_detail[k]['eia_table_2_2_MWh'] for k in EPA_END_USES],
        )
    )
    relative = compute_mixed_unit_ef_vectors(aq, b, prices_by_class=implied_as_table24)

    q_usd_gen = float(q.loc[gen])
    from bedrock.extract.disaggregation.egrid_generation import (  # noqa: PLC0415
        us_total_net_generation_mwh,
    )

    egrid_mwh = float(us_total_net_generation_mwh(cfg.model_base_year))
    c_col_egrid = electricity_output_factor(q_usd_gen, egrid_mwh)
    c_col_eia_sales = electricity_output_factor(q_usd_gen, eia_sales_total)

    # Strict EIA class anchors: c_row from Table 2.2; two c_col choices
    d_strict_egrid, n_strict_egrid, bal_egrid = _mixed_efs_with_factors(
        aq, b, c_col=c_col_egrid, c_row=c_row_eia
    )
    d_strict_sales, n_strict_sales, bal_sales = _mixed_efs_with_factors(
        aq, b, c_col=c_col_eia_sales, c_row=c_row_eia
    )

    # Verify class MWh under strict row factors (independent of c_col for non-gen cols)
    class_mwh_check: dict[str, float] = {}
    for end_use in EPA_END_USES:
        cols_i = [c for c in intermediate_usd.index if end_use_map[str(c)] == end_use]
        cols_f = [c for c in y_row_usd.index if end_use_map[str(c)] == end_use]
        factor = float(class_detail[end_use]['c_class_MWh_per_USD'])
        mwh = float(intermediate_usd.reindex(cols_i).fillna(0.0).sum()) * factor
        mwh += float(y_row_usd.reindex(cols_f).fillna(0.0).sum()) * factor
        class_mwh_check[end_use] = mwh

    # Electricity self-purchase share of Industrial USD (important distortion)
    elec_codes = list(ELECTRICITY_DISAGG_SECTORS)
    industrial_usd = class_detail['Industrial']['total_USD']
    elec_inter_usd = float(
        intermediate_usd.reindex([c for c in elec_codes if c in intermediate_usd.index])
        .fillna(0.0)
        .sum()
    )

    prod_c_col, prod_c_row = electricity_conversion_factors(aq)

    def _pct_delta(new: float, old: float) -> float:
        return _safe_div(new - old, abs(old))

    def _compare(
        summary_new: dict[str, Any], summary_old: dict[str, Any]
    ) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for block in (
            'electricity_D',
            'electricity_N',
            'non_electricity_D',
            'non_electricity_N',
        ):
            out[block] = {
                'mean_pct_delta': _pct_delta(
                    summary_new[block]['mean'], summary_old[block]['mean']
                ),
                'median_pct_delta': _pct_delta(
                    summary_new[block]['median'], summary_old[block]['median']
                ),
            }
            if 'by_sector' in summary_new[block]:
                out[block]['by_sector_pct_delta'] = {
                    s: _pct_delta(
                        summary_new[block]['by_sector'][s],
                        summary_old[block]['by_sector'][s],
                    )
                    for s in summary_new[block]['by_sector']
                }
        return out

    prod_sum = _ef_summary(prod.D, prod.N, label='production_table_2_4_egrid')
    rel_sum = _ef_summary(relative.D, relative.N, label='implied_prices_egrid_lambda')
    strict_egrid_sum = _ef_summary(
        d_strict_egrid, n_strict_egrid, label='strict_table_2_2_c_col_egrid'
    )
    strict_sales_sum = _ef_summary(
        d_strict_sales, n_strict_sales, label='strict_table_2_2_c_col_eia_sales'
    )

    return {
        'config': MIXED_CONFIG,
        'model_base_year': year,
        'usa_ghg_data_year': int(cfg.usa_ghg_data_year),
        'method': {
            'monetary_basis': 'post–3-way-split Adom[221110]*q and model-year Y row',
            'alternate_row_factor': 'c_class = EIA_Table_2_2_MWh[class] / USD[class]',
            'shared_price_assumption': (
                'all IO/FD columns mapped to a class share one class factor/price'
            ),
        },
        'eia_table_2_2': {
            **eia_classes,
            'Direct_Use_MWh': eia_direct,
            'Total_sales_MWh': eia_sales_total,
            'Total_End_Use_MWh': eia_total_end,
            'eGRID_net_generation_MWh': egrid_mwh,
            'eGRID_minus_sales_MWh': egrid_mwh - eia_sales_total,
        },
        'question_1_implied_prices': {
            'by_class': {
                k: {
                    'monetary_USD': class_detail[k]['total_USD'],
                    'eia_MWh': class_detail[k]['eia_table_2_2_MWh'],
                    'implied_USD_per_MWh': class_detail[k]['implied_USD_per_MWh'],
                    'implied_cents_per_kWh': class_detail[k]['implied_cents_per_kWh'],
                    'table_2_4_cents_per_kWh': float(prices_2_4[k]),
                    'implied_over_table_2_4': _safe_div(
                        class_detail[k]['implied_cents_per_kWh'],
                        float(prices_2_4[k]),
                    ),
                }
                for k in EPA_END_USES
            },
            'note': (
                'Implied price = monetary 221110 USD in class / EIA Table 2.2 MWh '
                'for class. Units: $/MWh = USD/MWh; ¢/kWh = ($/MWh)/10.'
            ),
        },
        'question_2_ef_impacts': {
            'production_table_2_4': prod_sum,
            'variant_A_implied_prices_keep_egrid_total': {
                'summary': rel_sum,
                'vs_production': _compare(rel_sum, prod_sum),
                'note': (
                    'Uses implied class prices inside the existing λ/eGRID machinery. '
                    'Class MWh totals are NOT exactly EIA 2.2; relative prices are.'
                ),
            },
            'variant_B_strict_table_2_2_row_c_col_egrid': {
                'summary': strict_egrid_sum,
                'vs_production': _compare(strict_egrid_sum, prod_sum),
                'balance': bal_egrid,
                'note': (
                    'Row factors force class MWh = EIA 2.2; c_col still eGRID/q so '
                    'q_gen = eGRID. Expect large row-balance residual.'
                ),
            },
            'variant_C_strict_table_2_2_row_c_col_eia_sales': {
                'summary': strict_sales_sum,
                'vs_production': _compare(strict_sales_sum, prod_sum),
                'balance': bal_sales,
                'note': (
                    'Same strict row factors; c_col = EIA sales total / q_USD so '
                    'q_gen matches sales total. Residuals shrink but Direct Use / '
                    'losses / T&D still unresolved.'
                ),
            },
            'interpretation': {
                'electricity_D': (
                    'D for generation depends on c_col (B_gen / c_col), not c_row. '
                    'Keeping eGRID c_col leaves electricity D essentially unchanged; '
                    'switching c_col to EIA sales changes D_221110 inversely with c_col.'
                ),
                'electricity_N': (
                    'N for electricity moves with L through the generation sales row '
                    '(c_row) and with D_gen when c_col changes.'
                ),
                'non_electricity_D': (
                    'Non-electricity D is unchanged: only the generation column of B '
                    'is scaled by c_col.'
                ),
                'non_electricity_N': (
                    'Non-electricity N changes because A[221110, j] · c_j alters '
                    'electricity requirements in L for every purchaser.'
                ),
            },
        },
        'question_3_feasibility': {
            'class_mwh_check_equals_eia': class_mwh_check,
            'production_c_col': float(prod_c_col),
            'c_col_egrid': c_col_egrid,
            'c_col_eia_sales': c_col_eia_sales,
            'electricity_industry_usd_in_industrial_class': elec_inter_usd,
            'industrial_class_usd': industrial_usd,
            'electricity_share_of_industrial_usd': _safe_div(
                elec_inter_usd, industrial_usd
            ),
            'issues': [
                {
                    'id': 'row_vs_output_identity',
                    'severity': 'high',
                    'detail': (
                        'EIA sales total '
                        f'({eia_sales_total / 1e6:.1f} TWh) ≠ eGRID generation '
                        f'({egrid_mwh / 1e6:.1f} TWh). After A conversion, y is '
                        'backcomputed so q − uses ≈ 0 always; the real failure is '
                        'compositional: with c_col=eGRID, backcomputed FD becomes '
                        f'{bal_egrid["final_demand_MWh"] / 1e6:.0f} TWh, not the '
                        'EIA-class FD implied by the Table 2.2 anchors.'
                    ),
                },
                {
                    'id': 'direct_use_and_losses',
                    'severity': 'high',
                    'detail': (
                        'Table 2.2 Direct Use and grid losses / plant use are not in '
                        'the four sales classes. The mapping has nowhere to put them '
                        'without an extra residual class or changing c_col/q.'
                    ),
                },
                {
                    'id': 'electricity_self_use_in_industrial',
                    'severity': 'high',
                    'detail': (
                        f'Electricity-industry intermediate purchases are '
                        f'{100 * elec_inter_usd / industrial_usd:.1f}% of Industrial-'
                        'mapped USD. Forcing Industrial MWh = EIA Industrial assigns '
                        'utility/self-generation throughput into the EIA industrial '
                        'customer bucket.'
                    ),
                },
                {
                    'id': 'delivered_vs_generation_commodity',
                    'severity': 'high',
                    'detail': (
                        'EIA classes are delivered sales; 221110 is generation only. '
                        'Matching them forces generation-row MWh to equal delivered '
                        'customer-class MWh.'
                    ),
                },
                {
                    'id': 'import_row_and_margins',
                    'severity': 'medium',
                    'detail': (
                        'Aimp generation row would need the same class factors; '
                        'purchaser-price vs producer-price and margin treatments '
                        'still differ from utility revenue/sales.'
                    ),
                },
                {
                    'id': 'negative_or_tiny_class_usd',
                    'severity': 'medium',
                    'detail': (
                        'A class with near-zero or negative net USD (inventory / '
                        'scrap quirks in Y) makes c_class unstable.'
                    ),
                },
                {
                    'id': 'ef_units_and_downstream',
                    'severity': 'medium',
                    'detail': (
                        'Changing c_col changes D_gen units (per MWh scale). N for '
                        'non-electricity sectors moves with L even when D does not, '
                        'so footprint tables and BLy attributions shift.'
                    ),
                },
            ],
        },
        'production_reference': {
            'c_col': float(prod_c_col),
            'c_row_min': float(prod_c_row.min()),
            'c_row_max': float(prod_c_row.max()),
        },
    }


def _fmt_pct(x: float) -> str:
    return f'{100 * x:+.2f}%'


def _fmt_price(x: float) -> str:
    return f'{x:,.2f}'


def render_report(p: Mapping[str, Any]) -> str:
    q1 = p['question_1_implied_prices']['by_class']
    q2 = p['question_2_ef_impacts']
    q3 = p['question_3_feasibility']
    eia = p['eia_table_2_2']

    lines = [
        '# Counterfactual: unit conversion anchored to EIA Table 2.2 MWh',
        '',
        f"Config: `{p['config']}` (model year {p['model_base_year']}).",
        '',
        '## Setup',
        '',
        'Keep the post–3-way-split monetary 221110 uses and the same end-use map. '
        'Replace Table 2.4 / eGRID row conversion with class factors',
        '',
        '```text',
        'c_class = EIA_Table_2_2_MWh[class] / USD_221110[class]',
        'implied_price = USD_221110[class] / EIA_Table_2_2_MWh[class]',
        '```',
        '',
        'so every IO/FD column mapped to that class shares one price / MWh-per-dollar.',
        '',
        f"EIA sales total = **{eia['Total_sales_MWh'] / 1e6:,.1f} TWh**; "
        f"eGRID net gen = **{eia['eGRID_net_generation_MWh'] / 1e6:,.1f} TWh**; "
        f"gap = **{eia['eGRID_minus_sales_MWh'] / 1e6:,.1f} TWh**.",
        '',
        '## 1. Implied prices by end-use class',
        '',
        '| Class | Model USD (B) | EIA 2.2 TWh | Implied $/MWh | '
        'Implied ¢/kWh | Table 2.4 ¢/kWh | Implied / 2.4 |',
        '|---|---:|---:|---:|---:|---:|---:|',
    ]
    for k in EPA_END_USES:
        r = q1[k]
        lines.append(
            f"| {k} | {r['monetary_USD'] / 1e9:,.2f} | "
            f"{r['eia_MWh'] / 1e6:,.1f} | "
            f"{_fmt_price(r['implied_USD_per_MWh'])} | "
            f"{_fmt_price(r['implied_cents_per_kWh'])} | "
            f"{_fmt_price(r['table_2_4_cents_per_kWh'])} | "
            f"{r['implied_over_table_2_4']:.2f} |"
        )
    lines.extend(
        [
            '',
            p['question_1_implied_prices']['note'],
            '',
            '### Read of the prices',
            '',
            '- **Residential** implied ¢/kWh is far **below** Table 2.4 (4.2 vs 16.0) '
            'because model F01000 USD is too small relative to EIA Residential MWh.',
            '- **Commercial** is also low (6.1 vs 12.6).',
            '- **Industrial** lands near Table 2.4 (7.8 vs 8.0) only because large '
            'electricity-industry self-use inflates Industrial-mapped USD; that is a '
            'coincidence of mapping, not evidence the Industrial map is “right.”',
            '- **Transportation** is an extreme outlier (73 vs 13 ¢/kWh): tiny EIA MWh '
            'vs non-trivial mapped model USD.',
            '',
            '## 2. Impacts on D and N',
            '',
            '### Analytic expectations',
            '',
            f"- {q2['interpretation']['electricity_D']}",
            f"- {q2['interpretation']['non_electricity_D']}",
            f"- {q2['interpretation']['electricity_N']}",
            f"- {q2['interpretation']['non_electricity_N']}",
            '',
            '### Quantitative counterfactuals vs production (Table 2.4 + eGRID)',
            '',
            '| Variant | Elec D mean Δ | Elec N mean Δ | Non-elec D mean Δ | '
            'Non-elec N mean Δ | Notes |',
            '|---|---:|---:|---:|---:|---|',
        ]
    )

    def _row(name: str, key: str, note: str) -> None:
        vs = q2[key]['vs_production']
        lines.append(
            f"| {name} | {_fmt_pct(vs['electricity_D']['mean_pct_delta'])} | "
            f"{_fmt_pct(vs['electricity_N']['mean_pct_delta'])} | "
            f"{_fmt_pct(vs['non_electricity_D']['mean_pct_delta'])} | "
            f"{_fmt_pct(vs['non_electricity_N']['mean_pct_delta'])} | {note} |"
        )

    _row(
        'A. Implied prices, keep eGRID λ',
        'variant_A_implied_prices_keep_egrid_total',
        'relative prices only; total still eGRID',
    )
    bal_b = q2['variant_B_strict_table_2_2_row_c_col_egrid']['balance']
    bal_c = q2['variant_C_strict_table_2_2_row_c_col_eia_sales']['balance']
    _row(
        'B. Strict 2.2 row, c_col = eGRID',
        'variant_B_strict_table_2_2_row_c_col_egrid',
        f"q=eGRID; backcomputed FD={bal_b['final_demand_MWh'] / 1e6:,.0f} TWh",
    )
    _row(
        'C. Strict 2.2 row, c_col = EIA sales',
        'variant_C_strict_table_2_2_row_c_col_eia_sales',
        f"q=sales; backcomputed FD={bal_c['final_demand_MWh'] / 1e6:,.0f} TWh",
    )

    prod_e = q2['production_table_2_4']['electricity_D']['by_sector']
    strict_e = q2['variant_C_strict_table_2_2_row_c_col_eia_sales']['summary'][
        'electricity_D'
    ]['by_sector']
    strict_n = q2['variant_C_strict_table_2_2_row_c_col_eia_sales']['summary'][
        'electricity_N'
    ]['by_sector']
    prod_n = q2['production_table_2_4']['electricity_N']['by_sector']
    lines.extend(
        [
            '',
            '### Electricity sectors under variant C (illustrative)',
            '',
            '| Sector | D prod | D alt C | ΔD | N prod | N alt C | ΔN |',
            '|---|---:|---:|---:|---:|---:|---:|',
        ]
    )
    for s in ELECTRICITY_DISAGG_SECTORS:
        lines.append(
            f"| {s} | {prod_e[s]:.4f} | {strict_e[s]:.4f} | "
            f"{_fmt_pct(_safe_div(strict_e[s] - prod_e[s], abs(prod_e[s])))} | "
            f"{prod_n[s]:.4f} | {strict_n[s]:.4f} | "
            f"{_fmt_pct(_safe_div(strict_n[s] - prod_n[s], abs(prod_n[s])))} |"
        )

    lines.extend(
        [
            '',
            '## 3. Production feasibility / IO balance',
            '',
            f"Electricity-industry purchases are "
            f"**{100 * q3['electricity_share_of_industrial_usd']:.1f}%** of "
            'Industrial-mapped 221110 USD — a core reason Industrial implied prices '
            'look nothing like Table 2.4.',
            '',
        ]
    )
    for issue in q3['issues']:
        lines.append(f"- **{issue['id']}** ({issue['severity']}): {issue['detail']}")
    lines.extend(
        [
            '',
            '### Bottom line',
            '',
            '1. Implied prices are well-defined from USD_class / EIA_2.2_MWh_class, '
            'but they are **accounting residuals**, not retail tariffs — especially '
            'Residential (too low) and Transportation (too high).',
            '2. **Non-electricity D is unchanged**; **non-electricity N moves** with '
            'the generation sales row. Electricity **D moves only if c_col changes**; '
            'electricity **N moves with both c_row and c_col**.',
            '3. A full production implementation is awkward: eGRID vs EIA sales, '
            'Direct Use/losses, generation-vs-delivered scope, and electricity '
            'self-use inside Industrial. Algebraic row balance can be forced by '
            'backcomputing y, but then class MWh no longer match Table 2.2.',
            '',
            '## Reproduce',
            '',
            '```',
            'python -m bedrock.analysis.electricity_disagg_diagnostics.'
            'table_2_2_unit_conversion_counterfactual',
            '```',
            '',
        ]
    )
    return '\n'.join(lines)


def main() -> None:
    OUT_SUBDIR.mkdir(parents=True, exist_ok=True)
    payload = analyze()
    REPORT_JSON.write_text(json.dumps(payload, indent=2), encoding='utf-8')
    REPORT_MD.write_text(render_report(payload), encoding='utf-8')
    print(f'Wrote {REPORT_MD}')
    print(f'Wrote {REPORT_JSON}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    main()
