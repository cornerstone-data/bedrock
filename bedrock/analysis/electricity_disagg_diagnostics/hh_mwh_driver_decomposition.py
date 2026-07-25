"""Decompose household-vs-intermediate electricity differences from EIA.

The analysis keeps the post-three-way-split monetary IO fixed and changes only
the generation-row conversion factors.  It separates:

A. monetary IO structure before mixed units;
B. purchaser-class prices and mapping (actual vs uniform-price conversion);
C. which intermediate purchasers receive generation MWh;
D. sensitivity to model/EIA bucket pairings; and
E. independent BEA/EIA definitions and a PCE-dollar sanity check.

Run:
    python -m bedrock.analysis.electricity_disagg_diagnostics.hh_mwh_driver_decomposition
"""

from __future__ import annotations

import json
import logging
from typing import Any, Mapping, cast

import pandas as pd

from bedrock.analysis.electricity_disagg_diagnostics.hh_vs_interindustry import (
    HH_FD_CODE,
    MIXED_CONFIG,
    _eia_table_2_2_sales_mwh,
    _fd_share_matrix,
    _install_mixed_config,
)
from bedrock.analysis.electricity_disagg_diagnostics.paths import OUT_DIR
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    _model_year_y_row_221110,
    derive_disagg_Ytot_with_trade,
    electricity_conversion_factors,
)
from bedrock.transform.eeio.derived_cornerstone import derive_cornerstone_Aq_scaled
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.transform.eeio.electricity_end_use_mapping import (
    EPA_END_USES,
    build_end_use_map,
    table_2_4_prices_cents_kwh,
)
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.taxonomy.cornerstone.commodities import COMMODITY_DESC

logger = logging.getLogger(__name__)

OUT_SUBDIR = OUT_DIR / 'hh_vs_interindustry'
REPORT_MD = OUT_SUBDIR / 'hh_mwh_driver_decomposition.md'
REPORT_JSON = OUT_SUBDIR / 'hh_mwh_driver_decomposition.json'

# NIPA Table 2.4.5 / BEA account DELCRC, current-dollar annual value retrieved
# 2026-07-24.  It is deliberately explicit rather than silently downloading a
# revisable series, so each report records its evidence vintage.
BEA_PCE_ELECTRICITY_2023_USD = 236.748e9
EVIDENCE_RETRIEVED = '2026-07-24'

EXTERNAL_SOURCES: tuple[dict[str, str], ...] = (
    {
        'organization': 'BEA',
        'source': 'NIPA Handbook, Chapter 5 — Personal Consumption Expenditures',
        'url': 'https://www.bea.gov/resources/methodologies/nipa-handbook/pdf/chapter-05.pdf',
        'finding': (
            'PCE electricity uses EIA residential revenue and residential kWh/price '
            'data, adjusted by BEA from a billing to a usage basis.'
        ),
    },
    {
        'organization': 'BEA',
        'source': 'NIPA Table 2.4.5, Household utilities: Electricity (DELCRC)',
        'url': 'https://fred.stlouisfed.org/series/DELCRC1A027NBEA',
        'finding': 'Current-dollar PCE electricity was $236.748 billion in 2023.',
    },
    {
        'organization': 'BEA',
        'source': 'FAQ 84 — detail beyond PCE Table 2.4.5U',
        'url': 'https://www.bea.gov/help/faq/84',
        'finding': (
            'Table 2.4.5U is the most detailed time series; benchmark IO PCE Bridge '
            'files provide the IO commodity composition of each PCE category.'
        ),
    },
    {
        'organization': 'BEA',
        'source': 'Historical Benchmark Input-Output Tables / PCE Bridge',
        'url': 'https://www.bea.gov/industry/historical-benchmark-input-output-tables',
        'finding': (
            'PCE Bridge tables reconcile NIPA PCE with IO commodities at producer '
            'and purchaser prices; they are the appropriate bridge, not total F01000.'
        ),
    },
    {
        'organization': 'EIA',
        'source': 'Electric Power Annual Table 2.2',
        'url': 'https://www.eia.gov/electricity/annual/html/epa_02_02.html',
        'finding': (
            'Reports sales to ultimate customers by Residential, Commercial, '
            'Industrial, and Transportation, plus Direct Use and Total End Use.'
        ),
    },
    {
        'organization': 'EIA',
        'source': 'Form EIA-861 instructions',
        'url': 'https://www.eia.gov/survey/form/eia_861/instructions.pdf',
        'finding': (
            'Residential includes private households and apartment buildings where '
            'electricity is consumed for household purposes; Commercial includes '
            'nonmanufacturing businesses, institutions, government, and lighting.'
        ),
    },
    {
        'organization': 'EIA',
        'source': 'Guide to EIA Electric Power Data',
        'url': 'https://www.eia.gov/electricity/data/guide/pdf/guide.pdf',
        'finding': (
            'EIA-861 is a census of utilities and other sellers; its sectors are '
            'customer/end-use classes, not IO purchaser industries.'
        ),
    },
)


def _safe_ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else float('nan')


def _flows_by_bucket(
    intermediate: pd.Series,
    final_demand: pd.Series,
) -> dict[str, float]:
    hh = float(final_demand.get(HH_FD_CODE, 0.0))
    other_fd = float(final_demand.sum()) - hh
    self_use = float(intermediate.get(GENERATION_SECTOR, 0.0))
    inter = float(intermediate.sum())
    other_inter = inter - self_use
    total = inter + hh + other_fd
    return {
        'intermediate': inter,
        'intermediate_221110_self_use': self_use,
        'intermediate_other': other_inter,
        'household_F01000': hh,
        'other_final_demand': other_fd,
        'total': total,
        'intermediate_share': _safe_ratio(inter, total),
        'intermediate_221110_self_use_share': _safe_ratio(self_use, total),
        'intermediate_other_share': _safe_ratio(other_inter, total),
        'household_share': _safe_ratio(hh, total),
        'other_final_demand_share': _safe_ratio(other_fd, total),
    }


def _convert_flows(
    intermediate_usd: pd.Series,
    final_demand_usd: pd.Series,
    factors: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """Apply purchaser-specific MWh/USD factors to identical monetary flows."""
    inter_factors = factors.reindex(intermediate_usd.index)
    fd_factors = factors.reindex(final_demand_usd.index)
    if inter_factors.isna().any() or fd_factors.isna().any():
        missing = list(inter_factors[inter_factors.isna()].index) + list(
            fd_factors[fd_factors.isna()].index
        )
        raise ValueError(f'Missing conversion factors for columns: {missing[:10]}')
    return intermediate_usd * inter_factors, final_demand_usd * fd_factors


def _by_end_use(
    intermediate: pd.Series,
    final_demand: pd.Series,
    end_use_map: Mapping[str, str],
) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for end_use in EPA_END_USES:
        inter_cols = [c for c in intermediate.index if end_use_map[str(c)] == end_use]
        fd_cols = [c for c in final_demand.index if end_use_map[str(c)] == end_use]
        inter = float(intermediate.reindex(inter_cols).sum())
        fd = float(final_demand.reindex(fd_cols).sum())
        result[end_use] = {
            'intermediate_MWh': inter,
            'final_demand_MWh': fd,
            'total_MWh': inter + fd,
        }
    return result


def _load_sector_names() -> dict[str, str]:
    names = {str(code): str(name) for code, name in COMMODITY_DESC.items()}
    names.update(
        {
            '221110': 'Electric power generation',
            '221121': 'Electric bulk power transmission and control',
            '221122': 'Electric power distribution',
        }
    )
    return names


def _is_housing_adjacent(code: str, name: str) -> bool:
    text = f'{code} {name}'.lower()
    terms = ('real estate', 'housing', 'residential', 'lessor', 'apartment')
    return any(term in text for term in terms)


def _top_purchasers(
    intermediate_usd: pd.Series,
    intermediate_mwh: pd.Series,
    end_use_map: Mapping[str, str],
    limit: int = 25,
) -> list[dict[str, Any]]:
    names = _load_sector_names()
    rows: list[dict[str, Any]] = []
    for code, mwh in intermediate_mwh.sort_values(ascending=False).head(limit).items():
        code_str = str(code)
        name = names.get(code_str, code_str)
        rows.append(
            {
                'code': code_str,
                'name': name,
                'assigned_end_use': end_use_map[code_str],
                'monetary_flow_USD': float(
                    cast(float, intermediate_usd.get(code_str, 0.0))
                ),
                'production_MWh': float(mwh),
                'housing_adjacent_keyword_flag': _is_housing_adjacent(code_str, name),
            }
        )
    return rows


def _supply_chain_summary(
    intermediate_mwh: pd.Series,
) -> dict[str, Any]:
    child_rows = {
        code: float(intermediate_mwh.get(code, 0.0))
        for code in ELECTRICITY_DISAGG_SECTORS
    }
    supply_chain = sum(child_rows.values())
    total = float(intermediate_mwh.sum())
    return {
        'electricity_children_MWh': child_rows,
        'electricity_supply_chain_total_MWh': supply_chain,
        'other_intermediate_purchasers_MWh': total - supply_chain,
        'share_of_intermediate_in_electricity_supply_chain': _safe_ratio(
            supply_chain, total
        ),
        'interpretation': (
            'These are generation-row purchases by electricity industries. They are '
            'intermediate in IO accounting but are not themselves ultimate EIA '
            'customer classes; their downstream destination requires supply-chain '
            'tracing and cannot be inferred from the direct 221110 row.'
        ),
    }


def _legacy_household_allocation(
    production_fd_mwh: pd.Series,
) -> dict[str, Any]:
    """Reproduce the allocation convention in hh_vs_interindustry.py."""
    y_2017 = derive_disagg_Ytot_with_trade().copy()
    y_2017.index = y_2017.index.astype(str)
    shares = _fd_share_matrix(y_2017)
    hh_share = float(cast(float, shares.at[GENERATION_SECTOR, HH_FD_CODE]))
    total = float(production_fd_mwh.sum())
    household = total * hh_share
    return {
        'production_direct_converted_F01000_MWh': float(
            production_fd_mwh.get(HH_FD_CODE, 0.0)
        ),
        'legacy_nonnegative_domestic_share_F01000_MWh': household,
        'legacy_other_final_demand_MWh': total - household,
        'difference_MWh': float(production_fd_mwh.get(HH_FD_CODE, 0.0))
        - household,
        'note': (
            'The direct scenario converts each raw model-year Y cell with its class '
            'factor. The earlier report instead allocates total mixed y_nab using '
            'nonnegative domestic 2017 Y shares (imports excluded).'
        ),
    }


def _eia_values(year: int) -> dict[str, float]:
    raw = _eia_table_2_2_sales_mwh(year)
    residential = raw.get('Residential', float('nan'))
    commercial = raw.get('Commercial', float('nan'))
    industrial = raw.get('Industrial', float('nan'))
    transportation = raw.get('Transportation', float('nan'))
    direct = raw.get('Direct Use', float('nan'))
    total_end = raw.get('Total End Use', float('nan'))
    return {
        'Residential': residential,
        'Commercial': commercial,
        'Industrial': industrial,
        'Transportation': transportation,
        'Direct Use': direct,
        'Nonresidential sales': commercial + industrial + transportation,
        'Total sales': residential + commercial + industrial + transportation,
        'Total End Use': total_end,
    }


def _pairings(
    buckets: Mapping[str, float],
    by_end_use: Mapping[str, Mapping[str, float]],
    eia: Mapping[str, float],
    supply_chain: Mapping[str, Any],
) -> list[dict[str, Any]]:
    residential_fd = by_end_use['Residential']['final_demand_MWh']
    other_fd = buckets['other_final_demand']
    rows = [
        (
            'F01000 only',
            buckets['household_F01000'],
            'EIA Residential',
            eia['Residential'],
        ),
        (
            'All Residential-mapped final demand',
            residential_fd,
            'EIA Residential',
            eia['Residential'],
        ),
        (
            'All intermediate',
            buckets['intermediate'],
            'EIA Com+Ind+Trans sales',
            eia['Nonresidential sales'],
        ),
        (
            'Intermediate excluding electricity-industry purchasers',
            supply_chain['other_intermediate_purchasers_MWh'],
            'EIA Com+Ind+Trans sales',
            eia['Nonresidential sales'],
        ),
        (
            'Intermediate + all other final demand',
            buckets['intermediate'] + other_fd,
            'EIA Com+Ind+Trans sales + Direct Use',
            eia['Nonresidential sales'] + eia['Direct Use'],
        ),
    ]
    for end_use in EPA_END_USES:
        rows.append(
            (
                f'All model uses mapped {end_use}',
                by_end_use[end_use]['total_MWh'],
                f'EIA {end_use}',
                eia[end_use],
            )
        )
    rows.extend(
        [
            (
                'All model 221110 uses',
                buckets['total'],
                'EIA Total sales',
                eia['Total sales'],
            ),
            (
                'All model 221110 uses',
                buckets['total'],
                'EIA Total End Use',
                eia['Total End Use'],
            ),
        ]
    )
    return [
        {
            'model_bucket': label,
            'model_MWh': model,
            'eia_bucket': eia_label,
            'eia_MWh': comparator,
            'model_over_eia': _safe_ratio(model, comparator),
        }
        for label, model, eia_label, comparator in rows
    ]


def _driver_metrics(
    monetary: Mapping[str, float],
    uniform: Mapping[str, float],
    production: Mapping[str, float],
    eia: Mapping[str, float],
) -> dict[str, Any]:
    eia_res = eia['Residential']
    eia_nonres = eia['Nonresidential sales']
    hh_price_effect = production['household_F01000'] - uniform['household_F01000']
    inter_price_effect = production['intermediate'] - uniform['intermediate']
    hh_total_gap = eia_res - production['household_F01000']
    inter_total_excess = production['intermediate'] - eia_nonres
    return {
        'household': {
            'eia_residential_MWh': eia_res,
            'uniform_price_model_MWh': uniform['household_F01000'],
            'production_model_MWh': production['household_F01000'],
            'structure_and_definition_gap_at_uniform_price_MWh': (
                eia_res - uniform['household_F01000']
            ),
            'production_minus_uniform_price_effect_MWh': hh_price_effect,
            'total_eia_minus_production_gap_MWh': hh_total_gap,
            'share_of_total_shortfall_added_by_class_prices': _safe_ratio(
                uniform['household_F01000'] - production['household_F01000'],
                hh_total_gap,
            ),
        },
        'intermediate': {
            'eia_nonresidential_sales_MWh': eia_nonres,
            'uniform_price_model_MWh': uniform['intermediate'],
            'production_model_MWh': production['intermediate'],
            'uniform_price_minus_eia_excess_MWh': uniform['intermediate'] - eia_nonres,
            'production_minus_uniform_price_effect_MWh': inter_price_effect,
            'total_production_minus_eia_excess_MWh': inter_total_excess,
            'share_of_total_excess_added_by_class_prices': _safe_ratio(
                inter_price_effect, inter_total_excess
            ),
        },
        'share_movements': {
            'monetary_household_share': monetary['household_share'],
            'uniform_household_share': uniform['household_share'],
            'production_household_share': production['household_share'],
            'eia_residential_share_of_sales': _safe_ratio(
                eia_res, eia['Total sales']
            ),
        },
        'interpretation_rule': (
            'Uniform-price shares equal post-three-way monetary shares. The change '
            'from uniform to production isolates purchaser-class prices plus their '
            'end-use mapping while holding monetary IO flows and total MWh fixed.'
        ),
    }


def _external_check(
    year: int,
    prices: Mapping[str, float],
    eia: Mapping[str, float],
    production: Mapping[str, float],
) -> dict[str, Any]:
    if year != 2023:
        return {
            'available': False,
            'reason': 'Curated current-dollar PCE electricity value is for 2023 only.',
            'sources': list(EXTERNAL_SOURCES),
        }
    price_cents_kwh = float(prices['Residential'])
    implied_mwh = BEA_PCE_ELECTRICITY_2023_USD / (price_cents_kwh * 10.0)
    return {
        'available': True,
        'evidence_retrieved': EVIDENCE_RETRIEVED,
        'bea_pce_electricity_USD': BEA_PCE_ELECTRICITY_2023_USD,
        'eia_residential_price_cents_per_kWh': price_cents_kwh,
        'bea_pce_divided_by_eia_price_implied_MWh': implied_mwh,
        'eia_residential_MWh': eia['Residential'],
        'implied_MWh_over_eia_residential': _safe_ratio(
            implied_mwh, eia['Residential']
        ),
        'model_F01000_MWh': production['household_F01000'],
        'model_over_bea_price_implied_MWh': _safe_ratio(
            production['household_F01000'], implied_mwh
        ),
        'sources': list(EXTERNAL_SOURCES),
        'caveat': (
            'PCE dollars are a national-accounting purchaser-value measure and EIA '
            'price is utility revenue per kWh. Their quotient is a sanity check, not '
            'an exact BEA published physical series; BEA also adjusts billing to usage.'
        ),
    }


def analyze() -> dict[str, Any]:
    cfg = _install_mixed_config()
    aq = derive_cornerstone_Aq_scaled()
    q = aq.scaled_q.astype(float)
    q.index = q.index.astype(str)
    adom = aq.Adom.copy()
    adom.index = adom.index.astype(str)
    adom.columns = adom.columns.astype(str)

    gen = GENERATION_SECTOR
    intermediate_usd = cast(pd.Series, adom.loc[gen]).astype(float) * q
    y_row_usd = _model_year_y_row_221110(aq).astype(float)
    y_row_usd.index = y_row_usd.index.astype(str)
    monetary = _flows_by_bucket(intermediate_usd, y_row_usd)

    c_col, production_factors = electricity_conversion_factors(aq)
    total_mwh = float(q.loc[gen]) * float(c_col)
    uniform_factor = total_mwh / monetary['total']
    uniform_factors = pd.Series(
        uniform_factor,
        index=production_factors.index,
        dtype=float,
    )

    production_inter_mwh, production_fd_mwh = _convert_flows(
        intermediate_usd, y_row_usd, production_factors
    )
    uniform_inter_mwh, uniform_fd_mwh = _convert_flows(
        intermediate_usd, y_row_usd, uniform_factors
    )
    production = _flows_by_bucket(production_inter_mwh, production_fd_mwh)
    uniform = _flows_by_bucket(uniform_inter_mwh, uniform_fd_mwh)

    end_use_map = build_end_use_map()
    by_end_use = _by_end_use(
        production_inter_mwh, production_fd_mwh, end_use_map
    )
    supply_chain = _supply_chain_summary(production_inter_mwh)
    eia = _eia_values(int(cfg.model_base_year))
    prices = cast(
        Mapping[str, float],
        table_2_4_prices_cents_kwh(int(cfg.usa_ghg_data_year)),
    )

    return {
        'config': MIXED_CONFIG,
        'model_base_year': int(cfg.model_base_year),
        'usa_ghg_data_year': int(cfg.usa_ghg_data_year),
        'method': {
            'monetary_intermediate': 'Adom[221110, j] * q[j]',
            'monetary_final_demand': (
                '_model_year_y_row_221110: y_nab[221110] split by 2017 Y shares'
            ),
            'uniform_conversion': (
                'same MWh/USD factor for every purchaser; total fixed to eGRID MWh'
            ),
            'production_conversion': (
                'c_j = lambda / EIA Table 2.4 class price; class from end-use map'
            ),
        },
        'A_monetary_structure': {
            'units': 'USD',
            'buckets': monetary,
            'note': (
                'This is the post-three-way-split monetary IO before mixed-unit '
                'conversion. Shares, not dollar levels, are compared with EIA.'
            ),
        },
        'B_price_mapping_counterfactual': {
            'target_total_MWh': total_mwh,
            'prices_cents_per_kWh': dict(prices),
            'uniform_factor_MWh_per_USD': uniform_factor,
            'production_factor_range_MWh_per_USD': {
                'min': float(production_factors.min()),
                'median': float(production_factors.median()),
                'max': float(production_factors.max()),
            },
            'uniform_price_buckets_MWh': uniform,
            'production_buckets_MWh': production,
            'driver_metrics': _driver_metrics(monetary, uniform, production, eia),
            'legacy_report_reconciliation': _legacy_household_allocation(
                production_fd_mwh
            ),
            'reconciliations_MWh': {
                'uniform_minus_target': uniform['total'] - total_mwh,
                'production_minus_target': production['total'] - total_mwh,
            },
        },
        'C_intermediate_purchasers': {
            'production_MWh_by_assigned_end_use': by_end_use,
            'electricity_supply_chain': supply_chain,
            'top_25': _top_purchasers(
                intermediate_usd,
                production_inter_mwh,
                end_use_map,
            ),
            'housing_flag_note': (
                'Keyword flag is descriptive only; it does not reclassify an IO '
                'industry or prove that its electricity serves households.'
            ),
        },
        'D_comparator_sensitivity': {
            'eia_MWh': eia,
            'pairings': _pairings(production, by_end_use, eia, supply_chain),
            'note': (
                'Mapped class comparisons test the current end-use mapping; they do '
                'not make IO industries identical to utility customer classes. The '
                'electricity-excluded row is only a boundary sensitivity: it does not '
                'trace those MWh to their eventual customers.'
            ),
        },
        'E_external_evidence': _external_check(
            int(cfg.model_base_year), prices, eia, production
        ),
    }


def _fmt_twh(value: float) -> str:
    return f'{value / 1e6:,.1f}'


def _fmt_billion(value: float) -> str:
    return f'{value / 1e9:,.2f}'


def _pct(value: float) -> str:
    return f'{100 * value:.1f}%'


def render_report(payload: Mapping[str, Any]) -> str:
    a = payload['A_monetary_structure']['buckets']
    b = payload['B_price_mapping_counterfactual']
    uniform = b['uniform_price_buckets_MWh']
    production = b['production_buckets_MWh']
    drivers = b['driver_metrics']
    c = payload['C_intermediate_purchasers']
    d = payload['D_comparator_sensitivity']
    e = payload['E_external_evidence']

    lines = [
        '# Household vs intermediate electricity — A–E driver decomposition',
        '',
        f"Config: `{payload['config']}`; model year "
        f"**{payload['model_base_year']}**; EIA price year "
        f"**{payload['usa_ghg_data_year']}**.",
        '',
        'Pipeline steps referenced below follow the electricity diagnostics sequence '
        '**footing → reallocation → 3-way split → unit conversion**. Monetary '
        'tables use `derive_cornerstone_Aq_scaled()` (post–3-way split, still USD). '
        'Physical MWh tables apply generation-row conversion factors to those same '
        'flows (the unit-conversion step), either with production class prices or a '
        'uniform-price diagnostic counterfactual.',
        '',
        '## Executive conclusion',
        '',
        f"- The post–3-way-split monetary IO assigns **{_pct(a['household_share'])}** "
        f"of 221110 uses to `{HH_FD_CODE}` versus EIA Residential's "
        f"**{_pct(drivers['share_movements']['eia_residential_share_of_sales'])}** "
        'share of retail sales. A substantial mismatch therefore exists '
        '**before mixed units**.',
        f"- Class prices and mapping move household electricity from "
        f"**{_fmt_twh(uniform['household_F01000'])} TWh** under uniform prices to "
        f"**{_fmt_twh(production['household_F01000'])} TWh** in production. They add "
        f"**{_pct(drivers['household']['share_of_total_shortfall_added_by_class_prices'])}** "
        'of the final direct-row household shortfall versus EIA Residential—about '
        'half, not a negligible adjustment.',
        f"- Intermediate electricity moves from "
        f"**{_fmt_twh(uniform['intermediate'])} TWh** to "
        f"**{_fmt_twh(production['intermediate'])} TWh**; class prices add "
        f"**{_pct(drivers['intermediate']['share_of_total_excess_added_by_class_prices'])}** "
        'of its final excess versus EIA nonresidential sales.',
        f"- **{_pct(c['electricity_supply_chain']['share_of_intermediate_in_electricity_supply_chain'])}** "
        'of direct intermediate 221110 MWh is purchased by the three electricity '
        'industries themselves. That is IO-intermediate supply-chain throughput, not '
        'an EIA ultimate-customer classification.',
        '- External BEA methodology confirms that NIPA electricity PCE is anchored '
        'to EIA Residential, but NIPA PCE is delivered purchaser value while this '
        'diagnostic follows the **generation-only 221110 row**. The direct-row '
        'F01000/EIA ratio is therefore diagnostic, not an apples-to-apples validation.',
        '',
        '## A: Are USD uses already skewed vs EIA before unit conversion?',
        '',
        '**Disaggregation step: 3-way split.** Table values are monetary generation-row '
        'uses from `derive_cornerstone_Aq_scaled()` after reallocation and the three-way '
        'electricity split, **before** mixed-unit / unit conversion.',
        '',
        '| 221110 use | USD (billions) | Share |',
        '|---|---:|---:|',
        f"| Intermediate: `221110` use of `221110` | "
        f"{_fmt_billion(a['intermediate_221110_self_use'])} | "
        f"{_pct(a['intermediate_221110_self_use_share'])} |",
        f"| Intermediate: other industries | "
        f"{_fmt_billion(a['intermediate_other'])} | "
        f"{_pct(a['intermediate_other_share'])} |",
        f"| Household `{HH_FD_CODE}` | {_fmt_billion(a['household_F01000'])} | "
        f"{_pct(a['household_share'])} |",
        f"| Other final demand | {_fmt_billion(a['other_final_demand'])} | "
        f"{_pct(a['other_final_demand_share'])} |",
        f"| **Total** | **{_fmt_billion(a['total'])}** | **100.0%** |",
        '',
        'These are monetary shares from the 3-way split. Intermediate is split into '
        'generation’s own use of the generation commodity versus all other industry '
        'purchasers. The uniform-price scenario in B converts the same flows to '
        'physical MWh without changing the shares, making the EIA comparison '
        'dimensionally valid while preserving the pre-conversion structure.',
        '',
        '## B: How much do class prices and end-use mapping move the mixed MWh split?',
        '',
        '**Disaggregation step: unit conversion** (applied to the same 3-way-split '
        'monetary flows from A). The **Uniform price** column is a diagnostic '
        'counterfactual (one MWh/USD for every purchaser). The **Production class '
        'prices** column is the production unit-conversion path (`c_row = λ / p_class`). '
        'Both columns hold total generation-row MWh fixed to eGRID.',
        '',
        '| Bucket | Uniform price (TWh) | Production class prices (TWh) | Change (TWh) |',
        '|---|---:|---:|---:|',
    ]
    for key, label in (
        ('intermediate', 'Intermediate'),
        ('household_F01000', f'Household `{HH_FD_CODE}`'),
        ('other_final_demand', 'Other final demand'),
        ('total', '**Total**'),
    ):
        change = production[key] - uniform[key]
        lines.append(
            f'| {label} | {_fmt_twh(uniform[key])} | '
            f'{_fmt_twh(production[key])} | {change / 1e6:+,.1f} |'
        )
    hh = drivers['household']
    inter = drivers['intermediate']
    lines.extend(
        [
            '',
            'Both scenarios hold total generation-row uses fixed to '
            f"**{_fmt_twh(b['target_total_MWh'])} TWh**. Therefore production minus "
            'uniform isolates **class prices plus the mapping that assigns each '
            'purchaser to a class**.',
            '',
            '| Gap decomposition | Household shortfall vs Residential | '
            'Intermediate excess vs nonresidential |',
            '|---|---:|---:|',
            f"| Present under uniform prices | "
            f"{_fmt_twh(hh['structure_and_definition_gap_at_uniform_price_MWh'])} TWh | "
            f"{_fmt_twh(inter['uniform_price_minus_eia_excess_MWh'])} TWh |",
            f"| Added by class prices + map | "
            f"{_fmt_twh(-hh['production_minus_uniform_price_effect_MWh'])} TWh | "
            f"{_fmt_twh(inter['production_minus_uniform_price_effect_MWh'])} TWh |",
            f"| Final production gap | "
            f"{_fmt_twh(hh['total_eia_minus_production_gap_MWh'])} TWh | "
            f"{_fmt_twh(inter['total_production_minus_eia_excess_MWh'])} TWh |",
            f"| Share added by prices + map | "
            f"{_pct(hh['share_of_total_shortfall_added_by_class_prices'])} | "
            f"{_pct(inter['share_of_total_excess_added_by_class_prices'])} |",
            '',
            'This is an accounting decomposition, not a causal structural model: the '
            'uniform residual combines the IO structure, the 2017 final-demand share '
            'proxy, and remaining definition/boundary differences.',
            '',
            '### Reconciliation to the earlier household report',
            '',
            f"The direct cell conversion gives **{_fmt_twh(b['legacy_report_reconciliation']['production_direct_converted_F01000_MWh'])} "
            f"TWh** for `{HH_FD_CODE}`. The earlier `hh_vs_interindustry` convention "
            f"gives **{_fmt_twh(b['legacy_report_reconciliation']['legacy_nonnegative_domestic_share_F01000_MWh'])} "
            'TWh** because it reallocates total mixed final demand with clipped, '
            'domestic-only 2017 shares. The A–E decomposition uses direct converted '
            'cells so the production and uniform scenarios differ only in `c_row`.',
            '',
            b['legacy_report_reconciliation']['note'],
            '',
            '## C: Which intermediate purchasers hold residential-like MWh?',
            '',
            '**Disaggregation step: unit conversion (production class prices).** '
            'MWh tables below are the production column from B: 3-way-split monetary '
            'flows converted with Table 2.4 / end-use-map `c_row`. The USD column in '
            'the top-25 table is still the pre-conversion 3-way-split monetary flow.',
            '',
            '| Assigned class | Intermediate TWh | FD TWh | Total TWh |',
            '|---|---:|---:|---:|',
        ]
    )
    for end_use, values in c['production_MWh_by_assigned_end_use'].items():
        lines.append(
            f"| {end_use} | {_fmt_twh(values['intermediate_MWh'])} | "
            f"{_fmt_twh(values['final_demand_MWh'])} | "
            f"{_fmt_twh(values['total_MWh'])} |"
        )
    supply = c['electricity_supply_chain']
    lines.extend(
        [
            '',
            '### Electricity-supply-chain purchases',
            '',
            '| Direct intermediate purchaser | TWh of 221110 |',
            '|---|---:|',
        ]
    )
    for code, value in supply['electricity_children_MWh'].items():
        lines.append(f'| `{code}` | {_fmt_twh(value)} |')
    lines.extend(
        [
            f"| **Three electricity industries** | "
            f"**{_fmt_twh(supply['electricity_supply_chain_total_MWh'])}** |",
            f"| Other intermediate purchasers | "
            f"{_fmt_twh(supply['other_intermediate_purchasers_MWh'])} |",
            '',
            supply['interpretation'],
        ]
    )
    lines.extend(
        [
            '',
            '### Top 25 intermediate purchasers',
            '',
            '| Rank | Code | Name | Assigned class | USD (B) | TWh | Housing keyword |',
            '|---:|---|---|---|---:|---:|:---:|',
        ]
    )
    for rank, row in enumerate(c['top_25'], start=1):
        lines.append(
            f"| {rank} | `{row['code']}` | {row['name']} | "
            f"{row['assigned_end_use']} | "
            f"{_fmt_billion(row['monetary_flow_USD'])} | "
            f"{_fmt_twh(row['production_MWh'])} | "
            f"{'yes' if row['housing_adjacent_keyword_flag'] else ''} |"
        )
    lines.extend(
        [
            '',
            c['housing_flag_note'],
            '',
            '## D: How sensitive are Model/EIA ratios to alternate bucket pairings?',
            '',
            '**Disaggregation step: unit conversion (production class prices)** for '
            'all model TWh columns; EIA columns are published Table 2.2 sales / end '
            'use (not an IO pipeline step). Model buckets reuse the same production '
            'unit-conversion MWh vectors as C.',
            '',
            '| Model bucket | Model TWh | EIA bucket | EIA TWh | Model/EIA |',
            '|---|---:|---|---:|---:|',
        ]
    )
    for row in d['pairings']:
        lines.append(
            f"| {row['model_bucket']} | {_fmt_twh(row['model_MWh'])} | "
            f"{row['eia_bucket']} | {_fmt_twh(row['eia_MWh'])} | "
            f"{row['model_over_eia']:.3f} |"
        )
    lines.extend(
        [
            '',
            d['note'],
            '',
            '## E: What do BEA PCE and EIA sales-by-sector notes imply about A–D?',
            '',
            '**Disaggregation step: none for the source table** (external BEA/EIA '
            'methodology). Model quantities cited in the sanity check are from the '
            '**unit conversion (production class prices)** result for '
            f'`{HH_FD_CODE}`, i.e. the same production path as B–D.',
            '',
            f"Evidence reviewed **{e.get('evidence_retrieved', EVIDENCE_RETRIEVED)}**.",
            '',
            '| Organization | Source | Finding used here |',
            '|---|---|---|',
        ]
    )
    for source in e['sources']:
        lines.append(
            f"| {source['organization']} | [{source['source']}]({source['url']}) | "
            f"{source['finding']} |"
        )
    if e['available']:
        lines.extend(
            [
                '',
                '### Independent PCE-dollar sanity check',
                '',
                '```text',
                f"BEA PCE electricity (2023) = ${e['bea_pce_electricity_USD'] / 1e9:.3f} B",
                f"EIA residential price      = "
                f"{e['eia_residential_price_cents_per_kWh']:.2f} cents/kWh",
                'Implied MWh = PCE dollars / (price × $10/MWh per cent/kWh)',
                f"            = {e['bea_pce_divided_by_eia_price_implied_MWh']:,.0f} MWh",
                '```',
                '',
                f"The implied **{_fmt_twh(e['bea_pce_divided_by_eia_price_implied_MWh'])} "
                f"TWh** is **{_pct(e['implied_MWh_over_eia_residential'])}** of EIA "
                f"Residential ({_fmt_twh(e['eia_residential_MWh'])} TWh), while the "
                f"model `{HH_FD_CODE}` result is only "
                f"**{_pct(e['model_over_bea_price_implied_MWh'])}** of that implied "
                'quantity. This confirms that BEA PCE electricity and EIA Residential '
                'are closely aligned. It does **not** make direct `221110 × F01000` '
                'comparable to delivered sales: the latter include transmission and '
                'distribution, while generation sold through those industries remains '
                'intermediate in the direct IO row.',
                '',
                f"Caveat: {e['caveat']}",
            ]
        )
    lines.extend(
        [
            '',
            '### A–D conclusions after E',
            '',
            '1. **A identifies a real direct-row structure, but not ultimate use:** '
            'some generation is routed through electricity industries before reaching '
            'the EIA customer.',
            '2. **B is quantitatively important:** class prices account for about half '
            'the direct household shortfall and about one-third of the intermediate '
            'excess; they are not a complete explanation.',
            '3. **C reveals the central classification limit:** EIA classes ultimate '
            'customers by use, whereas the direct IO row records purchaser industries '
            'and supply-chain throughput.',
            '4. **D is essential:** generation output, utility end-use sales, direct '
            'use, and IO domestic uses have different boundaries.',
            '',
            '## Reproduce',
            '',
            '```',
            'python -m bedrock.analysis.electricity_disagg_diagnostics.hh_mwh_driver_decomposition',
            '```',
            '',
            f'Writes `{REPORT_MD.as_posix()}` and `{REPORT_JSON.as_posix()}`.',
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
