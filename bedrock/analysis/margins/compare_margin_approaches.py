"""Compare commodity-level PRO:PUR ratios across margin model approaches and years.

Scenarios (each at ``derive_margins_cornerstone_usa_at_year(year)`` unless noted):

  useeio       — ``useeio_phoebe_23`` with ``useeio_margins`` (Rho PRO inflation)
  cornerstone  — ``2025_usa_cornerstone_full_model`` with industry-avg margins
                   (V-norm commodity PI on PRO)
  ceda         — ``derive_phi_ceda_usa`` mapped to Cornerstone commodities (IO year)

Usage::

    uv run python -m bedrock.analysis.margins.compare_margin_approaches

Outputs:
  output/plots/margin_approach_comparison_<year>.png
  output/margin_approach_comparison.csv
"""

from __future__ import annotations

import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

OUT = os.path.join(os.path.dirname(__file__), 'output')
PLOTS = os.path.join(OUT, 'plots')
os.makedirs(PLOTS, exist_ok=True)

from bedrock.transform.iot.derive_PRO_to_PUR_ratio import (  # noqa: E402
    derive_margins_cornerstone_usa_at_year,
    derive_phi_ceda_usa,
)
from bedrock.utils.config.config_controllers import temp_usa_config  # noqa: E402
from bedrock.utils.config.usa_config import get_usa_config  # noqa: E402
from bedrock.utils.taxonomy.mappings.bea_v2017_sector__cornerstone_commodity import (  # noqa: E402
    load_bea_v2017_sector_commodity_to_cornerstone_commodity,
)
from bedrock.utils.taxonomy.usa_taxonomy_correspondence_helpers import (  # noqa: E402
    load_ceda_v7_commodity__cornerstone_commodity_correspondence,
)

_PANEL_YEAR = 2024
_CORNERSTONE_SCENARIOS = ('useeio', 'cornerstone', 'ceda')
_COLORS = {
    'useeio': 'tab:orange',
    'cornerstone': 'tab:green',
    'ceda': 'tab:blue',
}

_CACHE_MODULES = (
    'bedrock.extract.iot.io_2017',
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio',
)


def _ratio_from_margins(margins: pd.DataFrame) -> pd.Series:
    return (margins["Producers' Value"] / margins["Purchasers' Value"]).replace(
        [np.inf, -np.inf, np.nan], 1.0
    )


def _comparison_years() -> tuple[int, ...]:
    with temp_usa_config('useeio_phoebe_23', cache_bearing_modules=_CACHE_MODULES):
        base_year = get_usa_config().usa_base_io_data_year
    years = [base_year]
    if _PANEL_YEAR not in years:
        years.append(_PANEL_YEAR)
    return tuple(years)


def _ceda_ratios_on_cornerstone() -> pd.Series:
    _ceda_corresp = load_ceda_v7_commodity__cornerstone_commodity_correspondence()
    _ceda_corresp_norm = _ceda_corresp.div(_ceda_corresp.sum(axis=1), axis=0).fillna(0.0)
    with temp_usa_config('v8_ceda_2025_usa', cache_bearing_modules=_CACHE_MODULES):
        ratio_ceda_by_sector = derive_phi_ceda_usa()
    return _ceda_corresp_norm @ ratio_ceda_by_sector


def _ratios_by_scenario(year: int, *, ratio_ceda: pd.Series) -> dict[str, pd.Series]:
    print(f'Computing margin ratios for {year}...')
    with temp_usa_config('useeio_phoebe_23', cache_bearing_modules=_CACHE_MODULES):
        ratio_useeio = _ratio_from_margins(
            derive_margins_cornerstone_usa_at_year(year)
        )
    with temp_usa_config(
        '2025_usa_cornerstone_full_model', cache_bearing_modules=_CACHE_MODULES
    ):
        ratio_cornerstone = _ratio_from_margins(
            derive_margins_cornerstone_usa_at_year(year)
        )
    return {
        'useeio': ratio_useeio,
        'cornerstone': ratio_cornerstone,
        'ceda': ratio_ceda,
    }


def _plot_year(
    long_df: pd.DataFrame, active_sectors: list[str], year: int
) -> str:
    plot_df = long_df[long_df['year'] == year]
    n_scenarios = len(_CORNERSTONE_SCENARIOS)
    n_sectors = len(active_sectors)
    group_width = 0.8
    violin_width = group_width / n_scenarios

    fig, ax = plt.subplots(figsize=(max(10, n_sectors * 2.5), 6))

    for g_idx, sector in enumerate(active_sectors):
        for s_idx, scenario in enumerate(_CORNERSTONE_SCENARIOS):
            data = plot_df.loc[
                (plot_df['sector'] == sector) & (plot_df['scenario'] == scenario),
                'ratio',
            ].dropna()
            if len(data) < 2:
                ax.scatter(
                    [g_idx + (s_idx - (n_scenarios - 1) / 2) * violin_width],
                    data.values if len(data) else [1.0],
                    color=_COLORS[scenario],
                    s=20,
                    zorder=3,
                )
                continue
            pos = g_idx + (s_idx - (n_scenarios - 1) / 2) * violin_width
            parts = ax.violinplot(
                data,
                positions=[pos],
                widths=violin_width * 0.9,
                showmedians=True,
                showextrema=False,
            )
            for pc in parts['bodies']:  # type: ignore[attr-defined]
                pc.set_facecolor(_COLORS[scenario])
                pc.set_alpha(0.6)
            parts['cmedians'].set_color(_COLORS[scenario])
            parts['cmedians'].set_linewidth(1.5)

    ax.axhline(
        1.0, color='black', linewidth=0.8, linestyle='--', alpha=0.5, label='ratio = 1'
    )
    ax.set_xticks(range(n_sectors))
    ax.set_xticklabels(active_sectors, rotation=45, ha='right', fontsize=9)
    ax.set_ylabel('PRO:PUR ratio')
    ax.set_title(
        f'PRO:PUR ratio by BEA sector and margin approach ({year} USD margins)\n'
        '(Cornerstone commodities; CEDA column is IO-year only)'
    )
    ax.grid(True, axis='y', linestyle=':', alpha=0.4)

    legend_handles = [
        plt.Rectangle((0, 0), 1, 1, fc=_COLORS[s], alpha=0.6, label=s)
        for s in _CORNERSTONE_SCENARIOS
    ]
    ax.legend(handles=legend_handles, loc='upper right', fontsize=8)

    fig.tight_layout()
    plot_path = os.path.join(PLOTS, f'margin_approach_comparison_{year}.png')
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    return plot_path


def main() -> None:
    years = _comparison_years()
    ratio_ceda = _ceda_ratios_on_cornerstone()
    ratio_columns: dict[str, pd.Series] = {}
    for year in years:
        for scenario, series in _ratios_by_scenario(year, ratio_ceda=ratio_ceda).items():
            ratio_columns[f'{scenario}_{year}'] = series

    ratio_df = pd.DataFrame(ratio_columns)
    ratio_df.index.name = 'commodity'

    sector_map = load_bea_v2017_sector_commodity_to_cornerstone_commodity()
    rows: list[dict[str, object]] = []
    for year in years:
        for sector, commodities in sector_map.items():
            for commodity in commodities:
                if commodity not in ratio_df.index:
                    continue
                for scenario in _CORNERSTONE_SCENARIOS:
                    col = f'{scenario}_{year}'
                    if col not in ratio_df.columns:
                        continue
                    rows.append(
                        {
                            'year': year,
                            'sector': sector,
                            'commodity': commodity,
                            'scenario': scenario,
                            'ratio': ratio_df.loc[commodity, col],
                        }
                    )
    long_df = pd.DataFrame(rows)

    non_unity = long_df.groupby('sector')['ratio'].apply(
        lambda s: (s - 1.0).abs().max() > 1e-6
    )
    active_sectors = non_unity[non_unity].index.tolist()
    print(f'\n{len(active_sectors)} of {len(sector_map)} sectors have non-unity ratios.')

    for year in years:
        plot_path = _plot_year(long_df, active_sectors, year)
        print(f'Plot saved to: {plot_path}')

    csv_path = os.path.join(OUT, 'margin_approach_comparison.csv')
    ratio_df.to_csv(csv_path)
    print(f'Cornerstone table saved to: {csv_path}')


if __name__ == '__main__':
    main()
