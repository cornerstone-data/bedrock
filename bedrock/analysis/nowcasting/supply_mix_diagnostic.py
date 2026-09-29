"""How a new nowcast build's Supply moves the commodity emission factors.

Two builds of the after-redefinition Make, year by year, compared on three
things:

- **product mix** - each industry's split of its output across commodities,
  ``V / x`` by row. Moved by half the L1 distance between the two builds, in
  percentage points: the share of the industry's output that changed product.
- **market shares** - each commodity's split across the industries that make
  it, ``V / q`` by column. Same measure. This is the one the model uses:
  ``B_commodity = (E / x) @ Vnorm``, so a commodity's direct factor is the
  share-weighted mean of its makers' intensities.
- **the EF effect** - how far the commodity factor moves, split exactly into

  ``mix``       ``b_base @ (Vnorm_new - Vnorm_base)`` - the market shares alone,
                with every industry's intensity held at the base build's;
  ``intensity`` ``(b_new - b_base) @ Vnorm_new`` - the industries' own output
                moving under a fixed ``E``.

  The two sum to the whole move, which ``--check`` asserts.

``E`` is one GHG FBS vintage for both builds, so every difference here is the
tables. ``Vnorm`` is the scrap-corrected one the production path uses.

Direct factors only. Market shares also enter ``A`` through ``Vnorm``, and
what that does to ``N`` is the L flux ranking's job
(:mod:`bedrock.analysis.nowcasting.L_flux_priority`).

::

    uv run python -m bedrock.analysis.nowcasting.supply_mix_diagnostic
    uv run python -m bedrock.analysis.nowcasting.supply_mix_diagnostic --check
    uv run python -m bedrock.analysis.nowcasting.supply_mix_diagnostic \\
        --base-mut v0.3.0_4276083 --new-mut v0.3.0_92b7a8a --years 2023 2024
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from bedrock.analysis.time_series_B_matrix.B_change_diagnostics import (
    CACHE_BEARING_MODULES,
    CONFIG_TEMPLATE,
    YEARS,
    stratified_E,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_V,
    derive_cornerstone_Vnorm_scrap_corrected,
)
from bedrock.utils.config.config_controllers import temp_usa_config
from bedrock.utils.taxonomy.cornerstone.industries import INDUSTRY_DESC

logger = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent / 'output' / 'supply_mix'

#: v0.4's MUT and the FBS both releases are compared on.
BASE_MUT = 'v0.3.0_4276083'
NEW_MUT = 'v0.3.0_92b7a8a'
FBS = 'v0.3.0_99655e9'

#: A share moved by more than this (0.1pp) is counted as moved.
MOVED = 0.001

_NAME: dict[str, str] = {str(k): str(v) for k, v in INDUSTRY_DESC.items()}


@dataclass(frozen=True)
class Build:
    """One year of one build: its Make and the factor inputs derived from it."""

    V: pd.DataFrame  # industry x commodity, USD
    Vnorm: pd.DataFrame  # scrap-corrected market shares, industry x commodity
    b: pd.Series  # CO2e per dollar of industry output, under the shared E


def load_build(year: int, mut: str, E_sector: pd.Series) -> Build:
    with temp_usa_config(
        CONFIG_TEMPLATE.format(year=year),
        cache_bearing_modules=CACHE_BEARING_MODULES,
        nowcast_mut_vintage=mut,
    ):
        V = derive_cornerstone_V()
        Vnorm = derive_cornerstone_Vnorm_scrap_corrected()
    x = V.sum(axis=1)
    b = (
        (E_sector.reindex(V.index).fillna(0.0) / x)
        .replace([np.inf, -np.inf], 0.0)
        .fillna(0.0)
    )
    return Build(V=V, Vnorm=Vnorm, b=b)


def half_l1(a: pd.DataFrame, b: pd.DataFrame, axis: Literal[0, 1]) -> pd.Series:
    """Half the L1 distance between two share matrices, along *axis*."""
    return 0.5 * (a - b).abs().sum(axis=axis)


def shares(V: pd.DataFrame, axis: Literal[0, 1]) -> pd.DataFrame:
    """Row shares (axis=1, product mix) or column shares (axis=0, market shares)."""
    total = V.sum(axis=axis)
    other: Literal[0, 1] = 0 if axis == 1 else 1
    return V.div(total.where(total != 0), axis=other).fillna(0.0)


def compare_year(base: Build, new: Build) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Commodity and industry tables for one year."""
    V_new = new.V.reindex(index=base.V.index, columns=base.V.columns).fillna(0.0)
    Vn_new = new.Vnorm.reindex(index=base.Vnorm.index, columns=base.Vnorm.columns)
    Vn_new = Vn_new.fillna(0.0)
    b_new = new.b.reindex(base.b.index).fillna(0.0)

    ms_moved = half_l1(shares(base.V, 0), shares(V_new, 0), axis=0)
    pm_moved = half_l1(shares(base.V, 1), shares(V_new, 1), axis=1)

    d_base = base.b @ base.Vnorm
    d_new = b_new @ Vn_new
    mix = base.b @ (Vn_new - base.Vnorm)
    intensity = (b_new - base.b) @ Vn_new

    # the industry whose share of each commodity rose most, and fell most
    dshare = shares(V_new, 0) - shares(base.V, 0)
    q_base, q_new = base.V.sum(axis=0), V_new.sum(axis=0)
    denom = d_base.where(d_base.abs() > 0)
    commodities = pd.DataFrame(
        {
            'name': [_NAME.get(str(c), str(c)) for c in base.V.columns],
            'q_base': q_base,
            'q_new': q_new,
            'market_share_moved_pp': 100 * ms_moved,
            'top_gainer': dshare.idxmax(axis=0),
            'top_gainer_pp': 100 * dshare.max(axis=0),
            'top_loser': dshare.idxmin(axis=0),
            'top_loser_pp': 100 * dshare.min(axis=0),
            'B_base': d_base,
            'B_new': d_new,
            'pct_change_B': 100 * (d_new - d_base) / denom,
            'pct_mix_effect': 100 * mix / denom,
            'pct_intensity_effect': 100 * intensity / denom,
        }
    ).rename_axis('commodity')
    x_base, x_new = base.V.sum(axis=1), V_new.sum(axis=1)
    industries = pd.DataFrame(
        {
            'name': [_NAME.get(str(i), str(i)) for i in base.V.index],
            'x_base': x_base,
            'x_new': x_new,
            'pct_change_x': 100 * (x_new / x_base.where(x_base != 0) - 1),
            'product_mix_moved_pp': 100 * pm_moved,
        }
    ).rename_axis('industry')
    return commodities, industries


def summarize(year: int, com: pd.DataFrame, ind: pd.DataFrame) -> dict[str, object]:
    wq = com['q_base'] / com['q_base'].sum()
    wx = ind['x_base'] / ind['x_base'].sum()
    mix = com['pct_mix_effect'].fillna(0.0)
    return {
        'year': year,
        'industries_mix_moved': int((ind['product_mix_moved_pp'] > 100 * MOVED).sum()),
        'product_mix_moved_wmean_pp': float((ind['product_mix_moved_pp'] * wx).sum()),
        'commodities_share_moved': int(
            (com['market_share_moved_pp'] > 100 * MOVED).sum()
        ),
        'market_share_moved_wmean_pp': float((com['market_share_moved_pp'] * wq).sum()),
        'market_share_moved_max_pp': float(com['market_share_moved_pp'].max()),
        'market_share_moved_max_at': str(com['market_share_moved_pp'].idxmax()),
        'mix_effect_median_abs_pct': float(mix.abs().median()),
        'mix_effect_wmean_abs_pct': float((mix.abs() * wq).sum()),
        'commodities_mix_effect_over_1pct': int((mix.abs() > 1).sum()),
        'commodities_mix_effect_over_5pct': int((mix.abs() > 5).sum()),
    }


def run(
    years: tuple[int, ...] = YEARS,
    base_mut: str = BASE_MUT,
    new_mut: str = NEW_MUT,
    fbs: str = FBS,
    check: bool = False,
) -> dict[str, pd.DataFrame]:
    commodity_parts, industry_parts, summary = [], [], []
    for year in years:
        E = stratified_E(year, fbs).groupby('sector')['CO2e'].sum()
        base = load_build(year, base_mut, E)
        new = load_build(year, new_mut, E)
        com, ind = compare_year(base, new)
        if check:
            _check(year, base, new, com)
        commodity_parts.append(com.assign(year=year))
        industry_parts.append(ind.assign(year=year))
        summary.append(summarize(year, com, ind))
        logger.info('%d: %s', year, summary[-1])

    tables = {
        'supply_mix_summary': pd.DataFrame(summary).set_index('year'),
        'supply_mix_by_commodity': pd.concat(commodity_parts),
        'supply_mix_by_industry': pd.concat(industry_parts),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, table in tables.items():
        table.to_csv(OUT_DIR / f'{name}_{base_mut}_vs_{new_mut}.csv')
    _report(tables)
    return tables


def _check(year: int, base: Build, new: Build, com: pd.DataFrame) -> None:
    """The identities the tables rest on."""
    for label, build in (('base', base), ('new', new)):
        col = shares(build.V, 0).sum(axis=0)
        made = build.V.sum(axis=0) != 0
        worst = float((col[made] - 1).abs().max())
        assert worst < 1e-9, f'{year} {label}: market shares do not sum to 1 ({worst})'
    # mix + intensity partition the factor move exactly
    gap = (
        com['B_new']
        - com['B_base']
        - (com['pct_mix_effect'] + com['pct_intensity_effect']) / 100 * com['B_base']
    )
    scale = com['B_base'].abs().max()
    worst = float(gap[com['B_base'].abs() > 0].abs().max() / scale)
    assert worst < 1e-9, f'{year}: mix + intensity != the factor move ({worst:.1e})'
    logger.info('OK  %d: shares sum to 1; mix + intensity partition dB', year)


def _report(tables: dict[str, pd.DataFrame]) -> None:
    logger.info('Summary:\n%s', tables['supply_mix_summary'].round(3).to_string())
    com = tables['supply_mix_by_commodity']
    wq = com['q_base'] / com.groupby('year')['q_base'].transform('sum')
    top = com.assign(weighted=com['pct_mix_effect'].abs() * wq)
    top = top.sort_values('weighted', ascending=False).head(20)
    logger.info(
        'Largest output-weighted mix effects on B:\n%s',
        top[
            [
                'year',
                'name',
                'market_share_moved_pp',
                'top_gainer',
                'top_gainer_pp',
                'top_loser',
                'top_loser_pp',
                'pct_mix_effect',
                'pct_intensity_effect',
            ]
        ]
        .round(2)
        .to_string(),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(message)s')
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--base-mut', default=BASE_MUT)
    parser.add_argument('--new-mut', default=NEW_MUT)
    parser.add_argument('--fbs', default=FBS)
    parser.add_argument('--years', type=int, nargs='+', default=list(YEARS))
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    run(
        years=tuple(args.years),
        base_mut=args.base_mut,
        new_mut=args.new_mut,
        fbs=args.fbs,
        check=args.check,
    )
