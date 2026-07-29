"""Compare ``nowcast.py``'s ``derive_initial_Y_pur`` against the officially
published 2017 detail Use table (``Use_SUT_Framework_2017_DET.xlsx``).

This is a 2017-only check: that workbook is BEA's benchmark-year detail Use
table, published once, not a per-year series - it exists to validate the
*method* against a known-correct answer for the one year we have ground truth
for, not to validate other nowcasted years directly.

Both sides are purchaser price - the published Use_SUT_Framework workbook's
final-demand section is PUR, same as ``derive_initial_Y_pur``, so no PRO<->PUR
conversion applies here (that's a real dimension elsewhere - see
``bedrock/transform/iot/derive_PRO_to_PUR_ratio.py`` - just not relevant to
this comparison).


"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.extract.flowbyactivity import getFlowByActivity
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.eeio.nowcast import derive_initial_Y_pur
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_final_demand import BEA_2017_FINAL_DEMAND_CODES

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / 'output'
CELLWISE_CSV_PATH = OUTPUT_DIR / 'nowcast_initial_Y_pur_vs_use_sut_framework_2017.csv'
BRIDGE_CSV_PATH = OUTPUT_DIR / 'nowcast_initial_Y_pur_vs_bridges_2017.csv'


def load_use_sut_framework_final_demand_2017() -> pd.DataFrame:
    """
    Final-demand section (commodity x BEA_2017_FINAL_DEMAND_CODE) of the
    officially published 2017 detail Use table, purchaser price, after
    redefinition. USD (source workbook is in million USD).

    Row scope mirrors ``bea_parse``'s "Detail_Use_SUT" handling: only rows
    above the ``'T005'`` ("Total Intermediate") checksum row are true
    commodities - rows at/after it are industries, value-added components, and
    table totals, not commodities.
    """
    df = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    t005_loc = df.index.get_loc('T005')
    assert isinstance(t005_loc, int)
    commodity_rows = df.index[:t005_loc]

    present = [c for c in BEA_2017_FINAL_DEMAND_CODES if c in df.columns]
    missing = [c for c in BEA_2017_FINAL_DEMAND_CODES if c not in df.columns]
    if missing:
        logger.warning(
            'Use_SUT_Framework_2017_DET.xlsx has no column for final-demand '
            'code(s) %s; treating as all-zero.',
            missing,
        )

    fd = df.loc[commodity_rows, present].astype(float) * MILLION_CURRENCY_TO_CURRENCY
    fd = fd.reindex(columns=BEA_2017_FINAL_DEMAND_CODES, fill_value=0.0)
    fd.index.name = 'commodity'
    fd.columns.name = 'final_demand_code'
    return fd


def compare_initial_Y_pur_to_use_sut_framework_2017() -> pd.DataFrame:
    """
    Per-final-demand-code total comparison: ``derive_initial_Y_pur(2017)``
    (purchaser price) vs. the published Use_SUT_Framework_2017_DET.xlsx
    (also purchaser price, but before redefinition).
    """
    ours = derive_initial_Y_pur(2017)
    baseline = load_use_sut_framework_final_demand_2017()

    ours_totals = ours.sum(axis=0)
    baseline_totals = baseline.reindex(columns=ours.columns, fill_value=0.0).sum(axis=0)
    diff = ours_totals - baseline_totals
    pct_diff = (diff / baseline_totals.replace(0, np.nan)) * 100

    return pd.DataFrame(
        {
            'ours_PUR': ours_totals,
            'baseline_PUR_after_redef': baseline_totals,
            'abs_diff': diff,
            'pct_diff': pct_diff,
        }
    )


def cellwise_initial_Y_pur_vs_use_sut_framework_2017() -> pd.DataFrame:
    """
    Long/tidy (one row per commodity x final_demand_code cell) comparison,
    for downstream visualization/analysis (e.g. heatmaps, scatter plots,
    filtering by commodity or code) rather than the column-total summary in
    ``compare_initial_Y_pur_to_use_sut_framework_2017``.

    Columns: ``commodity``, ``final_demand_code``, ``ours_PUR``,
    ``baseline_PUR_after_redef``, ``abs_diff``, ``pct_diff``.
    """
    ours = derive_initial_Y_pur(2017)
    baseline = load_use_sut_framework_final_demand_2017()

    all_commodities = ours.index.union(baseline.index)
    ours = ours.reindex(
        index=all_commodities, columns=BEA_2017_FINAL_DEMAND_CODES, fill_value=0.0
    )
    baseline = baseline.reindex(
        index=all_commodities, columns=BEA_2017_FINAL_DEMAND_CODES, fill_value=0.0
    )

    ours_long = ours.reset_index().melt(
        id_vars='commodity', var_name='final_demand_code', value_name='ours_PUR'
    )
    baseline_long = baseline.reset_index().melt(
        id_vars='commodity',
        var_name='final_demand_code',
        value_name='baseline_PUR_after_redef',
    )
    cellwise = ours_long.merge(baseline_long, on=['commodity', 'final_demand_code'])
    cellwise['abs_diff'] = cellwise['ours_PUR'] - cellwise['baseline_PUR_after_redef']
    cellwise['pct_diff'] = (
        cellwise['abs_diff'] / cellwise['baseline_PUR_after_redef'].replace(0, np.nan)
    ) * 100
    return cellwise


def export_cellwise_comparison(path: Path = CELLWISE_CSV_PATH) -> Path:
    """Write the cell-wise comparison to CSV for a separate analysis/visualization script."""
    path.parent.mkdir(parents=True, exist_ok=True)
    cellwise_initial_Y_pur_vs_use_sut_framework_2017().to_csv(path, index=False)
    logger.info('Wrote cell-wise comparison to %s', path)
    return path


def load_bridge_totals_by_commodity_2017(source: str, year: int = 2017) -> pd.Series:
    """
    A bridge's purchaser-value total per commodity, in USD.

    The bridges are the attribution sources the corresponding final-demand
    column is built from, so they are the intermediate step between the NIPA
    line totals and the Use table: ``ActivityProducedBy`` is the PCE/PEQ
    category, ``ActivityConsumedBy`` the BEA commodity it resolves to.

    :param source: ``'BEA_PCEBridge'`` or ``'BEA_PEQBridge'``
    :param year: source year
    :return: Series indexed by commodity, USD
    """
    fba = getFlowByActivity(source, year)
    purchaser = fba[fba['FlowName'] == "Purchasers' Value"]
    units = set(purchaser['Unit'].dropna().unique())
    if units != {'Million USD'}:
        raise ValueError(f'{source} purchaser rows have unexpected units {units}')
    return (
        purchaser.groupby('ActivityConsumedBy')['FlowAmount'].sum()
        * MILLION_CURRENCY_TO_CURRENCY
    ).rename('bridge_PUR')


#: Final-demand column -> the bridge it is attributed against.
FINAL_DEMAND_BRIDGES = {'F01000': 'BEA_PCEBridge', 'F02E00': 'BEA_PEQBridge'}


def compare_initial_Y_pur_to_bridges_2017() -> pd.DataFrame:
    """
    Three-way per-commodity comparison of ``derive_initial_Y_pur(2017)``, the
    bridge it is attributed against, and the published Use table.

    Splitting the difference this way separates two failures that the
    ours-vs-Use comparison alone conflates:

    - ``ours_minus_bridge`` non-zero means our attribution did not reproduce
      the bridge's own split of a category across commodities.
    - ``bridge_minus_baseline`` non-zero means the bridge and the Use table
      disagree at source, which no amount of attribution work will close.

    Covers only the columns that are built from a bridge
    (``FINAL_DEMAND_BRIDGES``); the government and structures columns are
    attributed against the Use table itself and have no bridge to check.

    Columns: ``final_demand_code``, ``commodity``, ``ours_PUR``,
    ``bridge_PUR``, ``baseline_PUR_after_redef``, the three pairwise
    differences, and percent differences for each.
    """
    ours = derive_initial_Y_pur(2017)
    baseline = load_use_sut_framework_final_demand_2017()

    frames = []
    for code, source in FINAL_DEMAND_BRIDGES.items():
        bridge = load_bridge_totals_by_commodity_2017(source)
        commodities = ours.index.union(baseline.index).union(bridge.index)
        frame = pd.DataFrame(
            {
                'ours_PUR': ours.reindex(commodities).get(code, pd.Series(dtype=float)),
                'bridge_PUR': bridge.reindex(commodities),
                'baseline_PUR_after_redef': baseline.reindex(commodities).get(
                    code, pd.Series(dtype=float)
                ),
            },
            index=commodities,
        ).fillna(0.0)
        frame.index.name = 'commodity'
        frame.insert(0, 'bridge_source', source)
        frame.insert(0, 'final_demand_code', code)
        frames.append(frame.reset_index())

    out = pd.concat(frames, ignore_index=True)
    for left, right in (
        ('ours', 'bridge'),
        ('bridge', 'baseline'),
        ('ours', 'baseline'),
    ):
        lcol = 'baseline_PUR_after_redef' if left == 'baseline' else f'{left}_PUR'
        rcol = 'baseline_PUR_after_redef' if right == 'baseline' else f'{right}_PUR'
        out[f'{left}_minus_{right}'] = out[lcol] - out[rcol]
        out[f'{left}_minus_{right}_pct'] = (
            out[f'{left}_minus_{right}'] / out[rcol].replace(0, np.nan)
        ) * 100
    return out


def export_bridge_comparison(path: Path = BRIDGE_CSV_PATH) -> Path:
    """Write the three-way bridge comparison to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    compare_initial_Y_pur_to_bridges_2017().to_csv(path, index=False)
    logger.info('Wrote bridge comparison to %s', path)
    return path


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    summary = compare_initial_Y_pur_to_use_sut_framework_2017()
    with pd.option_context(
        'display.width', 120, 'display.float_format', '{:,.0f}'.format
    ):
        print(summary)
    print(f'\nGrand total, ours (PUR): {summary["ours_PUR"].sum():,.0f}')
    print(
        f'Grand total, baseline (PUR, after redef): {summary["baseline_PUR_after_redef"].sum():,.0f}'
    )

    out_path = export_cellwise_comparison()
    print(f'\nCell-wise comparison written to {out_path}')

    bridges = compare_initial_Y_pur_to_bridges_2017()
    print('\nAgainst the bridges, by final-demand code (millions):')
    with pd.option_context(
        'display.width', 140, 'display.float_format', '{:,.0f}'.format
    ):
        print(
            bridges.groupby(['final_demand_code', 'bridge_source'])[
                ['ours_PUR', 'bridge_PUR', 'baseline_PUR_after_redef']
            ].sum()
            / MILLION_CURRENCY_TO_CURRENCY
        )
    bridge_path = export_bridge_comparison()
    print(f'\nBridge comparison written to {bridge_path}')


if __name__ == '__main__':
    main()
