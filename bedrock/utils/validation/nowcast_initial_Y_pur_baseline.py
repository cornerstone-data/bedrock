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

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.eeio.nowcast import derive_initial_Y_pur
from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_final_demand import BEA_2017_FINAL_DEMAND_CODES

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent / 'output'
CELLWISE_CSV_PATH = OUTPUT_DIR / 'nowcast_initial_Y_pur_vs_use_sut_framework_2017.csv'


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


if __name__ == '__main__':
    main()
