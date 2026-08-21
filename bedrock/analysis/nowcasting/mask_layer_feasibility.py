"""Measure the Step 5 mask layer against the 2017 detail SUT.

Step 5 wants to hold some cells fixed through the balance -- the ones a source
reports directly.  Two questions have to be answered before that mask can be
designed, and neither is answerable by argument: **how much freedom does each
mask cost**, and **can either candidate engine express it at all**.  This
script answers the first, on the one year where the published answer exists.
The engine question is in [`mask_layer_plan.md`](mask_layer_plan.md).

The metric is **leverage**.  A margin whose free cells hold only a sliver of
its mass has to move that sliver enormously to deliver a small change in its
target::

    leverage = |margin total| / |free mass in the margin|

leverage 1 means the free cells move 1% for a 1% target change; leverage 10
means they move 10%; leverage ``inf`` means the margin cannot move at all and
the mask has made it infeasible.  Leverage is the number that decides whether
a mask is affordable, and it is invisible in a cell count -- freezing the FD
block freezes 2.7% of the Use panel's nonzero *cells* and 39.9% of its
*dollars*.

What the 2017 measurement shows
-------------------------------

**Freezing whole blocks is not affordable.**  The FD block alone is 39.9% of
the Use panel's mass; FD plus value added is 74.2%.  Freeze the FD block and
27 commodity rows lose every degree of freedom on the Use side, with 51 more
above 10x leverage -- 78 of 402 commodities, a fifth of the table.

**But leverage has to be read across both tables, not one.**  The commodity
identity ``T016 == T019`` can close on either side, so a frozen Use row is
only fatal if the Supply row is frozen too.  Of those 27, twenty-six have a
Supply row that absorbs the whole adjustment at a ratio near 1.0.  Exactly one
commodity is genuinely stuck: ``S00900``, whose Use row is 100% final demand
and whose Supply side can absorb 0.9% of it.  ``4200ID`` is empty in every
block and is vacuous rather than stuck.

**Some masks and some targets are the same fact.**  ``F06C00`` and ``F07C00``
carry exactly one nonzero commodity row each; ``F10C00`` and the three
government IP columns carry three or four.  For those, masking the cells and
targeting the column total are the same constraint written twice.  Above four
rows the column total starts adding information the cells do not.

**A negative column target is real, not hypothetical.**  ``F03000`` is -37,568
in 2020 and swings 1,248% year over year -- the largest move of any final
demand column by an order of magnitude.

Detail gross output, and the valuation that goes with it
--------------------------------------------------------

``BEA_Detail_GrossOutput_IO_<year>`` is already extracted for 2017-2024, all
402 detail industries, so detail gross output is **observed rather than
nowcast** for the Phase 1 years.  It is published at **producer** prices while
the SUT column identity is at **basic** prices, and the wedge is exact::

    GO(producer) = T007(basic) + T00TOP - T00SUB     per industry

which holds to a maximum of $4 million per industry on a $34 trillion total.
Ignoring it puts 86 industries more than 1% off and the economy total 695,632
high.  See ``gross_output_valuation``.
"""

import argparse
import glob
import logging

import numpy as np
import pandas as pd

from bedrock.extract.iot.io_2017 import (
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_sut,
)
from bedrock.transform.iot.nowcast_mask import (
    BLOCKS,
    balance_industries,
    build_sut_masks,
    published_2017_panel,
)
from bedrock.transform.iot.nowcast_sut_gras import engine
from bedrock.transform.iot.nowcast_targets import (
    build_target_set,
    hard_target_residuals,
)
from bedrock.utils.economic.balance import (
    SutMask,
    offset_targets,
    restore_fixed_blocks,
    split_fixed_blocks,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

logger = logging.getLogger(__name__)

COMMODITIES = list(USA_2017_COMMODITY_CODES)
INDUSTRIES = list(USA_2017_INDUSTRY_CODES)
FINAL_DEMAND = list(SUT_FINAL_DEMAND_CODES)

# The three rows that sum to VABAS.  T00TOP and T00SUB are *not* part of the
# industry-output identity -- they are the wedge from VABAS to VAPRO, i.e.
# from basic to producer prices.  Measured: T018 == T005 + VABAS to $1M.
VA_BASIC_ROWS = ['V00100', 'T00OTOP', 'V00300']
VA_PRODUCT_TAX_ROWS = ['T00TOP', 'T00SUB']
VA_ROWS = VA_BASIC_ROWS + VA_PRODUCT_TAX_ROWS

# Supply columns between basic (T013) and purchaser (T016) value.  The
# trailing space on TRADE is BEA's, in the published workbook.
SUPPLY_TRAILING = ['MCIF', 'MADJ', 'TRADE ', 'TRANS', 'MDTY', 'TOP', 'SUB']

# Final demand columns whose NIPA source line lands on one commodity, or close
# to it.  These are the only cells the FD block reports *directly*; every other
# column reaches its commodities through a 2017 bridge or share.
ONE_TO_ONE_FD = ['F06C00', 'F07C00', 'F10C00', 'F06N00', 'F07N00', 'F10N00']

# Commodities to hold out of the balance rather than mask.  S00900 is derived
# from an identity (-F010 + Supply T016) and has almost no Supply-side freedom;
# 4200ID is empty in every block.
EXCLUDE_FROM_BALANCE = ['S00900', '4200ID']

GROSS_OUTPUT_GLOB = (
    'bedrock/extract/output_data/BEA_Detail_GrossOutput_IO_{year}_*.parquet'
)


def _use_panel() -> tuple[pd.DataFrame, np.ndarray]:
    """The Use panel the balancer sees: commodities + VA rows by industries +
    FD columns, and its structural-zero pattern."""
    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    panel = pd.DataFrame(
        0.0,
        index=COMMODITIES + VA_ROWS,
        columns=INDUSTRIES + FINAL_DEMAND,
    )
    panel.loc[COMMODITIES, INDUSTRIES] = use.loc[COMMODITIES, INDUSTRIES].astype(float)
    panel.loc[COMMODITIES, FINAL_DEMAND] = use.loc[COMMODITIES, FINAL_DEMAND].astype(
        float
    )
    # VA x FD stays zero: structurally empty, and it must stay that way.
    panel.loc[VA_ROWS, INDUSTRIES] = use.loc[VA_ROWS, INDUSTRIES].astype(float)
    return panel, (panel.to_numpy() != 0)


def _leverage(total_mass: np.ndarray, free_mass: np.ndarray) -> np.ndarray:
    """|total| / |free|, with an empty margin scored 1.0 rather than inf --
    a margin with nothing in it is not constrained by the mask.

    Argument order matches ``utils.economic.balance.feasibility.leverage`` and
    the formula it computes -- total over free.  The two used to take the same
    two arrays in opposite orders, which is a silent wrong answer when porting
    a check from one to the other."""
    with np.errstate(divide='ignore', invalid='ignore'):
        lev = np.where(free_mass > 0, total_mass / free_mass, np.inf)
    return np.where(total_mass == 0, 1.0, lev)


def mask_scenarios(pattern: np.ndarray) -> dict[str, np.ndarray]:
    """Candidate masks, as boolean arrays over the Use panel."""
    n_c, n_i = len(COMMODITIES), len(INDUSTRIES)
    n_va = len(VA_ROWS)
    rows_c = slice(0, n_c)
    rows_va = slice(n_c, n_c + n_va)
    cols_i = slice(0, n_i)

    def empty() -> np.ndarray:
        return np.zeros_like(pattern)

    one_to_one = empty()
    for code in ONE_TO_ONE_FD:
        one_to_one[rows_c, n_i + FINAL_DEMAND.index(code)] = True

    whole_fd = empty()
    whole_fd[rows_c, n_i:] = True

    fd_and_va = whole_fd.copy()
    fd_and_va[rows_va, cols_i] = True

    return {
        'S0 structural zeros only': empty(),
        'S1 + 1:1 NIPA->commodity FD cells': one_to_one,
        'S2 + whole FD block': whole_fd,
        'S3 + whole FD block + VA block': fd_and_va,
    }


def leverage_table(panel: pd.DataFrame, pattern: np.ndarray) -> pd.DataFrame:
    """One row per candidate mask: what it freezes and what it costs."""
    values = np.abs(panel.to_numpy())
    n_c = len(COMMODITIES)
    rows = []
    for name, frozen in mask_scenarios(pattern).items():
        free = pattern & ~frozen
        free_mass = np.where(free, values, 0.0)
        row_lev = _leverage(values.sum(axis=1), free_mass.sum(axis=1))[:n_c]
        col_lev = _leverage(values.sum(axis=0), free_mass.sum(axis=0))
        rows.append(
            {
                'mask': name,
                'frozen_cells': int((pattern & frozen).sum()),
                'frozen_%_mass': 100 * values[pattern & frozen].sum() / values.sum(),
                'rows_immovable': int(np.isinf(row_lev).sum()),
                'rows_lev_gt_10': int((row_lev > 10).sum()),
                'cols_immovable': int(np.isinf(col_lev).sum()),
                'median_row_lev': float(np.median(row_lev[np.isfinite(row_lev)])),
            }
        )
    return pd.DataFrame(rows).set_index('mask')


def joint_freedom(panel: pd.DataFrame) -> pd.DataFrame:
    """Per commodity, freedom on the Use side and the Supply side together,
    with the whole FD block frozen.  The commodity identity can close on
    either side, so only a commodity frozen on *both* is infeasible."""
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    n_i = len(INDUSTRIES)
    values = np.abs(panel.to_numpy()[: len(COMMODITIES)])
    use_free = values[:, :n_i].sum(axis=1)  # FD frozen: intermediate only
    use_total = values.sum(axis=1)
    supply_free = np.abs(
        supply.loc[COMMODITIES, INDUSTRIES].astype(float).to_numpy()
    ).sum(axis=1) + np.abs(
        supply.loc[COMMODITIES, SUPPLY_TRAILING].astype(float).to_numpy()
    ).sum(
        axis=1
    )
    out = pd.DataFrame(
        {
            'use_total': use_total,
            'use_free': use_free,
            'supply_free': supply_free,
        },
        index=COMMODITIES,
    )
    out['joint_free_share'] = (out.use_free + out.supply_free) / (
        out.use_total + out.supply_free
    ).replace(0, np.nan)
    out['supply_cover'] = out.supply_free / out.use_total.replace(0, np.nan)
    return out


def fd_column_totals() -> pd.DataFrame:
    """Final demand column totals 2017-2024 from the summary SUT.  The point
    of interest is which targets go negative and which move furthest."""
    non_commodity = {
        'T001',
        'T005',
        'V001',
        'T00OTOP',
        'T00OSUB',
        'V003',
        'VABAS',
        'T018',
        'T00TOP',
        'T00SUB',
        'VAPRO',
    }
    summary_codes = [code[:4] for code in FINAL_DEMAND]
    totals = {}
    for year in range(2017, 2025):
        table = _load_usa_summary_sut('Use_SUT_summary', year)  # type: ignore[arg-type]
        cols = [c for c in summary_codes if c in table.columns]
        rows = [
            r
            for r in table.index
            if isinstance(r, str)
            and r not in non_commodity
            and not r.startswith('Note')
        ]
        totals[year] = table.loc[rows, cols].apply(pd.to_numeric, errors='coerce').sum()
    return pd.DataFrame(totals)


def gross_output_valuation() -> pd.DataFrame:
    """Published detail gross output against the Supply table, per industry.

    Returns the observed wedge and the wedge the Use table's product-tax rows
    predict.  They agree to $4M per industry, which is what makes the
    conversion usable as a Step 5 constraint.
    """
    supply = _load_2017_detail_supply_use_usa('Supply_detail')
    use = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    paths = sorted(glob.glob(GROSS_OUTPUT_GLOB.format(year=2017)))
    if not paths:
        raise FileNotFoundError(GROSS_OUTPUT_GLOB.format(year=2017))
    published = (
        pd.read_parquet(paths[0])
        .set_index('ActivityProducedBy')['FlowAmount']
        .reindex(INDUSTRIES)
    )
    basic = supply.loc[COMMODITIES, INDUSTRIES].astype(float).sum(axis=0)
    top = use.loc['T00TOP'].reindex(INDUSTRIES).astype(float)
    sub = use.loc['T00SUB'].reindex(INDUSTRIES).astype(float)
    return pd.DataFrame(
        {
            'published_producer': published,
            'supply_basic': basic,
            'wedge_observed': published - basic,
            'wedge_predicted': top - sub,
            'residual': (published - basic) - (top - sub),
        }
    )


def report() -> str:
    panel, pattern = _use_panel()
    lines: list[str] = []

    lines += [
        'MASK LEVERAGE — Use panel, 407 rows x 421 cols, 2017 detail',
        '',
        leverage_table(panel, pattern).round(2).to_string(),
        '',
        '  frozen_%_mass is the number that matters, not frozen_cells: the FD',
        '  block is 2.7% of the 47,087 nonzero cells and 39.9% of the dollars.',
        '',
    ]

    joint = joint_freedom(panel)
    stuck = joint[(joint.use_free == 0) & (joint.use_total > 0)]
    hard = joint[joint.joint_free_share < 0.10]
    lines += [
        'JOINT FREEDOM — the identity can close on either side',
        '',
        f'  commodity rows immovable on the Use side : {len(stuck)}',
        f'  of those, Supply covers the whole row 1:1: '
        f'{int((stuck.supply_cover >= 0.99).sum())}',
        f'  genuinely stuck (joint free share < 10%) : {sorted(hard.index.tolist())}',
        '',
        stuck[['use_total', 'supply_free', 'supply_cover']]
        .sort_values('use_total', ascending=False)
        .head(12)
        .round(2)
        .to_string(),
        '',
        '  Read this as: freezing final demand does not break the balance, it',
        '  *relocates* it onto the Supply table for a fifth of commodities.',
        '  That is a modelling choice, and it should be a deliberate one.',
        '',
    ]

    totals = fd_column_totals()
    negative = totals[(totals < 0).any(axis=1)]
    # Transposed rather than ``pct_change(axis=1)``: same year-over-year
    # change per FD code, but pandas-stubs does not carry the ``axis`` kwarg.
    moves = (totals.T.pct_change().T.abs().max(axis=1) * 100).sort_values(
        ascending=False
    )
    lines += [
        'FD COLUMN TARGETS 2017-2024 — sign and movement',
        '',
        (
            negative.round(0).to_string()
            if len(negative)
            else '  no column goes negative'
        ),
        '',
        '  largest year-over-year move, % :',
        moves.head(5).round(1).to_string(),
        '',
        "  A negative column target is not hypothetical. ceda's engine does",
        '  np.maximum(col_targets, 0.0) on the way in, which destroys F03000',
        '  in 2020 silently rather than raising.',
        '',
    ]

    valuation = gross_output_valuation()
    lines += [
        'DETAIL GROSS OUTPUT — observed, but at producer prices',
        '',
        f'  published (producer) total : {valuation.published_producer.sum():>14,.0f}',
        f'  Supply table (basic) total : {valuation.supply_basic.sum():>14,.0f}',
        f'  wedge, observed            : {valuation.wedge_observed.sum():>14,.0f}',
        f'  wedge, T00TOP - T00SUB     : {valuation.wedge_predicted.sum():>14,.0f}',
        f'  max |residual| per industry: {valuation.residual.abs().max():>14,.0f}',
        '',
        '  GO(producer) = T007(basic) + T00TOP - T00SUB, exactly. Imposing',
        '  published gross output on the SUT column without the conversion',
        '  puts 86 industries more than 1% off.',
    ]
    return '\n'.join(lines)


def _checks() -> list[tuple[str, bool, str]]:
    """Invariants this analysis rests on. A regression here means a finding in
    the docstring above has stopped being true."""
    panel, pattern = _use_panel()
    table = leverage_table(panel, pattern)
    joint = joint_freedom(panel)
    valuation = gross_output_valuation()
    totals = fd_column_totals()

    fd_mass = float(
        pd.to_numeric(table.loc['S2 + whole FD block', 'frozen_%_mass'], errors='raise')
    )
    hard = joint[joint.joint_free_share < 0.10].index.tolist()
    return [
        (
            'freezing the FD block freezes ~40% of the Use panel',
            bool(39.0 < fd_mass < 41.0),
            f'{fd_mass:.1f}%',
        ),
        (
            'S00900 is the only commodity stuck on both sides',
            hard == ['S00900'],
            f'{hard}',
        ),
        (
            'F03000 has a negative column total in the window',
            bool((totals.loc['F030'] < 0).any()),
            f'min {totals.loc["F030"].min():,.0f}',
        ),
        (
            'GO(producer) - T007(basic) == T00TOP - T00SUB per industry',
            bool(valuation.residual.abs().max() <= 10),
            f'max residual {valuation.residual.abs().max():,.0f}',
        ),
        *_hard_target_checks(),
    ]


def _hard_target_checks() -> list[tuple[str, bool, str]]:
    """Every hard target in the production set, against the published tables.

    These are the numbers that justify the identity *definitions* rather than
    the mask, so they belong here with the rest of the 2017 evidence.  A
    definition error moves one of them and nothing else in the pipeline would
    say so -- this is how ``T12`` was caught being written as a sum-to-zero
    when the balance's sign convention makes it a difference, wrong by exactly
    ``2 x 59,876``.

    Tolerance is 100 rather than 0: BEA publishes no cell below 1 million, so
    every one of these carries publication rounding.  The worst on 2017 is 21.
    """
    residuals = hard_target_residuals(2017)
    return [
        (
            f'{name} holds on the published 2017 tables',
            bool(row.max_abs_residual <= 100),
            f'{int(row.margins)} margins, max {row.max_abs_residual:,.0f}',
        )
        for name, row in residuals.iterrows()
    ]


def _engine_hard_residuals(
    year: int = 2017,
) -> tuple[pd.DataFrame, str]:
    """Hard |evaluate(X) - pre-offset values| after engine + restore.

    Optional 2017 replay for ``--check-engine``. Not a unit test.
    T1 is UGO305-A when the extract parquet is present; otherwise Use
    industry column sums (weaker — not the sourced series).
    """
    seeds: dict[str, pd.DataFrame] = {
        block: published_2017_panel(block) for block in BLOCKS
    }
    masks: dict[str, SutMask] = {
        str(name): mask for name, mask in build_sut_masks(year).items()
    }
    try:
        targets = build_target_set(year)
        t1_source = 'UGO305-A extract parquet'
    except FileNotFoundError:
        go = seeds['use'][list(balance_industries())].sum()
        go.index.name = 'industry'
        targets = build_target_set(year, gross_output=go)
        t1_source = 'Use industry column sums (no UGO305-A parquet)'
    original = {t.name: t.values.copy() for t in targets if t.hard}
    frozen, free = split_fixed_blocks(seeds, masks)
    residual = offset_targets(targets, frozen)
    out = engine(free, residual, masks)
    restored = restore_fixed_blocks(out.blocks, frozen)
    rows = []
    for target in targets:
        if not target.hard:
            continue
        err = (target.evaluate(restored) - original[target.name]).abs()
        rows.append(
            {
                'target': target.name,
                'max_abs_residual': float(err.max()),
            }
        )
    return pd.DataFrame(rows).set_index('target'), t1_source


def main(check: bool = False, check_engine: bool = False) -> int:
    # --check-engine is independent of --check and of the GO parquet
    # report() needs. Print the leverage report on the default path and
    # on --check; skip it when only the engine replay is requested.
    if check or not check_engine:
        print(report())
    failed = 0
    if check:
        print('\nCHECKS')
        for name, passed, detail in _checks():
            print(f'  {"PASS" if passed else "FAIL"}  {name}  ({detail})')
            failed += not passed
    if check_engine:
        print('\nENGINE')
        table, t1_source = _engine_hard_residuals()
        print(f'  T1 source: {t1_source}')
        print(table.to_string())
        worst = float(table['max_abs_residual'].max())
        ok = worst <= 100.0
        print(
            f'  {"PASS" if ok else "FAIL"}  hard residuals after engine+restore  '
            f'(max {worst:.4g}, need <= 100)'
        )
        failed += not ok
    if check or check_engine:
        return 1 if failed else 0
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='exit non-zero if one of the documented findings has regressed',
    )
    parser.add_argument(
        '--check-engine',
        action='store_true',
        help='run engine+restore on published 2017; hard residuals must be <= 100',
    )
    raise SystemExit(main(**vars(parser.parse_args())))
