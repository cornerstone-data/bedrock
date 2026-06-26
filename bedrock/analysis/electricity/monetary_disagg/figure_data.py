"""Production pipeline matrices for monetary disagg report figures."""

from __future__ import annotations

import pandas as pd

from bedrock.analysis.electricity.disaggregation_matrices import (
    derive_post_reallocation_checkpoint,
)
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    derive_disagg_io_bundle,
    derive_disagg_Ytot_with_trade,
)
from bedrock.transform.eeio.electricity_disaggregation import (
    ELECTRICITY_AGGREGATE,
    _frame_cell_float,
    build_electricity_disagg_go_weights,
    build_electricity_disagg_use_intersection_weights,
    get_electricity_commodity_row_weights,
)
from bedrock.utils.math.formulas import compute_q, compute_x
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS

ELEC: list[str] = list(ELECTRICITY_DISAGG_SECTORS)
AGG = ELECTRICITY_AGGREGATE

SECTOR_SHORT: dict[str, str] = {
    '221110': 'Generation',
    '221121': 'Transmission',
    '221122': 'Distribution',
}

FUEL_COMMODITIES: tuple[str, ...] = (
    '212100',
    '211000',
    '324110',
    '424700',
    '221200',
)


def production_checkpoint() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    """Post-reallocation V/U/VA/Y before PR3 steps (stage 2)."""
    v, udom, uimp, va, y = derive_post_reallocation_checkpoint()
    return v, udom, uimp, va, y


def production_disaggregated() -> (
    tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]
):
    """Post-PR3.1 V/U/VA/Y from production pipeline."""
    bundle = derive_disagg_io_bundle()
    y = derive_disagg_Ytot_with_trade()
    return bundle.V, bundle.Udom, bundle.Uimp, bundle.VA, y


def make_intersection_block(v: pd.DataFrame) -> pd.DataFrame:
    """3×3 Make diagonal block (industry × commodity)."""
    return v.loc[ELEC, ELEC].astype(float)


def use_intersection_block(udom: pd.DataFrame, uimp: pd.DataFrame) -> pd.DataFrame:
    """3×3 Use intersection (dom + imp)."""
    block = udom.loc[ELEC, ELEC].astype(float) + uimp.loc[ELEC, ELEC].astype(float)
    return block


def industry_column_block(
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    *,
    rows: list[str] | None = None,
) -> pd.DataFrame:
    """Purchaser commodity rows × three electricity industry columns."""
    u = (udom + uimp).astype(float)
    if rows is None:
        rows = list(FUEL_COMMODITIES) + ['541000', '518200', '233400']
    present = [r for r in rows if r in u.index]
    return u.loc[present, ELEC]


def commodity_row_block(
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    y: pd.DataFrame,
    *,
    purchaser_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Three electricity commodity rows × selected purchaser columns (U + Y)."""
    if purchaser_cols is None:
        purchaser_cols = ['212100', '541000', '233400', 'F01000', 'F02000', 'F05000']
    u = (udom + uimp).astype(float)
    cols = [c for c in purchaser_cols if c in u.columns or c in y.columns]
    out = pd.DataFrame(index=ELEC, columns=cols, dtype=float)
    for col in cols:
        for code in ELEC:
            u_val = (
                _frame_cell_float(u, code, col)
                if code in u.index and col in u.columns
                else 0.0
            )
            y_val = (
                _frame_cell_float(y, code, col)
                if code in y.index and col in y.columns
                else 0.0
            )
            out.at[code, col] = u_val + y_val
    return out


def weight_summary() -> pd.DataFrame:
    """Production weight vectors used at each step."""
    w_go = build_electricity_disagg_go_weights()
    w_int = build_electricity_disagg_use_intersection_weights()
    w_row = get_electricity_commodity_row_weights()
    return pd.DataFrame(
        {
            'w_go (steps 1 & 3)': w_go,
            'w_int (step 2)': w_int,
            'w_row (step 4 & Y)': w_row,
        }
    )


def balance_summary() -> pd.DataFrame:
    """Aggregate preservation metrics before vs after production disaggregation."""
    v_pre, udom_pre, uimp_pre, va_pre, y_pre_df = production_checkpoint()
    v_post, udom_post, uimp_post, va_post, y_post_df = production_disaggregated()

    x_pre = float(compute_x(V=v_pre)[AGG])
    x_post = sum(float(compute_x(V=v_post)[c]) for c in ELEC)
    q_pre = float(compute_q(V=v_pre)[AGG])
    q_post = sum(float(compute_q(V=v_post)[c]) for c in ELEC)

    use_pre = float(udom_pre.loc[AGG].sum()) + float(uimp_pre.loc[AGG].sum())
    use_post = sum(
        float(udom_post.loc[c].sum()) + float(uimp_post.loc[c].sum()) for c in ELEC
    )
    y_pre = float(y_pre_df.loc[AGG].sum()) if AGG in y_pre_df.index else 0.0
    y_post_sum = float(y_post_df.loc[ELEC].sum().sum())

    va_pre_sum = float(va_pre[AGG].sum())
    va_post_sum = float(va_post[ELEC].sum().sum())

    col_pre = float(udom_pre[AGG].sum()) + float(uimp_pre[AGG].sum()) + va_pre_sum

    return pd.DataFrame(
        [
            {
                'metric': 'Make gross output x (row sum)',
                'before_221100': x_pre,
                'after_children_sum': x_post,
                'delta': x_post - x_pre,
            },
            {
                'metric': 'Make commodity output q (col sum)',
                'before_221100': q_pre,
                'after_children_sum': q_post,
                'delta': q_post - q_pre,
            },
            {
                'metric': 'Use commodity row (221100 purchases)',
                'before_221100': use_pre + y_pre,
                'after_children_sum': use_post + y_post_sum,
                'delta': (use_post + y_post_sum) - (use_pre + y_pre),
            },
            {
                'metric': 'VA column total (221100 industry)',
                'before_221100': va_pre_sum,
                'after_children_sum': va_post_sum,
                'delta': va_post_sum - va_pre_sum,
            },
            {
                'metric': 'GO identity residual (x − U−VA col)',
                'before_221100': x_pre - col_pre,
                'after_children_sum': float('nan'),
                'delta': float('nan'),
            },
        ]
    )
