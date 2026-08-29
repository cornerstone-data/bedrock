"""Apply 2017-learned per-cell redefinition ratios to a before-redef MUT.

Learns sparse movement ratios ``(before - after) / industry_GO`` from the
published 2017 before/after MUT pair and reapplies them with year-``t`` gross
output. Make, Use, VA, and Import are cellwise; margins use GO ratios for
industry buyers and absolute USD residuals for non-industry buyers.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.utils.economic.units import MILLION_CURRENCY_TO_CURRENCY
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

ATOL = 0.5 * MILLION_CURRENCY_TO_CURRENCY

MARGINS_VALUE_COLUMNS = (
    "Producers' Value",
    'Transportation',
    'Wholesale',
    'Retail',
    "Purchasers' Value",
)

_REDEF_DIR = Path(__file__).resolve().parents[2] / 'analysis' / 'nowcasting'
RATIOS_V_PATH = _REDEF_DIR / 'redefinition_ratios_2017_V.parquet'
RATIOS_U_PATH = _REDEF_DIR / 'redefinition_ratios_2017_U.parquet'
RATIOS_VA_PATH = _REDEF_DIR / 'redefinition_ratios_2017_VA.parquet'
RATIOS_UIMP_PATH = _REDEF_DIR / 'redefinition_ratios_2017_Uimp.parquet'
RATIOS_MARGINS_PATH = _REDEF_DIR / 'redefinition_ratios_2017_margins.parquet'

_INDUSTRY_SET = frozenset(USA_2017_INDUSTRY_CODES)

_V_STR_COLS = ('industry', 'commodity')
_CELL_STR_COLS = ('row_code', 'industry')
_MARGINS_STR_COLS = (
    'industry_code',
    'commodity_code',
    'value_column',
    'scale',
)


@dataclass(frozen=True)
class RedefinitionRatios:
    """Sparse 2017 redefinition movement ratios for the MUT quartet plus VA."""

    V: pd.DataFrame
    U: pd.DataFrame
    VA: pd.DataFrame
    Uimp: pd.DataFrame
    margins: pd.DataFrame


def industry_gross_output(V: pd.DataFrame) -> pd.Series:
    """Industry GO as Make row sums, in USD."""
    return V.sum(axis=1).astype(float)


def _usd(value: object) -> float:
    return float(np.asarray(value, dtype=float).reshape(-1)[0])


def _go(x: pd.Series, industry: str) -> float:
    if industry not in x.index:
        return 0.0
    return _usd(x.loc[industry])


def _cellwise_ratios(
    before: pd.DataFrame,
    after: pd.DataFrame,
    x: pd.Series,
    *,
    row_name: str,
    col_name: str,
    industry_is_column: bool,
) -> pd.DataFrame:
    """Sparse ratios for a dense table; control industry is row or column."""
    left, right = before.align(after, fill_value=0.0)
    stacked = (left.astype(float) - right.astype(float)).stack(future_stack=True)
    if not isinstance(stacked, pd.Series):
        raise TypeError('expected stacked Series from cellwise delta')
    keep = stacked.abs() > ATOL
    delta = stacked.loc[keep]
    if delta.empty:
        return pd.DataFrame(columns=[row_name, col_name, 'ratio'])

    frame = delta.reset_index(name='delta')
    frame.columns = [row_name, col_name, 'delta']
    frame[row_name] = frame[row_name].astype(str)
    frame[col_name] = frame[col_name].astype(str)
    industry_col = col_name if industry_is_column else row_name
    if industry_is_column:
        frame = frame.loc[frame[industry_col].isin(_INDUSTRY_SET)].copy()
    if frame.empty:
        return pd.DataFrame(columns=[row_name, col_name, 'ratio'])

    go = frame[industry_col].map(lambda code: _go(x, str(code))).astype(float)
    frame['ratio'] = np.where(go.abs() > ATOL, frame['delta'] / go, 0.0)
    return frame[[row_name, col_name, 'ratio']].reset_index(drop=True)


def _margins_ratios(
    margins_before: pd.DataFrame,
    margins_after: pd.DataFrame,
    x: pd.Series,
) -> pd.DataFrame:
    left, right = margins_before.align(margins_after, fill_value=0.0)
    cols = [c for c in MARGINS_VALUE_COLUMNS if c in left.columns]
    stacked = (left[cols].astype(float) - right[cols].astype(float)).stack(
        future_stack=True
    )
    if not isinstance(stacked, pd.Series):
        raise TypeError('expected stacked Series from margins delta')
    keep = stacked.abs() > ATOL
    delta = stacked.loc[keep]
    empty_cols = [
        'industry_code',
        'commodity_code',
        'value_column',
        'amount',
        'scale',
    ]
    if delta.empty:
        return pd.DataFrame(columns=empty_cols)

    frame = delta.reset_index(name='delta')
    # MultiIndex (industry, commodity) + value_column
    frame.columns = [
        'industry_code',
        'commodity_code',
        'value_column',
        'delta',
    ]
    frame['industry_code'] = frame['industry_code'].astype(str)
    frame['commodity_code'] = frame['commodity_code'].astype(str)
    frame['value_column'] = frame['value_column'].astype(str)

    is_industry = frame['industry_code'].isin(_INDUSTRY_SET)
    go = frame['industry_code'].map(lambda code: _go(x, str(code))).astype(float)
    amount = np.where(
        is_industry,
        np.where(go.abs() > ATOL, frame['delta'] / go, 0.0),
        frame['delta'],
    )
    scale = np.where(is_industry, 'go_ratio', 'absolute')
    return pd.DataFrame(
        {
            'industry_code': frame['industry_code'],
            'commodity_code': frame['commodity_code'],
            'value_column': frame['value_column'],
            'amount': amount,
            'scale': scale,
        }
    )


def compute_redefinition_ratios(
    V_before: pd.DataFrame,
    U_before: pd.DataFrame,
    VA_before: pd.DataFrame,
    Uimp_before: pd.DataFrame,
    margins_before: pd.DataFrame,
    V_after: pd.DataFrame,
    U_after: pd.DataFrame,
    VA_after: pd.DataFrame,
    Uimp_after: pd.DataFrame,
    margins_after: pd.DataFrame,
) -> RedefinitionRatios:
    """Learn sparse GO ratios from a before/after MUT pair (no path I/O)."""
    x = industry_gross_output(V_before)
    return RedefinitionRatios(
        V=_cellwise_ratios(
            V_before,
            V_after,
            x,
            row_name='industry',
            col_name='commodity',
            industry_is_column=False,
        ),
        U=_cellwise_ratios(
            U_before,
            U_after,
            x,
            row_name='row_code',
            col_name='industry',
            industry_is_column=True,
        ),
        VA=_cellwise_ratios(
            VA_before,
            VA_after,
            x,
            row_name='row_code',
            col_name='industry',
            industry_is_column=True,
        ),
        Uimp=_cellwise_ratios(
            Uimp_before,
            Uimp_after,
            x,
            row_name='row_code',
            col_name='industry',
            industry_is_column=True,
        ),
        margins=_margins_ratios(margins_before, margins_after, x),
    )


def write_redefinition_ratios(
    ratios: RedefinitionRatios, directory: Path = _REDEF_DIR
) -> None:
    """Write the five tracked ratio parquets."""
    directory.mkdir(parents=True, exist_ok=True)
    ratios.V.to_parquet(directory / RATIOS_V_PATH.name, index=False)
    ratios.U.to_parquet(directory / RATIOS_U_PATH.name, index=False)
    ratios.VA.to_parquet(directory / RATIOS_VA_PATH.name, index=False)
    ratios.Uimp.to_parquet(directory / RATIOS_UIMP_PATH.name, index=False)
    ratios.margins.to_parquet(directory / RATIOS_MARGINS_PATH.name, index=False)


def _read_str_frame(path: Path, str_cols: tuple[str, ...]) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    for col in str_cols:
        if col in frame.columns:
            frame[col] = frame[col].astype(str)
    return frame


def load_redefinition_ratios(directory: Path = _REDEF_DIR) -> RedefinitionRatios:
    """Load the five tracked ratio parquets."""
    return RedefinitionRatios(
        V=_read_str_frame(directory / RATIOS_V_PATH.name, _V_STR_COLS),
        U=_read_str_frame(directory / RATIOS_U_PATH.name, _CELL_STR_COLS),
        VA=_read_str_frame(directory / RATIOS_VA_PATH.name, _CELL_STR_COLS),
        Uimp=_read_str_frame(directory / RATIOS_UIMP_PATH.name, _CELL_STR_COLS),
        margins=_read_str_frame(
            directory / RATIOS_MARGINS_PATH.name, _MARGINS_STR_COLS
        ),
    )


def _apply_cellwise(
    frame: pd.DataFrame,
    ratios: pd.DataFrame,
    x: pd.Series,
    *,
    row_col: str,
    industry_col: str,
    industry_is_column: bool,
) -> pd.DataFrame:
    out = frame.copy().astype(float)
    if ratios.empty:
        return out
    for row in ratios.itertuples(index=False):
        row_code = str(getattr(row, row_col))
        industry = str(getattr(row, industry_col))
        ratio = _usd(getattr(row, 'ratio'))
        move = ratio * _go(x, industry)
        if industry_is_column:
            if row_code not in out.index:
                continue
            if industry not in out.columns:
                continue
            out.loc[row_code, industry] = _usd(out.loc[row_code, industry]) - move
        else:
            if industry not in out.index:
                continue
            if row_code not in out.columns:
                continue
            out.loc[industry, row_code] = _usd(out.loc[industry, row_code]) - move
    return out


def _apply_margins(
    margins: pd.DataFrame, ratios: pd.DataFrame, x: pd.Series
) -> pd.DataFrame:
    out = margins.copy().astype(float)
    if ratios.empty:
        return out
    for row in ratios.itertuples(index=False):
        industry = str(row.industry_code)
        commodity = str(row.commodity_code)
        col = str(row.value_column)
        amount = _usd(getattr(row, 'amount'))
        scale = str(row.scale)
        key = (industry, commodity)
        if key not in out.index:
            extra = pd.DataFrame(
                0.0,
                index=pd.MultiIndex.from_tuples([key], names=list(margins.index.names)),
                columns=out.columns,
            )
            out = pd.concat([out, extra])
        if col not in out.columns:
            out[col] = 0.0
        move = amount * _go(x, industry) if scale == 'go_ratio' else amount
        out.loc[key, col] = _usd(out.loc[key, col]) - move
    return out


def _scrub_float_dust(frame: pd.DataFrame) -> pd.DataFrame:
    """Zero residual float dust so table_match does not count it as EXTRA."""
    numeric = frame.astype(float)
    return numeric.mask(numeric.abs() < 1e-3, 0.0)


def apply_redefinition_ratios(
    V: pd.DataFrame,
    U: pd.DataFrame,
    VA: pd.DataFrame,
    Uimp: pd.DataFrame,
    margins: pd.DataFrame,
    *,
    ratios: RedefinitionRatios,
    x: pd.Series | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply learned ratios to a before-redef MUT. Never mutates inputs."""
    V_after = V.copy()
    U_after = U.copy()
    VA_after = VA.copy()
    Uimp_after = Uimp.copy()
    margins_after = margins.copy()
    x_t = industry_gross_output(V) if x is None else x.astype(float)

    V_after = _apply_cellwise(
        V_after,
        ratios.V,
        x_t,
        row_col='commodity',
        industry_col='industry',
        industry_is_column=False,
    )
    U_after = _apply_cellwise(
        U_after,
        ratios.U,
        x_t,
        row_col='row_code',
        industry_col='industry',
        industry_is_column=True,
    )
    VA_after = _apply_cellwise(
        VA_after,
        ratios.VA,
        x_t,
        row_col='row_code',
        industry_col='industry',
        industry_is_column=True,
    )
    Uimp_after = _apply_cellwise(
        Uimp_after,
        ratios.Uimp,
        x_t,
        row_col='row_code',
        industry_col='industry',
        industry_is_column=True,
    )
    margins_after = _apply_margins(margins_after, ratios.margins, x_t)
    return (
        _scrub_float_dust(V_after),
        _scrub_float_dust(U_after),
        _scrub_float_dust(VA_after),
        _scrub_float_dust(Uimp_after),
        _scrub_float_dust(margins_after),
    )
