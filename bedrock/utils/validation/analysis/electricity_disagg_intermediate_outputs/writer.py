"""Export V and extended U before/after 221100 electricity co-production reallocation."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

import bedrock.utils.config.common as common
from bedrock.publish.excel.writer import _apply_loc_suffix, _assemble_extended_U
from bedrock.transform.eeio.derived_cornerstone import (
    _derive_cornerstone_U_after_waste,
    _derive_cornerstone_V_after_waste,
    _derive_cornerstone_VA_after_waste,
    _derive_cornerstone_Ytot_with_trade,
)
from bedrock.utils.config.usa_config import USA_CONFIG_ENV_VAR
from bedrock.utils.math.handle_negatives import handle_negative_matrix_values
from bedrock.utils.taxonomy.cornerstone.final_demand import FINAL_DEMANDS

logger = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent
OUTPUT_FILENAME = 'V_and_U_after_221100_reallocation.xlsx'
_WRITTEN_CONFIGS: set[str] = set()


def _industry_cols(extended_u: pd.DataFrame, va: pd.DataFrame) -> pd.Index:
    return extended_u.columns.intersection(va.columns)


def _commodity_rows(extended_u: pd.DataFrame, intermediate: pd.DataFrame) -> pd.Index:
    return extended_u.index.intersection(intermediate.index)


def _totals_summary(
    label: str,
    *,
    v: pd.DataFrame,
    extended_u: pd.DataFrame,
    va: pd.DataFrame,
    intermediate: pd.DataFrame,
) -> pd.DataFrame:
    """One row per sector: Make and Use industry (x) and commodity (q) totals."""
    industry_cols = _industry_cols(extended_u, va)
    commodity_rows = _commodity_rows(extended_u, intermediate)

    x_make = v.sum(axis=1)
    x_use = extended_u.loc[:, industry_cols].sum(axis=0)
    q_make = v.sum(axis=0)
    q_use = extended_u.loc[commodity_rows, :].sum(axis=1)

    x_idx = x_make.index.union(x_use.index)
    q_idx = q_make.index.union(q_use.index)

    out = pd.DataFrame(
        {
            'stage': label,
            'x_make': x_make.reindex(x_idx).fillna(0.0),
            'x_use': x_use.reindex(x_idx).fillna(0.0),
            'q_make': q_make.reindex(q_idx).fillna(0.0),
            'q_use': q_use.reindex(q_idx).fillna(0.0),
        }
    )
    out.index.name = 'sector'
    out['x_diff_make_minus_use'] = out['x_make'] - out['x_use']
    out['q_diff_make_minus_use'] = out['q_make'] - out['q_use']
    return out


def _delta_summary(before: pd.DataFrame, after: pd.DataFrame) -> pd.DataFrame:
    """After minus before for each total column (aligned on sector index)."""
    cols = ['x_make', 'x_use', 'q_make', 'q_use']
    idx = before.index.union(after.index)
    delta = after[cols].reindex(idx).fillna(0.0) - before[cols].reindex(idx).fillna(0.0)
    delta.columns = [f'delta_{c}' for c in cols]
    delta['any_nonzero'] = (delta.abs() > 1.0).any(axis=1)
    return delta.sort_values('delta_x_make', key=lambda s: s.abs(), ascending=False)


def _output_path() -> Path:
    return OUTPUT_DIR / OUTPUT_FILENAME


def _with_publish_loc_suffix(frame: pd.DataFrame) -> pd.DataFrame:
    out = _apply_loc_suffix(frame)
    assert isinstance(out, pd.DataFrame)
    return out


def _extended_use_tables(
    *,
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    va: pd.DataFrame,
    y_fd: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    udom_nonneg = handle_negative_matrix_values(udom.copy())
    uimp_nonneg = handle_negative_matrix_values(uimp.copy())
    intermediate = udom_nonneg + uimp_nonneg
    extended = _assemble_extended_U(
        intermediate=intermediate,
        fd=y_fd,
        va=va.copy(),
    )
    return extended, intermediate


def maybe_write_electricity_disagg_intermediate_outputs(
    *,
    v: pd.DataFrame,
    udom: pd.DataFrame,
    uimp: pd.DataFrame,
    va: pd.DataFrame,
) -> Path | None:
    """Write before/after reallocation V, extended U, and totals summaries to Excel.

    Runs only during production-style pipelines (``download_fba_on_api_error``)
    and skips pytest sessions.
    """
    if not common.download_fba_on_api_error:
        return None
    if os.environ.get('PYTEST_CURRENT_TEST'):
        return None

    config_file = os.environ.get(USA_CONFIG_ENV_VAR)
    if not config_file:
        return None
    if config_file in _WRITTEN_CONFIGS:
        return None

    y_fd = _derive_cornerstone_Ytot_with_trade()[list(FINAL_DEMANDS)]

    v_before = _derive_cornerstone_V_after_waste()
    udom_before, uimp_before = _derive_cornerstone_U_after_waste()
    va_before = _derive_cornerstone_VA_after_waste()
    u_before, intermediate_before = _extended_use_tables(
        udom=udom_before,
        uimp=uimp_before,
        va=va_before,
        y_fd=y_fd,
    )

    u_after, intermediate_after = _extended_use_tables(
        udom=udom,
        uimp=uimp,
        va=va,
        y_fd=y_fd,
    )

    summary_before = _totals_summary(
        'waste_only',
        v=v_before,
        extended_u=u_before,
        va=va_before,
        intermediate=intermediate_before,
    )
    summary_after = _totals_summary(
        'after_electricity',
        v=v,
        extended_u=u_after,
        va=va,
        intermediate=intermediate_after,
    )
    totals_delta = _delta_summary(summary_before, summary_after)

    output_path = _output_path()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        _with_publish_loc_suffix(v_before).to_excel(
            writer, sheet_name='V_waste_only', index=True
        )
        _with_publish_loc_suffix(u_before).to_excel(
            writer, sheet_name='U_waste_only', index=True
        )
        _with_publish_loc_suffix(v.copy()).to_excel(
            writer, sheet_name='V_after_electricity', index=True
        )
        _with_publish_loc_suffix(u_after).to_excel(
            writer, sheet_name='U_after_electricity', index=True
        )
        summary_before.reset_index().to_excel(
            writer, sheet_name='totals_waste_only', index=False
        )
        summary_after.reset_index().to_excel(
            writer, sheet_name='totals_after_electricity', index=False
        )
        totals_delta.reset_index().to_excel(
            writer, sheet_name='totals_delta', index=False
        )

    _WRITTEN_CONFIGS.add(config_file)
    logger.info(
        'Wrote electricity disagg intermediate outputs (7 sheets) to %s',
        output_path.resolve(),
    )
    print(f'Wrote electricity disagg intermediate outputs to {output_path.resolve()}')
    return output_path
