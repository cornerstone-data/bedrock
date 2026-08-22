"""Snapshot today's 3-way + mixed-units production (P0 freeze).

Run from repo root::

    python -m bedrock.analysis.electricity_disagg_eia.snapshot_current_production
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from bedrock.analysis.electricity_disagg_eia.paths import (
    DISAGG_CONFIG,
    MIXED_CONFIG,
    config_dir,
    ensure_dirs,
)
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.publish.model_objects import get_B, get_D, get_N, get_q
from bedrock.transform.allocation.derived import derive_E_usa
from bedrock.transform.eeio.cornerstone_disagg_pipeline import (
    _model_year_y_row_221110,
    derive_disagg_Ytot_with_trade,
    electricity_conversion_factors,
    electricity_mixed_units_enabled,
)
from bedrock.transform.eeio.derived_cornerstone import (
    derive_cornerstone_Aq_mixed_units,
    derive_cornerstone_Aq_scaled,
    derive_cornerstone_U_set,
    derive_cornerstone_x_after_redefinition,
)
from bedrock.transform.eeio.electricity_disaggregation import GENERATION_SECTOR
from bedrock.transform.eeio.electricity_end_use_mapping import build_end_use_map
from bedrock.utils.config.usa_config import (
    get_usa_config,
    reset_usa_config,
    set_global_usa_config,
)
from bedrock.utils.math.formulas import backcompute_y_from_A_and_q
from bedrock.utils.schemas.cornerstone_schemas import ELECTRICITY_DISAGG_SECTORS
from bedrock.utils.validation.calculate_national_accounting_balance_diagnostics import (
    _compute_bly_series,
)

HH_FD = 'F01000'
EXPORT_FD = 'F04000'
CLASS_ORDER = (
    'Residential',
    'Commercial',
    'Industrial',
    'Transportation',
    'Exports',
    'HH',
)
_REPO_ROOT = Path(__file__).resolve().parents[3]


def _git_sha() -> str:
    return subprocess.check_output(
        ['git', 'rev-parse', 'HEAD'],
        cwd=_REPO_ROOT,
        text=True,
    ).strip()


def _series_to_frame(s: pd.Series, value_name: str) -> pd.DataFrame:
    out = s.astype(float).rename(value_name).to_frame()
    out.index.name = 'sector'
    return out


def _class_label(code: str, end_use_map: dict[str, str]) -> str:
    if code == EXPORT_FD:
        return 'Exports'
    return end_use_map.get(code, 'Commercial')


def _class_generation(
    gen_use_y: pd.Series[float],
    end_use_map: dict[str, str],
    *,
    unit: str,
) -> pd.DataFrame:
    """Class totals; Commercial excludes F04000; HH is F01000 (also in Residential)."""
    totals = {c: 0.0 for c in CLASS_ORDER}
    for code, val in gen_use_y.items():
        label = _class_label(str(code), end_use_map)
        if label not in totals:
            continue
        totals[label] += float(val)
    if EXPORT_FD in gen_use_y.index:
        totals['Exports'] = float(gen_use_y[EXPORT_FD])
    if HH_FD in gen_use_y.index:
        totals['HH'] = float(gen_use_y[HH_FD])
    return pd.DataFrame(
        {
            'class': list(CLASS_ORDER),
            'value': [totals[c] for c in CLASS_ORDER],
            'unit': unit,
        }
    )


def _generation_use_y(
    *,
    mixed: bool,
    aq: object,
    aq_monetary: object,
    q: pd.Series[float],
    uset: object,
    y_table: pd.DataFrame,
) -> pd.Series[float]:
    gen = GENERATION_SECTOR
    if mixed:
        adom = aq.Adom  # type: ignore[attr-defined]
        u_gen = (adom.loc[gen].astype(float) * q.astype(float)).astype(float)
        y_gen = _model_year_y_row_221110(aq).astype(float)
        return u_gen.add(y_gen, fill_value=0.0)
    u_gen = uset.Udom.loc[gen].astype(float)  # type: ignore[attr-defined]
    y_gen = y_table.loc[gen].astype(float)
    return u_gen.add(y_gen, fill_value=0.0)


def _snapshot_one(config_stem: str) -> None:
    reset_usa_config()
    clear_all_publish_caches()
    set_global_usa_config(config_stem)
    cfg = get_usa_config()
    mixed = electricity_mixed_units_enabled()
    out = config_dir(config_stem)
    out.mkdir(parents=True, exist_ok=True)

    aq_monetary = derive_cornerstone_Aq_scaled()
    aq = derive_cornerstone_Aq_mixed_units() if mixed else aq_monetary
    q = get_q()
    x = derive_cornerstone_x_after_redefinition()
    uset = derive_cornerstone_U_set()
    y_table = derive_disagg_Ytot_with_trade()
    y_back = backcompute_y_from_A_and_q(A=aq.Adom, q=q)

    gen = GENERATION_SECTOR
    gen_use_y = _generation_use_y(
        mixed=mixed,
        aq=aq,
        aq_monetary=aq_monetary,
        q=q,
        uset=uset,
        y_table=y_table,
    )

    elec = list(ELECTRICITY_DISAGG_SECTORS)
    udom_3x3 = uset.Udom.reindex(index=elec, columns=elec).astype(float)
    uimp_3x3 = uset.Uimp.reindex(index=elec, columns=elec).astype(float)
    intersection = pd.concat(
        {'Udom': udom_3x3, 'Uimp': uimp_3x3},
        names=['table', 'commodity'],
    )
    electricity_rows_y = pd.concat(
        {
            'Udom': uset.Udom.reindex(index=elec).astype(float),
            'Uimp': uset.Uimp.reindex(index=elec).astype(float),
            'Y': y_table.reindex(index=elec).astype(float),
        },
        names=['table', 'commodity'],
    )

    E = derive_E_usa()
    D = get_D()
    N = get_N()
    B = get_B()
    bly = _compute_bly_series(B=B, Adom=aq.Adom, y=y_back)

    end_use_map = build_end_use_map()
    unit = 'MWh' if mixed else 'USD'
    class_mwh = _class_generation(gen_use_y, end_use_map, unit=unit)

    c_col: float | None = None
    implied_p: float | str = 'N/A'
    if mixed:
        c_col, c_row = electricity_conversion_factors(aq_monetary)
        c_row.astype(float).rename('c_row').to_frame().to_parquet(out / 'c_row.parquet')
        if c_col and c_col != 0.0:
            implied_p = float(1.0 / c_col)

    ugg = None
    if gen in uset.Udom.index and gen in uset.Udom.columns:
        ugg = float(uset.Udom.at[gen, gen])
    f01000_gen = float(gen_use_y[HH_FD]) if HH_FD in gen_use_y.index else None

    metadata = {
        'config': config_stem,
        'git_sha': _git_sha(),
        'datetime_utc': datetime.now(timezone.utc).isoformat(),
        'flags': {
            'implement_electricity_reallocation': (
                cfg.implement_electricity_reallocation
            ),
            'implement_electricity_disaggregation': (
                cfg.implement_electricity_disaggregation
            ),
            'implement_electricity_mixed_units': cfg.implement_electricity_mixed_units,
            'apply_io_year_adjustments': cfg.apply_io_year_adjustments,
            'model_base_year': cfg.model_base_year,
            'usa_ghg_data_year': cfg.usa_ghg_data_year,
        },
        'mixed_units': mixed,
        'p': implied_p if mixed else 'N/A',
        'p_note': (
            'implied q_$[221110]/eGRID (today 1/c_col); not D0 p'
            if mixed
            else 'no single production p on 3-way-only path'
        ),
        'c_col': c_col,
        'generation_use_y_total': float(gen_use_y.sum()),
        'U[G,G]_udom': ugg,
        'F01000_generation': f01000_gen,
    }

    _series_to_frame(q, 'q').to_parquet(out / 'q.parquet')
    _series_to_frame(x, 'x').to_parquet(out / 'x.parquet')
    _series_to_frame(gen_use_y, 'generation_use_y').to_parquet(
        out / 'use_y_generation.parquet'
    )
    intersection.to_parquet(out / 'intersection_3x3.parquet')
    electricity_rows_y.to_parquet(out / 'electricity_rows_y.parquet')
    E.to_parquet(out / 'E.parquet')
    D.to_parquet(out / 'D.parquet')
    N.to_parquet(out / 'N.parquet')
    _series_to_frame(bly, 'BLy').to_parquet(out / 'BLy.parquet')
    class_mwh.to_parquet(out / 'class_generation_mwh.parquet', index=False)
    (out / 'run_metadata.json').write_text(
        json.dumps(metadata, indent=2, default=str) + '\n',
        encoding='utf-8',
    )


def main() -> None:
    ensure_dirs()
    for stem in (DISAGG_CONFIG, MIXED_CONFIG):
        print(f'snapshot {stem} ...', flush=True)
        _snapshot_one(stem)
        print(f'  wrote {config_dir(stem)}', flush=True)


if __name__ == '__main__':
    main()
