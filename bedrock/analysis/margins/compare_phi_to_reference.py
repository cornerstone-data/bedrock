"""Compare model-computed Phi (PRO:PUR ratio) against external reference data.

USEEIO: ``derive_phi_cornerstone_usa_at_year`` vs the Phi sheet in the pinned
        USEEIO Excel workbook at ``usa_base_io_data_year`` and 2024.
CEDA:   ``derive_phi_ceda_usa`` vs the 'Purchaser - producer conversion' sheet
        in the CEDA 2025 Excel workbook (IO-year only; no year panel).

Usage::

    uv run python -m bedrock.analysis.margins.compare_phi_to_reference

Outputs:
  output/plots/phi_comparison.png
  output/phi_comparison_useeio_<year>.csv
  output/phi_comparison_ceda.csv
"""

from __future__ import annotations

import os
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

OUT = os.path.join(os.path.dirname(__file__), 'output')
PLOTS = os.path.join(OUT, 'plots')
os.makedirs(PLOTS, exist_ok=True)

from bedrock.transform.iot.derive_PRO_to_PUR_ratio import (  # noqa: E402
    derive_phi_ceda_usa,
    derive_phi_cornerstone_usa_at_year,
)
from bedrock.utils.config.config_controllers import temp_usa_config  # noqa: E402
from bedrock.utils.config.usa_config import get_usa_config  # noqa: E402
from bedrock.utils.io.gcp import download_gcs_file_if_not_exists  # noqa: E402
from bedrock.utils.snapshots.loader import useeio_baseline_local_dir  # noqa: E402
from bedrock.utils.validation.useeio_excel_baseline import (  # noqa: E402
    ensure_useeio_xlsx_local,
    load_useeio_baseline_pin_overrides,
)

_PIN_JSON = os.path.join(
    os.path.dirname(__file__),
    '..',
    '..',
    'utils',
    'snapshots',
    'useeio_baseline_pin.json',
)
_CEDA_GS_URI = (
    'gs://cornerstone-default/snapshots/CEDA_2025/CEDA 2025 (updated 2025-11-12).xlsx'
)
_GCS_PREFIX = 'gs://cornerstone-default/'
_USEEIO_PANEL_YEAR = 2024

_CACHE_MODULES = (
    'bedrock.extract.iot.io_2017',
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio',
)


def _xlsx_local_path(gs_uri: str) -> str:
    """Local cache path for any xlsx under gs://cornerstone-default/."""
    safe = re.sub(r'[^a-zA-Z0-9_.-]+', '_', gs_uri.removeprefix(_GCS_PREFIX))
    path = os.path.join(useeio_baseline_local_dir(), safe)
    return path if path.lower().endswith('.xlsx') else path + '.xlsx'


def _ensure_ceda_xlsx_local() -> str:
    local = _xlsx_local_path(_CEDA_GS_URI)
    if not os.path.isfile(local):
        rest = _CEDA_GS_URI.removeprefix(_GCS_PREFIX).strip('/')
        parts = rest.split('/')
        download_gcs_file_if_not_exists(parts[-1], '/'.join(parts[:-1]), local)
    return local


def _load_useeio_phi_reference(local_path: str, year: int) -> pd.Series:
    """Phi sheet: row 1 = year headers, col A = sector codes ({code}/US)."""
    raw = pd.read_excel(local_path, sheet_name='Phi', header=None, engine='openpyxl')
    headers = (
        raw.iloc[0, 1:].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    )
    sectors = raw.iloc[1:, 0].astype(str).str.strip()
    values = raw.iloc[1:, 1:].copy()
    values.columns = pd.Index(headers)
    values.index = pd.Index(sectors)
    year_str = str(year)
    if year_str not in values.columns:
        available = values.columns.tolist()
        raise ValueError(f'Year {year} not in USEEIO Phi sheet; available: {available}')
    phi = values[year_str].astype(float)
    phi.index = pd.Index(
        [s[:-3] if s.endswith('/US') else s for s in phi.index], name='sector'
    )
    return phi.dropna()


def _load_ceda_phi_reference(local_path: str) -> pd.Series:
    """'Purchaser - producer conversion': headers in row 5 (B onward), data in row 6."""
    raw = pd.read_excel(
        local_path,
        sheet_name='Purchaser - producer conversion',
        header=None,
        engine='openpyxl',
    )
    headers = raw.iloc[4, 1:].astype(str).str.strip()
    data_row = raw.iloc[5, 1:].copy()
    phi = pd.Series(
        pd.to_numeric(pd.Series(data_row), errors='coerce').values,
        index=pd.Index(headers),
        name='phi_reference',
    )
    phi.index.name = 'sector'
    return phi.dropna()


def _compute_useeio_phi_model(year: int) -> pd.Series:
    """Phi from the publish margins path at *year* USD."""
    phi = derive_phi_cornerstone_usa_at_year(year).astype(float)
    phi.index.name = 'sector'
    return phi


def _scatter_comparison(
    ax: plt.Axes, model: pd.Series, ref: pd.Series, title: str
) -> pd.DataFrame:
    """Scatter model vs reference; return aligned comparison DataFrame."""
    common = model.index.intersection(ref.index)
    x = ref.reindex(common).astype(float)
    y = model.reindex(common).astype(float)
    mask = x.notna() & y.notna()
    x, y = x[mask], y[mask]

    ax.scatter(x, y, s=8, alpha=0.5, color='steelblue', linewidths=0)
    lo = min(float(x.min()), float(y.min()), 0.0) - 0.02
    hi = max(float(x.max()), float(y.max()), 1.0) + 0.02
    ax.plot([lo, hi], [lo, hi], 'k--', linewidth=0.8, label='1:1')
    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.set_xlabel('Reference Phi (external)')
    ax.set_ylabel('Model Phi (bedrock)')
    ax.set_title(title)
    ax.legend(fontsize=7)
    if len(x) > 1:
        corr = float(x.corr(y))
        mae = float((y - x).abs().mean())
        med_rel = float(((y - x) / x.replace(0.0, np.nan)).abs().median())
        ax.text(
            0.04,
            0.92,
            f'n={len(x)}  r={corr:.3f}  MAE={mae:.4f}  med|rel|={med_rel:.3f}',
            transform=ax.transAxes,
            fontsize=7.5,
        )

    diff = y - x
    return pd.DataFrame(
        {'phi_model': y, 'phi_reference': x, 'diff': diff, 'abs_diff': diff.abs()}
    ).reindex(common)


def _useeio_comparison_years() -> tuple[int, ...]:
    with temp_usa_config('useeio_phoebe_23', cache_bearing_modules=_CACHE_MODULES):
        base_year = get_usa_config().usa_base_io_data_year
    years = [base_year]
    if _USEEIO_PANEL_YEAR not in years:
        years.append(_USEEIO_PANEL_YEAR)
    return tuple(years)


def main() -> None:
    useeio_years = _useeio_comparison_years()
    useeio_model_by_year: dict[int, pd.Series] = {}
    print('Computing USEEIO model Phi...')
    with temp_usa_config('useeio_phoebe_23', cache_bearing_modules=_CACHE_MODULES):
        for year in useeio_years:
            print(f'  year {year}...')
            useeio_model_by_year[year] = _compute_useeio_phi_model(year)

    print('Loading USEEIO reference Phi...')
    pin = load_useeio_baseline_pin_overrides(_PIN_JSON)
    useeio_gs_uri = pin['useeio_baseline_xlsx_gs_uri']
    useeio_local = _xlsx_local_path(useeio_gs_uri)
    ensure_useeio_xlsx_local(
        useeio_gs_uri, pin['useeio_baseline_xlsx_sha256'], useeio_local
    )
    useeio_ref_by_year = {
        year: _load_useeio_phi_reference(useeio_local, year) for year in useeio_years
    }

    print('Computing CEDA model Phi...')
    with temp_usa_config('v8_ceda_2025_usa', cache_bearing_modules=_CACHE_MODULES):
        phi_ceda_model = derive_phi_ceda_usa()

    print('Loading CEDA reference Phi...')
    ceda_local = _ensure_ceda_xlsx_local()
    phi_ceda_ref = _load_ceda_phi_reference(ceda_local)

    n_axes = len(useeio_years) + 1
    fig, axes = plt.subplots(1, n_axes, figsize=(5 * n_axes, 5))
    if n_axes == 1:
        axes = [axes]

    useeio_tables: dict[int, pd.DataFrame] = {}
    for ax, year in zip(axes, useeio_years, strict=False):
        model = useeio_model_by_year[year]
        ref = useeio_ref_by_year[year]
        print(
            f'\nUSEEIO {year}: {len(model.dropna())} model sectors, '
            f'{len(ref)} reference sectors'
        )
        useeio_tables[year] = _scatter_comparison(
            ax, model, ref, f'USEEIO Phi ({year})'
        )

    print(
        f'\nCEDA: {len(phi_ceda_model)} model sectors, '
        f'{len(phi_ceda_ref)} reference sectors'
    )
    df_ceda = _scatter_comparison(
        axes[-1], phi_ceda_model, phi_ceda_ref, 'CEDA Phi (IO year)'
    )

    fig.suptitle('PRO:PUR Phi — model (bedrock) vs external reference', fontsize=11)
    fig.tight_layout()
    plot_path = os.path.join(PLOTS, 'phi_comparison.png')
    fig.savefig(plot_path, dpi=150)
    plt.close(fig)
    print(f'\nPlot saved to: {plot_path}')

    for year, table in useeio_tables.items():
        path = os.path.join(OUT, f'phi_comparison_useeio_{year}.csv')
        table.to_csv(path)
        print(f'USEEIO CSV ({year}): {path}')

    ceda_csv = os.path.join(OUT, 'phi_comparison_ceda.csv')
    df_ceda.to_csv(ceda_csv)
    print(f'CEDA CSV: {ceda_csv}')


if __name__ == '__main__':
    main()
