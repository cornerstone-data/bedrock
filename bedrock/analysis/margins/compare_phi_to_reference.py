"""Compare model-computed Phi (PRO:PUR ratio) against external reference data.

USEEIO: bedrock Phi at BEA detail commodity level vs the Phi sheet in the pinned
        USEEIO Excel workbook, column = model_base_year.
CEDA:   bedrock Phi at CEDA v7 sector level vs the 'Purchaser - producer
        conversion' sheet in the CEDA 2025 Excel workbook (header row 5,
        data row 6).

Outputs:
  output/plots/phi_comparison.png   — side-by-side scatter plots
  output/phi_comparison_useeio.csv  — USEEIO sector-level comparison table
  output/phi_comparison_ceda.csv    — CEDA sector-level comparison table
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
    _margins_by_commodity,
    _useeio_margins_filters,
    derive_phi_ceda_usa,
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


def _compute_useeio_phi_model() -> pd.Series:
    """PRO:PUR at BEA detail commodity level under the active USEEIO config."""
    margins = _margins_by_commodity(
        _useeio_margins_filters, abs_negative_producers_value=True
    )
    phi = (margins["Producers' Value"] / margins["Purchasers' Value"]).replace(
        [np.inf, -np.inf, np.nan], 1
    )
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
        ax.text(
            0.04,
            0.92,
            f'n={len(x)}  r={corr:.3f}  MAE={mae:.4f}',
            transform=ax.transAxes,
            fontsize=7.5,
        )

    diff = y - x
    return pd.DataFrame(
        {'phi_model': y, 'phi_reference': x, 'diff': diff, 'abs_diff': diff.abs()}
    ).reindex(common)


_CACHE_MODULES = (
    'bedrock.extract.iot.io_2017',
    'bedrock.transform.iot.derive_PRO_to_PUR_ratio',
)

# ─── USEEIO ──────────────────────────────────────────────────────────────────
print('Computing USEEIO model Phi...')
with temp_usa_config('useeio_phoebe_23', cache_bearing_modules=_CACHE_MODULES):
    phi_useeio_model = _compute_useeio_phi_model()
    useeio_year = get_usa_config().model_base_year

print('Loading USEEIO reference Phi...')
_pin = load_useeio_baseline_pin_overrides(_PIN_JSON)
_useeio_gs_uri = _pin['useeio_baseline_xlsx_gs_uri']
_useeio_local = _xlsx_local_path(_useeio_gs_uri)
ensure_useeio_xlsx_local(
    _useeio_gs_uri, _pin['useeio_baseline_xlsx_sha256'], _useeio_local
)
phi_useeio_ref = _load_useeio_phi_reference(_useeio_local, useeio_year)

# ─── CEDA ────────────────────────────────────────────────────────────────────
print('Computing CEDA model Phi...')
with temp_usa_config('v8_ceda_2025_usa', cache_bearing_modules=_CACHE_MODULES):
    phi_ceda_model = derive_phi_ceda_usa()

print('Loading CEDA reference Phi...')
_ceda_local = _ensure_ceda_xlsx_local()
phi_ceda_ref = _load_ceda_phi_reference(_ceda_local)

# ─── Comparison summary ───────────────────────────────────────────────────────
print(
    f'\nUSEEIO: {len(phi_useeio_model.dropna())} model sectors, '
    f'{len(phi_useeio_ref)} reference sectors'
)
print(
    f'CEDA: {len(phi_ceda_model)} model sectors, {len(phi_ceda_ref)} reference sectors'
)

# ─── Plots ────────────────────────────────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
df_useeio = _scatter_comparison(
    ax1, phi_useeio_model, phi_useeio_ref, f'USEEIO Phi ({useeio_year})'
)
df_ceda = _scatter_comparison(ax2, phi_ceda_model, phi_ceda_ref, 'CEDA Phi')
fig.suptitle('PRO:PUR Phi — model (bedrock) vs external reference', fontsize=11)
fig.tight_layout()
plot_path = os.path.join(PLOTS, 'phi_comparison.png')
fig.savefig(plot_path, dpi=150)
plt.close(fig)
print(f'\nPlot saved to: {plot_path}')

# ─── CSVs ─────────────────────────────────────────────────────────────────────
useeio_csv = os.path.join(OUT, 'phi_comparison_useeio.csv')
df_useeio.to_csv(useeio_csv)
print(f'USEEIO CSV: {useeio_csv}')

ceda_csv = os.path.join(OUT, 'phi_comparison_ceda.csv')
df_ceda.to_csv(ceda_csv)
print(f'CEDA CSV: {ceda_csv}')
