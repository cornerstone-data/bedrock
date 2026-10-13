"""Side-by-side margin SEF: Zenodo v1.4.0, Phoebe rebuild, Cornerstone v0.3 and v0.6.

Zenodo publishes NAICS-6 rows mapped to ``Reference USEEIO Code`` (BEA detail).
Rows whose reference field lists **multiple** codes (comma-separated, e.g.
``230301, 233230``) are dropped. Remaining rows collapse to one value per
reference code (mean across NAICS rows sharing the same code). Bedrock joins on
the same USEEIO commodity codes.

Values are purchaser-price CO2e factors at ``--dollar_year`` (default 2024,
matching Zenodo's published unit).

Usage (PowerShell, repo root)::

    uv run python -m bedrock.analysis.margins.compare_sef_margins_sources

Optional:
  --phoebe-sef-csv PATH (required; pinned SEF from a pre-retirement phoebe run)
  --v0-3-sef-csv PATH
  --v0-6-sef-csv PATH
  --zenodo-xlsx PATH  (defaults to cached download under
                       ``bedrock/utils/snapshots/data/zenodo_sef_v1.4.0/``)
  --dollar_year 2024
  --output-csv PATH
  --summary-csv PATH

Outputs:
  output/sef_margins_comparison.csv  (one row per commodity)
  output/sef_margins_summary.csv     (one row per model pair)
  output/plots/sef_margins_v0_6_vs_zenodo_hist.png  (margin % change, Zenodo -> v0.6)
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import urllib.request
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import bedrock.utils.config.common as common
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.publish.emission_factors.writer import write_emission_factors
from bedrock.utils.config.settings import GIT_HASH_LONG
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config
from bedrock.utils.validation.analysis.plotting import (
    DEFAULT_XLIM,
    apply_axis_fonts,
    percent_histogram,
    save_and_close,
    setup_mpl,
)

logger = logging.getLogger(__name__)

_PKG_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PKG_DIR.parents[3]
_DEFAULT_OUTPUT = _PKG_DIR / 'output' / 'sef_margins_comparison.csv'
_DEFAULT_SUMMARY = _PKG_DIR / 'output' / 'sef_margins_summary.csv'
_DEFAULT_HIST_PNG = 'sef_margins_v0_6_vs_zenodo_hist.png'

_ZENODO_RECORD_ID = 17202747
_ZENODO_FILENAME = 'SupplyChainGHGEmissionFactorsv1.4.0.xlsx'
ZENODO_DOI = '10.5281/zenodo.17202747'
_CACHE_DIR = (
    _REPO_ROOT / 'bedrock' / 'utils' / 'snapshots' / 'data' / 'zenodo_sef_v1.4.0'
)

REF_CODE_COL = 'Reference USEEIO Code'
BEDROCK_CODE_COL = 'Cornerstone Commodity Code'

COL_WITHOUT = 'Supply Chain Emission Factors without Margins'
COL_MARGINS = 'Margins of Supply Chain Emission Factors'
COL_WITH = 'Supply Chain Emission Factors with Margins'
SEF_VALUE_COLS: tuple[str, ...] = (COL_WITHOUT, COL_MARGINS, COL_WITH)

_V0_3_CONFIG = '2025_usa_cornerstone_v0_3'
_V0_6_CONFIG = '2025_usa_cornerstone_v0_6'

# Model pairs summarized as (model, reference).
_SUMMARY_PAIRS: tuple[tuple[str, str], ...] = (
    ('phoebe', 'zenodo'),
    ('v0_3', 'zenodo'),
    ('v0_6', 'zenodo'),
    ('v0_3', 'phoebe'),
    ('v0_6', 'v0_3'),
)


def _zenodo_xlsx_cache_path() -> Path:
    return _CACHE_DIR / _ZENODO_FILENAME


def ensure_zenodo_xlsx_local(path: Path | None = None) -> Path:
    """Download Zenodo v1.4.0 SEF workbook when missing locally."""
    local = path or _zenodo_xlsx_cache_path()
    if local.is_file():
        return local
    local.parent.mkdir(parents=True, exist_ok=True)
    api_url = f'https://zenodo.org/api/records/{_ZENODO_RECORD_ID}'
    logger.info('fetching Zenodo record metadata: %s', api_url)
    with urllib.request.urlopen(api_url, timeout=120) as resp:
        meta = json.load(resp)
    files = meta.get('files', [])
    match = next((f for f in files if f.get('key') == _ZENODO_FILENAME), None)
    if match is None:
        raise FileNotFoundError(
            f'{_ZENODO_FILENAME!r} not found on Zenodo record {_ZENODO_RECORD_ID}'
        )
    download_url = match['links']['self']
    logger.info('downloading %s -> %s', download_url, local)
    with urllib.request.urlopen(download_url, timeout=600) as resp:
        local.write_bytes(resp.read())
    return local


_UNIT_COL = 'Unit'
_UNIT_PATTERN = re.compile(r'/\s*(\d{4})\s*USD,\s*(purchaser|producer) price')


def parse_price_basis(units: pd.Series, source: str) -> tuple[int, str]:
    """``(dollar_year, 'purchaser' | 'producer')`` shared by every ``Unit`` row.

    Raises when a row does not parse or rows disagree, so a source can never be
    compared on a mixed or unstated price basis.
    """
    parsed = units.astype(str).str.extract(_UNIT_PATTERN)
    if parsed.isna().any().any():
        bad = units[parsed.isna().any(axis=1)].unique()[:5].tolist()
        raise ValueError(f'{source}: unparseable Unit values {bad!r}')
    bases = {(int(y), p) for y, p in parsed.itertuples(index=False)}
    if len(bases) != 1:
        raise ValueError(f'{source}: mixed price bases {sorted(bases)!r}')
    return bases.pop()


def check_same_price_basis(models: dict[str, pd.DataFrame]) -> tuple[int, str]:
    """Raise unless every model carries the same dollar year and price type."""
    bases = {label: frame.attrs['price_basis'] for label, frame in models.items()}
    if len(set(bases.values())) != 1:
        raise ValueError(f'SEF sources are on different price bases: {bases!r}')
    basis: tuple[int, str] = next(iter(bases.values()))
    logger.info(
        'price basis: %d USD, %s price for %s', basis[0], basis[1], ', '.join(bases)
    )
    return basis


def _reference_code_lists_multiple_codes(value: object) -> bool:
    return ',' in str(value).strip()


def load_zenodo_sef_by_reference_code(
    xlsx_path: Path,
    *,
    value_cols: tuple[str, ...] = SEF_VALUE_COLS,
) -> pd.DataFrame:
    """Zenodo SEF columns indexed by Reference USEEIO Code."""
    raw = pd.read_excel(xlsx_path, sheet_name='CO2e', engine='openpyxl')
    if REF_CODE_COL not in raw.columns:
        raise KeyError(
            f'CO2e sheet missing {REF_CODE_COL!r}; columns={list(raw.columns)!r}'
        )
    missing = [c for c in value_cols if c not in raw.columns]
    if missing:
        raise KeyError(f'CO2e sheet missing {missing!r}; columns={list(raw.columns)!r}')

    df = raw[[REF_CODE_COL, *value_cols]].copy()
    df[REF_CODE_COL] = df[REF_CODE_COL].astype(str).str.strip()
    for col in value_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=[REF_CODE_COL])

    multi_code_rows = df[REF_CODE_COL].map(_reference_code_lists_multiple_codes)
    n_multi_code = int(multi_code_rows.sum())
    if n_multi_code > 0:
        logger.info(
            'excluding %d Zenodo rows whose Reference USEEIO Code lists multiple codes',
            n_multi_code,
        )
    df = df.loc[~multi_code_rows]

    out = df.groupby(REF_CODE_COL, sort=True)[list(value_cols)].mean().astype(float)
    out.index.name = 'useeio_code'
    out.attrs['price_basis'] = parse_price_basis(raw[_UNIT_COL], 'zenodo')
    return out


def load_bedrock_sef(
    sef_csv: Path,
    *,
    value_cols: tuple[str, ...] = SEF_VALUE_COLS,
) -> pd.DataFrame:
    """Bedrock SEF CSV columns indexed by commodity code (``/US`` stripped)."""
    table = pd.read_csv(sef_csv)
    if BEDROCK_CODE_COL not in table.columns:
        raise KeyError(f'SEF CSV missing {BEDROCK_CODE_COL!r}')
    missing = [c for c in value_cols if c not in table.columns]
    if missing:
        raise KeyError(f'SEF CSV missing {missing!r}; columns={list(table.columns)!r}')

    codes = table[BEDROCK_CODE_COL].astype(str).str.strip().str.removesuffix('/US')
    out = pd.DataFrame(
        {
            col: pd.to_numeric(table[col], errors='coerce').to_numpy()
            for col in value_cols
        },
        index=pd.Index(codes, name='useeio_code'),
    ).astype(float)
    out.attrs['price_basis'] = parse_price_basis(table[_UNIT_COL], str(sef_csv))
    return out


def publish_sef(config_name: str, dollar_year: int) -> Path:
    """Publish purchaser-price CO2e SEF CSV for ``config_name`` at ``dollar_year``."""
    if not GIT_HASH_LONG:
        raise RuntimeError('GIT_HASH_LONG is not set')
    out_dir = (
        _REPO_ROOT / 'bedrock' / 'publish' / 'output' / GIT_HASH_LONG / config_name
    )
    clear_all_publish_caches()
    reset_usa_config(should_reset_env_var=True)
    set_global_usa_config(config_name)
    common.download_fba_on_api_error = True
    paths = write_emission_factors(
        str(out_dir),
        config_name=config_name,
        dollar_year=dollar_year,
        purchaser_price=True,
    )
    return Path(paths['co2e'])


def _pct_diff(numer: pd.Series, denom: pd.Series) -> pd.Series:
    return (numer - denom) / denom.replace(0.0, np.nan)


def summarize_pair(
    label: str, reference: str, values: pd.Series, ref_values: pd.Series
) -> dict[str, object]:
    """Agreement of ``values`` with ``ref_values`` on the codes both carry."""
    paired = pd.concat([values, ref_values], axis=1, join='inner').dropna()
    b = paired.iloc[:, 0].astype(float)
    r = paired.iloc[:, 1].astype(float)
    pct = _pct_diff(b, r).replace([np.inf, -np.inf], np.nan).dropna()
    has_pct = not pct.empty
    return {
        'model': label,
        'reference': reference,
        'joined': len(paired),
        'model_sum': float(b.sum()),
        'reference_sum': float(r.sum()),
        'sum_ratio': float(b.sum() / r.sum()) if r.sum() != 0.0 else np.nan,
        'corr': float(b.corr(r)) if len(paired) > 1 else np.nan,
        'median_pct': float(pct.median()) if has_pct else np.nan,
        'median_abs_pct': float(pct.abs().median()) if has_pct else np.nan,
        'within_5pct': int((pct.abs() <= 0.05).sum()),
        'within_25pct': int((pct.abs() <= 0.25).sum()),
        'n_pct': len(pct),
    }


def build_margins_comparison(models: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Wide table of margin and without-margin SEF by USEEIO commodity code.

    ``models`` maps a label (``zenodo``, ``phoebe``, ``v0_3``, ``v0_6``) to its
    SEF frame; percent differences follow ``_SUMMARY_PAIRS``.
    """
    codes = pd.Index([], name='useeio_code')
    for frame in models.values():
        codes = codes.union(frame.index)
    codes = codes.sort_values()
    out = pd.DataFrame(index=pd.Index(codes, name='useeio_code'))

    for label, frame in models.items():
        out[f'{label}_margins'] = frame.reindex(codes)[COL_MARGINS]
    for label, frame in models.items():
        out[f'{label}_without'] = frame.reindex(codes)[COL_WITHOUT]
    for label, ref in _SUMMARY_PAIRS:
        if label in models and ref in models:
            out[f'pct_{label}_vs_{ref}'] = _pct_diff(
                out[f'{label}_margins'], out[f'{ref}_margins']
            )
    return out.reset_index()


def build_margins_summary(comparison: pd.DataFrame) -> pd.DataFrame:
    """One row per ``_SUMMARY_PAIRS`` entry, on margin SEF."""
    wide = comparison.set_index('useeio_code')
    rows = [
        summarize_pair(label, ref, wide[f'{label}_margins'], wide[f'{ref}_margins'])
        for label, ref in _SUMMARY_PAIRS
        if f'{label}_margins' in wide and f'{ref}_margins' in wide
    ]
    return pd.DataFrame(rows)


def plot_margins_change_histogram(
    comparison: pd.DataFrame,
    out_png: Path,
    *,
    model: str = 'v0_6',
    reference: str = 'zenodo',
    model_label: str = 'Cornerstone v0.6',
    reference_label: str = 'USEEIO SEF v1.4.0 (Zenodo)',
    top_n: int = 15,
    price_basis: tuple[int, str] = (2024, 'purchaser'),
) -> Path:
    """EF-diagnostics-style histogram of margin SEF % change, ``reference`` -> ``model``.

    Bins, median line, and +/-10/20% guides come from the diagnostics
    ``percent_histogram``. Commodities with a zero reference margin have no
    percent change and are left out; values past the x-limits are counted in
    the text box rather than drawn.
    """
    setup_mpl()
    wide = comparison.set_index('useeio_code')
    m = wide[f'{model}_margins']
    r = wide[f'{reference}_margins']
    pct = (_pct_diff(m, r) * 100).replace([np.inf, -np.inf], np.nan).dropna()
    paired = pd.concat([m, r], axis=1, join='inner').loc[pct.index]
    sum_ratio_pct = (paired.iloc[:, 0].sum() / paired.iloc[:, 1].sum() - 1) * 100

    lo, hi = DEFAULT_XLIM
    n_below = int((pct < lo).sum())
    n_above = int((pct > hi).sum())
    top = pct.reindex(pct.abs().sort_values(ascending=False).index).head(top_n)
    lines = [
        f'Commodities: {len(pct)}',
        f'Median: {pct.median():+.1f}%',
        f'Median |%|: {pct.abs().median():.1f}%',
        f'Sum of factors: {sum_ratio_pct:+.1f}%',
        f'Within ±20%: {int((pct.abs() <= 20).sum())}',
        f'Beyond x-range: {n_below} below, {n_above} above',
        '',
        f'Largest |%| (top {top_n}):',
        *(f'{code}: {val:+.0f}%' for code, val in top.items()),
    ]

    fig, ax = plt.subplots(figsize=(14, 9))
    percent_histogram(
        ax,
        pct,
        xlim=DEFAULT_XLIM,
        xlabel='Percentage Diff (%)',
        ylabel='Count',
        title=(
            f'Margin emission factors: {reference_label} -> {model_label}\n'
            f'kg CO2e per {price_basis[0]} USD, {price_basis[1]} price, both sides'
        ),
        text_box='\n'.join(lines),
        text_box_fontsize=11,
        legend_fontsize=13,
    )
    apply_axis_fonts(ax)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    save_and_close(fig, out_png)
    return out_png


def _resolve_sef(csv: Path | None, config: str, dollar_year: int) -> Path:
    if csv is not None:
        return csv
    logger.info('publishing %s at dollar_year=%d', config, dollar_year)
    return publish_sef(config, dollar_year)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dollar_year', type=int, default=2024)
    parser.add_argument('--phoebe-sef-csv', type=Path, default=None)
    parser.add_argument('--v0-3-sef-csv', type=Path, default=None)
    parser.add_argument('--v0-6-sef-csv', type=Path, default=None)
    parser.add_argument('--zenodo-xlsx', type=Path, default=None)
    parser.add_argument('--output-csv', type=Path, default=_DEFAULT_OUTPUT)
    parser.add_argument('--summary-csv', type=Path, default=_DEFAULT_SUMMARY)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

    zenodo_path = ensure_zenodo_xlsx_local(args.zenodo_xlsx)
    zenodo = load_zenodo_sef_by_reference_code(zenodo_path)

    if args.phoebe_sef_csv is None:
        parser.error(
            '--phoebe-sef-csv is required: the useeio_phoebe_23 config was '
            'retired with the USEEIO-recreation flags; use a pinned phoebe SEF '
            'CSV from a prior publish run.'
        )
    phoebe_path = args.phoebe_sef_csv
    logger.info('phoebe SEF: %s', phoebe_path)
    phoebe = load_bedrock_sef(phoebe_path)

    models: dict[str, pd.DataFrame] = {'zenodo': zenodo, 'phoebe': phoebe}
    for label, csv, config in (
        ('v0_3', args.v0_3_sef_csv, _V0_3_CONFIG),
        ('v0_6', args.v0_6_sef_csv, _V0_6_CONFIG),
    ):
        path = _resolve_sef(csv, config, args.dollar_year)
        logger.info('%s SEF: %s', label, path)
        models[label] = load_bedrock_sef(path)

    dollar_year, price_type = check_same_price_basis(models)
    if dollar_year != args.dollar_year:
        raise ValueError(
            f'SEF sources are in {dollar_year} USD, not --dollar_year '
            f'{args.dollar_year}'
        )
    comparison = build_margins_comparison(models)
    summary = build_margins_summary(comparison)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(args.output_csv, index=False)
    args.summary_csv.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(args.summary_csv, index=False)

    with pd.option_context('display.width', 200, 'display.max_columns', None):
        logger.info('margin SEF summary:\n%s', summary.to_string(index=False))
    logger.info(
        'rows=%d (%s)',
        len(comparison),
        ' '.join(
            f'{label}={int(comparison[f"{label}_margins"].notna().sum())}'
            for label in models
        ),
    )
    logger.info('wrote %s and %s', args.output_csv, args.summary_csv)
    png = plot_margins_change_histogram(
        comparison,
        args.output_csv.parent / 'plots' / _DEFAULT_HIST_PNG,
        price_basis=(dollar_year, price_type),
    )
    logger.info('wrote %s', png)
    logger.info('Zenodo source: https://doi.org/%s', ZENODO_DOI)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
