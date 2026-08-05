"""Side-by-side margin SEF: Zenodo v1.4.0, Phoebe rebuild, Cornerstone v0.3.

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
  --zenodo-xlsx PATH  (defaults to cached download under
                       ``bedrock/utils/snapshots/data/zenodo_sef_v1.4.0/``)
  --dollar_year 2024
  --output-csv PATH

Outputs:
  output/sef_margins_zenodo_phoebe_v0_3.csv
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

import bedrock.utils.config.common as common
from bedrock.publish.cache_reset import clear_all_publish_caches
from bedrock.publish.emission_factors.writer import write_emission_factors
from bedrock.utils.config.settings import GIT_HASH_LONG
from bedrock.utils.config.usa_config import reset_usa_config, set_global_usa_config

logger = logging.getLogger(__name__)

_PKG_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PKG_DIR.parents[3]
_DEFAULT_OUTPUT = _PKG_DIR / 'output' / 'sef_margins_zenodo_phoebe_v0_3.csv'

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
    )
    return out.astype(float)


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


def _summarize_vs_zenodo(label: str, margins: pd.Series, zenodo: pd.Series) -> None:
    common_codes = margins.index.intersection(zenodo.index)
    paired = pd.concat(
        [margins.reindex(common_codes), zenodo.reindex(common_codes)], axis=1
    ).dropna()
    b = paired.iloc[:, 0].astype(float)
    r = paired.iloc[:, 1].astype(float)
    pct = _pct_diff(b, r).replace([np.inf, -np.inf], np.nan).dropna()
    sum_ratio = float(b.sum() / r.sum()) if float(r.sum()) != 0.0 else float('nan')
    logger.info(
        '%s vs zenodo: joined=%d sum_ratio=%.4f corr=%.4f '
        'median_pct=%.4f median_abs_pct=%.4f within_5pct=%d/%d',
        label,
        len(common_codes),
        sum_ratio,
        float(b.corr(r)) if len(common_codes) > 1 else float('nan'),
        float(pct.median()) if not pct.empty else float('nan'),
        float(pct.abs().median()) if not pct.empty else float('nan'),
        int((pct.abs() <= 0.05).sum()) if not pct.empty else 0,
        len(pct),
    )


def build_margins_comparison(
    zenodo: pd.DataFrame,
    phoebe: pd.DataFrame,
    v0_3: pd.DataFrame,
) -> pd.DataFrame:
    """Wide table of margin (and without) SEF by USEEIO commodity code."""
    codes = zenodo.index.union(phoebe.index).union(v0_3.index).sort_values()
    out = pd.DataFrame({'useeio_code': codes})
    out = out.set_index('useeio_code')

    out['zenodo_margins'] = zenodo.reindex(codes)[COL_MARGINS]
    out['phoebe_margins'] = phoebe.reindex(codes)[COL_MARGINS]
    out['v0_3_margins'] = v0_3.reindex(codes)[COL_MARGINS]

    out['zenodo_without'] = zenodo.reindex(codes)[COL_WITHOUT]
    out['phoebe_without'] = phoebe.reindex(codes)[COL_WITHOUT]
    out['v0_3_without'] = v0_3.reindex(codes)[COL_WITHOUT]

    out['pct_phoebe_vs_zenodo'] = _pct_diff(
        out['phoebe_margins'], out['zenodo_margins']
    )
    out['pct_v0_3_vs_zenodo'] = _pct_diff(out['v0_3_margins'], out['zenodo_margins'])
    out['pct_v0_3_vs_phoebe'] = _pct_diff(out['v0_3_margins'], out['phoebe_margins'])

    return out.reset_index()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dollar_year', type=int, default=2024)
    parser.add_argument('--phoebe-sef-csv', type=Path, default=None)
    parser.add_argument('--v0-3-sef-csv', type=Path, default=None)
    parser.add_argument('--zenodo-xlsx', type=Path, default=None)
    parser.add_argument('--output-csv', type=Path, default=_DEFAULT_OUTPUT)
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

    if args.v0_3_sef_csv is None:
        logger.info('publishing %s at dollar_year=%d', _V0_3_CONFIG, args.dollar_year)
        v0_3_path = publish_sef(_V0_3_CONFIG, args.dollar_year)
    else:
        v0_3_path = args.v0_3_sef_csv
    logger.info('v0.3 SEF: %s', v0_3_path)
    v0_3 = load_bedrock_sef(v0_3_path)

    comparison = build_margins_comparison(zenodo, phoebe, v0_3)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(args.output_csv, index=False)

    _summarize_vs_zenodo(
        'phoebe',
        comparison.set_index('useeio_code')['phoebe_margins'],
        comparison.set_index('useeio_code')['zenodo_margins'],
    )
    _summarize_vs_zenodo(
        'v0_3',
        comparison.set_index('useeio_code')['v0_3_margins'],
        comparison.set_index('useeio_code')['zenodo_margins'],
    )
    logger.info(
        'rows=%d (zenodo=%d phoebe=%d v0_3=%d)',
        len(comparison),
        int(comparison['zenodo_margins'].notna().sum()),
        int(comparison['phoebe_margins'].notna().sum()),
        int(comparison['v0_3_margins'].notna().sum()),
    )
    logger.info('wrote %s', args.output_csv)
    logger.info('Zenodo source: https://doi.org/%s', ZENODO_DOI)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
