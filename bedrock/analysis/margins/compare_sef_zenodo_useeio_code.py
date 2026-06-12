"""Compare bedrock SEF export to Zenodo v1.4.0 on Reference USEEIO Code.

Zenodo publishes NAICS-6 rows mapped to ``Reference USEEIO Code`` (BEA detail).
Rows whose reference field lists **multiple** codes (comma-separated, e.g.
``230301, 233230``) are dropped. Remaining rows collapse to one value per
reference code (mean across NAICS rows sharing the same code). Bedrock joins on
the same USEEIO commodity codes.

Usage (PowerShell, repo root)::

    uv run python -m bedrock.analysis.margins.compare_sef_zenodo_useeio_code \\
        --config_name useeio_phoebe_23 --dollar_year 2024

Optional: ``--zenodo-xlsx PATH`` (defaults to cached download under
``bedrock/utils/snapshots/data/zenodo_sef_v1.4.0/``).
"""

from __future__ import annotations

import argparse
import json
import logging
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_PKG_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PKG_DIR.parents[3]
_ZENODO_RECORD_ID = 17202747
_ZENODO_FILENAME = 'SupplyChainGHGEmissionFactorsv1.4.0.xlsx'
_ZENODO_DOI = '10.5281/zenodo.17202747'
_CACHE_DIR = (
    _REPO_ROOT / 'bedrock' / 'utils' / 'snapshots' / 'data' / 'zenodo_sef_v1.4.0'
)
_DEFAULT_OUTPUT = _PKG_DIR / 'output' / 'sef_zenodo_useeio_code_comparison.csv'

_REF_CODE_COL = 'Reference USEEIO Code'
_ZENODO_WITHOUT_COL = 'Supply Chain Emission Factors without Margins'
_BEDROCK_CODE_COL = 'Cornerstone Commodity Code'
_BEDROCK_WITHOUT_COL = 'Supply Chain Emission Factors without Margins'


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


def load_zenodo_without_margins_by_reference_code(xlsx_path: Path) -> pd.Series:
    """Zenodo without-margins SEF indexed by Reference USEEIO Code."""
    raw = pd.read_excel(xlsx_path, sheet_name='CO2e', engine='openpyxl')
    if _REF_CODE_COL not in raw.columns:
        raise KeyError(
            f'CO2e sheet missing {_REF_CODE_COL!r}; columns={list(raw.columns)!r}'
        )
    if _ZENODO_WITHOUT_COL not in raw.columns:
        raise KeyError(
            f'CO2e sheet missing {_ZENODO_WITHOUT_COL!r}; columns={list(raw.columns)!r}'
        )
    df = raw[[_REF_CODE_COL, _ZENODO_WITHOUT_COL]].copy()
    df[_REF_CODE_COL] = df[_REF_CODE_COL].astype(str).str.strip()
    df[_ZENODO_WITHOUT_COL] = pd.to_numeric(df[_ZENODO_WITHOUT_COL], errors='coerce')
    df = df.dropna(subset=[_REF_CODE_COL, _ZENODO_WITHOUT_COL])

    multi_code_rows = df[_REF_CODE_COL].map(_reference_code_lists_multiple_codes)
    n_multi_code = int(multi_code_rows.sum())
    if n_multi_code > 0:
        logger.info(
            'excluding %d Zenodo rows whose Reference USEEIO Code lists multiple codes',
            n_multi_code,
        )
    df = df.loc[~multi_code_rows]

    out = (
        df.groupby(_REF_CODE_COL, sort=True)[_ZENODO_WITHOUT_COL]
        .mean()
        .astype(float)
    )
    out.index.name = 'useeio_code'
    return out


def load_bedrock_without_margins(sef_csv: Path) -> pd.Series:
    table = pd.read_csv(sef_csv)
    if _BEDROCK_CODE_COL not in table.columns:
        raise KeyError(f'SEF CSV missing {_BEDROCK_CODE_COL!r}')
    codes = table[_BEDROCK_CODE_COL].astype(str).str.strip().str.removesuffix('/US')
    values = pd.to_numeric(table[_BEDROCK_WITHOUT_COL], errors='coerce')
    return pd.Series(values.values, index=codes, name='bedrock_without_margins')


def compare_series(
    bedrock: pd.Series,
    zenodo: pd.Series,
) -> pd.DataFrame:
    common = bedrock.index.intersection(zenodo.index)
    out = pd.DataFrame(
        {
            'useeio_code': common,
            'bedrock_without_margins': bedrock.reindex(common).astype(float),
            'zenodo_without_margins': zenodo.reindex(common).astype(float),
        }
    )
    out['abs_diff'] = out['bedrock_without_margins'] - out['zenodo_without_margins']
    denom = out['zenodo_without_margins'].replace(0.0, np.nan)
    out['perc_diff'] = out['abs_diff'] / denom
    return out.sort_values('useeio_code').reset_index(drop=True)


def _publish_sef(config_name: str, dollar_year: int) -> Path:
    import bedrock.utils.config.common as common
    from bedrock.publish.cache_reset import clear_all_publish_caches
    from bedrock.publish.emission_factors.writer import write_emission_factors
    from bedrock.utils.config.settings import GIT_HASH_LONG
    from bedrock.utils.config.usa_config import set_global_usa_config

    if not GIT_HASH_LONG:
        raise RuntimeError('GIT_HASH_LONG is not set')
    out_dir = (
        _REPO_ROOT / 'bedrock' / 'publish' / 'output' / GIT_HASH_LONG / config_name
    )
    clear_all_publish_caches()
    set_global_usa_config(config_name)
    common.download_fba_on_api_error = True
    paths = write_emission_factors(
        str(out_dir),
        config_name=config_name,
        dollar_year=dollar_year,
        purchaser_price=True,
    )
    return Path(paths['co2e'])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config_name', default='useeio_phoebe_23')
    parser.add_argument('--dollar_year', type=int, default=2024)
    parser.add_argument('--sef-csv', type=Path, default=None)
    parser.add_argument('--zenodo-xlsx', type=Path, default=None)
    parser.add_argument('--output-csv', type=Path, default=_DEFAULT_OUTPUT)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

    zenodo_path = ensure_zenodo_xlsx_local(args.zenodo_xlsx)
    zenodo = load_zenodo_without_margins_by_reference_code(zenodo_path)

    if args.sef_csv is None:
        sef_path = _publish_sef(args.config_name, args.dollar_year)
    else:
        sef_path = args.sef_csv
    logger.info('bedrock SEF: %s', sef_path)

    bedrock = load_bedrock_without_margins(sef_path)
    comparison = compare_series(bedrock, zenodo)

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    comparison.to_csv(args.output_csv, index=False)

    perc = comparison['perc_diff'].dropna()
    logger.info(
        'joined sectors=%d bedrock-only=%d zenodo-only=%d',
        len(comparison),
        len(bedrock.index.difference(zenodo.index)),
        len(zenodo.index.difference(bedrock.index)),
    )
    if not perc.empty:
        logger.info(
            'perc_diff vs Zenodo (Reference USEEIO Code): median=%.4f std=%.4f',
            float(perc.median()),
            float(perc.std()),
        )
    logger.info('wrote %s', args.output_csv)
    logger.info('Zenodo source: https://doi.org/%s', _ZENODO_DOI)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
