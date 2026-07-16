"""Side-by-side margin SEF: Zenodo v1.4.0, Phoebe rebuild, Cornerstone v0.3.

Joins on Reference USEEIO Code (same multi-code drop / mean collapse as
``compare_sef_zenodo_useeio_code``). Values are purchaser-price CO2e factors at
``--dollar_year`` (default 2024, matching Zenodo's published unit).

Usage (PowerShell, repo root)::

    uv run python -m bedrock.analysis.margins.compare_sef_margins_sources

Optional:
  --phoebe-sef-csv PATH
  --v0-3-sef-csv PATH
  --zenodo-xlsx PATH
  --dollar_year 2024
  --output-csv PATH

Outputs:
  output/sef_margins_zenodo_phoebe_v0_3.csv
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from bedrock.analysis.margins.compare_sef_zenodo_useeio_code import (
    COL_MARGINS,
    COL_WITHOUT,
    ZENODO_DOI,
    ensure_zenodo_xlsx_local,
    load_bedrock_sef,
    load_zenodo_sef_by_reference_code,
    publish_sef,
)

logger = logging.getLogger(__name__)

_PKG_DIR = Path(__file__).resolve().parent
_DEFAULT_OUTPUT = _PKG_DIR / 'output' / 'sef_margins_zenodo_phoebe_v0_3.csv'

_PHOEBE_CONFIG = 'useeio_phoebe_23'
_V0_3_CONFIG = '2025_usa_cornerstone_v0_3'


def _pct_diff(numer: pd.Series, denom: pd.Series) -> pd.Series:
    return (numer - denom) / denom.replace(0.0, np.nan)


def _summarize_vs_zenodo(label: str, margins: pd.Series, zenodo: pd.Series) -> None:
    common = margins.index.intersection(zenodo.index)
    paired = pd.concat(
        [margins.reindex(common), zenodo.reindex(common)], axis=1
    ).dropna()
    b = paired.iloc[:, 0].astype(float)
    r = paired.iloc[:, 1].astype(float)
    pct = _pct_diff(b, r).replace([np.inf, -np.inf], np.nan).dropna()
    sum_ratio = float(b.sum() / r.sum()) if float(r.sum()) != 0.0 else float('nan')
    logger.info(
        '%s vs zenodo: joined=%d sum_ratio=%.4f corr=%.4f '
        'median_pct=%.4f median_abs_pct=%.4f within_5pct=%d/%d',
        label,
        len(common),
        sum_ratio,
        float(b.corr(r)) if len(common) > 1 else float('nan'),
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

    out['pct_phoebe_vs_zenodo'] = _pct_diff(out['phoebe_margins'], out['zenodo_margins'])
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
        logger.info('publishing %s at dollar_year=%d', _PHOEBE_CONFIG, args.dollar_year)
        phoebe_path = publish_sef(_PHOEBE_CONFIG, args.dollar_year)
    else:
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
