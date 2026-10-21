"""Compare bedrock's per-source totals with CEDA's own source-level E.

Applies the crosswalk to the FBS row by row, values every gas in AR5 CO2e (the
basis of CEDA's ``E_USA_ES`` snapshots), and sets each CEDA source's bedrock
total beside CEDA v7.1 and v8.1. Bedrock is shown for all sectors and for
industries only: CEDA's matrix and bedrock's published E both leave out final
demand (``F*``, mainly households).

The CEDA snapshots are older inventory years than the 2024 FBS, so a few
percent of difference is real change between years. What this is for is a
source at zero on one side, or several times the other.

Usage::

    python -m bedrock.analysis.emissions_source_crosswalk.compare_with_ceda \
        <retained FBS parquet> --shares <shares csv> [--ceda-root ../ceda]
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from bedrock.analysis.emissions_source_crosswalk.apply_crosswalk import (
    KEYS,
    SPECIES_AR5_KEY,
    apply_crosswalk,
    fbs_keys,
    load_crosswalk,
    speciate_unspecified_hfcs,
    split_nested_lines,
)
from bedrock.analysis.emissions_source_crosswalk.inventory_fbs_sources import (
    OUTPUT_DIR,
    load_ceda_sources,
)
from bedrock.utils.emissions.gwp import GWP100_AR5_CEDA

# CEDA snapshot keys under ceda/snapshots/snapshot_data (see ceda/snapshots/releases.py).
CEDA_SNAPSHOTS = {
    'ceda_v7_1': 'historical/c422c0a23c4794c6c338d70217585a3eae1a7ac0',
    'ceda_v8_1': '1f5997da00e94b89633c125374b16d2aa4c37fe3',
}
AR5_KEY = {
    'Carbon dioxide': 'CO2',
    'Methane': 'CH4',
    'Nitrous oxide': 'N2O',
    'Sulfur hexafluoride': 'SF6',
    'Nitrogen trifluoride': 'NF3',
    **SPECIES_AR5_KEY,
}
RATIO_FLAG = 2.0  # flag a source when the two sides differ by more than this factor
MIN_MT = 0.5  # ignore sources smaller than this on both sides


def main() -> None:
    parser = argparse.ArgumentParser(description=(__doc__ or '').splitlines()[0])
    parser.add_argument('fbs', type=Path)
    parser.add_argument('--shares', type=Path, required=True)
    parser.add_argument(
        '--ceda-root',
        type=Path,
        default=Path(__file__).resolve().parents[3].parent / 'ceda',
    )
    args = parser.parse_args()

    ceda = load_ceda_sources(args.ceda_root)
    bedrock = bedrock_by_source(args.fbs, args.shares, set(ceda['code']))
    table = compare(bedrock, ceda_snapshots(args.ceda_root), ceda)

    OUTPUT_DIR.mkdir(exist_ok=True)
    out = OUTPUT_DIR / f'{args.fbs.stem}_vs_ceda.csv'
    table.to_csv(out, index=False)
    _print_summary(table)
    print(f'\nWrote {out}')


def bedrock_by_source(
    fbs_path: Path, shares_path: Path, codes: set[str]
) -> pd.DataFrame:
    """AR5 CO2e per CEDA source, all sectors and industries only (Mt)."""
    fbs = pd.read_parquet(fbs_path)
    shares = pd.read_csv(shares_path, dtype=str, keep_default_na=False).astype(
        {'share': float}
    )
    fbs = speciate_unspecified_hfcs(split_nested_lines(fbs, shares))
    fbs = fbs.assign(
        ActivityProducedBy=fbs['ActivityProducedBy'].astype(object).fillna('')
    )
    mapped = apply_crosswalk(fbs_keys(fbs), load_crosswalk(codes))
    rows = fbs.merge(
        mapped[[*KEYS, 'ceda_source', 'status']], on=KEYS, how='left', validate='m:1'
    )
    rows = rows[rows['status'] == 'mapped']

    gwp = rows['Flowable'].map(AR5_KEY).map(GWP100_AR5_CEDA)
    rows = rows.assign(
        ar5=rows['FlowAmount'] * gwp.where(rows['Unit'] == 'kg', 1.0),
        industry=~rows['SectorProducedBy'].astype(str).str.startswith('F'),
    )
    if rows['ar5'].isna().any():
        missing = rows.loc[rows['ar5'].isna(), 'Flowable'].unique()
        raise ValueError(f'No AR5 GWP for {list(missing)}')
    return pd.DataFrame(
        {
            'bedrock_all': rows.groupby('ceda_source')['ar5'].sum() / 1e9,
            'bedrock_industries': rows[rows['industry']]
            .groupby('ceda_source')['ar5']
            .sum()
            / 1e9,
        }
    ).fillna(0.0)


def ceda_snapshots(ceda_root: Path) -> pd.DataFrame:
    """Per-source totals (Mt AR5 CO2e) of the cached CEDA ``E_USA_ES`` snapshots."""
    data = ceda_root / 'ceda' / 'snapshots' / 'snapshot_data'
    return pd.DataFrame(
        {
            name: pd.read_parquet(data / key / 'E_USA_ES.parquet').sum(axis=1) / 1e9
            for name, key in CEDA_SNAPSHOTS.items()
        }
    )


def compare(
    bedrock: pd.DataFrame, snapshots: pd.DataFrame, ceda: pd.DataFrame
) -> pd.DataFrame:
    table = (
        ceda.set_index('code')[['description', 'gas']]
        .join(bedrock)
        .join(snapshots)
        .fillna(0.0)
    )
    ref = table['ceda_v8_1']
    ours = table['bedrock_industries']
    table['ratio_vs_v8_1'] = ours / ref.where(ref != 0)
    big = (ours.abs() >= MIN_MT) | (ref.abs() >= MIN_MT)
    off = (table['ratio_vs_v8_1'] > RATIO_FLAG) | (
        table['ratio_vs_v8_1'] < 1 / RATIO_FLAG
    )
    one_sided = (ours == 0) != (ref == 0)
    table['flag'] = (big & (off | one_sided)).map({True: 'check', False: ''})
    return (
        table.rename_axis('ceda_source')
        .reset_index()
        .sort_values('ceda_v8_1', ascending=False)
        .reset_index(drop=True)
    )


def _print_summary(table: pd.DataFrame) -> None:
    cols = ['bedrock_all', 'bedrock_industries', 'ceda_v7_1', 'ceda_v8_1']
    print('Totals, Mt CO2e (AR5):')
    print(table[cols].sum().round(1).to_string())
    by_gas = table.groupby(
        table['gas'].where(~table['gas'].str.match('HFC|C[0-9]F|CF4'), 'HFC/PFC')
    )[cols].sum()
    print('\nBy gas:')
    print(by_gas.round(1).to_string())
    flagged = table[table['flag'] == 'check']
    print(
        f'\nFlagged ({len(flagged)}): zero on one side, or off by more than {RATIO_FLAG}×'
    )
    print(
        flagged[
            [
                'ceda_source',
                'bedrock_industries',
                'ceda_v7_1',
                'ceda_v8_1',
                'ratio_vs_v8_1',
            ]
        ]
        .round(2)
        .to_string(index=False)
    )


if __name__ == '__main__':
    main()
