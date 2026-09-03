"""Key-sector total EF (N) over nowcast model years 2017-2024.

Cheap path: derive B + A per year (no full snapshot build), inflate N to 2024$,
write CSV + line chart next to this file. Each row records the NowcastMUT
vintage the year resolved to, since the nowcast YAMLs leave it unpinned.

PNG/CSV are gitignored (repo-wide ``*.png`` / ``*.csv`` plus this folder's
``.gitignore``).

::

    uv run python -m bedrock.analysis.nowcasting.results.nowcast_key_sector_n_series
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from bedrock.analysis.nowcasting.results._ef_smoke_lib import (
    COMPARE_DOLLAR_YEAR,
    NOWCAST_YEARS,
    SECTOR_LABELS,
    SECTORS,
    config_stem,
    efs_from_live_config,
)
from bedrock.utils.validation.analysis.plotting import setup_mpl
from bedrock.utils.validation.diagnostics_helpers import (
    inflation_adjust_ef_denom_to_new_base_year,
)

OUT_DIR = Path(__file__).resolve().parent


def main(*, inflate_to: int = COMPARE_DOLLAR_YEAR) -> None:
    rows: list[dict[str, object]] = []
    for year in NOWCAST_YEARS:
        print(f'deriving {config_stem(year)} ...')
        efs = efs_from_live_config(year)
        N = efs.N
        if inflate_to != year:
            N = inflation_adjust_ef_denom_to_new_base_year(
                N, new_base_year=inflate_to, old_base_year=year
            )
        for code in SECTORS:
            if code not in N.index:
                print(f'  WARN: {code} missing in {year}')
                continue
            rows.append(
                {
                    'year': year,
                    'sector': code,
                    'sector_label': SECTOR_LABELS.get(code, code),
                    'N': float(N.loc[code]),
                    'dollar_year': inflate_to,
                    'mut_vintage': efs.mut_vintage,
                }
            )

    df = pd.DataFrame(rows)
    csv_path = OUT_DIR / f'key_sector_N_series_to_{inflate_to}.csv'
    df.to_csv(csv_path, index=False)
    print('wrote', csv_path)

    setup_mpl()
    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    for code in SECTORS:
        sub = df[df['sector'] == code].sort_values('year')
        if sub.empty:
            continue
        ax.plot(
            sub['year'],
            sub['N'],
            marker='o',
            label=SECTOR_LABELS.get(code, code),
        )
    ax.set_xlabel('Model year (nowcast IO + GHG)')
    ax.set_ylabel(f'N (kg CO2e / {inflate_to}$)')
    ax.set_title(f'Key-sector total EF (N): nowcast configs, inflated to {inflate_to}$')
    ax.legend(loc='best', fontsize=8, ncol=2)
    ax.set_xticks(list(NOWCAST_YEARS))
    fig.tight_layout()
    png = OUT_DIR / f'key_sector_N_series_to_{inflate_to}.png'
    fig.savefig(png, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print('wrote', png)

    # Index each sector to the first year in the series (2017, the BEA benchmark
    # year rebuilt through the nowcast pipeline).
    base_year = int(min(NOWCAST_YEARS))
    indexed_rows: list[dict[str, object]] = []
    for sector_key, sub in df.groupby('sector'):
        code_s = str(sector_key)
        sub = sub.sort_values('year')
        base = sub.loc[sub['year'] == base_year, 'N']
        if base.empty or float(base.iloc[0]) == 0.0:
            print(f'  WARN: cannot index {code_s} to {base_year}')
            continue
        n0 = float(base.iloc[0])
        for _, r in sub.iterrows():
            indexed_rows.append(
                {
                    'year': int(r['year']),
                    'sector': code_s,
                    'sector_label': str(r['sector_label']),
                    'N': float(r['N']),
                    'N_index': float(r['N']) / n0,
                    'base_year': base_year,
                    'dollar_year': inflate_to,
                    'mut_vintage': r['mut_vintage'],
                }
            )
    idx_df = pd.DataFrame(indexed_rows)
    idx_csv = OUT_DIR / f'key_sector_N_series_indexed_to_{base_year}.csv'
    idx_df.to_csv(idx_csv, index=False)
    print('wrote', idx_csv)

    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    for code in SECTORS:
        sub = idx_df[idx_df['sector'] == code].sort_values('year')
        if sub.empty:
            continue
        ax.plot(
            sub['year'],
            sub['N_index'],
            marker='o',
            label=SECTOR_LABELS.get(code, code),
        )
    ax.axhline(1.0, color='black', lw=0.8, ls='--', alpha=0.6)
    ax.set_xlabel('Model year (nowcast IO + GHG)')
    ax.set_ylabel(f'N index ({base_year} = 1), {inflate_to}$ denominators')
    ax.set_title(f'Key-sector total EF (N), indexed to {base_year}=1')
    ax.legend(loc='best', fontsize=8, ncol=2)
    ax.set_xticks(list(NOWCAST_YEARS))
    fig.tight_layout()
    idx_png = OUT_DIR / f'key_sector_N_series_indexed_to_{base_year}.png'
    fig.savefig(idx_png, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print('wrote', idx_png)


if __name__ == '__main__':
    main()
