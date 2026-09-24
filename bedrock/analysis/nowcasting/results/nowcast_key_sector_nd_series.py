"""Key-sector domestic-only total EF (N_d) over nowcast model years 2017-2024.

Same cheap path as :mod:`nowcast_key_sector_n_series`, with one change that is
the whole point of the script: the Leontief inverse is built from ``Adom``
alone rather than from ``Adom + Aimp``.

Why a domestic-only series is worth having next to ``N``
--------------------------------------------------------

``N`` moves for two reasons at once: the domestic technology changed, or the
import assumption changed. Only the first is something this model observes.
``Aimp`` here carries the proportionality assumption of
:func:`~bedrock.transform.iot.nowcast_mut.import_matrix` and a *domestic*
``B`` applied to imported goods; the country split and country-specific
emissions live in CEDA, not in bedrock (see the nowcast methods paper, Step 6).
So ``N_d`` is the part of the movement this build can defend on its own, and
``N - N_d`` is the part that rides on the import treatment.

⚠️ ``N_d`` is **not** a footprint. It is the total EF an economy would carry if
imported inputs contributed nothing, so it is a lower bound on ``N`` and is
only interpretable as a *comparison across years of the same construction*.
Do not publish it as "the domestic footprint".

Outputs, written next to this file (gitignored, like the ``N`` series):

``key_sector_Nd_series_to_<yr>.csv`` / ``.png``
    N_d by sector and year, inflated to a common dollar year.
``key_sector_Nd_series_indexed_to_2017.csv`` / ``.png``
    The same, indexed to the first year, which is how the technology movement
    reads without the level differences between sectors swamping it.
``key_sector_import_share_of_N.csv`` / ``.png``
    ``1 - N_d/N`` per sector and year: how much of the total EF the import
    treatment is carrying.

::

    uv run python -m bedrock.analysis.nowcasting.results.nowcast_key_sector_nd_series
    uv run python -m bedrock.analysis.nowcasting.results.nowcast_key_sector_nd_series --check

``--check`` runs the identities that would otherwise be a unit test (see
``bedrock/analysis`` convention: diagnostics carry their checks as a flag).
"""

from __future__ import annotations

import argparse
import time
import typing as ta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from bedrock.analysis.nowcasting.results._ef_smoke_lib import (
    COMPARE_DOLLAR_YEAR,
    NOWCAST_YEARS,
    SECTOR_LABELS,
    SECTORS,
    _as_float_series,
    activate_live_config,
    config_stem,
)
from bedrock.utils.validation.analysis.plotting import setup_mpl
from bedrock.utils.validation.diagnostics_helpers import (
    inflation_adjust_ef_denom_to_new_base_year,
)

OUT_DIR = Path(__file__).resolve().parent


class YearNd(ta.NamedTuple):
    """Total EF with and without imported inputs, for one model year."""

    N: pd.Series[float]
    N_domestic: pd.Series[float]
    mut_vintage: str | None


def nd_from_live_config(year: int) -> YearNd:
    """N and N_d for one nowcast year, from one derivation of B and A.

    Both vectors come from the same ``B`` and the same ``Adom``; only the
    matrix inverted differs, so any difference between them is the import
    block and nothing else.
    """
    from bedrock.transform.eeio.derived import (  # noqa: PLC0415
        derive_Aq_usa,
        derive_B_usa_non_finetuned,
    )
    from bedrock.utils.math.formulas import (  # noqa: PLC0415
        compute_L_matrix,
        compute_M_matrix,
        compute_n,
    )

    vintage = activate_live_config(year)
    t0 = time.time()
    B = derive_B_usa_non_finetuned()
    aq = derive_Aq_usa()

    n_total = _as_float_series(
        compute_n(M=compute_M_matrix(B=B, L=compute_L_matrix(A=aq.Adom + aq.Aimp)))
    )
    n_dom = _as_float_series(
        compute_n(M=compute_M_matrix(B=B, L=compute_L_matrix(A=aq.Adom)))
    )
    print(
        f'  year={year}: MUT vintage {vintage}; B/A/N in {time.time() - t0:.1f}s  '
        f'N.sum={float(n_total.sum()):.4g}  N_d.sum={float(n_dom.sum()):.4g}'
    )
    return YearNd(N=n_total, N_domestic=n_dom, mut_vintage=vintage)


def _line_chart(
    df: pd.DataFrame,
    value_col: str,
    ylabel: str,
    title: str,
    png_name: str,
    *,
    hline: float | None = None,
    legend_below: bool = False,
) -> None:
    setup_mpl()
    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    for code in SECTORS:
        sub = df[df['sector'] == code].sort_values('year')
        if sub.empty:
            continue
        ax.plot(
            sub['year'], sub[value_col], marker='o', label=SECTOR_LABELS.get(code, code)
        )
    if hline is not None:
        ax.axhline(hline, color='black', lw=0.8, ls='--', alpha=0.6)
    ax.set_xlabel('Model year (nowcast IO + GHG)')
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(list(NOWCAST_YEARS))
    if legend_below:
        ax.legend(
            loc='upper center',
            bbox_to_anchor=(0.5, -0.14),
            ncol=4,
            fontsize=8,
            frameon=False,
        )
    else:
        ax.legend(loc='best', fontsize=8, ncol=2)
    fig.tight_layout()
    path = OUT_DIR / png_name
    fig.savefig(path, dpi=150, bbox_inches='tight')
    plt.close(fig)
    print('wrote', path)


def build(inflate_to: int = COMPARE_DOLLAR_YEAR) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for year in NOWCAST_YEARS:
        print(f'deriving {config_stem(year)} ...')
        efs = nd_from_live_config(year)
        n_total, n_dom = efs.N, efs.N_domestic
        if inflate_to != year:
            n_total = inflation_adjust_ef_denom_to_new_base_year(
                n_total, new_base_year=inflate_to, old_base_year=year
            )
            n_dom = inflation_adjust_ef_denom_to_new_base_year(
                n_dom, new_base_year=inflate_to, old_base_year=year
            )
        for code in SECTORS:
            if code not in n_dom.index:
                print(f'  WARN: {code} missing in {year}')
                continue
            total, dom = float(n_total.loc[code]), float(n_dom.loc[code])
            rows.append(
                {
                    'year': year,
                    'sector': code,
                    'sector_label': SECTOR_LABELS.get(code, code),
                    'N': total,
                    'N_domestic': dom,
                    'import_share_of_N': (1.0 - dom / total) if total else float('nan'),
                    'dollar_year': inflate_to,
                    'mut_vintage': efs.mut_vintage,
                }
            )
    return pd.DataFrame(rows)


def check(df: pd.DataFrame) -> int:
    """Identities that hold by construction; a failure means the A split moved."""
    bad = 0
    over = df[df['N_domestic'] > df['N'] * (1.0 + 1e-9)]
    if len(over):
        bad += 1
        print(f'FAIL: N_d exceeds N on {len(over)} rows; Aimp cannot be negative mass')
        print(over[['year', 'sector', 'N', 'N_domestic']].to_string(index=False))
    neg = df[df['N_domestic'] < 0]
    if len(neg):
        bad += 1
        print(f'FAIL: negative N_d on {len(neg)} rows')
    missing = set(NOWCAST_YEARS) - set(df['year'])
    if missing:
        bad += 1
        print(f'FAIL: no rows for {sorted(missing)}')
    vintages = sorted({str(v) for v in df['mut_vintage']})
    print(f'MUT vintages resolved: {vintages}')
    if not bad:
        share = df['import_share_of_N']
        print(
            f'OK: {len(df)} rows, N_d <= N everywhere; import share of N runs '
            f'{share.min():.1%} to {share.max():.1%}, median {share.median():.1%}'
        )
    return bad


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--inflate-to', type=int, default=COMPARE_DOLLAR_YEAR)
    parser.add_argument(
        '--check',
        action='store_true',
        help='Run the construction identities and report, then exit.',
    )
    args = parser.parse_args()

    df = build(args.inflate_to)
    csv_path = OUT_DIR / f'key_sector_Nd_series_to_{args.inflate_to}.csv'
    df.to_csv(csv_path, index=False)
    print('wrote', csv_path)

    if args.check:
        return check(df)

    _line_chart(
        df,
        'N_domestic',
        f'N_d (kg CO2e / {args.inflate_to}$)',
        f'Key-sector domestic-only total EF (N_d), inflated to {args.inflate_to}$',
        f'key_sector_Nd_series_to_{args.inflate_to}.png',
    )

    base_year = int(min(NOWCAST_YEARS))
    idx_rows: list[dict[str, object]] = []
    for sector_key, sub in df.groupby('sector'):
        sub = sub.sort_values('year')
        base = sub.loc[sub['year'] == base_year, 'N_domestic']
        if base.empty or float(base.iloc[0]) == 0.0:
            print(f'  WARN: cannot index {sector_key} to {base_year}')
            continue
        n0 = float(base.iloc[0])
        for _, r in sub.iterrows():
            idx_rows.append(
                {
                    'year': int(r['year']),
                    'sector': str(sector_key),
                    'sector_label': str(r['sector_label']),
                    'N_domestic': float(r['N_domestic']),
                    'Nd_index': float(r['N_domestic']) / n0,
                    'base_year': base_year,
                    'dollar_year': args.inflate_to,
                    'mut_vintage': r['mut_vintage'],
                }
            )
    idx_df = pd.DataFrame(idx_rows)
    idx_csv = OUT_DIR / f'key_sector_Nd_series_indexed_to_{base_year}.csv'
    idx_df.to_csv(idx_csv, index=False)
    print('wrote', idx_csv)
    _line_chart(
        idx_df,
        'Nd_index',
        f'N_d index ({base_year} = 1)',
        f'Key-sector domestic-only total EF (N_d), indexed to {base_year}=1',
        f'key_sector_Nd_series_indexed_to_{base_year}.png',
        hline=1.0,
        legend_below=True,
    )

    share_csv = OUT_DIR / 'key_sector_import_share_of_N.csv'
    df[
        [
            'year',
            'sector',
            'sector_label',
            'N',
            'N_domestic',
            'import_share_of_N',
            'mut_vintage',
        ]
    ].to_csv(share_csv, index=False)
    print('wrote', share_csv)
    _line_chart(
        df,
        'import_share_of_N',
        'share of N carried by imported inputs',
        'How much of the total EF rides on the import treatment (1 - N_d/N)',
        'key_sector_import_share_of_N.png',
        legend_below=True,
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
