"""Grade service trade by summary row against BEA's published annual tables (#658).

#771 builds every service trade row as its published 2017 value moved by a
growth blend of the ITA categories that feed it. That makes 2017 exact by
construction, so a 2017 comparison says nothing about the movers. BEA
estimates the **summary** supply-use tables every year, so the published
summary rows are the annual answer key: this module rolls a balanced build's
service exports and imports up to summary rows and grades 2018-2024.

What is graded
--------------

- **Exports:** the balanced Use SUT's ``F04000``, against the published
  summary Use SUT's ``F040``.
- **Imports:** the balanced Supply's ``MCIF``, against the published summary
  Supply's ``MCIF``.

Both sides are read at purchasers' prices in BEA's own layout, so the margin
rows compare like for like. Service rows are the summary commodities from
``42`` onward, without government. ``Other`` and ``Used`` are reported
separately below the table.

⚠️ **The reference is the release the build reads** (``as_read``). Grading a
build against a different BEA release mixes its revisions into the errors:
on ``v1.0.0_5514c3b``, air transportation (``481``) imports read 28% short
against the release before the 2026 annual update, and within 2% against the
release the build was made from.

⚠️ **A one-sided test** (#658). BEA builds its summary trade rows partly from
the same ITA tables, so a good score says we reproduce BEA's construction. A
bad score is the informative direction.

What it finds on ``v1.0.0_5514c3b``
-----------------------------------

**Service totals are close in both directions.** Service exports sit within
+0.7% to -3.5% of BEA, and service imports 0.6-6.1% under, every year. #785
conditions ``MCIF`` on the published summary Supply table, so the import
errors are the balance moving cells off that control; no service import row
misses by more than 6.1bn a year on average.

**The export rows carry the error**, because ``F04000`` is not conditioned and
row errors offset in the total. Rows with a mean absolute error over 1bn,
2018-2024:

==========  ===============================  ==========  ===========
row         commodity                        MAE $M      mean err %
==========  ===============================  ==========  ===========
``523``     securities and investments           7,204         +7.0
``622``     hospitals                            6,404        -77.0
``521CI``   banking and credit                   6,404         -6.5
``5412OP``  other professional services          5,539         -2.8
``5415``    computer systems design              4,662         -6.9
``514``     data processing and information      2,872        +42.3
``524``     insurance carriers                   2,813         -9.8
``561``     administrative and support           1,834        -37.9
``55``      management of companies              1,558        +41.1
``532RL``   rental and leasing                   1,172         +1.1
``512``     motion pictures and sound            1,052         +4.2
``ORE``     other real estate                    1,042        +34.3
``5411``    legal services                       1,037         +5.9
==========  ===============================  ==========  ===========

- ``622`` hospitals is the largest relative miss, with ``621`` ambulatory
  care (-76.9%) beside it. The hospital mover is ITA ``TravelHealth``, 1.1bn
  in 2017, which collapsed in 2020. BEA's hospital exports rose every year,
  from 2.8bn in 2017 to 14.0bn in 2024, through the pandemic as well. No ITA
  travel series moves that way, so BEA is not booking this row from medical
  travel alone.
- ``523`` and ``521CI`` miss by equal and opposite amounts (+5.8bn and
  -5.2bn a year on average). That is the signature of a split between them,
  not two independent mover errors: the export crosswalk maps the ITA
  ``Financial`` categories across both rows.

``Other`` is BEA's rest-of-world adjustment row. Its export cell (``S00900``,
-204bn in 2017) enters at Step 6 (#1158), after the balance, so the balanced
tables carry none of it, and it is left out of the totals above.

CLI::

    uv run python -m bedrock.analysis.nowcasting.trade_data.service_trade_vs_published
    uv run python -m bedrock.analysis.nowcasting.trade_data.service_trade_vs_published \\
        --build v1.0.0_5514c3b --check
"""

from __future__ import annotations

import argparse
import functools
from pathlib import Path

import pandas as pd

from bedrock.extract.iot.io_2017 import _load_usa_summary_sut
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)

YEARS: tuple[int, ...] = tuple(range(2017, 2025))
GRADED_YEARS: tuple[int, ...] = YEARS[1:]

#: Summary rows reported apart from the service rows.
SEPARATE_ROWS = ('Other', 'Used')

#: The build the docstring figures come from, and what ``--check`` reproduces.
DOCUMENTED_BUILD = 'v1.0.0_5514c3b'
DOCUMENTED_EXPORTS_MAE_USD_M = {
    '523': 7_204,
    '622': 6_404,
    '521CI': 6_404,
    '5412OP': 5_539,
    '5415': 4_662,
    '514': 2_872,
    '524': 2_813,
    '561': 1_834,
    '55': 1_558,
    '532RL': 1_172,
    '512': 1_052,
    'ORE': 1_042,
    '5411': 1_037,
}
#: Service totals, ours / published - 1, every year 2017-2024.
DOCUMENTED_TOTAL_GAP_RANGE = {'exports': (-0.035, 0.007), 'imports': (-0.061, -0.006)}


@functools.cache
def _to_summary() -> pd.Series:
    mapping = load_bea_v2017_commodity_to_bea_v2017_summary()
    return pd.Series({code: parents[0] for code, parents in mapping.items()})


def service_rows() -> list[str]:
    """Summary commodity rows from ``42`` onward, without government."""
    goods = ('111', '113', '21', '22', '23', '3')
    return sorted(
        {s for s in _to_summary() if not s.startswith(goods) and not s.startswith('G')}
        - set(SEPARATE_ROWS)
    )


def default_build() -> str:
    """The balanced build the canonical config ships, e.g. ``v1.0.0_5514c3b``."""
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    build = get_usa_config().nowcast_mut_vintage
    if not build:
        raise ValueError('the active config names no nowcast_mut_vintage; pass --build')
    return build


@functools.cache
def balanced_table(table: str, year: int, build: str) -> pd.DataFrame:
    """``Balanced_Detail_<table>_<year>_<build>``, $M, fetched from GCS if absent."""
    from bedrock.transform.iot.nowcast_sut_assembly import (  # noqa: PLC0415
        GCS_BALANCED_SUT_DIR,
    )
    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415
    from bedrock.utils.io.gcp import download_gcs_file  # noqa: PLC0415

    name = f'Balanced_Detail_{table}_{year}_{build}.parquet'
    path = Path(FBS_DIR) / name
    if not path.exists():
        download_gcs_file(name, GCS_BALANCED_SUT_DIR, str(path))
    return pd.read_parquet(path).astype(float)


def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    stripped = {str(c).strip(): c for c in frame.columns}
    return frame[stripped[name]]


def _published(table: str, year: int, column: str) -> pd.Series:
    frame = _load_usa_summary_sut(table, year)  # type: ignore[arg-type]
    frame.index = pd.Index([str(i).strip() for i in frame.index])
    return pd.to_numeric(_column(frame, column), errors='coerce').fillna(0.0)


def trade_by_row(build: str) -> pd.DataFrame:
    """Service exports and imports by summary row and year, ours and published, $M."""
    to_summary = _to_summary()
    rows = service_rows() + list(SEPARATE_ROWS)
    records = []
    for year in YEARS:
        exports = (
            _column(balanced_table('Use_SUT', year, build), 'F04000')
            .groupby(to_summary)
            .sum()
        )
        imports = (
            _column(balanced_table('Supply', year, build), 'MCIF')
            .groupby(to_summary)
            .sum()
        )
        published_exports = _published('Use_SUT_summary', year, 'F040')
        published_imports = _published('Supply_summary', year, 'MCIF')
        for row in rows:
            for flow, ours, theirs in (
                ('exports', exports, published_exports),
                ('imports', imports, published_imports),
            ):
                records.append(
                    {
                        'year': year,
                        'row': row,
                        'flow': flow,
                        'ours': float(ours.get(row, 0.0)),
                        'published': float(theirs.get(row, 0.0)),
                    }
                )
    table = pd.DataFrame(records)
    table['error'] = table['ours'] - table['published']
    return table


def grade(table: pd.DataFrame, flow: str) -> pd.DataFrame:
    """Per service row, 2018-2024: mean absolute error, mean error and its share."""
    span = table[
        (table['flow'] == flow)
        & table['year'].isin(GRADED_YEARS)
        & ~table['row'].isin(SEPARATE_ROWS)
    ]
    graded = span.groupby('row').agg(
        mae=('error', lambda e: e.abs().mean()),
        mean_error=('error', 'mean'),
        published_mean=('published', 'mean'),
    )
    graded['mean_error_share'] = graded['mean_error'] / graded['published_mean'].where(
        graded['published_mean'].abs() > 1.0
    )
    return graded.sort_values('mae', ascending=False)


def service_totals(table: pd.DataFrame, flow: str) -> pd.DataFrame:
    """Service totals by year, ours and published, without the separate rows."""
    rows = table[(table['flow'] == flow) & ~table['row'].isin(SEPARATE_ROWS)]
    totals = rows.groupby('year')[['ours', 'published']].sum()
    totals['gap'] = totals['ours'] / totals['published'] - 1
    return totals


def report(build: str) -> pd.DataFrame:
    table = trade_by_row(build)
    pd.set_option('display.width', 200)
    for flow in ('exports', 'imports'):
        graded = grade(table, flow)
        print(f'\n== {flow}, build {build}: service rows, 2018-2024, $M')
        print(
            graded[graded['mae'] > 500].to_string(
                float_format=lambda x: f'{x:,.1f}',
                formatters={'mean_error_share': lambda x: f'{x:+.1%}'},
            )
        )
        print('\nservice total by year, $M:')
        print(
            service_totals(table, flow).T.to_string(
                float_format=lambda x: f'{x:,.3f}' if abs(x) < 1 else f'{x:,.0f}'
            )
        )
        separate = table[(table['flow'] == flow) & table['row'].isin(SEPARATE_ROWS)]
        print('\nreported apart, ours / published, $M:')
        print(
            separate.pivot(index='row', columns='year', values=['ours', 'published'])
            .round(0)
            .to_string()
        )
    return table


def check(build: str) -> int:
    """Reproduce the docstring figures; non-zero exit if any has moved."""
    if build != DOCUMENTED_BUILD:
        print(f'--check reproduces {DOCUMENTED_BUILD}, not {build}')
        return 1
    table = trade_by_row(build)
    failures = []
    exports = grade(table, 'exports')['mae']
    for row, documented in DOCUMENTED_EXPORTS_MAE_USD_M.items():
        if abs(exports[row] - documented) > 1.0:
            failures.append(f'exports {row}: MAE {exports[row]:,.1f} vs {documented:,}')
    for flow, (low, high) in DOCUMENTED_TOTAL_GAP_RANGE.items():
        gaps = service_totals(table, flow)['gap']
        if not gaps.between(low - 5e-4, high + 5e-4).all():
            failures.append(
                f'{flow} service total outside {low:+.1%}..{high:+.1%}: '
                f'{gaps.round(4).to_dict()}'
            )
    if failures:
        print('docstring figures moved:\n  ' + '\n  '.join(failures))
        return 1
    print(f'OK: docstring figures reproduce on {build}')
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--build',
        help="balanced build label (default: the canonical config's nowcast_mut_vintage)",
    )
    parser.add_argument('--check', action='store_true', help='reproduce the docstring')
    args = parser.parse_args()
    build = args.build or default_build()
    raise SystemExit(check(build) if args.check else (report(build) is None))
