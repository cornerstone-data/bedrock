"""Grade each final-demand column total of a balanced nowcast build against BEA.

For every year of a named build, sum each final-demand column of the balanced
detail Use SUT, roll it to its four-character summary code, and compare it with
the same column of BEA's published summary Use SUT for that year.  Totals only:
the composition of a column is :mod:`~bedrock.analysis.nowcasting.sections`'
``use_summary_sut`` picture, which has no pass/fail.

Why a separate check
--------------------

``use_summary_sut`` grades cells and reads whichever balanced build is newest
on disk by modification time.  This module takes the build label explicitly
(default: the canonical config's ``nowcast_mut_vintage``) and fetches it from
GCS when it is not local, so the answer is about the tables the model ships.

⚠️ **Most final-demand columns cannot fail this check by construction.**  The
NIPA-built columns carry an observed annual total, and
:mod:`~bedrock.transform.iot.nowcast_fd_conditioning` scales them onto the
published summary composition, so they sit within 0.6% every year.  The
columns that can drift are the ones built outside NIPA: exports ``F040``
(trade data, not conditioned) and inventories ``F030`` (#746).  A column that
moves here has escaped its control, which is the point of grading totals.

What it finds on ``v0.5.0_a166975`` (2026-10-29)
------------------------------------------------

❌ **``F040`` exports run 2.6-10.3% short and ``F010`` PCE 0.6-1.5% high, in
every year** (#1158).  The rest-of-world adjustment ``S00900`` is held out of
the balance and re-enters the MUT as a zero row, so nonresidents' spending in
the United States stays in PCE instead of moving to exports.  BEA's ``Other``
row carries it: 204bn of exports in 2017, 79bn in 2020-21, 224bn in 2024.  The
gap follows international travel, so it bends the trend as well as the level.
Excluding that row, exports sit within 2% of BEA every year.  PCE is the
larger column, so the same dollars stay under the 2% bar there: ``F040``
fails, ``F010`` does not.

❌ **``F07S`` federal nondefense software investment is 27% short in 2023
alone** (5.3bn), against 0.0-0.4% in every other year.

⚠️ **``F030`` inventories misses by 2-28bn**, -22% to +64%.  The column is a
small net of large flows, so the relative gap is not meaningful by itself; the
dollar gap is what the tolerance below grades.

Tolerance
---------

A column fails when its gap exceeds **both** 2% of the published total and
1bn.  2% is the bar set for this review: a change that moves a commodity or
industry total by more than 2% is worth making before release.  The 1bn floor
keeps rounding on near-zero columns (``F06C``, ``F10N`` and the like) from
reading as failures.

⚠️ **Which BEA release is the reference matters.**  The default
(``--reference as_read``) is the workbook the build itself reads, so a pass
means the build hit its own controls.  ``--reference latest`` reads the
1997-2024 workbook for every year, the release gross output comes from; for
2019-2022 that widens the ``F040`` gap by up to 1.4 points, and private
software investment ``F02S`` in 2022 moves from +0.4% to -6.9% (#1157).

Run::

    uv run python -m bedrock.analysis.nowcasting.final_demand_vs_published
    uv run python -m bedrock.analysis.nowcasting.final_demand_vs_published --check
    uv run python -m bedrock.analysis.nowcasting.final_demand_vs_published \\
        --build v0.5.0_a166975 --reference latest
"""

from __future__ import annotations

import argparse
import functools
import typing as ta
from pathlib import Path

import pandas as pd

from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_commodity_summary import (
    USA_2017_SUMMARY_COMMODITY_CODES,
)
from bedrock.utils.taxonomy.bea.v2017_summary_final_demand import (
    USA_2017_SUMMARY_FINAL_DEMAND_CODES,
)

YEARS: tuple[int, ...] = tuple(range(2017, 2025))

#: A column fails when its gap exceeds both of these.  See the module docstring.
RELATIVE_TOLERANCE = 0.02
ABSOLUTE_TOLERANCE_USD_M = 1_000.0

#: The summary row that carries ``S00900`` (with ``S00300``) in BEA's tables.
REST_OF_WORLD_SUMMARY_ROW = 'Other'

Reference = ta.Literal['as_read', 'latest']


def default_build() -> str:
    """The balanced build the canonical config ships, e.g. ``v0.5.0_a166975``."""
    from bedrock.utils.config.usa_config import get_usa_config  # noqa: PLC0415

    build = get_usa_config().nowcast_mut_vintage
    if not build:
        raise ValueError('the active config names no nowcast_mut_vintage; pass --build')
    return build


@functools.cache
def balanced_use(year: int, build: str) -> pd.DataFrame:
    """``Balanced_Detail_Use_SUT_<year>_<build>``, $M, fetched from GCS if absent."""
    from bedrock.transform.iot.nowcast_sut_assembly import (  # noqa: PLC0415
        GCS_BALANCED_SUT_DIR,
    )
    from bedrock.utils.config.settings import FBS_DIR  # noqa: PLC0415
    from bedrock.utils.io.gcp import download_gcs_file  # noqa: PLC0415

    name = f'Balanced_Detail_Use_SUT_{year}_{build}.parquet'
    path = Path(FBS_DIR) / name
    if not path.exists():
        download_gcs_file(name, GCS_BALANCED_SUT_DIR, str(path))
    return pd.read_parquet(path).astype(float)


@functools.cache
def published_use(year: int, reference: Reference) -> pd.DataFrame:
    """BEA's summary Use SUT for *year*, $M.

    ``as_read`` goes through the build's own loader and its vintage pin;
    ``latest`` reads the 1997-2024 workbook for every year.
    """
    import bedrock.extract.iot.io_2017 as io  # noqa: PLC0415
    from bedrock.utils.taxonomy.bea.matrix_mappings import (  # noqa: PLC0415
        USA_SUMMARY_SUT_MAPPING_1997_2024,
    )

    if reference == 'as_read':
        frame = io._load_usa_summary_sut('Use_SUT_summary', year)  # type: ignore[arg-type]
    else:
        frame = io.load_from_gcs(
            name=USA_SUMMARY_SUT_MAPPING_1997_2024['Use_SUT_summary'],
            sub_bucket=io.GCS_USA_SUP_DIR,
            local_dir=io.LOCAL_USA_SUP_DIR,
            loader=lambda pth: pd.read_excel(
                pth, sheet_name=str(year), skiprows=5, dtype={'Unnamed: 0': str}
            ),
        ).set_index('Unnamed: 0')
    frame.index = pd.Index([str(i).strip() for i in frame.index])
    frame.columns = pd.Index([str(c).strip() for c in frame.columns])
    return frame.replace('...', 0).apply(pd.to_numeric, errors='coerce').fillna(0.0)


def column_totals(
    build: str, reference: Reference = 'as_read', years: ta.Iterable[int] = YEARS
) -> pd.DataFrame:
    """One row per (year, summary final-demand column): ours, BEA's, the gap. $M."""
    commodities = set(USA_2017_COMMODITY_CODES)
    rows = []
    for year in years:
        use = balanced_use(year, build)
        detail_fd = [
            c for c in use.columns if c[:4] in USA_2017_SUMMARY_FINAL_DEMAND_CODES
        ]
        ours = (
            use.loc[[r for r in use.index if r in commodities], detail_fd]
            .sum()
            .groupby(lambda code: code[:4])
            .sum()
        )
        published = published_use(year, reference)
        # F050 imports is a MUT column, not part of the SUT Use table.
        codes = [
            c for c in USA_2017_SUMMARY_FINAL_DEMAND_CODES if c in published.columns
        ]
        theirs = published.reindex(index=list(USA_2017_SUMMARY_COMMODITY_CODES))[
            codes
        ].sum()
        rest_of_world = published.loc[REST_OF_WORLD_SUMMARY_ROW]
        for code in codes:
            rows.append(
                {
                    'year': year,
                    'column': code,
                    'ours': float(ours.get(code, 0.0)),
                    'published': float(theirs[code]),
                    'published_rest_of_world': float(rest_of_world[code]),
                }
            )
    table = pd.DataFrame(rows)
    table['gap'] = table['ours'] - table['published']
    table['relative_gap'] = table['gap'] / table['published'].abs().where(
        table['published'] != 0
    )
    table['fails'] = (table['gap'].abs() > ABSOLUTE_TOLERANCE_USD_M) & (
        table['relative_gap'].abs() > RELATIVE_TOLERANCE
    )
    return table


def main(build: str | None, reference: Reference, check: bool) -> int:
    build = build or default_build()
    table = column_totals(build, reference)
    print(f'final-demand column totals, build {build}, reference {reference!r}\n')

    print('relative gap, ours vs published (columns that ever exceed 0.5%):')
    relative = table.pivot(index='column', columns='year', values='relative_gap')
    moving = relative[(relative.abs() > 0.005).any(axis=1)]
    print(moving.to_string(float_format=lambda x: f'{x:+.1%}'))

    print('\ngap, $bn:')
    print(
        (table.pivot(index='column', columns='year', values='gap') / 1e3)
        .loc[moving.index]
        .to_string(float_format=lambda x: f'{x:+.1f}')
    )

    print(
        f'\nBEA {REST_OF_WORLD_SUMMARY_ROW!r} row (carries the rest-of-world '
        'adjustment S00900), $bn:'
    )
    print(
        (
            table[table['column'].isin(['F010', 'F040'])].pivot(
                index='column', columns='year', values='published_rest_of_world'
            )
            / 1e3
        ).to_string(float_format=lambda x: f'{x:+.1f}')
    )

    failures = table[table['fails']]
    if failures.empty:
        print('\nOK: every column within 2% or 1bn of BEA, every year')
        return 0
    print(
        f'\n{len(failures)} column-year(s) outside both 2% and 1bn of BEA:\n'
        + failures[['year', 'column', 'gap', 'relative_gap']].to_string(
            index=False,
            formatters={
                'gap': lambda x: f'{x / 1e3:+.1f}bn',
                'relative_gap': lambda x: f'{x:+.1%}',
            },
        )
    )
    return 1 if check else 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--build',
        help='balanced build label, e.g. v0.5.0_a166975 (default: the canonical '
        "config's nowcast_mut_vintage)",
    )
    parser.add_argument(
        '--reference',
        choices=('as_read', 'latest'),
        default='as_read',
        help='as_read: the workbook the build reads (default); latest: the '
        '1997-2024 workbook for every year (#1157)',
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='exit non-zero if any column-year is outside both 2%% and 1bn',
    )
    raise SystemExit(main(**vars(parser.parse_args())))
