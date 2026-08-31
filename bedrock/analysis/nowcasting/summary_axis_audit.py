"""Is the detail-to-summary crosswalk still a valid aggregator? (#724, precondition)

``Detail_Supply_<year>`` disaggregates the published **summary** Supply block
onto a detail mix, and :mod:`bedrock.transform.iot.nowcast_supply_go_control`
adds a second control on the same block.  Both rest on one assumption that is
never checked anywhere else: **that summary code ``g`` holds exactly the sum of
the detail codes the crosswalk gives it, in every year.**

That assumption is not safe by construction.  BEA moves content between summary
codes in an annual release without moving it in the benchmark - the case Wes
raised is used-car PCE, described as moving out of *Used goods* and into PCE in
the summary tables while the 2017 benchmark still books it on the used-goods
commodity.  A move of that kind is invisible to every totals check the repo has,
because both sides still add up; it shows only as content crossing a summary
boundary the detail axis does not have.

⚠️ **The used-goods case cannot be seen on the industry axis at all.** Used goods
is a commodity with no industry - ``S00401``/``S00402`` have zero domestic output
by definition - so it moves commodity totals and never appears in gross output.
Any audit that looks only at industry columns will report clean.

The four tests
--------------

1. :func:`vintage_diff` - **the two workbook vintages**, on the years both
   carry.  ``io_2017._load_usa_summary_sut`` pins 2017-2022 to
   ``*_2017-2022_Summary.xlsx`` and 2023-2024 to ``*_1997-2024_Summary.xlsx``,
   so a reclassification introduced in the newer vintage would enter our series
   at the 2022/2023 seam and look economic.
2. :func:`crosswalk_agreement` - the **two independent detail-to-summary
   mappings** the repo holds, compared code by code.
3. :func:`aggregation_identity` - aggregating the published 2017 **detail** SUT
   with that crosswalk against the published 2017 **summary** SUT, on the
   industry block *and* the final-demand block.  2017 is the one year both are
   published, and TEST 1 shows the two vintages agree there exactly, so any
   nonzero cell is a crosswalk or definitional fault rather than a revision.
4. :func:`used_goods_series` - the used-goods row itself, year by year, read
   both consistently and the way the build reads it.

What it found, 2026-08-29, on ``d6ae3c1d``
-------------------------------------------

✅ **The crosswalk is sound and the used-goods move is not present in any table
we read.**

=========================================  ==============================
check                                      result
=========================================  ==============================
BEA concordance vs NAICS crosswalk         402/402 agree, 0 missing
2017 detail -> summary, Supply industries  gross \\|diff\\| **309** on 33.8tn
2017 detail -> summary, Use industries     gross \\|diff\\| **2,205** on 14.9tn
2017 detail -> summary, Use final demand   gross \\|diff\\| **151** on 74.7tn
``Used`` row, every FD column, 2017        matches cell for cell
Supply 2017, old vintage vs new            gross \\|diff\\| **0**
=========================================  ==============================

The ``Used`` commodity's PCE runs 209,786 (2017) -> 223,959 -> 221,823 ->
222,211 -> 311,742 -> 307,942 -> 308,150 -> 274,602 (2024) with no step that
looks like a reclassification; the 2021 jump is the used-vehicle price surge and
it reverses.  The Make/Use (MUT) tables carry a *different level* for the same
row - 81,328 against 209,786 in 2017 - but that gap is stable across the whole
span and is redefinitions, not a break.

⚠️ **One real finding: the vintage seam.** The two workbooks disagree materially
on 2019-2022, and our build straddles them:

===============================  ==========  ==========  ==========
2022 quantity                       old wb      new wb        diff
===============================  ==========  ==========  ==========
Supply ``3361MV`` ``T007``          719,944     691,039     +28,905
Use ``Used`` ``T019``               304,725     325,290     -20,565
Use ``3361MV`` ``T019``           1,467,207   1,485,727     -18,520
===============================  ==========  ==========  ==========

2017 is identical in both, 2018 is identical in both, and 2023-2024 read the new
one, so the seam is confined to 2019-2022.

⚠️ **It does not reach the GO control**, and that is worth stating plainly rather
than leaving to inference: the control redistributes *within* a summary group and
takes the group total from whatever vintage the summary block came from, so a
vintage disagreement in the group total passes through it untouched.  What the
seam does contaminate is any comparison of levels across the 2022/2023 boundary.

Run::

    uv run python -m bedrock.analysis.nowcasting.summary_axis_audit
    uv run python -m bedrock.analysis.nowcasting.summary_axis_audit --check
"""

from __future__ import annotations

import argparse
import typing as ta

import pandas as pd

from bedrock.extract.iot.constants import GCS_USA_SUP_DIR
from bedrock.extract.iot.io_2017 import (
    LOCAL_USA_SUP_DIR,
    _load_2017_detail_supply_use_usa,
    _load_usa_summary_mut,
    _load_usa_summary_sut,
)
from bedrock.utils.io.gcp import load_from_gcs
from bedrock.utils.taxonomy.bea.matrix_mappings import USA_SUMMARY_SUT_YEARS
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES
from bedrock.utils.taxonomy.bea.v2017_final_demand import SUT_FINAL_DEMAND_CODES
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES
from bedrock.utils.taxonomy.mappings.bea_v2017_commodity__bea_v2017_summary import (
    load_bea_v2017_commodity_to_bea_v2017_summary,
)
from bedrock.utils.taxonomy.mappings.bea_v2017_industry__bea_v2017_summary import (
    load_bea_v2017_industry_to_bea_v2017_summary,
)

#: The two Supply-Use workbook vintages, and the years each carries.  Both hold
#: 2017-2022, which is what makes the comparison possible at all.
OLD_VINTAGE = {
    'Supply': 'Supply_Tables_2017-2022_Summary.xlsx',
    'Use': 'Use_Tables_Supply-Use_Framework_2017-2022_Summary.xlsx',
}
NEW_VINTAGE = {
    'Supply': 'Supply_Tables_1997-2024_Summary.xlsx',
    'Use': 'Use_Tables_Supply-Use_Framework_1997-2024_Summary.xlsx',
}

#: Years both workbooks publish.
OVERLAP_YEARS = (2017, 2018, 2019, 2020, 2021, 2022)

#: The whole published span, typed to the loaders' own literal.
SPAN: tuple[USA_SUMMARY_SUT_YEARS, ...] = (
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
)

#: Rounding slack, million USD.  The workbooks are whole millions and the detail
#: and summary tables round independently, so exact agreement is not available.
#: Anything an order of magnitude above this is content, not rounding.
ROUNDING_TOLERANCE = 5_000.0


def _strip(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out.index = out.index.astype(str).str.strip()
    out.columns = out.columns.astype(str).str.strip()
    return out


def _numeric(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.apply(pd.to_numeric, errors='coerce').fillna(0.0)


def _load_vintage(filename: str, year: int) -> pd.DataFrame:
    """One year of one Supply-Use workbook, by file name rather than by pin.

    Deliberately not :func:`~bedrock.extract.iot.io_2017._load_usa_summary_sut`:
    that picks the workbook *by year*, which is the behaviour under test here.
    """
    frame = load_from_gcs(
        name=filename,
        sub_bucket=GCS_USA_SUP_DIR,
        local_dir=LOCAL_USA_SUP_DIR,
        loader=lambda pth: pd.read_excel(
            pth, sheet_name=str(year), skiprows=5, dtype={'Unnamed: 0': str}
        ),
    )
    return _strip(frame.set_index('Unnamed: 0').replace('...', 0).fillna(0))


# ------------------------------------------------------------------- TEST 1


def vintage_diff(table: str, year: int) -> pd.DataFrame:
    """Old workbook minus new workbook, one summary table, one year.

    Rows and columns are the labels the two vintages share.  A reclassification
    shows as a small number of large, offsetting cells; an ordinary revision
    shows as broad movement with a level change to match.
    """
    old = _load_vintage(OLD_VINTAGE[table], year)
    new = _load_vintage(NEW_VINTAGE[table], year)
    rows = [r for r in old.index if r in new.index and r != 'IOCode']
    cols = [
        c
        for c in old.columns
        if c in new.columns
        and not c.startswith(('Commodities', 'Industries', 'Unnamed'))
    ]
    return _numeric(old.loc[rows, cols]) - _numeric(new.loc[rows, cols])


# ------------------------------------------------------------------- TEST 2


def crosswalk_agreement() -> pd.DataFrame:
    """The two detail-to-summary mappings, compared on both axes.

    ``bea_v2017_{industry,commodity}__bea_v2017_summary`` read BEA's own
    concordance workbook; :func:`~bedrock.analysis.nowcasting.pxi_mix_test._detail_to_summary`
    reads the repo's NAICS crosswalk CSV.  They are built from different files
    and are used interchangeably across the nowcast, so a disagreement would be
    silent.

    ⚠️ **Compare each axis over its own code list.**  The industry concordance
    has no entry for the four commodity-only codes (``S00300``, ``S00401``,
    ``S00402``, ``S00900``) or for the final-demand and value-added codes, and
    the NAICS crosswalk does - so comparing over the union manufactures 27
    "disagreements" that are only one mapping being asked a question it is not
    for.  ``S00401``/``S00402`` are exactly the used-goods codes this audit
    exists for, so they must be checked on the **commodity** concordance.
    """
    from bedrock.analysis.nowcasting.pxi_mix_test import (  # noqa: PLC0415
        _detail_to_summary,
    )

    naics = _detail_to_summary()
    rows = []
    for axis, codes, loader in (
        (
            'industry',
            USA_2017_INDUSTRY_CODES,
            load_bea_v2017_industry_to_bea_v2017_summary,
        ),
        (
            'commodity',
            USA_2017_COMMODITY_CODES,
            load_bea_v2017_commodity_to_bea_v2017_summary,
        ),
    ):
        concordance: dict[str, str] = {
            str(code): parents[0]
            for code, parents in loader().items()
            if len(parents) == 1
        }
        for code in codes:
            left, right = concordance.get(str(code)), naics.get(str(code))
            if left != right:
                rows.append(
                    {
                        'axis': axis,
                        'detail': code,
                        'bea_concordance': left,
                        'naics_crosswalk': right,
                    }
                )
    return pd.DataFrame(
        rows, columns=['axis', 'detail', 'bea_concordance', 'naics_crosswalk']
    )


# ------------------------------------------------------------------- TEST 3


def _roll(
    frame: pd.DataFrame, rowmap: dict[str, str], colmap: dict[str, str]
) -> pd.DataFrame:
    numeric = _numeric(_strip(frame))
    rows = pd.Series(numeric.index, index=numeric.index).map(rowmap)
    rolled = numeric[rows.notna()].groupby(rows[rows.notna()]).sum()
    cols = pd.Series(rolled.columns, index=rolled.columns).map(colmap)
    kept = rolled.loc[:, cols.notna()]
    return kept.T.groupby(cols[cols.notna()]).sum().T


def _final_demand_map() -> dict[str, str]:
    """Detail final-demand codes to their summary parents, by prefix.

    Detail codes are six characters (``F01000``) against the summary's four
    (``F010``), and the prefix is unambiguous: no summary code is a prefix of
    another.  ``T001`` and ``T019`` are carried through unchanged so the two
    margins are checked alongside the cells.
    """
    summary = _strip(_load_usa_summary_sut('Use_SUT_summary', 2017))
    parents = [c for c in summary.columns if c.startswith('F')]
    mapping: dict[str, str] = {'T001': 'T001', 'T019': 'T019'}
    for code in SUT_FINAL_DEMAND_CODES:
        hits = [p for p in parents if str(code).startswith(p)]
        if len(hits) != 1:
            raise ValueError(
                f'{code} maps to {hits} summary final-demand codes; the prefix '
                f'rule needs exactly one. BEA changed the summary FD axis.'
            )
        mapping[str(code)] = hits[0]
    return mapping


def aggregation_identity(block: str) -> pd.DataFrame:
    """Aggregated published 2017 detail minus published 2017 summary.

    ``block`` is one of ``'supply'``, ``'use'`` (the industry columns) or
    ``'use_fd'`` (the final-demand columns, where the used-goods case lives).
    """
    commodity_map: dict[str, str] = {
        str(code): parents[0]
        for code, parents in load_bea_v2017_commodity_to_bea_v2017_summary().items()
    }
    industry_map: dict[str, str] = {
        str(code): parents[0]
        for code, parents in load_bea_v2017_industry_to_bea_v2017_summary().items()
    }
    names: dict[
        str,
        tuple[
            ta.Literal['Supply_detail', 'Use_SUT_detail'],
            ta.Literal['Supply_summary', 'Use_SUT_summary'],
            dict[str, str] | None,
        ],
    ] = {
        'supply': ('Supply_detail', 'Supply_summary', industry_map),
        'use': ('Use_SUT_detail', 'Use_SUT_summary', industry_map),
        'use_fd': ('Use_SUT_detail', 'Use_SUT_summary', None),
    }
    detail_name, summary_name, column_map = names[block]
    if column_map is None:
        column_map = _final_demand_map()

    got = _roll(
        _load_2017_detail_supply_use_usa(detail_name), commodity_map, column_map
    )
    published = _numeric(_strip(_load_usa_summary_sut(summary_name, 2017)))
    rows = [r for r in got.index if r in published.index]
    cols = [c for c in got.columns if c in published.columns]
    return got.loc[rows, cols] - published.loc[rows, cols]


# ------------------------------------------------------------------- TEST 4


def used_goods_series() -> pd.DataFrame:
    """The used-goods row across the span, three ways.

    ``sut_*`` reads the newer workbook for every year, so it is one consistent
    vintage; ``build_*`` reads what the pinned loader actually hands the build;
    ``mut_*`` is the Make/Use table, a different object carrying the same name.
    """
    rows = []
    for year in SPAN:
        consistent_use = _load_vintage(NEW_VINTAGE['Use'], year)
        consistent_supply = _load_vintage(NEW_VINTAGE['Supply'], year)
        build_use = _strip(_load_usa_summary_sut('Use_SUT_summary', year))
        make_use = _strip(_load_usa_summary_mut('Use_summary', year))

        def cell(frame: pd.DataFrame, row: str, column: str) -> float:
            if row not in frame.index or column not in frame.columns:
                return float('nan')
            return float(pd.to_numeric(frame.loc[row, column], errors='coerce'))

        rows.append(
            {
                'year': year,
                'sut_Used_F010': cell(consistent_use, 'Used', 'F010'),
                'sut_Used_T019': cell(consistent_use, 'Used', 'T019'),
                'sut_Used_T007': cell(consistent_supply, 'Used', 'T007'),
                'sut_3361MV_F010': cell(consistent_use, '3361MV', 'F010'),
                'build_Used_F010': cell(build_use, 'Used', 'F010'),
                'build_Used_T019': cell(build_use, 'Used', 'T019'),
                'mut_Used_F010': cell(make_use, 'Used', 'F010'),
            }
        )
    frame = pd.DataFrame(rows).set_index('year')
    frame['vintage_seam'] = frame['build_Used_T019'] - frame['sut_Used_T019']
    frame['pce_yoy_%'] = 100 * frame['sut_Used_F010'].pct_change()
    return frame


# ------------------------------------------------------------------- report


def audit() -> dict[str, object]:
    """Every test, as one dictionary of results."""
    return {
        'crosswalk': crosswalk_agreement(),
        'supply_2017': aggregation_identity('supply'),
        'use_2017': aggregation_identity('use'),
        'use_fd_2017': aggregation_identity('use_fd'),
        'vintage_supply_2017': vintage_diff('Supply', 2017),
        'vintage_supply_2022': vintage_diff('Supply', 2022),
        'vintage_use_2022': vintage_diff('Use', 2022),
        'used_goods': used_goods_series(),
    }


def failures(results: dict[str, object]) -> list[str]:
    """The findings that would invalidate crosswalk-based aggregation."""
    problems: list[str] = []
    crosswalk = results['crosswalk']
    assert isinstance(crosswalk, pd.DataFrame)
    if not crosswalk.empty:
        problems.append(
            f'{len(crosswalk)} detail codes get different summary parents from '
            f'the two mappings: {sorted(crosswalk["detail"])[:10]}'
        )
    for key, label, scale in (
        ('supply_2017', 'Supply industry block', 33_772_555),
        ('use_2017', 'Use industry block', 14_856_021),
        ('use_fd_2017', 'Use final-demand block', 74_672_869),
    ):
        frame = results[key]
        assert isinstance(frame, pd.DataFrame)
        gross = float(frame.abs().to_numpy().sum())
        if gross > ROUNDING_TOLERANCE:
            problems.append(
                f'{label}: aggregated detail misses published summary by '
                f'{gross:,.0f} $M on {scale:,} - beyond rounding, so the '
                f'crosswalk no longer aggregates that block'
            )
    vintage_2017 = results['vintage_supply_2017']
    assert isinstance(vintage_2017, pd.DataFrame)
    gross_2017 = float(vintage_2017.abs().to_numpy().sum())
    if gross_2017 > ROUNDING_TOLERANCE:
        problems.append(
            f'the two workbook vintages disagree on 2017 by {gross_2017:,.0f} $M; '
            f'the benchmark year was supposed to be fixed, so the aggregation '
            f'identity below is no longer measuring what it claims'
        )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--check',
        action='store_true',
        help='exit nonzero if the crosswalk no longer aggregates the published tables',
    )
    parser.add_argument('--top', type=int, default=10, help='rows to list per detail')
    args = parser.parse_args()
    pd.set_option('display.width', 220)

    results = audit()
    crosswalk = results['crosswalk']
    assert isinstance(crosswalk, pd.DataFrame)

    print('\n=== TEST 2: the two detail-to-summary mappings ===')
    if crosswalk.empty:
        print(
            '  402/402 on both axes agree; no detail code is missing a parent '
            'in either mapping.'
        )
    else:
        print(crosswalk.to_string(index=False))

    print(
        '\n=== TEST 3: aggregated published 2017 detail vs published 2017 summary ==='
    )
    for key, label in (
        ('supply_2017', 'Supply, industry columns'),
        ('use_2017', 'Use, industry columns'),
        ('use_fd_2017', 'Use, final-demand columns'),
    ):
        frame = results[key]
        assert isinstance(frame, pd.DataFrame)
        print(
            f'  {label:<28} cells {frame.shape}  gross |diff| '
            f'{frame.abs().to_numpy().sum():>12,.0f}  max cell '
            f'{frame.abs().to_numpy().max():>6,.0f}'
        )
    used_fd = results['use_fd_2017']
    assert isinstance(used_fd, pd.DataFrame)
    if 'Used' in used_fd.index:
        worst = used_fd.loc['Used'].abs().max()
        print(
            f'  the Used row across every final-demand column: max |diff| {worst:,.0f}'
        )

    print('\n=== TEST 1: workbook vintages, on the years both publish ===')
    for key, label in (
        ('vintage_supply_2017', 'Supply 2017'),
        ('vintage_supply_2022', 'Supply 2022'),
        ('vintage_use_2022', 'Use 2022'),
    ):
        frame = results[key]
        assert isinstance(frame, pd.DataFrame)
        print(
            f'  {label:<12} gross |diff| {frame.abs().to_numpy().sum():>14,.0f}  '
            f'max cell {frame.abs().to_numpy().max():>12,.0f}'
        )
    supply_2022 = results['vintage_supply_2022']
    assert isinstance(supply_2022, pd.DataFrame)
    worst_rows = supply_2022.abs().sum(axis=1).sort_values(ascending=False)
    print('  Supply 2022, largest rows moved by the revision:')
    print(worst_rows.head(args.top).round(0).to_string())

    print('\n=== TEST 4: the used-goods row, year by year ($M) ===')
    used = results['used_goods']
    assert isinstance(used, pd.DataFrame)
    print(used.round(1).to_string())

    problems = failures(results)
    print('')
    if problems:
        for problem in problems:
            print(f'FAIL {problem}')
    else:
        print(
            'OK   the crosswalk aggregates both published 2017 blocks to rounding, '
            'the two mappings agree, and no summary code has moved content it '
            'does not have detail for.'
        )
    return 1 if (problems and args.check) else 0


if __name__ == '__main__':
    raise SystemExit(main())
