"""Checks on the 191-line to BEA-detail value added and intermediate inputs.

Every number quoted in
:mod:`bedrock.transform.iot.derived_intermediate_and_value_added` is measured
here. Run with no flag for all of them::

    uv run python -m bedrock.analysis.nowcasting.underlying_industry_coverage

The checks need the three ``UGdpByInd`` workbooks and the 2017 detail Use SUT,
so they read GCS and are a CLI rather than a unit test; the allocation
arithmetic itself is covered by
``bedrock/transform/iot/__tests__/test_derived_intermediate_and_value_added.py``.

``--mapping`` is the one that matters when the BEA vintage changes. It rebuilds
the 191-line to detail correspondence from ``UGO205-A`` and ``UGO305-A`` and
compares it to the checked-in constant. A failure there means BEA reordered or
respecified the underlying industry list and the constant must be regenerated.
"""

from __future__ import annotations

import argparse
import typing as ta

import pandas as pd

from bedrock.extract.iot.constants import (
    UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING,
)
from bedrock.extract.iot.gdp import (
    SECTOR_NAME_COL,
    derive_underlying_line_mapping,
    load_go_underlying,
    load_ii_underlying,
    load_va_underlying,
    underlying_leaf_lines,
)
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.derived_intermediate_and_value_added import (
    ANCHOR_YEAR,
    DERIVED_YEARS,
    detail_gross_output_panel,
    detail_intermediate_inputs_panel,
    detail_value_added_panel,
)
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

YEAR_COLUMNS = [str(year) for year in DERIVED_YEARS]


def _row(frame: pd.DataFrame, label: object, columns: ta.Sequence[str]) -> pd.Series:
    """One row of a frame, narrowed to a float Series over ``columns``.

    ``.loc[label, columns]`` is what this means, written so pandas-stubs can
    type it: a scalar label always yields a Series here, and every caller wants
    floats.
    """
    row = ta.cast('pd.Series', frame.loc[ta.cast(ta.Any, label)])
    return row.reindex(list(columns)).astype(float)


def mapping() -> pd.DataFrame:
    """Re-derive the 191-line correspondence and compare to the constant."""
    derived = derive_underlying_line_mapping()
    constant = UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING
    codes = [code for line in sorted(constant) for code in constant[line]]
    disagreeing = [
        line
        for line in sorted(set(derived) | set(constant))
        if derived.get(line) != constant.get(line)
    ]
    return pd.DataFrame(
        [
            {'check': 'leaf lines', 'value': len(underlying_leaf_lines())},
            {'check': 'lines derived', 'value': len(derived)},
            {'check': 'lines in constant', 'value': len(constant)},
            {'check': 'lines disagreeing', 'value': len(disagreeing)},
            {'check': 'detail codes covered', 'value': len(codes)},
            {'check': 'detail codes distinct', 'value': len(set(codes))},
            {'check': 'model schema industries', 'value': len(USA_2017_INDUSTRY_CODES)},
            {
                'check': 'codes outside the schema',
                'value': len(set(codes) - set(USA_2017_INDUSTRY_CODES)),
            },
        ]
    ).set_index('check')


def identity() -> pd.DataFrame:
    """``UGO205-A = UII205-A + UVA205-A`` on the source frame, and at detail."""
    gross_output = load_go_underlying()
    residual = (
        gross_output[YEAR_COLUMNS]
        - load_ii_underlying()[YEAR_COLUMNS]
        - load_va_underlying()[YEAR_COLUMNS]
    )
    detail_residual = (
        detail_gross_output_panel()
        - detail_intermediate_inputs_panel()
        - detail_value_added_panel()
    )
    return pd.DataFrame(
        [
            {
                'frame': 'published 191-row (suppressed cells dropped)',
                'cells': int(residual.notna().to_numpy().sum()),
                'max_abs_$M': float(residual.abs().to_numpy(na_value=0).max()),
            },
            {
                'frame': 'derived 402-industry detail',
                'cells': int(detail_residual.size),
                'max_abs_$M': float(detail_residual.abs().to_numpy().max()),
            },
        ]
    ).set_index('frame')


def reconciliation() -> pd.DataFrame:
    """Derived detail summed back to each of the 138 lines, all 28 years."""
    records = []
    panels = (
        ('VAPRO vs UVA205-A', detail_value_added_panel(), load_va_underlying()),
        ('T005 vs UII205-A', detail_intermediate_inputs_panel(), load_ii_underlying()),
    )
    for name, panel, source in panels:
        worst_line, worst_year, worst = 0, 0, 0.0
        checked = 0
        for line, children in UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING.items():
            built = panel.loc[children].sum(axis=0)
            published = _row(source, line, YEAR_COLUMNS)
            published.index = [int(year) for year in published.index]
            gap = (built - published).abs().dropna()
            checked += len(gap)
            if len(gap) and gap.max() > worst:
                worst = float(gap.max())
                worst_line, worst_year = line, int(ta.cast(int, gap.idxmax()))
        records.append(
            {
                'series': name,
                'cells_checked': checked,
                'max_abs_$M': round(worst, 3),
                'worst_line': worst_line,
                'worst_year': worst_year,
            }
        )
    return pd.DataFrame(records).set_index('series')


def anchor() -> pd.DataFrame:
    """The derived 2017 column against the published detail Use SUT margin."""
    workbook = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    industries = list(USA_2017_INDUSTRY_CODES)
    records = []
    pairs = (
        ('VAPRO', detail_value_added_panel(), 'VAPRO'),
        ('T005', detail_intermediate_inputs_panel(), 'T005'),
    )
    for name, panel, row in pairs:
        built = panel[ANCHOR_YEAR].reindex(industries)
        published = _row(workbook, row, industries)
        gap = (built - published).abs()
        records.append(
            {
                'series': name,
                'built_$M': round(float(built.sum())),
                'published_$M': round(float(published.sum())),
                'max_abs_cell_$M': round(float(gap.max()), 3),
                'cells_over_$1M': int((gap > 1).sum()),
                'worst': str(gap.idxmax()),
            }
        )
    return pd.DataFrame(records).set_index('series')


def totals() -> pd.DataFrame:
    """Economy-wide derived value added against BEA's published GDP."""
    published = _row(load_va_underlying(), 1, YEAR_COLUMNS)
    panel = detail_value_added_panel()
    return pd.DataFrame(
        [
            {
                'year': year,
                'derived_$B': round(float(panel[year].sum()) / 1000, 1),
                'published_$B': round(float(published[str(year)]) / 1000, 1),
                'diff_$M': round(
                    float(panel[year].sum()) - float(published[str(year)]), 1
                ),
            }
            for year in DERIVED_YEARS
        ]
    ).set_index('year')


def signs() -> pd.DataFrame:
    """Where the derived series go negative, and whether BEA does too."""
    workbook = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    industries = list(USA_2017_INDUSTRY_CODES)
    records = []
    pairs = (
        ('VAPRO', detail_value_added_panel(), 'VAPRO'),
        ('T005', detail_intermediate_inputs_panel(), 'T005'),
    )
    for name, panel, row in pairs:
        negative = panel < 0
        published = _row(workbook, row, industries)
        records.append(
            {
                'series': name,
                'negative_cells': int(negative.to_numpy().sum()),
                'industries': ', '.join(panel.index[negative.any(axis=1)]),
                'years': ', '.join(
                    str(int(year)) for year in panel.columns[negative.any(axis=0)]
                ),
                'min_$M': round(float(panel.to_numpy().min()), 1),
                'published_2017_negatives': ', '.join(published.index[published < 0]),
            }
        )
    return pd.DataFrame(records).set_index('series')


def suppression() -> pd.DataFrame:
    """The two lines BEA suppresses, and what the residual recovers for them."""
    intermediate = load_ii_underlying()
    suppressed = intermediate.index[intermediate[YEAR_COLUMNS].isna().any(axis=1)]
    panel = detail_intermediate_inputs_panel()
    records = []
    for line in suppressed:
        children = UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING[int(line)]
        records.append(
            {
                'line': int(line),
                'name': str(intermediate.loc[line, SECTOR_NAME_COL]),
                'industries': ', '.join(children),
                'suppressed_years': int(
                    intermediate.loc[line, YEAR_COLUMNS].isna().sum()
                ),
                'derived_2017_$M': round(
                    float(panel.loc[children, ANCHOR_YEAR].sum()), 3
                ),
                'derived_max_abs_$M': round(
                    float(panel.loc[children].abs().to_numpy().max()), 3
                ),
            }
        )
    return pd.DataFrame(records).set_index('line')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--mapping', action='store_true', help='re-derive the 191-line correspondence'
    )
    parser.add_argument(
        '--identity', action='store_true', help='GO = II + VA, source and derived'
    )
    parser.add_argument(
        '--reconcile',
        action='store_true',
        help='derived detail summed back to the lines',
    )
    parser.add_argument(
        '--anchor', action='store_true', help='2017 vs the published detail Use SUT'
    )
    parser.add_argument(
        '--totals',
        action='store_true',
        help='economy-wide value added vs published GDP',
    )
    parser.add_argument('--signs', action='store_true', help='negative cells')
    parser.add_argument(
        '--suppression', action='store_true', help='the two suppressed lines'
    )
    args = parser.parse_args()
    chosen = any(
        (
            args.mapping,
            args.identity,
            args.reconcile,
            args.anchor,
            args.totals,
            args.signs,
            args.suppression,
        )
    )

    if args.mapping or not chosen:
        print('\n191-line to BEA detail correspondence, re-derived from gross output')
        print('(lines disagreeing must be 0; anything else means regenerate)\n')
        print(mapping().to_string())
    if args.identity or not chosen:
        print('\nGO = II + VA\n')
        print(identity().to_string())
    if args.reconcile or not chosen:
        print("\nDerived detail summed back to BEA's published 191-row lines\n")
        print(reconciliation().to_string())
    if args.anchor or not chosen:
        print(f'\n{ANCHOR_YEAR} vs the published detail Use SUT column margin\n')
        print(anchor().to_string())
    if args.totals or not chosen:
        print('\nEconomy-wide derived value added vs published GDP\n')
        print(totals().to_string())
    if args.signs or not chosen:
        print('\nNegative cells\n')
        print(signs().to_string())
    if args.suppression or not chosen:
        print('\nLines BEA suppresses in UII205-A, recovered as the residual\n')
        print(suppression().to_string())
    print()


if __name__ == '__main__':
    main()
