"""Nimble comparison of candidate data against BEA reference tables.

Built for reconnaissance, not for provenance: alignment falls back to fuzzy name
matching and reports the weak links rather than demanding an exact crosswalk, so
a new dataset can be sanity-checked against BEA in a few lines.  When a number
needs to be defensible, promote the comparison to a real crosswalk instead.

    from bedrock.analysis.nimble_compare import bea_matrix_row, compare, nipa_sheet

    print(
        compare(
            candidate=nipa_sheet(path, 'T60200D-A', 2017).leaves(),
            reference=bea_matrix_row('V00100'),
            rollup='industry_to_summary',
        ).report()
    )
"""

from bedrock.analysis.nimble_compare.compare import ROLLUPS, Comparison, compare
from bedrock.analysis.nimble_compare.loaders import (
    bea_matrix_column,
    bea_matrix_row,
    bea_summary_sut_row,
    detail_industry_to_summary,
    fba_series,
    frame_series,
    nipa_sheet,
    summary_industry_names,
    table_series,
)
from bedrock.analysis.nimble_compare.matching import Alignment, align
from bedrock.analysis.nimble_compare.series import (
    LabeledSeries,
    normalize_code,
    normalize_name,
)

__all__ = [
    'Alignment',
    'Comparison',
    'LabeledSeries',
    'ROLLUPS',
    'align',
    'bea_matrix_column',
    'bea_matrix_row',
    'bea_summary_sut_row',
    'compare',
    'detail_industry_to_summary',
    'fba_series',
    'frame_series',
    'nipa_sheet',
    'normalize_code',
    'normalize_name',
    'summary_industry_names',
    'table_series',
]
