"""Nimble comparison of candidate data against BEA reference tables.

Built for reconnaissance, not for provenance: alignment falls back to fuzzy name
matching and reports the weak links rather than demanding an exact crosswalk, so
a new dataset can be sanity-checked against BEA in a few lines.  When a number
needs to be defensible, promote the comparison to a real crosswalk instead.

    from bedrock.analysis.compare_NIPA_to_IOT import (
        bea_matrix_row, compare, nipa_flat_table
    )

    print(
        compare(
            candidate=nipa_flat_table('T60200D', 2017).leaves(),
            reference=bea_matrix_row('V00100'),
            rollup='industry_to_summary',
        ).report()
    )
"""

from bedrock.analysis.compare_NIPA_to_IOT.compare import ROLLUPS, Comparison, compare
from bedrock.analysis.compare_NIPA_to_IOT.hierarchy import (
    RESIDUAL_MARKERS,
    RESIDUAL_TOKENS,
    markers_for,
    split_residual,
    token_relation,
)
from bedrock.analysis.compare_NIPA_to_IOT.loaders import (
    BEA_MATRICES,
    FRAMEWORK_NAMES,
    UNAVAILABLE_MATRICES,
    VALUATION_NAMES,
    bea_matrix_column,
    bea_matrix_row,
    describe_matrices,
    detail_industry_to_summary,
    fba_series,
    frame_series,
    nipa_flat_files_path,
    nipa_flat_table,
    nipa_sheet,
    resolve_matrix,
    summary_industry_names,
    table_series,
    where_is,
)
from bedrock.analysis.compare_NIPA_to_IOT.matching import Alignment, align
from bedrock.analysis.compare_NIPA_to_IOT.series import (
    LabeledSeries,
    normalize_code,
    normalize_name,
)

__all__ = [
    'Alignment',
    'Comparison',
    'BEA_MATRICES',
    'FRAMEWORK_NAMES',
    'LabeledSeries',
    'UNAVAILABLE_MATRICES',
    'VALUATION_NAMES',
    'RESIDUAL_MARKERS',
    'RESIDUAL_TOKENS',
    'ROLLUPS',
    'align',
    'bea_matrix_column',
    'bea_matrix_row',
    'compare',
    'describe_matrices',
    'detail_industry_to_summary',
    'fba_series',
    'frame_series',
    'markers_for',
    'nipa_flat_files_path',
    'nipa_flat_table',
    'nipa_sheet',
    'normalize_code',
    'normalize_name',
    'split_residual',
    'resolve_matrix',
    'summary_industry_names',
    'table_series',
    'token_relation',
    'where_is',
]
