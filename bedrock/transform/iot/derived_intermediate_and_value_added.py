"""Value added and intermediate inputs at BEA 2017 detail, 1997-2024.

BEA publishes both series annually on its "underlying" 191-row industry frame:
``UVA205-A`` in ``ValueAdded.xlsx`` and ``UII205-A`` in
``IntermediateInputs.xlsx``, beside the ``UGO205-A``/``UGO305-A`` gross output
tables this repo already reads. Neither is published at the 402-industry detail
the model runs on, so this module allocates the 138 leaf lines of that frame
down to detail.

Both series are at producer prices: ``UGO205-A = UII205-A + UVA205-A`` holds to
9 million USD across all 191 rows and all 28 years, so ``UVA205-A`` is
``VAPRO``.

Allocation
----------

Value added is allocated; intermediate inputs are the residual
``GO_detail - VAPRO_detail``. For a leaf line ``g`` with detail children ``C``
and year ``t``::

    weight_c(t) = VAPRO_c(2017) / GO_c(2017) * GO_c(t)
    VAPRO_c(t)  = VAPRO_g(t) * weight_c(t) / sum(weight_c'(t) for c' in C)
    T005_c(t)   = GO_c(t) - VAPRO_c(t)

``VAPRO_c(2017)`` is the published 2017 detail Use SUT column margin. The
weight holds each industry's 2017 ratio to gross output fixed and moves it on
that industry's own observed detail gross output; the rescale puts the group
back on BEA's published total. At ``t = 2017`` the rescale is the identity and
the result is the published detail column margin, reproduced to 1 million USD
per industry.

Taking intermediate inputs as the residual rather than allocating them
separately is what makes ``GO = T005 + VAPRO`` hold per industry, which is the
form the nowcast's T1 target imposes. It costs nothing in fidelity to BEA:
detail gross output sums to the line exactly, so the residual reproduces
``UII205-A``'s own line totals to 3 million USD as well. It also removes the
suppression problem -- BEA suppresses intermediate inputs in every year on
lines 83 (Customs duties) and 176 (Private households), both single-industry
lines, and the residual recovers the published zero for each.

Signs
-----

``S00201`` (state and local government passenger transit) carries a negative
2017 ``VAPRO`` of -10,069 million USD in the published table, and the derived
series is negative there in every year. That is BEA's number, not an artifact.

The residual intermediate inputs go negative in 11 cells, all in ``5191A0``
(other information services) and all in 2002-2015; the nowcast span 2017-2024
is clear. Allocating intermediate inputs and taking value added as the residual
instead is worse on this axis: 13 spurious negatives across three industries,
reaching -13,117 million USD.

Coverage
--------

The 205-A tables and ``UGO305-A`` both run 1997-2024, so both series are
derivable for that whole span. The anchor is a single 2017 benchmark and does
not bound the range.

Caveat carried from the source workbooks: "The Bureau of Economic Analysis does
not include these detailed estimates in the published tables because their
quality is significantly less than that of the higher level aggregates in which
they are included."
"""

from __future__ import annotations

import functools
import typing as ta

import numpy as np
import pandas as pd

from bedrock.extract.iot.constants import (
    UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING,
)
from bedrock.extract.iot.gdp import (
    SECTOR_NAME_COL,
    UNDERLYING_YEARS,
    load_go_detail,
    load_va_underlying,
)
from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.helpers import SECTOR_CODE_COL, map_detail_table
from bedrock.utils.taxonomy.bea.v2017_industry import USA_2017_INDUSTRY_CODES

#: The benchmark year the within-group weights are anchored on.
ANCHOR_YEAR = 2017

#: Years both source tables publish.
DERIVED_YEARS = UNDERLYING_YEARS

#: Row of the 2017 detail Use SUT the value-added allocation is anchored on.
ANCHOR_ROW = 'VAPRO'


def derive_detail_value_added(year: int) -> pd.Series:
    """``VAPRO`` by BEA 2017 detail industry for one year, million USD."""
    return _column(detail_value_added_panel(), year)


def derive_detail_intermediate_inputs(year: int) -> pd.Series:
    """``T005`` by BEA 2017 detail industry for one year, million USD."""
    return _column(detail_intermediate_inputs_panel(), year)


@functools.cache
def detail_value_added_panel() -> pd.DataFrame:
    """``VAPRO`` by detail industry and year, 402 x 28, million USD."""
    years = [str(year) for year in DERIVED_YEARS]
    lines = sorted(UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING)
    group_values = load_va_underlying().loc[lines, years].astype(float)
    group_values.columns = [int(year) for year in group_values.columns]
    return allocate_underlying_to_detail(
        group_values,
        anchor=value_added_anchor(),
        gross_output=detail_gross_output_panel(),
        mapping=UNDERLYING_LINE_TO_BEA_2017_INDUSTRY_MAPPING,
    )


@functools.cache
def detail_intermediate_inputs_panel() -> pd.DataFrame:
    """``T005`` by detail industry and year, 402 x 28, million USD.

    The residual ``GO - VAPRO``; see the module docstring for why it is not
    allocated from ``UII205-A`` directly.
    """
    value_added = detail_value_added_panel()
    return detail_gross_output_panel().reindex(value_added.index) - value_added


@functools.cache
def detail_gross_output_panel() -> pd.DataFrame:
    """``UGO305-A`` summed to the 402 BEA detail industries, million USD.

    The 414 rows of ``UGO305-A`` carry 402 distinct codes -- electric power
    generation and "all other retail" are each split further than the model's
    schema -- so the rows are summed to code before use.
    """
    detail = map_detail_table(load_go_detail())
    if detail[SECTOR_CODE_COL].isna().any():
        unmapped = detail.loc[detail[SECTOR_CODE_COL].isna(), SECTOR_NAME_COL].tolist()
        raise ValueError(f'UGO305-A rows with no BEA code: {unmapped}')
    years = [str(year) for year in DERIVED_YEARS]
    panel = detail.groupby(SECTOR_CODE_COL)[years].sum().astype(float)
    panel.columns = [int(year) for year in panel.columns]
    panel = panel.reindex(list(USA_2017_INDUSTRY_CODES))
    if panel.isna().to_numpy().any():
        missing = list(panel.index[panel.isna().any(axis=1)])
        raise KeyError(f'UGO305-A is missing detail industries {missing}')
    panel.index.name = 'industry'
    return panel


@functools.cache
def value_added_anchor() -> pd.Series:
    """Published 2017 detail Use SUT ``VAPRO`` by industry, million USD."""
    workbook = _load_2017_detail_supply_use_usa('Use_SUT_detail')
    row = workbook.loc[ANCHOR_ROW]
    if isinstance(row, pd.DataFrame):
        raise ValueError(
            f'{ANCHOR_ROW} matches {len(row)} rows of the 2017 detail Use SUT; '
            'expected 1'
        )
    anchor = row.reindex(list(USA_2017_INDUSTRY_CODES)).astype(float)
    if anchor.isna().any():
        missing = list(anchor.index[anchor.isna()])
        raise KeyError(f'2017 detail Use SUT is missing industries {missing}')
    anchor.index.name = 'industry'
    return anchor


def _column(panel: pd.DataFrame, year: int) -> pd.Series:
    if year not in panel.columns:
        raise ValueError(
            f'no detail series for {year}; published '
            f'{DERIVED_YEARS[0]}-{DERIVED_YEARS[-1]}'
        )
    series = panel[year].copy()
    series.index.name = 'industry'
    series.name = year
    return series


def allocate_underlying_to_detail(
    group_values: pd.DataFrame,
    anchor: pd.Series,
    gross_output: pd.DataFrame,
    mapping: ta.Mapping[int, ta.Sequence[str]],
) -> pd.DataFrame:
    """Spread each underlying line across its detail industries.

    Parameters
    ----------
    group_values
        One row per underlying line, one column per year, million USD.
    anchor
        The :data:`ANCHOR_YEAR` detail column margin by industry, million USD.
    gross_output
        Detail gross output by industry and year, million USD. Must carry
        :data:`ANCHOR_YEAR` and every column of ``group_values``.
    mapping
        Underlying line to the detail industry codes beneath it.

    Returns a frame of detail industries by year, in ``mapping`` code order.
    """
    if ANCHOR_YEAR not in gross_output.columns:
        raise ValueError(f'gross output does not carry the anchor year {ANCHOR_YEAR}')
    missing_years = [y for y in group_values.columns if y not in gross_output.columns]
    if missing_years:
        raise ValueError(f'gross output is missing years {missing_years}')
    suppressed = group_values.index[group_values.isna().any(axis=1)]
    if len(suppressed):
        raise ValueError(f'underlying lines {list(suppressed)} carry suppressed cells')

    codes = [code for line in sorted(mapping) for code in mapping[line]]
    missing_codes = sorted(set(codes) - set(anchor.index))
    if missing_codes:
        raise KeyError(f'anchor is missing industries {missing_codes}')

    # Checked on the denominator rather than on the quotient: a zero
    # denominator against a non-zero anchor gives inf, not NaN, and would
    # otherwise carry through the weights silently.
    base = gross_output[ANCHOR_YEAR].reindex(codes)
    broken = list(base.index[base.isna() | (base == 0)])
    if broken:
        raise ValueError(f'no {ANCHOR_YEAR} gross output for industries {broken}')
    ratio = anchor.reindex(codes) / base

    years = list(group_values.columns)
    weights = gross_output.reindex(index=codes, columns=years).mul(ratio, axis=0)

    out = pd.DataFrame(
        index=pd.Index(codes, name='industry'), columns=years, dtype=float
    )
    for line in sorted(mapping):
        children = list(mapping[line])
        values = group_values.loc[line]
        block = weights.loc[children]
        total = block.sum(axis=0)
        # Dividing by NaN leaves the share NaN in exactly the years whose
        # weights cancel to zero, and nowhere else: neither gross output nor
        # the ratio carries a NaN by this point.
        share = block.div(total.where(total != 0, np.nan), axis=1)
        if share.isna().to_numpy().any():
            # No ratio to carry, so fall back to that year's gross output
            # share, which is never zero.
            fallback = gross_output.reindex(index=children, columns=years)
            share = share.fillna(fallback.div(fallback.sum(axis=0), axis=1))
        out.loc[children, years] = share.mul(values, axis=1).to_numpy()
    return out


__all__ = [
    'ANCHOR_YEAR',
    'DERIVED_YEARS',
    'allocate_underlying_to_detail',
    'derive_detail_intermediate_inputs',
    'derive_detail_value_added',
    'detail_gross_output_panel',
    'detail_intermediate_inputs_panel',
    'detail_value_added_panel',
    'value_added_anchor',
]
