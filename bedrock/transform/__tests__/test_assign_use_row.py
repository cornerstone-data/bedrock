"""Contract for ``assign_use_row_from_clean_parameter`` (#538).

The value-added methods produce Use table *rows* where the final-demand methods
produce Use table *columns*, so the attributed sector has to end up on
``SectorConsumedBy`` and ``clean_parameter`` on ``SectorProducedBy`` -- the
transpose of ``assign_sector_consumed_by_from_clean_parameter``. These pin the
three things that would go wrong silently: the transpose itself, a missing
``clean_parameter``, and an activity set that already populated the column the
transpose writes into.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bedrock.transform.flowbyclean import (
    assign_sector_consumed_by_from_clean_parameter,
    assign_use_row_from_clean_parameter,
)
from bedrock.transform.flowbysector import FlowBySector

_ROW_CODE = 'V00100'


def _fbs(rows: list[dict[str, Any]], clean_parameter: str | None) -> FlowBySector:
    base = {
        'Flowable': 'USD',
        'Class': 'Money',
        'SectorProducedBy': pd.NA,
        'SectorConsumedBy': pd.NA,
        'SectorSourceName': 'BEA_2017_Code',
        'Context': '',
        'Location': '00000',
        'LocationSystem': 'FIPS_2015',
        'FlowAmount': 1.0,
        'Unit': 'Million USD',
        'FlowType': 'TECHNOSPHERE_FLOW',
        'Year': 2017,
        'MetaSources': 'BEA_NIPA',
        'DataReliability': 5.0,
        'TemporalCorrelation': 5.0,
        'GeographicalCorrelation': 5.0,
        'TechnologicalCorrelation': 5.0,
        'DataCollection': 5.0,
    }
    config = {} if clean_parameter is None else {'clean_parameter': clean_parameter}
    return FlowBySector(
        pd.DataFrame([{**base, **row} for row in rows]),
        config=config,
        full_name='test.NIPA_VA_compensation',
    )


def test_transposes_the_attributed_sector_onto_consumed_by() -> None:
    """The industry moves to ``SectorConsumedBy`` and the row code lands on
    ``SectorProducedBy``.

    Failure mode this catches: assigning ``clean_parameter`` to
    ``SectorProducedBy`` without moving the industry first, which overwrites the
    only place the industry is recorded and collapses every cell of the row onto
    one sector pair.
    """
    fbs = _fbs(
        [
            {'SectorProducedBy': '111200', 'FlowAmount': 2497.0},
            {'SectorProducedBy': '531HST', 'FlowAmount': 18920.0},
        ],
        _ROW_CODE,
    )
    out = assign_use_row_from_clean_parameter(fbs)
    assert list(out['SectorProducedBy']) == [_ROW_CODE, _ROW_CODE]
    assert list(out['SectorConsumedBy']) == ['111200', '531HST']
    assert list(out['FlowAmount']) == [2497.0, 18920.0]


def test_is_the_transpose_of_the_final_demand_helper() -> None:
    """Same input, the two helpers fill opposite columns.

    Stated as a test because the pair is easy to reach for by name and get
    backwards: the final-demand one leaves the attributed sector where it is.
    """
    rows = [{'SectorProducedBy': '111200'}]
    demand = assign_sector_consumed_by_from_clean_parameter(_fbs(rows, 'F01000'))
    value_added = assign_use_row_from_clean_parameter(_fbs(rows, _ROW_CODE))
    assert list(demand['SectorProducedBy']) == ['111200']
    assert list(demand['SectorConsumedBy']) == ['F01000']
    assert list(value_added['SectorProducedBy']) == [_ROW_CODE]
    assert list(value_added['SectorConsumedBy']) == ['111200']


def test_requires_clean_parameter() -> None:
    """No ``clean_parameter`` is a config error, not a silently empty row."""
    with pytest.raises(ValueError, match='clean_parameter'):
        assign_use_row_from_clean_parameter(
            _fbs([{'SectorProducedBy': '111200'}], None)
        )


def test_refuses_to_overwrite_a_populated_consumed_by() -> None:
    """A two-sided cell means the transpose would discard a side.

    An activity set that populated ``SectorConsumedBy`` before attribution has
    already corrupted ``PrimarySector`` for a ``TECHNOSPHERE_FLOW`` source
    (#539), so this should fail loudly rather than reorient over the top of it.
    """
    fbs = _fbs(
        [{'SectorProducedBy': '111200', 'SectorConsumedBy': 'F01000'}], _ROW_CODE
    )
    with pytest.raises(ValueError, match='populated SectorConsumedBy'):
        assign_use_row_from_clean_parameter(fbs)
