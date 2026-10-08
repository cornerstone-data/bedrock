"""``NEVER_IMPORTED_COMMODITIES`` against the published 2017 Supply table (#751).

The constant is hand-listed, and it has to be: **104 of 402 commodities carry a
zero 2017 ``MCIF``**, but only about a quarter of them are zero for a
*structural* reason. The rest are one year's observation of a flow that moves,
and freezing those is the error :data:`TRADE_FLOW_SUPPLY_COLUMNS` exists to
undo. Since the split cannot be derived from the zeros alone, something has to
hold the hand-written list to the published table — that is this file.

⚠️ **These tests pin the set, not the build.** ``Trade_Imports_2017`` is clean
on all 27 today - but it is clean *by accident*, because the frozen 2017
``MCIF`` attribution weight happens to be zero on them, so the weight has been
doing the guard's job. #729 and #670 both contemplate changing that weight.
:func:`never_imported_violations` is the check for the build; it lives at the
call sites rather than here.
"""

from __future__ import annotations

import pandas as pd
import pytest

from bedrock.extract.iot.io_2017 import _load_2017_detail_supply_use_usa
from bedrock.transform.iot.nowcast_mask import (
    NEVER_IMPORTED_COMMODITIES,
    NEVER_IMPORTED_TRADE_COMMODITIES,
    NEVER_IMPORTED_TRANSPORT_COMMODITIES,
    structural_zero_mask,
)
from bedrock.utils.taxonomy.bea.v2017_commodity import USA_2017_COMMODITY_CODES

COMMODITIES = list(USA_2017_COMMODITY_CODES)


@pytest.fixture(scope='module')
def published_mcif() -> pd.Series:
    frame = _load_2017_detail_supply_use_usa('Supply_detail')
    frame.columns = frame.columns.str.strip()
    return (
        pd.to_numeric(frame['MCIF'], errors='coerce').reindex(COMMODITIES).fillna(0.0)
    )


def test_every_never_imported_commodity_is_zero_in_the_published_table(
    published_mcif: pd.Series,
) -> None:
    """The pin. If BEA imports one of these, the set is wrong, not the data."""
    missing = [c for c in NEVER_IMPORTED_COMMODITIES if c not in published_mcif.index]
    assert not missing, f'not on the 2017 detail commodity axis: {missing}'
    held = published_mcif.reindex(list(NEVER_IMPORTED_COMMODITIES))
    offending = held[held != 0.0]
    assert offending.empty, (
        'published 2017 MCIF is nonzero for commodities the set calls '
        f'never-imported: {offending.to_dict()}'
    )


def test_the_trade_set_is_the_margin_giving_commodities(
    published_mcif: pd.Series,
) -> None:
    """Wholesale and retail, whose output *is* a margin — plus customs duties."""
    assert len(NEVER_IMPORTED_TRADE_COMMODITIES) == 20
    assert '4200ID' in NEVER_IMPORTED_TRADE_COMMODITIES
    for code in NEVER_IMPORTED_TRADE_COMMODITIES:
        assert code.startswith(('42', '44', '45', '4B')), code


def test_the_transport_set_excludes_the_modes_bea_does_import(
    published_mcif: pd.Series,
) -> None:
    """⚠️ The transport modes are **not** uniform, which is why this is a list.

    ``481000`` air, ``491000`` postal and ``492000`` couriers carry real
    published imports, so a blanket "transport is not imported" rule would be
    wrong. Only the seven BEA publishes at zero are held.
    """
    assert len(NEVER_IMPORTED_TRANSPORT_COMMODITIES) == 7
    for code in ('481000', '491000', '492000'):
        assert code not in NEVER_IMPORTED_COMMODITIES
        assert published_mcif[code] > 0.0, code


def test_most_published_mcif_zeros_are_left_free(published_mcif: pd.Series) -> None:
    """The set must stay far smaller than the zeros, or it is doing Tier 0 again.

    104 commodities have a zero 2017 ``MCIF``; holding all of them is exactly
    the assumption #749 removed. Holding 27 is the structural subset.
    """
    zeros = published_mcif[published_mcif == 0.0]
    assert len(zeros) > 3 * len(NEVER_IMPORTED_COMMODITIES)
    freed = set(zeros.index) - set(NEVER_IMPORTED_COMMODITIES)
    assert len(freed) == len(zeros) - len(NEVER_IMPORTED_COMMODITIES)


def test_the_published_panel_masks_never_imported_and_frees_the_rest() -> None:
    """End to end on the real panel: the narrowing actually reaches the mask."""
    zeros = structural_zero_mask('supply')
    for code in NEVER_IMPORTED_COMMODITIES:
        if code in zeros.index:
            assert bool(zeros.loc[code, 'MCIF']), f'{code} lost its structural zero'
    freed = [
        code
        for code in zeros.index
        if code not in NEVER_IMPORTED_COMMODITIES and not zeros.loc[code, 'MCIF']
    ]
    assert freed, 'no commodity had its MCIF zero freed; the exemption is inert'
